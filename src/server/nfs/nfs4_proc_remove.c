// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <inttypes.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
#include "nfs_internal.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_pnfs.h"

/* REMOVE of a name inside a named-attribute directory deletes the named stream
 * of that name from the base file: the base PATH-opened, and REMOVE_STREAM
 * against it.  One run. */
static void
chimera_nfs4_remove_attrdir_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request    *req = private_data;
    struct REMOVE4res     *res =
        &req->res_compound.resarray[req->index].opremove;
    enum chimera_vfs_error error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* change_info4 is non-atomic and best-effort here (mirrors the ordinary
     * REMOVE path, which also leaves it zeroed). */
    res->resok4.cinfo.atomic = 0;
    res->resok4.cinfo.before = 0;
    res->resok4.cinfo.after  = 0;

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_remove_attrdir_complete */

static void
chimera_nfs4_remove_attrdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct REMOVE4args          *args =
        &req->args_compound->argarray[req->index].opremove;
    struct chimera_vfs_compound *compound;
    const uint8_t               *base;
    int                          base_len;

    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, base, base_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_remove_stream(compound,
                                           (const char *) args->target.data,
                                           (int) args->target.len);

    chimera_vfs_compound_submit(compound, chimera_nfs4_remove_attrdir_complete,
                                req);
} /* chimera_nfs4_remove_attrdir */


/*
 * REMOVE.  When pNFS is enabled the target may be a flex-files file whose data
 * lives in a backing file on a data server; removing the last link must also
 * delete that backing file or it leaks.  The pNFS layout state is an opaque
 * attribute on the file (CHIMERA_VFS_ATTR_PNFS_LAYOUT, {deviceid, backing-fh}),
 * so the target is LOOKed UP first to capture it, the MDS file removed, and
 * then -- if it was the last link to a pNFS-backed file -- the backing file on
 * the steered data server deleted (best effort: the MDS namespace entry is
 * already gone).
 *
 * The MDS half is ONE run:
 *
 *   PUTFH(parent) SAVEFH OPEN_CURRENT(dir) LOOKUP RECALL(nowait) RESTOREFH
 *   REMOVE
 *
 * SAVEFH parks the parent because the LOOKUP moves the current object onto the
 * victim -- which is what the RECALL must address -- and the REMOVE resolves
 * its name in the parent again.  The data-server half is a second run, on
 * another mount, through a handle the first one never holds: the reason the
 * sequence scan refuses an NFSv4 REMOVE outright while pNFS is enabled.
 */
#define NFS4_REMOVE_OP_LOOKUP 3
#define NFS4_REMOVE_OP_RECALL 4
#define NFS4_REMOVE_OP_REMOVE 6

struct nfs4_remove_ctx {
    struct nfs_request *req;
    uint8_t             deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    uint64_t            fileid;
    int                 have_layout;
    char                backing_name[24];
};

static void
nfs4_remove_finish(
    struct nfs4_remove_ctx *ctx,
    nfsstat4                status)
{
    struct nfs_request *req = ctx->req;
    struct REMOVE4res  *res = &req->res_compound.resarray[req->index].opremove;

    res->status = status;
    chimera_nfs4_compound_complete(req, status);
} /* nfs4_remove_finish */

static void
nfs4_remove_ds_backing_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error(
            "pNFS: failed to delete data-server backing file (err=%d); space leaked",
            error_code);
    }

    /* Best effort: the MDS file is already gone, so a failure to delete the
     * backing file only leaks data-server space; do not fail the REMOVE. */
    nfs4_remove_finish(private_data, NFS4_OK);
} /* nfs4_remove_ds_backing_complete */

/*
 * The gate is where the two decisions that sit BETWEEN the ops are made, and
 * both are decisions the sequence cannot express as an op's own status.
 *
 * On the LOOKUP: capture the layout, and -- when the resolve failed -- run past
 * the recall and let the sequence go on, because the per-op path ignored a
 * failing lookup and unlinked anyway.  The REMOVE is what reports what is
 * actually wrong with the name.
 *
 * On the RECALL: a caching holder still in the way is RFC 7530 SS10.4.4's
 * NFS4ERR_DELAY.  The VFS error is only a stop signal -- the completion reads
 * which op carried it.
 */
static void
chimera_nfs4_remove_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_remove_ctx               *ctx = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_compound_op       *edit;

    if (index == NFS4_REMOVE_OP_LOOKUP) {
        /* Assigned from what this op reported, never accumulated: a second
         * execution computes the same answer. */
        ctx->have_layout = 0;

        if (*status != CHIMERA_VFS_OK) {
            edit = chimera_vfs_compound_op_edit(compound,
                                                NFS4_REMOVE_OP_RECALL);

            if (edit) {
                edit->skip = 1;
            }

            *status = CHIMERA_VFS_OK;
            return;
        }

        op = chimera_vfs_compound_op(compound, index);

        /* Capture the pNFS layout iff this is the last link to the file; a
         * file with remaining hard links must keep its backing data. */
        if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) &&
            op->attr.va_pnfs_len >= CHIMERA_VFS_DEVICEID_SIZE &&
            (op->attr.va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
            op->attr.va_nlink == 1) {
            memcpy(ctx->deviceid, op->attr.va_pnfs, CHIMERA_VFS_DEVICEID_SIZE);
            ctx->fileid = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_INUM) ?
                op->attr.va_ino : 0;
            ctx->have_layout = 1;
        }

        return;
    }

    if (index == NFS4_REMOVE_OP_RECALL && *status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound, index);

        if (op->recall_still_open) {
            *status = CHIMERA_VFS_EAGAIN;
        }
    }
} /* chimera_nfs4_remove_gate */

static void
nfs4_remove_mds_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_remove_ctx               *ctx = private_data;
    struct chimera_vfs                   *vfs = ctx->req->thread->shared->vfs;
    const struct chimera_vfs_ds          *ds;
    const struct chimera_vfs_compound_op *op;
    struct REMOVE4res                    *res =
        &ctx->req->res_compound.resarray[ctx->req->index].opremove;
    struct chimera_vfs_compound          *ds_compound;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        /* The recall's own stop is the only one that is not an error of the
         * unlink: a holder is still in the way and the client retries. */
        op = chimera_vfs_compound_op(compound, NFS4_REMOVE_OP_RECALL);

        nfsstat4 status = (op->status == CHIMERA_VFS_EAGAIN) ?
            NFS4ERR_DELAY : chimera_nfs4_errno_to_nfsstat4(error_code);

        chimera_vfs_compound_free(compound);
        nfs4_remove_finish(ctx, status);
        return;
    }

    /* change_info4 for the parent directory (RFC 7530 §16.25.5), from its
     * change attribute pre/post the unlink. */
    op = chimera_vfs_compound_op(compound, NFS4_REMOVE_OP_REMOVE);

    struct chimera_vfs_attrs pre  = op->dir_pre_attr;
    struct chimera_vfs_attrs post = op->dir_post_attr;

    chimera_vfs_compound_free(compound);

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, &pre, &post);

    if (!ctx->have_layout) {
        nfs4_remove_finish(ctx, NFS4_OK);
        return;
    }

    ds = chimera_vfs_pnfs_find_device(vfs, ctx->deviceid);
    if (!ds || ds->root_fh_len == 0) {
        nfs4_remove_finish(ctx, NFS4_OK);
        return;
    }

    snprintf(ctx->backing_name, sizeof(ctx->backing_name), "%016" PRIx64,
             ctx->fileid);

    chimera_nfs_info("pNFS: deleting data-server backing file %s for removed file",
                     ctx->backing_name);

    ds_compound = chimera_vfs_compound_alloc(ctx->req->thread->vfs_thread,
                                             &ctx->req->cred);

    chimera_vfs_compound_add_putfh(ds_compound, ds->root_fh, ds->root_fh_len);
    chimera_vfs_compound_add_remove(ds_compound, ctx->backing_name,
                                    strlen(ctx->backing_name), 0, 0, 0);

    chimera_vfs_compound_submit(ds_compound, nfs4_remove_ds_backing_complete,
                                ctx);
} /* nfs4_remove_mds_complete */

void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVE4args          *args = &argop->opremove;
    struct REMOVE4res           *res  = &resop->opremove;
    struct nfs4_remove_ctx      *ctx;
    struct chimera_vfs_compound *compound;
    int                          delegations, pnfs;

    req->handle = NULL;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->target);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* REMOVE inside a named-attribute directory: drop the named stream. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        chimera_nfs4_remove_attrdir(thread, req);
        return;
    }

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
    memset(ctx, 0, sizeof(*ctx));
    ctx->req = req;

    /* Resolve the victim first when delegations OR pNFS are in play: the FH is
     * what the recall addresses (RFC 7530 §10.4.4), and the pNFS attrs are what
     * name its data-server backing file afterwards.  With neither, the resolve
     * and the recall are skipped where they stand, so every index the gate and
     * the completion were written against stays put. */
    delegations = chimera_server_config_get_nfs4_delegations(
        thread->shared->config);
    pnfs = chimera_vfs_pnfs_enabled(thread->shared->vfs);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_lookup(compound,
                                    (const char *) args->target.data,
                                    (int) args->target.len,
                                    CHIMERA_VFS_ATTR_FH |
                                    CHIMERA_VFS_ATTR_PNFS_LAYOUT |
                                    CHIMERA_VFS_ATTR_NLINK |
                                    CHIMERA_VFS_ATTR_INUM,
                                    0);
    chimera_vfs_compound_add_recall(compound, NULL, 0, 0,
                                    CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);
    chimera_vfs_compound_add_restorefh(compound);
    chimera_vfs_compound_add_remove(compound,
                                    (const char *) args->target.data,
                                    (int) args->target.len,
                                    0,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME);

    if (!delegations && !pnfs) {
        chimera_vfs_compound_op_set_skip(compound, NFS4_REMOVE_OP_LOOKUP, 1);
    }

    if (!delegations) {
        chimera_vfs_compound_op_set_skip(compound, NFS4_REMOVE_OP_RECALL, 1);
    }

    chimera_vfs_compound_set_gate(compound, chimera_nfs4_remove_gate, ctx);

    chimera_vfs_compound_submit(compound, nfs4_remove_mds_complete, ctx);
} /* chimera_nfs4_remove */
