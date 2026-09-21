// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "server/server.h"
#include "vfs/vfs_compound.h"
#include "nfs4_status.h"

/*
 * RENAME as one run.
 *
 *   PUTFH(target dir) OPEN_CURRENT(dir)
 *   PUTFH(source dir) LOOKUP(oldname) RECALL(nowait)
 *   PUTFH(target dir) LOOKUP(newname) RECALL(nowait)
 *   PUTFH(source dir) SAVEFH PUTFH(target dir) RENAME
 *
 * The first open is the target directory's openability check the per-op path
 * made before anything else.  The two LOOKUP/RECALL pairs are RFC 7530
 * §10.4.4: a delegation on the file being moved, and one on the file being
 * renamed over, are recalled before the rename and the client is told to retry.
 * rename_at takes two FILE HANDLES, so the last three ops only put the cursors
 * where the op reads them -- the saved slot for the source, the current one for
 * the target -- and nothing is opened for it.
 */
#define NFS4_RENAME_OP_SRC_LOOKUP 3
#define NFS4_RENAME_OP_SRC_RECALL 4
#define NFS4_RENAME_OP_DST_LOOKUP 6
#define NFS4_RENAME_OP_DST_RECALL 7
#define NFS4_RENAME_OP_RENAME     11

/*
 * A name that does not resolve is not an error here: the source's absence is
 * the rename's own ENOENT to report, and the destination's is the ordinary
 * no-overwrite case.  Either way there is nothing to recall, so the resolve's
 * failure is swallowed and the recall behind it run past -- which is what the
 * per-op path did by ignoring the lookup's status.
 *
 * A caching holder still in the way after the recall is NFS4ERR_DELAY.  The VFS
 * error is only a stop signal; the completion reads which op carried it.
 */
static void
chimera_nfs4_rename_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_compound_op       *edit;

    if (index == NFS4_RENAME_OP_SRC_LOOKUP ||
        index == NFS4_RENAME_OP_DST_LOOKUP) {
        if (*status == CHIMERA_VFS_OK) {
            return;
        }

        edit = chimera_vfs_compound_op_edit(compound, index + 1);

        if (edit) {
            edit->skip = 1;
        }

        *status = CHIMERA_VFS_OK;
        return;
    }

    if ((index == NFS4_RENAME_OP_SRC_RECALL ||
         index == NFS4_RENAME_OP_DST_RECALL) && *status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound, index);

        if (op->recall_still_open) {
            *status = CHIMERA_VFS_EAGAIN;
        }
    }
} /* chimera_nfs4_rename_gate */

static void
chimera_nfs4_rename_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct RENAME4res                    *res = &req->res_compound.resarray[req->index].oprename;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;
    nfsstat4                              status;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        const struct chimera_vfs_compound_op *src =
            chimera_vfs_compound_op(compound, NFS4_RENAME_OP_SRC_RECALL);
        const struct chimera_vfs_compound_op *dst =
            chimera_vfs_compound_op(compound, NFS4_RENAME_OP_DST_RECALL);

        status = (src->status == CHIMERA_VFS_EAGAIN ||
                  dst->status == CHIMERA_VFS_EAGAIN) ?
            NFS4ERR_DELAY : chimera_nfs4_errno_to_nfsstat4(error_code);

        chimera_vfs_compound_free(compound);

        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    /* rename_at reports BOTH directories' change_info -- from_dir_* for the
     * source and dir_* for the target -- which is what the reply has a slot
     * for. */
    op = chimera_vfs_compound_op(compound, NFS4_RENAME_OP_RENAME);

    struct chimera_vfs_attrs from_pre  = op->from_dir_pre_attr;
    struct chimera_vfs_attrs from_post = op->from_dir_post_attr;
    struct chimera_vfs_attrs to_pre    = op->dir_pre_attr;
    struct chimera_vfs_attrs to_post   = op->dir_post_attr;

    chimera_vfs_compound_free(compound);

    res->status = NFS4_OK;

    chimera_nfs4_set_changeinfo(&res->resok4.source_cinfo, &from_pre,
                                &from_post);
    chimera_nfs4_set_changeinfo(&res->resok4.target_cinfo, &to_pre, &to_post);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_rename_complete */

void
chimera_nfs4_rename(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RENAME4args          *args = &argop->oprename;
    struct RENAME4res           *res  = &resop->oprename;
    struct chimera_vfs_compound *compound;
    nfsstat4                     status;

    if (req->fhlen == 0 || req->saved_fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = chimera_nfs4_validate_name(&args->oldname);

    if (status == NFS4_OK) {
        status = chimera_nfs4_validate_name(&args->newname);
    }

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);

    chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen);
    chimera_vfs_compound_add_lookup(compound,
                                    (const char *) args->oldname.data,
                                    (int) args->oldname.len,
                                    CHIMERA_VFS_ATTR_FH, 0);
    chimera_vfs_compound_add_recall(compound, NULL, 0, 0,
                                    CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_lookup(compound,
                                    (const char *) args->newname.data,
                                    (int) args->newname.len,
                                    CHIMERA_VFS_ATTR_FH, 0);
    chimera_vfs_compound_add_recall(compound, NULL, 0, 0,
                                    CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);

    chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen);
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_rename(compound,
                                    (const char *) args->oldname.data,
                                    (int) args->oldname.len,
                                    (const char *) args->newname.data,
                                    (int) args->newname.len,
                                    0,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME);

    /* Without delegations there is nothing to recall, and the per-op path made
     * neither resolve.  Skipping them where they stand keeps every index the
     * gate and the completion were written against. */
    if (!chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        chimera_vfs_compound_op_set_skip(compound, NFS4_RENAME_OP_SRC_LOOKUP, 1);
        chimera_vfs_compound_op_set_skip(compound, NFS4_RENAME_OP_SRC_RECALL, 1);
        chimera_vfs_compound_op_set_skip(compound, NFS4_RENAME_OP_DST_LOOKUP, 1);
        chimera_vfs_compound_op_set_skip(compound, NFS4_RENAME_OP_DST_RECALL, 1);
    }

    chimera_vfs_compound_set_gate(compound, chimera_nfs4_rename_gate, req);

    chimera_vfs_compound_submit(compound, chimera_nfs4_rename_complete, req);
} /* chimera_nfs4_rename */
