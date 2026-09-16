// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
static void
chimera_nfs3_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct SETATTR3res                res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    /* The gate's EINVAL is a stand-in: a guarded SETATTR whose ctime had moved
     * owes the client NFS3ERR_NOT_SYNC, and no errno means that. */
    if (req->nfs3_guard_failed) {
        res.status = NFS3ERR_NOT_SYNC;
    }

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.obj_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.obj_wcc, pre_attr, post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    /* The open belonged to the sequence and went with it. */

    nfs_request_free(thread, req);
} /* chimera_nfs3_setattr_complete */

/*
 * The ctime guard, answered while the sequence is still running.
 *
 * RFC 1813's guarded SETATTR is a compare-and-set: the change must not happen
 * if the object's ctime has moved.  A GETATTR ahead of the SETATTR is only
 * half of that -- something has to refuse to go on -- and that is what a gate
 * is.  It answers from the attributes the GETATTR already fetched, so it does
 * no I/O and can be asked again if the sequence is ever retried.
 */
static void
chimera_nfs3_setattr_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct SETATTR3args                  *args = req->args_setattr;
    const struct chimera_vfs_compound_op *op;

    if (*status != CHIMERA_VFS_OK || (int) index != req->nfs3_guard_index) {
        return;
    }

    op = chimera_vfs_compound_op(compound, index);

    if (!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_CTIME) ||
        op->attr.va_ctime.tv_sec  != args->guard.obj_ctime.seconds ||
        op->attr.va_ctime.tv_nsec != args->guard.obj_ctime.nseconds) {
        /* Carried out of band: NFS3ERR_NOT_SYNC has no errno that means it. */
        req->nfs3_guard_failed = 1;
        *status                = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs3_setattr_gate */

static void
chimera_nfs3_setattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              pre_attr, post_attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&pre_attr, 0, sizeof(pre_attr));
    memset(&post_attr, 0, sizeof(post_attr));

    /* A guard that refused stopped the sequence at the GETATTR, so the SETATTR
     * never ran and there is no wcc to report -- which is right: nothing
     * changed. */
    if (!req->nfs3_guard_failed) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        pre_attr  = op->pre_attr;
        post_attr = op->attr;
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_setattr_complete(status, &pre_attr, NULL, &post_attr, req);
} /* chimera_nfs3_setattr_sequence_complete */

void
chimera_nfs3_setattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct chimera_vfs_attrs         *attr;
    struct SETATTR3res                res;
    unsigned int                      open_flags;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_setattr(req, args);
    nfs3_trace_setattr(req, args);

    req->args_setattr = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (args->new_attributes.size.set_it) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    chimera_nfs3_sattr3_to_va(attr, &args->new_attributes);

    req->nfs3_guard_index  = -1;
    req->nfs3_guard_failed = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound, open_flags, 0);

    if (args->guard.check) {
        req->nfs3_guard_index =
            chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_CTIME);
        chimera_vfs_compound_set_gate(compound, chimera_nfs3_setattr_gate, req);
    }

    chimera_vfs_compound_add_setattr(compound, NULL, attr,
                                     CHIMERA_NFS3_ATTR_WCC_MASK,
                                     CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_setattr_sequence_complete, req);

} /* chimera_nfs3_setattr */
