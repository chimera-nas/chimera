// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
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

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.obj_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.obj_wcc, pre_attr, post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    chimera_vfs_release(thread->vfs_thread, req->handle);

    nfs_request_free(thread, req);
} /* chimera_nfs3_setattr_complete */

static void
chimera_nfs3_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct SETATTR3args              *args   = req->args_setattr;
    struct SETATTR3res                res;
    struct chimera_vfs_attrs         *attr;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
        chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

        chimera_nfs3_sattr3_to_va(attr, &args->new_attributes);

        chimera_vfs_setattr(thread->vfs_thread, &req->cred,
                            handle,
                            attr,
                            CHIMERA_NFS3_ATTR_WCC_MASK,
                            CHIMERA_NFS3_ATTR_MASK,
                            chimera_nfs3_setattr_complete,
                            req);

    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.obj_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_setattr_open_callback */

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
    struct nfs_request               *req;
    unsigned int                      open_flags;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_setattr(req, args);

    req->args_setattr = args;

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (args->new_attributes.size.set_it) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
    }

    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     args->object.data.data,
                     args->object.data.len,
                     open_flags,
                     chimera_nfs3_setattr_open_callback,
                     req);

} /* chimera_nfs3_setattr */
