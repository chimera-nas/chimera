// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <string.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

/* PATHCONF's limits are server constants, independent of the object, but the
 * handle is still validated the way every other file-handle-bearing procedure
 * validates it: decoded (a bogus handle is NFS3ERR_BADHANDLE) then resolved
 * with a GETATTR (a stale handle is NFS3ERR_NOENT).  memfs's open_fh is lazy
 * and does not resolve the inode, so the GETATTR is what actually catches a
 * freed object; its attributes fill obj_attributes, which RFC 1813 3.3.20
 * defines as the post-operation attributes of the argument object. */
static void
chimera_nfs3_pathconf_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct PATHCONF3res               res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);

        res.resok.case_insensitive = 0;
        res.resok.case_preserving  = 1;
        res.resok.no_trunc         = 1;
        res.resok.linkmax          = UINT32_MAX;
        res.resok.name_max         = 255;
        res.resok.chown_restricted = 1;   /* POSIX: chown is superuser-only */
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_pathconf_complete */

static void
chimera_nfs3_pathconf_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct PATHCONF3res               res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs_thread, &req->cred,
                            handle,
                            CHIMERA_NFS3_ATTR_MASK,
                            chimera_nfs3_pathconf_complete,
                            req);
    } else {
        res.status                                   = chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.obj_attributes.attributes_follow = 0;
        rc                                           = shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, NULL, &res,
                                                                                                   req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_pathconf_open_callback */

void
chimera_nfs3_pathconf(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct PATHCONF3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct PATHCONF3res               res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_pathconf(req, args);
    nfs3_trace_pathconf(req, args);

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_pathconf_open_callback,
                        req);
} /* chimera_nfs3_pathconf */
