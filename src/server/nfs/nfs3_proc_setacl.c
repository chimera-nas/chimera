// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_posix.h"
#include "nfs3_acl.h"

static void
chimera_nfs3_setacl_complete(
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
    struct SETACL3res                 res;
    int                               rc;

    res.status = (nacl_nfsstat3) chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NACL_NFS3_OK) {
        chimera_nfs3_acl_set_post_op_attr(&res.resok.attr, post_attr);
    }

    rc = shared->nfsacl_v3.send_reply_NFSACLPROC3_SETACL(evpl, NULL, &res,
                                                         req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    chimera_vfs_release(thread->vfs_thread, req->handle);

    nfs_request_free(thread, req);
} /* chimera_nfs3_setacl_complete */

static void
chimera_nfs3_setacl_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct SETACL3args               *args   = req->args_setacl;
    struct SETACL3res                 res;
    struct chimera_vfs_attrs         *attr;
    struct chimera_acl               *acl;
    struct chimera_posix_acl_entry   *pa, *pd;
    int                               rc;

    if (error_code != CHIMERA_VFS_OK) {
        res.status = (nacl_nfsstat3) chimera_vfs_error_to_nfsstat3(error_code);
        rc         = shared->nfsacl_v3.send_reply_NFSACLPROC3_SETACL(
            evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    req->handle = handle;


    /* Reject oversized ACLs before allocating / translating. */
    if (args->num_acl_access > CHIMERA_NFSACL_MAXENTRIES ||
        args->num_acl_default > CHIMERA_NFSACL_MAXENTRIES) {
        res.status = NACL_NFS3ERR_INVAL;
        rc         = shared->nfsacl_v3.send_reply_NFSACLPROC3_SETACL(
            evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        chimera_vfs_release(thread->vfs_thread, req->handle);
        nfs_request_free(thread, req);
        return;
    }

    /* Translate the wire (tag,id,perm) entries into the protocol-neutral POSIX
     * entry struct the translation layer consumes. */
    pa = xdr_dbuf_alloc_space((args->num_acl_access + 1) * sizeof(*pa),
                              req->encoding->dbuf);
    pd = xdr_dbuf_alloc_space((args->num_acl_default + 1) * sizeof(*pd),
                              req->encoding->dbuf);
    acl = xdr_dbuf_alloc_space(chimera_acl_size(CHIMERA_ACL_MAX_ACES),
                               req->encoding->dbuf);
    chimera_nfs_abort_if(!pa || !pd || !acl, "Failed to allocate ACL space");

    for (uint32_t i = 0; i < args->num_acl_access; i++) {
        pa[i].e_tag  = args->acl_access[i].tag & ~CHIMERA_NFS_ACL_DEFAULT;
        pa[i].e_id   = args->acl_access[i].id;
        pa[i].e_perm = args->acl_access[i].perm;
    }
    for (uint32_t i = 0; i < args->num_acl_default; i++) {
        pd[i].e_tag  = args->acl_default[i].tag & ~CHIMERA_NFS_ACL_DEFAULT;
        pd[i].e_id   = args->acl_default[i].id;
        pd[i].e_perm = args->acl_default[i].perm;
    }

    rc = chimera_acl_from_posix(pa, args->num_acl_access,
                                pd, args->num_acl_default,
                                acl, CHIMERA_ACL_MAX_ACES);
    if (rc < 0) {
        res.status = NACL_NFS3ERR_INVAL;
        rc         = shared->nfsacl_v3.send_reply_NFSACLPROC3_SETACL(
            evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        chimera_vfs_release(thread->vfs_thread, req->handle);
        nfs_request_free(thread, req);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    /* Set ATTR_ACL alone: memfs/cairn store the canonical ACL (and re-derive
     * mode from it); the linux backend, which only persists mode, projects the
     * ACL down to mode itself (vfs_acl.h -- documented lossy).  Adding ATTR_MODE
     * here would suppress that projection. */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ACL;
    attr->va_acl      = acl;

    chimera_vfs_setattr(thread->vfs_thread, &req->cred, req->handle,
                        attr, 0, CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_setacl_complete, req);
} /* chimera_nfs3_setacl_open_callback */

void
chimera_nfs3_setacl(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETACL3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred(&req->cred, cred);

    req->args_setacl = args;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        args->fh.data.data,
                        args->fh.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_setacl_open_callback,
                        req);
} /* chimera_nfs3_setacl */
