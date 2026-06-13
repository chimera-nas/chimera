// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_posix.h"
#include "nfs3_acl.h"

void
chimera_nfs3_acl_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->nfsacl_v3.send_reply_NFSACLPROC3_NULL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs3_acl_null */

static void
chimera_nfs3_getacl_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct GETACL3args               *args   = req->args_getacl;
    struct GETACL3res                 res;
    int                               rc;

    res.status = (nacl_nfsstat3) chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status != NACL_NFS3_OK) {
        res.attr.attributes_follow = 0;
        goto out;
    }

    {
        /* Synthesise a canonical ACL from mode if the backend returned none
         * (it normally will -- memfs/cairn store it, linux derives it). */
        uint8_t                         synth_buf[sizeof(struct chimera_acl) +
                                                  5 * sizeof(struct chimera_ace)];
        struct chimera_acl             *synth = (struct chimera_acl *) synth_buf;
        const struct chimera_acl       *acl;
        int                             is_dir;
        unsigned                        cap;
        struct chimera_posix_acl_entry *pa, *pd;
        struct nfsacl_entry            *wa, *wd;
        uint32_t                        aclcnt = 0, dfaclcnt = 0;
        uint32_t                        want_access, want_default;

        if (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL && attr->va_acl) {
            acl = attr->va_acl;
        } else {
            chimera_acl_from_mode(attr->va_mode, synth, 5);
            acl = synth;
        }

        is_dir       = S_ISDIR(attr->va_mode);
        want_access  = args->mask & (CHIMERA_NFS_ACL | CHIMERA_NFS_ACLCNT);
        want_default = args->mask & (CHIMERA_NFS_DFACL | CHIMERA_NFS_DFACLCNT);

        cap = acl->num_aces + 8;

        pa = xdr_dbuf_alloc_space(cap * sizeof(*pa), req->encoding->dbuf);
        pd = xdr_dbuf_alloc_space(cap * sizeof(*pd), req->encoding->dbuf);
        wa = xdr_dbuf_alloc_space(cap * sizeof(*wa), req->encoding->dbuf);
        wd = xdr_dbuf_alloc_space(cap * sizeof(*wd), req->encoding->dbuf);
        chimera_nfs_abort_if(!pa || !pd || !wa || !wd,
                             "Failed to allocate ACL space");

        rc = chimera_acl_to_posix(acl, attr->va_uid, attr->va_gid, is_dir,
                                  pa, cap, &aclcnt, pd, cap, &dfaclcnt);
        if (rc < 0) {
            res.status                 = NACL_NFS3ERR_SERVERFAULT;
            res.attr.attributes_follow = 0;
            goto out;
        }

        chimera_nfs3_acl_set_post_op_attr(&res.resok.attr, attr);
        /* The reply mask must echo every requested bit the server honours: the
        * Linux client rejects the reply with EINVAL if (req.mask & res.mask)
        * != req.mask.  We support all four, so reflect them and emit the
        * matching (possibly empty) array -- a directory with no default ACL
        * still answers an NFS_DFACL request with a zero-length default ACL. */
        res.resok.mask = 0;

        if (want_access) {
            for (uint32_t i = 0; i < aclcnt; i++) {
                wa[i].tag  = pa[i].e_tag;
                wa[i].id   = pa[i].e_id;
                wa[i].perm = pa[i].e_perm;
            }
            res.resok.acl_access_count = aclcnt;
            res.resok.num_acl_access   = aclcnt;
            res.resok.acl_access       = aclcnt ? wa : NULL;
            res.resok.mask            |= args->mask &
                (CHIMERA_NFS_ACL | CHIMERA_NFS_ACLCNT);
        } else {
            res.resok.acl_access_count = 0;
            res.resok.num_acl_access   = 0;
            res.resok.acl_access       = NULL;
        }

        /* Echo the requested default bits even when the directory has no
         * default ACL (the client rejects the reply with EINVAL unless
         * res.mask covers req.mask); a directory with no default ACL answers
         * with a zero-length default array, exactly as Linux nfsd does. */
        if (want_default && is_dir) {
            for (uint32_t i = 0; i < dfaclcnt; i++) {
                /* Default-ACL entries carry the NFS_ACL_DEFAULT flag in the
                 * wire tag (Linux fs/nfs_common/nfsacl.c). */
                wd[i].tag  = pd[i].e_tag | CHIMERA_NFS_ACL_DEFAULT;
                wd[i].id   = pd[i].e_id;
                wd[i].perm = pd[i].e_perm;
            }
            res.resok.acl_default_count = dfaclcnt;
            res.resok.num_acl_default   = dfaclcnt;
            res.resok.acl_default       = dfaclcnt ? wd : NULL;
            res.resok.mask             |= args->mask &
                (CHIMERA_NFS_DFACL | CHIMERA_NFS_DFACLCNT);
        } else {
            res.resok.acl_default_count = 0;
            res.resok.num_acl_default   = 0;
            res.resok.acl_default       = NULL;
        }
    }

 out:
    chimera_vfs_release(thread->vfs_thread, req->handle);


    rc = shared->nfsacl_v3.send_reply_NFSACLPROC3_GETACL(evpl, NULL, &res,
                                                         req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_getacl_complete */

static void
chimera_nfs3_getacl_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct GETACL3res                 res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs_thread, &req->cred, handle,
                            CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_ACL,
                            chimera_nfs3_getacl_complete, req);
    } else {
        res.status                 = (nacl_nfsstat3) chimera_vfs_error_to_nfsstat3(error_code);
        res.attr.attributes_follow = 0;
        rc                         = shared->nfsacl_v3.send_reply_NFSACLPROC3_GETACL(
            evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_getacl_open_callback */

void
chimera_nfs3_getacl(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct GETACL3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred(&req->cred, cred);


    req->args_getacl = args;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        args->fh.data.data,
                        args->fh.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_getacl_open_callback,
                        req);
} /* chimera_nfs3_getacl */
