// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_mknod_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct MKNOD3res                  res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        if (r_attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                                                r_attr->va_fh,
                                                                r_attr->va_fh_len,
                                                                req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_MKNOD(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mknod_complete */

static void
chimera_nfs3_mknod_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct MKNOD3args                *args   = req->args_mknod;
    struct MKNOD3res                  res;
    struct chimera_vfs_attrs         *attr;
    struct sattr3                    *sattr;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
        chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

        switch (args->what.type) {
            case NF3CHR:
            case NF3BLK:
                sattr = &args->what.device.dev_attributes;
                chimera_nfs3_sattr3_to_va(attr, sattr);
                attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
                attr->va_mode      = (attr->va_mode & ~S_IFMT) | chimera_nfs3_type_to_vfs(args->what.type);
                attr->va_set_mask |= CHIMERA_VFS_ATTR_RDEV;
                attr->va_rdev      = ((uint64_t) args->what.device.spec.specdata1 << 32) |
                    (uint64_t) args->what.device.spec.specdata2;
                break;
            case NF3SOCK:
            case NF3FIFO:
                sattr = &args->what.pipe_attributes;
                chimera_nfs3_sattr3_to_va(attr, sattr);
                attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
                attr->va_mode      = (attr->va_mode & ~S_IFMT) | chimera_nfs3_type_to_vfs(args->what.type);
                attr->va_set_mask |= CHIMERA_VFS_ATTR_RDEV;
                attr->va_rdev      = 0;
                break;
            default:
                chimera_vfs_release(thread->vfs_thread, handle);
                res.status = NFS3ERR_BADTYPE;
                chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
                rc = shared->nfs_v3.send_reply_NFSPROC3_MKNOD(evpl, NULL, &res, req->encoding);
                chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
                nfs_request_free(thread, req);
                return;
        } /* switch */

        chimera_vfs_mknod_at(thread->vfs_thread, &req->cred,
                             handle,
                             args->where.name.str,
                             args->where.name.len,
                             attr,
                             CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                             CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                             CHIMERA_NFS3_ATTR_MASK,
                             chimera_nfs3_mknod_complete,
                             req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_MKNOD(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_mknod_open_callback */

void
chimera_nfs3_mknod(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct MKNOD3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_mknod(req, args);

    req->args_mknod = args;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        args->where.dir.data.data,
                        args->where.dir.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_mknod_open_callback,
                        req);
} /* chimera_nfs3_mknod */
