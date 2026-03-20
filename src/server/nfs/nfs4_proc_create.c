// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs4_status.h"

static void
chimera_nfs4_create_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct CREATE4args *args = &req->args_compound->argarray[req->index].opcreate;
    struct CREATE4res  *res  = &req->res_compound.resarray[req->index].opcreate;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        return;
    }

    res->status = NFS4_OK;

    res->resok4.attrset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t), req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4.attrset == NULL, "Failed to allocate space");
    res->resok4.num_attrset = chimera_nfs4_mask2attr(set_attr,
                                                     args->createattrs.num_attrmask,
                                                     args->createattrs.attrmask,
                                                     res->resok4.attrset);

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        res->status = NFS4ERR_SERVERFAULT;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, NFS4ERR_SERVERFAULT);
        return;
    }

    memcpy(req->fh, attr->va_fh, attr->va_fh_len);
    req->fhlen = attr->va_fh_len;

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_create_complete */

static void
chimera_nfs4_create_symlink_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct CREATE4args *args = &req->args_compound->argarray[req->index].opcreate;
    struct CREATE4res  *res  = &req->res_compound.resarray[req->index].opcreate;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        return;
    }

    res->status = NFS4_OK;

    res->resok4.attrset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t), req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4.attrset == NULL, "Failed to allocate space");
    res->resok4.num_attrset = chimera_nfs4_mask2attr(attr,
                                                     args->createattrs.num_attrmask,
                                                     args->createattrs.attrmask,
                                                     res->resok4.attrset);

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        res->status = NFS4ERR_SERVERFAULT;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, NFS4ERR_SERVERFAULT);
        return;
    }

    memcpy(req->fh, attr->va_fh, attr->va_fh_len);
    req->fhlen = attr->va_fh_len;

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_create_symlink_complete */

static void
chimera_nfs4_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE4args               *args;
    struct chimera_vfs_attrs         *attr;

    args = &req->args_compound->argarray[req->index].opcreate;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    chimera_nfs4_unmarshall_attrs(attr,
                                  args->createattrs.num_attrmask,
                                  args->createattrs.attrmask,
                                  args->createattrs.attr_vals.data,
                                  args->createattrs.attr_vals.len);

    if (error_code == CHIMERA_VFS_OK) {

        switch (args->objtype.type) {
            case NF4DIR:
                req->handle = handle;
                chimera_vfs_mkdir_at(thread->vfs_thread, &req->cred,
                                     handle,
                                     args->objname.data,
                                     args->objname.len,
                                     attr,
                                     CHIMERA_VFS_ATTR_FH,
                                     CHIMERA_VFS_ATTR_MTIME,
                                     CHIMERA_VFS_ATTR_MTIME,
                                     chimera_nfs4_create_complete,
                                     req);
                break;
            case NF4BLK:
            case NF4CHR:
            case NF4SOCK:
            case NF4FIFO:
                req->handle        = handle;
                attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
                switch (args->objtype.type) {
                    case NF4BLK:
                        attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFBLK;
                        attr->va_rdev = ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                            (uint64_t) args->objtype.devdata.specdata2;
                        break;
                    case NF4CHR:
                        attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFCHR;
                        attr->va_rdev = ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                            (uint64_t) args->objtype.devdata.specdata2;
                        break;
                    case NF4SOCK:
                        attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFSOCK;
                        attr->va_rdev = 0;
                        break;
                    default: /* NF4FIFO */
                        attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFIFO;
                        attr->va_rdev = 0;
                        break;
                } /* switch */
                chimera_vfs_mknod_at(thread->vfs_thread, &req->cred,
                                     handle,
                                     args->objname.data,
                                     args->objname.len,
                                     attr,
                                     CHIMERA_VFS_ATTR_FH,
                                     CHIMERA_VFS_ATTR_MTIME,
                                     CHIMERA_VFS_ATTR_MTIME,
                                     chimera_nfs4_create_complete,
                                     req);
                break;
            case NF4ATTRDIR:
            case NF4NAMEDATTR:
                chimera_nfs4_compound_complete(req, NFS4ERR_NOTSUPP);
                chimera_vfs_release(thread->vfs_thread, handle);
                return;
            case NF4LNK:
                if (args->objtype.linkdata.len == 0) {
                    struct CREATE4res *res = &req->res_compound.resarray[req->index].opcreate;
                    res->status = NFS4ERR_INVAL;
                    chimera_nfs4_compound_complete(req, NFS4ERR_INVAL);
                    chimera_vfs_release(thread->vfs_thread, handle);
                    return;
                }
                req->handle = handle;

                chimera_vfs_symlink_at(
                    thread->vfs_thread,
                    &req->cred,
                    handle,
                    args->objname.data,
                    args->objname.len,
                    args->objtype.linkdata.data,
                    args->objtype.linkdata.len,
                    attr,
                    CHIMERA_VFS_ATTR_FH,
                    CHIMERA_VFS_ATTR_MTIME,
                    CHIMERA_VFS_ATTR_MTIME,
                    chimera_nfs4_create_symlink_complete,
                    req);
                break;
            default:
                chimera_nfs4_compound_complete(req,
                                               NFS4ERR_BADTYPE);
                chimera_vfs_release(thread->vfs_thread, handle);
                return;
        } /* switch */
    } else {
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
    }
} /* chimera_nfs4_create_open_callback */


void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CREATE4args *args = &argop->opcreate;
    struct CREATE4res  *res  = &resop->opcreate;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->objname);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_createattrs(args->createattrs.num_attrmask,
                                                    args->createattrs.attrmask);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_create_open_callback,
                        req);

} /* chimera_nfs4_create */
