// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static int
chimera_nfs4_check_access(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    int                             need_read,
    int                             need_write,
    int                             need_exec)
{
    uint32_t mode = attr->va_mode;
    int      r, w, x;

    if (cred->uid == 0) {
        /* Root can read/write anything; execute requires at least one x bit */
        r = 1;
        w = 1;
        x = !!(mode & (S_IXUSR | S_IXGRP | S_IXOTH));
    } else if ((uint64_t) cred->uid == attr->va_uid) {
        r = !!(mode & S_IRUSR);
        w = !!(mode & S_IWUSR);
        x = !!(mode & S_IXUSR);
    } else {
        int in_group = ((uint64_t) cred->gid == attr->va_gid);

        if (!in_group) {
            for (uint32_t i = 0; i < cred->ngids; i++) {
                if ((uint64_t) cred->gids[i] == attr->va_gid) {
                    in_group = 1;
                    break;
                }
            }
        }

        if (in_group) {
            r = !!(mode & S_IRGRP);
            w = !!(mode & S_IWGRP);
            x = !!(mode & S_IXGRP);
        } else {
            r = !!(mode & S_IROTH);
            w = !!(mode & S_IWOTH);
            x = !!(mode & S_IXOTH);
        }
    }

    if (need_read && !r) {
        return 0;
    }

    if (need_write && !w) {
        return 0;
    }

    if (need_exec && !x) {
        return 0;
    }

    return 1;
} /* chimera_nfs4_check_access */

static void
chimera_nfs4_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    struct ACCESS4args *args   = &req->args_compound->argarray[req->index].opaccess;
    struct ACCESS4res  *res    = &req->res_compound.resarray[req->index].opaccess;
    uint32_t            access = 0;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if ((args->access & ACCESS4_READ) &&
        chimera_nfs4_check_access(attr, &req->cred, 1, 0, 0)) {
        access |= ACCESS4_READ;
    }

    if ((args->access & ACCESS4_LOOKUP) &&
        chimera_nfs4_check_access(attr, &req->cred, 0, 0, 1)) {
        access |= ACCESS4_LOOKUP;
    }

    if ((args->access & ACCESS4_MODIFY) &&
        chimera_nfs4_check_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_MODIFY;
    }

    if ((args->access & ACCESS4_EXTEND) &&
        chimera_nfs4_check_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_EXTEND;
    }

    if ((args->access & ACCESS4_DELETE) &&
        chimera_nfs4_check_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_DELETE;
    }

    if ((args->access & ACCESS4_EXECUTE) &&
        chimera_nfs4_check_access(attr, &req->cred, 0, 0, 1)) {
        access |= ACCESS4_EXECUTE;
    }

    res->status           = NFS4_OK;
    res->resok4.supported = args->access;
    res->resok4.access    = access;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_access_complete */

static void
chimera_nfs4_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_nfs4_access_complete,
                        req);
} /* chimera_nfs4_access_open_callback */

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ACCESS4args *args = &argop->opaccess;
    struct ACCESS4res  *res  = &resop->opaccess;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        res->status           = NFS4_OK;
        res->resok4.supported = args->access;
        res->resok4.access    = args->access;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_access_open_callback,
                        req);
} /* chimera_nfs4_access */
