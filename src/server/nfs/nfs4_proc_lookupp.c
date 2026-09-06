// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_mount_table.h"

static bool
chimera_nfs4_fh_is_vfs_mount_root(
    struct chimera_vfs *vfs,
    const uint8_t      *fh,
    uint32_t            fhlen)
{
    struct chimera_vfs_mount *mount;
    bool                      is_root = false;

    if (fhlen < CHIMERA_VFS_MOUNT_ID_SIZE) {
        return false;
    }

    urcu_qsbr_read_lock();
    mount = chimera_vfs_mount_table_lookup(vfs->mount_table, fh);
    if (mount &&
        mount->pathlen > 0 &&
        mount->root_fh_len == (int) fhlen &&
        memcmp(mount->root_fh, fh, fhlen) == 0) {
        is_root = true;
    }
    urcu_qsbr_read_unlock();

    return is_root;
} /* chimera_nfs4_fh_is_vfs_mount_root */

static void
chimera_nfs4_lookupp_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUPP4res *res    = &req->res_compound.resarray[req->index].oplookupp;

    if (error_code == CHIMERA_VFS_OK) {
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            status = NFS4ERR_SERVERFAULT;
        } else {
            memcpy(req->fh, attr->va_fh, attr->va_fh_len);
            req->fhlen = attr->va_fh_len;
        }
    }

    res->status = status;
    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lookupp_complete */

static void
chimera_nfs4_lookupp_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUPP4res *res    = &req->res_compound.resarray[req->index].oplookupp;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        /*
         * Resolve parent via lookup_at(handle, "..").  Backends that
         * understand directory structure (memfs, cairn, diskfs) handle
         * ".." natively; pass-through backends (linux, io_uring) get
         * parent attrs from fstatat(parent_fd, "..").
         */
        chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, req->compound,
                              handle,
                              "..",
                              2,
                              CHIMERA_VFS_ATTR_FH,
                              0,
                              chimera_nfs4_lookupp_complete,
                              req);
    } else {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
    }
} /* chimera_nfs4_lookupp_open_callback */

/*
 * Continuation once the "/" export's root FH is known (root_fh == NULL when
 * there is no "/" export, or when its path failed to resolve).
 */
static void
chimera_nfs4_lookupp_continue(
    enum chimera_vfs_error            error_code,
    const uint8_t                    *root_fh,
    uint32_t                          root_fh_len,
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct LOOKUPP4res *res = &req->res_compound.resarray[req->index].oplookupp;

    (void) error_code;

    /*
     * RFC 7530 §16.10.5: LOOKUPP with the current filehandle at the root of
     * the namespace has no parent to return, so it fails with NFS4ERR_NOENT.
     * With a "/" export the namespace root is its real backend root.
     */
    if (root_fh != NULL &&
        root_fh_len == (uint32_t) req->fhlen &&
        memcmp(root_fh, req->fh, root_fh_len) == 0) {
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /*
     * Export roots are mounted as entries in the NFSv4 namespace root.  A
     * LOOKUPP from such a filehandle must return the namespace root FH -- the
     * "/" export's real root when one is configured, the synthetic
     * pseudo-root otherwise -- not the backend's physical parent handle.
     */
    if (chimera_nfs4_fh_is_vfs_mount_root(thread->vfs, req->fh, req->fhlen)) {
        struct chimera_nfs_export root_export;
        uint32_t                  fhlen;

        if (thread->shared->root_export_id != 0) {
            if (root_fh == NULL ||
                chimera_nfs_get_export_copy(thread->shared, "/",
                                            &root_export) != 0) {
                /* The parent is the "/" export's root but it did not
                 * resolve; there is no correct handle to return. */
                res->status = NFS4ERR_SERVERFAULT;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }

            /* Crossing back into the "/" export is an export entry like any
             * other: its security policy and identity apply. */
            if (!chimera_nfs_export_sec_ok(&root_export, req->sec_bit)) {
                res->status = NFS4ERR_WRONGSEC;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }

            chimera_nfs_set_export(req, &root_export);

            memcpy(req->fh, root_fh, root_fh_len);
            req->fhlen  = root_fh_len;
            res->status = NFS4_OK;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        nfs4_root_get_fh(req->fh, &fhlen);
        req->fhlen  = fhlen;
        res->status = NFS4_OK;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->compound,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_lookupp_open_callback,
                        req);
} /* chimera_nfs4_lookupp_continue */

void
chimera_nfs4_lookupp(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOOKUPP4res *res = &resop->oplookupp;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /*
     * RFC 7530 §16.10.5: LOOKUPP with the current filehandle at the root of
     * the namespace has no parent to return, so it fails with NFS4ERR_NOENT.
     */
    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (thread->shared->root_export_id != 0) {
        /* A "/" export exists: its root FH is needed both to recognize the
         * namespace root (parent-less) and as the parent of sibling export
         * roots. */
        nfs4_root_export_fh_get(thread, req, chimera_nfs4_lookupp_continue);
        return;
    }

    chimera_nfs4_lookupp_continue(CHIMERA_VFS_ENOENT, NULL, 0, thread, req);
} /* chimera_nfs4_lookupp */
