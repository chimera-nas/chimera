// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/*
 * OPENATTR (RFC 7530 §16.21 / RFC 8881 §18.17): set the current filehandle to
 * the named-attribute directory of the current object.  Chimera projects NFSv4
 * named attributes onto the VFS named-stream primitives (the same backend
 * storage as SMB alternate data streams), so the attr directory is a synthetic
 * server-side object whose handle is the base file's fh with a magic prefix
 * (nfs4_named_attr.h).  Its entries -- the named streams -- are created/listed/
 * removed via open_stream/list_streams/remove_stream when ops run against the
 * attr-dir fh.
 *
 * `createdir` is advisory here: the attr directory is conceptually always
 * present for a streams-capable regular file (entries are created on demand by
 * OPEN), so OPENATTR succeeds regardless and the directory simply enumerates
 * empty until a named attribute is written.
 */

static void
chimera_nfs4_openattr_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request  *req = private_data;
    struct OPENATTR4res *res = &req->res_compound.resarray[req->index].opopenattr;
    uint8_t              attrdir_fh[NFS4_FHSIZE];
    int                  attrdir_len;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The VFS named-stream backend attaches streams only to regular files, so a
     * named-attribute directory exists only for a regular file. */
    if (!S_ISREG(attr->va_mode)) {
        res->status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    attrdir_len = chimera_nfs4_make_attrdir_fh(attrdir_fh, req->fh, req->fhlen);

    memcpy(req->fh, attrdir_fh, attrdir_len);
    req->fhlen = attrdir_len;

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_openattr_getattr_complete */

static void
chimera_nfs4_openattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req = private_data;
    struct OPENATTR4res *res = &req->res_compound.resarray[req->index].opopenattr;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_openattr_getattr_complete,
                        req);
} /* chimera_nfs4_openattr_open_callback */

void
chimera_nfs4_openattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPENATTR4res *res = &resop->opopenattr;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The pseudo-root and an existing attr directory have no named-attribute
     * directory of their own. */
    if (fh_is_nfs4_root(req->fh, req->fhlen) ||
        chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        res->status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Named streams are one VFS feature shared by SMB ADS and NFSv4 named
     * attributes: gate on the same switch, and require the backend that owns
     * this object to support named streams. */
    if (!chimera_server_config_get_named_streams(thread->shared->config) ||
        !(chimera_vfs_module_capabilities(thread->vfs_thread, req->fh, req->fhlen) &
          CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        res->status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_openattr_open_callback,
                        req);
} /* chimera_nfs4_openattr */
