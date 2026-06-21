// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_listxattrs_complete(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct nfs_request    *req = private_data;
    struct LISTXATTRS4res *res = &req->res_compound.resarray[req->index].oplistxattrs;
    const char            *p   = names;
    uint32_t               i, emitted;
    int                    rc;

    if (error_code != CHIMERA_VFS_OK) {
        /* A too-small maxcount maps to TOOSMALL rather than XATTR2BIG. */
        res->lxr_status = (error_code == CHIMERA_VFS_ERANGE) ?
            NFS4ERR_TOOSMALL : chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    res->lxr_status = NFS4_OK;

    /* The backend returns fully-qualified names from every namespace; RFC 8276
     * scopes NFS xattrs to the user namespace.  Count the user.* names so the
     * result array is sized to what we will actually emit. */
    emitted = 0;
    for (i = 0; i < count; i++) {
        uint32_t namelen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, namelen)) {
            emitted++;
        }
        p += namelen + 1;
    }

    rc = xdr_dbuf_alloc_array(&res->lxr_value, lxr_names, emitted,
                              req->encoding->dbuf);
    if (rc) {
        res->lxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    /* names is a sequence of NUL-terminated names. Strip the "user." prefix and
     * drop non-user namespaces, pointing each result entry directly at the
     * staging buffer (which lives until the reply is sent). */
    p       = names;
    emitted = 0;
    for (i = 0; i < count; i++) {
        uint32_t namelen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, namelen)) {
            res->lxr_value.lxr_names[emitted].xn_name.data =
                (void *) (p + CHIMERA_VFS_XATTR_USER_PREFIX_LEN);
            res->lxr_value.lxr_names[emitted].xn_name.len =
                namelen - CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
            emitted++;
        }
        p += namelen + 1;
    }

    res->lxr_value.lxr_cookie = cookie;
    res->lxr_value.lxr_eof    = eof;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, res->lxr_status);
} /* chimera_nfs4_listxattrs_complete */

static void
chimera_nfs4_listxattrs_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request     *req  = private_data;
    struct LISTXATTRS4args *args = &req->args_compound->argarray[req->index].oplistxattrs;
    struct LISTXATTRS4res  *res  = &req->res_compound.resarray[req->index].oplistxattrs;
    void                   *buffer;
    uint32_t                avail, maxbuf;

    if (error_code != CHIMERA_VFS_OK) {
        res->lxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    req->handle = handle;

    avail  = req->encoding->dbuf->size - req->encoding->dbuf->used;
    maxbuf = avail > 8192 ? avail - 8192 : 0;
    if (maxbuf > args->lxa_maxcount) {
        maxbuf = args->lxa_maxcount;
    }

    buffer = xdr_dbuf_alloc_space(maxbuf, req->encoding->dbuf);
    if (!buffer) {
        res->lxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    chimera_vfs_list_xattrs(req->thread->vfs_thread, &req->cred,
                            handle,
                            args->lxa_cookie,
                            buffer,
                            maxbuf,
                            chimera_nfs4_listxattrs_complete,
                            req);
} /* chimera_nfs4_listxattrs_open_callback */

void
chimera_nfs4_listxattrs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LISTXATTRS4res *res = &resop->oplistxattrs;

    if (req->fhlen == 0) {
        res->lxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_listxattrs_open_callback,
                        req);
} /* chimera_nfs4_listxattrs */
