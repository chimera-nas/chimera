// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/*
 * Stage an RFC 8276 xattr name -- which arrives without a namespace prefix --
 * as the fully-qualified "user." name the VFS expects, in the response dbuf
 * (which outlives the async VFS op).  Shared by all four xattr handlers and by
 * the VFS-compound path, which builds the same name when it encodes the op.
 *
 * Returns NFS4_OK and sets the name and its length, or the status to fail the
 * op with.
 */
nfsstat4
chimera_nfs4_xattr_stage_name(
    struct nfs_request *req,
    const void         *wire_name,
    uint32_t            wire_len,
    char              **name,
    int                *namelen)
{
    uint32_t namecap;
    char    *buf;
    int      len;

    if (wire_len == 0) {
        return NFS4ERR_INVAL;
    }

    namecap = CHIMERA_VFS_XATTR_USER_PREFIX_LEN + wire_len;
    buf     = xdr_dbuf_alloc_space(namecap, req->encoding->dbuf);

    if (!buf) {
        return NFS4ERR_RESOURCE;
    }

    len = chimera_vfs_xattr_build_user(buf, namecap, wire_name, wire_len);

    if (len < 0) {
        return NFS4ERR_NAMETOOLONG;
    }

    *name    = buf;
    *namelen = len;

    return NFS4_OK;
} /* chimera_nfs4_xattr_stage_name */

/*
 * How large a GETXATTR value / LISTXATTRS name buffer may be staged in the
 * reply, given the headroom left in it.  Both leave 8192 bytes for the rest of
 * the compound reply; GETXATTR additionally caps the value at 64K.
 */
uint32_t
chimera_nfs4_xattr_stage_max(
    struct nfs_request *req,
    uint32_t            cap)
{
    uint32_t avail  = req->encoding->dbuf->size - req->encoding->dbuf->used;
    uint32_t maxval = avail > 8192 ? avail - 8192 : 0;

    return maxval > cap ? cap : maxval;
} /* chimera_nfs4_xattr_stage_max */

/*
 * Put an already-read xattr value on the wire.  The per-op path has the VFS
 * write straight into the reserved buffer and passes NULL; the VFS-compound
 * path holds the value by the time it fills the result and passes it here.
 */
nfsstat4
chimera_nfs4_getxattr_fill(
    struct nfs_request  *req,
    struct GETXATTR4res *res,
    const void          *value,
    uint32_t             value_len)
{
    if (xdr_dbuf_opaque_copy(&res->gxr_value, value, value_len,
                             req->encoding->dbuf)) {
        return NFS4ERR_RESOURCE;
    }

    return NFS4_OK;
} /* chimera_nfs4_getxattr_fill */

static void
chimera_nfs4_getxattr_complete(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_request  *req = private_data;
    struct GETXATTR4res *res = &req->res_compound.resarray[req->index].opgetxattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->gxr_status    = NFS4_OK;
        res->gxr_value.len = value_len;
        /* gxr_value.data already points at the dbuf-backed buffer. */
    } else {
        res->gxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, res->gxr_status);
} /* chimera_nfs4_getxattr_complete */

static void
chimera_nfs4_getxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req  = private_data;
    struct GETXATTR4args *args = &req->args_compound->argarray[req->index].opgetxattr;
    struct GETXATTR4res  *res  = &req->res_compound.resarray[req->index].opgetxattr;
    uint32_t              maxval;
    char                 *name;
    int                   namelen, rc;

    if (error_code != CHIMERA_VFS_OK) {
        res->gxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    req->handle = handle;

    res->gxr_status = chimera_nfs4_xattr_stage_name(req, args->gxa_name.data,
                                                    args->gxa_name.len,
                                                    &name, &namelen);
    if (res->gxr_status != NFS4_OK) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    /* Stage the value into the response dbuf, leaving headroom for the rest
     * of the compound reply. */
    maxval = chimera_nfs4_xattr_stage_max(req, CHIMERA_NFS4_GETXATTR_MAX);

    rc = xdr_dbuf_alloc_opaque(&res->gxr_value, maxval, req->encoding->dbuf);
    if (rc) {
        res->gxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    chimera_vfs_get_xattr(req->thread->vfs_thread, &req->cred,
                          handle,
                          name,
                          namelen,
                          res->gxr_value.data,
                          maxval,
                          chimera_nfs4_getxattr_complete,
                          req);
} /* chimera_nfs4_getxattr_open_callback */

void
chimera_nfs4_getxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETXATTR4res *res = &resop->opgetxattr;

    if (req->fhlen == 0) {
        res->gxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_getxattr_open_callback,
                        req);
} /* chimera_nfs4_getxattr */
