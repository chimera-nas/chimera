// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"

nfsstat4
chimera_nfs4_xattr_validate_name(uint32_t wire_len)
{
    if (!wire_len) {
        return NFS4ERR_INVAL;
    }
    return wire_len > CHIMERA_VFS_XATTR_NAME_MAX - CHIMERA_VFS_XATTR_USER_PREFIX_LEN ?
           NFS4ERR_NAMETOOLONG : NFS4_OK;
} /* chimera_nfs4_xattr_validate_name */

/*
 * Stage an RFC 8276 xattr name -- which arrives without a namespace prefix --
 * as the fully-qualified "user." name the VFS expects, in the response dbuf
 * (which outlives every attempt of the VFS compound).
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

    nfsstat4 status = chimera_nfs4_xattr_validate_name(wire_len);

    if (status != NFS4_OK) {
        return status;
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

/* Copy the compound-owned xattr value into the accepted reply. */
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

    res->gxr_status = chimera_nfs4_xattr_validate_name(argop->opgetxattr.gxa_name.len);
    if (res->gxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_getxattr */
