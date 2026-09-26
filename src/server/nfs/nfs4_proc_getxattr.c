// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_compound.h"

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

/* PUTFH, OPEN_CURRENT, GETXATTR: the fetch is op 2 of the run. */
#define NFS4_GETXATTR_OP_GETXATTR 2

static void
chimera_nfs4_getxattr_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct GETXATTR4res                  *res = &req->res_compound.resarray[req->index].opgetxattr;
    const struct chimera_vfs_compound_op *xop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->gxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    /* The value lives in the sequence's own buffer, so it is copied into the
     * reply before the free -- where the per-op path had the backend write
     * straight into the reply buffer. */
    xop = chimera_vfs_compound_op(compound, NFS4_GETXATTR_OP_GETXATTR);

    res->gxr_status = chimera_nfs4_getxattr_fill(req, res, xop->buffer,
                                                 xop->buffer_len);

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->gxr_status);
} /* chimera_nfs4_getxattr_complete */

void
chimera_nfs4_getxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETXATTR4args        *args = &argop->opgetxattr;
    struct GETXATTR4res         *res  = &resop->opgetxattr;
    struct chimera_vfs_compound *compound;
    char                        *name;
    int                          namelen;

    if (req->fhlen == 0) {
        res->gxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    /* The name is staged before the sequence rather than after the open,
     * because the adder takes it -- which is where the VFS-compound path
     * stages it too. */
    res->gxr_status = chimera_nfs4_xattr_stage_name(req, args->gxa_name.data,
                                                    args->gxa_name.len,
                                                    &name, &namelen);

    if (res->gxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_getxattr(compound, name, namelen,
                                      chimera_nfs4_xattr_stage_max(
                                          req, CHIMERA_NFS4_GETXATTR_MAX));

    chimera_vfs_compound_submit(compound, chimera_nfs4_getxattr_complete, req);
} /* chimera_nfs4_getxattr */
