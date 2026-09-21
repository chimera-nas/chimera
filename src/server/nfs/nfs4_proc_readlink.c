// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_compound.h"

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

/*
 * RFC 7530 §16.22: READLINK is valid only on a symbolic link; a non-symlink
 * current filehandle is NFS4ERR_INVAL.  The type comes from a getattr issued
 * against the object -- if a backend completed getattr without reporting MODE
 * we cannot prove the object is a link, so fail closed (NFS4ERR_SERVERFAULT)
 * rather than reading a non-symlink.
 */
nfsstat4
chimera_nfs4_readlink_check_type(const struct chimera_vfs_attrs *attr)
{
    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        return NFS4ERR_SERVERFAULT;
    }

    if (!S_ISLNK(attr->va_mode)) {
        return NFS4ERR_INVAL;
    }

    return NFS4_OK;
} /* chimera_nfs4_readlink_check_type */

/*
 * Reserve the READLINK4 reply buffer and, when `target` is given, copy the
 * already-resolved link into it.  The per-op path passes NULL and lets the VFS
 * write the target straight into the reserved buffer (the completion then
 * shortens link.len to what was written); the VFS-compound path, which has the
 * target in hand by the time it fills the result, passes it here.
 */
nfsstat4
chimera_nfs4_readlink_fill(
    struct nfs_request  *req,
    struct READLINK4res *res,
    const char          *target,
    uint32_t             target_len)
{
    res->resok4.link.data = xdr_dbuf_alloc_space(4096, req->encoding->dbuf);

    if (!res->resok4.link.data) {
        return NFS4ERR_RESOURCE;
    }

    if (target) {
        if (target_len > 4096) {
            target_len = 4096;
        }
        memcpy(res->resok4.link.data, target, target_len);
        res->resok4.link.len = target_len;
    } else {
        res->resok4.link.len = 4096;
    }

    return NFS4_OK;
} /* chimera_nfs4_readlink_fill */

/* PUTFH, OPEN_CURRENT, GETATTR, READLINK. */
#define NFS4_READLINK_OP_GETATTR  2
#define NFS4_READLINK_OP_READLINK 3

/*
 * The type gate, applied where the per-op path applied it: after the stat and
 * before the read.  A veto here fails the GETATTR and stops the run, so the
 * READLINK behind it never reaches a backend -- which is what the two chained
 * calls did.  The statuses are the ones the sequence path uses for the same
 * gate, so both spellings of READLINK refuse a non-symlink identically.
 */
static void
chimera_nfs4_readlink_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *gop;

    if (index != NFS4_READLINK_OP_GETATTR || *status != CHIMERA_VFS_OK) {
        return;
    }

    gop = chimera_vfs_compound_op(compound, index);

    switch (chimera_nfs4_readlink_check_type(&gop->attr)) {
        case NFS4_OK:
            break;
        case NFS4ERR_INVAL:
            *status = CHIMERA_VFS_EINVAL;
            break;
        default:
            *status = CHIMERA_VFS_EFAULT;
            break;
    } /* switch */
} /* chimera_nfs4_readlink_gate */

static void
chimera_nfs4_readlink_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct READLINK4res                  *res = &req->res_compound.resarray[req->index].opreadlink;
    const struct chimera_vfs_compound_op *rop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The target belongs to the compound, so it is copied out before the
     * free -- which is what the fill's compound-path argument is for. */
    rop         = chimera_vfs_compound_op(compound, NFS4_READLINK_OP_READLINK);
    res->status = chimera_nfs4_readlink_fill(req, res, rop->target,
                                             rop->target_len);

    chimera_vfs_compound_free(compound);

    chimera_nfs_abort_if(res->status != NFS4_OK, "Failed to allocate space");

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_readlink_complete */

void
chimera_nfs4_readlink(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READLINK4res         *res = &resop->opreadlink;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* PATH and NOFOLLOW: the object is a symlink and the point is to read it,
     * not to resolve through it.  The gate and the READLINK act on the same
     * object, so they share the one open. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_NOFOLLOW, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
    chimera_vfs_compound_add_readlink(compound);

    chimera_vfs_compound_set_gate(compound, chimera_nfs4_readlink_gate, req);

    chimera_vfs_compound_submit(compound, chimera_nfs4_readlink_complete, req);
} /* chimera_nfs4_readlink */
