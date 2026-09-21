// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_compound.h"

/*
 * Marshal a COMMIT4 result from the attributes the flush reported for the
 * object it flushed.  Shared by the per-op path below and by the VFS-compound
 * path, which comes back holding the same pre-flush attributes.
 */
nfsstat4
chimera_nfs4_commit_fill(
    struct nfs_request             *req,
    struct COMMIT4res              *res,
    const struct chimera_vfs_attrs *pre_attr)
{
    if ((pre_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(pre_attr->va_mode)) {
        /* RFC 7530 §16.4: COMMIT only applies to regular files. A directory
         * yields NFS4ERR_ISDIR; any other non-regular object NFS4ERR_INVAL. */
        return S_ISDIR(pre_attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
    }

    memcpy(res->resok4.writeverf,
           &req->thread->shared->nfs_verifier,
           sizeof(res->resok4.writeverf));

    return NFS4_OK;
} /* chimera_nfs4_commit_fill */

/*
 * PUTFH, OPEN_CURRENT(meta), GETATTR(MODE), OPEN_CURRENT(data), COMMIT.
 *
 * The regular-file rule is applied BEFORE the flush, from a stat taken through
 * a path open, because a data open of a FIFO blocks -- the same two-open shape
 * the per-op path had, and the one the VFS-compound path encodes.  The two
 * handles come from different caches, so the second OPEN_CURRENT is a second
 * open and not a re-use.
 */
#define NFS4_COMMIT_OP_TYPE   2
#define NFS4_COMMIT_OP_COMMIT 4

/*
 * The type rule, applied as the stat finishes so nothing behind it runs.  The
 * VFS error is only a stop signal: the completion re-derives the NFSv4 status
 * from the mode this op reported, because NFSv4 distinguishes a directory from
 * a symlink from any other special file and POSIX does not.
 */
static void
chimera_nfs4_commit_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *top;

    if (index != NFS4_COMMIT_OP_TYPE || *status != CHIMERA_VFS_OK) {
        return;
    }

    top = chimera_vfs_compound_op(compound, index);

    if ((top->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(top->attr.va_mode)) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs4_commit_gate */

static void
chimera_nfs4_commit_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct COMMIT4res                    *res = &req->res_compound.resarray[req->index].opcommit;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound, NFS4_COMMIT_OP_TYPE);

        if (op->status != CHIMERA_VFS_OK &&
            (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
            !S_ISREG(op->attr.va_mode)) {
            res->status = chimera_nfs4_data_nonreg_status(op->attr.va_mode);
        } else {
            res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        }
    } else {
        /* The rule again, against the attributes the flush itself reported --
         * as the per-op completion applied it -- and the write verifier. */
        op          = chimera_vfs_compound_op(compound, NFS4_COMMIT_OP_COMMIT);
        res->status = chimera_nfs4_commit_fill(req, res, &op->pre_attr);
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_commit_complete */

void
chimera_nfs4_commit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct COMMIT4args          *args = &argop->opcommit;
    struct COMMIT4res           *res  = &resop->opcommit;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_NOFOLLOW, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_commit(compound, args->offset, args->count,
                                    CHIMERA_VFS_ATTR_MODE, 0);

    chimera_vfs_compound_set_gate(compound, chimera_nfs4_commit_gate, req);

    chimera_vfs_compound_submit(compound, chimera_nfs4_commit_complete, req);
} /* chimera_nfs4_commit */
