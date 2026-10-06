// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "nfs4_procs.h"
#include "nfs4_status.h"

/* Marshal an accepted COMMIT result from the flush's pre-operation attrs. */
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

void
chimera_nfs4_commit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct COMMIT4res *res = &resop->opcommit;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_commit */
