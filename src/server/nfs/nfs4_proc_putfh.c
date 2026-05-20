// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTFH4args *args = &argop->opputfh;
    struct  PUTFH4res *res  = &resop->opputfh;

    /* RFC 7530 §16.20.5: a structurally invalid handle (wrong length, unknown
     * mount) is NFS4ERR_BADHANDLE. Existence of the target is not checked here
     * — a well-formed but deleted handle surfaces NFS4ERR_STALE on the
     * operation that dereferences it.
     *
     * The synthetic NFSv4 pseudo-root handle is not a VFS mount handle (it is
     * resolved by the server's fh_is_nfs4_root fast path), so accept it
     * explicitly; the kernel client PUTFHs it during mount. */
    if (!fh_is_nfs4_root(args->object.data, args->object.len) &&
        (args->object.len > NFS4_FHSIZE ||
         !chimera_vfs_fh_is_plausible(thread->vfs_thread,
                                      args->object.data,
                                      args->object.len))) {
        res->status = NFS4ERR_BADHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = NFS4_OK;

    memcpy(req->fh, args->object.data, args->object.len);
    req->fhlen = args->object.len;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putfh */