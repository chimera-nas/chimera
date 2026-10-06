// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "server/server.h"
#include "nfs4_named_attr.h"

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTFH4args *args = &argop->opputfh;
    struct  PUTFH4res *res  = &resop->opputfh;

    /* The synthetic NFSv4 pseudo-root handle is not a wrapped VFS handle (it is
     * resolved by the server's fh_is_nfs4_root fast path), so accept it
     * explicitly; the kernel client PUTFHs it during mount.  It belongs to no
     * export, so clear the current export and drop any squash.
     *
     * When a "/" export exists the namespace root is that export's real
     * backend directory and the synthetic handle is never minted; a client
     * presenting one holds it from a configuration that no longer exists
     * (the "/" export was added at runtime), so it is stale and the client
     * must remount. */
    if (fh_is_nfs4_root(args->object.data, args->object.len)) {
        if (thread->shared->root_export_id != 0) {
            res->status = NFS4ERR_STALE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        memcpy(req->fh, args->object.data, args->object.len);
        req->fhlen     = args->object.len;
        req->export_id = 0;
        req->cred      = req->orig_cred;
        res->status    = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* RFC 7530 §16.20.5: a structurally invalid handle (wrong length, unknown
     * mount) is NFS4ERR_BADHANDLE.  Decode authenticates the wire handle and
     * recovers the inner VFS handle (into req->fh), the owning export id and
     * the squashed credential.  A forged/tampered handle fails authentication
     * here; a well-formed but deleted handle surfaces NFS4ERR_STALE on the
     * operation that dereferences it. */
    int decode_rc = args->object.len > NFS4_FHSIZE ? CHIMERA_NFS_FH_BADHANDLE :
        chimera_nfs_fh_decode(req, args->object.data, args->object.len,
                              req->fh, &req->fhlen);

    if (decode_rc == CHIMERA_NFS_FH_WRONGSEC) {
        /* Handle is valid but its export does not permit this RPC security
         * flavor; the client renegotiates via SECINFO (RFC 7530 §16.20.5). */
        res->status = NFS4ERR_WRONGSEC;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (decode_rc != CHIMERA_NFS_FH_OK) {
        res->status = NFS4ERR_BADHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The marker is visible only after authenticated decoding. Validate its
     * base, while leaving the protocol cursor marked for the shared encoder. */
    const uint8_t *base     = req->fh;
    int            base_len = req->fhlen;
    if (chimera_nfs4_fh_is_attrdir(base, base_len)) {
        chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);
    }
    if (!chimera_vfs_fh_is_plausible(thread->vfs_thread, base, base_len)) {
        res->status = NFS4ERR_BADHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = NULL;
    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_putfh */
