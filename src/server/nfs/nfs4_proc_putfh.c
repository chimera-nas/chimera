// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "server/server.h"
#include "nfs4_named_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"

/* A synthetic named-attribute-directory handle (OPENATTR result): validate that
 * the underlying base file still exists, then keep the *decoded* marked handle
 * (already in req->fh from PUTFH's decode) as the current fh so GETFH/SAVEFH
 * round-trip it back to the client.  The attr-dir-aware ops (READDIR/LOOKUP/
 * OPEN/REMOVE/GETATTR) recognise it on req->fh. */
static void
chimera_nfs4_putfh_attrdir_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request     *req = private_data;
    struct PUTFH4res       *res = &req->res_compound.resarray[req->index].opputfh;
    enum chimera_vfs_error  error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_putfh_errno(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* req->fh already holds the decoded marked (MAGIC-prefixed) handle from the
     * PUTFH decode; leave it current unchanged. */
    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putfh_attrdir_complete */

/*
 * The zero-link staleness rule PUTFH applies to the handle it has just opened
 * and stat'd.  `fh` is the decoded (inner VFS) handle, which is also the key
 * under which open state is tracked.  A zero-link inode is only still valid if
 * some open pins it; that open may belong to any client (the REMOVE and this
 * PUTFH can arrive on a different connection than the OPEN), so the check is
 * server-wide, not per-connection.
 *
 * Shared with the VFS-compound path, which runs the same open+stat as part of
 * the sequence it submits.
 */
nfsstat4
chimera_nfs4_putfh_check_stale(
    struct nfs_request             *req,
    const struct chimera_vfs_attrs *attr,
    const uint8_t                  *fh,
    int                             fhlen)
{
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
        attr->va_nlink == 0 &&
        !nfs4_clients_have_open_state(&req->thread->shared->nfs4_shared_clients,
                                      fh, fhlen)) {
        return NFS4ERR_STALE;
    }

    return NFS4_OK;
} /* chimera_nfs4_putfh_check_stale */

/* PUTFH, OPEN_CURRENT, GETATTR: the stat is op 2 of the run. */
#define NFS4_PUTFH_OP_GETATTR 2

static void
chimera_nfs4_putfh_validate_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct PUTFH4res                     *res = &req->res_compound.resarray[req->index].opputfh;
    const struct chimera_vfs_compound_op *gop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_putfh_errno(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    gop         = chimera_vfs_compound_op(compound, NFS4_PUTFH_OP_GETATTR);
    res->status = chimera_nfs4_putfh_check_stale(req, &gop->attr,
                                                 req->fh, req->fhlen);

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_putfh_validate_complete */

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTFH4args           *args = &argop->opputfh;
    struct  PUTFH4res           *res  = &resop->opputfh;
    struct chimera_vfs_compound *compound;

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

    /* A named-attribute-directory handle is the base file's VFS fh with a magic
     * prefix.  It is wrapped on the wire like any handle (OPENATTR builds the
     * marked inner fh; GETFH wraps it), so the marker is only visible AFTER
     * decode -- checking the wire bytes would miss it behind the wrap header.
     * Validate the base it wraps; the decoded marked form stays the current fh
     * so the attr-dir-aware ops (READDIR/LOOKUP/OPEN/REMOVE/GETATTR) route on
     * it. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        const uint8_t *base;
        int            base_len;

        chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

        if (base_len > NFS4_FHSIZE ||
            !chimera_vfs_fh_is_plausible(thread->vfs_thread, base, base_len)) {
            res->status = NFS4ERR_BADHANDLE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

        chimera_vfs_compound_add_putfh(compound, base, base_len);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH, 0);

        chimera_vfs_compound_submit(compound,
                                    chimera_nfs4_putfh_attrdir_complete, req);
        return;
    }

    if (!chimera_vfs_fh_is_plausible(thread->vfs_thread, req->fh, req->fhlen)) {
        res->status = NFS4ERR_BADHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* A PATH open, because the validation reads nothing through the handle: it
     * is the link count that decides, and a data open of a FIFO blocks.  The
     * handle belongs to the run, so the per-op slot stays empty. */
    res->status = NFS4_OK;
    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_NLINK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs4_putfh_validate_complete, req);
} /* chimera_nfs4_putfh */
