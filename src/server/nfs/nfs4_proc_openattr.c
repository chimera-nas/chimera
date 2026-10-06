// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_named_attr.h"
#include "server/server.h"

/*
 * OPENATTR (RFC 7530 §16.21 / RFC 8881 §18.17): set the current filehandle to
 * the named-attribute directory of the current object.  Chimera projects NFSv4
 * named attributes onto the VFS named-stream primitives (the same backend
 * storage as SMB alternate data streams), so the attr directory is a synthetic
 * server-side object whose handle is the base file's fh with a magic prefix
 * (nfs4_named_attr.h).  Its entries -- the named streams -- are created/listed/
 * removed via open_stream/list_streams/remove_stream when ops run against the
 * attr-dir fh.
 *
 * `createdir` is advisory here: the attr directory is conceptually always
 * present for a streams-capable regular file (entries are created on demand by
 * OPEN), so OPENATTR succeeds regardless and the directory simply enumerates
 * empty until a named attribute is written.
 */


void
chimera_nfs4_openattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPENATTR4res *res = &resop->opopenattr;

    req->handle = NULL;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The pseudo-root and an existing attr directory have no named-attribute
     * directory of their own. */
    if (fh_is_nfs4_root(req->fh, req->fhlen) ||
        chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        res->status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_openattr */
