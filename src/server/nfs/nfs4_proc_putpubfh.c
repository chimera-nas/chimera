// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "vfs/vfs.h"

/* RFC 7530 §16.21: PUTPUBFH sets the current FH to the server's "public"
 * filehandle. Chimera does not distinguish a separate public namespace from
 * the root namespace, so PUTPUBFH is a synonym for PUTROOTFH. */
void
chimera_nfs4_putpubfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTPUBFH4res *res = &resop->opputpubfh;
    uint32_t             fhlen;

    nfs4_root_get_fh(req->fh, &fhlen);
    req->fhlen = fhlen;

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putpubfh */
