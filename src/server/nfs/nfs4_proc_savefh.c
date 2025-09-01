// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

void
chimera_nfs4_savefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct  SAVEFH4res *res = &resop->opsavefh;

    res->status = NFS4_OK;

    memcpy(req->saved_fh, req->fh, req->fhlen);
    req->saved_fhlen = req->fhlen;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_savefh */
