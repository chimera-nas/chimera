// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETFH4res *res = &resop->opgetfh;

    xdr_dbuf_opaque_copy(&res->resok4.object,
                         req->fh,
                         req->fhlen,
                         req->msg->dbuf);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getfh */