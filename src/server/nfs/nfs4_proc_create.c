// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"

/* Request-only checks shared by the coalescing scan and standalone dispatch. */
nfsstat4
chimera_nfs4_validate_create(const struct CREATE4args *args)
{
    nfsstat4 status = chimera_nfs4_validate_name(&args->objname);

    if (status != NFS4_OK) {
        return status;
    }
    status = chimera_nfs4_validate_createattrs(args->createattrs.num_attrmask,
                                               args->createattrs.attrmask);
    if (status != NFS4_OK) {
        return status;
    }
    switch (args->objtype.type) {
        case NF4DIR:
        case NF4BLK:
        case NF4CHR:
        case NF4SOCK:
        case NF4FIFO:
            return NFS4_OK;
        case NF4LNK:
            return args->objtype.linkdata.len ? NFS4_OK : NFS4ERR_INVAL;
        case NF4ATTRDIR:
        case NF4NAMEDATTR:
            return NFS4ERR_NOTSUPP;
        default:
            return NFS4ERR_BADTYPE;
    } /* switch */
} /* chimera_nfs4_validate_create */

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CREATE4res *res = &resop->opcreate;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }
    res->status = chimera_nfs4_validate_create(&argop->opcreate);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }
    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_create */
