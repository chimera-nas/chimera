// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_protocol.h"
#include "nfs4_session.h"
#include "vfs/vfs_pnfs.h"

/* These responses consume no protocol state. The caller owns any dbuf output
 * and publishes it only after its enclosing compound has accepted finish. */
nfsstat4
chimera_nfs4_protocol_status(
    struct nfs_request      *req,
    const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_GETDEVICELIST:
            /* Deprecated discovery; clients use LAYOUTGET + GETDEVICEINFO. */
            return NFS4ERR_NOTSUPP;
        case OP_LAYOUTSTATS:
        case OP_LAYOUTERROR:
            /* Reports are acknowledged; no statistics or mirror changes are
             * published by the current implementation. */
            return chimera_vfs_pnfs_feature_enabled(req->thread->shared->vfs) ? NFS4_OK : NFS4ERR_NOTSUPP;
        case OP_DELEGPURGE:
            /* In 4.1+ the SEQUENCE client, not the ignored clientid argument,
             * determines state protection. A 4.0 caller has no session rule. */
            if (req->session) {
                const struct nfs4_client_principal principal = {
                    .flavor          = req->principal_flavor,
                    .uid             = req->principal_uid,
                    .gid             = req->principal_gid,
                    .machinename     = req->principal_machinename,
                    .machinename_len = req->principal_machinename_len,
                };
                if (!nfs4_client_mach_cred_ok(req->session->nfs4_session_client, &principal)) {
                    return NFS4ERR_WRONG_CRED;
                }
            }
            return NFS4_OK;
        default:
            return NFS4ERR_OP_ILLEGAL;
    } /* switch */
} /* chimera_nfs4_protocol_status */
