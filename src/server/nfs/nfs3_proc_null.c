// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs3_dump.h"

void
chimera_nfs3_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    nfs3_dump_null(NULL);

    int                               rc = shared->nfs_v3.send_reply_NFSPROC3_NULL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* nfs3_null */
