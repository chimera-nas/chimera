// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_dump.h"
void
chimera_nfs4_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    nfs4_dump_null(NULL);

    rc = shared->nfs_v4.send_reply_NFSPROC4_NULL(evpl, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs4_null */