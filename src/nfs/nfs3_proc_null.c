#include "nfs3_procs.h"

void
chimera_nfs3_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->nfs_v3.send_reply_NFSPROC3_NULL(evpl, msg);
} /* nfs3_null */
