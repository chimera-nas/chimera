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

    nfs4_dump_null(NULL);

    shared->nfs_v4.send_reply_NFSPROC4_NULL(evpl, msg);
} /* chimera_nfs4_null */