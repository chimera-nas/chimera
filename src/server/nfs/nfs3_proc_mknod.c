#include "nfs3_procs.h"
#include "nfs3_dump.h"

void
chimera_nfs3_mknod(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct MKNOD3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct MKNOD3res                  res;

    /* XXX fill this in at some point */
    res.status = NFS3ERR_NOTSUPP;

    nfs3_dump_mknod(NULL, args);

    shared->nfs_v3.send_reply_NFSPROC3_MKNOD(evpl, &res, msg);
} /* chimera_nfs3_mknod */
