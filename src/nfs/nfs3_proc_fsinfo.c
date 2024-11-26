#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_fsinfo_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  attr_mask,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSINFO3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.maxfilesize         = UINT64_MAX;
        res.resok.time_delta.seconds  = 0;
        res.resok.time_delta.nseconds = 1;
        res.resok.rtmax               = 1024 * 1024;
        res.resok.rtpref              = 1024 * 1024;
        res.resok.rtmult              = 4096;
        res.resok.wtmax               = 1024 * 1024;
        res.resok.wtpref              = 1024 * 1024;
        res.resok.wtmult              = 4096;
        res.resok.dtpref              = 64 * 1024;
        res.resok.properties          = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_FSINFO(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsinfo_complete */

void
chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSINFO3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req              = nfs_request_alloc(thread);
    req->args_fsinfo = args;
    req->conn        = conn;
    req->msg         = msg;

    chimera_vfs_getattr(thread->vfs,
                        args->fsroot.data.data,
                        args->fsroot.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_fsinfo_complete,
                        req);

} /* chimera_nfs3_fsinfo */