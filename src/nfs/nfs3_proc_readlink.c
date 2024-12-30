
#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_readlink_complete(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READLINK3res              *res;

    res = &req->res_readlink;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        res->resok.symlink_attributes.attributes_follow = 0;

        res->resok.data.len = targetlen;
    }

    shared->nfs_v3.send_reply_NFSPROC3_READLINK(evpl, res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_readlink_complete */

void
chimera_nfs3_readlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READLINK3args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct READLINK3res              *res;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_readlink(req, args);
    res = &req->res_readlink;

    xdr_dbuf_reserve_str(&res->resok, data, 4096, msg->dbuf);

    chimera_vfs_readlink(thread->vfs_thread,
                         args->symlink.data.data,
                         args->symlink.data.len,
                         res->resok.data.str,
                         res->resok.data.len,
                         chimera_nfs3_readlink_complete,
                         req);
} /* chimera_nfs3_readlink */
