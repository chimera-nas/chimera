#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_lookup_complete(
    enum chimera_vfs_error error_code,
    const void            *fh,
    int                    fhlen,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct LOOKUP3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        xdr_dbuf_opaque_copy(&res.resok.object.data, fh, fhlen, msg->dbuf);
        res.resok.dir_attributes.attributes_follow = 0;
        res.resok.obj_attributes.attributes_follow = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_lookup_complete */

void
chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct LOOKUP3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req              = nfs_request_alloc(thread, conn, msg);
    req->args_lookup = args;

    chimera_vfs_lookup(thread->vfs,
                       args->what.dir.data.data,
                       args->what.dir.data.len,
                       args->what.name.str,
                       args->what.name.len,
                       chimera_nfs3_lookup_complete,
                       req);
} /* chimera_nfs3_lookup */
