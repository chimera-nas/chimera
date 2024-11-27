#include "nfs4_procs.h"
#include "rpc2/rpc2.h"

void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_argop4                *argop;
    struct nfs_resop4                *resop;

 again:

    chimera_nfs_info("nfs4 compound operation %d/%d: entry status %d",
                     req->index,
                     req->res_compound.num_resarray,
                     status);

    if (status != NFS4_OK) {
        req->res_compound.status = status;
        chimera_nfs_info("nfs4 compound operation %d/%d: error %d",
                         req->index,
                         req->res_compound.num_resarray,
                         status);
        req->index = req->res_compound.num_resarray;
    }

    if (req->index >= req->res_compound.num_resarray) {
        chimera_nfs_info("nfs4 compound operation complete");

        dump_COMPOUND4res("res", &req->res_compound);

        shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            &req->res_compound,
            req->msg);

        nfs_request_free(thread, req);
        return;
    }

    argop = &req->args_compound->argarray[req->index];
    resop = &req->res_compound.resarray[req->index];

    chimera_nfs_info("nfs4 compound operation %d/%d: %d",
                     req->index,
                     req->res_compound.num_resarray,
                     argop->argop);

    resop->resop = argop->argop;

    thread->active = 1;

    switch (argop->argop) {
        case OP_ACCESS:
            chimera_nfs4_access(thread, req, argop, resop);
            break;
        case OP_GETFH:
            chimera_nfs4_getfh(thread, req, argop, resop);
            break;
        case OP_PUTROOTFH:
            chimera_nfs4_putrootfh(thread, req, argop, resop);
            break;
        case OP_GETATTR:
            chimera_nfs4_getattr(thread, req, argop, resop);
            break;
        case OP_CREATE:
            chimera_nfs4_create(thread, req, argop, resop);
            break;
        case OP_LOOKUP:
            chimera_nfs4_lookup(thread, req, argop, resop);
            break;
        case OP_PUTFH:
            chimera_nfs4_putfh(thread, req, argop, resop);
            break;
        case OP_OPEN:
            chimera_nfs4_open(thread, req, argop, resop);
            break;
        case OP_READDIR:
            chimera_nfs4_readdir(thread, req, argop, resop);
            break;
        case OP_CLOSE:
            chimera_nfs4_close(thread, req, argop, resop);
            break;
        case OP_SETCLIENTID:
            chimera_nfs4_setclientid(thread, req, argop, resop);
            break;
        case OP_SETCLIENTID_CONFIRM:
            chimera_nfs4_setclientid_confirm(thread, req, argop, resop);
            break;
        default:
            chimera_nfs_error("Unsupported operation: %d", argop->argop);
            chimera_nfs4_compound_process(req, NFS4ERR_OP_ILLEGAL);
            break;
    }     /* switch */

    thread->active = 0;

    if (thread->again) {
        thread->again = 0;
        req->index++;
        goto again;
    }
} /* chimera_nfs4_compound_process */

void
chimera_nfs4_compound(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct COMPOUND4args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    dump_COMPOUND4args("args", args);

    req = nfs_request_alloc(thread, conn, msg);

    req->session              = conn->private_data;
    req->args_compound        = args;
    req->res_compound.status  = NFS4_OK;
    req->res_compound.tag.len = 0;

    xdr_dbuf_reserve(&req->res_compound,
                     resarray,
                     args->num_argarray,
                     msg->dbuf);

    req->index = 0;

    chimera_nfs4_compound_process(req, NFS4_OK);

} /* chimera_nfs4_compound */