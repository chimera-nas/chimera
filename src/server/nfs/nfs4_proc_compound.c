// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"
#include "nfs4_dump.h"
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

    if (status != NFS4_OK) {
        req->res_compound.status = status;
        req->index               = req->res_compound.num_resarray;
    }

    if (req->index >= req->res_compound.num_resarray) {

        //dump_COMPOUND4res("res", &req->res_compound);

        shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            &req->res_compound,
            req->msg);

        nfs_request_free(thread, req);
        return;
    }

    argop = &req->args_compound->argarray[req->index];
    resop = &req->res_compound.resarray[req->index];

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
        case OP_SETATTR:
            chimera_nfs4_setattr(thread, req, argop, resop);
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
        case OP_SAVEFH:
            chimera_nfs4_savefh(thread, req, argop, resop);
            break;
        case OP_LINK:
            chimera_nfs4_link(thread, req, argop, resop);
            break;
        case OP_RENAME:
            chimera_nfs4_rename(thread, req, argop, resop);
            break;
        case OP_OPEN:
            chimera_nfs4_open(thread, req, argop, resop);
            break;
        case OP_READDIR:
            chimera_nfs4_readdir(thread, req, argop, resop);
            break;
        case OP_READ:
            chimera_nfs4_read(thread, req, argop, resop);
            break;
        case OP_WRITE:
            chimera_nfs4_write(thread, req, argop, resop);
            break;
        case OP_COMMIT:
            chimera_nfs4_commit(thread, req, argop, resop);
            break;
        case OP_CLOSE:
            chimera_nfs4_close(thread, req, argop, resop);
            break;
        case OP_REMOVE:
            chimera_nfs4_remove(thread, req, argop, resop);
            break;
        case OP_READLINK:
            chimera_nfs4_readlink(thread, req, argop, resop);
            break;
        case OP_SETCLIENTID:
            chimera_nfs4_setclientid(thread, req, argop, resop);
            break;
        case OP_SETCLIENTID_CONFIRM:
            chimera_nfs4_setclientid_confirm(thread, req, argop, resop);
            break;
        case OP_EXCHANGE_ID:
            chimera_nfs4_exchange_id(thread, req, argop, resop);
            break;
        case OP_CREATE_SESSION:
            chimera_nfs4_create_session(thread, req, argop, resop);
            break;
        case OP_DESTROY_SESSION:
            chimera_nfs4_destroy_session(thread, req, argop, resop);
            break;
        case OP_DESTROY_CLIENTID:
            chimera_nfs4_destroy_clientid(thread, req, argop, resop);
            break;
        case OP_SEQUENCE:
            chimera_nfs4_sequence(thread, req, argop, resop);
            break;
        case OP_RECLAIM_COMPLETE:
            chimera_nfs4_reclaim_complete(thread, req, argop, resop);
            break;
        case OP_SECINFO_NO_NAME:
            chimera_nfs4_secinfo_no_name(thread, req, argop, resop);
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

    req = nfs_request_alloc(thread, conn, msg);

    nfs4_dump_compound(req, args);

    req->session              = conn->private_data;
    req->args_compound        = args;
    req->res_compound.status  = NFS4_OK;
    req->res_compound.tag.len = 0;
    req->fhlen                = 0;
    req->saved_fhlen          = 0;

    xdr_dbuf_reserve(&req->res_compound,
                     resarray,
                     args->num_argarray,
                     msg->dbuf);

    req->index = 0;

    chimera_nfs4_compound_process(req, NFS4_OK);

} /* chimera_nfs4_compound */