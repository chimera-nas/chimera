#include <stdio.h>
#include <errno.h>

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"

#include "uthash/utlist.h"

static inline nfsstat4
chimera_nfs4_errno_to_nfsstat4(int err)
{
    switch (err) {
        case 0:
            return NFS4_OK;
        case EPERM:
            return NFS4ERR_PERM;
        case ENOENT:
            return NFS4ERR_NOENT;
        case EIO:
            return NFS4ERR_IO;
        case ENXIO:
            return NFS4ERR_NXIO;
        case EACCES:
            return NFS4ERR_ACCESS;
        case EEXIST:
            return NFS4ERR_EXIST;
        case EXDEV:
            return NFS4ERR_XDEV;
        case ENOTDIR:
            return NFS4ERR_NOTDIR;
        case EISDIR:
            return NFS4ERR_ISDIR;
        case EINVAL:
            return NFS4ERR_INVAL;
        case EFBIG:
            return NFS4ERR_FBIG;
        case ENOSPC:
            return NFS4ERR_NOSPC;
        case EROFS:
            return NFS4ERR_ROFS;
        case EMLINK:
            return NFS4ERR_MLINK;
        case ENAMETOOLONG:
            return NFS4ERR_NAMETOOLONG;
        case ENOTEMPTY:
            return NFS4ERR_NOTEMPTY;
        case EDQUOT:
            return NFS4ERR_DQUOT;
        case ESTALE:
            return NFS4ERR_STALE;
        case EBADF:
            return NFS4ERR_BADHANDLE;
        case ENOTSUP:
            return NFS4ERR_NOTSUPP;
        case EOVERFLOW:
            return NFS4ERR_TOOSMALL;
        case EFAULT:
            return NFS4ERR_SERVERFAULT;
        default:
            chimera_nfs_error("Unknown errno value in translation: %d", err);
            return NFS4ERR_IO;
    } /* switch */
} /* chimera_nfs4_errno_to_nfsstat4 */

static void
chimera_nfs4_compound_process(
    struct nfs4_request *req,
    nfsstat4             status);

static inline void
chimera_nfs4_compound_complete(
    struct nfs4_request *req,
    nfsstat4             status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (status != NFS4_OK) {
        req->res.status = status;
        chimera_nfs_info("nfs4 compound operation %d/%d: error %d",
                         req->index,
                         req->res.num_resarray,
                         status);
        req->index = req->res.num_resarray;
    }

    if (thread->active) {
        thread->again = 1;
    } else {
        chimera_nfs4_compound_process(req, status);
    }

} /* chimera_nfs4_compound_complete */

static void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    GETFH4res *res = &resop->opgetfh;

    xdr_dbuf_opaque_copy(&res->resok4.object,
                         req->fh,
                         req->fhlen,
                         req->msg->dbuf);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getfh */

static void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    PUTROOTFH4res *res = &resop->opputrootfh;

    chimera_vfs_getrootfh(thread->shared->vfs,
                          req->fh,
                          &req->fhlen);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */

static void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    GETATTR4res *res = &resop->opgetattr;

    res->status                              = NFS4_OK;
    res->resok4.obj_attributes.num_attrmask  = 0;
    res->resok4.obj_attributes.attr_vals.len = 0;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getattr */

static void
chimera_nfs4_lookup_complete(
    int         error_code,
    const void *fh,
    int         fhlen,
    void       *private_data)
{
    struct nfs4_request *req    = private_data;
    nfsstat4             status = chimera_nfs4_errno_to_nfsstat4(error_code);
    LOOKUP4res          *res    = &req->res.resarray[req->index].oplookup;

    res->status = status;

    chimera_nfs4_compound_complete(req, status);

} /* chimera_nfs4_lookup_complete */

static void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    struct chimera_vfs *vfs  = thread->shared->vfs;
    LOOKUP4args        *args = &argop->oplookup;

    chimera_vfs_lookup(vfs,
                       req->fh,
                       req->fhlen,
                       args->objname.data,
                       chimera_nfs4_lookup_complete,
                       req);
} /* chimera_nfs4_lookup */

static void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    SETCLIENTID4args                 *args   = &argop->opsetclientid;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs4_session              *session;

    resop->opsetclientid.resok4.clientid = nfs4_client_register(
        &thread->shared->nfs4_shared_clients,
        args->client.id.data,
        args->client.id.len,
        *(uint64_t *) args->client.verifier,
        40,
        NULL, NULL);


    //conn->nfs4_clientid = sci->sci_r_clientid;

    session = nfs4_create_session(
        &shared->nfs4_shared_clients,
        resop->opsetclientid.resok4.clientid,
        1,
        NULL,
        NULL);


    resop->opsetclientid.status = NFS4_OK;

    memcpy(&resop->opsetclientid.resok4.setclientid_confirm,
           session->nfs4_session_id,
           sizeof(session->nfs4_session_id));

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */

static void
chimera_nfs4_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    //SETCLIENTID_CONFIRM4args *args = &argop->opsetclientid_confirm;

    /*
     * In a real implementation, we would:
     * 1. Verify the clientid matches one from a previous SETCLIENTID
     * 2. Verify the confirmation verifier matches
     * 3. Create/update the confirmed client record
     * 4. Remove any unconfirmed records for this client
     */

    resop->opsetclientid_confirm.status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid_confirm */

static struct nfs4_request *
nfs4_request_alloc(struct chimera_server_nfs_thread *thread)
{
    struct nfs4_request *req;

    if (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }

    return req;
} /* nfs4_request_alloc */

static void
nfs4_request_free(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req)
{
    LL_PREPEND(thread->free_requests, req);
} /* nfs4_request_free */

static void
chimera_nfs4_compound_process(
    struct nfs4_request *req,
    nfsstat4             status)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    nfs_argop4                       *argop;
    nfs_resop4                       *resop;

 again:

    chimera_nfs_info("nfs4 compound operation %d/%d: entry status %d",
                     req->index,
                     req->res.num_resarray,
                     status);

    if (status != NFS4_OK) {
        req->res.status = status;
        chimera_nfs_info("nfs4 compound operation %d/%d: error %d",
                         req->index,
                         req->res.num_resarray,
                         status);
        req->index = req->res.num_resarray;
    }

    if (req->index >= req->res.num_resarray) {
        chimera_nfs_info("nfs4 compound operation complete");

        shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            &req->res,
            req->msg);

        nfs4_request_free(thread, req);
        return;
    }

    argop = &req->args->argarray[req->index];
    resop = &req->res.resarray[req->index];

    chimera_nfs_info("nfs4 compound operation %d/%d: %d",
                     req->index,
                     req->res.num_resarray,
                     argop->argop);

    resop->resop = argop->argop;

    thread->active = 1;

    switch (argop->argop) {
        case OP_GETFH:
            chimera_nfs4_getfh(thread, req, argop, resop);
            break;
        case OP_PUTROOTFH:
            chimera_nfs4_putrootfh(thread, req, argop, resop);
            break;
        case OP_GETATTR:
            chimera_nfs4_getattr(thread, req, argop, resop);
            break;
        case OP_LOOKUP:
            chimera_nfs4_lookup(thread, req, argop, resop);
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
    struct nfs4_request              *req;

    chimera_nfs_info("nfs4 compound call entry");

    req = nfs4_request_alloc(thread);

    req->conn        = conn;
    req->msg         = msg;
    req->args        = args;
    req->res.status  = NFS4_OK;
    req->res.tag.len = 0;

    xdr_dbuf_reserve(&req->res,
                     resarray,
                     args->num_argarray,
                     msg->dbuf);

    req->index = 0;

    chimera_nfs4_compound_process(req, NFS4_OK);

} /* chimera_nfs4_compound */

void
chimera_nfs4_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->nfs_v4.send_reply_NFSPROC4_NULL(evpl, msg);
} /* chimera_nfs4_null */