#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_status.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "nfs4_attr.h"

#include "uthash/utlist.h"

static inline void
chimera_nfs4_compound_complete(
    struct nfs_request *req,
    nfsstat4            status);


static void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
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
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    PUTROOTFH4res *res = &resop->opputrootfh;

    chimera_vfs_getrootfh(thread->vfs,
                          req->fh,
                          &req->fhlen);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */

static void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    PUTFH4args *args = &argop->opputfh;
    PUTFH4res  *res  = &resop->opputfh;

    res->status = NFS4_OK;

    memcpy(req->fh, args->object.data, args->object.len);
    req->fhlen = args->object.len;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putfh */

static void
chimera_nfs4_getattr_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  attr_mask,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    GETATTR4args       *args = &req->args_compound->argarray[req->index].
        opgetattr;
    GETATTR4res        *res = &req->res_compound.resarray[req->index].opgetattr
    ;

    res->status = NFS4_OK;

    xdr_dbuf_reserve(&res->resok4.obj_attributes,
                     attrmask,
                     3,
                     req->msg->dbuf);

    xdr_dbuf_alloc_opaque(&res->resok4.obj_attributes.attr_vals,
                          4096,
                          req->msg->dbuf);

    chimera_nfs4_marshall_attrs(attr,
                                args->num_attr_request,
                                args->attr_request,
                                &res->resok4.obj_attributes.num_attrmask,
                                res->resok4.obj_attributes.attrmask,
                                3,
                                res->resok4.obj_attributes.attr_vals.data,
                                &res->resok4.obj_attributes.attr_vals.len);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getattr_complete */

static void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    uint64_t      attr_mask = 0;
    GETATTR4args *args      = &argop->opgetattr;

    attr_mask = chimera_nfs4_getattr2mask(args->attr_request,
                                          args->num_attr_request);

    chimera_vfs_getattr(thread->vfs,
                        req->fh,
                        req->fhlen,
                        attr_mask,
                        chimera_nfs4_getattr_complete,
                        req);
} /* chimera_nfs4_getattr */

static void
chimera_nfs4_lookup_complete(
    enum chimera_vfs_error error_code,
    const void            *fh,
    int                    fhlen,
    void                  *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    LOOKUP4res         *res    = &req->res_compound.resarray[req->index].
        oplookup;

    res->status = status;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(req->fh, fh, fhlen);
        req->fhlen = fhlen;
    }

    chimera_nfs4_compound_complete(req, status);

} /* chimera_nfs4_lookup_complete */

static void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    LOOKUP4args *args = &argop->oplookup;

    chimera_vfs_lookup(thread->vfs,
                       req->fh,
                       req->fhlen,
                       args->objname.data,
                       args->objname.len,
                       chimera_nfs4_lookup_complete,
                       req);
} /* chimera_nfs4_lookup */

static int
chimera_nfs4_readdir_callback(
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request *req = arg;
    struct entry4      *entry, *prev_entry;
    READDIR4args       *args = &req->args_compound->argarray[req->index].
        opreaddir;
    READDIR4resok      *res = &req->res_compound.resarray[req->index].opreaddir
        .resok4;

    chimera_nfs_debug("readdir callback: cookie %llu, name %.*s, attrs %p",
                      cookie,
                      namelen,
                      name,
                      attrs);

    prev_entry = res->reply.entries;

    xdr_dbuf_reserve_ll(&res->reply,
                        entries,
                        req->msg->dbuf);

    entry = res->reply.entries;

    entry->cookie = cookie;

    xdr_dbuf_reserve(&entry->attrs,
                     attrmask,
                     args->num_attr_request,
                     req->msg->dbuf);

    xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                          4096,
                          req->msg->dbuf);

    chimera_nfs4_marshall_attrs(attrs,
                                args->num_attr_request,
                                args->attr_request,
                                &entry->attrs.num_attrmask,
                                entry->attrs.attrmask,
                                3,
                                entry->attrs.attr_vals.data,
                                &entry->attrs.attr_vals.len);

    entry->nextentry = prev_entry;

    xdr_dbuf_opaque_copy(&entry->name, name, namelen, req->msg->dbuf);

    return 0;
} /* chimera_nfs4_readdir_callback */

static void
chimera_nfs4_readdir_complete(
    enum chimera_vfs_error error_code,
    uint64_t               cookie,
    uint32_t               eof,
    void                  *private_data)
{
    struct nfs_request *req = private_data;
    READDIR4res        *res = &req->res_compound.resarray[req->index].
        opreaddir;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);

    res->status = status;

    memcpy(res->resok4.cookieverf, &cookie, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof = eof;

    chimera_nfs_debug("readdir complete: cookie %llu, error %d",
                      cookie,
                      error_code);

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_readdir_complete */

static void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    READDIR4args *args = &argop->opreaddir;
    READDIR4res  *res  = &resop->opreaddir;

    res->resok4.reply.entries = NULL;

    chimera_vfs_readdir(thread->vfs,
                        req->fh,
                        req->fhlen,
                        args->cookie,
                        chimera_nfs4_readdir_callback,
                        chimera_nfs4_readdir_complete,
                        req);
} /* chimera_nfs4_readdir */

static void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    CLOSE4args *args = &argop->opclose;

    chimera_nfs_debug("close: seqid %u", args->seqid);
} /* chimera_nfs4_close */

static void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
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
    struct nfs_request               *req,
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

static void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    nfs_argop4                       *argop;
    nfs_resop4                       *resop;

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
        case OP_PUTFH:
            chimera_nfs4_putfh(thread, req, argop, resop);
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

static inline void
chimera_nfs4_compound_complete(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (status != NFS4_OK) {
        req->res_compound.status = status;
        chimera_nfs_info("nfs4 compound operation %d/%d: error %d",
                         req->index,
                         req->res_compound.num_resarray,
                         status);
        req->index = req->res_compound.num_resarray;
    }

    if (thread->active) {
        thread->again = 1;
    } else {
        chimera_nfs4_compound_process(req, status);
    }

} /* chimera_nfs4_compound_complete */

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

    chimera_nfs_info("nfs4 compound call entry");

    req = nfs_request_alloc(thread);

    req->conn                 = conn;
    req->msg                  = msg;
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