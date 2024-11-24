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
    struct nfs4_request *req,
    nfsstat4             status);


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

    chimera_vfs_getrootfh(thread->vfs,
                          req->fh,
                          &req->fhlen);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */

static void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
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

static inline void
chimera_nfs4_attr_append_uint32(
    void   **attrs,
    uint32_t value)
{
    *(uint32_t *) *attrs = chimera_nfs_hton32(value);
    *attrs              += sizeof(uint32_t);
} /* chimera_nfs4_attr_append_uint32 */

static void
chimera_nfs4_attr_append_uint64(
    void   **attrs,
    uint64_t value)
{
    *(uint64_t *) *attrs = chimera_nfs_hton64(value);
    *attrs              += sizeof(uint64_t);
} /* chimera_nfs4_attr_append_uint64 */

static void
chimera_nfs4_attr_append_utf8str(
    void      **attrs,
    const char *value,
    int         len)
{
    int pad;

    chimera_nfs4_attr_append_uint32(attrs, len);
    memcpy(*attrs, value, len);

    pad = (4 - (len % 4)) % 4;

    if (pad ) {
        memset(*attrs + len, 0, pad);
    }
    *attrs += len + pad;
} /* chimera_nfs4_attr_append_utf8str */

static void
chimera_nfs4_getattr_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  attr_mask,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs4_request *req  = private_data;
    GETATTR4args        *args = &req->args->argarray[req->index].
        opgetattr;
    GETATTR4res         *res = &req->res.resarray[req->index].opgetattr
    ;
    uint32_t            *req_mask     = args->attr_request;
    int                  num_req_mask = args->num_attr_request;
    uint32_t            *rsp_mask;
    int                  num_rsp_mask = 0;
    void                *attrs, *attrsbase;

    res->status = NFS4_OK;

    xdr_dbuf_reserve(&res->resok4.obj_attributes,
                     attrmask,
                     3,
                     req->msg->dbuf);

    xdr_dbuf_alloc_opaque(&res->resok4.obj_attributes.attr_vals,
                          4096,
                          req->msg->dbuf);

    rsp_mask = res->resok4.obj_attributes.attrmask;

    memset(rsp_mask, 0, sizeof(uint32_t) * 3);

    attrs     = res->resok4.obj_attributes.attr_vals.data;
    attrsbase = attrs;

    if (num_req_mask >= 1) {
        chimera_nfs_debug("getattr request mask %08x", req_mask[0]);
        if (req_mask[0] & (1 << FATTR4_SUPPORTED_ATTRS)) {
            rsp_mask[0] |= (1 << FATTR4_SUPPORTED_ATTRS);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
            chimera_nfs4_attr_append_uint32(&attrs,
                                            FATTR4_SUPPORTED_ATTRS |
                                            FATTR4_TYPE |
                                            FATTR4_FH_EXPIRE_TYPE |
                                            FATTR4_CHANGE |
                                            FATTR4_SIZE |
                                            FATTR4_LINK_SUPPORT |
                                            FATTR4_SYMLINK_SUPPORT |
                                            FATTR4_NAMED_ATTR |
                                            FATTR4_FSID |
                                            FATTR4_UNIQUE_HANDLES |
                                            FATTR4_LEASE_TIME |
                                            FATTR4_RDATTR_ERROR |
                                            FATTR4_FILEHANDLE |
                                            FATTR4_FILEID |
                                            FATTR4_MODE |
                                            FATTR4_NUMLINKS |
                                            FATTR4_OWNER |
                                            FATTR4_OWNER_GROUP |
                                            FATTR4_SPACE_USED |
                                            FATTR4_TIME_ACCESS |
                                            FATTR4_TIME_MODIFY |
                                            FATTR4_TIME_METADATA);
        }

        if (req_mask[0] & (1 << FATTR4_TYPE)) {
            rsp_mask[0] |= (1 << FATTR4_TYPE);
            num_rsp_mask = 1;

            if (S_ISREG(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4REG);
            } else if (S_ISDIR(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4DIR);
            } else if (S_ISCHR(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4CHR);
            } else if (S_ISBLK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4BLK);
            } else if (S_ISFIFO(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4FIFO);
            } else if (S_ISSOCK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4SOCK);
            } else if (S_ISLNK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4LNK);
            } else {
                chimera_nfs4_attr_append_uint32(&attrs, NF4REG);
            }
        }

        if (req_mask[0] & (1 << FATTR4_FH_EXPIRE_TYPE)) {
            rsp_mask[0] |= (1 << FATTR4_FH_EXPIRE_TYPE);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_CHANGE)) {
            rsp_mask[0] |= (1 << FATTR4_CHANGE);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_SIZE)) {
            rsp_mask[0] |= (1 << FATTR4_SIZE);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_size);
        }

        if (req_mask[0] & (1 << FATTR4_LINK_SUPPORT)) {
            rsp_mask[0] |= (1 << FATTR4_LINK_SUPPORT);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_SYMLINK_SUPPORT)) {
            rsp_mask[0] |= (1 << FATTR4_SYMLINK_SUPPORT);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_NAMED_ATTR)) {
            rsp_mask[0] |= (1 << FATTR4_NAMED_ATTR);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_FSID)) {
            rsp_mask[0] |= (1 << FATTR4_FSID);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, 42);
            chimera_nfs4_attr_append_uint64(&attrs, 42);

        }

        if (req_mask[0] & (1 << FATTR4_FILEID)) {
            rsp_mask[0] |= (1 << FATTR4_FILEID);
            num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_ino);
        }
    }

    if (num_req_mask >= 2) {
        if (req_mask[1] & (1 << (FATTR4_MODE - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_MODE - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mode & ~S_IFMT);
        }

        if (req_mask[1] & (1 << (FATTR4_NUMLINKS - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_NUMLINKS - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_nlink);
        }

        if (req_mask[1] & (1 << (FATTR4_OWNER - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_OWNER - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str(&attrs, "root", 4);
        }

        if (req_mask[1] & (1 << (FATTR4_OWNER_GROUP - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_OWNER_GROUP - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str(&attrs, "root", 4);
        }

        if (req_mask[1] & (1 << (FATTR4_SPACE_USED - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_SPACE_USED - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_size);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_ACCESS - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_TIME_ACCESS - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_atime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_atime.tv_nsec);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_MODIFY - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_TIME_MODIFY - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_mtime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mtime.tv_nsec);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_METADATA - 32))) {
            rsp_mask[1] |= (1 << (FATTR4_TIME_METADATA - 32));
            num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_ctime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_ctime.tv_nsec);
        }
    }

    chimera_nfs_debug("sending getattr repply with %d words and %d bytes",
                      num_rsp_mask,
                      attrs - attrsbase);

    res->resok4.obj_attributes.num_attrmask  = num_rsp_mask;
    res->resok4.obj_attributes.attr_vals.len = attrs - attrsbase;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getattr_complete */

static void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    uint64_t      attr_mask = 0;
    GETATTR4args *args      = &argop->opgetattr;

    attr_mask = chimera_nfs4_getattr2mask(args->attr_request,
                                          args->num_attr_request);

    chimera_nfs_debug("getattr to mask %08x", attr_mask);

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
    struct nfs4_request *req = arg;
    struct entry4       *entry, *prev_entry;

    READDIR4resok       *res = &req->res.resarray[req->index].opreaddir.resok4;

    chimera_nfs_debug("readdir callback: cookie %llu, name %.*s, attrs %p",
                      cookie,
                      namelen,
                      name,
                      attrs);

    prev_entry = res->reply.entries;

    xdr_dbuf_reserve(&res->reply,
                     entries,
                     1,
                     req->msg->dbuf);

    res->reply.num_entries = 1;

    entry = res->reply.entries;

    entry->cookie              = cookie;
    entry->attrs.num_attrmask  = 0;
    entry->attrs.attr_vals.len = 0;
    entry->num_nextentry       = 0;
    xdr_dbuf_opaque_copy(&entry->name, name, namelen, req->msg->dbuf);

    if (prev_entry) {
        entry->nextentry     = prev_entry;
        entry->num_nextentry = 1;
    } else {
        entry->nextentry     = NULL;
        entry->num_nextentry = 0;
    }

    return 0;
} /* chimera_nfs4_readdir_callback */

static void
chimera_nfs4_readdir_complete(
    enum chimera_vfs_error error_code,
    uint64_t               cookie,
    uint32_t               eof,
    void                  *private_data)
{
    struct nfs4_request *req    = private_data;
    READDIR4res         *res    = &req->res.resarray[req->index].opreaddir;
    nfsstat4             status = chimera_nfs4_errno_to_nfsstat4(error_code);

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
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    READDIR4args *args = &argop->opreaddir;
    READDIR4res  *res  = &resop->opreaddir;

    res->resok4.reply.entries     = NULL;
    res->resok4.reply.num_entries = 0;

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
    struct nfs4_request              *req,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    CLOSE4args *args = &argop->opclose;

    chimera_nfs_debug("close: seqid %u", args->seqid);
} /* chimera_nfs4_close */

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