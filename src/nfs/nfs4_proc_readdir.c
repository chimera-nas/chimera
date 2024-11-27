#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static int
chimera_nfs4_readdir_callback(
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request   *req = arg;
    struct entry4        *entry, *prev_entry;
    struct READDIR4args  *args = &req->args_compound->argarray[req->index].
        opreaddir;
    struct READDIR4resok *res = &req->res_compound.resarray[req->index].
        opreaddir
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
    struct READDIR4res *res = &req->res_compound.resarray[req->index].
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

void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READDIR4args *args = &argop->opreaddir;
    struct READDIR4res  *res  = &resop->opreaddir;
    uint64_t             attrmask;

    res->resok4.reply.entries = NULL;

    attrmask = chimera_nfs4_getattr2mask(args->attr_request,
                                         args->num_attr_request);

    chimera_vfs_readdir(thread->vfs,
                        req->fh,
                        req->fhlen,
                        attrmask,
                        args->cookie,
                        chimera_nfs4_readdir_callback,
                        chimera_nfs4_readdir_complete,
                        req);
} /* chimera_nfs4_readdir */