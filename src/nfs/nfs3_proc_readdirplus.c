#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"

static int
chimera_nfs3_readdirplus_callback(
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request     *req = arg;
    struct evpl_rpc2_msg   *msg = req->msg;
    struct READDIRPLUS3res *res = &req->res_readdirplus;
    struct entryplus3      *entry, *preventry;

    chimera_nfs_debug("readdir callback: cookie %lu name %.*s",
                      cookie, namelen, name);

    preventry = res->resok.reply.entries;

    xdr_dbuf_reserve_ll(&res->resok.reply, entries, msg->dbuf);

    entry = res->resok.reply.entries;

    entry->cookie = cookie;
    xdr_dbuf_strncpy(entry, name, name, namelen, msg->dbuf);

    if (attrs->va_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        entry->name_attributes.attributes_follow = 1;
        chimera_nfs3_marshall_attrs(attrs, &entry->name_attributes.attributes);
    } else {
        entry->name_attributes.attributes_follow = 0;
    }

    if (attrs->va_mask & CHIMERA_VFS_ATTR_FH) {
        entry->name_handle.handle_follows = 1;

        xdr_dbuf_opaque_copy(&entry->name_handle.handle.data,
                             attrs->va_fh,
                             attrs->va_fh_len,
                             msg->dbuf);

    } else {
        entry->name_handle.handle_follows = 0;
    }

    entry->nextentry = preventry;
    return 0;
} /* chimera_nfs3_readdir_callback */

static void
chimera_nfs3_readdirplus_complete(
    enum chimera_vfs_error error_code,
    uint64_t               cookie,
    uint32_t               eof,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    struct evpl                      *evpl   = req->thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READDIRPLUS3res           *res    = &req->res_readdirplus;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        res->resok.dir_attributes.attributes_follow = 0;
        res->resok.reply.eof                        = !!eof;
    }

    shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, res, msg);

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdirplus_complete */

void
chimera_nfs3_readdirplus(
    struct evpl             *evpl,
    struct evpl_rpc2_conn   *conn,
    struct READDIRPLUS3args *args,
    struct evpl_rpc2_msg    *msg,
    void                    *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct READDIRPLUS3res           *res;
    uint64_t                          attrmask;

    req                   = nfs_request_alloc(thread, conn, msg);
    req->args_readdirplus = args;

    res = &req->res_readdirplus;

    res->resok.reply.entries = NULL;

    attrmask = CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH;

    chimera_vfs_readdir(thread->vfs,
                        args->dir.data.data,
                        args->dir.data.len,
                        attrmask,
                        args->cookie,
                        chimera_nfs3_readdirplus_callback,
                        chimera_nfs3_readdirplus_complete,
                        req);
} /* chimera_nfs3_readdirplus */
