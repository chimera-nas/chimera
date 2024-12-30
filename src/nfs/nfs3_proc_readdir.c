#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static int
chimera_nfs3_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request             *req  = arg;
    struct evpl_rpc2_msg           *msg  = req->msg;
    struct READDIR3args            *args = req->args_readdir;
    struct entry3                  *entry;
    struct nfs_nfs3_readdir_cursor *cursor;

    cursor = &req->readdir3_cursor;

    if (cursor->count >= args->count) {
        return -1;
    }

    xdr_dbuf_alloc_space(entry, sizeof(*entry), msg->dbuf);

    entry->fileid    = inum;
    entry->cookie    = cookie;
    entry->nextentry = NULL;

    xdr_dbuf_strncpy(entry, name, name, namelen, msg->dbuf);

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    cursor->count++;

    return 0;
} /* chimera_nfs3_readdir_callback */

static void
chimera_nfs3_readdir_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  cookie,
    uint32_t                  eof,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    struct evpl                      *evpl   = req->thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READDIR3res               *res    = &req->res_readdir;
    struct nfs_nfs3_readdir_cursor   *cursor = &req->readdir3_cursor;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        if ((dir_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) ==
            CHIMERA_NFS3_ATTR_MASK) {
            res->resok.dir_attributes.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(dir_attr,
                                        &res->resok.dir_attributes.attributes);
        } else {
            res->resok.dir_attributes.attributes_follow = 0;
        }
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
    }

    shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, res, msg);

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdirplus_complete */

void
chimera_nfs3_readdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READDIR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct READDIR3res               *res;
    struct nfs_nfs3_readdir_cursor   *cursor;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_readdir(req, args);

    req->args_readdir = args;

    res    = &req->res_readdir;
    cursor = &req->readdir3_cursor;

    cursor->count   = 0;
    cursor->entries = NULL;
    cursor->last    = NULL;

    res->resok.reply.entries = NULL;


    chimera_vfs_readdir(thread->vfs,
                        args->dir.data.data,
                        args->dir.data.len,
                        0,
                        args->cookie,
                        chimera_nfs3_readdir_callback,
                        chimera_nfs3_readdir_complete,
                        req);
} /* chimera_nfs3_readdir */
