#include "nfs3_procs.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "common/format.h"
#include "uthash/utlist.h"
#include "vfs/vfs_procs.h"


#define CHIMERA_NFS3_ATTR_MASK ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

void
chimera_nfs3_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->nfs_v3.send_reply_NFSPROC3_NULL(evpl, msg);
} /* nfs3_null */

static void
chimera_nfs3_getattr_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  attr_mask,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct GETATTR3res                res;

    res.status = NFS3_OK;

    chimera_nfs3_marshall_attrs(attr, &res.resok.obj_attributes);

    shared->nfs_v3.send_reply_NFSPROC3_GETATTR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_getattr_complete */

void
chimera_nfs3_getattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    GETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req               = nfs_request_alloc(thread);
    req->args_getattr = args;
    req->conn         = conn;
    req->msg          = msg;

    chimera_vfs_getattr(thread->vfs,
                        args->object.data.data,
                        args->object.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_getattr_complete,
                        req);
} /* chimera_nfs3_getattr */

void
chimera_nfs3_setattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_SETATTR
} /* chimera_nfs3_setattr */

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

    res.status = NFS3_OK;
    xdr_dbuf_opaque_copy(&res.resok.object.data, fh, fhlen, msg->dbuf);
    res.resok.dir_attributes.attributes_follow = 0;
    res.resok.obj_attributes.attributes_follow = 0;

    shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_lookup_complete */

void
chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LOOKUP3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req              = nfs_request_alloc(thread);
    req->args_lookup = args;
    req->conn        = conn;
    req->msg         = msg;


    chimera_nfs_debug("lookup: name %.*s",
                      args->what.name.len, args->what.name.str);

    chimera_vfs_lookup(thread->vfs,
                       args->what.dir.data.data,
                       args->what.dir.data.len,
                       args->what.name.str,
                       args->what.name.len,
                       chimera_nfs3_lookup_complete,
                       req);
} /* chimera_nfs3_lookup */

void
chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    ACCESS3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_ACCESS
} /* chimera_nfs3_access */

void
chimera_nfs3_readlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READLINK3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READLINK
} /* chimera_nfs3_readlink */

void
chimera_nfs3_read(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READ3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READ
} /* chimera_nfs3_read */

void
chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    WRITE3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_WRITE
} /* chimera_nfs3_write */

void
chimera_nfs3_create(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    CREATE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_CREATE
} /* chimera_nfs3_create */

void
chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_MKDIR
} /* chimera_nfs3_mkdir */

void
chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SYMLINK3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_SYMLINK
} /* chimera_nfs3_symlink */

void
chimera_nfs3_mknod(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKNOD3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_MKNOD
} /* chimera_nfs3_mknod */

void
chimera_nfs3_remove(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    REMOVE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_REMOVE
} /* chimera_nfs3_remove */

void
chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RMDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_RMDIR
} /* chimera_nfs3_rmdir */

void
chimera_nfs3_rename(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RENAME3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_RENAME
} /* chimera_nfs3_rename */

void
chimera_nfs3_link(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LINK3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_LINK
} /* chimera_nfs3_link */

void
chimera_nfs3_readdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READDIR
} /* chimera_nfs3_readdir */

static int
chimera_nfs3_readdir_callback(
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

    entry->name_attributes.attributes_follow = 1;
    chimera_nfs3_marshall_attrs(attrs, &entry->name_attributes.attributes);

    entry->name_handle.handle_follows = 0;

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

    res->status                                 = NFS3_OK;
    res->resok.dir_attributes.attributes_follow = 0;
    res->resok.reply.eof                        = !!eof;

    shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, res, msg);

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdirplus_complete */

void
chimera_nfs3_readdirplus(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIRPLUS3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct READDIRPLUS3res           *res;

    req                   = nfs_request_alloc(thread);
    req->args_readdirplus = args;
    req->conn             = conn;
    req->msg              = msg;

    res = &req->res_readdirplus;

    res->resok.reply.entries = NULL;

    chimera_vfs_readdir(thread->vfs,
                        args->dir.data.data,
                        args->dir.data.len,
                        args->cookie,
                        chimera_nfs3_readdir_callback,
                        chimera_nfs3_readdirplus_complete,
                        req);
} /* chimera_nfs3_readdirplus */

void
chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSSTAT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_FSSTAT
} /* chimera_nfs3_fsstat */

static void
chimera_nfs3_fsinfo_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  attr_mask,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSINFO3res                 res;

    res.status                    = NFS3_OK;
    res.resok.maxfilesize         = UINT64_MAX;
    res.resok.time_delta.seconds  = 0;
    res.resok.time_delta.nseconds = 1;
    res.resok.rtmax               = 1024 * 1024;
    res.resok.rtpref              = 1024 * 1024;
    res.resok.rtmult              = 4096;
    res.resok.wtmax               = 1024 * 1024;
    res.resok.wtpref              = 1024 * 1024;
    res.resok.dtpref              = 1024 * 1024;
    res.resok.wtmult              = 4096;
    res.resok.properties          = 0;

    shared->nfs_v3.send_reply_NFSPROC3_FSINFO(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsinfo_complete */

void
chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSINFO3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    char                              fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->fsroot.data.data, args->fsroot.data.
               len);

    chimera_nfs_debug("fsinfo fh %s", fhstr);

    req              = nfs_request_alloc(thread);
    req->args_fsinfo = args;
    req->conn        = conn;
    req->msg         = msg;

    chimera_vfs_getattr(thread->vfs,
                        args->fsroot.data.data,
                        args->fsroot.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_fsinfo_complete,
                        req);

} /* chimera_nfs3_fsinfo */

void
chimera_nfs3_pathconf(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    PATHCONF3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_PATHCONF
} /* chimera_nfs3_pathconf */

void
chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    COMMIT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_COMMIT
} /* chimera_nfs3_commit */