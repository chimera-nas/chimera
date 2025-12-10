// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
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

    entry = xdr_dbuf_alloc_space(sizeof(*entry), msg->dbuf);
    chimera_nfs_abort_if(entry == NULL, "Failed to allocate space");

    entry->fileid    = inum;
    entry->cookie    = cookie;
    entry->nextentry = NULL;

    int rc = xdr_dbuf_alloc_string(&entry->name, name, namelen, msg->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate string");

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
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    struct evpl                      *evpl   = req->thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READDIR3res               *res    = &req->res_readdir;
    struct nfs_nfs3_readdir_cursor   *cursor = &req->readdir3_cursor;
    int                               rc;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res->resok.dir_attributes, dir_attr);
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, res, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdirplus_complete */

static void
chimera_nfs3_readdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READDIR3args              *args   = req->args_readdir;
    struct READDIR3res               *res    = &req->res_readdir;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_readdir(thread->vfs_thread,
                            handle,
                            0,
                            CHIMERA_NFS3_ATTR_MASK,
                            args->cookie,
                            chimera_nfs3_readdir_callback,
                            chimera_nfs3_readdir_complete,
                            req);

    } else {
        res->status = chimera_vfs_error_to_nfsstat3(error_code);
        rc          = shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, res, msg);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_readdir_open_callback */

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

    chimera_vfs_open(thread->vfs_thread,
                     args->dir.data.data,
                     args->dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_readdir_open_callback,
                     req);

} /* chimera_nfs3_readdir */
