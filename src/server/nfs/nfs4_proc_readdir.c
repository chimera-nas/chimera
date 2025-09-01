// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
static int
chimera_nfs4_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request             *req = arg;
    struct evpl_rpc2_msg           *msg = req->msg;
    uint32_t                        dbuf_cur;
    uint32_t                        dbuf_before = msg->dbuf->used;
    struct entry4                  *entry;
    struct READDIR4args            *args = &req->args_compound->argarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor;

    cursor = &req->readdir4_cursor;

    xdr_dbuf_alloc_space(entry, sizeof(*entry), req->msg->dbuf);

    xdr_dbuf_opaque_copy(&entry->name, name, namelen, req->msg->dbuf);

    entry->cookie    = cookie;
    entry->nextentry = NULL;

    xdr_dbuf_reserve(&entry->attrs,
                     attrmask,
                     args->num_attr_request,
                     req->msg->dbuf);

    xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                          256,
                          req->msg->dbuf);

    chimera_nfs4_marshall_attrs(attrs,
                                args->num_attr_request,
                                args->attr_request,
                                &entry->attrs.num_attrmask,
                                entry->attrs.attrmask,
                                3,
                                entry->attrs.attr_vals.data,
                                &entry->attrs.attr_vals.len);

    dbuf_cur = msg->dbuf->used - dbuf_before;

    if (cursor->count + dbuf_cur > args->maxcount) {
        return -1;
    }

    cursor->count += dbuf_cur;

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    return 0;
} /* chimera_nfs4_readdir_callback */

static void
chimera_nfs4_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request             *req = private_data;
    struct READDIR4res             *res = &req->res_compound.resarray[req->index
        ].
        opreaddir;
    nfsstat4                        status = chimera_nfs4_errno_to_nfsstat4(
        error_code);
    struct nfs_nfs4_readdir_cursor *cursor = &req->readdir4_cursor;

    res->status = status;

    memcpy(res->resok4.cookieverf, &cookie, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = eof;
    res->resok4.reply.entries = cursor->entries;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_readdir_complete */

static void
chimera_nfs4_readdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct READDIR4args              *args   = &req->args_compound->argarray[req->index].opreaddir;
    struct READDIR4res               *res    = &req->res_compound.resarray[req->index].opreaddir;
    uint64_t                          attrmask;

    req->handle = handle;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }
    attrmask = chimera_nfs4_attr2mask(args->attr_request,
                                      args->num_attr_request);
    chimera_vfs_readdir(thread->vfs_thread,
                        handle,
                        attrmask,
                        0,
                        args->cookie,
                        chimera_nfs4_readdir_callback,
                        chimera_nfs4_readdir_complete,
                        req);
} /* chimera_nfs4_readlink_open_callback */
void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READDIR4res             *res = &req->res_compound.resarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor;

    cursor = &req->readdir4_cursor;

    cursor->count   = 256;
    cursor->entries = NULL;
    cursor->last    = NULL;

    res->resok4.reply.entries = NULL;

    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_readdir_open_callback,
                     req);

} /* chimera_nfs4_readdir */