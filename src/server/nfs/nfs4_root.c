// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "common/logging.h"
#include "common/macros.h"


SYMBOL_EXPORT void
nfs4_root_getattr(
    struct chimera_server_nfs_thread *thread,
    struct chimera_vfs_attrs         *attr,
    uint64_t                          attr_mask)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               num_links;

    num_links = chimera_nfs_export_count(shared) + 2;

    memset(attr, 0, sizeof(*attr));

    attr->va_set_mask = CHIMERA_VFS_ATTR_MASK_STAT;

    /* Synthetic root directory attribute */
    attr->va_mode  = S_IFDIR | 0755;
    attr->va_nlink = num_links;
    attr->va_uid   = 0;
    attr->va_gid   = 0;
    attr->va_size  = 4096;
    clock_gettime(CLOCK_REALTIME, &attr->va_atime);
    attr->va_mtime = attr->va_atime;
    attr->va_ctime = attr->va_atime;
    attr->va_ino   = 2;
    attr->va_dev   = 0;
    attr->va_rdev  = 0;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_total = 0;
        attr->va_fs_space_free  = 0;
        attr->va_fs_space_avail = 0;
        attr->va_fs_space_used  = 0;
        attr->va_fs_files_total = 0;
        attr->va_fs_files_free  = 0;
        attr->va_fs_files_avail = 0;
        attr->va_fsid           = 0;
    }
} /* nfs4_getattr_root */

struct nfs4_root_readdir_lookup_ctx {
    enum chimera_vfs_error error_code;
    struct entry4       *entry;
    struct READDIR4args *args;
};

static void
nfs4_root_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].oplookup;

    res->status = status;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS4 lookup: no file handle was returned");

        memcpy(req->fh, attr->va_fh, attr->va_fh_len);
        req->fhlen = attr->va_fh_len;
    }

    chimera_nfs4_compound_complete(req, status);
} /* nfs4_root_lookup_complete */

SYMBOL_EXPORT void
nfs4_root_lookup(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req)
{
    struct chimera_server_nfs_shared *shared = nfs_thread->shared;
    struct LOOKUP4args               *args   = &req->args_compound->argarray[req->index].oplookup;
    struct LOOKUP4res                *res    = &req->res_compound.resarray[req->index].oplookup;
    int                               rc;
    char                             *full_path = NULL;
    uint8_t                           root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          root_fh_len;

    /**
     * We are doing a lookup on the export path. The path
     * can contain multiple components, so we need to use
     * the chimera_vfs_lookup() logic from the root file handle
     * to find the mount point file handle.
     */

    rc = chimera_nfs_find_export_path(shared, args->objname.data, args->objname.len, &full_path);
    if (rc) {
        // Export not found, return error
        chimera_nfs_error("lookup for unknown export '%.*s'",
                          args->objname.len, (const char *) args->objname.data);
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOENT);
        return;
    }
    req->handle = NULL; // Ensure handle is NULL so that the lookup callback does not attempt to release it
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(nfs_thread->vfs_thread,
                       &req->cred,
                       root_fh,
                       root_fh_len,
                       full_path,
                       strlen(full_path),
                       CHIMERA_VFS_ATTR_FH,
                       0,
                       nfs4_root_lookup_complete,
                       req);
    free(full_path);
    return;

} /* nfs4_root_lookup */

static void
nfs4_root_readdir_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attrs,
    void                     *private_data)
{
    struct nfs4_root_readdir_lookup_ctx *ctx   = private_data;
    struct READDIR4args                 *args  = ctx->args;
    struct entry4                       *entry = ctx->entry;

    if (error_code != CHIMERA_VFS_OK) {
        abort();
    }
    chimera_nfs4_marshall_attrs(attrs,
                                args->num_attr_request,
                                args->attr_request,
                                &entry->attrs.num_attrmask,
                                entry->attrs.attrmask,
                                3,
                                entry->attrs.attr_vals.data,
                                &entry->attrs.attr_vals.len);
} /* nfs4_root_readdir_lookup_callback */

struct nfs4_root_readdir_itr_ctx {
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    struct chimera_vfs_thread      *vfs_thread;
    struct nfs_request             *req;
    struct nfs_nfs4_readdir_cursor *cursor;
    uint64_t                        attrmask;
    uint64_t                        cookie;
    int                             index;
    enum chimera_vfs_error error_code;
};

static int
nfs4_root_readdir_itr_cb(
    const struct chimera_nfs_export *export,
    void                            *private_data)
{
    struct nfs4_root_readdir_itr_ctx   *ctx    = private_data;
    struct nfs_request                 *req    = ctx->req;
    struct READDIR4args                *args   = &req->args_compound->argarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor     *cursor = ctx->cursor;
    uint32_t                            dbuf_cur;
    uint32_t                            dbuf_before = req->encoding->dbuf->used;
    struct nfs4_root_readdir_lookup_ctx lookup_ctx;
    struct entry4                      *entry;
    const char                         *export_name = export->name;
    int                                 rc;

    if (ctx->index < ctx->cookie) {
        ctx->index++;
        return 0;
    }
    if (ctx->error_code != CHIMERA_VFS_OK) {
        return -1;
    }
    // remove leading '/' from export name if present
    while (export_name && export_name[0] == '/') {
        export_name++;
    }
    if (!export_name) {
        chimera_nfs_error("Invalid export name %s for export path %s", export->name, export->path);
        return 0;
    }

    // Report error if any / character is present in the export name after removing leading /
    if (strchr(export_name, '/')) {
        chimera_nfs_error("Invalid export name %s for export path %s: export name cannot contain '/' character",
                          export_name, export->path);
        return 0;
    }
    /* allocate a new entry and populate it with the export name and path */
    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    if (!entry) {
        req->encoding->dbuf->used = dbuf_before;
        ctx->error_code           = CHIMERA_VFS_EOVERFLOW;
        return -1;
    }

    rc = xdr_dbuf_opaque_copy(&entry->name, export_name, strlen(export_name), req->encoding->dbuf);
    if (rc) {
        // TODO: do we need to free the entry here?
        req->encoding->dbuf->used = dbuf_before;
        ctx->error_code           = CHIMERA_VFS_EOVERFLOW;
        return -1;
    }
    entry->cookie    = ctx->index;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_array(&entry->attrs, attrmask, 3, req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        ctx->error_code           = CHIMERA_VFS_EOVERFLOW;
        return -1;
    }

    rc = xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                               256,
                               req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        ctx->error_code           = CHIMERA_VFS_EOVERFLOW;
        return -1;
    }
    lookup_ctx.entry      = entry;
    lookup_ctx.args       = args;
    lookup_ctx.error_code = CHIMERA_VFS_OK;
    chimera_vfs_lookup(ctx->vfs_thread,
                       &req->cred,
                       ctx->root_fh,
                       ctx->root_fh_len,
                       export->path,
                       strlen(export->path),
                       CHIMERA_VFS_ATTR_FH,
                       ctx->attrmask,
                       nfs4_root_readdir_lookup_callback,
                       &lookup_ctx);
    if (lookup_ctx.error_code != CHIMERA_VFS_OK) {
        ctx->error_code           = lookup_ctx.error_code;
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }
    dbuf_cur = req->encoding->dbuf->used - dbuf_before;

    if (cursor->count + dbuf_cur > args->maxcount ||
        req->encoding->dbuf->used + 8192 > (uint32_t) req->encoding->dbuf->size) {
        req->encoding->dbuf->used = dbuf_before;
        ctx->error_code           = CHIMERA_VFS_EOVERFLOW;
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
} /* nfs4_root_readdir_itr_cb */

SYMBOL_EXPORT void
nfs4_root_readdir(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req)
{
    struct READDIR4args              *args   = &req->args_compound->argarray[req->index].opreaddir;
    struct chimera_server_nfs_shared *shared = nfs_thread->shared;
    struct nfs4_root_readdir_itr_ctx  ctx;
    struct nfs_nfs4_readdir_cursor   *cursor;
    struct READDIR4res               *res = &req->res_compound.resarray[req->index].opreaddir;
    int                               eof = 1;


    cursor                    = &req->readdir4_cursor;
    res->resok4.reply.entries = NULL;
    res->status               = NFS4_OK;

    cursor->count   = 256;
    cursor->entries = NULL;
    cursor->last    = NULL;

    ctx.error_code = CHIMERA_VFS_OK;
    ctx.vfs_thread = nfs_thread->vfs_thread;
    ctx.req        = req;
    ctx.cursor     = cursor;
    ctx.index      = 0;
    ctx.cookie     = args->cookie;
    ctx.attrmask   = chimera_nfs4_attr2mask(args->attr_request,
                                            args->num_attr_request);
    chimera_vfs_get_root_fh(ctx.root_fh, &ctx.root_fh_len);

    /* Iterate over exports and populate the readdir response */
    chimera_nfs_iterate_exports(shared, nfs4_root_readdir_itr_cb, &ctx);

    if (ctx.error_code == CHIMERA_VFS_EOVERFLOW) {
        /* If we hit overflow, it means there are more entries to be read */
        eof = 0;
    } else if (ctx.error_code != CHIMERA_VFS_OK) {
        /* For any other error, return the error code */
        chimera_nfs_error("Error iterating exports for readdir: %d", ctx.error_code);
        res->status = chimera_nfs4_errno_to_nfsstat4(ctx.error_code);
    }

    res->resok4.reply.eof     = eof;
    res->resok4.reply.entries = cursor->entries;

    chimera_nfs4_compound_complete(req, res->status);
} /* nfs4_root_readdir */