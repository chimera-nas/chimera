// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
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
    uint32_t                        dbuf_cur;
    uint32_t                        dbuf_before = req->encoding->dbuf->used;
    struct entry4                  *entry;
    struct READDIR4args            *args = &req->args_compound->argarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor;
    int                             rc;

    cursor = &req->readdir4_cursor;

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    if (!entry) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    rc = xdr_dbuf_opaque_copy(&entry->name, name, namelen, req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    entry->cookie    = cookie;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_array(&entry->attrs, attrmask, 3, req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    uint32_t attrvals_cap = 256;
    if (attrs->va_set_mask & CHIMERA_VFS_ATTR_ACL) {
        attrvals_cap += chimera_nfs4_acl_wire_size(attrs->va_acl);
    }

    rc = xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                               attrvals_cap,
                               req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    chimera_nfs4_marshall_attrs(attrs,
                                args->num_attr_request,
                                args->attr_request,
                                &entry->attrs.num_attrmask,
                                entry->attrs.attrmask,
                                3,
                                entry->attrs.attr_vals.data,
                                &entry->attrs.attr_vals.len,
                                attrvals_cap,
                                req->minorversion,
                                /* entries share the directory's backend/fs */
                                chimera_nfs4_pnfs_layout_type(req->thread->vfs_thread,
                                                              req->thread->shared->vfs,
                                                              req->fh, req->fhlen),
                                chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                             req->fh, req->fhlen),
                                chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s,
                                /* Named-attribute files report NF4REG (their
                                 * mode-derived type), like ordinary directory
                                 * entries -- see the type note in nfs4_proc_getattr. */
                                0);

    dbuf_cur = req->encoding->dbuf->used - dbuf_before;

    if (cursor->count + dbuf_cur > args->maxcount ||
        req->encoding->dbuf->used + 8192 > (uint32_t) req->encoding->dbuf->size) {
        req->encoding->dbuf->used = dbuf_before;
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
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request             *req    = private_data;
    struct READDIR4res             *res    = &req->res_compound.resarray[req->index].opreaddir;
    nfsstat4                        status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct nfs_nfs4_readdir_cursor *cursor = &req->readdir4_cursor;
    uint64_t                        cv;

    /* RFC 7530 §16.24.4: if not even one entry fit in maxcount and we are
     * not at end-of-directory, the buffer is too small. Returning an empty,
     * non-eof page would stall a paging client. */
    if (status == NFS4_OK && !eof && cursor->entries == NULL) {
        status = NFS4ERR_TOOSMALL;
    }

    res->status = status;

    if (status != NFS4_OK) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    cv = verifier ? verifier : cookie;
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

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
    uint64_t cookieverf;
    memcpy(&cookieverf, args->cookieverf, sizeof(cookieverf));
    chimera_vfs_readdir(thread->vfs_thread, &req->cred,
                        handle,
                        attrmask,
                        0,
                        args->cookie,
                        cookieverf,
                        0,
                        chimera_nfs4_readdir_callback,
                        chimera_nfs4_readdir_complete,
                        req);
} /* chimera_nfs4_readdir_open_callback */

/* ---- Named-attribute directory READDIR ----------------------------------
 *
 * The synthetic attr directory has no backend directory to enumerate; its
 * entries are the base file's named streams.  We stat the base once (the entries
 * inherit its owner/timestamps), list the streams (with their file handles), and
 * synthesize one entry per named stream.  memfs returns the full stream list in
 * one shot, so a single pass with index-based cookies covers continuation. */

struct nfs4_attrdir_readdir_ctx {
    struct nfs_request      *req;
    struct chimera_vfs_attrs base_attr;
    uint8_t                  records[64 * 1024];
};

static void
chimera_nfs4_readdir_attrdir_list_callback(
    enum chimera_vfs_error error_code,
    const void            *records,
    uint32_t               records_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct nfs4_attrdir_readdir_ctx *ctx   = private_data;
    struct nfs_request              *req   = ctx->req;
    uint32_t                         in    = 0;
    uint32_t                         index = 0;
    uint32_t                         r_eof = eof;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_readdir_complete(error_code, req->handle, 0, 0, 0, NULL, req);
        free(ctx);
        return;
    }

    while (in < records_len) {
        struct chimera_vfs_stream_entry entry;
        const char                     *name;
        const uint8_t                  *fh;
        struct chimera_vfs_attrs        attr;
        uint32_t                        reclen;

        memcpy(&entry, (const uint8_t *) records + in, sizeof(entry));
        name = (const char *) records + in + sizeof(entry);
        fh   = (const uint8_t *) records + in + sizeof(entry) + entry.name_len;

        reclen = (sizeof(entry) + entry.name_len + entry.fh_len + 7) & ~7u;

        /* Skip the unnamed default fork ("::$DATA") -- it is the file's own data,
         * not a named attribute. */
        if (entry.name_len == 0) {
            in += reclen;
            continue;
        }

        index++;

        /* Index-based cookie: resume after the client's last-seen entry. */
        if (index <= cookie) {
            in += reclen;
            continue;
        }

        attr               = ctx->base_attr;
        attr.va_size       = entry.size;
        attr.va_space_used = entry.alloc;
        attr.va_set_mask  |= CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_SPACE_USED;

        if (entry.fh_len) {
            memcpy(attr.va_fh, fh, entry.fh_len);
            attr.va_fh_len    = entry.fh_len;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
            /* Give each named attribute a distinct, stable fileid derived from
             * its handle (the base inode's ino would collide across streams). */
            attr.va_ino       = chimera_vfs_hash(fh, entry.fh_len);
            attr.va_set_mask |= CHIMERA_VFS_ATTR_INUM;
        }

        if (chimera_nfs4_readdir_callback(attr.va_ino, index, name,
                                          entry.name_len, &attr, req) != 0) {
            /* Entry did not fit: stop here, more remain. */
            r_eof = 0;
            break;
        }

        in += reclen;
    }

    chimera_nfs4_readdir_complete(CHIMERA_VFS_OK, req->handle, index, 0, r_eof,
                                  NULL, req);
    free(ctx);
} /* chimera_nfs4_readdir_attrdir_list_callback */

static void
chimera_nfs4_readdir_attrdir_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs4_attrdir_readdir_ctx *ctx = private_data;
    struct nfs_request              *req = ctx->req;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_readdir_complete(error_code, req->handle, 0, 0, 0, NULL, req);
        free(ctx);
        return;
    }

    ctx->base_attr = *attr;

    chimera_vfs_list_streams(req->thread->vfs_thread, &req->cred,
                             req->handle,
                             0,
                             ctx->records,
                             sizeof(ctx->records),
                             1, /* want per-stream file handles */
                             chimera_nfs4_readdir_attrdir_list_callback,
                             ctx);
} /* chimera_nfs4_readdir_attrdir_getattr_callback */

static void
chimera_nfs4_readdir_attrdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs4_attrdir_readdir_ctx *ctx  = private_data;
    struct nfs_request              *req  = ctx->req;
    struct READDIR4args             *args = &req->args_compound->argarray[req->index].opreaddir;

    if (error_code != CHIMERA_VFS_OK) {
        req->handle = NULL;
        chimera_nfs4_readdir_complete(error_code, NULL, 0, 0, 0, NULL, req);
        free(ctx);
        return;
    }

    req->handle = handle;

    /* Stat the base file once; every named attribute inherits its
     * owner/group/timestamps (size/fh/fileid are overridden per stream). */
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        chimera_nfs4_attr2mask(args->attr_request,
                                               args->num_attr_request),
                        chimera_nfs4_readdir_attrdir_getattr_callback,
                        ctx);
} /* chimera_nfs4_readdir_attrdir_open_callback */

static void
chimera_nfs4_readdir_attrdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct nfs4_attrdir_readdir_ctx *ctx;
    const uint8_t                   *base;
    int                              base_len;

    ctx      = calloc(1, sizeof(*ctx));
    ctx->req = req;

    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        base, base_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_readdir_attrdir_open_callback,
                        ctx);
} /* chimera_nfs4_readdir_attrdir */

void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READDIR4args            *args = &argop->opreaddir;
    struct READDIR4res             *res  = &req->res_compound.resarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* maxcount bounds the entire READDIR4resok. If it cannot even hold the
     * cookie verifier and the empty-directory reply (8 + 4 + 4 bytes), the
     * server returns NFS4ERR_TOOSMALL. */
    if (args->maxcount < 16) {
        res->status = NFS4ERR_TOOSMALL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Write-only attributes (time_*_set) cannot be read back per entry. */
    res->status = chimera_nfs4_validate_getattr_request(args->num_attr_request,
                                                        args->attr_request);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        nfs4_root_readdir(thread, req);
        return;
    }

    /* RFC 7530 §16.24: cookie values 1 and 2 are reserved and must never be
     * sent by a client (0 means "start of directory"). The VFS backends emit
     * only cookie 0 or values >= 3 for regular directories, so a reserved
     * cookie here is a client error. The pseudo-root, handled above, uses its
     * own export-index cookie space and is intentionally exempt. */
    if (args->cookie == 1 || args->cookie == 2) {
        res->status = NFS4ERR_BAD_COOKIE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    cursor = &req->readdir4_cursor;

    /* Fixed READDIR4resok overhead counted against maxcount: cookieverf (8)
     * + the dirlist4 "entry present" and "eof" booleans (4 each). Keeping
     * this tight ensures a small maxcount on a continuation still admits at
     * least one entry rather than returning an empty, non-eof page. */
    cursor->count   = 16;
    cursor->entries = NULL;
    cursor->last    = NULL;

    res->resok4.reply.entries = NULL;

    /* READDIR of a synthetic named-attribute directory: enumerate the base
     * file's named streams instead of a real directory. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        chimera_nfs4_readdir_attrdir(thread, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_readdir_open_callback,
                        req);

} /* chimera_nfs4_readdir */
