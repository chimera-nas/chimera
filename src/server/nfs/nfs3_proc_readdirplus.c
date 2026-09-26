// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "nfs3_compound.h"

static void
chimera_nfs3_readdirplus_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct nfs3_compound *ctx = private_data;
    struct nfs_request   *req = ctx->req;

    req->encoding->dbuf->used = ctx->arena_mark;
    memset(&req->readdirplus3_cursor, 0, sizeof(req->readdirplus3_cursor));
    req->readdirplus3_cursor.count = 256;
} /* chimera_nfs3_readdirplus_reset */

static int
chimera_nfs3_readdirplus_callback(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs3_compound               *ctx  = arg;
    struct nfs_request                 *req  = ctx->req;
    struct READDIRPLUS3args            *args = req->args_readdirplus;
    struct entryplus3                  *entry;
    struct nfs_nfs3_readdirplus_cursor *cursor;
    uint32_t                            entry_cur, dirinfo_cur, fh_cur = 0;
    int                                 rc;

    cursor = &req->readdirplus3_cursor;

    if (req->encoding->dbuf->size - req->encoding->dbuf->used <
        sizeof(*entry) + (uint32_t) namelen + CHIMERA_NFS_FH_MAX + 128) {
        return -1;
    }

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    chimera_nfs_abort_if(entry == NULL, "Failed to allocate space");

    entry->cookie    = cookie;
    entry->fileid    = inum;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_string(&entry->name, name, namelen, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate string");

    chimera_nfs3_set_post_op_attr(&entry->name_attributes, attrs);

    if (attrs->va_set_mask & CHIMERA_VFS_ATTR_FH) {
        uint8_t wire[CHIMERA_NFS_FH_MAX];
        int     wirelen;

        /* Entries share the directory's export; wrap each child handle. */
        chimera_nfs_fh_encode(req, attrs->va_fh, attrs->va_fh_len, wire, &wirelen);
        entry->name_handle.handle_follows = 1;

        rc = xdr_dbuf_opaque_copy(&entry->name_handle.handle.data,
                                  wire,
                                  wirelen,
                                  req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to copy opaque");

        fh_cur = 4 + ((wirelen + 3) & ~3);
    } else {
        entry->name_handle.handle_follows = 0;
    }

    /* XDR wire size of directory info (fileid + name + cookie) per RFC 1813:
     * value_follows (4) + fileid3 (8) + filename3 (4 + padded len) + cookie3 (8) */
    dirinfo_cur = 24 + ((namelen + 3) & ~3);

    /* RFC 1813 3.3.17: maxcount bounds the XDR-encoded READDIRPLUS3resok, so
    * the budget has to be charged in wire bytes.  The dbuf bytes this entry
    * consumed are the in-memory entryplus3 (pointers, xdr_string descriptors,
    * padding) and over-charge by a fixed per-entry delta, which made the
    * server return fewer entries than maxcount allowed.  On the wire an
    * entryplus3 adds a post_op_attr (4, plus fattr3's 84 when present) and a
    * post_op_fh3 (4, plus the padded handle when present) to the dirinfo. */
    entry_cur = dirinfo_cur + 4 +
        (entry->name_attributes.attributes_follow ? 84 : 0) + 4 + fh_cur;

    if (cursor->count + entry_cur > args->maxcount ||
        cursor->dircount + dirinfo_cur > args->dircount) {
        chimera_nfs_debug("readdirplus: exceeded limits (count %d+%d vs max %d, dircount %d+%d vs max %d)",
                          (int) cursor->count, entry_cur, args->maxcount,
                          (int) cursor->dircount, dirinfo_cur, args->dircount);
        return -1;
    }

    cursor->count    += entry_cur;
    cursor->dircount += dirinfo_cur;

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    return 0;
} /* chimera_nfs3_readdir_callback */

static void
chimera_nfs3_readdirplus_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req      = ctx->req;
    const struct chimera_vfs_compound_op *op       = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *dir_attr = &op->dir_post_attr;
    uint32_t                              eof      = op->eof;
    uint64_t                              verifier = op->r_verifier;

    struct chimera_server_nfs_shared     *shared = req->thread->shared;
    struct evpl                          *evpl   = req->thread->evpl;
    struct READDIRPLUS3res               *res    = &req->res_readdirplus;
    struct nfs_nfs3_readdirplus_cursor   *cursor = &req->readdirplus3_cursor;
    int                                   rc;

    res->status = nfs3_compound_status(ctx);

    /* RFC 1813 3.3.17: if maxcount/dircount were too small to hold even a
     * single entry (dircount == 0 being the degenerate case) and we are not at
     * end-of-directory, report TOOSMALL rather than an empty, non-eof reply
     * that a paging client would retry forever. */
    if (res->status == NFS3_OK && !eof && cursor->entries == NULL) {
        res->status = NFS3ERR_TOOSMALL;
    }

    if (res->status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res->resok.dir_attributes, dir_attr);
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
        memcpy(res->resok.cookieverf,
               &verifier,
               sizeof(res->resok.cookieverf));
    } else {
        /* Initialize the failure post-op attr so a recycled request slot does
         * not leak stale attribute bytes on the error reply. */
        chimera_nfs3_set_post_op_attr(&res->resfail.dir_attributes, dir_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, NULL, res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");


    nfs3_compound_free(ctx);
    nfs_request_free(req->thread, req);

} /* chimera_nfs3_readdirplus_complete */

void
chimera_nfs3_readdirplus(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIRPLUS3args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread   *thread = private_data;
    struct nfs_request                 *req;
    struct READDIRPLUS3res             *res;
    struct nfs_nfs3_readdirplus_cursor *cursor;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_readdirplus(req, args);
    nfs3_trace_readdirplus(req, args);

    req->args_readdirplus = args;

    res = &req->res_readdirplus;

    res->resok.reply.entries = NULL;

    cursor = &req->readdirplus3_cursor;

    cursor->count    = 256; /* reserve some space for non-entry serialization */
    cursor->dircount = 0;
    cursor->entries  = NULL;
    cursor->last     = NULL;

    res->status = chimera_nfs3_decode_fh(req, args->dir.data.data, args->dir.data.len);
    if (res->status != NFS3_OK) {
        int      rc;
        nfsstat3 fh_status = res->status;
        memset(res, 0, sizeof(*res));
        res->status = fh_status;
        rc          = thread->shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, NULL, res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx = nfs3_compound_alloc(req,
                                                           CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                                           CHIMERA_VFS_OPEN_DIRECTORY);
    struct chimera_vfs_compound *compound = ctx->compound;
    uint64_t                     verifier;
    memcpy(&verifier, args->cookieverf, sizeof(verifier));
    ctx->arena_mark = req->encoding->dbuf->used;
    ctx->result     = chimera_vfs_compound_add_readdir_stream(compound, args->cookie, verifier,
                                                              CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                                                              chimera_nfs3_readdirplus_reset,
                                                              chimera_nfs3_readdirplus_callback, ctx);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH, 0,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_op_args(compound, ctx->result)->readdir_flags = CHIMERA_VFS_READDIR_EMIT_DOT;
    chimera_vfs_compound_submit(compound, chimera_nfs3_readdirplus_complete, ctx);
} /* chimera_nfs3_readdirplus */
