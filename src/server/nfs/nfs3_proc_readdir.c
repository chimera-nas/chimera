// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data);

static int
chimera_nfs3_readdir_append_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request             *req  = arg;
    struct READDIR3args            *args = req->args_readdir;
    struct entry3                  *entry;
    struct nfs_nfs3_readdir_cursor *cursor;
    uint32_t                        entry_cur;
    int                             rc;

    cursor = &req->readdir3_cursor;

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    chimera_nfs_abort_if(entry == NULL, "Failed to allocate space");

    entry->fileid    = inum;
    entry->cookie    = cookie;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_string(&entry->name, name, namelen, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate string");

    /* RFC 1813 3.3.16: count bounds the XDR-encoded READDIR3resok, so the
     * budget has to be charged in wire bytes.  The bytes this entry consumed in
     * the dbuf are the in-memory struct entry3 (nextentry pointer, xdr_string
     * descriptor, padding) and over-charge by a fixed per-entry delta, which
     * made the server return fewer entries than count allowed.  On the wire an
     * entry3 is value_follows (4) + fileid3 (8) + filename3 (4 + padded len) +
     * cookie3 (8). */
    entry_cur = 24 + ((namelen + 3) & ~3);

    if (cursor->count + entry_cur > args->count) {
        return -1;
    }

    cursor->count += entry_cur;

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    return 0;
} /* chimera_nfs3_readdir_append_entry */

/* The reversibility half of the streaming contract: everything the append above
 * advances is wound back.  The entries themselves are dbuf allocations that a
 * retry simply re-makes -- reset has to undo the CALLER's state, not reclaim
 * memory the request already owns and frees with itself. */
static void
chimera_nfs3_readdir_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct nfs_request             *req    = private_data;
    struct nfs_nfs3_readdir_cursor *cursor = &req->readdir3_cursor;

    cursor->count   = 0;
    cursor->entries = NULL;
    cursor->last    = NULL;
} /* chimera_nfs3_readdir_reset */

static int
chimera_nfs3_readdir_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    return chimera_nfs3_readdir_append_entry(inum, cookie, name, namelen, attrs,
                                           private_data);
} /* chimera_nfs3_readdir_append */

static void
chimera_nfs3_readdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              dir_attr;
    enum chimera_vfs_error                status;
    uint64_t                              verifier;
    uint32_t                              eof;

    status = chimera_vfs_compound_status(compound);

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    /* Both arms carry dir_attributes. */
    dir_attr = op->dir_post_attr;
    verifier = op->r_verifier;
    eof      = op->eof;

    chimera_vfs_compound_free(compound);

    chimera_nfs3_readdir_complete(status, NULL, 0, verifier, eof, &dir_attr, req);
} /* chimera_nfs3_readdir_sequence_complete */

static void
chimera_nfs3_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    struct evpl                      *evpl   = req->thread->evpl;
    struct READDIR3res               *res    = &req->res_readdir;
    struct nfs_nfs3_readdir_cursor   *cursor = &req->readdir3_cursor;
    int                               rc;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    /* RFC 1813 3.3.16: if count was too small to hold even a single entry and
     * we are not at end-of-directory, report TOOSMALL.  An empty, non-eof
     * reply would leave a paging client retrying the same request forever. */
    if (res->status == NFS3_OK && !eof && cursor->entries == NULL) {
        res->status = NFS3ERR_TOOSMALL;
    }

    if (res->status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res->resok.dir_attributes, dir_attr);
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
        memcpy(res->resok.cookieverf, &verifier, sizeof(res->resok.cookieverf));
    } else {
        /* The encoder serializes resfail.dir_attributes on error; initialize it
         * so a recycled request slot does not leak stale attribute bytes. */
        chimera_nfs3_set_post_op_attr(&res->resfail.dir_attributes, dir_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, NULL, res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    /* The open belonged to the sequence and went with it. */

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdir_complete */ /* chimera_nfs3_readdirplus_complete */


void
chimera_nfs3_readdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    uint64_t                          cookieverf;
    struct READDIR3res               *res;
    struct nfs_nfs3_readdir_cursor   *cursor;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_readdir(req, args);
    nfs3_trace_readdir(req, args);

    req->args_readdir = args;

    res    = &req->res_readdir;
    cursor = &req->readdir3_cursor;

    cursor->count   = 256; /* reserve space for non-entry serialization */
    cursor->entries = NULL;
    cursor->last    = NULL;

    res->resok.reply.entries = NULL;

    res->status = chimera_nfs3_decode_fh(req, args->dir.data.data, args->dir.data.len);
    if (res->status != NFS3_OK) {
        int      rc;
        nfsstat3 fh_status = res->status;
        memset(res, 0, sizeof(*res));
        res->status = fh_status;
        rc          = thread->shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, NULL, res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    memcpy(&cookieverf, args->cookieverf, sizeof(cookieverf));

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    /* Streaming, because the entries are marshalled into the request's own dbuf
     * as they arrive and the sequence keeps no copy of them. */
    chimera_vfs_compound_add_readdir_stream(compound,
                                            args->cookie,
                                            cookieverf,
                                            0,
                                            CHIMERA_NFS3_ATTR_MASK,
                                            CHIMERA_VFS_READDIR_EMIT_DOT,
                                            NULL, 0,
                                            chimera_nfs3_readdir_reset,
                                            chimera_nfs3_readdir_append,
                                            req);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_readdir_sequence_complete, req);

} /* chimera_nfs3_readdir */
