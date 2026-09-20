// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"

/*
 * Marshal one directory entry into the reply and append it to `cursor`,
 * enforcing the READDIR's maxcount and the reply buffer's floor.  Returns 0 if
 * the entry was taken and -1 if it did not fit -- the signal that stops the
 * enumeration (and, for the VFS-compound path, the point at which the page
 * truncates).
 *
 * `dir_fh` is the directory being read: its backend decides the layout type and
 * xattr support reported for every entry.  The per-op path passes req->fh; the
 * VFS-compound path passes the handle the sequence had current when the READDIR
 * ran, which req->fh no longer holds by the time results are filled.
 */
int
chimera_nfs4_readdir_entry_fill(
    struct nfs_request             *req,
    struct READDIR4args            *args,
    struct nfs_nfs4_readdir_cursor *cursor,
    const uint8_t                  *dir_fh,
    int                             dir_fhlen,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs)
{
    uint32_t       dbuf_cur;
    uint32_t       dbuf_before = req->encoding->dbuf->used;
    struct entry4 *entry;
    int            rc;

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
                                                              dir_fh, dir_fhlen),
                                chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                             dir_fh, dir_fhlen),
                                chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s,
                                req->export_id,
                                req->thread->shared->fh_key,
                                req->thread->shared->fh_sign,
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
} /* chimera_nfs4_readdir_entry_fill */

static int
chimera_nfs4_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request  *req  = arg;
    struct READDIR4args *args = &req->args_compound->argarray[req->index].opreaddir;

    (void) inum;

    return chimera_nfs4_readdir_entry_fill(req, args, &req->readdir4_cursor,
                                           req->fh, req->fhlen,
                                           cookie, name, namelen, attrs);
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
                        NULL, 0, /* no search-pattern filter */
                        chimera_nfs4_readdir_callback,
                        chimera_nfs4_readdir_complete,
                        req);
} /* chimera_nfs4_readdir_open_callback */

/* ---- Named-attribute directory READDIR ----------------------------------
 *
 * The synthetic attr directory has no backend directory to enumerate; its
 * entries are the base file's named streams.  Stat the base once (the entries
 * inherit its owner/timestamps), list the streams (with their file handles),
 * and synthesize one entry per named stream.  memfs returns the full stream
 * list in one shot, so a single pass with index-based cookies covers
 * continuation.
 *
 * All of it is one run: PUTFH the base, PATH-open it, GETATTR, LIST_STREAMS.
 * The page the last op stages belongs to the compound, so the synthesis reads
 * it before the sequence is freed -- as does the stat, whose ACL (when one was
 * asked for) the op owns for exactly that long.
 */
#define NFS4_ATTRDIR_READDIR_OP_GETATTR 2
#define NFS4_ATTRDIR_READDIR_OP_LIST    3

/* The stream page the LIST_STREAMS op stages.  Sized as the per-op buffer was,
 * and the same bound the client's maxcount is judged against afterwards. */
#define NFS4_ATTRDIR_READDIR_PAGE       (64 * 1024)

static void
chimera_nfs4_readdir_attrdir_finish(
    struct nfs_request *req,
    nfsstat4            status,
    uint64_t            cookie,
    uint32_t            eof)
{
    struct READDIR4res             *res =
        &req->res_compound.resarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor = &req->readdir4_cursor;
    uint64_t                        cv;

    /* RFC 7530 §16.24.4: if not even one entry fit in maxcount and we are not
     * at end-of-directory, the buffer is too small. */
    if (status == NFS4_OK && !eof && cursor->entries == NULL) {
        status = NFS4ERR_TOOSMALL;
    }

    res->status = status;

    if (status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    cv = cookie;
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = eof;
    res->resok4.reply.entries = cursor->entries;

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_readdir_attrdir_finish */

static void
chimera_nfs4_readdir_attrdir_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *gop, *lop;
    const uint8_t                        *records;
    uint32_t                              records_len, in = 0, index = 0;
    uint32_t                              r_eof;
    uint64_t                              cookie;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        chimera_nfs4_readdir_attrdir_finish(
            req, chimera_nfs4_errno_to_nfsstat4(error_code), 0, 0);
        return;
    }

    gop = chimera_vfs_compound_op(compound,
                                  NFS4_ATTRDIR_READDIR_OP_GETATTR);
    lop = chimera_vfs_compound_op(compound,
                                  NFS4_ATTRDIR_READDIR_OP_LIST);
    records     = lop->buffer;
    records_len = lop->buffer_len;
    r_eof       = lop->eof;
    cookie      = lop->r_cookie;

    while (in < records_len) {
        struct chimera_vfs_stream_entry entry;
        const char                     *name;
        const uint8_t                  *fh;
        struct chimera_vfs_attrs        attr;
        uint32_t                        reclen;

        memcpy(&entry, records + in, sizeof(entry));
        name = (const char *) records + in + sizeof(entry);
        fh   = records + in + sizeof(entry) + entry.name_len;

        reclen = (sizeof(entry) + entry.name_len + entry.fh_len + 7) & ~7u;

        /* Skip the unnamed default fork ("::$DATA") -- it is the file's own
         * data, not a named attribute. */
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

        attr               = gop->attr;
        attr.va_size       = entry.size;
        attr.va_space_used = entry.alloc;
        attr.va_set_mask  |= CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_SPACE_USED;

        if (entry.fh_len) {
            memcpy(attr.va_fh, fh, entry.fh_len);
            attr.va_fh_len    = entry.fh_len;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
            /* Give each named attribute a distinct, stable fileid derived from
             * its handle (the base inode's ino would collide across
             * streams). */
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

    chimera_vfs_compound_free(compound);

    chimera_nfs4_readdir_attrdir_finish(req, NFS4_OK, index, r_eof);
} /* chimera_nfs4_readdir_attrdir_complete */

static void
chimera_nfs4_readdir_attrdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct READDIR4args         *args =
        &req->args_compound->argarray[req->index].opreaddir;
    struct chimera_vfs_compound *compound;
    const uint8_t               *base;
    int                          base_len;

    req->handle = NULL;

    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, base, base_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    /* Stat the base once; every named attribute inherits its
     * owner/group/timestamps (size/fh/fileid are overridden per stream). */
    chimera_vfs_compound_add_getattr(compound,
                                     chimera_nfs4_attr2mask(
                                         args->attr_request,
                                         args->num_attr_request));
    chimera_vfs_compound_add_list_streams(compound, 0,
                                          NFS4_ATTRDIR_READDIR_PAGE,
                                          1 /* want per-stream file handles */);

    chimera_vfs_compound_submit(compound, chimera_nfs4_readdir_attrdir_complete,
                                req);
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
     * own export-position cookie space (also >= 3) and applies the same check
     * in nfs4_root_readdir. */
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
