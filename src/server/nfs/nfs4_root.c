// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "nfs4_root_cookie.h"
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

    /* The FSID and statfs fills below are gated on va_req_mask, which the
     * memset just cleared, so carry the caller's resolved mask into the attrs
     * -- without it neither block ever runs and the pseudo-root answers a
     * GETATTR for fsid (a REQUIRED attribute, RFC 7530 §5.6) or for any statfs
     * attribute with the bit dropped from the returned bitmap. */
    attr->va_req_mask = attr_mask;
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

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FSID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FSID;
        attr->va_fsid      = 0;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) {
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
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            res->status = NFS4ERR_SERVERFAULT;
            status      = NFS4ERR_SERVERFAULT;
        } else {
            memcpy(req->fh, attr->va_fh, attr->va_fh_len);
            req->fhlen = attr->va_fh_len;
        }
    }

    chimera_nfs4_compound_complete(req, status);
} /* nfs4_root_lookup_complete */

SYMBOL_EXPORT void
nfs4_root_lookup_export(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req,
    const struct chimera_nfs_export  *export,
    const char                       *full_path)
{
    struct LOOKUP4res *res = &req->res_compound.resarray[req->index].oplookup;
    uint8_t            root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t           root_fh_len;

    /* Enforce the export's security-flavor policy at the namespace-root
     * boundary: a client traversing into the export under a disallowed flavor
     * gets NFS4ERR_WRONGSEC and renegotiates via SECINFO. */
    if (!chimera_nfs_export_sec_ok(export, req->sec_bit)) {
        res->status = NFS4ERR_WRONGSEC;
        chimera_nfs4_compound_complete(req, NFS4ERR_WRONGSEC);
        return;
    }

    /* Entering an export: adopt its id (so the handle minted for the client
     * carries it) and apply its squash policy. */
    chimera_nfs_set_export(req, export);

    while (full_path[0] == '/') {
        full_path++;
    }

    if (full_path[0] == '\0') {
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
} /* nfs4_root_lookup_export */

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

    /**
     * We are doing a lookup on the export path. The path
     * can contain multiple components, so we need to use
     * the chimera_vfs_lookup() logic from the root file handle
     * to find the mount point file handle.
     */

    const struct chimera_nfs_export *export = NULL;

    rc = chimera_nfs_find_export_path(shared, args->objname.data, args->objname.len, &full_path, &export);
    if (rc) {
        // Export not found, return error
        chimera_nfs_error("lookup for unknown export '%.*s'",
                          args->objname.len, (const char *) args->objname.data);
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOENT);
        return;
    }

    nfs4_root_lookup_export(nfs_thread, req, export, full_path);
    free(full_path);
} /* nfs4_root_lookup */

/*
 * "/" (root) export FH resolution.
 *
 * When a "/" export exists the NFSv4 namespace root is that export's real
 * backend directory.  Its FH is needed in two places: PUTROOTFH installs it
 * as the current FH, and LOOKUP/LOOKUPP/SECINFO must recognize "the current
 * FH is the namespace root" to graft sibling exports over it as junctions.
 * The resolved FH is cached in shared state (root_export_fh, guarded by
 * exports_lock) keyed by the export's id, so the recognizers are a memcmp in
 * the steady state; the cache is primed by whichever caller needs it first
 * and is invalidated by removal of the "/" export (id mismatch covers
 * re-addition, which assigns a fresh id).
 *
 * Resolution runs with the requesting client's (squashed) credential, the
 * same choice the v3 MOUNT path makes, and follows symlinks in the export
 * path as v3 MOUNT does.
 */

struct nfs4_root_export_fh_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_request               *req;
    nfs4_root_export_fh_callback_t    callback;
    uint16_t                          export_id;
};

static void
nfs4_root_export_fh_resolve_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs4_root_export_fh_ctx   *ctx    = private_data;
    struct chimera_server_nfs_shared *shared = ctx->thread->shared;

    if (error_code == CHIMERA_VFS_OK &&
        (!attr || !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH))) {
        error_code = CHIMERA_VFS_EIO;
    }

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, NULL, 0, ctx->thread, ctx->req);
        free(ctx);
        return;
    }

    pthread_mutex_lock(&shared->exports_lock);
    /* Prime the cache unless the "/" export changed while the resolve was in
     * flight; a stale prime would mis-recognize the old root. */
    if (shared->root_export_id == ctx->export_id) {
        memcpy(shared->root_export_fh, attr->va_fh, attr->va_fh_len);
        shared->root_export_fh_len = attr->va_fh_len;
        shared->root_export_fh_id  = ctx->export_id;
    }
    pthread_mutex_unlock(&shared->exports_lock);

    ctx->callback(CHIMERA_VFS_OK, attr->va_fh, attr->va_fh_len,
                  ctx->thread, ctx->req);
    free(ctx);
} /* nfs4_root_export_fh_resolve_complete */

SYMBOL_EXPORT void
nfs4_root_export_fh_resolve(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs4_root_export_fh_callback_t    callback)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct chimera_nfs_export        *cur, root_export;
    struct nfs4_root_export_fh_ctx   *ctx;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          fh_len;
    uint16_t                          root_id;
    int                               found = 0;
    const char                       *path;

    pthread_mutex_lock(&shared->exports_lock);
    root_id = shared->root_export_id;

    if (root_id != 0) {
        LL_FOREACH(shared->exports, cur)
        {
            if (cur->id == root_id) {
                root_export = *cur;
                found       = 1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&shared->exports_lock);

    if (!found) {
        callback(CHIMERA_VFS_ENOENT, NULL, 0, thread, req);
        return;
    }

    path = root_export.path;
    while (path[0] == '/') {
        path++;
    }

    chimera_vfs_get_root_fh(fh, &fh_len);

    if (path[0] == '\0') {
        /* The "/" export's path is the VFS root itself; nothing to resolve. */
        pthread_mutex_lock(&shared->exports_lock);
        if (shared->root_export_id == root_id) {
            memcpy(shared->root_export_fh, fh, fh_len);
            shared->root_export_fh_len = fh_len;
            shared->root_export_fh_id  = root_id;
        }
        pthread_mutex_unlock(&shared->exports_lock);
        callback(CHIMERA_VFS_OK, fh, fh_len, thread, req);
        return;
    }

    ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate root export fh context");

    ctx->thread    = thread;
    ctx->req       = req;
    ctx->callback  = callback;
    ctx->export_id = root_id;

    chimera_vfs_lookup(thread->vfs_thread,
                       &req->cred,
                       fh,
                       fh_len,
                       path,
                       strlen(path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       nfs4_root_export_fh_resolve_complete,
                       ctx);
} /* nfs4_root_export_fh_resolve */

SYMBOL_EXPORT void
nfs4_root_export_fh_get(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs4_root_export_fh_callback_t    callback)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          fh_len = 0;
    uint16_t                          root_id;

    pthread_mutex_lock(&shared->exports_lock);
    root_id = shared->root_export_id;

    if (root_id != 0 && shared->root_export_fh_id == root_id) {
        memcpy(fh, shared->root_export_fh, shared->root_export_fh_len);
        fh_len = shared->root_export_fh_len;
    }
    pthread_mutex_unlock(&shared->exports_lock);

    if (root_id == 0) {
        callback(CHIMERA_VFS_ENOENT, NULL, 0, thread, req);
        return;
    }

    if (fh_len) {
        callback(CHIMERA_VFS_OK, fh, fh_len, thread, req);
        return;
    }

    nfs4_root_export_fh_resolve(thread, req, callback);
} /* nfs4_root_export_fh_get */

static void
nfs4_root_junction_check_fh_ready(
    enum chimera_vfs_error            error_code,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    nfs4_root_junction_resume_t resume = req->root_junction_resume;
    int                         at_root;

    req->root_junction_resume = NULL;

    /* A root FH that cannot be resolved just means the junction view is
     * unavailable; the op proceeds as an ordinary VFS operation. */
    at_root = (error_code == CHIMERA_VFS_OK &&
               fh_len == (uint32_t) req->fhlen &&
               memcmp(fh, req->fh, fh_len) == 0);

    resume(thread, req, at_root);
} /* nfs4_root_junction_check_fh_ready */

SYMBOL_EXPORT void
nfs4_root_junction_check(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs4_root_junction_resume_t       resume)
{
    /* Lockless gate, same publication rules as exports_by_id: the current FH
     * can only be the namespace root if it was minted under the "/" export. */
    uint16_t root_id = thread->shared->root_export_id;

    if (root_id == 0 || req->export_id != root_id) {
        resume(thread, req, 0);
        return;
    }

    req->root_junction_resume = resume;
    nfs4_root_export_fh_get(thread, req, nfs4_root_junction_check_fh_ready);
} /* nfs4_root_junction_check */

/*
 * Pseudo-fs root READDIR.
 *
 * Each entry's attributes require resolving the export's backing path via
 * chimera_vfs_lookup(), which completes asynchronously, so the exports are
 * walked one at a time by a state machine: issue the lookup for the current
 * export, marshal its attrs in the completion callback, then advance to the
 * next export, and complete the compound only after the walk finishes.
 *
 * The export list is snapshotted up front (under exports_lock, inside
 * chimera_nfs_iterate_exports) because exports may be removed via the REST
 * API while lookups are in flight; the walk must not retain pointers into
 * the live list.
 *
 * The pseudo-root has its own cookie space: entry cookies are the export's
 * snapshot position biased by +3 to stay clear of the reserved cookie values
 * 0-2 (RFC 7530 §16.24.4), and a client cookie resumes the walk at the
 * position after the entry that carried it.  The cookie is validated against
 * the snapshot (nfs4_root_cookie.h) before the walk starts; a reserved or
 * out-of-range cookie gets NFS4ERR_BAD_COOKIE rather than truncating into the
 * int resume position.
 *
 * Cookies are positional and the cookieverf is always zero, so a client
 * mid-walk across a list mutation is not protected.  chimera_nfs_add_export
 * prepends, so an addition shifts every position by one and the client
 * re-reads an entry it already saw; a removal shifts the other way and an
 * export the client has not reached yet is skipped.  Only a cookie left past
 * the end of the shrunken snapshot is caught, as NFS4ERR_BAD_COOKIE.  Closing
 * that needs an export-list generation counter returned as the cookieverf,
 * which is what RFC 7530 §16.24.4 provides it for.
 */

struct nfs4_root_readdir_export {
    char    *name;  /* leading '/' stripped, validated single component */
    char    *path;
    uint16_t id;
};

struct nfs4_root_readdir_state {
    struct nfs_request              *req;
    struct chimera_vfs_thread       *vfs_thread;
    uint8_t                          root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         root_fh_len;
    uint64_t                         attrmask;
    struct nfs4_root_readdir_export *exports;
    int                              num_exports;
    int                              pos;         /* current snapshot position */
    int                              first_pos;   /* resume position from args->cookie */
    struct entry4                   *entry;       /* entry awaiting its lookup */
    uint32_t                         dbuf_before; /* dbuf watermark for rollback */
    enum chimera_vfs_error error_code;
    int                              in_advance;  /* advance() loop is on the stack */
    int                              lookup_done; /* current lookup completed synchronously */
};

struct nfs4_root_readdir_snap {
    struct nfs4_root_readdir_export *exports;
    int                              count;
    int                              capacity;
};

static int
nfs4_root_readdir_snap_cb(
    const struct chimera_nfs_export *export,
    void                            *private_data)
{
    struct nfs4_root_readdir_snap   *snap = private_data;
    struct nfs4_root_readdir_export *e;
    const char                      *export_name = export->name;

    /* The "/" export is the namespace root itself, not an entry within it,
     * so it is never listed.  (Only reachable mid-transition: while a "/"
     * export exists PUTROOTFH serves its real backend root and this walk
     * does not run.) */
    if (strcmp(export_name, "/") == 0) {
        return 0;
    }

    // remove leading '/' from export name if present
    while (export_name[0] == '/') {
        export_name++;
    }

    /* The pseudo-root lists each export as a single directory entry, so the
     * name must be exactly one non-empty path component */
    if (export_name[0] == '\0' || strchr(export_name, '/')) {
        chimera_nfs_error("Invalid export name %s for export path %s: "
                          "export name must be a single non-empty path component",
                          export->name, export->path);
        return 0;
    }

    if (snap->count == snap->capacity) {
        snap->capacity = snap->capacity ? snap->capacity * 2 : 8;
        snap->exports  = realloc(snap->exports,
                                 snap->capacity * sizeof(*snap->exports));
        chimera_nfs_abort_if(snap->exports == NULL, "Failed to allocate export snapshot");
    }

    e       = &snap->exports[snap->count++];
    e->name = strdup(export_name);
    e->path = strdup(export->path);
    e->id   = export->id;
    chimera_nfs_abort_if(e->name == NULL || e->path == NULL, "Failed to allocate export snapshot");

    return 0;
} /* nfs4_root_readdir_snap_cb */

static void
nfs4_root_readdir_exports_free(
    struct nfs4_root_readdir_export *exports,
    int                              count)
{
    int i;

    for (i = 0; i < count; i++) {
        free(exports[i].name);
        free(exports[i].path);
    }

    free(exports);
} /* nfs4_root_readdir_exports_free */

static void
nfs4_root_readdir_finish(struct nfs4_root_readdir_state *state)
{
    struct nfs_request             *req    = state->req;
    struct READDIR4res             *res    = &req->res_compound.resarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor = &req->readdir4_cursor;
    int                             eof    = 1;

    if (state->error_code == CHIMERA_VFS_EOVERFLOW) {
        /* Overflow means there are more entries to be read */
        eof = 0;
        if (cursor->entries == NULL) {
            /* RFC 7530 §16.24.4: not even one entry fit in maxcount and we
             * are not at end-of-directory.  Returning an empty, non-eof page
             * would stall a paging client. */
            res->status = NFS4ERR_TOOSMALL;
        }
    } else if (state->error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error("Error iterating exports for readdir: %d", state->error_code);
        res->status = chimera_nfs4_errno_to_nfsstat4(state->error_code);
    }

    /* The pseudo-root export list has no change verifier; cookies are
     * positional and best-effort across list mutations. */
    memset(res->resok4.cookieverf, 0, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = eof;
    res->resok4.reply.entries = res->status == NFS4_OK ? cursor->entries : NULL;

    nfs4_root_readdir_exports_free(state->exports, state->num_exports);

    chimera_nfs4_compound_complete(req, res->status);

    free(state);
} /* nfs4_root_readdir_finish */

static void nfs4_root_readdir_advance(
    struct nfs4_root_readdir_state *state);

static void
nfs4_root_readdir_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attrs,
    void                     *private_data)
{
    struct nfs4_root_readdir_state *state  = private_data;
    struct nfs_request             *req    = state->req;
    struct READDIR4args            *args   = &req->args_compound->argarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor *cursor = &req->readdir4_cursor;
    struct entry4                  *entry  = state->entry;
    uint32_t                        dbuf_cur;

    state->lookup_done = 1;

    if (error_code != CHIMERA_VFS_OK) {
        /* An export root that fails to resolve (deleted directory, config
         * typo, momentarily unavailable backend) is a routine condition, not
         * a server fault.  Surface it as an NFS4ERR_* READDIR status instead
         * of aborting the whole process (RFC 7530 §16.24 / RFC 8881 §18.23:
         * report errors as status). */
        req->encoding->dbuf->used = state->dbuf_before;
        state->error_code         = error_code;
    } else {
        chimera_nfs4_marshall_attrs(attrs,
                                    args->num_attr_request,
                                    args->attr_request,
                                    &entry->attrs.num_attrmask,
                                    entry->attrs.attrmask,
                                    3,
                                    entry->attrs.attr_vals.data,
                                    &entry->attrs.attr_vals.len,
                                    256,
                                    0,
                                    0, /* pNFS not advertised on the pseudo-fs root */
                                    0, /* pseudo-fs root has no xattr-capable backend */
                                    0,
                                    req->thread->shared->nfs_lease_time_s,
                                    state->exports[state->pos].id,
                                    req->thread->shared->fh_key,
                                    req->thread->shared->fh_sign);

        dbuf_cur = req->encoding->dbuf->used - state->dbuf_before;

        if (cursor->count + dbuf_cur > args->maxcount ||
            req->encoding->dbuf->used + 8192 > (uint32_t) req->encoding->dbuf->size) {
            req->encoding->dbuf->used = state->dbuf_before;
            state->error_code         = CHIMERA_VFS_EOVERFLOW;
        } else {
            cursor->count += dbuf_cur;

            if (cursor->entries) {
                cursor->last->nextentry = entry;
                cursor->last            = entry;
            } else {
                cursor->entries = entry;
                cursor->last    = entry;
            }
            state->pos++;
        }
    }

    /* If the lookup completed synchronously the advance() loop is still on
     * the stack and continues the walk itself; re-entering it here would
     * recurse once per export. */
    if (!state->in_advance) {
        nfs4_root_readdir_advance(state);
    }
} /* nfs4_root_readdir_lookup_callback */

static void
nfs4_root_readdir_advance(struct nfs4_root_readdir_state *state)
{
    struct nfs_request              *req = state->req;
    struct nfs4_root_readdir_export *export;
    struct entry4                   *entry;
    int                              rc;

    state->in_advance = 1;

    while (state->error_code == CHIMERA_VFS_OK && state->pos < state->num_exports) {

        if (state->pos < state->first_pos) {
            state->pos++;
            continue;
        }

        export             = &state->exports[state->pos];
        state->dbuf_before = req->encoding->dbuf->used;

        /* allocate a new entry and populate it with the export name */
        entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
        if (!entry) {
            state->error_code = CHIMERA_VFS_EOVERFLOW;
            break;
        }

        rc = xdr_dbuf_opaque_copy(&entry->name, export->name, strlen(export->name), req->encoding->dbuf);
        if (rc) {
            req->encoding->dbuf->used = state->dbuf_before;
            state->error_code         = CHIMERA_VFS_EOVERFLOW;
            break;
        }

        /* Resuming with this cookie skips every export up to and including
         * this one; the bias keeps clear of reserved cookies 0-2.  Paired with
         * nfs4_root_readdir_cookie_first_pos, which inverts it. */
        entry->cookie    = nfs4_root_readdir_pos_cookie(state->pos);
        entry->nextentry = NULL;

        rc = xdr_dbuf_alloc_array(&entry->attrs, attrmask, 3, req->encoding->dbuf);
        if (rc) {
            req->encoding->dbuf->used = state->dbuf_before;
            state->error_code         = CHIMERA_VFS_EOVERFLOW;
            break;
        }

        /* Per-entry attribute buffer.  test_pseudo_root sizes its export list
         * off this to force a multi-page listing; shrinking it means raising
         * that test's PSEUDO_ROOT_MAX_PAGE_ENTRIES too, or its page-boundary
         * coverage lapses. */
        rc = xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                                   256,
                                   req->encoding->dbuf);
        if (rc) {
            req->encoding->dbuf->used = state->dbuf_before;
            state->error_code         = CHIMERA_VFS_EOVERFLOW;
            break;
        }

        state->entry       = entry;
        state->lookup_done = 0;

        chimera_vfs_lookup(state->vfs_thread,
                           &req->cred,
                           state->root_fh,
                           state->root_fh_len,
                           export->path,
                           strlen(export->path),
                           CHIMERA_VFS_ATTR_FH,
                           state->attrmask,
                           nfs4_root_readdir_lookup_callback,
                           state);

        if (!state->lookup_done) {
            /* Lookup is in flight; its callback resumes the walk. */
            state->in_advance = 0;
            return;
        }
    }

    state->in_advance = 0;

    nfs4_root_readdir_finish(state);
} /* nfs4_root_readdir_advance */

SYMBOL_EXPORT void
nfs4_root_readdir(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req)
{
    struct READDIR4args              *args   = &req->args_compound->argarray[req->index].opreaddir;
    struct chimera_server_nfs_shared *shared = nfs_thread->shared;
    struct READDIR4res               *res    = &req->res_compound.resarray[req->index].opreaddir;
    struct nfs_nfs4_readdir_cursor   *cursor;
    struct nfs4_root_readdir_state   *state;
    struct nfs4_root_readdir_snap     snap      = { NULL, 0, 0 };
    int                               first_pos = 0;

    chimera_nfs_iterate_exports(shared, nfs4_root_readdir_snap_cb, &snap);

    /* The valid cookie range depends on the export count, so the cookie is
     * validated against the snapshot -- before any walk state is allocated. */
    res->status = nfs4_root_readdir_cookie_first_pos(args->cookie, snap.count,
                                                     &first_pos);

    if (res->status != NFS4_OK) {
        nfs4_root_readdir_exports_free(snap.exports, snap.count);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    cursor                    = &req->readdir4_cursor;
    res->resok4.reply.entries = NULL;

    cursor->count   = 256;
    cursor->entries = NULL;
    cursor->last    = NULL;

    state = calloc(1, sizeof(*state));
    chimera_nfs_abort_if(state == NULL, "Failed to allocate readdir state");

    state->req         = req;
    state->vfs_thread  = nfs_thread->vfs_thread;
    state->exports     = snap.exports;
    state->num_exports = snap.count;
    state->first_pos   = first_pos;
    state->error_code  = CHIMERA_VFS_OK;
    state->attrmask    = chimera_nfs4_attr2mask(args->attr_request,
                                                args->num_attr_request);
    chimera_vfs_get_root_fh(state->root_fh, &state->root_fh_len);

    nfs4_root_readdir_advance(state);
} /* nfs4_root_readdir */
