// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "nfs4_root_cookie.h"
#include "vfs/vfs_compound.h"
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

/* Every export path is resolved the same way: PUTROOT seats the VFS root and
 * LOOKUP_PATH walks the export's (possibly multi-component) path from it.  The
 * walk is one op rather than a chain because the path IS one call -- the VFS
 * has always had the multi-component form, and the sequence is how a caller
 * reaches it without holding the root handle itself. */

SYMBOL_EXPORT void
nfs4_root_lookup_export(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req,
    const struct chimera_nfs_export  *export,
    const char                       *full_path)
{
    struct LOOKUP4res        *res = &req->res_compound.resarray[req->index].oplookup;
    struct chimera_nfs_export snapshot;

    /* Name lookup may have returned a live record. Keep a locked copy of its
     * policy across asynchronous work and reject a replaced export. */
    if (chimera_nfs_get_export_copy(nfs_thread->shared, export->name, &snapshot) != 0 ||
        snapshot.id != export->id || strcmp(snapshot.path, export->path)) {
        res->status = NFS4ERR_DELAY;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }
    export = &snapshot;

    /* Enforce the export's security-flavor policy at the namespace-root
     * boundary: a client traversing into the export under a disallowed flavor
     * gets NFS4ERR_WRONGSEC and renegotiates via SECINFO. */
    if (!chimera_nfs_export_sec_ok(export, req->sec_bit)) {
        res->status = NFS4ERR_WRONGSEC;
        chimera_nfs4_compound_complete(req, NFS4ERR_WRONGSEC);
        return;
    }

    while (full_path[0] == '/') {
        full_path++;
    }

    if (full_path[0] == '\0') {
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOENT);
        return;
    }

    chimera_nfs4_compound_export(nfs_thread, req, export, full_path);
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
     * We are doing a lookup on the export path. The path can contain
     * multiple components, so it is resolved as a LOOKUP_PATH from the
     * VFS root a PUTROOT seats -- see nfs4_root_lookup_export.
     */

    const struct chimera_nfs_export  *export = NULL;

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

/* The namespace root cache is published only after the shared encoder accepts
 * resolution against an unchanged export snapshot. Removing an export clears
 * the cache; an in-flight resolution also checks path and policy, since callers
 * may explicitly reuse the removed export's id. */
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

    evpl_mutex_lock(&shared->exports_lock);
    root_id = shared->root_export_id;

    if (root_id != 0 && shared->root_export_fh_id == root_id) {
        memcpy(fh, shared->root_export_fh, shared->root_export_fh_len);
        fh_len = shared->root_export_fh_len;
    }
    evpl_mutex_unlock(&shared->exports_lock);

    if (root_id == 0) {
        callback(CHIMERA_VFS_ENOENT, NULL, 0, thread, req);
        return;
    }

    if (fh_len) {
        callback(CHIMERA_VFS_OK, fh, fh_len, thread, req);
        return;
    }

    chimera_nfs4_compound_root_resolve(thread, req, callback);
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

    if (error_code == CHIMERA_VFS_EAGAIN) {
        nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                                  &req->res_compound.resarray[req->index], NFS4ERR_DELAY);
        chimera_nfs4_compound_complete(req, NFS4ERR_DELAY);
        return;
    }

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
 * Each entry's attributes require resolving the export's backing path, a
 * PUTROOT and a LOOKUP_PATH. All entries fitting one response page contribute
 * their operations to the shared compound. Attribute callbacks stage private
 * reply bytes, and accepted completion publishes the page. Retry runs every
 * lookup and overwrites every attribute result before a page can be accepted.
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
    char                           *name; /* leading '/' stripped, validated single component */
    char                           *path;
    uint16_t                        id;
    uint64_t                        cookie;
    struct nfs4_root_readdir_state *state;
};

struct nfs4_root_readdir_state {
    struct nfs_request              *req;
    struct READDIR4args             *args;
    nfsstat4                        *verify_status;
    struct nfs4_root_readdir_export *exports;
    int                              num_exports;
    struct nfs_nfs4_readdir_cursor   cursor;
    bool                             eof, planned_eof, page_full;
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

void
nfs4_root_readdir_free(struct nfs4_root_readdir_state *state)
{
    if (!state) {
        return;
    }
    for (int i = 0; i < state->num_exports; i++) {
        free(state->exports[i].name);
        free(state->exports[i].path);
    }
    free(state->exports);
    free(state);
} /* nfs4_root_readdir_free */

void
nfs4_root_readdir_reset(
    struct nfs4_root_readdir_state *state,
    uint64_t                        reply_reserve)
{
    if (!state) {
        return;
    }
    memset(&state->cursor, 0, sizeof(state->cursor));
    state->cursor.count         = 16;
    state->cursor.reply_reserve = reply_reserve;
    state->eof                  = state->planned_eof;
    state->page_full            = false;
} /* nfs4_root_readdir_reset */

void
nfs4_root_readdir_fill(
    const struct nfs4_root_readdir_state *state,
    struct READDIR4res                   *res)
{
    /* Positional cookies retain their existing best-effort mutation semantics. */
    memset(res->resok4.cookieverf, 0, sizeof(res->resok4.cookieverf));
    res->resok4.reply.eof     = state->eof;
    res->resok4.reply.entries = state->cursor.entries;
} /* nfs4_root_readdir_fill */

static void
nfs4_root_readdir_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_root_readdir_export  *entry  = private_data;
    struct chimera_server_nfs_shared *shared = entry->state->req->thread->shared;

    /* Refuse an entry whose fixed wire fields cannot fit before resolving
    * its backing export. Attribute bytes are checked after the lookup. */
    uint64_t                          minimum = 24 + ((strlen(entry->name) + 3) & ~(size_t) 3);

    if (entry->state->page_full ||
        entry->state->cursor.count + minimum > entry->state->args->maxcount) {
        entry->state->page_full = true;
        entry->state->eof       = false;
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    /* A rejected finish must not replay a removed or repointed export and
     * mint its old object's handle under a reused export id. */
    evpl_mutex_lock(&shared->exports_lock);
    const struct chimera_nfs_export *current = shared->exports_by_id[entry->id];
    const char                      *name    = current ? current->name : "";
    while (*name == '/') {
        name++;
    }
    bool                             valid = current && !strcmp(name, entry->name) && !strcmp(current->path, entry->
                                                                                              path);
    evpl_mutex_unlock(&shared->exports_lock);
    if (!valid) {
        *entry->state->verify_status = NFS4ERR_DELAY;
        *status                      = CHIMERA_VFS_EAGAIN;
    }
} /* nfs4_root_readdir_prepare */

static void
nfs4_root_readdir_lookup_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_root_readdir_export      *export = private_data;
    struct nfs4_root_readdir_state       *state  = export->state;
    struct nfs_request                   *req    = state->req;
    const struct chimera_vfs_compound_op *vop    = chimera_vfs_compound_op(compound, index);

    if (*status != CHIMERA_VFS_OK || vop->skipped) {
        return;
    }
    struct chimera_vfs_attrs              attrs = vop->attr;
    if (!(attrs.va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(attrs.va_fh, vop->fh, vop->fh_len);
        attrs.va_fh_len    = vop->fh_len;
        attrs.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }
    /* Marshal while backend-owned attributes, including ACLs, are valid.
     * Page limits count actual wire bytes; arena limits retain the enclosing
     * compound's pending fixed results. A retry rebuilds this private page. */
    if (chimera_nfs4_readdir_entry_fill(req, export->id, state->args, &state->cursor,
                                        NULL, 0, export->cookie, export->name, strlen(export->name), &attrs)) {
        state->eof       = false;
        state->page_full = true;
    }
} /* nfs4_root_readdir_lookup_complete */

static void
nfs4_root_readdir_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_root_readdir_state *state = private_data;

    if (chimera_vfs_compound_op(compound, index)->skipped) {
        return;
    }
    if (*status == CHIMERA_VFS_OK && !state->eof && !state->cursor.entries) {
        *state->verify_status = NFS4ERR_TOOSMALL;
        *status               = CHIMERA_VFS_ERANGE;
    }
} /* nfs4_root_readdir_complete */

int
nfs4_root_readdir_add(
    struct chimera_vfs_compound     *compound,
    struct nfs_request              *req,
    struct READDIR4args             *args,
    struct nfs4_root_readdir_state **out,
    nfsstat4                        *input_status,
    nfsstat4                        *verify_status)
{
    struct nfs4_root_readdir_snap   snap  = { NULL, 0, 0 };
    struct nfs4_root_readdir_state *state = calloc(1, sizeof(*state));
    int                             first = 0;
    uint64_t                        mask  = chimera_nfs4_attr2mask(args->attr_request, args->num_attr_request);

    chimera_nfs_abort_if(!state, "Failed to allocate root readdir state");
    *out = state;
    chimera_nfs_iterate_exports(req->thread->shared, nfs4_root_readdir_snap_cb, &snap);
    state->req           = req;
    state->args          = args;
    state->verify_status = verify_status;
    state->exports       = snap.exports;
    state->num_exports   = snap.count;
    state->planned_eof   = true;
    *input_status        = nfs4_root_readdir_cookie_first_pos(args->cookie, snap.count, &first);
    if (*input_status != NFS4_OK) {
        return chimera_vfs_compound_add_checkpoint(compound);
    }

    for (int pos = first; pos < snap.count; pos++) {
        struct nfs4_root_readdir_export *export = &state->exports[pos];
        const char                      *path   = export->path;
        while (*path == '/') {
            path++;
        }
        /* Build descriptors only. The runtime page stops at the first actual
         * entry that does not fit; its suffix helpers then skip themselves. */
        if (chimera_vfs_compound_num_ops(compound) + (*path ? 3 : 4) > CHIMERA_VFS_COMPOUND_MAX_OPS) {
            state->planned_eof = false;
            break;
        }
        export->cookie = nfs4_root_readdir_pos_cookie(pos);
        export->state  = state;
        int putroot = chimera_vfs_compound_add_putroot(compound);
        if (putroot < 0) {
            return -1;
        }
        chimera_vfs_compound_set_op_prepare(compound, putroot, nfs4_root_readdir_prepare, export);
        int index;
        if (*path) {
            index = chimera_vfs_compound_add_lookup_path(compound, path, strlen(path),
                                                         mask | CHIMERA_VFS_ATTR_FH, 0);
        } else {
            /* An empty path has no final component to supply LOOKUP attrs. */
            index = chimera_vfs_compound_add_open(compound, NULL, 0,
                                                  CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                                                  0, NULL, 0, 0, 0);
            if (index < 0) {
                return -1;
            }
            chimera_vfs_compound_set_op_prepare(compound, index, nfs4_root_readdir_prepare, export);
            index = chimera_vfs_compound_add_getattr(compound, mask | CHIMERA_VFS_ATTR_FH);
        }
        if (index < 0) {
            return -1;
        }
        chimera_vfs_compound_set_op_callbacks(compound, index,
                                              nfs4_root_readdir_prepare,
                                              nfs4_root_readdir_lookup_complete, export);
    }
    int checkpoint = chimera_vfs_compound_add_checkpoint(compound);
    if (checkpoint >= 0) {
        chimera_vfs_compound_set_op_callbacks(compound, checkpoint, NULL, nfs4_root_readdir_complete, state);
    }
    return checkpoint;
} /* nfs4_root_readdir_add */
