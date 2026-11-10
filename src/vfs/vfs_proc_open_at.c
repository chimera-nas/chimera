// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include <string.h>
#include "vfs_procs.h"
#include "vfs_pnfs.h"
#include "vfs_internal.h"
#include "vfs_mount_table.h"
#include "vfs_release.h"
#include "sdk/vfs_access.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_notify.h"
#include "common/macros.h"

/* Whether the engine applies POSIX open semantics (type checks and, for
 * non-exempt credentials, the access gate) to this open at completion --
 * see chimera_vfs_open_at_hdl_callback.  SMB (AUTH_ATTR) evaluates its own
 * access model and legitimately opens directories with write-ish desired
 * access, so it is exempt; so are internal INFERRED opens (NFS3 create and
 * the path-setattr plumbing), which keep their protocol-specific
 * per-operation semantics. */
static inline int
chimera_vfs_open_at_checked(
    const struct chimera_vfs_cred *cred,
    unsigned int                   flags)
{
    return !(flags & CHIMERA_VFS_OPEN_INFERRED) &&
           cred->flavor != CHIMERA_VFS_AUTH_ATTR;
} /* chimera_vfs_open_at_checked */

static void
chimera_vfs_open_at_reply(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_at_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->open_at.set_attr,
             &request->open_at.r_attr,
             &request->open_at.r_dir_pre_attr,
             &request->open_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_at_reply */

static void
chimera_vfs_open_at_truncate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request     *request = private_data;
    struct chimera_vfs_open_handle *handle  = request->io_handle;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(request->thread, handle);
        handle          = NULL;
        request->status = error_code;
    } else {
        uint64_t want = request->open_at.r_attr.va_req_mask;

        request->open_at.r_attr             = *post_attr;
        request->open_at.r_attr.va_req_mask = want;
        memcpy(request->open_at.r_attr.va_fh, handle->fh, handle->fh_len);
        request->open_at.r_attr.va_fh_len    = handle->fh_len;
        request->open_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }
    chimera_vfs_open_at_reply(request, handle);
} /* chimera_vfs_open_at_truncate_complete */

static void
chimera_vfs_open_at_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_thread     *thread = request->thread;
    struct chimera_vfs_name_cache *cache  = thread->vfs->vfs_name_cache;

    if (request->status == CHIMERA_VFS_OK) {
        /* A create is a directory content change: raise it for change
         * watchers and directory-lease holders exactly as mkdir_at does,
         * whatever protocol it arrived by.  The SMB create path opts out
         * (CHIMERA_VFS_OPEN_NO_NOTIFY): it emits its own richer event with
         * disposition policy and directory-lease key sparing. */
        if (request->open_at.r_created &&
            !(request->open_at.flags & CHIMERA_VFS_OPEN_NO_NOTIFY)) {
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->open_at.handle->fh,
                                    request->open_at.handle->fh_len,
                                    CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                    request->open_at.name,
                                    request->open_at.namelen,
                                    NULL, 0);
        }

        /* A path-only open returns an opaque per-open token, not a stable child
         * fh; caching name->token would hand out a dead token, so do not insert
         * one.  But a create still MUST invalidate any stale entry for the name
         * -- above all the negative (tombstone) entry a prior unlink left, which
         * would otherwise make the just-created object look absent until the
         * tombstone ages out (path-only lookup_at repopulates a correct stable
         * fh on the next miss). */
        if (!chimera_vfs_module_is_path_only(request->module)) {
            chimera_vfs_name_cache_insert(thread, cache,
                                          request->open_at.handle->fh_hash,
                                          request->open_at.handle->fh,
                                          request->open_at.handle->fh_len,
                                          request->open_at.name_hash,
                                          request->open_at.name,
                                          request->open_at.namelen,
                                          request->open_at.r_attr.va_fh,
                                          request->open_at.r_attr.va_fh_len);
        } else if (request->open_at.r_created) {
            chimera_vfs_name_cache_remove(cache,
                                          request->open_at.handle->fh_hash,
                                          request->open_at.handle->fh,
                                          request->open_at.handle->fh_len,
                                          request->open_at.name_hash,
                                          request->open_at.name,
                                          request->open_at.namelen);
        }

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      request->open_at.handle->fh_hash,
                                      request->open_at.handle->fh,
                                      request->open_at.handle->fh_len,
                                      &request->open_at.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      chimera_vfs_hash(request->open_at.r_attr.va_fh, request->open_at.r_attr.
                                                       va_fh_len),
                                      request->open_at.r_attr.va_fh,
                                      request->open_at.r_attr.va_fh_len,
                                      &request->open_at.r_attr);
    }

    if (handle) {
        handle->r_created = request->open_at.r_created;
    }

    /* Fail closed on an under-filled reply.  The semantics block below reads
     * mode, uid, gid and (on an ACL-native backend) the ACL from r_attr; a
     * backend that completed the open without them would otherwise have the
     * access gate skipped outright (no MODE) or evaluated against zeros read
     * as root:root.  Only opens the gate must actually decide are refused: a
     * gate-exempt credential, a file this open just created, and an open
     * requesting no data access need nothing from these attrs. */
    if (request->status == CHIMERA_VFS_OK && handle &&
        !request->open_at.r_created &&
        chimera_vfs_open_at_checked(request->cred, request->open_at.flags) &&
        chimera_vfs_open_required_access(request->open_at.flags) &&
        chimera_vfs_open_gate_needed(request->module->capabilities,
                                     request->cred)) {
        uint64_t missing = chimera_vfs_gate_attrs_missing(
            &request->open_at.r_attr, request->module->capabilities);

        if (missing) {
            chimera_vfs_error("open_at: module %s replied without attrs 0x%llx "
                              "the access gate needs; refusing the open",
                              request->module->name,
                              (unsigned long long) missing);
            chimera_vfs_release(thread, handle);
            handle          = NULL;
            request->status = CHIMERA_VFS_EIO;
        }
    }

    /* POSIX open semantics, evaluated against the just-returned attrs
     * (mirroring the checks the plain-open wrapper applies on its lookup
     * path, which this create/openat path previously skipped entirely):
     *
     *  - O_NOFOLLOW and the object is a symlink -> ELOOP (an O_PATH-style
     *    open of the link itself is allowed);
     *  - a directory opened with write intent -> EISDIR;
     *  - a FIFO/socket/device opened with data access -> ENXIO (no
     *    blocking-open or device semantics behind a NAS client);
     *  - for non-exempt credentials, authorize the requested access:
     *    an existing file the caller may not access this way fails
     *    EACCES, while a freshly created file grants the requested
     *    access unconditionally (no permission check applies to a file
     *    that did not exist).  The effective grant is stamped on the
     *    handle so subsequent I/O is immune to later mode changes. */
    if (request->status == CHIMERA_VFS_OK && handle &&
        chimera_vfs_open_at_checked(request->cred, request->open_at.flags) &&
        (request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE)) {

        unsigned int           f      = request->open_at.flags;
        uint32_t               mode   = request->open_at.r_attr.va_mode;
        enum chimera_vfs_error status = CHIMERA_VFS_OK;

        if (S_ISLNK(mode) && !(f & CHIMERA_VFS_OPEN_NOFOLLOW) &&
            !(f & CHIMERA_VFS_OPEN_PATH)) {
            /* A symlink the open will FOLLOW (chimera_vfs_open's create
             * leg restarts resolution on the link target): neither type
             * checks nor the access gate apply to the link itself -- a
             * symlink's permission bits are never consulted, and the
             * restarted open authorizes the real target. */
        } else if (S_ISLNK(mode) && (f & CHIMERA_VFS_OPEN_NOFOLLOW) &&
                   !(f & CHIMERA_VFS_OPEN_PATH)) {
            status = CHIMERA_VFS_ELOOP;
        } else if (S_ISDIR(mode) &&
                   (f & (CHIMERA_VFS_OPEN_WRITE_ONLY |
                         CHIMERA_VFS_OPEN_TRUNCATE))) {
            status = CHIMERA_VFS_EISDIR;
        } else if (!S_ISREG(mode) && !S_ISDIR(mode) && !S_ISLNK(mode) &&
                   (f & (CHIMERA_VFS_OPEN_READ_ONLY |
                         CHIMERA_VFS_OPEN_WRITE_ONLY))) {
            status = CHIMERA_VFS_ENXIO;
        } else if (chimera_vfs_open_gate_needed(request->module->capabilities,
                                                request->cred)) {
            uint32_t required = chimera_vfs_open_required_access(f);
            uint32_t granted  =
                chimera_vfs_access_check(&request->open_at.r_attr,
                                         request->cred,
                                         CHIMERA_ACE_MASK_ALL);

            if (request->open_at.r_created) {
                granted |= required;
            }

            if ((granted & required) != required) {
                status = CHIMERA_VFS_EACCES;
            } else {
                chimera_vfs_handle_stamp_access(handle, granted);
            }
        } else {
            /* Gate-exempt credential (root, AUTH_NONE): DAC grants this open
             * everything, and POSIX binds that to the descriptor -- a later
             * setuid() must not revoke I/O on it, so record the full grant
             * rather than leaving the per-op gates to re-evaluate under
             * whatever credential wields the descriptor later. */
            chimera_vfs_handle_stamp_access(handle, CHIMERA_ACE_MASK_ALL);
        }

        if (status != CHIMERA_VFS_OK) {
            chimera_vfs_release(thread, handle);
            handle          = NULL;
            request->status = status;
        }
    }

    if (request->status == CHIMERA_VFS_OK && handle &&
        request->open_at.deferred_set_attr &&
        (!request->open_at.r_created ||
         ((request->open_at.deferred_set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
          request->open_at.deferred_set_attr->va_size != 0))) {
        struct chimera_vfs_attrs *trunc = &request->io_pnfs_sync_attr;

        request->io_handle = handle;
        memset(trunc, 0, sizeof(*trunc));
        trunc->va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        trunc->va_size     = (request->open_at.original_flags & CHIMERA_VFS_OPEN_TRUNCATE)
            ? 0 : request->open_at.deferred_set_attr->va_size;
        chimera_vfs_fsetattr(thread, request->cred, handle, trunc, 0,
                             request->open_at.r_attr.va_req_mask,
                             chimera_vfs_open_at_truncate_complete, request);
        return;
    }

    chimera_vfs_open_at_reply(request, handle);
} /* chimera_vfs_open_at_hdl_callback */

static void
chimera_vfs_open_finish(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread = request->thread;
    uint64_t                        fh_hash;
    struct vfs_open_cache          *cache;
    struct chimera_vfs_open_handle *handle;

    /* A create the parent gate refused went to the module as a plain open, so
     * a missing name comes back ENOENT.  What the caller was refused is the
     * create: report that, as the create would have.  The remap also catches
     * an ENOENT the refusal did not cause -- a prefix or parent racing away
     * between the gate and the dispatch -- so leave a trace for whoever
     * debugs the resulting permission error. */
    if (request->status == CHIMERA_VFS_ENOENT &&
        (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE_REFUSED)) {
        chimera_vfs_debug("open_at: reporting a module ENOENT as EACCES: "
                          "the parent gate had refused this open's create");
        request->status = CHIMERA_VFS_EACCES;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_PATH) {
        cache = thread->vfs->vfs_open_path_cache;
    } else {
        cache = thread->vfs->vfs_open_file_cache;
    }

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_abort_if(!(request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "open_at: no fh returned from vfs module");

        fh_hash = chimera_vfs_hash(request->open_at.r_attr.va_fh,
                                   request->open_at.r_attr.va_fh_len);

        if (chimera_vfs_open_handle_retained(
                request->open_at.flags,
                request->module->capabilities)) {
            chimera_vfs_open_cache_insert(
                thread,
                cache,
                request->module,
                request,
                request->open_at.r_attr.va_fh,
                request->open_at.r_attr.va_fh_len,
                fh_hash,
                request->open_at.r_vfs_private,
                request->open_at.flags,
                chimera_vfs_open_at_hdl_callback);
        } else {

            /* This is an inferred open from the likes of NFS3 create
             * where caller does not need to hold a reference count
             * and our module does not need open handles, so
             * we can synthesize a handle and return it immediately.
             */

            handle = chimera_vfs_synth_handle_alloc(thread);

            memcpy(handle->fh, request->open_at.r_attr.va_fh, request->open_at.r_attr.va_fh_len);
            handle->vfs_module  = request->module;
            handle->fh_len      = request->open_at.r_attr.va_fh_len;
            handle->fh_hash     = fh_hash;
            handle->vfs_private = 0;

            chimera_vfs_open_at_hdl_callback(request, handle);

        }

    } else {
        chimera_vfs_open_at_hdl_callback(request, NULL);
    }
} /* chimera_vfs_open_finish */

/* Continuation after the non-atomic handle-state record has been persisted to
 * the default KV (best-effort: a failure leaves the file open without its
 * durable record, which only the in-memory backends ever hit). */
static void
chimera_vfs_open_hs_put_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_error("open_at: failed to persist handle-state to default KV: %d",
                          error_code);
    }

    chimera_vfs_open_finish(request);
} /* chimera_vfs_open_hs_put_complete */

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_handle_state *hs = request->open_at.handle_state;

    if (request->open_at.deferred_set_attr) {
        request->open_at.set_attr = request->open_at.deferred_set_attr;
        /* Restore the deferred mutation, preserving a backend's metadata-only
         * fallback classification for the returned descriptor. */
        request->open_at.flags |= request->open_at.original_flags & CHIMERA_VFS_OPEN_TRUNCATE;
    }

    /* Backends that persist handle-state atomically (CAP_ATOMIC_HANDLE_STATE)
     * have already stored it as part of the open.  For backends without native
     * KV, the VFS core persists the record to the default KV instead, keyed by
     * the new file's fh so close/recovery find it on the same backend.  This is
     * a separate, non-atomic put; it is only reached by in-memory backends
     * (memfs) and passthrough, where cross-crash atomicity is moot. */
    if (request->status == CHIMERA_VFS_OK && hs &&
        !(request->module->capabilities & CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE) &&
        request->thread->vfs->kv_module &&
        (request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH)) {

        chimera_vfs_put_key_at(request->thread, request->cred,
                               request->open_at.r_attr.va_fh,
                               request->open_at.r_attr.va_fh_len,
                               hs->key, hs->key_len,
                               hs->value, hs->value_len,
                               chimera_vfs_open_hs_put_complete, request);
        return;
    }

    chimera_vfs_open_finish(request);
} /* chimera_vfs_open_complete */

static void
chimera_vfs_open_at_toolong(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_toolong_ctx *ctx      = private_data;
    chimera_vfs_open_at_callback_t  callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    chimera_vfs_toolong_free(ctx);

    /* The one open_at completion without the caller's set_attr: the shared
     * too-long continuation carries only the callback pair.  Harmless while
     * the servers touch set_attr on EEXIST alone, which this leg cannot
     * answer. */
    callback(status, NULL, NULL, NULL, NULL, NULL, arg);
} /* chimera_vfs_open_at_toolong */

/*
 * Parent pre-step for open_at.  It fetches the directory that holds (or will
 * hold) the name once, for either or both of two reasons:
 *
 *  - the parent gate (chimera_vfs_open_at_parent_gate_needed): the engine
 *    decides the open's parent-directory access itself, against the parent's
 *    attrs and ACL as the engine resolves them, and tells the module so with
 *    CHIMERA_VFS_OPEN_PARENT_CHECKED.  A POSIX (AUTH_UNIX) caller needs
 *    EXECUTE on the parent for any open the engine checks -- refused before
 *    anything is dispatched, so nothing about the name leaks -- and WRITE_DATA
 *    on top to create; an SMB (AUTH_ATTR) caller needs WRITE_DATA (ADD_FILE)
 *    alone to create, traverse being bypassed in that model.  A create the
 *    caller is refused is never dispatched as a create (see
 *    chimera_vfs_open_at_pre_complete), so no race can turn the refusal into
 *    a new name;
 *
 *  - group inheritance, for a backend that cannot derive the new file's group
 *    from its parent directory (CHIMERA_VFS_CAP_CREATE_GID_ENGINE): name the
 *    group from the parent.  See chimera_vfs_create_inherit_gid.
 *
 * The parent is not `handle` on a path-only backend -- `name` is a whole
 * in-mount path there, so the directory that will hold the new file is the
 * prefix before its last '/' and only a lookup can name it.  For the gid
 * inheritance alone the lookup does NOT follow a final symlink: a symlinked
 * prefix is followed by the create itself, and all the inheritance wants is
 * the mode and gid of whatever the name denotes.  The gate does follow it:
 * what it decides is access to the directory the name lands in, and a
 * symlink's own mode decides nothing.  A failed lookup is not the gid
 * inheritance's business to report -- the create produces the right error
 * for an unreachable prefix -- so without the gate it just proceeds without
 * inheriting; the gate, which cannot decide without the parent, fails the
 * open with the fetch's error.
 *
 * The resume context is malloc'd rather than taken from the request gate
 * scratch.  The scratch is for the gate helpers' own async chain; holding it
 * across a chimera_vfs_lookup() is not something it supports, and doing so
 * makes an unrelated later op in the same trace answer EINVAL where it owes
 * ELOOP.  That interaction is not understood, so this stays out of it.
 */
struct chimera_vfs_open_at_pre_ctx {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    const char                      *name;
    int                              namelen;
    unsigned int                     flags;
    struct chimera_vfs_attrs        *set_attr;
    uint64_t                         attr_mask;
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
    struct chimera_vfs_handle_state *handle_state;
    chimera_vfs_open_at_callback_t   callback;
    void                            *private_data;
    int                              gate;  /* the parent gate applies */
    int                              gid;   /* gid inheritance applies */
    int                              base;  /* offset of name's final component */
    /* The directory a refused exclusive create probes for the name. */
    uint8_t                          probe_fh[CHIMERA_VFS_FH_SIZE];
    int                              probe_fh_len;
};

/*
 * Whether the engine checks this open's parent directory itself: every
 * creating open by a caller the access gate does not exempt (not root, not
 * AUTH_NONE) on a backend that does not delegate DAC to a real enforcer.  An
 * open that cannot create stays with the module's own traversal check, so it
 * costs no extra round trip.
 */
static int
chimera_vfs_open_at_parent_gate_needed(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    flags)
{
    return chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred) &&
           (flags & CHIMERA_VFS_OPEN_CREATE);
} /* chimera_vfs_open_at_parent_gate_needed */

static void chimera_vfs_open_at_hs_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *set_attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data);

/* A refused exclusive create is never dispatched at all: whether the name
 * exists decides it.  An existing name collides (EEXIST), as it would have
 * with the create right -- POSIX and Windows both answer the collision ahead
 * of the create right -- while a missing one is the create the caller was
 * refused (EACCES).  Two known differences from a dispatched create, both
 * fail-closed: the probe's exact-match lookup does not reproduce an
 * AUTH_ATTR caller's case-insensitive collision (EACCES where the dispatch
 * would say EEXIST), and an existing symlink leaf answers the collision
 * rather than ELOOP.
 *
 * The probe runs as the engine (uid 0, exempt from every gate), not as the
 * caller.  What the caller owes before a name in the parent is examined, the
 * parent gate has already settled -- search for a POSIX caller, nothing for
 * an SMB one, whose traverse is bypassed -- and the caller's credential would
 * have the lookup gate ask an SMB caller for the search it does not owe.  The
 * probe resolves nothing the gate did not judge: only the final component, in
 * the directory whose attrs the gate evaluated.  On the handle that is the
 * module's own literal-name lookup, never the engine's path walker, which
 * resolves '/' and '..' -- as the exempt uid 0, that would answer EEXIST for
 * nested names the caller cannot traverse (U19 in the gate test). */
static const struct chimera_vfs_cred chimera_vfs_open_at_probe_cred = {
    .flavor = CHIMERA_VFS_AUTH_UNIX,
    .uid    = 0,
    .gid    = 0,
};

static void
chimera_vfs_open_at_excl_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_open_at_pre_ctx *ctx      = private_data;
    chimera_vfs_open_at_callback_t      callback = ctx->callback;
    void                               *arg      = ctx->private_data;
    struct chimera_vfs_attrs           *set_attr = ctx->set_attr;

    free(ctx);

    if (error_code == CHIMERA_VFS_OK) {
        error_code = CHIMERA_VFS_EEXIST;
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        error_code = CHIMERA_VFS_EACCES;
    }

    /* The caller's set_attr rides the completion, as it does on a
     * dispatched open: the NFS servers reuse it for the EXCLUSIVE
     * verifier re-open of the colliding name. */
    callback(error_code, NULL, set_attr, NULL, NULL, NULL, arg);
} /* chimera_vfs_open_at_excl_lookup_complete */

static void
chimera_vfs_open_at_excl_lookup_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    (void) dir_attr;
    chimera_vfs_open_at_excl_lookup_complete(error_code, attr, private_data);
} /* chimera_vfs_open_at_excl_lookup_at_complete */

static void
chimera_vfs_open_at_pre_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_open_at_pre_ctx *ctx    = private_data;
    struct chimera_vfs_open_at_pre_ctx  c      = *ctx;
    unsigned int                        flags  = c.flags;
    enum chimera_vfs_error              status = CHIMERA_VFS_OK;

    if (!c.gate) {
        if (error_code == CHIMERA_VFS_OK) {
            chimera_vfs_create_inherit_gid(c.set_attr, attr);
        }
    } else if (error_code != CHIMERA_VFS_OK) {
        status = error_code;
    } else {
        uint64_t missing = chimera_vfs_gate_attrs_missing(
            attr, c.handle->vfs_module->capabilities);
        uint32_t granted;

        if (missing) {
            chimera_vfs_error("open_at: module %s replied to the parent fetch "
                              "without attrs 0x%llx the parent gate needs; "
                              "refusing the open",
                              c.handle->vfs_module->name,
                              (unsigned long long) missing);
            status = CHIMERA_VFS_EIO;
        } else if (!S_ISDIR(attr->va_mode)) {
            /* What the descriptor names first, as every create gate
             * (chimera_vfs_gate_fh) and the modules' own checks order it:
             * the type error is not masked as EACCES. */
            status = CHIMERA_VFS_ENOTDIR;
        } else if ((attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
                   attr->va_nlink == 0) {
            /* ...whether it still exists second: nothing is created in or
             * resolved through a removed directory. */
            status = CHIMERA_VFS_ENOENT;
        } else {
            granted = chimera_vfs_access_check(attr, c.cred,
                                               CHIMERA_ACE_MASK_ALL);

            if (c.cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
                !(granted & CHIMERA_ACE_EXECUTE)) {
                /* No search, no resolution: refused before anything about
                 * the name is examined (chimera #1771). */
                status = CHIMERA_VFS_EACCES;
            } else if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
                flags |= CHIMERA_VFS_OPEN_PARENT_CHECKED;
            } else if (granted & CHIMERA_ACE_WRITE_DATA) {
                if (c.gid) {
                    chimera_vfs_create_inherit_gid(c.set_attr, attr);
                }
                flags |= CHIMERA_VFS_OPEN_PARENT_CHECKED;
            } else if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
                /* The probed directory is the handle -- asked for the literal
                 * name, one component, exactly what a dispatched create would
                 * have named -- or on a path-only backend the prefix the
                 * gate's lookup resolved, asked for the final component.  The
                 * context rides the probe; its callback frees it. */
                if (c.base == 0) {
                    chimera_vfs_lookup_at(
                        c.thread,
                        &chimera_vfs_open_at_probe_cred,
                        c.handle, c.name, c.namelen,
                        CHIMERA_VFS_ATTR_FH, 0,
                        chimera_vfs_open_at_excl_lookup_at_complete,
                        ctx);
                    return;
                }
                if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
                    memcpy(ctx->probe_fh, attr->va_fh, attr->va_fh_len);
                    ctx->probe_fh_len = attr->va_fh_len;
                    chimera_vfs_lookup(c.thread,
                                       &chimera_vfs_open_at_probe_cred,
                                       ctx->probe_fh, ctx->probe_fh_len,
                                       c.name + c.base, c.namelen - c.base,
                                       CHIMERA_VFS_ATTR_FH, 0,
                                       chimera_vfs_open_at_excl_lookup_complete,
                                       ctx);
                    return;
                }
                chimera_vfs_error("open_at: module %s replied to the parent "
                                  "fetch without the fh the exclusive-create "
                                  "probe needs; refusing the open",
                                  c.handle->vfs_module->name);
                status = CHIMERA_VFS_EIO;
            } else {
                /* Refused the create, not the open: an existing name still
                 * opens.  The request is dispatched without the create (see
                 * chimera_vfs_open_at_hs_dispatch), so the module cannot
                 * create the name however the directory changes meanwhile,
                 * and a miss reports the refusal (chimera_vfs_open_finish). */
                flags |= CHIMERA_VFS_OPEN_PARENT_CHECKED |
                    CHIMERA_VFS_OPEN_CREATE_REFUSED;
            }
        }
    }

    free(ctx);

    if (status != CHIMERA_VFS_OK) {
        c.callback(status, NULL, c.set_attr, NULL, NULL, NULL, c.private_data);
        return;
    }

    chimera_vfs_open_at_hs_dispatch(c.thread, c.cred, c.handle, c.name,
                                    c.namelen, flags, c.set_attr,
                                    c.attr_mask, c.pre_attr_mask,
                                    c.post_attr_mask, c.handle_state,
                                    c.callback, c.private_data);
} /* chimera_vfs_open_at_pre_complete */

/* The parent IS `handle` (an FH-relative backend, or a single-component name
 * on a path-only one): its attrs are one getattr away, no lookup needed. */
static void
chimera_vfs_open_at_pre_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    chimera_vfs_open_at_pre_complete(error_code, attr, private_data);
} /* chimera_vfs_open_at_pre_getattr_complete */

SYMBOL_EXPORT void
chimera_vfs_open_at_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *set_attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data)
{
    int gid, gate;

    /* Only the engine says it has checked the parent. */
    flags &= ~(CHIMERA_VFS_OPEN_PARENT_CHECKED | CHIMERA_VFS_OPEN_CREATE_REFUSED);

    gid = (flags & CHIMERA_VFS_OPEN_CREATE) && set_attr &&
        !(set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
        (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CREATE_GID_ENGINE);
    gate = chimera_vfs_open_at_parent_gate_needed(thread, cred, handle, flags);

    if (gid || gate) {
        struct chimera_vfs_open_at_pre_ctx *ctx;
        uint64_t                            mask = CHIMERA_VFS_ATTR_MASK_STAT;
        int                                 plen = 0;
        int                                 base = 0;

        /* Only an FS_PATH_OP module's `name` spans components, so only there
         * does a slash name a different parent than the handle.  Everywhere
         * else the parent IS the handle: a slash is just a byte in the final
         * component, and splitting on it would have the gate judge (and the
         * modules then skip) a directory the open never touches. */
        if (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
            plen = namelen;
            while (plen > 0 && name[plen - 1] != '/') {
                plen--;
            }
            base = plen;
            while (plen > 0 && name[plen - 1] == '/') {
                plen--;
            }
        }

        ctx = calloc(1, sizeof(*ctx));

        if (!ctx) {
            chimera_vfs_error("open_at: parent pre-step context allocation "
                              "failed; failing the open with EIO");
            callback(CHIMERA_VFS_EIO, NULL, set_attr, NULL, NULL, NULL,
                     private_data);
            return;
        }

        ctx->thread         = thread;
        ctx->cred           = cred;
        ctx->handle         = handle;
        ctx->name           = name;
        ctx->namelen        = namelen;
        ctx->flags          = flags;
        ctx->set_attr       = set_attr;
        ctx->attr_mask      = attr_mask;
        ctx->pre_attr_mask  = pre_attr_mask;
        ctx->post_attr_mask = post_attr_mask;
        ctx->handle_state   = handle_state;
        ctx->callback       = callback;
        ctx->private_data   = private_data;
        ctx->gate           = gate;
        ctx->gid            = gid;
        ctx->base           = plen > 0 ? base : 0;

        /* The gate evaluates the parent's ACL as well as its mode.  The fetch
         * is dispatched like any other, so on a windows-identity mount the
         * ACL arrives resolved.  It also wants the parent's fh, which a
         * refused exclusive create probes. */
        if (gate) {
            mask |= CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_FH;
        }

        if (plen > 0) {
            chimera_vfs_lookup(thread, cred, handle->fh, handle->fh_len,
                               name, plen, mask,
                               gate ? CHIMERA_VFS_LOOKUP_FOLLOW : 0,
                               chimera_vfs_open_at_pre_complete, ctx);
        } else {
            chimera_vfs_getattr(thread, cred, handle, mask,
                                chimera_vfs_open_at_pre_getattr_complete, ctx);
        }
        return;
    }

    chimera_vfs_open_at_hs_dispatch(thread, cred, handle, name, namelen, flags,
                                    set_attr, attr_mask, pre_attr_mask,
                                    post_attr_mask, handle_state, callback,
                                    private_data);
} /* chimera_vfs_open_at_hs */

static void
chimera_vfs_open_at_hs_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *set_attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;
    uint8_t                     cross_fh[CHIMERA_VFS_FH_SIZE + 16];
    int                         cross_fh_len;

    chimera_vfs_abort_if(!set_attr, "no setattr provided");

    /* A mount point opens as the root of what is mounted on it.  The gate's
     * verdict was about this directory, not the mounted root, so the
     * engine-internal flags do not cross with the open. */
    if (chimera_vfs_mount_table_cover_root(thread->vfs->mount_table,
                                           handle->fh, handle->fh_len,
                                           name, namelen,
                                           cross_fh, &cross_fh_len, NULL) == 0) {
        chimera_vfs_open_at_redirect(thread, cred, cross_fh, cross_fh_len,
                                     flags & ~(CHIMERA_VFS_OPEN_PARENT_CHECKED |
                                               CHIMERA_VFS_OPEN_CREATE_REFUSED),
                                     set_attr, attr_mask, callback, private_data);
        return;
    }

    /* On a creating open the trailing component is a new name; reject one longer
     * than {NAME_MAX} -- but search permission on the directory that would hold
     * it is owed first (chimera_vfs_name_too_long_handle).  FS_PATH_OP backends
     * receive the whole path as `name` and let the kernel enforce this. */
    if ((flags & CHIMERA_VFS_OPEN_CREATE) &&
        !(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) &&
        namelen >= CHIMERA_VFS_NAME_MAX) {
        chimera_vfs_name_too_long_handle(thread, cred, handle,
                                         chimera_vfs_open_at_toolong,
                                         callback, private_data);
        return;
    }

    /* An FS_PATH_OP backend receives the whole path as `name` and answers a
     * too-long name with ENOENT, not ENAMETOOLONG (the server cannot distinguish
     * the two from a wire CREATE).  Enforce the POSIX limits here, per component
     * -- so chmod/chown/utimens/open by an over-long path report ENAMETOOLONG
     * rather than ENOENT.  Unlike the handle-relative case above this cannot
     * defer to a search check: `name` is a whole path, and the directory that
     * holds an over-long component past the first is not resolved here.  The
     * path-only backend resolves the prefix itself and answers the denial. */
    if (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        int complen = 0, i;

        if (namelen >= CHIMERA_VFS_PATH_MAX) {
            callback(CHIMERA_VFS_ENAMETOOLONG, NULL, set_attr, NULL, NULL,
                     NULL, private_data);
            return;
        }
        for (i = 0; i < namelen; i++) {
            if (name[i] == '/') {
                complen = 0;
            } else if (++complen >= CHIMERA_VFS_NAME_MAX) {
                callback(CHIMERA_VFS_ENAMETOOLONG, NULL, set_attr, NULL, NULL,
                         NULL, private_data);
                return;
            }
        }
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, set_attr, NULL, NULL,
                 NULL, private_data);
        return;
    }

    request->opcode                    = CHIMERA_VFS_OP_OPEN_AT;
    request->complete                  = chimera_vfs_open_complete;
    request->open_at.handle            = handle;
    request->open_at.name              = name;
    request->open_at.namelen           = namelen;
    request->open_at.name_hash         = chimera_vfs_hash(name, namelen);
    request->open_at.flags             = flags;
    request->open_at.set_attr          = set_attr;
    request->open_at.handle_state      = handle_state;
    request->open_at.r_created         = 0;
    request->open_at.deferred_set_attr = NULL;

    /* CREATE's size is a mutation even for inferred/stateless opens. Defer it
     * on every backend so the setattr gate authorizes the existing inode first.
     * pNFS O_TRUNC additionally needs to resize the redirected backing file. */
    if (((flags & CHIMERA_VFS_OPEN_CREATE) &&
         (set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) ||
        (chimera_vfs_pnfs_io_possible(thread, handle) &&
         (flags & CHIMERA_VFS_OPEN_TRUNCATE))) {
        request->open_at.deferred_set_attr      = set_attr;
        request->open_at.original_flags         = flags;
        request->io_pnfs_sync_attr              = *set_attr;
        request->io_pnfs_sync_attr.va_set_mask &= ~CHIMERA_VFS_ATTR_SIZE;
        request->open_at.set_attr               = &request->io_pnfs_sync_attr;
        request->open_at.flags                 &= ~CHIMERA_VFS_OPEN_TRUNCATE;
    }

    request->open_at.r_attr.va_req_mask = attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;

    /* The completion checks need the mode (and, for the access gate,
     * uid/gid plus a native ACL if any); make sure the backend returns
     * them. */
    if (chimera_vfs_open_at_checked(cred, flags)) {
        request->open_at.r_attr.va_req_mask |=
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL;
    }
    request->open_at.r_attr.va_set_mask          = 0;
    request->open_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->open_at.r_dir_pre_attr.va_set_mask  = 0;
    request->open_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->open_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    /* A create the parent gate refused has had every decision above made as
     * the create it was asked as -- a SIZE in its set_attr is still deferred,
     * so the post-open setattr authorizes it against the existing file -- but
     * it reaches the module as a plain open, which cannot create the name. */
    if (flags & CHIMERA_VFS_OPEN_CREATE_REFUSED) {
        request->open_at.flags &= ~(CHIMERA_VFS_OPEN_CREATE |
                                    CHIMERA_VFS_OPEN_EXCLUSIVE);
    }

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open_at_hs_dispatch */

SYMBOL_EXPORT void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data)
{
    chimera_vfs_open_at_hs(thread, cred, handle, name, namelen, flags, set_attr,
                           attr_mask, pre_attr_mask, post_attr_mask, NULL,
                           callback, private_data);
} /* chimera_vfs_open_at */
