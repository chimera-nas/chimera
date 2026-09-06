// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <xxhash.h>
#include <uthash.h>
#include <utlist.h>

#include "vfs/vfs.h"
#include "vfs/sdk/vfs_fh.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/vfs_mount_table.h"
#include "common/logging.h"
#include "common/misc.h"
#include "metrics/metrics.h"
#include "vfs/vfs_dump.h"

/* Default period between close-thread cache sweeps (open-path/open-file
 * cache LRU tick + NFSv4 idle-state reap). Every wake the timer walks the
 * caches unconditionally, so on an otherwise idle pod this is the dominant
 * source of baseline CPU (~180m/pod at 100ms). 1s keeps the deferred-close
 * / idle-state reap latency well inside a typical NFS lease/DRC window
 * while cutting idle CPU by ~10x. Callers who need aggressive close
 * reclamation can override via the CHIMERA_CLOSE_SWEEP_INTERVAL_MS env
 * var (units: milliseconds; clamped to [10, 60000]). */
#define CHIMERA_CLOSE_SWEEP_INTERVAL_US_DEFAULT 1000000UL

#ifndef container_of
#define container_of(ptr, type, member) ({            \
        typeof(((type *) 0)->member) * __mptr = (ptr); \
        (type *) ((char *) __mptr - offsetof(type, member)); })
#endif // ifndef container_of

/* Canonical access bits an open with the given flags requires against
 * the target file. */
static inline uint32_t
chimera_vfs_open_required_access(unsigned int flags)
{
    uint32_t required = 0;

    /* Access intent is signalled positively: O_RDONLY -> READ_ONLY,
     * O_WRONLY -> WRITE_ONLY, O_RDWR -> both.  A raw open with neither bit
     * (e.g. an O_PATH-style handle open) requests no data access and is not
     * gated. */
    if (flags & CHIMERA_VFS_OPEN_READ_ONLY) {
        required |= CHIMERA_ACE_READ_DATA;
    }
    if (flags & CHIMERA_VFS_OPEN_WRITE_ONLY) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }
    if (flags & CHIMERA_VFS_OPEN_TRUNCATE) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }
    return required;
} /* chimera_vfs_open_required_access */

/* Record an open-time effective-access grant on a handle.  POSIX (and the
 * NFSv4/SMB stateful-open models) bind I/O rights when the file is opened;
 * the I/O paths consult granted_access instead of re-deriving from the
 * file's current mode, so later chmods do not revoke existing descriptors.
 * The open cache shards handles by (fh, access_mode, cred_hash), so grants
 * from same-credential opens of one file share a handle: union them --
 * a successful open never narrows what an earlier open of the same handle
 * legitimately obtained. */
static inline void
chimera_vfs_handle_stamp_access(
    struct chimera_vfs_open_handle *handle,
    uint32_t                        granted)
{
    if (handle->granted_valid) {
        handle->granted_access |= granted;
    } else {
        handle->granted_access = granted;
        handle->granted_valid  = 1;
    }
    /* Every caller of this helper stamps at open time; the grant binds to
     * the handle (see granted_bound in vfs_request.h). */
    handle->granted_bound = 1;
} /* chimera_vfs_handle_stamp_access */

/* chimera_vfs_debug/info/error/fatal/abort and the *_if variants come from
 * sdk/vfs_log.h (via vfs.h): they are part of the module-facing SDK. */

/* ERR_PTR style error handling for request allocation */
#define CHIMERA_VFS_MAX_ERRNO 4095
#define CHIMERA_VFS_ERR_PTR(err) ((void *) (long) (-(err)))
#define CHIMERA_VFS_PTR_ERR(ptr) ((int) (-(long) (ptr)))
#define CHIMERA_VFS_IS_ERR(ptr)  ((unsigned long) (ptr) > (unsigned long) -CHIMERA_VFS_MAX_ERRNO)

/* Parse a comma-separated key[=value] options string into mount_options,
 * copying keys/values into the caller's buffer.  Shared by the mount and
 * mkfs paths.  Defined in vfs_proc_mount.c. */
int
chimera_vfs_parse_mount_options(
    const char                       *options,
    struct chimera_vfs_mount_options *mount_options,
    char                             *buffer,
    int                               buffer_size,
    char                             *errbuf,
    size_t                            errbuf_len);

/* Structure for readdir entries stored in bounce buffer */
struct chimera_vfs_readdir_entry {
    uint64_t                 inum;
    uint64_t                 cookie;
    uint32_t                 namelen;
    struct chimera_vfs_attrs attrs;
    /* Name follows immediately after this struct */
};

/* chimera_vfs_hash comes from sdk/vfs_utils.h (via vfs.h): it is part of
 * the module-facing SDK, compiled in vfs_sdk_utils.c. */

static inline struct chimera_vfs_find_result *
chimera_vfs_find_result_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_find_result *result;

    if (thread->free_find_results) {
        result = thread->free_find_results;
        LL_DELETE(thread->free_find_results, result);
    } else {
        result = calloc(1, sizeof(struct chimera_vfs_find_result));
    }

    return result;
} /* chimera_vfs_find_result_alloc */

static inline void
chimera_vfs_find_result_free(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_find_result *result)
{
    LL_PREPEND(thread->free_find_results, result);
} /* chimera_vfs_find_result_free */

/*
 * Resolve the mount that owns `fh` into the two things a request needs from
 * it: the module to route to, and the backend's mount_private for the named
 * filesystem the handle belongs to.
 *
 * Both are read under the RCU read lock and copied out.  The mount itself is
 * deliberately not returned: umount frees it, so a caller holding the pointer
 * past the read-side critical section would be reading freed memory.
 *
 * `gated` separates the two questions the callers ask.  Routing is gated: a
 * mount that umount has claimed must not accept new work.  Filesystem
 * resolution is not, because umount's own closes run against exactly such a
 * mount and still have to find the filesystem their handle belongs to.
 */
static inline struct chimera_vfs_module *
chimera_vfs_resolve_mount(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen,
    int                        gated,
    void                     **r_mount_private)
{
    struct chimera_vfs        *vfs = thread->vfs;
    struct chimera_vfs_mount  *mount;
    struct chimera_vfs_module *module;

    if (r_mount_private) {
        *r_mount_private = NULL;
    }

    /*
     * Reject implausible handle lengths before they reach any sink.  The lower
     * bound guarantees a mount-id prefix to route on; the upper bound is the
     * security-critical half: a valid chimera handle never exceeds
     * CHIMERA_VFS_FH_SIZE, but the NFS3/NFS4 XDR decoders do not enforce the
     * wire `opaque<>` bound (see nfs3_attr.h), so an oversized handle would
     * otherwise be memcpy'd into the fixed CHIMERA_VFS_FH_SIZE handle buffers
     * (e.g. vfs_proc_open_fh.c, vfs_open_cache.h, vfs_state.c) and overflow
     * them.  This is the common chokepoint every fh-routed op funnels through,
     * so bounding here protects all of them; callers already treat a NULL
     * module as ESTALE/BADHANDLE.
     */
    if (fhlen < CHIMERA_VFS_MOUNT_ID_SIZE || fhlen > CHIMERA_VFS_FH_SIZE) {
        return NULL;
    }

    urcu_qsbr_read_lock();

    mount = chimera_vfs_mount_table_lookup(vfs->mount_table, fh);

    if (mount) {
        if (r_mount_private) {
            *r_mount_private = mount->mount_private;
        }
        module = (gated && mount->unmounting) ? NULL : mount->module;
    } else {
        module = NULL;
    }

    urcu_qsbr_read_unlock();

    return module;
} /* chimera_vfs_resolve_mount */

static inline struct chimera_vfs_module *
chimera_vfs_get_module(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    return chimera_vfs_resolve_mount(thread, fh, fhlen, 1, NULL);
} /* chimera_vfs_get_module */

/* True when the mount that owns `fh` is served by a path-only module (no
 * persistent file handles -- see chimera_vfs_module_is_path_only). */
static inline int
chimera_vfs_fh_is_path_only(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    struct chimera_vfs_module *module = chimera_vfs_get_module(thread, fh, fhlen);

    return module && chimera_vfs_module_is_path_only(module);
} /* chimera_vfs_fh_is_path_only */

/*
 * Routing decision for an fh-routed KV operation (handle-state put on open,
 * delete_key_at, search_keys_at).
 *
 *   - If the backend serving `fh` implements KV natively (CHIMERA_VFS_CAP_KV,
 *     e.g. cairn/diskfs), the op is dispatched to that backend and the record
 *     is stored co-located with the file (no key transformation).
 *
 *   - Otherwise the op is dispatched to the configured default KV module, and
 *     keys are namespaced with a single leading byte equal to the source
 *     backend's fh_magic.  Because every handle on a backend shares one
 *     fh_magic, the open/delete/search paths agree on the namespace, and keys
 *     from different backends can never collide in the shared default KV.
 */
struct chimera_vfs_kv_route {
    struct chimera_vfs_module *module;   /* where the op is dispatched          */
    int                        fallback; /* 1 = default KV w/ ns prefix; 0 = native */
    uint8_t                    ns;       /* namespace byte (source fh_magic)    */
};

static inline void
chimera_vfs_kv_route_fh(
    struct chimera_vfs_thread   *thread,
    const void                  *fh,
    int                          fhlen,
    struct chimera_vfs_kv_route *route)
{
    struct chimera_vfs_module *module = chimera_vfs_get_module(thread, fh, fhlen);

    if (module && (module->capabilities & CHIMERA_VFS_CAP_KV)) {
        route->module   = module;
        route->fallback = 0;
        route->ns       = 0;
    } else {
        route->module   = thread->vfs->kv_module;
        route->fallback = 1;
        route->ns       = module ? module->fh_magic : (uint8_t) CHIMERA_VFS_FH_MAGIC_ROOT;
    }
} /* chimera_vfs_kv_route_fh */

/*
 * Take a request off the thread's free list purely as storage for a gated
 * operation's resume context (the `gate` arm of chimera_vfs_request).
 *
 * This deliberately bypasses chimera_vfs_request_alloc_common: a gate-scratch
 * request is never dispatched, so it wants none of the per-operation
 * accounting -- no opcode, no module or capability check, no trace span, no
 * latency stopwatch, and no place on thread->active_requests (where it would
 * show up as an operation in flight that no backend is working on).  It is
 * bookkeeping-free storage that happens to be pooled, which is the whole
 * point: a gated operation now allocates nothing.
 *
 * Alloc and free must run on the same thread, which they do: the free list is
 * per-thread and unlocked, and a gate's completion is delivered on the thread
 * that started it (chimera_vfs_process_completion drains onto the owning
 * thread), exactly as chimera_vfs_request_free already relies on.
 *
 * Returns the scratch area itself; chimera_vfs_gate_scratch_free() recovers
 * the request from it.
 */
static inline void *
chimera_vfs_gate_scratch_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_request *request;

    if (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
    } else {
        request              = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread      = thread;
        request->plugin_data = malloc(CHIMERA_VFS_PLUGIN_DATA_SIZE);
    }

    return request->gate.data;
} /* chimera_vfs_gate_scratch_alloc */

static inline void
chimera_vfs_gate_scratch_free(
    struct chimera_vfs_thread *thread,
    void                      *scratch)
{
    struct chimera_vfs_request *request;

    request = container_of(scratch, struct chimera_vfs_request, gate.data);

    LL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_gate_scratch_free */

/*
 * Common request allocation helper with capability enforcement.
 * Returns ERR_PTR on failure:
 *   - CHIMERA_VFS_ESTALE if module is NULL
 *   - CHIMERA_VFS_ENOTSUP if module lacks required capability
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_common(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_module     *module,
    void                          *mount_private,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash,
    uint32_t                       required_cap)
{
    struct chimera_vfs_request *request;

    if (!module) {
        return CHIMERA_VFS_ERR_PTR(CHIMERA_VFS_ESTALE);
    }

    if (!(module->capabilities & required_cap)) {
        return CHIMERA_VFS_ERR_PTR(CHIMERA_VFS_ENOTSUP);
    }

    if (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
    } else {
        request              = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread      = thread;
        request->plugin_data = malloc(CHIMERA_VFS_PLUGIN_DATA_SIZE);
    }
    request->status        = CHIMERA_VFS_UNSET;
    request->cred          = cred;
    request->module        = module;
    request->mount_private = mount_private;

    /* Reset implicit-lease mediation state: requests are pooled and not
     * fully memset on reuse, so a prior op's owner/pin must not leak in. */
    request->io_owner_valid       = 0;
    request->io_sync_wait         = 0;
    request->notify_gate          = NULL;
    request->notify_gate_wrapped  = 0;
    request->notify_gate_resume   = 0;
    request->io_recall_all        = 0;
    request->io_recall_flush_only = 0;
    request->io_recall_single     = 0;
    request->io_recall_retain     = 0;
    request->io_sync_write        = 0;
    request->io_next              = NULL;
    request->io_lease_file        = NULL;
    request->io_handle            = NULL;
    request->io_owns_lease_ref    = 0;

    /* Default to autocommit; compound-aware proc entry points overwrite this
     * with their explicit `compound` argument right after allocation.  The
     * enlisted mark is owned by the dispatch guard alone. */
    request->compound          = NULL;
    request->compound_enlisted = 0;

    if (fh && fhlen > 0) {
        memcpy(request->fh, fh, fhlen);
    }
    request->fh_len      = fhlen;
    request->fh_hash     = fh_hash;
    request->active_prev = NULL;
    request->active_next = NULL;

    prometheus_stopwatch_start(&request->start_time);

    request->wait_reason   = NULL;
    request->wait_since_ns = 0;
    request->wait_arg0     = 0;
    request->wait_arg1     = 0;
    request->wait_arg2     = 0;

    /* Start this op's trace span as a child of the protocol span that issued it
     * (thread->otel_parent, set synchronously just before this call).  Capture
     * the parent pointer for sibling propagation (see chimera_vfs_complete), then
     * consume thread->otel_parent so a follow-up op that forgot to set one drops
     * its span rather than misattributing it.  ~free when not recording. */
    request->otel_parent = thread->otel_parent;
    otel_span_start_child(&request->otel, NULL, OTEL_SPAN_INTERNAL,
                          request->otel_parent);
    thread->otel_parent = NULL;

    thread->num_active_requests++;
    DL_APPEND2(thread->active_requests, request, active_prev, active_next);

    return request;
} /* chimera_vfs_request_alloc_common */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_hash(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash)
{
    void                      *mount_private;
    struct chimera_vfs_module *module = chimera_vfs_resolve_mount(thread, fh, fhlen, 1,
                                                                  &mount_private);

    return chimera_vfs_request_alloc_common(thread, cred, module, mount_private,
                                            fh, fhlen, fh_hash, CHIMERA_VFS_CAP_FS);
} /* chimera_vfs_request_alloc_by_hash */


static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_anon(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_key)
{
    uint64_t fh_hash = chimera_vfs_hash(&fh_key, sizeof(fh_key));

    return chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);
} /* chimera_vfs_request_alloc_by_hash */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen)
{
    uint64_t fh_hash = chimera_vfs_hash(fh, fhlen);

    return chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);
} /* chimera_vfs_request_alloc */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_handle(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle)
{
    return chimera_vfs_request_alloc_by_hash(thread, cred, handle->fh, handle->fh_len, handle->fh_hash);
} /* chimera_vfs_request_alloc_by_handle */

/*
 * Allocate a request with a pre-determined module (no mount table lookup).
 * Use this when the module is already known, e.g., from an open handle.
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_with_module(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash,
    struct chimera_vfs_module     *module)
{
    void *mount_private;

    /* The caller already knows the module, but the backend still needs to be
     * told which of its filesystems the handle belongs to.  Resolved ungated:
     * this path carries umount's own closes, whose mount is by definition
     * claimed, and refusing them would strand the state they release. */
    chimera_vfs_resolve_mount(thread, fh, fhlen, 0, &mount_private);

    return chimera_vfs_request_alloc_common(thread, cred, module, mount_private,
                                            fh, fhlen, fh_hash, CHIMERA_VFS_CAP_FS);
} /* chimera_vfs_request_alloc_with_module */

/*
 * Allocate a request for KV operations.
 * Uses the pre-configured kv_module instead of looking up by FH.
 * The key is hashed to determine the delegation thread for blocking modules.
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_kv(
    struct chimera_vfs_thread *thread,
    const void                *key,
    uint32_t                   key_len)
{
    struct chimera_vfs *vfs      = thread->vfs;
    uint64_t            key_hash = chimera_vfs_hash(key, key_len);

    return chimera_vfs_request_alloc_common(thread, NULL, vfs->kv_module, NULL,
                                            NULL, 0, key_hash, CHIMERA_VFS_CAP_KV);
} /* chimera_vfs_request_alloc_kv */

static inline struct chimera_vfs_open_handle *
chimera_vfs_synth_handle_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_open_handle *handle;

    if (thread->free_synth_handles) {
        handle = thread->free_synth_handles;
        LL_DELETE(thread->free_synth_handles, handle);
    } else {
        handle           = calloc(1, sizeof(struct chimera_vfs_open_handle));
        handle->cache_id = CHIMERA_VFS_OPEN_ID_SYNTHETIC;
    }
    handle->granted_valid = 0;
    return handle;
} /* chimera_vfs_synth_handle_alloc */

static inline void
chimera_vfs_synth_handle_free(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_abort_if(handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC, "real handle freed by synthetic procedure");

    LL_PREPEND(thread->free_synth_handles, handle);
} /* chimera_vfs_synth_handle_free */

/*
 * May this request's completion PUBLISH into the shared observation layer
 * (attr-cache inserts/refreshes, name-cache inserts, change-notify emits)?
 *
 * An op ENLISTED in an explicit compound completes before its compound
 * commits: its effects are provisional, staged in the owning engine, and a
 * commit that loses optimistic validation rolls them back.  Publishing them
 * at op completion would let every other consumer observe state that may
 * never commit -- phantom cache/notify entries that later path walks resolve
 * through, surfacing as intermittent ESTALE/ENOENT/EEXIST against committed
 * state.  Worse, an enlisted read's results (GETATTR/LOOKUP) may reflect the
 * compound's own uncommitted staging (engine read-your-writes), so they must
 * not populate the shared caches even when the compound later commits.
 *
 * The discriminator is the post-guard compound pointer: an enlisted request
 * keeps request->compound through completion (the chokepoint consumes only
 * the enlisted mark), while ejected and standalone requests reach completion
 * with NULL there (the dispatch guard clears it on every ejection and
 * core-layer failure).  Ejected/standalone ops autocommit, so they MUST keep
 * publishing.  The compound control ops also carry the pointer (for object
 * recovery) but publish nothing.
 *
 * This gates PUBLICATIONS only.  Invalidations/removals on a provisional
 * effect stay unconditional: removing a possibly-valid entry is safe;
 * leaving a phantom is not.  A gated MUTATION site must therefore fall back
 * to invalidating the same key(s) it would have published (else the
 * pre-compound entries survive the compound's COMMIT and are served stale);
 * gated READ-ONLY sites (getattr/lookup/read/commit refreshes) need no such
 * arm -- reads change no state, so the pre-compound entry remains valid for
 * standalone observers until a mutation lands.
 */
static inline int
chimera_vfs_request_publishes(const struct chimera_vfs_request *request)
{
    return request->compound == NULL;
} /* chimera_vfs_request_publishes */

static inline void
chimera_vfs_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    /* Enlisted-op completion bookkeeping.  Keyed on the enlisted mark the
     * dispatch guard stamped -- request->compound alone over-identifies: a
     * request can fail in core layers (claim-layer EACCES, dispatch-time
     * ESTALE/EROFS) still carrying the consumer's compound pointer without
     * ever having been enlisted, and the compound control ops carry it
     * purely for routing/recovery.  Runs on the beginning thread (delegated
     * completions are drained back onto it before the op completion runs),
     * so the counters need no atomics.  The mark is consumed so the
     * decrement fires exactly once per enlisted request.
     *
     * Conflict suppression: a member op may itself deliver
     * ECOMPOUND_CONFLICT.  If the compound lost any op to ejection, or its
     * consumer did not opt into replay (RETRYABLE), a replay is forbidden --
     * rewrite to the retriable-but-never-replayed ECOMPOUND_EXHAUSTED here,
     * in the core, rather than trusting drivers to honor the rule. */
    if (request->compound_enlisted) {
        struct chimera_vfs_compound *compound = request->compound;

        request->compound_enlisted = 0;

#ifdef CHIMERA_SANITIZE
        chimera_vfs_abort_if(compound->inflight_ops == 0,
                             "enlisted op %s completing with no inflight op accounted",
                             chimera_vfs_op_name(request->opcode));
#endif /* ifdef CHIMERA_SANITIZE */
        compound->inflight_ops--;

        if (request->status == CHIMERA_VFS_ECOMPOUND_CONFLICT &&
            (compound->ejected_mutating_ops > 0 ||
             !(compound->flags & CHIMERA_VFS_COMPOUND_RETRYABLE))) {
            request->status = CHIMERA_VFS_ECOMPOUND_EXHAUSTED;
        }
    }

    request->elapsed_ns = prometheus_stopwatch_elapsed_ns(&request->start_time);

    if (thread->metrics.op_latency_series) {
        prometheus_time_histogram_sample(thread->metrics.op_latency_series[request->opcode],
                                         &request->start_time);
    }

    /* Annotate (op name, attrs, status) and end the trace span.  The macro's
     * recording test is an inline flag check (unsampled ops pay nothing), and
     * the whole call compiles out when tracing is disabled. */
    chimera_vfs_trace_complete(request);

    /* Re-publish this op's trace parent so the proto completion callback that
     * runs next (on this thread) issues any chained sibling VFS op under the same
     * protocol span.  Consumed (cleared) at that op's alloc. */
    thread->otel_parent = request->otel_parent;

    chimera_vfs_dump_reply(request);
} /* chimera_vfs_complete */

static inline void
chimera_vfs_request_free(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *request)
{

#ifdef __clang_analyzer__
    chimera_vfs_abort_if(request->active_prev != request && request->active_next == NULL,
                         "clang static analysis thinks this can happen");
#endif /* ifdef __clang_analyzer__ */

    DL_DELETE2(thread->active_requests, request, active_prev, active_next);

    thread->num_active_requests--;

    LL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */

static inline void
chimera_vfs_complete_delegate(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    pthread_mutex_lock(&thread->lock);
    DL_APPEND(thread->pending_complete_requests, request);
    pthread_mutex_unlock(&thread->lock);

    evpl_ring_doorbell(&thread->doorbell);
} /* chimera_vfs_complete_delegate */

/* Marshal a parked I/O request back to its owning thread to resume.  The
 * lease pump that unblocks it may run on any thread (the one that released a
 * lease, acked/revoked a break, or ran the idle reaper), but the request's
 * dispatch and reply must run on request->thread, whose connection iovecs are
 * thread-local.  The owning thread drains pending_io_resume in
 * chimera_vfs_process_completion(). */
static inline void
chimera_vfs_io_resume_post(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    pthread_mutex_lock(&thread->lock);
    DL_APPEND(thread->pending_io_resume, request);
    pthread_mutex_unlock(&thread->lock);

    evpl_ring_doorbell(&thread->doorbell);
} /* chimera_vfs_io_resume_post */

static inline void
chimera_vfs_post_to_delegation(
    struct chimera_vfs_request           *request,
    struct chimera_vfs_delegation_thread *delegation_thread)
{
    request->complete_delegate = request->complete;
    request->complete          = chimera_vfs_complete_delegate;

    pthread_mutex_lock(&delegation_thread->lock);
    DL_APPEND(delegation_thread->requests, request);
    pthread_mutex_unlock(&delegation_thread->lock);

    evpl_ring_doorbell(&delegation_thread->doorbell);
} /* chimera_vfs_post_to_delegation */

/* Returns 1 if the request would mutate the filesystem and so must be rejected
 * on a read-only mount, 0 otherwise.  For OPEN_AT / OPEN_FH / OPEN_STREAM the
 * decision is flag-dependent: a pure read-only open is permitted, but an open
 * that requests create, write or truncate is a mutation.  Every op not listed
 * here (READ, READLINK, GETATTR, LOOKUP, READDIR, ACCESS, COMMIT, SEEK, LOCK,
 * GET_XATTR, LIST_XATTRS, LIST_STREAMS, GET_LAYOUT, GETPARENT, the KV reads,
 * etc.) is treated as non-mutating and dispatched normally. */
static inline int
chimera_vfs_op_is_mutating(const struct chimera_vfs_request *request)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_WRITE:
        case CHIMERA_VFS_OP_REMOVE_AT:
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT:
        case CHIMERA_VFS_OP_RENAME_AT:
        case CHIMERA_VFS_OP_SETATTR:
        case CHIMERA_VFS_OP_LINK_AT:
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_ALLOCATE:
        case CHIMERA_VFS_OP_SET_XATTR:
        case CHIMERA_VFS_OP_REMOVE_XATTR:
        case CHIMERA_VFS_OP_REMOVE_STREAM:
        case CHIMERA_VFS_OP_COPY_RANGE:
        case CHIMERA_VFS_OP_CLONE_RANGE:
        case CHIMERA_VFS_OP_MOVE_RANGE:
        case CHIMERA_VFS_OP_PUT_KEY:
        case CHIMERA_VFS_OP_DELETE_KEY:
            return 1;
        case CHIMERA_VFS_OP_OPEN_AT:
            return !!(request->open_at.flags &
                      (CHIMERA_VFS_OPEN_CREATE |
                       CHIMERA_VFS_OPEN_WRITE_ONLY |
                       CHIMERA_VFS_OPEN_TRUNCATE));
        case CHIMERA_VFS_OP_OPEN_FH:
            return !!(request->open_fh.flags &
                      (CHIMERA_VFS_OPEN_CREATE |
                       CHIMERA_VFS_OPEN_WRITE_ONLY |
                       CHIMERA_VFS_OPEN_TRUNCATE));
        case CHIMERA_VFS_OP_OPEN_STREAM:
            return !!(request->open_stream.flags &
                      (CHIMERA_VFS_OPEN_CREATE |
                       CHIMERA_VFS_OPEN_WRITE_ONLY |
                       CHIMERA_VFS_OPEN_TRUNCATE));
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_op_is_mutating */

/* Returns 1 for the KV-class opcodes (the put/get/delete/search key family).
 * KV requests are allocated against the pool-wide kv_module with
 * mount_private == NULL (chimera_vfs_request_alloc_kv): KV namespaces are
 * pool-wide, so mount identity does not apply to them.  The dispatch
 * enlistment guard uses this to match KV ops on module equality alone. */
static inline int
chimera_vfs_op_is_kv(uint32_t opcode)
{
    switch (opcode) {
        case CHIMERA_VFS_OP_PUT_KEY:
        case CHIMERA_VFS_OP_GET_KEY:
        case CHIMERA_VFS_OP_DELETE_KEY:
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_op_is_kv */

/* Byte cap on the interior payload a single streaming-emulation fallback
 * (vfs_proc_copy_range.c / vfs_proc_write_same.c) may enlist in one
 * compound.  An enlisted chunk write is staged in the owning engine's
 * compound state until COMPOUND_END (a cairn compound's data batch holds
 * every enlisted write payload in memory), so an enlisted bulk op would
 * otherwise pin its entire payload there.  Once a fallback has issued this
 * many bytes under its caller's compound, the remaining chunks run
 * standalone -- the best-effort degradation the compound contract sanctions
 * (see the ejection rules in sdk/vfs_request.h). */
#define CHIMERA_VFS_COMPOUND_BULK_CAP (8 * 1024 * 1024)

/* Returns 1 if the request targets a read-only mount, 0 otherwise (including
 * when the mount cannot be resolved -- such requests fall through to normal
 * dispatch which surfaces ESTALE).  The relevant fh for every mutating op is in
 * request->fh (the handle/parent/target fh); its first CHIMERA_VFS_MOUNT_ID_SIZE
 * bytes are the mount_id. */
static inline int
chimera_vfs_mount_is_readonly(const struct chimera_vfs_request *request)
{
    struct chimera_vfs_mount_attrs attrs;

    if (request->fh_len < CHIMERA_VFS_MOUNT_ID_SIZE) {
        return 0;
    }

    if (chimera_vfs_mount_table_lookup_attrs(request->thread->vfs->mount_table,
                                             request->fh, &attrs) != 0) {
        return 0;
    }

    return !!(attrs.flags & CHIMERA_VFS_MOUNT_ATTR_READONLY);
} /* chimera_vfs_mount_is_readonly */

/* vfs_notify.c: swap in the sync-coherence completion gate on namespace
 * mutations when sync watchers exist (see vfs_notify.h).  Declared here
 * rather than pulling vfs_notify.h into every dispatch consumer. */
void
chimera_vfs_notify_gate_install(
    struct chimera_vfs_request *request);

/* vfs_proc_compound_begin.c: construct and dispatch the fire-and-forget
 * backend OP_COMPOUND_BEGIN for a compound whose owner {module,
 * mount_private, route_hash} is already stamped, so the backend initializes
 * its per-compound state ahead of the first enlisted op (dispatch FIFO order
 * on the compound's route).  Shared by the eager-bind path in
 * chimera_vfs_compound_begin and the lazy-bind arm of chimera_vfs_dispatch;
 * safe to call from inside dispatch because the begin opcode is exempt from
 * the enlistment guard, so the recursion terminates. */
void
chimera_vfs_compound_dispatch_begin(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_compound   *compound,
    const void                    *fh,
    int                            fhlen);

static inline void
chimera_vfs_dispatch(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread            *thread = request->thread;
    struct chimera_vfs                   *vfs    = thread->vfs;
    struct chimera_vfs_module            *module = request->module;
    struct chimera_vfs_delegation_thread *delegation_thread;
    int                                   thread_id;

    chimera_vfs_dump_request(request);

    /* Namespace mutations gate their completion on sync watchers'
     * invalidation acks; a single relaxed load when none are registered. */
    chimera_vfs_notify_gate_install(request);

    if (!module || !thread->module_private[module->fh_magic]) {
        /* Failing in the core, before any backend saw the op: detach it from
         * any compound.  It neither enlists (no enlisted mark, so completion
         * bookkeeping skips it) nor ejects (it never executed anywhere, so
         * it cannot invalidate a replay).  The compound control ops keep
         * theirs: their completions recover the compound object from
         * request->compound. */
        if (request->compound &&
            request->opcode != CHIMERA_VFS_OP_COMPOUND_BEGIN &&
            request->opcode != CHIMERA_VFS_OP_COMPOUND_END) {
            request->compound = NULL;
        }
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Read-only mount enforcement: reject mutating ops with EROFS before they
     * reach the backend (or a delegation thread). */
    if (chimera_vfs_op_is_mutating(request) &&
        chimera_vfs_mount_is_readonly(request)) {
        /* Same core-level failure as above: detach without enlist/eject
         * accounting.  (Mutating ops are never the compound control ops.) */
        request->compound = NULL;
        request->status   = CHIMERA_VFS_EROFS;
        request->complete(request);
        return;
    }

    /* Enlistment guard: lazy bind, enlist, or eject.  The compound control
     * ops (OP_COMPOUND_BEGIN/_END) are exempt -- they ARE the machinery and
     * carry request->compound purely for routing and object recovery.
     *
     *   - LOOSE compound: never binds; every op runs standalone.
     *
     *   - Unbound compound (module == NULL): the first op on a
     *     compound-capable mount BINDS it -- stamp the owner {module,
     *     mount_private} and pin route_hash to this op's own fh_hash, then
     *     dispatch the backend OP_COMPOUND_BEGIN *first* (fire-and-forget,
     *     with this op's cred, routed identically, so dispatch FIFO order
     *     puts the backend's state setup ahead of this op; the recursion
     *     into chimera_vfs_dispatch is safe because the begin opcode is
     *     exempt from this guard) -- and then enlist this op.  An op on a
     *     non-capable mount is ejected instead, leaving the compound
     *     unbound for a later capable op.
     *
     *   - Bound compound: enlistment requires the op's {module,
     *     mount_private} to match the owner.  KV-class ops are the one
     *     carve-out: they match on module equality alone (see the clause
     *     below).  On mismatch -- e.g. a path walk carried the caller's
     *     compound across a mount/junction
     *     boundary -- the op is EJECTED: it runs standalone (autocommit),
     *     routes by its own fh_hash below, and its effects are independent
     *     of the compound's commit/abort.  This is deliberate best-effort
     *     semantics, and it is also the memory-safety barrier: a backend
     *     recovers its own compound object by casting request->compound, so
     *     a foreign backend must never see another backend's compound
     *     object.
     *
     * Every ejection is counted; a MUTATING ejection additionally forfeits
     * the compound's replay right (an ejected mutation commits standalone,
     * so a from-the-top replay would double-execute it -- the
     * conflict-suppression rewrite in chimera_vfs_complete / the end
     * completion enforces that).  Read-only ejections -- e.g. the
     * root-module resolution hop every path walk starts with -- are
     * harmless to re-execute and only bump the observability counter. */
    if (request->compound &&
        request->opcode != CHIMERA_VFS_OP_COMPOUND_BEGIN &&
        request->opcode != CHIMERA_VFS_OP_COMPOUND_END) {
        struct chimera_vfs_compound *compound = request->compound;

        if (compound->flags & CHIMERA_VFS_COMPOUND_LOOSE) {
            request->compound = NULL;
            compound->ejected_ops++;
            if (chimera_vfs_op_is_mutating(request)) {
                compound->ejected_mutating_ops++;
            }
        } else if (!compound->module) {
            if (module->capabilities & CHIMERA_VFS_CAP_COMPOUND) {
                /* Lazy bind. */
                compound->module        = module;
                compound->mount_private = request->mount_private;
                compound->route_hash    = request->fh_hash;

                chimera_vfs_compound_dispatch_begin(thread, request->cred,
                                                    compound,
                                                    request->fh,
                                                    request->fh_len);

#ifdef CHIMERA_SANITIZE
                chimera_vfs_abort_if(compound->inflight_ops != 0,
                                     "compound op %s issued with %u enlisted op(s) still in flight",
                                     chimera_vfs_op_name(request->opcode),
                                     compound->inflight_ops);
#endif /* ifdef CHIMERA_SANITIZE */
                request->compound_enlisted = 1;
                compound->enlisted_ops++;
                compound->inflight_ops++;
            } else {
                request->compound = NULL;
                compound->ejected_ops++;
                if (chimera_vfs_op_is_mutating(request)) {
                    compound->ejected_mutating_ops++;
                }
            }
        } else if (module == compound->module &&
                   (request->mount_private == compound->mount_private ||
                    /* KV clause: KV requests are allocated against the
                     * pool-wide kv_module with mount_private == NULL
                     * (chimera_vfs_request_alloc_kv).  KV namespaces are
                     * pool-wide -- mount identity does not apply to them --
                     * so when the KV module owns the compound, a KV-class
                     * op enlists on module equality alone; the plain
                     * {module, mount_private} test could otherwise never
                     * enlist one. */
                    chimera_vfs_op_is_kv(request->opcode))) {
#ifdef CHIMERA_SANITIZE
            chimera_vfs_abort_if(compound->inflight_ops != 0,
                                 "compound op %s issued with %u enlisted op(s) still in flight",
                                 chimera_vfs_op_name(request->opcode),
                                 compound->inflight_ops);
#endif /* ifdef CHIMERA_SANITIZE */
            request->compound_enlisted = 1;
            compound->enlisted_ops++;
            compound->inflight_ops++;
        } else {
            request->compound = NULL;
            compound->ejected_ops++;
            if (chimera_vfs_op_is_mutating(request)) {
                compound->ejected_mutating_ops++;
            }
        }
    }

    /* A compound lives on one backend thread (thread-local backend state); an
     * enlisted op (or the end op) must run on the thread the begin ran on, so
     * route by the compound's affinity key rather than this op's own fh_hash. */
    uint64_t route_hash = request->compound ? request->compound->route_hash
                                               : request->fh_hash;

    if ((module->capabilities & CHIMERA_VFS_CAP_BLOCKING) &&
        vfs->num_sync_delegation_threads > 0) {
        thread_id         = route_hash % vfs->num_sync_delegation_threads;
        delegation_thread = &vfs->sync_delegation_threads[thread_id];
        chimera_vfs_post_to_delegation(request, delegation_thread);
    } else if (vfs->num_async_delegation_threads > 0) {
        thread_id         = route_hash % vfs->num_async_delegation_threads;
        delegation_thread = &vfs->async_delegation_threads[thread_id];
        chimera_vfs_post_to_delegation(request, delegation_thread);
    } else {
        module->dispatch(request, thread->module_private[module->fh_magic]);
    }
} /* chimera_vfs_dispatch */

static inline void
chimera_vfs_copy_attr(
    struct chimera_vfs_attrs       *dest,
    const struct chimera_vfs_attrs *src)
{
    dest->va_req_mask = src->va_req_mask;
    dest->va_set_mask = src->va_set_mask;

    if (src->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        memcpy(dest->va_fh, src->va_fh, src->va_fh_len);
        dest->va_fh_len = src->va_fh_len;
    }

    if (src->va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        dest->va_dev        = src->va_dev;
        dest->va_ino        = src->va_ino;
        dest->va_mode       = src->va_mode;
        dest->va_nlink      = src->va_nlink;
        dest->va_uid        = src->va_uid;
        dest->va_gid        = src->va_gid;
        dest->va_rdev       = src->va_rdev;
        dest->va_size       = src->va_size;
        dest->va_space_used = src->va_space_used;
        dest->va_atime      = src->va_atime;
        dest->va_mtime      = src->va_mtime;
        dest->va_ctime      = src->va_ctime;
    }

    if (src->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) {
        dest->va_fs_space_avail = src->va_fs_space_avail;
        dest->va_fs_space_free  = src->va_fs_space_free;
        dest->va_fs_space_total = src->va_fs_space_total;
        dest->va_fs_space_used  = src->va_fs_space_used;
        dest->va_fs_files_total = src->va_fs_files_total;
        dest->va_fs_files_free  = src->va_fs_files_free;
        dest->va_fs_files_avail = src->va_fs_files_avail;
    }
} /* chimera_vfs_copy_attr */
