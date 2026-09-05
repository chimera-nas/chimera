// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

SYMBOL_EXPORT uint64_t
chimera_vfs_compound_alloc_ts(struct chimera_vfs_thread *thread)
{
    /* Globally-unique compound priority without a shared atomic.  The high
     * bits are a TSC-anchored, per-thread strictly-increasing counter (age order
     * -> a longer-lived compound outranks a newcomer, so WFG victim selection -- abort
     * the highest ts -- is starvation-free); the low CHIMERA_VFS_COMPOUND_THREAD_BITS
     * carry this thread's dense id so two threads never collide.  Shifting the
     * TSC down by the thread bits first means (hi << bits) can never overflow,
     * regardless of the raw counter magnitude; the per-thread bump guarantees
     * uniqueness for two compounds that land in the same TSC tick.  compound_ts_hi is
     * seeded to 1 at thread init so the returned ts is never 0 (which is
     * reserved for autocommit compounds). */
    uint64_t hi = chimera_vfs_now_ticks() >> CHIMERA_VFS_COMPOUND_THREAD_BITS;

    if (hi <= thread->compound_ts_hi) {
        hi = thread->compound_ts_hi + 1;
    }
    thread->compound_ts_hi = hi;

    return (hi << CHIMERA_VFS_COMPOUND_THREAD_BITS) | thread->compound_thread_id;
} /* chimera_vfs_compound_alloc_ts */

/* Fire-and-forget completion for the backend begin op: there is no caller
* waiting on it (the compound handle was returned synchronously), so just
* retire the request.  Any backend setup ran in the begin handler on the
* owning thread, ahead of the first enlisted op by dispatch FIFO order. */
static void
chimera_vfs_compound_begin_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    chimera_vfs_complete(request);
    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_compound_begin_complete */

SYMBOL_EXPORT void
chimera_vfs_compound_dispatch_begin(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_compound   *compound,
    const void                    *fh,
    int                            fhlen)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_common(thread, cred, compound->module,
                                               compound->mount_private,
                                               fh, fhlen, compound->route_hash,
                                               CHIMERA_VFS_CAP_COMPOUND);

    /* Both callers (the eager-bind path below and the lazy-bind arm of
     * chimera_vfs_dispatch) stamp a non-NULL module that advertises
     * CAP_COMPOUND before calling, so the allocation cannot fail. */
    chimera_vfs_abort_if(CHIMERA_VFS_IS_ERR(request),
                         "compound begin allocation failed for a bound compound");

    request->opcode             = CHIMERA_VFS_OP_COMPOUND_BEGIN;
    request->complete           = chimera_vfs_compound_begin_complete;
    request->compound           = compound;
    request->proto_private_data = NULL;

    /* Routes by compound->route_hash (request->compound is set and the begin
     * opcode is exempt from the enlistment guard), so it queues ahead of
     * every enlisted op on the compound's one owning thread. */
    chimera_vfs_dispatch(request);
} /* chimera_vfs_compound_dispatch_begin */

/* Acquire a compound blob from the thread pool.  Blobs are fixed-size
 * (vfs->max_compound_size: the largest CAP_COMPOUND module's compound_size,
 * floored at the bare core header) so any blob can bind to any capable
 * module.  Zeroed on every acquisition: backend begin handlers assume zeroed
 * per-compound state (historically the calloc at begin provided it). */
static struct chimera_vfs_compound *
chimera_vfs_compound_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_compound *compound;
    uint32_t                     size = thread->vfs->max_compound_size;

    if (thread->free_compounds) {
        compound = thread->free_compounds;
        LL_DELETE(thread->free_compounds, compound);
        memset(compound, 0, size);
    } else {
        compound = calloc(1, size);
    }

    return compound;
} /* chimera_vfs_compound_alloc */

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_vfs_compound_begin(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *hint_fh,
    int                            hint_fhlen,
    enum chimera_vfs_compound_mode mode,
    uint64_t                       ts,
    uint32_t                       flags)
{
    struct chimera_vfs_module   *module;
    struct chimera_vfs_compound *compound;
    void                        *mount_private = NULL;

    compound = chimera_vfs_compound_alloc(thread);

    compound->ts    = ts;
    compound->mode  = mode;
    compound->flags = flags;
    /* module stays NULL: UNBOUND.  An unbound compound binds lazily in
     * chimera_vfs_dispatch at the first enlisted op on a compound-capable
     * mount, or ends without ever binding at zero backend cost. */

    /* Every live compound (bound or not) is registered so thread destroy can
     * flag a consumer that leaked one; end removes it. */
    DL_APPEND(thread->active_compounds, compound);

    if (hint_fh && !(flags & CHIMERA_VFS_COMPOUND_LOOSE)) {
        module = chimera_vfs_resolve_mount(thread, hint_fh, hint_fhlen, 1,
                                           &mount_private);

        if (module && (module->capabilities & CHIMERA_VFS_CAP_COMPOUND)) {
            /* EAGER BIND: the hint names a compound-capable mount, so stamp
             * the owner now and route by the hint's hash -- the same file
             * always lands on the same worker (read/write locality), and
             * distinct files spread across workers.  This preserves the NFS3
             * server's affinity behavior, where the RPC's decoded fh steers
             * the compound before the first op is built. */
            compound->module        = module;
            compound->mount_private = mount_private;
            compound->route_hash    = chimera_vfs_hash(hint_fh, hint_fhlen);

            /* Eager fire-and-forget begin op: lets the backend set up
             * per-compound state on the owning thread before the first
             * enlisted op arrives. */
            chimera_vfs_compound_dispatch_begin(thread, cred, compound,
                                                hint_fh, hint_fhlen);
        }
    }

    return compound;
} /* chimera_vfs_compound_begin */

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_vfs_compound_loose(struct chimera_vfs_thread *thread)
{
    /* The per-thread LOOSE singleton (allocated at thread init): never binds,
     * never joins the active registry (and so never trips the teardown leak
     * check), and end on it is a synchronous OK that recycles nothing.  Its
     * counters are meaningless -- it is shared by every caller on the thread
     * and nothing ever consults them. */
    return thread->loose_compound;
} /* chimera_vfs_compound_loose */
