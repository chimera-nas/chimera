// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * In-memory durable/persistent SMB3 handle registry.
 *
 * This is the initial, memory-only implementation: a durable open survives the
 * teardown of its owning connection by remaining allocated and being indexed
 * here, keyed by its (globally unique) persistent id.  A reconnect within the
 * grace window re-homes the surviving open into the new tree; otherwise the
 * per-thread sweeper reaps it.
 *
 * Ownership / refcount invariant:
 *   - A durable open is registered live (parked == false) at CREATE grant and
 *     forgotten when it is finally destroyed (normal close or reap).
 *   - While live it sits in a tree's open_files[] hash with the usual refcount
 *     (1 == tree membership).  The registry holds only a weak pointer and never
 *     dereferences a non-parked entry's open_file.
 *   - At disconnect the open is removed from its tree but NOT freed; the
 *     registry's reference becomes its sole owner (refcnt stays 1, PARKED set).
 *   - A reconnect re-homes it (PARKED cleared); the sweeper frees it.
 *
 * Locking: the registry lock is a leaf taken either alone (register / claim /
 * sweep-collect) or nested INSIDE a tree bucket lock (park / forget).  No path
 * acquires a bucket lock while holding the registry lock, and the heavyweight
 * VFS teardown in the sweeper runs after the registry lock is dropped, so the
 * global order bucket -> registry -> vfs_state holds with no inversion.
 */

#include "smb_internal.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

struct chimera_smb_durable_recover_ctx {
    struct chimera_server_smb_shared *shared;
};

SYMBOL_EXPORT void
chimera_smb_durable_table_init(struct chimera_smb_durable_table *table)
{
    pthread_mutex_init(&table->lock, NULL);
    table->by_pid = NULL;
} /* chimera_smb_durable_table_init */

SYMBOL_EXPORT void
chimera_smb_durable_table_destroy(struct chimera_smb_durable_table *table)
{
    struct chimera_smb_durable_entry *entry, *tmp;

    /* By the time the server is destroyed all sessions are gone; any entries
     * still here are parked opens that outlived their grace window without a
     * sweep, or were never reaped.  Free the bookkeeping; the open_file objects
     * themselves belong to thread free-lists that are torn down separately. */
    entry = table->by_pid;
    HASH_CLEAR(hh, table->by_pid);
    while (entry) {
        tmp = entry->hh.next;
        free(entry);
        entry = tmp;
    }

    pthread_mutex_destroy(&table->lock);
} /* chimera_smb_durable_table_destroy */

SYMBOL_EXPORT void
chimera_smb_durable_register(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open_file,
    uint64_t                          session_id,
    uint32_t                          owner_uid,
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              persistent)
{
    struct chimera_smb_durable_entry *entry = calloc(1, sizeof(*entry));

    entry->persistent_id = open_file->file_id.pid;
    entry->session_id    = session_id;
    entry->owner_uid     = owner_uid;
    entry->open_file     = open_file;
    entry->parked        = false;
    entry->persistent    = persistent;
    entry->cold          = false;
    memcpy(entry->create_guid, open_file->create_guid, sizeof(entry->create_guid));
    memcpy(entry->client_guid, client_guid, sizeof(entry->client_guid));

    if (name_len > sizeof(entry->name)) {
        name_len = sizeof(entry->name);
    }
    entry->name_len = name_len;
    memcpy(entry->name, name, name_len);

    pthread_mutex_lock(&shared->durable.lock);
    HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_register */

/* Insert a cold entry recovered from a backend record at startup.  open_file is
 * NULL until a reconnect re-opens the file.  Skips duplicates (idempotent). */
SYMBOL_EXPORT void
chimera_smb_durable_recover_entry(
    struct chimera_server_smb_shared        *shared,
    const struct chimera_smb_durable_record *record)
{
    struct chimera_smb_durable_entry *entry, *existing;
    uint64_t                          pid = record->persistent_id;
    uint32_t                          name_len;

    entry = calloc(1, sizeof(*entry));

    entry->persistent_id = record->persistent_id;
    entry->session_id    = record->session_id;
    entry->open_file     = NULL;
    entry->parked        = true;
    entry->persistent    = true;
    entry->cold          = true;
    memcpy(entry->create_guid, record->create_guid, sizeof(entry->create_guid));
    memcpy(entry->client_guid, record->client_guid, sizeof(entry->client_guid));

    name_len = record->name_len;
    if (name_len > sizeof(entry->name)) {
        name_len = sizeof(entry->name);
    }
    entry->name_len = name_len;
    memcpy(entry->name, record->name, name_len);

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &pid, sizeof(pid), existing);
    if (existing) {
        pthread_mutex_unlock(&shared->durable.lock);
        free(entry);
        return;
    }
    HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);

    /* Keep the id allocator ahead of every recovered persistent id so a fresh
     * open can never collide with a not-yet-reclaimed one. */
    if (atomic_load(&shared->next_persistent_id) <= pid) {
        atomic_store(&shared->next_persistent_id, pid + 1);
    }
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_recover_entry */

SYMBOL_EXPORT void
chimera_smb_durable_forget(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id)
{
    struct chimera_smb_durable_entry *entry;

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);
    if (entry) {
        HASH_DELETE(hh, shared->durable.by_pid, entry);
    }
    pthread_mutex_unlock(&shared->durable.lock);

    if (entry) {
        free(entry);
    }
} /* chimera_smb_durable_forget */

/* Fire-and-forget context for a delete-on-close unlink issued while a parked
 * durable open is torn down.  Unlike the CLOSE path there is no request to
 * complete, so the doc_info is copied here and freed in the final callback. */
struct chimera_smb_durable_doc {
    struct chimera_vfs_thread      *vfs_thread;
    struct chimera_vfs_doc_info     doc_info;
    struct chimera_vfs_open_handle *parent_handle;
    /* FH of the file this delete-on-close targets, so the async remove only
     * unlinks the name while it still resolves to THIS object -- not a fresh
     * file another opener created with the same name in the meantime (the
     * grace-timer reap of a DELETE_ON_CLOSE handle can land arbitrarily late
     * under load). */
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    int                             file_fh_len;
};

static void
chimera_smb_durable_doc_remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_durable_doc *ctx = private_data;

    if (error_code) {
        chimera_smb_debug("durable delete-on-close: remove_at failed for '%.*s' (error %d)",
                          ctx->doc_info.name_len, ctx->doc_info.name, error_code);
    }
    chimera_vfs_release(ctx->vfs_thread, ctx->parent_handle);
    chimera_vfs_close(ctx->vfs_thread, ctx->doc_info.close_module,
                      ctx->doc_info.close_private, ctx->doc_info.close_hash, NULL, NULL);
    free(ctx);
} /* chimera_smb_durable_doc_remove_cb */

static void
chimera_smb_durable_doc_open_parent_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_durable_doc *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_debug("durable delete-on-close: open parent failed for '%.*s' (error %d)",
                          ctx->doc_info.name_len, ctx->doc_info.name, error_code);
        chimera_vfs_close(ctx->vfs_thread, ctx->doc_info.close_module,
                          ctx->doc_info.close_private, ctx->doc_info.close_hash, NULL, NULL);
        free(ctx);
        return;
    }
    ctx->parent_handle = oh;
    chimera_vfs_remove_at_match_fh(ctx->vfs_thread, &ctx->doc_info.cred, oh,
                                   ctx->doc_info.name, ctx->doc_info.name_len,
                                   ctx->file_fh, ctx->file_fh_len, 0, 0,
                                   NULL, /* parent_lease_skip */
                                   chimera_smb_durable_doc_remove_cb, ctx);
} /* chimera_smb_durable_doc_open_parent_cb */

/*
 * Release a parked durable open's VFS handle honoring delete-on-close: if the
 * handle was delete-pending (a DELETE_ON_CLOSE durable handle whose last close
 * is this teardown), the file is unlinked asynchronously (no request needed).
 * Requires a live event-loop pump, so it must NOT be used on the shutdown drain.
 */
static void
chimera_smb_durable_release_handle(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_durable_doc *ctx;
    struct chimera_vfs_doc_info     doc_info;
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    int                             file_fh_len;
    int                             need_doc;

    if (!open_file->handle) {
        return;
    }

    /* Capture the target file's FH before release_doc clears the handle, so the
     * async unlink below is inode-scoped (see struct chimera_smb_durable_doc). */
    file_fh_len = open_file->handle->fh_len;
    memcpy(file_fh, open_file->handle->fh, file_fh_len);

    need_doc          = chimera_vfs_release_doc(thread->vfs_thread, open_file->handle, &doc_info);
    open_file->handle = NULL;

    if (!need_doc || doc_info.parent_fh_len == 0) {
        return;
    }

    ctx                = malloc(sizeof(*ctx));
    ctx->vfs_thread    = thread->vfs_thread;
    ctx->doc_info      = doc_info;
    ctx->parent_handle = NULL;
    ctx->file_fh_len   = file_fh_len;
    memcpy(ctx->file_fh, file_fh, file_fh_len);

    chimera_vfs_open_fh(thread->vfs_thread, &ctx->doc_info.cred,
                        ctx->doc_info.parent_fh, ctx->doc_info.parent_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_smb_durable_doc_open_parent_cb, ctx);
} /* chimera_smb_durable_release_handle */

/* Purge a parked (disconnected) *durable* open by persistent id when a new,
 * conflicting open arrives: MS-SMB2 has the disconnected handle yield.  Tears
 * down its leases / share reservation / byte-range locks and VFS handle (the
 * same teardown the grace-timer sweeper does).  Returns true iff a matching
 * parked entry was found and purged.
 *
 * Persistent handles are deliberately excluded: they outrank a conflicting
 * fresh open (a different client must reclaim via CreateGuid, not displace),
 * and their teardown also issues an async backend KV-record delete that cannot
 * complete while the event loop is blocked here in the CREATE dispatch. */
SYMBOL_EXPORT bool
chimera_smb_durable_purge_parked(
    struct chimera_server_smb_thread *thread,
    uint64_t                          persistent_id,
    bool                              include_persistent)
{
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);
    if (entry && entry->parked && (include_persistent || !entry->persistent) &&
        !entry->cold && entry->open_file) {
        HASH_DELETE(hh, shared->durable.by_pid, entry);
        open_file = entry->open_file;
        free(entry);
    }
    pthread_mutex_unlock(&shared->durable.lock);

    if (!open_file) {
        return false;
    }

    chimera_smb_open_file_drain_locks(thread, open_file);
    chimera_smb_durable_release_handle(thread, open_file);
    chimera_smb_open_file_free(thread, open_file);
    return true;
} /* chimera_smb_durable_purge_parked */

/* A conflicting CREATE found a still-live (not-yet-parked) durable open by its
 * persistent id and could not purge it.  Decide whether that holder is racing its
 * own disconnect and will yield (so the conflicting CREATE should retry rather
 * than deny: MS-SMB2 has the disconnected non-persistent handle yield, and a
 * short retry lets the park complete so the purge then succeeds), and if so how
 * confidently -- see enum chimera_smb_durable_yield.
 *
 * Only non-persistent, non-cold entries with a live open_file are candidates;
 * persistent handles do not yield and are excluded (NONE), as is an unknown id.
 * #839 covered only the post-disconnect stages (create_conn cleared / conn flagged
 * disconnecting); CONFIRMED here also covers the pre-notify bind-already-closing
 * stage AND the just-parked stage (purge_parked lost the park race by an instant),
 * and SPECULATIVE adds the even-earlier FIN-not-yet-read window: any candidate
 * durable holder with no disconnect signal yet gets a SHORT speculative retry,
 * because it must yield if its owner is in fact disconnecting, and merely incurs a
 * brief bounded delay before the same SHARING_VIOLATION if it is genuinely live. */
SYMBOL_EXPORT enum chimera_smb_durable_yield
chimera_smb_durable_conn_disconnecting(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id)
{
    struct chimera_smb_durable_entry *entry;
    enum chimera_smb_durable_yield    yield = CHIMERA_SMB_DURABLE_YIELD_NONE;

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);
    if (entry && !entry->persistent && !entry->cold && entry->open_file) {
        struct chimera_smb_conn *cc = entry->open_file->create_conn;

        /* The holder yields once its disconnect parks it.  Four observable stages
         * of that disconnect, earliest first:
         *
         *   1. PRE-NOTIFY: the peer's TCP close (read-side FIN) has been seen by
         *      libevpl, which has scheduled (deferred) the teardown -- but the
         *      server has not yet run EVPL_NOTIFY_DISCONNECTED for this conn at
         *      all, so create_conn is still set and disconnecting is still 0.
         *      The bind is already closing, detected via evpl_bind_is_closing().
         *   2. NOTIFY started: EVPL_NOTIFY_DISCONNECTED has run far enough to set
         *      conn->disconnecting, but the teardown has not parked the handle.
         *   3. conn_free done: create_conn has been cleared (cc == NULL).
         *   4. PARKED: the teardown has already parked the handle (entry->parked).
         *      A conflicting CREATE's chimera_smb_durable_purge_parked races the
         *      park: if it probed an instant before parked flipped to 1 it returns
         *      false, then re-checks here and finds the now-parked entry -- a sure
         *      yield, the very next retry's purge_parked will reap it.
         *
         * Any of the four is a CONFIRMED disconnect: the holder is on its way out
         * and will release its share reservation, so retry on the full budget. */
        if (entry->parked || (cc == NULL) || cc->disconnecting ||
            (cc->bind && evpl_bind_is_closing(cc->bind))) {
            yield = CHIMERA_SMB_DURABLE_YIELD_CONFIRMED;
        } else {
            /* No disconnect signal yet, but this is the even earlier window the
             * pre-notify retry alone still misses: the conflicting CREATE was
             * processed before the server's event loop has even read the holder's
             * already-sent FIN, so none of the three signals above are set yet.
             * The holder is a non-persistent durable open, which MS-SMB2 requires
             * to yield once its owner disconnects -- so this conflict can resolve
             * in exactly two ways: the holder is disconnecting (and within a couple
             * of ticks its FIN is read, a subsequent probe flips to CONFIRMED, and
             * it parks + yields), or it is genuinely live and staying (the conflict
             * is real).  Retry SPECULATIVELY on a SHORT budget: it covers the
             * FIN-read latency for the disconnecting case, and merely adds a brief
             * bounded delay before the same SHARING_VIOLATION for the genuinely-live
             * case -- never a wrong answer, and the live path is the much rarer one
             * (a second open against a still-connected durable handle). */
            yield = CHIMERA_SMB_DURABLE_YIELD_SPECULATIVE;
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);

    return yield;
} /* chimera_smb_durable_conn_disconnecting */

SYMBOL_EXPORT void
chimera_smb_durable_park(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_durable_entry *entry;
    uint64_t                          pid = open_file->file_id.pid;
    struct timespec                   now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &pid, sizeof(pid), entry);
    if (entry) {
        /* A resilient open is held for its resiliency timeout; a durable open
         * for its durable timeout.  When an open is both, keep it for the
         * longer of the two. */
        uint64_t timeout_ms = open_file->durable_timeout_ms;
        if (open_file->resilient && open_file->resilient_timeout_ms > timeout_ms) {
            timeout_ms = open_file->resilient_timeout_ms;
        }
        entry->parked            = true;
        entry->deadline          = now;
        entry->deadline.tv_sec  += timeout_ms / 1000;
        entry->deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (entry->deadline.tv_nsec >= 1000000000L) {
            entry->deadline.tv_sec  += 1;
            entry->deadline.tv_nsec -= 1000000000L;
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);

    /* Mark the caching grant's lease AND the share reservation parked so the
     * vfs_state conflict matrix treats this disconnected holder as
     * courtesy-held: a compatible new open coexists (keep) and a write-cache
     * holder is evicted by the caching-acquire path (purge).  The share-resv
     * flag stops the sole-opener rule from capping a new opener's lease to R on
     * account of a disconnected holder.  Cleared on reconnect. */
    if (open_file->grant) {
        open_file->grant->lease.parked = 1;
    }
    if (open_file->share_lease_inserted) {
        open_file->share_lease.parked = 1;
    }
} /* chimera_smb_durable_park */

SYMBOL_EXPORT struct chimera_smb_open_file *
chimera_smb_durable_claim(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id,
    const uint8_t                    *create_guid,
    const uint8_t                    *client_guid,
    uint32_t                          owner_uid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              has_lease_ctx,
    const uint8_t                    *lease_key,
    bool                             *r_cold,
    bool                             *r_retry,
    uint32_t                         *status)
{
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;
    bool                              had_lease;

    *r_cold  = false;
    *r_retry = false;

    pthread_mutex_lock(&shared->durable.lock);

    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);

    /* Did the surviving open hold a lease (vs a plain oplock / nothing)?  The
     * lease-key / lease-context reconnect checks below only apply to leases.
     * Cold (recovered) entries have no live open, so treat as no lease. */
    had_lease = entry && entry->open_file &&
        entry->open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE;

    if (!entry) {
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (!entry->parked) {
        /* The handle is flagged live -- either genuinely still open on another
         * channel (a second reconnect must not steal it), OR its previous
         * connection has dropped but that disconnect has not been processed yet
         * (a cross-connection race: the reconnect's CREATE reached this thread
         * before the old connection's teardown parked the handle).  We cannot
         * tell the two apart here, so ask the caller to retry briefly: once the
         * disconnect is processed the entry becomes parked and the retry
         * reclaims it; a genuinely-live handle never parks and the retry budget
         * lapses into OBJECT_NAME_NOT_FOUND. */
        *r_retry = true;
        *status  = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (entry->open_file &&
               (entry->open_file->flags & CHIMERA_SMB_OPEN_FILE_YIELDED)) {
        /* While disconnected, this open's write-caching oplock/lease was
         * forcibly revoked to admit a conflicting open — it yielded (MS-SMB2
         * 3.3.4.6/3.3.4.7 close a disconnected open whose batch oplock /
         * write-caching lease breaks), so the reconnect must not find it.
         * The grace-timer sweep reaps the carcass. */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (create_guid && memcmp(entry->create_guid, create_guid, 16) != 0) {
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if ((had_lease || entry->persistent) && client_guid &&
               memcmp(entry->client_guid, client_guid, 16) != 0) {
        /* Reconnect from a different client.  MS-SMB2 3.3.5.9.7 binds the
         * ClientGuid check to leased opens: when the surviving open holds a
         * lease, a ClientGuid mismatch fails with STATUS_OBJECT_NAME_NOT_FOUND
         * (the handle is not visible to this client).  An oplock-only *durable*
         * handle has no such binding — it may be reconnected from a new
         * transport with a different ClientGuid (identity is the persistent id,
         * plus the create_guid for v2).  Persistent handles keep the check
         * regardless (their reclaim is governed by create_guid + owner). */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (had_lease && !has_lease_ctx) {
        /* 3.3.5.9.7: open holds a lease but the reconnect omitted the lease
         * create context — OBJECT_NAME_NOT_FOUND. */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (had_lease && has_lease_ctx && lease_key &&
               memcmp(entry->open_file->lease_key, lease_key, 16) != 0) {
        /* 3.3.5.9.7: lease key in the reconnect does not match the open's. */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (had_lease && name_len > 0 &&
               (entry->name_len != name_len ||
                memcmp(entry->name, name, name_len) != 0)) {
        /* A leased reconnect that names a (non-empty) file other than the one
         * the handle was opened on is malformed (MS-SMB2 3.3.5.9.7).  An empty
         * name -- the usual durable-reconnect form, always so for v2 -- and a
         * non-lease (oplock) reconnect both ignore the name entirely: the
         * handle's identity is its persistent id, plus the create_guid for v2. */
        *status = SMB2_STATUS_INVALID_PARAMETER;
    } else if (!entry->cold && entry->owner_uid != owner_uid) {
        /* MS-SMB2 3.3.5.9.7 step 9: the reconnecting session's user must be the
         * same user that owns the durable/resilient open (Open.DurableOwner).  A
         * reclaim by a different user is denied with STATUS_ACCESS_DENIED.  Cold
         * (server-restart-recovered) entries carry no in-memory owner, so the
         * check applies only to warm handles. */
        *status = SMB2_STATUS_ACCESS_DENIED;
    } else if (entry->cold) {
        /* Recovered-after-restart entry: there is no live open to re-home.
         * Remove it and tell the caller to re-open the file (cold reclaim);
         * the reopen path re-registers a fresh warm entry. */
        HASH_DELETE(hh, shared->durable.by_pid, entry);
        free(entry);
        *r_cold = true;
        *status = SMB2_STATUS_SUCCESS;
    } else {
        /* Warm reclaim: flip out of the parked state so the sweeper leaves it
        * alone, and hand the surviving open back to the caller to re-home. */
        entry->parked = false;
        open_file     = entry->open_file;
        *status = SMB2_STATUS_SUCCESS;
    }

    pthread_mutex_unlock(&shared->durable.lock);

    return open_file;
} /* chimera_smb_durable_claim */

/*
 * DH2Q create_guid replay lookup (MS-SMB2 3.3.5.9.10).
 *
 * A CREATE that carries a DurableHandleRequestV2 (DH2Q) plus the
 * SMB2_FLAGS_REPLAY_OPERATION header flag is a replay of the original durable
 * create whose reply the client lost.  The durable identity is the create_guid
 * (unlike a DH2C/DHnC reconnect, which is keyed by persistent id), so we scan
 * the (small) registry for a matching entry and classify the outcome:
 *
 *   CHIMERA_SMB_GUID_REPLAY_RECLAIM  - a *parked* (disconnected) durable open of
 *       the same client matches: warm-reclaim it (clear parked) and return the
 *       surviving open via *r_open_file for the caller to re-home, reporting the
 *       ORIGINAL create_action.  (durable-reconnect-replay1, replay-twice-durable)
 *
 *   CHIMERA_SMB_GUID_REPLAY_DUPLICATE - a *live* durable open with this
 *       create_guid exists on a DIFFERENT connection of the same client: the
 *       replay collides with the still-open original and is rejected with
 *       STATUS_DUPLICATE_OBJECTID.  (durable-reconnect-replay2)
 *
 *   CHIMERA_SMB_GUID_REPLAY_DENIED   - a *parked* durable open matches a replay
 *       but the reclaim fails MS-SMB2 3.3.5.9.10 replay verification (the
 *       surviving open holds a lease whose LeaseKey differs from the one named in
 *       the replayed create, or the requested handle type oplock-vs-lease differs):
 *       rejected with STATUS_ACCESS_DENIED.  (SMB2Model ReplayCreateDurableHandleV2
 *       *PersistentTestCaseS54 / S1575.)
 *
 *   CHIMERA_SMB_GUID_REPLAY_NONE     - no registry entry carries this guid (or
 *       the only live match is the requesting connection's own open, handled by
 *       the live open_files scan): the caller proceeds with a fresh create.
 */
SYMBOL_EXPORT enum chimera_smb_guid_replay_result
chimera_smb_durable_claim_by_guid(
    struct chimera_server_smb_shared *shared,
    const uint8_t                    *create_guid,
    const uint8_t                    *client_guid,
    const struct chimera_smb_conn    *req_conn,
    int                               is_replay,
    bool                              has_lease_ctx,
    const uint8_t                    *lease_key,
    struct chimera_smb_open_file    **r_open_file)
{
    struct chimera_smb_durable_entry   *entry, *tmp;
    enum chimera_smb_guid_replay_result result = CHIMERA_SMB_GUID_REPLAY_NONE;

    *r_open_file = NULL;

    pthread_mutex_lock(&shared->durable.lock);

    HASH_ITER(hh, shared->durable.by_pid, entry, tmp)
    {
        if (entry->cold || !entry->open_file) {
            continue;
        }
        if (memcmp(entry->create_guid, create_guid, 16) != 0) {
            continue;
        }
        if (client_guid &&
            memcmp(entry->client_guid, client_guid, 16) != 0) {
            continue;
        }

        if (entry->parked) {
            if (entry->open_file->flags & CHIMERA_SMB_OPEN_FILE_YIELDED) {
                /* The disconnected open's caching state was revoked to admit a
                 * conflicting open; it is no longer reclaimable. */
                continue;
            }
            if (!is_replay) {
                /* A non-replay create whose create_guid matches a still-durable
                 * (parked) open of this client collides with it. */
                result = CHIMERA_SMB_GUID_REPLAY_DUPLICATE;
                break;
            }
            /* MS-SMB2 3.3.5.9.10 replay verification: a found Open is reclaimed
             * only if the replayed create matches its handle type and (when
             * leased) its LeaseKey -- otherwise STATUS_ACCESS_DENIED.  Mirrors
             * the live-open replay check in chimera_smb_create_guid_replay; the
             * DH2C/DHnC reconnect path enforces the same in chimera_smb_durable_claim. */
            {
                bool open_is_lease = entry->open_file->oplock_level ==
                    SMB2_OPLOCK_LEVEL_LEASE;

                if (has_lease_ctx != open_is_lease ||
                    (has_lease_ctx && lease_key &&
                     memcmp(entry->open_file->lease_key, lease_key, 16) != 0)) {
                    result = CHIMERA_SMB_GUID_REPLAY_DENIED;
                    break;
                }
            }
            entry->parked = false;
            *r_open_file  = entry->open_file;
            result        = CHIMERA_SMB_GUID_REPLAY_RECLAIM;
            break;
        }

        /* Live durable open with this create_guid (the caller's live open_files
         * scan already returned any replay-eligible same-tree open).
         *   - A different connection's open: any create here collides
         *     (DUPLICATE_OBJECTID): durable-reconnect-replay2.
         *   - This connection's own open reached here only because it is NOT
         *     replay-eligible (a non-replay op has used it) or the create is not
         *     a replay.  A non-replay create collides (DUPLICATE_OBJECTID,
         *     replay6 @5310); an ineligible replay is "ignored" and falls
         *     through to a fresh open (replay-twice-durable, replay6 @5288). */
        if (entry->open_file->create_conn != req_conn || !is_replay) {
            result = CHIMERA_SMB_GUID_REPLAY_DUPLICATE;
            break;
        }
        /* else: ineligible replay on our own live open -> NONE (fresh open). */
    }

    pthread_mutex_unlock(&shared->durable.lock);

    return result;
} /* chimera_smb_durable_claim_by_guid */

void
chimera_smb_durable_sweep(struct chimera_server_smb_thread *thread)
{
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_durable_entry *entry, *tmp;
    struct chimera_smb_durable_entry *expired = NULL;  /* singly-linked via hh.next reuse */
    struct timespec                   now;

    /* Collect expired parked entries under the registry lock (removing them
     * from the hash so a peer thread's sweep cannot also claim them), then do
     * the heavyweight teardown after the lock is dropped. */
    clock_gettime(CLOCK_MONOTONIC, &now);

    pthread_mutex_lock(&shared->durable.lock);

    HASH_ITER(hh, shared->durable.by_pid, entry, tmp)
    {
        if (!entry->parked) {
            continue;
        }
        /* Persistent handles do not expire on the durable grace timer (they
         * live until explicit close or admin action), and cold entries have no
         * live open to tear down — skip both. */
        if (entry->persistent || entry->cold || !entry->open_file) {
            continue;
        }
        if (chimera_timespec_cmp(&now, &entry->deadline) < 0) {
            continue;
        }
        HASH_DELETE(hh, shared->durable.by_pid, entry);
        entry->reap_next = expired;
        expired          = entry;
    }

    pthread_mutex_unlock(&shared->durable.lock);

    while (expired) {
        struct chimera_smb_open_file *open_file = expired->open_file;
        entry   = expired;
        expired = expired->reap_next;

        chimera_smb_debug("durable: reaping expired handle pid=%lx '%.*s'",
                          open_file->file_id.pid, open_file->name_len, open_file->name);

        chimera_smb_open_file_drain_locks(thread, open_file);
        /* Grace-timer reap honors delete-on-close (a DELETE_ON_CLOSE durable
         * handle whose last close is this expiry unlinks the file). */
        chimera_smb_durable_release_handle(thread, open_file);
        chimera_smb_open_file_free(thread, open_file);

        free(entry);
    }
} /* chimera_smb_durable_sweep */

/* Release every registry entry's live open at thread shutdown.  A parked
 * durable/persistent handle keeps its VFS open handle referenced
 * indefinitely; without this the VFS close thread can never reach a
 * quiescent (zero open handles) state and chimera_vfs_destroy hangs.
 *
 * Runs from the SMB thread-destroy path, which still has a live vfs_thread
 * (protocols are destroyed before the VFS, precisely so they can release
 * their open handles).  By that point all connections are gone, so any
 * remaining entry with a live open_file is orphaned state to reclaim --
 * persistent handles included (their in-memory open must be released even
 * though the on-disk record is intentionally left for restart recovery).
 * Cold entries (open_file == NULL) only hold bookkeeping, freed by
 * chimera_smb_durable_table_destroy.
 *
 * The open's share reservation / byte-range locks are deliberately NOT drained
 * via chimera_smb_open_file_drain_locks: that releases the lease with a pump,
 * and pumping a pending acquire queued behind it (e.g. a blocking lock whose
 * connection already dropped) runs a completion callback that allocates a reply
 * iovec on this teardown thread for a dead connection, tripping the cross-thread
 * iovec guard.  At shutdown those waiters have no live connection to answer, so
 * dropping them is correct.  The embedded share/range leases are reclaimed
 * wholesale when chimera_vfs_state_destroy frees the per-file state (it never
 * walks the lease lists).
 *
 * The caching grant (oplock / SMB2 lease), however, is a standalone heap object
 * the SMB layer owns -- vfs_state_destroy frees the per-file state but never the
 * grant -- so a parked handle's grant must be released here explicitly or it
 * leaks.  Release it with pump=false to free the grant memory (and unlink its
 * lease) without waking a waiter on a dead connection. */
SYMBOL_EXPORT void
chimera_smb_durable_drain_all(struct chimera_server_smb_thread *thread)
{
    struct chimera_server_smb_shared *shared    = thread->shared;
    struct chimera_vfs_state         *vfs_state =
        thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_durable_entry *entry, *tmp;
    struct chimera_smb_durable_entry *reap = NULL;

    pthread_mutex_lock(&shared->durable.lock);

    HASH_ITER(hh, shared->durable.by_pid, entry, tmp)
    {
        if (!entry->open_file) {
            continue;
        }
        HASH_DELETE(hh, shared->durable.by_pid, entry);
        entry->reap_next = reap;
        reap             = entry;
    }

    pthread_mutex_unlock(&shared->durable.lock);

    while (reap) {
        struct chimera_smb_open_file *open_file = reap->open_file;
        entry = reap;
        reap  = reap->reap_next;

        if (open_file->handle) {
            chimera_vfs_release(thread->vfs_thread, open_file->handle);
            open_file->handle = NULL;
        }
        /* Release the standalone caching grant (vfs_state_destroy won't); no
         * pump -- there is no live connection left to answer a woken waiter. */
        if (open_file->grant) {
            chimera_smb_grant_remove_member(open_file->grant, open_file);
            chimera_vfs_caching_grant_release(vfs_state, open_file->grant,
                                              false /*pump*/);
            open_file->grant                  = NULL;
            open_file->caching_lease_inserted = false;
        }
        if (open_file->caching_file_state) {
            chimera_vfs_state_put(vfs_state, open_file->caching_file_state);
            open_file->caching_file_state = NULL;
        }
        chimera_smb_open_file_free(thread, open_file);

        free(entry);
    }
} /* chimera_smb_durable_drain_all */

/* ------------------------------------------------------------------ *
*  Record (de)serialization for backend persistence                  *
* ------------------------------------------------------------------ */

static inline void
durable_put_le32(
    uint8_t  *b,
    uint32_t *p,
    uint32_t  v)
{
    b[*p]     = v & 0xff;
    b[*p + 1] = (v >> 8) & 0xff;
    b[*p + 2] = (v >> 16) & 0xff;
    b[*p + 3] = (v >> 24) & 0xff;
    *p       += 4;
} /* durable_put_le32 */

static inline void
durable_put_le64(
    uint8_t  *b,
    uint32_t *p,
    uint64_t  v)
{
    durable_put_le32(b, p, (uint32_t) v);
    durable_put_le32(b, p, (uint32_t) (v >> 32));
} /* durable_put_le64 */

SYMBOL_EXPORT uint32_t
chimera_smb_durable_key(
    uint8_t *buf,
    uint64_t persistent_id)
{
    uint32_t p = CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN;

    memcpy(buf, CHIMERA_SMB_DURABLE_KEY_PREFIX, CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN);
    durable_put_le64(buf, &p, persistent_id);
    return p;  /* == CHIMERA_SMB_DURABLE_KEY_LEN */
} /* chimera_smb_durable_key */

SYMBOL_EXPORT uint32_t
chimera_smb_durable_serialize(
    uint8_t                                 *buf,
    uint32_t                                 buf_size,
    const struct chimera_smb_durable_record *record)
{
    uint32_t p = 0;

    if (buf_size < CHIMERA_SMB_DURABLE_REC_HDR_LEN + record->name_len) {
        return 0;
    }

    durable_put_le32(buf, &p, CHIMERA_SMB_DURABLE_RECORD_MAGIC);
    durable_put_le64(buf, &p, record->persistent_id);
    memcpy(buf + p, record->create_guid, 16);
    p += 16;
    memcpy(buf + p, record->client_guid, 16);
    p += 16;
    durable_put_le64(buf, &p, record->session_id);
    durable_put_le32(buf, &p, record->durable_flags);
    durable_put_le64(buf, &p, record->durable_timeout_ms);
    durable_put_le32(buf, &p, record->desired_access);
    durable_put_le32(buf, &p, record->share_access);
    durable_put_le32(buf, &p, record->name_len);
    memcpy(buf + p, record->name, record->name_len);
    p += record->name_len;

    return p;
} /* chimera_smb_durable_serialize */

SYMBOL_EXPORT int
chimera_smb_durable_deserialize(
    const uint8_t                     *buf,
    uint32_t                           buf_len,
    struct chimera_smb_durable_record *record)
{
    uint32_t p = 4;

    if (buf_len < CHIMERA_SMB_DURABLE_REC_HDR_LEN ||
        smb_wire_le32(buf) != CHIMERA_SMB_DURABLE_RECORD_MAGIC) {
        return -1;
    }

    record->persistent_id = smb_wire_le64(buf + p);
    p                    += 8;
    memcpy(record->create_guid, buf + p, 16);
    p += 16;
    memcpy(record->client_guid, buf + p, 16);
    p                         += 16;
    record->session_id         = smb_wire_le64(buf + p);
    p                         += 8;
    record->durable_flags      = smb_wire_le32(buf + p);
    p                         += 4;
    record->durable_timeout_ms = smb_wire_le64(buf + p);
    p                         += 8;
    record->desired_access     = smb_wire_le32(buf + p);
    p                         += 4;
    record->share_access       = smb_wire_le32(buf + p);
    p                         += 4;
    record->name_len           = smb_wire_le32(buf + p);
    p                         += 4;

    if (record->name_len > SMB_FILENAME_MAX || p + record->name_len > buf_len) {
        return -1;
    }
    memcpy(record->name, buf + p, record->name_len);

    return 0;
} /* chimera_smb_durable_deserialize */

/* ------------------------------------------------------------------ *
*  Startup recovery: rebuild cold entries from a share's backend      *
* ------------------------------------------------------------------ */

static int
chimera_smb_durable_recover_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct chimera_smb_durable_recover_ctx *ctx = private_data;
    struct chimera_smb_durable_record       record;

    /* Keys are returned in order; once we walk past the "smbdh" prefix there
     * are no more handle records, so stop the scan. */
    if (key_len < CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN ||
        memcmp(key, CHIMERA_SMB_DURABLE_KEY_PREFIX, CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN) != 0) {
        return 1;
    }

    if (chimera_smb_durable_deserialize(value, value_len, &record) == 0) {
        chimera_smb_durable_recover_entry(ctx->shared, &record);
    }

    return 0;
} /* chimera_smb_durable_recover_cb */

static void
chimera_smb_durable_recover_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    free(private_data);
} /* chimera_smb_durable_recover_complete */

/* Best-effort, idempotent scan of a share's backend for persisted handle
 * records, rebuilding cold registry entries.  `fh` is any handle on the share's
 * backend (the share root); the search routes to that backend.  Runs on an SMB
 * thread (has a vfs_thread).  A reconnect that races this scan simply falls
 * back to a fresh open. */
SYMBOL_EXPORT void
chimera_smb_durable_recover_share(
    struct chimera_server_smb_thread *thread,
    const void                       *fh,
    int                               fh_len)
{
    struct chimera_smb_durable_recover_ctx *ctx = calloc(1, sizeof(*ctx));

    ctx->shared = thread->shared;

    chimera_vfs_search_keys_at(thread->vfs_thread, NULL, fh, fh_len,
                               CHIMERA_SMB_DURABLE_KEY_PREFIX,
                               CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN,
                               NULL, 0, 0,
                               chimera_smb_durable_recover_cb,
                               chimera_smb_durable_recover_complete,
                               ctx);
} /* chimera_smb_durable_recover_share */
