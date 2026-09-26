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
#include "smb_procs.h"
#include "smb_durable_compound.h"
#include "common/misc.h"
#include "common/compound_retry.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

struct chimera_smb_durable_recover_ctx {
    struct chimera_server_smb_thread *thread;
    struct chimera_smb_durable_entry *prepared;
    struct chimera_vfs_compound      *next_page;
    struct evpl_timer                 continuation;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          fh_len;
};

/* A parked open outlives its original tree, so the registry independently
 * pins its share.  Share retirement is observed without taking shares_lock
 * under the registry lock; cleanup still runs on an SMB/VFS event thread. */
static bool
chimera_smb_durable_share_retired(const struct chimera_smb_durable_entry *entry)
{
    return entry->share && __atomic_load_n(&entry->share->retired, __ATOMIC_ACQUIRE);
} /* chimera_smb_durable_share_retired */

static void
chimera_smb_durable_entry_free(struct chimera_smb_durable_entry *entry)
{
    chimera_smb_share_release(entry->share);
    free(entry);
} /* chimera_smb_durable_entry_free */

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
        chimera_smb_durable_entry_free(entry);
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
    entry->share         = open_file->tree ? open_file->tree->share : NULL;
    if (entry->share) {
        __atomic_fetch_add(&entry->share->refcnt, 1, __ATOMIC_RELAXED);
    }
    entry->parked     = false;
    entry->persistent = persistent;
    entry->cold       = false;
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

struct chimera_smb_durable_entry *
chimera_smb_durable_registration_prepare(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open)
{
    struct chimera_smb_durable_entry *entry    = calloc(1, sizeof(*entry));
    struct chimera_smb_durable_entry *prepared = NULL;

    if (!entry) {
        return NULL;
    }
    entry->persistent_id = open->file_id.pid;
    entry->session_id    = request->session_handle->session->session_id;
    entry->owner_uid     = request->session_handle->session->cred.uid;
    entry->open_file     = open;
    entry->share         = open->tree ? open->tree->share : NULL;
    if (entry->share) {
        __atomic_fetch_add(&entry->share->refcnt, 1, __ATOMIC_RELAXED);
    }
    memcpy(entry->client_guid, request->compound->conn->client_guid, sizeof(entry->client_guid));
    memcpy(entry->create_guid, open->create_guid, sizeof(entry->create_guid));
    entry->name_len = open->name_len;
    if (entry->name_len > sizeof(entry->name)) {
        entry->name_len = sizeof(entry->name);
    }
    memcpy(entry->name, open->name, entry->name_len);
    HASH_ADD(hh, prepared, persistent_id, sizeof(entry->persistent_id), entry);
    return prepared;
} /* chimera_smb_durable_registration_prepare */

void
chimera_smb_durable_registration_discard(struct chimera_smb_durable_entry **prepared)
{
    struct chimera_smb_durable_entry *entry = *prepared;

    if (!entry) {
        return;
    }
    HASH_CLEAR(hh, *prepared);
    chimera_smb_durable_entry_free(entry);
} /* chimera_smb_durable_registration_discard */

void
chimera_smb_durable_registration_publish(
    struct chimera_server_smb_shared  *shared,
    struct chimera_smb_durable_entry **prepared)
{
    struct chimera_smb_durable_entry *entry = *prepared, *existing;

    if (!entry) {
        return;
    }
    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &entry->persistent_id,
              sizeof(entry->persistent_id), existing);
    if (!existing) {
        if (!shared->durable.by_pid) {
            shared->durable.by_pid = entry;
            *prepared              = NULL;
        } else {
            unsigned int noexpand = shared->durable.by_pid->hh.tbl->noexpand;
            HASH_CLEAR(hh, *prepared);
            shared->durable.by_pid->hh.tbl->noexpand = 1;
            HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);
            shared->durable.by_pid->hh.tbl->noexpand = noexpand;
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_registration_publish */

static struct chimera_smb_durable_entry *
chimera_smb_durable_prepare_record(const struct chimera_smb_durable_record *record)
{
    struct chimera_smb_durable_entry *entry;
    uint32_t                          name_len;

    if (record->persistent_id == UINT64_MAX) {
        return NULL;
    }
    entry = calloc(1, sizeof(*entry));
    if (!entry) {
        return NULL;
    }

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
    return entry;
} /* chimera_smb_durable_prepare_record */

static void
chimera_smb_durable_advance_pid(
    struct chimera_server_smb_shared *shared,
    uint64_t                          pid)
{
    uint64_t next = atomic_load(&shared->next_persistent_id);

    /* CREATE allocates without durable.lock. A plain load/store here can move
     * the allocator backwards over a concurrent allocation. */
    while (next <= pid && !atomic_compare_exchange_weak(
               &shared->next_persistent_id, &next, pid + 1)) {
    }
} /* chimera_smb_durable_advance_pid */

/* Insert a cold entry recovered from a backend record at startup.  open_file is
 * NULL until a reconnect re-opens the file.  Skips duplicates (idempotent). */
SYMBOL_EXPORT void
chimera_smb_durable_recover_entry(
    struct chimera_server_smb_shared        *shared,
    const struct chimera_smb_durable_record *record)
{
    struct chimera_smb_durable_entry *entry = chimera_smb_durable_prepare_record(record), *existing;
    uint64_t                          pid = record->persistent_id;

    if (!entry) {
        return;
    }

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &pid, sizeof(pid), existing);
    if (existing) {
        pthread_mutex_unlock(&shared->durable.lock);
        chimera_smb_durable_entry_free(entry);
        return;
    }
    HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);

    /* Keep the id allocator ahead of every recovered persistent id so a fresh
     * open can never collide with a not-yet-reclaimed one. */
    chimera_smb_durable_advance_pid(shared, pid);
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
        chimera_smb_durable_entry_free(entry);
    }
} /* chimera_smb_durable_forget */

/* A record can precede SMB access/share/type admission. Until gen_open_file
 * publishes an open, terminal failure owns only the request's saved record
 * identity. Retire that record with a typed compound before sending its error. */
struct smb_unpublished_record_cleanup {
    struct chimera_smb_request *request;
    uint32_t status;
};

static void
smb_unpublished_record_removed(struct chimera_vfs_compound *compound, void *private_data)
{
    struct smb_unpublished_record_cleanup *ctx = private_data;
    struct chimera_smb_request *request = ctx->request;
    uint32_t status = ctx->status;
    enum chimera_vfs_error error = chimera_vfs_compound_status(compound);
    if (error != CHIMERA_VFS_OK && error != CHIMERA_VFS_ENOENT) {
        chimera_smb_error("unpublished CREATE record deletion failed: pid=%lx error=%d",
            request->create.persist_pid, error);
    }
    chimera_vfs_compound_free(compound);
    free(ctx);
    chimera_smb_complete_request(request, status);
}

bool
chimera_smb_create_cleanup_failed_record(struct chimera_smb_request *request, uint32_t status)
{
    if (!request->create.persist_fh_len || request->create.r_open_file ||
        status == SMB2_STATUS_SUCCESS || status == SMB2_STATUS_PENDING) { return false; }
    /* The route is cleanup ownership, including a PUT with ambiguous failure;
     * it is independent of successful-record proof. Clear both before dispatch
     * to prevent recursive cleanup on the terminal continuation. */
    uint32_t fh_len = request->create.persist_fh_len;
    request->create.persist_fh_len = 0;
    request->create.persist_record_written = false;
    struct smb_unpublished_record_cleanup *ctx = calloc(1, sizeof(*ctx));
    struct chimera_vfs_compound *compound = ctx ?
        chimera_vfs_compound_alloc(request->compound->thread->vfs_thread,
            &request->session_handle->session->cred) : NULL;
    if (!compound) {
        free(ctx);
        chimera_smb_error("cannot delete unpublished CREATE record: pid=%lx",
            request->create.persist_pid);
        return false;
    }
    uint8_t key[CHIMERA_SMB_DURABLE_KEY_LEN];
    uint32_t key_len = chimera_smb_durable_key(key, request->create.persist_pid);
    ctx->request = request; ctx->status = status;
    chimera_vfs_compound_add_putfh(compound, request->create.persist_fh, fh_len);
    chimera_vfs_compound_add_delete_key_at(compound, key, key_len);
    chimera_frontend_compound_submit(compound, smb_unpublished_record_removed, ctx);
    return true;
}

struct smb_failed_create_cleanup {
    struct chimera_smb_request *request;
    struct chimera_smb_open_file *open;
    uint32_t status;
};

static void
smb_failed_create_record_removed(struct chimera_vfs_compound *compound, void *private_data)
{
    struct smb_failed_create_cleanup *ctx = private_data;
    struct chimera_smb_request *request = ctx->request;
    enum chimera_vfs_error error = chimera_vfs_compound_status(compound);
    if (error != CHIMERA_VFS_OK && error != CHIMERA_VFS_ENOENT) {
        chimera_smb_error("failed CREATE durable record deletion failed: pid=%lx error=%d",
            ctx->open->file_id.pid, error);
    }
    chimera_vfs_compound_free(compound);
    uint32_t status = ctx->status;
    chimera_smb_open_file_release(request, ctx->open);
    free(ctx);
    chimera_smb_complete_request(request, status);
}

void
chimera_smb_create_failed_open(struct chimera_smb_request *request, uint32_t status)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file *open = request->create.r_open_file;
    /* This path owns published-open cleanup, including its record. Do not
     * redispatch the unpublished-record hook after r_open_file is cleared. */
    request->create.persist_record_written = false;
    request->create.persist_fh_len = 0;
    struct smb_failed_create_cleanup *ctx = NULL;
    struct chimera_vfs_compound *compound = NULL;
    if (open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) {
        uint8_t fh[CHIMERA_VFS_FH_SIZE], key[CHIMERA_SMB_DURABLE_KEY_LEN];
        uint32_t fh_len = 0;
        struct chimera_vfs_file_state *file = open->share_file_state;
        if (file) { pthread_mutex_lock(&file->lock); }
        /* A concurrent logical CLOSE can already have detached its handle.
         * The open's retained file state still identifies the record's backend. */
        if (open->handle) {
            fh_len = open->handle->fh_len;
            memcpy(fh, open->handle->fh, fh_len);
        } else if (file) {
            fh_len = file->fh_len;
            memcpy(fh, file->fh, fh_len);
        }
        if (file) { pthread_mutex_unlock(&file->lock); }
        ctx = calloc(1, sizeof(*ctx));
        if (ctx && fh_len) {
            compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                &request->session_handle->session->cred);
        }
        if (compound) {
            ctx->request = request;
            ctx->open = open;
            ctx->status = status;
            uint32_t key_len = chimera_smb_durable_key(key, open->file_id.pid);
            chimera_vfs_compound_add_putfh(compound, fh, fh_len);
            chimera_vfs_compound_add_delete_key_at(compound, key, key_len);
        } else {
            free(ctx);
            ctx = NULL;
            /* The request is already failing. Still retire the unreachable
             * public open if resource exhaustion prevents record cleanup. */
            chimera_smb_error("cannot delete failed CREATE durable record: pid=%lx",
                open->file_id.pid);
        }
    }

    /* These are terminal effects of the failed, already accepted CREATE/EA
     * prefix. They are performed once, outside the retryable deletion attempt.
     * No reconnect may discover the failed open while its record is removed. */
    request->create.r_open_file = NULL;
    if (!memcmp(&request->compound->saved_file_id, &open->file_id, sizeof(open->file_id))) {
        request->compound->saved_file_id.pid = UINT64_MAX;
        request->compound->saved_file_id.vid = UINT64_MAX;
    }
    struct chimera_smb_tree *tree = open->tree;
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    struct chimera_smb_open_file *live;
    struct chimera_smb_durable_entry *entry;
    bool owner_ref = false;
    /* A peer PreviousSessionId can park the already published open while its
     * EA callback is pending. Parking transfers the tree's owning reference
     * to the registry. Detach both under the same bucket -> registry lock
     * order as parking, so exactly one owner is transferred to this cleanup.
     * Reclaim cannot rehome it while CREATE still holds its caller reference. */
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    pthread_mutex_lock(&thread->shared->durable.lock);
    HASH_FIND(hh, tree->open_files[bucket], &open->file_id, sizeof(open->file_id), live);
    if (live == open) {
        HASH_DELETE(hh, tree->open_files[bucket], open);
        owner_ref = true;
    }
    HASH_FIND(hh, thread->shared->durable.by_pid, &open->file_id.pid,
        sizeof(open->file_id.pid), entry);
    if (entry && entry->open_file == open) {
        if (entry->parked) { owner_ref = true; }
        HASH_DELETE(hh, thread->shared->durable.by_pid, entry);
    } else {
        entry = NULL;
    }
    open->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
    pthread_mutex_unlock(&thread->shared->durable.lock);
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
    if (entry) { chimera_smb_durable_entry_free(entry); }
    if (owner_ref) { chimera_smb_open_file_release(request, open); }
    if (compound) {
        chimera_frontend_compound_submit(compound, smb_failed_create_record_removed, ctx);
        return;
    }
    chimera_smb_open_file_release(request, open);
    chimera_smb_complete_request(request, status);
}

static void
chimera_smb_durable_release_handle(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_doc_info doc_info;

    if (open_file->handle && chimera_smb_release_doc(thread, open_file, &doc_info)) {
        chimera_smb_teardown_doc_unlink(thread, &doc_info);
    }
} /* chimera_smb_durable_release_handle */

static void
smb_durable_retired(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    void                             *private_data)
{
    struct chimera_smb_durable_entry *entry = private_data;

    chimera_smb_durable_release_handle(thread, open);
    chimera_smb_open_file_drain_locks(thread, open);
    atomic_store(&open->refcnt, 0);
    chimera_smb_open_file_free(thread, open);
    if (entry) {
        chimera_smb_durable_entry_free(entry);
    }
} /* smb_durable_retired */

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
        chimera_smb_durable_entry_free(entry);
    }
    pthread_mutex_unlock(&shared->durable.lock);

    if (!open_file) {
        return false;
    }

    open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
    chimera_smb_open_file_retire_async(thread, open_file, smb_durable_retired, NULL);
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
        /* Called while the original tree bucket is live/locked. Only an
         * actual parked entry owns this pin; an already-forgotten open must
         * not acquire an unpaired tree reference. */
        if (!open_file->durable_tree_pin) {
            open_file->tree->compound_pins++;
            open_file->durable_tree_pin = open_file->tree;
        }
        /* The disconnect-survival timeout: a resiliency request SETS the open's
         * timeout (MS-SMB2 3.3.5.15.9 -- it governs even when the open is also
         * durable/persistent, and may be SHORTER than the durable default), so
         * when the open is resilient the resiliency timeout wins outright; a
         * plain durable open uses its durable timeout. */
        uint64_t timeout_ms = open_file->resilient
            ? open_file->resilient_timeout_ms
            : open_file->durable_timeout_ms;

        entry->resilient = open_file->resilient;
        /* A pure persistent (CA) handle with no resiliency timeout holds the
         * file until an explicit reclaim/close: it never expires on the grace
         * timer.  A persistent+resilient handle DOES expire at the resiliency
         * timeout (test_resiliency_*_after_timeout). */
        entry->never_expires     = entry->persistent && !open_file->resilient;
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

    if (!entry) {
        return;
    }

    /* Park the caching grant's claim AND the share reservation so the claim
     * core treats this disconnected holder as courtesy-held (advertised H and
     * the H denial are masked while parked): a compatible new open coexists
     * (keep).  A durable-only write-cache holder is then evicted by the
     * caching-acquire path (purge, the MS-SMB2 yield); a resilient/persistent
     * holder within its timeout is spared by that path
     * (chimera_smb_durable_parked_hold) so a conflicting open caps its own
     * grant instead.  The parked share reservation stops the sole-opener rule
     * from capping a new opener's lease to R on account of a disconnected
     * holder.  Cleared on reconnect (chimera_smb_durable_rehome). */
    if (open_file->grant) {
        chimera_vfs_claim_park(&open_file->grant->claim, true);
    }
    if (open_file->share_lease_inserted) {
        chimera_vfs_claim_park(chimera_smb_share_claim(open_file), true);
    }
} /* chimera_smb_durable_park */

/* Classify how a parked durable holder (by persistent id) treats a conflicting
 * fresh open -- see enum chimera_smb_durable_hold.  A durable-only holder, an
 * expired holder (past its deadline, whether or not the sweep has reaped it
 * yet), an unknown id, or a live (not-parked) entry all YIELD (NONE).  A
 * resilient holder within its timeout is courtesy-held and CAPs the opener; a
 * persistent holder within its timeout BLOCKs the opener (FILE_NOT_AVAILABLE).
 * Persistent outranks resilient (a persistent+resilient holder BLOCKs). */
SYMBOL_EXPORT enum chimera_smb_durable_hold
chimera_smb_durable_parked_hold(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id)
{
    struct chimera_smb_durable_entry *entry;
    enum chimera_smb_durable_hold     hold = CHIMERA_SMB_DURABLE_HOLD_NONE;
    struct timespec                   now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);
    if (entry && entry->parked && !entry->cold && entry->open_file) {
        bool within = entry->never_expires ||
            chimera_timespec_cmp(&now, &entry->deadline) < 0;
        if (within) {
            if (entry->persistent) {
                hold = CHIMERA_SMB_DURABLE_HOLD_BLOCK;
            } else if (entry->resilient) {
                hold = CHIMERA_SMB_DURABLE_HOLD_CAP;
            }
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);

    return hold;
} /* chimera_smb_durable_parked_hold */

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
    bool                              reconnect_persistent,
    bool                             *r_cold,
    bool                             *r_retry,
    uint32_t                         *status)
{
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;
    bool                              had_lease;
    struct timespec                   now;

    *r_cold  = false;
    *r_retry = false;

    clock_gettime(CLOCK_MONOTONIC, &now);

    pthread_mutex_lock(&shared->durable.lock);

    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);

    /* Did the surviving open hold a lease (vs a plain oplock / nothing)?  The
     * lease-key / lease-context reconnect checks below only apply to leases.
     * Cold (recovered) entries have no live open, so treat as no lease. */
    had_lease = entry && entry->open_file &&
        entry->open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE;

    if (!entry || chimera_smb_durable_share_retired(entry)) {
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
    } else if (entry->open_file && atomic_load(&entry->open_file->refcnt) != 1) {
        /* Do not rehome bucket ownership or reset refcnt while old-channel
         * compounds still own the parked open. The existing reconnect timer
         * retries once those requests have drained. */
        *r_retry = true;
        *status  = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (!entry->never_expires && !entry->cold &&
               chimera_timespec_cmp(&now, &entry->deadline) >= 0) {
        /* Lazy expiry: the parked handle has outlived its disconnect-survival
         * deadline (durable timeout, or a shorter resiliency timeout).  Whether
         * or not the grace-timer sweep has physically reaped it yet, it is gone
         * for reclaim purposes -- OBJECT_NAME_NOT_FOUND.  The sweep frees the
         * carcass (test_resiliency_*_after_timeout). */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
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
    } else if (reconnect_persistent && !entry->persistent) {
        /* MS-SMB2 3.3.5.9.12: the reconnect set SMB2_DHANDLE_FLAG_PERSISTENT but
         * the surviving Open is not persistent (Open.IsPersistent == FALSE) ->
         * STATUS_INVALID_PARAMETER (pike durable test_durable_reconnect_v2_fails). */
        *status = SMB2_STATUS_INVALID_PARAMETER;
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
        chimera_smb_durable_entry_free(entry);
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
        if (entry->cold || !entry->open_file ||
            chimera_smb_durable_share_retired(entry)) {
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
            if (atomic_load(&entry->open_file->refcnt) != 1) {
                result = CHIMERA_SMB_GUID_REPLAY_RETRY;
                break;
            }
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

struct chimera_smb_durable_reap_ctx {
    struct chimera_server_smb_thread *thread;
    struct chimera_smb_durable_entry *entry;
};

static void
chimera_smb_durable_reap_finish(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_durable_entry *entry)
{
    struct chimera_smb_open_file *open_file = entry->open_file;

    open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
    chimera_smb_open_file_retire_async(thread, open_file, smb_durable_retired, entry);
} /* chimera_smb_durable_reap_finish */

static void
chimera_smb_durable_reap_record_removed(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_durable_reap_ctx *ctx        = private_data;
    enum chimera_vfs_error               error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    ctx->thread->maintenance_compounds--;
    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENOENT) {
        chimera_smb_error("retired share durable record deletion failed: pid=%lx error=%d",
                          ctx->entry->persistent_id, error_code);
    }
    chimera_smb_durable_reap_finish(ctx->thread, ctx->entry);
    free(ctx);
} /* chimera_smb_durable_reap_record_removed */

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
        /* A pure persistent (CA) handle does not expire on the grace timer (it
         * lives until explicit close or admin action), and cold entries have no
         * live open to tear down — skip both.  A persistent+resilient handle is
         * NOT never_expires: its resiliency timeout governs and it IS reaped. */
        if (entry->cold || !entry->open_file) {
            continue;
        }
        if (!chimera_smb_durable_share_retired(entry) &&
            (entry->never_expires || chimera_timespec_cmp(&now, &entry->deadline) < 0)) {
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

        if (chimera_smb_durable_share_retired(entry) &&
            (open_file->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) && open_file->handle) {
            uint8_t                              key[CHIMERA_SMB_DURABLE_KEY_LEN];
            uint32_t                             key_len = chimera_smb_durable_key(key, open_file->file_id.pid);
            struct chimera_smb_durable_reap_ctx *ctx     = malloc(sizeof(*ctx));

            if (!ctx) {
                chimera_smb_error("cannot allocate durable record deletion: pid=%lx", entry->persistent_id);
                chimera_smb_durable_reap_finish(thread, entry);
                continue;
            }
            ctx->thread = thread;
            ctx->entry  = entry;
            /* Keep the backing handle pinned until the durable record has
             * been deleted, so rmfs cannot race this administrative revoke. */
            struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
                thread->vfs_thread, chimera_vfs_get_server_cred());
            chimera_vfs_compound_add_putfh(compound, open_file->handle->fh, open_file->handle->fh_len);
            chimera_vfs_compound_add_delete_key_at(compound, key, key_len);
            thread->maintenance_compounds++;
            chimera_frontend_compound_submit(compound, chimera_smb_durable_reap_record_removed, ctx);
            continue;
        }
        /* Grace expiry and share retirement both honor delete-on-close. */
        chimera_smb_durable_reap_finish(thread, entry);
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
 * The cache grant (oplock / SMB2 lease), however, is a standalone heap object
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
        /* Release the standalone cache grant (vfs_state_destroy won't); no
         * pump -- there is no live connection left to answer a woken waiter. */
        if (open_file->grant) {
            chimera_smb_grant_remove_member(open_file->grant, open_file);
            chimera_vfs_claim_grant_release(vfs_state, open_file->grant,
                                            false /*pump*/);
            open_file->grant                  = NULL;
            open_file->caching_lease_inserted = false;
        }
        if (open_file->caching_file_state) {
            chimera_vfs_state_put(vfs_state, open_file->caching_file_state);
            open_file->caching_file_state = NULL;
        }
        chimera_smb_open_file_free(thread, open_file);

        chimera_smb_durable_entry_free(entry);
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

/* Recovery publishes one bounded, accepted page at a time. Each page builds a
 * private hash before finish, so publication cannot allocate after acceptance.
 * A failed/rejected page never installs records or advances the ID allocator. */
static void
chimera_smb_durable_recover_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_durable_recover_ctx *ctx = private_data;
    struct chimera_smb_durable_entry       *entry, *tmp;

    (void) compound;
    HASH_ITER(hh, ctx->prepared, entry, tmp)
    {
        HASH_DELETE(hh, ctx->prepared, entry);
        chimera_smb_durable_entry_free(entry);
    }
} /* chimera_smb_durable_recover_reset */

static void
chimera_smb_durable_recover_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_smb_durable_recover_ctx *ctx = private_data;
    const struct chimera_vfs_compound_op   *op  = chimera_vfs_compound_op(compound, index);

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    for (uint32_t i = 0; i < op->kv_num_entries; i++) {
        const struct chimera_vfs_compound_kv_entry *kv = &op->kv_entries[i];
        struct chimera_smb_durable_record           record;
        struct chimera_smb_durable_entry           *entry, *existing;
        if (chimera_smb_durable_deserialize(kv->value, kv->value_len, &record) != 0 ||
            record.persistent_id == UINT64_MAX) {
            continue;
        }
        HASH_FIND(hh, ctx->prepared, &record.persistent_id, sizeof(record.persistent_id), existing);
        if (existing) {
            continue;
        }
        entry = chimera_smb_durable_prepare_record(&record);
        if (!entry) {
            *status = CHIMERA_VFS_ENOSPC; return;
        }
        HASH_ADD(hh, ctx->prepared, persistent_id, sizeof(entry->persistent_id), entry);
    }
} /* chimera_smb_durable_recover_prepare */

static void
chimera_smb_durable_recover_publish(struct chimera_smb_durable_recover_ctx *ctx)
{
    struct chimera_server_smb_shared *shared = ctx->thread->shared;
    struct chimera_smb_durable_entry *entry, *tmp, *existing;

    pthread_mutex_lock(&shared->durable.lock);
    if (!shared->durable.by_pid) {
        shared->durable.by_pid = ctx->prepared;
        ctx->prepared          = NULL;
        HASH_ITER(hh, shared->durable.by_pid, entry, tmp)
        {
            chimera_smb_durable_advance_pid(shared, entry->persistent_id);
        }
    } else {
        unsigned int noexpand = shared->durable.by_pid->hh.tbl->noexpand;
        shared->durable.by_pid->hh.tbl->noexpand = 1;
        HASH_ITER(hh, ctx->prepared, entry, tmp)
        {
            HASH_DELETE(hh, ctx->prepared, entry);
            HASH_FIND(hh, shared->durable.by_pid, &entry->persistent_id,
                      sizeof(entry->persistent_id), existing);
            if (existing) {
                chimera_smb_durable_entry_free(entry); continue;
            }
            HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);
            chimera_smb_durable_advance_pid(shared, entry->persistent_id);
        }
        shared->durable.by_pid->hh.tbl->noexpand = noexpand;
    }
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_recover_publish */

static struct chimera_vfs_compound * chimera_smb_durable_recover_page(
    struct chimera_smb_durable_recover_ctx *ctx,
    const void                             *start,
    uint32_t                                start_len);
static void chimera_smb_durable_recover_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

static void
chimera_smb_durable_recover_continue(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_durable_recover_ctx *ctx = (struct chimera_smb_durable_recover_ctx *)
        ((char *) timer - offsetof(struct chimera_smb_durable_recover_ctx, continuation));
    struct chimera_vfs_compound            *compound = ctx->next_page;

    (void) evpl;
    ctx->next_page = NULL;
    chimera_frontend_compound_submit(compound, chimera_smb_durable_recover_complete, ctx);
} /* chimera_smb_durable_recover_continue */

static void
chimera_smb_durable_recover_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_durable_recover_ctx *ctx    = private_data;
    enum chimera_vfs_error                  status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, 1);
        chimera_smb_durable_recover_publish(ctx);
        if (op->kv_more) {
            /* Builder copies the inclusive resume key before this page frees it. */
            ctx->next_page = chimera_smb_durable_recover_page(ctx, op->kv_next_key, op->kv_next_key_len);
        }
    } else {
        chimera_smb_error("durable recovery page failed: error=%d", status);
    }
    chimera_smb_durable_recover_reset(compound, ctx);
    chimera_vfs_compound_free(compound);
    if (ctx->next_page) {
        /* Avoid unbounded recursion when backend dispatch completes inline. */
        evpl_add_oneshot_timer(ctx->thread->evpl, &ctx->continuation,
                               chimera_smb_durable_recover_continue, 1);
    } else {
        ctx->thread->maintenance_compounds--;
        free(ctx);
    }
} /* chimera_smb_durable_recover_complete */

static struct chimera_vfs_compound *
chimera_smb_durable_recover_page(
    struct chimera_smb_durable_recover_ctx *ctx,
    const void                             *start,
    uint32_t                                start_len)
{
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        ctx->thread->vfs_thread, chimera_vfs_get_server_cred());
    /* Exclusive successor of the durable prefix bounds the scan, including
     * fallback KV namespaces, without a side-effectful per-record callback. */
    static const char            end[] = "smbdi";

    chimera_vfs_compound_add_putfh(compound, ctx->fh, ctx->fh_len);
    int                          search = chimera_vfs_compound_add_search_keys_at(compound,
                                                                                  start, start_len, end, sizeof(end) - 1
                                                                                  ,
                                                                                  CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE,
                                                                                  128, 1024 * 1024);
    chimera_vfs_compound_set_op_callbacks(compound, search, NULL,
                                          chimera_smb_durable_recover_prepare, ctx);
    chimera_vfs_compound_set_attempt_reset(compound, chimera_smb_durable_recover_reset, ctx);
    return compound;
} /* chimera_smb_durable_recover_page */

/* Best-effort, idempotent recovery, in bounded accepted pages. A reconnect that
 * races a not-yet-accepted page retains the existing fresh-open fallback. */
SYMBOL_EXPORT void
chimera_smb_durable_recover_share(
    struct chimera_server_smb_thread *thread,
    const void                       *fh,
    int                               fh_len)
{
    if (!fh || fh_len <= 0 || fh_len > CHIMERA_VFS_FH_SIZE) {
        return;
    }
    struct chimera_smb_durable_recover_ctx *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return;
    }
    ctx->thread = thread;
    ctx->fh_len = fh_len;
    memcpy(ctx->fh, fh, fh_len);
    struct chimera_vfs_compound            *compound = chimera_smb_durable_recover_page(ctx,
                                                                                        CHIMERA_SMB_DURABLE_KEY_PREFIX,
                                                                                        CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN);
    thread->maintenance_compounds++;
    chimera_frontend_compound_submit(compound, chimera_smb_durable_recover_complete, ctx);
} /* chimera_smb_durable_recover_share */
