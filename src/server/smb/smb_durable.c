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
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              persistent)
{
    struct chimera_smb_durable_entry *entry = calloc(1, sizeof(*entry));

    entry->persistent_id = open_file->file_id.pid;
    entry->session_id    = session_id;
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
    uint64_t                          persistent_id)
{
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;

    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);
    if (entry && entry->parked && !entry->persistent && !entry->cold &&
        entry->open_file) {
        HASH_DELETE(hh, shared->durable.by_pid, entry);
        open_file = entry->open_file;
        free(entry);
    }
    pthread_mutex_unlock(&shared->durable.lock);

    if (!open_file) {
        return false;
    }

    chimera_smb_open_file_drain_locks(thread, open_file);
    if (open_file->handle) {
        chimera_vfs_release(thread->vfs_thread, open_file->handle);
        open_file->handle = NULL;
    }
    chimera_smb_open_file_free(thread, open_file);
    return true;
} /* chimera_smb_durable_purge_parked */

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
        entry->parked            = true;
        entry->deadline          = now;
        entry->deadline.tv_sec  += open_file->durable_timeout_ms / 1000;
        entry->deadline.tv_nsec += (open_file->durable_timeout_ms % 1000) * 1000000L;
        if (entry->deadline.tv_nsec >= 1000000000L) {
            entry->deadline.tv_sec  += 1;
            entry->deadline.tv_nsec -= 1000000000L;
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_park */

SYMBOL_EXPORT struct chimera_smb_open_file *
chimera_smb_durable_claim(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id,
    const uint8_t                    *create_guid,
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              has_lease_ctx,
    const uint8_t                    *lease_key,
    bool                             *r_cold,
    uint32_t                         *status)
{
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;
    bool                              had_lease;

    *r_cold = false;

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
        /* The handle is still live on another (multi-)channel — a second
         * reconnect cannot steal it. */
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
        if (open_file->handle) {
            chimera_vfs_release(thread->vfs_thread, open_file->handle);
            open_file->handle = NULL;
        }
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
 * Only the VFS open handle is released here; the open's leases / share
 * reservation / byte-range locks are deliberately NOT drained.  Draining a
 * lease pumps any pending acquire queued behind it (e.g. a blocking lock
 * whose connection already dropped), whose completion callback would try to
 * send a reply -- allocating a reply iovec on this teardown thread for a
 * dead connection, which trips the cross-thread iovec guard.  At shutdown
 * those pending waiters have no live connection to answer, so dropping them
 * is correct; the leftover lease objects are reclaimed wholesale when
 * chimera_vfs_state_destroy frees the per-file state (it never walks the
 * lease lists), and no operation runs after thread destroy to observe a
 * dangling lease. */
SYMBOL_EXPORT void
chimera_smb_durable_drain_all(struct chimera_server_smb_thread *thread)
{
    struct chimera_server_smb_shared *shared = thread->shared;
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
                               NULL, 0,
                               chimera_smb_durable_recover_cb,
                               chimera_smb_durable_recover_complete,
                               ctx);
} /* chimera_smb_durable_recover_share */
