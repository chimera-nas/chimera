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
#include "vfs/vfs_release.h"

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
    HASH_ITER(hh, table->by_pid, entry, tmp)
    {
        HASH_DELETE(hh, table->by_pid, entry);
        free(entry);
    }

    pthread_mutex_destroy(&table->lock);
} /* chimera_smb_durable_table_destroy */

SYMBOL_EXPORT void
chimera_smb_durable_register(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open_file,
    uint64_t                          session_id,
    const char                       *name,
    uint32_t                          name_len)
{
    struct chimera_smb_durable_entry *entry = calloc(1, sizeof(*entry));

    entry->persistent_id = open_file->file_id.pid;
    entry->session_id    = session_id;
    entry->open_file     = open_file;
    entry->parked        = false;
    memcpy(entry->create_guid, open_file->create_guid, sizeof(entry->create_guid));

    if (name_len > sizeof(entry->name)) {
        name_len = sizeof(entry->name);
    }
    entry->name_len = name_len;
    memcpy(entry->name, name, name_len);

    pthread_mutex_lock(&shared->durable.lock);
    HASH_ADD(hh, shared->durable.by_pid, persistent_id, sizeof(entry->persistent_id), entry);
    pthread_mutex_unlock(&shared->durable.lock);
} /* chimera_smb_durable_register */

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
    uint64_t                          session_id,
    const char                       *name,
    uint32_t                          name_len,
    uint32_t                         *status)
{
    struct chimera_smb_durable_entry *entry;
    struct chimera_smb_open_file     *open_file = NULL;

    pthread_mutex_lock(&shared->durable.lock);

    HASH_FIND(hh, shared->durable.by_pid, &persistent_id, sizeof(persistent_id), entry);

    if (!entry) {
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (!entry->parked) {
        /* The handle is still live on another (multi-)channel — a second
         * reconnect cannot steal it. */
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (create_guid && memcmp(entry->create_guid, create_guid, 16) != 0) {
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else if (entry->session_id != session_id) {
        /* A different session/user may not reclaim the handle. */
        *status = SMB2_STATUS_ACCESS_DENIED;
    } else if (entry->name_len != name_len || memcmp(entry->name, name, name_len) != 0) {
        *status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } else {
        /* Claim it: flip out of the parked state so the sweeper leaves it
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
        expired = expired->hh.next;

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
