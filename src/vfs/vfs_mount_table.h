// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>
#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"

/*
 * URCU-based mount table for fast lock-free lookups by mount ID.
 *
 * Writers (insert/remove) are protected by a mutex - these are rare operations.
 * Readers (lookup) use RCU and require no locks - attrs are copied by value
 * for safe access without holding RCU read lock after return.
 */

struct chimera_vfs_mount_table_entry {
    struct chimera_vfs_mount             *mount;
    struct chimera_vfs_mount_table_entry *next;
    struct rcu_head                       rcu;
};

struct chimera_vfs_mount_table {
    struct chimera_vfs_mount_table_entry **buckets;
    uint32_t                               num_buckets;
    uint32_t                               num_buckets_mask;
    pthread_mutex_t                        lock;
};

static inline struct chimera_vfs_mount_table *
chimera_vfs_mount_table_create(uint32_t num_buckets_bits)
{
    struct chimera_vfs_mount_table *table;

    table = calloc(1, sizeof(*table));

    table->num_buckets      = 1 << num_buckets_bits;
    table->num_buckets_mask = table->num_buckets - 1;
    table->buckets          = calloc(table->num_buckets, sizeof(*table->buckets));

    pthread_mutex_init(&table->lock, NULL);

    return table;
} /* chimera_vfs_mount_table_create */

static inline void
chimera_vfs_mount_table_entry_free_rcu(struct rcu_head *head)
{
    struct chimera_vfs_mount_table_entry *entry;

    entry = container_of(head, struct chimera_vfs_mount_table_entry, rcu);
    free(entry);
} /* chimera_vfs_mount_table_entry_free_rcu */

static inline void
chimera_vfs_mount_table_destroy(struct chimera_vfs_mount_table *table)
{
    struct chimera_vfs_mount_table_entry *entry, *next;
    uint32_t                              i;

    rcu_barrier();

    for (i = 0; i < table->num_buckets; i++) {
        entry = table->buckets[i];
        while (entry) {
            next = entry->next;
            /* Free the mount and its path */
            free(entry->mount->path);
            free(entry->mount);
            free(entry);
            entry = next;
        }
    }

    pthread_mutex_destroy(&table->lock);
    free(table->buckets);
    free(table);
} /* chimera_vfs_mount_table_destroy */

static inline void
chimera_vfs_mount_table_insert(
    struct chimera_vfs_mount_table *table,
    struct chimera_vfs_mount       *mount)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint64_t                              hash;
    uint32_t                              bucket;

    entry        = calloc(1, sizeof(*entry));
    entry->mount = mount;

    hash   = chimera_vfs_hash(mount->mount_id, mount->mount_id_len);
    bucket = hash & table->num_buckets_mask;

    pthread_mutex_lock(&table->lock);

    entry->next = table->buckets[bucket];
    rcu_assign_pointer(table->buckets[bucket], entry);

    pthread_mutex_unlock(&table->lock);
} /* chimera_vfs_mount_table_insert */

static inline void
chimera_vfs_mount_table_remove(
    struct chimera_vfs_mount_table *table,
    const uint8_t                  *mount_id,
    int                             mount_id_len)
{
    struct chimera_vfs_mount_table_entry *entry, *prev, *removed = NULL;
    uint64_t                              hash;
    uint32_t                              bucket;

    hash   = chimera_vfs_hash(mount_id, mount_id_len);
    bucket = hash & table->num_buckets_mask;

    pthread_mutex_lock(&table->lock);

    prev  = NULL;
    entry = table->buckets[bucket];

    while (entry) {
        if (entry->mount->mount_id_len == mount_id_len &&
            memcmp(entry->mount->mount_id, mount_id, mount_id_len) == 0) {
            removed = entry;

            if (prev) {
                rcu_assign_pointer(prev->next, entry->next);
            } else {
                rcu_assign_pointer(table->buckets[bucket], entry->next);
            }
            break;
        }
        prev  = entry;
        entry = entry->next;
    }

    pthread_mutex_unlock(&table->lock);

    if (removed) {
        call_rcu(&removed->rcu, chimera_vfs_mount_table_entry_free_rcu);
    }
} /* chimera_vfs_mount_table_remove */

/*
 * Lookup mount attrs by mount ID.
 * Returns 0 on success with attrs copied to r_attrs, -1 if not found.
 * Caller does NOT need to hold any locks after this returns.
 */
static inline int
chimera_vfs_mount_table_lookup_attrs(
    struct chimera_vfs_mount_table *table,
    const uint8_t                  *mount_id,
    int                             mount_id_len,
    struct chimera_vfs_mount_attrs *r_attrs)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint64_t                              hash;
    uint32_t                              bucket;
    int                                   rc = -1;

    hash   = chimera_vfs_hash(mount_id, mount_id_len);
    bucket = hash & table->num_buckets_mask;

    urcu_memb_read_lock();

    entry = rcu_dereference(table->buckets[bucket]);

    while (entry) {
        if (entry->mount->mount_id_len == mount_id_len &&
            memcmp(entry->mount->mount_id, mount_id, mount_id_len) == 0) {
            /* Copy attrs by value for safe access after RCU unlock */
            *r_attrs = entry->mount->attrs;
            rc       = 0;
            break;
        }
        entry = rcu_dereference(entry->next);
    }

    urcu_memb_read_unlock();

    return rc;
} /* chimera_vfs_mount_table_lookup_attrs */

/*
 * Lookup full mount pointer by mount ID.
 * Returns mount pointer or NULL if not found.
 * IMPORTANT: Caller MUST call urcu_memb_read_lock() before and
 * urcu_memb_read_unlock() after using the returned pointer.
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_lookup(
    struct chimera_vfs_mount_table *table,
    const uint8_t                  *mount_id,
    int                             mount_id_len)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *mount = NULL;
    uint64_t                              hash;
    uint32_t                              bucket;

    hash   = chimera_vfs_hash(mount_id, mount_id_len);
    bucket = hash & table->num_buckets_mask;

    entry = rcu_dereference(table->buckets[bucket]);

    while (entry) {
        if (entry->mount->mount_id_len == mount_id_len &&
            memcmp(entry->mount->mount_id, mount_id, mount_id_len) == 0) {
            mount = entry->mount;
            break;
        }
        entry = rcu_dereference(entry->next);
    }

    return mount;
} /* chimera_vfs_mount_table_lookup */

/*
 * Count the number of mounts in the table.
 * Uses RCU read lock internally.
 */
static inline int
chimera_vfs_mount_table_count(struct chimera_vfs_mount_table *table)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   count = 0;

    urcu_memb_read_lock();

    for (i = 0; i < table->num_buckets; i++) {
        entry = rcu_dereference(table->buckets[i]);
        while (entry) {
            count++;
            entry = rcu_dereference(entry->next);
        }
    }

    urcu_memb_read_unlock();

    return count;
} /* chimera_vfs_mount_table_count */

/*
 * Iteration callback type.
 * Return 0 to continue iteration, non-zero to stop.
 */
typedef int (*chimera_vfs_mount_table_iter_cb)(
    struct chimera_vfs_mount *mount,
    void                     *private_data);

/*
 * Iterate over all mounts in the table.
 * Callback is called with RCU read lock held.
 * Returns 0 if all mounts visited, or the non-zero return from callback.
 */
static inline int
chimera_vfs_mount_table_foreach(
    struct chimera_vfs_mount_table *table,
    chimera_vfs_mount_table_iter_cb callback,
    void                           *private_data)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   rc = 0;

    urcu_memb_read_lock();

    for (i = 0; i < table->num_buckets && rc == 0; i++) {
        entry = rcu_dereference(table->buckets[i]);
        while (entry && rc == 0) {
            rc    = callback(entry->mount, private_data);
            entry = rcu_dereference(entry->next);
        }
    }

    urcu_memb_read_unlock();

    return rc;
} /* chimera_vfs_mount_table_foreach */

/*
 * Find a mount by path prefix match.
 * Returns the mount whose path is a prefix of the given path,
 * or NULL if not found. Uses RCU read lock internally.
 *
 * IMPORTANT: The returned mount pointer is only valid while RCU read lock
 * is held. If caller needs to use the mount after this returns, they must
 * copy necessary data or hold their own RCU read lock.
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_find_by_path(
    struct chimera_vfs_mount_table *table,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *found = NULL;
    uint32_t                              i;

    urcu_memb_read_lock();

    for (i = 0; i < table->num_buckets && !found; i++) {
        entry = rcu_dereference(table->buckets[i]);
        while (entry) {
            if (entry->mount->pathlen <= pathlen &&
                memcmp(entry->mount->path, path, entry->mount->pathlen) == 0 &&
                (entry->mount->pathlen == pathlen ||
                 path[entry->mount->pathlen] == '/')) {
                found = entry->mount;
                break;
            }
            entry = rcu_dereference(entry->next);
        }
    }

    urcu_memb_read_unlock();

    return found;
} /* chimera_vfs_mount_table_find_by_path */

/*
 * Find and remove a mount by exact path match.
 * Returns the mount pointer if found and removed, NULL if not found.
 * The returned mount pointer is owned by the caller who must free it.
 * This function is atomic - find and remove are done under the writer lock.
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_remove_by_path(
    struct chimera_vfs_mount_table *table,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs_mount_table_entry *entry, *prev;
    struct chimera_vfs_mount             *mount = NULL;
    uint32_t                              i;

    pthread_mutex_lock(&table->lock);

    for (i = 0; i < table->num_buckets && !mount; i++) {
        prev  = NULL;
        entry = table->buckets[i];
        while (entry) {
            if (entry->mount->pathlen == pathlen &&
                memcmp(entry->mount->path, path, pathlen) == 0) {
                mount = entry->mount;

                if (prev) {
                    rcu_assign_pointer(prev->next, entry->next);
                } else {
                    rcu_assign_pointer(table->buckets[i], entry->next);
                }

                call_rcu(&entry->rcu, chimera_vfs_mount_table_entry_free_rcu);
                break;
            }
            prev  = entry;
            entry = entry->next;
        }
    }

    pthread_mutex_unlock(&table->lock);

    return mount;
} /* chimera_vfs_mount_table_remove_by_path */

/*
 * Lookup a mount by name and copy its mount ID.
 * The name is compared using strncmp against mount paths.
 * Returns 0 on success with mount_id/mount_id_len copied, -1 if not found.
 * This is safe for use without holding RCU read lock after return.
 */
static inline int
chimera_vfs_mount_table_lookup_mount_id_by_name(
    struct chimera_vfs_mount_table *table,
    const char                     *name,
    int                             namelen,
    uint8_t                        *r_mount_id,
    int                            *r_mount_id_len)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   rc = -1;

    urcu_memb_read_lock();

    for (i = 0; i < table->num_buckets && rc != 0; i++) {
        entry = rcu_dereference(table->buckets[i]);
        while (entry) {
            if (strncmp(entry->mount->path, name, namelen) == 0) {
                memcpy(r_mount_id, entry->mount->mount_id, entry->mount->mount_id_len);
                *r_mount_id_len = entry->mount->mount_id_len;
                rc              = 0;
                break;
            }
            entry = rcu_dereference(entry->next);
        }
    }

    urcu_memb_read_unlock();

    return rc;
} /* chimera_vfs_mount_table_lookup_mount_id_by_name */
