// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <string.h>
#include "common/atomic.h"
#include "common/thread.h"
#include "common/chimera_rcu.h"
#include "vfs/vfs.h"
#include "vfs/sdk/vfs_fh.h"

/*
 * Extract bucket index directly from mount_id.
 * Since the mount_id is already a 128-bit hash, we can just use
 * the first 8 bytes directly as the bucket index without re-hashing.
 */
static inline uint64_t
chimera_vfs_mount_table_bucket_index(const void *mount_id)
{
    uint64_t value;

    memcpy(&value, mount_id, sizeof(value));
    return value;
} /* chimera_vfs_mount_table_bucket_index */

/*
 * Mount table for fast lookups by mount ID.
 *
 * Writers (insert/remove) are protected by a mutex - these are rare operations.
 * Readers (lookup) hold the table's RCU domain, which is free under urcu and a
 * read lock without it - attrs are copied by value for safe access without
 * holding the read side after return.
 */

struct chimera_vfs_mount_table_entry {
    struct chimera_vfs_mount             *mount;
    struct chimera_vfs_mount_table_entry *next;
    chimera_rcu_head                      rcu;
};

/*
 * Second index over the same mounts, keyed by mount point: the hash of the
 * covered directory's parent handle and name (chimera_vfs_mount_cover_hash).
 * Only mounts placed in the namespace below "/" have an entry here.
 */
struct chimera_vfs_mount_cover_entry {
    struct chimera_vfs_mount             *mount;
    struct chimera_vfs_mount_cover_entry *next;
    chimera_rcu_head                      rcu;
};

struct chimera_vfs_mount_table {
    struct chimera_vfs_mount_table_entry **buckets;
    struct chimera_vfs_mount_cover_entry **cover_buckets;
    uint32_t                               num_buckets;
    uint32_t                               num_buckets_mask;
    /* Number of cover entries; lets the per-lookup mount point check skip
     * the hash when nothing is mounted below "/". */
    int                                    num_covers;
    struct chimera_rcu_domain              rcu;
    evpl_mutex_t                           lock;
};

static inline uint64_t
chimera_vfs_mount_cover_hash(
    const void *parent_fh,
    int         parent_fh_len,
    const char *name,
    int         namelen)
{
    return chimera_vfs_hash(parent_fh, parent_fh_len) ^
           (chimera_vfs_hash(name, namelen) * 0x9E3779B97F4A7C15ULL);
} /* chimera_vfs_mount_cover_hash */

/*
 * Normalize a namespace mount path into out: no leading, trailing or repeated
 * '/', so "/" becomes "" and "//a/b/" becomes "a/b".  Returns the length, or
 * -1 for a path with a "." or ".." component or one that does not fit.
 */
static inline int
chimera_vfs_mount_path_normalize(
    const char *in,
    char       *out,
    int         outlen)
{
    int len = 0;

    while (*in) {
        const char *comp;
        int         complen;

        while (*in == '/') {
            in++;
        }
        comp = in;
        while (*in && *in != '/') {
            in++;
        }
        complen = in - comp;

        if (complen == 0) {
            break;
        }
        if ((complen == 1 && comp[0] == '.') ||
            (complen == 2 && comp[0] == '.' && comp[1] == '.')) {
            return -1;
        }
        if (len + (len ? 1 : 0) + complen >= outlen) {
            return -1;
        }
        if (len) {
            out[len++] = '/';
        }
        memcpy(out + len, comp, complen);
        len += complen;
    }

    out[len] = '\0';
    return len;
} /* chimera_vfs_mount_path_normalize */

static inline struct chimera_vfs_mount_table *
chimera_vfs_mount_table_create(uint32_t num_buckets_bits)
{
    struct chimera_vfs_mount_table *table;

    table = calloc(1, sizeof(*table));

    table->num_buckets      = 1 << num_buckets_bits;
    table->num_buckets_mask = table->num_buckets - 1;
    table->buckets          = calloc(table->num_buckets, sizeof(*table->buckets));
    table->cover_buckets    = calloc(table->num_buckets, sizeof(*table->cover_buckets));

    chimera_rcu_domain_init(&table->rcu);
    evpl_mutex_init(&table->lock, NULL);

    return table;
} /* chimera_vfs_mount_table_create */

static inline void
chimera_vfs_mount_table_entry_free_rcu(chimera_rcu_head *head)
{
    struct chimera_vfs_mount_table_entry *entry;

    entry = container_of(head, struct chimera_vfs_mount_table_entry, rcu);
    free(entry);
} /* chimera_vfs_mount_table_entry_free_rcu */

static inline void
chimera_vfs_mount_table_destroy(struct chimera_vfs_mount_table *table)
{
    struct chimera_vfs_mount_table_entry *entry, *next;
    struct chimera_vfs_mount_cover_entry *cover, *cover_next;
    uint32_t                              i;

    chimera_rcu_barrier();

    for (i = 0; i < table->num_buckets; i++) {
        for (cover = table->cover_buckets[i]; cover; cover = cover_next) {
            cover_next = cover->next;
            free(cover);
        }
        entry = table->buckets[i];
        while (entry) {
            next = entry->next;
            /* Free the mount and its path */
            free(entry->mount->path);
            free(entry->mount->module_path);
            free(entry->mount->options);
            free(entry->mount->cover_name);
            free(entry->mount);
            free(entry);
            entry = next;
        }
    }

    free(table->cover_buckets);

    evpl_mutex_destroy(&table->lock);
    chimera_rcu_domain_destroy(&table->rcu);
    free(table->buckets);
    free(table);
} /* chimera_vfs_mount_table_destroy */

static inline void
chimera_vfs_mount_table_insert(
    struct chimera_vfs_mount_table *table,
    struct chimera_vfs_mount       *mount)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint64_t                              index;
    uint32_t                              bucket;

    entry        = calloc(1, sizeof(*entry));
    entry->mount = mount;

    /* mount_id is the first 16 bytes of root_fh */
    index  = chimera_vfs_mount_table_bucket_index(mount->root_fh);
    bucket = index & table->num_buckets_mask;

    evpl_mutex_lock(&table->lock);

    entry->next = table->buckets[bucket];
    chimera_rcu_assign(table->buckets[bucket], entry);

    if (mount->cover_parent_fh_len > 0) {
        struct chimera_vfs_mount_cover_entry *cover;

        cover        = calloc(1, sizeof(*cover));
        cover->mount = mount;
        bucket       = mount->cover_hash & table->num_buckets_mask;
        cover->next  = table->cover_buckets[bucket];
        chimera_rcu_assign(table->cover_buckets[bucket], cover);
        chimera_atomic_add_fetch(&table->num_covers, 1, CHIMERA_MEMORY_RELEASE);
    }

    evpl_mutex_unlock(&table->lock);
} /* chimera_vfs_mount_table_insert */

static inline void
chimera_vfs_mount_table_cover_entry_free_rcu(chimera_rcu_head *head)
{
    struct chimera_vfs_mount_cover_entry *entry;

    entry = container_of(head, struct chimera_vfs_mount_cover_entry, rcu);
    free(entry);
} /* chimera_vfs_mount_table_cover_entry_free_rcu */

/*
 * Unlink this exact mount from both indexes.  Umount removes the mount it
 * claimed rather than whatever currently answers to its path or id: the
 * built-in root and a mount at "/" share the path "", and two mounts of the
 * same backend root share an id.
 */
static inline void
chimera_vfs_mount_table_remove_mount(
    struct chimera_vfs_mount_table *table,
    struct chimera_vfs_mount       *mount)
{
    struct chimera_vfs_mount_table_entry *entry, *prev;
    struct chimera_vfs_mount_cover_entry *cover, *cover_prev;
    uint32_t                              bucket;

    chimera_rcu_publish_begin(&table->rcu);
    evpl_mutex_lock(&table->lock);

    bucket = chimera_vfs_mount_table_bucket_index(mount->root_fh) & table->num_buckets_mask;
    prev   = NULL;

    for (entry = table->buckets[bucket]; entry; prev = entry, entry = entry->next) {
        if (entry->mount == mount) {
            if (prev) {
                chimera_rcu_replace(&table->rcu, prev->next, entry->next,
                                    &entry->rcu,
                                    chimera_vfs_mount_table_entry_free_rcu);
            } else {
                chimera_rcu_replace(&table->rcu, table->buckets[bucket], entry->next,
                                    &entry->rcu,
                                    chimera_vfs_mount_table_entry_free_rcu);
            }
            break;
        }
    }

    if (mount->cover_parent_fh_len > 0) {
        bucket     = mount->cover_hash & table->num_buckets_mask;
        cover_prev = NULL;

        for (cover = table->cover_buckets[bucket]; cover;
             cover_prev = cover, cover = cover->next) {
            if (cover->mount == mount) {
                if (cover_prev) {
                    chimera_rcu_replace(&table->rcu, cover_prev->next, cover->next,
                                        &cover->rcu,
                                        chimera_vfs_mount_table_cover_entry_free_rcu);
                } else {
                    chimera_rcu_replace(&table->rcu, table->cover_buckets[bucket],
                                        cover->next, &cover->rcu,
                                        chimera_vfs_mount_table_cover_entry_free_rcu);
                }
                chimera_atomic_add_fetch(&table->num_covers, -1, CHIMERA_MEMORY_RELEASE);
                break;
            }
        }
    }

    evpl_mutex_unlock(&table->lock);
    chimera_rcu_publish_end(&table->rcu);
} /* chimera_vfs_mount_table_remove_mount */

/*
 * The mount whose mount point is `name` in the directory `parent_fh`, or NULL.
 * Caller holds the table's read side for as long as it uses the result.
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_lookup_cover(
    struct chimera_vfs_mount_table *table,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             namelen)
{
    struct chimera_vfs_mount_cover_entry *cover;
    struct chimera_vfs_mount             *mount;
    uint64_t                              hash;

    if (chimera_atomic_load_n(&table->num_covers, CHIMERA_MEMORY_RELAXED) == 0) {
        return NULL;
    }

    hash = chimera_vfs_mount_cover_hash(parent_fh, parent_fh_len, name, namelen);

    for (cover = chimera_rcu_deref(table->cover_buckets[hash & table->num_buckets_mask]);
         cover; cover = chimera_rcu_deref(cover->next)) {
        mount = cover->mount;
        if (mount->cover_hash == hash &&
            !mount->unmounting &&
            mount->cover_parent_fh_len == parent_fh_len &&
            mount->cover_namelen == namelen &&
            memcmp(mount->cover_parent_fh, parent_fh, parent_fh_len) == 0 &&
            memcmp(mount->cover_name, name, namelen) == 0) {
            return mount;
        }
    }

    return NULL;
} /* chimera_vfs_mount_table_lookup_cover */

/*
 * If `name` in `parent_fh` is a mount point, copy out the mounted root's
 * handle (when r_root_fh is non-NULL) and its mount-time attributes (when
 * r_attrs is non-NULL) and return 0; otherwise return -1.
 */
static inline int
chimera_vfs_mount_table_cover_root(
    struct chimera_vfs_mount_table *table,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             namelen,
    uint8_t                        *r_root_fh,
    int                            *r_root_fh_len,
    struct chimera_vfs_attrs       *r_attrs)
{
    struct chimera_vfs_mount *mount;
    int                       rc = -1;

    if (chimera_atomic_load_n(&table->num_covers, CHIMERA_MEMORY_RELAXED) == 0) {
        return -1;
    }

    chimera_rcu_read_lock(&table->rcu);

    mount = chimera_vfs_mount_table_lookup_cover(table, parent_fh, parent_fh_len,
                                                 name, namelen);

    if (mount) {
        if (r_root_fh) {
            memcpy(r_root_fh, mount->root_fh, mount->root_fh_len);
            *r_root_fh_len = mount->root_fh_len;
        }
        if (r_attrs) {
            *r_attrs = mount->root_attrs;
        }
        rc = 0;
    }

    chimera_rcu_read_unlock(&table->rcu);

    return rc;
} /* chimera_vfs_mount_table_cover_root */

/*
 * If `fh` is the root of a mount placed below "/", copy out the handle of the
 * directory holding its mount point -- where ".." from the mount root leads --
 * and, when r_name is non-NULL (CHIMERA_VFS_NAME_MAX bytes), the mount
 * point's name, and return 0; otherwise return -1.
 */
static inline int
chimera_vfs_mount_table_cover_parent_name(
    struct chimera_vfs_mount_table *table,
    const void                     *fh,
    int                             fhlen,
    uint8_t                        *r_parent_fh,
    int                            *r_parent_fh_len,
    char                           *r_name,
    int                            *r_namelen)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *mount;
    int                                   rc = -1;

    if (fhlen < CHIMERA_VFS_MOUNT_ID_SIZE ||
        chimera_atomic_load_n(&table->num_covers, CHIMERA_MEMORY_RELAXED) == 0) {
        return -1;
    }

    chimera_rcu_read_lock(&table->rcu);

    entry = chimera_rcu_deref(table->buckets[
                                  chimera_vfs_mount_table_bucket_index(fh) &
                                  table->num_buckets_mask]);

    for (; entry; entry = chimera_rcu_deref(entry->next)) {
        mount = entry->mount;
        if (mount->cover_parent_fh_len > 0 &&
            mount->root_fh_len == fhlen &&
            memcmp(mount->root_fh, fh, fhlen) == 0) {
            memcpy(r_parent_fh, mount->cover_parent_fh, mount->cover_parent_fh_len);
            *r_parent_fh_len = mount->cover_parent_fh_len;
            if (r_name) {
                memcpy(r_name, mount->cover_name, mount->cover_namelen);
                *r_namelen = mount->cover_namelen;
            }
            rc = 0;
            break;
        }
    }

    chimera_rcu_read_unlock(&table->rcu);

    return rc;
} /* chimera_vfs_mount_table_cover_parent_name */

static inline int
chimera_vfs_mount_table_cover_parent(
    struct chimera_vfs_mount_table *table,
    const void                     *fh,
    int                             fhlen,
    uint8_t                        *r_parent_fh,
    int                            *r_parent_fh_len)
{
    return chimera_vfs_mount_table_cover_parent_name(table, fh, fhlen, r_parent_fh,
                                                     r_parent_fh_len, NULL, NULL);
} /* chimera_vfs_mount_table_cover_parent */

/*
 * True when another mount's mount point lies inside `mount`'s filesystem, so
 * `mount` cannot be unmounted (or replaced, for the root) without orphaning it.
 */
static inline int
chimera_vfs_mount_table_has_submounts(
    struct chimera_vfs_mount_table *table,
    struct chimera_vfs_mount       *mount)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   found = 0;

    evpl_mutex_lock(&table->lock);

    for (i = 0; i < table->num_buckets && !found; i++) {
        for (entry = table->buckets[i]; entry; entry = entry->next) {
            if (entry->mount != mount &&
                entry->mount->cover_parent_fh_len >= CHIMERA_VFS_MOUNT_ID_SIZE &&
                memcmp(entry->mount->cover_parent_fh, mount->root_fh,
                       CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
                found = 1;
                break;
            }
        }
    }

    evpl_mutex_unlock(&table->lock);

    return found;
} /* chimera_vfs_mount_table_has_submounts */

/*
 * Copy out the root handle of the namespace root: the mount at "/" over the
 * built-in rootfs, or the rootfs when there is none.  Returns 0, or -1 if
 * nothing at all is at "/" (only before the rootfs is mounted).
 */
static inline int
chimera_vfs_mount_table_namespace_root(
    struct chimera_vfs_mount_table *table,
    uint8_t                        *r_root_fh,
    uint32_t                       *r_root_fh_len)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   rc = -1;

    chimera_rcu_read_lock(&table->rcu);

    for (i = 0; i < table->num_buckets; i++) {
        for (entry = chimera_rcu_deref(table->buckets[i]); entry;
             entry = chimera_rcu_deref(entry->next)) {
            if (!entry->mount->detached && entry->mount->pathlen == 0 &&
                (rc != 0 || !entry->mount->builtin)) {
                memcpy(r_root_fh, entry->mount->root_fh, entry->mount->root_fh_len);
                *r_root_fh_len = entry->mount->root_fh_len;
                rc             = 0;
                if (!entry->mount->builtin) {
                    break;
                }
            }
        }
    }

    chimera_rcu_read_unlock(&table->rcu);

    return rc;
} /* chimera_vfs_mount_table_namespace_root */

static inline void
chimera_vfs_mount_table_remove(
    struct chimera_vfs_mount_table *table,
    const uint8_t                  *mount_id)
{
    struct chimera_vfs_mount_table_entry *entry, *prev;
    uint64_t                              index;
    uint32_t                              bucket;

    index  = chimera_vfs_mount_table_bucket_index(mount_id);
    bucket = index & table->num_buckets_mask;

    chimera_rcu_publish_begin(&table->rcu);
    evpl_mutex_lock(&table->lock);

    prev  = NULL;
    entry = table->buckets[bucket];

    while (entry) {
        /* Compare the full 16-byte mount_id (first 16 bytes of root_fh) */
        if (memcmp(entry->mount->root_fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
            if (prev) {
                chimera_rcu_replace(&table->rcu, prev->next, entry->next,
                                    &entry->rcu,
                                    chimera_vfs_mount_table_entry_free_rcu);
            } else {
                chimera_rcu_replace(&table->rcu, table->buckets[bucket], entry->next,
                                    &entry->rcu,
                                    chimera_vfs_mount_table_entry_free_rcu);
            }
            break;
        }
        prev  = entry;
        entry = entry->next;
    }

    evpl_mutex_unlock(&table->lock);
    chimera_rcu_publish_end(&table->rcu);
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
    struct chimera_vfs_mount_attrs *r_attrs)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint64_t                              index;
    uint32_t                              bucket;
    int                                   rc = -1;

    index  = chimera_vfs_mount_table_bucket_index(mount_id);
    bucket = index & table->num_buckets_mask;

    chimera_rcu_read_lock(&table->rcu);

    entry = chimera_rcu_deref(table->buckets[bucket]);

    while (entry) {
        /* Compare the full 16-byte mount_id (first 16 bytes of root_fh) */
        if (memcmp(entry->mount->root_fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
            /* Copy attrs by value for safe access after RCU unlock */
            *r_attrs = entry->mount->attrs;
            rc       = 0;
            break;
        }
        entry = chimera_rcu_deref(entry->next);
    }

    chimera_rcu_read_unlock(&table->rcu);

    return rc;
} /* chimera_vfs_mount_table_lookup_attrs */

/*
 * Lookup full mount pointer by mount ID.
 * Returns mount pointer or NULL if not found.
 * IMPORTANT: Caller MUST bracket this call and every use of the returned
 * pointer with chimera_rcu_read_lock(&table->rcu) /
 * chimera_rcu_read_unlock(&table->rcu).
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_lookup(
    struct chimera_vfs_mount_table *table,
    const uint8_t                  *mount_id)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *mount = NULL;
    uint64_t                              index;
    uint32_t                              bucket;

    index  = chimera_vfs_mount_table_bucket_index(mount_id);
    bucket = index & table->num_buckets_mask;

    entry = chimera_rcu_deref(table->buckets[bucket]);

    while (entry) {
        /* Compare the full 16-byte mount_id (first 16 bytes of root_fh) */
        if (memcmp(entry->mount->root_fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
            mount = entry->mount;
            break;
        }
        entry = chimera_rcu_deref(entry->next);
    }

    return mount;
} /* chimera_vfs_mount_table_lookup */

/*
 * Count the number of mounts in the table.
 * Takes the table's read side internally.
 */
static inline int
chimera_vfs_mount_table_count(struct chimera_vfs_mount_table *table)
{
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   count = 0;

    chimera_rcu_read_lock(&table->rcu);

    for (i = 0; i < table->num_buckets; i++) {
        entry = chimera_rcu_deref(table->buckets[i]);
        while (entry) {
            count++;
            entry = chimera_rcu_deref(entry->next);
        }
    }

    chimera_rcu_read_unlock(&table->rcu);

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
 * Callback is called with the table's read side held.
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

    chimera_rcu_read_lock(&table->rcu);

    for (i = 0; i < table->num_buckets && rc == 0; i++) {
        entry = chimera_rcu_deref(table->buckets[i]);
        while (entry && rc == 0) {
            rc    = callback(entry->mount, private_data);
            entry = chimera_rcu_deref(entry->next);
        }
    }

    chimera_rcu_read_unlock(&table->rcu);

    return rc;
} /* chimera_vfs_mount_table_foreach */

/*
 * Find the mount a namespace path lies in: the mount whose path is the
 * longest whole-component prefix of `path` ("" matches everything, so the
 * root mount answers when nothing deeper does).  Detached mounts have no
 * namespace position and never match.  Takes the table's read side
 * internally in the unprotected form.
 *
 * IMPORTANT: The returned mount pointer is only valid while the table's read
 * side is held. If caller needs to use the mount after this returns, they must
 * copy necessary data or take the read side themselves.
 */
/* The protected form also supports callers holding table->lock, including
 * startup/management threads that are not registered liburcu readers. */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_find_by_path_protected(
    struct chimera_vfs_mount_table *table,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *found = NULL;
    uint32_t                              i;

    while (pathlen > 0 && path[0] == '/') {
        path++;
        pathlen--;
    }

    for (i = 0; i < table->num_buckets; i++) {
        entry = chimera_rcu_deref(table->buckets[i]);
        while (entry) {
            struct chimera_vfs_mount *mount = entry->mount;

            if (!mount->detached &&
                mount->pathlen <= (uint32_t) pathlen &&
                memcmp(mount->path, path, mount->pathlen) == 0 &&
                (mount->pathlen == 0 ||
                 mount->pathlen == (uint32_t) pathlen ||
                 path[mount->pathlen] == '/') &&
                (!found || mount->pathlen > found->pathlen ||
                 (mount->pathlen == found->pathlen && found->builtin))) {
                found = mount;
            }
            entry = chimera_rcu_deref(entry->next);
        }
    }

    return found;
} /* chimera_vfs_mount_table_find_by_path_protected */

static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_find_by_path(
    struct chimera_vfs_mount_table *table,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs_mount *found;

    chimera_rcu_read_lock(&table->rcu);
    found = chimera_vfs_mount_table_find_by_path_protected(table, path, pathlen);
    chimera_rcu_read_unlock(&table->rcu);
    return found;
} /* chimera_vfs_mount_table_find_by_path */


/*
 * Whether a path proc (open, mkdir, mknod, symlink, link, remove, rename) may
 * hand its path whole to the backend of the directory it starts from -- that
 * backend's CAP_FS_PATH_OP route -- instead of walking it component by
 * component.  Only a path-only backend, which has no other way.  A backend
 * that can also walk would resolve the path itself (the kernel, for the
 * passthrough backends), with its own ideas of "..", symlinks and what lies
 * below a mount point, none of which are the namespace's -- once a
 * passthrough filesystem can be the namespace root, an absolute path starts
 * on it.  A path from the namespace root takes the rebase route below even
 * when a path-only filesystem is mounted at "/", as it does for one mounted
 * below it.
 */
static inline int
chimera_vfs_path_op_whole(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_module *module,
    const void                      *start_fh,
    int                              start_fh_len)
{
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;

    if (!chimera_vfs_module_is_path_only(module)) {
        return 0;
    }

    if (chimera_vfs_mount_table_namespace_root(thread->vfs->mount_table,
                                               root_fh, &root_fh_len) == 0 &&
        root_fh_len == (uint32_t) start_fh_len &&
        memcmp(root_fh, start_fh, start_fh_len) == 0) {
        return 0;
    }

    return 1;
} /* chimera_vfs_path_op_whole */

/*
 * Path-only deep-path rebasing, shared by the path procs (open, mkdir, remove,
 * rename).
 *
 * Those operations take a starting fh and a path.  When the start is the
 * namespace root and the path resolves into a path-only mount, the
 * intermediate directories have NO re-openable file handles, so the usual
 * "resolve immediate parent dir + dispatch _at on the leaf" scheme breaks for
 * anything deeper than a single level.
 *
 * If `path` (slash-stripped, relative to the namespace root) falls under a
 * path-only mount -- the deepest mount whose path prefixes it, which may be
 * the mount at "/" itself -- copy out that
 * mount's re-openable root fh and return the byte offset of the in-mount
 * remainder within `path`.  The caller then opens the mount root as a
 * directory handle and dispatches the _at op with the entire in-mount sub-path
 * as the name; the path-only backend resolves the whole sub-path in one
 * operation.  Returns -1 otherwise (the caller keeps its FH-relative
 * behavior).
 */
static inline int
chimera_vfs_pathonly_rebase(
    struct chimera_vfs_thread *thread,
    const void                *start_fh,
    int                        start_fh_len,
    const char                *path,
    int                        pathlen,
    uint8_t                   *r_root_fh,
    int                       *r_root_fh_len)
{
    struct chimera_vfs_mount_table *table = thread->vfs->mount_table;
    struct chimera_vfs_mount       *mount, *root;
    int                             offset = -1;

    chimera_rcu_read_lock(&table->rcu);

    root  = chimera_vfs_mount_table_find_by_path_protected(table, "", 0);
    mount = chimera_vfs_mount_table_find_by_path_protected(table, path, pathlen);

    if (root && mount &&
        root->root_fh_len == start_fh_len &&
        memcmp(root->root_fh, start_fh, start_fh_len) == 0 &&
        chimera_vfs_module_is_path_only(mount->module)) {

        memcpy(r_root_fh, mount->root_fh, mount->root_fh_len);
        *r_root_fh_len = mount->root_fh_len;

        offset = mount->pathlen;
        /* Skip the separating slash to land on the in-mount remainder. */
        while (offset < pathlen && path[offset] == '/') {
            offset++;
        }
    }

    chimera_rcu_read_unlock(&table->rcu);

    return offset;
} /* chimera_vfs_pathonly_rebase */

/*
 * Find a mount by exact path match, leaving it in the table.
 *
 * umount needs the mount before it is unlinked: its cached handles are closed
 * while it is still routable, so those closes resolve the mount exactly as any
 * other op would.  Returns NULL if no mount has that path.
 */
static inline struct chimera_vfs_mount *
chimera_vfs_mount_table_find_exact(
    struct chimera_vfs_mount_table *table,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs_mount_table_entry *entry;
    struct chimera_vfs_mount             *found = NULL;
    uint32_t                              i;

    evpl_mutex_lock(&table->lock);

    /* "" names both the rootfs and anything mounted over it: the latter. */
    for (i = 0; i < table->num_buckets; i++) {
        for (entry = table->buckets[i]; entry; entry = entry->next) {
            if (entry->mount->pathlen == (uint32_t) pathlen &&
                memcmp(entry->mount->path, path, pathlen) == 0 &&
                (!found || found->builtin)) {
                found = entry->mount;
            }
        }
    }

    evpl_mutex_unlock(&table->lock);

    return found;
} /* chimera_vfs_mount_table_find_exact */


