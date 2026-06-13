// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * memkv: an in-memory, key-value-only VFS backend (CHIMERA_VFS_CAP_KV, no
 * filesystem).  It is the default backing store for the VFS KV API and for
 * handle-state records of filesystem backends that cannot persist KV natively.
 *
 * Storage is a fixed set of hash-sharded red-black trees keyed by
 * chimera_vfs_hash(key); each shard has its own mutex so unrelated keys do not
 * contend.  This is the same design that previously lived inside memfs.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <jansson.h>
#include <utlist.h>

#include "common/rbtree.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "memkv.h"
#include "common/logging.h"
#include "common/macros.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#define CHIMERA_MEMKV_DEFAULT_SHARDS 256

#define chimera_memkv_error(...) chimera_error("memkv", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memkv_abort_if(cond, ...) \
        chimera_abort_if(cond, "memkv", __FILE__, __LINE__, __VA_ARGS__)

struct memkv_entry {
    uint64_t            hash;
    uint32_t            key_len;
    uint32_t            value_len;
    struct rb_node      node;
    struct memkv_entry *next;
    void               *key;
    void               *value;
};

struct memkv_shard {
    struct rb_tree  entries;
    pthread_mutex_t lock;
};

struct memkv_shared {
    struct memkv_shard *shards;
    int                 num_shards;
};

struct memkv_thread {
    struct memkv_shared *shared;
    struct memkv_entry  *free_entry;
};

static inline struct memkv_entry *
memkv_entry_alloc(
    struct memkv_thread *thread,
    uint64_t             hash,
    const void          *key,
    uint32_t             key_len,
    const void          *value,
    uint32_t             value_len)
{
    struct memkv_entry *entry;

    entry = thread->free_entry;

    if (entry) {
        LL_DELETE(thread->free_entry, entry);
        /* Free old key/value if reusing */
        free(entry->key);
        free(entry->value);
    } else {
        entry = malloc(sizeof(*entry));
    }

    entry->hash      = hash;
    entry->key_len   = key_len;
    entry->value_len = value_len;
    entry->key       = malloc(key_len);
    entry->value     = malloc(value_len);
    memcpy(entry->key, key, key_len);
    memcpy(entry->value, value, value_len);

    return entry;
} /* memkv_entry_alloc */

static inline void
memkv_entry_free(
    struct memkv_thread *thread,
    struct memkv_entry  *entry)
{
    LL_PREPEND(thread->free_entry, entry);
} /* memkv_entry_free */

static void
memkv_entry_release(
    struct rb_node *node,
    void           *private_data)
{
    struct memkv_entry *entry = container_of(node, struct memkv_entry, node);

    free(entry->key);
    free(entry->value);
    free(entry);
} /* memkv_entry_release */

static void *
memkv_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct memkv_shared *shared = calloc(1, sizeof(*shared));
    int                  i;

    shared->num_shards = CHIMERA_MEMKV_DEFAULT_SHARDS;

    if (cfgdata && cfgdata[0] != '\0') {
        json_error_t json_error;
        json_t      *cfg = json_loads(cfgdata, 0, &json_error);

        chimera_memkv_abort_if(!cfg, "Failed to parse memkv config: %s",
                               json_error.text);

        json_t      *shards = json_object_get(cfg, "num_shards");
        if (shards && json_is_integer(shards)) {
            json_int_t v = json_integer_value(shards);
            if (v > 0 && v <= 65536) {
                shared->num_shards = (int) v;
            }
        }

        json_decref(cfg);
    }

    shared->shards = calloc(shared->num_shards, sizeof(*shared->shards));

    for (i = 0; i < shared->num_shards; i++) {
        rb_tree_init(&shared->shards[i].entries);
        pthread_mutex_init(&shared->shards[i].lock, NULL);
    }

    return shared;
} /* memkv_init */

static void
memkv_destroy(void *private_data)
{
    struct memkv_shared *shared = private_data;
    int                  i;

    for (i = 0; i < shared->num_shards; i++) {
        rb_tree_destroy(&shared->shards[i].entries, memkv_entry_release, NULL);
        pthread_mutex_destroy(&shared->shards[i].lock);
    }
    free(shared->shards);
    free(shared);
} /* memkv_destroy */

static void *
memkv_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct memkv_shared *shared = private_data;
    struct memkv_thread *thread = calloc(1, sizeof(*thread));

    (void) evpl;
    thread->shared = shared;

    return thread;
} /* memkv_thread_init */

static void
memkv_thread_destroy(void *private_data)
{
    struct memkv_thread *thread = private_data;
    struct memkv_entry  *entry;

    while (thread->free_entry) {
        entry = thread->free_entry;
        LL_DELETE(thread->free_entry, entry);
        free(entry->key);
        free(entry->value);
        free(entry);
    }

    free(thread);
} /* memkv_thread_destroy */

static void
memkv_put_key(
    struct memkv_thread        *thread,
    struct memkv_shared        *shared,
    struct chimera_vfs_request *request)
{
    uint64_t            hash;
    int                 shard_idx;
    struct memkv_shard *shard;
    struct memkv_entry *entry, *existing;

    hash      = chimera_vfs_hash(request->put_key.key, request->put_key.key_len);
    shard_idx = hash % shared->num_shards;
    shard     = &shared->shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, existing);

    if (existing) {
        free(existing->value);
        existing->value_len = request->put_key.value_len;
        existing->value     = malloc(request->put_key.value_len);
        memcpy(existing->value, request->put_key.value, request->put_key.value_len);
    } else {
        entry = memkv_entry_alloc(thread, hash,
                                  request->put_key.key, request->put_key.key_len,
                                  request->put_key.value, request->put_key.value_len);
        rb_tree_insert(&shard->entries, hash, entry);
    }

    pthread_mutex_unlock(&shard->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memkv_put_key */

static void
memkv_get_key(
    struct memkv_thread        *thread,
    struct memkv_shared        *shared,
    struct chimera_vfs_request *request)
{
    uint64_t            hash;
    int                 shard_idx;
    struct memkv_shard *shard;
    struct memkv_entry *entry;

    hash      = chimera_vfs_hash(request->get_key.key, request->get_key.key_len);
    shard_idx = hash % shared->num_shards;
    shard     = &shared->shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Return pointer to value - caller must use before callback returns */
    request->get_key.r_value     = entry->value;
    request->get_key.r_value_len = entry->value_len;

    pthread_mutex_unlock(&shard->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memkv_get_key */

static void
memkv_delete_key(
    struct memkv_thread        *thread,
    struct memkv_shared        *shared,
    struct chimera_vfs_request *request)
{
    uint64_t            hash;
    int                 shard_idx;
    struct memkv_shard *shard;
    struct memkv_entry *entry;

    hash      = chimera_vfs_hash(request->delete_key.key, request->delete_key.key_len);
    shard_idx = hash % shared->num_shards;
    shard     = &shared->shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    rb_tree_remove(&shard->entries, &entry->node);

    pthread_mutex_unlock(&shard->lock);

    memkv_entry_free(thread, entry);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memkv_delete_key */

static int
memkv_key_in_range(
    const void *key,
    uint32_t    key_len,
    const void *start_key,
    uint32_t    start_key_len,
    const void *end_key,
    uint32_t    end_key_len,
    uint32_t    flags)
{
    int cmp;

    /* Compare key to start_key */
    if (start_key_len > 0) {
        cmp = memcmp(key, start_key,
                     key_len < start_key_len ? key_len : start_key_len);
        if (cmp < 0 || (cmp == 0 && key_len < start_key_len)) {
            return 0; /* key < start_key */
        }
    }

    /* Compare key to end_key */
    if (end_key_len > 0) {
        cmp = memcmp(key, end_key,
                     key_len < end_key_len ? key_len : end_key_len);
        if (cmp > 0 || (cmp == 0 && key_len > end_key_len)) {
            return 0; /* key > end_key */
        }
        if ((flags & CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE) &&
            cmp == 0 && key_len == end_key_len) {
            return 0; /* key == end_key, end is exclusive */
        }
    }

    return 1; /* key is in range */
} /* memkv_key_in_range */

/* One matched key/value, copied out from under the shard lock so the search
 * can sort across shards and invoke the callback without holding any lock (the
 * callback is caller-supplied and may re-enter the backend). */
struct memkv_search_item {
    void    *key;
    uint32_t key_len;
    void    *value;
    uint32_t value_len;
};

static int
memkv_search_item_cmp(
    const void *a,
    const void *b)
{
    const struct memkv_search_item *ia  = a;
    const struct memkv_search_item *ib  = b;
    uint32_t                        min = ia->key_len < ib->key_len ? ia->key_len : ib->key_len;
    int                             cmp = memcmp(ia->key, ib->key, min);

    if (cmp) {
        return cmp;
    }
    /* Shorter key (a byte-prefix of the other) sorts first. */
    return (ia->key_len > ib->key_len) - (ia->key_len < ib->key_len);
} /* memkv_search_item_cmp */

static void
memkv_search_keys(
    struct memkv_thread        *thread,
    struct memkv_shared        *shared,
    struct chimera_vfs_request *request)
{
    int                                i;
    size_t                             n = 0, cap = 0, k;
    struct memkv_shard                *shard;
    struct memkv_entry                *entry;
    struct memkv_search_item          *items = NULL, *grown;
    chimera_vfs_search_keys_callback_t callback = request->search_keys.callback;
    enum chimera_vfs_error             status   = CHIMERA_VFS_OK;

    (void) thread;

    /* Entries are stored hash-ordered (sharded by hash), so collect every
     * in-range match across all shards, then sort by key to return results in
     * the backend's natural (lexicographic) key order.  Keys and values are
     * copied so the callback runs without any shard lock held. */
    for (i = 0; i < shared->num_shards; i++) {
        shard = &shared->shards[i];

        pthread_mutex_lock(&shard->lock);

        rb_tree_first(&shard->entries, entry);

        while (entry) {
            if (memkv_key_in_range(entry->key,
                                   entry->key_len,
                                   request->search_keys.start_key,
                                   request->search_keys.start_key_len,
                                   request->search_keys.end_key,
                                   request->search_keys.end_key_len,
                                   request->search_keys.flags)) {
                struct memkv_search_item *item;

                if (n == cap) {
                    cap   = cap ? cap * 2 : 16;
                    grown = realloc(items, cap * sizeof(*items));
                    if (!grown) {
                        pthread_mutex_unlock(&shard->lock);
                        status = CHIMERA_VFS_EIO;
                        goto out;
                    }
                    items = grown;
                }

                item            = &items[n];
                item->key_len   = entry->key_len;
                item->value_len = entry->value_len;
                item->key       = malloc(entry->key_len);
                item->value     = entry->value_len ? malloc(entry->value_len) : NULL;

                if (!item->key || (entry->value_len && !item->value)) {
                    free(item->key);
                    free(item->value);
                    pthread_mutex_unlock(&shard->lock);
                    status = CHIMERA_VFS_EIO;
                    goto out;
                }

                memcpy(item->key, entry->key, entry->key_len);
                if (entry->value_len) {
                    memcpy(item->value, entry->value, entry->value_len);
                }
                n++;
            }

            entry = rb_tree_next(&shard->entries, entry);
        }

        pthread_mutex_unlock(&shard->lock);
    }

    if (n > 1) {
        qsort(items, n, sizeof(*items), memkv_search_item_cmp);
    }

    for (k = 0; k < n; k++) {
        if (callback(items[k].key, items[k].key_len,
                     items[k].value, items[k].value_len,
                     request->proto_private_data)) {
            break; /* caller aborted the search */
        }
    }

 out:
    for (k = 0; k < n; k++) {
        free(items[k].key);
        free(items[k].value);
    }
    free(items);

    request->status = status;
    request->complete(request);
} /* memkv_search_keys */

static void
memkv_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memkv_thread *thread = private_data;
    struct memkv_shared *shared = thread->shared;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_PUT_KEY:
            memkv_put_key(thread, shared, request);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            memkv_get_key(thread, shared, request);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            memkv_delete_key(thread, shared, request);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            memkv_search_keys(thread, shared, request);
            break;
        default:
            chimera_memkv_error("memkv_dispatch: unsupported operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* memkv_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_memkv = {
    .name           = "memkv",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_MEMKV,
    .capabilities   = CHIMERA_VFS_CAP_KV,
    .init           = memkv_init,
    .destroy        = memkv_destroy,
    .thread_init    = memkv_thread_init,
    .thread_destroy = memkv_thread_destroy,
    .dispatch       = memkv_dispatch,
};
