// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>

#include <uthash.h>
#include <utlist.h>
#include <evpl/evpl.h>

#include "smb_account_cache.h"

#define ACCOUNT_CACHE_KEY_MAX    (2 * CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX + 2)
#define ACCOUNT_CACHE_NS_PER_SEC 1000000000LL

struct account_cache_entry {
    char                                key[ACCOUNT_CACHE_KEY_MAX];
    struct chimera_vfs_identity_account acct;
    int64_t                             expires; /* account_cache_now() ns */
    UT_hash_handle                      hh;
    struct account_cache_entry         *prev;
    struct account_cache_entry         *next;
};

/* One keyed table: hash for lookup, list in insert order for FIFO eviction. */
struct account_cache_table {
    struct account_cache_entry *hash;
    struct account_cache_entry *fifo;
    int                         count;
};

struct chimera_smb_account_cache {
    evpl_mutex_t               lock;
    int                        max_entries;
    int                        ttl;
    struct account_cache_table by_sid;
    struct account_cache_table by_name;
};

/* Monotonic time in nanoseconds.  Whole seconds would let a TTL end up to a
 * second early: an entry put at 0.9 s with a 1 s TTL would expire at 1.0 s. */
static int64_t
account_cache_now(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * ACCOUNT_CACHE_NS_PER_SEC + ts.tv_nsec;
} /* account_cache_now */

static void
account_cache_remove(
    struct account_cache_table *table,
    struct account_cache_entry *entry)
{
    HASH_DEL(table->hash, entry);
    DL_DELETE(table->fifo, entry);
    table->count--;
    free(entry);
} /* account_cache_remove */

static void
account_cache_table_clear(struct account_cache_table *table)
{
    while (table->fifo) {
        account_cache_remove(table, table->fifo);
    }
} /* account_cache_table_clear */

static void
account_cache_table_put(
    struct account_cache_table                *table,
    int                                        max_entries,
    int64_t                                    expires,
    const char                                *key,
    const struct chimera_vfs_identity_account *acct)
{
    struct account_cache_entry *entry;
    size_t                      klen = strlen(key);

    if (klen == 0 || klen >= ACCOUNT_CACHE_KEY_MAX) {
        return;
    }

    HASH_FIND(hh, table->hash, key, klen, entry);
    if (entry) {
        account_cache_remove(table, entry);
    }

    while (table->count >= max_entries && table->fifo) {
        account_cache_remove(table, table->fifo);
    }

    entry = calloc(1, sizeof(*entry));
    if (!entry) {
        return;
    }

    memcpy(entry->key, key, klen);
    entry->acct    = *acct;
    entry->expires = expires;

    HASH_ADD(hh, table->hash, key, klen, entry);
    DL_APPEND(table->fifo, entry);
    table->count++;
} /* account_cache_table_put */

static int
account_cache_table_get(
    struct account_cache_table          *table,
    int64_t                              now,
    const char                          *key,
    struct chimera_vfs_identity_account *out)
{
    struct account_cache_entry *entry;
    size_t                      klen = strlen(key);

    if (klen == 0 || klen >= ACCOUNT_CACHE_KEY_MAX) {
        return 0;
    }

    HASH_FIND(hh, table->hash, key, klen, entry);
    if (!entry) {
        return 0;
    }

    if (now >= entry->expires) {
        account_cache_remove(table, entry);
        return 0;
    }

    *out = entry->acct;
    return 1;
} /* account_cache_table_get */

/* Lower-case (ASCII) `in` into `out`; returns 0 when it does not fit. */
static int
account_cache_fold(
    char       *out,
    size_t      out_len,
    const char *in)
{
    size_t i;

    for (i = 0; in[i]; i++) {
        if (i + 1 >= out_len) {
            return 0;
        }
        out[i] = (char) tolower((unsigned char) in[i]);
    }
    out[i] = '\0';
    return 1;
} /* account_cache_fold */


struct chimera_smb_account_cache *
chimera_smb_account_cache_create(
    int max_entries,
    int ttl_seconds)
{
    struct chimera_smb_account_cache *cache;

    cache = calloc(1, sizeof(*cache));
    if (!cache) {
        return NULL;
    }

    cache->max_entries = max_entries > 0 ? max_entries : 1;
    cache->ttl         = ttl_seconds;
    evpl_mutex_init(&cache->lock, NULL);

    return cache;
} /* chimera_smb_account_cache_create */

void
chimera_smb_account_cache_destroy(struct chimera_smb_account_cache *cache)
{
    if (!cache) {
        return;
    }

    account_cache_table_clear(&cache->by_sid);
    account_cache_table_clear(&cache->by_name);
    evpl_mutex_destroy(&cache->lock);
    free(cache);
} /* chimera_smb_account_cache_destroy */

void
chimera_smb_account_cache_put(
    struct chimera_smb_account_cache          *cache,
    const struct chimera_vfs_identity_account *acct,
    const char                                *as_sent)
{
    char    qualified[ACCOUNT_CACHE_KEY_MAX + 1];
    char    key[ACCOUNT_CACHE_KEY_MAX] = { 0 };
    int64_t expires;
    int     n;

    if (acct->domain[0]) {
        n = snprintf(qualified, sizeof(qualified), "%s\\%s", acct->domain, acct->name);
    } else {
        n = snprintf(qualified, sizeof(qualified), "%s", acct->name);
    }

    evpl_mutex_lock(&cache->lock);

    expires = account_cache_now() + (int64_t) cache->ttl * ACCOUNT_CACHE_NS_PER_SEC;

    account_cache_table_put(&cache->by_sid, cache->max_entries, expires, acct->sid, acct);

    if (n > 0 && account_cache_fold(key, sizeof(key), qualified)) {
        account_cache_table_put(&cache->by_name, cache->max_entries, expires, key, acct);
    }

    if (as_sent && account_cache_fold(key, sizeof(key), as_sent)) {
        account_cache_table_put(&cache->by_name, cache->max_entries, expires, key, acct);
    }

    evpl_mutex_unlock(&cache->lock);
} /* chimera_smb_account_cache_put */

int
chimera_smb_account_cache_get_sid(
    struct chimera_smb_account_cache    *cache,
    const char                          *sid,
    struct chimera_vfs_identity_account *out)
{
    int found;

    evpl_mutex_lock(&cache->lock);
    found = account_cache_table_get(&cache->by_sid, account_cache_now(), sid, out);
    evpl_mutex_unlock(&cache->lock);

    return found;
} /* chimera_smb_account_cache_get_sid */

int
chimera_smb_account_cache_get_name(
    struct chimera_smb_account_cache    *cache,
    const char                          *name,
    struct chimera_vfs_identity_account *out)
{
    char key[ACCOUNT_CACHE_KEY_MAX] = { 0 };
    int  found;

    if (!account_cache_fold(key, sizeof(key), name)) {
        return 0;
    }

    evpl_mutex_lock(&cache->lock);
    found = account_cache_table_get(&cache->by_name, account_cache_now(), key, out);
    evpl_mutex_unlock(&cache->lock);

    return found;
} /* chimera_smb_account_cache_get_name */
