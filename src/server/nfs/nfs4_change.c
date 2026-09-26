// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdlib.h>
#include <string.h>
#include <uthash.h>
#include "nfs4_change.h"

struct nfs4_change_entry {
    uint8_t        fh[CHIMERA_VFS_FH_SIZE];
    uint32_t       len;
    uint64_t       raw, value;
    bool           valid;
    UT_hash_handle hh;
};
struct nfs4_change_table {
    evpl_mutex_t              lock;
    struct nfs4_change_entry *entries;
};
struct nfs4_change_observation {
    struct nfs4_change_entry       *entry;
    uint64_t                        raw, value;
    struct nfs4_change_observation *next;
};

static uint64_t
raw_change(const struct chimera_vfs_attrs *attrs)
{
    return (attrs->va_set_mask & CHIMERA_VFS_ATTR_CHANGE) ? attrs->va_change :
           (uint64_t) attrs->va_ctime.tv_sec * 1000000000ULL + attrs->va_ctime.tv_nsec;
} /* raw_change */

struct nfs4_change_table *
nfs4_change_table_create(void)
{
    struct nfs4_change_table *table = calloc(1, sizeof(*table));

    if (table) {
        evpl_mutex_init(&table->lock, NULL);
    }
    return table;
} /* nfs4_change_table_create */

void
nfs4_change_table_free(struct nfs4_change_table *table)
{
    if (!table) {
        return;
    }
    struct nfs4_change_entry *entry, *tmp;
    HASH_ITER(hh, table->entries, entry, tmp)
    {
        HASH_DEL(table->entries, entry);
        free(entry);
    }
    evpl_mutex_destroy(&table->lock);
    free(table);
} /* nfs4_change_table_free */

bool
nfs4_change_register(
    struct nfs4_change_table *table,
    const uint8_t            *fh,
    uint32_t                  len)
{
    struct nfs4_change_entry *entry;

    if (!table || !len || len > CHIMERA_VFS_FH_SIZE) {
        return false;
    }
    evpl_mutex_lock(&table->lock);
    HASH_FIND(hh, table->entries, fh, len, entry);
    if (!entry && HASH_COUNT(table->entries) < NFS4_CHANGE_FLOOR_LIMIT) {
        entry = calloc(1, sizeof(*entry));
        if (entry) {
            memcpy(entry->fh, fh, len);
            entry->len = len;
            HASH_ADD_KEYPTR(hh, table->entries, entry->fh, len, entry);
        }
    }
    evpl_mutex_unlock(&table->lock);
    return entry != NULL;
} /* nfs4_change_register */

nfsstat4
nfs4_change_project(
    struct nfs4_change_table        *table,
    const uint8_t                   *fh,
    uint32_t                         len,
    struct chimera_vfs_attrs        *attrs,
    struct nfs4_change_observation **pending,
    struct nfs4_change_observation **observation)
{
    struct nfs4_change_entry *entry;
    uint64_t                  base, floor, raw, value;
    bool                      valid;
    nfsstat4                  status = NFS4_OK;

    *observation = NULL;
    if (!table || !len || !(attrs->va_set_mask & (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME))) {
        return NFS4_OK;
    }
    raw = raw_change(attrs);
    evpl_mutex_lock(&table->lock);
    HASH_FIND(hh, table->entries, fh, len, entry);
    if (!entry) {
        evpl_mutex_unlock(&table->lock);
        return NFS4_OK;
    }
    base  = entry->raw;
    floor = entry->value;
    valid = entry->valid;
    evpl_mutex_unlock(&table->lock);
    /* Earlier operations in this attempt have not published their version. */
    for (struct nfs4_change_observation *o = *pending; o; o = o->next) {
        if (o->entry == entry && (!valid || o->value >= floor)) {
            base  = o->raw;
            floor = o->value;
            valid = true;
            break;
        }
    }
    value = raw;
    if (valid) {
        uint64_t delta = raw >= base ? raw - base : 1;
        if (delta > UINT64_MAX - floor) {
            return NFS4ERR_RESOURCE;
        }
        value = floor + delta;
        if (raw < base) {
            /* Do not give two concurrent reset observations the same visible
             * token. Accept a private rebase with DELAY; a fresh attempt then
             * sees the new baseline and preserves subsequent raw increments. */
            status = NFS4ERR_DELAY;
        }
    }
    struct nfs4_change_observation *o = calloc(1, sizeof(*o));
    if (!o) {
        return NFS4ERR_RESOURCE;
    }
    o->entry = entry;
    o->raw   = raw;
    o->value = value;
    o->next  = *pending;
    *pending = *observation = o;
    if (status == NFS4_OK) {
        attrs->va_change    = value;
        attrs->va_set_mask |= CHIMERA_VFS_ATTR_CHANGE;
    }
    return status;
} /* nfs4_change_project */

void
nfs4_change_observe(
    struct nfs4_change_observation *observation,
    const struct chimera_vfs_attrs *attrs)
{
    if (observation) {
        observation->value = raw_change(attrs);
    }
} /* nfs4_change_observe */

void
nfs4_change_finish(
    struct nfs4_change_table        *table,
    struct nfs4_change_observation **pending,
    bool                             accepted)
{
    while (*pending) {
        struct nfs4_change_observation *o = *pending;
        *pending = o->next;
        if (accepted) {
            evpl_mutex_lock(&table->lock);
            if (!o->entry->valid || o->value > o->entry->value) {
                o->entry->raw   = o->raw;
                o->entry->value = o->value;
                o->entry->valid = true;
            }
            evpl_mutex_unlock(&table->lock);
        }
        free(o);
    }
} /* nfs4_change_finish */
