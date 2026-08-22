// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <uthash.h>

#include "fuse_node_table.h"
#include "common/logging.h"
#include "vfs/vfs_attrs.h"

#define chimera_fuse_node_abort_if(cond, ...) \
        chimera_abort_if(cond, "fuse", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_fuse_node {
    uint64_t       nodeid;
    uint64_t       lookup_count;
    uint32_t       fh_len;
    uint8_t        fh[CHIMERA_VFS_FH_SIZE];
    UT_hash_handle hh_id;
    UT_hash_handle hh_fh;
};

struct chimera_fuse_node_table {
    pthread_mutex_t           lock;
    struct chimera_fuse_node *by_id;
    struct chimera_fuse_node *by_fh;
    uint64_t                  next_nodeid;
};

struct chimera_fuse_node_table *
chimera_fuse_node_table_create(void)
{
    struct chimera_fuse_node_table *table = calloc(1, sizeof(*table));

    pthread_mutex_init(&table->lock, NULL);

    /* FUSE_ROOT_ID (1) is reserved for the mount root. */
    table->next_nodeid = 2;

    return table;
} /* chimera_fuse_node_table_create */

void
chimera_fuse_node_table_destroy(struct chimera_fuse_node_table *table)
{
    struct chimera_fuse_node *node, *tmp;

#ifndef __clang_analyzer__
    /* uthash blows clangs mind */
    HASH_ITER(hh_id, table->by_id, node, tmp)
    {
        HASH_DELETE(hh_id, table->by_id, node);
        HASH_DELETE(hh_fh, table->by_fh, node);
        free(node);
    }
#endif /* ifndef __clang_analyzer__ */

    pthread_mutex_destroy(&table->lock);

    free(table);
} /* chimera_fuse_node_table_destroy */

uint64_t
chimera_fuse_node_insert(
    struct chimera_fuse_node_table *table,
    const uint8_t                  *fh,
    uint32_t                        fh_len)
{
    struct chimera_fuse_node *node;
    uint64_t                  nodeid;

    chimera_fuse_node_abort_if(fh_len > CHIMERA_VFS_FH_SIZE,
                               "fuse node insert: fh_len %u exceeds max", fh_len);

    pthread_mutex_lock(&table->lock);

    HASH_FIND(hh_fh, table->by_fh, fh, fh_len, node);

    if (!node) {
        node = calloc(1, sizeof(*node));

        node->nodeid = table->next_nodeid++;
        node->fh_len = fh_len;
        memcpy(node->fh, fh, fh_len);

        HASH_ADD(hh_id, table->by_id, nodeid, sizeof(node->nodeid), node);
        HASH_ADD_KEYPTR(hh_fh, table->by_fh, node->fh, node->fh_len, node);
    }

    node->lookup_count++;

    nodeid = node->nodeid;

    pthread_mutex_unlock(&table->lock);

    return nodeid;
} /* chimera_fuse_node_insert */

int
chimera_fuse_node_get_fh(
    struct chimera_fuse_node_table *table,
    uint64_t                        nodeid,
    uint8_t                        *fh_out,
    uint32_t                       *fh_len_out)
{
    struct chimera_fuse_node *node;

    pthread_mutex_lock(&table->lock);

    HASH_FIND(hh_id, table->by_id, &nodeid, sizeof(nodeid), node);

    if (!node) {
        pthread_mutex_unlock(&table->lock);
        return -1;
    }

    memcpy(fh_out, node->fh, node->fh_len);
    *fh_len_out = node->fh_len;

    pthread_mutex_unlock(&table->lock);

    return 0;
} /* chimera_fuse_node_get_fh */

void
chimera_fuse_node_forget(
    struct chimera_fuse_node_table *table,
    uint64_t                        nodeid,
    uint64_t                        nlookup)
{
    struct chimera_fuse_node *node;

    pthread_mutex_lock(&table->lock);

    HASH_FIND(hh_id, table->by_id, &nodeid, sizeof(nodeid), node);

    /* A FORGET for an unknown nodeid is kernel/daemon count drift; there is
     * nothing to reply to, so note it and move on. */
    if (!node) {
        pthread_mutex_unlock(&table->lock);
        chimera_error("fuse", __FILE__, __LINE__,
                      "fuse forget for unknown nodeid %llu",
                      (unsigned long long) nodeid);
        return;
    }

    if (nlookup >= node->lookup_count) {
        HASH_DELETE(hh_id, table->by_id, node);
        HASH_DELETE(hh_fh, table->by_fh, node);
        free(node);
    } else {
        node->lookup_count -= nlookup;
    }

    pthread_mutex_unlock(&table->lock);
} /* chimera_fuse_node_forget */
