// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <utlist.h>

#include <sys/mman.h>

#include "evpl/evpl.h"

struct diskfs_slab {
    void               *buffer;
    uint64_t            size;
    uint64_t            used;
    struct diskfs_slab *next;
};

struct diskfs_element {
    void                  *buffer;
    struct diskfs_element *next;
} __attribute__((aligned(8)));

struct diskfs_bucket {
    struct diskfs_element *elements;
};

struct slab_allocator {
    struct diskfs_slab    *slabs;
    struct diskfs_bucket  *buckets;
    struct diskfs_element *free_elements;
    uint64_t               slab_size;
    uint64_t               max_element_size;
};


static struct slab_allocator *
slab_allocator_create(
    uint64_t max_element_size,
    uint64_t slab_size)
{
    struct slab_allocator *allocator = calloc(1, sizeof(struct slab_allocator));
    uint64_t               max_bucket_id;

    if (!allocator) {
        return NULL;
    }

    allocator->slab_size        = slab_size;
    allocator->max_element_size = max_element_size;

    max_bucket_id = ((max_element_size + 7) & ~7) >> 3;

    allocator->slabs   = NULL;
    allocator->buckets = calloc(max_bucket_id + 1, sizeof(struct diskfs_bucket));

    return allocator;
} /* slab_allocator_create */

static void
slab_allocator_destroy(struct slab_allocator *allocator)
{
    struct diskfs_slab *slab;

    while (allocator->slabs) {
        slab = allocator->slabs;
        LL_DELETE(allocator->slabs, slab);
        free(slab);
    }

    free(allocator->buckets);
    free(allocator);
} /* slab_allocator_destroy */

static void *
slab_allocator_alloc_new_chunk(
    struct slab_allocator *allocator,
    uint64_t               size)
{
    struct diskfs_slab *slab = NULL;
    void               *ptr;

    if (allocator->slabs) {
        slab = allocator->slabs;
        if (slab->used + size > slab->size) {
            slab = NULL;
        }
    }

    if (!slab) {
        void *slab_private;

        slab = calloc(1, sizeof(*slab));

        slab->buffer = evpl_slab_alloc(&slab_private);

        slab->size = allocator->slab_size;
        slab->used = 0;
        LL_PREPEND(allocator->slabs, slab);
    }

    ptr         = slab->buffer + slab->used;
    slab->used += size;

    return ptr;
} /* slab_allocator_alloc_new_chunk */

void *
slab_allocator_alloc(
    struct slab_allocator *allocator,
    uint64_t               size)
{
    uint64_t               asize     = (size + 7) & ~7;
    uint64_t               bucket_id = asize >> 3;
    struct diskfs_bucket  *bucket    = &allocator->buckets[bucket_id];
    struct diskfs_element *element;
    void                  *ptr;

    if (bucket->elements) {
        element = bucket->elements;
        LL_DELETE(bucket->elements, element);

        ptr = element->buffer;
        LL_PREPEND(allocator->free_elements, element);

        return ptr;
    }

    return slab_allocator_alloc_new_chunk(allocator, asize);
} /* slab_allocator_alloc */

static void *
slab_allocator_alloc_perm(
    struct slab_allocator *allocator,
    uint64_t               size)
{
    struct diskfs_slab *slab = NULL;
    void               *ptr;
    uint64_t            pad;

    if (allocator->slabs) {
        slab = allocator->slabs;

        pad = 64 - (slab->used & 63);

        if (slab->used + size + pad > slab->size) {
            slab = NULL;
        }
    }

    if (!slab) {
        void *slab_private;

        slab = calloc(1, sizeof(*slab));

        slab->buffer = evpl_slab_alloc(&slab_private);

        slab->size = allocator->slab_size;
        slab->used = 0;
        LL_PREPEND(allocator->slabs, slab);

        pad = 0;
    }

    ptr         = slab->buffer + slab->used + pad;
    slab->used += size + pad;

    return ptr;
} /* slab_allocator_alloc_perm */

static void
slab_allocator_free(
    struct slab_allocator *allocator,
    void                  *ptr,
    uint64_t               size)
{
    uint64_t               bucket_id = ((size + 7) & ~7) >> 3;
    struct diskfs_bucket  *bucket    = &allocator->buckets[bucket_id];
    struct diskfs_element *element;

    if (allocator->free_elements) {
        element = allocator->free_elements;
        LL_DELETE(allocator->free_elements, element);
    } else {
        element = slab_allocator_alloc_new_chunk(allocator, sizeof(struct diskfs_element));
    }

    element->buffer = ptr;
    LL_PREPEND(bucket->elements, element);

} /* slab_allocator_free */
