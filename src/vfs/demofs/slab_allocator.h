#pragma once

#include <sys/mman.h>
#include "uthash/utlist.h"

struct demofs_slab {
    void               *buffer;
    uint64_t            size;
    uint64_t            used;
    uint32_t            is_mmap;
    struct demofs_slab *next;
};

struct demofs_element {
    void                  *buffer;
    struct demofs_element *next;
};

struct demofs_bucket {
    struct demofs_element *elements;
};

struct slab_allocator {
    struct demofs_slab    *slabs;
    struct demofs_bucket  *buckets;
    struct demofs_element *free_elements;
    uint64_t               slab_size;
    uint64_t               max_element_size;
    uint64_t               max_memory;
};


static struct slab_allocator *
slab_allocator_create(
    uint64_t max_memory,
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
    allocator->max_memory       = max_memory;

    max_bucket_id = ((max_element_size + 7) & ~7) >> 3;

    allocator->slabs   = NULL;
    allocator->buckets = calloc(max_bucket_id, sizeof(struct demofs_bucket));

    return allocator;
} /* slab_allocator_create */

static void
slab_allocator_destroy(struct slab_allocator *allocator)
{
    struct demofs_slab *slab;

    while (allocator->slabs) {
        slab = allocator->slabs;
        LL_DELETE(allocator->slabs, slab);
        if (slab->is_mmap) {
            munmap(slab->buffer, slab->size);
        } else {
            free(slab->buffer);
        }
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
    struct demofs_slab *slab = NULL;
    void               *ptr;

    if (allocator->slabs) {
        slab = allocator->slabs;
        if (slab->used + size > slab->size) {
            slab = NULL;
        }
    }

    if (!slab) {
        slab = calloc(1, sizeof(*slab));

        slab->buffer = mmap(NULL, allocator->slab_size,
                            PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                            -1, 0);

        if (slab->buffer == MAP_FAILED) {
            slab->buffer = malloc(allocator->slab_size);
            if (!slab->buffer) {
                free(slab);
                return NULL;
            }
            slab->is_mmap = 0;
        } else {
            slab->is_mmap = 1;
        }

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
    uint64_t               bucket_id = ((size + 7) & ~7) >> 3;
    struct demofs_bucket  *bucket    = &allocator->buckets[bucket_id];
    struct demofs_element *element;
    void                  *ptr;

    if (bucket->elements) {
        element = bucket->elements;
        LL_DELETE(bucket->elements, element);

        ptr = element->buffer;
        LL_PREPEND(allocator->free_elements, element);

        return ptr;
    }

    return slab_allocator_alloc_new_chunk(allocator, size);
} /* slab_allocator_alloc */

void
slab_allocator_free(
    struct slab_allocator *allocator,
    void                  *ptr,
    uint64_t               size)
{
    uint64_t               bucket_id = ((size + 7) & ~7) >> 3;
    struct demofs_bucket  *bucket    = &allocator->buckets[bucket_id];
    struct demofs_element *element;

    if (allocator->free_elements) {
        element = allocator->free_elements;
        LL_DELETE(allocator->free_elements, element);
    } else {
        element = slab_allocator_alloc_new_chunk(allocator, sizeof(struct demofs_element));
    }

    element->buffer = ptr;
    LL_PREPEND(bucket->elements, element);

} /* slab_allocator_free */
