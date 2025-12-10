// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>
#include <stdlib.h>

#include "evpl/evpl.h"

struct evpl_iovec_cursor {
    struct evpl_iovec *iov;
    int                offset;
    int                consumed;
    int                niov;
};


static inline void
evpl_iovec_cursor_init(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    int                       niov)
{
    cursor->iov      = iov;
    cursor->niov     = niov;
    cursor->consumed = 0;
    cursor->offset   = 0;
} /* evpl_iovec_cursor_init */


static inline int
evpl_iovec_cursor_get_blob(
    struct evpl_iovec_cursor *cursor,
    void                     *blob,
    int                       length)
{
    int   chunk, left = length;
    void *ptr = blob;

    while (left && cursor->niov) {
        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        memcpy(ptr, cursor->iov->data + cursor->offset, chunk);

        ptr  += chunk;
        left -= chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        return -1;
    }

    return 0;
} /* evpl_iovec_cursor_get_blob */

static void
evpl_iovec_cursor_copy(
    struct evpl_iovec_cursor *cursor,
    void                     *out,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        memcpy(out, cursor->iov->data + cursor->offset, chunk);

        left -= chunk;
        out  += chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;
        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_copy */

static inline void
evpl_iovec_cursor_skip(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        left -= chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_skip */

static void
evpl_iovec_cursor_zero(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        memset(cursor->iov->data + cursor->offset, 0, chunk);

        left -= chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_zero */

static inline void *
evpl_iovec_cursor_data(struct evpl_iovec_cursor *cursor)
{
    return cursor->iov->data + cursor->offset;
} /* evpl_iovec_cursor_data */

static inline void
evpl_iovec_cursor_get_uint8(
    struct evpl_iovec_cursor *cursor,
    uint8_t                  *value)
{
    evpl_iovec_cursor_copy(cursor, value, sizeof(uint8_t));
} /* evpl_iovec_cursor_get_uint8 */


static inline void
evpl_iovec_cursor_get_uint16(
    struct evpl_iovec_cursor *cursor,
    uint16_t                 *value)
{
    evpl_iovec_cursor_skip(cursor, (2 - (cursor->consumed & 1)) & 1);
    evpl_iovec_cursor_copy(cursor, value, sizeof(uint16_t));
} /* evpl_iovec_cursor_get_uint16 */

static inline void
evpl_iovec_cursor_get_uint32(
    struct evpl_iovec_cursor *cursor,
    uint32_t                 *value)
{
    evpl_iovec_cursor_skip(cursor, (4 - (cursor->consumed & 3)) & 3);
    evpl_iovec_cursor_copy(cursor, value, sizeof(uint32_t));
} /* evpl_iovec_cursor_get_uint32 */

static inline void
evpl_iovec_cursor_get_uint64(
    struct evpl_iovec_cursor *cursor,
    uint64_t                 *value)
{
    evpl_iovec_cursor_skip(cursor, (8 - (cursor->consumed & 7)) & 7);
    evpl_iovec_cursor_copy(cursor, value, sizeof(uint64_t));
} /* evpl_iovec_cursor_get_uint64 */

static inline void
evpl_iovec_cursor_append_uint8(
    struct evpl_iovec_cursor *cursor,
    uint8_t                   value)
{
    *((uint8_t *) evpl_iovec_cursor_data(cursor)) = value;
    evpl_iovec_cursor_skip(cursor, sizeof(uint8_t));
} /* evpl_iovec_cursor_append_uint8 */

static inline void
evpl_iovec_cursor_append_uint16(
    struct evpl_iovec_cursor *cursor,
    uint16_t                  value)
{
    evpl_iovec_cursor_zero(cursor, (2 - (cursor->consumed & 1)) & 1);
    *((uint16_t *) evpl_iovec_cursor_data(cursor)) = value;
    evpl_iovec_cursor_skip(cursor, sizeof(uint16_t));
} /* evpl_iovec_cursor_append_uint16 */

static inline void
evpl_iovec_cursor_append_uint32(
    struct evpl_iovec_cursor *cursor,
    uint32_t                  value)
{
    evpl_iovec_cursor_zero(cursor, (4 - (cursor->consumed & 3)) & 3);
    *((uint32_t *) evpl_iovec_cursor_data(cursor)) = value;
    evpl_iovec_cursor_skip(cursor, sizeof(uint32_t));
} /* evpl_iovec_cursor_append_uint32 */

static inline void
evpl_iovec_cursor_append_uint64(
    struct evpl_iovec_cursor *cursor,
    uint64_t                  value)
{
    evpl_iovec_cursor_zero(cursor, (8 - (cursor->consumed & 7)) & 7);
    *((uint64_t *) evpl_iovec_cursor_data(cursor)) = value;
    evpl_iovec_cursor_skip(cursor, sizeof(uint64_t));
} /* evpl_iovec_cursor_append_uint64 */

static inline void
evpl_iovec_cursor_append_blob_unaligned(
    struct evpl_iovec_cursor *cursor,
    void                     *blob,
    int                       length)
{
    memcpy(evpl_iovec_cursor_data(cursor), blob, length);
    evpl_iovec_cursor_skip(cursor, length);
} /* evpl_iovec_cursor_append_blob */


static inline void
evpl_iovec_cursor_append_blob(
    struct evpl_iovec_cursor *cursor,
    void                     *blob,
    int                       length)
{
    evpl_iovec_cursor_skip(cursor, (4 - (cursor->consumed & 3)) & 3);
    evpl_iovec_cursor_append_blob_unaligned(cursor, blob, length);
} /* evpl_iovec_cursor_append_blob */

static int
evpl_iovec_cursor_move(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    int                       maxiov,
    int                       length,
    int                       addrefs)
{
    int chunk, left = length, niov = 0;

    while (left && cursor->niov && niov < maxiov) {
        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        iov[niov].data         = cursor->iov->data + cursor->offset;
        iov[niov].length       = chunk;
        iov[niov].private_data = cursor->iov->private_data;

        if (addrefs) {
            evpl_iovec_addref(&iov[niov]);
        }

        niov++;
        left -= chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    return niov;
} /* evpl_iovec_cursor_move */

static inline void
evpl_iovec_cursor_inject_unaligned(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    int                       niov,
    int                       length)
{
    struct evpl_iovec saved = *cursor->iov;

    cursor->iov->length = cursor->offset;

    saved.data   += cursor->offset;
    saved.length -= cursor->offset;

    cursor->offset = 0;

    cursor->iov++;

    memcpy(cursor->iov, iov, niov * sizeof(struct evpl_iovec));
    cursor->iov  += niov;
    cursor->niov += niov;

    *cursor->iov = saved;

} /* evpl_iovec_cursor_inject */


static inline void
evpl_iovec_cursor_inject(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    int                       niov,
    int                       length)
{
    evpl_iovec_cursor_zero(cursor, (8 - (cursor->consumed & 7)) & 7);

    evpl_iovec_cursor_inject_unaligned(cursor, iov, niov, length);
    cursor->consumed += length;
} /* evpl_iovec_cursor_inject */

static inline int
evpl_iovec_cursor_consumed(struct evpl_iovec_cursor *cursor)
{
    return cursor->consumed;
} /* evpl_iovec_cursor_consumed */

static inline void
evpl_iovec_cursor_reset_consumed(struct evpl_iovec_cursor *cursor)
{
    cursor->consumed = 0;
} /* evpl_iovec_cursor_reset_consumed */

static void
evpl_iovec_cursor_align64(struct evpl_iovec_cursor *cursor)
{

    if (cursor->consumed & 7) {
        evpl_iovec_cursor_skip(cursor, 8 - (cursor->consumed & 7));
    }
} /* evpl_iovec_cursor_align */