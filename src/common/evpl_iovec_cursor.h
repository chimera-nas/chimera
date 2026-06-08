// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>
#include <stdlib.h>
#include <limits.h>

#include "evpl/evpl.h"

struct evpl_iovec_cursor {
    struct evpl_iovec *iov;
    int                offset;
    int                consumed;
    int                niov;
    /* Soft upper bound (in bytes, measured against `consumed`) on how far the
     * checked `_try_*` readers below may advance.  INT_MAX means "unbounded",
     * which is the behaviour every non-SMB caller relies on.  The SMB ingest
     * path clamps this to the current message's window so a malformed request
     * cannot read into the next compound element or off the received segment. */
    int                limit;
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
    cursor->limit    = INT_MAX;
} /* evpl_iovec_cursor_init */

/*
 * Clamp the checked readers to at most `length` bytes beyond the current
 * `consumed` position.  Used by the SMB request parser to fence each compound
 * element to its own [start,end) window.
 */
static inline void
evpl_iovec_cursor_set_limit(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    cursor->limit = cursor->consumed + length;
} /* evpl_iovec_cursor_set_limit */

/* Bytes still readable within the current limit (never negative). */
static inline int
evpl_iovec_cursor_remaining(struct evpl_iovec_cursor *cursor)
{
    int rem = cursor->limit - cursor->consumed;

    return rem > 0 ? rem : 0;
} /* evpl_iovec_cursor_remaining */


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

/*
 * Checked variants of copy/skip/get_uintN for parsing untrusted (client) data.
 *
 * Unlike the helpers above -- which abort() the whole (threaded) process on a
 * short read -- these return -1 when the requested bytes would exceed either the
 * available iovec data or the active limit, so the caller can reject the request
 * cleanly.  On failure the caller abandons the request, so any partial cursor
 * advancement is harmless.  The limit defaults to INT_MAX, so a cursor that never
 * calls set_limit behaves like an ordinary bounds-by-data read.
 */
static inline int
evpl_iovec_cursor_try_copy(
    struct evpl_iovec_cursor *cursor,
    void                     *out,
    int                       length)
{
    if (length < 0 || evpl_iovec_cursor_remaining(cursor) < length) {
        return -1;
    }
    return evpl_iovec_cursor_get_blob(cursor, out, length);
} /* evpl_iovec_cursor_try_copy */

static inline int
evpl_iovec_cursor_try_skip(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    if (length < 0 || evpl_iovec_cursor_remaining(cursor) < length) {
        return -1;
    }

    while (length && cursor->niov) {
        int chunk = cursor->iov->length - cursor->offset;

        if (length < chunk) {
            chunk = length;
        }

        length -= chunk;

        cursor->offset   += chunk;
        cursor->consumed += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    return length ? -1 : 0;
} /* evpl_iovec_cursor_try_skip */

static inline int
evpl_iovec_cursor_try_get_uint8(
    struct evpl_iovec_cursor *cursor,
    uint8_t                  *value)
{
    return evpl_iovec_cursor_try_copy(cursor, value, sizeof(uint8_t));
} /* evpl_iovec_cursor_try_get_uint8 */

static inline int
evpl_iovec_cursor_try_get_uint16(
    struct evpl_iovec_cursor *cursor,
    uint16_t                 *value)
{
    if (evpl_iovec_cursor_try_skip(cursor, (2 - (cursor->consumed & 1)) & 1)) {
        return -1;
    }
    return evpl_iovec_cursor_try_copy(cursor, value, sizeof(uint16_t));
} /* evpl_iovec_cursor_try_get_uint16 */

static inline int
evpl_iovec_cursor_try_get_uint32(
    struct evpl_iovec_cursor *cursor,
    uint32_t                 *value)
{
    if (evpl_iovec_cursor_try_skip(cursor, (4 - (cursor->consumed & 3)) & 3)) {
        return -1;
    }
    return evpl_iovec_cursor_try_copy(cursor, value, sizeof(uint32_t));
} /* evpl_iovec_cursor_try_get_uint32 */

static inline int
evpl_iovec_cursor_try_get_uint64(
    struct evpl_iovec_cursor *cursor,
    uint64_t                 *value)
{
    if (evpl_iovec_cursor_try_skip(cursor, (8 - (cursor->consumed & 7)) & 7)) {
        return -1;
    }
    return evpl_iovec_cursor_try_copy(cursor, value, sizeof(uint64_t));
} /* evpl_iovec_cursor_try_get_uint64 */

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

        if (addrefs) {
            evpl_iovec_clone_segment(&iov[niov], cursor->iov, cursor->offset, chunk);
        } else {
            evpl_iovec_move_segment(&iov[niov], cursor->iov, cursor->offset, chunk);
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
    struct evpl_iovec saved;
    int               i;
    int               remainder_offset = cursor->offset;
    int               remainder_length = cursor->iov->length - cursor->offset;

    /* Clone the remainder portion with proper refcounting before truncation */
    evpl_iovec_clone_segment(&saved, cursor->iov, remainder_offset, remainder_length);

    /* Truncate original iovec to just the consumed portion */
    cursor->iov->length = cursor->offset;

    cursor->offset = 0;

    cursor->iov++;

    for (i = 0; i < niov; i++) {
        evpl_iovec_move(&cursor->iov[i], &iov[i]);
    }

    cursor->iov += niov;
    cursor->niov = (cursor->niov > niov + 1) ? cursor->niov - niov - 1 : 1;

    /* Transfer remainder to its final position with proper ownership */
    evpl_iovec_move(cursor->iov, &saved);

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