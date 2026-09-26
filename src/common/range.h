// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include <stdbool.h>
#include <stdint.h>

/* Exclusive range endpoints need one carry bit to distinguish byte zero
 * from 2^64. Keep that bit explicit on compilers without 128-bit integers. */
struct chimera_range_endpoint {
    uint64_t value;
    bool     carry;
};

static inline struct chimera_range_endpoint
chimera_range_offset(uint64_t offset)
{
    return (struct chimera_range_endpoint) { offset, false };
} // chimera_range_offset

static inline struct chimera_range_endpoint
chimera_range_eof(void)
{
    return (struct chimera_range_endpoint) { 0, true };
} // chimera_range_eof

static inline struct chimera_range_endpoint
chimera_range_end(
    uint64_t offset,
    uint64_t length)
{
    uint64_t end = offset + length;

    return (struct chimera_range_endpoint) { end, end < offset };
} // chimera_range_end

static inline int
chimera_range_compare(
    struct chimera_range_endpoint a,
    struct chimera_range_endpoint b)
{
    if (a.carry != b.carry) {
        return a.carry ? 1 : -1;
    }
    return a.value < b.value ? -1 : a.value > b.value;
} // chimera_range_compare

/* POSIX SEEK_END accepts signed offsets and lengths. Return a nonnegative
 * interval whose last byte fits off_t, without overflowing signed arithmetic. */
static inline bool
chimera_range_seek_end(
    uint64_t  eof,
    int64_t   offset,
    int64_t   length,
    uint64_t *start_out,
    uint64_t *length_out)
{
    uint64_t start, size;

    if (offset < 0) {
        uint64_t magnitude = 0 - (uint64_t) offset;
        if (eof < magnitude) {
            return false;
        }
        start = eof - magnitude;
    } else {
        if (eof > UINT64_MAX - (uint64_t) offset) {
            return false;
        }
        start = eof + (uint64_t) offset;
    }
    size = length < 0 ? 0 - (uint64_t) length : (uint64_t) length;
    if (length < 0) {
        if (start < size) {
            return false;
        }
        start -= size;
    }
    if (start > INT64_MAX || (size && size - 1 > INT64_MAX - start)) {
        return false;
    }
    *start_out  = start;
    *length_out = size;
    return true;
} // chimera_range_seek_end
