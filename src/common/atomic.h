// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

/* Atomics on the existing naturally aligned scalar fields. GCC/Clang provide
 * typed builtins; MSVC uses native Interlocked operations. Windows currently
 * provides the stronger sequentially consistent ordering for every operation.
 * These helpers intentionally do not reinterpret non-atomic objects as C11
 * _Atomic objects, which would change their C type and potentially their ABI. */
#define CHIMERA_MEMORY_RELAXED 0
#define CHIMERA_MEMORY_ACQUIRE 2
#define CHIMERA_MEMORY_RELEASE 3
#define CHIMERA_MEMORY_ACQ_REL 4
#define CHIMERA_MEMORY_SEQ_CST 5

#ifdef _MSC_VER
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <intrin.h>

static inline uint64_t
chimera_native_compare(
    volatile void *p,
    uint64_t       expected,
    uint64_t       desired,
    size_t         size)
{
    switch (size) {
        case 1: return (uint8_t) _InterlockedCompareExchange8((volatile char *) p, (char) desired, (char) expected);
        case 2: return (uint16_t) _InterlockedCompareExchange16((volatile short *) p, (short) desired, (short) expected)
        ;
        case 4: return (uint32_t) _InterlockedCompareExchange((volatile long *) p, (long) desired, (long) expected);
        case 8: return (uint64_t) _InterlockedCompareExchange64((volatile __int64 *) p, (__int64) desired, (__int64)
                                                                expected);
        default: abort();
    } // switch
} // chimera_native_compare

static inline uint64_t
chimera_native_exchange(
    volatile void *p,
    uint64_t       desired,
    size_t         size)
{
    switch (size) {
        case 1: return (uint8_t) _InterlockedExchange8((volatile char *) p, (char) desired);
        case 2: return (uint16_t) _InterlockedExchange16((volatile short *) p, (short) desired);
        case 4: return (uint32_t) _InterlockedExchange((volatile long *) p, (long) desired);
        case 8: return (uint64_t) _InterlockedExchange64((volatile __int64 *) p, (__int64) desired);
        default: abort();
    } // switch
} // chimera_native_exchange

static inline uint64_t
chimera_native_add(
    volatile void *p,
    uint64_t       value,
    size_t         size)
{
    switch (size) {
        case 1: return (uint8_t) _InterlockedExchangeAdd8((volatile char *) p, (char) value);
        case 2: return (uint16_t) _InterlockedExchangeAdd16((volatile short *) p, (short) value);
        case 4: return (uint32_t) _InterlockedExchangeAdd((volatile long *) p, (long) value);
        case 8: return (uint64_t) _InterlockedExchangeAdd64((volatile __int64 *) p, (__int64) value);
        default: abort();
    } // switch
} // chimera_native_add

static inline uint64_t
chimera_native_add_result(
    volatile void *p,
    uint64_t       value,
    size_t         size)
{
    return chimera_native_add(p, value, size) + value;
} // chimera_native_add_result

static inline uint64_t
chimera_native_or(
    volatile void *p,
    uint64_t       value,
    size_t         size)
{
    switch (size) {
        case 1: return (uint8_t) _InterlockedOr8((volatile char *) p, (char) value);
        case 2: return (uint16_t) _InterlockedOr16((volatile short *) p, (short) value);
        case 4: return (uint32_t) _InterlockedOr((volatile long *) p, (long) value);
        case 8: return (uint64_t) _InterlockedOr64((volatile __int64 *) p, (__int64) value);
        default: abort();
    } // switch
} // chimera_native_or

static inline uint64_t
chimera_native_and(
    volatile void *p,
    uint64_t       value,
    size_t         size)
{
    switch (size) {
        case 1: return (uint8_t) _InterlockedAnd8((volatile char *) p, (char) value);
        case 2: return (uint16_t) _InterlockedAnd16((volatile short *) p, (short) value);
        case 4: return (uint32_t) _InterlockedAnd((volatile long *) p, (long) value);
        case 8: return (uint64_t) _InterlockedAnd64((volatile __int64 *) p, (__int64) value);
        default: abort();
    } // switch
} // chimera_native_and

static inline int
chimera_native_cas(
    volatile void *p,
    void          *expected,
    uint64_t       desired,
    size_t         size)
{
    uint64_t before = 0, observed;

    if (size != 1 && size != 2 && size != 4 && size != 8) {
        abort();
    }
    memcpy(&before, expected, size);
    observed = chimera_native_compare(p, before, desired, size);
    if (observed == before) {
        return 1;
    }
    memcpy(expected, &observed, size);
    return 0;
} // chimera_native_cas

#define chimera_atomic_load_n(p, order) \
        ((__typeof__(*(p)))chimera_native_compare((volatile void *) (p), 0, 0, sizeof(*(p))))
#define chimera_atomic_store_n(p, v, order) \
        ((void) chimera_native_exchange((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_exchange_n(p, v, order) \
        ((__typeof__(*(p)))chimera_native_exchange((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_fetch_add(p, v, order) \
        ((__typeof__(*(p)))chimera_native_add((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_fetch_sub(p, v, order) \
        ((__typeof__(*(p)))chimera_native_add((volatile void *) (p), 0 - (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_add_fetch(p, v, order) \
        ((__typeof__(*(p)))chimera_native_add_result((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_sub_fetch(p, v, order) \
        ((__typeof__(*(p)))chimera_native_add_result((volatile void *) (p), 0 - (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_fetch_or(p, v, order) \
        ((__typeof__(*(p)))chimera_native_or((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_fetch_and(p, v, order) \
        ((__typeof__(*(p)))chimera_native_and((volatile void *) (p), (uint64_t) (v), sizeof(*(p))))
#define chimera_atomic_compare_exchange_n(p, expected, desired, weak, success, failure) \
        chimera_native_cas((volatile void *) (p), (expected), (uint64_t) (desired), sizeof(*(p)))
#else // ifdef _MSC_VER
#define chimera_atomic_load_n             __atomic_load_n
#define chimera_atomic_store_n            __atomic_store_n
#define chimera_atomic_exchange_n         __atomic_exchange_n
#define chimera_atomic_fetch_add          __atomic_fetch_add
#define chimera_atomic_fetch_sub          __atomic_fetch_sub
#define chimera_atomic_add_fetch          __atomic_add_fetch
#define chimera_atomic_sub_fetch          __atomic_sub_fetch
#define chimera_atomic_fetch_or           __atomic_fetch_or
#define chimera_atomic_fetch_and          __atomic_fetch_and
#define chimera_atomic_compare_exchange_n __atomic_compare_exchange_n
#endif // ifdef _MSC_VER
