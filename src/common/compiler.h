// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include <stdint.h>
#include <stdatomic.h>

#ifdef _MSC_VER
#include <intrin.h>
#define CHIMERA_ALIGNED(n) __declspec(align(n))
#define CHIMERA_THREAD_LOCAL __declspec(thread)
#define CHIMERA_NORETURN __declspec(noreturn)
#define CHIMERA_NOINLINE __declspec(noinline)
#define CHIMERA_UNUSED
#define CHIMERA_EXPECT(x, value) (x)
#define chimera_bswap32 _byteswap_ulong
#define chimera_bswap64 _byteswap_uint64
#define typeof __typeof__
static inline unsigned chimera_clz32(uint32_t x)
{
    unsigned long bit;
    return _BitScanReverse(&bit, x) ? 31 - bit : 32;
}
static inline unsigned chimera_clz64(uint64_t x)
{
    unsigned long bit;
    return _BitScanReverse64(&bit, x) ? 63 - bit : 64;
}
static inline unsigned chimera_ctz32(uint32_t x)
{
    unsigned long bit;
    return _BitScanForward(&bit, x) ? bit : 32;
}
static inline unsigned chimera_ctz64(uint64_t x)
{
    unsigned long bit;
    return _BitScanForward64(&bit, x) ? bit : 64;
}
#else
#define CHIMERA_ALIGNED(n) __attribute__((aligned(n)))
#define CHIMERA_THREAD_LOCAL _Thread_local
#define CHIMERA_NORETURN __attribute__((noreturn))
#define CHIMERA_NOINLINE __attribute__((noinline))
#define CHIMERA_UNUSED __attribute__((unused))
#define CHIMERA_EXPECT(x, value) __builtin_expect((x), (value))
#define chimera_bswap32 __builtin_bswap32
#define chimera_bswap64 __builtin_bswap64
#define chimera_clz32 __builtin_clz
#define chimera_clz64 __builtin_clzll
#define chimera_ctz32 __builtin_ctz
#define chimera_ctz64 __builtin_ctzll
#endif
