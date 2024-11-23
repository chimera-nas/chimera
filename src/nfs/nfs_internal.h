#pragma once

#include "common/logging.h"

static inline uint32_t
chimera_nfs_hton32(uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap32(value);
#else  /* if __BYTE_ORDER == __LITTLE_ENDIAN */
    return value;
#endif /* if __BYTE_ORDER == __LITTLE_ENDIAN */
} /* rpc2_hton32 */

static inline uint32_t
chimera_nfs_ntoh32(uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap32(value);
#else  /* if __BYTE_ORDER == __LITTLE_ENDIAN */
    return value;
#endif /* if __BYTE_ORDER == __LITTLE_ENDIAN */
} /* rpc2_ntoh32 */

static inline uint64_t
chimera_nfs_hton64(uint64_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap64(value);
#else  /* if __BYTE_ORDER == __LITTLE_ENDIAN */
    return value;
#endif /* if __BYTE_ORDER == __LITTLE_ENDIAN */
} /* rpc2_hton64 */

static inline uint64_t
chimera_nfs_ntoh64(uint64_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap64(value);
#else  /* if __BYTE_ORDER == __LITTLE_ENDIAN */
    return value;
#endif /* if __BYTE_ORDER == __LITTLE_ENDIAN */
} /* rpc2_ntoh64 */

#define chimera_nfs_debug(...) chimera_debug("nfs", __VA_ARGS__)
#define chimera_nfs_info(...)  chimera_info("nfs", __VA_ARGS__)
#define chimera_nfs_error(...) chimera_error("nfs", __VA_ARGS__)
#define chimera_nfs_fatal(...) chimera_fatal("nfs", __VA_ARGS__)
#define chimera_nfs_abort(...) chimera_abort("core", __VA_ARGS__)

#define chimera_nfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "nfs", __VA_ARGS__)
