// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#define VFS_DAOS_FH_ONLY
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vfs_daos.h"

#define CHECK(condition) do { \
            if (!(condition)) { \
                fprintf(stderr, "line %d: %s\n", __LINE__, #condition); \
                exit(EXIT_FAILURE); \
            } \
} while (0)

int
main(void)
{
    uint8_t            uuid[16] = { 1 }, other_uuid[16] = { 2 };
    uint8_t            mount[16], same[16], other[16];
    uint8_t            fh[345], before[345];
    struct vfs_daos_fh key = { 0x0123456789abcdefULL, 0xfedcba9876543210ULL, 1 };
    struct vfs_daos_fh decoded;
    const uint8_t      expected[16] = {
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
        0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10
    };
    uint32_t           length;

    CHECK(vfs_daos_fh_mount(uuid, &key, fh, &length) == 0);
    memcpy(mount, fh, 16);
    uint8_t            reference[33];
    CHECK(chimera_vfs_encode_fh_mount(uuid, fh + 16, 17, reference) == 33);
    CHECK(!memcmp(reference, fh, 33));
    CHECK(vfs_daos_fh_mount(uuid, &key, fh, &length) == 0);
    memcpy(same, fh, 16);
    CHECK(!memcmp(mount, same, 16));
    CHECK(vfs_daos_fh_mount(other_uuid, &key, fh, &length) == 0);
    memcpy(other, fh, 16);
    CHECK(memcmp(mount, other, 16));
    key.lo++;
    CHECK(vfs_daos_fh_mount(uuid, &key, fh, &length) == 0);
    CHECK(memcmp(mount, fh, 16));
    key.lo--;
    CHECK(vfs_daos_fh_mount(NULL, &key, fh, &length) == EINVAL);
    for (key.type = 1; key.type <= 3; key.type++) {
        memset(fh, 0xa5, sizeof(fh));
        CHECK(vfs_daos_fh_encode(mount, &key, fh + 1, &length) == 0);
        CHECK(length == 33);
        CHECK(fh[0] == 0xa5 && fh[34] == 0xa5);
        CHECK(!memcmp(fh + 1, mount, 16));
        CHECK(!memcmp(fh + 17, expected, 16));
        CHECK(fh[33] == key.type);
        CHECK(vfs_daos_fh_decode(mount, fh + 1, length, &decoded) == 0);
        CHECK(decoded.hi == key.hi && decoded.lo == key.lo && decoded.type == key.type);
        CHECK(vfs_daos_fh_decode(other, fh + 1, length, &decoded) == ESTALE);
        for (size_t n = 0; n <= 344; n++) {
            if (n != 33) {
                CHECK(vfs_daos_fh_decode(mount, fh + 1, n, &decoded) == ESTALE);
            }
        }
        CHECK(vfs_daos_fh_decode(mount, fh + 1, SIZE_MAX, &decoded) == ESTALE);
    }
    for (unsigned int type = 0; type <= 255; type++) {
        if (type >= 1 && type <= 3) {
            continue;
        }
        fh[33] = type;
        CHECK(vfs_daos_fh_decode(mount, fh + 1, 33, &decoded) == ESTALE);
        key.type = type;
        memcpy(before, fh, sizeof(fh));
        length = 123;
        CHECK(vfs_daos_fh_encode(mount, &key, fh, &length) == EINVAL);
        CHECK(length == 123 && !memcmp(fh, before, sizeof(fh)));
    }
    key.hi   = key.lo = 0;
    key.type = 2;
    CHECK(vfs_daos_fh_encode(mount, &key, fh, &length) == 0);
    CHECK(vfs_daos_fh_decode(mount, fh, length, &decoded) == 0);
    CHECK(decoded.hi == 0 && decoded.lo == 0 && decoded.type == 2);
    key.hi = key.lo = UINT64_MAX;
    CHECK(vfs_daos_fh_encode(mount, &key, fh, &length) == 0);
    CHECK(vfs_daos_fh_decode(mount, fh, length, &decoded) == 0);
    CHECK(decoded.hi == UINT64_MAX && decoded.lo == UINT64_MAX);
    CHECK(vfs_daos_fh_decode(mount, NULL, 33, &decoded) == ESTALE);
    CHECK(vfs_daos_fh_decode(mount, fh, 33, NULL) == ESTALE);
    puts("Fixed OID codec: canonical bytes, types, lengths, prefix and round trips passed");
    return 0;
} /* main */
