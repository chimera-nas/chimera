// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 owner / owner_group attribute decoder (nfs4_attr.h).
 *
 * RFC 7530 section 5.9: a who string the server cannot translate is
 * NFS4ERR_BADOWNER on SETATTR, OPEN(create) and CREATE.  The decoder used
 * to parse the string with strtoul, so any name ("alice@domain", "OWNER@",
 * "nobody") silently became uid 0 and the file ended up owned by root.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "nfs_internal.h"
#include "nfs4_xdr.h"
#include "nfs4_attr.h"

/*
 * Explicit check rather than assert(): the static-analysis build compiles
 * with -DNDEBUG, which turns assert() into a no-op.  CHECK always evaluates.
 */
#define CHECK(cond)                                               \
        do {                                                      \
            if (!(cond)) {                                        \
                fprintf(stderr,                                   \
                        "test_attr_owner: FAILED at %s:%d: %s\n", \
                        __FILE__, __LINE__, # cond);              \
                return 1;                                         \
            }                                                     \
        } while (0)

#define BOGUS     "idmaptest_unmapped@totally-bogus-nonexistent-domain.invalid"
#define ROOT_AT   "root@totally-bogus-nonexistent-domain.invalid"

/* A value no decoder writes, so "left untouched" is observable. */
#define UNTOUCHED 0xdeadbeefu

/* XDR opaque: big-endian length, bytes, zero pad to 4.  Returns bytes
 * written. */
static uint32_t
put_who(
    uint8_t    *buf,
    const char *who,
    uint32_t    len)
{
    uint32_t padded = (len + 3) & ~3u;

    buf[0] = (uint8_t) (len >> 24);
    buf[1] = (uint8_t) (len >> 16);
    buf[2] = (uint8_t) (len >> 8);
    buf[3] = (uint8_t) len;
    memcpy(buf + 4, who, len);
    memset(buf + 4 + len, 0, padded - len);
    return 4 + padded;
} /* put_who */

/* Decode a lone FATTR4_OWNER (is_group == 0) or FATTR4_OWNER_GROUP (1)
 * through chimera_nfs4_unmarshall_attrs with a two-word request mask and no
 * ACL buffer. */
static nfsstat4
decode_who(
    const char               *who,
    uint32_t                  len,
    int                       is_group,
    struct chimera_vfs_attrs *attr)
{
    uint8_t  blob[4 + CHIMERA_IDMAP_WHO_MAX + 4];
    uint32_t req_mask[2];
    uint32_t blob_len;

    memset(attr, 0, sizeof(*attr));
    attr->va_uid = UNTOUCHED;
    attr->va_gid = UNTOUCHED;

    req_mask[0] = 0;
    req_mask[1] = is_group ? (1U << (FATTR4_OWNER_GROUP - 32))
                           : (1U << (FATTR4_OWNER - 32));
    blob_len = put_who(blob, who, len);

    return chimera_nfs4_unmarshall_attrs(attr, 2, req_mask, blob, blob_len,
                                         NULL, 0);
} /* decode_who */

static int
test_numeric_owner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("25201", 5, 0, &attr) == NFS4_OK);
    CHECK(attr.va_set_mask & CHIMERA_VFS_ATTR_UID);
    CHECK(attr.va_uid == 25201);
    return 0;
} /* test_numeric_owner */

static int
test_numeric_group(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("2000", 4, 1, &attr) == NFS4_OK);
    CHECK(attr.va_set_mask & CHIMERA_VFS_ATTR_GID);
    CHECK(attr.va_gid == 2000);
    return 0;
} /* test_numeric_group */

/* The ticket's case: a name@domain the server's nss cannot resolve. */
static int
test_unresolvable_name_is_badowner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who(BOGUS, (uint32_t) strlen(BOGUS), 0, &attr) ==
          NFS4ERR_BADOWNER);
    CHECK(!(attr.va_set_mask & CHIMERA_VFS_ATTR_UID));
    CHECK(attr.va_uid == UNTOUCHED);

    CHECK(decode_who(BOGUS, (uint32_t) strlen(BOGUS), 1, &attr) ==
          NFS4ERR_BADOWNER);
    CHECK(!(attr.va_set_mask & CHIMERA_VFS_ATTR_GID));
    CHECK(attr.va_gid == UNTOUCHED);
    return 0;
} /* test_unresolvable_name_is_badowner */

/* "nobody" with no '@' is neither numeric nor a name@domain: RFC 7530
 * section 5.9 only says it "may" designate the anonymous user, and knfsd
 * refuses it. */
static int
test_bare_nobody_is_badowner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("nobody", 6, 0, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    return 0;
} /* test_bare_nobody_is_badowner */

/* The idmap decodes special-whos for ACL ACEs; they name no uid or gid. */
static int
test_special_who_is_badowner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("OWNER@", 6, 0, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    CHECK(decode_who("EVERYONE@", 9, 1, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_gid == UNTOUCHED);
    return 0;
} /* test_special_who_is_badowner */

/* Likewise a SID string, which the idmap decodes for ACE whos -- including
 * the unix-id forms (S-1-22-*, S-1-5-88-*) that decode to a user or group
 * principal: RFC 7530 section 5.9 owners are numeric or name@domain, and
 * the SID parse would otherwise turn an overflowing RID into uid 0. */
static int
test_sid_is_badowner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("S-1-1-0", 7, 0, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    CHECK(decode_who("S-1-22-1-1000", 13, 0, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    CHECK(decode_who("S-1-22-2-1000", 13, 1, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_gid == UNTOUCHED);
    CHECK(decode_who("S-1-5-88-1-4294967296", 21, 0, &attr) ==
          NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    return 0;
} /* test_sid_is_badowner */

static int
test_overflow_is_badowner(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who("4294967296", 10, 0, &attr) == NFS4ERR_BADOWNER);
    CHECK(attr.va_uid == UNTOUCHED);
    return 0;
} /* test_overflow_is_badowner */

#ifndef _WIN32
/* A local part nss knows resolves whatever the domain says: this is a
 * named root, not the bug.  The domain is not enforced (no NFSv4 domain is
 * configured). */
static int
test_known_name_resolves(void)
{
    struct chimera_vfs_attrs attr;

    CHECK(decode_who(ROOT_AT, (uint32_t) strlen(ROOT_AT), 0, &attr) ==
          NFS4_OK);
    CHECK(attr.va_set_mask & CHIMERA_VFS_ATTR_UID);
    CHECK(attr.va_uid == 0);
    return 0;
} /* test_known_name_resolves */
#endif /* ifndef _WIN32 */

/* A four-byte numeric owner at the very end of an exactly sized heap blob:
 * no padding, no terminator, nothing after it.  The old strtoul read past
 * the end (ASan flags it); the bounded parse stays inside. */
static int
test_owner_at_blob_end_stays_in_bounds(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[2] = { 0, 1U << (FATTR4_OWNER - 32) };
    uint8_t                 *blob        = malloc(8);
    nfsstat4                 rc;

    CHECK(blob != NULL);
    CHECK(put_who(blob, "2520", 4) == 8);

    memset(&attr, 0, sizeof(attr));
    attr.va_uid = UNTOUCHED;
    rc          = chimera_nfs4_unmarshall_attrs(&attr, 2, req_mask, blob, 8,
                                                NULL, 0);
    free(blob);

    CHECK(rc == NFS4_OK);
    CHECK(attr.va_set_mask & CHIMERA_VFS_ATTR_UID);
    CHECK(attr.va_uid == 2520);
    return 0;
} /* test_owner_at_blob_end_stays_in_bounds */

int
main(void)
{
    int failed = 0;

    failed += test_numeric_owner();
    failed += test_numeric_group();
    failed += test_unresolvable_name_is_badowner();
    failed += test_bare_nobody_is_badowner();
    failed += test_special_who_is_badowner();
    failed += test_sid_is_badowner();
    failed += test_overflow_is_badowner();
#ifndef _WIN32
    failed += test_known_name_resolves();
#endif /* ifndef _WIN32 */
    /* Last: under ASan the pre-fix overread aborts the whole run. */
    failed += test_owner_at_blob_end_stays_in_bounds();

    if (failed) {
        fprintf(stderr, "test_attr_owner: %d check(s) FAILED\n", failed);
        return 1;
    }
    printf("test_attr_owner: all checks passed\n");
    return 0;
} /* main */
