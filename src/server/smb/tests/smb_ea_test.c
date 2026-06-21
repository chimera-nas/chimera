// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "server/smb/smb_ea.h"

#define CHECK(cond)                                              \
        do {                                                     \
            if (!(cond)) {                                       \
                fprintf(stderr,                                  \
                        "smb_ea_test: FAILED at %s:%d: %s\n",    \
                        __FILE__, __LINE__, # cond);             \
                return 1;                                        \
            }                                                    \
        } while (0)

/* Build a two-entry FILE_FULL_EA_INFORMATION list with emit_one, then read it
 * back with parse_one and confirm the names/values/chaining round-trip. */
static int
test_full_roundtrip(void)
{
    uint8_t  buf[256];
    uint32_t off = 0;
    uint32_t n1, n2;

    n1 = chimera_smb_ea_full_emit_one(buf, "FOO", 3, "barbar", 6, 0);
    /* aligned: 8 + 3 + 1 + 6 = 18 -> 20 */
    CHECK(n1 == 20);
    n2 = chimera_smb_ea_full_emit_one(buf + n1, "X", 1, "", 0, 1);
    /* last entry, not aligned: 8 + 1 + 1 + 0 = 10 */
    CHECK(n2 == 10);

    struct chimera_smb_ea_entry e;
    off = 0;
    CHECK(chimera_smb_ea_full_parse_one(buf, n1 + n2, &off, &e) == 0);
    CHECK(e.flags == 0);
    CHECK(e.name_len == 3 && memcmp(e.name, "FOO", 3) == 0);
    CHECK(e.value_len == 6 && memcmp(e.value, "barbar", 6) == 0);
    CHECK(off == 20);   /* advanced to the second entry */

    CHECK(chimera_smb_ea_full_parse_one(buf, n1 + n2, &off, &e) == 0);
    CHECK(e.name_len == 1 && memcmp(e.name, "X", 1) == 0);
    CHECK(e.value_len == 0);
    CHECK(off == n1 + n2);   /* last entry -> off == len */
    return 0;
} /* test_full_roundtrip */

/* A truncated entry (NextEntryOffset points past the buffer / name+value exceed
 * the bound) must be rejected, not over-read. */
static int
test_full_parse_bounds(void)
{
    uint8_t                     buf[16];
    uint32_t                    off = 0;
    struct chimera_smb_ea_entry e;

    memset(buf, 0, sizeof(buf));
    /* name_len 200, value_len 0 in an 8-byte-header-only buffer -> overrun. */
    buf[5] = 200;
    CHECK(chimera_smb_ea_full_parse_one(buf, 8, &off, &e) == -1);

    /* a 4-byte buffer is too small even for the fixed header. */
    off = 0;
    CHECK(chimera_smb_ea_full_parse_one(buf, 4, &off, &e) == -1);
    return 0;
} /* test_full_parse_bounds */

static int
test_get_parse(void)
{
    /* FILE_GET_EA_INFORMATION: NextEntryOffset(4)=0, EaNameLength(1)=3, "FOO", NUL */
    uint8_t     buf[16] = { 0, 0, 0, 0, 3, 'F', 'O', 'O', 0 };
    uint32_t    off     = 0;
    const char *name;
    uint32_t    name_len;

    CHECK(chimera_smb_ea_get_parse_one(buf, 9, &off, &name, &name_len) == 0);
    CHECK(name_len == 3 && memcmp(name, "FOO", 3) == 0);
    CHECK(off == 9);
    return 0;
} /* test_get_parse */

static int
test_name_eq(void)
{
    CHECK(chimera_smb_ea_name_eq("foo", 3, "FOO", 3) == 1);
    CHECK(chimera_smb_ea_name_eq("FoO", 3, "fOo", 3) == 1);
    CHECK(chimera_smb_ea_name_eq("foo", 3, "food", 4) == 0);
    CHECK(chimera_smb_ea_name_eq("bar", 3, "baz", 3) == 0);
    return 0;
} /* test_name_eq */

static int
test_status_map(void)
{
    CHECK(chimera_smb_ea_status(CHIMERA_VFS_OK) == SMB2_STATUS_SUCCESS);
    CHECK(chimera_smb_ea_status(CHIMERA_VFS_ENOTSUP) == SMB2_STATUS_EAS_NOT_SUPPORTED);
    CHECK(chimera_smb_ea_status(CHIMERA_VFS_ENODATA) == SMB2_STATUS_NONEXISTENT_EA_ENTRY);
    CHECK(chimera_smb_ea_status(CHIMERA_VFS_EFBIG) == SMB2_STATUS_EA_TOO_LARGE);
    return 0;
} /* test_status_map */

/* The EaSize per-entry contribution must match the OS/2 FEALIST formula
 * (4 + name + 1 + value), shared with the backends via vfs_xattr_name.h. */
static int
test_ea_size_formula(void)
{
    CHECK(chimera_vfs_xattr_ea_entry_size(3, 6) == 4 + 3 + 1 + 6);
    CHECK(chimera_vfs_xattr_ea_entry_size(0, 0) == 5);
    CHECK(CHIMERA_VFS_XATTR_EA_LIST_OVERHEAD == 4);
    return 0;
} /* test_ea_size_formula */

int
main(void)
{
    if (test_full_roundtrip() || test_full_parse_bounds() || test_get_parse() ||
        test_name_eq() || test_status_map() || test_ea_size_formula()) {
        return 1;
    }
    printf("smb_ea_test: all checks passed\n");
    return 0;
} /* main */
