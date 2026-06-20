// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "nfs_internal.h"
#include "nfs4_xdr.h"
#include "nfs4_attr.h"
#include "nfs4_op_matrix.h"

static uint32_t
get_u32(
    const uint8_t *buf,
    int            index)
{
    return chimera_nfs_ntoh32(((const uint32_t *) buf)[index]);
} /* get_u32 */

static uint64_t
get_u64(
    const uint8_t *buf,
    int            index)
{
    return chimera_nfs_ntoh64(((const uint64_t *) buf)[index]);
} /* get_u64 */

static void
test_unimplemented_delegation_ops_not_advertised(void)
{
    assert(nfs4_op_check_minor(OP_GET_DIR_DELEGATION, 1, 1, true) == NFS4ERR_NOTSUPP);
    assert(nfs4_op_check_minor(OP_GET_DIR_DELEGATION, 2, 1, true) == NFS4ERR_NOTSUPP);
    assert(nfs4_op_check_minor(OP_WANT_DELEGATION, 1, 1, true) == NFS4ERR_NOTSUPP);
    assert(nfs4_op_check_minor(OP_WANT_DELEGATION, 2, 1, true) == NFS4ERR_NOTSUPP);
} /* test_unimplemented_delegation_ops_not_advertised */

static void
test_open_arguments_supported_attrs_follows_delegation_config(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    memset(&attr, 0, sizeof(attr));
    req_mask[0] = (1 << FATTR4_SUPPORTED_ATTRS);

    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 1, 0, 0, 0, 60, 0, NULL, 0);

    assert(num_rsp_mask == 1);
    assert(rsp_mask[0] == (1 << FATTR4_SUPPORTED_ATTRS));
    assert(get_u32(attrvals, 0) == 3);
    assert((get_u32(attrvals, 3) & (1 << (FATTR4_OPEN_ARGUMENTS - 64))) == 0);

    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 1, 0, 0, 1, 60, 0, NULL, 0);

    assert(num_rsp_mask == 1);
    assert(rsp_mask[0] == (1 << FATTR4_SUPPORTED_ATTRS));
    assert(get_u32(attrvals, 0) == 3);
    assert((get_u32(attrvals, 3) & (1 << (FATTR4_OPEN_ARGUMENTS - 64))) != 0);
} /* test_open_arguments_supported_attrs_follows_delegation_config */

static void
test_open_arguments_attr_follows_delegation_config(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    memset(&attr, 0, sizeof(attr));
    req_mask[2] = (1 << (FATTR4_OPEN_ARGUMENTS - 64));

    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 1, 0, 0, 0, 60, 0, NULL, 0);

    assert(num_rsp_mask == 0);
    assert(attrvals_len == 0);
    assert((rsp_mask[2] & (1 << (FATTR4_OPEN_ARGUMENTS - 64))) == 0);

    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 1, 0, 0, 1, 60, 0, NULL, 0);

    assert(num_rsp_mask == 3);
    assert(attrvals_len == 40);
    assert((rsp_mask[2] & (1 << (FATTR4_OPEN_ARGUMENTS - 64))) != 0);
    assert(get_u32(attrvals, 4) == 1);
    assert(get_u32(attrvals, 5) ==
           ((1 << OPEN_ARGS_SHARE_ACCESS_WANT_ANY_DELEG) |
            (1 << OPEN_ARGS_SHARE_ACCESS_WANT_NO_DELEG)));
} /* test_open_arguments_attr_follows_delegation_config */

static void
test_xattr_support_value_follows_backend_capability(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    memset(&attr, 0, sizeof(attr));
    req_mask[2] = (1 << (FATTR4_XATTR_SUPPORT - 64));

    /* xattr_supported = 0 -> the per-object value encodes FALSE. */
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);

    assert(num_rsp_mask == 3);
    assert(attrvals_len == 4);
    assert((rsp_mask[2] & (1 << (FATTR4_XATTR_SUPPORT - 64))) != 0);
    assert(get_u32(attrvals, 0) == 0);

    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));

    /* xattr_supported = 1 -> the per-object value encodes TRUE. */
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 1, 0, 60, 0, NULL, 0);

    assert(num_rsp_mask == 3);
    assert(attrvals_len == 4);
    assert((rsp_mask[2] & (1 << (FATTR4_XATTR_SUPPORT - 64))) != 0);
    assert(get_u32(attrvals, 0) == 1);
} /* test_xattr_support_value_follows_backend_capability */

static void
test_change_attr_type_advertised_for_v42(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    memset(&attr, 0, sizeof(attr));
    req_mask[0] = (1 << FATTR4_SUPPORTED_ATTRS);

    /* change_attr_type (attr 79) is an NFSv4.2 attribute: not advertised at 4.1. */
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 1, 0, 0, 0, 60, 0, NULL, 0);
    assert((get_u32(attrvals, 3) & (1 << (FATTR4_CHANGE_ATTR_TYPE - 64))) == 0);

    /* Advertised at 4.2. */
    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);
    assert((get_u32(attrvals, 3) & (1 << (FATTR4_CHANGE_ATTR_TYPE - 64))) != 0);
} /* test_change_attr_type_advertised_for_v42 */

static void
test_change_attr_type_value_follows_native_change(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    req_mask[2] = (1 << (FATTR4_CHANGE_ATTR_TYPE - 64));

    /* Backend with a native change counter -> MONOTONIC_INCR. */
    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask = CHIMERA_VFS_ATTR_CHANGE;
    attr.va_change   = 7;
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);
    assert(num_rsp_mask == 3);
    assert(attrvals_len == 4);
    assert((rsp_mask[2] & (1 << (FATTR4_CHANGE_ATTR_TYPE - 64))) != 0);
    assert(get_u32(attrvals, 0) == NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR);

    /* Backend without a native counter (ctime fallback) -> TIME_METADATA. */
    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask = CHIMERA_VFS_ATTR_CTIME;
    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);
    assert(get_u32(attrvals, 0) == NFS4_CHANGE_TYPE_IS_TIME_METADATA);
} /* test_change_attr_type_value_follows_native_change */

static void
test_change_value_native_vs_ctime(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    req_mask[0] = (1 << FATTR4_CHANGE);

    /* Native counter: fattr4_change is the raw counter value. */
    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask = CHIMERA_VFS_ATTR_CHANGE;
    attr.va_change   = 0x1122334455667788ULL;
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);
    assert((rsp_mask[0] & (1 << FATTR4_CHANGE)) != 0);
    assert(get_u64(attrvals, 0) == 0x1122334455667788ULL);

    /* No native counter: fattr4_change is derived from ctime (sec*1e9 + nsec). */
    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask      = CHIMERA_VFS_ATTR_CTIME;
    attr.va_ctime.tv_sec  = 1000;
    attr.va_ctime.tv_nsec = 500;
    memset(rsp_mask, 0, sizeof(rsp_mask));
    memset(attrvals, 0, sizeof(attrvals));
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 0, 0, 60, 0, NULL, 0);
    assert(get_u64(attrvals, 0) == 1000ULL * 1000000000ULL + 500ULL);
} /* test_change_value_native_vs_ctime */

/* fattr4 values must be packed in ascending attribute order so the client,
 * which decodes the response bitmap low-to-high, reads each value at the right
 * offset.  change_attr_type (79) and xattr_support (82) live in word 2 and are
 * co-requested by the Linux v4.2 client at mount; emitting them out of order
 * misframes the reply (this regressed nfstest_xattr). */
static void
test_word2_values_packed_in_ascending_order(void)
{
    struct chimera_vfs_attrs attr;
    uint32_t                 req_mask[3] = { 0 };
    uint32_t                 rsp_mask[3] = { 0 };
    uint32_t                 num_rsp_mask;
    uint32_t                 attrvals_len;
    uint8_t                  attrvals[256];

    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask = CHIMERA_VFS_ATTR_CHANGE; /* native -> MONOTONIC_INCR */
    attr.va_change   = 1;
    req_mask[2]      = (1 << (FATTR4_CHANGE_ATTR_TYPE - 64)) |
        (1 << (FATTR4_XATTR_SUPPORT - 64));

    /* xattr_supported = 1 passed in. */
    chimera_nfs4_marshall_attrs(&attr, 3, req_mask, &num_rsp_mask, rsp_mask, 3,
                                attrvals, &attrvals_len, sizeof(attrvals), 2, 0, 1, 0, 60, 0, NULL, 0);

    assert(num_rsp_mask == 3);
    assert((rsp_mask[2] & (1 << (FATTR4_CHANGE_ATTR_TYPE - 64))) != 0);
    assert((rsp_mask[2] & (1 << (FATTR4_XATTR_SUPPORT - 64))) != 0);
    assert(attrvals_len == 8); /* two uint32 values */

    /* Ascending: change_attr_type (79) first, then xattr_support (82). */
    assert(get_u32(attrvals, 0) == NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR);
    assert(get_u32(attrvals, 1) == 1);
} /* test_word2_values_packed_in_ascending_order */

int
main(void)
{
    test_unimplemented_delegation_ops_not_advertised();
    test_open_arguments_supported_attrs_follows_delegation_config();
    test_open_arguments_attr_follows_delegation_config();
    test_xattr_support_value_follows_backend_capability();
    test_change_attr_type_advertised_for_v42();
    test_change_attr_type_value_follows_native_change();
    test_change_value_native_vs_ctime();
    test_word2_values_packed_in_ascending_order();

    return 0;
} /* main */
