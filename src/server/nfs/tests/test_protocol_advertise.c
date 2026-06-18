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

int
main(void)
{
    test_unimplemented_delegation_ops_not_advertised();
    test_open_arguments_supported_attrs_follows_delegation_config();
    test_open_arguments_attr_follows_delegation_config();
    test_xattr_support_value_follows_backend_capability();

    return 0;
} /* main */
