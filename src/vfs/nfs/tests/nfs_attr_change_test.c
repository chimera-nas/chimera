// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include "nfs_internal.h"

int
main(void)
{
    uint32_t                 mask[2];
    uint8_t                  bytes[24];
    uint32_t                 type   = chimera_nfs_hton32(NF4REG);
    uint64_t                 change = chimera_nfs_hton64(UINT64_C(0x123456789abcdef0));
    uint64_t                 size   = chimera_nfs_hton64(8193);
    uint32_t                 mode   = chimera_nfs_hton32(0640);
    struct chimera_vfs_attrs attrs = { 0 }, before = { 0 }, after = { 0 };
    struct change_info4      cinfo = { .atomic = 1, .before = 41, .after = 43 };
    struct fattr4            wire  = { 0 };

    chimera_nfs4_attr_request_stat(mask);
    assert(mask[0] & (1U << FATTR4_CHANGE));
    mask[0] = (1U << FATTR4_TYPE) | (1U << FATTR4_CHANGE) | (1U << FATTR4_SIZE);
    mask[1] = 1U << (FATTR4_MODE - 32);
    memcpy(bytes, &type, 4);
    memcpy(bytes + 4, &change, 8);
    memcpy(bytes + 12, &size, 8);
    memcpy(bytes + 20, &mode, 4);
    wire.num_attrmask   = 2;
    wire.attrmask       = mask;
    wire.attr_vals.data = bytes;
    wire.attr_vals.len  = sizeof(bytes);
    chimera_nfs4_unmarshall_fattr(&wire, &attrs);
    assert(attrs.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
    assert(attrs.va_change == UINT64_C(0x123456789abcdef0));
    assert(attrs.va_size == 8193 && attrs.va_mode == (S_IFREG | 0640));
    chimera_nfs4_unmarshall_cinfo(&cinfo, &before, &after);
    assert(before.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
    assert(after.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
    assert(before.va_change == 41 && after.va_change == 43);
    return 0;
} /* main */
