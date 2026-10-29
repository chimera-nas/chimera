// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#define VFS_DAOS_FH_ONLY
#include <errno.h>
#include "vfs_daos.h"

int
vfs_daos_fh_mount(
    const uint8_t             uuid[16],
    const struct vfs_daos_fh *key,
    uint8_t                  *fh,
    uint32_t                 *length)
{
    uint8_t encoded[VFS_DAOS_FH_SIZE];

    if (!uuid || !fh || !length) {
        return EINVAL;
    }
    int     rc = vfs_daos_fh_encode(uuid, key, encoded, length);
    if (!rc) {
        *length = chimera_vfs_encode_fh_mount(uuid, encoded + 16,
                                              VFS_DAOS_FH_FRAGMENT_SIZE, fh);
    }
    return rc;
} /* vfs_daos_fh_mount */

int
vfs_daos_fh_encode(
    const uint8_t             id[16],
    const struct vfs_daos_fh *key,
    uint8_t                  *fh,
    uint32_t                 *length)
{
    uint8_t fragment[VFS_DAOS_FH_FRAGMENT_SIZE];

    if (!id || !key || !fh || !length || key->type < 1 || key->type > 3) {
        return EINVAL;
    }
    for (unsigned int i = 0; i < 8; i++) {
        fragment[i]     = (uint8_t) (key->hi >> (56 - 8 * i));
        fragment[8 + i] = (uint8_t) (key->lo >> (56 - 8 * i));
    }
    fragment[16] = key->type;
    *length      = chimera_vfs_encode_fh_parent(id, fragment, sizeof(fragment), fh);
    return 0;
} /* vfs_daos_fh_encode */

int
vfs_daos_fh_decode(
    const uint8_t       id[16],
    const void         *fh,
    size_t              length,
    struct vfs_daos_fh *key)
{
    const uint8_t     *bytes   = fh;
    struct vfs_daos_fh decoded = { 0 };

    if (!id || !fh || !key || length != VFS_DAOS_FH_SIZE ||
        memcmp(id, fh, 16) || bytes[32] < 1 || bytes[32] > 3) {
        return ESTALE;
    }
    for (unsigned int i = 0; i < 8; i++) {
        decoded.hi = (decoded.hi << 8) | bytes[16 + i];
        decoded.lo = (decoded.lo << 8) | bytes[24 + i];
    }
    decoded.type = bytes[32];
    *key         = decoded;
    return 0;
} /* vfs_daos_fh_decode */
