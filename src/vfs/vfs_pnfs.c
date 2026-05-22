// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "vfs/vfs.h"
#include "vfs/vfs_pnfs.h"
#include "common/macros.h"

SYMBOL_EXPORT struct chimera_vfs_pnfs *
chimera_vfs_pnfs_create(void)
{
    struct chimera_vfs_pnfs *pnfs;

    pnfs = calloc(1, sizeof(*pnfs));
    atomic_init(&pnfs->steer_rr, 0);

    return pnfs;
} /* chimera_vfs_pnfs_create */

SYMBOL_EXPORT void
chimera_vfs_pnfs_destroy(struct chimera_vfs_pnfs *pnfs)
{
    free(pnfs);
} /* chimera_vfs_pnfs_destroy */

SYMBOL_EXPORT void
chimera_vfs_pnfs_set_enabled(
    struct chimera_vfs *vfs,
    int                 enabled)
{
    vfs->pnfs->enabled = enabled;
} /* chimera_vfs_pnfs_set_enabled */

SYMBOL_EXPORT int
chimera_vfs_pnfs_enabled(const struct chimera_vfs *vfs)
{
    return vfs->pnfs && vfs->pnfs->enabled && vfs->pnfs->num_ds > 0;
} /* chimera_vfs_pnfs_enabled */

SYMBOL_EXPORT int
chimera_vfs_pnfs_add_device(
    struct chimera_vfs *vfs,
    const char         *netid,
    const char         *uaddr,
    const char         *backing_path)
{
    struct chimera_vfs_pnfs *pnfs = vfs->pnfs;
    struct chimera_vfs_ds   *ds;
    int                      idx;

    if (pnfs->num_ds >= CHIMERA_PNFS_MAX_DS) {
        return -1;
    }

    idx = pnfs->num_ds++;
    ds  = &pnfs->ds[idx];

    /* Deterministic, stable-within-instance deviceid derived from the
     * registration index (the on-wire value is opaque to clients). */
    memset(ds->deviceid, 0, CHIMERA_VFS_DEVICEID_SIZE);
    ds->deviceid[0]                             = 'D';
    ds->deviceid[1]                             = 'S';
    ds->deviceid[CHIMERA_VFS_DEVICEID_SIZE - 1] = (uint8_t) (idx + 1);

    snprintf(ds->netid, sizeof(ds->netid), "%s", netid ? netid : "tcp");
    snprintf(ds->uaddr, sizeof(ds->uaddr), "%s", uaddr ? uaddr : "");
    snprintf(ds->backing_path, sizeof(ds->backing_path), "%s", backing_path ? backing_path : "");

    ds->root_fh_len = 0;

    return idx;
} /* chimera_vfs_pnfs_add_device */

SYMBOL_EXPORT void
chimera_vfs_pnfs_set_device_root(
    struct chimera_vfs *vfs,
    int                 idx,
    const void         *root_fh,
    uint32_t            root_fh_len)
{
    struct chimera_vfs_ds *ds;

    if (!vfs->pnfs || idx < 0 || idx >= vfs->pnfs->num_ds) {
        return;
    }

    ds              = &vfs->pnfs->ds[idx];
    ds->root_fh_len = root_fh_len;
    memcpy(ds->root_fh, root_fh, root_fh_len);
} /* chimera_vfs_pnfs_set_device_root */

SYMBOL_EXPORT int
chimera_vfs_pnfs_num_devices(const struct chimera_vfs *vfs)
{
    return vfs->pnfs ? vfs->pnfs->num_ds : 0;
} /* chimera_vfs_pnfs_num_devices */

SYMBOL_EXPORT struct chimera_vfs_ds *
chimera_vfs_pnfs_get_device(
    const struct chimera_vfs *vfs,
    int                       idx)
{
    if (!vfs->pnfs || idx < 0 || idx >= vfs->pnfs->num_ds) {
        return NULL;
    }

    return (struct chimera_vfs_ds *) &vfs->pnfs->ds[idx];
} /* chimera_vfs_pnfs_get_device */

SYMBOL_EXPORT const struct chimera_vfs_ds *
chimera_vfs_pnfs_find_device(
    const struct chimera_vfs *vfs,
    const uint8_t            *deviceid)
{
    int i;

    if (!vfs->pnfs) {
        return NULL;
    }

    for (i = 0; i < vfs->pnfs->num_ds; i++) {
        if (memcmp(vfs->pnfs->ds[i].deviceid, deviceid, CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            return &vfs->pnfs->ds[i];
        }
    }

    return NULL;
} /* chimera_vfs_pnfs_find_device */

SYMBOL_EXPORT struct chimera_vfs_ds *
chimera_vfs_pnfs_steer(struct chimera_vfs *vfs)
{
    struct chimera_vfs_pnfs *pnfs = vfs->pnfs;
    uint32_t                 i, start;

    if (!pnfs || !pnfs->enabled || pnfs->num_ds == 0) {
        return NULL;
    }

    /* Round-robin, but only over data servers whose backing root has been
     * resolved (mounted).  Skip any that aren't ready yet. */
    start = atomic_fetch_add(&pnfs->steer_rr, 1);
    for (i = 0; i < (uint32_t) pnfs->num_ds; i++) {
        struct chimera_vfs_ds *ds = &pnfs->ds[(start + i) % pnfs->num_ds];
        if (ds->root_fh_len) {
            return ds;
        }
    }

    return NULL;
} /* chimera_vfs_pnfs_steer */
