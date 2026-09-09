// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
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

/* The pNFS feature is configured on.  Unlike chimera_vfs_pnfs_enabled() this
 * does NOT require a configured data-server table: a layout-SOURCING backend
 * (CHIMERA_VFS_CAP_LAYOUT_SOURCE) needs no chimera DS table.  Used as the
 * protocol gate for the layout ops and pNFS advertisement; the orchestrated
 * path additionally needs a DS (chimera_vfs_pnfs_steer() returns NULL if none). */
SYMBOL_EXPORT int
chimera_vfs_pnfs_feature_enabled(const struct chimera_vfs *vfs)
{
    return vfs->pnfs && vfs->pnfs->enabled;
} /* chimera_vfs_pnfs_feature_enabled */

SYMBOL_EXPORT int
chimera_vfs_pnfs_add_device(
    struct chimera_vfs *vfs,
    const char         *netid,
    const char         *uaddr,
    const char         *rdma_uaddr,
    const char         *backing_path,
    int                 version,
    int                 minorversion)
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
    snprintf(ds->rdma_uaddr, sizeof(ds->rdma_uaddr), "%s", rdma_uaddr ? rdma_uaddr : "");
    snprintf(ds->backing_path, sizeof(ds->backing_path), "%s", backing_path ? backing_path : "");
    ds->version      = version ? version : 3;
    ds->minorversion = minorversion;

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

/*
 * Opaque per-file pNFS layout blob: [deviceid:16][fhlen:1][backing-fh].
 *
 * The blob is persisted verbatim by the backend as CHIMERA_VFS_ATTR_PNFS_LAYOUT
 * and is otherwise meaningless to it.  It lives here rather than in the NFS
 * server because both consumers need it: the NFS server, to encode a layout for
 * a client, and the VFS data redirect, to find the backing file that MDS-path
 * I/O must be sent to.
 *
 * backing_fh is the handle AS THE MDS HOLDS IT, so it can be handed straight to
 * chimera_vfs_open_fh: for a remote data server that is the nfs module's
 * [mount-id][server-index][native] wrapper, and for a local one (backing_local)
 * a raw VFS handle in the backing mount.  Deriving the client-facing form from
 * it is the NFS server's job (see chimera_nfs4_encode_ff_layout's callers) --
 * the two differ, and only the stored form is openable here.
 */
SYMBOL_EXPORT uint32_t
chimera_vfs_pnfs_blob_pack(
    uint8_t       *blob,
    const uint8_t *deviceid,
    const uint8_t *backing_fh,
    uint32_t       backing_fh_len)
{
    memcpy(blob, deviceid, CHIMERA_VFS_DEVICEID_SIZE);
    blob[CHIMERA_VFS_DEVICEID_SIZE] = (uint8_t) backing_fh_len;
    memcpy(blob + CHIMERA_VFS_DEVICEID_SIZE + 1, backing_fh, backing_fh_len);
    return CHIMERA_VFS_DEVICEID_SIZE + 1 + backing_fh_len;
} /* chimera_vfs_pnfs_blob_pack */

SYMBOL_EXPORT int
chimera_vfs_pnfs_blob_unpack(
    const uint8_t  *blob,
    uint32_t        blob_len,
    const uint8_t **r_deviceid,
    const uint8_t **r_backing_fh,
    uint32_t       *r_backing_fh_len)
{
    uint32_t fh_len;

    if (blob_len < CHIMERA_VFS_DEVICEID_SIZE + 1) {
        return -1;
    }

    fh_len = blob[CHIMERA_VFS_DEVICEID_SIZE];

    if (fh_len == 0 ||
        blob_len < CHIMERA_VFS_DEVICEID_SIZE + 1 + fh_len) {
        return -1;
    }

    if (r_deviceid) {
        *r_deviceid = blob;
    }
    if (r_backing_fh) {
        *r_backing_fh = blob + CHIMERA_VFS_DEVICEID_SIZE + 1;
    }
    if (r_backing_fh_len) {
        *r_backing_fh_len = fh_len;
    }

    return 0;
} /* chimera_vfs_pnfs_blob_unpack */

/*
 * True when `fh` names an object inside a pNFS data-server backing mount.
 *
 * A DS backing file is an ordinary file in an ordinary chimera mount, so
 * without this test the MDS-side data redirect would apply to backing files
 * too: one that somehow carried a layout blob would redirect to itself, and
 * every forwarded I/O would re-enter the redirect.  Residency is switched off
 * for the whole backing mount rather than guarded per file, because no file
 * under it is ever DS-resident by construction -- it IS the data server.
 *
 * A handle's first CHIMERA_VFS_MOUNTID_SIZE bytes are its mount id, and every
 * resolved DS root handle carries its backing mount's, so the test is a
 * comparison against at most CHIMERA_PNFS_MAX_DS ids.  Data servers whose
 * backing root has not been resolved yet have root_fh_len 0 and match nothing;
 * they cannot be steered to either (see chimera_vfs_pnfs_steer).
 */
SYMBOL_EXPORT int
chimera_vfs_pnfs_fh_is_ds_backing(
    const struct chimera_vfs *vfs,
    const void               *fh,
    int                       fhlen)
{
    const struct chimera_vfs_pnfs *pnfs = vfs->pnfs;
    int                            i;

    if (!pnfs || fhlen < CHIMERA_VFS_MOUNTID_SIZE) {
        return 0;
    }

    for (i = 0; i < pnfs->num_ds; i++) {
        const struct chimera_vfs_ds *ds = &pnfs->ds[i];

        if (ds->root_fh_len >= CHIMERA_VFS_MOUNTID_SIZE &&
            memcmp(ds->root_fh, fh, CHIMERA_VFS_MOUNTID_SIZE) == 0) {
            return 1;
        }
    }

    return 0;
} /* chimera_vfs_pnfs_fh_is_ds_backing */

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
