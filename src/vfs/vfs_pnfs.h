// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>

#include "vfs_attrs.h"   /* CHIMERA_VFS_FH_SIZE */

/*
 * Shared pNFS device table (flex-files layout, RFC 8435).
 *
 * One entry per data server (DS).  A DS is a plain memfs NFS export that this
 * metadata server has mounted via the `nfs` VFS client module; each pNFS file's
 * data lives in a real backing file the MDS creates on a DS.  The table is the
 * single authoritative DS mapping, owned by the VFS layer (reachable via
 * struct chimera_vfs) so both consumers share it:
 *
 *   - the backend (memfs) steers a new file to a DS, creates its backing file
 *     there, and reports the layout segment, and
 *   - the NFS protocol layer answers GETDEVICEINFO with the DS net address.
 *
 * Per DS we keep both the client-facing network address (for the layout) and
 * the MDS-facing backing root: the chimera handle (via the nfs module) of the
 * DS export directory under which the MDS creates backing files.
 */

#define CHIMERA_PNFS_MAX_DS       8
#define CHIMERA_VFS_DEVICEID_SIZE 16     /* == NFS4_DEVICEID4_SIZE */
#define CHIMERA_VFS_MOUNTID_SIZE  16     /* == CHIMERA_VFS_MOUNT_ID_SIZE */
#define CHIMERA_PNFS_BACKING_MAX  256

struct chimera_vfs;

struct chimera_vfs_ds {
    uint8_t  deviceid[CHIMERA_VFS_DEVICEID_SIZE]; /* stable id advertised to clients */
    char     netid[8];                            /* RFC5665 netid, e.g. "tcp"       */
    char     uaddr[64];                           /* RFC5665 universal address (DS)  */
    char     backing_path[CHIMERA_PNFS_BACKING_MAX]; /* chimera path where the DS is
                                                      * nfs-mounted, e.g. "/ds0"     */
    /* Resolved once the backing mount is established: the DS export root as an
     * nfs-module chimera file handle, under which the MDS creates backing
     * files (root_fh_len > 0 marks the DS ready for steering). */
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t root_fh_len;
};

struct chimera_vfs_pnfs {
    int                   enabled;
    int                   num_ds;
    _Atomic uint32_t      steer_rr;               /* round-robin steering counter */
    struct chimera_vfs_ds ds[CHIMERA_PNFS_MAX_DS];
};

struct chimera_vfs_pnfs * chimera_vfs_pnfs_create(
    void);

void chimera_vfs_pnfs_destroy(
    struct chimera_vfs_pnfs *pnfs);

void chimera_vfs_pnfs_set_enabled(
    struct chimera_vfs *vfs,
    int                 enabled);

int chimera_vfs_pnfs_enabled(
    const struct chimera_vfs *vfs);

/*
 * Register a data server.  The deviceid is assigned deterministically from the
 * registration order so it is stable for the lifetime of the server instance.
 * backing_path is the chimera pseudo-path where the DS export is mounted via
 * the nfs module; its root handle is resolved later (after mounts) with
 * chimera_vfs_pnfs_set_device_root().  Returns the device index or -1.
 */
int chimera_vfs_pnfs_add_device(
    struct chimera_vfs *vfs,
    const char         *netid,
    const char         *uaddr,
    const char         *backing_path);

/* Record the resolved DS backing-root file handle for device idx. */
void chimera_vfs_pnfs_set_device_root(
    struct chimera_vfs *vfs,
    int                 idx,
    const void         *root_fh,
    uint32_t            root_fh_len);

int chimera_vfs_pnfs_num_devices(
    const struct chimera_vfs *vfs);

struct chimera_vfs_ds * chimera_vfs_pnfs_get_device(
    const struct chimera_vfs *vfs,
    int                       idx);

const struct chimera_vfs_ds * chimera_vfs_pnfs_find_device(
    const struct chimera_vfs *vfs,
    const uint8_t            *deviceid);

/*
 * Choose a data server for a newly created file.  Returns the chosen device,
 * or NULL if pNFS is disabled / no devices are configured / no DS has had its
 * backing root resolved yet.
 */
struct chimera_vfs_ds * chimera_vfs_pnfs_steer(
    struct chimera_vfs *vfs);
