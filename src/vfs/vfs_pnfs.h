// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>      /* snprintf, for chimera_vfs_pnfs_backing_name */
#include <inttypes.h>   /* PRIx64 */

#include "sdk/vfs_attrs.h"          /* CHIMERA_VFS_FH_SIZE */
#include "sdk/vfs_error.h"          /* enum chimera_vfs_error */
#include "sdk/vfs_pnfs_layout.h"  /* layout segment/device descriptors */

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

#define CHIMERA_PNFS_MAX_DS      8
#define CHIMERA_VFS_MOUNTID_SIZE 16 /* == CHIMERA_VFS_MOUNT_ID_SIZE */
#define CHIMERA_PNFS_BACKING_MAX 256

struct chimera_vfs;

struct chimera_vfs_ds {
    uint8_t  deviceid[CHIMERA_VFS_DEVICEID_SIZE]; /* stable id advertised to clients */
    char     netid[8];              /* RFC5665 netid, e.g. "tcp"       */
    char     uaddr[64];             /* RFC5665 universal address (DS)  */
    /* Optional second (RDMA) universal address.  When non-empty the device
     * advertises BOTH this "rdma" netaddr (preferred) and the "tcp" netaddr
     * above, so RDMA-capable clients use RDMA and others fall back to TCP. */
    char     rdma_uaddr[64];
    int      version;               /* NFS version advertised for DS   */
    int      minorversion;          /* NFS minor version (4.x)         */
    uint8_t  backing_local;         /* 1 = backing is a local (non-nfs)
                                     * mount served by this server, so
                                     * the DS handle is the backing
                                     * handle as-is; 0 = nfs-proxy DS,
                                     * strip the wrapper to recover the
                                     * remote native handle            */
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

int chimera_vfs_pnfs_feature_enabled(
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
    const char         *rdma_uaddr,
    const char         *backing_path,
    int                 version,
    int                 minorversion);

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

struct chimera_vfs_thread;
struct chimera_vfs_cred;
struct chimera_vfs_open_handle;

/*
 * Resolve which handle a data operation on `handle` must actually be issued
 * against.  `io_handle` is the DS backing file when the file is DS-resident
 * (redirected = 1, and the caller owns a reference it must chimera_vfs_release)
 * or `handle` itself otherwise (redirected = 0, no reference taken).
 *
 * Resolved lazily on the first data operation rather than at open, so a pure
 * pNFS client -- which does its I/O on the data server and never issues an
 * MDS-path data operation -- pays nothing for a fallback it does not use.  The
 * callback may fire synchronously on the fast path.
 */
typedef void (*chimera_vfs_pnfs_io_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *io_handle,
    int                             redirected,
    void                           *private_data);

/* Cheap predicate: could a data op on this handle need redirecting at all?
 * False for every handle when no data server is configured, and for anything
 * inside a data server's own backing mount.  Lets the caller skip allocating
 * an async context on the common path. */
int chimera_vfs_pnfs_io_possible(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_open_handle *handle);

struct chimera_vfs_request;
struct chimera_vfs_attrs;

typedef void (*chimera_vfs_pnfs_sync_callback_t)(
    struct chimera_vfs_request *request);

/* Drop-in replacement for chimera_vfs_dispatch() in a data op: sends the op to
 * wherever the file's bytes live.  required_cap is the backend capability the
 * caller gated on (0 if none), re-checked after a redirect because the data
 * server's backend may not implement it. */
void chimera_vfs_pnfs_dispatch(
    struct chimera_vfs_request *request,
    int                         for_write,
    uint64_t                    required_cap);

/* Push the size/mtime a redirected op produced on the backing file back onto
 * the MDS inode, then continue to `next`.  A no-op (straight to `next`) when the
 * op was not redirected or did not succeed. */
void chimera_vfs_pnfs_sync_mds(
    struct chimera_vfs_request      *request,
    const struct chimera_vfs_attrs  *backing_post,
    uint64_t                         end_offset,
    chimera_vfs_pnfs_sync_callback_t next);

void chimera_vfs_pnfs_resolve_io(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    int                             for_write,
    chimera_vfs_pnfs_io_callback_t  callback,
    void                           *private_data);

/*
 * Pack/unpack the opaque per-file layout blob the backend persists as
 * CHIMERA_VFS_ATTR_PNFS_LAYOUT: [deviceid:16][fhlen:1][backing-fh].  The
 * backing handle is stored as the MDS holds it, so it can be passed directly to
 * chimera_vfs_open_fh; the client-facing form is derived from it by the NFS
 * server.  unpack returns 0 and points the out-params into `blob` (no copy), or
 * -1 if the blob is malformed or truncated.
 */
uint32_t chimera_vfs_pnfs_blob_pack(
    uint8_t       *blob,
    const uint8_t *deviceid,
    const uint8_t *backing_fh,
    uint32_t       backing_fh_len);

int chimera_vfs_pnfs_blob_unpack(
    const uint8_t  *blob,
    uint32_t        blob_len,
    const uint8_t **r_deviceid,
    const uint8_t **r_backing_fh,
    uint32_t       *r_backing_fh_len);

/* Buffer size for chimera_vfs_pnfs_backing_name: <mountid hex> '_' <fileid hex>. */
#define CHIMERA_VFS_PNFS_BACKING_NAME_MAX (CHIMERA_VFS_MOUNTID_SIZE * 2 + 1 + 16 + 1)

/*
 * Name of the data-server file backing an MDS file, written into `name` (at
 * least CHIMERA_VFS_PNFS_BACKING_NAME_MAX bytes).  Backing files live flat in a
 * single directory on the data server, so the name has to identify the MDS file
 * globally.
 *
 * A fileid is unique only within one filesystem, so it cannot do that alone:
 * every MDS filesystem sharing a data server would collide in that flat
 * namespace -- two unrelated files, one backing file.  Qualifying it with the
 * MDS mount id (the leading CHIMERA_VFS_MOUNTID_SIZE bytes every handle carries)
 * makes the pair unique.  TRUNCATE at create time does NOT cover this: it
 * "resolves" a collision by destroying the other file's data.
 *
 * Both paths that can create a backing file -- LAYOUTGET (nfs4_pnfs.c) and the
 * non-pNFS write redirect (vfs_pnfs_io.c) -- must agree on it, or a file
 * materialized by one is invisible to the other.  Hence one shared helper.
 */
static inline void
chimera_vfs_pnfs_backing_name(
    char          *name,
    const uint8_t *mds_fh,
    uint64_t       fileid)
{
    int i, n = 0;

    for (i = 0; i < CHIMERA_VFS_MOUNTID_SIZE; i++) {
        n += snprintf(name + n, CHIMERA_VFS_PNFS_BACKING_NAME_MAX - n, "%02x",
                      mds_fh[i]);
    }
    snprintf(name + n, CHIMERA_VFS_PNFS_BACKING_NAME_MAX - n, "_%016" PRIx64,
             fileid);
} /* chimera_vfs_pnfs_backing_name */

/*
 * True when fh names an object inside a data-server backing mount, i.e. one of
 * the mounts named by chimera_vfs_ds.backing_path.  Such a mount is the storage
 * behind other files' layouts, so nothing under it is ever itself DS-resident;
 * the data redirect uses this to avoid re-entering itself.
 */
int chimera_vfs_pnfs_fh_is_ds_backing(
    const struct chimera_vfs *vfs,
    const void               *fh,
    int                       fhlen);

/*
 * Choose a data server for a newly created file.  Returns the chosen device,
 * or NULL if pNFS is disabled / no devices are configured / no DS has had its
 * backing root resolved yet.
 */
struct chimera_vfs_ds * chimera_vfs_pnfs_steer(
    struct chimera_vfs *vfs);
