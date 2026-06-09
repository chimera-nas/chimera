// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * NFSv4.1 pNFS CLIENT support (flex-files, RFC 8435).
 *
 * The chimera NFS4 client is a VFS proxy module.  When pNFS is enabled on a
 * mount and the metadata server (MDS) confirms USE_PNFS_MDS, the client fetches
 * a flex-files layout for a file (LAYOUTGET), resolves the data server it points
 * at (GETDEVICEINFO), and then drives READ/WRITE straight to that data server
 * (DS) over NFSv3 -- a DS is modeled as another dynamically-registered
 * chimera_nfs_client_server, so DS I/O reuses the existing v3 send path.  On
 * close the client reports the new high-water mark back to the MDS
 * (LAYOUTCOMMIT) and returns the layout (LAYOUTRETURN).
 *
 * pNFS is a pure optimization overlay: whenever it is disabled, the MDS is not
 * pNFS-capable, no usable layout exists, or any step fails, the client behaves
 * exactly as the non-pNFS path and issues the I/O to the MDS.
 *
 * The opaque loc_body (ff_layout4) and da_addr_body (ff_device_addr4) are
 * hand-decoded here, the read-side inverse of the server's hand-encoders in
 * src/server/nfs/nfs4_pnfs.c (the flex-files XDR is not in the generated
 * nfs4.x).  Block-layout decoding is intentionally deferred (Phase 2).
 */

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "vfs/vfs.h"
#include "vfs/vfs_pnfs.h"     /* chimera_vfs_layout_segment / _device, classes */
#include "nfs4_xdr.h"         /* struct stateid4 */

/* Layout-type wire constants.  LAYOUT4_BLOCK_VOLUME is in the generated
 * layouttype4 enum; flex-files (0x4) is not, so define it explicitly exactly as
 * the server does. */
#define CHIMERA_NFS4_LAYOUT4_FLEX_FILES   0x4
#define CHIMERA_NFS4_LAYOUT4_BLOCK_VOLUME 0x3

/* First cut: chimera's MDS hands out whole-file, single-mirror, single-DS
* flex-files layouts, so one segment is all the client tracks per file. */
#define CHIMERA_NFS4_CLIENT_MAX_SEGMENTS  1

/* Per-file layout-acquisition state (atomic, drives one-shot LAYOUTGET). */
enum chimera_nfs4_layout_acq_state {
    CHIMERA_NFS4_LAYOUT_NONE      = 0,  /* calloc zero: not yet attempted     */
    CHIMERA_NFS4_LAYOUT_ACQUIRING = 1,  /* a LAYOUTGET is in flight           */
    CHIMERA_NFS4_LAYOUT_VALID     = 2,  /* layout + device resolved, usable   */
    CHIMERA_NFS4_LAYOUT_UNAVAIL   = 3,  /* sticky: acquisition failed, use MDS */
};

/*
 * Per-open-file decoded layout, embedded in chimera_nfs4_open_state.  Embedded
 * (not a pointer) so allocation never races across threads; the `state` field
 * gates concurrent first-touch with an atomic compare-exchange.
 */
struct chimera_nfs4_layout {
    atomic_int                        state;        /* enum ..._acq_state       */
    uint32_t                          iomode;       /* LAYOUTIOMODE4 we hold    */
    struct stateid4                   layout_stateid; /* from LAYOUTGET          */
    uint32_t                          num_segments;
    struct chimera_vfs_layout_segment segments[CHIMERA_NFS4_CLIENT_MAX_SEGMENTS];
    int                               ds_server_index; /* resolved DS server slot */
    int                               return_on_close;
    int                               layoutcommit_needed;
    /* Highest byte+1 written via the DS, reported to the MDS at LAYOUTCOMMIT.
     * Atomic because concurrent writes on a shared open handle each update it;
     * losing the max would truncate the file size the MDS records. */
    _Atomic uint64_t                  last_write_offset;

    /* The file's own local handle, captured at LAYOUTGET, so close can PUTFH it
     * to the MDS for LAYOUTCOMMIT/LAYOUTRETURN (close carries no fh). */
    uint8_t                           file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          file_fh_len;

    /* Membership in shared->pnfs_layouts (the layout registry), so the
     * back-channel CB_LAYOUTRECALL handler can find this layout by file handle
     * and fence its DS I/O.  Guarded by shared->pnfs_layout_lock. */
    int                               registered;
    struct chimera_nfs4_layout       *reg_next;

    /* While state == ACQUIRING, concurrent read/write requests for this file
     * are parked here (linked via request->next) instead of being sent to the
     * MDS -- a fallback would inject a compound on the same NFSv4.1 session slot
     * as the in-flight LAYOUTGET/GETDEVICEINFO and the server would reject the
     * out-of-order seqid (NFS4ERR_SEQ_MISORDERED).  They are replayed once
     * acquisition resolves.  acq_thread/shared/private are the dispatch context
     * captured when acquisition started, used to replay parked requests. */
    pthread_mutex_t                   acq_lock;
    struct chimera_vfs_request       *acq_waiters;
    struct chimera_nfs_thread        *acq_thread;
    struct chimera_nfs_shared        *acq_shared;
    void                             *acq_private;
};

struct chimera_nfs_thread;
struct chimera_nfs_shared;
struct chimera_nfs_client_server_thread;
struct chimera_nfs4_open_state;

/*
 * Read/write redirect entry points.  Each returns 1 if it took ownership of the
 * request (a DS I/O was issued, or a layout acquisition was kicked off and the
 * request will be re-dispatched on completion), or 0 if the caller should fall
 * back to issuing the I/O to the MDS.  server_thread is the MDS server thread.
 */
int chimera_nfs4_pnfs_read(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_vfs_request              *request,
    void                                    *private_data,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_nfs4_open_state          *open_state);

int chimera_nfs4_pnfs_write(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_vfs_request              *request,
    void                                    *private_data,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_nfs4_open_state          *open_state);

/*
 * Close-time layout teardown.  Returns 1 if it took ownership of the request
 * (it will asynchronously LAYOUTCOMMIT/LAYOUTRETURN, then free the open state
 * and complete the request), or 0 if there was no layout and the caller should
 * free + complete as usual.
 */
int chimera_nfs4_pnfs_close(
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    struct chimera_nfs4_open_state *open_state);

/*
 * Commit-time layout flush.  Returns 1 if it took ownership of the request (a
 * LAYOUTCOMMIT was issued to report the file's high-water size to the MDS and
 * the request will complete on its reply), or 0 if there is nothing to flush
 * and the caller should complete the commit itself.  Unlike close, the layout
 * is NOT returned -- the file stays open.
 */
int chimera_nfs4_pnfs_commit(
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    struct chimera_nfs4_open_state *open_state);

/*
 * Layout registry (shared->pnfs_layouts).  register publishes a now-VALID
 * layout so a back-channel CB_LAYOUTRECALL can find it; unregister removes it
 * before the owning open state is freed.  Both are idempotent and safe to call
 * for layouts that were never registered.
 */
void chimera_nfs4_pnfs_layout_register(
    struct chimera_nfs_shared  *shared,
    struct chimera_nfs4_layout *layout);

void chimera_nfs4_pnfs_layout_unregister(
    struct chimera_nfs_shared  *shared,
    struct chimera_nfs4_layout *layout);
