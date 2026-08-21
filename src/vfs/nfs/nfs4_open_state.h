// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs/vfs.h"
#include "nfs4_xdr.h"
#include "nfs4_pnfs.h"

/*
 * NFS4 Open State
 *
 * This structure tracks per-handle state for NFS4 opens:
 * 1. Stateid - the NFS4.1 stateid returned by OPEN, needed for READ/WRITE/CLOSE
 * 2. Dirty tracking - to issue COMMIT on close if unstable writes were performed
 * 3. Silly rename - when removing an open file, rename to .nfs<hex(fh)> instead
 *
 * One of these belongs to each VFS handle, and everything in it is that
 * handle's own -- the layout above all.  A layout is a per-handle thing here:
 * only the handle that did the LAYOUTGET drives its I/O to the data server,
 * while a handle without one writes to the MDS and the file size follows as a
 * matter of course.  Give two handles on a file one layout between them and
 * every write goes to the DS, leaving the size to reach the MDS only via a
 * LAYOUTCOMMIT that is issued lazily -- and a recall can fence the layout with
 * the writes still unpublished.
 *
 * What is *not* per handle is the open on the server, which is why the CLOSE
 * that ends it is counted separately in chimera_nfs4_open_file.
 */

struct chimera_nfs4_open_state {
    uint8_t                    server_index; /* NFS server index for dispatch routing */
    struct stateid4            stateid;    /* NFS4 stateid for this open */
    uint32_t                   seqid;      /* Sequence ID for state operations */
    uint32_t                   access;     /* Share access mode */
    atomic_int                 dirty;      /* Count of uncommitted unstable writes */
    int                        silly_renamed; /* File has been silly renamed */
    uint8_t                    dir_fh_len; /* Directory fh for silly remove on close */
    uint8_t                    dir_fh[CHIMERA_VFS_FH_SIZE];

    /*
     * Credentials for silly remove on close.
     */
    struct chimera_vfs_cred    silly_remove_cred;

    /*
     * pNFS (flex-files) per-handle layout.  Zero-initialized by calloc means
     * layout.state == CHIMERA_NFS4_LAYOUT_NONE (no layout yet); only touched
     * when pNFS is enabled and the MDS is pNFS-capable.
     */
    struct chimera_nfs4_layout layout;
};

/*
 * The open a file has on the server, which the handles above share whether they
 * mean to or not.
 *
 * The server keys open state on (open-owner, file handle) and this client uses
 * one open-owner per server (server->nfs4_owner_id), so a second OPEN of a file
 * already open is an upgrade -- the same stateid back with a bumped seqid --
 * and not an independent open (RFC 8881 §9.1.1).  A single CLOSE therefore ends
 * the file for every opener, so it may only be sent once the last handle is
 * done; sending one per handle destroys state the others are still reading and
 * writing through.
 *
 * Counting that here, rather than sharing the whole open state, is what keeps
 * the layout per handle: this holds only what the wire CLOSE needs.
 *
 * Share bits need no such care because every OPEN this client sends asks for
 * SHARE_ACCESS_BOTH / SHARE_DENY_NONE, so an upgrade never widens anything and
 * there is nothing for OPEN_DOWNGRADE to narrow on the way out.
 */
struct chimera_nfs4_open_file {
    /* All guarded by server->open_state_lock, hash linkage included: the count
    * reaching zero and the unhashing that retires the entry have to be one
    * step, or a concurrent open could revive one already bound for a CLOSE. */
    int             refcnt;
    struct stateid4 stateid;
    uint8_t         fh_len;
    uint8_t         fh[CHIMERA_VFS_FH_SIZE];
    UT_hash_handle  hh;
};

/*
 * Allocate and initialize a new open state.
 */
static inline struct chimera_nfs4_open_state *
chimera_nfs4_open_state_alloc(void)
{
    struct chimera_nfs4_open_state *state;

    state = calloc(1, sizeof(*state));
    if (state) {
        atomic_init(&state->dirty, 0);
        state->seqid = 1;
        pthread_mutex_init(&state->layout.acq_lock, NULL);
    }

    return state;
} /* chimera_nfs4_open_state_alloc */

/*
 * True when a real OPEN stateid was issued by the server.
 *
 * Only chimera_nfs4_open_at obtains one.  open_fh has no way to OPEN by file
 * handle (that needs CLAIM_FH), so a file reached only that way keeps the
 * all-zero anonymous stateid and READ/WRITE fall back to it.  Such an open holds
 * nothing on the server, so CLOSE has nothing to release -- and a CLOSE naming
 * the anonymous stateid would be rejected with NFS4ERR_BAD_STATEID.
 */
static inline int
chimera_nfs4_stateid_is_open(const struct stateid4 *stateid)
{
    static const struct stateid4 anonymous = { 0 };

    return memcmp(stateid, &anonymous, sizeof(anonymous)) != 0;
} /* chimera_nfs4_stateid_is_open */

/*
 * Free an open state.
 */
static inline void
chimera_nfs4_open_state_free(struct chimera_nfs4_open_state *state)
{
    pthread_mutex_destroy(&state->layout.acq_lock);
    free(state);
} /* chimera_nfs4_open_state_free */

/*
 * Mark a file as having dirty (unstable) data.
 */
static inline void
chimera_nfs4_open_state_mark_dirty(struct chimera_nfs4_open_state *state)
{
    atomic_fetch_add(&state->dirty, 1);
} /* chimera_nfs4_open_state_mark_dirty */

/*
 * Clear dirty count after a successful COMMIT.
 */
static inline int
chimera_nfs4_open_state_clear_dirty(
    struct chimera_nfs4_open_state *state,
    int                             committed_count)
{
    return atomic_fetch_sub(&state->dirty, committed_count) - committed_count;
} /* chimera_nfs4_open_state_clear_dirty */

/*
 * Get the current dirty count.
 */
static inline int
chimera_nfs4_open_state_get_dirty(struct chimera_nfs4_open_state *state)
{
    return atomic_load(&state->dirty);
} /* chimera_nfs4_open_state_get_dirty */
