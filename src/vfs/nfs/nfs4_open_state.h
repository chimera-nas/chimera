// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/thread.h"
#include <stdint.h>
#include <stddef.h>
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
 * One belongs to each VFS handle. The server OPEN and pNFS layout belong to
 * the shared chimera_nfs4_open_file, because the server coalesces opens from
 * this client's single open owner and the layout covers the file as a whole.
 */

struct chimera_nfs4_open_state {
    uint8_t                        server_index; /* NFS server index for dispatch routing */
    struct stateid4                stateid; /* NFS4 stateid for this open */
    uint32_t                       seqid;  /* Sequence ID for state operations */
    uint32_t                       access; /* Share access mode */
    atomic_int                     dirty;  /* Count of uncommitted unstable writes */
    int                            silly_renamed; /* File has been silly renamed */
    uint8_t                        dir_fh_len; /* Directory fh for silly remove on close */
    uint8_t                        dir_fh[CHIMERA_VFS_FH_SIZE];

    /*
     * Credentials for silly remove on close.
     */
    struct chimera_vfs_cred        silly_remove_cred;

    /* Shared server OPEN and pNFS layout for this file. */
    struct chimera_nfs4_open_file *open_file;
    struct stateid4                close_stateid;
};

_Static_assert(offsetof(struct chimera_nfs4_open_state, server_index) == 0,
               "NFS close dispatch requires server_index at offset zero");

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
 * The shared layout also keeps DS writes from different handles under one
 * high-water mark and one recall fence.
 *
 * Each OPEN requests its caller's access with SHARE_DENY_NONE.  The server
 * unions access on upgrades; this client retains that union until the last
 * close rather than sending OPEN_DOWNGRADE as individual handles depart.
 */
struct chimera_nfs4_open_file {
    /* All guarded by server->open_state_lock, hash linkage included: the count
    * reaching zero and the unhashing that retires the entry have to be one
    * step, or a concurrent open could revive one already bound for a CLOSE. */
    int                        refcnt;
    /* Wire CLOSEs in flight for this file.  The entry stays hashed while one
     * is outstanding so a concurrent OPEN of the same file can see it: the
     * server keys state on (owner, fh) and this client uses one owner, so an
     * OPEN that lands before an in-flight CLOSE coalesces into the very
     * state that CLOSE then destroys.  chimera_nfs4_open_file_get detects
     * that (see the seqid rule there) and the opener re-sends.  A fresh OPEN
     * may replace this entry in the hash before the CLOSE reply arrives;
     * the old close retains its pointer and owns its layout until completion. */
    int                        closing;
    struct stateid4            stateid;
    uint8_t                    fh_len;
    uint8_t                    fh[CHIMERA_VFS_FH_SIZE];
    struct chimera_nfs4_layout layout;
    UT_hash_handle             hh;
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
    }

    return state;
} /* chimera_nfs4_open_state_alloc */

/*
 * True when a real OPEN stateid was issued by the server.
 *
 * Only chimera_vfs_nfs4_open_at obtains one.  open_fh has no way to OPEN by file
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
