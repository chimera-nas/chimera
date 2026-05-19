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

/*
 * NFS4 Open State
 *
 * This structure tracks per-file state for NFS4 opens:
 * 1. Stateid - the NFS4.1 stateid returned by OPEN, needed for READ/WRITE/CLOSE
 * 2. Dirty tracking - to issue COMMIT on close if unstable writes were performed
 * 3. Silly rename - when removing an open file, rename to .nfs<hex(fh)> instead
 *
 * The state is allocated on open, stored in vfs_private, and freed on close.
 */

struct chimera_nfs4_open_state {
    uint8_t                 server_index;  /* NFS server index for dispatch routing */
    struct stateid4         stateid;       /* NFS4 stateid for this open */
    uint32_t                seqid;         /* Sequence ID for state operations */
    uint32_t                access;        /* Share access mode */
    atomic_int              dirty;         /* Count of uncommitted unstable writes */
    int                     silly_renamed; /* File has been silly renamed */
    uint8_t                 dir_fh_len;    /* Directory fh for silly remove on close */
    uint8_t                 dir_fh[CHIMERA_VFS_FH_SIZE];

    /*
     * Credentials for silly remove on close.
     */
    struct chimera_vfs_cred silly_remove_cred;
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
