// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs/vfs.h"

/*
 * NFS3 Open State
 *
 * This structure tracks per-file state for NFS3 opens:
 * 1. Dirty tracking - to issue COMMIT on close if unstable writes were performed
 * 2. Silly rename - when removing an open file, rename to .nfs<hex(fh)> instead
 *
 * The state is allocated on open, stored in vfs_private, and freed on close.
 * For remove operations, the VFS open cache is used to check if a file is open.
 */

struct chimera_nfs3_open_state {
    uint8_t                 server_index; /* NFS server index for dispatch routing */
    atomic_int              dirty; /* Count of uncommitted unstable writes */
    int                     silly_renamed; /* File has been silly renamed */
    uint8_t                 dir_fh_len; /* Directory fh for silly remove on close */
    uint8_t                 dir_fh[CHIMERA_VFS_FH_SIZE];

    /*
     * Credentials for silly remove on close.
     * These are captured from the REMOVE request that triggered the silly rename,
     * NOT from the original open. They are used ONLY for the silly remove RPC
     * when the file is finally closed.
     */
    struct chimera_vfs_cred silly_remove_cred;
};

/*
 * Convert a file handle to a silly rename name.
 * Format: .nfs<hex(fh)>
 * The buffer must be at least 5 + (2 * fh_len) + 1 bytes.
 *
 * Returns the length of the generated name (excluding null terminator).
 */
static inline int
chimera_nfs3_silly_name_from_fh(
    const uint8_t *fh,
    int            fh_len,
    char          *out_name,
    int            out_name_max)
{
    static const char hex[] = "0123456789abcdef";
    int               i, len;

    /* .nfs prefix + 2 hex chars per byte + null terminator */
    len = 4 + (fh_len * 2);

    if (len + 1 > out_name_max) {
        return -1;
    }

    out_name[0] = '.';
    out_name[1] = 'n';
    out_name[2] = 'f';
    out_name[3] = 's';

    for (i = 0; i < fh_len; i++) {
        out_name[4 + i * 2]     = hex[(fh[i] >> 4) & 0xf];
        out_name[4 + i * 2 + 1] = hex[fh[i] & 0xf];
    }

    out_name[len] = '\0';

    return len;
} /* chimera_nfs3_silly_name_from_fh */

/*
 * Allocate and initialize a new open state.
 */
static inline struct chimera_nfs3_open_state *
chimera_nfs3_open_state_alloc(void)
{
    struct chimera_nfs3_open_state *state;

    state = calloc(1, sizeof(*state));
    if (state) {
        atomic_init(&state->dirty, 0);
    }

    return state;
} /* chimera_nfs3_open_state_alloc */

/*
 * Free an open state.
 */
static inline void
chimera_nfs3_open_state_free(struct chimera_nfs3_open_state *state)
{
    free(state);
} /* chimera_nfs3_open_state_free */

/*
 * Mark a file as having dirty (unstable) data.
 *
 * Uses atomic increment for lock-free operation on the write hot path.
 * Each UNSTABLE write increments the counter; COMMIT decrements it.
 */
static inline void
chimera_nfs3_open_state_mark_dirty(struct chimera_nfs3_open_state *state)
{
    atomic_fetch_add(&state->dirty, 1);
} /* chimera_nfs3_open_state_mark_dirty */

/*
 * Clear dirty count after a successful COMMIT.
 *
 * This decrements the dirty counter by the count captured before the COMMIT
 * was issued. This handles races correctly: if writes happen during COMMIT,
 * they add to the counter, and after subtracting the pre-captured count,
 * we still see those new writes.
 *
 * Returns the remaining dirty count (> 0 means more uncommitted writes exist).
 */
static inline int
chimera_nfs3_open_state_clear_dirty(
    struct chimera_nfs3_open_state *state,
    int                             committed_count)
{
    return atomic_fetch_sub(&state->dirty, committed_count) - committed_count;
} /* chimera_nfs3_open_state_clear_dirty */

/*
 * Get the current dirty count.
 *
 * Returns the current number of uncommitted unstable writes.
 */
static inline int
chimera_nfs3_open_state_get_dirty(struct chimera_nfs3_open_state *state)
{
    return atomic_load(&state->dirty);
} /* chimera_nfs3_open_state_get_dirty */

/*
 * Mark a file as having been silly renamed.
 *
 * Stores the directory fh and credentials so they can be used to remove
 * the silly file when the file is finally closed. The credentials are
 * captured from the REMOVE request that triggered the silly rename.
 *
 * Returns 1 if successfully marked, -1 if already silly renamed.
 */
static inline int
chimera_nfs3_open_state_mark_silly(
    struct chimera_nfs3_open_state *state,
    const uint8_t                  *dir_fh,
    int                             dir_fh_len,
    const struct chimera_vfs_cred  *cred)
{
    if (state->silly_renamed) {
        return -1;
    }

    state->silly_renamed = 1;
    state->dir_fh_len    = dir_fh_len;
    memcpy(state->dir_fh, dir_fh, dir_fh_len);

    /* Store credentials for silly remove on close */
    if (cred) {
        state->silly_remove_cred = *cred;
    } else {
        memset(&state->silly_remove_cred, 0, sizeof(state->silly_remove_cred));
    }

    return 1;
} /* chimera_nfs3_open_state_mark_silly */
