// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs/vfs.h"
#include "nfs_internal.h"
#include <xxhash.h>

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
    uint8_t                         server_index; /* Common NFS3/NFS4 dispatch prefix. */
    struct chimera_nfs_shared      *shared;
    struct chimera_nfs3_open_state *next;
    unsigned int                    bucket;
    atomic_int                      dirty; /* Count of uncommitted unstable writes */
    int                             silly_renamed; /* File has been silly renamed */
    uint8_t                         dir_fh_len; /* Directory fh for silly remove on close */
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                         file_fh_len; /* File fh -> silly name for remove on close */
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];

    /*
     * Credentials for silly remove on close.
     * These are captured from the REMOVE request that triggered the silly rename,
     * NOT from the original open. They are used ONLY for the silly remove RPC
     * when the file is finally closed.
     */
    struct chimera_vfs_cred         silly_remove_cred;

    /*
     * The credential that opened this handle.  POSIX binds I/O rights at
     * open(2); NFS3 is stateless and the server re-checks DAC on every READ /
     * WRITE with whatever credential the RPC carries.  Issuing I/O with the
     * opening credential (exactly what the Linux kernel client's open context
     * does) preserves the opening identity. Non-owners still face current
     * server DAC after chmod; an owner's I/O uses the server owner override.
     */
    int                             open_cred_valid;
    struct chimera_vfs_cred         open_cred;
};

_Static_assert(offsetof(struct chimera_nfs3_open_state, server_index) == 0,
               "NFS close dispatch requires server_index at offset zero");

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
chimera_nfs3_open_state_alloc(
    struct chimera_nfs_shared *shared,
    const uint8_t             *fh,
    int                        fh_len)
{
    struct chimera_nfs3_open_state *state;

    state = calloc(1, sizeof(*state));
    if (state) {
        atomic_init(&state->dirty, 0);
        state->shared      = shared;
        state->file_fh_len = fh_len;
        memcpy(state->file_fh, fh, fh_len);
        state->bucket = XXH3_64bits(fh, fh_len) & 255;
        evpl_mutex_lock(&shared->nfs3_open_lock);
        LL_PREPEND(shared->nfs3_open_states[state->bucket], state);
        evpl_mutex_unlock(&shared->nfs3_open_lock);
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
    const uint8_t                  *file_fh,
    int                             file_fh_len,
    const struct chimera_vfs_cred  *cred)
{
    struct chimera_nfs_shared      *shared = state->shared;
    struct chimera_nfs3_open_state *other;

    evpl_mutex_lock(&shared->nfs3_open_lock);
    LL_FOREACH(shared->nfs3_open_states[state->bucket], other)
    {
        if (other->file_fh_len == file_fh_len &&
            memcmp(other->file_fh, file_fh, file_fh_len) == 0 &&
            other->silly_renamed) {
            evpl_mutex_unlock(&shared->nfs3_open_lock);
            return -1;
        }
    }
    state->silly_renamed = 1;
    state->dir_fh_len    = dir_fh_len;
    memcpy(state->dir_fh, dir_fh, dir_fh_len);
    state->file_fh_len = file_fh_len;
    memcpy(state->file_fh, file_fh, file_fh_len);

    /* Store credentials for silly remove on close */
    if (cred) {
        state->silly_remove_cred = *cred;
    } else {
        memset(&state->silly_remove_cred, 0, sizeof(state->silly_remove_cred));
    }

    evpl_mutex_unlock(&shared->nfs3_open_lock);
    return 1;
} /* chimera_nfs3_open_state_mark_silly */

/* The name pins the file, not one cached open. Transfer its cleanup to another
 * open until the final backend handle closes; otherwise evicting an idle
 * credential/access variant can unlink a file still used by another handle. */
static inline void
chimera_nfs3_open_state_detach(struct chimera_nfs3_open_state *state)
{
    struct chimera_nfs_shared      *shared = state->shared;
    struct chimera_nfs3_open_state *other;

    evpl_mutex_lock(&shared->nfs3_open_lock);
    LL_DELETE(shared->nfs3_open_states[state->bucket], state);
    if (state->silly_renamed) {
        LL_FOREACH(shared->nfs3_open_states[state->bucket], other)
        {
            if (other->file_fh_len == state->file_fh_len &&
                memcmp(other->file_fh, state->file_fh, state->file_fh_len) == 0) {
                other->silly_renamed = 1;
                other->dir_fh_len    = state->dir_fh_len;
                memcpy(other->dir_fh, state->dir_fh, state->dir_fh_len);
                other->silly_remove_cred = state->silly_remove_cred;
                state->silly_renamed     = 0;
                break;
            }
        }
    }
    evpl_mutex_unlock(&shared->nfs3_open_lock);
} /* chimera_nfs3_open_state_detach */
