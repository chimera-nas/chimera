// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_pnfs.h"

/*
 * The unlink side of tracking a file that is unlinked while referenced (see
 * vfs_unlinked.h).  remove_at and rename_at, which every protocol reaches, use
 * these steps around the unlink they perform when the victim is a regular file
 * whose data may live on a data server:
 *
 *   1. find out what the victim is (a lookup, unless the caller already has its
 *      file handle),
 *   2. take a pin on it: a real open, which must come before the unlink because
 *      diskfs will not open by handle a file that has no links and no handle,
 *   3. let the unlink run,
 *   4. read the link count through the pin; if it is zero create the record
 *      that tells the purge what to delete, and
 *   5. drop the pin through the normal release path.
 *
 * A server that has not switched tracking on does none of this.
 */

struct chimera_vfs_unlink_pin;

/* What the lookup in step 1 learnt, or all the caller knew. */
struct chimera_vfs_unlink_victim {
    uint8_t  fh[CHIMERA_VFS_FH_SIZE + 16];
    uint16_t fh_len;
    /* The type and layout are only known after a lookup; a caller that brought
     * the file handle gets them from the pin's attributes instead. */
    uint8_t  type_known;
    uint8_t  regular;
    uint8_t  have_ino;
    uint64_t ino;
    uint32_t layout_len;
    uint8_t  layout[CHIMERA_VFS_PNFS_LAYOUT_MAX];
};

typedef void (*chimera_vfs_unlink_victim_cb)(
    int                                     found,
    const struct chimera_vfs_unlink_victim *victim,
    void                                   *private_data);

/* True when the unlink of `name` in the directory `parent_fh` should be
 * tracked: tracking is on and the name is not a data server's backing file
 * (the data itself, never placed on a data server). */
int
chimera_vfs_unlink_track_wanted(
    struct chimera_vfs_thread *thread,
    const uint8_t             *parent_fh,
    int                        parent_fh_len,
    const char                *name,
    int                        namelen);

/* Step 1 by name.  `found` is 0 when the name does not resolve. */
void
chimera_vfs_unlink_victim_lookup(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    int                            parent_fh_len,
    const char                    *name,
    int                            namelen,
    chimera_vfs_unlink_victim_cb   callback,
    void                          *private_data);

typedef void (*chimera_vfs_unlink_pin_cb)(
    struct chimera_vfs_unlink_pin *pin,
    void                          *private_data);

/* Step 2.  Calls back with the pin, or with NULL when the unlink should simply
 * go ahead untracked (the victim is not a regular file, or the pin could not be
 * taken, which is logged). */
void
chimera_vfs_unlink_pin_take(
    struct chimera_vfs_thread              *thread,
    const struct chimera_vfs_cred          *cred,
    const struct chimera_vfs_unlink_victim *victim,
    chimera_vfs_unlink_pin_cb               callback,
    void                                   *private_data);

/* Steps 4 and 5.  `unlink_status` is how the unlink went; on failure the pin is
* only dropped.  Calls `done` once the record exists (or has been ruled out). */
void
chimera_vfs_unlink_pin_finish(
    struct chimera_vfs_unlink_pin *pin,
    enum chimera_vfs_error         unlink_status,
    void (                        *done )(void *private_data),
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_stats(
    uint64_t *r_pins,
    uint64_t *r_lookups);
