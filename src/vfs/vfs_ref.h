// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "common/misc.h"
#include "vfs/sdk/vfs_error.h"

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_vfs_open_handle;

/*
 * Exported to protocol servers.  Names no layout or pNFS term: a caller learns
 * only whether the VFS is still holding an unlinked file on account of some
 * reference.
 */

/* Switch on tracking of files that are unlinked while referenced.  Call once,
 * before traffic, when the server needs it (the VFS also switches it on when a
 * data server is registered).  With it off the calls below are no-ops. */
SYMBOL_EXPORT void
chimera_vfs_ref_tracking_enable(
    struct chimera_vfs *vfs);

/* True while `fh` names an unlinked file that still has a strong or
 * recallable reference.  False for a linked file, an unknown file, or with
 * tracking off.  Takes one open cache shard lock. */
SYMBOL_EXPORT bool
chimera_vfs_ref_unlinked_referenced(
    struct chimera_vfs *vfs,
    const uint8_t      *fh,
    uint16_t            fh_len);

/*
 * Recallable references.
 *
 * A holder of an open handle can agree that its reference be asked back: it
 * registers a callback, and when the file has been unlinked and no strong
 * reference (a real open) is left, the VFS calls the callback once so the
 * holder can start giving the reference up.  The callback only starts that; the
 * holder releases the reference later, with chimera_vfs_ref_release_recallable,
 * and the file's data goes when the last reference does.
 */

enum chimera_vfs_recall_reason {
    CHIMERA_VFS_RECALL_UNLINKED, /* the file has no link and no strong reference left */
    CHIMERA_VFS_RECALL_UMOUNT,   /* the file's mount is being unmounted */
};

/*
 * Runs on the VFS close thread, outside every VFS lock, so it may call back
 * into the VFS, but it must stay short: it must not wait for the network or for
 * another VFS operation to finish.  It is given the file handle and the holder's
 * `arg`, never the chimera_vfs_recallable, because the holder may release (and
 * free) that on another thread at the same moment.
 */
typedef void (*chimera_vfs_recall_fn)(
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    enum chimera_vfs_recall_reason reason,
    void                          *arg);

struct chimera_vfs_recallable {
    /* Set by the holder before registering. */
    chimera_vfs_recall_fn           fn;
    /* Must outlive every registration that carries it.  The VFS calls each
     * distinct (fn, arg) pair once per file per firing, however many
     * recallable references carry it, so a holder with several references on
     * one file is called once. */
    void                           *arg;

    /* Owned by the VFS from registration to release. */
    struct chimera_vfs_open_handle *handle;
    uint8_t                         fired;
    struct chimera_vfs_recallable  *prev, *next;
};

/*
 * Turn one reference the caller already holds on `handle` into a recallable
 * one.  Never calls r->fn.  Any VFS thread.  Returns CHIMERA_VFS_OK, or
 *   CHIMERA_VFS_ESTALE  the file's purge has been decided;
 *   CHIMERA_VFS_EAGAIN  the file is unlinked and the caller's reference is its
 *                       last strong one, so it would be recalled at once.
 * On error the caller still holds its plain reference and releases it.
 */
SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_ref_make_recallable(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_recallable  *r);

/* Drop a recallable reference: unlink r and release its handle reference.  r
 * may be reused or freed on return.  Any VFS thread, including from the
 * callback itself. */
SYMBOL_EXPORT void
chimera_vfs_ref_release_recallable(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_recallable *r);
