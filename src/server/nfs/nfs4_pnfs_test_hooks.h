// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* nfs4_pnfs_test_hooks.h -- test-only seams for LAYOUTGET; every pointer is
 * NULL in production.
 *
 * A test sets the pointers while the server is quiescent and clears them
 * once its traffic has drained; the server reads them without
 * synchronisation.  A hook runs on whichever server thread reaches its call
 * site. */

#include <stdint.h>
#include "nfs.h"

struct chimera_server_nfs_thread;
struct chimera_vfs_thread;

struct chimera_nfs_pnfs_test_hooks {
    /* Called by LAYOUTGET after its VFS work is done and just before it
     * registers or advances the layout, i.e. in the window in which a recall
     * of the same file may start on another thread.  `thread` is the thread
     * running the LAYOUTGET; a hook may pass it to
     * chimera_nfs_pnfs_test_recall. */
    void  (*layoutget_before_register)(
        struct chimera_server_nfs_thread *thread,
        const uint8_t                    *fh,
        uint16_t                          fh_len,
        void                             *arg);
    /* Called by LAYOUTGET for a file the client holds no layout on yet, after
     * its VFS work and just before it turns its open of the file into a
     * recallable reference: the window in which the other references on an
     * unlinked file may all go.  `vfs_thread` is the calling thread's. */
    void  (*layoutget_before_recallable)(
        struct chimera_vfs_thread *vfs_thread,
        const uint8_t             *fh,
        uint16_t                   fh_len,
        void                      *arg);
    void *arg;
};

/* CHIMERA_NFS_DATA, not SYMBOL_EXPORT: MSVC users must dllimport data. */
extern CHIMERA_NFS_DATA struct chimera_nfs_pnfs_test_hooks chimera_nfs_pnfs_test_hooks;

/* Start a recall of every layout held on fh, as a conflicting operation does;
 * resume(resume_arg) runs once the last holder has returned. */
SYMBOL_EXPORT void
chimera_nfs_pnfs_test_recall(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    void (                           *resume )(
        void *arg),
    void                             *resume_arg);
