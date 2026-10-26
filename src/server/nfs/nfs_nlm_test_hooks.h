// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* nfs_nlm_test_hooks.h -- test-only seams; every pointer is NULL in production.
 *
 * A test sets the pointers while the server is quiescent and clears them
 * once its traffic has drained; the server reads them without
 * synchronisation.  A hook runs on whichever server thread reaches its call
 * site and must not re-enter the NLM server. */

#include <stdbool.h>
#include <stdint.h>
#include "nfs.h"

struct nlm_grant_request;

struct chimera_nfs_nlm_test_hooks {
    /* Called on entry to the lock acquire completion, before nlm_state.mutex. */
    void  (*acquire_cb_enter)(
        int32_t svid,
        bool    was_blocked,
        void   *arg);
    /* Called right after a GRANTED callback is handed to the granter.  Runs
     * under nlm_state.mutex, so it must not block. */
    void  (*grant_submitted)(
        const struct nlm_grant_request *req,
        void                           *arg);
    void *arg;
};

/* CHIMERA_NFS_DATA, not SYMBOL_EXPORT: MSVC users must dllimport data. */
extern CHIMERA_NFS_DATA struct chimera_nfs_nlm_test_hooks chimera_nfs_nlm_test_hooks;
