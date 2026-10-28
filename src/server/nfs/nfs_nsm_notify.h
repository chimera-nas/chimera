// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "sm_inter_xdr.h"
#include "nfs_nsm_state.h"

struct evpl_thread;

struct nsm_notify_job {
    struct nsm_notify_target *targets;
    uint32_t                  count;
    uint32_t                  state;
    char                      my_name[SM_MAXSTRLEN + 1];
    /* The previous worker, or NULL.  The new worker joins it before it starts
     * notifying, so the server thread never waits on a worker and notifies
     * run one at a time, in order. */
    struct evpl_thread       *predecessor;
    struct nsm_state         *nsm;
};

/* Hand job to the reboot-notify machinery; ownership of job passes to it.
 * At most one worker is notifying and one is waiting its turn: a job
 * submitted while a worker is still waiting replaces that worker's
 * targets and name, raises its state to the higher of the two, and starts
 * no thread, so a flood of restarts cannot grow the thread count.  The
 * newest worker is nsm->notify_thread; evpl_thread_destroy on it joins it
 * and, through it, its predecessor.  Setting nsm->notify_stop first makes
 * both give up early: a wait ends at its next 100 ms tick and no further
 * host is started. */
void
nsm_notify_submit(
    struct nsm_state      *nsm,
    struct nsm_notify_job *job);
