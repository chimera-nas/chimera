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
};

/* Start the reboot-notify worker for job; ownership of job passes to the
 * worker and is released by evpl_thread_destroy on the returned thread. */
struct evpl_thread *
nsm_notify_spawn(
    struct nsm_notify_job *job);
