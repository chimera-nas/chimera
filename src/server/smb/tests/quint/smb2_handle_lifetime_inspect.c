// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#define _GNU_SOURCE
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include <stdatomic.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"

atomic_int                          lifetime_arm, lifetime_held, lifetime_resume, lifetime_attempts;
atomic_int                          lifetime_fallback_close, lifetime_native_closes;
static                              _Thread_local struct chimera_smb_request *pending;
static struct chimera_smb_request  *held_request;
static struct chimera_vfs_compound *held_compound;
static struct evpl_timer            hold_timer;
static bool                         retry;

__attribute__((visibility("default"))) int
chimera_smb_vfs_compound_try(struct chimera_smb_compound *wire)
{
    typedef int (*try_fn)(
        struct chimera_smb_compound *);
    try_fn                      next = dlsym(RTLD_NEXT, "chimera_smb_vfs_compound_try");
    assert(next);
    struct chimera_smb_request *request = wire->requests[wire->complete_requests];
    if (request->smb2_hdr.command == SMB2_CLOSE) {
        if (atomic_load(&lifetime_fallback_close)) {
            return 0;
        }
        int admitted = next(wire);
        assert(admitted);
        atomic_fetch_add(&lifetime_native_closes, 1);
        return admitted;
    }
    int mode = atomic_exchange(&lifetime_arm, 0);
    if (mode) {
        assert(!pending);
        pending = request;
        retry   = mode == 2;
        return 0;
    }
    return next(wire);
} /* chimera_smb_vfs_compound_try */

static void
resume_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_load(&lifetime_resume)) {
        evpl_add_oneshot_timer(evpl, timer, resume_tick, 1000);
        return;
    }
    /* Real wire work below must still be able to use its admitted handle:
    * QUERY_INFO builds another sequence, and READ may replay this one. */
    chimera_vfs_compound_finish_result(held_compound,
                                       retry ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* resume_tick */

static void
held_finish(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    (void) arg;
    assert(chimera_vfs_compound_execution_status(compound) == CHIMERA_VFS_OK);
    if (atomic_fetch_add(&lifetime_attempts, 1)) {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
        return;
    }
    held_compound = compound;
    evpl_add_oneshot_timer(held_request->compound->thread->evpl,
                           &hold_timer, resume_tick, 1000);
    atomic_store(&lifetime_held, 1);
} /* held_finish */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *arg)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn next = dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (pending && pending->vfs_compound == compound) {
        assert(!chimera_vfs_compound_num_groups(compound));
        held_request = pending;
        pending      = NULL;
        chimera_vfs_compound_set_finish_handler(compound, held_finish, NULL);
    }
    next(compound, callback, arg);
} /* chimera_vfs_compound_submit */
