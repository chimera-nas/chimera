// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * KV-fallback lease persistence (Phase 4): for a non-CAP_LEASE backend the core
 * persists each durable protocol lease to a KV store and removes it on release;
 * chimera_vfs_lease_recover_mount() scans them back at restart.
 *
 * This drives a real VFS instance (so the lease service thread exists and the
 * KV ops dispatch) with the default memkv KV.  A lease is acquired on a file
 * handle that resolves to no module (routes to the default KV), and we verify a
 * record appears, then disappears after release, via the recovery scan.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_internal.h"
#include "common/logging.h"
#include "prometheus-c.h"

static int passed = 0, failed = 0;
#define CHECK(cond, name) do { if (cond) { fprintf(stderr, "  PASS: %s\n", name); passed++; } \
                               else { fprintf(stderr, "  FAIL: %s\n", name); failed++; } } while (0)

struct ctx {
    struct evpl               *evpl;
    struct chimera_vfs        *vfs;
    struct chimera_vfs_thread *vfs_thread;
};

/* --- recovery scan: count records --------------------------------------- */
struct recover_state {
    int count;
    int done;
};

static int
recover_count_cb(
    const struct chimera_vfs_lease *lease,
    const uint8_t                  *fh,
    uint8_t                         fh_len,
    void                           *private_data)
{
    struct recover_state *rs = private_data;

    (void) lease; (void) fh; (void) fh_len;
    rs->count++;
    return 0;
} /* recover_count_cb */

static void
recover_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct recover_state *rs = private_data;

    (void) error_code;
    rs->done = 1;
} /* recover_complete */

/* Run a recovery scan over the fake fh's mount and return the record count. */
static int
recover_count(
    struct ctx    *ctx,
    const uint8_t *fh,
    uint8_t        fh_len)
{
    struct recover_state rs = { 0 };

    chimera_vfs_lease_recover_mount(ctx->vfs_thread, fh, fh_len,
                                    recover_count_cb, recover_complete, &rs);
    while (!rs.done) {
        evpl_continue(ctx->evpl);
    }
    return rs.count;
} /* recover_count */

static void
acquire_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted_lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    int *done = private_data;

    (void) granted_lease; (void) conflict;
    assert(result == CHIMERA_VFS_LEASE_GRANTED);
    *done = 1;
} /* acquire_cb */

int
main(
    int    argc,
    char **argv)
{
    struct ctx                         ctx = { 0 };
    struct chimera_vfs_module_cfg      module_cfgs[1];
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_lease           lease;
    struct chimera_vfs_pending_acquire ticket;
    uint8_t                            fh[CHIMERA_VFS_FH_SIZE];
    uint64_t                           fh_hash;
    int                                acq_done = 0;
    int                                i, count;

    (void) argc; (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memkv",
            sizeof(module_cfgs[0].module_name) - 1);

    struct prometheus_metrics *metrics = prometheus_metrics_create(NULL, NULL, 0);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl);
    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 1, "memkv", 60, 0, metrics);
    assert(ctx.vfs);
    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread);

    /* The lease service thread (close thread) starts asynchronously; persistence
     * is skipped until it is up.  Wait for it before acquiring. */
    for (i = 0; i < 100000 && !ctx.vfs->vfs_state->service_thread; i++) {
        evpl_continue(ctx.evpl);
        usleep(100);
    }
    CHECK(ctx.vfs->vfs_state->service_thread != NULL, "lease service thread is up");

    /* A fake fh resolves to no module -> KV ops route to the default memkv. */
    memset(fh, 0, sizeof(fh));
    fh[0]                       = 0x77;
    fh[CHIMERA_VFS_FH_SIZE - 1] = 0x42;
    fh_hash                     = chimera_vfs_hash(fh, sizeof(fh));

    file = chimera_vfs_state_get(ctx.vfs->vfs_state, fh, sizeof(fh), fh_hash, true);
    assert(file);

    /* No leases persisted yet. */
    CHECK(recover_count(&ctx, fh, sizeof(fh)) == 0, "no records before acquire");

    /* Acquire a byte-range write lock for an NLM owner (a durable, persisted
     * lease -- not the ephemeral INTERNAL implicit lease). */
    memset(&lease, 0, sizeof(lease));
    lease.kind             = CHIMERA_VFS_LEASE_RANGE;
    lease.mode.granted     = CHIMERA_VFS_LEASE_MODE_W;
    lease.offset           = 4096;
    lease.length           = 1024;
    lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NLM;
    lease.owner.client_key = 0xABCD;
    lease.owner.owner_lo   = 0x1234;

    chimera_vfs_lease_acquire(ctx.vfs_thread, ctx.vfs->vfs_state, file, &lease,
                              &ticket, false, acquire_cb, &acq_done);
    CHECK(acq_done == 1, "lease acquired (granted synchronously)");

    /* The persist is marshaled to the (separate) lease service thread; the
     * recovery scan runs inline on this thread, so just re-scan with a short
     * delay until the record appears.  (Pumping this thread's evpl here is
     * unnecessary and can block in epoll_wait with nothing pending.) */
    count = 0;
    for (i = 0; i < 5000; i++) {
        count = recover_count(&ctx, fh, sizeof(fh));
        if (count >= 1) {
            break;
        }
        usleep(1000);
    }
    CHECK(count == 1, "persisted lease record recovered after acquire");

    /* Release -> the record must be removed. */
    chimera_vfs_lease_release(ctx.vfs->vfs_state, file, &lease);

    count = 1;
    for (i = 0; i < 5000; i++) {
        count = recover_count(&ctx, fh, sizeof(fh));
        if (count == 0) {
            break;
        }
        usleep(1000);
    }
    CHECK(count == 0, "lease record removed after release");

    chimera_vfs_state_put(ctx.vfs->vfs_state, file);
    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "\nResults: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
} /* main */
