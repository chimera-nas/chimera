/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 */

#include "server/smb/smb_internal.h"
#include "vfs/vfs_clock.h"

/* Run the waiting CREATE's cache admission synchronously from the claim
 * completion. This makes the close/admit interleaving deterministic, without
 * depending on which SMB worker processes the completion doorbell first. */
struct waiter {
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim       cache;
    int                            completed;
    uint8_t                        mode;
};

static void
acquired(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct waiter *waiter = private_data;

    (void) claim;
    (void) conflict;
    waiter->completed++;
    if (result == CHIMERA_CLAIM_GRANTED) {
        waiter->mode = chimera_vfs_claim_grant_cap_mode(waiter->file,
                                                        &waiter->cache, false);
    }
} /* acquired */

static void
break_cache(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    (void) claim;
    (void) needed_mode;
    (void) private_data;
    /* The holder answers the break by closing, not by acknowledging. */
} /* break_cache */

static int
run_case(int coalesced)
{
    struct chimera_vfs_state          *state      = chimera_vfs_state_init();
    struct chimera_vfs                 vfs        = { .vfs_state = state };
    struct chimera_vfs_thread          vfs_thread = { .vfs = &vfs };
    struct chimera_server_smb_thread   thread     = { .vfs_thread = &vfs_thread };
    struct chimera_smb_open_file       opens[2]   = { 0 };
    struct chimera_claim_owner         owner      = {
        .proto      = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = 1,
        .owner_lo   = 1,
        .key        = { 1 },
    };
    struct chimera_vfs_claim           cache, peer;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_vfs_file_state     *file;
    struct waiter                      waiter = { 0 };
    uint8_t                            fh[]   = { 1 };
    int                                failed = 0;

    file        = chimera_vfs_state_get(state, fh, sizeof(fh), 1, true);
    waiter.file = file;
    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H, &owner);
    cache.break_cb = break_cache;

    for (int i = 0; i <= coalesced; i++) {
        struct chimera_smb_open_file *open = &opens[i];
        bool                          seeded;

        open->share_file_state   = chimera_vfs_state_get(state, fh, sizeof(fh), 1, true);
        open->caching_file_state = chimera_vfs_state_get(state, fh, sizeof(fh), 1, true);
        if (chimera_vfs_claim_grant_acquire(state, file, &cache, 0, 0,
                                            CHIMERA_CLAIM_GRANT_EXACT, open, &seeded,
                                            &open->grant, NULL) != CHIMERA_CLAIM_GRANTED) {
            abort();
        }
        if (!seeded) {
            chimera_smb_grant_add_member(open->grant, open);
        }
        open->caching_lease_inserted = true;
        owner.owner_lo               = i + 2;
        chimera_vfs_claim_init_smb_open(&open->share_lease, CHIMERA_CLAIM_D, 0, &owner);
        open->share_lease.own_cache  = open->grant;
        open->share_lease.cb_private = open;
        if (chimera_vfs_claim_try_acquire(state, file, &open->share_lease, NULL) !=
            CHIMERA_CLAIM_GRANTED) {
            abort();
        }
        open->share_lease_inserted = true;
    }

    owner.client_key = 2;
    owner.owner_lo   = 4;
    memset(owner.key, 0, sizeof(owner.key));
    chimera_vfs_claim_init_smb_open(&peer, CHIMERA_CLAIM_D,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_D, &owner);
    chimera_vfs_claim_init_oplock(&waiter.cache, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                  &owner);
    chimera_vfs_claim_acquire(NULL, state, file, &peer, &ticket, true, false,
                              acquired, NULL, &waiter);
    if (waiter.completed || !ticket.queued) {
        fprintf(stderr, "FAIL: CREATE did not wait for the handle lease\n");
        abort();
    }

    chimera_smb_open_file_drain_locks(&thread, &opens[0]);
    if (coalesced) {
        if (waiter.completed || opens[1].grant->refcount != 1) {
            fprintf(stderr, "FAIL: closing one member lost the shared lease\n");
            failed = 1;
        }
        chimera_smb_open_file_drain_locks(&thread, &opens[1]);
    }
    if (waiter.completed != 1 || waiter.mode != (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW)) {
        fprintf(stderr, "FAIL: coalesced=%d completions=%d cache=0x%02x, expected exclusive\n",
                coalesced, waiter.completed, waiter.mode);
        failed = 1;
    }

    chimera_vfs_claim_release(state, file, &peer);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
    return failed;
} /* run_case */

int
main(void)
{
    int failed;

    /* This probe bypasses VFS startup, but lease-break deadlines still need
     * the process clock, including Windows' performance-counter frequency. */
    chimera_vfs_clock_init();
    failed  = run_case(0);
    failed |= run_case(1);
    chimera_vfs_clock_shutdown();
    return failed;
} /* main */
