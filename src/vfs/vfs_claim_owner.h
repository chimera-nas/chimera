// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"

#include "vfs/vfs_claim_types.h"

struct chimera_vfs_state;

/*
 * Owner pins: references on claims whose allocation the core does not own.
 *
 * The core selects a claim under file->lock and then dispatches a break or a
 * revoke to it with the lock dropped.  A cache claim is safe there because it
 * is embedded in a refcounted grant; a delegation or a FUSE grant is not, and
 * its owner may free it in that window.  Rather than growing a refcount on
 * every claim, the server layer registers the owner's own lifetime primitive
 * for the claim kinds it owns.
 */

#define CHIMERA_VFS_CLAIM_MAX_OWNER_KINDS 8

/* try_get takes a reference only while the object is still alive.  It runs
 * under file->lock, must not block, and must fail once the owner has
 * committed to teardown.  put drops the reference and MUST complete the
 * teardown when this is the last one -- calling the claim release, unlinking
 * it, and freeing the object -- because the path that normally does so may
 * already have run.  `state` is the core's vfs state, so the owner does not
 * have to recover it.  private_data is the claim's cb_private, so a claim
 * carries no metadata for this. */
struct chimera_vfs_claim_owner_ops {
    bool (*try_get)(
        struct chimera_vfs_claim *claim,
        void                     *private_data);
    void (*put)(
        void                     *private_data,
        struct chimera_vfs_state *state);
};

/* Registered by the server layer at init, for (construct, owner.proto). */
SYMBOL_EXPORT void
chimera_vfs_claim_owner_register(
    enum chimera_claim_construct              construct,
    uint8_t                                   proto,
    const struct chimera_vfs_claim_owner_ops *ops);

/* A held reference on a selected claim: the grant pin for a cache claim, the
 * owner pin for everything else.  Take it with file->lock held, before the
 * claim is used with the lock dropped; drop it once the dispatch completes.
 *
 * `private_data` is a snapshot of claim->cb_private taken at the same time
 * as the pin, under file->lock.  The pin protects the OWNER's lifetime
 * (e.g. the nfs_client), not the claim's own allocation -- a RANGE claim
 * lives inside its lock-owner's lease record, which an unrelated unlock can
 * unlink and free independently of the client.  So ref_drop() must call
 * put() with this saved pointer rather than re-reading claim->cb_private,
 * which may by then be inside freed memory. */
struct chimera_vfs_claim_ref {
    struct chimera_vfs_claim                 *claim;
    struct chimera_vfs_claim_grant           *grant;
    const struct chimera_vfs_claim_owner_ops *ops;
    void                                     *private_data;
};

/* False when the claim has neither a grant nor a registered owner, or the
 * owner's try_get refused because teardown has committed. */
SYMBOL_EXPORT bool
chimera_vfs_claim_ref_take(
    struct chimera_vfs_claim     *claim,
    struct chimera_vfs_claim_ref *ref);

SYMBOL_EXPORT void
chimera_vfs_claim_ref_drop(
    struct chimera_vfs_state     *state,
    struct chimera_vfs_claim_ref *ref);
