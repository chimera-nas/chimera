// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "vfs.h"
#include "vfs_claim_journal.h"

struct chimera_vfs_claim_range_attempt;

/* Local-only execution helper. All methods/callbacks run on the owning event
 * loop. Cancellation completes at most once, including from on_wait. No
 * backend lock projection, rollback, or legacy accepted claim transfer.
 * Failed immediate admission polls with 1..100ms backoff when wait=true;
 * timeout_ms=0 means protocol-required indefinite wait. A finite timeout
 * reports ordinary EAGAIN, never a finish rejection. Same-owner exclusive
 * conflicts do not wait. An idle owner binding is released while polling;
 * earlier successful journal deltas retain their owner pin until completion.
 * Future backend transactions must account for those potentially long waits.
 *
 * on_wait is an explicit protocol coordination callback, not a pure op
 * callback. It may emit an interim once per request; caller keeps that memo
 * outside retry-resettable state. is_canceled is a pure predicate checked on
 * the owning loop before each poll; external teardown can atomically flag
 * request-scoped storage without queuing raw pointers to another worker.
 * Inputs/contexts live through completion.
 */
struct chimera_vfs_claim_range_attempt * chimera_vfs_claim_range_attempt_alloc(
    struct chimera_vfs_thread *thread);
void chimera_vfs_claim_range_attempt_execute(
    struct chimera_vfs_claim_range_attempt *attempt,
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t count,
    bool unlock,
    bool wait,
    uint32_t timeout_ms,
    void ( *on_wait )(void *),
    bool ( *is_canceled )(void *),
    void ( *complete )(const struct chimera_vfs_claim_batch_result *, void *),
    void *private_data);
bool chimera_vfs_claim_range_attempt_cancel(
    struct chimera_vfs_claim_range_attempt *attempt);
void chimera_vfs_claim_range_attempt_free(
    struct chimera_vfs_claim_range_attempt *attempt);
