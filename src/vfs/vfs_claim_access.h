// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "vfs_claim.h"

struct chimera_vfs_claim_access_owner;
struct chimera_vfs_claim_access_journal;

/* Explicit control reservation, acquired outside pure frontend callbacks.
 * Zero-initialize each holder. Acquisition never waits, pins file on success,
 * and is idempotent for the same holder/file/non-NULL cookie. Different holders
 * with the same cookie may coexist; conflicting cookies fail without changes.
 * New ACCESS insertions are denied unless their transient admission_cookie
 * matches. Already queued tickets stay queued until the last holder releases.
 * Hold through accepted frontend publication (and optionally across retries);
 * release outside frontend/file locks because it pumps waiting acquisitions.
 * No filesystem rollback or owner retirement is implied by this barrier.
 */
struct chimera_vfs_claim_access_fence {
    struct chimera_vfs_file_state *file;
    const void *cookie;
};
bool chimera_vfs_claim_access_fence_acquire(
    struct chimera_vfs_claim_access_fence *fence,
    struct chimera_vfs_file_state *file, const void *cookie);
void chimera_vfs_claim_access_fence_release(struct chimera_vfs_claim_access_fence *fence);

/* Canonical ACCESS-claim storage. Allocate before admission, from an unlinked
 * template, then use claim(owner) for ordinary claim admission/park/shrink.
 * Every release for this owner MUST go through retire, never claim_release.
 * Protocol identities and registries remain frontend-owned. The token owns a
 * file-state reference; the frontend retains callback/handle anchors through
 * retirement. Final put requires retirement and no journal pin.
 */
struct chimera_vfs_claim_access_owner *chimera_vfs_claim_access_owner_alloc(
    struct chimera_vfs_file_state *file, const struct chimera_vfs_claim *template_claim);
struct chimera_vfs_claim *chimera_vfs_claim_access_owner_claim(
    struct chimera_vfs_claim_access_owner *owner);
void chimera_vfs_claim_access_owner_ref(struct chimera_vfs_claim_access_owner *owner);
void chimera_vfs_claim_access_owner_put(struct chimera_vfs_claim_access_owner *owner);
/* Thread-safe cutoff: blocks new journal staging immediately, defers unlink
 * while an existing journal holds admission stable. Poll is_retired on the
 * owning frontend loop while retaining a token ref; no callbacks cross threads.
 * CLOSE must not reply before retirement is complete. */
void chimera_vfs_claim_access_owner_retire(struct chimera_vfs_claim_access_owner *owner);
bool chimera_vfs_claim_access_owner_is_retired(struct chimera_vfs_claim_access_owner *owner);

/* One execution-thread journal; owner retirement may run on other threads.
 * stage_retire reserves an edit lease and keeps the public claim linked until
 * accepted publication. Subsequent reservations use excluded() to see the
 * private close. Other requests continue to see the public admission blocker.
 * No allocation or admission at seal/publish; ordinary group failures preserve
 * successful earlier retirements. Reset restores unaccepted private removals,
 * but honors an independently requested external owner retirement.
 */
struct chimera_vfs_claim_access_journal *chimera_vfs_claim_access_journal_alloc(uint32_t capacity);
enum chimera_vfs_error chimera_vfs_claim_access_journal_retire(
    struct chimera_vfs_claim_access_journal *journal,
    struct chimera_vfs_claim_access_owner *owner);
/* Stage a subset-only rights change without changing public admission. Owner
 * must be a private reservation whose frontend cannot mutate its rights while
 * journal-pinned. Multiple narrows compose; narrowing a retired row is invalid.
 * Public rights change only at accepted publish, without callbacks until
 * complete. Reset discards the private view. Checkpoint/rewind also preserves
 * narrows preceding a later failed retirement admission. */
enum chimera_vfs_error chimera_vfs_claim_access_journal_narrow(
    struct chimera_vfs_claim_access_journal *journal,
    struct chimera_vfs_claim_access_owner *owner, uint8_t used, uint8_t denied);
/* Pure test against latest private narrowed ACCESS rows. Pair with excluded()
 * and ordinary public admission; exclusions alone are insufficient for narrows.
 * Uses the ordinary two-sided row predicate. Cache-grant sole-opener policy
 * still belongs to public admission; cache-grant conversion is not implied. */
enum chimera_vfs_claim_result chimera_vfs_claim_access_journal_test(
    const struct chimera_vfs_claim_access_journal *journal,
    const struct chimera_vfs_file_state *file, const struct chimera_vfs_claim *probe,
    struct chimera_vfs_claim_conflict *conflict);

/* Typed multi-owner admission rollback before filesystem execution: drop only
 * new reservations, preserving earlier command deltas and external cutoff. */
uint32_t chimera_vfs_claim_access_journal_checkpoint(
    const struct chimera_vfs_claim_access_journal *journal);
void chimera_vfs_claim_access_journal_rewind(
    struct chimera_vfs_claim_access_journal *journal, uint32_t checkpoint);
uint32_t chimera_vfs_claim_access_journal_excluded(
    const struct chimera_vfs_claim_access_journal *journal,
    const struct chimera_vfs_file_state *file,
    const struct chimera_vfs_claim **out, uint32_t capacity);
void chimera_vfs_claim_access_journal_seal(struct chimera_vfs_claim_access_journal *journal);
void chimera_vfs_claim_access_journal_publish(struct chimera_vfs_claim_access_journal *journal);
/* Publish frontend state before complete releases edit leases and wakes VFS
 * waiters. A staged CLOSE publisher must not await is_retired before complete:
 * accepted publish already unlinked the claim, and complete releases its pin. */
void chimera_vfs_claim_access_journal_complete(struct chimera_vfs_claim_access_journal *journal);
void chimera_vfs_claim_access_journal_reset(struct chimera_vfs_claim_access_journal *journal);
void chimera_vfs_claim_access_journal_free(struct chimera_vfs_claim_access_journal *journal);
