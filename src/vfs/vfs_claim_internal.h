// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs_claim.h"
#include "vfs_clock.h"

/* Internal interfaces shared by vfs_claim.c / vfs_claim_break.c /
 * vfs_claim_io.c.  Nothing here is consumed outside the claim core. */

#define CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS 30000
#define CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH           64
#define CHIMERA_VFS_CLAIM_DEFAULT_IMPLICIT_IDLE_MS  10000
#define CHIMERA_VFS_NFS_DELEG_RECALL_MS             15000
#define CHIMERA_VFS_NFS_DELEG_METAOP_MS             5000

/* ------------------------------------------------------------------ */
/* Deny rows (derived, never stored)                                  */
/* ------------------------------------------------------------------ */

enum chimera_claim_circle {
    CHIMERA_CIRCLE_OWNER = 0,
    CHIMERA_CIRCLE_KEY,
    CHIMERA_CIRCLE_CLIENT,
    CHIMERA_CIRCLE_OWNER_OR_KEY,
};

#define CHIMERA_CLAIM_TARGET_ACCESS (1u << CHIMERA_CLAIM_CLASS_ACCESS)
#define CHIMERA_CLAIM_TARGET_CACHE  (1u << CHIMERA_CLAIM_CLASS_CACHE)
#define CHIMERA_CLAIM_TARGET_RANGE  (1u << CHIMERA_CLAIM_CLASS_RANGE)

struct chimera_claim_deny_row {
    uint8_t mask;
    uint8_t circle;     /* enum chimera_claim_circle                     */
    uint8_t targets;    /* CHIMERA_CLAIM_TARGET_* the row applies to     */
    uint8_t admit_only; /* blocked-term only: never displaces standing   */
    uint8_t raw;        /* row exists at the claim's RAW used mode (lock */
                        /* rows: a mid-break W-cache still parks locks)  */
};

#define CHIMERA_CLAIM_MAX_DENY_ROWS 3

/* The admission mask table in executable form: derive `claim`'s deny rows
 * at its current advertised/raw mode.  Returns the row count. */
int
chimera_vfs_claim_deny_rows(
    const struct chimera_vfs_claim *claim,
    struct chimera_claim_deny_row   rows[CHIMERA_CLAIM_MAX_DENY_ROWS]);

/* Circle predicates over a (holder-side claim, counterparty claim) pair. */
bool
chimera_vfs_claim_same_holder(
    const struct chimera_vfs_claim *a,
    const struct chimera_vfs_claim *b);

bool
chimera_vfs_claim_circle_exempt(
    uint8_t                         circle,
    const struct chimera_vfs_claim *holder,
    const struct chimera_vfs_claim *probe);

/* Effective advertised mode (REVOKED contributes nothing; parked masks H). */
uint8_t
chimera_vfs_claim_advertised(
    const struct chimera_vfs_claim *claim);

bool
chimera_vfs_claim_revocable(
    const struct chimera_vfs_claim *claim);

/* 128-bit-safe half-open overlap; UINT64_MAX = to-EOF, 0 = zero-byte. */
bool
chimera_vfs_claim_range_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len);

/* ------------------------------------------------------------------ */
/* Admission internals                                                */
/* ------------------------------------------------------------------ */

/* Pure admission probe; caller holds file->lock.  On non-GRANTED,
 * *conflict_claim points at the first conflicting claim (internal pointer,
 * valid under the lock only) -- public callers get the by-value form. */
enum chimera_vfs_claim_result
chimera_vfs_claim_admit_locked(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *probe,
    struct chimera_vfs_claim      **conflict_claim);

/* Fill a by-value conflict record from an internal claim pointer. */
void
chimera_vfs_claim_conflict_fill(
    const struct chimera_vfs_claim    *claim,
    struct chimera_vfs_claim_conflict *out);

/* The CONTENDED floor map (with the delegation full-recall override and
 * the dir-lease / RqLs-H share-conflict H-only override). */
uint8_t
chimera_vfs_claim_contended_floor(
    const struct chimera_vfs_claim *probe,
    const struct chimera_vfs_claim *holder);

/* Insert / remove a claim on its file class list; caller holds file->lock. */
void
chimera_vfs_claim_link_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim);

void
chimera_vfs_claim_unlink_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim);

/* ------------------------------------------------------------------ */
/* Break internals                                                    */
/* ------------------------------------------------------------------ */

void
chimera_vfs_claim_begin_break_ex(
    struct chimera_vfs_state *state,
    struct chimera_vfs_claim *claim,
    uint8_t                   floor,
    uint32_t                  deadline_ms,
    bool                      one_shot);

/* Pin a cache claim's grant across an unlocked begin_break; caller holds
 * file->lock.  Returns NULL for grant-less claims. */
struct chimera_vfs_claim_grant *
chimera_vfs_claim_pin_grant(
    struct chimera_vfs_claim *claim);

bool
chimera_vfs_claim_deadline_passed(
    const struct chimera_vfs_claim *claim);

/* Courtesy reclaim (R13): holder's client is dead AND differs from the
 * probe's client. */
bool
chimera_vfs_claim_holder_reclaimable(
    const struct chimera_vfs_claim *holder,
    const struct chimera_vfs_claim *probe);

/* The trigger engine's collect/pin/break loop over an already-pinned file.
 * For CHIMERA_TRIGGER_RANGE_LOCK, retain != 0 flags an exclusive lock. */
void
chimera_vfs_claim_trigger_fire(
    struct chimera_vfs_state         *state,
    struct chimera_vfs_file_state    *file,
    enum chimera_claim_trigger        trigger,
    const struct chimera_claim_actor *actor,
    uint8_t                           retain);

/* NS_FULL / FLUSH / NS_UNLINK engines (return true while still blocked);
 * used by the parking io path and the public query verbs. */
bool
chimera_vfs_claim_trigger_ns_full(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_open_handle *skip_handle,
    bool                                  flush_only);

bool
chimera_vfs_claim_trigger_ns_unlink(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_open_handle *skip_handle,
    uint8_t                               retain);

/* ------------------------------------------------------------------ */
/* Backend projection internals                                       */
/* ------------------------------------------------------------------ */

/* Confirm a locally-granted RANGE claim with the backend before its ticket
 * callback fires; `thread` must be the CALLING vfs thread (its request pool
 * is per-thread and unlocked). */
void
chimera_vfs_claim_backend_project_range(
    struct chimera_vfs_thread          *thread,
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket);

bool
chimera_vfs_claim_backend_range_projects(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_thread     *thread);

/* Queue a projected RANGE token for release on the service thread (safe
 * from any thread; fire-and-forget). */
void
chimera_vfs_claim_backend_release_token(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    uint64_t                       token);

/* Hand a pump-granted projectable ticket to the service thread (the pump
 * has no vfs thread of its own to dispatch from). */
void
chimera_vfs_claim_backend_defer_ticket(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket);

/* ------------------------------------------------------------------ */
/* Pumps                                                              */
/* ------------------------------------------------------------------ */

void
chimera_vfs_claim_pump_pending(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

void
chimera_vfs_claim_pump_io(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

/* ------------------------------------------------------------------ */
/* Small helpers                                                      */
/* ------------------------------------------------------------------ */

/* Milliseconds elapsed from `then` to `now` (stopwatch ticks; clamps to 0). */
static inline uint64_t
chimera_vfs_claim_elapsed_ms(
    uint64_t then,
    uint64_t now)
{
    return now > then ? chimera_vfs_ticks_to_ns(now - then) / 1000000ULL : 0;
} /* chimera_vfs_claim_elapsed_ms */
