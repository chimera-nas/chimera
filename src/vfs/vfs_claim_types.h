// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

/*
 * Unified VFS claim vocabulary (the "two-table claim core").
 *
 * Every standing lease, share reservation, byte-range lock, delegation,
 * directory lease, and implicit I/O hold is one row shape -- a CLAIM -- and
 * every mutating operation is a TRIGGER evaluated against the same claims.
 *
 * The architecture is two tables plus one one-directional predicate:
 *
 *   Table 1 (admission): a symmetric used/denied predicate over per-circle
 *   deny masks, evaluated when a claim is inserted.  Deny masks always
 *   self-exempt at some circle -- there is no GLOBAL denial at admission.
 *
 *   Table 2 (triggers): invalidation events (a write, a lock being taken, a
 *   conflicting open, a namespace op, a directory content change, a flush)
 *   with per-row victim selection, retain floor, exemptions, and completion.
 *   The temporally-asymmetric rules (MS-FSA 2.1.5.18 et al.) live here, not
 *   in the admission predicate -- a symmetric conflict(a,b) provably cannot
 *   express them (a standing R-cache survives a foreign shared lock while a
 *   fresh R-cache behind the same lock is denied).
 *
 *   The I/O predicate: mandatory byte-range enforcement, one-directional,
 *   carried by MAND rows on lock claims and consulted only by callers that
 *   observe mandatory locks (the SMB data path).
 *
 * Semantics are derived from a claim's CONSTRUCT (what protocol object it
 * represents) by chimera_vfs_claim_deny_at() and friends in vfs_claim.c --
 * one auditable table function, not per-call-site mask assembly.  Consumers
 * build claims exclusively through the chimera_vfs_claim_init_*()
 * constructors, which stamp the construct.
 *
 * The module-facing slice of this vocabulary -- the capability masks, the
 * owner/actor identity blocks, the backend lease wire shapes, and the
 * pending-acquire ticket the request embeds -- lives in
 * sdk/vfs_claim_types.h so that CAP_LEASE backend arbiters consume it
 * without seeing the claim machinery itself.  This header holds the rest:
 * the claim row, grants, break state, and triggers, all core-private.
 */

#include "sdk/vfs_claim_types.h"

struct chimera_vfs_claim;
struct chimera_vfs_claim_grant;
struct chimera_vfs_file_state;
struct chimera_vfs_open_handle;
struct chimera_vfs_state;

/* -------------------------------------------------------------------- */
/* Constructs                                                           */
/* -------------------------------------------------------------------- */

/* The protocol object a claim represents.  The construct + the claim's
 * current advertised mode fully determine its admission deny rows, MAND
 * rows, revocability, cascade eligibility, and completion class -- see
 * chimera_vfs_claim_deny_at() in vfs_claim.c, which is the admission mask
 * table in executable form. */
enum chimera_claim_construct {
    CHIMERA_CONSTRUCT_NONE = 0,
    CHIMERA_CONSTRUCT_SMB_OPEN,       /* SMB share reservation (plain open) */
    CHIMERA_CONSTRUCT_SMB_OPEN_INERT, /* attribute-only open: all-zero      */
    CHIMERA_CONSTRUCT_NFS4_OPEN,      /* NFSv4 OPEN share/deny              */
    CHIMERA_CONSTRUCT_RQLS,           /* SMB2 RqLs lease (file)             */
    CHIMERA_CONSTRUCT_OPLOCK_II,      /* legacy LEVEL_II oplock             */
    CHIMERA_CONSTRUCT_OPLOCK_EX,      /* legacy EXCLUSIVE oplock            */
    CHIMERA_CONSTRUCT_OPLOCK_BATCH,   /* legacy BATCH oplock                */
    CHIMERA_CONSTRUCT_DIR_LEASE,      /* SMB3 directory lease               */
    CHIMERA_CONSTRUCT_DELEG_R,        /* NFSv4 read delegation              */
    CHIMERA_CONSTRUCT_DELEG_W,        /* NFSv4 write delegation             */
    CHIMERA_CONSTRUCT_LOCK_ADVISORY,  /* NLM / NFSv4 / POSIX byte-range     */
    CHIMERA_CONSTRUCT_LOCK_SMB,       /* SMB byte-range (mandatory)         */
    CHIMERA_CONSTRUCT_IMPLICIT,       /* chimera's internal I/O claim       */
    CHIMERA_CONSTRUCT_FUSE_GRANT,     /* FUSE kernel read-cache grant       */
    CHIMERA_CONSTRUCT_DENY_PROBE,     /* transient deny-only probe (rename  */
                                      /* dp_probe: used 0, deny D)          */
    CHIMERA_CONSTRUCT_COUNT
};

/* Storage class: which per-file list a claim lives on.  Purely an indexing
 * concern (walk efficiency, the MAND fast path); carries no semantics. */
enum chimera_claim_class {
    CHIMERA_CLAIM_CLASS_ACCESS = 0, /* share reservations, deny probes      */
    CHIMERA_CLAIM_CLASS_CACHE  = 1, /* leases, oplocks, delegations, impl.  */
    CHIMERA_CLAIM_CLASS_RANGE  = 2, /* byte-range locks                     */
    CHIMERA_CLAIM_CLASS_COUNT
};

/* -------------------------------------------------------------------- */
/* Break / completion state                                             */
/* -------------------------------------------------------------------- */

enum chimera_claim_break_state {
    CHIMERA_CLAIM_BREAK_IDLE     = 0,
    CHIMERA_CLAIM_BREAK_BREAKING = 1, /* break_cb invoked, awaiting ack     */
    CHIMERA_CLAIM_BREAK_ACKED    = 2, /* settled at 0 (inert until removed) */
    CHIMERA_CLAIM_BREAK_REVOKED  = 3, /* forcibly revoked                   */
};

/* When policy drops the claim's advertised mode during a break:
*   AT_BEGIN -- at break-begin, under file->lock, atomically with
*     {break_state, ack_required} (SMB: a mid-break holder scores at its
*     retained target so a coexisting acquirer proceeds immediately).
*   NEVER -- advertised holds until release/revoke (NFSv4 delegations: a
*     conflicting OPEN keeps getting BREAKING -> NFS4ERR_DELAY until
*     DELEGRETURN).  This policy choice IS the R11 guarantee. */
enum chimera_claim_advertise_drop {
    CHIMERA_CLAIM_ADVERTISE_AT_BEGIN = 0,
    CHIMERA_CLAIM_ADVERTISE_NEVER    = 1,
};

/* -------------------------------------------------------------------- */
/* Callbacks                                                            */
/* -------------------------------------------------------------------- */

/* Break notification -- invoked outside file->lock; the protocol kicks off
 * its wire break (OPLOCK_BREAK, CB_RECALL, FUSE_NOTIFY_INVAL_INODE) and
 * returns promptly.  needed_mode is the mode the holder is asked to drop to
 * (the current cascade step, or the floor for a one-shot break). */
typedef void (*chimera_vfs_claim_break_cb_t)(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *private_data);

/* Liveness probe (courtesy reclaim, R13).  NULL => always alive. */
typedef bool (*chimera_vfs_claim_is_alive_cb_t)(
    const struct chimera_vfs_claim *claim,
    void                           *private_data);

/* Forced-revocation notice (deadline expiry etc.).  NULL ok. */
typedef void (*chimera_vfs_claim_revoked_cb_t)(
    struct chimera_vfs_claim *claim,
    void                     *private_data);

/* -------------------------------------------------------------------- */
/* The claim                                                            */
/* -------------------------------------------------------------------- */

struct chimera_vfs_claim {
    enum chimera_claim_construct construct;
    enum chimera_claim_class klass;

    uint8_t                         used;       /* raw granted mode         */
    uint8_t                         advertised; /* what admission sees; see
                                                 * chimera_claim_advertise_drop */
    uint8_t                         denied;     /* ACCESS only: explicit
                                                 * share-deny bits (R/W/D)  */

    uint64_t                        offset;     /* RANGE only; others 0     */
    uint64_t                        length;     /* UINT64_MAX = to-EOF,
                                                 * 0 = genuine zero-byte    */

    struct chimera_claim_owner      owner;
    struct chimera_vfs_open_handle *op_handle;  /* HOLDER-circle anchor for
                                                 *  own-handle self-ops      */

    /* Holder-lite: the same open's cache grant, when this ACCESS claim's
     * open also holds one.  Replaces the old own_lease_key / cb_private
     * three-arm matching: a hard share conflict against this open may park
     * on own_cache's H break instead of denying (the batch escape, R8). */
    struct chimera_vfs_claim_grant *own_cache;

    /* Cache-class claims embedded in a refcounted grant point back to it;
     * NULL for ACCESS/RANGE claims and the implicit claim. */
    struct chimera_vfs_claim_grant *grant;

    /* Parked (disconnected durable) softening: advertised H and the H
     * denial are masked while set.  Maintained by chimera_vfs_claim_park. */
    uint8_t                         parked;

    /* Break machinery (all guarded by file->lock). */
    uint8_t                         break_state;
    uint8_t                         break_needed_mode;
    uint8_t                         break_floor;
    uint8_t                         break_notified;
    uint64_t                        break_deadline; /* stopwatch ticks      */

    /* Callbacks (per-claim; a grant's members share the grant's).  A claim
     * with no break_cb is unbreakable (binding). */
    chimera_vfs_claim_break_cb_t    break_cb;
    chimera_vfs_claim_is_alive_cb_t is_alive_cb;
    chimera_vfs_claim_revoked_cb_t  revoked_cb;
    void                           *cb_private;

    /* Node-local tag the protocol may stamp for conflict reporting (e.g.
     * the SMB open's pid for the durable-purge loop); copied by value into
     * chimera_vfs_claim_conflict.policy_tag. */
    uint64_t                        policy_tag;

    /* Backend projection: the CAP_LEASE token behind this claim (RANGE
     * records; 0 = not projected).  Written by the projection layer under
     * file->lock; released fire-and-forget on claim release. */
    uint64_t                        backend_token;

    struct chimera_vfs_file_state  *file;

    /* Intrusive linkage on the per-file class list. */
    struct chimera_vfs_claim       *prev;
    struct chimera_vfs_claim       *next;
};

/* -------------------------------------------------------------------- */
/* Cache grant (coalition)                                              */
/* -------------------------------------------------------------------- */

/* One grant per (file, owner-or-LeaseKey): the refcounted coalition object
 * N opens share.  Same shape as the old caching grant; construction and
 * teardown are centralized in the core (chimera_vfs_claim_grant_acquire /
 * _release) -- protocols no longer allocate grants themselves. */
struct chimera_vfs_claim_grant {
    struct chimera_vfs_claim        claim;      /* klass CACHE              */
    struct chimera_vfs_file_state  *file;
    uint32_t                        refcount;   /* members + transient pins */
    uint32_t                        epoch;      /* v2 leases; one bump per
                                                 * break EVENT (R37)        */
    uint8_t                         is_v2;
    uint8_t                         break_ack_required; /* published under
                                                         * file->lock atomically
                                                         * with break_state (R38)   */
    /* Protocol member list -- opaque to the core; manipulated under
     * file->lock (the SMB open_file holder chain). */
    void                           *members;
    struct chimera_vfs_claim_grant *grant_next;
};

/* -------------------------------------------------------------------- */
/* Conflicts                                                            */
/* -------------------------------------------------------------------- */

/* Conflict description BY VALUE (R72).  No pointers, no pin/unref protocol.
 * locktype derivation for LOCK-denied reporting: WRITE_LT iff
 * used & (W|CW|LW) -- a write delegation must report WRITE_LT though it
 * holds no LW.  Whole-file conflicts report offset 0, length UINT64_MAX. */
struct chimera_vfs_claim_conflict {
    struct chimera_claim_owner owner;
    enum chimera_claim_construct construct;
    uint8_t                    used;
    uint8_t                    breaking;     /* holder is mid-break         */
    uint8_t                    revocable;    /* holder could be recalled    */
    uint64_t                   offset;
    uint64_t                   length;
    uint64_t                   policy_tag;   /* node-local consumer stamp   */
};

/* One-shot blocked notification: fires exactly once, synchronously, iff the
 * acquire queues the ticket (NLM's NLM4_BLOCKED interim; SMB's PENDING). */
typedef void (*chimera_vfs_claim_blocked_cb_t)(
    void *private_data);


/* -------------------------------------------------------------------- */
/* Triggers (Table 2)                                                   */
/* -------------------------------------------------------------------- */

/* Invalidation events.  Row semantics (victim selection, floor, exemptions,
 * one-shot vs cascade, completion) live in vfs_claim_break.c's trigger
 * table.  Highlights:
 *   WRITE        - a data write invalidates read caches to 0; RqLs holders
 *                  self-exempt by owner/KEY unconditionally; a legacy
 *                  LEVEL_II never self-exempts (batch6).
 *   SMB_LOCK     - the SMB lock path's read-cache revocation (2.1.5.18),
 *                  same shape as WRITE, fired unconditionally pre-acquire.
 *   RANGE_LOCK   - core-side lock-vs-cache displacement, fired only after
 *                  lock-vs-lock admission passes; floor 0, one-shot,
 *                  HOLDER/KEY exemption gated on the holder retaining CW|H
 *                  (own pure-R cache breaks: brl1/brl3; own W/H cache
 *                  survives: brl2); CLIENT exemption for NFSv4
 *                  unconditional.
 *   OPEN_H       - phase-1 pre-share-check handle break (legacy batch
 *                  only); floor R (0 if truncating).
 *   OPEN_H_FORCE - the break_leases flavor: a genuine share conflict strips
 *                  an RqLs lease's H too (break_twice).
 *   OPEN_W       - phase-2 post-grant write-cache break; retain R|H.
 *   NS_UNLINK    - unlink/delete-on-close of an open file: single-step,
 *                  strips beyond-R holders to R, awaits the real ACK.
 *   NS_FULL      - full namespace recall (REMOVE/RENAME/data-setattr paths
 *                  via io_recall): cascades, recalls even the operating
 *                  client's own delegation.
 *   FLUSH        - attr-only setattr: CW holders only, NFSv4 delegations
 *                  always and awaited (DELEG20).
 *   DIR_CONTENT  - directory content change: dir leases to 0, one shot,
 *                  ParentLeaseKey exemption.
 */
enum chimera_claim_trigger {
    CHIMERA_TRIGGER_WRITE = 1,
    CHIMERA_TRIGGER_SMB_LOCK,
    CHIMERA_TRIGGER_RANGE_LOCK,
    CHIMERA_TRIGGER_OPEN_H,
    CHIMERA_TRIGGER_OPEN_H_FORCE,
    CHIMERA_TRIGGER_OPEN_W,
    CHIMERA_TRIGGER_NS_UNLINK,
    CHIMERA_TRIGGER_NS_FULL,
    CHIMERA_TRIGGER_FLUSH,
    CHIMERA_TRIGGER_DIR_CONTENT,
};
