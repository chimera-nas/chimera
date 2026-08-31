/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Registry of known chimera divergences from the SMB2 model.
 *
 * The models in ext/specs encode MS-SMB2 / MS-FSA, not chimera.  They are a
 * published corpus with other consumers, and the corpus is generated
 * UNCONDITIONALLY -- every instance and flavor, with nothing withheld on
 * account of what any one server does with it.  So where chimera and the
 * standard disagree, the disagreement is recorded HERE, in chimera, next to
 * the code that has to change.  It is not encoded in the specs project, and it
 * is never hidden by declining to generate the traces that would find it.
 *
 * Three outcomes, and the point of this file is to keep them apart:
 *
 *   1. a MODEL bug -- the spec says what chimera does.  Fix the model in
 *      ext/specs; nothing belongs here.
 *   2. a chimera DEVIATION -- chimera does something the standard does not
 *      describe.  Record it here, with a citation, so the suite keeps running
 *      and the divergence stays enumerable and attributable.
 *   3. an unanalyzed difference -- neither of the above yet.  It must fail.
 *
 * This file is what separates (2) from (3).  A divergence matching an entry is
 * reported as a DEVIATION and does not fail the run; anything else is a
 * MISMATCH and does.  Same contract as the POSIX suite's
 * src/posix/tests/quint/posix_deviations.py and as the Samba conformance
 * harness in ext/specs/harness/samba -- never used to hide an unanalyzed
 * failure, always carrying a citation, a root cause, and something that would
 * retire it.
 *
 * Reconcilability.  Only divergences that leave chimera's state matching the
 * model's are reconcilable, so replay can continue.  One that leaves the two
 * holding different state would make every later command in the trace report a
 * consequence rather than a finding; those are marked reconcilable = false and
 * abandon the trace at the point they occur.
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

struct smb2_mbt_deviation {
    const char *id;
    const char *spec;          /* MS-SMB2 / MS-FSA citation */
    const char *summary;
    const char *root_cause;    /* chimera source location */
    const char *candidate_fix;
    /* Model result tag this applies to ("RCreate", "RLock", ...); NULL = any. */
    const char *op;
    /* Status pair.  SMB2_MBT_ANY on either side means "any value". */
    uint32_t    expected;
    uint32_t    actual;
    bool        reconcilable;
};

#define SMB2_MBT_ANY 0xFFFFFFFFu

/* ------------------------------------------------------------------------
 * Reply-level deviations.
 * ------------------------------------------------------------------------ */

static const struct smb2_mbt_deviation smb2_mbt_deviations[] = {
    /* Empty by construction, not by luck: every reply-level divergence the
     * generated corpus found against chimera has been FIXED in chimera rather
     * than recorded here --
     *
     *   FLUSH without write access        smb_proc_flush.c
     *   LOCK without read or write access smb_proc_lock.c
     *   the attribute-only share bypass   smb_proc_create.c (held_granted /
     *                                     held_denied)
     *   the lease-key binding with        smb_proc_create.c (config.leases
     *   leasing disabled                  gate)
     *
     * A new entry here should be rare and should feel like a decision. */
    {
        .id   = "CD-3",
        .spec = "MS-FSA 2.1.5.1.2 (Open of Existing File): the sharing check "
            "precedes any modification of the file",
        .summary = "a truncating CREATE that is REFUSED with a sharing "
            "violation still truncates the file",
        .root_cause = "smb_proc_create.c builds the VFS open flags with "
            "CHIMERA_VFS_OPEN_TRUNCATE and hands them to "
            "chimera_vfs_open_at BEFORE the share-mode claim is "
            "arbitrated, so the backend has already emptied the file "
            "by the time the claim refuses the open.  A failed "
            "operation with a side effect, and a destructive one: "
            "the data is gone and the client is told the open did "
            "not happen.  Both the model and the wire answer "
            "STATUS_SHARING_VIOLATION, so nothing catches it at the "
            "CREATE -- it surfaces later as a read that should have "
            "returned data.  check_refused_create_side_effect in "
            "smb2_mbt_replay.c compares the model's post-state size "
            "against the wire's at the step that causes it.",
        .candidate_fix = "open without CHIMERA_VFS_OPEN_TRUNCATE, acquire the "
            "share claim, and only then truncate -- moving the "
            "ARCHIVE / AllocationSize stamping, which currently "
            "rides on the create-or-truncate open, to that same "
            "deferred step.  The named-stream path "
            "(chimera_smb_create_open_stream_chain) needs the "
            "same treatment.",
        .op       = "RCreateSideEffect",
        .expected = SMB2_MBT_ANY,
        .actual   = SMB2_MBT_ANY,
        /* chimera's file is now empty and the model's is not.  Every later
         * read, size query and hole check on that file would diverge. */
        .reconcilable = false,
    },
    { 0 }
};

/* Find the deviation covering this divergence, or NULL. */
static inline const struct smb2_mbt_deviation *
smb2_mbt_deviation_find(
    const char *op,
    uint32_t    expected,
    uint32_t    actual)
{
    const struct smb2_mbt_deviation *d;

    for (d = smb2_mbt_deviations; d->id; d++) {
        if (d->op && (!op || strcmp(d->op, op) != 0)) {
            continue;
        }
        if (d->expected != SMB2_MBT_ANY && d->expected != expected) {
            continue;
        }
        if (d->actual != SMB2_MBT_ANY && d->actual != actual) {
            continue;
        }
        return d;
    }
    return NULL;
} /* smb2_mbt_deviation_find */

/* ------------------------------------------------------------------------
 * Trace-level limits.
 *
 * Some divergences are not about one reply.  They are about whether this
 * replayer can drive a whole batch at all -- because chimera misbehaves in a
 * way that ends the trace, or because the trace is not deterministic against
 * it.  The corpus still generates those batches; this table is what turns the
 * batch into a reported SKIP instead of an unexplained pile of failures, and
 * it names the chimera bug that would retire the entry.
 *
 * Matched against the trace file's basename, which the generator names
 * <instance>_<flavor>_<steps>_<seed>_<seq>.itf.json.
 * ------------------------------------------------------------------------ */

struct smb2_mbt_trace_limit {
    const char *prefix;        /* matched against the trace basename */
    const char *id;
    const char *summary;
    const char *root_cause;
    const char *candidate_fix;
};

static const struct smb2_mbt_trace_limit smb2_mbt_trace_limits[] = {
    {
        .prefix  = "smb2Durable_",
        .id      = "CD-1",
        .summary = "a parked durable handle outlives the per-trace share and "
            "filesystem teardown, and pins the filesystem past the "
            "replayer's rmfs budget",
        .root_cause =
            "a durable handle PARKS across a transport drop by "
            "design, so it survives the connection, and the batch "
            "replayer removes the share and the filesystem after "
            "every trace.\n"
            "  The heap-use-after-free this entry used to describe "
            "in chimera_smb_sharemode_release() is FIXED: "
            "chimera_smb_remove_share() no longer frees the share "
            "inline, it unlinks and drops a reference "
            "(chimera_smb_share_release), so a connected tree can no "
            "longer outlive the sharemode table.\n"
            "  What remains is lifetime, not corruption.  A parked "
            "durable entry references its open_file weakly and holds "
            "no share reference at all, and the open keeps its VFS "
            "handle open for the whole durable timeout "
            "(CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS, 60 s).  memfs "
            "therefore reports open_count > 0 and rmfs returns EBUSY "
            "well past the replayer's SMB2_RMFS_RETRY_MAX budget (5 "
            "s), so teardown fails the run.  Nothing in remove_share "
            "reaps the registry entries whose opens were rooted at "
            "the share being removed.\n"
            "  A separate defect on this path -- "
            "chimera_smb_durable_rehome() hashing a reclaimed open "
            "into the reconnecting tree without updating "
            "open_file->tree, leaving every later release locking "
            "and unhashing against the tree the open was parked from "
            "(already returned to shared->free_trees) -- has been "
            "FIXED in smb_proc_create.c.",
        .candidate_fix = "give the durable registry a share back-reference (or "
            "put one on the open_file, taken at create and "
            "released on purge) and have chimera_smb_remove_share "
            "purge the parked entries rooted at that share, so a "
            "removed share cannot leave VFS handles open behind "
            "it.  The durable model and its ground-truth probe "
            "(smb2_durable_probe) both pass, so it is the "
            "teardown, not the durable semantics, that this entry "
            "is holding open.",
    },
    {
        .prefix  = "smb2Leases_",
        .id      = "CD-4",
        .summary = "a CREATE that will be refused with SHARING_VIOLATION parks "
            "behind the lease break it triggered instead of "
            "answering, stalling ~30 s and destroying the lease",
        .root_cause =
            "OWNERSHIP SETTLED (it was previously recorded here as "
            "undetermined).  Diagnosed by raising the harness's "
            "SMB2C_HANG_MS ceiling above chimera's own 30 s break "
            "deadline (CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS) "
            "and letting the trace run to completion.\n"
            "  Shape, from smb2Leases_stepLease_300_0x21_0 steps "
            "10-14: an open takes an R|H (handle-caching) lease on "
            "a file; a SECOND open of the same file, on the SAME "
            "connection, asks for a share mode that conflicts.  The "
            "model answers STATUS_SHARING_VIOLATION immediately and "
            "emits an ack-required lease break (R|H -> R) for the "
            "holder to acknowledge in a later step -- so its "
            "RCreate carries breaks={..} with parked=false.\n"
            "  chimera instead PARKS the doomed open.  "
            "chimera_vfs_claim_try_acquire reports BREAKING for the "
            "handle-caching LEASE holder, so the share-conflict "
            "block in chimera_smb_create_gen_open_file takes its "
            "`result == CHIMERA_CLAIM_BREAKING` arm -- which exists "
            "for a BATCH oplock holder that may still close and "
            "free the conflict -- sends an interim and waits.  That "
            "arm's own comment says leases are meant to fall "
            "through to the immediate-SHARING_VIOLATION arm below "
            "it ('Batch oplocks already broke via the BREAKING/park "
            "path above; this covers leases, which admission "
            "spares'), so the two disagree about which holders can "
            "park an opener.\n"
            "  Consequences, both observed: (1) the open stalls for "
            "the full 30 s break deadline and then returns exactly "
            "the STATUS_SHARING_VIOLATION the model gave at once; "
            "(2) the deadline force-revokes the break, so the "
            "holder's later, legitimate SMB2_OPLOCK_BREAK ack is "
            "rejected with STATUS_UNSUCCESSFUL -- the trace reports "
            "'BREAK_ACK (lease, fid 3) status: model 0x00000000 "
            "wire 0xc0000001'.  A client that opens a leased file "
            "with a conflicting share mode therefore blocks for 30 "
            "seconds and loses its lease.\n"
            "  The harness makes this a wedge rather than a report "
            "because do_create waits synchronously: the holder is "
            "the same connection, so the ack the server is waiting "
            "for cannot be sent while that wait is in progress.",
        .candidate_fix = "refuse the open at once when the share conflict is "
            "against a handle-caching LEASE holder -- break its H "
            "(the immediate-SHARING_VIOLATION arm already does "
            "this via CHIMERA_TRIGGER_OPEN_H_FORCE) and answer "
            "SHARING_VIOLATION, reserving the BREAKING park for "
            "the batch-oplock holder it was written for.  Check "
            "the smbtorture cases the park arm cites "
            "(smb2.replay.dhv2-pending1n-vs-violation-lease-*) "
            "before narrowing it.  Independently, teach do_create "
            "to defer a parked CREATE instead of blocking, so a "
            "future self-break turns into a reported mismatch "
            "rather than a 20 s wedge.",
    },
    {
        .prefix  = "smb2Replay_",
        .id      = "CD-2",
        .summary = "replay-flagged traces are not deterministic against "
            "chimera, so a divergence here cannot be trusted either way",
        .root_cause = "the exactly-once layer's create reply cache (Q-13) "
            "makes the observable outcome of an affected request "
            "depend on timing rather than on the request, so the "
            "same trace can replay green once and red the next "
            "time.  Replaying it would trade the suite's "
            "trustworthiness for coverage.  The deterministic half "
            "of this surface is pinned by smb2_replay_probe, which "
            "does pass.",
        .candidate_fix = "make the create reply cache's lookup and its "
            "eligibility window deterministic, then delete this "
            "entry and let the batch replay",
    },
    { 0 }
};

static inline const struct smb2_mbt_trace_limit *
smb2_mbt_trace_limit_find(const char *basename)
{
    const struct smb2_mbt_trace_limit *l;

    for (l = smb2_mbt_trace_limits; l->id; l++) {
        if (strncmp(basename, l->prefix, strlen(l->prefix)) == 0) {
            return l;
        }
    }
    return NULL;
} /* smb2_mbt_trace_limit_find */
