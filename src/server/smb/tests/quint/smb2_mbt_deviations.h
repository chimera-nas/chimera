/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * What this replayer cannot DRIVE -- and nothing else.
 *
 * This file used to hold two tables.  The first was a registry of known
 * chimera divergences from the SMB2 model: a reply that did not match was
 * looked up here, and a hit was reported as a DEVIATION rather than failing
 * the run.  That table is GONE, and with it the idea that the harness decides
 * what conformance is.
 *
 * A known divergence is now a branch in the MODEL, gated on the DEVS set the
 * cell's config binds (ext/specs/quint/smb2/smb2_ops.qnt, declared with its
 * citation in ext/specs/quint/smb2/corpus.schema.json).  The model predicts
 * what chimera actually does, so the trace's expectation is already the truth
 * and replay is an exact match with nothing to forgive.  Three things follow
 * that a registry could not give:
 *
 *   - a STATE-MUTATING divergence becomes expressible.  CD-3 -- a file
 *     truncated by a CREATE that was then refused -- had an identical reply on
 *     both sides and differed only in state, so the registry could only mark it
 *     non-reconcilable and ABANDON the trace.  Stated in the model instead, the
 *     model lost the data the same way and the rest of the trace kept testing,
 *     which is what made the bug measurable rather than merely known.  It is
 *     now fixed and no cell declares it.
 *   - the STRICT TWIN of each cell (same batches, DEVS = Set()) re-measures
 *     the conformance debt on every run, where a registry entry kept
 *     forgiving after the bug was fixed.
 *   - tools/devliveness.py fails a cell that enables a deviation its corpus
 *     never exercises, so an entry cannot outlive its fix.
 *
 * What remains here is a different kind of claim, and the reason the file
 * survives: not "this reply differs", but "this replayer cannot drive this
 * batch at all".  Those are below.
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

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
