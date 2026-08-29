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
            "filesystem teardown, and tearing them down under it "
            "corrupts the heap",
        .root_cause = "a durable handle PARKS across a transport drop by "
            "design, so it survives the connection.  The batch "
            "replayer removes the share and the filesystem after "
            "every trace, and chimera_smb_remove_share() frees the "
            "share and destroys its sharemode table with no regard "
            "for opens still referencing it -- a heap-use-after-free "
            "in chimera_smb_sharemode_release().  Keeping the share "
            "instead moves the problem to rmfs, which then spins on "
            "EBUSY until the parked handle's 60 s durable timeout.  "
            "The durable model and its ground-truth probe "
            "(smb2_durable_probe) both pass; it is the teardown that "
            "cannot survive a parked handle.",
        .candidate_fix = "settle share/handle lifetime: make remove_share "
            "release or adopt the opens still referencing the "
            "share, so a parked durable handle cannot outlive its "
            "sharemode table",
    },
    {
        .prefix  = "smb2Leases_",
        .id      = "CD-4",
        .summary = "replay wedges on an ack-required oplock/lease break: a "
            "CREATE parks on a deferral that never resumes",
        .root_cause = "the harness's own wedge detector fires after 20 s on a "
            "CREATE with an interim sent and a break queued "
            "(reply_ready=0), and aborts the run.  The capped "
            "sibling instance smb2ForceL2 -- the same model on a "
            "share carrying SMB2_SHAREFLAG_FORCE_LEVELII_OPLOCK, "
            "where every grant caps to a read cache and so no break "
            "is ever ack-required -- replays all eight of its traces "
            "to the end.  That localizes the wedge to the "
            "ack-required break lifecycle.  Which side owns it is "
            "NOT yet established: it is either a server deferral "
            "that never resumes, or an acknowledgment this replayer "
            "fails to send.  Recorded as a limit rather than a "
            "chimera bug for exactly that reason.",
        .candidate_fix = "drive one wedged trace under SMB2_MBT_DEBUG and "
            "settle which side is waiting; the deterministic half "
            "of this surface is already pinned green by "
            "smb2_oplock_probe, so the difference between that "
            "and this trace is the place to look",
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
