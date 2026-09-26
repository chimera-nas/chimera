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
    /* No trace families are excluded. CD-1 durable teardown now retires
     * parked handles with removed shares; CD-4 lease conflicts answer without
     * parking doomed CREATEs; CD-2 replay traces include the previously missing
     * SET_EOF cache invalidations in the model. All three families run through
     * the ordinary batch path alongside the other generated traces. */
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
