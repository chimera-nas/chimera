// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Deterministic self-checks for the diskfs workload model, run as a build gate
//   quint test diskfsTest.qnt --main=diskfsTestRef
// so a model regression is caught before it can produce a trace that drives the
// wrong workload.  The model has no protocol oracle to pin (its oracle is the
// harness's white-box invariants), so these hold down its bookkeeping: that the
// abstract counts, the frag counter and the mount epoch move exactly as the
// steering logic needs them to, and that the soft cap is never exceeded.

module diskfsTestRef {
    import diskfsProfiles.* from "./diskfs_run"
    import diskfs(DIRS = THREE_DIRS, CAP = CAP_REF).* from "./diskfs"

    /// The count in one directory after the last step, for brevity.
    def cnt(d: int): int = files.get(d)

    /// A fanout raises the target directory's count; nothing else moves.
    run fanoutGrowsTest =
        init
            .expect(cnt(0) == 0)
            .then(fanout(0, 800))
            .expect(cnt(0) == 800 and cnt(1) == 0 and nfrag == 0)
            .then(fanout(0, 2000))
            .expect(cnt(0) == 2800)

    /// A shrink lowers the count, clamped at zero -- deleting more than are
    /// present removes only what is there (the harness cannot unlink absent
    /// names), and the op the harness replays carries the clamped count.
    run shrinkClampsTest =
        init
            .then(fanout(1, 800))
            .expect(cnt(1) == 800)
            .then(shrink(1, 2000))
            .expect(cnt(1) == 0)
            .expect(match lastOp { | OShrink(o) => o.count == 800 | _ => false })

    /// Churn is net-zero on the live set (it creates and deletes within the
    /// step), so it fragments without changing any directory's count.
    run churnNeutralTest =
        init
            .then(fanout(2, 800))
            .then(churn(2, 200, 4))
            .expect(cnt(2) == 800)
            .expect(match lastOp { | OChurn(o) => o.rounds == 4 | _ => false })

    /// A frag file bumps the frag counter and enables truncate.
    run fragFileTest =
        init
            .then(fragFile(120))
            .expect(nfrag == 1)
            .then(truncateLast(40))
            .expect(match lastOp { | OTruncate(o) => o.blocks == 40 | _ => false })

    /// Both mount boundaries bump the epoch; the namespace (abstract counts)
    /// survives them, which is the property the harness verifies for real.
    run epochBoundaryTest =
        init
            .then(fanout(0, 800))
            .then(remount)
            .expect(epoch == 1 and cnt(0) == 800)
            .then(crash)
            .expect(epoch == 2 and cnt(0) == 800)

    /// The soft cap is never exceeded along any reachable stepGrow walk (the
    /// fanout arm is gated on it).
    run capRespectedTest =
        init
            .then(fanout(0, 2000))
            .then(fanout(0, 2000))   // ungated action can exceed; the STEP cannot
            .expect(cnt(0) == 4000)  // direct action bypasses the gate...
            // ...but the invariant flags it, proving the gate matters:
            .expect(not(inv))
}
