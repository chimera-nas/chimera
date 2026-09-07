// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Concrete instances of the diskfs workload model -- the units `quint run`
// executes:
//
//   quint run diskfs_run.qnt --main=diskfsRef --step=stepGrow --invariant=inv
//
// Two profiles ship.  They describe the same workload space and differ only in
// the per-directory soft cap, which bounds how deep a single directory's name
// tree is driven:
//
//   diskfsRef   the reference profile.  CAP = 3000 keeps one directory in the
//               height-2 band, where the leaf split / merge / borrow /
//               root-collapse machinery lives and where a trace stays cheap.
//
//   diskfsDeep  CAP = 12000 drives one directory past the SECOND split into a
//               height-3 tree, reaching the interior-node split / merge and the
//               multi-level rebalance cascade.  Expensive (tens of thousands of
//               creates), so the corpus draws only a couple of long traces from
//               it.

module diskfsProfiles {
    /// Three directories: enough to interleave growth in one against churn in
    /// another, without the workload spreading so thin that no single tree gets
    /// deep.
    pure val THREE_DIRS: Set[int] = Set(0, 1, 2)

    pure val CAP_REF: int  = 3000
    pure val CAP_DEEP: int = 12000
}

module diskfsRef {
    import diskfsProfiles.* from "./diskfs_run"
    import diskfs(DIRS = THREE_DIRS, CAP = CAP_REF).* from "./diskfs"
}

module diskfsDeep {
    import diskfsProfiles.* from "./diskfs_run"
    import diskfs(DIRS = THREE_DIRS, CAP = CAP_DEEP).* from "./diskfs"
}
