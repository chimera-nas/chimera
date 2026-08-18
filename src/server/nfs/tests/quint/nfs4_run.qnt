// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Concrete instances of the nfs4 model -- the units `quint run` executes:
//
//   quint run nfs4_run.qnt --main=nfs4Default --step=step --invariant=inv
//
// Operators add configurations here.  FeatIfAdvertised generates traces
// for both a supporting and a non-supporting server (the harness skips
// whichever does not match the live advertisement); FeatMandatory turns an
// advertisement mismatch into a suite failure, so a server that silently
// stops advertising a required feature cannot pass vacuously; FeatOff
// removes the feature from generation entirely.

module nfs4Default {
    // NFSv4.2, every optional feature exercised when advertised.
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 2,
        F_PNFS = FeatIfAdvertised,
        F_READ_DELEG = FeatIfAdvertised,
        F_WRITE_DELEG = FeatIfAdvertised,
        F_XATTR = FeatIfAdvertised,
        F_SPARSE = FeatIfAdvertised,
        F_COPY = FeatIfAdvertised,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4V40 {
    // Plain NFSv4.0: SETCLIENTID machinery, owner seqids, OPEN_CONFIRM.
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 0,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatIfAdvertised,
        F_WRITE_DELEG = FeatIfAdvertised,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4V41 {
    // NFSv4.1: sessions, slot replay discipline, pNFS when advertised.
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 1,
        F_PNFS = FeatIfAdvertised,
        F_READ_DELEG = FeatIfAdvertised,
        F_WRITE_DELEG = FeatIfAdvertised,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

// ---------------------------------------------------------------------
// memfs-tuned instances: the profile the CMake ctest suite actually runs
// against chimera + memfs.  The generic FeatIfAdvertised instances above
// are for a server whose capabilities are unknown a-priori (they skip
// the non-applicable half of the corpus); against a KNOWN backend that
// bias only produces spurious skips, so these pin every feature to what
// memfs really does:
//   - xattr / sparse / copy: memfs advertises CAP_XATTR / CAP_COPY_RANGE
//     and ALLOCATE/DEALLOCATE/SEEK, and there is no server knob to turn
//     them off, so FeatMandatory (the model never generates the
//     unsupported profile; a silent regression fails the suite).  The
//     model auto-gates these to MINOR >= 2.
//   - pNFS: memfs is not configured as a pNFS metadata server (the NFS4
//     layer does not advertise USE_PNFS_MDS), so FeatOff.
//   - delegations: enabled in the daemon config, but chimera only grants
//     once a CB_NULL callback-path probe confirms the back channel, which
//     the current synchronous replay harness does not satisfy -- so no
//     delegation is ever granted and FeatOff matches reality.  Testing
//     grants + CB_RECALL needs an async backchannel servicer in the
//     harness (see DESIGN-NFS4.md, deferred); flip these to
//     FeatIfAdvertised/FeatMandatory once that lands.
module nfs4Memfs42 {
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 2,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatOff,
        F_WRITE_DELEG = FeatOff,
        F_XATTR = FeatMandatory,
        F_SPARSE = FeatMandatory,
        F_COPY = FeatMandatory,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

// Delegation-enabled variants of the memfs instances: identical caps
// but with both delegation features asserted, exercising the grant /
// recall / return lifecycle that the plain instances pin off.  The
// grant policy these traces expect is the one nfs4_deleg_probe.c pins
// against the live server.
module nfs4Memfs42Deleg {
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 2,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatMandatory,
        F_WRITE_DELEG = FeatMandatory,
        F_XATTR = FeatMandatory,
        F_SPARSE = FeatMandatory,
        F_COPY = FeatMandatory,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4Memfs41Deleg {
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 1,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatMandatory,
        F_WRITE_DELEG = FeatMandatory,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4Memfs41 {
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 1,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatOff,
        F_WRITE_DELEG = FeatOff,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4Memfs40 {
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 0,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatOff,
        F_WRITE_DELEG = FeatOff,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4PnfsMandatory {
    // For servers expected to serve pNFS (e.g. a chimera build with the
    // layout table enabled): not advertising pNFS FAILS the suite.
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 2,
        F_PNFS = FeatMandatory,
        F_READ_DELEG = FeatIfAdvertised,
        F_WRITE_DELEG = FeatIfAdvertised,
        F_XATTR = FeatIfAdvertised,
        F_SPARSE = FeatIfAdvertised,
        F_COPY = FeatIfAdvertised,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}

module nfs4AllMandatory {
    // Full-feature conformance profile: everything chimera advertises
    // today is required; any silent regression in advertisement is red.
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 2,
        F_PNFS = FeatMandatory,
        F_READ_DELEG = FeatMandatory,
        F_WRITE_DELEG = FeatMandatory,
        F_XATTR = FeatMandatory,
        F_SPARSE = FeatMandatory,
        F_COPY = FeatMandatory,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"
}
