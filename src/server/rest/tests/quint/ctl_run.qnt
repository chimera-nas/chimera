// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Concrete instances of the ctl model -- the units `quint run` executes:
//
//   quint run ctl_run.qnt --main=ctlRef --step=step --invariant=inv
//
// Two ship, and both describe the same server; they differ only in what the
// walk is allowed to do, which is a corpus-shaping decision rather than a
// configuration one.
//
//   ctlRef    the reference profile.  Administrative calls and NFS requests
//             interleaved, which is the only way to reach the interaction
//             this suite exists for -- a filehandle taken while an export
//             existed, presented after it was deleted.
//
//   ctlAdmin  the same model walked with --step=stepAdminOnly.  No protocol
//             traffic, so every step is an API call and the referential
//             refusals (a mount pinned by an export, a filesystem pinned by a
//             mount) come up several times as often.  It exists because those
//             refusals are cheap to reach and expensive to reach BY ACCIDENT
//             in a mixed walk, where two thirds of the steps are NFS.
//
// The two share every constant.  A profile that changed one would be
// describing a different server, and there is only one server here.

module ctlProfiles {

    /**
     * Two filesystems and two mounts.
     *
     * Two rather than one because the interesting refusals are about one
     * resource pinning another, and with a single name of each kind a trace
     * cannot have a pinned mount and a free one at the same time -- it would
     * only ever see the refusal or only ever see the success.
     */
    pure val TWO_FSS: Set[str] = Set("f0", "f1")
    pure val TWO_MOUNTS: Set[str] = Set("m0", "m1")

    /**
     * Two exports, one per mount, each with a pinned id.
     *
     * The ids are arbitrary but explicit: an auto-assigned id would depend on
     * the order exports happened to be created in, and a filehandle carries
     * that id, so the meaning of a handle would depend on trace history in a
     * way nothing in the API expresses.  Pinning them also keeps a re-created
     * export the SAME export, which is what makes "delete and re-create"
     * mean a configuration reload rather than a different export reusing a
     * number.
     *
     * One export per mount, so that deleting an export frees its mount and the
     * mount-deletion refusal has both answers available.
     */
    pure val TWO_EXPORTS: str -> { path: str, id: int } =
        Map("/e0" -> { path: "m0", id: 11 },
            "/e1" -> { path: "m1", id: 12 })

    /// One SMB share and one S3 bucket, both rooted at m0, so that a mount can
    /// be pinned by something other than an export.  That matters: the mount
    /// deletion refusal must fire for any referrer, and a suite that only ever
    /// pinned mounts with exports would not notice a check that looked at
    /// exports alone.
    pure val ONE_SHARE: str -> str = Map("s0" -> "m0")
    pure val ONE_BUCKET: str -> str = Map("b0" -> "m0")

    /// Two user records.  The user endpoints have no protocol consequence in
    /// this model, so two is enough to exercise the collection contract.
    pure val TWO_USERS: Set[str] = Set("u0", "u1")

    /**
     * Root and one unprivileged caller.
     *
     * The smallest pair that separates the three squash modes: `none` leaves
     * both alone, `all` maps both, and `root` maps exactly one.  With a single
     * credential two of the three modes would be indistinguishable, and the
     * property that a deleted export stops squashing would be invisible for
     * whichever one was left out.
     */
    pure val TWO_CREDS: Set[int] = Set(0, 1000)

    /// The anonymous identity a squashed caller is mapped to.  65534 is the
    /// conventional "nobody", and is far enough from both credentials above
    /// that a squash that did not happen cannot be mistaken for one that did.
    pure val ANON: int = 65534
}

module ctlRef {
    import ctlApi.* from "./ctl_api"
    import ctlProfiles.* from "./ctl_run"

    import ctl(
        FSS = TWO_FSS,
        MOUNTS = TWO_MOUNTS,
        EXPORTS = TWO_EXPORTS,
        SHARES = ONE_SHARE,
        BUCKETS = ONE_BUCKET,
        USERS = TWO_USERS,
        CREDS = TWO_CREDS,
        ANONUID = ANON,
        SEED_FS = "f0",
        SEED_MOUNT = "m0",
        SEED_EXPORT = "/e0"
    ).* from "./ctl"
}
