// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Deterministic self-checks for the ctl model, executed with
//   quint test ctlTest.qnt --main=ctlTestRef
//
// They pin the model's own logic without a server, so a model regression is
// caught before it can invalidate a generated trace.  Each one names the rule
// it is holding down and, where the rule is a choice rather than a
// consequence, why that choice.

module ctlTestRef {
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

    // ---- shorthand ---------------------------------------------------

    pure val ROOT = 0
    pure val USER = 1000

    /// The status the last step reported, whatever kind of step it was.
    val st: int = match lastOp {
        | OApi(o) => o.st
        | OMnt(o) => o.st
        | OGetattr(o) => o.st
        | OCreate(o) => o.st
    }

    /// The owner a create landed on; -1 when the last step was not one, or
    /// when it did not get far enough to write anything.
    val ownerUid: int = match lastOp {
        | OCreate(o) => o.uid
        | OApi(_) => -1
        | OMnt(_) => -1
        | OGetattr(_) => -1
    }

    // Administrative shorthands: the model's `api` takes the request and the
    // outcome, which is what makes it an oracle; a test only wants to say
    // "issue this".
    action fsAdd(n: str): bool = api(RFsCreate(n), fsCreate(ctlst, n))
    action fsDel(n: str): bool = api(RFsDelete(n), fsDelete(ctlst, n))
    action mntAdd(n: str, f: str): bool =
        api(RMountCreate({ name: n, fs: f }), mountCreate(ctlst, n, f))
    action mntDel(n: str): bool = api(RMountDelete(n), mountDelete(ctlst, n))
    action expAdd(n: str, ro: bool, sq: str): bool =
        api(RExportCreate({ name: n, ro: ro, squash: sq }),
            exportCreate(ctlst, n, { path: EXPORTS.get(n).path,
                                     id: EXPORTS.get(n).id,
                                     ro: ro, squash: sq }))
    action expDel(n: str): bool = api(RExportDelete(n), exportDelete(ctlst, n))
    action shrAdd(n: str): bool =
        api(RShareCreate(n), shareCreate(ctlst, n, SHARES.get(n)))
    action shrDel(n: str): bool = api(RShareDelete(n), shareDelete(ctlst, n))
    action bktAdd(n: str): bool =
        api(RBucketCreate(n), bucketCreate(ctlst, n, BUCKETS.get(n)))
    action usrAdd(n: str): bool = api(RUserCreate(n), userCreate(ctlst, n))
    action usrDel(n: str): bool = api(RUserDelete(n), userDelete(ctlst, n))

    // ==================================================================
    // A collection behaves like a collection
    // ==================================================================

    /**
     * Create, read, delete, and the two errors on the edges.
     *
     * The second create must be a conflict rather than an overwrite: an
     * overwrite would silently re-point a mount or change an export's access
     * mode under a client that is already using it, and the caller would have
     * no way to know it had replaced rather than added.  The second delete
     * must be a not-found rather than a success, because "there is no such
     * resource" and "I removed it for you" are different facts and a
     * configuration tool acts differently on them.
     */
    run collectionContractTest =
        init
            .then(usrAdd("u0"))
            .expect(st == CREATED and ctlst.users.contains("u0"))
            .then(usrAdd("u0"))
            .expect(st == CONFLICT)
            .then(usrDel("u0"))
            .expect(st == NO_CONTENT and not(ctlst.users.contains("u0")))
            .then(usrDel("u0"))
            .expect(st == NOT_FOUND)

    // ==================================================================
    // References are honoured
    // ==================================================================

    /**
     * A filesystem is pinned by any mount attached to it, and a mount by any
     * export, share or bucket rooted at it.
     *
     * The share and the bucket are in here on purpose.  A check that looked
     * only at exports would pass every test that pinned a mount with an
     * export, and this suite would never notice -- so the mount is pinned
     * three separate ways, and released one referrer at a time.
     */
    run referentialIntegrityTest =
        init
            // The seeded export pins m0, which pins f0.
            .expect(ctlst.exports.keys() == Set("/e0"))
            .then(mntDel("m0"))
            .expect(st == CONFLICT)
            .then(fsDel("f0"))
            .expect(st == CONFLICT)
            // Add two more referrers, then remove the export: still pinned.
            .then(shrAdd("s0"))
            .then(bktAdd("b0"))
            .then(expDel("/e0"))
            .then(mntDel("m0"))
            .expect(st == CONFLICT)
            .then(shrDel("s0"))
            .then(mntDel("m0"))
            .expect(st == CONFLICT)
            // Only with the last referrer gone does the mount release, and
            // only then does the filesystem.
            .then(api(RBucketDelete("b0"), bucketDelete(ctlst, "b0")))
            .then(fsDel("f0"))
            .expect(st == CONFLICT)
            .then(mntDel("m0"))
            .expect(st == NO_CONTENT)
            .then(fsDel("f0"))
            .expect(st == NO_CONTENT)

    /**
     * Attaching a filesystem that does not exist is the caller's error, so it
     * is 404 -- the same answer the filesystems endpoint gives for a module it
     * does not have.  Reporting 500 would say the server failed at something
     * it never attempted, and would leave a caller unable to tell a typo from
     * an outage.
     */
    run mountUnknownFsTest =
        init
            .then(mntAdd("m1", "f1"))
            .expect(st == NOT_FOUND and not(ctlst.mounts.keys().contains("m1")))
            .then(fsAdd("f1"))
            .then(mntAdd("m1", "f1"))
            .expect(st == CREATED)

    // ==================================================================
    // An export's reachability
    // ==================================================================

    /// A name that is not an export is not mountable, and neither is an
    /// export whose path names no live mount -- the latter being the price of
    /// letting an export be declared before its mount exists.
    run mountReachabilityTest =
        init
            .then(mnt("/e1"))
            .expect(st == MNT3ERR_NOENT)
            // /e1 exists now, but m1 is not mounted.
            .then(expAdd("/e1", false, SQ_NONE))
            .then(mnt("/e1"))
            .expect(st == MNT3ERR_NOENT and nhandles == 0)
            // With m1 mounted it becomes reachable.
            .then(fsAdd("f1"))
            .then(mntAdd("m1", "f1"))
            .then(mnt("/e1"))
            .expect(st == MNT3_OK and nhandles == 1)

    // ==================================================================
    // The claim this suite exists for
    // ==================================================================

    /**
     * Deleting an export revokes the filehandles minted under it.
     *
     * This is what makes the exports API an access control and not a
     * directory listing.  A server that keeps serving those handles has not
     * merely been slow to notice: since read-only, squash and the security
     * flavor list all live on the export record, it is serving them with no
     * policy at all -- see the two tests below for what that costs.
     */
    run deletedExportRevokesHandlesTest =
        init
            .then(mnt("/e0"))
            .expect(st == MNT3_OK)
            .then(getattr(0, ROOT))
            .expect(st == NFS3_OK)
            .then(expDel("/e0"))
            .then(getattr(0, ROOT))
            .expect(st == NFS3ERR_STALE)
            .then(create(0, ROOT))
            .expect(st == NFS3ERR_STALE)

    /**
     * Read-only is a per-request check against the export record, so it holds
     * for handles that already exist -- and stops existing when the record
     * does.  A server that let the handle through after the deletion would
     * turn a read-only export into a writable one at the moment an
     * administrator tried to withdraw it altogether.
     */
    run readOnlyIsPerRequestTest =
        init
            .then(mnt("/e0"))
            .then(create(0, ROOT))
            .expect(st == NFS3_OK)
            // Make it read-only without touching the client's handle.
            .then(expDel("/e0"))
            .then(expAdd("/e0", true, SQ_NONE))
            .then(create(0, ROOT))
            .expect(st == NFS3ERR_ROFS)
            // ...and withdrawing it entirely is not a way to get the write.
            .then(expDel("/e0"))
            .then(create(0, ROOT))
            .expect(st == NFS3ERR_STALE)

    /**
     * The three squash modes, told apart by two callers.
     *
     * `none` leaves both alone; `all` maps both; `root` maps exactly one, and
     * that asymmetry is why two credentials are needed -- with root alone,
     * `root` and `all` are the same function.
     */
    run squashModesTest =
        init
            .then(mnt("/e0"))
            .then(create(0, ROOT))
            .expect(ownerUid == ROOT)
            .then(create(0, USER))
            .expect(ownerUid == USER)
            .then(expDel("/e0"))
            .then(expAdd("/e0", false, SQ_ROOT))
            .then(create(0, ROOT))
            .expect(ownerUid == ANON)
            .then(create(0, USER))
            .expect(ownerUid == USER)
            .then(expDel("/e0"))
            .then(expAdd("/e0", false, SQ_ALL))
            .then(create(0, ROOT))
            .expect(ownerUid == ANON)
            .then(create(0, USER))
            .expect(ownerUid == ANON)

    /**
     * The sharpest consequence of serving a handle whose export is gone.
     *
     * Under squash=all every caller writes as the anonymous identity.  If the
     * handle survives the export's deletion, the very next write lands as
     * whoever the caller actually is -- root -- and an administrator has
     * escalated a client by trying to remove its access.  The model requires
     * NFS3ERR_STALE, so the escalation is unreachable; the harness's probe
     * (ctl_proto_probe, P2c) records that chimera currently answers NFS3_OK
     * with owner 0.
     */
    run deletedExportCannotUnsquashTest =
        init
            .then(expDel("/e0"))
            .then(expAdd("/e0", false, SQ_ALL))
            .then(mnt("/e0"))
            .then(create(0, ROOT))
            .expect(st == NFS3_OK and ownerUid == ANON)
            .then(expDel("/e0"))
            .then(create(0, ROOT))
            .expect(st == NFS3ERR_STALE and ownerUid == -1)

    /**
     * Re-creating the same export revives its handles, and the new policy
     * applies to them.
     *
     * This is a configuration reload (`exportfs -r`), and a server that broke
     * every client's handles on one would be unusable -- which is why the id
     * is a property of the export rather than a serial number.  The other
     * half is that the handle picks up whatever policy the export now carries:
     * policy is read per request, so there is no such thing as a handle that
     * remembers the terms it was issued under.
     */
    run exportReloadRevivesHandlesTest =
        init
            .then(mnt("/e0"))
            .then(expDel("/e0"))
            .then(getattr(0, ROOT))
            .expect(st == NFS3ERR_STALE)
            .then(expAdd("/e0", true, SQ_ALL))
            .then(getattr(0, ROOT))
            .expect(st == NFS3_OK)
            .then(create(0, ROOT))
            .expect(st == NFS3ERR_ROFS)

    /**
     * A remount is a new namespace as far as an old handle is concerned.
     *
     * The inner VFS handle is scoped to one attachment of one filesystem, so
     * even the same filesystem coming back at the same mount name does not
     * revive it.  Stated here rather than left implicit because it is the one
     * staleness rule that has nothing to do with exports, and a reader who saw
     * only the export rule might reasonably assume it was the whole story.
     */
    run remountInvalidatesHandlesTest =
        init
            .then(mnt("/e0"))
            .expect(st == MNT3_OK)
            .then(expDel("/e0"))
            .then(mntDel("m0"))
            .expect(st == NO_CONTENT)
            .then(mntAdd("m0", "f0"))
            .then(expAdd("/e0", false, SQ_NONE))
            // The export is back, with the same id -- but the mount under it
            // is a different attachment, so the old handle is still stale.
            .then(getattr(0, ROOT))
            .expect(st == NFS3ERR_STALE)
            // A fresh MOUNT gets a handle into the new attachment.
            .then(mnt("/e0"))
            .then(getattr(1, ROOT))
            .expect(st == NFS3_OK)

    // ==================================================================
    // Reads
    // ==================================================================

    /// A read reports presence and changes nothing.  Trivial, and worth
    /// pinning: a create that returns 201 and then does not appear is the
    /// failure this catches, and it is invisible to a test that only looks at
    /// the create's own status.
    run readsFollowWritesTest =
        init
            .then(apiRead(RExportGet("/e1"),
                          getStatus(ctlst.exports.keys().contains("/e1"))))
            .expect(st == NOT_FOUND)
            .then(expAdd("/e1", false, SQ_NONE))
            .then(apiRead(RExportGet("/e1"),
                          getStatus(ctlst.exports.keys().contains("/e1"))))
            .expect(st == OK)
            .then(expDel("/e1"))
            .then(apiRead(RExportGet("/e1"),
                          getStatus(ctlst.exports.keys().contains("/e1"))))
            .expect(st == NOT_FOUND)
}
