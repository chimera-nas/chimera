// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/**
 * ctl.qnt -- the control plane and the data path in one state machine.
 *
 * The administrative API is only half of what it does.  Creating an export
 * makes a namespace reachable over NFS; deleting one is supposed to make it
 * unreachable, and that second half is not visible from the API at all -- both
 * calls return 204, and the difference shows up only in what the next NFS
 * request sees.  A model of the endpoints alone would be a model of the
 * paperwork.
 *
 * So the state here is the administrative state PLUS the filehandles a client
 * is holding, and a step is either a REST call or an NFS request.  The
 * property the whole suite exists for falls out of putting the two together:
 *
 *   `handleLive` -- an operation on a filehandle whose export no longer exists
 *   is NFS3ERR_STALE.
 *
 * That is not a chimera behaviour being written down; chimera currently serves
 * such a handle, and serves it with NO access policy, because read-only,
 * squash and security-flavor all live on the export record the handle's
 * stamped id no longer finds (nfs_common.h, chimera_nfs_fh_decode).  A file
 * written through a squash=all handle after its export is deleted lands owned
 * by root.  Linux resolves the export on every request
 * (nfsd_set_fh_dentry -> rqst_exp_find) and answers ESTALE, which is what
 * makes `exportfs -u` a revocation rather than a note in a file.  The model
 * says what an administrator has the right to expect, and the harness reports
 * the difference.
 *
 * Two things are deliberately NOT modelled, because getting them right needs a
 * decision about the wire format rather than about the control plane:
 *
 *   * An export deleted and re-created at a DIFFERENT path.  A filehandle
 *     carries its export's id and nothing about the path, so an id that comes
 *     back pointing somewhere else would revive handles into the old
 *     namespace.  Linux is immune because its handle carries the filesystem's
 *     fsid rather than an index into a table.  Each export name here is pinned
 *     to one path (EXPORTS) so the corpus never asks the question.
 *
 *   * Re-created with the same path and id, which the model DOES allow and
 *     requires to revive the handles: that is a configuration reload
 *     (`exportfs -r`), and a server that broke every client's handles on one
 *     would be unusable.  Policy is read per request, so a re-creation with a
 *     different access mode or squash applies to handles that already exist --
 *     which is the same rule stated once, not a special case.
 */
module ctl {

    import ctlApi.* from "./ctl_api"

    // ==================================================================
    // Configuration
    // ==================================================================

    /// The filesystem names the traces may create, within one backend module.
    const FSS: Set[str]

    /// The mount names the traces may attach at.
    const MOUNTS: Set[str]

    /**
     * The exports, each pinned to one path and one id.
     *
     * Pinned rather than chosen per creation, for two different reasons.  The
     * id is stamped into every filehandle, so letting the server auto-assign
     * would make a handle's meaning depend on the order exports happened to be
     * created in -- and re-creating an export would then silently either
     * revive or orphan handles depending on how many others existed at the
     * time.  The path is pinned because varying it across a re-creation is the
     * case this model does not claim to specify (see the module comment).
     */
    const EXPORTS: str -> { path: str, id: int }

    /// SMB share and S3 bucket names, each pinned to the mount it roots at.
    /// Their protocol surface is not modelled here (the harness has no SMB or
    /// S3 client on this loop yet); what is modelled is that they are
    /// collections with the same create/read/delete contract as the others,
    /// and that they pin the mount underneath them.
    const SHARES: str -> str
    const BUCKETS: str -> str

    /// The user records the traces may add and remove.
    const USERS: Set[str]

    /// The credentials NFS requests arrive under.  Root and one unprivileged
    /// user, which is the smallest pair that tells the three squash modes
    /// apart: only `all` maps the unprivileged one, only `none` leaves root
    /// alone, and `root` is the one that does exactly one of the two.
    const CREDS: Set[int]

    /// The identity a squashed caller is mapped to (the export's anonuid).
    const ANONUID: int

    // ==================================================================
    // Status codes
    // ==================================================================

    /// mountstat3 (RFC 1813 Appendix I).
    pure val MNT3_OK       = 0
    pure val MNT3ERR_NOENT = 2

    /// nfsstat3 (RFC 1813 Section 2.6).
    pure val NFS3_OK       = 0
    pure val NFS3ERR_ROFS  = 30
    pure val NFS3ERR_STALE = 70

    // ==================================================================
    // Filehandles
    // ==================================================================

    /**
     * A filehandle as the wire carries it, reduced to what decides its fate.
     *
     * `exp` is the export id stamped into it at mint time and presented on
     * every later request; `mnt` and `inst` stand for the inner VFS handle,
     * which is scoped to one attachment of one filesystem.  A handle outlives
     * both -- that is the entire problem -- so it records what it was minted
     * against rather than pointing at anything.
     */
    type Handle = { exp: int, mnt: str, inst: int }

    /**
     * Is this handle still one the server may serve?
     *
     * Two independent ways it stops being one, and the model requires the same
     * answer for both because a client cannot act differently on them:
     *
     *   - its export is gone.  The id names nothing, so there is no access
     *     policy to apply, and serving it would mean serving it unpoliced.
     *   - the mount it lives in is gone, or has been replaced by another
     *     attachment.  The inner handle is scoped to an attachment, so a
     *     remount is a new namespace as far as an old handle is concerned,
     *     even when the same filesystem comes back.
     */
    pure def handleLive(c: Ctl, h: Handle): bool = and {
        exportById(c, h.exp).size() > 0,
        c.mounts.keys().contains(h.mnt),
        c.mounts.keys().contains(h.mnt) implies c.mounts.get(h.mnt).inst == h.inst,
    }

    /// The export a live handle is served under.  Policy is read here, per
    /// request, rather than captured when the handle was minted -- so a change
    /// of access mode or squash reaches handles that already exist.
    pure def handleExport(c: Ctl, h: Handle): Export =
        c.exports.get(exportById(c, h.exp).fold("", (a, x) => if (a == "") x else a))

    /**
     * The identity a caller acts under, after the export's squash policy.
     *
     * SQ_NONE passes everything through, SQ_ALL maps everything to the
     * anonymous identity, and SQ_ROOT maps only uid 0 -- the classic
     * root_squash, which exists so that a client's administrator is not the
     * server's.
     */
    pure def squashUid(sq: str, cred: int): int =
        if (sq == SQ_ALL) ANONUID
        else if (sq == SQ_ROOT and cred == 0) ANONUID
        else cred

    // ==================================================================
    // Steps
    // ==================================================================

    /**
     * What one step did, in the form the harness replays and checks.
     *
     * Every arm carries the answer the server must give, so the oracle is the
     * trace itself: the harness issues the call and compares.  OCreate carries
     * the resulting owner as well as the status, because that is where a lost
     * squash policy shows up -- the status is 0 either way.
     */
    type Op =
        | OApi({ req: Req, st: int })
        | OMnt({ name: str, st: int, slot: int })
        | OGetattr({ slot: int, cred: int, st: int })
        // `seq` is the serial the fresh name is built from; the harness
        // renders it ("f<seq>").  An int rather than a string because quint
        // has no int-to-string, and because the harness needs the number
        // anyway to keep its own naming in step.
        | OCreate({ slot: int, cred: int, seq: int, st: int, uid: int })

    // ==================================================================
    // State
    // ==================================================================

    var ctlst: Ctl            // the administrative state
    var handles: int -> Handle // filehandles the client is holding, by slot
    var nhandles: int         // slots used so far; the next MOUNT takes this one
    var nfiles: int           // names minted so far, so every create is fresh
    var lastOp: Op

    /**
     * The name of the export the initial state provides, and the mount and
     * filesystem under it.
     *
     * The model starts from a server that is already serving something rather
     * than from an empty one.  That is not a shortcut: an empty start makes a
     * random walk spend most of a trace assembling a filesystem, a mount and
     * an export before a single NFS request can succeed, and the interaction
     * this suite exists for -- a handle taken before a deletion and presented
     * after it -- needs all three to have existed FIRST.  A walk from empty
     * reaches it rarely enough that the corpus would be mostly setup.
     *
     * Nothing is lost, because the trace may delete every one of them: the
     * empty state is one step away, and building back up from it is a
     * sequence the walk still takes.
     */
    const SEED_FS: str
    const SEED_MOUNT: str
    const SEED_EXPORT: str

    action init = all {
        ctlst' = { fss: Set(SEED_FS),
                   mounts: Map(SEED_MOUNT -> { fs: SEED_FS, inst: 1 }),
                   exports: Map(SEED_EXPORT ->
                                { path: EXPORTS.get(SEED_EXPORT).path,
                                  id: EXPORTS.get(SEED_EXPORT).id,
                                  ro: false, squash: SQ_NONE }),
                   shares: Map(), buckets: Map(), users: Set(), nextInst: 2 },
        handles' = Map(),
        nhandles' = 0,
        nfiles' = 0,
        lastOp' = OApi({ req: RConfig, st: OK }),
    }

    // ==================================================================
    // Administrative steps
    // ==================================================================

    /// Apply one REST call and record the status it must return.
    action api(r: Req, res: ApiRes): bool = all {
        ctlst' = res.c,
        handles' = handles,
        nhandles' = nhandles,
        nfiles' = nfiles,
        lastOp' = OApi({ req: r, st: res.st }),
    }

    /// A read: it changes nothing, and its status is whether the name is there.
    action apiRead(r: Req, st: int): bool =
        api(r, { st: st, c: ctlst })

    // ==================================================================
    // Protocol steps
    // ==================================================================

    /**
     * MOUNTPROC3_MNT of an export by name.
     *
     * Reachable exactly when the export exists AND its path names a live
     * mount.  The second condition is where the "an export may be declared
     * over a mount that does not exist yet" choice in ctl_api.qnt is paid for:
     * such an export is a perfectly good administrative record and simply
     * cannot be mounted.
     */
    action mnt(name: str): bool = {
        val known = ctlst.exports.keys().contains(name)
        val e     = if (known) ctlst.exports.get(name)
                    else { path: "", id: 0, ro: false, squash: SQ_NONE }
        val ready = known and ctlst.mounts.keys().contains(e.path)
        all {
            ctlst' = ctlst,
            nfiles' = nfiles,
            handles' = if (ready)
                           handles.put(nhandles,
                                       { exp: e.id, mnt: e.path,
                                         inst: ctlst.mounts.get(e.path).inst })
                       else handles,
            nhandles' = if (ready) nhandles + 1 else nhandles,
            lastOp' = OMnt({ name: name,
                             st: if (ready) MNT3_OK else MNT3ERR_NOENT,
                             slot: if (ready) nhandles else -1 }),
        }
    }

    /// A non-mutating request on a held handle.  It asks one question: is the
    /// handle still one the server will serve?
    action getattr(slot: int, cred: int): bool = {
        val h = handles.get(slot)
        all {
            ctlst' = ctlst, handles' = handles, nhandles' = nhandles,
            nfiles' = nfiles,
            lastOp' = OGetattr({ slot: slot, cred: cred,
                                 st: if (handleLive(ctlst, h)) NFS3_OK
                                     else NFS3ERR_STALE }),
        }
    }

    /**
     * A mutating request on a held handle: create a file in the directory it
     * names.
     *
     * The name is always fresh, so the answer never depends on what the
     * directory already holds -- this suite is about the control plane, and
     * the filesystem semantics belong to the nfs3 model.  What it does depend
     * on is the export's two access policies, checked in the server's own
     * order: the handle must be servable at all, then the export must not be
     * read-only, and only then does the write happen -- under the identity the
     * squash policy assigns.
     */
    action create(slot: int, cred: int): bool = {
        val h    = handles.get(slot)
        val live = handleLive(ctlst, h)
        val e    = if (live) handleExport(ctlst, h)
                   else { path: "", id: 0, ro: false, squash: SQ_NONE }
        val st   = if (not(live)) NFS3ERR_STALE
                   else if (e.ro) NFS3ERR_ROFS
                   else NFS3_OK
        all {
            ctlst' = ctlst, handles' = handles, nhandles' = nhandles,
            nfiles' = nfiles + 1,
            lastOp' = OCreate({ slot: slot, cred: cred,
                                seq: nfiles,
                                st: st,
                                uid: if (st == NFS3_OK) squashUid(e.squash, cred)
                                     else -1 }),
        }
    }

    // ==================================================================
    // The step relation
    // ==================================================================

    /// The administrative half: every endpoint, over every name.
    action stepApi: bool = {
        nondet f = FSS.oneOf()
        nondet m = MOUNTS.oneOf()
        nondet x = EXPORTS.keys().oneOf()
        nondet s = SHARES.keys().oneOf()
        nondet b = BUCKETS.keys().oneOf()
        nondet u = USERS.oneOf()
        nondet ro = Set(false, true).oneOf()
        nondet sq = Set(SQ_NONE, SQ_ROOT, SQ_ALL).oneOf()
        any {
            api(RFsCreate(f), fsCreate(ctlst, f)),
            api(RFsDelete(f), fsDelete(ctlst, f)),

            api(RMountCreate({ name: m, fs: f }), mountCreate(ctlst, m, f)),
            api(RMountDelete(m), mountDelete(ctlst, m)),
            apiRead(RMountGet(m),
                    getStatus(ctlst.mounts.keys().contains(m))),
            apiRead(RMountList, OK),

            api(RExportCreate({ name: x, ro: ro, squash: sq }),
                exportCreate(ctlst, x,
                             { path: EXPORTS.get(x).path,
                               id: EXPORTS.get(x).id, ro: ro, squash: sq })),
            api(RExportDelete(x), exportDelete(ctlst, x)),
            apiRead(RExportGet(x),
                    getStatus(ctlst.exports.keys().contains(x))),
            apiRead(RExportList, OK),

            api(RShareCreate(s), shareCreate(ctlst, s, SHARES.get(s))),
            api(RShareDelete(s), shareDelete(ctlst, s)),
            apiRead(RShareGet(s), getStatus(ctlst.shares.keys().contains(s))),
            apiRead(RShareList, OK),

            api(RBucketCreate(b), bucketCreate(ctlst, b, BUCKETS.get(b))),
            api(RBucketDelete(b), bucketDelete(ctlst, b)),
            apiRead(RBucketGet(b),
                    getStatus(ctlst.buckets.keys().contains(b))),
            apiRead(RBucketList, OK),

            api(RUserCreate(u), userCreate(ctlst, u)),
            api(RUserDelete(u), userDelete(ctlst, u)),
            apiRead(RUserGet(u), getStatus(ctlst.users.contains(u))),
            apiRead(RUserList, OK),

            apiRead(RConfig, OK),
        }
    }

    /**
     * The protocol half.
     *
     * MOUNT is always available -- of a name that exists or one that does not,
     * which is how the corpus reaches both answers.  The handle operations
     * need a handle, and deliberately draw from EVERY slot ever minted rather
     * than only the live ones: a handle whose export has since been deleted is
     * the case this suite exists for, and excluding it would leave the model's
     * central claim untested.
     */
    action stepProto: bool = {
        nondet x = EXPORTS.keys().oneOf()
        nondet c = CREDS.oneOf()
        any {
            mnt(x),
            all {
                nhandles > 0,
                nondet slot = 0.to(nhandles - 1).oneOf()
                any {
                    getattr(slot, c),
                    create(slot, c),
                }
            },
        }
    }

    action step = any {
        stepApi,
        stepProto,
    }

    /**
     * A walk that varies an export's POLICY under a client that keeps using
     * it.
     *
     * The mixed walks reach this only by accident, and rarely: they spend
     * most of their administrative steps on mounts and filesystems, and a
     * mount deleted out from under a handle strands it -- after which every
     * later request on it answers STALE and the policy is never consulted
     * again.  So the states where an export is read-only or squashing AND a
     * live handle is presented to it go untested, which is precisely where
     * "policy is read per request" either holds or does not.
     *
     * This flavour never touches mounts or filesystems.  It re-creates the
     * exports with different access modes and squash policies and keeps
     * asking, which is what makes a policy change observable through a handle
     * that predates it.
     */
    action stepPolicy: bool = {
        nondet x = EXPORTS.keys().oneOf()
        nondet sq = Set(SQ_NONE, SQ_ROOT, SQ_ALL).oneOf()
        any {
            stepProto,
            stepProto,
            // A read-WRITE export with some squash policy.  Weighted up
            // because it is the only configuration in which a write actually
            // happens, and therefore the only one in which a squash policy is
            // observable at all: read-only and deleted are both answered
            // before the identity is ever consulted.  An even weighting spent
            // most of the walk on states that cannot see the thing this
            // flavour exists to see.
            api(RExportCreate({ name: x, ro: false, squash: sq }),
                exportCreate(ctlst, x,
                             { path: EXPORTS.get(x).path,
                               id: EXPORTS.get(x).id, ro: false, squash: sq })),
            any {
                api(RExportCreate({ name: x, ro: true, squash: sq }),
                    exportCreate(ctlst, x,
                                 { path: EXPORTS.get(x).path,
                                   id: EXPORTS.get(x).id, ro: true,
                                   squash: sq })),
                api(RExportDelete(x), exportDelete(ctlst, x)),
            },
        }
    }

    /// A walk that only administers: every step is an API call, so the
    /// referential-integrity refusals (a mount pinned by an export, a
    /// filesystem pinned by a mount) come up several times as often as in a
    /// mixed walk, where most steps are NFS.
    action stepAdminOnly = stepApi

    /**
     * A walk weighted towards the data path.
     *
     * Two thirds protocol, and the administrative third is restricted to the
     * calls that change what a handle sees -- creating and deleting exports,
     * and mounts.  This is the flavour that produces long runs of requests on
     * handles across an export's lifetime, which is where the squash and
     * read-only policies are observed to still apply (or not).
     */
    action stepProtoHeavy: bool = {
        nondet x = EXPORTS.keys().oneOf()
        nondet m = MOUNTS.oneOf()
        nondet f = FSS.oneOf()
        nondet ro = Set(false, true).oneOf()
        nondet sq = Set(SQ_NONE, SQ_ROOT, SQ_ALL).oneOf()
        any {
            stepProto,
            stepProto,
            any {
                api(RExportCreate({ name: x, ro: ro, squash: sq }),
                    exportCreate(ctlst, x,
                                 { path: EXPORTS.get(x).path,
                                   id: EXPORTS.get(x).id, ro: ro, squash: sq })),
                api(RExportDelete(x), exportDelete(ctlst, x)),
                api(RMountCreate({ name: m, fs: f }), mountCreate(ctlst, m, f)),
                api(RMountDelete(m), mountDelete(ctlst, m)),
                api(RFsCreate(f), fsCreate(ctlst, f)),
            },
        }
    }

    // ==================================================================
    // Invariants
    // ==================================================================

    /**
     * What no sequence of API calls may produce.
     *
     * Both of these are consequences of the refusals in ctl_api.qnt rather
     * than extra requirements, which is the point: they state the reason those
     * refusals exist, so a model change that dropped one is caught here rather
     * than by a trace that happens to notice.
     */
    val inv = and {
        // A mount over a filesystem that does not exist.  mountCreate requires
        // the target, and fsDelete is refused while a mount holds it.
        ctlst.mounts.keys().forall(m => ctlst.fss.contains(ctlst.mounts.get(m).fs)),

        // Two exports sharing an id.  A filehandle names its export by id
        // alone, so a duplicate would make a handle ambiguous -- there would
        // be no fact of the matter about which policy applies to it.
        ctlst.exports.keys().forall(a =>
            ctlst.exports.keys().forall(b =>
                (ctlst.exports.get(a).id == ctlst.exports.get(b).id)
                    implies (a == b))),

        // A live handle resolves to exactly one export.
        handles.keys().forall(s =>
            exportById(ctlst, handles.get(s).exp).size() <= 1),
    }
}
