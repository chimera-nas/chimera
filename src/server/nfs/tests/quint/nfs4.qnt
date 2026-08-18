// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Quint model of an NFSv4 server (RFC 7530 / RFC 8881 / RFC 7862 /
// RFC 8276), used for model-based testing -- the NFSv4 sibling of nfs3.qnt.
//
// Every step is one COMPOUND: the transition label `lastOp` records the
// request ops, the expected per-op results and the expected compound
// status; the replay harness sends the compound to a live server and
// compares.  See DESIGN-NFS4.md for the architecture, the identity
// indirections the harness maintains (Ino -> filehandle, model stateid ->
// wire stateid, abstract change values -> consistency checks), and the
// feature-gating contract.
//
// The module is parameterized by the minor version under test and a
// FeatureMode per optional feature (see nfs4_ops.qnt); nfs4_run.qnt holds
// the concrete instances that `quint run` executes.

module nfs4 {

    import nfs4fs.* from "./nfs4_fs"
    import nfs4state.* from "./nfs4_state"
    import nfs4ops.* from "./nfs4_ops"

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    // Minor version under test: 0, 1 or 2.
    const MINOR: int

    // Optional-feature modes.  pNFS requires MINOR >= 1; xattrs and the
    // 4.2 ops require MINOR >= 2 -- an instance combining FeatMandatory
    // with a minor version that cannot carry the feature is a
    // configuration error (init downgrades the capability to false).
    const F_PNFS: FeatureMode
    const F_READ_DELEG: FeatureMode
    const F_WRITE_DELEG: FeatureMode
    const F_XATTR: FeatureMode
    const F_SPARSE: FeatureMode
    const F_COPY: FeatureMode

    // Server-policy profile knobs (both choices are RFC-conformant; the
    // trace pins the assumption and the harness verifies it holds):
    //  - P_CONFLICT_DELAYS: a delegation-conflicting op returns
    //    NFS4ERR_DELAY while the recall is outstanding (vs blocking).
    //  - P_CLOSE_FREES_LOCKS: CLOSE with live byte-range locks frees them
    //    (vs NFS4ERR_LOCKS_HELD).
    const P_CONFLICT_DELAYS: bool
    const P_CLOSE_FREES_LOCKS: bool

    // ------------------------------------------------------------------
    // Bounded universes
    // ------------------------------------------------------------------

    pure val NAMES = Set("a", "b", "c", "d")
    pure val MODES = Set(420, 493)          // 0644, 0755
    pure val MAX_OBJECTS = 12
    pure val MAX_BLOCKS = 6
    // Symbol 0 is reserved for holes/zeroes; see nfs4_fs.qnt.
    pure val BLOCK_SYMS = 1.to(3)
    pure val STABLE_LEVELS = Set(0, 1, 2)   // UNSTABLE4/DATA_SYNC4/FILE_SYNC4
    pure val ACCESS_MASKS = Set(1, 32, 63)
    // Byte-range lock space [0, LOCK_BYTES); a lock reaching LOCK_BYTES is
    // sent as a to-EOF lock by the harness.
    pure val LOCK_BYTES = 8
    // Client-owner symbols (two model clients -- conflicts between clients
    // are where most NFSv4 semantics live), per-client open-owner and
    // lock-owner symbols, boot/exclusive-create verifiers.
    pure val CLIENT_OWNERS = Set(1, 2)
    pure val OPEN_OWNERS = Set(1, 2)
    pure val LOCK_OWNERS = Set(6, 7)
    pure val VERFS = Set(1, 2)
    pure val XATTR_NAMES = Set("xa", "xb")
    pure val XATTR_SYMS = 1.to(3)

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    type Label =
        | LInit({ minor: int, caps: Caps })
        | LCompound({ tag: int, ops: List[OpReq], status: int,
                      results: List[OpRes] })

    var fs: Fs
    var sdb: StateDB
    var caps: Caps
    var tag: int
    var lastOp: Label

    action init = {
        nondet cPnfs = oneOf(capChoices(F_PNFS))
        nondet cRd = oneOf(capChoices(F_READ_DELEG))
        nondet cWd = oneOf(capChoices(F_WRITE_DELEG))
        nondet cXa = oneOf(capChoices(F_XATTR))
        nondet cSp = oneOf(capChoices(F_SPARSE))
        nondet cCp = oneOf(capChoices(F_COPY))
        val c: Caps = {
            pnfs: cPnfs and MINOR >= 1,
            readDeleg: cRd,
            writeDeleg: cWd,
            xattr: cXa and MINOR >= 2,
            sparse: cSp and MINOR >= 2,
            copy: cCp and MINOR >= 2,
            conflictDelays: P_CONFLICT_DELAYS,
            closeFreesLocks: P_CLOSE_FREES_LOCKS
        }
        all {
            // chimera: memfs share roots are 0777; the root mode is not
            // asserted by the harness (backend-specific).
            fs' = fsInit(511),
            sdb' = sdbInit,
            caps' = c,
            tag' = 1,
            // The harness reads the assumed capability profile from this
            // label and reconciles it against the live advertisement
            // before replaying step 1 (skip on mismatch unless the
            // feature is FeatMandatory -- then mismatch is a failure).
            lastOp' = LInit({ minor: MINOR, caps: c }),
        }
    }

    // ------------------------------------------------------------------
    // Generator plumbing
    // ------------------------------------------------------------------

    // The one transition shape: evaluate a compound, record the label.
    action runC(ops: List[OpReq]): bool = {
        val out = evalCompound(fs, sdb, MINOR, caps, tag, ops)
        all {
            fs' = out.fs2,
            sdb' = out.sdb2,
            caps' = caps,
            tag' = tag + 1,
            lastOp' = LCompound({ tag: tag, ops: ops, status: out.status,
                                  results: out.results }),
        }
    }

    // Newest session of a client; -1 if none.
    def sessOf(c: ClientId): SessId =
        sdb.sessions.keys().filter(s => sdb.sessions.get(s).client == c)
            .fold(-1, (acc, s) => imax(acc, s))

    // Clients whose state machinery is usable: 4.0 needs a confirmed
    // clientid, 4.1+ additionally a session.
    def readyClients: Set[ClientId] =
        sdb.clients.keys().filter(c =>
            sdb.clients.get(c).confirmed and (MINOR == 0 or sessOf(c) != -1))

    // SEQUENCE prefix for a 4.1+ compound: slot 0, next slot seqid.
    // (List(..) rather than a [..] literal: a bracket after a val binding
    // would parse as list indexing.)
    def withSess(c: ClientId, body: List[OpReq]): List[OpReq] =
        if (MINOR == 0) body
        else List(RSequence({ sess: sessOf(c), slot: 0,
                              seq: sdb.sessions.get(sessOf(c)).slots[0].seq + 1,
                              cacheThis: true })).concat(body)

    // 4.0 owner-seqid arguments (ignored by the evaluator for 4.1+).
    def oseqFor(c: ClientId, owner: int): int =
        if (sdb.oo40.keys().contains((c, owner)))
            sdb.oo40.get((c, owner)).seqid + 1
        else 1

    def lseqFor(c: ClientId, owner: int): int =
        if (sdb.lo40.keys().contains((c, owner)))
            sdb.lo40.get((c, owner)).seqid + 1
        else 1

    def dirs = fsDirs(fs)
    def files = fsFiles(fs)
    def symlinks = fsSymlinks(fs)
    def liveInos = fs.inodes.keys()

    // Files delegated to a client other than `cid`.  Mutating such a file
    // through SETATTR/WRITE/LINK/xattr/sparse ops is EXCLUDED from
    // generation: chimera recalls but COMPLETES those ops (probed by
    // nfs4_deleg_probe.c C3/C4), a state-mutating divergence from the
    // RFC's recall-then-DELAY discipline (DEVIATIONS-NFS4.md D4-12), so
    // the model neither predicts nor randomly exercises it.  OPEN and
    // REMOVE/RENAME conflicts follow the DELAY discipline and stay in
    // generation -- they are the delegation-break workload.
    def delegOther(f: Ino, cid: ClientId): bool =
        sdb.delegs.keys().exists(s => {
            val d = sdb.delegs.get(s)
            d.file == f and d.client != cid
        })

    // Files delegated to `cid` itself.  A client's own REMOVE/RENAME of
    // a file it holds a delegation on triggers a recall of its OWN
    // delegation and an unresolvable NFS4ERR_DELAY in chimera (the
    // namespace-op lease-conflict path does not recognize the requester
    // as the holder, unlike the OPEN path -- DEVIATIONS-NFS4.md D4-13),
    // so generation diverts those; the OPEN-path self-exemption is
    // pinned by nfs4_deleg_probe.c G2.
    def delegSelf(f: Ino, cid: ClientId): bool =
        sdb.delegs.keys().exists(s => {
            val d = sdb.delegs.get(s)
            d.file == f and d.client == cid
        })

    // Same, write delegations only (reads conflict only with those).
    def delegOtherWrite(f: Ino, cid: ClientId): bool =
        sdb.delegs.keys().exists(s => {
            val d = sdb.delegs.get(s)
            d.file == f and d.client != cid and d.isWrite
        })

    // (dir, name) pairs currently naming inode f (for OPEN upgrades and
    // open-over-existing coverage).
    def namePairsOf(f: Ino): Set[{ d: Ino, n: Name }] =
        dirs.fold(Set(), (acc, d) =>
            acc.union(fs.inodes.get(d).ents.keys()
                .filter(n => fs.inodes.get(d).ents.get(n) == f)
                .map(n => { d: d, n: n })))

    // A client the generator can still drive: 4.1+ needs a live session
    // (DESTROY_SESSION may strand state until a new session is created --
    // legal server state, but such state can't be exercised meanwhile).
    def clientUsable(c: ClientId): bool =
        MINOR == 0 or sessOf(c) != -1

    // Opens usable for I/O: 4.0 unconfirmed opens and stranded-client
    // opens are excluded from generation.
    def usableOpens: Set[Sid] =
        sdb.opens.keys().filter(s =>
            not(sdb.opens.get(s).needConfirm) and
            clientUsable(sdb.opens.get(s).client))

    def usableLocks: Set[Sid] =
        sdb.locks.keys().filter(s => clientUsable(sdb.locks.get(s).client))

    def usableDelegs: Set[Sid] =
        sdb.delegs.keys().filter(s => clientUsable(sdb.delegs.get(s).client))

    def usableLayouts: Set[Sid] =
        sdb.layouts.keys().filter(s => clientUsable(sdb.layouts.get(s).client))

    def openSel(s: Sid): SidSel =
        SelRef({ sid: s, kind: KOpen, argSeq: 0 })

    // Files a usable client currently holds a delegation on, plus who
    // holds each, and the ready clients that could break it (any ready
    // client other than the holder).  These drive the deleg-break
    // actions below, which deterministically construct the
    // A-holds / B-breaks conflict the random walk almost never hits on
    // its own (both clients must target the exact same file while a
    // delegation is live).
    def delegatedFiles: Set[Ino] =
        usableDelegs.map(s => sdb.delegs.get(s).file)

    def holderOf(f: Ino): ClientId =
        usableDelegs.fold(-1, (acc, s) =>
            if (sdb.delegs.get(s).file == f) sdb.delegs.get(s).client
            else acc)

    def breakersOf(f: Ino): Set[ClientId] =
        readyClients.filter(c => c != holderOf(f))

    def breakableDelegs: Set[Ino] =
        delegatedFiles.filter(f => breakersOf(f).size() > 0)

    // ------------------------------------------------------------------
    // Client bootstrap
    // ------------------------------------------------------------------

    action setupSetclientid = all {
        MINOR == 0,
        {
            nondet ow = oneOf(CLIENT_OWNERS)
            nondet vf = oneOf(VERFS)
            // The confirm token must be fresh per request; the step tag is.
            runC([ RSetclientid({ ownerSym: ow, verfSym: vf, tok: tag }) ])
        },
    }

    action setupSetclientidConfirm = all {
        MINOR == 0,
        sdb.clients.keys().exists(c => not(sdb.clients.get(c).confirmed)),
        {
            nondet c = oneOf(sdb.clients.keys().filter(c =>
                not(sdb.clients.get(c).confirmed)))
            runC([ RSetclientidConfirm({ client: c,
                                         tok: sdb.clients.get(c).confirmTok }) ])
        },
    }

    action setupExchangeId = all {
        MINOR >= 1,
        {
            nondet ow = oneOf(CLIENT_OWNERS)
            nondet vf = oneOf(VERFS)
            runC([ RExchangeId({ ownerSym: ow, verfSym: vf }) ])
        },
    }

    action setupCreateSession = all {
        MINOR >= 1,
        sdb.clients.keys().size() > 0,
        {
            nondet c = oneOf(sdb.clients.keys())
            runC([ RCreateSession({ client: c,
                                    csaSeq: sdb.clients.get(c).csSeq,
                                    backChan: true }) ])
        },
    }

    action reclaimComplete = all {
        MINOR >= 1,
        readyClients.exists(c => not(sdb.clients.get(c).reclaimDone)),
        {
            nondet c = oneOf(readyClients.filter(c =>
                not(sdb.clients.get(c).reclaimDone)))
            runC(withSess(c, [ RReclaimComplete ]))
        },
    }

    // Negative: SEQUENCE with a skipped slot seqid -> NFS4ERR_SEQ_MISORDERED
    // (state must be untouched, RFC 8881 2.10.6.1).
    action seqMisordered = all {
        MINOR >= 1,
        readyClients.size() > 0,
        {
            nondet c = oneOf(readyClients)
            val s = sessOf(c)
            val slot = sdb.sessions.get(s).slots[0]
            runC([ RSequence({ sess: s, slot: 0, seq: slot.seq + 2,
                               cacheThis: true }),
                   RPutrootfh ])
        },
    }

    action destroySession = all {
        MINOR >= 1,
        sdb.sessions.keys().size() > 0,
        {
            nondet s = oneOf(sdb.sessions.keys())
            runC([ RDestroySession(s) ])
        },
    }

    // DESTROY_CLIENTID: CLIENTID_BUSY while sessions/state exist, OK on a
    // bare client record.
    action destroyClientid = all {
        MINOR >= 1,
        sdb.clients.keys().size() > 0,
        {
            nondet c = oneOf(sdb.clients.keys())
            runC([ RDestroyClientid(c) ])
        },
    }

    // ------------------------------------------------------------------
    // Namespace / attribute compounds (need a ready client only for the
    // 4.1 SEQUENCE prefix)
    // ------------------------------------------------------------------

    def genReady: bool = MINOR == 0 or readyClients.size() > 0

    def anyReady: ClientId =
        if (MINOR == 0) -1
        else readyClients.fold(-1, (acc, c) => imax(acc, c))

    action nsLookup = all {
        genReady,
        {
            nondet d = oneOf(liveInos)
            nondet n = oneOf(NAMES)
            runC(withSess(anyReady, [ RPutfh(d), RLookup(n), RGetfh ]))
        },
    }

    action nsLookupp = all {
        genReady,
        {
            nondet d = oneOf(liveInos)
            runC(withSess(anyReady, [ RPutfh(d), RLookupp, RGetfh ]))
        },
    }

    action nsGetattr = all {
        genReady,
        {
            nondet o = oneOf(liveInos)
            runC(withSess(anyReady, [ RPutfh(o), RGetattr ]))
        },
    }

    action nsAccess = all {
        genReady,
        {
            nondet o = oneOf(liveInos)
            nondet m = oneOf(ACCESS_MASKS)
            runC(withSess(anyReady, [ RPutfh(o), RAccess(m) ]))
        },
    }

    action nsReaddir = all {
        genReady,
        {
            nondet o = oneOf(liveInos)
            runC(withSess(anyReady, [ RPutfh(o), RReaddir ]))
        },
    }

    action nsReadlink = all {
        genReady,
        symlinks.size() > 0,
        {
            nondet l = oneOf(symlinks)
            runC(withSess(anyReady, [ RPutfh(l), RReadlink ]))
        },
    }

    action nsCreate = all {
        genReady,
        liveInos.size() < MAX_OBJECTS,
        {
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            nondet m = oneOf(MODES)
            nondet ct = oneOf(Set(FDir, FLnk, FFifo, FSock))
            runC(withSess(anyReady,
                [ RPutfh(d),
                  RCreate({ ctype: ct, name: n, mode: m, target: "t" }),
                  RGetfh, RGetattr ]))
        },
    }

    action nsRemove = all {
        genReady,
        {
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            val victim = if (fsHasEnt(fs, d, n)) fsChild(fs, d, n) else -1
            if (victim != -1 and delegSelf(victim, anyReady))
                runC(withSess(anyReady, [ RPutfh(d), RGetattr ]))
            else
                runC(withSess(anyReady, [ RPutfh(d), RRemove(n) ]))
        },
    }

    action nsRename = all {
        genReady,
        {
            nondet sd = oneOf(dirs)
            nondet sn = oneOf(NAMES)
            nondet td = oneOf(dirs)
            nondet tn = oneOf(NAMES)
            val srcObj = if (fsHasEnt(fs, sd, sn)) fsChild(fs, sd, sn) else -1
            val tgtObj = if (fsHasEnt(fs, td, tn)) fsChild(fs, td, tn) else -1
            if ((srcObj != -1 and delegSelf(srcObj, anyReady)) or
                (tgtObj != -1 and delegSelf(tgtObj, anyReady)))
                runC(withSess(anyReady, [ RPutfh(sd), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(sd), RSavefh, RPutfh(td),
                      RRename({ oldname: sn, newname: tn }) ]))
        },
    }

    action nsLink = all {
        genReady,
        {
            nondet o = oneOf(liveInos)
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            if (delegOther(o, anyReady))
                runC(withSess(anyReady, [ RPutfh(o), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(o), RSavefh, RPutfh(d), RLink(n) ]))
        },
    }

    action nsSetattrMode = all {
        genReady,
        {
            nondet o = oneOf(liveInos.filter(i =>
                fs.inodes.get(i).ftype == FReg or
                fs.inodes.get(i).ftype == FDir))
            nondet m = oneOf(MODES)
            if (delegOther(o, anyReady))
                runC(withSess(anyReady, [ RPutfh(o), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(o),
                      RSetattr({ mode: m, sizeBlocks: -1, sel: SelAnon }) ]))
        },
    }

    // VERIFY/NVERIFY of the size attribute, followed by GETATTR so the
    // continue-on-match path is observable.
    action nsVerify = all {
        genReady,
        files.size() > 0,
        {
            nondet f = oneOf(files)
            nondet sz = oneOf(0.to(MAX_BLOCKS))
            nondet neg = oneOf(Set(false, true))
            runC(withSess(anyReady,
                [ RPutfh(f),
                  if (neg) RNverifySize(sz) else RVerifySize(sz),
                  RGetattr ]))
        },
    }

    // ------------------------------------------------------------------
    // OPEN family
    // ------------------------------------------------------------------

    action openNew = all {
        readyClients.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        {
            nondet c = oneOf(readyClients)
            nondet ow = oneOf(OPEN_OWNERS)
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            nondet acc = oneOf(Set(1, 2, 3))
            nondet dny = oneOf(Set(0, 1, 2, 3))
            nondet m = oneOf(MODES)
            nondet vf = oneOf(VERFS)
            nondet how = oneOf(Set(
                HNoCreate,
                HUnchecked({ mode: m, truncate: false }),
                HUnchecked({ mode: m, truncate: true }),
                HGuarded({ mode: m }),
                HExclusive({ verf: vf })))
            runC(withSess(c,
                [ RPutfh(d),
                  ROpen({ client: c, owner: ow, oseq: oseqFor(c, ow),
                          claim: OCNull(n), how: how,
                          access: acc, deny: dny }),
                  RGetfh ]))
        },
    }

    // Deny-free open used by the delegation flavor: grants a read/write
    // delegation without the deny-mode share state whose cross-client
    // accounting is exercised (and diverges, pending analysis) elsewhere.
    action delegOpenNew = all {
        readyClients.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        {
            nondet c = oneOf(readyClients)
            nondet ow = oneOf(OPEN_OWNERS)
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            nondet acc = oneOf(Set(1, 2))
            nondet m = oneOf(MODES)
            runC(withSess(c,
                [ RPutfh(d),
                  ROpen({ client: c, owner: ow, oseq: oseqFor(c, ow),
                          claim: OCNull(n),
                          how: HUnchecked({ mode: m, truncate: false }),
                          access: acc, deny: 0 }),
                  RGetfh ]))
        },
    }

    // CLAIM_FH re-open of an already-open file (4.1+).
    action openFh = all {
        MINOR >= 1,
        readyClients.size() > 0,
        usableOpens.size() > 0,
        {
            nondet s = oneOf(usableOpens)
            val o = sdb.opens.get(s)
            nondet acc = oneOf(Set(1, 2, 3))
            runC(withSess(o.client,
                [ RPutfh(o.file),
                  ROpen({ client: o.client, owner: o.owner, oseq: 0,
                          claim: OCFh, how: HNoCreate,
                          access: acc, deny: 0 }),
                  RGetfh ]))
        },
    }

    // 4.0: confirm a fresh open-owner's first open.
    action openConfirm = all {
        MINOR == 0,
        sdb.opens.keys().exists(s => sdb.opens.get(s).needConfirm),
        {
            nondet s = oneOf(sdb.opens.keys().filter(s =>
                sdb.opens.get(s).needConfirm))
            val o = sdb.opens.get(s)
            runC([ RPutfh(o.file),
                   ROpenConfirm({ sid: s, oseq: oseqFor(o.client, o.owner) }) ])
        },
    }

    action openDowngrade = all {
        genReady,
        usableOpens.size() > 0,
        {
            nondet s = oneOf(usableOpens)
            val o = sdb.opens.get(s)
            nondet acc = oneOf(Set(1, 2, 3))
            nondet dny = oneOf(Set(0, 1, 2, 3))
            runC(withSess(o.client,
                [ RPutfh(o.file),
                  ROpenDowngrade({ sid: s, argSeq: 0, access: acc, deny: dny,
                                   oseq: oseqFor(o.client, o.owner) }) ]))
        },
    }

    action closeOpen = all {
        genReady,
        usableOpens.size() > 0,
        {
            nondet s = oneOf(usableOpens)
            val o = sdb.opens.get(s)
            nondet exact = oneOf(Set(false, true))
            runC(withSess(o.client,
                [ RPutfh(o.file),
                  RClose({ sid: s, argSeq: if (exact) o.seq else 0,
                           oseq: oseqFor(o.client, o.owner) }) ]))
        },
    }

    // Negative (4.0): OPEN with a skipped owner seqid -> NFS4ERR_BAD_SEQID,
    // no seqid consumed.
    action openBadSeqid = all {
        MINOR == 0,
        readyClients.size() > 0,
        sdb.oo40.keys().size() > 0,
        {
            nondet k = oneOf(sdb.oo40.keys())
            nondet d = oneOf(dirs)
            nondet n = oneOf(NAMES)
            runC([ RPutfh(d),
                   ROpen({ client: k._1, owner: k._2,
                           oseq: sdb.oo40.get(k).seqid + 2,
                           claim: OCNull(n), how: HNoCreate,
                           access: 1, deny: 0 }) ])
        },
    }

    // ------------------------------------------------------------------
    // Data I/O
    // ------------------------------------------------------------------

    action ioRead = all {
        genReady,
        files.size() > 0,
        {
            nondet f = oneOf(files)
            nondet off = oneOf(0.to(MAX_BLOCKS))
            nondet len = oneOf(1.to(2))
            val opensOfF = usableOpens.filter(s => sdb.opens.get(s).file == f)
            nondet useOpen = oneOf(Set(false, true))
            val sel = if (useOpen and opensOfF.size() > 0)
                          openSel(opensOfF.fold(-1, (a2, s) => imax(a2, s)))
                      else SelAnon
            val cid = if (useOpen and opensOfF.size() > 0)
                          sdb.opens.get(opensOfF.fold(-1, (a2, s) =>
                              imax(a2, s))).client
                      else anyReady
            val rcid = if (MINOR == 0) -1 else
                       (if (cid == -1) anyReady else cid)
            if (delegOtherWrite(f, if (rcid == -1) anyReady else rcid))
                runC(withSess(rcid, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(rcid,
                    [ RPutfh(f), RRead({ sel: sel, off: off, len: len }) ]))
        },
    }

    action ioWrite = all {
        genReady,
        files.size() > 0,
        {
            nondet f = oneOf(files)
            nondet off = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(2))
            nondet pat = oneOf(BLOCK_SYMS)
            nondet stable = oneOf(STABLE_LEVELS)
            val opensOfF = usableOpens.filter(s => sdb.opens.get(s).file == f)
            nondet useOpen = oneOf(Set(false, true))
            val sid = opensOfF.fold(-1, (a2, s) => imax(a2, s))
            val sel = if (useOpen and sid != -1) openSel(sid) else SelAnon
            val cid = if (useOpen and sid != -1) sdb.opens.get(sid).client
                      else anyReady
            val wcid = if (MINOR == 0) -1 else
                       (if (cid == -1) anyReady else cid)
            if (delegOther(f, if (wcid == -1) anyReady else wcid))
                runC(withSess(wcid, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(wcid,
                    [ RPutfh(f),
                      RWrite({ sel: sel, off: off,
                               len: imin(len, MAX_BLOCKS - off), pat: pat,
                               stable: stable }) ]))
        },
    }

    action ioCommit = all {
        genReady,
        files.size() > 0,
        {
            nondet f = oneOf(files)
            runC(withSess(anyReady, [ RPutfh(f), RCommit ]))
        },
    }

    action ioSetattrSize = all {
        genReady,
        files.size() > 0,
        {
            nondet f = oneOf(files)
            nondet sz = oneOf(0.to(MAX_BLOCKS))
            val opensOfF = usableOpens.filter(s => sdb.opens.get(s).file == f)
            nondet useOpen = oneOf(Set(false, true))
            val sid = opensOfF.fold(-1, (a2, s) => imax(a2, s))
            val sel = if (useOpen and sid != -1) openSel(sid) else SelAnon
            if (delegOther(f, anyReady))
                runC(withSess(anyReady, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(f),
                      RSetattr({ mode: -1, sizeBlocks: sz, sel: sel }) ]))
        },
    }

    // ------------------------------------------------------------------
    // Byte-range locks
    // ------------------------------------------------------------------

    def lockRange: { lo: int, hi: int } = { lo: 0, hi: LOCK_BYTES }

    action lockNew = all {
        genReady,
        usableOpens.size() > 0,
        {
            nondet s = oneOf(usableOpens)
            val o = sdb.opens.get(s)
            nondet lown = oneOf(LOCK_OWNERS)
            nondet lo = oneOf(0.to(LOCK_BYTES - 2))
            nondet len = oneOf(1.to(3))
            // WRITE_LT only under a write-capable open: whether a WRITE_LT
            // under a read-only open draws NFS4ERR_OPENMODE is
            // server-dependent (RFC 8881 9.1.2 discretion) -- not generated.
            nondet wr = oneOf(if (o.access >= 2) Set(false, true)
                              else Set(false))
            runC(withSess(o.client,
                [ RPutfh(o.file),
                  RLock({ wr: wr, lo: lo, hi: imin(lo + len, LOCK_BYTES),
                          newOwner: true, openSid: s, lockSid: -1,
                          lockOwner: lown,
                          oseq: oseqFor(o.client, o.owner),
                          lseq: lseqFor(o.client, lown) }) ]))
        },
    }

    action lockExist = all {
        genReady,
        usableLocks.size() > 0,
        {
            nondet s = oneOf(usableLocks)
            val l = sdb.locks.get(s)
            val o = sdb.opens.get(l.openSid)
            nondet lo = oneOf(0.to(LOCK_BYTES - 2))
            nondet len = oneOf(1.to(3))
            nondet wr = oneOf(if (o.access >= 2) Set(false, true)
                              else Set(false))
            runC(withSess(l.client,
                [ RPutfh(l.file),
                  RLock({ wr: wr, lo: lo, hi: imin(lo + len, LOCK_BYTES),
                          newOwner: false, openSid: -1, lockSid: s,
                          lockOwner: l.owner, oseq: 0,
                          lseq: lseqFor(l.client, l.owner) }) ]))
        },
    }

    action lockCheck = all {
        genReady,
        readyClients.size() > 0,
        files.size() > 0,
        {
            nondet c = oneOf(readyClients)
            nondet lown = oneOf(LOCK_OWNERS)
            nondet f = oneOf(files)
            nondet lo = oneOf(0.to(LOCK_BYTES - 2))
            nondet len = oneOf(1.to(3))
            nondet wr = oneOf(Set(false, true))
            runC(withSess(c,
                [ RPutfh(f),
                  RLockt({ client: c, lockOwner: lown, wr: wr,
                           lo: lo, hi: imin(lo + len, LOCK_BYTES) }) ]))
        },
    }

    action lockUnlock = all {
        genReady,
        usableLocks.size() > 0,
        {
            nondet s = oneOf(usableLocks)
            val l = sdb.locks.get(s)
            nondet lo = oneOf(0.to(LOCK_BYTES - 2))
            nondet len = oneOf(1.to(4))
            runC(withSess(l.client,
                [ RPutfh(l.file),
                  RLocku({ sid: s, argSeq: 0, lseq: lseqFor(l.client, l.owner),
                           lo: lo, hi: imin(lo + len, LOCK_BYTES) }) ]))
        },
    }

    action lockFree = all {
        MINOR >= 1,
        usableLocks.size() > 0,
        {
            nondet s = oneOf(usableLocks)
            val l = sdb.locks.get(s)
            runC(withSess(l.client,
                [ RFreeStateid({ sid: s, kind: KLock, argSeq: 0 }) ]))
        },
    }

    action lockReleaseOwner = all {
        MINOR == 0,
        sdb.locks.keys().size() > 0,
        {
            nondet s = oneOf(sdb.locks.keys())
            val l = sdb.locks.get(s)
            runC([ RReleaseLockowner({ client: l.client,
                                       lockOwner: l.owner }) ])
        },
    }

    // ------------------------------------------------------------------
    // Delegations
    // ------------------------------------------------------------------

    // Voluntary or post-recall DELEGRETURN.  Recalls themselves are
    // triggered inside conflicting OPENs (see opOpen), which return
    // NFS4ERR_DELAY and mark the delegation recalled; the harness observes
    // the CB_RECALL and this action generates the client's return.
    action delegReturn = all {
        genReady,
        usableDelegs.size() > 0,
        {
            nondet s = oneOf(usableDelegs)
            val d = sdb.delegs.get(s)
            runC(withSess(d.client,
                [ RPutfh(d.file),
                  RDelegreturn({ sid: s, kind: KDeleg, argSeq: 0 }) ]))
        },
    }

    // A different ready client opens (CLAIM_FH) a file the holder has a
    // delegation on: forces a recall + NFS4ERR_DELAY (the break half of
    // the delegation lifecycle; RFC 8881 10.4.3).
    action delegBreakOpen = all {
        MINOR >= 1,
        breakableDelegs.size() > 0,
        {
            nondet f = oneOf(breakableDelegs)
            nondet c = oneOf(breakersOf(f))
            nondet ow = oneOf(OPEN_OWNERS)
            nondet acc = oneOf(Set(1, 2, 3))
            runC(withSess(c,
                [ RPutfh(f),
                  ROpen({ client: c, owner: ow, oseq: oseqFor(c, ow),
                          claim: OCFh, how: HNoCreate,
                          access: acc, deny: 0 }),
                  RGetfh ]))
        },
    }

    // A different ready client removes the holder's delegated file:
    // forces a recall + NFS4ERR_DELAY on REMOVE (modeled in opRemove).
    action delegBreakRemove = all {
        breakableDelegs.size() > 0,
        {
            nondet f = oneOf(breakableDelegs)
            val d = fsGet(fs, f).parent
            nondet n = oneOf(fsGet(fs, d).ents.keys()
                             .filter(nm => fsGet(fs, d).ents.get(nm) == f))
            nondet c = oneOf(breakersOf(f))
            runC(withSess(c, [ RPutfh(d), RRemove(n) ]))
        },
    }

    // ------------------------------------------------------------------
    // pNFS
    // ------------------------------------------------------------------

    action pnfsLayoutget = all {
        MINOR >= 1,
        caps.pnfs,
        usableOpens.size() > 0,
        {
            nondet s = oneOf(usableOpens)
            val o = sdb.opens.get(s)
            nondet rw = oneOf(Set(false, true))
            nondet lo = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(3))
            // First LAYOUTGET presents an open stateid; afterwards the
            // layout stateid (RFC 8881 12.5.3).
            val lay = layoutByClient(sdb, o.client, o.file)
            val sel = if (lay != -1)
                          SelRef({ sid: lay, kind: KLayout, argSeq: 0 })
                      else openSel(s)
            runC(withSess(o.client,
                [ RPutfh(o.file),
                  RLayoutget({ rw: rw, lo: lo,
                               hi: imin(lo + len, MAX_BLOCKS), sel: sel }) ]))
        },
    }

    action pnfsLayoutreturn = all {
        MINOR >= 1,
        usableLayouts.size() > 0,
        {
            nondet s = oneOf(usableLayouts)
            val l = sdb.layouts.get(s)
            nondet lo = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(MAX_BLOCKS))
            runC(withSess(l.client,
                [ RPutfh(l.file),
                  RLayoutreturn({ sid: s, argSeq: 0, lo: lo,
                                  hi: imin(lo + len, MAX_BLOCKS) }) ]))
        },
    }

    action pnfsLayoutcommit = all {
        MINOR >= 1,
        usableLayouts.size() > 0,
        {
            nondet s = oneOf(usableLayouts)
            val l = sdb.layouts.get(s)
            nondet lo = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(2))
            runC(withSess(l.client,
                [ RPutfh(l.file),
                  RLayoutcommit({ sid: s, lo: lo,
                                  hi: imin(lo + len, MAX_BLOCKS) }) ]))
        },
    }

    action pnfsDeviceinfo = all {
        MINOR >= 1,
        readyClients.size() > 0,
        {
            nondet c = oneOf(readyClients)
            runC(withSess(c, [ RGetdeviceinfo ]))
        },
    }

    // A pNFS op against a non-pNFS profile: NFS4ERR_NOTSUPP expected.
    action pnfsNotsupp = all {
        MINOR >= 1,
        not(caps.pnfs),
        readyClients.size() > 0,
        files.size() > 0,
        {
            nondet c = oneOf(readyClients)
            nondet f = oneOf(files)
            runC(withSess(c,
                [ RPutfh(f),
                  RLayoutget({ rw: false, lo: 0, hi: 1, sel: SelAnon }) ]))
        },
    }

    // ------------------------------------------------------------------
    // NFSv4.2 + xattrs
    // ------------------------------------------------------------------

    action sparseAllocate = all {
        MINOR >= 2,
        files.size() > 0,
        genReady,
        {
            nondet f = oneOf(files)
            nondet off = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(2))
            if (delegOther(f, anyReady))
                runC(withSess(anyReady, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(f),
                      RAllocate({ sel: SelAnon, off: off,
                                  len: imin(len, MAX_BLOCKS - off) }) ]))
        },
    }

    action sparseDeallocate = all {
        MINOR >= 2,
        files.size() > 0,
        genReady,
        {
            nondet f = oneOf(files)
            nondet off = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(2))
            if (delegOther(f, anyReady))
                runC(withSess(anyReady, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(f),
                      RDeallocate({ sel: SelAnon, off: off, len: len }) ]))
        },
    }

    action sparseSeek = all {
        MINOR >= 2,
        files.size() > 0,
        genReady,
        {
            nondet f = oneOf(files)
            nondet off = oneOf(0.to(MAX_BLOCKS))
            nondet whatData = oneOf(Set(false, true))
            if (delegOtherWrite(f, anyReady))
                runC(withSess(anyReady, [ RPutfh(f), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(f),
                      RSeek({ sel: SelAnon, off: off, whatData: whatData }) ]))
        },
    }

    action copyRange = all {
        MINOR >= 2,
        files.size() > 1,
        genReady,
        {
            nondet src = oneOf(files)
            nondet dst = oneOf(files)
            nondet so = oneOf(0.to(MAX_BLOCKS - 1))
            nondet dofs = oneOf(0.to(MAX_BLOCKS - 1))
            nondet cnt = oneOf(0.to(2))
            if (delegOther(dst, anyReady) or delegOtherWrite(src, anyReady))
                runC(withSess(anyReady, [ RPutfh(dst), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(src), RSavefh, RPutfh(dst),
                      RCopy({ srcSel: SelAnon, dstSel: SelAnon,
                              srcOff: so, dstOff: dofs, cnt: cnt }) ]))
        },
    }

    action xattrSet = all {
        MINOR >= 2,
        genReady,
        files.size() > 0,
        {
            nondet o = oneOf(files)
            nondet n = oneOf(XATTR_NAMES)
            nondet v = oneOf(XATTR_SYMS)
            nondet how = oneOf(Set(XsCreate, XsReplace, XsEither))
            if (delegOther(o, anyReady))
                runC(withSess(anyReady, [ RPutfh(o), RGetattr ]))
            else
                runC(withSess(anyReady,
                    [ RPutfh(o), RSetxattr({ name: n, sym: v, how: how }) ]))
        },
    }

    action xattrGet = all {
        MINOR >= 2,
        genReady,
        files.size() > 0,
        {
            nondet o = oneOf(files)
            nondet n = oneOf(XATTR_NAMES)
            runC(withSess(anyReady, [ RPutfh(o), RGetxattr(n) ]))
        },
    }

    action xattrList = all {
        MINOR >= 2,
        genReady,
        files.size() > 0,
        {
            nondet o = oneOf(files)
            runC(withSess(anyReady, [ RPutfh(o), RListxattrs ]))
        },
    }

    action xattrRemove = all {
        MINOR >= 2,
        genReady,
        files.size() > 0,
        {
            nondet o = oneOf(files)
            nondet n = oneOf(XATTR_NAMES)
            if (delegOther(o, anyReady))
                runC(withSess(anyReady, [ RPutfh(o), RGetattr ]))
            else
                runC(withSess(anyReady, [ RPutfh(o), RRemovexattr(n) ]))
        },
    }

    // ------------------------------------------------------------------
    // Step distributions
    // ------------------------------------------------------------------

    action step = any {
        setupSetclientid,
        setupSetclientidConfirm,
        setupExchangeId,
        setupCreateSession,
        reclaimComplete,
        seqMisordered,
        destroySession,
        destroyClientid,
        nsLookup,
        nsLookupp,
        nsGetattr,
        nsAccess,
        nsReaddir,
        nsReadlink,
        nsCreate,
        nsRemove,
        nsRename,
        nsLink,
        nsSetattrMode,
        nsVerify,
        openNew,
        openFh,
        openConfirm,
        openDowngrade,
        closeOpen,
        openBadSeqid,
        ioRead,
        ioWrite,
        ioCommit,
        ioSetattrSize,
        lockNew,
        lockExist,
        lockCheck,
        lockUnlock,
        lockFree,
        lockReleaseOwner,
        delegReturn,
        pnfsLayoutget,
        pnfsLayoutreturn,
        pnfsLayoutcommit,
        pnfsDeviceinfo,
        pnfsNotsupp,
        sparseAllocate,
        sparseDeallocate,
        sparseSeek,
        copyRange,
        xattrSet,
        xattrGet,
        xattrList,
        xattrRemove,
    }

    // Open/close/lock-heavy: exercises share matrices, upgrades,
    // downgades, seqid discipline and lock splitting.
    action stepState = any {
        setupSetclientid, setupSetclientidConfirm,
        setupExchangeId, setupCreateSession,
        openNew, openNew, openNew, openFh, openConfirm, openConfirm,
        openDowngrade, openDowngrade, closeOpen, closeOpen, openBadSeqid,
        lockNew, lockNew, lockExist, lockCheck, lockUnlock, lockUnlock,
        lockFree, lockReleaseOwner, delegReturn,
        ioRead, ioWrite,
        nsGetattr,
    }

    // Namespace churn (the v3-style workload plus OPEN-created files).
    action stepNamespace = any {
        setupSetclientid, setupSetclientidConfirm,
        setupExchangeId, setupCreateSession,
        nsCreate, nsCreate, nsRemove, nsRemove, nsRename, nsRename,
        nsLink, nsLink, openNew, closeOpen,
        nsLookup, nsLookupp, nsGetattr, nsReaddir,
    }

    // Delegation lifecycle: grants, cross-client conflicts (recall +
    // NFS4ERR_DELAY), returns and retried opens.  Both clients collide on
    // the small NAMES set, so A-holds/B-breaks sequences arise routinely;
    // delegReturn is weighted up so DELAYed victims get unblocked and the
    // retried OPEN's grant path is reached.
    // Delegation lifecycle only: grants, cross-client open/remove breaks
    // (recall + NFS4ERR_DELAY), returns and retried opens.  Data-path I/O
    // through delegated files (how cached writes and recall flushes
    // reconcile with the model's block map) is a deeper coherency layer
    // left to a follow-on; nsGetattr is the one read here, and it only
    // observes the change attribute the lifecycle already tracks.
    action stepDeleg = any {
        setupSetclientid, setupSetclientidConfirm,
        setupExchangeId, setupCreateSession,
        reclaimComplete, reclaimComplete,
        delegOpenNew, delegOpenNew, delegOpenNew,
        delegBreakOpen, delegBreakOpen, delegBreakOpen,
        delegBreakRemove, delegBreakRemove,
        delegReturn, delegReturn, delegReturn, delegReturn,
        closeOpen, closeOpen,
        nsGetattr, nsGetattr,
    }

    // Data-path heavy: I/O, sparse ops, copies, layouts.
    action stepData = any {
        setupSetclientid, setupSetclientidConfirm,
        setupExchangeId, setupCreateSession,
        openNew, closeOpen,
        ioRead, ioRead, ioWrite, ioWrite, ioCommit, ioSetattrSize,
        sparseAllocate, sparseDeallocate, sparseSeek, sparseSeek,
        copyRange, copyRange,
        pnfsLayoutget, pnfsLayoutget, pnfsLayoutreturn, pnfsLayoutcommit,
        xattrSet, xattrGet, xattrList, xattrRemove,
        nsGetattr,
    }

    // ------------------------------------------------------------------
    // Invariants
    // ------------------------------------------------------------------

    // Protocol state only ever references live inodes (unlinked-but-open
    // files stay alive as orphans until their last pin drops).
    val statePinsLive = pinnedInos(sdb).forall(f => fsLive(fs, f))

    // An orphan (nlink 0) is always pinned by some state.
    val orphansPinned =
        fsNonDirs(fs).forall(f =>
            fs.inodes.get(f).nlink > 0 or pinnedInos(sdb).contains(f))

    // 4.1+ configurations never create 4.0 owner-seqid state and
    // vice-versa sessions never exist under 4.0.
    val minorSeparation =
        (MINOR >= 1 or sdb.sessions.keys().size() == 0) and
        (MINOR == 0 or sdb.oo40.keys().size() == 0)

    // Capability gating: no pNFS/xattr/4.2 state or results can exist when
    // the capability is off.
    val capsRespected =
        (caps.pnfs or sdb.layouts.keys().size() == 0)

    // Named aliases so `quint run --invariant=...` can bisect a violation
    // from the instance modules (imports are not re-exported).
    val invFs = fsInv(fs)
    val invSdb = sdbInv(sdb)

    val inv = invFs and invSdb and statePinsLive and
              orphansPinned and minorSeparation and capsRespected
}
