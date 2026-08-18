// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// SMB2/3 stateful generator: config constants, the capability profile
// chosen at init, generator actions (session/tree bootstrap, the CREATE
// family, I/O, byte-range locks, oplock/lease grant+break+ack, set/query
// info, compound chains, deliberate negatives), step distributions and
// invariants.
//
// This is the SMB sibling of nfs4.qnt.  Only this module holds var state
// and constants; smb2state and smb2ops are pure and importable without
// instantiation, so smb2Test.qnt unit-tests the CREATE matrix and the
// conflict/break rules standalone.
//
// One transition = one SMB2 message (a singleton or a NextCommand-chained
// compound), recorded in `lastOp` as an ITF label the replay harness reads.

module smb2 {

    import nfs4fs.* from "../../../nfs/tests/quint/nfs4_fs"
    import smb2state.* from "./smb2_state"
    import smb2ops.* from "./smb2_ops"

    // ------------------------------------------------------------------
    // Feature configuration (see smb2ops.FeatureMode)
    // ------------------------------------------------------------------

    // Negotiated dialect the trace pins (2.0.2=0x0202 ... 3.1.1=0x0311);
    // recorded in LInit for the harness, not otherwise branched on here.
    const DIALECT: int

    const F_OPLOCKS: FeatureMode      // legacy oplocks (set_smb_oplocks)
    const F_LEASES: FeatureMode       // SMB2 leases (set_smb_leases)
    const F_DIRLEASE: FeatureMode     // directory leases
    const F_DURABLE: FeatureMode      // durable handles

    // Policy: does a conflicting CREATE break a peer's lease at OPEN time
    // (MS-FSA-correct = true), or does an SMB2 lease survive the open and
    // break only on later I/O / namespace (the chimera Seam-A asymmetry)?
    const P_LEASE_OPEN_BREAK: bool

    // ------------------------------------------------------------------
    // Config constants (bound the state space; the harness maps symbols to
    // wire values)
    // ------------------------------------------------------------------

    pure val SHARES = Set(1)
    pure val CLIENTS = Set(1, 2)              // two client symbols
    pure val NAMES = Set("a", "b", "c")
    pure val LEASE_KEYS = Set(1, 2)
    pure val MAX_OBJECTS = 10
    pure val MAX_SESSIONS = 2
    pure val MAX_TREES = 3
    pure val MAX_OPENS = 6
    pure val MAX_BLOCKS = 4
    pure val BLOCK_SYMS = Set(1, 2, 3)
    pure val LOCK_BYTES = 6

    // DesiredAccess profiles the generator draws from (R/W/H/D).
    pure val ACCESS_R  = { r: true,  w: false, h: true,  d: false }
    pure val ACCESS_W  = { r: false, w: true,  h: true,  d: false }
    pure val ACCESS_RW = { r: true,  w: true,  h: true,  d: false }
    pure val ACCESS_RWD = { r: true, w: true,  h: true,  d: true }
    pure val ACCESS_SET = Set(ACCESS_R, ACCESS_W, ACCESS_RW, ACCESS_RWD)

    // ShareAccess profiles (which axes peers may share).
    pure val SHARE_NONE = { r: false, w: false, d: false }
    pure val SHARE_R    = { r: true,  w: false, d: false }
    pure val SHARE_RW   = { r: true,  w: true,  d: false }
    pure val SHARE_RWD  = { r: true,  w: true,  d: true }
    pure val SHARE_SET = Set(SHARE_NONE, SHARE_R, SHARE_RW, SHARE_RWD)

    pure val DISP_SET = Set(DispOpen, DispCreate, DispOpenIf,
                            DispOverwriteIf, DispSupersede)

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    type Label =
        | LInit({ dialect: int, caps: Caps })
        | LMsg({ tag: int, msg: Msg, results: List[Res] })

    var sdb: StateDB
    var caps: Caps
    var tag: int
    var lastOp: Label

    action init = {
        nondet cOp = oneOf(capChoices(F_OPLOCKS))
        nondet cLe = oneOf(capChoices(F_LEASES))
        nondet cDl = oneOf(capChoices(F_DIRLEASE))
        nondet cDu = oneOf(capChoices(F_DURABLE))
        val c: Caps = {
            oplocks: cOp,
            leases: cLe,
            dirLeases: cDl and cLe,
            durable: cDu,
            leaseOpenBreak: P_LEASE_OPEN_BREAK
        }
        all {
            // chimera: memfs share roots are 0777; the root mode is not
            // asserted by the harness (backend-specific).
            sdb' = stInit(511),
            caps' = c,
            tag' = 1,
            lastOp' = LInit({ dialect: DIALECT, caps: c }),
        }
    }

    // The one transition shape: evaluate a message, record the label.
    action runM(msg: Msg): bool = {
        val out = evalMsg(sdb, caps, msg)
        all {
            sdb' = out.sdb2,
            caps' = caps,
            tag' = tag + 1,
            lastOp' = LMsg({ tag: tag, msg: msg, results: out.results }),
        }
    }

    // A singleton (non-compound) message on session/tree.
    def one(sess: SessionId, tree: TreeId, c: Cmd): Msg =
        { sess: sess, tree: tree, related: false, cmds: [ c ] }

    // ------------------------------------------------------------------
    // Convenience projections
    // ------------------------------------------------------------------

    def fsv: Fs = sdb.fs
    def dirs: Set[Ino] = fsDirs(sdb.fs)
    def files: Set[Ino] = fsFiles(sdb.fs)
    def liveInos: Set[Ino] = sdb.fs.inodes.keys()
    def sessions: Set[SessionId] = sdb.sessions.keys()
    def trees: Set[TreeId] = sdb.trees.keys()
    def opens: Set[FileId] = sdb.opens.keys()

    def treeSess(t: TreeId): SessionId = sdb.trees.get(t).session

    // Opens with read / write access, for I/O generation.
    def readOpens: Set[FileId] =
        opens.filter(f => sdb.opens.get(f).granted.r)
    def writeOpens: Set[FileId] =
        opens.filter(f => sdb.opens.get(f).granted.w)

    // Opens whose caching grant is mid-break (a break ack is due).
    def breakingOpens: Set[FileId] =
        opens.filter(f => sdb.opens.get(f).breaking)

    // ------------------------------------------------------------------
    // Bootstrap: session + tree
    // ------------------------------------------------------------------

    action smbSessionSetup = all {
        sessions.size() < MAX_SESSIONS,
        {
            nondet cs = oneOf(CLIENTS)
            runM({ sess: -1, tree: -1, related: false,
                   cmds: [ CSessionSetup({ clientSym: cs }) ] })
        },
    }

    action smbTreeConnect = all {
        sessions.size() > 0,
        trees.size() < MAX_TREES,
        {
            nondet s = oneOf(sessions)
            nondet sh = oneOf(SHARES)
            runM({ sess: s, tree: -1, related: false,
                   cmds: [ CTreeConnect({ share: sh }) ] })
        },
    }

    // ------------------------------------------------------------------
    // CREATE family
    // ------------------------------------------------------------------

    action smbCreate = all {
        trees.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        opens.size() < MAX_OPENS,
        {
            nondet t = oneOf(trees)
            nondet n = oneOf(NAMES)
            nondet disp = oneOf(DISP_SET)
            nondet acc = oneOf(ACCESS_SET)
            nondet sh = oneOf(SHARE_SET)
            nondet doc = oneOf(Set(false, true))
            nondet lk = oneOf(LEASE_KEYS)
            nondet opl = oneOf(Set(
                OrNone,
                OrBatch,
                OrExclusive,
                OrLease({ key: lk, r: true, w: true, h: true }),
                OrLease({ key: lk, r: true, w: false, h: true })))
            runM(one(treeSess(t), t,
                CCreate({ dir: ROOT, name: n, disp: disp, access: acc,
                          share: sh, isDir: false, delOnClose: doc,
                          oplock: opl })))
        },
    }

    // Create a directory (no oplock, share-all) to exercise the type matrix.
    action smbMkdir = all {
        trees.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        opens.size() < MAX_OPENS,
        {
            nondet t = oneOf(trees)
            nondet n = oneOf(NAMES)
            runM(one(treeSess(t), t,
                CCreate({ dir: ROOT, name: n, disp: DispOpenIf,
                          access: ACCESS_R, share: SHARE_RWD, isDir: true,
                          delOnClose: false, oplock: OrNone })))
        },
    }

    action smbClose = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            runM(one(o.session, o.tree, CClose(FidRef(f))))
        },
    }

    // Open-for-delete + close in one related compound: the SMB idiom for
    // unlinking a path (there is no path-based REMOVE).  Frees an object
    // slot in a single transition, so the random walk cannot exhaust
    // MAX_OBJECTS and deadlock.  A no-op if the name is absent (the CREATE
    // fails and first-error-stops skips the CLOSE).
    action smbDeleteExisting = all {
        trees.size() > 0,
        opens.size() < MAX_OPENS,
        {
            nondet t = oneOf(trees)
            nondet n = oneOf(NAMES)
            runM({ sess: treeSess(t), tree: t, related: true,
                   cmds: [
                       CCreate({ dir: ROOT, name: n, disp: DispOpen,
                                 access: ACCESS_RWD, share: SHARE_RWD,
                                 isDir: false, delOnClose: true,
                                 oplock: OrNone }),
                       CClose(FidRelated) ] })
        },
    }

    // ------------------------------------------------------------------
    // I/O
    // ------------------------------------------------------------------

    action smbRead = all {
        readOpens.size() > 0,
        {
            nondet f = oneOf(readOpens)
            val o = sdb.opens.get(f)
            nondet off = oneOf(0.to(MAX_BLOCKS))
            nondet len = oneOf(1.to(MAX_BLOCKS))
            runM(one(o.session, o.tree,
                CRead({ fid: FidRef(f), off: off, len: len })))
        },
    }

    action smbWrite = all {
        writeOpens.size() > 0,
        {
            nondet f = oneOf(writeOpens)
            val o = sdb.opens.get(f)
            nondet off = oneOf(0.to(MAX_BLOCKS - 1))
            nondet len = oneOf(1.to(MAX_BLOCKS))
            nondet pat = oneOf(BLOCK_SYMS)
            runM(one(o.session, o.tree,
                CWrite({ fid: FidRef(f), off: off, len: len, pat: pat })))
        },
    }

    action smbFlush = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            runM(one(o.session, o.tree, CFlush(FidRef(f))))
        },
    }

    // ------------------------------------------------------------------
    // Byte-range locks
    // ------------------------------------------------------------------

    action smbLock = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            nondet lo = oneOf(0.to(LOCK_BYTES - 1))
            nondet span = oneOf(1.to(2))
            nondet wr = oneOf(Set(false, true))
            runM(one(o.session, o.tree,
                CLock({ fid: FidRef(f), lo: lo, hi: lo + span, wr: wr,
                        unlock: false, failImmediately: true })))
        },
    }

    action smbUnlock = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            nondet lo = oneOf(0.to(LOCK_BYTES - 1))
            nondet span = oneOf(1.to(2))
            runM(one(o.session, o.tree,
                CLock({ fid: FidRef(f), lo: lo, hi: lo + span, wr: false,
                        unlock: true, failImmediately: true })))
        },
    }

    // ------------------------------------------------------------------
    // SET_INFO / QUERY_INFO
    // ------------------------------------------------------------------

    action smbSetEof = all {
        writeOpens.size() > 0,
        {
            nondet f = oneOf(writeOpens)
            val o = sdb.opens.get(f)
            nondet sz = oneOf(0.to(MAX_BLOCKS))
            runM(one(o.session, o.tree,
                CSetEof({ fid: FidRef(f), sizeBlocks: sz })))
        },
    }

    action smbSetDisposition = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            nondet del = oneOf(Set(false, true))
            runM(one(o.session, o.tree,
                CSetDisposition({ fid: FidRef(f), del: del })))
        },
    }

    action smbSetRename = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            nondet nn = oneOf(NAMES)
            runM(one(o.session, o.tree,
                CSetRename({ fid: FidRef(f), newdir: ROOT, newname: nn })))
        },
    }

    action smbQuery = all {
        opens.size() > 0,
        {
            nondet f = oneOf(opens)
            val o = sdb.opens.get(f)
            runM(one(o.session, o.tree, CQueryBasic(FidRef(f))))
        },
    }

    // ------------------------------------------------------------------
    // Oplock / lease break ack (drains a break so a stalled op can retry)
    // ------------------------------------------------------------------

    action smbBreakAck = all {
        breakingOpens.size() > 0,
        {
            nondet f = oneOf(breakingOpens)
            val o = sdb.opens.get(f)
            runM(one(o.session, o.tree, CBreakAck(FidRef(f))))
        },
    }

    // ------------------------------------------------------------------
    // Compound (NextCommand chain): CREATE + WRITE + CLOSE, related so the
    // WRITE/CLOSE inherit the CREATE's FileId.  Exercises the related-op
    // FileId threading and first-error-stops.
    // ------------------------------------------------------------------

    action smbCompoundCreateWriteClose = all {
        trees.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        opens.size() < MAX_OPENS,
        {
            nondet t = oneOf(trees)
            nondet n = oneOf(NAMES)
            nondet pat = oneOf(BLOCK_SYMS)
            runM({ sess: treeSess(t), tree: t, related: true,
                   cmds: [
                       CCreate({ dir: ROOT, name: n, disp: DispOpenIf,
                                 access: ACCESS_RW, share: SHARE_RW,
                                 isDir: false, delOnClose: false,
                                 oplock: OrNone }),
                       CWrite({ fid: FidRelated, off: 0, len: 1, pat: pat }),
                       CClose(FidRelated) ] })
        },
    }

    // ------------------------------------------------------------------
    // Step distributions
    // ------------------------------------------------------------------

    action step = any {
        smbSessionSetup,
        smbTreeConnect,
        smbCreate,
        smbMkdir,
        smbClose,
        smbRead,
        smbWrite,
        smbFlush,
        smbLock,
        smbUnlock,
        smbSetEof,
        smbSetDisposition,
        smbSetRename,
        smbQuery,
        smbBreakAck,
        smbDeleteExisting,
        smbCompoundCreateWriteClose,
    }

    // Share/create-matrix heavy: CREATE arbitration, close, query.
    action stepShare = any {
        smbSessionSetup, smbTreeConnect,
        smbCreate, smbCreate, smbCreate, smbMkdir,
        smbClose, smbClose, smbQuery,
        smbSetDisposition, smbSetRename,
        smbDeleteExisting, smbDeleteExisting,
    }

    // Oplock/lease lifecycle: grants, cross-handle break, ack, retry.
    action stepLease = any {
        smbSessionSetup, smbTreeConnect,
        smbCreate, smbCreate, smbCreate,
        smbWrite, smbWrite,
        smbBreakAck, smbBreakAck, smbBreakAck,
        smbClose, smbClose,
        smbQuery,
    }

    // Data / lock heavy.
    action stepData = any {
        smbSessionSetup, smbTreeConnect,
        smbCreate, smbClose,
        smbRead, smbRead, smbWrite, smbWrite, smbFlush,
        smbLock, smbLock, smbUnlock, smbSetEof,
        smbQuery,
    }

    // Replayable core (file CREATE matrix + share arbitration + I/O +
    // delete-on-close + compounds): exactly the commands the in-process C
    // replayer builds on the wire today (CREATE/CLOSE/READ/WRITE), with no
    // directories, locks, set/query-info or oplock/lease breaks -- those
    // extend the replayer in a later pass.  smbCreate here is file-only.
    action stepIo = any {
        smbSessionSetup, smbTreeConnect,
        smbCreate, smbCreate, smbCreate,
        smbClose, smbClose,
        smbRead, smbWrite, smbWrite,
        smbDeleteExisting,
        smbCompoundCreateWriteClose,
    }

    // A CREATE that never requests delete-on-close (nor an oplock): the
    // deterministic CREATE disposition/type matrix + share arbitration alone.
    action smbCreateCore = all {
        trees.size() > 0,
        liveInos.size() < MAX_OBJECTS,
        opens.size() < MAX_OPENS,
        {
            nondet t = oneOf(trees)
            nondet n = oneOf(NAMES)
            nondet disp = oneOf(DISP_SET)
            nondet acc = oneOf(ACCESS_SET)
            nondet sh = oneOf(SHARE_SET)
            runM(one(treeSess(t), t,
                CCreate({ dir: ROOT, name: n, disp: disp, access: acc,
                          share: sh, isDir: false, delOnClose: false,
                          oplock: OrNone })))
        },
    }

    // The CREATE-matrix + share + I/O + compound core with NO delete-on-close.
    // Delete-on-close semantics (deferral vs. immediate removal when another
    // handle is open) are a coherency subtlety that a dedicated ground-truth
    // probe must pin before generated traces bake an expectation -- the same
    // pin-ground-truth-first discipline the oplock probe follows -- so they
    // are excluded here.
    action stepCore = any {
        smbSessionSetup, smbTreeConnect,
        smbCreateCore, smbCreateCore, smbCreateCore,
        smbClose, smbClose,
        smbRead, smbWrite, smbWrite,
        smbCompoundCreateWriteClose,
    }

    // ------------------------------------------------------------------
    // Invariants
    // ------------------------------------------------------------------

    // Structural fs + protocol-state invariants.
    val invFs = fsInv(sdb.fs)
    val invSt = stInv(sdb)

    // No caching state exists when both oplocks and leases are off.
    val capsRespected =
        (caps.oplocks or caps.leases) or
        opens.forall(f => cachingLevelNone(sdb.opens.get(f).caching))

    // A directory-lease-only cap (dirLeases without leases) never arises.
    val capsWellFormed = not(caps.dirLeases) or caps.leases

    val inv = invFs and invSt and capsRespected and capsWellFormed
}
