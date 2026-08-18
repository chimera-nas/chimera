// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// SMB2/3 command evaluator.
//
// `Cmd` describes one command of an SMB2 message (which may be a singleton
// or a NextCommand-chained compound); `Res` the reply the server must
// produce for it.  `evalMsg` interprets a command list with real SMB2
// compound semantics: related (SMB2_FLAGS_RELATED_OPERATIONS) commands
// inherit the FileId produced by the preceding CREATE and a failure aborts
// the remainder of the chain; unrelated commands are independent
// (MS-SMB2 3.3.5.2.7).  This is the direct analogue of nfs4_ops.evalCompound
// threading current/saved filehandle.
//
// The module is pure: the generator (smb2.qnt) owns the state variables and
// calls evalMsg from its actions; smb2Test.qnt drives it directly with
// hand-built messages.
//
// Conventions:
//  - FileIds, session ids and tree ids are model handles; the harness
//    learns the wire values (the opaque (persistent,volatile) FileId,
//    SessionId, TreeId) from replies (CREATE, SESSION_SETUP, TREE_CONNECT).
//  - Where MS-SMB2/MS-FSA leave a server choice (oplock/lease grant policy,
//    the OPEN-time lease-break asymmetry) the choice is pinned by the
//    capability/policy profile (`Caps`) and annotated `// discretion:`.
//  - Only the coherency-relevant NTSTATUS codes are modeled exactly
//    (SHARING_VIOLATION, OBJECT_NAME_COLLISION, DELETE_PENDING, ...);
//    info-class/name-validation fidelity is deviation material (DESIGN §5).

module smb2ops {

    import nfs4fs.* from "../../../nfs/tests/quint/nfs4_fs"
    import smb2state.* from "./smb2_state"

    // ------------------------------------------------------------------
    // NTSTATUS (MS-ERREF 2.3.1; numerically the wire values)
    // ------------------------------------------------------------------

    pure val ST_SUCCESS = 0x00000000
    pure val ST_PENDING = 0x00000103
    pure val ST_UNSUCCESSFUL = 0xC0000001
    pure val ST_INVALID_PARAMETER = 0xC000000D
    pure val ST_END_OF_FILE = 0xC0000011
    pure val ST_ACCESS_DENIED = 0xC0000022
    pure val ST_OBJECT_NAME_NOT_FOUND = 0xC0000034
    pure val ST_OBJECT_NAME_COLLISION = 0xC0000035
    pure val ST_OBJECT_PATH_NOT_FOUND = 0xC000003A
    pure val ST_SHARING_VIOLATION = 0xC0000043
    pure val ST_FILE_LOCK_CONFLICT = 0xC0000054
    pure val ST_LOCK_NOT_GRANTED = 0xC0000055
    pure val ST_DELETE_PENDING = 0xC0000056
    pure val ST_RANGE_NOT_LOCKED = 0xC000007E
    pure val ST_FILE_IS_A_DIRECTORY = 0xC00000BA
    pure val ST_NOT_SUPPORTED = 0xC00000BB
    pure val ST_NETWORK_NAME_DELETED = 0xC00000CB
    pure val ST_DIRECTORY_NOT_EMPTY = 0xC0000101
    pure val ST_NOT_A_DIRECTORY = 0xC0000103
    pure val ST_FILE_CLOSED = 0xC0000128
    pure val ST_USER_SESSION_DELETED = 0xC0000203

    // CreateAction (MS-SMB2 2.2.14).
    pure val ACT_SUPERSEDED = 0
    pure val ACT_OPENED = 1
    pure val ACT_CREATED = 2
    pure val ACT_OVERWRITTEN = 3

    // ------------------------------------------------------------------
    // Feature configuration and capability / policy profile
    // ------------------------------------------------------------------

    // Same operator-facing mode as the NFS model:
    //  - FeatOff: never exercised; traces contain no dependent steps.
    //  - FeatIfAdvertised: init nondeterministically assumes support; the
    //    corpus covers both, replay skips on mismatch.
    //  - FeatMandatory: fixed true; a server that fails to advertise fails
    //    the suite rather than passing vacuously.
    type FeatureMode = FeatOff | FeatIfAdvertised | FeatMandatory

    pure def capChoices(m: FeatureMode): Set[bool] =
        match m {
            | FeatOff => Set(false)
            | FeatIfAdvertised => Set(false, true)
            | FeatMandatory => Set(true)
        }

    type Caps = {
        // legacy oplocks advertised (set_smb_oplocks).
        oplocks: bool,
        // SMB2 leases (RqLs v1/v2) advertised (LEASING cap; set_smb_leases).
        leases: bool,
        // directory leases (set_smb_directory_leases).
        dirLeases: bool,
        // durable handles v1/v2 observed grantable.
        durable: bool,
        // discretion: does a conflicting CREATE break a peer's lease at OPEN
        // time (MS-FSA-correct = true), or does an SMB2 RqLs lease survive a
        // conflicting open and break only on the later I/O / namespace op
        // (the chimera Seam-A asymmetry, vfs_state.c ~L971)?  The pure model
        // pins the MS-correct value; the coherency probe (Increment 4)
        // records chimera's real answer, and a mismatch is a deviation.
        leaseOpenBreak: bool
    }

    // ------------------------------------------------------------------
    // Request / reply shapes
    // ------------------------------------------------------------------

    type Disposition =
        | DispSupersede
        | DispOpen
        | DispCreate
        | DispOpenIf
        | DispOverwrite
        | DispOverwriteIf

    // DesiredAccess reduced to the R/W/H(=handle/attr)/D(=delete) axes the
    // model reasons about.
    type DesiredAccess = { r: bool, w: bool, h: bool, d: bool }

    // ShareAccess as the three shared axes (R/W/D); the denied set is the
    // complement, computed at CREATE time.
    type ShareAccess = { r: bool, w: bool, d: bool }

    // Oplock/lease request in a CREATE.
    type OplockReq =
        | OrNone
        | OrLevelII
        | OrExclusive
        | OrBatch
        | OrLease({ key: LeaseKey, r: bool, w: bool, h: bool })

    // Which FileId a command targets: the related-compound current fid, or
    // an explicit learned fid.
    type FidSel = FidRelated | FidRef(FileId)

    type Cmd =
        | CSessionSetup({ clientSym: int })
        | CLogoff
        | CTreeConnect({ share: int })
        | CTreeDisconnect
        | CCreate({ dir: Ino, name: Name, disp: Disposition,
                    access: DesiredAccess, share: ShareAccess,
                    isDir: bool, delOnClose: bool, oplock: OplockReq })
        | CClose(FidSel)
        | CRead({ fid: FidSel, off: int, len: int })
        | CWrite({ fid: FidSel, off: int, len: int, pat: int })
        | CFlush(FidSel)
        | CLock({ fid: FidSel, lo: int, hi: int, wr: bool, unlock: bool,
                  failImmediately: bool })
        | CSetEof({ fid: FidSel, sizeBlocks: int })
        | CSetDisposition({ fid: FidSel, del: bool })
        | CSetRename({ fid: FidSel, newdir: Ino, newname: Name })
        | CQueryBasic(FidSel)
        | CBreakAck(FidSel)

    type AttrSnap = { ftype: Ftype, nlink: int, sizeBlocks: int,
                      change: int, deletePending: bool }

    type Res =
        | RSessionSetup({ st: int, sess: SessionId })
        | RLogoff({ st: int })
        | RTreeConnect({ st: int, tree: TreeId })
        | RTreeDisconnect({ st: int })
        | RCreate({ st: int, fid: FileId, act: int, ino: Ino,
                    caching: Caching, breaks: Set[FileId] })
        | RClose({ st: int, gone: Ino })
        | RRead({ st: int, count: int, eof: bool, blocks: List[int] })
        | RWrite({ st: int, count: int, breaks: Set[FileId] })
        | RFlush({ st: int })
        | RLock({ st: int })
        | RSetEof({ st: int })
        | RSetDisposition({ st: int })
        | RSetRename({ st: int, breaks: Set[FileId] })
        | RQueryBasic({ st: int, attrs: AttrSnap })
        | RBreakAck({ st: int })

    // ------------------------------------------------------------------
    // FsStatus -> NTSTATUS (MS-FSA 2.1.5.1 status mapping)
    // ------------------------------------------------------------------

    pure def fsStatusToNt(s: FsStatus): int =
        match s {
            | FsOk => ST_SUCCESS
            | FsNoent => ST_OBJECT_NAME_NOT_FOUND
            | FsExist => ST_OBJECT_NAME_COLLISION
            | FsNotdir => ST_NOT_A_DIRECTORY
            | FsIsdir => ST_FILE_IS_A_DIRECTORY
            | FsNotempty => ST_DIRECTORY_NOT_EMPTY
            | FsInval => ST_INVALID_PARAMETER
            | FsNoxattr => ST_NOT_SUPPORTED
        }

    // ------------------------------------------------------------------
    // Access / share helpers
    // ------------------------------------------------------------------

    pure def wantOf(a: DesiredAccess): Access = { r: a.r, w: a.w, d: a.d }

    // Denied set = complement of ShareAccess over R/W/D (MS-FSA: a bit not
    // in ShareAccess means concurrent access along that axis is denied).
    pure def denyOf(s: ShareAccess): Access =
        { r: not(s.r), w: not(s.w), d: not(s.d) }

    // A CREATE that only asks for attribute/handle access (no R/W/D) never
    // takes part in share arbitration (SMB2_SHAREMODE bypass).
    pure def attrOnly(a: DesiredAccess): bool =
        not(a.r) and not(a.w) and not(a.d)

    // ------------------------------------------------------------------
    // CREATE disposition / existence decision (MS-FSA 2.1.5.1)
    // ------------------------------------------------------------------

    // Resolve the disposition against existing state, producing the
    // filesystem effect and the CreateAction.  Returns st != SUCCESS on a
    // disposition/type error; otherwise ino names the target and the fs is
    // advanced (created / truncated / superseded) as required.
    //
    // SUPERSEDE is modeled as truncate-or-create keeping the CreateAction
    // FILE_SUPERSEDED (exact delete-recreate identity churn is deferred;
    // the coherency-relevant effect is the zero-length + action code).
    pure def decideCreate(fs: Fs, dir: Ino, name: Name, disp: Disposition,
                          isDir: bool):
            { fs2: Fs, st: int, ino: Ino, act: int } = {
        val dnode = fs.inodes.get(dir)
        if (dnode.ftype != FDir)
            { fs2: fs, st: ST_NOT_A_DIRECTORY, ino: -1, act: 0 }
        else {
            val ex = fsHasEnt(fs, dir, name)
            val ino = if (ex) fsChild(fs, dir, name) else -1
            val isdir = ex and fs.inodes.get(ino).ftype == FDir
            // type mismatch: a directory-create over a file (or vice versa),
            // or opening an existing object as the wrong type.
            val typeBad =
                ex and (if (isDir) not(isdir) else isdir)
            if (ex and typeBad)
                { fs2: fs, st: if (isDir) ST_NOT_A_DIRECTORY
                               else ST_FILE_IS_A_DIRECTORY,
                  ino: -1, act: 0 }
            else
                decideByDisp(fs, dir, name, disp, isDir, ex, ino, isdir)
        }
    }

    pure def mkNode(isDir: bool, mode: int, chgv: int): Inode =
        if (isDir) mkDir(mode, 0, chgv) else mkFile(mode, chgv)

    pure def decideByDisp(fs: Fs, dir: Ino, name: Name, disp: Disposition,
                          isDir: bool, ex: bool, ino: Ino, isdir: bool):
            { fs2: Fs, st: int, ino: Ino, act: int } = {
        // create a fresh object under (dir,name).
        val create = {
            val r = fsMknode(fs, dir, name, mkNode(isDir, 420, fs.chg))
            { fs2: r.fs2, st: fsStatusToNt(r.st), ino: r.ino,
              act: ACT_CREATED }
        }
        // open the existing object (no content change).
        val open = { fs2: fs, st: ST_SUCCESS, ino: ino, act: ACT_OPENED }
        // truncate the existing object to zero length; a directory cannot
        // be overwritten (MS-FSA: OVERWRITE/SUPERSEDE require a data stream).
        // Two flavors differing only in the CreateAction they report.
        val truncOver =
            if (isdir) { fs2: fs, st: ST_INVALID_PARAMETER, ino: -1, act: 0 }
            else { fs2: fsResize(fs, ino, 0), st: ST_SUCCESS, ino: ino,
                   act: ACT_OVERWRITTEN }
        val truncSuper =
            if (isdir) { fs2: fs, st: ST_INVALID_PARAMETER, ino: -1, act: 0 }
            else { fs2: fsResize(fs, ino, 0), st: ST_SUCCESS, ino: ino,
                   act: ACT_SUPERSEDED }
        match disp {
            | DispOpen =>
                if (ex) open
                else { fs2: fs, st: ST_OBJECT_NAME_NOT_FOUND, ino: -1,
                       act: 0 }
            | DispCreate =>
                if (ex) { fs2: fs, st: ST_OBJECT_NAME_COLLISION, ino: -1,
                              act: 0 }
                else create
            | DispOpenIf =>
                if (ex) open else create
            | DispOverwrite =>
                if (ex) truncOver
                else { fs2: fs, st: ST_OBJECT_NAME_NOT_FOUND, ino: -1,
                       act: 0 }
            | DispOverwriteIf =>
                if (ex) truncOver else create
            | DispSupersede =>
                if (ex) truncSuper else create
        }
    }

    // ------------------------------------------------------------------
    // Oplock / lease grant policy (discretion, pinned by Caps)
    // ------------------------------------------------------------------

    // Compute the caching grant for a freshly inserted handle `self` on file
    // f.  Exclusive/batch oplocks need sole access (no other open); a lease
    // computes RWH from access and coexisting grants (W only when sole);
    // level II is always grantable.  Gated by caps.oplocks / caps.leases.
    pure def grantCaching(sdb: StateDB, f: Ino, self: FileId,
                          oplock: OplockReq, caps: Caps): Caching = {
        val others = opensOfFile(sdb, f).exclude(Set(self))
        val sole = others.size() == 0
        match oplock {
            | OrNone => CNone
            | OrLevelII =>
                if (caps.oplocks) COplock(OpLevelII) else CNone
            | OrExclusive =>
                if (caps.oplocks and sole) COplock(OpExclusive)
                else if (caps.oplocks) COplock(OpLevelII)
                else CNone
            | OrBatch =>
                if (caps.oplocks and sole) COplock(OpBatch)
                else if (caps.oplocks) COplock(OpLevelII)
                else CNone
            | OrLease(x) =>
                if (not(caps.leases)) CNone
                else CLease({ key: x.key,
                              st: { r: x.r,
                                    // discretion: W caching only to a sole
                                    // opener; H (handle) caching survives
                                    // coexisting readers.
                                    w: x.w and sole,
                                    h: x.h },
                              epoch: 1 })
        }
    }

    // Whether opening file f with desired access `want` must break a peer's
    // caching grant.  writeOrNs is true when the opener wants write or
    // delete (forces a full break to NONE); a read-only conflicting open
    // downgrades exclusive/batch grants.  Leases are broken at OPEN time
    // only when caps.leaseOpenBreak (the Seam-A policy).
    pure def openBreakHolders(sdb: StateDB, f: Ino, want: Access,
                              caps: Caps): Set[FileId] = {
        val writeOrNs = want.w or want.d
        opensOfFile(sdb, f).filter(fid => {
            val o = sdb.opens.get(fid)
            not(cachingLevelNone(o.caching)) and
            match o.caching {
                | COplock(l) =>
                    writeOrNs or l == OpExclusive or l == OpBatch
                | CLease(x) =>
                    caps.leaseOpenBreak and (writeOrNs or x.st.w or x.st.h)
                | CNone => false
            }
        })
    }

    // ------------------------------------------------------------------
    // Command dispatch
    // ------------------------------------------------------------------

    type Acc = {
        sdb: StateDB,
        caps: Caps,
        related: bool,
        sess: SessionId,
        tree: TreeId,
        cfid: FileId,        // current related fid (last CREATE), -1 if none
        done: bool,
        results: List[Res]
    }

    pure def resolveFid(acc: Acc, sel: FidSel): FileId =
        match sel {
            | FidRelated => acc.cfid
            | FidRef(f) => f
        }

    pure def sessOk(acc: Acc): bool = acc.sdb.sessions.keys().contains(acc.sess)
    pure def treeOk(acc: Acc): bool = acc.sdb.trees.keys().contains(acc.tree)

    // Append a result and, for a related compound, stop on error (SMB2
    // first-error-stops; PENDING and SUCCESS continue).
    pure def emit(acc: Acc, r: Res, st: int): Acc =
        acc.with("results", acc.results.append(r))
           .with("done", acc.related and st != ST_SUCCESS and st != ST_PENDING)

    pure def stOf(r: Res): int =
        match r {
            | RSessionSetup(x) => x.st
            | RLogoff(x) => x.st
            | RTreeConnect(x) => x.st
            | RTreeDisconnect(x) => x.st
            | RCreate(x) => x.st
            | RClose(x) => x.st
            | RRead(x) => x.st
            | RWrite(x) => x.st
            | RFlush(x) => x.st
            | RLock(x) => x.st
            | RSetEof(x) => x.st
            | RSetDisposition(x) => x.st
            | RSetRename(x) => x.st
            | RQueryBasic(x) => x.st
            | RBreakAck(x) => x.st
        }

    pure def dispatch(acc: Acc, c: Cmd): Acc =
        match c {
            | CSessionSetup(x) => doCmdSessionSetup(acc, x.clientSym)
            | CLogoff => doCmdLogoff(acc)
            | CTreeConnect(x) => doCmdTreeConnect(acc, x.share)
            | CTreeDisconnect => doCmdTreeDisconnect(acc)
            | CCreate(x) => doCmdCreate(acc, x)
            | CClose(sel) => doCmdClose(acc, sel)
            | CRead(x) => doCmdRead(acc, x)
            | CWrite(x) => doCmdWrite(acc, x)
            | CFlush(sel) => doCmdFlush(acc, sel)
            | CLock(x) => doCmdLock(acc, x)
            | CSetEof(x) => doCmdSetEof(acc, x)
            | CSetDisposition(x) => doCmdSetDisposition(acc, x)
            | CSetRename(x) => doCmdSetRename(acc, x)
            | CQueryBasic(sel) => doCmdQueryBasic(acc, sel)
            | CBreakAck(sel) => doCmdBreakAck(acc, sel)
        }

    // --- session / tree ----------------------------------------------

    pure def doCmdSessionSetup(acc: Acc, clientSym: int): Acc = {
        val r = doSessionSetup(acc.sdb, clientSym)
        // a new session becomes the message's active session for the rest
        // of a related chain.
        emit(acc.with("sdb", r.sdb2).with("sess", r.sess),
             RSessionSetup({ st: ST_SUCCESS, sess: r.sess }), ST_SUCCESS)
    }

    pure def doCmdLogoff(acc: Acc): Acc =
        if (not(sessOk(acc)))
            emit(acc, RLogoff({ st: ST_USER_SESSION_DELETED }),
                 ST_USER_SESSION_DELETED)
        else
            emit(acc.with("sdb", doLogoff(acc.sdb, acc.sess)),
                 RLogoff({ st: ST_SUCCESS }), ST_SUCCESS)

    pure def doCmdTreeConnect(acc: Acc, share: int): Acc =
        if (not(sessOk(acc)))
            emit(acc, RTreeConnect({ st: ST_USER_SESSION_DELETED, tree: -1 }),
                 ST_USER_SESSION_DELETED)
        else {
            val r = doTreeConnect(acc.sdb, acc.sess, share)
            emit(acc.with("sdb", r.sdb2).with("tree", r.tree),
                 RTreeConnect({ st: ST_SUCCESS, tree: r.tree }), ST_SUCCESS)
        }

    pure def doCmdTreeDisconnect(acc: Acc): Acc =
        if (not(treeOk(acc)))
            emit(acc, RTreeDisconnect({ st: ST_NETWORK_NAME_DELETED }),
                 ST_NETWORK_NAME_DELETED)
        else
            emit(acc.with("sdb", doTreeDisconnect(acc.sdb, acc.tree)),
                 RTreeDisconnect({ st: ST_SUCCESS }), ST_SUCCESS)

    // --- CREATE -------------------------------------------------------

    pure def createErr(acc: Acc, st: int): Acc =
        emit(acc, RCreate({ st: st, fid: -1, act: 0, ino: -1,
                            caching: CNone, breaks: Set() }), st)

    pure def doCmdCreate(acc, x) =
        if (not(sessOk(acc))) createErr(acc, ST_USER_SESSION_DELETED)
        else if (not(treeOk(acc))) createErr(acc, ST_NETWORK_NAME_DELETED)
        // FILE_DELETE_ON_CLOSE requires DELETE access in DesiredAccess
        // (MS-FSA 2.1.5.1: an open that sets delete-on-close without DELETE
        // access is failed with STATUS_ACCESS_DENIED, checked before name
        // resolution).
        else if (x.delOnClose and not(x.access.d))
            createErr(acc, ST_ACCESS_DENIED)
        else {
            val dec = decideCreate(acc.sdb.fs, x.dir, x.name, x.disp, x.isDir)
            if (dec.st != ST_SUCCESS) createErr(acc, dec.st)
            // delete pending on the target -> DELETE_PENDING (only for an
            // existing object being opened; a fresh create has a new ino).
            else if (acc.sdb.deletePending.contains(dec.ino) and
                     dec.act == ACT_OPENED)
                createErr(acc, ST_DELETE_PENDING)
            else {
                val want = wantOf(x.access)
                val deny = denyOf(x.share)
                val sdb1 = acc.sdb.with("fs", dec.fs2)
                // 1. oplock/lease break: a conflicting open recalls peer
                //    caching grants; the open stalls (PENDING) until drain.
                val holders =
                    if (attrOnly(x.access)) Set()
                    else openBreakHolders(sdb1, dec.ino, want, acc.caps)
                if (holders.size() > 0) {
                    val sdb2 = doBreakAll(sdb1, dec.ino, -1, want.w or want.d)
                    emit(acc.with("sdb", sdb2),
                         RCreate({ st: ST_PENDING, fid: -1, act: 0,
                                   ino: dec.ino, caching: CNone,
                                   breaks: holders }), ST_PENDING)
                }
                else if (not(attrOnly(x.access)) and
                         breakInProgress(sdb1, dec.ino, -1))
                    // a peer break is still draining -> keep stalling.
                    emit(acc.with("sdb", sdb1),
                         RCreate({ st: ST_PENDING, fid: -1, act: 0,
                                   ino: dec.ino, caching: CNone,
                                   breaks: Set() }), ST_PENDING)
                // 2. share reservation check (MS-FSA SharingViolation).
                else if (not(attrOnly(x.access)) and
                         shareConflict(sdb1, dec.ino, want, deny))
                    createErr(acc.with("sdb", sdb1), ST_SHARING_VIOLATION)
                else {
                    // 3. insert the open and grant caching.
                    val ins = doInsertOpen(sdb1, acc.sess, acc.tree, dec.ino,
                        x.dir, x.name,
                        { r: x.access.r, w: x.access.w, h: x.access.h,
                          d: x.access.d },
                        deny, x.delOnClose)
                    val gr = grantCaching(ins.sdb2, dec.ino, ins.fid,
                                          x.oplock, acc.caps)
                    val sdb3 = doSetCaching(ins.sdb2, ins.fid, gr)
                    val sdb4 = if (x.delOnClose)
                                   sdb3.with("deletePending",
                                             sdb3.deletePending.union(Set(dec.ino)))
                               else sdb3
                    emit(acc.with("sdb", sdb4).with("cfid", ins.fid),
                         RCreate({ st: ST_SUCCESS, fid: ins.fid,
                                   act: dec.act, ino: dec.ino,
                                   caching: gr, breaks: Set() }), ST_SUCCESS)
                }
            }
        }

    // --- fid-targeted ops --------------------------------------------

    pure def fidBad(acc: Acc): bool =
        not(sessOk(acc)) or not(treeOk(acc))

    pure def doCmdClose(acc: Acc, sel: FidSel): Acc = {
        val fid = resolveFid(acc, sel)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RClose({ st: ST_FILE_CLOSED, gone: -1 }), ST_FILE_CLOSED)
        else {
            val r = doClose(acc.sdb, fid)
            emit(acc.with("sdb", r.sdb2),
                 RClose({ st: ST_SUCCESS, gone: r.gone }), ST_SUCCESS)
        }
    }

    pure def doCmdRead(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RRead({ st: ST_FILE_CLOSED, count: -1, eof: false,
                              blocks: [] }), ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            if (not(o.granted.r))
                emit(acc, RRead({ st: ST_ACCESS_DENIED, count: -1,
                                  eof: false, blocks: [] }), ST_ACCESS_DENIED)
            // mandatory byte-range lock enforcement (MS-FSA 2.1.4.2).
            else if (rangeIoConflict(acc.sdb, o.file, fid, x.off, x.len, false))
                emit(acc, RRead({ st: ST_FILE_LOCK_CONFLICT, count: -1,
                                  eof: false, blocks: [] }),
                     ST_FILE_LOCK_CONFLICT)
            else {
                val nd = acc.sdb.fs.inodes.get(o.file)
                val rd = fsReadData(nd, x.off, x.len)
                // a read wholly at/after EOF returns STATUS_END_OF_FILE.
                if (x.off >= nd.sizeBlocks and x.len > 0)
                    emit(acc, RRead({ st: ST_END_OF_FILE, count: 0,
                                      eof: true, blocks: [] }), ST_END_OF_FILE)
                else
                    emit(acc, RRead({ st: ST_SUCCESS, count: rd.count,
                                      eof: rd.eof, blocks: rd.syms }),
                         ST_SUCCESS)
            }
        }
    }

    pure def doCmdWrite(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RWrite({ st: ST_FILE_CLOSED, count: -1, breaks: Set() }),
                 ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            if (not(o.granted.w))
                emit(acc, RWrite({ st: ST_ACCESS_DENIED, count: -1,
                                   breaks: Set() }), ST_ACCESS_DENIED)
            else if (rangeIoConflict(acc.sdb, o.file, fid, x.off, x.len, true))
                emit(acc, RWrite({ st: ST_FILE_LOCK_CONFLICT, count: -1,
                                   breaks: Set() }), ST_FILE_LOCK_CONFLICT)
            else {
                // break_reads_for_write: this write recalls every OTHER
                // handle's read cache to NONE (protocol-agnostic, Seam 4a).
                // The write itself completes -- the D4-12 ordering hazard
                // (write lands before the recall drains) applies here too.
                val holders = breakHolders(acc.sdb, o.file, fid, true)
                val sdb1 = doBreakAll(acc.sdb, o.file, fid, true)
                val wr = fsWrite(sdb1.fs, o.file, x.off, x.len, x.pat)
                emit(acc.with("sdb", sdb1.with("fs", wr.fs2)),
                     RWrite({ st: ST_SUCCESS, count: x.len, breaks: holders }),
                     ST_SUCCESS)
            }
        }
    }

    pure def doCmdFlush(acc: Acc, sel: FidSel): Acc = {
        val fid = resolveFid(acc, sel)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RFlush({ st: ST_FILE_CLOSED }), ST_FILE_CLOSED)
        else
            emit(acc, RFlush({ st: ST_SUCCESS }), ST_SUCCESS)
    }

    pure def doCmdLock(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RLock({ st: ST_FILE_CLOSED }), ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            if (x.unlock)
                if (not(rangeLockedBy(acc.sdb, o.file, fid, x.lo, x.hi)))
                    emit(acc, RLock({ st: ST_RANGE_NOT_LOCKED }),
                         ST_RANGE_NOT_LOCKED)
                else
                    emit(acc.with("sdb", doUnlock(acc.sdb, o.file, fid,
                                                  x.lo, x.hi)),
                         RLock({ st: ST_SUCCESS }), ST_SUCCESS)
            else if (lockConflict(acc.sdb, o.file, fid, x.lo, x.hi, x.wr))
                // FAIL_IMMEDIATELY -> LOCK_NOT_GRANTED; a blocking lock that
                // cannot be granted synchronously is modeled the same way
                // (async blocking-grant observability is deferred, DESIGN
                // "deliberately deferred").
                emit(acc, RLock({ st: if (x.failImmediately) ST_LOCK_NOT_GRANTED
                                       else ST_LOCK_NOT_GRANTED }),
                     ST_LOCK_NOT_GRANTED)
            else
                emit(acc.with("sdb", doLock(acc.sdb, o.file, fid,
                                            x.lo, x.hi, x.wr)),
                     RLock({ st: ST_SUCCESS }), ST_SUCCESS)
        }
    }

    pure def doCmdSetEof(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RSetEof({ st: ST_FILE_CLOSED }), ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            if (not(o.granted.w))
                emit(acc, RSetEof({ st: ST_ACCESS_DENIED }), ST_ACCESS_DENIED)
            else
                emit(acc.with("sdb",
                         acc.sdb.with("fs", fsResize(acc.sdb.fs, o.file,
                                                     x.sizeBlocks))),
                     RSetEof({ st: ST_SUCCESS }), ST_SUCCESS)
        }
    }

    pure def doCmdSetDisposition(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RSetDisposition({ st: ST_FILE_CLOSED }), ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            // a directory must be empty to set delete-on-close.
            if (x.del and fsGet(acc.sdb.fs, o.file).ftype == FDir and
                fsGet(acc.sdb.fs, o.file).ents.keys().size() > 0)
                emit(acc, RSetDisposition({ st: ST_DIRECTORY_NOT_EMPTY }),
                     ST_DIRECTORY_NOT_EMPTY)
            else
                emit(acc.with("sdb", doSetDisposition(acc.sdb, fid, x.del)),
                     RSetDisposition({ st: ST_SUCCESS }), ST_SUCCESS)
        }
    }

    pure def doCmdSetRename(acc, x) = {
        val fid = resolveFid(acc, x.fid)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RSetRename({ st: ST_FILE_CLOSED, breaks: Set() }),
                 ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            // SetFileRenameInformation with ReplaceIfExists = false
            // (MS-FSA 2.1.5.14.11): a rename onto an existing different name
            // fails with OBJECT_NAME_COLLISION.  Modeling the no-replace case
            // keeps the target's identity/handles intact -- replacing a
            // still-open directory target is exactly what the fs core cannot
            // represent, and MS rename does not replace directories anyway.
            if (fsHasEnt(acc.sdb.fs, x.newdir, x.newname) and
                fsChild(acc.sdb.fs, x.newdir, x.newname) != o.file)
                emit(acc, RSetRename({ st: ST_OBJECT_NAME_COLLISION,
                                       breaks: Set() }),
                     ST_OBJECT_NAME_COLLISION)
            else {
            // namespace mutation recalls every caching holder (Seam 4b).
            val holders = breakHolders(acc.sdb, o.file, fid, true)
            val sdb1 = doBreakAll(acc.sdb, o.file, fid, true)
            // target is absent or the same file, so fsRename never replaces.
            val rn = fsRename(sdb1.fs, o.dir, o.name, x.newdir, x.newname,
                              pinnedFiles(sdb1))
            if (rn.st != FsOk)
                emit(acc, RSetRename({ st: fsStatusToNt(rn.st),
                                       breaks: Set() }), fsStatusToNt(rn.st))
            else {
                // Rename is by handle (FileId), not by path: EVERY open of
                // this inode now names (newdir,newname).  Syncing all handles
                // keeps o.dir/o.name current so a later rename or
                // delete-on-close through any handle targets the right name
                // (a stale name would otherwise operate on whatever object
                // currently sits at the old path).
                val sdb2 = sdb1.with("fs", rn.fs2)
                    .with("opens", sdb1.opens.keys().mapBy(k => {
                        val oo = sdb1.opens.get(k)
                        if (oo.file == o.file)
                            oo.with("dir", x.newdir).with("name", x.newname)
                        else oo
                    }))
                emit(acc.with("sdb", sdb2),
                     RSetRename({ st: ST_SUCCESS, breaks: holders }),
                     ST_SUCCESS)
            }
            }
        }
    }

    pure def doCmdQueryBasic(acc: Acc, sel: FidSel): Acc = {
        val fid = resolveFid(acc, sel)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RQueryBasic({ st: ST_FILE_CLOSED,
                                    attrs: { ftype: FReg, nlink: 0,
                                             sizeBlocks: 0, change: 0,
                                             deletePending: false } }),
                 ST_FILE_CLOSED)
        else {
            val o = acc.sdb.opens.get(fid)
            val nd = fsGet(acc.sdb.fs, o.file)
            emit(acc, RQueryBasic({ st: ST_SUCCESS,
                                    attrs: { ftype: nd.ftype, nlink: nd.nlink,
                                             sizeBlocks: nd.sizeBlocks,
                                             change: nd.change,
                                             deletePending:
                                                 acc.sdb.deletePending
                                                    .contains(o.file) } }),
                 ST_SUCCESS)
        }
    }

    // --- oplock / lease break ack ------------------------------------

    pure def doCmdBreakAck(acc: Acc, sel: FidSel): Acc = {
        val fid = resolveFid(acc, sel)
        if (not(acc.sdb.opens.keys().contains(fid)))
            emit(acc, RBreakAck({ st: ST_FILE_CLOSED }), ST_FILE_CLOSED)
        // idempotency: an ack on a grant that is not mid-break is an error
        // (MS-SMB2 3.3.5.22.1: the 3 xfail "ack on already-settled break"
        // rows -> STATUS_UNSUCCESSFUL).
        else if (not(acc.sdb.opens.get(fid).breaking))
            emit(acc, RBreakAck({ st: ST_UNSUCCESSFUL }), ST_UNSUCCESSFUL)
        else
            emit(acc.with("sdb", doAckBreak(acc.sdb, fid)),
                 RBreakAck({ st: ST_SUCCESS }), ST_SUCCESS)
    }

    // ------------------------------------------------------------------
    // Message (compound) evaluator
    // ------------------------------------------------------------------

    type Msg = { sess: SessionId, tree: TreeId, related: bool, cmds: List[Cmd] }

    type MsgOutcome = { sdb2: StateDB, results: List[Res] }

    pure def evalMsg(sdb: StateDB, caps: Caps, msg: Msg): MsgOutcome = {
        val a0: Acc = {
            sdb: sdb, caps: caps, related: msg.related,
            sess: msg.sess, tree: msg.tree, cfid: -1,
            done: false, results: []
        }
        val aN = msg.cmds.foldl(a0, (acc, c) =>
            if (acc.done) acc else dispatch(acc, c))
        { sdb2: aN.sdb, results: aN.results }
    }
}
