// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// SMB2/3 protocol state model: sessions, tree connects, opens (handles),
// share reservations, the unified oplock/lease caching grant, byte-range
// locks and delete-on-close.
//
// Like nfs4_state.qnt this module is pure: every function maps a StateDB
// (plus arguments) to a new StateDB and an abstract outcome; the compound
// evaluator in smb2_ops.qnt maps outcomes to NTSTATUS codes and wire
// shapes.  Keeping it pure lets smb2Test.qnt unit-test the share-conflict
// and oplock/lease-break rules without a model in the loop.
//
// The filesystem core is the SAME module the NFSv4 model uses
// (nfs4_fs.qnt): SMB share modes, oplocks/leases and byte-range locks are
// `protocol = SMB2` entries in the very state object NFSv4 OPEN share/deny,
// delegations and NLM locks drive (survey Seam 1/2).  That shared substrate
// is the whole reason the SMB model lives beside the NFS one.
//
// Conventions:
//  - Model handles (SessionId, TreeId, FileId, LeaseKey) are small ints;
//    the harness learns the wire values (SessionId, TreeId, the opaque
//    (persistent,volatile) FileId, the 16-byte lease key) from replies.
//  - Share access is stored as the DENIED set (the complement of SMB2
//    ShareAccess), so `shareConflict` is symmetric with the NFSv4 deny
//    check: a proposed access intersecting an existing open's deny, or a
//    proposed deny intersecting an existing open's access, is a violation.
//  - Byte-range locks use a per-byte map over a small lock space, exactly
//    as nfs4state.LockState.bytes, so overlap/exact-unlock are exact by
//    construction.  SMB locks are mandatory (enforced against READ/WRITE);
//    the `mandatory` flag records that, the PROPERTY-3c adjudication point.

module smb2state {

    import nfs4fs.* from "../../../nfs/tests/quint/nfs4_fs"

    type SessionId = int
    type TreeId = int
    type FileId = int
    type LeaseKey = int

    // ------------------------------------------------------------------
    // Caching grants (oplocks + leases, unified)
    // ------------------------------------------------------------------

    // Legacy oplock levels (MS-SMB2 2.2.14 SMB2_OPLOCK_LEVEL_*).
    type OplockLevel = OpNone | OpLevelII | OpExclusive | OpBatch

    // RWH lease caching bits (MS-SMB2 2.2.13 lease state):
    //   r = SMB2_LEASE_READ_CACHING, w = WRITE_CACHING, h = HANDLE_CACHING.
    type LeaseState = { r: bool, w: bool, h: bool }

    // One caching grant.  Legacy oplocks and RqLs leases are ONE list in
    // the VFS (caching_leases); the model unifies them as a caching grant
    // with a level, exactly as nfs4state models a delegation.  A keyless
    // COplock carries a level; a keyed CLease carries RWH bits + epoch.
    type Caching =
        | CNone
        | COplock(OplockLevel)
        | CLease({ key: LeaseKey, st: LeaseState, epoch: int })

    // ------------------------------------------------------------------
    // Sessions, trees, opens
    // ------------------------------------------------------------------

    // ~ nfs4state Client.  Auth is a null/guest path (crypto out of scope);
    // the model exercises session lifecycle that matters for handle
    // ownership: new / logoff.
    type Session = { client: int, valid: bool }

    type Tree = { session: SessionId, share: int }

    // R/W/D access triple used for share arbitration.  H (handle caching)
    // and the full DesiredAccess mask are not conflict-bearing, so the
    // conflict axes are exactly read/write/delete.
    type Access = { r: bool, w: bool, d: bool }

    // One open handle.  The SMB analogue of nfs4state.OpenState, keyed by an
    // opaque FileId rather than an (owner,file) stateid, and carrying the
    // SMB-only bits (H caching, delete share, delete-on-close, the caching
    // grant this handle holds and its break progress).
    type Open = {
        session: SessionId,
        tree: TreeId,
        file: Ino,
        // the (dir,name) this handle was opened under, so delete-on-close
        // can unlink the exact name (the harness's Ino->path binding).
        dir: Ino,
        name: Name,
        // granted DesiredAccess as R/W/H/D bits; denied as the complement
        // of ShareAccess over R/W/D.  Both land in one VFS SHARE entry.
        granted: { r: bool, w: bool, h: bool, d: bool },
        denied: Access,
        caching: Caching,
        // a break notification was sent for this grant and not yet acked;
        // breakTo is the level the grant settles to once the holder acks
        // (the SMB analogue of nfs4state.Deleg.recalled + target level).
        breaking: bool,
        breakTo: Caching,
        deleteOnClose: bool
    }

    type LockT = LRead | LWrite

    type StateDB = {
        fs: Fs,
        sessions: SessionId -> Session,
        trees: TreeId -> Tree,
        opens: FileId -> Open,
        nextSess: int,
        nextTree: int,
        nextFile: int,
        // per-file byte-range locks: (Ino, byte) -> lock type.  Mirrors
        // nfs4state.LockState.bytes but pooled per file (SMB locks are
        // owned by the handle; the model keys by (file,byte) and records
        // the owning FileId so exact-range unlock is per handle).
        rangeLocks: (Ino, int) -> { t: LockT, owner: FileId },
        // inodes with a delete pending (a delete-on-close handle exists or
        // SetDisposition(delete) was issued): a new CREATE observing this
        // returns STATUS_DELETE_PENDING (MS-FSA 2.1.5.1.1).
        deletePending: Set[Ino]
    }

    pure def stInit(rootMode: int): StateDB = {
        fs: fsInit(rootMode),
        sessions: Map(),
        trees: Map(),
        opens: Map(),
        nextSess: 0,
        nextTree: 0,
        nextFile: 0,
        rangeLocks: Map(),
        deletePending: Set()
    }

    pure def smapDrop(m, k) = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    // ------------------------------------------------------------------
    // Session / tree lifecycle
    // ------------------------------------------------------------------

    // SESSION_SETUP: establish a session for a client symbol (MS-SMB2
    // 3.3.5.5; the null/guest auth path).  Returns the new session id.
    pure def doSessionSetup(sdb: StateDB, clientSym: int):
            { sdb2: StateDB, sess: SessionId } = {
        val s = sdb.nextSess
        { sdb2: sdb
              .with("sessions", sdb.sessions.put(s, { client: clientSym,
                                                      valid: true }))
              .with("nextSess", s + 1),
          sess: s }
    }

    // LOGOFF (MS-SMB2 3.3.5.6): the session's trees and opens go away; the
    // handles' caching grants and locks are released with them.
    pure def doLogoff(sdb: StateDB, sess: SessionId): StateDB = {
        val deadTrees = sdb.trees.keys().filter(t =>
            sdb.trees.get(t).session == sess)
        val deadOpens = sdb.opens.keys().filter(f =>
            sdb.opens.get(f).session == sess)
        deadOpens.fold(sdb, (acc, f) => doCloseRaw(acc, f))
           .with("trees", sdb.trees.keys().exclude(deadTrees)
                 .mapBy(t => sdb.trees.get(t)))
           .with("sessions", smapDrop(sdb.sessions, sess))
    }

    // TREE_CONNECT (MS-SMB2 3.3.5.7): bind a share to a session.
    pure def doTreeConnect(sdb: StateDB, sess: SessionId, share: int):
            { sdb2: StateDB, tree: TreeId } = {
        val t = sdb.nextTree
        { sdb2: sdb
              .with("trees", sdb.trees.put(t, { session: sess, share: share }))
              .with("nextTree", t + 1),
          tree: t }
    }

    pure def doTreeDisconnect(sdb: StateDB, tree: TreeId): StateDB = {
        val deadOpens = sdb.opens.keys().filter(f =>
            sdb.opens.get(f).tree == tree)
        deadOpens.fold(sdb, (acc, f) => doCloseRaw(acc, f))
           .with("trees", smapDrop(sdb.trees, tree))
    }

    // ------------------------------------------------------------------
    // Share reservations (MS-FSA 2.1.4.1 SharingViolation)
    // ------------------------------------------------------------------

    pure def accItsc(a: Access, b: Access): bool =
        (a.r and b.r) or (a.w and b.w) or (a.d and b.d)

    // Granted DesiredAccess projected onto the conflict axes.
    pure def grantedAcc(o: Open): Access =
        { r: o.granted.r, w: o.granted.w, d: o.granted.d }

    // A proposed open (desired access `want`, denied set `deny`) conflicts
    // with an existing open iff its access intersects that open's deny, or
    // its deny intersects that open's access (MS-FSA 2.1.4.1; the symmetric
    // share-reservation check, identical in shape to NFSv4 shareConflict).
    // Attribute-only opens (no r/w/d) never conflict.
    pure def shareConflict(sdb: StateDB, f: Ino, want: Access,
                           deny: Access): bool =
        sdb.opens.keys().exists(fid => {
            val o = sdb.opens.get(fid)
            o.file == f and
            (accItsc(want, o.denied) or accItsc(deny, grantedAcc(o)))
        })

    // The set of opens of file f (any session).
    pure def opensOfFile(sdb: StateDB, f: Ino): Set[FileId] =
        sdb.opens.keys().filter(fid => sdb.opens.get(fid).file == f)

    // Every inode currently pinned by an open handle.  A namespace op that
    // would unlink such an inode orphans it instead (unlink-while-open),
    // exactly as NFSv4 pins open files against REMOVE/RENAME.
    pure def pinnedFiles(sdb: StateDB): Set[Ino] =
        sdb.opens.keys().map(fid => sdb.opens.get(fid).file)

    // ------------------------------------------------------------------
    // Opens
    // ------------------------------------------------------------------

    // Install a new open handle over file f (the share check has already
    // passed).  Returns the fresh FileId.  Caching is set separately by the
    // grant step so the CREATE handler controls oplock/lease policy.
    pure def doInsertOpen(sdb: StateDB, sess: SessionId, tree: TreeId,
                          f: Ino, dir: Ino, name: Name,
                          granted: { r: bool, w: bool, h: bool, d: bool },
                          deny: Access, delOnClose: bool):
            { sdb2: StateDB, fid: FileId } = {
        val fid = sdb.nextFile
        { sdb2: sdb
              .with("opens", sdb.opens.put(fid, {
                  session: sess, tree: tree, file: f, dir: dir, name: name,
                  granted: granted, denied: deny, caching: CNone,
                  breaking: false, breakTo: CNone,
                  deleteOnClose: delOnClose }))
              .with("nextFile", fid + 1),
          fid: fid }
    }

    // Drop an open and everything pinned to it: its byte-range locks and,
    // if it was the last opener, a delete-pending reap.  Does NOT run the
    // delete-on-close unlink -- doClose does that first, then calls this.
    pure def doCloseRaw(sdb: StateDB, fid: FileId): StateDB = {
        val keptLocks = sdb.rangeLocks.keys()
            .filter(k => sdb.rangeLocks.get(k).owner != fid)
            .mapBy(k => sdb.rangeLocks.get(k))
        sdb.with("opens", smapDrop(sdb.opens, fid))
           .with("rangeLocks", keptLocks)
    }

    // CLOSE (MS-SMB2 3.3.5.10 + MS-FSA 2.1.5.4): release the handle.  SMB
    // delete-on-close deletes at the LAST close when the file is
    // delete-pending (MS-FSA 2.1.5.4: the directory entry persists until the
    // final handle drops), so no pinned-orphan intermediate arises -- unlike
    // NFSv4 REMOVE-of-open which orphans immediately.  The one orphan source
    // is a RENAME that replaced a still-open target (fsRename orphans it);
    // that nlink-0 inode is reaped when its own last handle closes.
    pure def doClose(sdb: StateDB, fid: FileId):
            { sdb2: StateDB, gone: Ino } = {
        val o = sdb.opens.get(fid)
        val isLast = opensOfFile(sdb, o.file).exclude(Set(fid)).size() == 0
        if (isLast and sdb.deletePending.contains(o.file) and
            fsHasEnt(sdb.fs, o.dir, o.name) and
            fsChild(sdb.fs, o.dir, o.name) == o.file) {
            // last close of a delete-pending file: unlink now (no pins left).
            val rm = fsRemove(sdb.fs, o.dir, o.name, Set())
            val sdb1 = sdb.with("fs", rm.fs2)
                          .with("deletePending",
                                sdb.deletePending.exclude(Set(o.file)))
            { sdb2: doCloseRaw(sdb1, fid), gone: rm.gone }
        } else if (isLast and fsLive(sdb.fs, o.file) and
                   fsGet(sdb.fs, o.file).nlink == 0) {
            // reap a rename-orphaned inode once its last handle drops.
            val sdb1 = sdb.with("fs", fsReap(sdb.fs, o.file))
                          .with("deletePending",
                                sdb.deletePending.exclude(Set(o.file)))
            { sdb2: doCloseRaw(sdb1, fid), gone: o.file }
        } else {
            { sdb2: doCloseRaw(sdb, fid), gone: -1 }
        }
    }

    // SetDisposition (MS-FSA 2.1.5.14.2 SetFileDispositionInformation):
    // arm or disarm delete-on-close on a handle and mark/unmark the inode
    // delete-pending.
    pure def doSetDisposition(sdb: StateDB, fid: FileId, del: bool): StateDB = {
        val o = sdb.opens.get(fid)
        sdb.with("opens", sdb.opens.setBy(fid, x =>
                x.with("deleteOnClose", del)))
           .with("deletePending",
                 if (del) sdb.deletePending.union(Set(o.file))
                 else sdb.deletePending.exclude(Set(o.file)))
    }

    // ------------------------------------------------------------------
    // Oplock / lease caching grants and the break lifecycle
    // ------------------------------------------------------------------

    // Set (or replace) the caching grant on a handle.
    pure def doSetCaching(sdb: StateDB, fid: FileId, c: Caching): StateDB =
        sdb.with("opens", sdb.opens.setBy(fid, o => o.with("caching", c)))

    pure def cachingLevelNone(c: Caching): bool =
        match c {
            | CNone => true
            | COplock(l) => l == OpNone
            | CLease(x) => not(x.st.r) and not(x.st.w) and not(x.st.h)
        }

    // Compute the level a grant settles to when broken.  toNone forces a
    // full break (write / namespace conflict, break_reads_for_write);
    // otherwise the grant downgrades one notch (a read-compatible open
    // conflict): batch/exclusive -> level II, and a lease drops W then H
    // then R, one bit per break (MS-SMB2 3.3.4.7 cascading RWH->RH->R->NONE).
    pure def breakTarget(c: Caching, toNone: bool): Caching =
        match c {
            | CNone => CNone
            | COplock(l) =>
                if (toNone) COplock(OpNone)
                else COplock(OpLevelII)
            | CLease(x) =>
                if (toNone) CLease({ key: x.key,
                                     st: { r: false, w: false, h: false },
                                     epoch: x.epoch + 1 })
                else if (x.st.w) CLease({ key: x.key,
                                          st: { r: x.st.r, w: false, h: x.st.h },
                                          epoch: x.epoch + 1 })
                else if (x.st.h) CLease({ key: x.key,
                                          st: { r: x.st.r, w: false, h: false },
                                          epoch: x.epoch + 1 })
                else CLease({ key: x.key,
                              st: { r: false, w: false, h: false },
                              epoch: x.epoch + 1 })
        }

    // Begin a break on the handle's grant (send the notification): record
    // the target level and mark the grant breaking.  Idempotent if already
    // breaking toward the same-or-lower level.
    pure def doBeginBreak(sdb: StateDB, fid: FileId, toNone: bool): StateDB = {
        val o = sdb.opens.get(fid)
        sdb.with("opens", sdb.opens.setBy(fid, x => x
              .with("breaking", true)
              .with("breakTo", breakTarget(x.caching, toNone))))
    }

    // Handles other than `self` that hold a caching grant on file f which a
    // new access must break.  writeOrNs=true means a write or namespace
    // conflict (breaks every read cache to NONE); false means a
    // read-compatible open conflict (downgrades exclusive grants).  A grant
    // already fully broken (CNone) needs no break.
    pure def breakHolders(sdb: StateDB, f: Ino, self: FileId,
                          writeOrNs: bool): Set[FileId] =
        opensOfFile(sdb, f).exclude(Set(self)).filter(fid => {
            val o = sdb.opens.get(fid)
            not(cachingLevelNone(o.caching)) and
            (writeOrNs or match o.caching {
                | COplock(l) => l == OpExclusive or l == OpBatch
                | CLease(x) => x.st.w or x.st.h
                | CNone => false
            })
        })

    // Emit breaks to every holder that a conflicting access must recall.
    pure def doBreakAll(sdb: StateDB, f: Ino, self: FileId,
                        writeOrNs: bool): StateDB = {
        val holders = breakHolders(sdb, f, self, writeOrNs)
        holders.fold(sdb, (acc, fid) => doBeginBreak(acc, fid, writeOrNs))
    }

    // Is any handle other than `self` mid-break on file f?  Such a
    // conflicting op must stall (STATUS_PENDING) until the break drains.
    pure def breakInProgress(sdb: StateDB, f: Ino, self: FileId): bool =
        opensOfFile(sdb, f).exclude(Set(self)).exists(fid =>
            sdb.opens.get(fid).breaking)

    // Acknowledge a break (OPLOCK_BREAK / LEASE_BREAK ack): the grant
    // settles to breakTo and the break clears.  The caller checks the
    // idempotency rule (ack on a settled grant -> STATUS_UNSUCCESSFUL)
    // before calling this.
    pure def doAckBreak(sdb: StateDB, fid: FileId): StateDB =
        sdb.with("opens", sdb.opens.setBy(fid, o => o
              .with("caching", o.breakTo)
              .with("breaking", false)))

    // ------------------------------------------------------------------
    // Byte-range locks (MS-FSA 2.1.4.10 / MS-SMB2 3.3.5.14)
    // ------------------------------------------------------------------

    // Does a lock of type wr over [lo,hi) requested by handle `self`
    // conflict with a lock held by a DIFFERENT handle?  WRITE conflicts
    // with everything; READ conflicts with WRITE.  Same-handle locks are
    // replaced/merged, never in conflict.
    pure def lockConflict(sdb: StateDB, f: Ino, self: FileId,
                          lo: int, hi: int, wr: bool): bool =
        range(lo, hi).foldl(false, (acc, b) =>
            acc or (sdb.rangeLocks.keys().contains((f, b)) and {
                val e = sdb.rangeLocks.get((f, b))
                e.owner != self and (wr or e.t == LWrite)
            }))

    // Install a lock over [lo,hi) owned by handle `self`.
    pure def doLock(sdb: StateDB, f: Ino, self: FileId,
                    lo: int, hi: int, wr: bool): StateDB =
        sdb.with("rangeLocks", range(lo, hi).foldl(sdb.rangeLocks, (acc, b) =>
            acc.put((f, b), { t: if (wr) LWrite else LRead, owner: self })))

    // Is [lo,hi) exactly held by handle `self`?  SMB unlock must match an
    // existing lock range or return RANGE_NOT_LOCKED (MS-FSA 2.1.5.8).
    pure def rangeLockedBy(sdb: StateDB, f: Ino, self: FileId,
                           lo: int, hi: int): bool =
        range(lo, hi).foldl(true, (acc, b) =>
            acc and sdb.rangeLocks.keys().contains((f, b)) and
            sdb.rangeLocks.get((f, b)).owner == self)

    pure def doUnlock(sdb: StateDB, f: Ino, self: FileId,
                      lo: int, hi: int): StateDB = {
        val kept = sdb.rangeLocks.keys()
            .filter(k => not(k._1 == f and k._2 >= lo and k._2 < hi and
                             sdb.rangeLocks.get(k).owner == self))
            .mapBy(k => sdb.rangeLocks.get(k))
        sdb.with("rangeLocks", kept)
    }

    // A READ/WRITE over [off,off+len) is blocked by a mandatory lock held
    // by a DIFFERENT handle (writes conflict with any lock, reads only with
    // a WRITE lock) -- MS-FSA 2.1.4.2/2.1.4.3.  This is the coherency point
    // PROPERTY-3c: SMB locks are mandatory, so I/O consults them.
    pure def rangeIoConflict(sdb: StateDB, f: Ino, self: FileId,
                             off: int, len: int, isWrite: bool): bool =
        range(off, off + len).foldl(false, (acc, b) =>
            acc or (sdb.rangeLocks.keys().contains((f, b)) and {
                val e = sdb.rangeLocks.get((f, b))
                e.owner != self and (isWrite or e.t == LWrite)
            }))

    // ------------------------------------------------------------------
    // Structural invariants (model self-checks)
    // ------------------------------------------------------------------

    // Every open belongs to a live session and tree and names a live inode.
    pure def stOpensLive(sdb: StateDB): bool =
        sdb.opens.keys().forall(fid => {
            val o = sdb.opens.get(fid)
            sdb.sessions.keys().contains(o.session) and
            sdb.trees.keys().contains(o.tree) and
            fsLive(sdb.fs, o.file)
        })

    // Every tree belongs to a live session.
    pure def stTreesLive(sdb: StateDB): bool =
        sdb.trees.keys().forall(t =>
            sdb.sessions.keys().contains(sdb.trees.get(t).session))

    // No two opens' share reservations conflict pairwise (the grant-time
    // check keeps this: shareConflict includes existing opens).
    pure def stSharesConsistent(sdb: StateDB): bool =
        sdb.opens.keys().forall(f1 =>
            sdb.opens.keys().forall(f2 => {
                val a = sdb.opens.get(f1)
                val b = sdb.opens.get(f2)
                f1 == f2 or a.file != b.file or
                (not(accItsc(grantedAcc(a), b.denied)) and
                 not(accItsc(grantedAcc(b), a.denied)))
            }))

    // Conflicting byte-range locks never coexist (different owners, same
    // byte, at least one WRITE).
    pure def stLocksDisjoint(sdb: StateDB): bool =
        sdb.rangeLocks.keys().forall(k => {
            val e = sdb.rangeLocks.get(k)
            // a byte holds exactly one entry (map key), so the only way a
            // conflict could exist is a WRITE coexisting with itself -- the
            // per-byte map makes cross-owner write overlap impossible by
            // construction.  This invariant guards the doLock merge logic.
            e.t == LRead or e.t == LWrite
        })

    // Range locks only reference bytes owned by a live open of that file.
    pure def stLocksAnchored(sdb: StateDB): bool =
        sdb.rangeLocks.keys().forall(k =>
            sdb.opens.keys().contains(sdb.rangeLocks.get(k).owner) and
            sdb.opens.get(sdb.rangeLocks.get(k).owner).file == k._1)

    // A grant that is mid-break has a target no stronger than its current
    // level (a break only ever downgrades).
    pure def stBreaksDowngrade(sdb: StateDB): bool =
        sdb.opens.keys().forall(fid => {
            val o = sdb.opens.get(fid)
            not(o.breaking) or cachingRank(o.breakTo) <= cachingRank(o.caching)
        })

    // A coarse ordering on caching strength for the break-downgrade check.
    pure def cachingRank(c: Caching): int =
        match c {
            | CNone => 0
            | COplock(l) =>
                match l {
                    | OpNone => 0
                    | OpLevelII => 1
                    | OpExclusive => 3
                    | OpBatch => 4
                }
            | CLease(x) =>
                (if (x.st.r) 1 else 0) + (if (x.st.h) 1 else 0) +
                (if (x.st.w) 2 else 0)
        }

    pure def stInv(sdb: StateDB): bool =
        stOpensLive(sdb) and stTreesLive(sdb) and stSharesConsistent(sdb) and
        stLocksDisjoint(sdb) and stLocksAnchored(sdb) and
        stBreaksDowngrade(sdb)
}
