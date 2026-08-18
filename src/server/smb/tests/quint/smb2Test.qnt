// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Deterministic self-checks for the SMB2 model, executed with
//   quint test smb2Test.qnt --main=smb2Test
// They validate the model's own transition logic without a server, so a
// model regression is caught before it can invalidate generated traces --
// the SMB sibling of nfs4Test.qnt.  The instance pins every optional
// feature to FeatMandatory so the capability profile is deterministic.

module smb2Test {
    import nfs4fs.* from "../../../nfs/tests/quint/nfs4_fs"
    import smb2state.* from "./smb2_state"
    import smb2ops.* from "./smb2_ops"
    import smb2(
        DIALECT = 0x0311,
        F_OPLOCKS = FeatMandatory,
        F_LEASES = FeatMandatory,
        F_DIRLEASE = FeatMandatory,
        F_DURABLE = FeatOff,
        P_LEASE_OPEN_BREAK = true
    ).* from "./smb2"

    // ------------------------------------------------------------------
    // Result accessors
    // ------------------------------------------------------------------

    def lastResults: List[Res] =
        match lastOp {
            | LMsg(l) => l.results
            | LInit(_) => []
        }

    def resAt(i: int): Res = lastResults[i]
    def stAt(i: int): int = stOf(resAt(i))

    val noCreate = { st: -1, fid: -1, act: -1, ino: -1, caching: CNone,
                     breaks: Set() }
    def asCreate(r) =
        match r { | RCreate(x) => x | _ => noCreate }

    val noRead = { st: -1, count: -1, eof: false, blocks: [] }
    def asRead(r) =
        match r { | RRead(x) => x | _ => noRead }

    val noWrite = { st: -1, count: -1, breaks: Set() }
    def asWrite(r) =
        match r { | RWrite(x) => x | _ => noWrite }

    val noQuery = { st: -1, attrs: { ftype: FReg, nlink: 0, sizeBlocks: 0,
                                     change: 0, deletePending: false } }
    def asQuery(r) =
        match r { | RQueryBasic(x) => x | _ => noQuery }

    // Common CREATE argument shorthand (non-directory).
    def create(dir: Ino, name: Name, disp: Disposition, acc: DesiredAccess,
               sh: ShareAccess, doc: bool, opl: OplockReq): Cmd =
        CCreate({ dir: dir, name: name, disp: disp, access: acc, share: sh,
                  isDir: false, delOnClose: doc, oplock: opl })

    // ------------------------------------------------------------------
    // Bootstrap: one session (client 1) + one tree.
    // ------------------------------------------------------------------

    run bootRun =
        init
            .then(runM({ sess: -1, tree: -1, related: false,
                         cmds: [ CSessionSetup({ clientSym: 1 }) ] }))
            .expect(stAt(0) == ST_SUCCESS and sdb.sessions.keys().contains(0))
            .then(runM(one(0, -1, CTreeConnect({ share: 1 }))))
            .expect(stAt(0) == ST_SUCCESS and sdb.trees.keys().contains(0))

    // Bootstrap plus a second session (client 2) + tree for cross-session
    // conflict tests: session 1, tree 1.
    run boot2Run =
        bootRun
            .then(runM({ sess: -1, tree: -1, related: false,
                         cmds: [ CSessionSetup({ clientSym: 2 }) ] }))
            .expect(stAt(0) == ST_SUCCESS and sdb.sessions.keys().contains(1))
            .then(runM(one(1, -1, CTreeConnect({ share: 1 }))))
            .expect(stAt(0) == ST_SUCCESS and sdb.trees.keys().contains(1))

    // ------------------------------------------------------------------
    // CREATE disposition / type matrix (MS-FSA 2.1.5.1)
    // ------------------------------------------------------------------

    run createMatrixTest =
        bootRun
            // OPEN of a missing name -> OBJECT_NAME_NOT_FOUND.
            .then(runM(one(0, 0, create(ROOT, "a", DispOpen, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_OBJECT_NAME_NOT_FOUND)
            // CREATE of a fresh file -> CREATED, fid 0.
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).act == ACT_CREATED)
            .expect(asCreate(resAt(0)).fid == 0)
            // CREATE over the existing file -> OBJECT_NAME_COLLISION.
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_OBJECT_NAME_COLLISION)
            // OPEN_IF of the existing file -> OPENED.
            .then(runM(one(0, 0, create(ROOT, "a", DispOpenIf, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).act == ACT_OPENED)
            // OVERWRITE of a missing name -> OBJECT_NAME_NOT_FOUND.
            .then(runM(one(0, 0, create(ROOT, "z", DispOverwrite, ACCESS_RW,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_OBJECT_NAME_NOT_FOUND)
            // SUPERSEDE of the existing file -> FILE_SUPERSEDED, size 0.
            .then(runM(one(0, 0, create(ROOT, "a", DispSupersede, ACCESS_RW,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and
                    asCreate(resAt(0)).act == ACT_SUPERSEDED)

    // A directory-create then a non-directory OPEN of the same name is a
    // type mismatch -> FILE_IS_A_DIRECTORY.
    run createTypeMismatchTest =
        bootRun
            .then(runM(one(0, 0, CCreate({ dir: ROOT, name: "d",
                                           disp: DispCreate, access: ACCESS_R,
                                           share: SHARE_RWD, isDir: true,
                                           delOnClose: false,
                                           oplock: OrNone }))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).act == ACT_CREATED)
            .then(runM(one(0, 0, create(ROOT, "d", DispOpen, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_FILE_IS_A_DIRECTORY)

    // ------------------------------------------------------------------
    // Share reservations (MS-FSA SharingViolation)
    // ------------------------------------------------------------------

    run shareConflictTest =
        boot2Run
            // Client 1 opens "a" for read+write, sharing READ only (deny
            // WRITE + DELETE).
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_R, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS)
            // Client 2 opens "a" for WRITE -> intersects client 1's deny-W
            // -> SHARING_VIOLATION.
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_W,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SHARING_VIOLATION)
            // Client 2 opens "a" for READ, sharing R+W+D so it denies
            // nothing of client 1's held R+W -> OK (client 1 shares READ,
            // so client 2's read access is permitted too).
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS)
            // An attribute-only open bypasses share arbitration entirely.
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen,
                                        { r: false, w: false, h: true,
                                          d: false }, SHARE_NONE, false,
                                        OrNone))))
            .expect(stAt(0) == ST_SUCCESS)

    // ------------------------------------------------------------------
    // Oplock/lease break lifecycle
    // ------------------------------------------------------------------

    // A sole opener gets a batch oplock; a conflicting open drives a break
    // notification, the opener stalls (PENDING), the holder acks (downgrade
    // to level II), and the retried open then succeeds.
    run oplockBreakOnOpenTest =
        boot2Run
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RW, false, OrBatch))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(asCreate(resAt(0)).caching == COplock(OpBatch))
            // Client 2's conflicting open -> break emitted, PENDING.
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_R,
                                        SHARE_RW, false, OrNone))))
            .expect(stAt(0) == ST_PENDING)
            .expect(asCreate(resAt(0)).breaks == Set(0))
            .expect(sdb.opens.get(0).breaking and
                    sdb.opens.get(0).breakTo == COplock(OpLevelII))
            // Holder acks -> grant settles to level II, break clears.
            .then(runM(one(0, 0, CBreakAck(FidRef(0)))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(sdb.opens.get(0).caching == COplock(OpLevelII) and
                    not(sdb.opens.get(0).breaking))
            // Idempotency: a second ack on the settled grant -> UNSUCCESSFUL.
            .then(runM(one(0, 0, CBreakAck(FidRef(0)))))
            .expect(stAt(0) == ST_UNSUCCESSFUL)
            // Retried open now succeeds (no exclusive grant left to break).
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_R,
                                        SHARE_RW, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).fid == 1)

    // break_reads_for_write (Seam 4a): a writer coexisting with a level-II
    // read cache breaks that cache to NONE when it writes.  The writer
    // opened first (no cache), so the reader's later level-II grant is not
    // broken at open; the WRITE op is what recalls it.
    run oplockBreakOnWriteTest =
        boot2Run
            // Writer opens first, no oplock -> no cache.
            .then(runM(one(0, 0, create(ROOT, "b", DispCreate, ACCESS_RW,
                                        SHARE_RW, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).fid == 0)
            // Reader opens, level II granted (writer holds no cache to break).
            .then(runM(one(1, 1, create(ROOT, "b", DispOpen, ACCESS_R,
                                        SHARE_RW, false, OrLevelII))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(asCreate(resAt(0)).caching == COplock(OpLevelII) and
                    asCreate(resAt(0)).fid == 1)
            // Writer writes -> reader's level-II cache is recalled to NONE.
            .then(runM(one(0, 0, CWrite({ fid: FidRef(0), off: 0, len: 1,
                                          pat: 1 }))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(asWrite(resAt(0)).breaks == Set(1))
            .expect(sdb.opens.get(1).breaking and
                    sdb.opens.get(1).breakTo == COplock(OpNone))

    // A lease grants RWH to a sole opener; write caching (W) is only handed
    // to a sole opener (discretion), and the epoch starts at 1.
    run leaseGrantTest =
        bootRun
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RW, false,
                                        OrLease({ key: 7, r: true, w: true,
                                                  h: true })))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(match asCreate(resAt(0)).caching {
                | CLease(x) => x.st.r and x.st.w and x.st.h and
                               x.key == 7 and x.epoch == 1
                | _ => false })

    // ------------------------------------------------------------------
    // Byte-range locks (mandatory)
    // ------------------------------------------------------------------

    run lockConflictTest =
        boot2Run
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RW, false, OrNone))))
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_RW,
                                        SHARE_RW, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).fid == 1)
            // Handle 0 write-locks [0,2).
            .then(runM(one(0, 0, CLock({ fid: FidRef(0), lo: 0, hi: 2,
                                         wr: true, unlock: false,
                                         failImmediately: true }))))
            .expect(stAt(0) == ST_SUCCESS)
            // Handle 1's overlapping write lock -> LOCK_NOT_GRANTED.
            .then(runM(one(1, 1, CLock({ fid: FidRef(1), lo: 1, hi: 2,
                                         wr: true, unlock: false,
                                         failImmediately: true }))))
            .expect(stAt(0) == ST_LOCK_NOT_GRANTED)
            // Mandatory enforcement: handle 1's WRITE over a locked byte
            // -> FILE_LOCK_CONFLICT (PROPERTY-3c).
            .then(runM(one(1, 1, CWrite({ fid: FidRef(1), off: 0, len: 1,
                                          pat: 1 }))))
            .expect(stAt(0) == ST_FILE_LOCK_CONFLICT)
            // Handle 0 unlocks the exact range; handle 1's unlock of an
            // unheld range -> RANGE_NOT_LOCKED.
            .then(runM(one(0, 0, CLock({ fid: FidRef(0), lo: 0, hi: 2,
                                         wr: false, unlock: true,
                                         failImmediately: true }))))
            .expect(stAt(0) == ST_SUCCESS)
            .then(runM(one(1, 1, CLock({ fid: FidRef(1), lo: 0, hi: 1,
                                         wr: false, unlock: true,
                                         failImmediately: true }))))
            .expect(stAt(0) == ST_RANGE_NOT_LOCKED)

    // ------------------------------------------------------------------
    // Delete-on-close / delete-pending / unlink-while-open
    // ------------------------------------------------------------------

    run deleteOnCloseTest =
        boot2Run
            // Client 1 opens "a" with FILE_DELETE_ON_CLOSE.
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RWD,
                                        SHARE_RWD, true, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and
                    sdb.deletePending.contains(asCreate(resAt(0)).ino))
            // A concurrent OPEN observes the pending delete -> DELETE_PENDING.
            .then(runM(one(1, 1, create(ROOT, "a", DispOpen, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_DELETE_PENDING)
            // Closing the delete-on-close handle unlinks the file.
            .then(runM(one(0, 0, CClose(FidRef(0)))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(not(fsLive(sdb.fs, 1)) and sdb.fs.removed.contains(1))

    // Delete-deferred-to-last-close (MS-FSA 2.1.5.4): with delete-disposition
    // armed and a second handle still open, the directory entry persists
    // (nlink 1, delete-pending) until the final handle closes.
    run unlinkWhileOpenTest =
        bootRun
            .then(runM(one(0, 0, create(ROOT, "b", DispCreate, ACCESS_RWD,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).fid == 0)
            .then(runM(one(0, 0, create(ROOT, "b", DispOpen, ACCESS_R,
                                        SHARE_RWD, false, OrNone))))
            .expect(stAt(0) == ST_SUCCESS and asCreate(resAt(0)).fid == 1)
            // Arm delete-disposition on handle 0 and close it: the file
            // remains (handle 1 still open), now delete-pending.
            .then(runM(one(0, 0, CSetDisposition({ fid: FidRef(0),
                                                   del: true }))))
            .expect(stAt(0) == ST_SUCCESS)
            .then(runM(one(0, 0, CClose(FidRef(0)))))
            .expect(stAt(0) == ST_SUCCESS)
            .expect(fsLive(sdb.fs, 1) and fsGet(sdb.fs, 1).nlink == 1 and
                    sdb.deletePending.contains(1))
            // Closing the last handle performs the deferred unlink.
            .then(runM(one(0, 0, CClose(FidRef(1)))))
            .expect(stAt(0) == ST_SUCCESS and not(fsLive(sdb.fs, 1)))

    // ------------------------------------------------------------------
    // Compound (NextCommand) threading
    // ------------------------------------------------------------------

    run compoundThreadingTest =
        bootRun
            // Related CREATE+WRITE+CLOSE: WRITE and CLOSE inherit the
            // CREATE's FileId; all three execute and succeed.
            .then(runM({ sess: 0, tree: 0, related: true,
                         cmds: [
                             create(ROOT, "a", DispOpenIf, ACCESS_RW,
                                    SHARE_RW, false, OrNone),
                             CWrite({ fid: FidRelated, off: 0, len: 1,
                                      pat: 2 }),
                             CClose(FidRelated) ] }))
            .expect(lastResults.length() == 3)
            .expect(stAt(0) == ST_SUCCESS and stAt(1) == ST_SUCCESS and
                    stAt(2) == ST_SUCCESS)
            .expect(asCreate(resAt(0)).act == ACT_CREATED)
            .expect(sdb.opens.keys().size() == 0 and
                    fsGet(sdb.fs, 1).sizeBlocks == 1)
            // Related first-error-stops: a failing CREATE aborts the chain,
            // so only its result is present.
            .then(runM({ sess: 0, tree: 0, related: true,
                         cmds: [
                             create(ROOT, "zz", DispOpen, ACCESS_R,
                                    SHARE_RWD, false, OrNone),
                             CWrite({ fid: FidRelated, off: 0, len: 1,
                                      pat: 1 }),
                             CClose(FidRelated) ] }))
            .expect(stAt(0) == ST_OBJECT_NAME_NOT_FOUND and
                    lastResults.length() == 1)
            // Unrelated compound: both commands execute independently.
            .then(runM({ sess: 0, tree: 0, related: false,
                         cmds: [ CClose(FidRef(99)), CClose(FidRef(98)) ] }))
            .expect(lastResults.length() == 2 and
                    stAt(0) == ST_FILE_CLOSED and stAt(1) == ST_FILE_CLOSED)

    // ------------------------------------------------------------------
    // Change attribute consistency across a mutation
    // ------------------------------------------------------------------

    run changeAttrTest =
        bootRun
            .then(runM(one(0, 0, create(ROOT, "a", DispCreate, ACCESS_RW,
                                        SHARE_RW, false, OrNone))))
            .then(runM(one(0, 0, CQueryBasic(FidRef(0)))))
            .expect(stAt(0) == ST_SUCCESS)
            .then(runM(one(0, 0, CWrite({ fid: FidRef(0), off: 0, len: 2,
                                          pat: 3 }))))
            .expect(stAt(0) == ST_SUCCESS)
            .then(runM(one(0, 0, CQueryBasic(FidRef(0)))))
            .expect(stAt(0) == ST_SUCCESS and
                    asQuery(resAt(0)).attrs.sizeBlocks == 2)

    // ------------------------------------------------------------------
    // Random-walk invariant preservation
    // ------------------------------------------------------------------

    run randomWalkTest =
        init.then(80.reps(_ => all { inv, step }))

    run randomShareWalkTest =
        init.then(60.reps(_ => all { inv, stepShare }))

    run randomLeaseWalkTest =
        init.then(60.reps(_ => all { inv, stepLease }))

    run randomDataWalkTest =
        init.then(60.reps(_ => all { inv, stepData }))
}
