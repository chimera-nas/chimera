// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Deterministic self-checks for the NFS4 model, executed with
//   quint test nfs4Test.qnt --main=nfs4Test42
//   quint test nfs4Test.qnt --main=nfs4Test40
// They validate the model's own transition logic without a server, so a
// model regression is caught before it can invalidate generated traces.

// ---------------------------------------------------------------------
// NFSv4.2 instance, every feature on (all-mandatory = deterministic caps)
// ---------------------------------------------------------------------
module nfs4Test42 {
    import nfs4fs.* from "./nfs4_fs"
    import nfs4state.* from "./nfs4_state"
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

    def lastStatus: int =
        match lastOp {
            | LCompound(l) => l.status
            | LInit(x) => -1
        }

    def lastResults: List[OpRes] =
        match lastOp {
            | LCompound(l) => l.results
            | LInit(x) => []
        }

    def resAt(i: int): OpRes = lastResults[i]

    val noOpen = { st: -999, sid: -1, seq: 0, needConfirm: false,
                   cinfo: { before: 0, after: 0 }, deleg: DNone,
                   recalls: Set() }
    def asOpen(r) =
        match r {
            | SOpen(x) => x
            | _ => noOpen
        }

    def asLock(r) =
        match r {
            | SLock(x) => x
            | _ => { st: -999, sid: -1, seq: 0, deniedOwner: -1 }
        }

    def asRead(r) =
        match r {
            | SRead(x) => x
            | _ => { st: -999, count: -1, eof: false, blocks: [] }
        }

    def asSeek(r) =
        match r {
            | SSeek(x) => x
            | _ => { st: -999, eof: false, offset: -1 }
        }

    def asLayoutget(r) =
        match r {
            | SLayoutget(x) => x
            | _ => { st: -999, sid: -1, seq: 0, lo: -1, hi: -1 }
        }

    def asLayoutreturn(r) =
        match r {
            | SLayoutreturn(x) => x
            | _ => { st: -999, present: false, seq: 0 }
        }

    // The session bootstrap used by every 4.1/4.2 test: EXCHANGE_ID
    // (client 0) + CREATE_SESSION (session 0).
    run bootRun =
        init
            .then(runC([ RExchangeId({ ownerSym: 1, verfSym: 1 }) ]))
            .expect(lastStatus == OK and sdb.clients.keys().contains(0))
            .then(runC([ RCreateSession({ client: 0, csaSeq: 1,
                                          backChan: true }) ]))
            .expect(lastStatus == OK and sdb.sessions.keys().contains(0))
            .expect(sdb.clients.get(0).confirmed)

    def seq0(n: int): OpReq =
        RSequence({ sess: 0, slot: 0, seq: n, cacheThis: true })

    // Bootstrap plus the client's global RECLAIM_COMPLETE (RFC 8881
    // 18.51.3 gates all lock-obtaining ops behind it).  Slot 1 keeps the
    // tests' slot-0 sequence numbering untouched.
    run bootRcRun =
        bootRun
            .then(runC([ RSequence({ sess: 0, slot: 1, seq: 1,
                                     cacheThis: false }),
                         RReclaimComplete ]))
            .expect(lastStatus == OK)

    // --- sessions -----------------------------------------------------

    run sequenceDisciplineTest =
        init
            .then(runC([ RExchangeId({ ownerSym: 1, verfSym: 1 }) ]))
            // CREATE_SESSION with the wrong csa_sequence: misordered, and
            // the seeded cache means seq-1 (=0) is also misordered.
            .then(runC([ RCreateSession({ client: 0, csaSeq: 0,
                                          backChan: true }) ]))
            .expect(lastStatus == E_SEQ_MISORDERED)
            .then(runC([ RCreateSession({ client: 0, csaSeq: 1,
                                          backChan: true }) ]))
            .expect(lastStatus == OK)
            // First SEQUENCE must carry slot seqid 1; 2 is misordered and
            // MUST NOT touch the slot.
            .then(runC([ seq0(2), RPutrootfh ]))
            .expect(lastStatus == E_SEQ_MISORDERED and lastResults.length() == 1)
            .then(runC([ seq0(1), RPutrootfh, RGetfh ]))
            .expect(lastStatus == OK and lastResults.length() == 3)
            // Non-exempt op without SEQUENCE -> OP_NOT_IN_SESSION.
            .then(runC([ RPutrootfh, RGetfh ]))
            .expect(lastStatus == E_OP_NOT_IN_SESSION)
            // SEQUENCE not first -> SEQUENCE_POS.
            .then(runC([ seq0(2), seq0(3) ]))
            .expect(lastStatus == E_SEQUENCE_POS)
            // (the failed second SEQUENCE consumed slot seq 2)
            .then(runC([ seq0(3), RPutrootfh ]))
            .expect(lastStatus == OK)
            // Unknown session / slot.
            .then(runC([ RSequence({ sess: 9, slot: 0, seq: 1,
                                     cacheThis: true }) ]))
            .expect(lastStatus == E_BADSESSION)
            .then(runC([ RSequence({ sess: 0, slot: 99, seq: 1,
                                     cacheThis: true }) ]))
            .expect(lastStatus == E_BADSLOT)

    run destroyClientidBusyTest =
        bootRun
            .then(runC([ RDestroyClientid(0) ]))
            .expect(lastStatus == E_CLIENTID_BUSY)
            .then(runC([ RDestroySession(0) ]))
            .expect(lastStatus == OK)
            .then(runC([ RDestroyClientid(0) ]))
            .expect(lastStatus == OK and sdb.clients.keys().size() == 0)

    run exchangeIdRebootTest =
        bootRun
            // Same owner, new verifier: client rebooted.  Fresh unconfirmed
            // clientid; the old confirmed record survives until the new one
            // is confirmed by CREATE_SESSION, which purges the old record.
            .then(runC([ RExchangeId({ ownerSym: 1, verfSym: 2 }) ]))
            .expect(lastStatus == OK and sdb.clients.keys() == Set(0, 1))
            .then(runC([ RCreateSession({ client: 1, csaSeq: 1,
                                          backChan: true }) ]))
            .then(runC([ RSequence({ sess: 1, slot: 1, seq: 1,
                                     cacheThis: false }),
                         RReclaimComplete ]))
            .expect(lastStatus == OK)
            .expect(lastStatus == OK)
            .expect(not(sdb.clients.keys().contains(0)) and
                    sdb.clients.get(1).confirmed)

    run reclaimCompleteTest =
        bootRun
            .then(runC([ seq0(1), RReclaimComplete ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(2), RReclaimComplete ]))
            .expect(lastStatus == E_COMPLETE_ALREADY)

    // --- compound machinery -------------------------------------------

    run compoundThreadingTest =
        bootRun
            // GETFH without a current filehandle; RESTOREFH without saved.
            .then(runC([ seq0(1), RGetfh ]))
            .expect(lastStatus == E_NOFILEHANDLE and lastResults.length() == 2)
            .then(runC([ seq0(2), RRestorefh ]))
            .expect(lastStatus == E_RESTOREFH)
            // SAVEFH/RESTOREFH round trip; sfh survives RESTOREFH.
            .then(runC([ seq0(3), RPutrootfh,
                         RCreate({ ctype: FDir, name: "d", mode: 493,
                                   target: "" }),
                         RGetfh ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(4), RPutfh(1), RSavefh, RPutrootfh,
                         RRestorefh, RGetfh, RRestorefh, RGetfh ]))
            .expect(lastStatus == OK)
            .expect(match resAt(5) { | SGetfh(x) => x.ino == 1 | _ => false })
            .expect(match resAt(7) { | SGetfh(x) => x.ino == 1 | _ => false })
            // First-error-stops: LOOKUP of a missing name halts before
            // the GETFH; compound status = the failing op's status.
            .then(runC([ seq0(5), RPutrootfh, RLookup("zz"), RGetfh ]))
            .expect(lastStatus == E_NOENT and lastResults.length() == 3)

    // --- OPEN family --------------------------------------------------

    // Create a file "f" under the root via OPEN (GUARDED), yielding open
    // stateid for (client 0, owner 1).
    run openCreateRun =
        bootRcRun
            .then(runC(([ seq0(1), RPutrootfh,
                          ROpen({ client: 0, owner: 1, oseq: 0,
                                  claim: OCNull("f"),
                                  how: HGuarded({ mode: 420 }),
                                  access: 3, deny: 0 }),
                          RGetfh ])))
            .expect(lastStatus == OK)
            // Initial open stateid seqid is exactly 1 (RFC 8881 8.2.2);
            // BOTH+deny-NONE open of a solely-opened file grants a write
            // delegation under the canonical profile.
            .expect(asOpen(resAt(2)).seq == 1 and asOpen(resAt(2)).sid == 0)
            .expect(match asOpen(resAt(2)).deleg {
                | DWrite(x) => true
                | _ => false })

    run openUpgradeDowngradeTest =
        bootRcRun
            // Two separate OPen events READ then WRITE by the same owner.
            .then(runC([ seq0(1), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"),
                                 how: HUnchecked({ mode: 420, truncate: false }),
                                 access: 1, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK and asOpen(resAt(2)).seq == 1)
            .then(runC([ seq0(2), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"),
                                 how: HNoCreate,
                                 access: 2, deny: 0 }), RGetfh ]))
            // Upgrade: same stateid, seqid bumped, union RW.
            .expect(lastStatus == OK and asOpen(resAt(2)).sid == 0 and
                    asOpen(resAt(2)).seq == 2)
            .expect(sdb.opens.get(0).access == 3)
            // Downgrade to READ: expressible (the READ open event exists).
            .then(runC([ seq0(3), RPutfh(1),
                         ROpenDowngrade({ sid: 0, argSeq: 0, access: 1,
                                          deny: 0, oseq: 0 }) ]))
            .expect(lastStatus == OK and sdb.opens.get(0).access == 1)
            // Downgrade to WRITE now: history collapsed to READ -> INVAL.
            .then(runC([ seq0(4), RPutfh(1),
                         ROpenDowngrade({ sid: 0, argSeq: 0, access: 2,
                                          deny: 0, oseq: 0 }) ]))
            .expect(lastStatus == E_INVAL)

    // One OPEN(BOTH) can never be downgraded to READ (the historical-
    // events rule, RFC 7530 16.19.4 / RFC 8881 18.18.3).
    run openDowngradeHistoryTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         ROpenDowngrade({ sid: 0, argSeq: 0, access: 1,
                                          deny: 0, oseq: 0 }) ]))
            .expect(lastStatus == E_INVAL)

    run shareConflictTest =
        openCreateRun
            // Another client: EXCHANGE_ID + CREATE_SESSION (session 1).
            .then(runC([ RExchangeId({ ownerSym: 2, verfSym: 1 }) ]))
            .then(runC([ RCreateSession({ client: 1, csaSeq: 1,
                                          backChan: true }) ]))
            .then(runC([ RSequence({ sess: 1, slot: 1, seq: 1,
                                     cacheThis: false }),
                         RReclaimComplete ]))
            .expect(lastStatus == OK)
            // Client 1 opens for WRITE: conflicts with client 0's write
            // delegation first -> DELAY + recall.
            .then(runC([ RSequence({ sess: 1, slot: 0, seq: 1,
                                     cacheThis: true }), RPutrootfh,
                         ROpen({ client: 1, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 2, deny: 0 }) ]))
            .expect(lastStatus == E_DELAY)
            .expect(asOpen(resAt(2)).recalls == Set(1))
            .expect(sdb.delegs.get(1).recalled)
            // Client 0 returns the delegation.
            .then(runC([ seq0(2), RPutfh(1),
                         RDelegreturn({ sid: 1, kind: KDeleg, argSeq: 0 }) ]))
            .expect(lastStatus == OK and sdb.delegs.keys().size() == 0)
            // Client 1 retries with deny=READ|WRITE -> now a share
            // conflict with client 0's open (access BOTH).
            .then(runC([ RSequence({ sess: 1, slot: 0, seq: 2,
                                     cacheThis: true }), RPutrootfh,
                         ROpen({ client: 1, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 3 }) ]))
            .expect(lastStatus == E_SHARE_DENIED)
            // The aggregate includes the requester's own opens: client 0
            // holds (BOTH, deny NONE), so its own re-open with deny=WRITE
            // self-conflicts (RFC 7530 9.9 / RFC 8881 9.7).
            .then(runC([ seq0(3), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 2 }) ]))
            .expect(lastStatus == E_SHARE_DENIED)
            // After closing the BOTH open, (READ, deny WRITE) is grantable;
            // the subsequent access=WRITE upgrade then self-denies.
            .then(runC([ seq0(4), RPutfh(1),
                         RClose({ sid: 0, argSeq: 0, oseq: 0 }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(5), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 2 }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(6), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 2, deny: 0 }) ]))
            .expect(lastStatus == E_SHARE_DENIED)

    run exclusiveCreateTest =
        bootRcRun
            .then(runC([ seq0(1), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("x"),
                                 how: HExclusive({ verf: 1 }),
                                 access: 1, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK)
            // Same-verifier retry: idempotent success, same file.
            .then(runC([ seq0(2), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("x"),
                                 how: HExclusive({ verf: 1 }),
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == OK)
            // Different verifier -> EXIST.
            .then(runC([ seq0(3), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("x"),
                                 how: HExclusive({ verf: 2 }),
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_EXIST)
            // A WRITE clears the stored verifier (it lives in
            // time_modify): the same-verifier retry stops matching.
            .then(runC([ seq0(4), RPutfh(1),
                         RWrite({ sel: SelAnon, off: 0, len: 1, pat: 1,
                                  stable: 2 }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(5), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("x"),
                                 how: HExclusive({ verf: 1 }),
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_EXIST)

    run openTypingTest =
        bootRcRun
            .then(runC([ seq0(1), RPutrootfh,
                         RCreate({ ctype: FDir, name: "d", mode: 493,
                                   target: "" }) ]))
            .then(runC([ seq0(2), RPutrootfh,
                         RCreate({ ctype: FLnk, name: "l", mode: 493,
                                   target: "t" }) ]))
            .then(runC([ seq0(3), RPutrootfh,
                         RCreate({ ctype: FFifo, name: "p", mode: 420,
                                   target: "" }) ]))
            // 4.1+: dir -> ISDIR, symlink -> SYMLINK, fifo -> WRONG_TYPE.
            .then(runC([ seq0(4), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("d"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_ISDIR)
            .then(runC([ seq0(5), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("l"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_SYMLINK)
            .then(runC([ seq0(6), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("p"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_WRONG_TYPE)
            // GUARDED create over an existing directory -> EXIST.
            .then(runC([ seq0(7), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("d"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_EXIST)
            // CREATE of a regular file via CREATE -> BADTYPE.
            .then(runC([ seq0(8), RPutrootfh,
                         RCreate({ ctype: FReg, name: "r", mode: 420,
                                   target: "" }) ]))
            .expect(lastStatus == E_BADTYPE)

    // --- I/O and stateids ---------------------------------------------

    run ioStateidTest =
        bootRcRun
            // READ-only open; write deleg not granted (access=1 -> read
            // deleg under the profile).
            .then(runC([ seq0(1), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("f"),
                                 how: HUnchecked({ mode: 420, truncate: false }),
                                 access: 1, deny: 2 }), RGetfh ]))
            .expect(lastStatus == OK)
            // WRITE via the read-only open stateid -> OPENMODE.
            .then(runC([ seq0(2), RPutfh(1),
                         RWrite({ sel: SelRef({ sid: 0, kind: KOpen,
                                                argSeq: 0 }),
                                  off: 0, len: 1, pat: 1, stable: 0 }) ]))
            .expect(lastStatus == E_OPENMODE)
            // Anonymous WRITE against the open's deny=WRITE -> LOCKED.
            .then(runC([ seq0(3), RPutfh(1),
                         RWrite({ sel: SelAnon, off: 0, len: 1, pat: 1,
                                  stable: 0 }) ]))
            .expect(lastStatus == E_LOCKED)
            // READ via the open stateid with the exact seqid works; a
            // future seqid is BAD_STATEID, a stale one OLD_STATEID.
            .then(runC([ seq0(4), RPutfh(1),
                         RRead({ sel: SelRef({ sid: 0, kind: KOpen,
                                               argSeq: 1 }),
                                 off: 0, len: 1 }) ]))
            .expect(lastStatus == OK and asRead(resAt(2)).eof)
            .then(runC([ seq0(5), RPutfh(1),
                         RRead({ sel: SelRef({ sid: 0, kind: KOpen,
                                               argSeq: 7 }),
                                 off: 0, len: 1 }) ]))
            .expect(lastStatus == E_BAD_STATEID)
            // Sparse write then read-back with hole in the middle.
            .then(runC([ seq0(6), RPutfh(1),
                         RSetattr({ mode: -1, sizeBlocks: -1, sel: SelAnon }) ]))
            .expect(lastStatus == OK)

    run readWriteDataTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         RWrite({ sel: SelRef({ sid: 0, kind: KOpen,
                                                argSeq: 0 }),
                                  off: 2, len: 2, pat: 3, stable: 0 }) ]))
            .expect(lastStatus == OK and
                    fsGet(fs, 1).sizeBlocks == 4)
            .then(runC([ seq0(3), RPutfh(1),
                         RRead({ sel: SelRef({ sid: 0, kind: KOpen,
                                               argSeq: 0 }),
                                 off: 1, len: 3 }) ]))
            .expect(lastStatus == OK and
                    asRead(resAt(2)).blocks == [ 0, 3, 3 ] and
                    asRead(resAt(2)).eof)
            .then(runC([ seq0(4), RPutfh(1), RCommit ]))
            .expect(lastStatus == OK)

    // --- unlink-while-open --------------------------------------------

    run unlinkWhileOpenTest =
        openCreateRun
            .then(runC([ seq0(2), RPutrootfh, RRemove("f") ]))
            .expect(lastStatus == OK)
            // The inode survives as an orphan (nlink 0) while the open
            // and the delegation pin it.
            .expect(fsLive(fs, 1) and fsGet(fs, 1).nlink == 0)
            .then(runC([ seq0(3), RPutfh(1), RGetattr ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(4), RPutfh(1),
                         RDelegreturn({ sid: 1, kind: KDeleg, argSeq: 0 }) ]))
            .expect(lastStatus == OK and fsLive(fs, 1))
            .then(runC([ seq0(5), RPutfh(1),
                         RClose({ sid: 0, argSeq: 0, oseq: 0 }) ]))
            .expect(lastStatus == OK)
            // Last pin gone: the orphan is reaped.
            .expect(not(fsLive(fs, 1)) and fs.removed.contains(1))

    // --- byte-range locks ---------------------------------------------

    run lockConflictSplitTest =
        openCreateRun
            // Deleg would conflict with the second client; return it first.
            .then(runC([ seq0(2), RPutfh(1),
                         RDelegreturn({ sid: 1, kind: KDeleg, argSeq: 0 }) ]))
            .then(runC([ seq0(3), RPutfh(1),
                         RLock({ wr: true, lo: 0, hi: 6, newOwner: true,
                                 openSid: 0, lockSid: -1, lockOwner: 6,
                                 oseq: 0, lseq: 0 }) ]))
            .expect(lastStatus == OK and asLock(resAt(2)).seq == 1)
            // Partial unlock splits the range (bytes 2-3 released).
            .then(runC([ seq0(4), RPutfh(1),
                         RLocku({ sid: 2, argSeq: 0, lseq: 0, lo: 2, hi: 4 }) ]))
            .expect(lastStatus == OK)
            .expect(sdb.locks.get(2).bytes.keys() == Set(0, 1, 4, 5))
            // Same owner re-locks 1-4 read (downgrade over the hole).
            .then(runC([ seq0(5), RPutfh(1),
                         RLock({ wr: false, lo: 1, hi: 5, newOwner: false,
                                 openSid: -1, lockSid: 2, lockOwner: 6,
                                 oseq: 0, lseq: 0 }) ]))
            .expect(lastStatus == OK)
            .expect(sdb.locks.get(2).bytes.get(1) == LRead and
                    sdb.locks.get(2).bytes.get(0) == LWrite)
            // Second client: conflicting write lock over byte 1 -> DENIED.
            .then(runC([ RExchangeId({ ownerSym: 2, verfSym: 1 }) ]))
            .then(runC([ RCreateSession({ client: 1, csaSeq: 1,
                                          backChan: true }) ]))
            .then(runC([ RSequence({ sess: 1, slot: 1, seq: 1,
                                     cacheThis: false }),
                         RReclaimComplete ]))
            .expect(lastStatus == OK)
            .then(runC([ RSequence({ sess: 1, slot: 0, seq: 1,
                                     cacheThis: true }), RPutrootfh,
                         ROpen({ client: 1, owner: 1, oseq: 0,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 2, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK)
            .then(runC([ RSequence({ sess: 1, slot: 0, seq: 2,
                                     cacheThis: true }), RPutfh(1),
                         RLock({ wr: true, lo: 1, hi: 2, newOwner: true,
                                 openSid: 3, lockSid: -1, lockOwner: 7,
                                 oseq: 0, lseq: 0 }) ]))
            .expect(lastStatus == E_DENIED and asLock(resAt(2)).deniedOwner == 6)
            // But a read lock on the read-locked byte 3 is compatible.
            .then(runC([ RSequence({ sess: 1, slot: 0, seq: 3,
                                     cacheThis: true }), RPutfh(1),
                         RLockt({ client: 1, lockOwner: 7, wr: false,
                                  lo: 3, hi: 4 }) ]))
            .expect(lastStatus == OK)
            // FREE_STATEID with locks held -> LOCKS_HELD; after unlocking
            // everything it succeeds.
            .then(runC([ seq0(6), RFreeStateid({ sid: 2, kind: KLock,
                                                 argSeq: 0 }) ]))
            .expect(lastStatus == E_LOCKS_HELD)
            .then(runC([ seq0(7), RPutfh(1),
                         RLocku({ sid: 2, argSeq: 0, lseq: 0, lo: 0, hi: 8 }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(8), RFreeStateid({ sid: 2, kind: KLock,
                                                 argSeq: 0 }) ]))
            .expect(lastStatus == OK and sdb.locks.keys().size() == 0)

    // --- pNFS ---------------------------------------------------------

    run layoutLifecycleTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         RDelegreturn({ sid: 1, kind: KDeleg, argSeq: 0 }) ]))
            // First LAYOUTGET uses the open stateid; layout stateid is
            // born with seqid 1.
            .then(runC([ seq0(3), RPutfh(1),
                         RLayoutget({ rw: true, lo: 0, hi: 4,
                                      sel: SelRef({ sid: 0, kind: KOpen,
                                                    argSeq: 0 }) }) ]))
            .expect(lastStatus == OK and asLayoutget(resAt(2)).seq == 1)
            // Second LAYOUTGET uses the layout stateid; seqid advances.
            .then(runC([ seq0(4), RPutfh(1),
                         RLayoutget({ rw: true, lo: 4, hi: 6,
                                      sel: SelRef({ sid: 2, kind: KLayout,
                                                    argSeq: 0 }) }) ]))
            .expect(lastStatus == OK and asLayoutget(resAt(2)).seq == 2)
            // LAYOUTCOMMIT over a held RW range.
            .then(runC([ seq0(5), RPutfh(1),
                         RLayoutcommit({ sid: 2, lo: 0, hi: 2 }) ]))
            .expect(lastStatus == OK)
            // Partial return keeps the stateid (lrs_present).
            .then(runC([ seq0(6), RPutfh(1),
                         RLayoutreturn({ sid: 2, argSeq: 0, lo: 0, hi: 3 }) ]))
            .expect(lastStatus == OK and asLayoutreturn(resAt(2)).present)
            // Returning the rest retires it.
            .then(runC([ seq0(7), RPutfh(1),
                         RLayoutreturn({ sid: 2, argSeq: 0, lo: 3, hi: 6 }) ]))
            .expect(lastStatus == OK and not(asLayoutreturn(resAt(2)).present))
            .expect(sdb.layouts.keys().size() == 0)
            // LAYOUTCOMMIT with no layout -> BAD_STATEID (freed sid).
            .then(runC([ seq0(8), RPutfh(1),
                         RLayoutcommit({ sid: 2, lo: 0, hi: 1 }) ]))
            .expect(lastStatus == E_BAD_STATEID)

    // --- 4.2 sparse ops + COPY ----------------------------------------

    run sparseOpsTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         RWrite({ sel: SelAnon, off: 3, len: 1, pat: 2,
                                  stable: 0 }) ]))
            .expect(lastStatus == OK)
            // SEEK for data from 0 skips the leading hole.
            .then(runC([ seq0(3), RPutfh(1),
                         RSeek({ sel: SelAnon, off: 0, whatData: true }) ]))
            .expect(lastStatus == OK and asSeek(resAt(2)).offset == 3)
            // SEEK for hole from 3: block 3 is data, next hole is EOF...
            // no: size is 4, so the virtual hole is at 4.
            .then(runC([ seq0(4), RPutfh(1),
                         RSeek({ sel: SelAnon, off: 3, whatData: false }) ]))
            .expect(lastStatus == OK and asSeek(resAt(2)).offset == 4)
            // SEEK at/after EOF -> NXIO.
            .then(runC([ seq0(5), RPutfh(1),
                         RSeek({ sel: SelAnon, off: 4, whatData: true }) ]))
            .expect(lastStatus == E_NXIO)
            // ALLOCATE extends and materializes zero blocks (data for
            // SEEK); DEALLOCATE punches a hole but keeps the size.
            .then(runC([ seq0(6), RPutfh(1),
                         RAllocate({ sel: SelAnon, off: 4, len: 2 }) ]))
            .expect(lastStatus == OK and fsGet(fs, 1).sizeBlocks == 6)
            .then(runC([ seq0(7), RPutfh(1),
                         RSeek({ sel: SelAnon, off: 4, whatData: true }) ]))
            .expect(lastStatus == OK and asSeek(resAt(2)).offset == 4)
            .then(runC([ seq0(8), RPutfh(1),
                         RDeallocate({ sel: SelAnon, off: 3, len: 1 }) ]))
            .expect(lastStatus == OK and fsGet(fs, 1).sizeBlocks == 6)
            .then(runC([ seq0(9), RPutfh(1),
                         RRead({ sel: SelAnon, off: 3, len: 1 }) ]))
            .expect(lastStatus == OK and asRead(resAt(2)).blocks == [ 0 ])

    run copyTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         RWrite({ sel: SelAnon, off: 0, len: 2, pat: 2,
                                  stable: 0 }) ]))
            .then(runC([ seq0(3), RPutrootfh,
                         ROpen({ client: 0, owner: 2, oseq: 0,
                                 claim: OCNull("g"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 3, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK)
            // COPY src=f (ino 1) dst=g (ino 2), whole file via cnt=0.
            .then(runC([ seq0(4), RPutfh(1), RSavefh, RPutfh(2),
                         RCopy({ srcSel: SelAnon, dstSel: SelAnon,
                                 srcOff: 0, dstOff: 1, cnt: 0 }) ]))
            .expect(lastStatus == OK and fsGet(fs, 2).sizeBlocks == 3)
            // Same-file COPY -> INVAL.
            .then(runC([ seq0(5), RPutfh(1), RSavefh, RPutfh(1),
                         RCopy({ srcSel: SelAnon, dstSel: SelAnon,
                                 srcOff: 0, dstOff: 4, cnt: 1 }) ]))
            .expect(lastStatus == E_INVAL)
            // Source range beyond EOF -> INVAL.
            .then(runC([ seq0(6), RPutfh(1), RSavefh, RPutfh(2),
                         RCopy({ srcSel: SelAnon, dstSel: SelAnon,
                                 srcOff: 5, dstOff: 0, cnt: 1 }) ]))
            .expect(lastStatus == E_INVAL)

    // --- xattrs -------------------------------------------------------

    run xattrTest =
        openCreateRun
            .then(runC([ seq0(2), RPutfh(1),
                         RSetxattr({ name: "xa", sym: 1, how: XsCreate }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(3), RPutfh(1),
                         RSetxattr({ name: "xa", sym: 2, how: XsCreate }) ]))
            .expect(lastStatus == E_EXIST)
            .then(runC([ seq0(4), RPutfh(1),
                         RSetxattr({ name: "xb", sym: 2, how: XsReplace }) ]))
            .expect(lastStatus == E_NOXATTR)
            .then(runC([ seq0(5), RPutfh(1),
                         RSetxattr({ name: "xa", sym: 3, how: XsEither }) ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(6), RPutfh(1), RGetxattr("xa") ]))
            .expect(lastStatus == OK and
                    match resAt(2) { | SGetxattr(x) => x.sym == 3 | _ => false })
            .then(runC([ seq0(7), RPutfh(1), RListxattrs ]))
            .expect(match resAt(2) {
                | SListxattrs(x) => x.names == Set("xa")
                | _ => false })
            .then(runC([ seq0(8), RPutfh(1), RRemovexattr("xa") ]))
            .expect(lastStatus == OK)
            .then(runC([ seq0(9), RPutfh(1), RGetxattr("xa") ]))
            .expect(lastStatus == E_NOXATTR)

    // --- namespace matrix (spot checks; the v3-style matrix lives in the
    //     fs layer and is exercised by the random walk) ----------------

    run renameMatrixTest =
        bootRcRun
            .then(runC([ seq0(1), RPutrootfh,
                         RCreate({ ctype: FDir, name: "a", mode: 493,
                                   target: "" }) ]))
            .then(runC([ seq0(2), RPutrootfh,
                         RCreate({ ctype: FDir, name: "c", mode: 493,
                                   target: "" }) ]))
            .then(runC([ seq0(3), RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 0,
                                 claim: OCNull("b"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 1, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK)
            // file over dir -> ISDIR (target of rename); dir over file ->
            // NOTDIR; dir into own subtree -> INVAL.
            .then(runC([ seq0(4), RPutrootfh, RSavefh, RPutrootfh,
                         RRename({ oldname: "b", newname: "a" }) ]))
            .expect(lastStatus == E_ISDIR)
            .then(runC([ seq0(5), RPutrootfh, RSavefh, RPutrootfh,
                         RRename({ oldname: "a", newname: "b" }) ]))
            .expect(lastStatus == E_NOTDIR)
            .then(runC([ seq0(6), RPutrootfh, RSavefh, RPutfh(1),
                         RRename({ oldname: "a", newname: "d" }) ]))
            .expect(lastStatus == E_INVAL)
            // dir over empty dir succeeds; the target dies.
            .then(runC([ seq0(7), RPutrootfh, RSavefh, RPutrootfh,
                         RRename({ oldname: "a", newname: "c" }) ]))
            .expect(lastStatus == OK and not(fsLive(fs, 2)))
            // REMOVE of a non-empty directory -> NOTEMPTY.
            .then(runC([ seq0(8), RPutfh(1),
                         RCreate({ ctype: FDir, name: "s", mode: 493,
                                   target: "" }) ]))
            .then(runC([ seq0(9), RPutrootfh, RRemove("c") ]))
            .expect(lastStatus == E_NOTEMPTY)
            // LOOKUPP walks back up.
            .then(runC([ seq0(10), RPutfh(4), RLookupp, RGetfh ]))
            .expect(lastStatus == OK and
                    match resAt(3) { | SGetfh(x) => x.ino == 1 | _ => false })
            // LINK of a directory -> ISDIR.
            .then(runC([ seq0(11), RPutfh(1), RSavefh, RPutrootfh,
                         RLink("z") ]))
            .expect(lastStatus == E_ISDIR)

    // A long random walk preserves the structural invariants.
    run randomWalkTest =
        init.then(120.reps(_ => all { inv, step }))

    // The state-heavy and data-heavy flavors hold the invariants too.
    run randomStateWalkTest =
        init.then(80.reps(_ => all { inv, stepState }))

    run randomDataWalkTest =
        init.then(80.reps(_ => all { inv, stepData }))
}

// ---------------------------------------------------------------------
// NFSv4.0 instance: SETCLIENTID machinery, owner seqids, OPEN_CONFIRM
// ---------------------------------------------------------------------
module nfs4Test40 {
    import nfs4fs.* from "./nfs4_fs"
    import nfs4state.* from "./nfs4_state"
    import nfs4ops.* from "./nfs4_ops"
    import nfs4(
        MINOR = 0,
        F_PNFS = FeatOff,
        F_READ_DELEG = FeatMandatory,
        F_WRITE_DELEG = FeatMandatory,
        F_XATTR = FeatOff,
        F_SPARSE = FeatOff,
        F_COPY = FeatOff,
        P_CONFLICT_DELAYS = true,
        P_CLOSE_FREES_LOCKS = true
    ).* from "./nfs4"

    def lastStatus: int =
        match lastOp {
            | LCompound(l) => l.status
            | LInit(x) => -1
        }

    def lastResults: List[OpRes] =
        match lastOp {
            | LCompound(l) => l.results
            | LInit(x) => []
        }

    def resAt(i: int): OpRes = lastResults[i]

    def asOpen(r) =
        match r {
            | SOpen(x) => x
            | _ => { st: -999, sid: -1, seq: 0, needConfirm: false,
                     cinfo: { before: 0, after: 0 }, deleg: DNone,
                     recalls: Set() }
        }

    // SETCLIENTID / SETCLIENTID_CONFIRM matrix (RFC 7530 16.33/16.34).
    run clientBootstrapTest =
        init
            .then(runC([ RSetclientid({ ownerSym: 1, verfSym: 1, tok: 100 }) ]))
            .expect(lastStatus == OK and sdb.clients.keys() == Set(0))
            // Confirm with a bogus token -> STALE_CLIENTID.
            .then(runC([ RSetclientidConfirm({ client: 0, tok: 999 }) ]))
            .expect(lastStatus == E_STALE_CLIENTID)
            .then(runC([ RSetclientidConfirm({ client: 0, tok: 100 }) ]))
            .expect(lastStatus == OK and sdb.clients.get(0).confirmed)
            // Retransmit of the confirm: idempotent success.
            .then(runC([ RSetclientidConfirm({ client: 0, tok: 100 }) ]))
            .expect(lastStatus == OK)
            // Reboot: same owner, new verifier -> fresh unconfirmed
            // record; old confirmed record survives until the confirm.
            .then(runC([ RSetclientid({ ownerSym: 1, verfSym: 2, tok: 101 }) ]))
            .expect(lastStatus == OK and sdb.clients.keys() == Set(0, 1))
            .then(runC([ RSetclientidConfirm({ client: 1, tok: 101 }) ]))
            .expect(lastStatus == OK)
            .expect(sdb.clients.keys() == Set(1) and
                    sdb.clients.get(1).confirmed)
            // RENEW of a dead clientid -> STALE_CLIENTID.
            .then(runC([ RRenew(0) ]))
            .expect(lastStatus == E_STALE_CLIENTID)
            .then(runC([ RRenew(1) ]))
            .expect(lastStatus == OK)

    // A confirmed client for the remaining tests.
    run boot40 =
        init
            .then(runC([ RSetclientid({ ownerSym: 1, verfSym: 1, tok: 100 }) ]))
            .then(runC([ RSetclientidConfirm({ client: 0, tok: 100 }) ]))
            .expect(sdb.clients.get(0).confirmed)

    // 4.0 open-owner discipline: first open demands OPEN_CONFIRM; the
    // stateid is unusable until confirmed; wrong confirm seqid kills the
    // open; delegation never accompanies a to-be-confirmed open.
    run openConfirmTest =
        boot40
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 1,
                                 claim: OCNull("f"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 3, deny: 0 }), RGetfh ]))
            .expect(lastStatus == OK)
            .expect(asOpen(resAt(1)).needConfirm)
            .expect(match asOpen(resAt(1)).deleg { | DNone => true | _ => false })
            // Using the unconfirmed stateid -> BAD_STATEID.
            .then(runC([ RPutfh(1),
                         RRead({ sel: SelRef({ sid: 0, kind: KOpen,
                                               argSeq: 0 }),
                                 off: 0, len: 1 }) ]))
            .expect(lastStatus == E_BAD_STATEID)
            // OPEN_CONFIRM with the right owner seqid (open used 1 -> 2).
            .then(runC([ RPutfh(1), ROpenConfirm({ sid: 0, oseq: 2 }) ]))
            .expect(lastStatus == OK and not(sdb.opens.get(0).needConfirm))
            // Now I/O works, and a second OPEN by the same owner does not
            // demand confirmation.
            .then(runC([ RPutfh(1),
                         RRead({ sel: SelRef({ sid: 0, kind: KOpen,
                                               argSeq: 0 }),
                                 off: 0, len: 1 }) ]))
            .expect(lastStatus == OK)
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 3,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == OK and not(asOpen(resAt(1)).needConfirm))

    run badSeqidTest =
        boot40
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 1,
                                 claim: OCNull("f"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 3, deny: 0 }) ]))
            // OPEN_CONFIRM against the wrong current filehandle ->
            // BAD_STATEID (which does not consume the owner seqid).
            .then(runC([ RPutrootfh, ROpenConfirm({ sid: 0, oseq: 2 }) ]))
            .expect(lastStatus == E_BAD_STATEID)
            .then(runC([ RPutfh(1), ROpenConfirm({ sid: 0, oseq: 2 }) ]))
            .expect(lastStatus == OK)
            // Skipped owner seqid -> BAD_SEQID, and the failed op does NOT
            // consume a seqid: the next in-sequence op still works.
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 9,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == E_BAD_SEQID)
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 1, oseq: 3,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == OK)
            // An error that consumes the seqid: SHARE_DENIED from a
            // deny-conflicting second owner.
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 2, oseq: 1,
                                 claim: OCNull("f"), how: HNoCreate,
                                 access: 1, deny: 3 }) ]))
            .expect(lastStatus == E_SHARE_DENIED)
            // owner 2's next request must use seqid 2 (the SHARE_DENIED
            // consumed 1)...
            .then(runC([ RPutrootfh,
                         ROpen({ client: 0, owner: 2, oseq: 2,
                                 claim: OCNull("g"),
                                 how: HGuarded({ mode: 420 }),
                                 access: 1, deny: 0 }) ]))
            .expect(lastStatus == OK)

    // 4.1-only ops inside a 4.0 compound are unknown opcodes.
    run minorGateTest =
        boot40
            .then(runC([ RExchangeId({ ownerSym: 2, verfSym: 1 }) ]))
            .expect(lastStatus == E_OP_ILLEGAL)
            .then(runC([ RPutrootfh, RGetxattr("xa") ]))
            .expect(lastStatus == E_OP_ILLEGAL)

    run randomWalk40Test =
        init.then(120.reps(_ => all { inv, step }))
}
