// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// NFSv4 COMPOUND evaluator.
//
// `OpReq` describes one operation of a COMPOUND request; `OpRes` the reply
// the server must produce for it.  `evalCompound` interprets a request list
// with the real COMPOUND semantics: current/saved filehandle threading,
// first-error-stops, per-op status, compound status = last executed op's
// status (RFC 7530 14.2/15.2.4; RFC 8881 16.2), 4.1 session gating
// (SEQUENCE-first, session-exempt singleton ops), and the 4.0 open-owner
// seqid discipline.
//
// The module is pure: the top-level model (nfs4.qnt) owns the state
// variables and calls evalCompound from its actions; nfs4Test.qnt drives
// it directly with hand-built compounds.
//
// Conventions:
//  - Inos, client ids, session ids and stateid indices are model handles;
//    the harness learns the wire values (filehandles, clientid4,
//    sessionid4, stateid4.other) from replies (GETFH, SETCLIENTID,
//    EXCHANGE_ID, CREATE_SESSION, OPEN, LOCK, LAYOUTGET...).
//  - Stateid *seqids* are predicted exactly; `argSeq` 0 in a request means
//    the wildcard "current seqid" (RFC 7530 9.1.4.4 / RFC 8881 8.2.2).
//  - Where the RFCs leave a server choice, the choice is pinned by the
//    capability/policy profile (`Caps`) and annotated `// discretion:`;
//    where two statuses are both conformant the modeled one is annotated
//    `// acceptance:` with the alternative, for the harness's registry.

module nfs4ops {

    import nfs4fs.* from "./nfs4_fs"
    import nfs4state.* from "./nfs4_state"

    // ------------------------------------------------------------------
    // nfsstat4 (RFC 7530 13.1 / RFC 8881 15.1; numerically the wire values)
    // ------------------------------------------------------------------

    pure val OK = 0
    pure val E_PERM = 1
    pure val E_NOENT = 2
    pure val E_IO = 5
    pure val E_NXIO = 6
    pure val E_ACCESS = 13
    pure val E_EXIST = 17
    pure val E_XDEV = 18
    pure val E_NOTDIR = 20
    pure val E_ISDIR = 21
    pure val E_INVAL = 22
    pure val E_FBIG = 27
    pure val E_NOSPC = 28
    pure val E_ROFS = 30
    pure val E_MLINK = 31
    pure val E_NAMETOOLONG = 63
    pure val E_NOTEMPTY = 66
    pure val E_DQUOT = 69
    pure val E_STALE = 70
    pure val E_BADHANDLE = 10001
    pure val E_BAD_COOKIE = 10003
    pure val E_NOTSUPP = 10004
    pure val E_TOOSMALL = 10005
    pure val E_SERVERFAULT = 10006
    pure val E_BADTYPE = 10007
    pure val E_DELAY = 10008
    pure val E_SAME = 10009
    pure val E_DENIED = 10010
    pure val E_EXPIRED = 10011
    pure val E_LOCKED = 10012
    pure val E_GRACE = 10013
    pure val E_FHEXPIRED = 10014
    pure val E_SHARE_DENIED = 10015
    pure val E_WRONGSEC = 10016
    pure val E_CLID_INUSE = 10017
    pure val E_RESOURCE = 10018
    pure val E_MOVED = 10019
    pure val E_NOFILEHANDLE = 10020
    pure val E_MINOR_VERS_MISMATCH = 10021
    pure val E_STALE_CLIENTID = 10022
    pure val E_STALE_STATEID = 10023
    pure val E_OLD_STATEID = 10024
    pure val E_BAD_STATEID = 10025
    pure val E_BAD_SEQID = 10026
    pure val E_NOT_SAME = 10027
    pure val E_LOCK_RANGE = 10028
    pure val E_SYMLINK = 10029
    pure val E_RESTOREFH = 10030
    pure val E_LEASE_MOVED = 10031
    pure val E_ATTRNOTSUPP = 10032
    pure val E_NO_GRACE = 10033
    pure val E_RECLAIM_BAD = 10034
    pure val E_RECLAIM_CONFLICT = 10035
    pure val E_BADXDR = 10036
    pure val E_LOCKS_HELD = 10037
    pure val E_OPENMODE = 10038
    pure val E_BADOWNER = 10039
    pure val E_BADCHAR = 10040
    pure val E_BADNAME = 10041
    pure val E_BAD_RANGE = 10042
    pure val E_LOCK_NOTSUPP = 10043
    pure val E_OP_ILLEGAL = 10044
    pure val E_DEADLOCK = 10045
    pure val E_FILE_OPEN = 10046
    pure val E_ADMIN_REVOKED = 10047
    pure val E_CB_PATH_DOWN = 10048
    // NFSv4.1 (RFC 8881 15.1)
    pure val E_BADIOMODE = 10049
    pure val E_BADLAYOUT = 10050
    pure val E_BADSESSION = 10052
    pure val E_BADSLOT = 10053
    pure val E_COMPLETE_ALREADY = 10054
    pure val E_CONN_NOT_BOUND = 10055
    pure val E_DELEG_ALREADY_WANTED = 10056
    pure val E_BACK_CHAN_BUSY = 10057
    pure val E_LAYOUTTRYLATER = 10058
    pure val E_LAYOUTUNAVAILABLE = 10059
    pure val E_NOMATCHING_LAYOUT = 10060
    pure val E_RECALLCONFLICT = 10061
    pure val E_UNKNOWN_LAYOUTTYPE = 10062
    pure val E_SEQ_MISORDERED = 10063
    pure val E_SEQUENCE_POS = 10064
    pure val E_REQ_TOO_BIG = 10065
    pure val E_REP_TOO_BIG = 10066
    pure val E_REP_TOO_BIG_TO_CACHE = 10067
    pure val E_RETRY_UNCACHED_REP = 10068
    pure val E_UNSAFE_COMPOUND = 10069
    pure val E_TOO_MANY_OPS = 10070
    pure val E_OP_NOT_IN_SESSION = 10071
    pure val E_HASH_ALG_UNSUPP = 10072
    pure val E_CLIENTID_BUSY = 10074
    pure val E_PNFS_IO_HOLE = 10075
    pure val E_SEQ_FALSE_RETRY = 10076
    pure val E_BAD_HIGH_SLOT = 10077
    pure val E_DEADSESSION = 10078
    pure val E_ENCR_ALG_UNSUPP = 10079
    pure val E_PNFS_NO_LAYOUT = 10080
    pure val E_NOT_ONLY_OP = 10081
    pure val E_WRONG_CRED = 10082
    pure val E_WRONG_TYPE = 10083
    pure val E_DIRDELEG_UNAVAIL = 10084
    pure val E_REJECT_DELEG = 10085
    pure val E_RETURNCONFLICT = 10086
    pure val E_DELEG_REVOKED = 10087
    // NFSv4.2 (RFC 7862 11.2) and xattrs (RFC 8276 8.2)
    pure val E_PARTNER_NOTSUPP = 10088
    pure val E_PARTNER_NO_AUTH = 10089
    pure val E_UNION_NOTSUPP = 10090
    pure val E_OFFLOAD_DENIED = 10091
    pure val E_WRONG_LFS = 10092
    pure val E_BADLABEL = 10093
    pure val E_OFFLOAD_NO_REQS = 10094
    pure val E_NOXATTR = 10095
    pure val E_XATTR2BIG = 10096

    // ------------------------------------------------------------------
    // Feature configuration and capability / policy profile
    // ------------------------------------------------------------------

    // Operator-facing mode for every optional NFSv4 feature:
    //  - FeatOff: never exercised; traces contain no dependent steps.
    //  - FeatIfAdvertised (default): init nondeterministically assumes the
    //    server does or does not support the feature; the corpus covers
    //    both, and the harness skips traces whose assumption mismatches
    //    the live server's advertisement (reported not-applicable).
    //  - FeatMandatory: the capability is fixed true; at replay time a
    //    server that fails to advertise it FAILS the suite instead of
    //    vacuously passing.
    type FeatureMode = FeatOff | FeatIfAdvertised | FeatMandatory

    pure def capChoices(m: FeatureMode): Set[bool] =
        match m {
            | FeatOff => Set(false)
            | FeatIfAdvertised => Set(false, true)
            | FeatMandatory => Set(true)
        }

    // Assumed at init (per the FeatureMode configuration, see nfs4.qnt) and
    // embedded in the trace.  The harness reconciles it against the live
    // server's advertisement/observed policy at replay time.
    type Caps = {
        // pNFS MDS advertised (EXCHGID4_FLAG_USE_PNFS_MDS + fs_layout_types);
        // false: every pNFS op is NFS4ERR_NOTSUPP (RFC 8881 13.1).
        pnfs: bool,
        // Delegation grant policy under the canonical conditions (grants
        // are always discretionary: RFC 7530 10.4 / RFC 8881 10.4).
        readDeleg: bool,
        writeDeleg: bool,
        // RFC 8276 xattr ops supported (xattr_support attribute).
        xattr: bool,
        // RFC 7862 ALLOCATE/DEALLOCATE/SEEK supported (per-op NOTSUPP).
        sparse: bool,
        // RFC 7862 intra-server synchronous COPY supported.
        copy: bool,
        // discretion: a conflicting op during delegation recall returns
        // NFS4ERR_DELAY (true) or blocks until the recall completes
        // (false).  Both are conformant (RFC 8881 10.4.4); only the DELAY
        // profile generates linear traces.
        conflictDelays: bool,
        // discretion: CLOSE with byte-range locks still held under the
        // open either frees them (true) or fails NFS4ERR_LOCKS_HELD
        // (false) -- (free) XOR (LOCKS_HELD) per RFC 7530 9.10 /
        // RFC 8881 9.8.
        closeFreesLocks: bool
    }

    // ------------------------------------------------------------------
    // Request / reply shapes
    // ------------------------------------------------------------------

    // Reference to a stateid the harness has learned: model index + class
    // + the seqid to put on the wire (0 = wildcard current).
    type SidRef = { sid: Sid, kind: SidKind, argSeq: int }

    // Stateid selector for I/O-class ops (READ/WRITE/SETATTR/SEEK/...).
    type SidSel =
        | SelAnon                  // all-zero anonymous stateid
        | SelBypass                // all-ones READ-bypass stateid
        | SelRef(SidRef)

    type OpenClaim =
        | OCNull(Name)             // CLAIM_NULL: cfh = dir, named child
        | OCFh                     // CLAIM_FH (4.1+): cfh = the file

    type OpenHow =
        | HNoCreate
        | HUnchecked({ mode: int, truncate: bool })
        | HGuarded({ mode: int })
        | HExclusive({ verf: int })

    type XattrSetHow = XsCreate | XsReplace | XsEither

    type OpReq =
        | RPutrootfh
        | RPutfh(Ino)
        | RGetfh
        | RSavefh
        | RRestorefh
        | RLookup(Name)
        | RLookupp
        | RGetattr
        | RReadlink
        | RAccess(int)
        | RReaddir
        | RCreate({ ctype: Ftype, name: Name, mode: int, target: str })
        | RRemove(Name)
        | RRename({ oldname: Name, newname: Name })
        | RLink(Name)
        | RSetattr({ mode: int, sizeBlocks: int, sel: SidSel })
        | RVerifySize(int)
        | RNverifySize(int)
        | RSetclientid({ ownerSym: int, verfSym: int, tok: int })
        | RSetclientidConfirm({ client: ClientId, tok: int })
        | RRenew(ClientId)
        | RReleaseLockowner({ client: ClientId, lockOwner: int })
        | RExchangeId({ ownerSym: int, verfSym: int })
        | RCreateSession({ client: ClientId, csaSeq: int, backChan: bool })
        | RSequence({ sess: SessId, slot: int, seq: int, cacheThis: bool })
        | RDestroySession(SessId)
        | RDestroyClientid(ClientId)
        | RReclaimComplete
        | RFreeStateid(SidRef)
        | ROpen({ client: ClientId, owner: int, oseq: int, claim: OpenClaim,
                  how: OpenHow, access: int, deny: int })
        | ROpenConfirm({ sid: Sid, oseq: int })
        | ROpenDowngrade({ sid: Sid, argSeq: int, access: int, deny: int,
                           oseq: int })
        | RClose({ sid: Sid, argSeq: int, oseq: int })
        | RRead({ sel: SidSel, off: int, len: int })
        | RWrite({ sel: SidSel, off: int, len: int, pat: int, stable: int })
        | RCommit
        | RLock({ wr: bool, lo: int, hi: int, newOwner: bool,
                  openSid: Sid, lockSid: Sid, lockOwner: int,
                  oseq: int, lseq: int })
        | RLockt({ client: ClientId, lockOwner: int, wr: bool, lo: int, hi: int })
        | RLocku({ sid: Sid, argSeq: int, lseq: int, lo: int, hi: int })
        | RDelegreturn(SidRef)
        | RLayoutget({ rw: bool, lo: int, hi: int, sel: SidSel })
        | RLayoutreturn({ sid: Sid, argSeq: int, lo: int, hi: int })
        | RLayoutcommit({ sid: Sid, lo: int, hi: int })
        | RGetdeviceinfo
        | RAllocate({ sel: SidSel, off: int, len: int })
        | RDeallocate({ sel: SidSel, off: int, len: int })
        | RSeek({ sel: SidSel, off: int, whatData: bool })
        | RCopy({ srcSel: SidSel, dstSel: SidSel,
                  srcOff: int, dstOff: int, cnt: int })
        | RGetxattr(Name)
        | RSetxattr({ name: Name, sym: int, how: XattrSetHow })
        | RListxattrs
        | RRemovexattr(Name)

    // Delegation granted in an OPEN reply.
    type DelegRes =
        | DNone
        | DRead({ sid: Sid })
        | DWrite({ sid: Sid })

    // Attribute snapshot for GETATTR-class checks.  change is the abstract
    // change value (harness checks consistency, not raw equality).
    type AttrSnap = { ftype: Ftype, mode: int, nlink: int,
                      sizeBlocks: int, change: int }

    type OpRes =
        | SPutrootfh({ st: int })
        | SPutfh({ st: int })
        | SGetfh({ st: int, ino: Ino })
        | SSavefh({ st: int })
        | SRestorefh({ st: int })
        | SLookup({ st: int, child: Ino })
        | SLookupp({ st: int, parent: Ino })
        | SGetattr({ st: int, attrs: AttrSnap })
        | SReadlink({ st: int, target: str })
        | SAccess({ st: int, supported: int, access: int })
        | SReaddir({ st: int, names: Set[Name] })
        | SCreate({ st: int, ino: Ino, cinfo: CInfo })
        | SRemove({ st: int, cinfo: CInfo })
        | SRename({ st: int, cinfoS: CInfo, cinfoT: CInfo })
        | SLink({ st: int, cinfo: CInfo })
        | SSetattr({ st: int })
        | SVerify({ st: int })
        | SNverify({ st: int })
        | SSetclientid({ st: int, client: ClientId, tok: int })
        | SSetclientidConfirm({ st: int })
        | SRenew({ st: int })
        | SReleaseLockowner({ st: int })
        | SExchangeId({ st: int, client: ClientId, csSeq: int,
                        confirmedR: bool, pnfsMds: bool })
        | SCreateSession({ st: int, sess: SessId })
        | SSequence({ st: int, replay: bool })
        | SDestroySession({ st: int })
        | SDestroyClientid({ st: int })
        | SReclaimComplete({ st: int })
        | SFreeStateid({ st: int })
        // recalls: delegation sids for which the harness must observe a
        // CB_RECALL before this op can eventually succeed (the op itself
        // returns NFS4ERR_DELAY under the conflictDelays profile).
        | SOpen({ st: int, sid: Sid, seq: int, needConfirm: bool,
                  cinfo: CInfo, deleg: DelegRes, recalls: Set[Sid] })
        | SOpenConfirm({ st: int, seq: int })
        | SOpenDowngrade({ st: int, seq: int })
        | SClose({ st: int })
        | SRead({ st: int, count: int, eof: bool, blocks: List[int] })
        | SWrite({ st: int, count: int, newSizeBlocks: int })
        | SCommit({ st: int })
        // denied: the owner symbol of one conflicting lock (-1 if OK).
        // acceptance: which conflicting lock a server reports when several
        // exist is discretionary; the harness checks the reported conflict
        // is real rather than comparing owners exactly.
        | SLock({ st: int, sid: Sid, seq: int, deniedOwner: int })
        | SLockt({ st: int, deniedOwner: int })
        | SLocku({ st: int, seq: int })
        | SDelegreturn({ st: int })
        | SLayoutget({ st: int, sid: Sid, seq: int, lo: int, hi: int })
        | SLayoutreturn({ st: int, present: bool, seq: int })
        | SLayoutcommit({ st: int, newSizeBlocks: int })
        | SGetdeviceinfo({ st: int })
        | SAllocate({ st: int })
        | SDeallocate({ st: int })
        | SSeek({ st: int, eof: bool, offset: int })
        | SCopy({ st: int, copied: int })
        | SGetxattr({ st: int, sym: int })
        | SSetxattr({ st: int })
        | SListxattrs({ st: int, names: Set[Name] })
        | SRemovexattr({ st: int })

    // ------------------------------------------------------------------
    // Evaluation accumulator
    // ------------------------------------------------------------------

    type Acc = {
        fs: Fs,
        sdb: StateDB,
        minor: int,
        caps: Caps,
        nOps: int,          // total ops in the compound (for NOT_ONLY_OP)
        reqTag: int,        // identity of this compound (SEQUENCE replay)
        idx: int,           // index of the op being evaluated
        cfh: Ino,           // -1 = unset
        sfh: Ino,           // -1 = unset
        seqDone: bool,      // a SEQUENCE op has been executed
        seqClient: ClientId,// client of the SEQUENCE's session (4.1)
        done: bool,         // stop: an op returned non-OK (or replay)
        status: int,        // status of the last executed op
        results: List[OpRes]
    }

    pure def statusOf(r: OpRes): int =
        match r {
            | SPutrootfh(x) => x.st
            | SPutfh(x) => x.st
            | SGetfh(x) => x.st
            | SSavefh(x) => x.st
            | SRestorefh(x) => x.st
            | SLookup(x) => x.st
            | SLookupp(x) => x.st
            | SGetattr(x) => x.st
            | SReadlink(x) => x.st
            | SAccess(x) => x.st
            | SReaddir(x) => x.st
            | SCreate(x) => x.st
            | SRemove(x) => x.st
            | SRename(x) => x.st
            | SLink(x) => x.st
            | SSetattr(x) => x.st
            | SVerify(x) => x.st
            | SNverify(x) => x.st
            | SSetclientid(x) => x.st
            | SSetclientidConfirm(x) => x.st
            | SRenew(x) => x.st
            | SReleaseLockowner(x) => x.st
            | SExchangeId(x) => x.st
            | SCreateSession(x) => x.st
            | SSequence(x) => x.st
            | SDestroySession(x) => x.st
            | SDestroyClientid(x) => x.st
            | SReclaimComplete(x) => x.st
            | SFreeStateid(x) => x.st
            | SOpen(x) => x.st
            | SOpenConfirm(x) => x.st
            | SOpenDowngrade(x) => x.st
            | SClose(x) => x.st
            | SRead(x) => x.st
            | SWrite(x) => x.st
            | SCommit(x) => x.st
            | SLock(x) => x.st
            | SLockt(x) => x.st
            | SLocku(x) => x.st
            | SDelegreturn(x) => x.st
            | SLayoutget(x) => x.st
            | SLayoutreturn(x) => x.st
            | SLayoutcommit(x) => x.st
            | SGetdeviceinfo(x) => x.st
            | SAllocate(x) => x.st
            | SDeallocate(x) => x.st
            | SSeek(x) => x.st
            | SCopy(x) => x.st
            | SGetxattr(x) => x.st
            | SSetxattr(x) => x.st
            | SListxattrs(x) => x.st
            | SRemovexattr(x) => x.st
        }

    // Append a result; halt the compound when it is an error
    // (first-error-stops, RFC 7530 14.2 / RFC 8881 16.2.4 -- this applies
    // even to "non-error" statuses like NFS4ERR_SAME).
    pure def emit(a: Acc, r: OpRes): Acc = {
        val st = statusOf(r)
        a.with("results", a.results.append(r))
         .with("status", st)
         .with("done", st != OK)
    }

    pure def cfhLive(a: Acc): bool = a.cfh != -1 and fsLive(a.fs, a.cfh)
    pure def cfhNode(a: Acc): Inode = fsGet(a.fs, a.cfh)

    // Shared filehandle gate: NOFILEHANDLE when unset (RFC 7530 13.1.2.5),
    // STALE when the object has been removed (RFC 7530 13.1.2.7; checked
    // at the op that uses the handle -- PUTFH may defer, R-CORE-102).
    // Returns OK when the cfh is usable.
    pure def fhGate(a: Acc): int =
        if (a.cfh == -1) E_NOFILEHANDLE
        else if (not(fsLive(a.fs, a.cfh))) E_STALE
        else OK

    // Map an abstract filesystem status onto nfsstat4.
    pure def fsStatus(st: FsStatus): int =
        match st {
            | FsOk => OK
            | FsNoent => E_NOENT
            | FsExist => E_EXIST
            | FsNotdir => E_NOTDIR
            | FsIsdir => E_ISDIR
            | FsNotempty => E_NOTEMPTY
            | FsInval => E_INVAL
            | FsNoxattr => E_NOXATTR
        }

    pure def snapOf(nd: Inode): AttrSnap =
        { ftype: nd.ftype, mode: nd.mode, nlink: nd.nlink,
          sizeBlocks: nd.sizeBlocks, change: nd.change }

    // Inodes that must survive losing their last name: files with opens,
    // delegations or layouts stay reachable through their filehandle until
    // the state goes away (RFC 7530 16.26.5 / RFC 8881 18.25.4).
    pure def pinnedInos(sdb: StateDB): Set[Ino] =
        sdb.opens.keys().map(s => sdb.opens.get(s).file)
            .union(sdb.delegs.keys().map(s => sdb.delegs.get(s).file))
            .union(sdb.layouts.keys().map(s => sdb.layouts.get(s).file))

    // Reap an orphan once its last pin is gone (called after CLOSE /
    // DELEGRETURN / final LAYOUTRETURN).
    pure def maybeReap(fs: Fs, sdb: StateDB, f: Ino): Fs =
        if (fsLive(fs, f) and fsGet(fs, f).nlink == 0 and
            not(pinnedInos(sdb).contains(f)))
            fsReap(fs, f)
        else fs

    // Sweep every unpinned orphan: needed after bulk state removal (a
    // client reboot purging its old incarnation's opens releases every
    // pin that incarnation held).
    pure def reapOrphans(fs: Fs, sdb: StateDB): Fs = {
        val dead = fs.inodes.keys().filter(i =>
            fsGet(fs, i).ftype != FDir and fsGet(fs, i).nlink == 0 and
            not(pinnedInos(sdb).contains(i)))
        fs.with("inodes", fs.inodes.keys().exclude(dead)
              .mapBy(i => fs.inodes.get(i)))
          .with("removed", fs.removed.union(dead))
    }

    // ------------------------------------------------------------------
    // 4.0 owner-seqid discipline (RFC 7530 9.1.7)
    // ------------------------------------------------------------------

    // Errors that do NOT consume an owner seqid (RFC 7530 9.1.7).
    pure def consumesSeqid(st: int): bool =
        not(Set(E_STALE_CLIENTID, E_STALE_STATEID, E_BAD_STATEID,
                E_BAD_SEQID, E_BADXDR, E_RESOURCE, E_NOFILEHANDLE,
                E_MOVED).contains(st))

    // Gate an open-owner-sequenced op (OPEN/OPEN_CONFIRM/OPEN_DOWNGRADE/
    // CLOSE).  A brand-new owner is accepted with any seqid (SHOULD be 1);
    // an existing owner must present last+1.  Replays (r == last) are not
    // generated by this model (deferred with the DRC work) and are treated
    // as BAD_SEQID here.
    pure def ooGateOk(sdb: StateDB, cid: ClientId, owner: int, oseq: int): bool = {
        val k = (cid, owner)
        if (not(sdb.oo40.keys().contains(k))) true
        else oseq == sdb.oo40.get(k).seqid + 1
    }

    // Record the outcome of a sequenced op: advance the owner seqid iff the
    // status consumes it; create the owner record on first use.
    pure def ooSettle(sdb: StateDB, cid: ClientId, owner: int, oseq: int,
                      st: int, nowConfirmed: bool): StateDB = {
        val k = (cid, owner)
        if (not(consumesSeqid(st))) sdb
        else if (sdb.oo40.keys().contains(k))
            sdb.with("oo40", sdb.oo40.setBy(k, x =>
                { seqid: oseq, confirmed: x.confirmed or nowConfirmed }))
        else
            sdb.with("oo40", sdb.oo40.put(k,
                { seqid: oseq, confirmed: nowConfirmed }))
    }

    pure def ooConfirmed(sdb: StateDB, cid: ClientId, owner: int): bool = {
        val k = (cid, owner)
        sdb.oo40.keys().contains(k) and sdb.oo40.get(k).confirmed
    }

    // Lock-owner seqid gate (4.0), same shape.
    pure def loGateOk(sdb: StateDB, cid: ClientId, owner: int, lseq: int): bool = {
        val k = (cid, owner)
        if (not(sdb.lo40.keys().contains(k))) true
        else lseq == sdb.lo40.get(k).seqid + 1
    }

    pure def loSettle(sdb: StateDB, cid: ClientId, owner: int, lseq: int,
                      st: int): StateDB = {
        val k = (cid, owner)
        if (not(consumesSeqid(st))) sdb
        else if (sdb.lo40.keys().contains(k))
            sdb.with("lo40", sdb.lo40.setBy(k, x => { seqid: lseq }))
        else
            sdb.with("lo40", sdb.lo40.put(k, { seqid: lseq }))
    }

    // ------------------------------------------------------------------
    // Stateid selector resolution for I/O ops
    // ------------------------------------------------------------------

    type IoSid =
        | IoAnon
        | IoBypass
        | IoState({ sid: Sid, kind: SidKind })
        | IoError(int)

    // Resolve an I/O stateid selector against the tables.  Checks:
    // liveness + seqid window (BAD/OLD_STATEID), the state's file must be
    // the current filehandle (RFC 7530 9.1.4.4 / RFC 8881 8.2.4), and 4.0
    // unconfirmed opens are unusable (RFC 7530 16.18.5).
    pure def resolveSel(a: Acc, sel: SidSel): IoSid =
        match sel {
            | SelAnon => IoAnon
            | SelBypass => IoBypass
            | SelRef(r) =>
                match checkSid(a.sdb, r.sid, r.kind, r.argSeq) {
                    | SidBad => IoError(E_BAD_STATEID)
                    | SidOld => IoError(E_OLD_STATEID)
                    | SidOk => {
                        val fOf = match r.kind {
                            | KOpen => a.sdb.opens.get(r.sid).file
                            | KLock => a.sdb.locks.get(r.sid).file
                            | KDeleg => a.sdb.delegs.get(r.sid).file
                            | KLayout => a.sdb.layouts.get(r.sid).file
                        }
                        if (fOf != a.cfh) IoError(E_BAD_STATEID)
                        else if (r.kind == KOpen and
                                 a.sdb.opens.get(r.sid).needConfirm)
                            IoError(E_BAD_STATEID)
                        else IoState({ sid: r.sid, kind: r.kind })
                    }
                }
        }

    // Access mode the stateid carries for I/O checks: 1 read, 2 write,
    // 3 both.  Delegations: read deleg = 1, write deleg = 3; lock stateids
    // inherit their open's mode.
    pure def ioMode(a: Acc, io: IoSid): int =
        match io {
            | IoState(s) =>
                match s.kind {
                    | KOpen => a.sdb.opens.get(s.sid).access
                    | KLock => a.sdb.opens.get(a.sdb.locks.get(s.sid).openSid).access
                    | KDeleg => if (a.sdb.delegs.get(s.sid).isWrite) 3 else 1
                    | KLayout => 0
                }
            | IoAnon => 3
            | IoBypass => 3
            | IoError(e) => 0
        }

    // Does any current open of file f deny the given I/O kind (1 = read,
    // 2 = write)?  Anonymous-stateid I/O against a deny mode fails
    // NFS4ERR_LOCKED (RFC 7530 9.1.4.3 / RFC 8881 8.2.3).
    pure def anyDeny(sdb: StateDB, f: Ino, kindBit: int): bool =
        sdb.opens.keys().exists(s => {
            val o = sdb.opens.get(s)
            o.file == f and bitsOverlap(o.deny, kindBit)
        })

    // ------------------------------------------------------------------
    // Per-op handlers
    // ------------------------------------------------------------------

    pure def opPutrootfh(a: Acc): Acc =
        emit(a.with("cfh", ROOT), SPutrootfh({ st: OK }))

    // The generator only presents live inos; a removed ino would surface
    // STALE here or at first use (R-CORE-102 leaves the position open).
    pure def opPutfh(a: Acc, i: Ino): Acc =
        emit(a.with("cfh", i), SPutfh({ st: OK }))

    pure def opGetfh(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SGetfh({ st: g, ino: -1 }))
        else emit(a, SGetfh({ st: OK, ino: a.cfh }))
    }

    pure def opSavefh(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SSavefh({ st: g }))
        else emit(a.with("sfh", a.cfh), SSavefh({ st: OK }))
    }

    // RESTOREFH: sfh copied to cfh, sfh survives (RFC 7530 16.29.4).
    pure def opRestorefh(a: Acc): Acc =
        if (a.sfh == -1) emit(a, SRestorefh({ st: E_RESTOREFH }))
        else emit(a.with("cfh", a.sfh), SRestorefh({ st: OK }))

    // LOOKUP (RFC 7530 16.13 / RFC 8881 18.13): single component; cfh a
    // symlink -> NFS4ERR_SYMLINK, other non-directory -> NFS4ERR_NOTDIR.
    pure def opLookup(a: Acc, n: Name): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SLookup({ st: g, child: -1 }))
        else {
            val nd = cfhNode(a)
            if (nd.ftype == FLnk)
                emit(a, SLookup({ st: E_SYMLINK, child: -1 }))
            else if (nd.ftype != FDir)
                emit(a, SLookup({ st: E_NOTDIR, child: -1 }))
            else if (not(fsHasEnt(a.fs, a.cfh, n)))
                emit(a, SLookup({ st: E_NOENT, child: -1 }))
            else {
                val c = fsChild(a.fs, a.cfh, n)
                emit(a.with("cfh", c), SLookup({ st: OK, child: c }))
            }
        }
    }

    // LOOKUPP (RFC 7530 16.14 / RFC 8881 18.14): parent of cfh; NOENT at
    // the namespace root.  acceptance: for a symlink cfh either NOTDIR
    // (op text) or SYMLINK (error table) is conformant; NOTDIR modeled.
    pure def opLookupp(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SLookupp({ st: g, parent: -1 }))
        else {
            val nd = cfhNode(a)
            if (nd.ftype != FDir)
                emit(a, SLookupp({ st: E_NOTDIR, parent: -1 }))
            else if (a.cfh == ROOT)
                emit(a, SLookupp({ st: E_NOENT, parent: -1 }))
            else
                emit(a.with("cfh", nd.parent),
                     SLookupp({ st: OK, parent: nd.parent }))
        }
    }

    pure def opGetattr(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK)
            emit(a, SGetattr({ st: g, attrs: { ftype: FReg, mode: 0, nlink: 0,
                                               sizeBlocks: 0, change: 0 } }))
        else emit(a, SGetattr({ st: OK, attrs: snapOf(cfhNode(a)) }))
    }

    // READLINK (RFC 7530 16.25 / RFC 8881 18.24): directory -> ISDIR,
    // other non-symlink -> INVAL ("should" wording).
    pure def opReadlink(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SReadlink({ st: g, target: "" }))
        else {
            val nd = cfhNode(a)
            if (nd.ftype == FDir) emit(a, SReadlink({ st: E_ISDIR, target: "" }))
            else if (nd.ftype != FLnk)
                emit(a, SReadlink({ st: E_INVAL, target: "" }))
            else emit(a, SReadlink({ st: OK, target: nd.target }))
        }
    }

    // ACCESS (RFC 7530 16.1 / RFC 8881 18.1): supported must be a subset
    // of the request; access a subset of supported.  discretion: the
    // harness runs as root over AUTH_SYS, and the model predicts
    // access == supported == requested (chimera grants root everything;
    // within RFC discretion, as in the v3 model).
    pure def opAccess(a: Acc, mask: int): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SAccess({ st: g, supported: 0, access: 0 }))
        else emit(a, SAccess({ st: OK, supported: mask, access: mask }))
    }

    // READDIR (RFC 7530 16.24 / RFC 8881 18.23): single-shot with a large
    // maxcount, as in the v3 model; cookie continuation is deferred.
    // "." and ".." are never returned (R-CORE-85).
    pure def opReaddir(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SReaddir({ st: g, names: Set() }))
        else {
            val nd = cfhNode(a)
            if (nd.ftype != FDir) emit(a, SReaddir({ st: E_NOTDIR, names: Set() }))
            else emit(a, SReaddir({ st: OK, names: nd.ents.keys() }))
        }
    }

    // CREATE (RFC 7530 16.4 / RFC 8881 18.4): non-regular types only;
    // NF4REG -> NFS4ERR_BADTYPE.  On success cfh moves to the new object.
    pure def opCreate(a: Acc, ar: { ctype: Ftype, name: Name, mode: int,
                                    target: str }): Acc = {
        val g = fhGate(a)
        val bad = { st: g, ino: -1, cinfo: { before: 0, after: 0 } }
        if (g != OK) emit(a, SCreate(bad))
        else if (ar.ctype == FReg)
            emit(a, SCreate(bad.with("st", E_BADTYPE)))
        else {
            val node =
                if (ar.ctype == FDir) mkDir(ar.mode, a.cfh, 0)
                else if (ar.ctype == FLnk) mkSymlink(ar.target, ar.mode, 0)
                else mkSpecial(ar.ctype, ar.mode, 0)
            val o = fsMknode(a.fs, a.cfh, ar.name, node)
            if (o.st != FsOk)
                emit(a, SCreate(bad.with("st", fsStatus(o.st))))
            else
                emit(a.with("fs", o.fs2).with("cfh", o.ino),
                     SCreate({ st: OK, ino: o.ino, cinfo: o.cinfo }))
        }
    }

    // REMOVE (RFC 7530 16.26 / RFC 8881 18.25): untyped; non-empty
    // directory -> NFS4ERR_NOTEMPTY.  Removal of an open file is
    // permitted (NFS4ERR_FILE_OPEN is a server MAY, R-CORE-71); the
    // object is orphaned while opens/delegations/layouts pin it and
    // reaped at the last CLOSE.
    pure def opRemove(a: Acc, n: Name): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SRemove({ st: g, cinfo: { before: 0, after: 0 } }))
        else {
            // Delegations held by other clients on the victim force a
            // recall; REMOVE observes NFS4ERR_DELAY until they return
            // (probed: nfs4_deleg_probe.c C3, same discipline as OPEN).
            val victim = if (fsHasEnt(a.fs, a.cfh, n))
                             fsChild(a.fs, a.cfh, n) else -1
            val recalls = if (victim == -1) Set() else
                delegsOnFile(a.sdb, victim).filter(s =>
                    a.sdb.delegs.get(s).client != a.seqClient)
            if (recalls.size() > 0) {
                emit(a.with("sdb", recalls.fold(a.sdb, (acc, s) =>
                                                doRecallDeleg(acc, s))),
                     SRemove({ st: E_DELAY, cinfo: { before: 0, after: 0 } }))
            } else {
                val o = fsRemove(a.fs, a.cfh, n, pinnedInos(a.sdb))
                emit(a.with("fs", o.fs2),
                     SRemove({ st: fsStatus(o.st), cinfo: o.cinfo }))
            }
        }
    }

    // RENAME (RFC 7530 16.27 / RFC 8881 18.26): sfh = source dir, cfh =
    // target dir.  acceptance: for a non-empty-directory target the model
    // predicts NOTEMPTY (Table 7 + POSIX servers); the op text's EXIST is
    // also conformant (R-CORE-73).
    pure def opRename(a: Acc, ar: { oldname: Name, newname: Name }): Acc = {
        val zero = { before: 0, after: 0 }
        if (a.sfh == -1 or a.cfh == -1)
            emit(a, SRename({ st: E_NOFILEHANDLE, cinfoS: zero, cinfoT: zero }))
        else if (not(fsLive(a.fs, a.sfh)) or not(fsLive(a.fs, a.cfh)))
            emit(a, SRename({ st: E_STALE, cinfoS: zero, cinfoT: zero }))
        else {
            // Rename-over unlinks the target: other-client delegations on
            // it force a recall + NFS4ERR_DELAY, mirroring REMOVE.  (The
            // source object merely changes name; no conflict.)
            val victim = if (fsHasEnt(a.fs, a.cfh, ar.newname))
                             fsChild(a.fs, a.cfh, ar.newname) else -1
            val recalls = if (victim == -1) Set() else
                delegsOnFile(a.sdb, victim).filter(s =>
                    a.sdb.delegs.get(s).client != a.seqClient)
            if (recalls.size() > 0) {
                emit(a.with("sdb", recalls.fold(a.sdb, (acc, s) =>
                                                doRecallDeleg(acc, s))),
                     SRename({ st: E_DELAY, cinfoS: zero, cinfoT: zero }))
            } else {
                val o = fsRename(a.fs, a.sfh, ar.oldname, a.cfh, ar.newname,
                                 pinnedInos(a.sdb))
                emit(a.with("fs", o.fs2),
                     SRename({ st: fsStatus(o.st), cinfoS: o.cinfoS,
                               cinfoT: o.cinfoT }))
            }
        }
    }

    // LINK (RFC 7530 16.9 / RFC 8881 18.9): sfh = object, cfh = target
    // dir.  acceptance: LINK of a directory is ISDIR (modeled; NOTSUPP
    // also in the valid list, R-CORE-78).
    pure def opLink(a: Acc, n: Name): Acc = {
        val zero = { before: 0, after: 0 }
        if (a.sfh == -1 or a.cfh == -1)
            emit(a, SLink({ st: E_NOFILEHANDLE, cinfo: zero }))
        else if (not(fsLive(a.fs, a.sfh)) or not(fsLive(a.fs, a.cfh)))
            emit(a, SLink({ st: E_STALE, cinfo: zero }))
        else {
            val o = fsLink(a.fs, a.sfh, a.cfh, n)
            emit(a.with("fs", o.fs2), SLink({ st: fsStatus(o.st), cinfo: o.cinfo }))
        }
    }

    // SETATTR (RFC 7530 16.32 / RFC 8881 18.30): mode and/or size.  The
    // stateid argument matters only when size is set (R-ATTR); a size set
    // on a directory is ISDIR in 4.0 and WRONG_TYPE in 4.1+ (per the
    // valid-error tables; the RFCs state no explicit rule).  Size changes
    // via a stateid lacking WRITE access -> NFS4ERR_OPENMODE (MUST,
    // RFC 8881 9.1.2).
    pure def opSetattr(a: Acc, ar: { mode: int, sizeBlocks: int,
                                     sel: SidSel }): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SSetattr({ st: g }))
        else {
            val nd = cfhNode(a)
            if (ar.sizeBlocks >= 0 and nd.ftype != FReg)
                emit(a, SSetattr({ st: if (nd.ftype == FDir and a.minor == 0)
                                           E_ISDIR
                                       else if (a.minor == 0) E_INVAL
                                       else E_WRONG_TYPE }))
            else if (ar.sizeBlocks >= 0) {
                val io = resolveSel(a, ar.sel)
                match io {
                    | IoError(e) => emit(a, SSetattr({ st: e }))
                    | IoAnon =>
                        if (anyDeny(a.sdb, a.cfh, 2))
                            emit(a, SSetattr({ st: E_LOCKED }))
                        else {
                            val fs1 = fsResize(a.fs, a.cfh, ar.sizeBlocks)
                            val fs2 = if (ar.mode >= 0) fsChmod(fs1, a.cfh, ar.mode)
                                      else fs1
                            emit(a.with("fs", fs2), SSetattr({ st: OK }))
                        }
                    | IoBypass =>
                        // READ-bypass acts as anonymous outside READ
                        // (RFC 8881 8.2.3); deny check still applies.
                        if (anyDeny(a.sdb, a.cfh, 2))
                            emit(a, SSetattr({ st: E_LOCKED }))
                        else {
                            val fs1 = fsResize(a.fs, a.cfh, ar.sizeBlocks)
                            val fs2 = if (ar.mode >= 0) fsChmod(fs1, a.cfh, ar.mode)
                                      else fs1
                            emit(a.with("fs", fs2), SSetattr({ st: OK }))
                        }
                    | IoState(s) =>
                        if (not(bitsOverlap(ioMode(a, io), 2)))
                            emit(a, SSetattr({ st: E_OPENMODE }))
                        else {
                            val fs1 = fsResize(a.fs, a.cfh, ar.sizeBlocks)
                            val fs2 = if (ar.mode >= 0) fsChmod(fs1, a.cfh, ar.mode)
                                      else fs1
                            emit(a.with("fs", fs2), SSetattr({ st: OK }))
                        }
                }
            } else if (ar.mode >= 0)
                emit(a.with("fs", fsChmod(a.fs, a.cfh, ar.mode)),
                     SSetattr({ st: OK }))
            else emit(a, SSetattr({ st: OK }))
        }
    }

    // VERIFY/NVERIFY on the size attribute (RFC 7530 16.35/16.15;
    // RFC 8881 18.31/18.15): same -> continue / NFS4ERR_SAME, differ ->
    // NFS4ERR_NOT_SAME / continue.
    pure def opVerifySize(a: Acc, sz: int): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SVerify({ st: g }))
        else if (cfhNode(a).sizeBlocks == sz) emit(a, SVerify({ st: OK }))
        else emit(a, SVerify({ st: E_NOT_SAME }))
    }

    pure def opNverifySize(a: Acc, sz: int): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SNverify({ st: g }))
        else if (cfhNode(a).sizeBlocks == sz) emit(a, SNverify({ st: E_SAME }))
        else emit(a, SNverify({ st: OK }))
    }

    // ------------------------------------------------------------------
    // Client management: 4.0
    // ------------------------------------------------------------------

    pure def opSetclientid(a: Acc, ar: { ownerSym: int, verfSym: int,
                                         tok: int }): Acc =
        if (a.minor != 0)
            // 4.0-only op in a 4.1+ compound: MNI, NFS4ERR_NOTSUPP
            // (RFC 8881 17).
            emit(a, SSetclientid({ st: E_NOTSUPP, client: -1, tok: -1 }))
        else {
            val r = applySetclientid(a.sdb, ar.ownerSym, ar.verfSym, ar.tok)
            emit(a.with("sdb", r.sdb2),
                 SSetclientid({ st: OK, client: r.clientid, tok: r.confirmTok }))
        }

    pure def opSetclientidConfirm(a: Acc, ar: { client: ClientId,
                                                tok: int }): Acc =
        if (a.minor != 0)
            emit(a, SSetclientidConfirm({ st: E_NOTSUPP }))
        else {
            // discretion: cbUp = true -- the harness always registers a
            // live callback service, so the callback probe succeeds.
            val r = applySetclientidConfirm(a.sdb, ar.client, ar.tok, true)
            match r.st {
                | ConfOk =>
                    // Confirming a rebooted client purges the old
                    // incarnation's state; orphans it pinned are reaped.
                    emit(a.with("sdb", r.sdb2)
                          .with("fs", reapOrphans(a.fs, r.sdb2)),
                         SSetclientidConfirm({ st: OK }))
                | ConfStale => emit(a, SSetclientidConfirm({ st: E_STALE_CLIENTID }))
            }
        }

    pure def opRenew(a: Acc, cid: ClientId): Acc =
        if (a.minor != 0) emit(a, SRenew({ st: E_NOTSUPP }))
        else if (not(a.sdb.clients.keys().contains(cid)) or
                 not(a.sdb.clients.get(cid).confirmed))
            emit(a, SRenew({ st: E_STALE_CLIENTID }))
        else emit(a, SRenew({ st: OK }))

    // RELEASE_LOCKOWNER (RFC 7530 16.37): frees the lock-owner's stateids;
    // NFS4ERR_LOCKS_HELD while any byte remains locked.
    pure def opReleaseLockowner(a: Acc, ar: { client: ClientId,
                                              lockOwner: int }): Acc =
        if (a.minor != 0) emit(a, SReleaseLockowner({ st: E_NOTSUPP }))
        else {
            val held = a.sdb.locks.keys().filter(s => {
                val l = a.sdb.locks.get(s)
                l.client == ar.client and l.owner == ar.lockOwner
            })
            if (held.exists(s => a.sdb.locks.get(s).bytes.keys().size() > 0))
                emit(a, SReleaseLockowner({ st: E_LOCKS_HELD }))
            else {
                val sdb2 = a.sdb
                    .with("locks", a.sdb.locks.keys().exclude(held)
                          .mapBy(s => a.sdb.locks.get(s)))
                    .with("freedSids", a.sdb.freedSids.union(held))
                emit(a.with("sdb", sdb2), SReleaseLockowner({ st: OK }))
            }
        }

    // ------------------------------------------------------------------
    // Client management and sessions: 4.1+
    // ------------------------------------------------------------------

    pure def opExchangeId(a: Acc, ar: { ownerSym: int, verfSym: int }): Acc =
        if (a.minor == 0)
            emit(a, SExchangeId({ st: E_OP_ILLEGAL, client: -1, csSeq: 0,
                                  confirmedR: false, pnfsMds: false }))
        else if (a.nOps > 1)
            // Outside a session EXCHANGE_ID must be the only op
            // (RFC 8881 18.35.3); compounds mixing it with SEQUENCE are
            // not generated.
            emit(a, SExchangeId({ st: E_NOT_ONLY_OP, client: -1, csSeq: 0,
                                  confirmedR: false, pnfsMds: false }))
        else {
            val r = applyExchangeId(a.sdb, ar.ownerSym, ar.verfSym)
            emit(a.with("sdb", r.sdb2),
                 SExchangeId({ st: OK, client: r.clientid, csSeq: r.csSeq,
                               confirmedR: r.confirmedR,
                               // EXCHGID4_FLAG_USE_PNFS_MDS vs USE_NON_PNFS
                               // per the capability profile.
                               pnfsMds: a.caps.pnfs }))
        }

    pure def opCreateSession(a: Acc, ar: { client: ClientId, csaSeq: int,
                                           backChan: bool }): Acc =
        if (a.minor == 0)
            emit(a, SCreateSession({ st: E_OP_ILLEGAL, sess: -1 }))
        else if (a.nOps > 1)
            emit(a, SCreateSession({ st: E_NOT_ONLY_OP, sess: -1 }))
        else {
            val r = applyCreateSession(a.sdb, ar.client, ar.csaSeq,
                                       NSLOTS, ar.backChan)
            match r.st {
                | CsOk =>
                    // The confirming CREATE_SESSION of a rebooted client
                    // purges the old incarnation's state (RFC 8881
                    // 18.35.4 case 5); reap orphans it pinned.
                    emit(a.with("sdb", r.sdb2)
                          .with("fs", reapOrphans(a.fs, r.sdb2)),
                         SCreateSession({ st: OK, sess: r.sess }))
                | CsReplay => emit(a, SCreateSession({ st: OK, sess: r.sess }))
                | CsMisordered =>
                    emit(a, SCreateSession({ st: E_SEQ_MISORDERED, sess: -1 }))
                | CsStale =>
                    emit(a, SCreateSession({ st: E_STALE_CLIENTID, sess: -1 }))
            }
        }

    // Fore-channel slot count the model's sessions use (the harness
    // negotiates at least this many via ca_maxrequests).
    pure val NSLOTS = 4

    pure def opSequence(a: Acc, ar: { sess: SessId, slot: int, seq: int,
                                      cacheThis: bool }): Acc =
        if (a.minor == 0)
            emit(a, SSequence({ st: E_OP_ILLEGAL, replay: false }))
        else if (a.idx != 0)
            // SEQUENCE anywhere but first -> NFS4ERR_SEQUENCE_POS
            // (RFC 8881 18.46.3).
            emit(a, SSequence({ st: E_SEQUENCE_POS, replay: false }))
        else {
            val r = applySequence(a.sdb, ar.sess, ar.slot, ar.seq,
                                  a.reqTag, ar.cacheThis)
            match r.st {
                | SeqNew => {
                    val cl = a.sdb.sessions.get(ar.sess).client
                    emit(a.with("sdb", r.sdb2).with("seqDone", true)
                          .with("seqClient", cl),
                         SSequence({ st: OK, replay: false }))
                }
                | SeqReplay =>
                    // The whole compound is a replay: the server answers
                    // from the slot's reply cache.  The harness compares
                    // against the reply it recorded for this slot/seq; the
                    // model stops predicting here (results after SEQUENCE
                    // are the cached ones).
                    emit(a, SSequence({ st: OK, replay: true }))
                      .with("done", true)
                | SeqFalseRetry =>
                    emit(a, SSequence({ st: E_SEQ_FALSE_RETRY, replay: false }))
                | SeqMisordered =>
                    emit(a, SSequence({ st: E_SEQ_MISORDERED, replay: false }))
                | SeqBadSession =>
                    emit(a, SSequence({ st: E_BADSESSION, replay: false }))
                | SeqBadSlot =>
                    emit(a, SSequence({ st: E_BADSLOT, replay: false }))
            }
        }

    pure def opDestroySession(a: Acc, sess: SessId): Acc =
        if (a.minor == 0) emit(a, SDestroySession({ st: E_OP_ILLEGAL }))
        else {
        val r = applyDestroySession(a.sdb, sess)
        if (r.ok) emit(a.with("sdb", r.sdb2), SDestroySession({ st: OK }))
        else emit(a, SDestroySession({ st: E_BADSESSION }))
        }

    pure def opDestroyClientid(a: Acc, cid: ClientId): Acc =
        if (a.minor == 0) emit(a, SDestroyClientid({ st: E_OP_ILLEGAL }))
        else if (a.nOps > 1)
            emit(a, SDestroyClientid({ st: E_NOT_ONLY_OP }))
        else if (not(a.sdb.clients.keys().contains(cid)))
            emit(a, SDestroyClientid({ st: E_STALE_CLIENTID }))
        else if (clientBusy(a.sdb, cid))
            emit(a, SDestroyClientid({ st: E_CLIENTID_BUSY }))
        else
            emit(a.with("sdb", purgeClient(a.sdb, cid)),
                 SDestroyClientid({ st: OK }))

    // RECLAIM_COMPLETE (RFC 8881 18.51, rca_one_fs = false): once per
    // client; a repeat is NFS4ERR_COMPLETE_ALREADY.
    pure def opReclaimComplete(a: Acc): Acc =
        if (a.minor == 0) emit(a, SReclaimComplete({ st: E_OP_ILLEGAL }))
        else {
        val cid = a.seqClient
        if (cid == -1) emit(a, SReclaimComplete({ st: E_OP_NOT_IN_SESSION }))
        else if (a.sdb.clients.get(cid).reclaimDone)
            emit(a, SReclaimComplete({ st: E_COMPLETE_ALREADY }))
        else
            emit(a.with("sdb", a.sdb.with("clients",
                     a.sdb.clients.setBy(cid, c => c.with("reclaimDone", true)))),
                 SReclaimComplete({ st: OK }))
    }

    // FREE_STATEID (RFC 8881 18.38): the model generates it for lock
    // stateids whose ranges are all unlocked; NFS4ERR_LOCKS_HELD while any
    // byte remains locked.  (Freeing revoked delegations/layouts is part
    // of the deferred restart scenarios.)
    pure def opFreeStateid(a: Acc, r: SidRef): Acc =
        if (a.minor == 0) emit(a, SFreeStateid({ st: E_OP_ILLEGAL }))
        else if (r.kind != KLock or not(sidLive(a.sdb, r.sid, KLock)))
            emit(a, SFreeStateid({ st: E_BAD_STATEID }))
        else if (a.sdb.locks.get(r.sid).bytes.keys().size() > 0)
            emit(a, SFreeStateid({ st: E_LOCKS_HELD }))
        else {
            val sdb2 = a.sdb
                .with("locks", smapDrop(a.sdb.locks, r.sid))
                .with("freedSids", a.sdb.freedSids.union(Set(r.sid)))
            emit(a.with("sdb", sdb2), SFreeStateid({ st: OK }))
        }

    // ------------------------------------------------------------------
    // OPEN family
    // ------------------------------------------------------------------

    // Read-delegation recall triggers for an OPEN (RFC 8881 10.4.3 /
    // 18.16.4): access includes WRITE, deny includes READ, or a truncating
    // create.  Write delegations conflict with any other-client open.
    pure def openRecallSet(sdb: StateDB, cid: ClientId, f: Ino,
                           access: int, deny: int, truncates: bool): Set[Sid] =
        delegsOnFile(sdb, f).filter(s => {
            val d = sdb.delegs.get(s)
            d.client != cid and
            (d.isWrite or access >= 2 or deny % 2 == 1 or truncates)
        })

    // The delegation-grant profile, pinned against chimera by
    // nfs4_deleg_probe.c (grants are always a server MAY -- RFC 8881
    // 10.4 -- so the model asserts chimera's observed, deterministic
    // in-process policy):
    //  - grants are active from the client's FIRST open once the
    //    backchannel is up (a 4.1 session created with the backchannel
    //    flag; no CB_NULL kick round-trip is observable in-process);
    //  - WRITE/BOTH open -> write delegation when no other client has
    //    the file open or delegated;
    //  - READ open -> read delegation when no other client has a
    //    write-mode open or a write delegation (read delegations and
    //    read opens coexist across clients);
    //  - no second grant to a client already holding a delegation on
    //    the file (and no self-recall);
    //  - never on a 4.0 open that demands OPEN_CONFIRM (RFC 7530
    //    9.1.11).
    pure def delegGrant(a: Acc, cid: ClientId, f: Ino, access: int, deny: int,
                        needConfirm: bool): int = {
        val othersOpenAny = a.sdb.opens.keys().exists(s => {
            val o = a.sdb.opens.get(s)
            o.file == f and o.client != cid
        })
        val othersOpenWrite = a.sdb.opens.keys().exists(s => {
            val o = a.sdb.opens.get(s)
            o.file == f and o.client != cid and o.access >= 2
        })
        val selfDeleg = delegsOnFile(a.sdb, f).exists(s =>
            a.sdb.delegs.get(s).client == cid)
        val otherDeleg = delegsOnFile(a.sdb, f).exists(s =>
            a.sdb.delegs.get(s).client != cid)
        val otherWriteDeleg = delegsOnFile(a.sdb, f).exists(s => {
            val d = a.sdb.delegs.get(s)
            d.client != cid and d.isWrite
        })
        val cbUp = a.sdb.clients.keys().contains(cid) and
                   a.sdb.clients.get(cid).cbUp
        if (needConfirm or selfDeleg or not(cbUp)) 0
        else if (access >= 2 and a.caps.writeDeleg and
                 not(othersOpenAny) and not(otherDeleg)) 2
        else if (access == 1 and a.caps.readDeleg and
                 not(othersOpenWrite) and not(otherWriteDeleg)) 1
        else 0
    }

    // Resolve an OPEN claim to a target file, creating it if asked.
    // Returns the typing/creation status plus the file and directory
    // change info.
    pure def openResolve(a: Acc, claim: OpenClaim, how: OpenHow):
            { st: int, fs2: Fs, file: Ino, cinfo: CInfo, truncated: bool } = {
        val zero = { before: 0, after: 0 }
        val base = { st: OK, fs2: a.fs, file: -1, cinfo: zero, truncated: false }
        match claim {
            | OCFh =>
                // CLAIM_FH (4.1+): cfh is the file itself; no create
                // (OPEN4_CREATE with CLAIM_FH is INVAL, RFC 8881 18.16.3).
                if (how != HNoCreate) base.with("st", E_INVAL)
                else {
                    val nd = cfhNode(a)
                    if (nd.ftype == FDir) base.with("st", E_ISDIR)
                    else if (nd.ftype == FLnk) base.with("st", E_SYMLINK)
                    else if (nd.ftype != FReg) base.with("st", E_WRONG_TYPE)
                    else base.with("file", a.cfh)
                }
            | OCNull(n) => {
                val dnd = cfhNode(a)
                if (dnd.ftype != FDir) base.with("st", E_NOTDIR)
                else {
                    val present = fsHasEnt(a.fs, a.cfh, n)
                    val child = if (present) fsChild(a.fs, a.cfh, n) else -1
                    val childT = if (present) fsGet(a.fs, child).ftype else FReg
                    // Typing of an existing non-regular target: 4.0 uses
                    // SYMLINK for every non-directory special file; 4.1
                    // splits ISDIR/SYMLINK/WRONG_TYPE (RFC 7530 16.16.6 /
                    // RFC 8881 18.16.4).
                    val typeErr =
                        if (childT == FDir) E_ISDIR
                        else if (childT == FLnk) E_SYMLINK
                        else if (childT != FReg)
                            (if (a.minor == 0) E_SYMLINK else E_WRONG_TYPE)
                        else OK
                    match how {
                        | HNoCreate =>
                            if (not(present)) base.with("st", E_NOENT)
                            else if (typeErr != OK) base.with("st", typeErr)
                            else base.with("file", child)
                                     .with("cinfo", { before: dnd.change,
                                                      after: dnd.change })
                        | HUnchecked(u) =>
                            if (present and typeErr != OK)
                                // acceptance: EXIST is also seen from
                                // servers that order the existence check
                                // first (R-OPEN-803, UNSPECIFIED order).
                                base.with("st", typeErr)
                            else if (present) {
                                val fs2 = if (u.truncate)
                                              fsResize(a.fs, child, 0)
                                          else a.fs
                                base.with("file", child).with("fs2", fs2)
                                    .with("truncated", u.truncate)
                                    .with("cinfo", { before: dnd.change,
                                                     after: dnd.change })
                            } else {
                                val o = fsMknode(a.fs, a.cfh, n,
                                                 mkFile(u.mode, 0))
                                base.with("file", o.ino).with("fs2", o.fs2)
                                    .with("cinfo", o.cinfo)
                            }
                        | HGuarded(gm) =>
                            if (present) base.with("st", E_EXIST)
                            else {
                                val o = fsMknode(a.fs, a.cfh, n,
                                                 mkFile(gm.mode, 0))
                                base.with("file", o.ino).with("fs2", o.fs2)
                                    .with("cinfo", o.cinfo)
                            }
                        | HExclusive(x) =>
                            // EXCLUSIVE4: verifier match on an existing
                            // regular file is idempotent success; any
                            // other existing object is EXIST
                            // (RFC 7530 16.16.5 / RFC 8881 18.16.3).
                            // The file mode after exclusive create is the
                            // server's derivation (0644 canonically).
                            if (present and childT == FReg and
                                fsGet(a.fs, child).xverf == x.verf and
                                x.verf != 0)
                                base.with("file", child)
                                    .with("cinfo", { before: dnd.change,
                                                     after: dnd.change })
                            else if (present) base.with("st", E_EXIST)
                            else {
                                val o = fsMknode(a.fs, a.cfh, n,
                                                 mkExclFile(420, x.verf, 0))
                                base.with("file", o.ino).with("fs2", o.fs2)
                                    .with("cinfo", o.cinfo)
                            }
                    }
                }
            }
        }
    }

    pure def opOpen(a: Acc, ar: { client: ClientId, owner: int, oseq: int,
                                  claim: OpenClaim, how: OpenHow,
                                  access: int, deny: int }): Acc = {
        val zero = { before: 0, after: 0 }
        val nores = { st: OK, sid: -1, seq: 0, needConfirm: false,
                      cinfo: zero, deleg: DNone, recalls: Set() }
        val g = fhGate(a)
        // 4.0: the owner-seqid gate runs first and BAD_SEQID does not
        // consume a seqid (RFC 7530 9.1.7/9.1.9).
        if (a.minor == 0 and
            (not(a.sdb.clients.keys().contains(ar.client)) or
             not(a.sdb.clients.get(ar.client).confirmed)))
            emit(a, SOpen(nores.with("st", E_STALE_CLIENTID)))
        else if (a.minor == 0 and
                 not(ooGateOk(a.sdb, ar.client, ar.owner, ar.oseq)))
            emit(a, SOpen(nores.with("st", E_BAD_SEQID)))
        else if (g != OK) {
            val a1 = emit(a, SOpen(nores.with("st", g)))
            if (a.minor == 0)
                a1.with("sdb", ooSettle(a1.sdb, ar.client, ar.owner, ar.oseq,
                                        g, false))
            else a1
        }
        else if (ar.access < 1 or ar.access > 3 or ar.deny < 0 or ar.deny > 3) {
            val a1 = emit(a, SOpen(nores.with("st", E_INVAL)))
            if (a.minor == 0)
                a1.with("sdb", ooSettle(a1.sdb, ar.client, ar.owner, ar.oseq,
                                        E_INVAL, false))
            else a1
        }
        else if (a.minor >= 1 and a.seqClient != -1 and
                 a.sdb.clients.keys().contains(a.seqClient) and
                 not(a.sdb.clients.get(a.seqClient).reclaimDone))
            // RFC 8881 18.51.3: a non-reclaim lock-obtaining operation
            // before the client's global RECLAIM_COMPLETE fails
            // NFS4ERR_GRACE.  (The gate precedes name resolution, so an
            // OPEN of a missing name is GRACE, not NOENT.)  LOCK needs no
            // twin gate: lock state requires an open stateid, which this
            // gate already forces behind RECLAIM_COMPLETE.
            emit(a, SOpen(nores.with("st", E_GRACE)))
        else {
            val res = openResolve(a, ar.claim, ar.how)
            if (res.st != OK) {
                val a1 = emit(a, SOpen(nores.with("st", res.st)))
                if (a.minor == 0)
                    a1.with("sdb", ooSettle(a1.sdb, ar.client, ar.owner,
                                            ar.oseq, res.st, false))
                else a1
            } else {
                // Delegations held by other clients force a recall before
                // the OPEN can proceed; the conflicting OPEN observes
                // NFS4ERR_DELAY (discretion: conflictDelays profile).
                val recalls = openRecallSet(a.sdb, ar.client, res.file,
                                            ar.access, ar.deny,
                                            res.truncated)
                if (recalls.size() > 0) {
                    val sdb1 = recalls.fold(a.sdb, (acc, s) =>
                        doRecallDeleg(acc, s))
                    val sdb2 = if (a.minor == 0)
                                   ooSettle(sdb1, ar.client, ar.owner,
                                            ar.oseq, E_DELAY, false)
                               else sdb1
                    emit(a.with("sdb", sdb2),
                         SOpen(nores.with("st", E_DELAY)
                                    .with("recalls", recalls)))
                }
                else if (shareConflict(a.sdb, ar.client, ar.owner, res.file,
                                       ar.access, ar.deny)) {
                    val sdb2 = if (a.minor == 0)
                                   ooSettle(a.sdb, ar.client, ar.owner,
                                            ar.oseq, E_SHARE_DENIED, false)
                               else a.sdb
                    // Note: the resolve step may have created/truncated
                    // the file before the share check; the RFCs do not
                    // order creation vs share conflict.  The model checks
                    // shares first against the pre-existing state and
                    // discards the create on conflict, matching the
                    // no-side-effects reading.
                    emit(a.with("sdb", sdb2),
                         SOpen(nores.with("st", E_SHARE_DENIED)))
                }
                else {
                    val needConf = a.minor == 0 and
                        not(ooConfirmed(a.sdb, ar.client, ar.owner))
                    val o = doOpen(a.sdb, ar.client, ar.owner, res.file,
                                   ar.access, ar.deny, needConf)
                    val dg = delegGrant(a.with("sdb", o.sdb2), ar.client,
                                        res.file, ar.access, ar.deny,
                                        needConf)
                    val withDeleg =
                        if (dg == 0)
                            { sdb2: o.sdb2, deleg: DNone }
                        else {
                            val d = doGrantDeleg(o.sdb2, ar.client, res.file,
                                                 dg == 2)
                            { sdb2: d.sdb2,
                              deleg: if (dg == 2) DWrite({ sid: d.sid })
                                     else DRead({ sid: d.sid }) }
                        }
                    val sdbF = if (a.minor == 0)
                                   ooSettle(withDeleg.sdb2, ar.client,
                                            ar.owner, ar.oseq, OK, false)
                               else withDeleg.sdb2
                    emit(a.with("fs", res.fs2).with("sdb", sdbF)
                          .with("cfh", res.file),
                         SOpen({ st: OK, sid: o.sid, seq: o.seq,
                                 needConfirm: o.needConfirm,
                                 cinfo: res.cinfo,
                                 deleg: withDeleg.deleg,
                                 recalls: Set() }))
                }
            }
        }
    }

    // OPEN_CONFIRM (RFC 7530 16.18, 4.0 only): owner seqid must be the
    // OPEN's seqid + 1; on success the owner is confirmed and the stateid
    // seqid advances (acceptance: a server returning seqid 1 here is also
    // conformant -- UNSPECIFIED, R-OPEN-502).
    pure def opOpenConfirm(a: Acc, ar: { sid: Sid, oseq: int }): Acc =
        if (a.minor != 0) emit(a, SOpenConfirm({ st: E_NOTSUPP, seq: 0 }))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SOpenConfirm({ st: g, seq: 0 }))
            else if (not(sidLive(a.sdb, ar.sid, KOpen)))
                emit(a, SOpenConfirm({ st: E_BAD_STATEID, seq: 0 }))
            else {
                val o = a.sdb.opens.get(ar.sid)
                if (o.file != a.cfh)
                    emit(a, SOpenConfirm({ st: E_BAD_STATEID, seq: 0 }))
                else if (not(o.needConfirm))
                    emit(a, SOpenConfirm({ st: E_BAD_STATEID, seq: 0 }))
                else if (not(ooGateOk(a.sdb, o.client, o.owner, ar.oseq)))
                    // Wrong seqid on OPEN_CONFIRM: the server releases the
                    // state of the original OPEN (RFC 7530 16.18.5).
                    emit(a.with("sdb", doClose(a.sdb, ar.sid)),
                         SOpenConfirm({ st: E_BAD_SEQID, seq: 0 }))
                else {
                    val r = doOpenConfirm(a.sdb, ar.sid)
                    val sdb2 = ooSettle(r.sdb2, o.client, o.owner, ar.oseq,
                                        OK, true)
                    emit(a.with("sdb", sdb2),
                         SOpenConfirm({ st: OK, seq: r.seq }))
                }
            }
        }

    pure def opOpenDowngrade(a: Acc, ar: { sid: Sid, argSeq: int, access: int,
                                           deny: int, oseq: int }): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SOpenDowngrade({ st: g, seq: 0 }))
        else if (not(sidLive(a.sdb, ar.sid, KOpen)))
            emit(a, SOpenDowngrade({ st: E_BAD_STATEID, seq: 0 }))
        else {
            val o = a.sdb.opens.get(ar.sid)
            if (a.minor == 0 and not(ooGateOk(a.sdb, o.client, o.owner, ar.oseq)))
                emit(a, SOpenDowngrade({ st: E_BAD_SEQID, seq: 0 }))
            else if (o.needConfirm)
                emit(a, SOpenDowngrade({ st: E_BAD_STATEID, seq: 0 }))
            else if (o.file != a.cfh)
                emit(a, SOpenDowngrade({ st: E_BAD_STATEID, seq: 0 }))
            else {
                val chk = checkSid(a.sdb, ar.sid, KOpen, ar.argSeq)
                val st0 = match chk {
                    | SidOk => OK
                    | SidOld => E_OLD_STATEID
                    | SidBad => E_BAD_STATEID
                }
                if (st0 != OK) {
                    val a1 = emit(a, SOpenDowngrade({ st: st0, seq: 0 }))
                    if (a.minor == 0)
                        a1.with("sdb", ooSettle(a1.sdb, o.client, o.owner,
                                                ar.oseq, st0, false))
                    else a1
                } else {
                    val r = doOpenDowngrade(a.sdb, ar.sid, ar.access, ar.deny)
                    val st = if (r.ok) OK else E_INVAL
                    val sdb2 = if (a.minor == 0)
                                   ooSettle(r.sdb2, o.client, o.owner,
                                            ar.oseq, st, false)
                               else r.sdb2
                    emit(a.with("sdb", sdb2),
                         SOpenDowngrade({ st: st, seq: if (r.ok) r.seq else 0 }))
                }
            }
        }
    }

    pure def opClose(a: Acc, ar: { sid: Sid, argSeq: int, oseq: int }): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SClose({ st: g }))
        else if (not(sidLive(a.sdb, ar.sid, KOpen)))
            emit(a, SClose({ st: E_BAD_STATEID }))
        else {
            val o = a.sdb.opens.get(ar.sid)
            if (a.minor == 0 and not(ooGateOk(a.sdb, o.client, o.owner, ar.oseq)))
                emit(a, SClose({ st: E_BAD_SEQID }))
            else if (o.needConfirm)
                // CLOSE of an unconfirmed open (RFC 7530 16.18.5).
                emit(a, SClose({ st: E_BAD_STATEID }))
            else if (o.file != a.cfh)
                emit(a, SClose({ st: E_BAD_STATEID }))
            else {
                val chk = checkSid(a.sdb, ar.sid, KOpen, ar.argSeq)
                val st0 = match chk {
                    | SidOk => OK
                    | SidOld => E_OLD_STATEID
                    | SidBad => E_BAD_STATEID
                }
                if (st0 != OK) {
                    val a1 = emit(a, SClose({ st: st0 }))
                    if (a.minor == 0)
                        a1.with("sdb", ooSettle(a1.sdb, o.client, o.owner,
                                                ar.oseq, st0, false))
                    else a1
                } else {
                    val lockSids = locksUnderOpen(a.sdb, ar.sid)
                    val locked = lockSids.exists(s =>
                        a.sdb.locks.get(s).bytes.keys().size() > 0)
                    if (locked and not(a.caps.closeFreesLocks)) {
                        val a1 = emit(a, SClose({ st: E_LOCKS_HELD }))
                        if (a.minor == 0)
                            a1.with("sdb", ooSettle(a1.sdb, o.client, o.owner,
                                                    ar.oseq, E_LOCKS_HELD,
                                                    false))
                        else a1
                    } else {
                        val sdb1 = doClose(a.sdb, ar.sid)
                        val sdb2 = if (a.minor == 0)
                                       ooSettle(sdb1, o.client, o.owner,
                                                ar.oseq, OK, false)
                                   else sdb1
                        // Last CLOSE of an unlinked-while-open file reaps
                        // the orphaned inode.
                        emit(a.with("sdb", sdb2)
                              .with("fs", maybeReap(a.fs, sdb2, o.file)),
                             SClose({ st: OK }))
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Data ops
    // ------------------------------------------------------------------

    // Type gate shared by READ/WRITE-class ops: directory -> ISDIR;
    // symlink -> SYMLINK (acceptance: INVAL also conformant in 4.0, the
    // op text vs Table 7 disagree, R-CORE-91); other non-regular ->
    // 4.0 INVAL / 4.1+ WRONG_TYPE.
    pure def dataTypeGate(a: Acc): int = {
        val t = cfhNode(a).ftype
        if (t == FReg) OK
        else if (t == FDir) E_ISDIR
        else if (t == FLnk) E_SYMLINK
        else if (a.minor == 0) E_INVAL
        else E_WRONG_TYPE
    }

    pure def opRead(a: Acc, ar: { sel: SidSel, off: int, len: int }): Acc = {
        val nores = { st: OK, count: 0, eof: false, blocks: [] }
        val g = fhGate(a)
        if (g != OK) emit(a, SRead(nores.with("st", g)))
        else {
            val tg = dataTypeGate(a)
            if (tg != OK) emit(a, SRead(nores.with("st", tg)))
            else {
                val io = resolveSel(a, ar.sel)
                match io {
                    | IoError(e) => emit(a, SRead(nores.with("st", e)))
                    | IoAnon =>
                        // Anonymous I/O against an existing deny-read open
                        // fails NFS4ERR_LOCKED (RFC 7530 9.1.4.3).
                        if (anyDeny(a.sdb, a.cfh, 1))
                            emit(a, SRead(nores.with("st", E_LOCKED)))
                        else {
                            val r = fsReadData(cfhNode(a), ar.off, ar.len)
                            emit(a, SRead({ st: OK, count: r.count,
                                            eof: r.eof, blocks: r.syms }))
                        }
                    | IoBypass =>
                        // READ-bypass: the server MAY bypass share checks
                        // (RFC 8881 8.2.3).  discretion: modeled as NOT
                        // bypassing -- chimera's unified VFS share
                        // enforcement applies to every accessor, so the
                        // bypass stateid behaves like the anonymous one.
                        if (anyDeny(a.sdb, a.cfh, 1))
                            emit(a, SRead(nores.with("st", E_LOCKED)))
                        else {
                            val r = fsReadData(cfhNode(a), ar.off, ar.len)
                            emit(a, SRead({ st: OK, count: r.count,
                                            eof: r.eof, blocks: r.syms }))
                        }
                    | IoState(s) =>
                        // discretion: READ via a write-only open is a MAY
                        // (RFC 8881 9.1.2); the model predicts OPENMODE
                        // and the generator avoids the case.
                        if (not(bitsOverlap(ioMode(a, io), 1)))
                            emit(a, SRead(nores.with("st", E_OPENMODE)))
                        else {
                            val r = fsReadData(cfhNode(a), ar.off, ar.len)
                            emit(a, SRead({ st: OK, count: r.count,
                                            eof: r.eof, blocks: r.syms }))
                        }
                }
            }
        }
    }

    pure def opWrite(a: Acc, ar: { sel: SidSel, off: int, len: int,
                                   pat: int, stable: int }): Acc = {
        val nores = { st: OK, count: 0, newSizeBlocks: 0 }
        val g = fhGate(a)
        if (g != OK) emit(a, SWrite(nores.with("st", g)))
        else {
            val tg = dataTypeGate(a)
            if (tg != OK) emit(a, SWrite(nores.with("st", tg)))
            else {
                val io = resolveSel(a, ar.sel)
                match io {
                    | IoError(e) => emit(a, SWrite(nores.with("st", e)))
                    | IoAnon =>
                        if (anyDeny(a.sdb, a.cfh, 2))
                            emit(a, SWrite(nores.with("st", E_LOCKED)))
                        else {
                            val w = fsWrite(a.fs, a.cfh, ar.off, ar.len, ar.pat)
                            emit(a.with("fs", w.fs2),
                                 SWrite({ st: OK, count: ar.len,
                                          newSizeBlocks: w.newSizeBlocks }))
                        }
                    | IoBypass =>
                        // All-ones stateid in WRITE is treated as
                        // anonymous (RFC 8881 8.2.3).
                        if (anyDeny(a.sdb, a.cfh, 2))
                            emit(a, SWrite(nores.with("st", E_LOCKED)))
                        else {
                            val w = fsWrite(a.fs, a.cfh, ar.off, ar.len, ar.pat)
                            emit(a.with("fs", w.fs2),
                                 SWrite({ st: OK, count: ar.len,
                                          newSizeBlocks: w.newSizeBlocks }))
                        }
                    | IoState(s) =>
                        // WRITE through a stateid lacking WRITE access is
                        // NFS4ERR_OPENMODE (MUST, RFC 8881 9.1.2).
                        if (not(bitsOverlap(ioMode(a, io), 2)))
                            emit(a, SWrite(nores.with("st", E_OPENMODE)))
                        else {
                            val w = fsWrite(a.fs, a.cfh, ar.off, ar.len, ar.pat)
                            emit(a.with("fs", w.fs2),
                                 SWrite({ st: OK, count: ar.len,
                                          newSizeBlocks: w.newSizeBlocks }))
                        }
                }
            }
        }
    }

    // COMMIT (RFC 7530 16.3 / RFC 8881 18.3): takes no stateid; flushing
    // nothing is not an error.  The harness checks the write verifier is
    // stable across the run (R-CORE-95).
    pure def opCommit(a: Acc): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SCommit({ st: g }))
        else {
            val tg = dataTypeGate(a)
            if (tg != OK) emit(a, SCommit({ st: tg }))
            else emit(a, SCommit({ st: OK }))
        }
    }

    // ------------------------------------------------------------------
    // Byte-range locks
    // ------------------------------------------------------------------

    pure def opLock(a: Acc, ar: { wr: bool, lo: int, hi: int, newOwner: bool,
                                  openSid: Sid, lockSid: Sid, lockOwner: int,
                                  oseq: int, lseq: int }): Acc = {
        val nores = { st: OK, sid: -1, seq: 0, deniedOwner: -1 }
        val g = fhGate(a)
        if (g != OK) emit(a, SLock(nores.with("st", g)))
        else if (ar.lo >= ar.hi)
            // Zero-length lock -> NFS4ERR_INVAL (RFC 7530 16.10.4 /
            // RFC 8881 18.10.3).
            emit(a, SLock(nores.with("st", E_INVAL)))
        else if (ar.newOwner) {
            // open_to_lock_owner4 arm: the open stateid anchors the new
            // lock state.
            if (not(sidLive(a.sdb, ar.openSid, KOpen)))
                emit(a, SLock(nores.with("st", E_BAD_STATEID)))
            else {
                val o = a.sdb.opens.get(ar.openSid)
                if (o.file != a.cfh or o.needConfirm)
                    emit(a, SLock(nores.with("st", E_BAD_STATEID)))
                else if (a.minor == 0 and
                         not(ooGateOk(a.sdb, o.client, o.owner, ar.oseq)))
                    emit(a, SLock(nores.with("st", E_BAD_SEQID)))
                else if (a.minor == 0 and
                         lockHeldByOwner(a.sdb, o.client, ar.lockOwner, o.file))
                    // 4.0: new_lock_owner=TRUE while this lock-owner still
                    // *holds* byte-range locks on the file is BAD_SEQID (RFC
                    // 7530 16.10.5); a fully-LOCKU'd stateid may be
                    // re-established, and 4.1 reuses the state (falls through
                    // to the merge in doLock).
                    emit(a, SLock(nores.with("st", E_BAD_SEQID)))
                else if (lockConflict(a.sdb, o.client, ar.lockOwner, o.file,
                                      ar.lo, ar.hi, ar.wr)) {
                    val who = lockConflictOwner(a.sdb, o.client, ar.lockOwner,
                                                o.file, ar.lo, ar.hi, ar.wr)
                    val a1 = emit(a, SLock(nores.with("st", E_DENIED)
                                                .with("deniedOwner", who)))
                    if (a.minor == 0)
                        a1.with("sdb", ooSettle(a1.sdb, o.client, o.owner,
                                                ar.oseq, E_DENIED, false))
                    else a1
                } else {
                    val r = doLock(a.sdb, o.client, ar.lockOwner, o.file,
                                   ar.openSid, ar.lo, ar.hi, ar.wr)
                    val sdb2 = if (a.minor == 0)
                                   loSettle(ooSettle(r.sdb2, o.client, o.owner,
                                                     ar.oseq, OK, false),
                                            o.client, ar.lockOwner, ar.lseq, OK)
                               else r.sdb2
                    emit(a.with("sdb", sdb2),
                         SLock(nores.with("sid", r.sid).with("seq", r.seq)))
                }
            }
        } else {
            // exist_lock_owner4 arm: the lock stateid itself.
            if (not(sidLive(a.sdb, ar.lockSid, KLock)))
                emit(a, SLock(nores.with("st", E_BAD_STATEID)))
            else {
                val l = a.sdb.locks.get(ar.lockSid)
                if (l.file != a.cfh)
                    emit(a, SLock(nores.with("st", E_BAD_STATEID)))
                else if (a.minor == 0 and
                         not(loGateOk(a.sdb, l.client, l.owner, ar.lseq)))
                    emit(a, SLock(nores.with("st", E_BAD_SEQID)))
                else if (lockConflict(a.sdb, l.client, l.owner, l.file,
                                      ar.lo, ar.hi, ar.wr)) {
                    val who = lockConflictOwner(a.sdb, l.client, l.owner,
                                                l.file, ar.lo, ar.hi, ar.wr)
                    val a1 = emit(a, SLock(nores.with("st", E_DENIED)
                                                .with("deniedOwner", who)))
                    if (a.minor == 0)
                        a1.with("sdb", loSettle(a1.sdb, l.client, l.owner,
                                                ar.lseq, E_DENIED))
                    else a1
                } else {
                    val r = doLock(a.sdb, l.client, l.owner, l.file,
                                   l.openSid, ar.lo, ar.hi, ar.wr)
                    val sdb2 = if (a.minor == 0)
                                   loSettle(r.sdb2, l.client, l.owner,
                                            ar.lseq, OK)
                               else r.sdb2
                    emit(a.with("sdb", sdb2),
                         SLock(nores.with("sid", r.sid).with("seq", r.seq)))
                }
            }
        }
    }

    // LOCKT (RFC 7530 16.11 / RFC 8881 18.11): stateless test by
    // lock-owner; the requester's own locks do not conflict (SHOULD).
    pure def opLockt(a: Acc, ar: { client: ClientId, lockOwner: int,
                                   wr: bool, lo: int, hi: int }): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SLockt({ st: g, deniedOwner: -1 }))
        else if (ar.lo >= ar.hi)
            emit(a, SLockt({ st: E_INVAL, deniedOwner: -1 }))
        else if (lockConflict(a.sdb, ar.client, ar.lockOwner, a.cfh,
                              ar.lo, ar.hi, ar.wr)) {
            val who = lockConflictOwner(a.sdb, ar.client, ar.lockOwner,
                                        a.cfh, ar.lo, ar.hi, ar.wr)
            emit(a, SLockt({ st: E_DENIED, deniedOwner: who }))
        }
        else emit(a, SLockt({ st: OK, deniedOwner: -1 }))
    }

    // LOCKU (RFC 7530 16.12 / RFC 8881 18.12): unlocking a never-locked
    // range succeeds; partial unlocks split (per-byte model).  The seqid
    // bump on a no-op unlock is a MAY; the model always bumps.
    //
    // LOCKU is a lock-owner-sequenced op (RFC 7530 9.1.7): 4.0 requires the
    // request `lseq` to be the lock-owner's last + 1 (a stateid seqid of 0 is
    // the wildcard for the *stateid*, `argSeq`, not for the owner sequence).
    // The gate precedes the stateid-seqid check (matching chimera's order);
    // only a successful unlock advances the owner seqid.
    pure def opLocku(a: Acc, ar: { sid: Sid, argSeq: int, lseq: int,
                                   lo: int, hi: int }): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SLocku({ st: g, seq: 0 }))
        else if (ar.lo >= ar.hi)
            emit(a, SLocku({ st: E_INVAL, seq: 0 }))
        else if (not(sidLive(a.sdb, ar.sid, KLock)))
            emit(a, SLocku({ st: E_BAD_STATEID, seq: 0 }))
        else {
            val l = a.sdb.locks.get(ar.sid)
            if (l.file != a.cfh)
                emit(a, SLocku({ st: E_BAD_STATEID, seq: 0 }))
            else if (a.minor == 0 and
                     not(loGateOk(a.sdb, l.client, l.owner, ar.lseq)))
                emit(a, SLocku({ st: E_BAD_SEQID, seq: 0 }))
            else {
                val chk = checkSid(a.sdb, ar.sid, KLock, ar.argSeq)
                match chk {
                    | SidOld => emit(a, SLocku({ st: E_OLD_STATEID, seq: 0 }))
                    | SidBad => emit(a, SLocku({ st: E_BAD_STATEID, seq: 0 }))
                    | SidOk => {
                        val r = doLocku(a.sdb, ar.sid, ar.lo, ar.hi)
                        val sdb2 = if (a.minor == 0)
                                       loSettle(r.sdb2, l.client, l.owner,
                                                ar.lseq, OK)
                                   else r.sdb2
                        emit(a.with("sdb", sdb2),
                             SLocku({ st: OK, seq: r.seq }))
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Delegations
    // ------------------------------------------------------------------

    pure def opDelegreturn(a: Acc, r: SidRef): Acc = {
        val g = fhGate(a)
        if (g != OK) emit(a, SDelegreturn({ st: g }))
        else if (r.kind != KDeleg or not(sidLive(a.sdb, r.sid, KDeleg)))
            emit(a, SDelegreturn({ st: E_BAD_STATEID }))
        else if (a.sdb.delegs.get(r.sid).file != a.cfh)
            emit(a, SDelegreturn({ st: E_BAD_STATEID }))
        else {
            val f = a.sdb.delegs.get(r.sid).file
            val sdb2 = doDelegReturn(a.sdb, r.sid)
            emit(a.with("sdb", sdb2).with("fs", maybeReap(a.fs, sdb2, f)),
                 SDelegreturn({ st: OK }))
        }
    }

    // ------------------------------------------------------------------
    // pNFS (all ops NFS4ERR_NOTSUPP when the MDS role is not advertised,
    // RFC 8881 13.1)
    // ------------------------------------------------------------------

    pure def opLayoutget(a: Acc, ar: { rw: bool, lo: int, hi: int,
                                       sel: SidSel }): Acc = {
        val nores = { st: OK, sid: -1, seq: 0, lo: 0, hi: 0 }
        if (a.minor < 1) emit(a, SLayoutget(nores.with("st", E_OP_ILLEGAL)))
        else if (not(a.caps.pnfs)) emit(a, SLayoutget(nores.with("st", E_NOTSUPP)))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SLayoutget(nores.with("st", g)))
            else if (cfhNode(a).ftype != FReg)
                emit(a, SLayoutget(nores.with("st", E_WRONG_TYPE)))
            else {
                val io = resolveSel(a, ar.sel)
                match io {
                    | IoError(e) => emit(a, SLayoutget(nores.with("st", e)))
                    | IoAnon => emit(a, SLayoutget(nores.with("st", E_BAD_STATEID)))
                    | IoBypass => emit(a, SLayoutget(nores.with("st", E_BAD_STATEID)))
                    | IoState(s) => {
                        val cid = match s.kind {
                            | KOpen => a.sdb.opens.get(s.sid).client
                            | KLock => a.sdb.locks.get(s.sid).client
                            | KDeleg => a.sdb.delegs.get(s.sid).client
                            | KLayout => a.sdb.layouts.get(s.sid).client
                        }
                        // The model grants exactly the requested range
                        // (a server MUST cover the requested range and MAY
                        // widen, RFC 8881 18.43.3 Table 22; the harness
                        // checks coverage, not equality).
                        val r = doLayoutGet(a.sdb, cid, a.cfh, ar.lo, ar.hi,
                                            ar.rw)
                        emit(a.with("sdb", r.sdb2),
                             SLayoutget({ st: OK, sid: r.sid, seq: r.seq,
                                          lo: ar.lo, hi: ar.hi }))
                    }
                }
            }
        }
    }

    pure def opLayoutreturn(a: Acc, ar: { sid: Sid, argSeq: int, lo: int,
                                          hi: int }): Acc = {
        val nores = { st: OK, present: false, seq: 0 }
        if (a.minor < 1) emit(a, SLayoutreturn(nores.with("st", E_OP_ILLEGAL)))
        else if (not(a.caps.pnfs)) emit(a, SLayoutreturn(nores.with("st", E_NOTSUPP)))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SLayoutreturn(nores.with("st", g)))
            else if (not(sidLive(a.sdb, ar.sid, KLayout)))
                emit(a, SLayoutreturn(nores.with("st", E_BAD_STATEID)))
            else {
                val chk = checkSid(a.sdb, ar.sid, KLayout, ar.argSeq)
                match chk {
                    | SidOld => emit(a, SLayoutreturn(nores.with("st", E_OLD_STATEID)))
                    | SidBad => emit(a, SLayoutreturn(nores.with("st", E_BAD_STATEID)))
                    | SidOk => {
                        // Returning an unheld range is success, not an
                        // error (RFC 8881 18.44.3).
                        val f = a.sdb.layouts.get(ar.sid).file
                        val r = doLayoutReturn(a.sdb, ar.sid, ar.lo, ar.hi)
                        emit(a.with("sdb", r.sdb2)
                              .with("fs", maybeReap(a.fs, r.sdb2, f)),
                             SLayoutreturn({ st: OK, present: r.remains,
                                             seq: r.seq }))
                    }
                }
            }
        }
    }

    // LAYOUTCOMMIT (RFC 8881 18.42): requires a held RW layout overlapping
    // the committed range (else NFS4ERR_BADLAYOUT); makes DS writes and
    // the new size visible at the MDS.  The model treats WRITEs as
    // MDS-visible already (single-node server), so only the size result
    // is interesting.
    pure def opLayoutcommit(a: Acc, ar: { sid: Sid, lo: int, hi: int }): Acc = {
        val nores = { st: OK, newSizeBlocks: 0 }
        if (a.minor < 1) emit(a, SLayoutcommit(nores.with("st", E_OP_ILLEGAL)))
        else if (not(a.caps.pnfs)) emit(a, SLayoutcommit(nores.with("st", E_NOTSUPP)))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SLayoutcommit(nores.with("st", g)))
            else if (not(sidLive(a.sdb, ar.sid, KLayout)))
                emit(a, SLayoutcommit(nores.with("st", E_BAD_STATEID)))
            else {
                val l = a.sdb.layouts.get(ar.sid)
                val overlapRw = range(ar.lo, ar.hi).foldl(false, (acc, b) =>
                    acc or (l.blocks.keys().contains(b) and l.blocks.get(b)))
                if (l.file != a.cfh)
                    emit(a, SLayoutcommit(nores.with("st", E_BAD_STATEID)))
                else if (not(overlapRw))
                    emit(a, SLayoutcommit(nores.with("st", E_BADLAYOUT)))
                else
                    emit(a, SLayoutcommit({ st: OK,
                        newSizeBlocks: fsGet(a.fs, l.file).sizeBlocks }))
            }
        }
    }

    pure def opGetdeviceinfo(a: Acc): Acc =
        if (a.minor < 1) emit(a, SGetdeviceinfo({ st: E_OP_ILLEGAL }))
        else if (not(a.caps.pnfs)) emit(a, SGetdeviceinfo({ st: E_NOTSUPP }))
        else emit(a, SGetdeviceinfo({ st: OK }))

    // ------------------------------------------------------------------
    // NFSv4.2 ops (each individually optional; NFS4ERR_NOTSUPP when the
    // capability profile says unsupported, RFC 7862 13)
    // ------------------------------------------------------------------

    // Common stateid+type gate for the 4.2 I/O-class ops; needWrite
    // selects the access-mode check.
    pure def io42Gate(a: Acc, sel: SidSel, needWrite: bool): int = {
        val g = fhGate(a)
        if (g != OK) g
        else {
            val tg = dataTypeGate(a)
            if (tg != OK) tg
            else {
                val io = resolveSel(a, sel)
                match io {
                    | IoError(e) => e
                    | IoAnon =>
                        if (anyDeny(a.sdb, a.cfh, if (needWrite) 2 else 1))
                            E_LOCKED
                        else OK
                    | IoBypass => OK
                    | IoState(s) =>
                        if (not(bitsOverlap(ioMode(a, io),
                                            if (needWrite) 2 else 1)))
                            E_OPENMODE
                        else OK
                }
            }
        }
    }

    pure def opAllocate(a: Acc, ar: { sel: SidSel, off: int, len: int }): Acc =
        if (a.minor < 2) emit(a, SAllocate({ st: E_OP_ILLEGAL }))
        else if (not(a.caps.sparse)) emit(a, SAllocate({ st: E_NOTSUPP }))
        else {
            val g = io42Gate(a, ar.sel, true)
            if (g != OK) emit(a, SAllocate({ st: g }))
            else if (ar.len <= 0) emit(a, SAllocate({ st: E_INVAL }))
            else emit(a.with("fs", fsAllocate(a.fs, a.cfh, ar.off, ar.len)),
                      SAllocate({ st: OK }))
        }

    pure def opDeallocate(a: Acc, ar: { sel: SidSel, off: int, len: int }): Acc =
        if (a.minor < 2) emit(a, SDeallocate({ st: E_OP_ILLEGAL }))
        else if (not(a.caps.sparse)) emit(a, SDeallocate({ st: E_NOTSUPP }))
        else {
            val g = io42Gate(a, ar.sel, true)
            if (g != OK) emit(a, SDeallocate({ st: g }))
            else if (ar.len <= 0) emit(a, SDeallocate({ st: E_INVAL }))
            else emit(a.with("fs", fsDeallocate(a.fs, a.cfh, ar.off, ar.len)),
                      SDeallocate({ st: OK }))
        }

    // SEEK (RFC 7862 15.11): offset at/after EOF -> NFS4ERR_NXIO; a data
    // search in a trailing hole returns eof=true at the size (the virtual
    // hole/EOF rule); allocated-but-unwritten blocks count as data.
    pure def opSeek(a: Acc, ar: { sel: SidSel, off: int, whatData: bool }): Acc = {
        val nores = { st: OK, eof: false, offset: 0 }
        if (a.minor < 2) emit(a, SSeek(nores.with("st", E_OP_ILLEGAL)))
        else if (not(a.caps.sparse)) emit(a, SSeek(nores.with("st", E_NOTSUPP)))
        else {
            val g = io42Gate(a, ar.sel, false)
            if (g != OK) emit(a, SSeek(nores.with("st", g)))
            else {
                val nd = cfhNode(a)
                if (ar.off >= nd.sizeBlocks)
                    emit(a, SSeek(nores.with("st", E_NXIO)))
                else {
                    val r = fsSeek(nd, ar.off, ar.whatData)
                    if (ar.whatData and not(r.found))
                        // No data before EOF: sr_eof = true at the size.
                        emit(a, SSeek({ st: OK, eof: true,
                                        offset: nd.sizeBlocks }))
                    else
                        emit(a, SSeek({ st: OK,
                                        eof: r.offset >= nd.sizeBlocks,
                                        offset: r.offset }))
                }
            }
        }
    }

    // Intra-server synchronous COPY (RFC 7862 15.2): sfh = source, cfh =
    // destination; same file -> NFS4ERR_INVAL (any ranges); source range
    // beyond the source size -> NFS4ERR_INVAL; cnt = 0 copies through the
    // source EOF.
    pure def opCopy(a: Acc, ar: { srcSel: SidSel, dstSel: SidSel,
                                  srcOff: int, dstOff: int, cnt: int }): Acc = {
        val nores = { st: OK, copied: 0 }
        if (a.minor < 2) emit(a, SCopy(nores.with("st", E_OP_ILLEGAL)))
        else if (not(a.caps.copy)) emit(a, SCopy(nores.with("st", E_NOTSUPP)))
        else if (a.sfh == -1 or a.cfh == -1)
            emit(a, SCopy(nores.with("st", E_NOFILEHANDLE)))
        else if (not(fsLive(a.fs, a.sfh)) or not(fsLive(a.fs, a.cfh)))
            emit(a, SCopy(nores.with("st", E_STALE)))
        else if (a.sfh == a.cfh)
            emit(a, SCopy(nores.with("st", E_INVAL)))
        else {
            val src = fsGet(a.fs, a.sfh)
            val dst = fsGet(a.fs, a.cfh)
            if (src.ftype != FReg or dst.ftype != FReg)
                emit(a, SCopy(nores.with("st",
                    if (src.ftype == FDir or dst.ftype == FDir) E_ISDIR
                    else E_WRONG_TYPE)))
            else {
                val cnt = if (ar.cnt == 0) imax(0, src.sizeBlocks - ar.srcOff)
                          else ar.cnt
                if (ar.srcOff + cnt > src.sizeBlocks or
                    (ar.cnt == 0 and ar.srcOff > src.sizeBlocks))
                    emit(a, SCopy(nores.with("st", E_INVAL)))
                else {
                    val data = fsReadData(src, ar.srcOff, cnt)
                    val w = range(0, cnt).foldl(a.fs, (acc, i) =>
                        fsWrite(acc, a.cfh, ar.dstOff + i, 1,
                                data.syms[i]).fs2)
                    emit(a.with("fs", w), SCopy({ st: OK, copied: cnt }))
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Extended attributes (RFC 8276; one per-fs gate: xattr_support)
    // ------------------------------------------------------------------

    pure def opGetxattr(a: Acc, n: Name): Acc =
        if (a.minor < 2) emit(a, SGetxattr({ st: E_OP_ILLEGAL, sym: 0 }))
        else if (not(a.caps.xattr)) emit(a, SGetxattr({ st: E_NOTSUPP, sym: 0 }))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SGetxattr({ st: g, sym: 0 }))
            else {
                val nd = cfhNode(a)
                if (not(nd.xattrs.keys().contains(n)))
                    emit(a, SGetxattr({ st: E_NOXATTR, sym: 0 }))
                else emit(a, SGetxattr({ st: OK, sym: nd.xattrs.get(n) }))
            }
        }

    pure def opSetxattr(a: Acc, ar: { name: Name, sym: int,
                                      how: XattrSetHow }): Acc =
        if (a.minor < 2) emit(a, SSetxattr({ st: E_OP_ILLEGAL }))
        else if (not(a.caps.xattr)) emit(a, SSetxattr({ st: E_NOTSUPP }))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SSetxattr({ st: g }))
            else {
                val how = match ar.how {
                    | XsCreate => XCreate
                    | XsReplace => XReplace
                    | XsEither => XEither
                }
                val r = fsSetXattr(a.fs, a.cfh, ar.name, ar.sym, how)
                emit(a.with("fs", r.fs2), SSetxattr({ st: fsStatus(r.st) }))
            }
        }

    pure def opListxattrs(a: Acc): Acc =
        if (a.minor < 2) emit(a, SListxattrs({ st: E_OP_ILLEGAL, names: Set() }))
        else if (not(a.caps.xattr))
            emit(a, SListxattrs({ st: E_NOTSUPP, names: Set() }))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SListxattrs({ st: g, names: Set() }))
            else emit(a, SListxattrs({ st: OK,
                                       names: cfhNode(a).xattrs.keys() }))
        }

    pure def opRemovexattr(a: Acc, n: Name): Acc =
        if (a.minor < 2) emit(a, SRemovexattr({ st: E_OP_ILLEGAL }))
        else if (not(a.caps.xattr)) emit(a, SRemovexattr({ st: E_NOTSUPP }))
        else {
            val g = fhGate(a)
            if (g != OK) emit(a, SRemovexattr({ st: g }))
            else {
                val r = fsRemoveXattr(a.fs, a.cfh, n)
                emit(a.with("fs", r.fs2), SRemovexattr({ st: fsStatus(r.st) }))
            }
        }

    // ------------------------------------------------------------------
    // The compound evaluator
    // ------------------------------------------------------------------

    // Ops that may legally appear outside a session in 4.1+ (RFC 8881
    // 18.46.3 plus DESTROY_CLIENTID per 18.50.3 -- the RFC's two lists
    // disagree; the 18.50.3 reading is modeled).
    pure def sessionExempt(r: OpReq): bool =
        match r {
            | RExchangeId(x) => true
            | RCreateSession(x) => true
            | RDestroySession(x) => true
            | RDestroyClientid(x) => true
            | RSequence(x) => true
            | _ => false
        }

    pure def dispatch(a: Acc, r: OpReq): Acc =
        match r {
            | RPutrootfh => opPutrootfh(a)
            | RPutfh(i) => opPutfh(a, i)
            | RGetfh => opGetfh(a)
            | RSavefh => opSavefh(a)
            | RRestorefh => opRestorefh(a)
            | RLookup(n) => opLookup(a, n)
            | RLookupp => opLookupp(a)
            | RGetattr => opGetattr(a)
            | RReadlink => opReadlink(a)
            | RAccess(m) => opAccess(a, m)
            | RReaddir => opReaddir(a)
            | RCreate(ar) => opCreate(a, ar)
            | RRemove(n) => opRemove(a, n)
            | RRename(ar) => opRename(a, ar)
            | RLink(n) => opLink(a, n)
            | RSetattr(ar) => opSetattr(a, ar)
            | RVerifySize(sz) => opVerifySize(a, sz)
            | RNverifySize(sz) => opNverifySize(a, sz)
            | RSetclientid(ar) => opSetclientid(a, ar)
            | RSetclientidConfirm(ar) => opSetclientidConfirm(a, ar)
            | RRenew(c) => opRenew(a, c)
            | RReleaseLockowner(ar) => opReleaseLockowner(a, ar)
            | RExchangeId(ar) => opExchangeId(a, ar)
            | RCreateSession(ar) => opCreateSession(a, ar)
            | RSequence(ar) => opSequence(a, ar)
            | RDestroySession(s) => opDestroySession(a, s)
            | RDestroyClientid(c) => opDestroyClientid(a, c)
            | RReclaimComplete => opReclaimComplete(a)
            | RFreeStateid(sr) => opFreeStateid(a, sr)
            | ROpen(ar) => opOpen(a, ar)
            | ROpenConfirm(ar) => opOpenConfirm(a, ar)
            | ROpenDowngrade(ar) => opOpenDowngrade(a, ar)
            | RClose(ar) => opClose(a, ar)
            | RRead(ar) => opRead(a, ar)
            | RWrite(ar) => opWrite(a, ar)
            | RCommit => opCommit(a)
            | RLock(ar) => opLock(a, ar)
            | RLockt(ar) => opLockt(a, ar)
            | RLocku(ar) => opLocku(a, ar)
            | RDelegreturn(sr) => opDelegreturn(a, sr)
            | RLayoutget(ar) => opLayoutget(a, ar)
            | RLayoutreturn(ar) => opLayoutreturn(a, ar)
            | RLayoutcommit(ar) => opLayoutcommit(a, ar)
            | RGetdeviceinfo => opGetdeviceinfo(a)
            | RAllocate(ar) => opAllocate(a, ar)
            | RDeallocate(ar) => opDeallocate(a, ar)
            | RSeek(ar) => opSeek(a, ar)
            | RCopy(ar) => opCopy(a, ar)
            | RGetxattr(n) => opGetxattr(a, n)
            | RSetxattr(ar) => opSetxattr(a, ar)
            | RListxattrs => opListxattrs(a)
            | RRemovexattr(n) => opRemovexattr(a, n)
        }

    type CompoundOutcome = {
        fs2: Fs,
        sdb2: StateDB,
        status: int,
        results: List[OpRes]
    }

    pure def mkCaps(pnfs: bool, readDeleg: bool, writeDeleg: bool,
                    xattr: bool, sparse: bool, copyOk: bool): Caps =
        { pnfs: pnfs, readDeleg: readDeleg, writeDeleg: writeDeleg,
          xattr: xattr, sparse: sparse, copy: copyOk,
          conflictDelays: true, closeFreesLocks: true }

    // Evaluate a whole COMPOUND.  reqTag identifies the request for the
    // 4.1 SEQUENCE replay/false-retry classification.
    pure def evalCompound(fs: Fs, sdb: StateDB, minor: int, caps: Caps,
                          reqTag: int, ops: List[OpReq]): CompoundOutcome = {
        val a0: Acc = {
            fs: fs, sdb: sdb, minor: minor, caps: caps,
            nOps: ops.length(), reqTag: reqTag, idx: 0,
            cfh: -1, sfh: -1, seqDone: false, seqClient: -1,
            done: false, status: OK, results: []
        }
        val aN = ops.foldl(a0, (acc, r) => {
            val acc2 =
                if (acc.done) acc
                else if (acc.minor >= 1 and acc.idx == 0 and
                         not(sessionExempt(r))) {
                    // First op of a 4.1+ compound must be SEQUENCE or a
                    // session-exempt op.
                    val res = SSequence({ st: E_OP_NOT_IN_SESSION,
                                          replay: false })
                    acc.with("results", acc.results.append(res))
                       .with("status", E_OP_NOT_IN_SESSION)
                       .with("done", true)
                }
                else dispatch(acc, r)
            acc2.with("idx", acc.idx + 1)
        })
        { fs2: aN.fs, sdb2: aN.sdb, status: aN.status, results: aN.results }
    }
}
