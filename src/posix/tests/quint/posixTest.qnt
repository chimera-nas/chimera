// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Model self-tests.  Two fully pinned instances so every run is
// deterministic:
//   posixTestStd   two unprivileged users, POSIX-canonical policy knobs
//                  (pwrite ignores O_APPEND, unlink(dir)=EPERM,
//                  ENOTEMPTY, sticky=EACCES, lock conflict=EAGAIN)
//   posixTestRoot  process 0 runs as root (privilege rules)
//
//   quint test posixTest.qnt --main=posixTestStd
//   quint test posixTest.qnt --main=posixTestRoot

module posixTestStd {
    import posixfs.* from "./posix_fs"
    import posixstate.* from "./posix_state"
    import posixops.* from "./posix_ops"
    import posix(
        F_COPY_RANGE = FeatMandatory,
        F_CLONE_RANGE = FeatMandatory,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatOff,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatOff,
        P_STRICT_ATIME = FeatMandatory,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatOff,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"

    // ---------------- helpers ----------------

    def lastE = match lastOp {
        | LCall(c) => errOf(c.res)
        | LInit(x) => -999
    }

    def lastFd = match lastOp {
        | LCall(c) => match c.res { | SFd(x) => x.fd | _ => -1 }
        | LInit(x) => -1
    }

    def lastOff = match lastOp {
        | LCall(c) => match c.res { | SOff(x) => x.off | _ => -1 }
        | LInit(x) => -1
    }

    def lastN = match lastOp {
        | LCall(c) => match c.res { | SRw(x) => x.n | _ => -1 }
        | LInit(x) => -1
    }

    val noStatR = { e: -999, ftype: FReg, mode: 0, uid: -1, gid: -1,
                    nlink: 0, sizeB: 0, ino: -1, atime: 0, mtime: 0,
                    ctime: 0 }

    def lastStatR = match lastOp {
        | LCall(c) => match c.res { | SStatR(x) => x | _ => noStatR }
        | LInit(x) => noStatR
    }

    def lastNames = match lastOp {
        | LCall(c) => match c.res { | SNamesR(x) => x.names | _ => Set("?") }
        | LInit(x) => Set("?")
    }

    def lastSid = match lastOp {
        | LCall(c) => match c.res { | SDirR(x) => x.sid | _ => -1 }
        | LInit(x) => -1
    }

    def lastTgt = match lastOp {
        | LCall(c) => match c.res {
            | SReadlinkR(x) => x.tgt
            | _ => NO_TARGET
          }
        | LInit(x) => NO_TARGET
    }

    val noGetlk = { e: -999, conflict: false, cpid: -1, cw: false }

    def lastGetlk = match lastOp {
        | LCall(c) => match c.res { | SGetlkR(x) => x | _ => noGetlk }
        | LInit(x) => noGetlk
    }

    val noGetfl = { e: -999, acc: AccR, appendF: false }

    def lastGetfl = match lastOp {
        | LCall(c) => match c.res { | SGetflR(x) => x | _ => noGetfl }
        | LInit(x) => noGetfl
    }

    def lastUmaskOld = match lastOp {
        | LCall(c) => match c.res { | SUmaskR(x) => x.old | _ => -1 }
        | LInit(x) => -1
    }

    pure def ap(cs: List[Name]): Path = { abs: true, comps: cs, slash: false }
    pure def apS(cs: List[Name]): Path = { abs: true, comps: cs, slash: true }
    pure def rp(cs: List[Name]): Path = { abs: false, comps: cs, slash: false }

    pure val FLR: OpenFlags = { acc: AccR, creat: false, excl: false,
                                trunc: false, appendF: false,
                                directory: false, nofollow: false, mode: 0 }
    pure val FLW: OpenFlags = FLR.with("acc", AccW)
    pure val FLRW: OpenFlags = FLR.with("acc", AccRW)
    pure def flC(m: int): OpenFlags =
        FLRW.with("creat", true).with("mode", m)
    pure def flCX(m: int): OpenFlags = flC(m).with("excl", true)

    // ---------------- pathname resolution ----------------

    run pathResolutionTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["a"]), mode: 493 })))
        .expect(lastE == OK)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["a", "b"]), mode: 493 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["a", "b", "c"]),
                               fl: flCX(420) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["a", "b", "c"]),
                               follow: true })))
        .expect(lastE == OK and lastStatR.ino == 3)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["a", "x"]), follow: true })))
        .expect(lastE == ENOENT)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["x", "b"]), follow: true })))
        .expect(lastE == ENOENT)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["a", "b", "c", "d"]),
                               follow: true })))
        .expect(lastE == ENOTDIR)
        .then(runOp(0, RStat({ dfd: -1, pth: apS(["a", "b", "c"]),
                               follow: true })))
        .expect(lastE == ENOTDIR)
        .then(runOp(0, RSymlink({ tgt: { abs: true, comps: ["a", "b"] },
                                  dfd: -1, pth: ap(["s"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["s", "c"]), follow: true })))
        .expect(lastE == OK and lastStatR.ino == 3)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["s"]), follow: false })))
        .expect(lastE == OK and lastStatR.ftype == FLnk)
        .then(runOp(0, RStat({ dfd: -1, pth: apS(["s"]), follow: false })))
        .expect(lastE == OK and lastStatR.ftype == FDir)
        .then(runOp(0, RSymlink({ tgt: { abs: false, comps: ["l"] },
                                  dfd: -1, pth: ap(["l"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["l"]), follow: true })))
        .expect(lastE == ELOOP)
        .then(runOp(0, RReadlink({ dfd: -1, pth: ap(["l"]) })))
        .expect(lastE == OK and lastTgt == { abs: false, comps: ["l"] })
        .then(runOp(0, RReadlink({ dfd: -1, pth: ap(["a"]) })))
        .expect(lastE == EINVAL)
        .then(runOp(0, RStat({ dfd: -1,
                               pth: { abs: true,
                                      comps: ["a", "..", "a", ".", "b"],
                                      slash: false },
                               follow: true })))
        .expect(lastE == OK and lastStatR.ino == 2)

    // ---------------- open flags ----------------

    run openCreateTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flCX(448) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flCX(448) })))
        .expect(lastE == EEXIST)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(448) })))
        .expect(lastE == OK and lastFd == 1)
        .then(runOp(0, RWrite({ fd: 0, len: 2, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RFstat({ fd: 1 })))
        .expect(lastStatR.sizeB == 2)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]),
                               fl: FLW.with("trunc", true) })))
        .expect(lastE == OK and lastFd == 2)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.sizeB == 0)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap([]), fl: FLW })))
        .expect(lastE == EISDIR)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["missing"]), fl: FLR })))
        .expect(lastE == ENOENT)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]),
                               fl: FLR.with("directory", true) })))
        .expect(lastE == ENOTDIR)
        .then(runOp(0, RSymlink({ tgt: { abs: true, comps: ["f"] },
                                  dfd: -1, pth: ap(["sf"]) })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["sf"]),
                               fl: FLR.with("nofollow", true) })))
        .expect(lastE == ELOOP)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["sf"]), fl: FLR })))
        .expect(lastE == OK and lastFd == 3)
        .then(runOp(0, RSymlink({ tgt: { abs: true, comps: ["nowhere"] },
                                  dfd: -1, pth: ap(["dang"]) })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["dang"]), fl: flCX(448) })))
        .expect(lastE == EEXIST)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["dang"]), fl: flC(448) })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["nowhere"]),
                               follow: false })))
        .expect(lastE == OK and lastStatR.ftype == FReg)

    // ---------------- umask and mode-000 creation ----------------

    run umaskModeTest =
        init
        .then(runOp(0, RUmask({ mask: 18 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["z"]), fl: flCX(511) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.mode == 493 and lastStatR.uid == 100 and
                lastStatR.gid == 10)
        .then(runOp(0, RUmask({ mask: 0 })))
        .expect(lastUmaskOld == 18)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["m0"]), fl: flCX(0) })))
        .expect(lastE == OK and lastFd == 1)
        .then(runOp(0, RWrite({ fd: 1, len: 1, pat: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, RPread({ fd: 1, off: 0, len: 1 })))
        .expect(lastE == OK and lastN == 1)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["m0"]), fl: FLR })))
        .expect(lastE == EACCES)

    // ---------------- permission classes ----------------

    run permClassTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["priv"]), mode: 448 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["priv", "f"]),
                               fl: flCX(511) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == OK)
        .then(runOp(1, RStat({ dfd: -1, pth: ap(["priv", "f"]),
                               follow: true })))
        .expect(lastE == EACCES)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["g"]), fl: flCX(432) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(1, ROpen({ dfd: -1, pth: ap(["g"]), fl: FLRW })))
        .expect(lastE == EACCES)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["g"]), u: -1, g: 30,
                                follow: true })))
        .expect(lastE == OK)
        .then(runOp(1, ROpen({ dfd: -1, pth: ap(["g"]), fl: FLRW })))
        .expect(lastE == OK and lastFd == 0)
        // Exclusive class selection: mode 0077 denies the owner even
        // though group/other would allow.
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["x"]), fl: flCX(63) })))
        .expect(lastE == OK and lastFd == 1)
        .then(runOp(0, RClose({ fd: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["x"]), fl: FLR })))
        .expect(lastE == EACCES)
        .then(runOp(1, ROpen({ dfd: -1, pth: ap(["x"]), fl: FLR })))
        .expect(lastE == OK)

    // ---------------- dup shares the open file description ----------------

    run dupOffsetTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(511) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RWrite({ fd: 0, len: 3, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RDup({ fd: 0 })))
        .expect(lastE == OK and lastFd == 1)
        .then(runOp(0, RLseek({ fd: 1, off: 1, wh: WSet })))
        .expect(lastOff == 1)
        .then(runOp(0, RLseek({ fd: 0, off: 0, wh: WCur })))
        .expect(lastOff == 1)
        .then(runOp(0, RRead({ fd: 0, len: 1 })))
        .expect(lastN == 1)
        .then(runOp(0, RLseek({ fd: 1, off: 0, wh: WCur })))
        .expect(lastOff == 2)
        .then(runOp(0, RFcntlSetfl({ fd: 0, appendF: true })))
        .expect(lastE == OK)
        .then(runOp(0, RFcntlGetfl({ fd: 1 })))
        .expect(lastGetfl.appendF)
        // An independent open() gets its own description.
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLR })))
        .expect(lastFd == 2)
        .then(runOp(0, RLseek({ fd: 2, off: 0, wh: WCur })))
        .expect(lastOff == 0)
        // dup2 closes its target implicitly and then shares.
        .then(runOp(0, RDup2({ fd: 0, nfd: 2 })))
        .expect(lastE == OK and lastFd == 2)
        .then(runOp(0, RLseek({ fd: 2, off: 0, wh: WCur })))
        .expect(lastOff == 2)
        .then(runOp(0, RDup2({ fd: 0, nfd: 0 })))
        .expect(lastE == OK and lastFd == 0)

    // ---------------- O_APPEND and POSIX pwrite ----------------

    run appendTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]),
                               fl: flC(511).with("appendF", true) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RWrite({ fd: 0, len: 2, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RLseek({ fd: 0, off: 0, wh: WSet })))
        .expect(lastOff == 0)
        .then(runOp(0, RWrite({ fd: 0, len: 1, pat: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.sizeB == 3)
        .then(runOp(0, RPread({ fd: 0, off: 0, len: 3 })))
        .expect(lastN == 3)
        // P_PWRITE_APPENDS is off: pwrite hits the given offset even with
        // O_APPEND set (the POSIX reading; Linux would append).
        .then(runOp(0, RPwrite({ fd: 0, off: 0, len: 1, pat: 3 })))
        .expect(lastE == OK)
        .then(runOp(0, RPread({ fd: 0, off: 0, len: 3 })))
        .expect(lastN == 3)

    // ---------------- unlink / rename while open ----------------

    run unlinkWhileOpenTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(511) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RWrite({ fd: 0, len: 2, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["f"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["f"]), follow: true })))
        .expect(lastE == ENOENT)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastE == OK and lastStatR.nlink == 0 and
                lastStatR.sizeB == 2)
        .then(runOp(0, RPread({ fd: 0, off: 0, len: 2 })))
        .expect(lastN == 2 and fsOrphans(fs) == Set(1))
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == OK and not(fsLive(fs, 1)))
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["g"]), fl: flC(511) })))
        .expect(lastFd == 0)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["h"]), fl: flC(511) })))
        .expect(lastFd == 1)
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["g"]),
                                 dfdNew: -1, pthNew: ap(["h"]) })))
        .expect(lastE == OK and fsOrphans(fs) == Set(3))
        .then(runOp(0, RClose({ fd: 1 })))
        .expect(lastE == OK and not(fsLive(fs, 3)))
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == OK)

    // ---------------- rename matrix ----------------

    run renameMatrixTest =
        init
        .then(runOp(0, RMknod({ pth: ap(["f1"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["f2"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d1"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d2"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d3"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["d3", "x"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        // file over file: target dies
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["f1"]),
                                 dfdNew: -1, pthNew: ap(["f2"]) })))
        .expect(lastE == OK and not(fsLive(fs, 2)))
        // file over dir / dir over file
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["f2"]),
                                 dfdNew: -1, pthNew: ap(["d1"]) })))
        .expect(lastE == EISDIR)
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["d1"]),
                                 dfdNew: -1, pthNew: ap(["f2"]) })))
        .expect(lastE == ENOTDIR)
        // dir over empty dir: target dies
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["d1"]),
                                 dfdNew: -1, pthNew: ap(["d2"]) })))
        .expect(lastE == OK and not(fsLive(fs, 4)))
        // dir over non-empty dir
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["d2"]),
                                 dfdNew: -1, pthNew: ap(["d3"]) })))
        .expect(lastE == ENOTEMPTY)
        // dir into its own subtree
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["d3"]),
                                 dfdNew: -1, pthNew: ap(["d3", "y"]) })))
        .expect(lastE == EINVAL)
        // rename onto another hard link of the same file: no-op success
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["f2"]),
                               dfdNew: -1, pthNew: ap(["f3"]),
                               followOld: false })))
        .expect(lastE == OK)
        .then(runOp(0, RRename({ dfdOld: -1, pthOld: ap(["f2"]),
                                 dfdNew: -1, pthNew: ap(["f3"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["f2"]), follow: true })))
        .expect(lastE == OK and lastStatR.nlink == 2)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["f3"]), follow: true })))
        .expect(lastE == OK and lastStatR.ino == 1)

    // ---------------- hard links ----------------

    run linkTest =
        init
        .then(runOp(0, RMknod({ pth: ap(["f"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["f"]), dfdNew: -1,
                               pthNew: ap(["g"]), followOld: false })))
        .expect(lastE == OK and fsGet(fs, 1).nlink == 2)
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["f"]), dfdNew: -1,
                               pthNew: ap(["g"]), followOld: false })))
        .expect(lastE == EEXIST)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["d"]), dfdNew: -1,
                               pthNew: ap(["e"]), followOld: false })))
        .expect(lastE == EPERM)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["g"]), follow: true })))
        .expect(lastStatR.ino == 1)
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["f"]) })))
        .expect(lastE == OK and fsLive(fs, 1) and fsGet(fs, 1).nlink == 1)
        // linkat with and without AT_SYMLINK_FOLLOW
        .then(runOp(0, RSymlink({ tgt: { abs: true, comps: ["g"] },
                                  dfd: -1, pth: ap(["sl"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["sl"]), dfdNew: -1,
                               pthNew: ap(["h"]), followOld: true })))
        .expect(lastE == OK and fsGet(fs, 1).nlink == 2)
        .then(runOp(0, RLink({ dfdOld: -1, pthOld: ap(["sl"]), dfdNew: -1,
                               pthNew: ap(["k"]), followOld: false })))
        .expect(lastE == OK and fsGet(fs, 3).nlink == 2)

    // ---------------- rmdir ----------------

    run rmdirTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["d", "x"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RRmdir({ dfd: -1, pth: ap(["d"]) })))
        .expect(lastE == ENOTEMPTY)
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["d", "x"]) })))
        .expect(lastE == OK)
        // unlink of a directory: EPERM in this instance's profile
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["d"]) })))
        .expect(lastE == EPERM)
        .then(runOp(0, RRmdir({ dfd: -1,
                                pth: { abs: true, comps: ["d", "."],
                                       slash: false } })))
        .expect(lastE == EINVAL)
        .then(runOp(0, RMknod({ pth: ap(["f"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RRmdir({ dfd: -1, pth: ap(["f"]) })))
        .expect(lastE == ENOTDIR)
        .then(runOp(0, RRmdir({ dfd: -1, pth: ap([]) })))
        .expect(lastE == EBUSY)
        // rmdir of a directory held open by a stream: orphan, then reap
        .then(runOp(0, ROpendir({ pth: ap(["d"]) })))
        .expect(lastE == OK and lastSid == 0)
        .then(runOp(0, RRmdir({ dfd: -1, pth: ap(["d"]) })))
        .expect(lastE == OK and fsOrphans(fs) == Set(1))
        .then(runOp(0, RReaddir({ sid: 0 })))
        .expect(lastE == OK and lastNames == Set())
        .then(runOp(0, RClosedir({ sid: 0 })))
        .expect(lastE == OK and not(fsLive(fs, 1)))

    // ---------------- sticky directories ----------------

    run stickyTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["t"]), mode: 1023 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["t", "f0"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(1, RMknod({ pth: ap(["t", "f1"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(1, RUnlink({ dfd: -1, pth: ap(["t", "f0"]) })))
        .expect(lastE == EACCES)
        .then(runOp(1, RUnlink({ dfd: -1, pth: ap(["t", "f1"]) })))
        .expect(lastE == OK)
        .then(runOp(1, RMknod({ pth: ap(["t", "f1"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["t", "f1"]) })))
        .expect(lastE == OK)
        .then(runOp(1, RRename({ dfdOld: -1, pthOld: ap(["t", "f0"]),
                                 dfdNew: -1, pthNew: ap(["t", "z"]) })))
        .expect(lastE == EACCES)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["t"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(1, RUnlink({ dfd: -1, pth: ap(["t", "f0"]) })))
        .expect(lastE == OK)

    // ---------------- chown rules and setid clearing ----------------

    run chownSetidTest =
        init
        .then(runOp(0, RMknod({ pth: ap(["f"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["f"]), u: 200, g: -1,
                                follow: true })))
        .expect(lastE == EPERM)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["f"]), u: -1, g: 30,
                                follow: true })))
        .expect(lastE == OK and fsGet(fs, 1).gid == 30)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["f"]), u: -1, g: 20,
                                follow: true })))
        .expect(lastE == EPERM)
        .then(runOp(1, RChown({ dfd: -1, pth: ap(["f"]), u: -1, g: 30,
                                follow: true })))
        .expect(lastE == EPERM)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["f"]), mode: 2541 })))
        .expect(lastE == OK and fsGet(fs, 1).mode == 2541)
        // chown of a regular file with execute bits clears setids
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["f"]), u: 100, g: -1,
                                follow: true })))
        .expect(lastE == OK and fsGet(fs, 1).mode == 493)
        // owner in the file's group keeps a requested S_ISGID
        .then(runOp(0, RMknod({ pth: ap(["f2"]), ft: FReg, mode: 448 })))
        .expect(lastE == OK)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["f2"]), mode: 1472 })))
        .expect(lastE == OK and fsGet(fs, 2).mode == 1472)

    // ---------------- utimensat tiers ----------------

    run utimensTest =
        init
        .then(runOp(1, RMknod({ pth: ap(["f"]), ft: FReg, mode: 384 })))
        .expect(lastE == OK)
        .then(runOp(0, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsNow,
                                  tm: TsNow })))
        .expect(lastE == EACCES)
        .then(runOp(0, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsVal(-1),
                                  tm: TsOmit })))
        .expect(lastE == EPERM)
        .then(runOp(1, RChmod({ dfd: -1, pth: ap(["f"]), mode: 438 })))
        .expect(lastE == OK)
        .then(runOp(0, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsNow,
                                  tm: TsNow })))
        .expect(lastE == OK)
        // write access never satisfies the explicit-times tier
        .then(runOp(0, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsVal(-1),
                                  tm: TsOmit })))
        .expect(lastE == EPERM)
        .then(runOp(1, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsVal(-1),
                                  tm: TsVal(-2) })))
        .expect(lastE == OK and fsGet(fs, 1).atime == -1 and
                fsGet(fs, 1).mtime == -2)
        .then(runOp(1, RUtimens({ dfd: -1, pth: ap(["f"]), ta: TsOmit,
                                  tm: TsOmit })))
        .expect(lastE == OK and fsGet(fs, 1).atime == -1 and
                fsGet(fs, 1).mtime == -2)

    // ---------------- record locks ----------------

    run lockTest =
        init
        .then(runOp(0, RMknod({ pth: ap(["f"]), ft: FReg, mode: 438 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLRW })))
        .expect(lastFd == 0)
        .then(runOp(1, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLRW })))
        .expect(lastFd == 0)
        .then(runOp(0, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkWr, lo: 0,
                                    hi: 4 })))
        .expect(lastE == OK)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkRd, lo: 2,
                                    hi: 3 })))
        .expect(lastE == EAGAIN)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CGetlk, lk: LkRd, lo: 2,
                                    hi: 3 })))
        .expect(lastGetlk.conflict and lastGetlk.cpid == 0 and lastGetlk.cw)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkRd, lo: 4,
                                    hi: 6 })))
        .expect(lastE == OK)
        // unlock a prefix: the write lock splits
        .then(runOp(0, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkUn, lo: 0,
                                    hi: 2 })))
        .expect(lastE == OK)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkRd, lo: 0,
                                    hi: 2 })))
        .expect(lastE == OK)
        // downgrade own write lock to read: replacement, then sharing
        .then(runOp(0, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkRd, lo: 2,
                                    hi: 4 })))
        .expect(lastE == OK)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkWr, lo: 2,
                                    hi: 4 })))
        .expect(lastE == EAGAIN)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlkw, lk: LkRd, lo: 2,
                                    hi: 4 })))
        .expect(lastE == OK)
        // closing ANY descriptor drops all of the process's locks
        .then(runOp(0, RDup({ fd: 0 })))
        .expect(lastFd == 1)
        .then(runOp(0, RClose({ fd: 1 })))
        .expect(lastE == OK)
        .then(runOp(1, RFcntlLock({ fd: 0, cmd: CSetlk, lk: LkWr, lo: 0,
                                    hi: 8 })))
        .expect(lastE == OK)
        // lock typing needs a matching open mode
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLR })))
        .expect(lastFd == 1)
        .then(runOp(0, RFcntlLock({ fd: 1, cmd: CSetlk, lk: LkWr, lo: 6,
                                    hi: 7 })))
        .expect(lastE == EBADF)

    run lockfTest =
        init
        .then(runOp(0, RMknod({ pth: ap(["f"]), ft: FReg, mode: 438 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLRW })))
        .expect(lastFd == 0)
        .then(runOp(1, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLRW })))
        .expect(lastFd == 0)
        .then(runOp(0, RLockf({ fd: 0, cmd: LfTlock, len: 2 })))
        .expect(lastE == OK)
        .then(runOp(1, RLockf({ fd: 0, cmd: LfTst, len: 2 })))
        .expect(lastE == EAGAIN)
        .then(runOp(1, RLseek({ fd: 0, off: 4, wh: WSet })))
        .expect(lastOff == 4)
        .then(runOp(1, RLockf({ fd: 0, cmd: LfTlock, len: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, RLockf({ fd: 0, cmd: LfUlock, len: 2 })))
        .expect(lastE == OK)
        .then(runOp(1, RLseek({ fd: 0, off: 0, wh: WSet })))
        .expect(lastOff == 0)
        .then(runOp(1, RLockf({ fd: 0, cmd: LfTst, len: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLR })))
        .expect(lastFd == 1)
        .then(runOp(0, RLockf({ fd: 1, cmd: LfTlock, len: 1 })))
        .expect(lastE == EBADF)

    // ---------------- directory streams ----------------

    run dirStreamTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["d", "a"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["d", "b"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpendir({ pth: ap(["d"]) })))
        .expect(lastE == OK and lastSid == 0)
        .then(runOp(0, RReaddir({ sid: 0 })))
        .expect(lastE == OK and lastNames == Set("a", "b"))
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["d", "a"]) })))
        .expect(lastE == OK)
        .then(runOp(0, RRewinddir({ sid: 0 })))
        .expect(lastE == OK)
        .then(runOp(0, RReaddir({ sid: 0 })))
        .expect(lastE == OK and lastNames == Set("b"))
        .then(runOp(0, RClosedir({ sid: 0 })))
        .expect(lastE == OK)
        .then(runOp(0, RReaddir({ sid: 0 })))
        .expect(lastE == EBADF)
        // opendir wants read permission; searchability is not required
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["r"]), mode: 292 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpendir({ pth: ap(["r"]) })))
        .expect(lastE == OK and lastSid == 1)
        .then(runOp(0, RClosedir({ sid: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["w"]), mode: 219 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpendir({ pth: ap(["w"]) })))
        .expect(lastE == EACCES)

    // ---------------- sparse files and SEEK_HOLE/SEEK_DATA ----------------

    // Byte-granular: offsets are block-scaled so the hole structure spans real
    // memfs blocks.  Content bytes are checked by the replay shadow; here we
    // assert block presence (dataBlocks) and the SEEK byte offsets.  ino 1 = f.
    run sparseSeekTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(511) })))
        .expect(lastFd == 0)
        // Write blocks 0-1, then extend to 6 blocks: blocks 2-5 are a hole.
        .then(runOp(0, RWrite({ fd: 0, len: 2 * BS, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RFtruncate({ fd: 0, len: 6 * BS })))
        .expect(lastE == OK)
        .then(runOp(0, RLseek({ fd: 0, off: 0, wh: WHole })))
        .expect(lastOff == 2 * BS)
        .then(runOp(0, RLseek({ fd: 0, off: 0, wh: WData })))
        .expect(lastOff == 0)
        .then(runOp(0, RLseek({ fd: 0, off: 2 * BS, wh: WData })))
        .expect(lastE == ENXIO)
        // Materialize block 4; SEEK_DATA from the hole now lands on it.
        .then(runOp(0, RPwrite({ fd: 0, off: 4 * BS, len: BS, pat: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, RLseek({ fd: 0, off: 2 * BS, wh: WData })))
        .expect(lastOff == 4 * BS and
                fs.inodes.get(1).dataBlocks == Set(0, 1, 4))
        // SEEK_DATA inside a data block returns the queried offset itself.
        .then(runOp(0, RLseek({ fd: 0, off: 4 * BS + 10, wh: WData })))
        .expect(lastOff == 4 * BS + 10)
        .then(runOp(0, RLseek({ fd: 0, off: 6 * BS, wh: WHole })))
        .expect(lastE == ENXIO)
        // Shrink to 1 byte: only block 0 survives.
        .then(runOp(0, RFtruncate({ fd: 0, len: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RPread({ fd: 0, off: 0, len: 6 * BS })))
        .expect(lastN == 1 and fs.inodes.get(1).dataBlocks == Set(0))

    // ---------------- copy_file_range / clone ----------------

    // Byte-granular, block-aligned (copy_range presence follows whole blocks).
    // ino 1 = s (src), ino 2 = d (dst).  Presence is asserted via dataBlocks;
    // exact bytes are the replay shadow's job.
    run copyCloneTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["s"]), fl: flC(511) })))
        .expect(lastFd == 0)
        // src: blocks 0-2 data, block 5 data, blocks 3-4 holes, size 6.
        .then(runOp(0, RWrite({ fd: 0, len: 3 * BS, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RPwrite({ fd: 0, off: 5 * BS, len: BS, pat: 2 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["d"]), fl: flC(511) })))
        .expect(lastFd == 1)
        .then(runOp(0, RWrite({ fd: 1, len: 6 * BS, pat: 3 })))
        .expect(lastE == OK)
        // copy src blocks [2,6) -- data, two holes, data -- over dst [0,4):
        // dst becomes {0,3 from copy} + {4,5 kept}, blocks 1-2 punched to hole.
        .then(runOp(0, RCopyRange({ fdIn: 0, offIn: 2 * BS, fdOut: 1,
                                    offOut: 0, len: 4 * BS })))
        .expect(lastE == OK and lastN == 4 * BS and
                fs.inodes.get(2).dataBlocks == Set(0, 3, 4, 5))
        // reading at or beyond src EOF copies nothing
        .then(runOp(0, RCopyRange({ fdIn: 0, offIn: 6 * BS, fdOut: 1,
                                    offOut: 0, len: 2 * BS })))
        .expect(lastE == OK and lastN == 0)
        // clone source range must lie within the source
        .then(runOp(0, RCloneRange({ fdDst: 1, offDst: 0, fdSrc: 0,
                                     offSrc: 4 * BS, len: 4 * BS })))
        .expect(lastE == EINVAL)
        .then(runOp(0, RCloneRange({ fdDst: 1, offDst: 0, fdSrc: 0,
                                     offSrc: 0, len: 2 * BS })))
        .expect(lastE == OK)
        .then(runOp(0, RPread({ fd: 1, off: 0, len: 2 * BS })))
        .expect(lastN == 2 * BS)
        // O_APPEND on the destination is rejected
        .then(runOp(0, RFcntlSetfl({ fd: 1, appendF: true })))
        .expect(lastE == OK)
        .then(runOp(0, RCopyRange({ fdIn: 0, offIn: 0, fdOut: 1, offOut: 0,
                                    len: BS })))
        .expect(lastE == EBADF)

    // ---------------- timestamp ordering ----------------

    run timestampTest =
        init
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(511) })))
        .expect(lastFd == 0)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.atime == lastStatR.mtime and
                lastStatR.mtime == lastStatR.ctime)
        .then(runOp(0, RWrite({ fd: 0, len: 1, pat: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.mtime == lastStatR.ctime and
                lastStatR.mtime > lastStatR.atime)
        .then(runOp(0, RRead({ fd: 0, len: 1 })))
        .expect(lastE == OK)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.atime > lastStatR.mtime)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["f"]), mode: 448 })))
        .expect(lastE == OK)
        .then(runOp(0, RFstat({ fd: 0 })))
        .expect(lastStatR.ctime > lastStatR.atime)
        .then(runOp(0, RStat({ dfd: -1, pth: ap([]), follow: true })))
        .expect(lastE == OK and lastStatR.mtime > 0)

    // ---------------- group inheritance and S_ISGID dirs ----------------

    run gidSgidTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 1517 })))
        .expect(lastE == OK and fsGet(fs, 1).gid == 10 and
                bitOn(fsGet(fs, 1).mode, MB_SGID))
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["d"]), u: -1, g: 30,
                                follow: true })))
        .expect(lastE == OK and fsGet(fs, 1).gid == 30 and
                bitOn(fsGet(fs, 1).mode, MB_SGID))
        // children of an S_ISGID directory take the directory's group
        .then(runOp(0, RMknod({ pth: ap(["d", "f"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK and fsGet(fs, 2).gid == 30)
        // subdirectory: group inherited, S_ISGID bit not (knob off)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d", "sub"]),
                                mode: 493 })))
        .expect(lastE == OK and fsGet(fs, 3).gid == 30 and
                not(bitOn(fsGet(fs, 3).mode, MB_SGID)))
        // plain directory: group = egid (P_GID_FROM_PARENT off)
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["p"]), mode: 493 })))
        .expect(lastE == OK and fsGet(fs, 4).gid == 10)

    // ---------------- *at calls through a dirfd ----------------

    run atCallTest =
        init
        .then(runOp(0, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RMknod({ pth: ap(["d", "f"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["d"]),
                               fl: FLR.with("directory", true) })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RStat({ dfd: 0, pth: rp(["f"]), follow: true })))
        .expect(lastE == OK and lastStatR.ino == 2)
        .then(runOp(0, RMkdir({ dfd: 0, pth: rp(["s"]), mode: 493 })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["d", "s"]),
                               follow: true })))
        .expect(lastE == OK and lastStatR.ino == 3)
        .then(runOp(0, RUnlink({ dfd: 0, pth: rp(["f"]) })))
        .expect(lastE == OK)
        // a non-directory dirfd is ENOTDIR; a dead one is EBADF
        .then(runOp(0, RMknod({ pth: ap(["g"]), ft: FReg, mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["g"]), fl: FLR })))
        .expect(lastFd == 1)
        .then(runOp(0, RStat({ dfd: 1, pth: rp(["x"]), follow: true })))
        .expect(lastE == ENOTDIR)
        .then(runOp(0, RStat({ dfd: 9, pth: rp(["x"]), follow: true })))
        .expect(lastE == EBADF)
        // absolute paths ignore the dirfd entirely
        .then(runOp(0, RStat({ dfd: 9, pth: ap(["d"]), follow: true })))
        .expect(lastE == OK)

    // ---------------- descriptor negatives ----------------

    run ebadfTest =
        init
        .then(runOp(0, RClose({ fd: 13 })))
        .expect(lastE == EBADF)
        .then(runOp(0, RRead({ fd: 13, len: 1 })))
        .expect(lastE == EBADF)
        .then(runOp(0, RFstat({ fd: 13 })))
        .expect(lastE == EBADF)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: flC(511) })))
        .expect(lastFd == 0)
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == OK)
        .then(runOp(0, RClose({ fd: 0 })))
        .expect(lastE == EBADF)
        // reads need read access, writes need write access
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLW })))
        .expect(lastFd == 0)
        .then(runOp(0, RRead({ fd: 0, len: 1 })))
        .expect(lastE == EBADF)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLR })))
        .expect(lastFd == 1)
        .then(runOp(0, RWrite({ fd: 1, len: 1, pat: 1 })))
        .expect(lastE == EBADF)
        .then(runOp(0, RFtruncate({ fd: 1, len: 0 })))
        .expect(lastE == EINVAL)

    // ---------------- random walks under the invariant ----------------

    run randomWalkNsTest =
        init.then(80.reps(_ => stepNamespace.expect(inv)))

    run randomWalkDescTest =
        init.then(80.reps(_ => stepDescriptors.expect(inv)))

    run randomWalkMixTest =
        init.then(120.reps(_ => step.expect(inv)))
}

module posixTestRoot {
    import posixfs.* from "./posix_fs"
    import posixstate.* from "./posix_state"
    import posixops.* from "./posix_ops"
    import posix(
        F_COPY_RANGE = FeatMandatory,
        F_CLONE_RANGE = FeatMandatory,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatMandatory,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatOff,
        P_STRICT_ATIME = FeatMandatory,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatOff,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"

    def lastE = match lastOp {
        | LCall(c) => errOf(c.res)
        | LInit(x) => -999
    }

    def lastFd = match lastOp {
        | LCall(c) => match c.res { | SFd(x) => x.fd | _ => -1 }
        | LInit(x) => -1
    }

    pure def ap(cs: List[Name]): Path = { abs: true, comps: cs, slash: false }

    pure val FLRW: OpenFlags = { acc: AccRW, creat: false, excl: false,
                                 trunc: false, appendF: false,
                                 directory: false, nofollow: false, mode: 0 }

    // Process 0 is root here: DAC bypass for read/write/search, the
    // execute rule still wants an x bit, chown/chmod anywhere, sticky
    // bypass, and the classic sgid-strip when a non-member owner chmods.
    run rootPrivTest =
        init
        .then(runOp(1, RMknod({ pth: ap(["f"]), ft: FReg, mode: 0 })))
        .expect(lastE == OK)
        .then(runOp(0, ROpen({ dfd: -1, pth: ap(["f"]), fl: FLRW })))
        .expect(lastE == OK and lastFd == 0)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["f"]), u: 100, g: 10,
                                follow: true })))
        .expect(lastE == OK and fsGet(fs, 1).uid == 100)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["f"]), mode: 438 })))
        .expect(lastE == OK)
        .then(runOp(0, RAccess({ dfd: -1, pth: ap(["f"]), r: true, w: true,
                                 x: false, eff: true })))
        .expect(lastE == OK)
        .then(runOp(0, RAccess({ dfd: -1, pth: ap(["f"]), r: false,
                                 w: false, x: true, eff: true })))
        .expect(lastE == EACCES)
        .then(runOp(0, RChmod({ dfd: -1, pth: ap(["f"]), mode: 493 })))
        .expect(lastE == OK)
        .then(runOp(0, RAccess({ dfd: -1, pth: ap(["f"]), r: false,
                                 w: false, x: true, eff: true })))
        .expect(lastE == OK)
        // root walks and modifies 0700 directories owned by others
        .then(runOp(1, RMkdir({ dfd: -1, pth: ap(["d"]), mode: 448 })))
        .expect(lastE == OK)
        .then(runOp(1, RMknod({ pth: ap(["d", "x"]), ft: FReg,
                                mode: 511 })))
        .expect(lastE == OK)
        .then(runOp(0, RStat({ dfd: -1, pth: ap(["d", "x"]),
                               follow: true })))
        .expect(lastE == OK)
        .then(runOp(1, RChmod({ dfd: -1, pth: ap(["d"]), mode: 1023 })))
        .expect(lastE == OK)
        .then(runOp(0, RUnlink({ dfd: -1, pth: ap(["d", "x"]) })))
        .expect(lastE == OK)
        // sgid silently stripped when a non-member owner chmods
        .then(runOp(1, RMknod({ pth: ap(["s"]), ft: FReg, mode: 448 })))
        .expect(lastE == OK)
        .then(runOp(0, RChown({ dfd: -1, pth: ap(["s"]), u: 200, g: 10,
                                follow: true })))
        .expect(lastE == OK)
        .then(runOp(1, RChmod({ dfd: -1, pth: ap(["s"]), mode: 1472 })))
        .expect(lastE == OK and fsGet(fs, 4).mode == 448)

    run rootWalkTest =
        init.then(80.reps(_ => step.expect(inv)))
}
