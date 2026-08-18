// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stateful POSIX file-API model: universes, trace labels, generator
// actions and invariants.  One model step is one syscall by one process;
// the transition label carries the request and the expected result, which
// is everything the replay harness needs to drive chimera's POSIX client
// and compare.
//
// Feature/policy gating: each FeatureMode constant below pins one optional
// feature or server-discretion policy.  init draws the concrete profile
// nondeterministically within the operator's constraints and records it in
// the LInit label; the harness reconciles the profile against the live
// implementation (skip on mismatch under FeatIfAdvertised, hard-fail under
// FeatMandatory), exactly as in the NFSv4 model.

module posix {

    import posixfs.* from "./posix_fs"
    import posixstate.* from "./posix_state"
    import posixops.* from "./posix_ops"

    // ------------------------------------------------------------------
    // Operator-facing feature/policy modes
    // ------------------------------------------------------------------

    // Optional features:
    const F_COPY_RANGE: FeatureMode    // copy_file_range
    const F_CLONE_RANGE: FeatureMode   // clone_file_range (reflink)
    const F_SEEK_HOLE: FeatureMode     // lseek SEEK_HOLE / SEEK_DATA
    const F_ROOT: FeatureMode          // a process with euid 0 exists

    // Server/implementation discretion (POSIX acceptance sets and
    // documented divergences); trueness per capChoices:
    const P_GID_FROM_PARENT: FeatureMode   // new-object gid = parent's
    const P_SGID_INHERIT: FeatureMode      // subdirs inherit S_ISGID
    const P_WRITE_CLEARS_SETIDS: FeatureMode // write/trunc clears setids
    const P_PWRITE_APPENDS: FeatureMode    // Linux-style pwrite+O_APPEND
    const P_RENAME_CTIME: FeatureMode      // rename marks moved ctime
    const P_STRICT_ATIME: FeatureMode      // harness enforces atime marks
    const P_STICKY_WRITE_ARM: FeatureMode  // sticky exempts write-perm
    const P_ERR_NOTEMPTY: FeatureMode      // ENOTEMPTY (else EEXIST)
    const P_ERR_STICKY_ACCES: FeatureMode  // EACCES (else EPERM)
    const P_ERR_UNLINKDIR_ISDIR: FeatureMode // EISDIR (else EPERM)
    const P_ERR_LOCK_AGAIN: FeatureMode    // EAGAIN (else EACCES)

    // ------------------------------------------------------------------
    // Universes
    // ------------------------------------------------------------------

    pure val PROCS: Set[Pid] = Set(0, 1)

    // Process credentials.  Process 0 is either root or user 100 (per
    // F_ROOT); process 1 is always user 200.  Group 30 is shared so the
    // group class is exercised both ways.
    pure def credsFor(withRoot: bool): Pid -> Cred =
        Map(
            0 -> { euid: if (withRoot) 0 else 100,
                   egid: 10, ruid: if (withRoot) 0 else 100, rgid: 10,
                   groups: Set(10, 30) },
            1 -> { euid: 200, egid: 20, ruid: 200, rgid: 20,
                   groups: Set(20, 30) }
        )

    pure val NAMES: Set[Name] = Set("a", "b", "c", "d")

    // Over-long names steered by genTooLong: NLONG exceeds NAME_MAX, PLONG
    // makes the whole path exceed PATH_MAX (both ENAMETOOLONG; see posix_ops).
    // Kept out of NAMES so ordinary generation never draws them.
    pure val LONGNAMES: Set[Name] = Set(NLONG, PLONG)

    // Interesting creation/chmod modes (decimal values of octal):
    //   0, 0700, 0755, 0777, 0444, 0222, 01777 (sticky), 02755 (sgid),
    //   04755 (suid)
    pure val MODES: Set[int] = Set(0, 448, 493, 511, 292, 146, 1023, 1517,
                                   2541)

    pure val UMASKS: Set[int] = Set(0, 18, 146)   // 0, 022, 0222

    pure val MAX_OBJECTS = 10
    pure val MAX_BLOCKS = 6
    // Byte-granular I/O domains.  G is a sub-block granule; the draws mix
    // block-aligned (0, BS) with non-aligned (G, BS+G) offsets and sub-block /
    // fills-to-boundary / full / block-crossing lengths, so writes and
    // truncates hit memfs's partial-block read-modify-write and tail-zero
    // paths while keeping block-aligned coverage.
    pure val G: int = 1024
    pure val OFFS: Set[int] = Set(0, G, BS, BS + G)
    pure val LENS: Set[int] = Set(G, BS - G, BS, BS + G)
    pure val PATS: Set[int] = Set(1, 2, 3)
    // copy_file_range/clone are kept block-aligned (their model presence maps
    // whole blocks; clone additionally requires block alignment).
    pure val OFFS_BLK: Set[int] = Set(0, BS, 2 * BS)
    pure val LENS_BLK: Set[int] = Set(BS, 2 * BS)
    // A high block index (>= memfs's initial 1024-block array) so a sparse
    // write grows the block-pointer array (cthon bigfile).  4 MiB.
    pure val BIG: int = 1024 * BS

    pure val LOCK_LOS: Set[int] = Set(0, 2, 4)
    pure val LOCK_LENS: Set[int] = Set(1, 2, 4)

    pure val UIDS: Set[int] = Set(-1, 0, 100, 200)
    pure val GIDS: Set[int] = Set(-1, 10, 20, 30)

    // Explicit utimensat values (reserved negative universe; the harness
    // maps each to a fixed wall-clock value and checks exact equality).
    pure val XTIMES: Set[int] = Set(-1, -2)

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    type Label =
        | LInit({ caps: Caps })
        | LCall({ pid: Pid, req: OpReq, res: OpRes })

    var fs: Fs
    var ps: Pstate
    var caps: Caps
    var lastOp: Label

    // ------------------------------------------------------------------
    // Path construction from the model tree
    // ------------------------------------------------------------------

    // Directories reachable by name from the root (orphans excluded, so
    // parent walks below are well-defined).
    pure def reachableDirs(fs0: Fs): Set[Ino] =
        fsDirs(fs0).intersect(fsReferenced(fs0))

    // The unique name of directory d in its parent.
    pure def dirNameIn(fs0: Fs, par: Ino, d: Ino): Name =
        fs0.inodes.get(par).ents.keys()
            .filter(n => fs0.inodes.get(par).ents.get(n) == d)
            .fold("", (a, x) => x)

    // Absolute component path of a reachable directory (bounded walk up).
    pure def pathToDir(fs0: Fs, d: Ino): List[Name] = {
        val init: { cur: Ino, acc: List[Name] } = { cur: d, acc: List() }
        range(0, 12).foldl(init, (st, _) =>
            if (st.cur == ROOT) st
            else {
                val par = fs0.inodes.get(st.cur).parent
                { cur: par,
                  acc: List(dirNameIn(fs0, par, st.cur)).concat(st.acc) }
            }).acc
    }

    // Absolute path naming entry n of reachable directory d.
    pure def entryPath(fs0: Fs, d: Ino, n: Name): Path =
        { abs: true, comps: pathToDir(fs0, d).append(n), slash: false }

    pure def dirPath(fs0: Fs, d: Ino): Path =
        { abs: true, comps: pathToDir(fs0, d), slash: false }

    // (directory, name) pairs for every live entry.
    pure def entryPairs(fs0: Fs): Set[(Ino, Name)] =
        reachableDirs(fs0)
            .map(d => fs0.inodes.get(d).ents.keys().map(n => (d, n)))
            .flatten()

    pure def entryPairsOf(fs0: Fs, ft: Ftype): Set[(Ino, Name)] =
        entryPairs(fs0).filter(pr =>
            fs0.inodes.get(fs0.inodes.get(pr._1).ents.get(pr._2)).ftype == ft)

    // ------------------------------------------------------------------
    // Generator domains over descriptor state
    // ------------------------------------------------------------------

    pure def pfds(ps0: Pstate): Set[(Pid, Fd)] = ps0.fds.keys()

    pure def pfdsReg(fs0: Fs, ps0: Pstate): Set[(Pid, Fd)] =
        ps0.fds.keys().filter(k =>
            fs0.inodes.get(ps0.ofds.get(ps0.fds.get(k)).ino).ftype == FReg)

    pure def sids(ps0: Pstate): Set[StreamId] = ps0.streams.keys()

    pure def roomFor(fs0: Fs): bool =
        fs0.inodes.keys().size() < MAX_OBJECTS

    // ------------------------------------------------------------------
    // Init and the one transition
    // ------------------------------------------------------------------

    action init = {
        nondet copyR = oneOf(capChoices(F_COPY_RANGE))
        nondet cloneR = oneOf(capChoices(F_CLONE_RANGE))
        nondet seekH = oneOf(capChoices(F_SEEK_HOLE))
        nondet rootP = oneOf(capChoices(F_ROOT))
        nondet gidPar = oneOf(capChoices(P_GID_FROM_PARENT))
        nondet sgidInh = oneOf(capChoices(P_SGID_INHERIT))
        nondet wClear = oneOf(capChoices(P_WRITE_CLEARS_SETIDS))
        nondet pwApp = oneOf(capChoices(P_PWRITE_APPENDS))
        nondet rnCt = oneOf(capChoices(P_RENAME_CTIME))
        nondet sAtime = oneOf(capChoices(P_STRICT_ATIME))
        nondet stArm = oneOf(capChoices(P_STICKY_WRITE_ARM))
        nondet eNe = oneOf(capChoices(P_ERR_NOTEMPTY))
        nondet eSt = oneOf(capChoices(P_ERR_STICKY_ACCES))
        nondet eUd = oneOf(capChoices(P_ERR_UNLINKDIR_ISDIR))
        nondet eLk = oneOf(capChoices(P_ERR_LOCK_AGAIN))
        val c: Caps = {
            copyRange: copyR, cloneRange: cloneR, seekHole: seekH,
            withRoot: rootP, gidFromParent: gidPar, sgidInherit: sgidInh,
            writeClearsSets: wClear, pwriteAppends: pwApp,
            renameCtime: rnCt, strictAtime: sAtime, stickyWriteArm: stArm,
            errNotempty: eNe, errStickyAcces: eSt,
            errUnlinkDirIsdir: eUd, errLockAgain: eLk
        }
        all {
            fs' = fsInit(511, 0, 0),
            ps' = psInit(credsFor(rootP)),
            caps' = c,
            lastOp' = LInit({ caps: c })
        }
    }

    action runOp(p: Pid, req: OpReq): bool = {
        val o = dispatch(fs, ps, caps, p, req)
        all {
            fs' = o.fs2,
            ps' = o.ps2,
            caps' = caps,
            lastOp' = LCall({ pid: p, req: req, res: o.res })
        }
    }

    // ------------------------------------------------------------------
    // Generator actions
    // ------------------------------------------------------------------

    action genMkdir = all {
        roomFor(fs),
        {
            nondet p = oneOf(PROCS)
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            nondet m = oneOf(MODES)
            nondet df = oneOf(Set(true, false))
            runOp(p, RMkdir({ dfd: -1, pth: entryPath(fs, d, n)
                                  .with("slash", df),
                              mode: m }))
        }
    }

    action genMknod = all {
        roomFor(fs),
        {
            nondet p = oneOf(PROCS)
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            nondet m = oneOf(MODES)
            // FBlk/FChr exercise the device-node path: creation needs
            // privilege (EPERM otherwise; oracle in opMknod, enforced in
            // vfs_proc_mknod_at.c).  pjd mknod/11.
            nondet ft = oneOf(Set(FReg, FFifo, FBlk, FChr))
            runOp(p, RMknod({ pth: entryPath(fs, d, n), ft: ft, mode: m }))
        }
    }

    // Symlink whose target names an existing entry (relative or absolute).
    action genSymlink = all {
        roomFor(fs),
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            nondet tgtPair = oneOf(entryPairs(fs))
            nondet asAbs = oneOf(Set(true, false))
            val tgt: SymTarget =
                if (asAbs)
                    { abs: true,
                      comps: pathToDir(fs, tgtPair._1).append(tgtPair._2) }
                else { abs: false, comps: List(tgtPair._2) }
            runOp(p, RSymlink({ tgt: tgt, dfd: -1,
                                pth: entryPath(fs, d, n) }))
        }
    }

    // Self-referential symlink: resolving through it must ELOOP.
    action genSymlinkSelf = all {
        roomFor(fs),
        {
            nondet p = oneOf(PROCS)
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            runOp(p, RSymlink({ tgt: { abs: false, comps: List(n) },
                                dfd: -1, pth: entryPath(fs, d, n) }))
        }
    }

    action genLink = all {
        roomFor(fs),
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet src = oneOf(entryPairs(fs))
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            nondet fo = oneOf(Set(true, false))
            runOp(p, RLink({ dfdOld: -1,
                             pthOld: entryPath(fs, src._1, src._2),
                             dfdNew: -1, pthNew: entryPath(fs, d, n),
                             followOld: fo }))
        }
    }

    action genUnlink = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            runOp(p, RUnlink({ dfd: -1,
                               pth: entryPath(fs, pr._1, pr._2) }))
        }
    }

    action genRmdir = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            runOp(p, RRmdir({ dfd: -1,
                              pth: entryPath(fs, pr._1, pr._2) }))
        }
    }

    action genRename = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet src = oneOf(entryPairs(fs))
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            runOp(p, RRename({ dfdOld: -1,
                               pthOld: entryPath(fs, src._1, src._2),
                               dfdNew: -1,
                               pthNew: entryPath(fs, d, n) }))
        }
    }

    action genReadlink = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            runOp(p, RReadlink({ dfd: -1,
                                 pth: entryPath(fs, pr._1, pr._2) }))
        }
    }

    // Lookup on an arbitrary synthesized path: hits ENOENT / ENOTDIR /
    // EACCES / ELOOP organically (including dot and dot-dot components).
    action genStatRaw = {
        nondet p = oneOf(PROCS)
        nondet n1 = oneOf(NAMES.union(Set(".", "..")))
        nondet n2 = oneOf(NAMES.union(Set(".", "..")))
        nondet depth = oneOf(Set(1, 2))
        nondet fol = oneOf(Set(true, false))
        nondet sl = oneOf(Set(true, false))
        val comps = if (depth == 1) List(n1) else List(n1, n2)
        runOp(p, RStat({ dfd: -1,
                         pth: { abs: true, comps: comps, slash: sl },
                         follow: fol }))
    }

    action genStatEntry = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet fol = oneOf(Set(true, false))
            runOp(p, RStat({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                             follow: fol }))
        }
    }

    // Over-long name (NLONG) or whole path (PLONG) driven through the
    // path-taking calls -> ENAMETOOLONG.  The over-long component is the
    // final one under a live directory, so its prefix resolves normally
    // first; each call check_path-guards its argument.  Covers the pjd
    // `<op>/02` (NAME_MAX) and `<op>/03` (PATH_MAX) cases across
    // create/remove/metadata/stat without building a real 4 KiB path.
    action genTooLong = {
        nondet p = oneOf(PROCS)
        nondet d = oneOf(reachableDirs(fs))
        nondet ln = oneOf(LONGNAMES)
        nondet k = oneOf(Set(0, 1, 2, 3, 4, 5, 6, 7, 8))
        val pth = entryPath(fs, d, ln)
        if (k == 0) runOp(p, RMkdir({ dfd: -1, pth: pth, mode: 493 }))
        else if (k == 1) runOp(p, RMknod({ pth: pth, ft: FFifo, mode: 420 }))
        else if (k == 2)
            runOp(p, RSymlink({ tgt: { abs: false, comps: List("a") },
                                dfd: -1, pth: pth }))
        else if (k == 3) runOp(p, RChmod({ dfd: -1, pth: pth, mode: 493 }))
        else if (k == 4)
            runOp(p, RChown({ dfd: -1, pth: pth, u: 0, g: 0, follow: true }))
        else if (k == 5) runOp(p, RUnlink({ dfd: -1, pth: pth }))
        else if (k == 6) runOp(p, RRmdir({ dfd: -1, pth: pth }))
        else if (k == 7) runOp(p, RTruncate({ pth: pth, len: 0 }))
        else runOp(p, RStat({ dfd: -1, pth: pth, follow: true }))
    }

    // A path that descends one level PAST an existing entry: a non-directory
    // entry -> ENOTDIR, a directory the actor cannot search -> EACCES, a
    // searchable directory missing the child -> ENOENT.  The tree-derived
    // paths only ever route through real, searchable directories, so these
    // path-prefix errors (pjd `<op>/01` ENOTDIR, `<op>/05` EACCES-search) are
    // otherwise starved for every op except the synthetic RStat generator.
    action genThruEntry = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet m = oneOf(NAMES)
            nondet k = oneOf(Set(0, 1, 2, 3, 4, 5, 6, 7))
            val pth: Path = { abs: true,
                              comps: pathToDir(fs, pr._1).append(pr._2)
                                       .append(m),
                              slash: false }
            if (k == 0) runOp(p, RMkdir({ dfd: -1, pth: pth, mode: 493 }))
            else if (k == 1) runOp(p, RMknod({ pth: pth, ft: FFifo, mode: 420 }))
            else if (k == 2) runOp(p, RChmod({ dfd: -1, pth: pth, mode: 493 }))
            else if (k == 3)
                runOp(p, RChown({ dfd: -1, pth: pth, u: 0, g: 0, follow: true }))
            else if (k == 4) runOp(p, RUnlink({ dfd: -1, pth: pth }))
            else if (k == 5) runOp(p, RRmdir({ dfd: -1, pth: pth }))
            else if (k == 6) runOp(p, RTruncate({ pth: pth, len: 0 }))
            else runOp(p, RStat({ dfd: -1, pth: pth, follow: true }))
        }
    }

    // Rename a directory into a name directly inside itself (src -> src/m):
    // src is then an ancestor of the destination -> EINVAL.  Undirected
    // rename generation almost never lands the destination inside the source
    // subtree, so pjd rename/18 is otherwise unreachable.
    action genRenameSubtree = all {
        entryPairsOf(fs, FDir).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet srcPair = oneOf(entryPairsOf(fs, FDir))
            nondet m = oneOf(NAMES)
            val dstPath: Path = { abs: true,
                                  comps: pathToDir(fs, srcPair._1)
                                           .append(srcPair._2).append(m),
                                  slash: false }
            runOp(p, RRename({ dfdOld: -1,
                               pthOld: entryPath(fs, srcPair._1, srcPair._2),
                               dfdNew: -1, pthNew: dstPath }))
        }
    }

    // ------------------------------------------------------------------
    // Descriptor-layer generators
    // ------------------------------------------------------------------

    action genOpenFile = all {
        entryPairsOf(fs, FReg).union(entryPairsOf(fs, FLnk)).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairsOf(fs, FReg)
                                  .union(entryPairsOf(fs, FLnk)))
            nondet acc = oneOf(Set(AccR, AccW, AccRW))
            nondet tr = oneOf(Set(true, false))
            nondet ap = oneOf(Set(true, false))
            nondet nf = oneOf(Set(true, false))
            // O_TRUNC with O_RDONLY is undefined (open page); never emit it
            // (the defensive EINVAL in opOpen stays unexercised by design).
            runOp(p, ROpen({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                             fl: { acc: acc, creat: false, excl: false,
                                   trunc: tr and acc != AccR, appendF: ap,
                                   directory: false,
                                   nofollow: nf, mode: 0 } }))
        }
    }

    action genOpenCreat = all {
        roomFor(fs),
        {
            nondet p = oneOf(PROCS)
            nondet d = oneOf(reachableDirs(fs))
            nondet n = oneOf(NAMES)
            nondet m = oneOf(MODES)
            nondet ex = oneOf(Set(true, false))
            nondet acc = oneOf(Set(AccW, AccRW))
            runOp(p, ROpen({ dfd: -1, pth: entryPath(fs, d, n),
                             fl: { acc: acc, creat: true, excl: ex,
                                   trunc: false, appendF: false,
                                   directory: false, nofollow: false,
                                   mode: m } }))
        }
    }

    // Open a directory read-only (usable as a dirfd for *at calls).
    action genOpenDir = {
        nondet p = oneOf(PROCS)
        nondet d = oneOf(reachableDirs(fs))
        nondet od = oneOf(Set(true, false))
        runOp(p, ROpen({ dfd: -1, pth: dirPath(fs, d),
                         fl: { acc: AccR, creat: false, excl: false,
                               trunc: false, appendF: false,
                               directory: od, nofollow: false, mode: 0 } }))
    }

    // A path-taking call routed through a directory descriptor.
    action genAtCall = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet n = oneOf(NAMES)
            nondet which = oneOf(Set(0, 1, 2, 3))
            val rel: Path = { abs: false, comps: List(n), slash: false }
            if (which == 0)
                runOp(k._1, RStat({ dfd: k._2, pth: rel, follow: true }))
            else if (which == 1 and roomFor(fs))
                runOp(k._1, RMkdir({ dfd: k._2, pth: rel, mode: 493 }))
            else if (which == 3)
                // utimensat through a real dirfd (exercises the dir_entry
                // acquire branch the AT_FDCWD path skips).
                runOp(k._1, RUtimens({ dfd: k._2, pth: rel, ta: TsNow,
                                       tm: TsNow }))
            else
                runOp(k._1, RUnlink({ dfd: k._2, pth: rel }))
        }
    }

    action genClose = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            runOp(k._1, RClose({ fd: k._2 }))
        }
    }

    action genDup = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet two = oneOf(Set(true, false))
            nondet nfd = oneOf(Set(0, 1, 2, 3))
            if (two) runOp(k._1, RDup2({ fd: k._2, nfd: nfd }))
            else runOp(k._1, RDup({ fd: k._2 }))
        }
    }

    action genLseek = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet off = oneOf(OFFS.union(Set(-1)))
            nondet wh = oneOf(Set(WSet, WCur, WEnd, WData, WHole))
            runOp(k._1, RLseek({ fd: k._2, off: off, wh: wh }))
        }
    }

    action genRead = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet len = oneOf(LENS)
            runOp(k._1, RRead({ fd: k._2, len: len }))
        }
    }

    action genWrite = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet len = oneOf(LENS)
            nondet pat = oneOf(PATS)
            runOp(k._1, RWrite({ fd: k._2, len: len, pat: pat }))
        }
    }

    action genPio = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet off = oneOf(OFFS)
            nondet len = oneOf(LENS)
            nondet pat = oneOf(PATS)
            nondet rd = oneOf(Set(true, false))
            if (rd) runOp(k._1, RPread({ fd: k._2, off: off, len: len }))
            else runOp(k._1, RPwrite({ fd: k._2, off: off, len: len,
                                       pat: pat }))
        }
    }

    // Vectored counterparts of genRead/genWrite/genPio.  Same draws; the
    // harness issues readv/writev/preadv2/pwritev2 instead of the scalar
    // calls, so the client's iovec-splitting path gets exercised.
    action genVread = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet len = oneOf(LENS)
            runOp(k._1, RReadv({ fd: k._2, len: len }))
        }
    }

    action genVwrite = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet len = oneOf(LENS)
            nondet pat = oneOf(PATS)
            runOp(k._1, RWritev({ fd: k._2, len: len, pat: pat }))
        }
    }

    action genVpio = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet off = oneOf(OFFS)
            nondet len = oneOf(LENS)
            nondet pat = oneOf(PATS)
            nondet rd = oneOf(Set(true, false))
            if (rd) runOp(k._1, RPreadv({ fd: k._2, off: off, len: len }))
            else runOp(k._1, RPwritev({ fd: k._2, off: off, len: len,
                                        pat: pat }))
        }
    }

    // Sparse write at a high block index to grow memfs's block-pointer array
    // (cthon bigfile).  Draws {0, BIG, BIG+BS} so a trace writes low then high
    // and hits both the array-double and the copy-old-array branches.
    action genBigWrite = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet off = oneOf(Set(0, BIG, BIG + BS))
            nondet pat = oneOf(PATS)
            runOp(k._1, RPwrite({ fd: k._2, off: off, len: BS, pat: pat }))
        }
    }

    action genTruncate = all {
        entryPairsOf(fs, FReg).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairsOf(fs, FReg))
            nondet len = oneOf(OFFS)
            runOp(p, RTruncate({ pth: entryPath(fs, pr._1, pr._2),
                                 len: len }))
        }
    }

    action genFtruncate = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet len = oneOf(OFFS)
            runOp(k._1, RFtruncate({ fd: k._2, len: len }))
        }
    }

    action genFstat = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            runOp(k._1, RFstat({ fd: k._2 }))
        }
    }

    // statfs/statvfs over existing entries (success) and constructed paths
    // (ENOENT/ENOTDIR prefix errors), and fstatfs/fstatvfs over live and bad
    // descriptors.  statvfs vs statfs picked per draw; both resolve the same,
    // only the syscall differs.
    action genStatfs = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet vfs = oneOf(Set(true, false))
            nondet raw = oneOf(Set(true, false))
            nondet pr = oneOf(entryPairs(fs))
            nondet n1 = oneOf(NAMES)
            nondet n2 = oneOf(NAMES)
            val pth = if (raw) { abs: true, comps: List(n1, n2), slash: false }
                      else entryPath(fs, pr._1, pr._2)
            if (vfs) runOp(p, RStatvfs({ pth: pth }))
            else runOp(p, RStatfs({ pth: pth }))
        }
    }

    action genFstatfs = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet vfs = oneOf(Set(true, false))
            if (vfs) runOp(k._1, RFstatvfs({ fd: k._2 }))
            else runOp(k._1, RFstatfs({ fd: k._2 }))
        }
    }

    action genFcntlMisc = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet which = oneOf(Set(0, 1, 2))
            nondet ap = oneOf(Set(true, false))
            nondet base = oneOf(Set(0, 2, 5))
            if (which == 0) runOp(k._1, RFcntlGetfl({ fd: k._2 }))
            else if (which == 1)
                runOp(k._1, RFcntlSetfl({ fd: k._2, appendF: ap }))
            else runOp(k._1, RFcntlDupfd({ fd: k._2, atLeast: base }))
        }
    }

    action genFsync = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet dataOnly = oneOf(Set(true, false))
            runOp(k._1, RFsync({ fd: k._2, dataOnly: dataOnly }))
        }
    }

    // Descriptor negatives: operations on a never-opened fd.
    action genEbadf = {
        nondet p = oneOf(PROCS)
        nondet which = oneOf(Set(0, 1, 2))
        val bad = 13
        if (which == 0) runOp(p, RClose({ fd: bad }))
        else if (which == 1) runOp(p, RRead({ fd: bad, len: 1 }))
        else runOp(p, RFstat({ fd: bad }))
    }

    // ------------------------------------------------------------------
    // Metadata generators
    // ------------------------------------------------------------------

    action genChmod = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet m = oneOf(MODES)
            runOp(p, RChmod({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                              mode: m }))
        }
    }

    action genFchmod = all {
        pfds(ps).size() > 0,
        {
            nondet k = oneOf(pfds(ps))
            nondet m = oneOf(MODES)
            runOp(k._1, RFchmod({ fd: k._2, mode: m }))
        }
    }

    action genChown = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet u = oneOf(UIDS)
            nondet g = oneOf(GIDS)
            nondet fol = oneOf(Set(true, false))
            runOp(p, RChown({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                              u: u, g: g, follow: fol }))
        }
    }

    action genUtimens = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet ta = oneOf(Set(TsNow, TsOmit, TsVal(-1), TsVal(-2)))
            nondet tm = oneOf(Set(TsNow, TsOmit, TsVal(-1), TsVal(-2)))
            runOp(p, RUtimens({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                                ta: ta, tm: tm }))
        }
    }

    action genAccess = all {
        entryPairs(fs).size() > 0,
        {
            nondet p = oneOf(PROCS)
            nondet pr = oneOf(entryPairs(fs))
            nondet wr = oneOf(Set(true, false))
            nondet ww = oneOf(Set(true, false))
            nondet wx = oneOf(Set(true, false))
            nondet eff = oneOf(Set(true, false))
            runOp(p, RAccess({ dfd: -1, pth: entryPath(fs, pr._1, pr._2),
                               r: wr, w: ww, x: wx, eff: eff }))
        }
    }

    action genUmask = {
        nondet p = oneOf(PROCS)
        nondet m = oneOf(UMASKS)
        runOp(p, RUmask({ mask: m }))
    }

    // ------------------------------------------------------------------
    // Locks
    // ------------------------------------------------------------------

    action genLockSet = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet lk = oneOf(Set(LkRd, LkWr, LkUn))
            nondet lo = oneOf(LOCK_LOS)
            nondet len = oneOf(LOCK_LENS)
            runOp(k._1, RFcntlLock({ fd: k._2, cmd: CSetlk, lk: lk,
                                     lo: lo, hi: lo + len }))
        }
    }

    // Blocking acquire: only issued when the model says it will not block.
    action genLockWait = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet lk = oneOf(Set(LkRd, LkWr))
            nondet lo = oneOf(LOCK_LOS)
            nondet len = oneOf(LOCK_LENS)
            all {
                not(lockConflict(ps, k._1,
                    fdOfd(ps, k._1, k._2).ino, lo, lo + len,
                    if (lk == LkRd) LRead else LWrite)),
                runOp(k._1, RFcntlLock({ fd: k._2, cmd: CSetlkw, lk: lk,
                                         lo: lo, hi: lo + len }))
            }
        }
    }

    action genLockGet = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet lk = oneOf(Set(LkRd, LkWr))
            nondet lo = oneOf(LOCK_LOS)
            nondet len = oneOf(LOCK_LENS)
            runOp(k._1, RFcntlLock({ fd: k._2, cmd: CGetlk, lk: lk,
                                     lo: lo, hi: lo + len }))
        }
    }

    action genLockf = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet cmd = oneOf(Set(LfTlock, LfUlock, LfTst))
            nondet len = oneOf(Set(0, 1, 2))
            runOp(k._1, RLockf({ fd: k._2, cmd: cmd, len: len }))
        }
    }

    // ------------------------------------------------------------------
    // Directory streams
    // ------------------------------------------------------------------

    action genOpendir = {
        nondet p = oneOf(PROCS)
        nondet d = oneOf(reachableDirs(fs))
        runOp(p, ROpendir({ pth: dirPath(fs, d) }))
    }

    action genReaddir = all {
        sids(ps).size() > 0,
        {
            nondet s = oneOf(sids(ps))
            nondet which = oneOf(Set(0, 1, 2, 3))
            nondet loc = oneOf(Set(0, 1))
            val p = ps.streams.get(s).pid
            if (which == 0) runOp(p, RReaddir({ sid: s }))
            else if (which == 1) runOp(p, RRewinddir({ sid: s }))
            else if (which == 2) runOp(p, RTelldir({ sid: s }))
            else runOp(p, RSeekdir({ sid: s, loc: loc }))
        }
    }

    action genClosedir = all {
        sids(ps).size() > 0,
        {
            nondet s = oneOf(sids(ps))
            runOp(ps.streams.get(s).pid, RClosedir({ sid: s }))
        }
    }

    // ------------------------------------------------------------------
    // copy / clone
    // ------------------------------------------------------------------

    action genCopyRange = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet kIn = oneOf(pfdsReg(fs, ps))
            nondet kOut = oneOf(pfdsReg(fs, ps).filter(k => k._1 == kIn._1))
            nondet oIn = oneOf(OFFS_BLK)
            nondet oOut = oneOf(OFFS_BLK)
            nondet len = oneOf(LENS_BLK)
            nondet clone = oneOf(Set(true, false))
            if (clone)
                runOp(kIn._1, RCloneRange({ fdDst: kOut._2, offDst: oOut,
                                            fdSrc: kIn._2, offSrc: oIn,
                                            len: len }))
            else
                runOp(kIn._1, RCopyRange({ fdIn: kIn._2, offIn: oIn,
                                           fdOut: kOut._2, offOut: oOut,
                                           len: len }))
        }
    }

    // fallocate/posix_fallocate on a regular-file descriptor.  mode 0 grows;
    // mode 1 punches a hole (KEEP_SIZE).  len 0 steers the EINVAL path; a
    // read-only draw from pfdsReg steers EBADF.
    action genFallocate = all {
        pfdsReg(fs, ps).size() > 0,
        {
            nondet k = oneOf(pfdsReg(fs, ps))
            nondet mode = oneOf(Set(0, 1))
            nondet off = oneOf(OFFS)
            nondet len = oneOf(Set(0).union(LENS))   // 0 steers the EINVAL path
            runOp(k._1, RFallocate({ fd: k._2, mode: mode, off: off,
                                     len: len }))
        }
    }

    // ------------------------------------------------------------------
    // Step distributions
    // ------------------------------------------------------------------

    action stepNamespace = any {
        genMkdir, genMknod, genSymlink, genSymlinkSelf, genLink,
        genUnlink, genRmdir, genRename, genReadlink, genStatRaw,
        genStatEntry, genTooLong, genThruEntry, genRenameSubtree
    }

    action stepDescriptors = any {
        genOpenFile, genOpenCreat, genOpenDir, genAtCall, genClose,
        genDup, genLseek, genRead, genWrite, genPio, genTruncate,
        genFtruncate, genFstat, genFstatfs, genFcntlMisc, genFsync, genEbadf
    }

    action stepMetadata = any {
        genChmod, genFchmod, genChown, genUtimens, genAccess, genUmask,
        genStatEntry, genStatfs, genFstatfs
    }

    action stepLocks = any {
        genLockSet, genLockWait, genLockGet, genLockf, genOpenFile,
        genClose
    }

    action stepDirs = any {
        genOpendir, genReaddir, genClosedir, genMkdir, genUnlink
    }

    action stepData = any {
        genRead, genWrite, genPio, genVread, genVwrite, genVpio, genLseek,
        genTruncate, genCopyRange, genFallocate, genStatfs, genFstatfs,
        genOpenFile, genOpenCreat, genClose
    }

    // Focused flavors for behaviors the broad mixes starve:

    // Permission interplay between the two credentials: ownership churn,
    // mode churn, and opens/access checks that race them.
    action stepPerms = any {
        genChmod, genFchmod, genChown, genUmask, genAccess, genOpenFile,
        genOpenCreat, genUtimens, genStatEntry
    }

    // Symlink webs: creation (incl. self-loops), resolution through
    // links (stat raw paths, link-follow), and namespace churn around
    // them.
    action stepLinks = any {
        genSymlink, genSymlinkSelf, genLink, genReadlink, genStatRaw,
        genStatEntry, genRename, genUnlink, genOpenFile
    }

    // Descriptor-table churn: duplicates, dirfd-relative calls (including
    // through directories removed while open), and fcntl traffic.
    action stepFds = any {
        genOpenFile, genOpenCreat, genOpenDir, genDup, genAtCall,
        genClose, genLseek, genFcntlMisc, genRmdir, genUnlink
    }

    // Over-long names/paths (ENAMETOOLONG): a little tree-building so the
    // over-long final component lands at varied depths, plus the over-long
    // path-op mix.
    action stepTooLong = any {
        genMkdir, genMknod, genTooLong
    }

    // Path-prefix errors (ENOTDIR non-dir prefix, EACCES-search) and
    // rename-into-own-subtree (EINVAL): build restricted and nested
    // directories, then drive the hostile-prefix and subtree-rename ops.
    action stepPrefix = any {
        genMkdir, genMknod, genChmod, genThruEntry, genRenameSubtree,
        genRmdir, genUnlink
    }

    // Device-node mknod (S_IFBLK/S_IFCHR): privileged creation succeeds,
    // unprivileged is EPERM (pjd mknod/11).  Concentrated so genMknod's
    // device draws are not starved by the broad namespace mix.
    action stepDevice = any {
        genMkdir, genMknod, genUnlink, genStatEntry
    }

    // fallocate/posix_fallocate (grow + PUNCH_HOLE|KEEP_SIZE): build and
    // populate regular files, then reserve/deallocate over them and read the
    // holes back.  Concentrated so the allocate path is not starved by the
    // broad data mix (pjd covers this via posix_fallocate; the coverage diff
    // flagged it as entirely unmodeled).  genClose recycles descriptors so the
    // process never piles up to MAX_FDS: a spurious EMFILE (PD24) on an
    // O_TRUNC open would otherwise skip a truncation the live 1024-slot table
    // still performs, an irreconcilable state divergence.
    action stepFallocate = any {
        genOpenCreat, genOpenFile, genClose, genWrite, genFallocate,
        genTruncate, genRead, genFstat
    }

    // Vectored I/O (readv/writev/preadv2/pwritev2): populate regular files,
    // then read and write them through iovec-splitting calls.  Concentrated
    // so the vectored client path is not starved by the broad data mix (the
    // coverage diff flagged posix_readv.c/posix_writev.c and the client's
    // vectored write path as entirely unmodeled).
    action stepVectored = any {
        genOpenCreat, genOpenFile, genClose, genVwrite, genVread,
        genVpio, genWrite, genTruncate, genFstat
    }

    // Large sparse files: grow the block-pointer array, then read/stat back.
    action stepBigfile = any {
        genOpenCreat, genOpenFile, genClose, genBigWrite, genFstat, genRead
    }

    action step = any {
        stepNamespace, stepDescriptors, stepMetadata, stepLocks,
        stepDirs, stepData
    }

    // ------------------------------------------------------------------
    // Invariants
    // ------------------------------------------------------------------

    val invFs = fsInv(fs)
    val invPs = psInv(ps)

    // Every inode referenced from descriptor state exists.
    val statePinsLive =
        psPinned(ps).subseteq(fs.inodes.keys()) and
        ps.locks.keys().forall(k => fs.inodes.keys().contains(k._2))

    // Orphans only survive while something pins them.
    val orphansPinned = fsOrphans(fs).subseteq(psPinned(ps))

    // A process holds record locks only on files it still has open.
    val locksHaveFd =
        ps.locks.keys().forall(k =>
            fdsOf(ps, k._1).exists(fd => fdOfd(ps, k._1, fd).ino == k._2))

    // The drawn profile respects the operator's modes.
    val capsRespected = all {
        capChoices(F_COPY_RANGE).contains(caps.copyRange),
        capChoices(F_CLONE_RANGE).contains(caps.cloneRange),
        capChoices(F_SEEK_HOLE).contains(caps.seekHole),
        capChoices(F_ROOT).contains(caps.withRoot),
        capChoices(P_GID_FROM_PARENT).contains(caps.gidFromParent),
        capChoices(P_SGID_INHERIT).contains(caps.sgidInherit),
        capChoices(P_WRITE_CLEARS_SETIDS).contains(caps.writeClearsSets),
        capChoices(P_PWRITE_APPENDS).contains(caps.pwriteAppends),
        capChoices(P_RENAME_CTIME).contains(caps.renameCtime),
        capChoices(P_STRICT_ATIME).contains(caps.strictAtime),
        capChoices(P_STICKY_WRITE_ARM).contains(caps.stickyWriteArm),
        capChoices(P_ERR_NOTEMPTY).contains(caps.errNotempty),
        capChoices(P_ERR_STICKY_ACCES).contains(caps.errStickyAcces),
        capChoices(P_ERR_UNLINKDIR_ISDIR).contains(caps.errUnlinkDirIsdir),
        capChoices(P_ERR_LOCK_AGAIN).contains(caps.errLockAgain)
    }

    val inv = all {
        invFs,
        invPs,
        statePinsLive,
        orphansPinned,
        locksHaveFd,
        capsRespected
    }
}
