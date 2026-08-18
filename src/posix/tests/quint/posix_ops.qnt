// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// POSIX.1-2024 syscall semantics: pathname resolution, the file access
// permission algorithm, and one handler per modeled function mapping
// arguments to errno results and state transitions over posixfs +
// posixstate.
//
// Sources: the numbered rule inventories in posix-notes/ (fetched and
// verified against the Issue 8 text).  Conventions:
//  - Every handler returns the full { fs2, ps2, res } outcome; res is a
//    sum type whose every variant carries the errno `e` (0 = success).
//  - Where POSIX gives implementations a choice that affects observable
//    behavior, the choice is pinned in the capability/policy profile
//    `Caps`, drawn nondeterministically at init within the operator's
//    FeatureMode constraints and recorded in the trace's init label, so
//    replay stays deterministic (same design as the NFSv4 model).
//  - Where POSIX names two errnos for one condition (acceptance sets like
//    rmdir-nonempty's {ENOTEMPTY, EEXIST}), the served value is a Caps
//    policy knob rather than a harness-side acceptance registry, keeping
//    the trace exact.
//  - Errno numeric values are Linux's (the harness platform).

module posixops {

    import posixfs.* from "./posix_fs"
    import posixstate.* from "./posix_state"

    // ------------------------------------------------------------------
    // Errno values (Linux numbering; 0 = success)
    // ------------------------------------------------------------------

    pure val OK = 0
    pure val EPERM = 1
    pure val ENOENT = 2
    pure val EIO = 5
    pure val ENXIO = 6
    pure val EBADF = 9
    pure val EAGAIN = 11
    pure val EACCES = 13
    pure val EBUSY = 16
    pure val EEXIST = 17
    pure val EXDEV = 18
    pure val ENOTDIR = 20
    pure val EISDIR = 21
    pure val EINVAL = 22
    pure val EMFILE = 24
    pure val EFBIG = 27
    pure val ENOSPC = 28
    pure val ESPIPE = 29
    pure val EROFS = 30
    pure val EMLINK = 31
    pure val EDEADLK = 35
    pure val ENAMETOOLONG = 36
    pure val ENOTEMPTY = 39
    pure val ELOOP = 40
    pure val EOPNOTSUPP = 95

    // ------------------------------------------------------------------
    // Feature / policy gating
    // ------------------------------------------------------------------

    // Operator-facing mode for each optional feature or server-discretion
    // policy: FeatOff pins it unsupported/false, FeatMandatory pins it
    // supported/true, FeatIfAdvertised lets init explore both and the
    // harness reconcile against the live server (skip on mismatch).
    type FeatureMode = FeatOff | FeatIfAdvertised | FeatMandatory

    pure def capChoices(m: FeatureMode): Set[bool] =
        match m {
            | FeatOff => Set(false)
            | FeatMandatory => Set(true)
            | FeatIfAdvertised => Set(false, true)
        }

    // The capability/policy profile pinned at init.
    type Caps = {
        // Optional features (absent => the op fails EOPNOTSUPP/EINVAL):
        copyRange: bool,     // copy_file_range supported
        cloneRange: bool,    // clone_file_range supported
        seekHole: bool,      // lseek SEEK_HOLE/SEEK_DATA supported
        // Universe shape:
        withRoot: bool,      // process 0 runs with euid 0
        // Creation-time discretion (POSIX acceptance sets):
        gidFromParent: bool, // new object gid = parent dir's gid (else egid)
        sgidInherit: bool,   // new subdir inherits parent's S_ISGID
        // Setid clearing / divergence policies:
        writeClearsSets: bool,  // write/truncate by unprivileged clears setids
        pwriteAppends: bool,    // pwrite honors O_APPEND (Linux divergence;
                                // POSIX: writes at the given offset)
        renameCtime: bool,      // rename marks the moved object's ctime
        strictAtime: bool,      // harness enforces atime marking exactly
        stickyWriteArm: bool,   // sticky check exempts write-permitted callers
        // Errno choices POSIX leaves open (true = first-named value):
        errNotempty: bool,      // ENOTEMPTY (else EEXIST) for non-empty dirs
        errStickyAcces: bool,   // EACCES (else EPERM) for sticky violations
        errUnlinkDirIsdir: bool, // EISDIR (else EPERM) for unlink(directory)
        errLockAgain: bool      // EAGAIN (else EACCES) for lock conflicts
    }

    pure def errNonempty(caps: Caps): int =
        if (caps.errNotempty) ENOTEMPTY else EEXIST
    pure def errSticky(caps: Caps): int =
        if (caps.errStickyAcces) EACCES else EPERM
    pure def errUnlinkDir(caps: Caps): int =
        if (caps.errUnlinkDirIsdir) EISDIR else EPERM
    pure def errLockBusy(caps: Caps): int =
        if (caps.errLockAgain) EAGAIN else EACCES

    // ------------------------------------------------------------------
    // The file access permission algorithm (XBD 4.5)
    // ------------------------------------------------------------------

    pure def isPriv(cr: Cred): bool = cr.euid == 0

    pure def inGroup(cr: Cred, g: int): bool =
        cr.egid == g or cr.groups.contains(g)

    // Exclusive class selection: owner class if euid matches (even when its
    // bits deny what another class would grant), else group class on any
    // group match, else other class.
    pure def classBit(cr: Cred, nd: Inode, ownerB: int, groupB: int,
                      otherB: int): bool =
        if (cr.euid == nd.uid) bitOn(nd.mode, ownerB)
        else if (inGroup(cr, nd.gid)) bitOn(nd.mode, groupB)
        else bitOn(nd.mode, otherB)

    pure def mayRead(cr: Cred, nd: Inode): bool =
        isPriv(cr) or classBit(cr, nd, MB_UR, MB_GR, MB_OR)

    pure def mayWrite(cr: Cred, nd: Inode): bool =
        isPriv(cr) or classBit(cr, nd, MB_UW, MB_GW, MB_OW)

    // Execute/search: privilege grants directory search outright but
    // executes a non-directory only when at least one x bit is set
    // (XBD 4.5).
    pure def mayExec(cr: Cred, nd: Inode): bool =
        if (isPriv(cr))
            nd.ftype == FDir or
                bitOn(nd.mode, MB_UX) or bitOn(nd.mode, MB_GX) or
                bitOn(nd.mode, MB_OX)
        else classBit(cr, nd, MB_UX, MB_GX, MB_OX)

    // Sticky-bit deletion rule (XBD 4.5): removing/renaming an entry in a
    // S_ISVTX directory needs euid == entry owner, euid == dir owner, or
    // privilege; Issue 8 leaves an additional write-permission-on-the-file
    // arm implementation-defined (caps.stickyWriteArm).
    pure def stickyOk(caps: Caps, cr: Cred, dnode: Inode, vnode: Inode): bool =
        not(bitOn(dnode.mode, MB_SVTX)) or
        isPriv(cr) or
        cr.euid == vnode.uid or
        cr.euid == dnode.uid or
        (caps.stickyWriteArm and mayWrite(cr, vnode))

    // Apply umask to a requested creation mode (bitwise m & ~mask over the
    // nine permission bits; the setid/sticky bits pass through untouched).
    pure def applyUmask(m: int, mask: int): int =
        [MB_SUID, MB_SGID, MB_SVTX, MB_UR, MB_UW, MB_UX,
         MB_GR, MB_GW, MB_GX, MB_OR, MB_OW, MB_OX]
            .foldl(0, (acc, b) =>
                if (bitOn(m, b) and not(b < MB_SVTX and bitOn(mask, b)))
                    acc + b
                else acc)

    // Group of a newly created object (acceptance set pinned in caps; a
    // S_ISGID parent directory forces the parent's gid).
    pure def newGid(caps: Caps, cr: Cred, dnode: Inode): int =
        if (bitOn(dnode.mode, MB_SGID) or caps.gidFromParent) dnode.gid
        else cr.egid

    // Mode of a newly created directory: umask-filtered, plus inherited
    // S_ISGID when the policy says so.
    pure def newDirMode(caps: Caps, m: int, mask: int, dnode: Inode): int = {
        val base = applyUmask(m, mask)
        if (caps.sgidInherit and bitOn(dnode.mode, MB_SGID) and
                not(bitOn(base, MB_SGID)))
            base + MB_SGID
        else base
    }

    // ------------------------------------------------------------------
    // Pathname resolution (XBD 4.16)
    // ------------------------------------------------------------------

    // An abstract pathname.  abs paths start at the filesystem root (the
    // model has no working directory; relative paths only occur in *at
    // calls, resolved from the dirfd).  slash records a trailing slash.
    type Path = { abs: bool, comps: List[Name], slash: bool }

    // Symlink expansion budget: POSIX guarantees at least SYMLOOP_MAX = 8
    // expansions resolve.  Generators keep acyclic chains far below this
    // (see DESIGN-POSIX.md), so hitting the budget means a genuine cycle
    // and the model can safely report ELOOP.
    pure val SYMLOOP = 8
    // Iteration fuel: components + expanded targets in this model's small
    // universes never exceed this.
    pure val RESOLVE_FUEL = 48

    // Over-long path components (ENAMETOOLONG steering).  NLONG stands for a
    // single component longer than NAME_MAX; PLONG stands for a whole path
    // longer than PATH_MAX.  The replay driver materializes them to real
    // over-long strings; keeping them as abstract sentinels lets the
    // generator reach the pjd `<op>/02` (NAME_MAX) and `<op>/03` (PATH_MAX)
    // scenarios without building a real 4 KiB path.  chimera guards PATH_MAX
    // in chimera_posix_check_path (before resolution, so it precedes ENOENT/
    // ENOTDIR) and NAME_MAX in the VFS layer (when the component is reached,
    // after its parent is validated) -- both surface as ENAMETOOLONG.
    pure val NLONG: Name = "@nlong"
    pure val PLONG: Name = "@plong"

    pure def pathTooLong(pth: Path): bool =
        pth.comps.foldl(false, (a, c) => a or c == PLONG)

    // Resolution outcome.
    //   err != 0             failed
    //   err == 0, ino >= 0   resolved to an existing object; par/leaf name
    //                        the final entry when it was reached through a
    //                        directory (par == -1 for a bare root path)
    //   err == 0, ino == -1  final component missing; par is the directory
    //                        that would receive it, leaf the name (only
    //                        callers that create treat this as success)
    type RRes = { err: int, ino: Ino, par: Ino, leaf: Name }

    type RWalk = {
        cur: Ino,          // directory being walked
        rest: List[Name],  // remaining components
        hops: int,         // symlink expansions so far
        done: bool,
        out: RRes
    }

    pure def rFail(e: int): RRes = { err: e, ino: -1, par: -1, leaf: "" }

    // One resolution step: consume the head component of w.rest.
    pure def rStep(fs: Fs, caps: Caps, cr: Cred, followFinal: bool,
                   w: RWalk): RWalk = {
        if (w.done or w.rest.length() == 0) w
        else {
            val cnode = fs.inodes.get(w.cur)
            val c = w.rest.head()
            val rest1 = w.rest.tail()
            val last = rest1.length() == 0
            if (cnode.ftype != FDir)
                w.with("done", true).with("out", rFail(ENOTDIR))
            else if (not(mayExec(cr, cnode)))
                w.with("done", true).with("out", rFail(EACCES))
            else if (c == NLONG)
                w.with("done", true).with("out", rFail(ENAMETOOLONG))
            else if (c == ".")
                if (last)
                    w.with("done", true).with("out",
                        { err: OK, ino: w.cur, par: w.cur, leaf: "." })
                else w.with("rest", rest1)
            else if (c == "..")
                if (last)
                    w.with("done", true).with("out",
                        { err: OK, ino: cnode.parent, par: w.cur, leaf: ".." })
                else w.with("cur", cnode.parent).with("rest", rest1)
            else if (not(fsHasEnt(fs, w.cur, c)))
                if (last)
                    // Missing final entry: report the would-be parent.
                    w.with("done", true).with("out",
                        { err: OK, ino: -1, par: w.cur, leaf: c })
                else
                    w.with("done", true).with("out", rFail(ENOENT))
            else {
                val child = fsChild(fs, w.cur, c)
                val chnode = fs.inodes.get(child)
                if (chnode.ftype == FLnk and (not(last) or followFinal))
                    // Expand the symlink into the remaining work list.
                    if (w.hops >= SYMLOOP)
                        w.with("done", true).with("out", rFail(ELOOP))
                    else
                        w.with("hops", w.hops + 1)
                         .with("cur", if (chnode.target.abs) ROOT else w.cur)
                         .with("rest", chnode.target.comps.concat(rest1))
                else if (last)
                    w.with("done", true).with("out",
                        { err: OK, ino: child, par: w.cur, leaf: c })
                else
                    w.with("cur", child).with("rest", rest1)
            }
        }
    }

    // Resolve `pth` starting from `start` (ignored for absolute paths).
    // followFinal selects whether a final-component symlink is expanded.
    // A trailing slash requires the result to be (or be creatable as) a
    // directory: an existing non-directory fails ENOTDIR, and a missing
    // final with a trailing slash resolves for the caller to judge (mkdir
    // allows it; open O_CREAT maps it to ENOENT).
    pure def resolve(fs: Fs, caps: Caps, cr: Cred, start: Ino, pth: Path,
                     followFinal: bool): RRes = {
        val begin = if (pth.abs) ROOT else start
        if (pathTooLong(pth))
            // Whole path over PATH_MAX: chimera_posix_check_path rejects it
            // up front, before any component is resolved.
            rFail(ENAMETOOLONG)
        else if (pth.comps.length() == 0)
            // "" is ENOENT; "/" resolves to the root itself.
            if (pth.abs) { err: OK, ino: ROOT, par: -1, leaf: "" }
            else rFail(ENOENT)
        else {
            // A trailing slash forces following a final symlink (XBD 4.16;
            // the slash names the directory the link resolves to), which
            // is why the ENOTDIR check below sees the followed object.
            val ff = followFinal or pth.slash
            val w0 = { cur: begin, rest: pth.comps, hops: 0, done: false,
                       out: rFail(EIO) }
            val wN = range(0, RESOLVE_FUEL).foldl(w0, (w, _) =>
                rStep(fs, caps, cr, ff, w))
            val out = if (wN.done) wN.out else rFail(ELOOP)
            if (out.err == OK and out.ino >= 0 and pth.slash and
                    fs.inodes.get(out.ino).ftype != FDir)
                rFail(ENOTDIR)
            else out
        }
    }

    // Resolution for pure lookups: a missing final component is ENOENT.
    pure def resolveEx(fs: Fs, caps: Caps, cr: Cred, start: Ino, pth: Path,
                       followFinal: bool): RRes = {
        val r = resolve(fs, caps, cr, start, pth, followFinal)
        if (r.err == OK and r.ino == -1) rFail(ENOENT) else r
    }

    // ------------------------------------------------------------------
    // Requests and results
    // ------------------------------------------------------------------

    // Descriptor-table capacity (bounds lowest-fd searches; universes keep
    // well below it, so EMFILE is never exercised).
    pure val MAX_FDS = 16

    // Whole-file lock ranges (l_len = 0) cover [0, LOCK_SPAN) of the
    // abstract byte universe; the harness maps that span to an unbounded
    // wire range.
    pure val LOCK_SPAN = 8

    type OpenFlags = {
        acc: AccMode,
        creat: bool,
        excl: bool,
        trunc: bool,
        appendF: bool,
        directory: bool,
        nofollow: bool,
        mode: int
    }

    type Whence = WSet | WCur | WEnd | WData | WHole

    type LockReq = LkRd | LkWr | LkUn
    type LockCmd = CSetlk | CSetlkw | CGetlk
    type LockfCmd = LfLock | LfTlock | LfUlock | LfTst

    type TimeSpec = TsNow | TsOmit | TsVal(int)

    // One request per modeled function.  Path-taking requests carry a
    // dirfd `dfd`; -1 means the plain (absolute-path) call and the harness
    // issues the non-at variant, otherwise the *at variant with that fd.
    type OpReq =
        | ROpen({ dfd: Fd, pth: Path, fl: OpenFlags })
        | RClose({ fd: Fd })
        | RDup({ fd: Fd })
        | RDup2({ fd: Fd, nfd: Fd })
        | RLseek({ fd: Fd, off: int, wh: Whence })
        | RRead({ fd: Fd, len: int })
        | RWrite({ fd: Fd, len: int, pat: int })
        | RPread({ fd: Fd, off: int, len: int })
        | RPwrite({ fd: Fd, off: int, len: int, pat: int })
        // Vectored I/O.  POSIX-observably identical to the scalar forms
        // (same bytes, errno, and offset advance), so the oracle delegates;
        // the harness realizes them by splitting the buffer across iovecs and
        // calling the readv/writev/preadv2/pwritev2 entry points, which the
        // scalar ops never exercise.
        | RReadv({ fd: Fd, len: int })
        | RWritev({ fd: Fd, len: int, pat: int })
        | RPreadv({ fd: Fd, off: int, len: int })
        | RPwritev({ fd: Fd, off: int, len: int, pat: int })
        // Filesystem-wide stat.  statfs/statvfs take a path, fstatfs/fstatvfs
        // a descriptor; the returned capacity fields are backend-specific and
        // untracked, so the oracle asserts only the errno (path resolution or
        // a live fd).
        | RStatfs({ pth: Path })
        | RStatvfs({ pth: Path })
        | RFstatfs({ fd: Fd })
        | RFstatvfs({ fd: Fd })
        | RTruncate({ pth: Path, len: int })
        | RFtruncate({ fd: Fd, len: int })
        | RStat({ dfd: Fd, pth: Path, follow: bool })
        | RFstat({ fd: Fd })
        | RChmod({ dfd: Fd, pth: Path, mode: int })
        | RFchmod({ fd: Fd, mode: int })
        | RChown({ dfd: Fd, pth: Path, u: int, g: int, follow: bool })
        | RFchown({ fd: Fd, u: int, g: int })
        | RUtimens({ dfd: Fd, pth: Path, ta: TimeSpec, tm: TimeSpec })
        | RFutimens({ fd: Fd, ta: TimeSpec, tm: TimeSpec })
        | RAccess({ dfd: Fd, pth: Path, r: bool, w: bool, x: bool, eff: bool })
        | RUmask({ mask: int })
        | RMkdir({ dfd: Fd, pth: Path, mode: int })
        | RMknod({ pth: Path, ft: Ftype, mode: int })
        | RSymlink({ tgt: SymTarget, dfd: Fd, pth: Path })
        | RLink({ dfdOld: Fd, pthOld: Path, dfdNew: Fd, pthNew: Path,
                  followOld: bool })
        | RUnlink({ dfd: Fd, pth: Path })
        | RRmdir({ dfd: Fd, pth: Path })
        | RRename({ dfdOld: Fd, pthOld: Path, dfdNew: Fd, pthNew: Path })
        | RReadlink({ dfd: Fd, pth: Path })
        | ROpendir({ pth: Path })
        | RReaddir({ sid: StreamId })
        | RRewinddir({ sid: StreamId })
        | RTelldir({ sid: StreamId })
        | RSeekdir({ sid: StreamId, loc: int })
        | RClosedir({ sid: StreamId })
        | RFcntlDupfd({ fd: Fd, atLeast: Fd })
        | RFcntlGetfl({ fd: Fd })
        | RFcntlSetfl({ fd: Fd, appendF: bool })
        | RFcntlLock({ fd: Fd, cmd: LockCmd, lk: LockReq, lo: int, hi: int })
        | RLockf({ fd: Fd, cmd: LockfCmd, len: int })
        | RFsync({ fd: Fd, dataOnly: bool })
        | RCopyRange({ fdIn: Fd, offIn: int, fdOut: Fd, offOut: int, len: int })
        | RCloneRange({ fdDst: Fd, offDst: int, fdSrc: Fd, offSrc: int,
                        len: int })
        | RFallocate({ fd: Fd, mode: int, off: int, len: int })

    // Every result variant carries the errno `e` (0 = success); payloads
    // are only meaningful when e == 0.
    type OpRes =
        | SOk({ e: int })
        | SFd({ e: int, fd: Fd })
        | SOff({ e: int, off: int })
        // Read/write result: byte count `n`, plus the target inode and the
        // landing byte offset so the replay can verify/update its byte shadow.
        // Exact content bytes are NOT modeled (they live in the shadow).
        | SRw({ e: int, n: int, ino: Ino, off: int })
        | SStatR({ e: int, ftype: Ftype, mode: int, uid: int, gid: int,
                   nlink: int, sizeB: int, ino: Ino,
                   atime: int, mtime: int, ctime: int })
        | SReadlinkR({ e: int, tgt: SymTarget })
        | SUmaskR({ e: int, old: int })
        | SGetflR({ e: int, acc: AccMode, appendF: bool })
        | SGetlkR({ e: int, conflict: bool, cpid: Pid, cw: bool })
        | SDirR({ e: int, sid: StreamId })
        | SNamesR({ e: int, names: Set[Name] })

    pure def errOf(res: OpRes): int =
        match res {
            | SOk(x) => x.e
            | SFd(x) => x.e
            | SOff(x) => x.e
            | SRw(x) => x.e
            | SStatR(x) => x.e
            | SReadlinkR(x) => x.e
            | SUmaskR(x) => x.e
            | SGetflR(x) => x.e
            | SGetlkR(x) => x.e
            | SDirR(x) => x.e
            | SNamesR(x) => x.e
        }

    // Handler outcome.
    type Out = { fs2: Fs, ps2: Pstate, res: OpRes }

    pure def out(fs: Fs, ps: Pstate, r: OpRes): Out =
        { fs2: fs, ps2: ps, res: r }

    pure def outErr(fs: Fs, ps: Pstate, e: int): Out =
        out(fs, ps, SOk({ e: e }))

    // Read/write result constructors.  Errors carry no shadow target
    // (ino -1); successes carry the target inode + landing byte offset.
    pure def rwErr(e: int): OpRes = SRw({ e: e, n: 0, ino: -1, off: 0 })
    pure def rwOk(n: int, ino: Ino, off: int): OpRes =
        SRw({ e: OK, n: n, ino: ino, off: off })

    // ------------------------------------------------------------------
    // Common gates and helpers
    // ------------------------------------------------------------------

    pure def accReads(a: AccMode): bool = a == AccR or a == AccRW
    pure def accWrites(a: AccMode): bool = a == AccW or a == AccRW

    // Resolve the starting inode for a path-taking call: absolute paths
    // ignore dfd entirely (POSIX openat); relative paths need dfd to be a
    // directory descriptor opened for reading (Issue 8 wording; EBADF for
    // a non-reading mode, ENOTDIR for a non-directory).  A dirfd whose
    // directory has been removed (an orphan pinned only by descriptors)
    // yields ENOENT: nothing may be created in or resolved through a
    // removed directory, and its dot-dot is dead.
    pure def atStart(fs: Fs, ps: Pstate, p: Pid, dfd: Fd, pth: Path):
            { e: int, start: Ino } =
        if (pth.abs) { e: OK, start: ROOT }
        else if (dfd == -1 or not(fdLive(ps, p, dfd))) { e: EBADF, start: -1 }
        else {
            val o = fdOfd(ps, p, dfd)
            if (fs.inodes.get(o.ino).ftype != FDir) { e: ENOTDIR, start: -1 }
            else if (not(accReads(o.acc))) { e: EBADF, start: -1 }
            else if (fsOrphans(fs).contains(o.ino)) { e: ENOENT, start: -1 }
            else { e: OK, start: o.ino }
        }

    // Reap every orphan that lost its last pin (called after close,
    // closedir and dup2's implicit close).
    pure def reapFree(fs: Fs, ps: Pstate): Fs = {
        val pinned = psPinned(ps)
        fsOrphans(fs).filter(i => not(pinned.contains(i)))
            .fold(fs, (acc, i) => fsReap(acc, i))
    }

    // Scan for the first data (wantData) or hole block at/after off; the
    // region at/after the size is a virtual hole (Issue 8 lseek).  A block is
    // data iff it is materialized (present in the map): a written pattern or a
    // fallocate-reserved zero block both count, a never-touched gap is a hole.
    pure def seekScan(nd: Inode, off: int, wantData: bool):
            { found: bool, off2: int } = {
        // memfs scans block-by-block from blkOf(off) but returns a byte
        // offset: max(block_start, off), so SEEK_DATA inside a data block
        // returns the queried offset itself.  A hole past all data is at the
        // size; data past all data is ENXIO.
        val last = ceilDiv(nd.size, BS) - 1
        val hitB = range(blkOf(off), last + 1).foldl(-1, (acc, b) =>
            if (acc >= 0) acc
            else if (nd.dataBlocks.contains(b) == wantData) b
            else acc)
        if (hitB >= 0) { found: true, off2: imax(hitB * BS, off) }
        else if (wantData) { found: false, off2: nd.size }
        else { found: true, off2: nd.size }
    }

    // ------------------------------------------------------------------
    // Descriptor-layer handlers
    // ------------------------------------------------------------------

    pure def opOpen(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                    pth: Path, fl: OpenFlags): Out = {
        val cr = psCred(ps, p)
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) out(fs, ps, SFd({ e: at.e, fd: -1 }))
        else {
            // O_CREAT|O_EXCL judges the final component without following:
            // an existing symlink there is EEXIST even when dangling.
            val r = if (fl.creat and fl.excl)
                        resolve(fs, caps, cr, at.start, pth, false)
                    else
                        resolve(fs, caps, cr, at.start, pth, not(fl.nofollow))
            if (r.err != OK) out(fs, ps, SFd({ e: r.err, fd: -1 }))
            else if (r.ino == -1)
                // Missing final component.
                if (not(fl.creat))
                    out(fs, ps, SFd({ e: ENOENT, fd: -1 }))
                else if (pth.slash)
                    out(fs, ps, SFd({ e: ENOENT, fd: -1 }))
                else if (not(mayWrite(cr, fs.inodes.get(r.par))))
                    out(fs, ps, SFd({ e: EACCES, fd: -1 }))
                else {
                    // Create: umask-filtered mode, owner euid, group per
                    // policy.  The new descriptor's rights come from the
                    // oflag alone -- no file-permission check on a file
                    // that did not exist (mode-000 creation works).
                    val m = applyUmask(fl.mode, ps.umask.get(p))
                    val node = mkFile(m, cr.euid,
                                      newGid(caps, cr, fs.inodes.get(r.par)))
                    val mk = fsMknode(fs, r.par, r.leaf, node)
                    val fd = fdLowest(ps, p, 0, MAX_FDS)
                    if (fd == -1) out(fs, ps, SFd({ e: EMFILE, fd: -1 }))
                    else out(mk.fs2,
                             psOpen(ps, p, fd, mk.ino, fl.acc, fl.appendF),
                             SFd({ e: OK, fd: fd }))
                }
            else {
                // Existing object.
                val nd = fs.inodes.get(r.ino)
                if (fl.creat and fl.excl)
                    out(fs, ps, SFd({ e: EEXIST, fd: -1 }))
                else if (nd.ftype == FLnk)
                    // Reached only under O_NOFOLLOW.
                    out(fs, ps, SFd({ e: ELOOP, fd: -1 }))
                else if (fl.directory and nd.ftype != FDir)
                    out(fs, ps, SFd({ e: ENOTDIR, fd: -1 }))
                else if (nd.ftype == FDir and accWrites(fl.acc))
                    out(fs, ps, SFd({ e: EISDIR, fd: -1 }))
                else if (nd.ftype != FReg and nd.ftype != FDir)
                    // FIFOs/specials: opening them blocks or needs device
                    // support; generators never do it (defensive).
                    out(fs, ps, SFd({ e: ENXIO, fd: -1 }))
                else if (accReads(fl.acc) and not(mayRead(cr, nd)))
                    out(fs, ps, SFd({ e: EACCES, fd: -1 }))
                else if (accWrites(fl.acc) and not(mayWrite(cr, nd)))
                    out(fs, ps, SFd({ e: EACCES, fd: -1 }))
                else if (fl.trunc and not(accWrites(fl.acc)))
                    // O_TRUNC|O_RDONLY is undefined; generators avoid it.
                    out(fs, ps, SFd({ e: EINVAL, fd: -1 }))
                else {
                    val fd = fdLowest(ps, p, 0, MAX_FDS)
                    if (fd == -1) out(fs, ps, SFd({ e: EMFILE, fd: -1 }))
                    else {
                        val fsT = if (fl.trunc and nd.ftype == FReg)
                                      fsResize(fs, r.ino, 0,
                                          caps.writeClearsSets and
                                          not(isPriv(cr)))
                                  else fs
                        out(fsT, psOpen(ps, p, fd, r.ino, fl.acc, fl.appendF),
                            SFd({ e: OK, fd: fd }))
                    }
                }
            }
        }
    }

    pure def opClose(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else {
            val ps2 = psClose(ps, p, fd)
            out(reapFree(fs, ps2), ps2, SOk({ e: OK }))
        }

    pure def opDup(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        if (not(fdLive(ps, p, fd))) out(fs, ps, SFd({ e: EBADF, fd: -1 }))
        else {
            val nfd = fdLowest(ps, p, 0, MAX_FDS)
            if (nfd == -1) out(fs, ps, SFd({ e: EMFILE, fd: -1 }))
            else out(fs, ps, SFd({ e: OK, fd: nfd }))
                     .with("ps2", psDup(ps, p, nfd, ps.fds.get((p, fd))))
        }

    pure def opDup2(fs: Fs, ps: Pstate, p: Pid, fd: Fd, nfd: Fd): Out =
        if (not(fdLive(ps, p, fd)) or nfd < 0 or nfd >= MAX_FDS)
            out(fs, ps, SFd({ e: EBADF, fd: -1 }))
        else if (nfd == fd)
            // dup2(fd, fd) returns fd without closing anything.
            out(fs, ps, SFd({ e: OK, fd: nfd }))
        else {
            // Implicit close of nfd has full close() semantics, including
            // the record-lock drop and orphan reaping.
            val ps1 = if (fdLive(ps, p, nfd)) psClose(ps, p, nfd) else ps
            val fs1 = reapFree(fs, ps1)
            out(fs1, psDup(ps1, p, nfd, ps1.fds.get((p, fd))),
                SFd({ e: OK, fd: nfd }))
        }

    pure def opLseek(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                     off: int, wh: Whence): Out =
        if (not(fdLive(ps, p, fd))) out(fs, ps, SOff({ e: EBADF, off: -1 }))
        else {
            val o = fdOfd(ps, p, fd)
            val nd = fs.inodes.get(o.ino)
            match wh {
                | WSet =>
                    if (off < 0) out(fs, ps, SOff({ e: EINVAL, off: -1 }))
                    else out(fs, psSetOffset(ps, p, fd, off),
                             SOff({ e: OK, off: off }))
                | WCur =>
                    if (o.offset + off < 0)
                        out(fs, ps, SOff({ e: EINVAL, off: -1 }))
                    else out(fs, psSetOffset(ps, p, fd, o.offset + off),
                             SOff({ e: OK, off: o.offset + off }))
                | WEnd =>
                    if (nd.size + off < 0)
                        out(fs, ps, SOff({ e: EINVAL, off: -1 }))
                    else out(fs, psSetOffset(ps, p, fd, nd.size + off),
                             SOff({ e: OK, off: nd.size + off }))
                | WData =>
                    if (not(caps.seekHole))
                        out(fs, ps, SOff({ e: EINVAL, off: -1 }))
                    else if (off < 0 or off >= nd.size)
                        out(fs, ps, SOff({ e: ENXIO, off: -1 }))
                    else {
                        val s = seekScan(nd, off, true)
                        if (not(s.found))
                            out(fs, ps, SOff({ e: ENXIO, off: -1 }))
                        else out(fs, psSetOffset(ps, p, fd, s.off2),
                                 SOff({ e: OK, off: s.off2 }))
                    }
                | WHole =>
                    if (not(caps.seekHole))
                        out(fs, ps, SOff({ e: EINVAL, off: -1 }))
                    else if (off < 0 or off >= nd.size)
                        out(fs, ps, SOff({ e: ENXIO, off: -1 }))
                    else {
                        val s = seekScan(nd, off, false)
                        out(fs, psSetOffset(ps, p, fd, s.off2),
                            SOff({ e: OK, off: s.off2 }))
                    }
            }
        }

    pure def opRead(fs: Fs, ps: Pstate, p: Pid, fd: Fd, len: int): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, rwErr(EBADF))
        else {
            val o = fdOfd(ps, p, fd)
            val nd = fs.inodes.get(o.ino)
            if (not(accReads(o.acc)))
                out(fs, ps, rwErr(EBADF))
            else if (nd.ftype == FDir)
                // Reading a directory fd is an acceptance set {EISDIR,
                // success}; generators avoid it (defensive canonical).
                out(fs, ps, rwErr(EISDIR))
            else {
                val rd = fsReadData(nd, o.offset, len)
                // Successful reads mark atime, including a 0-byte read at
                // EOF (read page RATIONALE).
                out(fsMark(fs, o.ino, true, false, false),
                    psSetOffset(ps, p, fd, o.offset + rd.count),
                    rwOk(rd.count, o.ino, o.offset))
            }
        }

    pure def opWrite(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                     len: int, pat: int): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, rwErr(EBADF))
        else {
            val cr = psCred(ps, p)
            val o = fdOfd(ps, p, fd)
            if (not(accWrites(o.acc)))
                out(fs, ps, rwErr(EBADF))
            else {
                // O_APPEND: the offset is set to the size immediately
                // before each write, atomically with it.
                val at = if (o.appendF) fs.inodes.get(o.ino).size
                         else o.offset
                val wr = fsWrite(fs, o.ino, at, len, pat,
                    caps.writeClearsSets and not(isPriv(cr)))
                out(wr.fs2, psSetOffset(ps, p, fd, at + len),
                    rwOk(len, o.ino, at))
            }
        }

    pure def opPread(fs: Fs, ps: Pstate, p: Pid, fd: Fd, off: int,
                     len: int): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, rwErr(EBADF))
        else {
            val o = fdOfd(ps, p, fd)
            val nd = fs.inodes.get(o.ino)
            if (not(accReads(o.acc)))
                out(fs, ps, rwErr(EBADF))
            else if (nd.ftype == FDir)
                // Same canonicalization as opRead: reading a directory
                // descriptor is EISDIR.
                out(fs, ps, rwErr(EISDIR))
            else if (off < 0)
                out(fs, ps, rwErr(EINVAL))
            else {
                // The file offset is neither used nor changed.
                val rd = fsReadData(nd, off, len)
                out(fsMark(fs, o.ino, true, false, false), ps,
                    rwOk(rd.count, o.ino, off))
            }
        }

    pure def opPwrite(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                      off: int, len: int, pat: int): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, rwErr(EBADF))
        else {
            val cr = psCred(ps, p)
            val o = fdOfd(ps, p, fd)
            if (not(accWrites(o.acc)))
                out(fs, ps, rwErr(EBADF))
            else if (off < 0)
                out(fs, ps, rwErr(EINVAL))
            else {
                // POSIX: pwrite writes at the given offset regardless of
                // O_APPEND; Linux appends anyway (caps.pwriteAppends).
                val at = if (o.appendF and caps.pwriteAppends)
                             fs.inodes.get(o.ino).size
                         else off
                val wr = fsWrite(fs, o.ino, at, len, pat,
                    caps.writeClearsSets and not(isPriv(cr)))
                out(wr.fs2, ps, rwOk(len, o.ino, at))
            }
        }

    pure def opTruncate(fs: Fs, ps: Pstate, caps: Caps, p: Pid, pth: Path,
                        len: int): Out = {
        val cr = psCred(ps, p)
        val r = resolveEx(fs, caps, cr, ROOT, pth, true)
        if (r.err != OK) outErr(fs, ps, r.err)
        else {
            val nd = fs.inodes.get(r.ino)
            if (nd.ftype == FDir) outErr(fs, ps, EISDIR)
            else if (nd.ftype != FReg) outErr(fs, ps, EINVAL)
            else if (len < 0) outErr(fs, ps, EINVAL)
            else if (not(mayWrite(cr, nd))) outErr(fs, ps, EACCES)
            else out(fsResize(fs, r.ino, len,
                         caps.writeClearsSets and not(isPriv(cr))),
                     ps, SOk({ e: OK }))
        }
    }

    pure def opFtruncate(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                         len: int): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else {
            val cr = psCred(ps, p)
            val o = fdOfd(ps, p, fd)
            if (not(accWrites(o.acc))) outErr(fs, ps, EINVAL)
            else if (len < 0) outErr(fs, ps, EINVAL)
            else if (fs.inodes.get(o.ino).ftype != FReg)
                outErr(fs, ps, EINVAL)
            else out(fsResize(fs, o.ino, len,
                         caps.writeClearsSets and not(isPriv(cr))),
                     ps, SOk({ e: OK }))
        }

    pure def opFsync(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        // Integrity completion is invisible to a no-crash model; only the
        // descriptor gate is observable.  No timestamps mark.
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else outErr(fs, ps, OK)

    // ------------------------------------------------------------------
    // Metadata handlers
    // ------------------------------------------------------------------

    pure def statRes(nd: Inode, i: Ino): OpRes =
        SStatR({ e: OK, ftype: nd.ftype, mode: nd.mode, uid: nd.uid,
                 gid: nd.gid, nlink: nd.nlink, sizeB: nd.size,
                 ino: i, atime: nd.atime, mtime: nd.mtime, ctime: nd.ctime })

    pure def statFail(e: int): OpRes =
        SStatR({ e: e, ftype: FReg, mode: 0, uid: -1, gid: -1, nlink: 0,
                 sizeB: 0, ino: -1, atime: 0, mtime: 0, ctime: 0 })

    // stat/lstat/fstatat: no permission is needed on the object itself,
    // only search along the path.  Marks are already realized (the model
    // assigns instants eagerly), matching XBD's rule that pending marks
    // must be flushed before a successful stat.
    pure def opStat(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                    pth: Path, follow: bool): Out = {
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) out(fs, ps, statFail(at.e))
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, follow)
            if (r.err != OK) out(fs, ps, statFail(r.err))
            else out(fs, ps, statRes(fs.inodes.get(r.ino), r.ino))
        }
    }

    pure def opFstat(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        if (not(fdLive(ps, p, fd))) out(fs, ps, statFail(EBADF))
        else {
            val i = fdOfd(ps, p, fd).ino
            out(fs, ps, statRes(fs.inodes.get(i), i))
        }

    // statfs/statvfs: like stat, resolve the path (search permission along
    // the prefix, following the final symlink) but need no permission on the
    // object.  The capacity/inode counts are backend-specific and untracked,
    // so success asserts only errno == OK.
    pure def opStatfs(fs: Fs, ps: Pstate, caps: Caps, p: Pid, pth: Path): Out = {
        val at = atStart(fs, ps, p, -1, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, true)
            if (r.err != OK) outErr(fs, ps, r.err)
            else outErr(fs, ps, OK)
        }
    }

    // fstatfs/fstatvfs: only a live descriptor is required.
    pure def opFstatfs(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else outErr(fs, ps, OK)

    // chmod: owner or privilege (EPERM otherwise).  A requested S_ISGID is
    // silently stripped from regular files when the unprivileged caller is
    // not a member of the file's group (success, not an error).
    pure def chmodCommon(fs: Fs, ps: Pstate, p: Pid, i: Ino, m: int): Out = {
        val cr = psCred(ps, p)
        val nd = fs.inodes.get(i)
        if (not(isPriv(cr)) and cr.euid != nd.uid) outErr(fs, ps, EPERM)
        else {
            val m2 = if (not(isPriv(cr)) and not(inGroup(cr, nd.gid)) and
                             nd.ftype == FReg and bitOn(m, MB_SGID))
                         m - MB_SGID
                     else m
            out(fsChmod(fs, i, m2), ps, SOk({ e: OK }))
        }
    }

    pure def opChmod(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                     pth: Path, m: int): Out = {
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, true)
            if (r.err != OK) outErr(fs, ps, r.err)
            else chmodCommon(fs, ps, p, r.ino, m)
        }
    }

    pure def opFchmod(fs: Fs, ps: Pstate, p: Pid, fd: Fd, m: int): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else chmodCommon(fs, ps, p, fdOfd(ps, p, fd).ino, m)

    pure def anyExec(m: int): bool =
        bitOn(m, MB_UX) or bitOn(m, MB_GX) or bitOn(m, MB_OX)

    // chown: privilege changes anything; the owner may keep the uid
    // (-1 or unchanged) and switch the gid within their own groups
    // (_POSIX_CHOWN_RESTRICTED); everyone else gets EPERM.  Setid bits
    // clear on regular files carrying an execute bit (Issue 8 makes the
    // no-execute case and the privileged case implementation-defined; the
    // model clears exactly when an execute bit is present).
    pure def chownCommon(fs: Fs, ps: Pstate, p: Pid, i: Ino, u: int,
                         g: int): Out = {
        val cr = psCred(ps, p)
        val nd = fs.inodes.get(i)
        // chown(-1, -1) names neither id: it changes nothing and so needs no
        // privilege -- a non-owner may issue it (Linux passes only ATTR_CTIME,
        // with no ownership check; pjd chown/00 "chown(-1,-1) succeeds even for
        // a non-owner").  No uid/gid and no set*id bit change.
        if (u == -1 and g == -1)
            // Bumps ctime (Linux sets ATTR_CTIME) but leaves uid/gid and the
            // set*id bits untouched.
            out(fsChown(fs, i, -1, -1, false), ps, SOk({ e: OK }))
        else {
            val allowed =
                isPriv(cr) or
                (cr.euid == nd.uid and
                 (u == -1 or u == nd.uid) and
                 (g == -1 or inGroup(cr, g)))
            if (not(allowed)) outErr(fs, ps, EPERM)
            else out(fsChown(fs, i, u, g, nd.ftype == FReg and anyExec(nd.mode)),
                     ps, SOk({ e: OK }))
        }
    }

    pure def opChown(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                     pth: Path, u: int, g: int, follow: bool): Out = {
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, follow)
            if (r.err != OK) outErr(fs, ps, r.err)
            else chownCommon(fs, ps, p, r.ino, u, g)
        }
    }

    pure def opFchown(fs: Fs, ps: Pstate, p: Pid, fd: Fd, u: int,
                      g: int): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else chownCommon(fs, ps, p, fdOfd(ps, p, fd).ino, u, g)

    pure def tsVal(t: TimeSpec): int =
        match t {
            | TsNow => T_NOW
            | TsOmit => 0
            | TsVal(v) => v
        }

    pure def tsExplicit(t: TimeSpec): bool =
        match t {
            | TsVal(v) => true
            | TsNow => false
            | TsOmit => false
        }

    // utimensat/futimens permission tiers (Issue 8):
    //  - both UTIME_OMIT: no checks at all, no state change (ctime need
    //    not be marked; the model does not mark it)
    //  - to-now (NULL or both UTIME_NOW): owner, write access, or
    //    privilege, else EACCES
    //  - any explicit value: owner or privilege only, else EPERM (write
    //    access never helps)
    pure def utimensCommon(fs: Fs, ps: Pstate, p: Pid, i: Ino,
                           ta: TimeSpec, tm: TimeSpec): Out = {
        val cr = psCred(ps, p)
        val nd = fs.inodes.get(i)
        if (ta == TsOmit and tm == TsOmit) outErr(fs, ps, OK)
        else {
            val explicit = tsExplicit(ta) or tsExplicit(tm)
            val owner = isPriv(cr) or cr.euid == nd.uid
            if (explicit and not(owner)) outErr(fs, ps, EPERM)
            else if (not(explicit) and not(owner) and not(mayWrite(cr, nd)))
                outErr(fs, ps, EACCES)
            else out(fsSetTimes(fs, i, ta != TsOmit, tsVal(ta),
                                tm != TsOmit, tsVal(tm)),
                     ps, SOk({ e: OK }))
        }
    }

    pure def opUtimens(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                       pth: Path, ta: TimeSpec, tm: TimeSpec): Out = {
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, true)
            if (r.err != OK) outErr(fs, ps, r.err)
            else utimensCommon(fs, ps, p, r.ino, ta, tm)
        }
    }

    pure def opFutimens(fs: Fs, ps: Pstate, p: Pid, fd: Fd, ta: TimeSpec,
                        tm: TimeSpec): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else utimensCommon(fs, ps, p, fdOfd(ps, p, fd).ino, ta, tm)

    // access/faccessat: the whole check -- path search included -- runs
    // with the real ids unless AT_EACCESS.
    pure def opAccess(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                      pth: Path, wr: bool, ww: bool, wx: bool,
                      eff: bool): Out = {
        val cr0 = psCred(ps, p)
        val cr = if (eff) cr0
                 else { euid: cr0.ruid, egid: cr0.rgid, ruid: cr0.ruid,
                        rgid: cr0.rgid, groups: cr0.groups }
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolveEx(fs, caps, cr, at.start, pth, true)
            if (r.err != OK) outErr(fs, ps, r.err)
            else {
                val nd = fs.inodes.get(r.ino)
                if ((wr and not(mayRead(cr, nd))) or
                        (ww and not(mayWrite(cr, nd))) or
                        (wx and not(mayExec(cr, nd))))
                    outErr(fs, ps, EACCES)
                else outErr(fs, ps, OK)
            }
        }
    }

    pure def opUmask(fs: Fs, ps: Pstate, p: Pid, mask: int): Out =
        out(fs, psSetUmask(ps, p, permBits(mask)),
            SUmaskR({ e: OK, old: ps.umask.get(p) }))

    // ------------------------------------------------------------------
    // Namespace handlers
    // ------------------------------------------------------------------

    // Shared create-path logic: resolve without following the final
    // component (an existing entry there -- symlink included, dangling
    // included -- is EEXIST), then require write on the parent.
    pure def createGate(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                        pth: Path, slashOk: bool):
            { e: int, par: Ino, leaf: Name } = {
        val cr = psCred(ps, p)
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) { e: at.e, par: -1, leaf: "" }
        else {
            val r = resolve(fs, caps, cr, at.start, pth, false)
            if (r.err != OK) { e: r.err, par: -1, leaf: "" }
            else if (r.ino != -1) { e: EEXIST, par: -1, leaf: "" }
            else if (pth.slash and not(slashOk))
                { e: ENOENT, par: -1, leaf: "" }
            else if (not(mayWrite(cr, fs.inodes.get(r.par))))
                { e: EACCES, par: -1, leaf: "" }
            else { e: OK, par: r.par, leaf: r.leaf }
        }
    }

    pure def opMkdir(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                     pth: Path, m: int): Out = {
        val cr = psCred(ps, p)
        val g = createGate(fs, ps, caps, p, dfd, pth, true)
        if (g.e != OK) outErr(fs, ps, g.e)
        else {
            val dnode = fs.inodes.get(g.par)
            val node = mkDir(newDirMode(caps, m, ps.umask.get(p), dnode),
                             cr.euid, newGid(caps, cr, dnode))
            val mk = fsMknode(fs, g.par, g.leaf, node)
            outErr(mk.fs2, ps, OK)
        }
    }

    pure def opMknod(fs: Fs, ps: Pstate, caps: Caps, p: Pid, pth: Path,
                     ft: Ftype, m: int): Out = {
        val cr = psCred(ps, p)
        val g = createGate(fs, ps, caps, p, -1, pth, false)
        if (g.e != OK) outErr(fs, ps, g.e)
        else if (ft == FDir or ft == FLnk) outErr(fs, ps, EINVAL)
        else if ((ft == FBlk or ft == FChr) and not(isPriv(cr)))
            outErr(fs, ps, EPERM)
        else {
            val dnode = fs.inodes.get(g.par)
            val mode = applyUmask(m, ps.umask.get(p))
            val node = if (ft == FReg) mkFile(mode, cr.euid,
                                              newGid(caps, cr, dnode))
                       else mkSpecial(ft, mode, cr.euid,
                                      newGid(caps, cr, dnode))
            outErr(fsMknode(fs, g.par, g.leaf, node).fs2, ps, OK)
        }
    }

    // symlink: the target is not validated (dangling is fine).  The link's
    // own permission bits are created as 0777 and never consulted by
    // resolution (Issue 8; umask application to symlinks is a documented
    // Linux no-op and the model follows it).
    pure def opSymlink(fs: Fs, ps: Pstate, caps: Caps, p: Pid,
                       tgt: SymTarget, dfd: Fd, pth: Path): Out = {
        val cr = psCred(ps, p)
        val g = createGate(fs, ps, caps, p, dfd, pth, false)
        if (g.e != OK) outErr(fs, ps, g.e)
        else {
            val dnode = fs.inodes.get(g.par)
            val node = mkSymlink(tgt, 511, cr.euid, newGid(caps, cr, dnode))
            outErr(fsMknode(fs, g.par, g.leaf, node).fs2, ps, OK)
        }
    }

    pure def opLink(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfdOld: Fd,
                    pthOld: Path, dfdNew: Fd, pthNew: Path,
                    followOld: bool): Out = {
        val cr = psCred(ps, p)
        val atO = atStart(fs, ps, p, dfdOld, pthOld)
        if (atO.e != OK) outErr(fs, ps, atO.e)
        else {
            val ro = resolveEx(fs, caps, cr, atO.start, pthOld, followOld)
            if (ro.err != OK) outErr(fs, ps, ro.err)
            else if (fs.inodes.get(ro.ino).ftype == FDir)
                outErr(fs, ps, EPERM)
            else {
                val g = createGate(fs, ps, caps, p, dfdNew, pthNew, false)
                if (g.e != OK) outErr(fs, ps, g.e)
                else {
                    val lk = fsLink(fs, ro.ino, g.par, g.leaf)
                    outErr(lk.fs2, ps,
                        if (lk.st == FsOk) OK
                        else if (lk.st == FsExist) EEXIST
                        else if (lk.st == FsIsdir) EPERM
                        else ENOTDIR)
                }
            }
        }
    }

    pure def opUnlink(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                      pth: Path): Out = {
        val cr = psCred(ps, p)
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolve(fs, caps, cr, at.start, pth, false)
            if (r.err != OK) outErr(fs, ps, r.err)
            else if (r.ino == -1) outErr(fs, ps, ENOENT)
            else if (r.par == -1) outErr(fs, ps, EBUSY)   // "/"
            else {
                val dnode = fs.inodes.get(r.par)
                val vnode = fs.inodes.get(r.ino)
                if (vnode.ftype == FDir) outErr(fs, ps, errUnlinkDir(caps))
                else if (not(mayWrite(cr, dnode))) outErr(fs, ps, EACCES)
                else if (not(stickyOk(caps, cr, dnode, vnode)))
                    outErr(fs, ps, errSticky(caps))
                else {
                    val rm = fsRemove(fs, r.par, r.leaf, psPinned(ps))
                    outErr(rm.fs2, ps, if (rm.st == FsOk) OK else ENOENT)
                }
            }
        }
    }

    pure def opRmdir(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                     pth: Path): Out = {
        val cr = psCred(ps, p)
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) outErr(fs, ps, at.e)
        else {
            val r = resolve(fs, caps, cr, at.start, pth, false)
            if (r.err != OK) outErr(fs, ps, r.err)
            else if (r.ino == -1) outErr(fs, ps, ENOENT)
            else if (r.leaf == ".") outErr(fs, ps, EINVAL)
            else if (r.leaf == "..") outErr(fs, ps, EINVAL)
            else if (r.par == -1 or r.ino == ROOT) outErr(fs, ps, EBUSY)
            else {
                val dnode = fs.inodes.get(r.par)
                val vnode = fs.inodes.get(r.ino)
                if (vnode.ftype != FDir) outErr(fs, ps, ENOTDIR)
                else if (vnode.ents.keys().size() > 0)
                    outErr(fs, ps, errNonempty(caps))
                else if (not(mayWrite(cr, dnode))) outErr(fs, ps, EACCES)
                else if (not(stickyOk(caps, cr, dnode, vnode)))
                    outErr(fs, ps, errSticky(caps))
                else {
                    val rm = fsRemove(fs, r.par, r.leaf, psPinned(ps))
                    outErr(rm.fs2, ps, if (rm.st == FsOk) OK else ENOENT)
                }
            }
        }
    }

    pure def opRename(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfdOld: Fd,
                      pthOld: Path, dfdNew: Fd, pthNew: Path): Out = {
        val cr = psCred(ps, p)
        val atO = atStart(fs, ps, p, dfdOld, pthOld)
        val atN = atStart(fs, ps, p, dfdNew, pthNew)
        if (atO.e != OK) outErr(fs, ps, atO.e)
        else if (atN.e != OK) outErr(fs, ps, atN.e)
        else {
            val rs = resolve(fs, caps, cr, atO.start, pthOld, false)
            if (rs.err != OK) outErr(fs, ps, rs.err)
            else if (rs.ino == -1) outErr(fs, ps, ENOENT)
            else if (rs.leaf == "." or rs.leaf == ".." or rs.par == -1)
                outErr(fs, ps, EINVAL)
            else {
                val rt = resolve(fs, caps, cr, atN.start, pthNew, false)
                if (rt.err != OK) outErr(fs, ps, rt.err)
                else if (rt.leaf == "." or rt.leaf == ".." or rt.par == -1)
                    outErr(fs, ps, EINVAL)
                else {
                    val sdnode = fs.inodes.get(rs.par)
                    val tdnode = fs.inodes.get(rt.par)
                    val srcNode = fs.inodes.get(rs.ino)
                    val noop = (rs.par == rt.par and rs.leaf == rt.leaf) or
                               (rt.ino != -1 and rt.ino == rs.ino)
                    if (not(mayWrite(cr, sdnode)) or
                            not(mayWrite(cr, tdnode)))
                        outErr(fs, ps, EACCES)
                    else if (not(stickyOk(caps, cr, sdnode, srcNode)))
                        outErr(fs, ps, errSticky(caps))
                    else if (rt.ino != -1 and not(noop) and
                             not(stickyOk(caps, cr, tdnode,
                                          fs.inodes.get(rt.ino))))
                        outErr(fs, ps, errSticky(caps))
                    else {
                        val rn = fsRename(fs, rs.par, rs.leaf, rt.par,
                                          rt.leaf, psPinned(ps))
                        val e = if (rn.st == FsOk) OK
                                else if (rn.st == FsNoent) ENOENT
                                else if (rn.st == FsInval) EINVAL
                                else if (rn.st == FsNotdir) ENOTDIR
                                else if (rn.st == FsIsdir) EISDIR
                                else errNonempty(caps)
                        val fsC = if (e == OK and not(noop) and
                                          caps.renameCtime)
                                      fsMark(rn.fs2, rs.ino, false, false,
                                             true)
                                  else rn.fs2
                        outErr(fsC, ps, e)
                    }
                }
            }
        }
    }

    pure def opReadlink(fs: Fs, ps: Pstate, caps: Caps, p: Pid, dfd: Fd,
                        pth: Path): Out = {
        val at = atStart(fs, ps, p, dfd, pth)
        if (at.e != OK) out(fs, ps, SReadlinkR({ e: at.e, tgt: NO_TARGET }))
        else {
            val r = resolveEx(fs, caps, psCred(ps, p), at.start, pth, false)
            if (r.err != OK)
                out(fs, ps, SReadlinkR({ e: r.err, tgt: NO_TARGET }))
            else {
                val nd = fs.inodes.get(r.ino)
                if (nd.ftype != FLnk)
                    out(fs, ps, SReadlinkR({ e: EINVAL, tgt: NO_TARGET }))
                else
                    // No read permission is required on the link itself;
                    // reading it marks its atime.
                    out(fsMark(fs, r.ino, true, false, false), ps,
                        SReadlinkR({ e: OK, tgt: nd.target }))
            }
        }
    }

    // ------------------------------------------------------------------
    // Directory streams
    // ------------------------------------------------------------------

    // opendir needs read permission on the directory (search along the
    // path is checked by resolution).
    pure def opOpendir(fs: Fs, ps: Pstate, caps: Caps, p: Pid,
                       pth: Path): Out = {
        val cr = psCred(ps, p)
        val r = resolveEx(fs, caps, cr, ROOT, pth, true)
        if (r.err != OK) out(fs, ps, SDirR({ e: r.err, sid: -1 }))
        else {
            val nd = fs.inodes.get(r.ino)
            if (nd.ftype != FDir) out(fs, ps, SDirR({ e: ENOTDIR, sid: -1 }))
            else if (not(mayRead(cr, nd)))
                out(fs, ps, SDirR({ e: EACCES, sid: -1 }))
            else {
                val od = psOpendir(ps, p, r.ino)
                out(fs, od.ps2, SDirR({ e: OK, sid: od.sid }))
            }
        }
    }

    // readdir modeled as one atomic full sweep (see DESIGN-POSIX.md): the
    // result is exactly the current entry set (dot entries stripped by the
    // harness), which is sound because no mutation interleaves within a
    // model step.  A sweep of a stream whose directory was removed returns
    // the empty set (POSIX leaves it unspecified; acceptance noted).
    // Reading the directory marks its atime.
    pure def opReaddir(fs: Fs, ps: Pstate, sid: StreamId): Out =
        if (not(ps.streams.keys().contains(sid)))
            out(fs, ps, SNamesR({ e: EBADF, names: Set() }))
        else {
            val i = ps.streams.get(sid).ino
            val nd = fs.inodes.get(i)
            if (fsOrphans(fs).contains(i))
                out(fs, ps, SNamesR({ e: OK, names: Set() }))
            else
                out(fsMark(fs, i, true, false, false), ps,
                    SNamesR({ e: OK, names: nd.ents.keys() }))
        }

    // rewinddir resets the harness's cursor and re-primes the snapshot;
    // with atomic sweeps there is no model state to change.
    pure def opRewinddir(fs: Fs, ps: Pstate, sid: StreamId): Out =
        if (not(ps.streams.keys().contains(sid))) outErr(fs, ps, EBADF)
        else outErr(fs, ps, OK)

    pure def opClosedir(fs: Fs, ps: Pstate, sid: StreamId): Out =
        if (not(ps.streams.keys().contains(sid))) outErr(fs, ps, EBADF)
        else {
            val ps2 = psClosedir(ps, sid)
            out(reapFree(fs, ps2), ps2, SOk({ e: OK }))
        }

    // telldir/seekdir: the stream cookie is opaque and backend-specific, and
    // the harness rewinds before each readdir, so neither has an observable
    // effect the model tracks — only stream validity is asserted (like
    // rewinddir).  telldir on a live stream returns its cookie; seekdir sets
    // it.  (XSI/Linux directory-stream extensions.)
    pure def opTelldir(fs: Fs, ps: Pstate, sid: StreamId): Out =
        if (not(ps.streams.keys().contains(sid))) outErr(fs, ps, EBADF)
        else outErr(fs, ps, OK)

    pure def opSeekdir(fs: Fs, ps: Pstate, sid: StreamId): Out =
        if (not(ps.streams.keys().contains(sid))) outErr(fs, ps, EBADF)
        else outErr(fs, ps, OK)

    // ------------------------------------------------------------------
    // fcntl / lockf
    // ------------------------------------------------------------------

    pure def opFcntlDupfd(fs: Fs, ps: Pstate, p: Pid, fd: Fd,
                          atLeast: Fd): Out =
        if (not(fdLive(ps, p, fd))) out(fs, ps, SFd({ e: EBADF, fd: -1 }))
        else if (atLeast < 0 or atLeast >= MAX_FDS)
            out(fs, ps, SFd({ e: EINVAL, fd: -1 }))
        else {
            val nfd = fdLowest(ps, p, atLeast, MAX_FDS)
            if (nfd == -1) out(fs, ps, SFd({ e: EMFILE, fd: -1 }))
            else out(fs, psDup(ps, p, nfd, ps.fds.get((p, fd))),
                     SFd({ e: OK, fd: nfd }))
        }

    pure def opFcntlGetfl(fs: Fs, ps: Pstate, p: Pid, fd: Fd): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, SGetflR({ e: EBADF, acc: AccR, appendF: false }))
        else {
            val o = fdOfd(ps, p, fd)
            out(fs, ps, SGetflR({ e: OK, acc: o.acc, appendF: o.appendF }))
        }

    // F_SETFL: only O_APPEND is modeled (the access mode is read-only and
    // O_NONBLOCK has no observable effect here).  The flag lives in the
    // open file description, so dup'd descriptors see the change.
    pure def opFcntlSetfl(fs: Fs, ps: Pstate, p: Pid, fd: Fd,
                          a: bool): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else out(fs, psSetAppend(ps, p, fd, a), SOk({ e: OK }))

    pure def lkType(lk: LockReq): LockT =
        if (lk == LkRd) LRead else LWrite

    pure def opFcntlLock(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                         cmd: LockCmd, lk: LockReq, lo: int,
                         hi: int): Out =
        if (not(fdLive(ps, p, fd)))
            out(fs, ps, SGetlkR({ e: EBADF, conflict: false, cpid: -1,
                                  cw: false }))
        else {
            val o = fdOfd(ps, p, fd)
            val i = o.ino
            if (fs.inodes.get(i).ftype != FReg)
                outErr(fs, ps, EINVAL)
            else if (lo < 0 or hi <= lo) outErr(fs, ps, EINVAL)
            else if (lk == LkRd and not(accReads(o.acc)) and cmd != CGetlk)
                outErr(fs, ps, EBADF)
            else if (lk == LkWr and not(accWrites(o.acc)) and cmd != CGetlk)
                outErr(fs, ps, EBADF)
            else match cmd {
                | CGetlk =>
                    if (lk == LkUn)
                        out(fs, ps, SGetlkR({ e: EINVAL, conflict: false,
                                              cpid: -1, cw: false }))
                    else {
                        val owners = lockConflictOwners(ps, p, i, lo, hi,
                                                        lkType(lk))
                        if (owners.size() == 0)
                            out(fs, ps, SGetlkR({ e: OK, conflict: false,
                                                  cpid: -1, cw: false }))
                        else {
                            // POSIX lets the server report ANY blocking
                            // lock; the model reports the lowest-pid owner
                            // and whether that owner holds a write lock in
                            // the range.  The harness checks the reported
                            // owner against the model's conflict set
                            // rather than this exact pick.
                            val cp = owners.fold(99999, (a, x) => imin(a, x))
                            val cwr = range(lo, hi).foldl(false, (a, b) => {
                                val m = lockBytes(ps, cp, i)
                                a or (m.keys().contains(b) and
                                      m.get(b) == LWrite)
                            })
                            out(fs, ps, SGetlkR({ e: OK, conflict: true,
                                                  cpid: cp, cw: cwr }))
                        }
                    }
                | CSetlk =>
                    if (lk == LkUn)
                        out(fs, lockUnset(ps, p, i, lo, hi), SOk({ e: OK }))
                    else if (lockConflict(ps, p, i, lo, hi, lkType(lk)))
                        outErr(fs, ps, errLockBusy(caps))
                    else
                        out(fs, lockSet(ps, p, i, lo, hi, lkType(lk)),
                            SOk({ e: OK }))
                | CSetlkw =>
                    // Blocking variant: generators only issue it when no
                    // conflict exists (the harness would block); the
                    // conflicting branch is defensive.
                    if (lk == LkUn)
                        out(fs, lockUnset(ps, p, i, lo, hi), SOk({ e: OK }))
                    else if (lockConflict(ps, p, i, lo, hi, lkType(lk)))
                        outErr(fs, ps, errLockBusy(caps))
                    else
                        out(fs, lockSet(ps, p, i, lo, hi, lkType(lk)),
                            SOk({ e: OK }))
            }
        }

    // lockf: exclusive locks only, section starts at the current offset
    // (len 0 = to end, modeled as the whole span), requires a descriptor
    // open for writing.
    pure def opLockf(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fd: Fd,
                     cmd: LockfCmd, len: int): Out =
        if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else {
            val o = fdOfd(ps, p, fd)
            val i = o.ino
            if (not(accWrites(o.acc))) outErr(fs, ps, EBADF)
            else if (fs.inodes.get(i).ftype != FReg) outErr(fs, ps, EINVAL)
            else if (len < 0) outErr(fs, ps, EINVAL)
            else {
                val lo = o.offset
                val hi = if (len == 0) imax(LOCK_SPAN, lo + 1)
                         else lo + len
                match cmd {
                    | LfUlock =>
                        out(fs, lockUnset(ps, p, i, lo, hi), SOk({ e: OK }))
                    | LfTst =>
                        if (lockConflict(ps, p, i, lo, hi, LWrite))
                            outErr(fs, ps, errLockBusy(caps))
                        else outErr(fs, ps, OK)
                    | LfLock =>
                        // Blocking variant; generators keep it
                        // conflict-free (see CSetlkw).
                        if (lockConflict(ps, p, i, lo, hi, LWrite))
                            outErr(fs, ps, errLockBusy(caps))
                        else out(fs, lockSet(ps, p, i, lo, hi, LWrite),
                                 SOk({ e: OK }))
                    | LfTlock =>
                        if (lockConflict(ps, p, i, lo, hi, LWrite))
                            outErr(fs, ps, errLockBusy(caps))
                        else out(fs, lockSet(ps, p, i, lo, hi, LWrite),
                                 SOk({ e: OK }))
                }
            }
        }

    // ------------------------------------------------------------------
    // copy_file_range / clone_file_range (Linux extensions; feature-gated)
    // ------------------------------------------------------------------

    pure def opCopyRange(fs: Fs, ps: Pstate, caps: Caps, p: Pid, fdIn: Fd,
                         offIn: int, fdOut: Fd, offOut: int,
                         len: int): Out =
        // Linux orders the descriptor checks before the filesystem-support
        // check (same as clone_range): copy_file_range's EBADF wins over
        // EOPNOTSUPP from a backend without copy offload.
        if (not(fdLive(ps, p, fdIn)) or not(fdLive(ps, p, fdOut)))
            out(fs, ps, rwErr(EBADF))
        else {
            val oi = fdOfd(ps, p, fdIn)
            val oo = fdOfd(ps, p, fdOut)
            if (not(accReads(oi.acc)) or not(accWrites(oo.acc)) or
                    oo.appendF)
                out(fs, ps, rwErr(EBADF))
            else if (not(caps.copyRange))
                out(fs, ps, rwErr(EOPNOTSUPP))
            else if (fs.inodes.get(oi.ino).ftype != FReg or
                     fs.inodes.get(oo.ino).ftype != FReg)
                out(fs, ps, rwErr(EINVAL))
            else if (offIn < 0 or offOut < 0)
                out(fs, ps, rwErr(EINVAL))
            else if (oi.ino == oo.ino and offIn < offOut + len and
                     offOut < offIn + len)
                // Overlapping ranges within one file.
                out(fs, ps, rwErr(EINVAL))
            else {
                val srcSz = fs.inodes.get(oi.ino).size
                val n = imax(0, imin(len, srcSz - offIn))
                if (n == 0)
                    out(fs, ps, rwOk(0, oo.ino, offOut))
                else {
                    val fs1 = fsCopyRange(fs, oi.ino, offIn, oo.ino,
                                          offOut, n)
                    out(fsMark(fs1, oi.ino, true, false, false), ps,
                        rwOk(n, oo.ino, offOut))
                }
            }
        }

    pure def opCloneRange(fs: Fs, ps: Pstate, caps: Caps, p: Pid,
                          fdDst: Fd, offDst: int, fdSrc: Fd, offSrc: int,
                          len: int): Out =
        // Linux orders the descriptor checks before the filesystem-support
        // check: FICLONERANGE's EBADF (bad fd / wrong access mode) wins over
        // EOPNOTSUPP from a backend without reflink support.
        if (not(fdLive(ps, p, fdDst)) or not(fdLive(ps, p, fdSrc)))
            outErr(fs, ps, EBADF)
        else {
            val od = fdOfd(ps, p, fdDst)
            val os = fdOfd(ps, p, fdSrc)
            if (not(accWrites(od.acc)) or not(accReads(os.acc)))
                outErr(fs, ps, EBADF)
            else if (not(caps.cloneRange)) outErr(fs, ps, EOPNOTSUPP)
            else if (fs.inodes.get(od.ino).ftype != FReg or
                     fs.inodes.get(os.ino).ftype != FReg)
                outErr(fs, ps, EINVAL)
            else if (offDst < 0 or offSrc < 0 or len <= 0)
                outErr(fs, ps, EINVAL)
            else if (offSrc + len > fs.inodes.get(os.ino).size)
                // The source range must lie within the source file.
                outErr(fs, ps, EINVAL)
            else if (blkOf(offSrc) * BS != offSrc or
                     blkOf(offDst) * BS != offDst or
                     (blkOf(len) * BS != len and
                      offSrc + len != fs.inodes.get(os.ino).size))
                // FICLONERANGE requires block-aligned offsets and a
                // block-multiple length (unless the length reaches src EOF).
                outErr(fs, ps, EINVAL)
            else if (od.ino == os.ino and offSrc < offDst + len and
                     offDst < offSrc + len)
                outErr(fs, ps, EINVAL)
            else
                out(fsCopyRange(fs, os.ino, offSrc, od.ino, offDst, len),
                    ps, SOk({ e: OK }))
        }

    // fallocate(2)/posix_fallocate(3).  mode 0 reserves [off, off+len) and
    // grows the file to off+len when it is shorter; mode 1 is the
    // FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE pair, which deallocates the
    // range and leaves the size unchanged.  Chimera validates off/len in the
    // libc-style wrapper BEFORE acquiring the descriptor, so EINVAL (off < 0
    // or len <= 0) precedes EBADF; a descriptor not open for writing is EBADF
    // (fallocate uses EBADF here, not the EINVAL ftruncate returns).  Both
    // modes mark mtime/ctime and neither clears the set-user/group-ID bits
    // (memfs_allocate bumps times only, never the mode).
    pure def opFallocate(fs: Fs, ps: Pstate, p: Pid, fd: Fd, mode: int,
                         off: int, len: int): Out =
        if (off < 0 or len <= 0) outErr(fs, ps, EINVAL)
        else if (not(fdLive(ps, p, fd))) outErr(fs, ps, EBADF)
        else {
            val o = fdOfd(ps, p, fd)
            if (not(accWrites(o.acc))) outErr(fs, ps, EBADF)
            else if (mode == 0)
                out(fsAllocate(fs, o.ino, off, len), ps, SOk({ e: OK }))
            else out(fsPunch(fs, o.ino, off, len), ps, SOk({ e: OK }))
        }

    // ------------------------------------------------------------------
    // Dispatch
    // ------------------------------------------------------------------

    pure def dispatch(fs: Fs, ps: Pstate, caps: Caps, p: Pid,
                      req: OpReq): Out =
        match req {
            | ROpen(a) => opOpen(fs, ps, caps, p, a.dfd, a.pth, a.fl)
            | RClose(a) => opClose(fs, ps, p, a.fd)
            | RDup(a) => opDup(fs, ps, p, a.fd)
            | RDup2(a) => opDup2(fs, ps, p, a.fd, a.nfd)
            | RLseek(a) => opLseek(fs, ps, caps, p, a.fd, a.off, a.wh)
            | RRead(a) => opRead(fs, ps, p, a.fd, a.len)
            | RWrite(a) => opWrite(fs, ps, caps, p, a.fd, a.len, a.pat)
            | RPread(a) => opPread(fs, ps, p, a.fd, a.off, a.len)
            | RPwrite(a) => opPwrite(fs, ps, caps, p, a.fd, a.off, a.len,
                                     a.pat)
            | RReadv(a) => opRead(fs, ps, p, a.fd, a.len)
            | RWritev(a) => opWrite(fs, ps, caps, p, a.fd, a.len, a.pat)
            | RPreadv(a) => opPread(fs, ps, p, a.fd, a.off, a.len)
            | RPwritev(a) => opPwrite(fs, ps, caps, p, a.fd, a.off, a.len,
                                      a.pat)
            | RStatfs(a) => opStatfs(fs, ps, caps, p, a.pth)
            | RStatvfs(a) => opStatfs(fs, ps, caps, p, a.pth)
            | RFstatfs(a) => opFstatfs(fs, ps, p, a.fd)
            | RFstatvfs(a) => opFstatfs(fs, ps, p, a.fd)
            | RTruncate(a) => opTruncate(fs, ps, caps, p, a.pth, a.len)
            | RFtruncate(a) => opFtruncate(fs, ps, caps, p, a.fd, a.len)
            | RStat(a) => opStat(fs, ps, caps, p, a.dfd, a.pth, a.follow)
            | RFstat(a) => opFstat(fs, ps, p, a.fd)
            | RChmod(a) => opChmod(fs, ps, caps, p, a.dfd, a.pth, a.mode)
            | RFchmod(a) => opFchmod(fs, ps, p, a.fd, a.mode)
            | RChown(a) => opChown(fs, ps, caps, p, a.dfd, a.pth, a.u, a.g,
                                   a.follow)
            | RFchown(a) => opFchown(fs, ps, p, a.fd, a.u, a.g)
            | RUtimens(a) => opUtimens(fs, ps, caps, p, a.dfd, a.pth, a.ta,
                                       a.tm)
            | RFutimens(a) => opFutimens(fs, ps, p, a.fd, a.ta, a.tm)
            | RAccess(a) => opAccess(fs, ps, caps, p, a.dfd, a.pth, a.r,
                                     a.w, a.x, a.eff)
            | RUmask(a) => opUmask(fs, ps, p, a.mask)
            | RMkdir(a) => opMkdir(fs, ps, caps, p, a.dfd, a.pth, a.mode)
            | RMknod(a) => opMknod(fs, ps, caps, p, a.pth, a.ft, a.mode)
            | RSymlink(a) => opSymlink(fs, ps, caps, p, a.tgt, a.dfd, a.pth)
            | RLink(a) => opLink(fs, ps, caps, p, a.dfdOld, a.pthOld,
                                 a.dfdNew, a.pthNew, a.followOld)
            | RUnlink(a) => opUnlink(fs, ps, caps, p, a.dfd, a.pth)
            | RRmdir(a) => opRmdir(fs, ps, caps, p, a.dfd, a.pth)
            | RRename(a) => opRename(fs, ps, caps, p, a.dfdOld, a.pthOld,
                                     a.dfdNew, a.pthNew)
            | RReadlink(a) => opReadlink(fs, ps, caps, p, a.dfd, a.pth)
            | ROpendir(a) => opOpendir(fs, ps, caps, p, a.pth)
            | RReaddir(a) => opReaddir(fs, ps, a.sid)
            | RRewinddir(a) => opRewinddir(fs, ps, a.sid)
            | RTelldir(a) => opTelldir(fs, ps, a.sid)
            | RSeekdir(a) => opSeekdir(fs, ps, a.sid)
            | RClosedir(a) => opClosedir(fs, ps, a.sid)
            | RFcntlDupfd(a) => opFcntlDupfd(fs, ps, p, a.fd, a.atLeast)
            | RFcntlGetfl(a) => opFcntlGetfl(fs, ps, p, a.fd)
            | RFcntlSetfl(a) => opFcntlSetfl(fs, ps, p, a.fd, a.appendF)
            | RFcntlLock(a) => opFcntlLock(fs, ps, caps, p, a.fd, a.cmd,
                                           a.lk, a.lo, a.hi)
            | RLockf(a) => opLockf(fs, ps, caps, p, a.fd, a.cmd, a.len)
            | RFsync(a) => opFsync(fs, ps, p, a.fd)
            | RCopyRange(a) => opCopyRange(fs, ps, caps, p, a.fdIn, a.offIn,
                                           a.fdOut, a.offOut, a.len)
            | RCloneRange(a) => opCloneRange(fs, ps, caps, p, a.fdDst,
                                             a.offDst, a.fdSrc, a.offSrc,
                                             a.len)
            | RFallocate(a) => opFallocate(fs, ps, p, a.fd, a.mode, a.off,
                                           a.len)
        }
}
