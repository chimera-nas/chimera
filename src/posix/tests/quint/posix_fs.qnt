// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Generic model of the filesystem underlying the POSIX file API.
//
// This is the POSIX-specialized sibling of the NFSv4 filesystem model
// (src/server/nfs/tests/quint/nfs4_fs.qnt).  The two modules share their
// core shapes on purpose -- inodes named by abstract integers, sparse block
// maps, dirent maps with parent pointers, orphan pinning for
// unlink-while-open -- so a future extraction of a common module is
// mechanical.  They differ where the protocols differ:
//   - here: uid/gid owners, the full 12 file mode bits, atime/mtime/ctime
//     as abstract instants, symlink targets as structured pathnames (the
//     POSIX layer resolves them component by component);
//   - only in nfs4_fs: xattrs, ALLOCATE's allocated-zero block state, the
//     NFS change attribute, the exclusive-create verifier.
// Neither module claims to exhaust the real chimera VFS, which is a richer
// NFS/SMB hybrid (NFSv4 ACLs, share modes, streams); each models exactly
// what its protocol can observe.
//
// Modeling conventions (shared with the NFS3/NFS4 models):
//  - Objects are named by an integer `Ino`; the replay harness maintains
//    the Ino -> real-identity mapping (learned from stat st_ino/st_dev).
//  - File contents are abstracted to block symbols: `blocks` maps a block
//    index to a nonzero symbol; an absent index is a hole and reads as
//    zeroes.  The harness expands symbol s at index i into a full block of
//    identical bytes at offset i * block-size.
//  - Timestamps are abstract instants drawn from a filesystem-wide
//    monotonic counter `clk`.  The harness never predicts wall-clock
//    values; it checks consistency: if the model timestamp is unchanged
//    between two observations the wire value must be unchanged, and if the
//    model timestamp advanced the wire value must be >= its previous value
//    (coarse server clocks make strict inequality uncheckable).  Explicit
//    times set by utimensat draw from a reserved negative universe and ARE
//    checked for exact wire equality (the harness keeps a table).
//  - This layer is mechanism only: no permission or credential checks
//    happen here.  The POSIX syscall layer (posix_ops.qnt) runs the access
//    algorithm and only then calls these mutators.

module posixfs {

    type Ino = int
    type Name = str

    type Ftype = FReg | FDir | FLnk | FFifo | FSock | FBlk | FChr

    // A symlink target: an abstract pathname.  abs=true means the target
    // starts at the filesystem root; comps may include "." and "..".
    type SymTarget = { abs: bool, comps: List[Name] }

    // Abstract outcome of a filesystem operation; the POSIX layer maps
    // these onto errno values (and adds all path/permission errnos itself).
    type FsStatus =
        | FsOk
        | FsNoent
        | FsExist
        | FsNotdir
        | FsIsdir
        | FsNotempty
        | FsInval

    // ------------------------------------------------------------------
    // File mode bits (numeric values of the octal constants)
    // ------------------------------------------------------------------

    pure val MB_SUID = 2048   // S_ISUID
    pure val MB_SGID = 1024   // S_ISGID
    pure val MB_SVTX = 512    // S_ISVTX (sticky)
    pure val MB_UR = 256
    pure val MB_UW = 128
    pure val MB_UX = 64
    pure val MB_GR = 32
    pure val MB_GW = 16
    pure val MB_GX = 8
    pure val MB_OR = 4
    pure val MB_OW = 2
    pure val MB_OX = 1

    // b must be one of the MB_* powers of two.
    pure def bitOn(m: int, b: int): bool = (m / b) % 2 == 1

    // The nine permission bits only (mask 0777).
    pure def permBits(m: int): int = m % 512

    // Drop S_ISUID and S_ISGID, keep sticky and permissions (chown/write
    // clearing rule; which callers apply it is POSIX-layer policy).
    pure def modeWithoutSetids(m: int): int = m % 1024

    // ------------------------------------------------------------------
    // Core types
    // ------------------------------------------------------------------

    // One record shape for every filesystem object; fields not meaningful
    // for a given type are inert (blocks/ents empty, target empty, parent
    // only meaningful for directories).
    type Inode = {
        ftype: Ftype,
        mode: int,            // 12 file mode bits
        uid: int,
        gid: int,
        nlink: int,
        // Present block indices (block = BS bytes); absent index = hole.
        // Byte content lives in the replay's fsx-style shadow, not the model;
        // the model tracks only which blocks are allocated (for SEEK_HOLE/
        // SEEK_DATA and fallocate/punch) and the byte size.  Indices are
        // always within [0, ceilDiv(size, BS)).
        dataBlocks: Set[int],
        size: int,
        ents: Name -> Ino,
        parent: Ino,
        target: SymTarget,
        // Abstract instants (positive: server clock; negative: explicit
        // utimensat values from the reserved universe).
        atime: int,
        mtime: int,
        ctime: int
    }

    type Fs = {
        inodes: Ino -> Inode,
        nextIno: Ino,
        removed: Set[Ino],
        // Monotonic source for abstract timestamp instants.
        clk: int
    }

    pure val ROOT: Ino = 0

    pure val NO_TARGET: SymTarget = { abs: false, comps: [] }

    // memfs block size.  Offsets/lengths/sizes in the model are BYTES; the
    // block-presence map (dataBlocks) is derived at this granularity so
    // SEEK_HOLE/SEEK_DATA and fallocate/punch match memfs's per-block storage.
    pure val BS: int = 4096

    pure def imin(a: int, b: int): int = if (a < b) a else b
    pure def imax(a: int, b: int): int = if (a > b) a else b

    // Integer division floors for the non-negative offsets/sizes used here.
    pure def blkOf(pos: int): int = pos / BS
    pure def ceilDiv(a: int, b: int): int = (a + b - 1) / b

    // The blocks touched by the byte range [off, off+len): floor(off/BS) ..
    // floor((off+len-1)/BS).  Empty for a zero-length range.
    pure def addBlocks(s: Set[int], off: int, len: int): Set[int] =
        if (len <= 0) s
        else range(blkOf(off), blkOf(off + len - 1) + 1)
            .foldl(s, (acc, i) => acc.union(Set(i)))

    // Quint has no builtin to delete a map key; rebuild without it.
    pure def mapDrop(m, k) = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    // ------------------------------------------------------------------
    // Inode constructors (times are assigned by fsMknode)
    // ------------------------------------------------------------------

    pure def mkDir(m: int, u: int, g: int): Inode = {
        ftype: FDir, mode: m, uid: u, gid: g, nlink: 2,
        dataBlocks: Set(), size: 0, ents: Map(), parent: ROOT,
        target: NO_TARGET, atime: 0, mtime: 0, ctime: 0
    }

    pure def mkFile(m: int, u: int, g: int): Inode = {
        ftype: FReg, mode: m, uid: u, gid: g, nlink: 1,
        dataBlocks: Set(), size: 0, ents: Map(), parent: ROOT,
        target: NO_TARGET, atime: 0, mtime: 0, ctime: 0
    }

    pure def mkSymlink(tgt: SymTarget, m: int, u: int, g: int): Inode = {
        ftype: FLnk, mode: m, uid: u, gid: g, nlink: 1,
        dataBlocks: Set(), size: 0, ents: Map(), parent: ROOT,
        target: tgt, atime: 0, mtime: 0, ctime: 0
    }

    pure def mkSpecial(ft: Ftype, m: int, u: int, g: int): Inode = {
        ftype: ft, mode: m, uid: u, gid: g, nlink: 1,
        dataBlocks: Set(), size: 0, ents: Map(), parent: ROOT,
        target: NO_TARGET, atime: 0, mtime: 0, ctime: 0
    }

    pure def fsInit(rootMode: int, rootUid: int, rootGid: int): Fs = {
        inodes: Map(ROOT -> mkDir(rootMode, rootUid, rootGid)),
        nextIno: 1,
        removed: Set(),
        clk: 1
    }

    // ------------------------------------------------------------------
    // Basic queries
    // ------------------------------------------------------------------

    pure def fsGet(fs: Fs, i: Ino): Inode = fs.inodes.get(i)
    pure def fsLive(fs: Fs, i: Ino): bool = fs.inodes.keys().contains(i)

    pure def fsDirs(fs: Fs): Set[Ino] =
        fs.inodes.keys().filter(i => fs.inodes.get(i).ftype == FDir)
    pure def fsFiles(fs: Fs): Set[Ino] =
        fs.inodes.keys().filter(i => fs.inodes.get(i).ftype == FReg)
    pure def fsSymlinks(fs: Fs): Set[Ino] =
        fs.inodes.keys().filter(i => fs.inodes.get(i).ftype == FLnk)
    pure def fsNonDirs(fs: Fs): Set[Ino] =
        fs.inodes.keys().filter(i => fs.inodes.get(i).ftype != FDir)

    pure def fsHasEnt(fs: Fs, d: Ino, n: Name): bool =
        fs.inodes.get(d).ents.keys().contains(n)

    pure def fsChild(fs: Fs, d: Ino, n: Name): Ino =
        fs.inodes.get(d).ents.get(n)

    // Inodes referenced by some directory entry (ROOT is inherently
    // referenced by the filesystem itself).
    pure def fsReferenced(fs: Fs): Set[Ino] =
        fsDirs(fs).map(d => fs.inodes.get(d).ents.keys()
                .map(n => fs.inodes.get(d).ents.get(n)))
            .flatten()
            .union(Set(ROOT))

    // Live inodes with no remaining name: unlinked-while-pinned survivors.
    pure def fsOrphans(fs: Fs): Set[Ino] =
        fs.inodes.keys().exclude(fsReferenced(fs))

    // All inos reachable from f inclusive (bounded tree walk; the tree has
    // at most |inodes| nodes, so that many expansions suffice).
    pure def fsDescOf(fs: Fs, f: Ino): Set[Ino] =
        range(0, fs.inodes.keys().size()).foldl(Set(f), (acc, _) =>
            acc.union(
                acc.filter(x => fs.inodes.get(x).ftype == FDir)
                   .map(x => fs.inodes.get(x).ents.keys()
                                .map(n => fs.inodes.get(x).ents.get(n)))
                   .flatten()))

    // ------------------------------------------------------------------
    // Timestamp bookkeeping
    // ------------------------------------------------------------------

    // Mark the selected timestamps of inode i with one fresh abstract
    // instant.  (POSIX XBD "File Times Update": fields marked by one call
    // may realize as one clock read; the harness only checks unchanged ==
    // and advanced >=, so sharing an instant per mark is sound.)
    pure def fsMark(fs: Fs, i: Ino, a: bool, m: bool, c: bool): Fs =
        fs.with("inodes", fs.inodes.setBy(i, x => {
            val x1 = if (a) x.with("atime", fs.clk) else x
            val x2 = if (m) x1.with("mtime", fs.clk) else x1
            if (c) x2.with("ctime", fs.clk) else x2
        })).with("clk", fs.clk + 1)

    // utimensat/futimens: set atime/mtime to explicit abstract values (from
    // the reserved negative universe) or to "now" (T_NOW), and mark ctime.
    pure val T_NOW = -1000000

    pure def fsSetTimes(fs: Fs, i: Ino,
                        doA: bool, va: int, doM: bool, vm: int): Fs =
        fs.with("inodes", fs.inodes.setBy(i, x => {
            val x1 = if (doA) x.with("atime", if (va == T_NOW) fs.clk else va)
                     else x
            val x2 = if (doM) x1.with("mtime", if (vm == T_NOW) fs.clk else vm)
                     else x1
            x2.with("ctime", fs.clk)
        })).with("clk", fs.clk + 1)

    // ------------------------------------------------------------------
    // Namespace operations
    // ------------------------------------------------------------------

    // Insert a fresh inode under (d, n).  Fails FsNotdir / FsExist.  On
    // success the new object's three timestamps and the parent's
    // mtime/ctime are marked (POSIX open/mkdir/mknod/symlink pages).
    pure def fsMknode(fs: Fs, d: Ino, n: Name, node: Inode):
            { fs2: Fs, st: FsStatus, ino: Ino } = {
        val dnode = fs.inodes.get(d)
        if (dnode.ftype != FDir)
            { fs2: fs, st: FsNotdir, ino: -1 }
        else if (fsHasEnt(fs, d, n))
            { fs2: fs, st: FsExist, ino: -1 }
        else {
            val child = fs.nextIno
            val node1 = if (node.ftype == FDir) node.with("parent", d) else node
            val fsA = fs
                .with("inodes", fs.inodes
                    .put(child, node1
                        .with("atime", fs.clk)
                        .with("mtime", fs.clk)
                        .with("ctime", fs.clk))
                    .setBy(d, dn => dn
                        .with("ents", dn.ents.put(n, child))
                        .with("mtime", fs.clk)
                        .with("ctime", fs.clk)
                        // A subdirectory's ".." adds a link to the parent.
                        .with("nlink", if (node.ftype == FDir) dn.nlink + 1
                                       else dn.nlink)))
                .with("nextIno", fs.nextIno + 1)
                .with("clk", fs.clk + 1)
            { fs2: fsA, st: FsOk, ino: child }
        }
    }

    // Remove the entry (d, n).  Callers split unlink vs rmdir typing (and
    // sticky-bit policy) before getting here; this handles the mechanism:
    //  - directories must be empty (FsNotempty)
    //  - `pinned` names inodes that must survive losing their last name
    //    (open file descriptions, directory streams).  A pinned victim is
    //    orphaned: name gone, nlink 0, inode alive, reaped by fsReap when
    //    the last pin goes.  POSIX requires exactly this for open files
    //    (unlink page) and allows removal of an open directory.
    //  - parent mtime/ctime are marked; a victim that survives with links
    //    remaining gets its ctime marked (unlink page: "if the file's link
    //    count is not 0").  An orphan's ctime is deliberately not marked;
    //    the harness treats orphan ctime as unobservable-by-path anyway.
    pure def fsRemove(fs: Fs, d: Ino, n: Name, pinned: Set[Ino]):
            { fs2: Fs, st: FsStatus, gone: Ino, orphaned: Ino } = {
        val dnode = fs.inodes.get(d)
        val same = { fs2: fs, st: FsOk, gone: -1, orphaned: -1 }
        if (dnode.ftype != FDir) same.with("st", FsNotdir)
        else if (not(fsHasEnt(fs, d, n))) same.with("st", FsNoent)
        else {
            val victim = fs.inodes.get(d).ents.get(n)
            val vnode = fs.inodes.get(victim)
            if (vnode.ftype == FDir and vnode.ents.keys().size() > 0)
                same.with("st", FsNotempty)
            else {
                val lastName = vnode.ftype == FDir or vnode.nlink == 1
                val dies = lastName and not(pinned.contains(victim))
                val orphans = lastName and pinned.contains(victim)
                val fsA = fs.with("inodes", fs.inodes.setBy(d, dn => dn
                    .with("ents", mapDrop(dn.ents, n))
                    .with("mtime", fs.clk)
                    .with("ctime", fs.clk)
                    .with("nlink", if (vnode.ftype == FDir) dn.nlink - 1
                                   else dn.nlink)))
                val fsB =
                    if (dies)
                        fsA.with("inodes", mapDrop(fsA.inodes, victim))
                           .with("removed", fsA.removed.union(Set(victim)))
                    else if (orphans)
                        // Orphaned: nlink drops to 0 (a removed directory
                        // sheds both "." and its parent entry).
                        fsA.with("inodes", fsA.inodes.setBy(victim, vn => vn
                            .with("nlink", 0)))
                    else
                        // Other links remain: ctime of the object marks.
                        fsA.with("inodes", fsA.inodes.setBy(victim, vn => vn
                            .with("nlink", vn.nlink - 1)
                            .with("ctime", fs.clk)))
                { fs2: fsB.with("clk", fs.clk + 1),
                  st: FsOk,
                  gone: if (dies) victim else -1,
                  orphaned: if (orphans) victim else -1 }
            }
        }
    }

    // Reap an orphaned inode once its last pin (open file description or
    // directory stream) is gone.
    pure def fsReap(fs: Fs, i: Ino): Fs =
        fs.with("inodes", mapDrop(fs.inodes, i))
          .with("removed", fs.removed.union(Set(i)))

    // rename(): full POSIX matrix (rename page).
    //  - source must exist (FsNoent)
    //  - self-rename or rename onto another link of the same object: no-op
    //    success, nothing changes ("shall return successfully and perform
    //    no other action")
    //  - a directory cannot move into its own subtree (FsInval)
    //  - existing target must be type-compatible (FsNotdir/FsIsdir) and,
    //    for directories, empty (FsNotempty); a replaced target dies, or is
    //    orphaned if pinned, or drops a link
    //  - moving a directory across parents updates its parent pointer and
    //    both parents' subdirectory link counts
    //  - both parents' mtime/ctime mark; a replaced target surviving with
    //    links gets ctime marked.  POSIX requires only the parents' times;
    //    whether the moved object's ctime marks is left to implementations
    //    (acceptance: harness tolerates either).
    pure def fsRename(fs: Fs, sd: Ino, sn: Name, td: Ino, tn: Name,
                      pinned: Set[Ino]):
            { fs2: Fs, st: FsStatus, gone: Ino, orphaned: Ino } = {
        val sdnode = fs.inodes.get(sd)
        val tdnode = fs.inodes.get(td)
        val same = { fs2: fs, st: FsOk, gone: -1, orphaned: -1 }
        if (sdnode.ftype != FDir or tdnode.ftype != FDir)
            same.with("st", FsNotdir)
        else if (not(fsHasEnt(fs, sd, sn))) same.with("st", FsNoent)
        else {
            val src = fs.inodes.get(sd).ents.get(sn)
            val srcNode = fs.inodes.get(src)
            if ((sd == td and sn == tn) or
                    (fsHasEnt(fs, td, tn) and fs.inodes.get(td).ents.get(tn) == src))
                // POSIX no-op: both names remain, nothing changes.
                same
            else if (srcNode.ftype == FDir and fsDescOf(fs, src).contains(td))
                same.with("st", FsInval)
            else {
                val tgtExists = fsHasEnt(fs, td, tn)
                val tgt = if (tgtExists) fs.inodes.get(td).ents.get(tn) else -1
                val tgtNode = if (tgtExists) fs.inodes.get(tgt) else srcNode
                if (tgtExists and srcNode.ftype == FDir and tgtNode.ftype != FDir)
                    same.with("st", FsNotdir)
                else if (tgtExists and srcNode.ftype != FDir and tgtNode.ftype == FDir)
                    same.with("st", FsIsdir)
                else if (tgtExists and tgtNode.ftype == FDir and
                         tgtNode.ents.keys().size() > 0)
                    same.with("st", FsNotempty)
                else {
                    val tgtLastName = tgtExists and
                        (tgtNode.ftype == FDir or tgtNode.nlink == 1)
                    val tgtDies = tgtLastName and not(pinned.contains(tgt))
                    val tgtOrphans = tgtLastName and pinned.contains(tgt)
                    // 1. drop the replaced target (if any); a pinned target
                    //    is orphaned (rename-over-open-file / open-dir)
                    val fsA =
                        if (tgtDies)
                            fs.with("inodes", mapDrop(fs.inodes, tgt))
                              .with("removed", fs.removed.union(Set(tgt)))
                        else if (tgtOrphans)
                            fs.with("inodes", fs.inodes.setBy(tgt, x => x
                                .with("nlink", 0)))
                        else if (tgtExists)
                            fs.with("inodes", fs.inodes.setBy(tgt, x => x
                                .with("nlink", x.nlink - 1)
                                .with("ctime", fs.clk)))
                        else fs
                    // 2. unhook the source name
                    val fsB = fsA.with("inodes", fsA.inodes.setBy(sd, dn => dn
                        .with("ents", mapDrop(dn.ents, sn))
                        .with("mtime", fs.clk)
                        .with("ctime", fs.clk)
                        .with("nlink",
                            if (srcNode.ftype == FDir and sd != td) dn.nlink - 1
                            else dn.nlink)))
                    // 3. hook up the target name.  Net subdir-link delta:
                    //    +1 if a directory arrives from another parent,
                    //    -1 if a directory target was replaced (it died or
                    //    orphaned).  Type mismatches were rejected above,
                    //    so a replaced target has the source's ftype.
                    val fsC = fsB.with("inodes", fsB.inodes.setBy(td, dn => dn
                        .with("ents", dn.ents.put(tn, src))
                        .with("mtime", fs.clk)
                        .with("ctime", fs.clk)
                        .with("nlink", dn.nlink
                            + (if (srcNode.ftype == FDir and sd != td) 1 else 0)
                            - (if (tgtExists and tgtNode.ftype == FDir) 1
                               else 0))))
                    // 4. a moved directory's ".." now points at td
                    val fsD =
                        if (srcNode.ftype == FDir and sd != td)
                            fsC.with("inodes", fsC.inodes.setBy(src, x =>
                                x.with("parent", td)))
                        else fsC
                    { fs2: fsD.with("clk", fs.clk + 1),
                      st: FsOk,
                      gone: if (tgtDies) tgt else -1,
                      orphaned: if (tgtOrphans) tgt else -1 }
                }
            }
        }
    }

    // link(): new name for an existing non-directory.  Linking a directory
    // is FsIsdir (the POSIX layer maps it to EPERM).  Adding a link marks
    // the object's ctime and the directory's mtime/ctime (link page).
    pure def fsLink(fs: Fs, obj: Ino, d: Ino, n: Name):
            { fs2: Fs, st: FsStatus } = {
        val dnode = fs.inodes.get(d)
        val same = { fs2: fs, st: FsOk }
        if (dnode.ftype != FDir) same.with("st", FsNotdir)
        else if (fs.inodes.get(obj).ftype == FDir) same.with("st", FsIsdir)
        else if (fsHasEnt(fs, d, n)) same.with("st", FsExist)
        else {
            val fsA = fs.with("inodes", fs.inodes
                .setBy(obj, x => x
                    .with("nlink", x.nlink + 1)
                    .with("ctime", fs.clk))
                .setBy(d, dn => dn
                    .with("ents", dn.ents.put(n, obj))
                    .with("mtime", fs.clk)
                    .with("ctime", fs.clk)))
            { fs2: fsA.with("clk", fs.clk + 1), st: FsOk }
        }
    }

    // ------------------------------------------------------------------
    // File content operations
    // ------------------------------------------------------------------

    // write()/pwrite(): the byte range [off, off+len) becomes data; the file
    // grows if needed and any gap below the old end stays a hole (sparse
    // write).  Only block presence is tracked here -- the exact bytes live in
    // the replay shadow.  Marks mtime/ctime.  dropSets applies the
    // setuid/setgid-clearing policy decided by the POSIX layer.  (pat is
    // carried for the op/replay; the model no longer stores content.)
    pure def fsWrite(fs: Fs, f: Ino, off: int, len: int, pat: int,
                     dropSets: bool):
            { fs2: Fs, newSize: int } = {
        val nd = fs.inodes.get(f)
        val nsz = imax(nd.size, off + len)
        { fs2: fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("dataBlocks", addBlocks(x.dataBlocks, off, len))
              .with("size", nsz)
              .with("mode", if (dropSets) modeWithoutSetids(x.mode) else x.mode)
              .with("mtime", fs.clk)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1),
          newSize: nsz }
    }

    // read()/pread(): the byte count for [off, min(off+len, size)); holes read
    // as zero.  The exact bytes are verified against the replay shadow; the
    // model predicts only the count and EOF.  Pure query; the POSIX layer
    // marks atime on success.
    pure def fsReadData(nd: Inode, off: int, len: int):
            { count: int, eof: bool } = {
        val ret = if (off >= nd.size) 0 else imin(len, nd.size - off)
        { count: ret, eof: off + ret >= nd.size }
    }

    // truncate()/ftruncate(): shrink keeps the blocks below ceilDiv(n, BS) --
    // the block holding the new EOF stays present with its tail zeroed --
    // and drops the rest; extension leaves the new tail a hole (reads as
    // zeroes; Issue 8 requires zero-fill extension).  Marks mtime/ctime
    // unconditionally.  The POSIX layer passes the setuid-clearing policy.
    pure def fsResize(fs: Fs, f: Ino, n: int, dropSets: bool): Fs = {
        val nd = fs.inodes.get(f)
        val kept = nd.dataBlocks.filter(i => i < ceilDiv(n, BS))
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("dataBlocks", kept)
              .with("size", n)
              .with("mode", if (dropSets) modeWithoutSetids(x.mode) else x.mode)
              .with("mtime", fs.clk)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)
    }

    // fallocate mode 0 (posix_fallocate): reserve [off, off+len) by
    // materializing the blocks it touches and growing the size to off+len when
    // shorter.  A materialized block reads as zero but is *allocated space* --
    // SEEK_HOLE/SEEK_DATA must see it as data, not a hole (RFC 7862 §11.1;
    // memfs_allocate materializes for exactly this reason).  Presence changes,
    // content does not.  Marks mtime/ctime.
    pure def fsAllocate(fs: Fs, f: Ino, off: int, len: int): Fs = {
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("dataBlocks", addBlocks(x.dataBlocks, off, len))
              .with("size", imax(x.size, off + len))
              .with("mtime", fs.clk)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)
    }

    // fallocate FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE: deallocate only the
    // blocks *fully* covered by [off, off+len) -- indices [ceil(off/BS),
    // floor((off+len)/BS)) -- which revert to holes; a partially-covered edge
    // block stays present (memfs zeroes the sub-range but keeps the block).
    // Size is unchanged.  Marks mtime/ctime unconditionally.
    pure def fsPunch(fs: Fs, f: Ino, off: int, len: int): Fs = {
        val lo = ceilDiv(off, BS)
        val hi = blkOf(off + len)
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("dataBlocks", x.dataBlocks.filter(i => i < lo or i >= hi))
              .with("mtime", fs.clk)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)
    }

    // chmod(): set the 12 mode bits; marks ctime.
    pure def fsChmod(fs: Fs, f: Ino, m: int): Fs =
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("mode", m)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)

    // chown(): set owner/group (-1 = keep); marks ctime even when the ids
    // are unchanged (chown page).  clearSets applies the mandatory
    // setuid/setgid clearing for regular files.
    pure def fsChown(fs: Fs, f: Ino, u: int, g: int, clearSets: bool): Fs =
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("uid", if (u == -1) x.uid else u)
              .with("gid", if (g == -1) x.gid else g)
              .with("mode", if (clearSets) modeWithoutSetids(x.mode) else x.mode)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)

    // copy_file_range()/clone: copy [soff, soff+len) of src over
    // [doff, doff+len) of dst; dst grows if needed.  Copy follows block
    // *presence*, not the symbol: a present source block (any pattern,
    // including a materialized zero from fallocate) copies to a present dst
    // block; an absent source block (a hole) leaves the dst block a hole.
    // Marks dst mtime/ctime.  The POSIX layer clamps len against the source
    // size before calling and marks src atime.
    // copy_file_range()/clone is generated BLOCK-ALIGNED (soff/doff/len are
    // multiples of BS), so the presence mapping is a plain per-block copy: a
    // present source block copies to a present dst block; a source hole leaves
    // the dst block a hole.  (The partial-edge copy-through-zeros path is left
    // to a later increment.)
    pure def fsCopyRange(fs: Fs, src: Ino, soff: int,
                         dst: Ino, doff: int, len: int): Fs = {
        val snode = fs.inodes.get(src)
        val dnode0 = fs.inodes.get(dst)
        val sb = blkOf(soff)
        val db = blkOf(doff)
        val nblocks = range(0, len / BS).foldl(dnode0.dataBlocks, (acc, i) =>
            if (snode.dataBlocks.contains(sb + i)) acc.union(Set(db + i))
            else acc.exclude(Set(db + i)))
        fs.with("inodes", fs.inodes.setBy(dst, x => x
              .with("dataBlocks", nblocks)
              .with("size", imax(x.size, doff + len))
              .with("mtime", fs.clk)
              .with("ctime", fs.clk)))
          .with("clk", fs.clk + 1)
    }

    // ------------------------------------------------------------------
    // Structural invariants (model self-checks; cheap, catch model bugs)
    // ------------------------------------------------------------------

    // Every directory entry points at a live inode.
    pure def fsEntsLive(fs: Fs): bool =
        fsDirs(fs).forall(d =>
            fs.inodes.get(d).ents.keys().forall(n =>
                fs.inodes.keys().contains(fs.inodes.get(d).ents.get(n))))

    // Removed inos never resurface.
    pure def fsRemovedStayDead(fs: Fs): bool =
        fs.removed.intersect(fs.inodes.keys()) == Set()

    // A non-directory's nlink equals the number of directory entries that
    // reference it (orphans: zero references, nlink 0 -- same formula).
    pure def fsNlinkConsistent(fs: Fs): bool =
        fsNonDirs(fs).forall(f =>
            fs.inodes.get(f).nlink == fsDirs(fs).fold(0, (acc, d) =>
                acc + fs.inodes.get(d).ents.keys()
                    .filter(n => fs.inodes.get(d).ents.get(n) == f).size()))

    // A referenced directory's nlink is 2 plus its subdirectory count, and
    // its children's parent pointers point back at it.  An orphaned
    // directory (removed while open) has nlink 0 and no entries.
    pure def fsDirLinksConsistent(fs: Fs): bool = {
        val refd = fsReferenced(fs)
        fsDirs(fs).forall(d => {
            val ents = fs.inodes.get(d).ents
            val subdirs = ents.keys().filter(n =>
                fs.inodes.get(ents.get(n)).ftype == FDir)
            if (refd.contains(d))
                fs.inodes.get(d).nlink == 2 + subdirs.size() and
                subdirs.forall(n => fs.inodes.get(ents.get(n)).parent == d)
            else
                fs.inodes.get(d).nlink == 0 and ents.keys().size() == 0
        })
    }

    // dataBlocks never holds a block at/after the size.  A present block is
    // allocated space (written, or a fallocate-materialized zero block); a
    // hole is the *absence* of a block.  The block holding the last byte is
    // index ceilDiv(size, BS) - 1.
    pure def fsBlocksInBounds(fs: Fs): bool =
        fs.inodes.keys().forall(i =>
            fs.inodes.get(i).dataBlocks.forall(b =>
                b < ceilDiv(fs.inodes.get(i).size, BS)))

    // The root is never removed, stays a directory, and is its own parent.
    pure def fsRootAlive(fs: Fs): bool =
        fs.inodes.keys().contains(ROOT) and
        fs.inodes.get(ROOT).ftype == FDir and
        fs.inodes.get(ROOT).parent == ROOT

    pure def fsInv(fs: Fs): bool =
        fsEntsLive(fs) and fsRemovedStayDead(fs) and fsNlinkConsistent(fs) and
        fsDirLinksConsistent(fs) and fsBlocksInBounds(fs) and fsRootAlive(fs)
}
