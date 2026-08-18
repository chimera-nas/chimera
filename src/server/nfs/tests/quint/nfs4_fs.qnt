// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Generic model of the filesystem underlying an NFSv4 server.
//
// This module is deliberately protocol-neutral: it models inodes, directory
// entries, hard links, sparse file extents, xattrs and an abstract change
// counter, and returns abstract FsStatus results.  The NFSv4 layer
// (nfs4_ops.qnt) maps FsStatus onto NFS4ERR_* codes and layers protocol
// state (opens, locks, delegations, layouts) on top.
//
// Modeling conventions (shared with the NFS3 model in nfs3.qnt):
//  - Objects are named by an integer `Ino`; the replay harness maintains the
//    Ino -> real-filehandle mapping, learning handles from GETFH replies.
//  - File contents are abstracted to block symbols.  A block is in one of
//    three states: a hole (unallocated, reads as zeroes), allocated-but-
//    unwritten (ALLOCATE; reads as zeroes, but counts as data for SEEK),
//    or written with a symbol.  The harness expands symbol s at block index
//    i into a full block of identical bytes at offset i * block-size.
//  - The change attribute is modeled as an abstract counter drawn from a
//    filesystem-wide monotonic source, so two distinct model states of an
//    inode always carry distinct abstract change values.  The harness never
//    predicts raw change values; it checks *consistency*: equal abstract
//    values must observe equal wire values and different abstract values
//    must observe different wire values (per inode), which is exactly what
//    RFC 7530 5.4 / RFC 8881 5.4 require of the change attribute.
//  - Directory inodes carry a parent pointer so LOOKUPP is predictable.
//  - Times, owner strings, fsid, and space accounting are not modeled; the
//    harness checks their consistency without predicting values.

module nfs4fs {

    type Ino = int
    type Name = str

    type Ftype = FReg | FDir | FLnk | FFifo | FSock | FBlk | FChr

    // One block of file content.
    //   BHole: unallocated; reads as zeroes; SEEK_HOLE finds it.
    //   BZero: allocated by ALLOCATE but never written; reads as zeroes.
    //          Treated as data by SEEK (the extent exists), matching the
    //          RFC 7862 15.11 model where only unallocated regions are holes.
    //   BData(s): written with block symbol s (s in 1..N, nonzero bytes).
    type BlockContent = BHole | BZero | BData(int)

    // Abstract outcome of a filesystem operation; the protocol layer maps
    // these to NFS4ERR_* codes.
    type FsStatus =
        | FsOk
        | FsNoent
        | FsExist
        | FsNotdir
        | FsIsdir
        | FsNotempty
        | FsInval
        | FsNoxattr

    // One record shape for every filesystem object; fields not meaningful
    // for a given type are inert (blocks/ents/xattrs empty, target "",
    // parent only meaningful for directories).
    type Inode = {
        ftype: Ftype,
        mode: int,
        nlink: int,
        // Sparse content map: block index -> content; absent = BHole.
        // Keys are always within [0, sizeBlocks).
        blocks: int -> BlockContent,
        sizeBlocks: int,
        ents: Name -> Ino,
        parent: Ino,
        target: str,
        // Abstract change-attribute value (unique across all inode states).
        change: int,
        // Extended attributes: name -> value symbol (RFC 8276).
        xattrs: Name -> int,
        // The verifier a file was EXCLUSIVE4-created with (0 = none); used
        // to model exclusive-create retry idempotency.  Cleared whenever
        // the attribute that stores the verifier (time_modify) changes.
        xverf: int
    }

    type Fs = {
        inodes: Ino -> Inode,
        nextIno: Ino,
        removed: Set[Ino],
        // Monotonic source for abstract change-attribute values.
        chg: int
    }

    // change_info4 prediction (abstract before/after values of a directory's
    // change attribute).  Whether the server reports atomic=true is its own
    // choice; the harness checks before/after consistency only when it does.
    type CInfo = { before: int, after: int }

    pure val ROOT: Ino = 0

    pure def imin(a: int, b: int): int = if (a < b) a else b
    pure def imax(a: int, b: int): int = if (a > b) a else b

    // Quint has no builtin to delete a map key; rebuild without it.
    pure def mapDrop(m, k) = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    // ------------------------------------------------------------------
    // Inode constructors
    // ------------------------------------------------------------------

    pure def mkDir(m: int, par: Ino, chgv: int): Inode = {
        ftype: FDir, mode: m, nlink: 2, blocks: Map(), sizeBlocks: 0,
        ents: Map(), parent: par, target: "", change: chgv,
        xattrs: Map(), xverf: 0
    }

    pure def mkFile(m: int, chgv: int): Inode = {
        ftype: FReg, mode: m, nlink: 1, blocks: Map(), sizeBlocks: 0,
        ents: Map(), parent: ROOT, target: "", change: chgv,
        xattrs: Map(), xverf: 0
    }

    pure def mkExclFile(m: int, v: int, chgv: int): Inode =
        mkFile(m, chgv).with("xverf", v)

    pure def mkSymlink(tgt: str, m: int, chgv: int): Inode = {
        ftype: FLnk, mode: m, nlink: 1, blocks: Map(), sizeBlocks: 0,
        ents: Map(), parent: ROOT, target: tgt, change: chgv,
        xattrs: Map(), xverf: 0
    }

    pure def mkSpecial(ft: Ftype, m: int, chgv: int): Inode = {
        ftype: ft, mode: m, nlink: 1, blocks: Map(), sizeBlocks: 0,
        ents: Map(), parent: ROOT, target: "", change: chgv,
        xattrs: Map(), xverf: 0
    }

    pure def fsInit(rootMode: int): Fs = {
        inodes: Map(ROOT -> mkDir(rootMode, ROOT, 0)),
        nextIno: 1,
        removed: Set(),
        chg: 1
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
    // Change-attribute bookkeeping
    // ------------------------------------------------------------------

    // Assign a fresh abstract change value to inode i.
    pure def bumpChange(fs: Fs, i: Ino): Fs =
        fs.with("inodes", fs.inodes.setBy(i, nd => nd.with("change", fs.chg)))
          .with("chg", fs.chg + 1)

    // ------------------------------------------------------------------
    // Namespace operations
    // ------------------------------------------------------------------

    // Insert a fresh inode under (d, n).  Fails FsNotdir / FsExist.
    // On success the new object gets fs.nextIno and the parent's change
    // advances (cinfo covers the parent directory).
    pure def fsMknode(fs: Fs, d: Ino, n: Name, node: Inode):
            { fs2: Fs, st: FsStatus, ino: Ino, cinfo: CInfo } = {
        val dnode = fs.inodes.get(d)
        if (dnode.ftype != FDir)
            { fs2: fs, st: FsNotdir, ino: -1,
              cinfo: { before: dnode.change, after: dnode.change } }
        else if (fsHasEnt(fs, d, n))
            { fs2: fs, st: FsExist, ino: -1,
              cinfo: { before: dnode.change, after: dnode.change } }
        else {
            val child = fs.nextIno
            val node1 = if (node.ftype == FDir) node.with("parent", d) else node
            val fsA = fs
                .with("inodes", fs.inodes
                    .put(child, node1.with("change", fs.chg))
                    .setBy(d, dn => dn
                        .with("ents", dn.ents.put(n, child))
                        .with("change", fs.chg + 1)
                        // A subdirectory's ".." adds a link to the parent.
                        .with("nlink", if (node.ftype == FDir) dn.nlink + 1
                                       else dn.nlink)))
                .with("nextIno", fs.nextIno + 1)
                .with("chg", fs.chg + 2)
            { fs2: fsA, st: FsOk, ino: child,
              cinfo: { before: dnode.change, after: fs.chg + 1 } }
        }
    }

    // REMOVE with NFSv4 semantics (RFC 7530 16.26 / RFC 8881 18.25): a
    // single untyped remove.  Non-directories are unlinked (the object dies
    // when its last link goes); directories must be empty (FsNotempty).
    //
    // `pinned` names inodes that must survive losing their last link
    // (NFSv4 unlink-while-open: an open file stays reachable through its
    // filehandle until the last CLOSE, RFC 7530 16.26.5).  A pinned victim
    // is orphaned -- name gone, nlink 0, inode alive -- and reaped later
    // via fsReap.
    pure def fsRemove(fs: Fs, d: Ino, n: Name, pinned: Set[Ino]):
            { fs2: Fs, st: FsStatus, cinfo: CInfo, gone: Ino, orphaned: Ino } = {
        val dnode = fs.inodes.get(d)
        val same = { fs2: fs, st: FsOk,
                     cinfo: { before: dnode.change, after: dnode.change },
                     gone: -1, orphaned: -1 }
        if (dnode.ftype != FDir) same.with("st", FsNotdir)
        else if (not(fsHasEnt(fs, d, n))) same.with("st", FsNoent)
        else {
            val victim = fs.inodes.get(d).ents.get(n)
            val vnode = fs.inodes.get(victim)
            if (vnode.ftype == FDir and vnode.ents.keys().size() > 0)
                same.with("st", FsNotempty)
            else {
                val dies = (vnode.ftype == FDir or vnode.nlink == 1) and
                           not(pinned.contains(victim))
                val orphans = vnode.ftype != FDir and vnode.nlink == 1 and
                              pinned.contains(victim)
                val fsA = fs.with("inodes", fs.inodes.setBy(d, dn => dn
                    .with("ents", mapDrop(dn.ents, n))
                    .with("change", fs.chg)
                    .with("nlink", if (vnode.ftype == FDir) dn.nlink - 1
                                   else dn.nlink)))
                val fsB =
                    if (dies)
                        fsA.with("inodes", mapDrop(fsA.inodes, victim))
                           .with("removed", fsA.removed.union(Set(victim)))
                    else
                        // Dropping a link is a metadata change on the
                        // object; an orphaned victim keeps nlink 0.
                        fsA.with("inodes", fsA.inodes.setBy(victim, vn => vn
                            .with("nlink", vn.nlink - 1)
                            .with("change", fs.chg + 1)))
                { fs2: fsB.with("chg", fs.chg + 2),
                  st: FsOk,
                  cinfo: { before: dnode.change, after: fs.chg },
                  gone: if (dies) victim else -1,
                  orphaned: if (orphans) victim else -1 }
            }
        }
    }

    // Reap an orphaned inode once its last pin (open/delegation/layout)
    // is gone.
    pure def fsReap(fs: Fs, i: Ino): Fs =
        fs.with("inodes", mapDrop(fs.inodes, i))
          .with("removed", fs.removed.union(Set(i)))

    // RENAME (RFC 7530 16.28 / RFC 8881 18.26): full POSIX matrix.
    //  - source must exist (FsNoent)
    //  - self-rename or rename onto another link of the same object: no-op OK
    //  - a directory cannot move into its own subtree (FsInval)
    //  - existing target must be type-compatible (FsNotdir/FsIsdir) and, for
    //    directories, empty (FsNotempty); a replaced target dies (or drops a
    //    link)
    //  - moving a directory across parents updates its parent pointer and
    //    both parents' subdirectory link counts
    pure def fsRename(fs: Fs, sd: Ino, sn: Name, td: Ino, tn: Name,
                      pinned: Set[Ino]):
            { fs2: Fs, st: FsStatus, cinfoS: CInfo, cinfoT: CInfo,
              gone: Ino, orphaned: Ino } = {
        val sdnode = fs.inodes.get(sd)
        val tdnode = fs.inodes.get(td)
        val same = { fs2: fs, st: FsOk,
                     cinfoS: { before: sdnode.change, after: sdnode.change },
                     cinfoT: { before: tdnode.change, after: tdnode.change },
                     gone: -1, orphaned: -1 }
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
                // Outcome of the target name: keep/replace/die.
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
                    val tgtDies = tgtExists and
                        (tgtNode.ftype == FDir or tgtNode.nlink == 1) and
                        not(pinned.contains(tgt))
                    val tgtOrphans = tgtExists and tgtNode.ftype != FDir and
                        tgtNode.nlink == 1 and pinned.contains(tgt)
                    // 1. drop the replaced target (if any); a pinned
                    //    target is orphaned (rename-over-open-file)
                    val fsA =
                        if (tgtDies)
                            fs.with("inodes", mapDrop(fs.inodes, tgt))
                              .with("removed", fs.removed.union(Set(tgt)))
                        else if (tgtExists)
                            fs.with("inodes", fs.inodes.setBy(tgt, x => x
                                .with("nlink", x.nlink - 1)
                                .with("change", fs.chg)))
                        else fs
                    // 2. unhook the source name
                    val fsB = fsA.with("inodes", fsA.inodes.setBy(sd, dn => dn
                        .with("ents", mapDrop(dn.ents, sn))
                        .with("change", fs.chg + 1)
                        .with("nlink",
                            if (srcNode.ftype == FDir and sd != td) dn.nlink - 1
                            else dn.nlink)))
                    // 3. hook up the target name.  Net subdir-link delta:
                    //    +1 if a directory arrives from another parent,
                    //    -1 if a directory target was replaced (it died).
                    //    Type mismatches were rejected above, so a replaced
                    //    target has the source's ftype.
                    val fsC = fsB.with("inodes", fsB.inodes.setBy(td, dn => dn
                        .with("ents", dn.ents.put(tn, src))
                        .with("change", fs.chg + 2)
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
                    // 5. RFC 7530 5.8.1.4 / POSIX: a successful rename marks
                    //    the renamed object's status-change time, so its
                    //    change attribute advances too (chimera/memfs bump
                    //    the moved inode's ctime -> change).
                    val fsE = fsD.with("inodes", fsD.inodes.setBy(src, x =>
                        x.with("change", fs.chg + 3)))
                    // A same-directory rename touches the one directory
                    // through both the unhook (fs.chg+1) and the hook
                    // (fs.chg+2) steps, so its final change field is
                    // fs.chg+2 -- both cinfos must report that value, or a
                    // later GETATTR(change) observes an after the rename
                    // never announced.  Cross-directory, source ends at
                    // fs.chg+1 and target at fs.chg+2.
                    { fs2: fsE.with("chg", fs.chg + 4),
                      st: FsOk,
                      cinfoS: { before: sdnode.change,
                                after: if (sd == td) fs.chg + 2
                                       else fs.chg + 1 },
                      cinfoT: { before: tdnode.change, after: fs.chg + 2 },
                      gone: if (tgtDies) tgt else -1,
                      orphaned: if (tgtOrphans) tgt else -1 }
                }
            }
        }
    }

    // LINK (RFC 7530 16.9 / RFC 8881 18.9): new name for an existing
    // non-directory.  Linking a directory is FsIsdir.  Adding a link is a
    // metadata change on the object (nlink/ctime) and a change on the dir.
    pure def fsLink(fs: Fs, obj: Ino, d: Ino, n: Name):
            { fs2: Fs, st: FsStatus, cinfo: CInfo } = {
        val dnode = fs.inodes.get(d)
        val same = { fs2: fs, st: FsOk,
                     cinfo: { before: dnode.change, after: dnode.change } }
        if (dnode.ftype != FDir) same.with("st", FsNotdir)
        else if (fs.inodes.get(obj).ftype == FDir) same.with("st", FsIsdir)
        else if (fsHasEnt(fs, d, n)) same.with("st", FsExist)
        else {
            val fsA = fs.with("inodes", fs.inodes
                .setBy(obj, x => x
                    .with("nlink", x.nlink + 1)
                    .with("change", fs.chg))
                .setBy(d, dn => dn
                    .with("ents", dn.ents.put(n, obj))
                    .with("change", fs.chg + 1)))
            { fs2: fsA.with("chg", fs.chg + 2),
              st: FsOk,
              cinfo: { before: dnode.change, after: fs.chg + 1 } }
        }
    }

    // ------------------------------------------------------------------
    // File content operations
    // ------------------------------------------------------------------

    pure def blockAt(nd: Inode, i: int): BlockContent =
        if (nd.blocks.keys().contains(i)) nd.blocks.get(i) else BHole

    pure def blockSym(bc: BlockContent): int =
        match bc {
            | BHole => 0
            | BZero => 0
            | BData(s) => s
        }

    // WRITE: blocks [off, off+len) := BData(pat); the file grows if needed
    // and any gap below the old end stays a hole (sparse write).  Data and
    // size changes advance the change attribute and clear the exclusive-
    // create verifier (the verifier lives in time_modify).
    pure def fsWrite(fs: Fs, f: Ino, off: int, len: int, pat: int):
            { fs2: Fs, newSizeBlocks: int } = {
        val nd = fs.inodes.get(f)
        val nsz = imax(nd.sizeBlocks, off + len)
        val nblocks = range(off, off + len).foldl(nd.blocks, (acc, i) =>
            acc.put(i, BData(pat)))
        { fs2: fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("blocks", nblocks)
              .with("sizeBlocks", nsz)
              .with("change", fs.chg)
              .with("xverf", 0)))
          .with("chg", fs.chg + 1),
          newSizeBlocks: nsz }
    }

    // READ: symbols for [off, min(off+len, size)); holes and allocated-zero
    // blocks read as symbol 0.
    pure def fsReadData(nd: Inode, off: int, len: int):
            { syms: List[int], count: int, eof: bool } = {
        val ret = if (off >= nd.sizeBlocks) 0 else imin(len, nd.sizeBlocks - off)
        { syms: range(off, off + ret).foldl([], (acc, i) =>
              acc.append(blockSym(blockAt(nd, i)))),
          count: ret,
          eof: off + ret >= nd.sizeBlocks }
    }

    // SETATTR size: truncate (drop blocks at/after n) or extend (the new
    // tail is a hole).  A resize to the *current* size changes no data or
    // metadata, so it is a no-op: the change attribute does not advance and
    // the exclusive-create verifier is not cleared (RFC 7530 5.8.1.4 -- the
    // change attribute moves only on an actual change; memfs matches this,
    // e.g. an O_TRUNC-style OPEN of an already-empty file).  A real size
    // change advances change and clears the verifier.
    pure def fsResize(fs: Fs, f: Ino, n: int): Fs = {
        val nd = fs.inodes.get(f)
        if (nd.sizeBlocks == n) fs
        else {
            val nblocks = nd.blocks.keys().filter(i => i < n).mapBy(i => nd.blocks.get(i))
            fs.with("inodes", fs.inodes.setBy(f, x => x
                  .with("blocks", nblocks)
                  .with("sizeBlocks", n)
                  .with("change", fs.chg)
                  .with("xverf", 0)))
              .with("chg", fs.chg + 1)
        }
    }

    // SETATTR mode (or any pure-metadata attribute): change advances, the
    // exclusive-create verifier survives (time_modify untouched).
    pure def fsChmod(fs: Fs, f: Ino, m: int): Fs =
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("mode", m)
              .with("change", fs.chg)))
          .with("chg", fs.chg + 1)

    // ALLOCATE (RFC 7862 15.1): blocks in [off, off+len) become allocated;
    // holes turn into BZero, written data is untouched.  The file grows if
    // off+len exceeds the size.
    pure def fsAllocate(fs: Fs, f: Ino, off: int, len: int): Fs = {
        val nd = fs.inodes.get(f)
        val nblocks = range(off, off + len).foldl(nd.blocks, (acc, i) =>
            match blockAt(nd, i) {
                | BHole => acc.put(i, BZero)
                | BZero => acc
                | BData(s) => acc
            })
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("blocks", nblocks)
              .with("sizeBlocks", imax(nd.sizeBlocks, off + len))
              .with("change", fs.chg)
              .with("xverf", 0)))
          .with("chg", fs.chg + 1)
    }

    // DEALLOCATE (RFC 7862 15.2): punch a hole in [off, off+len); the size
    // never changes (blocks at/after the size are ignored).
    pure def fsDeallocate(fs: Fs, f: Ino, off: int, len: int): Fs = {
        val nd = fs.inodes.get(f)
        val nblocks = nd.blocks.keys()
            .filter(i => i < off or i >= off + len)
            .mapBy(i => nd.blocks.get(i))
        fs.with("inodes", fs.inodes.setBy(f, x => x
              .with("blocks", nblocks)
              .with("change", fs.chg)
              .with("xverf", 0)))
          .with("chg", fs.chg + 1)
    }

    // SEEK (RFC 7862 15.11).  whatData=true searches for data (BData or
    // BZero: allocated extents are data), whatData=false for a hole.  The
    // region at/after the size is a virtual hole, so a hole search always
    // succeeds for off < size; a data search may hit EOF.
    //   found:  false means no such region before EOF (data search only)
    //   offset: first matching block index (valid when found)
    // The protocol layer maps off >= size to NFS4ERR_NXIO for both kinds.
    pure def fsSeek(nd: Inode, off: int, whatData: bool):
            { found: bool, offset: int } = {
        val hit = range(off, nd.sizeBlocks).foldl(-1, (acc, i) =>
            if (acc >= 0) acc
            else {
                val isData = match blockAt(nd, i) {
                    | BHole => false
                    | BZero => true
                    | BData(s) => true
                }
                if (isData == whatData) i else acc
            })
        if (hit >= 0)
            { found: true, offset: hit }
        else if (whatData)
            { found: false, offset: nd.sizeBlocks }
        else
            // Virtual hole at EOF.
            { found: true, offset: nd.sizeBlocks }
    }

    // ------------------------------------------------------------------
    // Extended attributes (RFC 8276)
    // ------------------------------------------------------------------

    type XattrHow = XCreate | XReplace | XEither

    pure def fsSetXattr(fs: Fs, o: Ino, n: Name, v: int, how: XattrHow):
            { fs2: Fs, st: FsStatus } = {
        val nd = fs.inodes.get(o)
        val present = nd.xattrs.keys().contains(n)
        if (how == XCreate and present)
            { fs2: fs, st: FsExist }
        else if (how == XReplace and not(present))
            { fs2: fs, st: FsNoxattr }
        else
            { fs2: fs.with("inodes", fs.inodes.setBy(o, x => x
                  .with("xattrs", x.xattrs.put(n, v))
                  .with("change", fs.chg)))
              .with("chg", fs.chg + 1),
              st: FsOk }
    }

    pure def fsRemoveXattr(fs: Fs, o: Ino, n: Name):
            { fs2: Fs, st: FsStatus } = {
        val nd = fs.inodes.get(o)
        if (not(nd.xattrs.keys().contains(n)))
            { fs2: fs, st: FsNoxattr }
        else
            { fs2: fs.with("inodes", fs.inodes.setBy(o, x => x
                  .with("xattrs", mapDrop(x.xattrs, n))
                  .with("change", fs.chg)))
              .with("chg", fs.chg + 1),
              st: FsOk }
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
    // reference it.
    pure def fsNlinkConsistent(fs: Fs): bool =
        fsNonDirs(fs).forall(f =>
            fs.inodes.get(f).nlink == fsDirs(fs).fold(0, (acc, d) =>
                acc + fs.inodes.get(d).ents.keys()
                    .filter(n => fs.inodes.get(d).ents.get(n) == f).size()))

    // A directory's nlink is 2 plus its subdirectory count, and its
    // children's parent pointers point back at it.
    pure def fsDirLinksConsistent(fs: Fs): bool =
        fsDirs(fs).forall(d => {
            val subdirs = fs.inodes.get(d).ents.keys().filter(n =>
                fs.inodes.get(fs.inodes.get(d).ents.get(n)).ftype == FDir)
            fs.inodes.get(d).nlink == 2 + subdirs.size() and
            subdirs.forall(n =>
                fs.inodes.get(fs.inodes.get(d).ents.get(n)).parent == d)
        })

    // Content maps never hold blocks at/after the size.
    pure def fsBlocksInBounds(fs: Fs): bool =
        fs.inodes.keys().forall(i =>
            fs.inodes.get(i).blocks.keys().forall(b =>
                b < fs.inodes.get(i).sizeBlocks))

    // The root is never removed, stays a directory, and is its own parent.
    pure def fsRootAlive(fs: Fs): bool =
        fs.inodes.keys().contains(ROOT) and
        fs.inodes.get(ROOT).ftype == FDir and
        fs.inodes.get(ROOT).parent == ROOT

    pure def fsInv(fs: Fs): bool =
        fsEntsLive(fs) and fsRemovedStayDead(fs) and fsNlinkConsistent(fs) and
        fsDirLinksConsistent(fs) and fsBlocksInBounds(fs) and fsRootAlive(fs)
}
