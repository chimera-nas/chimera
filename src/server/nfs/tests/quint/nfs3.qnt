// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Quint model of an NFSv3 server (RFC 1813), used for model-based testing.
//
// The model tracks an abstract filesystem and, on every step, records the
// RPC issued and the reply the server is expected to produce in the state
// variable `lastOp`.  Traces generated with `quint run --out-itf` are
// replayed against an in-process chimera server by nfs3_mbt_replay.c,
// which compares the
// server's actual replies against `lastOp` step by step.
//
// Modeling conventions:
//  - The model is RFC-first: where chimera's behavior is a deliberate
//    implementation choice (or a deviation pending triage), the choice is
//    annotated with a `chimera:` comment citing the source location or the
//    probe that established it.  Deviations from RFC 1813 that are pending
//    a server-side decision are additionally marked CANDIDATE-FIX; when
//    the server changes, flip the annotated expectation here and
//    regenerate (traces regenerate automatically at build time).
//  - File handles are opaque on the wire; the model names objects by an
//    integer `Fid` and the harness maintains the Fid -> real-FH mapping,
//    learning handles from LOOKUP/CREATE/MKDIR/SYMLINK/MKNOD/READDIRPLUS
//    replies.
//  - File contents are abstracted to a list of block symbols (small ints).
//    The harness expands block symbol s at index i into a full block of
//    identical bytes at offset i * block-size, so real offsets are
//    exercised while the model state stays tiny.
//  - Times, fileids, uid/gid, fsid and space-used are not modeled; the
//    harness checks fileid *consistency* (same Fid always reports the same
//    fileid) without predicting values.  The SETATTR ctime guard is
//    modeled as a *scenario* (none/match/stale): the harness constructs a
//    matching guard by fetching the live ctime with an auxiliary GETATTR,
//    or a deliberately stale one, and the model predicts the outcome.

module nfs3 {

    type Fid = int
    type Name = str

    type Ftype = TReg | TDir | TLnk | TFifo | TSock

    // One record shape for every filesystem object; fields not meaningful
    // for a given type are inert (data/ents empty, target "").
    type Node = {
        ftype: Ftype,
        mode: int,
        nlink: int,
        data: List[int],
        ents: Name -> Fid,
        target: str,
        // POSIX ownership (AUTH_SYS DAC): the object's owner uid/gid.  A node
        // is owned by its creator's credential; the root is owned by 0/0.
        uid: int,
        gid: int,
        // The verifier a file was EXCLUSIVE-created with (0 = not created
        // exclusively).  Used only to model RFC 1813 3.3.8 exclusive-create
        // idempotency: a retry with the same verifier must succeed.
        xverf: int
    }

    // ------------------------------------------------------------------
    // AUTH_SYS credentials and the POSIX permission algorithm (XBD 4.5)
    // ------------------------------------------------------------------

    // A caller's Unix credential: effective uid/gid plus supplementary gids.
    type Cred = { uid: int, gid: int, gids: Set[int] }

    pure val ROOT_CRED: Cred = { uid: 0, gid: 0, gids: Set() }
    pure val ALICE:     Cred = { uid: 1000, gid: 1000, gids: Set() }
    pure val BOB:       Cred = { uid: 1001, gid: 1001, gids: Set() }
    // The principals traces run operations as.  Root exercises the privileged
    // path; alice/bob exercise owner / group-or-other class distinctions.
    pure val CREDS = Set(ROOT_CRED, ALICE, BOB)

    // Owner-override (the NFS server's MAY_OWNER_OVERRIDE, RFC 1813 leaves the
    // server free to grant it): when true, a file's owner is granted READ and
    // WRITE regardless of the mode bits -- the Linux/knfsd behavior, and what
    // chimera's NFS server instructs the VFS to do.  When false, strict POSIX:
    // the owner is bound by the owner-class bits.  chimera's NFS server grants
    // owner-override, so the NFS model asserts it on; the strict (false) side
    // is exercised via the POSIX suite's per-backend profile.  Flip here to
    // check strict NFS in isolation.
    pure val P_OWNER_OVERRIDE: bool = true

    // Permission bit values within one rwx triad.
    pure def bitR(triad: int): bool = (triad / 4) % 2 == 1
    pure def bitW(triad: int): bool = (triad / 2) % 2 == 1
    pure def bitX(triad: int): bool = triad % 2 == 1

    // The mode triad that applies to `cred` on `node` (owner, else group if
    // the gid or a supplementary gid matches, else other).
    pure def classTriad(node: Node, cred: Cred): int =
        if (cred.uid == node.uid) (node.mode / 64) % 8
        else if (cred.gid == node.gid or cred.gids.contains(node.gid)) (node.mode / 8) % 8
        else node.mode % 8

    pure def isOwner(node: Node, cred: Cred): bool = cred.uid == node.uid

    // POSIX read/write/search permission.  Root bypasses read/write and may
    // always search a directory.  A non-root owner gets read/write
    // unconditionally under owner-override; otherwise the class triad decides.
    pure def canRead(node: Node, cred: Cred): bool =
        cred.uid == 0
        or (P_OWNER_OVERRIDE and isOwner(node, cred))
        or bitR(classTriad(node, cred))
    pure def canWrite(node: Node, cred: Cred): bool =
        cred.uid == 0
        or (P_OWNER_OVERRIDE and isOwner(node, cred))
        or bitW(classTriad(node, cred))
    // Directory search / path traversal (execute on a directory).
    pure def canSearch(node: Node, cred: Cred): bool =
        cred.uid == 0 or bitX(classTriad(node, cred))
    // May `cred` create/remove an entry in directory `dir` (write + search).
    pure def canModifyDir(dir: Node, cred: Cred): bool =
        canWrite(dir, cred) and canSearch(dir, cred)

    type CreateMode = Unchecked | Guarded | Exclusive

    // Transition label: the RPC issued by this step plus the reply the
    // server must produce.  Fid-valued reply fields are -1 on error paths;
    // -1 in OSetattr's mode/sizeBlocks means "not set" and in OCreate's
    // verf means "not exclusive".  OSetattr guard: 0 none, 1 match, 2 stale.
    type Op =
        | OInit
        | OLookup({ dir: Fid, name: Name, status: int, child: Fid })
        | OGetattr({ obj: Fid, status: int })
        | OSetattr({ obj: Fid, mode: int, sizeBlocks: int, guard: int, status: int })
        | OAccess({ obj: Fid, mask: int, status: int, access: int })
        | OCreate({ dir: Fid, name: Name, cmode: CreateMode, mode: int, verf: int,
                    status: int, obj: Fid, cred: Cred })
        | OMkdir({ dir: Fid, name: Name, mode: int, status: int, obj: Fid })
        | OSymlink({ dir: Fid, name: Name, target: str, status: int, obj: Fid })
        | OReadlink({ obj: Fid, status: int, target: str })
        | OMknod({ dir: Fid, name: Name, ftype: Ftype, mode: int, status: int, obj: Fid })
        | OWrite({ file: Fid, offset: int, count: int, pat: int, stable: int,
                   status: int, newSizeBlocks: int, cred: Cred })
        | ORead({ file: Fid, offset: int, count: int,
                  status: int, retCount: int, eof: bool, blocks: List[int], cred: Cred })
        | ORemove({ dir: Fid, name: Name, status: int })
        | ORmdir({ dir: Fid, name: Name, status: int })
        | ORename({ fromDir: Fid, fromName: Name, toDir: Fid, toName: Name, status: int })
        | OLink({ obj: Fid, dir: Fid, name: Name, status: int })
        | OReaddir({ dir: Fid, plus: bool, status: int, names: Set[Name] })
        | OCommit({ file: Fid, status: int })
        | OFsstat({ status: int })
        | OFsinfo({ status: int })
        | OPathconf({ obj: Fid, status: int })
        | OStaleGetattr({ obj: Fid, status: int })

    var fs: Fid -> Node
    var removed: Set[Fid]
    var nextFid: Fid
    var lastOp: Op

    // nfsstat3 values (RFC 1813 section 2.6), numerically identical to the
    // wire encoding so the harness compares raw status with no decode step.
    pure val OK = 0
    pure val ERR_ACCES = 13
    pure val ERR_NOENT = 2
    pure val ERR_EXIST = 17
    pure val ERR_NOTDIR = 20
    pure val ERR_ISDIR = 21
    pure val ERR_INVAL = 22
    pure val ERR_NOTEMPTY = 66
    pure val ERR_STALE = 70
    pure val ERR_NOT_SYNC = 10002

    pure val ROOT: Fid = 0

    // Bounded universes: small enough that name collisions, wrong-type
    // targets and directory reuse happen within a few dozen steps.
    pure val NAMES = Set("a", "b", "c", "d")
    pure val MODES = Set(420, 493)     // 0644, 0755 (Quint has no octal literals)
    pure val MAX_OBJECTS = 14
    pure val MAX_BLOCKS = 6
    pure val SYMLINK_TARGETS = Set("a", "a/b/target", "../up")
    pure val ACCESS_MASKS = Set(1, 32, 63)   // READ, EXECUTE, all bits
    pure val EXCL_VERFS = Set(1, 2)
    // Symbol 0 is reserved for holes (sparse regions read back as zeroes);
    // explicit writes use symbols 1..3, which the harness expands to blocks
    // of distinct non-zero bytes.
    pure val BLOCK_SYMS = 1.to(3)
    pure val STABLE_LEVELS = Set(0, 1, 2)   // UNSTABLE / DATA_SYNC / FILE_SYNC

    pure def imin(a: int, b: int): int = if (a < b) a else b
    pure def imax(a: int, b: int): int = if (a > b) a else b

    // Quint has no builtin to delete a map key; rebuild without it.
    pure def mapDrop(m, k) = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    pure def emptyDir(m: int, o: Cred): Node =
        { ftype: TDir, mode: m, nlink: 2, data: [], ents: Map(), target: "",
          uid: o.uid, gid: o.gid, xverf: 0 }
    pure def emptyFile(m: int, o: Cred): Node =
        { ftype: TReg, mode: m, nlink: 1, data: [], ents: Map(), target: "",
          uid: o.uid, gid: o.gid, xverf: 0 }
    pure def exclFile(m: int, v: int, o: Cred): Node =
        { ftype: TReg, mode: m, nlink: 1, data: [], ents: Map(), target: "",
          uid: o.uid, gid: o.gid, xverf: v }
    pure def symlinkNode(tgt: str, o: Cred): Node =
        // chimera: symlink mode is fixed 0755; the sattr3 mode in the
        // SYMLINK request is not applied (probed 2026-08-08; requested
        // 0777, observed 0755).
        { ftype: TLnk, mode: 493, nlink: 1, data: [], ents: Map(), target: tgt,
          uid: o.uid, gid: o.gid, xverf: 0 }
    pure def nodeOf(ft: Ftype, m: int, o: Cred): Node =
        { ftype: ft, mode: m, nlink: 1, data: [], ents: Map(), target: "",
          uid: o.uid, gid: o.gid, xverf: 0 }

    // Blocks [off, off+len) := pat, zero-filling any gap beyond the old end
    // (a sparse write reads back as zero blocks).
    pure def writeBlocks(d: List[int], off: int, len: int, pat: int): List[int] =
        range(0, imax(d.length(), off + len)).foldl([], (acc, i) =>
            acc.append(
                if (i >= off and i < off + len) pat
                else if (i < d.length()) d[i]
                else 0))

    // Truncate or zero-extend to exactly n blocks (SETATTR size).
    pure def resizeBlocks(d: List[int], n: int): List[int] =
        range(0, n).foldl([], (acc, i) =>
            acc.append(if (i < d.length()) d[i] else 0))

    def dirs = fs.keys().filter(f => fs.get(f).ftype == TDir)
    def files = fs.keys().filter(f => fs.get(f).ftype == TReg)
    def nonDirs = fs.keys().filter(f => fs.get(f).ftype != TDir)
    def symlinks = fs.keys().filter(f => fs.get(f).ftype == TLnk)

    def hasEnt(d: Fid, n: Name): bool = fs.get(d).ents.keys().contains(n)

    // All fids reachable from f inclusive (bounded tree walk; the tree has
    // at most MAX_OBJECTS nodes, so MAX_OBJECTS expansions suffice).
    def descOf(f: Fid): Set[Fid] =
        range(0, MAX_OBJECTS).foldl(Set(f), (acc, _) =>
            acc.union(
                acc.filter(x => fs.get(x).ftype == TDir)
                   .map(x => fs.get(x).ents.keys()
                                .map(n => fs.get(x).ents.get(n)))
                   .flatten()))

    action init = all {
        // chimera: memfs.c:1092 -- a fresh memfs share root is 0777 (511).
        fs' = Map(ROOT -> emptyDir(511, ROOT_CRED)),
        removed' = Set(),
        nextFid' = 1,
        lastOp' = OInit,
    }

    // ------------------------------------------------------------------
    // LOOKUP (RFC 1813 3.3.3)
    // ------------------------------------------------------------------

    // RFC 1813 3.3.3: LOOKUP on any non-directory handle is NFS3ERR_NOTDIR.
    // chimera deviates for a symlink handle (returns SERVERFAULT); that is
    // recorded as deviation `lookup-symlink-serverfault` in the nfs3_mbt_replay.c deviation registry
    // and reconciled by the harness, NOT encoded here.
    action applyLookup(d: Fid, n: Name): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' =
            if (fs.get(d).ftype != TDir)
                OLookup({ dir: d, name: n, status: ERR_NOTDIR, child: -1 })
            else if (hasEnt(d, n))
                OLookup({ dir: d, name: n, status: OK, child: fs.get(d).ents.get(n) })
            else
                OLookup({ dir: d, name: n, status: ERR_NOENT, child: -1 }),
    }

    // The dir argument is drawn from every live object, so LOOKUP with a
    // non-directory handle (-> NFS3ERR_NOTDIR) is exercised too.
    action lookup = {
        nondet d = oneOf(fs.keys())
        nondet n = oneOf(NAMES)
        applyLookup(d, n)
    }

    // ------------------------------------------------------------------
    // GETATTR (RFC 1813 3.3.1) -- the harness compares the returned attrs
    // against the post-state node, so the Op only needs the target.
    // ------------------------------------------------------------------

    action applyGetattr(o: Fid): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OGetattr({ obj: o, status: OK }),
    }

    action getattr = {
        nondet o = oneOf(fs.keys())
        applyGetattr(o)
    }

    // GETATTR through the handle of a fully removed object.  Immediately
    // after removal chimera returns NFS3ERR_NOENT (probed 2026-08-08), but
    // this is NOT reliably predictable over a long trace: memfs recycles an
    // inode's inum+gen once it is freed, so a later CREATE can hand out the
    // very handle a removed object held, and GETATTR then succeeds against
    // the new object.  Modeling that requires tracking memfs's exact
    // inum/gen allocation, which the abstract model deliberately avoids.
    // The action and Op variant are retained (the `removed` set still feeds
    // the RENAME/unlink logic) but staleGetattr is excluded from `step`;
    // stale-FH behaviour is deferred to a dedicated, allocation-aware
    // phase-3 probe.  Two facts worth pinning there: a syntactically bogus
    // handle returns NFS3ERR_STALE, whereas a validly-decoded-but-freed
    // handle returns NFS3ERR_NOENT (RFC 1813 would use STALE for both).
    action applyStaleGetattr(o: Fid): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OStaleGetattr({ obj: o, status: ERR_NOENT }),
    }

    // ------------------------------------------------------------------
    // SETATTR (RFC 1813 3.3.2) -- mode and/or size, with the ctime guard
    // modeled as a scenario: 0 = no guard, 1 = guard matching the live
    // ctime (harness fetches it), 2 = deliberately stale guard.
    // ------------------------------------------------------------------

    action applySetattr(o: Fid, m: int, sz: int, guard: int): bool = all {
        fs' =
            if (guard == 2) fs
            else fs.setBy(o, nd => {
                val nd1 = if (m >= 0) nd.with("mode", m) else nd
                // A size change updates mtime and clears the exclusive-create
                // verifier; a mode-only SETATTR touches ctime only and leaves
                // it intact (probed 2026-08-08).
                if (sz >= 0) nd1.with("data", resizeBlocks(nd1.data, sz)).with("xverf", 0)
                else nd1
            }),
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OSetattr({ obj: o, mode: m, sizeBlocks: sz, guard: guard,
                             status: if (guard == 2) ERR_NOT_SYNC else OK }),
    }

    action setattrMode = {
        nondet o = oneOf(fs.keys().filter(f =>
            fs.get(f).ftype == TReg or fs.get(f).ftype == TDir))
        nondet m = oneOf(MODES)
        nondet g = oneOf(Set(0, 1, 2))
        applySetattr(o, m, -1, g)
    }

    action setattrSize = all {
        files.size() > 0,
        {
            nondet f = oneOf(files)
            nondet sz = oneOf(0.to(MAX_BLOCKS))
            nondet g = oneOf(Set(0, 1, 2))
            applySetattr(f, -1, sz, g)
        },
    }

    // ------------------------------------------------------------------
    // ACCESS (RFC 1813 3.3.4) -- the harness always runs as root/AUTH_SYS.
    // chimera: root is granted every requested bit, including EXECUTE on a
    // file with no execute bits (probed 2026-08-08: mask 0x3f on a 0644
    // file returns 0x3f).  POSIX would withhold EXECUTE unless some x bit
    // is set; RFC 1813 leaves ACCESS semantics to the server, so this is
    // annotated rather than CANDIDATE-FIX.
    // ------------------------------------------------------------------

    action applyAccess(o: Fid, mask: int): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OAccess({ obj: o, mask: mask, status: OK, access: mask }),
    }

    action access = {
        nondet o = oneOf(fs.keys())
        nondet mask = oneOf(ACCESS_MASKS)
        applyAccess(o, mask)
    }

    // ------------------------------------------------------------------
    // CREATE (RFC 1813 3.3.8)
    // ------------------------------------------------------------------

    action applyCreate(d: Fid, n: Name, cm: CreateMode, m: int, c: Cred): bool =
        if (not(hasEnt(d, n)) and not(canModifyDir(fs.get(d), c))) all {
            // Creating a fresh entry needs write+search on the parent (XBD
            // 4.6 write, plus search to resolve).  A new file is owned by its
            // creator's credential.
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OCreate({ dir: d, name: n, cmode: cm, mode: m, verf: -1,
                                status: ERR_ACCES, obj: -1, cred: c }),
        } else if (hasEnt(d, n)) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' =
                if (cm == Guarded)
                    OCreate({ dir: d, name: n, cmode: cm, mode: m, verf: -1,
                              status: ERR_EXIST, obj: -1, cred: c })
                else
                    // UNCHECKED over an existing name succeeds and returns
                    // the existing object *of any type*, attributes
                    // untouched.  chimera: probed 2026-08-08 -- this holds
                    // even when the name is a directory or symlink (POSIX
                    // open(O_CREAT) would give EISDIR for a directory).
                    OCreate({ dir: d, name: n, cmode: cm, mode: m, verf: -1,
                              status: OK, obj: fs.get(d).ents.get(n), cred: c }),
        } else all {
            fs' = fs
                .put(nextFid, emptyFile(m, c))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, nextFid))),
            removed' = removed,
            nextFid' = nextFid + 1,
            lastOp' = OCreate({ dir: d, name: n, cmode: cm, mode: m, verf: -1,
                                status: OK, obj: nextFid, cred: c }),
        }

    action createCore = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        nondet cm = oneOf(Set(Unchecked, Guarded))
        nondet m = oneOf(MODES)
        nondet c = oneOf(CREDS)
        applyCreate(d, n, cm, m, c)
    }

    action create = all {
        fs.keys().size() < MAX_OBJECTS,
        createCore,
    }

    // EXCLUSIVE create (RFC 1813 3.3.8).  A fresh exclusive create succeeds
    // and the file materializes with mode 0644 (420, chimera-observed
    // 2026-08-08; the sattr in an exclusive create is the verifier, not a
    // mode).  RFC 1813: a retry that presents the *same* verifier as the
    // create in progress "should" succeed idempotently and return the same
    // object; a different verifier gives NFS3ERR_EXIST.  chimera returns
    // EXIST even for the same verifier -- recorded as deviation
    // `exclusive-create-retry-exist` (a status-only divergence: the file
    // exists and is unchanged under either outcome, so model state stays in
    // sync).
    action applyCreateExcl(d: Fid, n: Name, v: int): bool =
        if (hasEnt(d, n)) {
            val existing = fs.get(d).ents.get(n)
            val node = fs.get(existing)
            if (node.ftype == TReg and node.xverf == v and v != 0) all {
                // Same-verifier retry: RFC idempotent success, no change.
                fs' = fs,
                removed' = removed,
                nextFid' = nextFid,
                lastOp' = OCreate({ dir: d, name: n, cmode: Exclusive, mode: -1,
                                    verf: v, status: OK, obj: existing, cred: ROOT_CRED }),
            } else all {
                fs' = fs,
                removed' = removed,
                nextFid' = nextFid,
                lastOp' = OCreate({ dir: d, name: n, cmode: Exclusive, mode: -1,
                                    verf: v, status: ERR_EXIST, obj: -1, cred: ROOT_CRED }),
            }
        } else all {
            fs' = fs
                .put(nextFid, exclFile(420, v, ROOT_CRED))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, nextFid))),
            removed' = removed,
            nextFid' = nextFid + 1,
            lastOp' = OCreate({ dir: d, name: n, cmode: Exclusive, mode: -1,
                                verf: v, status: OK, obj: nextFid, cred: ROOT_CRED }),
        }

    action createExclCore = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        nondet v = oneOf(EXCL_VERFS)
        applyCreateExcl(d, n, v)
    }

    action createExcl = all {
        fs.keys().size() < MAX_OBJECTS,
        createExclCore,
    }

    // ------------------------------------------------------------------
    // MKDIR (RFC 1813 3.3.9)
    // ------------------------------------------------------------------

    action applyMkdir(d: Fid, n: Name, m: int): bool =
        if (hasEnt(d, n)) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OMkdir({ dir: d, name: n, mode: m, status: ERR_EXIST, obj: -1 }),
        } else all {
            fs' = fs
                .put(nextFid, emptyDir(m, ROOT_CRED))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, nextFid))),
            removed' = removed,
            nextFid' = nextFid + 1,
            lastOp' = OMkdir({ dir: d, name: n, mode: m, status: OK, obj: nextFid }),
        }

    action mkdirCore = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        nondet m = oneOf(MODES)
        applyMkdir(d, n, m)
    }

    action mkdir = all {
        fs.keys().size() < MAX_OBJECTS,
        mkdirCore,
    }

    // ------------------------------------------------------------------
    // SYMLINK (RFC 1813 3.3.10) / READLINK (3.3.5)
    // ------------------------------------------------------------------

    action applySymlink(d: Fid, n: Name, tgt: str): bool =
        if (hasEnt(d, n)) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OSymlink({ dir: d, name: n, target: tgt,
                                 status: ERR_EXIST, obj: -1 }),
        } else all {
            fs' = fs
                .put(nextFid, symlinkNode(tgt, ROOT_CRED))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, nextFid))),
            removed' = removed,
            nextFid' = nextFid + 1,
            lastOp' = OSymlink({ dir: d, name: n, target: tgt,
                                 status: OK, obj: nextFid }),
        }

    action symlinkCore = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        nondet tgt = oneOf(SYMLINK_TARGETS)
        applySymlink(d, n, tgt)
    }

    action symlink = all {
        fs.keys().size() < MAX_OBJECTS,
        symlinkCore,
    }

    action applyReadlink(l: Fid): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OReadlink({ obj: l, status: OK, target: fs.get(l).target }),
    }

    action readlink = all {
        symlinks.size() > 0,
        {
            nondet l = oneOf(symlinks)
            applyReadlink(l)
        },
    }

    // ------------------------------------------------------------------
    // MKNOD (RFC 1813 3.3.11) -- FIFO and SOCK only (BLK/CHR need device
    // numbers and privileges; phase 3 if ever).
    // ------------------------------------------------------------------

    action applyMknod(d: Fid, n: Name, ft: Ftype, m: int): bool =
        if (hasEnt(d, n)) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OMknod({ dir: d, name: n, ftype: ft, mode: m,
                               status: ERR_EXIST, obj: -1 }),
        } else all {
            fs' = fs
                .put(nextFid, nodeOf(ft, m, ROOT_CRED))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, nextFid))),
            removed' = removed,
            nextFid' = nextFid + 1,
            lastOp' = OMknod({ dir: d, name: n, ftype: ft, mode: m,
                               status: OK, obj: nextFid }),
        }

    action mknodCore = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        nondet ft = oneOf(Set(TFifo, TSock))
        nondet m = oneOf(MODES)
        applyMknod(d, n, ft, m)
    }

    action mknod = all {
        fs.keys().size() < MAX_OBJECTS,
        mknodCore,
    }

    // ------------------------------------------------------------------
    // WRITE (RFC 1813 3.3.7)
    // ------------------------------------------------------------------

    action applyWrite(f: Fid, off: int, len: int, pat: int, stable: int, c: Cred): bool =
        if (not(canWrite(fs.get(f), c))) all {
            // WRITE needs write permission on the file (XBD 4.6).  The owner
            // gets it unconditionally under owner-override.
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OWrite({ file: f, offset: off, count: len, pat: pat,
                               stable: stable, status: ERR_ACCES,
                               newSizeBlocks: fs.get(f).data.length(), cred: c }),
        } else all {
            // A WRITE changes mtime, which clears the stashed exclusive-create
            // verifier (probed 2026-08-08): a later same-verifier CREATE is then
            // a fresh request, not an idempotent retransmit.
            fs' = fs.setBy(f, fn =>
                fn.with("data", writeBlocks(fn.data, off, len, pat)).with("xverf", 0)),
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OWrite({
                file: f, offset: off, count: len, pat: pat, stable: stable,
                status: OK,
                newSizeBlocks: imax(fs.get(f).data.length(), off + len), cred: c
            }),
        }

    action writeCore = {
        nondet f = oneOf(files)
        nondet off = oneOf(0.to(MAX_BLOCKS - 1))
        nondet len = oneOf(1.to(2))
        nondet pat = oneOf(BLOCK_SYMS)
        nondet stable = oneOf(STABLE_LEVELS)
        nondet c = oneOf(CREDS)
        applyWrite(f, off, imin(len, MAX_BLOCKS - off), pat, stable, c)
    }

    action write = all {
        files.size() > 0,
        writeCore,
    }

    // ------------------------------------------------------------------
    // READ (RFC 1813 3.3.6)
    // ------------------------------------------------------------------

    action applyRead(f: Fid, off: int, len: int, c: Cred): bool =
        if (not(canRead(fs.get(f), c))) all {
            // READ needs read permission on the file; the owner gets it
            // unconditionally under owner-override.
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = ORead({ file: f, offset: off, count: len,
                              status: ERR_ACCES, retCount: 0, eof: false,
                              blocks: [], cred: c }),
        } else all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = {
                val sz = fs.get(f).data.length()
                val ret = if (off >= sz) 0 else imin(len, sz - off)
                ORead({
                    file: f, offset: off, count: len,
                    status: OK,
                    retCount: ret,
                    eof: off + ret >= sz,
                    blocks: if (ret == 0) [] else fs.get(f).data.slice(off, off + ret),
                    cred: c
                })
            },
        }

    action readCore = {
        nondet f = oneOf(files)
        nondet off = oneOf(0.to(MAX_BLOCKS))
        nondet len = oneOf(1.to(2))
        nondet c = oneOf(CREDS)
        applyRead(f, off, len, c)
    }

    action read = all {
        files.size() > 0,
        readCore,
    }

    // ------------------------------------------------------------------
    // COMMIT (RFC 1813 3.3.21) -- whole-file commit; the harness checks
    // the verifier matches the WRITE verifier for the run.
    // ------------------------------------------------------------------

    action applyCommit(f: Fid): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OCommit({ file: f, status: OK }),
    }

    action commit = all {
        files.size() > 0,
        {
            nondet f = oneOf(files)
            applyCommit(f)
        },
    }

    // ------------------------------------------------------------------
    // REMOVE (RFC 1813 3.3.12) -- non-directories only (unlink(2) semantics).
    //
    // chimera enforces the type: the NFS3 REMOVE proc passes
    // CHIMERA_VFS_REMOVE_ISNOTDIR to the VFS, so REMOVE of a directory is
    // NFS3ERR_ISDIR (matching POSIX unlink -> EISDIR), whether the directory
    // is empty or not.  A name over a multi-link file just drops that link.
    // ------------------------------------------------------------------

    def unlinkOutcome(d: Fid, n: Name): { fs2: Fid -> Node, rm2: Set[Fid], st: int } =
        if (not(hasEnt(d, n)))
            { fs2: fs, rm2: Set(), st: ERR_NOENT }
        else {
            val victim = fs.get(d).ents.get(n)
            if (fs.get(victim).ftype == TDir)
                { fs2: fs, rm2: Set(), st: ERR_ISDIR }
            else {
                val dies = fs.get(victim).nlink == 1
                val fsA = fs.setBy(d, dn => dn.with("ents", mapDrop(dn.ents, n)))
                if (dies)
                    { fs2: fsA.mapDrop(victim), rm2: Set(victim), st: OK }
                else
                    { fs2: fsA.setBy(victim, vn => vn.with("nlink", vn.nlink - 1)),
                      rm2: Set(), st: OK }
            }
        }

    action applyRemove(d: Fid, n: Name): bool = {
        val o = unlinkOutcome(d, n)
        all {
            fs' = o.fs2,
            removed' = removed.union(o.rm2),
            nextFid' = nextFid,
            lastOp' = ORemove({ dir: d, name: n, status: o.st }),
        }
    }

    action remove = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        applyRemove(d, n)
    }

    // ------------------------------------------------------------------
    // RMDIR (RFC 1813 3.3.13) -- directories only (rmdir(2) semantics).
    //
    // chimera enforces the type: the NFS3 RMDIR proc passes
    // CHIMERA_VFS_REMOVE_ISDIR to the VFS, so RMDIR of a non-directory is
    // NFS3ERR_NOTDIR.  A non-empty directory is NFS3ERR_NOTEMPTY.  Because
    // the type check is now enforced (no state change on a type mismatch),
    // RMDIR of any name is safe to generate -- server and model agree.
    // ------------------------------------------------------------------

    def rmdirOutcome(d: Fid, n: Name): { fs2: Fid -> Node, rm2: Set[Fid], st: int } =
        if (not(hasEnt(d, n)))
            { fs2: fs, rm2: Set(), st: ERR_NOENT }
        else {
            val victim = fs.get(d).ents.get(n)
            if (fs.get(victim).ftype != TDir)
                { fs2: fs, rm2: Set(), st: ERR_NOTDIR }
            else if (fs.get(victim).ents.keys().size() > 0)
                { fs2: fs, rm2: Set(), st: ERR_NOTEMPTY }
            else
                { fs2: fs.setBy(d, dn => dn.with("ents", mapDrop(dn.ents, n)))
                          .mapDrop(victim),
                  rm2: Set(victim), st: OK }
        }

    action applyRmdir(d: Fid, n: Name): bool = {
        val o = rmdirOutcome(d, n)
        all {
            fs' = o.fs2,
            removed' = removed.union(o.rm2),
            nextFid' = nextFid,
            lastOp' = ORmdir({ dir: d, name: n, status: o.st }),
        }
    }

    action rmdir = {
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        applyRmdir(d, n)
    }

    // ------------------------------------------------------------------
    // RENAME (RFC 1813 3.3.14) -- full POSIX matrix; chimera conforms on
    // every probed case (replace, ISDIR/NOTDIR mismatches, NOTEMPTY,
    // subtree cycles -> INVAL, self-rename and hardlink-pair no-ops).
    // ------------------------------------------------------------------

    def renameOutcome(sd: Fid, sn: Name, td: Fid, tn: Name):
            { fs2: Fid -> Node, rm2: Set[Fid], st: int } =
        if (not(hasEnt(sd, sn)))
            { fs2: fs, rm2: Set(), st: ERR_NOENT }
        else {
            val src = fs.get(sd).ents.get(sn)
            if ((sd == td and sn == tn) or
                    (hasEnt(td, tn) and fs.get(td).ents.get(tn) == src))
                // Renaming an entry onto itself, or onto another name for
                // the same object (hardlink pair): POSIX no-op, both names
                // remain.
                { fs2: fs, rm2: Set(), st: OK }
            else if (fs.get(src).ftype == TDir and descOf(src).contains(td))
                { fs2: fs, rm2: Set(), st: ERR_INVAL }
            else if (hasEnt(td, tn)) {
                val tgt = fs.get(td).ents.get(tn)
                if (fs.get(src).ftype == TDir and fs.get(tgt).ftype != TDir)
                    { fs2: fs, rm2: Set(), st: ERR_NOTDIR }
                else if (fs.get(src).ftype != TDir and fs.get(tgt).ftype == TDir)
                    { fs2: fs, rm2: Set(), st: ERR_ISDIR }
                else if (fs.get(tgt).ftype == TDir and
                         fs.get(tgt).ents.keys().size() > 0)
                    { fs2: fs, rm2: Set(), st: ERR_NOTEMPTY }
                else {
                    val tgtDies = fs.get(tgt).ftype == TDir or fs.get(tgt).nlink == 1
                    val fsA = if (tgtDies) fs.mapDrop(tgt)
                              else fs.setBy(tgt, x => x.with("nlink", x.nlink - 1))
                    val fsB = fsA.setBy(sd, dn => dn.with("ents", mapDrop(dn.ents, sn)))
                    val fsC = fsB.setBy(td, dn => dn.with("ents", dn.ents.put(tn, src)))
                    { fs2: fsC, rm2: if (tgtDies) Set(tgt) else Set(), st: OK }
                }
            } else {
                val fsB = fs.setBy(sd, dn => dn.with("ents", mapDrop(dn.ents, sn)))
                val fsC = fsB.setBy(td, dn => dn.with("ents", dn.ents.put(tn, src)))
                { fs2: fsC, rm2: Set(), st: OK }
            }
        }

    action applyRename(sd: Fid, sn: Name, td: Fid, tn: Name): bool = {
        val o = renameOutcome(sd, sn, td, tn)
        all {
            fs' = o.fs2,
            removed' = removed.union(o.rm2),
            nextFid' = nextFid,
            lastOp' = ORename({ fromDir: sd, fromName: sn, toDir: td, toName: tn,
                                status: o.st }),
        }
    }

    action rename = {
        nondet sd = oneOf(dirs)
        nondet sn = oneOf(NAMES)
        nondet td = oneOf(dirs)
        nondet tn = oneOf(NAMES)
        applyRename(sd, sn, td, tn)
    }

    // ------------------------------------------------------------------
    // LINK (RFC 1813 3.3.15) -- non-directories only; linking a directory
    // fails with NFS3ERR_ISDIR (probed 2026-08-08; RFC 1813 does not list
    // ISDIR for LINK but the choice matches POSIX intent).
    // ------------------------------------------------------------------

    action applyLink(o: Fid, d: Fid, n: Name): bool =
        // chimera: the directory-target check precedes the name-collision
        // check (memfs.c:4855 S_ISDIR before :4867 EEXIST), so linking a
        // directory is ISDIR even when the target name is already taken.
        if (fs.get(o).ftype == TDir) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OLink({ obj: o, dir: d, name: n, status: ERR_ISDIR }),
        } else if (hasEnt(d, n)) all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OLink({ obj: o, dir: d, name: n, status: ERR_EXIST }),
        } else all {
            fs' = fs
                .setBy(o, x => x.with("nlink", x.nlink + 1))
                .setBy(d, dn => dn.with("ents", dn.ents.put(n, o))),
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OLink({ obj: o, dir: d, name: n, status: OK }),
        }

    action link = {
        nondet o = oneOf(fs.keys())
        nondet d = oneOf(dirs)
        nondet n = oneOf(NAMES)
        applyLink(o, d, n)
    }

    // ------------------------------------------------------------------
    // READDIR / READDIRPLUS (RFC 1813 3.3.16/3.3.17) -- single-shot with a
    // large count; the harness compares the returned entry set against the
    // model's names plus "." and ".." (chimera emits both, probed
    // 2026-08-08), checks per-entry fileids for consistency, and for the
    // PLUS variant learns/verifies per-entry file handles.  Multi-request
    // cookie iteration is phase 3.
    // ------------------------------------------------------------------

    // RFC 1813 3.3.16/3.3.17: READDIR/READDIRPLUS on a non-directory is
    // NFS3ERR_NOTDIR.  chimera deviates for a symlink handle (SERVERFAULT);
    // recorded as `readdir-symlink-serverfault` in the nfs3_mbt_replay.c deviation registry.
    action applyReaddir(o: Fid, plus: bool): bool = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' =
            if (fs.get(o).ftype != TDir)
                OReaddir({ dir: o, plus: plus, status: ERR_NOTDIR, names: Set() })
            else
                OReaddir({ dir: o, plus: plus, status: OK,
                           names: fs.get(o).ents.keys() }),
    }

    action readdir = {
        nondet o = oneOf(fs.keys())
        nondet plus = oneOf(Set(false, true))
        applyReaddir(o, plus)
    }

    // ------------------------------------------------------------------
    // FSSTAT / FSINFO / PATHCONF (RFC 1813 3.3.18/3.3.19/3.3.20) -- the
    // interesting reply fields are constants of the server/backend; the
    // expected values live in nfs3_mbt_replay.c with citations.  PATHCONF accepts
    // any live object handle.
    // ------------------------------------------------------------------

    action fsstat = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OFsstat({ status: OK }),
    }

    action fsinfo = all {
        fs' = fs,
        removed' = removed,
        nextFid' = nextFid,
        lastOp' = OFsinfo({ status: OK }),
    }

    action pathconf = {
        nondet o = oneOf(fs.keys())
        all {
            fs' = fs,
            removed' = removed,
            nextFid' = nextFid,
            lastOp' = OPathconf({ obj: o, status: OK }),
        }
    }

    // ------------------------------------------------------------------

    action step = any {
        lookup,
        getattr,
        setattrMode,
        setattrSize,
        access,
        create,
        createExcl,
        mkdir,
        symlink,
        readlink,
        mknod,
        write,
        read,
        commit,
        remove,
        rmdir,
        rename,
        link,
        readdir,
        fsstat,
        fsinfo,
        pathconf,
    }

    // Biased step distributions used to generate additional trace flavors
    // (quint run --step=<name>).  `any` weights by duplicate branches, so
    // listing an action N times gives it ~N x the odds among enabled
    // branches; lookup/getattr are always enabled and act as the liveness
    // fallback.  These lift buckets that uniform stepping starves (measured
    // by coverage.py): successful rmdir, directory-into-empty-directory
    // rename, cross-parent directory moves, and idempotent exclusive-create
    // retries.

    // Namespace churn: heavy create/unlink/rename/link over all object types.
    action stepChurn = any {
        create, create, mkdir, mkdir, symlink, mknod,
        remove, remove, rmdir, rmdir, rename, rename, rename, link, link,
        lookup, getattr,
    }

    // Directory-heavy: many directories, few files inside them, so directories
    // stay empty and become rmdir / rename-replace targets.
    action stepDir = any {
        mkdir, mkdir, mkdir, rmdir, rmdir, rmdir, rename, rename, rename,
        lookup, getattr,
    }

    // Exclusive-create heavy with NO writes, so a file's stashed verifier
    // survives and same-verifier retries (idempotent OK) are exercised.
    action stepExcl = any {
        createExcl, createExcl, createExcl, createExcl,
        read, getattr, lookup, remove,
    }

    // Structural sanity invariants over the model itself (checked during
    // simulation; cheap and catch model bugs early).

    // Every directory entry points at a live object.
    val entsLive = dirs.forall(d =>
        fs.get(d).ents.keys().forall(n => fs.keys().contains(fs.get(d).ents.get(n))))

    // Removed fids never resurface.
    val removedStayDead = removed.intersect(fs.keys()) == Set()

    // A non-directory's nlink equals the number of directory entries that
    // reference it.
    val nlinkConsistent = nonDirs.forall(f =>
        fs.get(f).nlink == dirs.fold(0, (acc, d) =>
            acc + fs.get(d).ents.keys().filter(n => fs.get(d).ents.get(n) == f).size()))

    // The root is never removed and stays a directory.
    val rootAlive = fs.keys().contains(ROOT) and fs.get(ROOT).ftype == TDir

    val inv = entsLive and removedStayDead and nlinkConsistent and rootAlive
}
