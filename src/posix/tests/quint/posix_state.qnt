// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// POSIX process-visible kernel state: file descriptors, open file
// descriptions, record locks, directory streams, per-process credentials
// and umask.
//
// The two-level descriptor model is POSIX XBD's: a file DESCRIPTOR is a
// per-process integer naming an open file DESCRIPTION; dup/dup2 create new
// descriptors sharing one description, so the file offset and status flags
// (O_APPEND) are shared through them.  Independent open() calls create
// independent descriptions.  This is a classic NAS-client compliance trap:
// implementations that keep the offset per-descriptor pass most tests and
// break dup semantics.
//
// Record locks follow fcntl(): the lock OWNER is the process (not the
// descriptor), one lock type per byte per (process, file), new locks
// replace/merge the owner's old ones over the covered bytes, and -- the
// second classic trap -- closing ANY descriptor for a file drops ALL of
// the process's locks on that file, even if other descriptors remain open.
// Byte-range arithmetic (merge, split, exact ranges) falls out of modeling
// each owner's locks as a per-byte map, the same trick as the NFSv4 model.
//
// Directory streams are modeled as pins on an inode; readdir is exercised
// as an atomic full sweep (see DESIGN-POSIX.md), so the unspecified window
// POSIX leaves for concurrently-modified streams never opens inside a
// single model step.
//
// No fork/inheritance: processes exist from init with fixed credentials.
// The model has no policy here -- permission checks live in posix_ops.qnt.

module posixstate {

    import posixfs.* from "./posix_fs"

    type Pid = int
    type Fd = int
    type OfdId = int
    type StreamId = int

    // Access mode of an open file description (O_ACCMODE).
    type AccMode = AccR | AccW | AccRW

    // Credentials: effective ids (used by almost everything), real ids
    // (used by access()), supplementary groups.
    type Cred = {
        euid: int,
        egid: int,
        ruid: int,
        rgid: int,
        groups: Set[int]
    }

    // Open file description, shared by every descriptor dup'd from the
    // same open().  The offset is in abstract blocks.
    type Ofd = {
        ino: Ino,
        offset: int,
        acc: AccMode,
        appendF: bool
    }

    type LockT = LRead | LWrite

    type DirStream = {
        pid: Pid,
        ino: Ino
    }

    type Pstate = {
        // Descriptor tables: (pid, fd) -> open file description id.
        fds: (Pid, Fd) -> OfdId,
        ofds: OfdId -> Ofd,
        nextOfd: OfdId,
        // Record locks: (pid, ino) -> per-byte lock map.  Absent key or an
        // empty byte map means no locks.  Byte indices are abstract; a
        // whole-file lock (l_len = 0) covers [0, LOCK_BYTES) of the
        // universe, which the harness maps to an unbounded wire range.
        locks: (Pid, Ino) -> (int -> LockT),
        streams: StreamId -> DirStream,
        nextStream: StreamId,
        umask: Pid -> int,
        cred: Pid -> Cred
    }

    pure def psInit(creds: Pid -> Cred): Pstate = {
        fds: Map(),
        ofds: Map(),
        nextOfd: 0,
        locks: Map(),
        streams: Map(),
        nextStream: 0,
        umask: creds.keys().mapBy(p => 0),
        cred: creds
    }

    // ------------------------------------------------------------------
    // Descriptor-table queries
    // ------------------------------------------------------------------

    pure def fdLive(ps: Pstate, p: Pid, fd: Fd): bool =
        ps.fds.keys().contains((p, fd))

    pure def fdOfd(ps: Pstate, p: Pid, fd: Fd): Ofd =
        ps.ofds.get(ps.fds.get((p, fd)))

    pure def fdsOf(ps: Pstate, p: Pid): Set[Fd] =
        ps.fds.keys().filter(k => k._1 == p).map(k => k._2)

    // POSIX guarantees the lowest available descriptor (open, dup) or the
    // lowest >= arg (F_DUPFD).  maxFds bounds the search.
    pure def fdLowest(ps: Pstate, p: Pid, atLeast: Fd, maxFds: int): Fd =
        range(atLeast, maxFds).foldl(-1, (acc, n) =>
            if (acc >= 0) acc
            else if (fdLive(ps, p, n)) acc
            else n)

    // Descriptors (across all processes) sharing an open file description.
    pure def ofdRefs(ps: Pstate, o: OfdId): Set[(Pid, Fd)] =
        ps.fds.keys().filter(k => ps.fds.get(k) == o)

    // Inodes pinned against reaping: open descriptions and dir streams.
    pure def psPinned(ps: Pstate): Set[Ino] =
        ps.ofds.keys().map(o => ps.ofds.get(o).ino)
            .union(ps.streams.keys().map(s => ps.streams.get(s).ino))

    // ------------------------------------------------------------------
    // Open / close / dup
    // ------------------------------------------------------------------

    // Install a fresh open file description for (p, fd).
    pure def psOpen(ps: Pstate, p: Pid, fd: Fd, i: Ino, acc: AccMode,
                    appendF: bool): Pstate =
        ps.with("fds", ps.fds.put((p, fd), ps.nextOfd))
          .with("ofds", ps.ofds.put(ps.nextOfd,
              { ino: i, offset: 0, acc: acc, appendF: appendF }))
          .with("nextOfd", ps.nextOfd + 1)

    // Point (p, fd) at an existing description (dup/dup2/F_DUPFD).
    pure def psDup(ps: Pstate, p: Pid, fd: Fd, o: OfdId): Pstate =
        ps.with("fds", ps.fds.put((p, fd), o))

    // close(): remove the descriptor; drop ALL of p's record locks on the
    // underlying file (POSIX close page -- triggered by any descriptor for
    // the file, not just the last); free the description when its last
    // descriptor goes.  Reaping of orphaned inodes is coordinated by the
    // POSIX layer, which consults psPinned afterwards.
    pure def psClose(ps: Pstate, p: Pid, fd: Fd): Pstate = {
        val o = ps.fds.get((p, fd))
        val i = ps.ofds.get(o).ino
        val ps1 = ps
            .with("fds", mapDrop(ps.fds, (p, fd)))
            .with("locks", if (ps.locks.keys().contains((p, i)))
                               mapDrop(ps.locks, (p, i))
                           else ps.locks)
        if (ofdRefs(ps1, o).size() == 0)
            ps1.with("ofds", mapDrop(ps1.ofds, o))
        else ps1
    }

    // ------------------------------------------------------------------
    // Offsets and status flags
    // ------------------------------------------------------------------

    pure def psSetOffset(ps: Pstate, p: Pid, fd: Fd, off: int): Pstate =
        ps.with("ofds", ps.ofds.setBy(ps.fds.get((p, fd)), x =>
            x.with("offset", off)))

    pure def psSetAppend(ps: Pstate, p: Pid, fd: Fd, a: bool): Pstate =
        ps.with("ofds", ps.ofds.setBy(ps.fds.get((p, fd)), x =>
            x.with("appendF", a)))

    // ------------------------------------------------------------------
    // Record locks
    // ------------------------------------------------------------------

    pure def lockBytes(ps: Pstate, p: Pid, i: Ino): int -> LockT =
        if (ps.locks.keys().contains((p, i))) ps.locks.get((p, i)) else Map()

    // Does a lock of type t over [lo, hi) by p conflict with another
    // process's locks?  Read locks share; anything involving a write lock
    // conflicts.  A process never conflicts with itself (its own locks are
    // replaced).
    pure def lockConflict(ps: Pstate, p: Pid, i: Ino, lo: int, hi: int,
                          t: LockT): bool =
        ps.locks.keys().exists(k =>
            k._2 == i and k._1 != p and
            range(lo, hi).foldl(false, (acc, b) => acc or {
                val m = ps.locks.get(k)
                m.keys().contains(b) and
                    (t == LWrite or m.get(b) == LWrite)
            }))

    // The set of processes owning a conflicting lock (F_GETLK reporting).
    pure def lockConflictOwners(ps: Pstate, p: Pid, i: Ino, lo: int, hi: int,
                                t: LockT): Set[Pid] =
        ps.locks.keys().filter(k =>
            k._2 == i and k._1 != p and
            range(lo, hi).foldl(false, (acc, b) => acc or {
                val m = ps.locks.get(k)
                m.keys().contains(b) and
                    (t == LWrite or m.get(b) == LWrite)
            })).map(k => k._1)

    // Set p's lock over [lo, hi) to t, replacing whatever p held on those
    // bytes (POSIX: the new lock replaces the owner's old locks over the
    // covered region; merge and split fall out of the byte map).  The
    // caller has already checked lockConflict.
    pure def lockSet(ps: Pstate, p: Pid, i: Ino, lo: int, hi: int,
                     t: LockT): Pstate = {
        val nb = range(lo, hi).foldl(lockBytes(ps, p, i), (acc, b) =>
            acc.put(b, t))
        ps.with("locks", ps.locks.put((p, i), nb))
    }

    // Remove p's locks over [lo, hi) (F_UNLCK); splitting a larger lock
    // into two remnants falls out of the byte map.
    pure def lockUnset(ps: Pstate, p: Pid, i: Ino, lo: int, hi: int): Pstate = {
        val old = lockBytes(ps, p, i)
        val nb = old.keys().filter(b => b < lo or b >= hi)
                    .mapBy(b => old.get(b))
        if (nb.keys().size() == 0)
            ps.with("locks", if (ps.locks.keys().contains((p, i)))
                                 mapDrop(ps.locks, (p, i))
                             else ps.locks)
        else
            ps.with("locks", ps.locks.put((p, i), nb))
    }

    // ------------------------------------------------------------------
    // Directory streams
    // ------------------------------------------------------------------

    pure def psOpendir(ps: Pstate, p: Pid, i: Ino):
            { ps2: Pstate, sid: StreamId } =
        { ps2: ps.with("streams", ps.streams.put(ps.nextStream,
              { pid: p, ino: i }))
              .with("nextStream", ps.nextStream + 1),
          sid: ps.nextStream }

    pure def psClosedir(ps: Pstate, sid: StreamId): Pstate =
        ps.with("streams", mapDrop(ps.streams, sid))

    // ------------------------------------------------------------------
    // Credentials / umask
    // ------------------------------------------------------------------

    pure def psCred(ps: Pstate, p: Pid): Cred = ps.cred.get(p)

    pure def psSetUmask(ps: Pstate, p: Pid, m: int): Pstate =
        ps.with("umask", ps.umask.set(p, m))

    // ------------------------------------------------------------------
    // Structural invariants
    // ------------------------------------------------------------------

    // Every descriptor points at a live description; every description is
    // referenced by at least one descriptor.
    pure def psFdsConsistent(ps: Pstate): bool =
        ps.fds.keys().forall(k => ps.ofds.keys().contains(ps.fds.get(k))) and
        ps.ofds.keys().forall(o => ofdRefs(ps, o).size() > 0)

    // Offsets never go negative.
    pure def psOffsetsSane(ps: Pstate): bool =
        ps.ofds.keys().forall(o => ps.ofds.get(o).offset >= 0)

    // Lock tables hold no empty byte maps and no negative bytes, and a
    // process only holds locks on files it could have opened (liveness of
    // the ino is checked against the fs by the top-level module).
    pure def psLocksSane(ps: Pstate): bool =
        ps.locks.keys().forall(k => {
            val m = ps.locks.get(k)
            m.keys().size() > 0 and m.keys().forall(b => b >= 0)
        })

    pure def psInv(ps: Pstate): bool =
        psFdsConsistent(ps) and psOffsetsSane(ps) and psLocksSane(ps)
}
