// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Runnable instances of the POSIX model.
//
//   posixDefault      every feature and policy explored both ways; the
//                     harness reconciles each trace's drawn profile
//                     against the live implementation and skips
//                     non-applicable traces
//   posixStrict       POSIX-canonical behavior forced: optional features
//                     mandatory, divergence knobs pinned to the standard
//                     reading (pwrite ignores O_APPEND, unlink(dir) is
//                     EPERM, ...); an implementation that quietly drops a
//                     feature fails instead of passing vacuously
//   posixLinuxish     the common Linux deviations forced on, for testing
//                     against Linux-behaving backends
//
// Simulate:  quint run posix_run.qnt --main=posixDefault --invariant=inv

module posixDefault {
    import posix(
        F_COPY_RANGE = FeatIfAdvertised,
        F_CLONE_RANGE = FeatIfAdvertised,
        F_SEEK_HOLE = FeatIfAdvertised,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatIfAdvertised,
        P_SGID_INHERIT = FeatIfAdvertised,
        P_WRITE_CLEARS_SETIDS = FeatIfAdvertised,
        P_PWRITE_APPENDS = FeatIfAdvertised,
        P_RENAME_CTIME = FeatIfAdvertised,
        P_STRICT_ATIME = FeatIfAdvertised,
        P_STICKY_WRITE_ARM = FeatIfAdvertised,
        P_ERR_NOTEMPTY = FeatIfAdvertised,
        P_ERR_STICKY_ACCES = FeatIfAdvertised,
        P_ERR_UNLINKDIR_ISDIR = FeatIfAdvertised,
        P_ERR_LOCK_AGAIN = FeatIfAdvertised
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}

// The chimera-memfs profile, pinned to the behavior measured by
// `posix_replay.py --probe` on 2026-08-10 (see PROFILE there; the probe
// re-measures and `--check-profile` fails on drift).  Trace generation from
// this instance produces only profiles the live backend can satisfy, so the
// harness never skips a trace for profile mismatch.  F_ROOT stays
// explorable: the harness chooses the credentials, so both draws are real.
// P_ERR_LOCK_AGAIN is unobservable while memfs lacks record locks (PD1 in
// posix_deviations.py); pinned to the Linux-style EAGAIN reading.
module posixMemfs {
    import posix(
        F_COPY_RANGE = FeatMandatory,
        F_CLONE_RANGE = FeatMandatory,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatMandatory,
        P_STRICT_ATIME = FeatOff,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatMandatory,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}

// The chimera-diskfs/cairn profile, pinned to `--probe --backend diskfs`
// and `--backend cairn` measurements on 2026-08-11: both probe identically
// to memfs except clone_range is unsupported (F_CLONE_RANGE off).  One
// instance therefore serves trace generation for both backends.
module posixDisk {
    import posix(
        F_COPY_RANGE = FeatMandatory,
        F_CLONE_RANGE = FeatOff,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatMandatory,
        P_STRICT_ATIME = FeatOff,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatMandatory,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}

// The chimera NFS3-loopback profile (client -> in-process server ->
// backend), pinned to `--probe --backend nfs3_{memfs,diskfs,cairn}` on
// 2026-08-12: all three backends probe identically behind the server, so one
// instance serves the three.  No copy/clone offload (the VFS copy fallback
// refuses proxy handles) and no SEEK_DATA/SEEK_HOLE in the NFS3 protocol;
// atime propagates strictly (reads refresh it through the wire attributes).
module posixNfs3 {
    import posix(
        F_COPY_RANGE = FeatOff,
        F_CLONE_RANGE = FeatOff,
        F_SEEK_HOLE = FeatOff,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatMandatory,
        P_STRICT_ATIME = FeatMandatory,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatMandatory,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}

// The chimera NFS4-loopback profile, pinned the same day: again identical
// across the three backends.  NFS4.2 SEEK backs SEEK_DATA/SEEK_HOLE; atime
// is not strictly observed.  unlink(dir) now yields EISDIR
// (P_ERR_UNLINKDIR_ISDIR mandatory, matching nfs3 and the engine backends):
// the generic remove path enforces the rmdir/unlink type assertion for every
// backend (vfs_proc_remove.c), so the nfs4 client's type-agnostic bare REMOVE
// no longer removes a directory.
module posixNfs4 {
    import posix(
        F_COPY_RANGE = FeatOff,
        F_CLONE_RANGE = FeatOff,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatOff,
        P_SGID_INHERIT = FeatOff,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatOff,
        P_RENAME_CTIME = FeatMandatory,
        P_STRICT_ATIME = FeatOff,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatMandatory,
        P_ERR_UNLINKDIR_ISDIR = FeatMandatory,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}

module posixStrict {
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
    import posixops.* from "./posix_ops"
}

module posixLinuxish {
    import posix(
        F_COPY_RANGE = FeatMandatory,
        F_CLONE_RANGE = FeatIfAdvertised,
        F_SEEK_HOLE = FeatMandatory,
        F_ROOT = FeatIfAdvertised,
        P_GID_FROM_PARENT = FeatIfAdvertised,
        P_SGID_INHERIT = FeatIfAdvertised,
        P_WRITE_CLEARS_SETIDS = FeatMandatory,
        P_PWRITE_APPENDS = FeatMandatory,
        P_RENAME_CTIME = FeatMandatory,
        P_STRICT_ATIME = FeatOff,
        P_STICKY_WRITE_ARM = FeatOff,
        P_ERR_NOTEMPTY = FeatMandatory,
        P_ERR_STICKY_ACCES = FeatOff,
        P_ERR_UNLINKDIR_ISDIR = FeatMandatory,
        P_ERR_LOCK_AGAIN = FeatMandatory
    ).* from "./posix"
    import posixops.* from "./posix_ops"
}
