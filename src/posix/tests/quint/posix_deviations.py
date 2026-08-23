# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Registry of known chimera deviations from POSIX.1-2024.

The Quint model (posix.qnt and friends) always encodes the standard's
behavior (with implementation choices pinned in the trace's capability
profile).  Where chimera's POSIX client is known to diverge, the divergence
is recorded here rather than baked into the model, exactly as in the NFS3
suite (src/server/nfs/tests/quint/deviations.py):

  * the POSIX target stays visible -- when chimera is fixed, the model's
    expectation already matches and the test goes green with no edit;
  * every non-conformance is one enumerable line item with a citation,
    root cause, and candidate fix (see also DEVIATIONS-POSIX.md);
  * the replay harness can tell a *known* deviation (record as xfail, keep
    replaying) from an unexpected one (hard failure).

Only *status-only* deviations are reconcilable: ones where the client's
state after the diverging reply still matches the model's, so replay can
continue in sync.  State-mutating deviations (chimera changes file or
descriptor state differently than POSIX requires) would desync the model;
they are listed with reconcilable=False for documentation and surface as
hard divergences in any trace that crosses them.
"""

from dataclasses import dataclass, field
from typing import Callable, Optional

# Linux errno values referenced below.
OK = 0
EPERM = 1
ENOENT = 2
EBADF = 9
EACCES = 13
EEXIST = 17
EISDIR = 21
EINVAL = 22
EMFILE = 24
ENAMETOOLONG = 36
ENOSYS = 38
EOPNOTSUPP = 95


@dataclass(frozen=True)
class Deviation:
    id: str
    posix: str               # POSIX.1-2024 citation (page / XBD section)
    summary: str
    root_cause: str          # source location
    candidate_fix: str
    # Trace reconciliation (status-only deviations):
    ops: tuple = ()                     # lastOp req tags this applies to
    expected_status: Optional[int] = None   # None = any expected value
    actual_status: Optional[int] = None
    # Extra guard on (req_value, post_fs) -> bool; default always-true.
    context: Callable = field(default=lambda op, fs: True)
    reconcilable: bool = True           # False => documentation-only


KNOWN_DEVIATIONS = [
    Deviation(
        id="PD2",
        posix="fcntl() F_DUPFD/F_GETFL/F_SETFL (System Interfaces)",
        summary="chimera_posix_fcntl implements only the record-lock "
                "commands; F_DUPFD, F_GETFL and F_SETFL fail EINVAL",
        root_cause="src/posix/posix_fcntl.c (switch on cmd, default EINVAL)",
        candidate_fix="add F_DUPFD (fd table alloc-at-least), F_GETFL "
                      "(return oflags) and F_SETFL (update O_APPEND) paths",
        ops=("RFcntlDupfd", "RFcntlGetfl"),
        expected_status=None,
        actual_status=EINVAL,
        reconcilable=True,
    ),
    Deviation(
        id="PD2b",
        posix="fcntl() F_SETFL (System Interfaces)",
        summary="F_SETFL fails EINVAL (same root cause as PD2); NOTE this "
                "one mutates model state (the description's O_APPEND flag), "
                "so replay desyncs if a later write depends on it",
        root_cause="src/posix/posix_fcntl.c",
        candidate_fix="see PD2",
        ops=("RFcntlSetfl",),
        expected_status=None,
        actual_status=EINVAL,
        # Reconciled at the fcntl itself; a subsequent append-dependent
        # write in the same trace will still hard-fail (correctly so).
        reconcilable=True,
    ),
    Deviation(
        id="PD3",
        posix="fstatat()/faccessat() dirfd argument (System Interfaces)",
        summary="fstatat and faccessat only accept AT_FDCWD; a real "
                "directory descriptor fails ENOSYS",
        root_cause="src/posix/posix_fstatat.c, posix_faccessat.c "
                   "('For now, only support AT_FDCWD')",
        candidate_fix="resolve through the descriptor's open handle as "
                      "openat/mkdirat/unlinkat already do",
        ops=("RStat",),
        expected_status=None,
        actual_status=ENOSYS,
        context=lambda op, fs: op.get("dfd", -1) != -1,
        reconcilable=True,
    ),
    Deviation(
        id="PD7r",
        posix="read(): '[EBADF] ... not a valid file descriptor open for "
              "reading'",
        summary="read/pread through a write-only descriptor succeeds "
                "(access mode never enforced on the I/O path); the read "
                "side is status-only, the write side (PD7) corrupts state "
                "and is left unreconciled",
        root_cause="src/posix/posix_read.c etc. never check "
                   "fd_entry.oflags O_ACCMODE",
        candidate_fix="check O_ACCMODE in the fd I/O paths",
        ops=("RRead", "RPread"),
        expected_status=EBADF,
        actual_status=OK,
        # NOTE: the successful real read advances the real offset while the
        # model's failed read does not; a later sequential read on the same
        # descriptor may diverge (same root cause).
        reconcilable=True,
    ),
    Deviation(
        id="PD11a",
        posix="XBD 4.5 / read(): access permission is determined at open()",
        summary="a legitimate read through a valid descriptor fails EACCES "
                "because chimera re-runs the file-mode check per I/O "
                "(e.g. own file created mode 0222, opened O_RDWR)",
        root_cause="per-request authorization in the VFS/memfs I/O path "
                   "(server-style) with no open-time rights retention",
        candidate_fix="authorize at open; serve fd I/O under the open's "
                      "granted rights",
        ops=("RRead", "RPread"),
        expected_status=OK,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD11b",
        posix="read() [EBADF] vs per-op EACCES",
        summary="read through a write-only descriptor is denied, but with "
                "EACCES from the per-op mode check instead of EBADF from "
                "the descriptor access mode",
        root_cause="see PD7r/PD11a",
        candidate_fix="see PD7r",
        ops=("RRead", "RPread"),
        expected_status=EBADF,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD8",
        posix="read() on a directory (implementation-defined; Linux EISDIR)",
        summary="read on a directory descriptor returns fabricated "
                "zero-filled bytes; the model canonicalizes EISDIR",
        root_cause="memfs serves READ on a directory handle",
        candidate_fix="fail EISDIR in the client or memfs",
        ops=("RRead", "RPread"),
        expected_status=21,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD8w",
        posix="write() on a directory descriptor",
        summary="write through a read-only directory descriptor fails "
                "EISDIR; the model reports EBADF first (descriptor not "
                "open for writing) -- error-priority difference on a "
                "doubly-invalid call",
        root_cause="chimera checks the object type before the (never "
                   "enforced, see PD7) access mode",
        candidate_fix="fix PD7; either order is then defensible",
        ops=("RWrite", "RPwrite"),
        expected_status=EBADF,
        actual_status=21,
        reconcilable=True,
    ),
    Deviation(
        id="PD13",
        posix="open() of a FIFO with O_WRONLY blocks or fails ENXIO "
              "(O_NONBLOCK)",
        summary="opening a FIFO succeeds instantly with no FIFO semantics "
                "(the model canonicalizes ENXIO, the honest NAS answer)",
        root_cause="memfs treats FIFO inodes as plain files on OPEN",
        candidate_fix="fail ENXIO for FIFO/socket opens in the POSIX "
                      "client",
        ops=("ROpen",),
        expected_status=6,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD14",
        posix="open(): '[EISDIR] The named file is a directory and oflag "
              "includes O_WRONLY or O_RDWR'",
        summary="directories open successfully for writing",
        root_cause="no EISDIR gate in the client open path; memfs serves "
                   "writable opens of directories",
        candidate_fix="reject write-access opens of directories with "
                      "EISDIR in chimera_posix_open/openat",
        ops=("ROpen",),
        expected_status=21,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD15",
        posix="XBD 4.16: a trailing slash requires the path to resolve to "
              "a directory (ENOTDIR otherwise)",
        summary="trailing slashes on non-directories are ignored: "
                "stat('f/') succeeds, mkdir over an existing non-dir with "
                "a trailing slash reports EEXIST instead of ENOTDIR",
        root_cause="the client passes the raw path down; no trailing-slash "
                   "directory requirement anywhere in the stack",
        candidate_fix="enforce the trailing-slash rule during client path "
                      "validation",
        ops=("RStat", "RMkdir", "ROpen", "RUnlink", "RRmdir", "RTruncate",
             "RChmod", "RChown", "RUtimens", "RAccess", "RReadlink"),
        expected_status=20,
        actual_status=None,
        context=lambda op, fs: op.get("pth", {}).get("slash", False),
        reconcilable=True,
    ),
    Deviation(
        id="PD15b",
        posix="XBD 4.16: a trailing slash forces a final symlink to be "
              "followed (ELOOP for a self-loop)",
        summary="mkdir over a self-loop symlink named with a trailing "
                "slash reports EEXIST; the slash should force following "
                "and yield ELOOP",
        root_cause="see PD15 (trailing slashes ignored on the mutation "
                   "paths)",
        candidate_fix="see PD15",
        ops=("RMkdir",),
        expected_status=40,
        actual_status=EEXIST,
        context=lambda op, fs: op.get("pth", {}).get("slash", False),
        reconcilable=True,
    ),
    Deviation(
        id="PD15c",
        posix="XBD 4.16 vs mkdir(): a trailing slash on a DANGLING symlink",
        summary="mkdir('dangling/') -- the model follows the link and "
                "creates the target directory (strict XBD 4.16); chimera "
                "(like Linux) reports the existing symlink name.  NOTE: "
                "the model's state gains the created directory, so a later "
                "step touching it may still diverge.",
        root_cause="the client judges the slash-stripped name; creating "
                   "through the link would need readlink+re-resolution",
        candidate_fix="acceptance (Linux agrees with chimera here)",
        ops=("RMkdir",),
        expected_status=OK,
        actual_status=EEXIST,
        context=lambda op, fs: op.get("pth", {}).get("slash", False),
        reconcilable=True,
    ),
    Deviation(
        id="PD17h",
        posix="rmdir(): ENOTDIR (victim not a directory) vs EACCES "
              "(unwritable parent) priority (unspecified)",
        summary="rmdir of a non-directory in an unwritable parent reports "
                "EACCES; the model reports the type error first",
        root_cause="the delete gate's parent fast path denies before the "
                   "child's type is ever fetched",
        candidate_fix="none required; listed for visibility",
        ops=("RRmdir",),
        expected_status=20,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD17i",
        posix="rmdir(): ENOTEMPTY (victim not empty) vs EACCES (unwritable "
              "parent) priority (unspecified)",
        summary="rmdir of a non-empty directory in an unwritable parent "
                "reports EACCES; the model reports the emptiness error "
                "first",
        root_cause="the delete gate denies before the child is examined",
        candidate_fix="none required; listed for visibility",
        ops=("RRmdir",),
        expected_status=39,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD17a",
        posix="unlink() EISDIR/EPERM vs EACCES priority (unspecified)",
        summary="unlink(directory) in a parent without write permission "
                "reports EACCES; the model reports the type error first",
        root_cause="chimera checks parent write access before victim type",
        candidate_fix="none required (priority unspecified); listed so the "
                      "divergence stays visible",
        ops=("RUnlink",),
        expected_status=21,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD17b",
        posix="mkdir() EEXIST vs EACCES priority (unspecified)",
        summary="mkdir over an existing entry in a parent without write "
                "permission reports EACCES; the model reports EEXIST",
        root_cause="parent write access checked before existence",
        candidate_fix="none required; listed for visibility",
        ops=("RMkdir", "RSymlink", "RMknod", "RLink"),
        expected_status=17,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD17c",
        posix="link() EPERM (directory source) vs EACCES priority",
        summary="link with a directory source in an unwritable target "
                "parent reports EACCES; the model reports EPERM (POSIX "
                "requires EPERM for directory sources specifically)",
        root_cause="target-parent access checked before source type",
        candidate_fix="check the directory-source case first",
        ops=("RLink",),
        expected_status=1,
        actual_status=EACCES,
        reconcilable=True,
    ),
    Deviation(
        id="PD18",
        posix="access()/faccessat(): checks use the process's real (or "
              "with AT_EACCESS, effective) credentials",
        summary="faccessat evaluates permissions against the HOST process "
                "uid/gid (getuid()/getgid()) instead of the chimera "
                "credential, ignores supplementary groups and AT_EACCESS; "
                "running as root it grants nearly everything",
        root_cause="src/posix/posix_faccessat.c callback uses getuid()/"
                   "getgid() over a stat result",
        candidate_fix="evaluate against chimera_posix_effective_cred (or "
                      "issue a VFS ACCESS op with the request credential)",
        ops=("RAccess",),
        expected_status=EACCES,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD17d",
        posix="open()/mkdir()/link(): EEXIST vs EACCES/EPERM/ENOENT "
              "priority on doubly-invalid calls (unspecified)",
        summary="chimera reports the existing target (EEXIST) where the "
                "model reports the search/permission or source error "
                "first; both conditions hold, POSIX does not order them",
        root_cause="existence checked before parent access / source "
                   "resolution",
        candidate_fix="none required; listed for visibility",
        ops=("ROpen", "RMkdir", "RMknod", "RSymlink"),
        expected_status=EACCES,
        actual_status=EEXIST,
        reconcilable=True,
    ),
    Deviation(
        id="PD17e",
        posix="link(): EPERM (directory source) vs EEXIST (existing "
              "target) priority",
        summary="link with a directory source and an existing target "
                "reports EEXIST; the model reports the source error",
        root_cause="target existence checked before source resolution",
        candidate_fix="none required; listed for visibility",
        ops=("RLink",),
        expected_status=EPERM,
        actual_status=EEXIST,
        reconcilable=True,
    ),
    Deviation(
        id="PD17f",
        posix="link(): ENOENT (missing source) vs EEXIST (existing "
              "target) priority",
        summary="link with a missing source and an existing target "
                "reports EEXIST; the model reports the source error",
        root_cause="target existence checked before source resolution",
        candidate_fix="none required; listed for visibility",
        ops=("RLink",),
        expected_status=ENOENT,
        actual_status=EEXIST,
        reconcilable=True,
    ),
    Deviation(
        id="PD24",
        posix="model bound, not a chimera defect",
        summary="the model's per-process descriptor table holds 16 slots "
                "(MAX_FDS) and predicts EMFILE when full; chimera's table "
                "holds max_fds=1024, so the open succeeds",
        root_cause="differing table bounds by design",
        candidate_fix="n/a (the replayer closes the stray descriptor and "
                      "exempts any O_CREAT residue node from the final "
                      "audit)",
        ops=("ROpen", "RDup", "RFcntlDupfd", "ROpendir"),
        expected_status=EMFILE,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD20",
        posix="open(): access mode permission check applies to existing "
              "files whether or not O_CREAT is given",
        summary="open(existing, O_CREAT|O_WRONLY) succeeds on a file the "
                "caller has no write permission for; the same open without "
                "O_CREAT correctly fails EACCES",
        root_cause="the client/VFS create-path open skips the access-mode "
                   "check when the file turns out to already exist",
        candidate_fix="apply the access check on the open-existing arm of "
                      "the create path",
        ops=("ROpen",),
        expected_status=EACCES,
        actual_status=OK,
        # The model allocated no descriptor, so replay stays in sync (the
        # real descriptor leaks for the rest of the trace).
        reconcilable=True,
    ),
    Deviation(
        id="PD22",
        posix="lseek() SEEK_DATA/SEEK_HOLE: ENXIO for offsets at or past "
              "EOF (and for negative offsets)",
        summary="chimera returns EINVAL where the model (and Linux) return "
                "ENXIO for out-of-range SEEK_DATA/SEEK_HOLE offsets",
        root_cause="src/posix/posix_lseek.c validates the offset before "
                   "dispatch and reports EINVAL",
        candidate_fix="return ENXIO for out-of-range data/hole seeks",
        ops=("RLseek",),
        expected_status=6,
        actual_status=EINVAL,
        reconcilable=True,
    ),
    Deviation(
        id="PD19",
        posix="clone_file_range: source range must lie within the source "
              "file (EINVAL)",
        summary="clone_file_range with a source range beyond EOF returns "
                "success instead of EINVAL",
        root_cause="memfs clone path does not validate the source range",
        candidate_fix="validate offSrc+len <= source size",
        ops=("RCloneRange",),
        expected_status=EINVAL,
        actual_status=OK,
        reconcilable=True,
    ),
    Deviation(
        id="PD4",
        posix="write() with O_APPEND: 'the file offset shall be set to the "
              "end of the file prior to each write'",
        summary="O_APPEND is recorded at open but never honored: write() "
                "writes at the current offset instead of seeking to EOF",
        root_cause="src/posix/posix_write.c (no O_APPEND path; fd_entry "
                   "offset used unconditionally)",
        candidate_fix="resolve offset to EOF under O_APPEND atomically "
                      "with the write (VFS write with append semantics)",
        reconcilable=False,
    ),
    Deviation(
        id="PD5",
        posix="dup()/dup2(): 'shall refer to the same open file description' "
              "(shared file offset and status flags)",
        summary="duplicated descriptors get their own offset: dup() "
                "allocates a fresh fd_entry with offset 0 and dup2() "
                "resets the target's offset to 0; the two descriptors "
                "then seek/read/write independently",
        root_cause="src/posix/posix_dup.c (chimera_posix_fd_alloc zeroes "
                   "offset), posix_dup2.c (new_entry->offset = 0); "
                   "offset lives per fd_entry, not per open file "
                   "description",
        candidate_fix="introduce a shared description object (offset + "
                      "status flags) referenced by fd entries",
        reconcilable=False,
    ),
    Deviation(
        id="PD6",
        posix="linkat() AT_SYMLINK_FOLLOW (System Interfaces)",
        summary="linkat ignores its flags argument, so AT_SYMLINK_FOLLOW "
                "links the symlink itself instead of its target (and real "
                "dirfds fail ENOSYS)",
        root_cause="src/posix/posix_linkat.c ('flags ... not yet "
                   "implemented', AT_FDCWD-only)",
        candidate_fix="resolve oldpath with following when the flag is set",
        reconcilable=False,
    ),
    Deviation(
        id="PD25",
        posix="XBD 4.13 Pathname Resolution -- search permission on each "
              "prefix directory is required to examine the next component",
        summary="an over-long final component whose path-prefix directory "
                "denies the caller search permission returns ENAMETOOLONG, "
                "but pathname resolution is left-to-right: the missing search "
                "(x) on the prefix is EACCES and precedes the component-length "
                "check.  Verified against Linux: EACCES when the prefix is "
                "unsearchable, ENAMETOOLONG only when it is searchable.  Found "
                "by the ENAMETOOLONG generator (genTooLong).",
        root_cause="the VFS NAME_MAX check runs before the parent directory's "
                   "MAY_EXEC search-permission check during resolution",
        candidate_fix="check search permission on each prefix component before "
                      "validating the next component's length",
        ops=("RMkdir", "RMknod", "RSymlink", "RChmod", "RChown",
             "RUnlink", "RRmdir", "RTruncate", "RStat"),
        expected_status=EACCES,
        actual_status=ENAMETOOLONG,
        context=lambda op, fs: "@nlong" in op.get("pth", {}).get("comps", []),
        reconcilable=True,
    ),
    Deviation(
        id="PD26",
        posix="XBD 4.13 Pathname Resolution + System Interfaces open(): "
              "search (execute) permission is required on every directory in "
              "the pathname prefix, on every resolution; EISDIR (a directory "
              "opened for writing) is judged only after the prefix is "
              "traversed.  Verified against Linux (repro: /d mode 0444, "
              "/d/a -> symlink -> /d/d a directory, open(/d/a, O_CREAT|O_RDWR) "
              "by a non-owner): EACCES, because /d is unsearchable.",
        summary="opening a path whose non-final directory has had its execute "
                "bit removed still resolves when an earlier lookup cached the "
                "child's name->FH: chimera reuses the cached resolution and "
                "skips re-checking the prefix directory's execute permission, "
                "so an open that must be EACCES (unsearchable prefix) instead "
                "reaches the target directory and returns EISDIR.  chimera "
                "does enforce execute on a cold resolution (pjdfstest passes); "
                "only the name-cache-hit path bypasses it.  Found by the "
                "byte-granular open generator crossing a chmod that dropped a "
                "cached directory's execute bit.",
        root_cause="src/vfs path resolution vs vfs_name_cache: a name-cache "
                   "hit returns the child FH without re-running the parent "
                   "directory's MAY_EXEC check that inode_permission enforces "
                   "on every pathname resolution",
        candidate_fix="enforce directory execute permission during resolution "
                      "independent of name-cache hits (or drop cached child "
                      "resolutions when a directory's mode loses execute)",
        ops=("ROpen",),
        expected_status=EACCES,
        actual_status=EISDIR,
        # Narrow to write-mode opens: EISDIR only arises for a directory
        # reached under O_WRONLY/O_RDWR after the prefix is (wrongly) traversed.
        context=lambda op, fs: op.get("fl", {}).get("acc", {}).get("tag")
        in ("AccW", "AccRW"),
        reconcilable=True,
    ),
]


def reconcile(tag, req_value, expected_status, actual_status, post_fs):
    """Return the matching reconcilable Deviation, or None.

    Called only when actual_status != expected_status.
    """
    for dev in KNOWN_DEVIATIONS:
        if not dev.reconcilable:
            continue
        if dev.ops and tag not in dev.ops:
            continue
        if dev.expected_status is not None \
                and dev.expected_status != expected_status:
            continue
        if dev.actual_status is not None \
                and dev.actual_status != actual_status:
            continue
        if dev.context(req_value, post_fs):
            return dev
    return None
