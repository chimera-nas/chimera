#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Wire regressions with tag-specific VFS submission counts.

Uses the external pynfs runtime without changing its checkout. Run through
scripts/nfs4_compound_boundaries_test_wrapper.sh, which enables the server's
debug submission trace. Successful wire results alone cannot prove that a
request stayed inside one VFS compound, so every measured case checks both.
An optional wrapper preload rejects the first finish of read-only OPEN/CLOSE
sequences with EAGAIN. Fixture creation and writes remain separate; the test
does not pretend a retry can roll back backend mutations.
"""

import argparse
import copy
import os
from pathlib import Path
import re
import sys
import time

PYNFS = Path(os.environ.get("PYNFS_DIR", "/opt/pynfs"))
sys.path[:0] = [str(PYNFS / "nfs4.1"), str(PYNFS), str(PYNFS / "xdr")]

# Python versions without stdlib xdrlib use xdrlib3; pynfs still passes str
# for a few protocol strings. Keep the compatibility shim in this process.
try:
    from xdrlib3 import Packer
except ImportError:
    pass
else:
    _pack_string = Packer.pack_string

    def _pack_compat(self, value):
        return _pack_string(self, value.encode() if isinstance(value, str) else value)

    Packer.pack_string = _pack_compat

import nfs4client  # noqa: E402
import nfs_ops  # noqa: E402
from rpc.security import AuthSys  # noqa: E402
from xdrdef.nfs4_const import *  # noqa: E402,F403
from xdrdef.nfs4_type import (  # noqa: E402
    app_data_block4, channel_attrs4, createhow4, createtype4, creatverfattr, exist_lock_owner4, lock_owner4, locker4, nfsace4,
    open_claim4, open_owner4, open_to_lock_owner4, openflag4, stateid4,
)

op = nfs_ops.NFS4ops()
CURRENT = stateid4(1, b"\0" * 12)
ANONYMOUS = stateid4(0, b"\0" * 12)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class Probe:
    def __init__(self, args):
        self.args = args
        self.client = nfs4client.NFS4Client(args.host, args.port, args.minor)
        self.client.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"compound-probe"))
        record = self.client.new_client(f"compound-{os.getpid()}".encode())
        # Keep the negotiated wire limit separate from the server's 128 KiB
        # encoding arena. The ACL-budget case returns two valid ACL snapshots
        # whose actual wire representation exceeds pynfs's default 8 KiB.
        attrs = channel_attrs4(0, 1024 * 1024, 1024 * 1024, 1024 * 1024, 128, 8, [])
        self.session = record.create_session(fore_attrs=attrs)
        self.session.compound([op.reclaim_complete(False)])
        self.measured = []
        self.expected_runs = {}
        self.files = []
        self.directories = []
        res = self.call([op.putrootfh(), op.lookup(args.export.encode()), op.getfh()])
        self.directory = res.resarray[-1].object

    def call(self, operations, name=None, expected=NFS4_OK, runs=None, retry_delay=False, checks=True):
        tag = f"boundary_{name}".encode() if name else b"boundary_setup"
        res = self.session.compound(operations, tag=tag, handle_state_errors=False, checks=checks)
        if retry_delay:
            # Explicitly opted-in setup OPEN may break cached anonymous I/O
            # claims asynchronously. Measured requests are never retried here.
            require(name is None, "client retries must not obscure a measured VFS boundary")
            deadline = time.monotonic() + 3
            while res.status == NFS4ERR_DELAY and time.monotonic() < deadline:
                time.sleep(0.01)
                res = self.session.compound(operations, tag=tag, handle_state_errors=False, checks=checks)
        require(res.status == expected,
                f"{name or 'setup'}: expected {nfsstat4[expected]}, got {res!r}")
        if name:
            self.measured.append(tag.decode())
            if runs is not None:
                self.expected_runs[tag.decode()] = runs
            print(f"PASS wire {name}: {nfsstat4[res.status]}", flush=True)
        return res

    def open_op(self, name, attrs=None, create=True, owner=None,
                access=OPEN4_SHARE_ACCESS_BOTH, deny=OPEN4_SHARE_DENY_NONE, create_mode=GUARDED4):
        if attrs is None:
            attrs = {FATTR4_MODE: 0o666}
        if owner is None:
            owner = b"owner-" + name
        how = (openflag4(OPEN4_CREATE, createhow4(create_mode, attrs))
               if create else openflag4(OPEN4_NOCREATE))
        return op.open(0, access | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                       deny, open_owner4(0, owner), how, open_claim4(CLAIM_NULL, name))

    def create(self, name):
        name = name.encode()
        res = self.call([op.putfh(self.directory), self.open_op(name), op.getfh()])
        fh, sid = res.resarray[-1].object, res.resarray[-2].stateid
        self.files.append((name, fh, sid))
        return name, fh, sid

    def read(self, fh, sid):
        res = self.call([op.putfh(fh), op.read(sid, 0, 4096)])
        return res.resarray[-1].data

    def mark_closed(self, fh):
        self.replace_stateid(fh, None)

    def replace_stateid(self, fh, replacement):
        self.files = [(name, handle, replacement if handle == fh else sid)
                      for name, handle, sid in self.files]

    def check_trace(self):
        # Logging may be drained by another thread after the RPC reply arrives.
        deadline = time.monotonic() + 3
        counts = {}
        while True:
            text = Path(self.args.server_log).read_text(errors="replace")
            counts = {tag: len(re.findall(
                rf"NFS4_VFS_SUBMIT tag={re.escape(tag)}(?:\s|$)", text))
                for tag in self.measured}
            if all(counts[tag] >= len(self.expected_runs.get(tag, [None]))
                   for tag in self.measured) or time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        for tag, count in counts.items():
            if tag in self.expected_runs:
                runs = [(int(start), int(length)) for start, length in re.findall(
                    rf"NFS4_VFS_SUBMIT tag={re.escape(tag)} start=(\d+) count=(\d+)", text)]
                require(runs == self.expected_runs[tag],
                        f"{tag}: expected VFS runs {self.expected_runs[tag]}, observed {runs}")
                print(f"PASS boundary {tag}: expected VFS runs {runs}", flush=True)
                continue
            require(count == 1, f"{tag}: expected one VFS submission, observed {count}")
            print(f"PASS boundary {tag}: one VFS submission", flush=True)

    def cleanup(self):
        for name, fh, sid in reversed(self.files):
            if sid is not None:
                self.call([op.putfh(fh), op.close(0, sid)])
            self.call([op.putfh(self.directory), op.remove(name)])
        for name in reversed(self.directories):
            self.call([op.putfh(self.directory), op.remove(name)])


def test_resolved_io(p):
    _, a, sa = p.create("resolved-a")
    name, b, sb = p.create("resolved-b")
    data = b"resolved-current-filehandle"
    p.call([op.putfh(p.directory), op.lookup(name), op.write(sb, 0, FILE_SYNC4, data)],
           "lookup_write")
    res = p.call([op.putfh(p.directory), op.lookup(name), op.read(sb, 0, 4096)],
                 "lookup_read")
    require(res.resarray[-1].data == data, "LOOKUP+READ returned incorrect bytes")
    res = p.call([op.putfh(a), op.putfh(b), op.read(sb, 0, 4096)], "switch_read")
    require(res.resarray[-1].data == data, "second PUTFH did not select stateid B's file")

    res = p.call([op.putfh(a), op.putfh(b), op.write(sa, 0, FILE_SYNC4, b"BAD")],
                 "mismatched_write", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 3, "mismatched WRITE did not stop at the failing operation")
    require(p.read(a, sa) == b"" and p.read(b, sb) == data,
            "mismatched WRITE changed a file")
    p.call([op.putfh(a), op.putfh(b), op.setattr(sa, {FATTR4_SIZE: 0}),
            op.write(sb, 0, FILE_SYNC4, b"BAD")],
           "mismatched_setattr", NFS4ERR_BAD_STATEID)
    require(p.read(b, sb) == data, "mismatched size SETATTR or its suffix changed the file")
    if p.args.minor >= 2:
        pattern = app_data_block4(0, 4, 1, NFS4_UINT64_MAX, 0, 0, b"BAD!")
        mutations = [
            ("allocate", op.allocate(sa, 0, 128)),
            ("deallocate", op.deallocate(sa, 0, 4)),
            ("write_same", op.write_same(sa, FILE_SYNC4, pattern)),
            ("seek", op.seek(sa, 0, NFS4_CONTENT_DATA)),
        ]
        for name, operation in mutations:
            res = p.call([op.putfh(a), op.putfh(b), operation,
                          op.write(sb, 0, FILE_SYNC4, b"BAD")],
                         f"mismatched_{name}", NFS4ERR_BAD_STATEID)
            require(len(res.resarray) == 3, f"mismatched {name} did not stop its suffix")
            require(p.read(a, sa) == b"" and p.read(b, sb) == data,
                    f"mismatched {name} changed a file")

    # A distinct attribute structure belongs to each SETATTR. Reusing a single
    # request scratch object can silently turn this into two size=6 operations.
    p.call([op.putfh(p.directory), op.lookup(b"resolved-b"),
            op.setattr(sb, {FATTR4_SIZE: 3}), op.setattr(sb, {FATTR4_SIZE: 6})],
           "lookup_two_setattrs")
    require(p.read(b, sb) == data[:3] + b"\0" * 3,
            "two size SETATTRs did not retain distinct operation inputs")


def test_open_suffix(p):
    name = b"open-setattr-write"
    res = p.call([op.putfh(p.directory), p.open_op(name),
                  op.setattr(CURRENT, {FATTR4_SIZE: 5}),
                  op.write(CURRENT, 2, FILE_SYNC4, b"new"), op.getfh()],
                 "open_setattr_write")
    fh, sid = res.resarray[-1].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    require(p.read(fh, sid) == b"\0\0new", "OPEN+SETATTR+WRITE produced incorrect bytes")

    name = b"open-save-restore-write"
    res = p.call([op.putfh(p.directory), p.open_op(name), op.savefh(),
                  op.setattr(CURRENT, {FATTR4_SIZE: 7}), op.restorefh(),
                  op.write(CURRENT, 3, FILE_SYNC4, b"save"), op.getfh()],
                 "open_save_restore_current")
    fh, sid = res.resarray[-1].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    require(p.read(fh, sid) == b"\0\0\0save",
            "SAVEFH/RESTOREFH lost the reserved OPEN current stateid")

    if p.args.minor < 2:
        return
    name = b"open-sparse-write-same"
    pattern = app_data_block4(0, 4, 2, NFS4_UINT64_MAX, 0, 0, b"ABCD")
    res = p.call([op.putfh(p.directory), p.open_op(name),
                  op.allocate(CURRENT, 0, 16),
                  op.write_same(CURRENT, FILE_SYNC4, pattern),
                  op.deallocate(CURRENT, 8, 8),
                  op.seek(CURRENT, 0, NFS4_CONTENT_DATA), op.getfh()],
                 "open_sparse_write_same")
    fh, sid = res.resarray[-1].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    require(p.read(fh, sid) == b"ABCDABCD" + b"\0" * 8,
            "OPEN sparse/WRITE_SAME suffix produced incorrect bytes")


def test_lockt_veto(p):
    name, fh, sid = p.create("lockt-veto")
    original = b"lock-test-payload"
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, original)])
    holder = lock_owner4(0, b"holding-owner")
    lock = locker4(True, open_to_lock_owner4(0, sid, 0, holder))
    res = p.call([op.putfh(fh), op.lock(WRITE_LT, False, 0, 4, lock)])
    lock_sid = res.resarray[-1].lock_stateid
    probe_owner = lock_owner4(0, b"probing-owner")
    res = p.call([op.putfh(p.directory), op.lookup(name),
                  op.lockt(WRITE_LT, 0, 4, probe_owner),
                  op.write(sid, 32, FILE_SYNC4, b"MUST-NOT-EXECUTE")],
                 "lockt_denied", NFS4ERR_DENIED)
    require(len(res.resarray) == 3, "LOCKT DENIED did not stop before WRITE")
    denied = res.resarray[-1].denied
    require(denied.offset == 0 and denied.length == 4 and denied.locktype == WRITE_LT,
            f"LOCKT returned incorrect conflict range/type: {denied!r}")
    require(denied.owner.owner == holder.owner, "LOCKT lost conflicting owner identity")
    p.call([op.putfh(fh), op.locku(WRITE_LT, 0, lock_sid, 0, 4)])
    require(p.read(fh, sid) == original, "LOCKT DENIED allowed its WRITE suffix")
    p.call([op.putfh(p.directory), op.lookup(name),
            op.lockt(WRITE_LT, 0, 4, probe_owner),
            op.write(sid, len(original), FILE_SYNC4, b"!")], "lockt_allowed")
    require(p.read(fh, sid) == original + b"!", "successful LOCKT did not run WRITE suffix")


def new_lock(open_sid, owner, locktype=WRITE_LT, offset=0, length=32):
    return op.lock(locktype, False, offset, length,
                   locker4(True, open_to_lock_owner4(0, open_sid, 0, lock_owner4(0, owner))))


def existing_lock(sid, locktype=WRITE_LT, offset=0, length=32):
    return op.lock(locktype, False, offset, length,
                   locker4(False, lock_owner=exist_lock_owner4(sid, 0)))


def check_lock_denied(result, owner, locktype, offset, length):
    denied = result.denied
    require(denied.owner.owner == owner and denied.locktype == locktype and
            denied.offset == offset and denied.length == length,
            f"incorrect denied lock owner/range/type: {denied!r}")


def test_lock_open_current_retry(p):
    name, fh, initial = p.create("retry-lock-existing")
    original = b"lock-payload"
    p.call([op.putfh(fh), op.write(initial, 0, FILE_SYNC4, original)])
    owner = b"retry-lock-range-owner"
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"retry-lock-open-owner"),
                  new_lock(CURRENT, owner, length=4), op.read(CURRENT, 0, len(original)),
                  op.locku(WRITE_LT, 0, CURRENT, 0, 4), op.read(CURRENT, 0, len(original)),
                  op.test_stateid([initial, CURRENT])], "retry_open_lock_unlock_read")
    opened, locked, unlocked = res.resarray[1].stateid, res.resarray[2].lock_stateid, res.resarray[4].lock_stateid
    require(opened.seqid == 1 and locked.seqid == 1 and opened.other != locked.other,
            "OPEN and new LOCK did not allocate independent initial state identities")
    check_open_version(locked, unlocked, "new LOCKU version must advance once across retry")
    require(res.resarray[3].data == original and res.resarray[5].data == original and
            res.resarray[6].tsr_status_codes == [NFS4_OK, NFS4ERR_BAD_STATEID],
            "private LOCK/LOCKU current-stateid I/O or TEST_STATEID failed")
    res = p.call([op.test_stateid([locked, unlocked, stateid4(0, unlocked.other)]), op.putfh(fh),
                  op.lockt(WRITE_LT, 0, 4, lock_owner4(0, b"retry-lock-probe"))])
    require(res.resarray[0].tsr_status_codes == [NFS4ERR_OLD_STATEID, NFS4_OK, NFS4_OK],
            "accepted lock versions advanced twice or final unlock was not published")
    res = p.call([op.putfh(fh), op.close(0, opened), op.test_stateid([locked, unlocked]),
                  op.lockt(WRITE_LT, 0, 4, lock_owner4(0, b"retry-lock-probe"))],
                 "close_empty_child_lock")
    require(res.resarray[2].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2,
            "CLOSE retained its empty child lock state")


def test_lock_open_mode_parity(p):
    name, fh, original = p.create("lock-open-mode-parity")
    p.call([op.putfh(fh), op.write(original, 0, FILE_SYNC4, b"data")])
    for access, locktype, label in ((OPEN4_SHARE_ACCESS_READ, WRITE_LT, "read_open_write_lock"),
                                    (OPEN4_SHARE_ACCESS_WRITE, READ_LT, "write_open_read_lock")):
        res = p.call([op.putfh(p.directory),
                      p.open_op(name, create=False, owner=label.encode(), access=access),
                      new_lock(CURRENT, label.encode(), locktype, length=4),
                      op.getattr(1 << FATTR4_SIZE)], label)
        opened, locked = res.resarray[1].stateid, res.resarray[2].lock_stateid
        denied_io = (op.write(locked, 0, FILE_SYNC4, b"BAD")
                     if access == OPEN4_SHARE_ACCESS_READ else op.read(locked, 0, 4))
        p.call([op.putfh(fh), denied_io], label + "_io_veto", NFS4ERR_OPENMODE)
        p.call([op.putfh(fh), op.locku(locktype, 0, locked, 0, 4), op.close(0, opened)])
    require(p.read(fh, original) == b"data", "opposite-mode range lock elevated I/O rights")


def test_access_execute_permissions(p):
    _, fh, sid = p.create("access-execute-permissions")
    mask = ACCESS4_READ | ACCESS4_EXECUTE
    res = p.call([op.putfh(fh), op.setattr(sid, {FATTR4_MODE: 0o600}), op.access(mask),
                  op.setattr(sid, {FATTR4_MODE: 0o700}), op.access(mask)],
                 "access_execute_mode")
    require(res.resarray[2].supported == mask and res.resarray[2].access == ACCESS4_READ and
            res.resarray[4].supported == mask and res.resarray[4].access == mask,
            "privileged ACCESS did not respect executable mode bits")
    acl = [nfsace4(ACE4_ACCESS_DENIED_ACE_TYPE, 0, ACE4_EXECUTE, b"EVERYONE@"),
           nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, 0x1F01FF & ~ACE4_EXECUTE, b"OWNER@")]
    res = p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl}), op.access(mask)],
                 "access_execute_deny_ace")
    require(res.resarray[2].supported == mask and res.resarray[2].access == ACCESS4_READ,
            "DENY ACE was incorrectly counted as an executable grant")
    acl = [acl[1], nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, ACE4_EXECUTE, b"12501")]
    res = p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl}), op.access(mask)],
                 "access_execute_named_allow")
    require(res.resarray[2].supported == mask and res.resarray[2].access == mask,
            "privileged ACCESS failed to recognize an executable named ALLOW ACE")


def test_lock_ranges(p):
    _, fh, opened = p.create("lock-ranges")
    original = b"range-data"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    owner = b"range-holder"
    probe_owner = lock_owner4(0, b"range-probe")
    res = p.call([op.putfh(fh), new_lock(opened, owner)])
    sid = res.resarray[1].lock_stateid

    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 8, 8),
                  op.lockt(WRITE_LT, 8, 8, probe_owner), op.lockt(READ_LT, 0, 8, probe_owner)],
                 "locku_partial_split", NFS4ERR_DENIED)
    current = res.resarray[1].lock_stateid
    check_open_version(sid, current, "partial LOCKU version")
    require(len(res.resarray) == 4, "partial unlock did not release exactly its hole")
    check_lock_denied(res.resarray[3], owner, WRITE_LT, 0, 8)
    res = p.call([op.putfh(fh), op.lockt(READ_LT, 16, 16, probe_owner)], expected=NFS4ERR_DENIED)
    check_lock_denied(res.resarray[1], owner, WRITE_LT, 16, 16)
    p.call([op.putfh(fh), existing_lock(sid), op.write(opened, 0, FILE_SYNC4, b"BAD")],
           "lock_old_version_veto", NFS4ERR_OLD_STATEID)
    require(p.read(fh, opened) == original, "old lock stateid allowed suffix mutation")
    sid = current

    res = p.call([op.putfh(fh), existing_lock(sid, offset=4, length=20),
                  op.lockt(READ_LT, 8, 8, probe_owner)], "lock_overlap_merge", NFS4ERR_DENIED)
    current = res.resarray[1].lock_stateid
    check_open_version(sid, current, "overlapping LOCK version")
    check_lock_denied(res.resarray[2], owner, WRITE_LT, 0, 32)
    sid = current

    res = p.call([op.putfh(fh), existing_lock(sid, READ_LT, 8, 8),
                  op.lockt(READ_LT, 8, 8, probe_owner), op.lockt(WRITE_LT, 8, 8, probe_owner)],
                 "lock_mode_conversion", NFS4ERR_DENIED)
    current = res.resarray[1].lock_stateid
    check_open_version(sid, current, "lock mode conversion version")
    require(len(res.resarray) == 4, "converted READ range did not permit another reader")
    check_lock_denied(res.resarray[3], owner, READ_LT, 8, 8)
    sid = current

    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 128, 8),
                  op.lockt(READ_LT, 0, 8, probe_owner)], "locku_outside_extents", NFS4ERR_DENIED)
    current = res.resarray[1].lock_stateid
    check_open_version(sid, current, "out-of-range LOCKU still advances the accepted state version")
    check_lock_denied(res.resarray[2], owner, WRITE_LT, 0, 8)
    sid = current

    predicted = stateid4(sid.seqid + 1, sid.other)
    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 0, 32),
                  op.lockt(WRITE_LT, 0, 32, probe_owner), op.read(CURRENT, 0, len(original)),
                  op.test_stateid([sid, predicted, stateid4(0, sid.other)])], "locku_all_then_current_read")
    check_open_version(sid, res.resarray[1].lock_stateid, "full LOCKU version")
    require(res.resarray[3].data == original and
            res.resarray[4].tsr_status_codes == [NFS4ERR_OLD_STATEID, NFS4_OK, NFS4_OK],
            "LOCKU did not publish its private range/version view before following operations")
    sid = res.resarray[1].lock_stateid

    res = p.call([op.putfh(fh), existing_lock(sid, offset=64, length=0xffffffffffffffff),
                  op.lockt(READ_LT, 0xffffffffffffffff, 0xffffffffffffffff, probe_owner)],
                 "lock_to_eof_last_byte", NFS4ERR_DENIED)
    sid = res.resarray[1].lock_stateid
    check_lock_denied(res.resarray[2], owner, WRITE_LT, 64, 0xffffffffffffffff)
    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 96, 0xffffffffffffffff),
                  op.lockt(WRITE_LT, 96, 1, probe_owner), op.lockt(READ_LT, 64, 32, probe_owner)],
                 "locku_to_eof_preserves_prefix", NFS4ERR_DENIED)
    sid = res.resarray[1].lock_stateid
    check_lock_denied(res.resarray[3], owner, WRITE_LT, 64, 32)
    p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 0, 0xffffffffffffffff),
            op.lockt(WRITE_LT, 0, 0xffffffffffffffff, probe_owner)], "locku_to_eof_all")


def test_lock_private_owners(p):
    _, fh, opened = p.create("lock-private-owners")
    original = b"private-owner-data"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    first, second = b"private-lock-first", b"private-lock-second"
    res = p.call([op.putfh(fh), new_lock(opened, first, length=32),
                  new_lock(opened, second, offset=8, length=8),
                  op.write(opened, 0, FILE_SYNC4, b"BAD")],
                 "lock_private_owner_denial", NFS4ERR_DENIED)
    first_sid = res.resarray[1].lock_stateid
    check_lock_denied(res.resarray[2], first, WRITE_LT, 0, 32)
    require(len(res.resarray) == 3 and p.read(fh, first_sid) == original,
            "another private lock owner bypassed the first owner's tentative range")
    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, first_sid, 8, 8),
                  new_lock(opened, second, offset=8, length=8),
                  op.lockt(READ_LT, 8, 8, lock_owner4(0, b"private-third-probe"))],
                 "lock_private_owner_after_unlock", NFS4ERR_DENIED)
    first_sid, second_sid = res.resarray[1].lock_stateid, res.resarray[2].lock_stateid
    check_lock_denied(res.resarray[3], second, WRITE_LT, 8, 8)
    res = p.call([op.putfh(fh), op.lockt(READ_LT, 0, 8, lock_owner4(0, second))],
                 expected=NFS4ERR_DENIED)
    check_lock_denied(res.resarray[1], first, WRITE_LT, 0, 8)
    p.call([op.putfh(fh), op.locku(WRITE_LT, 0, first_sid, 0, 32),
            op.locku(WRITE_LT, 0, second_sid, 0, 32),
            op.lockt(WRITE_LT, 0, 32, lock_owner4(0, b"private-third-probe"))],
           "lock_private_owners_unlock_all")


def test_lock_prefix_and_denial(p):
    _, fh, opened = p.create("lock-prefix-denial")
    original = b"no-mutation"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    owner = b"prefix-lock-owner"
    probe_owner = lock_owner4(0, b"prefix-lock-probe")
    res = p.call([op.putfh(fh), new_lock(opened, owner, length=16),
                  op.lockt(WRITE_LT, 0, 4, probe_owner), op.write(opened, 0, FILE_SYNC4, b"BAD")],
                 "lock_accepted_prefix_denied_suffix", NFS4ERR_DENIED)
    sid = res.resarray[1].lock_stateid
    check_lock_denied(res.resarray[2], owner, WRITE_LT, 0, 16)
    require(len(res.resarray) == 3 and p.read(fh, sid) == original,
            "private LOCK did not veto conflicting suffix or preserve accepted prefix")
    res = p.call([op.putfh(fh), new_lock(opened, b"conflicting-lock-owner", offset=4, length=4),
                  op.write(opened, 0, FILE_SYNC4, b"BAD")], "lock_denied_owner_payload", NFS4ERR_DENIED)
    check_lock_denied(res.resarray[1], owner, WRITE_LT, 0, 16)
    require(len(res.resarray) == 2 and p.read(fh, sid) == original,
            "denied LOCK allowed its mutation suffix")
    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, sid, 0, 16),
                  op.verify({FATTR4_SIZE: 999}), op.write(opened, 0, FILE_SYNC4, b"BAD")],
                 "locku_accepted_prefix_error", NFS4ERR_NOT_SAME)
    check_open_version(sid, res.resarray[1].lock_stateid, "accepted LOCKU error-prefix version")
    p.call([op.putfh(fh), op.lockt(WRITE_LT, 0, 16, probe_owner)])
    require(p.read(fh, opened) == original, "later error discarded accepted unlock or mutated data")

    _, other, other_opened = p.create("lock-wrong-fh")
    p.call([op.putfh(other), op.write(other_opened, 0, FILE_SYNC4, b"other-file")])
    current = res.resarray[1].lock_stateid
    p.call([op.putfh(other), op.locku(WRITE_LT, 0, current, 0, 16),
            op.write(other_opened, 0, FILE_SYNC4, b"BAD")],
           "locku_wrong_fh_veto", NFS4ERR_BAD_STATEID)
    require(p.read(other, other_opened) == b"other-file", "mismatched LOCKU allowed suffix mutation")
    p.call([op.putfh(fh), existing_lock(current, length=0),
            op.write(opened, 0, FILE_SYNC4, b"BAD")], "lock_zero_length_veto", NFS4ERR_INVAL)
    require(p.read(fh, opened) == original, "invalid lock geometry allowed suffix mutation")


def test_close_lock_children(p):
    _, fh, opened = p.create("close-held-locks")
    original = b"close-child-data"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    owner = b"close-held-child-owner"
    res = p.call([op.putfh(fh), new_lock(opened, owner, length=16)])
    locked = res.resarray[1].lock_stateid
    res = p.call([op.test_stateid([opened, locked]), op.putfh(fh), op.close(0, opened),
                  op.test_stateid([opened, locked, stateid4(0, locked.other), CURRENT]),
                  op.lockt(WRITE_LT, 0, 16, lock_owner4(0, b"close-child-probe")),
                  op.read(ANONYMOUS, 0, len(original))], "close_held_child_lock")
    p.mark_closed(fh)
    require(res.resarray[0].tsr_status_codes == [NFS4_OK, NFS4_OK] and
            res.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 4 and
            res.resarray[5].data == original,
            "CLOSE did not tombstone its child and release held ranges before its suffix")

    _, fh, opened = p.create("close-lock-io-veto")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    res = p.call([op.putfh(fh), new_lock(opened, b"close-io-child", length=16)])
    locked = res.resarray[1].lock_stateid
    res = p.call([op.putfh(fh), op.close(0, opened), op.read(locked, 0, 1),
                  op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")],
                 "close_child_read_veto", NFS4ERR_BAD_STATEID)
    p.mark_closed(fh)
    require(len(res.resarray) == 3 and p.read(fh, ANONYMOUS) == original,
            "closed child state authorized I/O or allowed its mutation suffix")
    p.call([op.putfh(fh), op.lockt(WRITE_LT, 0, 16, lock_owner4(0, b"close-child-probe"))])

    _, fh, opened = p.create("close-lock-prefix-error")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    res = p.call([op.putfh(fh), new_lock(opened, b"close-prefix-child", length=16)])
    locked = res.resarray[1].lock_stateid
    p.call([op.putfh(fh), op.close(0, opened), op.verify({FATTR4_SIZE: 999}),
            op.read(ANONYMOUS, 0, len(original))], "close_child_accepted_prefix_error", NFS4ERR_NOT_SAME)
    p.mark_closed(fh)
    res = p.call([op.test_stateid([opened, locked]), op.putfh(fh),
                  op.lockt(WRITE_LT, 0, 16, lock_owner4(0, b"close-child-probe"))])
    require(res.resarray[0].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2,
            "later error discarded accepted child-lock CLOSE")

    if p.args.minor == 2:
        _, fh, opened = p.create("close-lock-advice-veto")
        p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
        res = p.call([op.putfh(fh), new_lock(opened, b"close-advice-child", length=16)])
        locked = res.resarray[1].lock_stateid
        p.call([op.putfh(fh), op.close(0, opened), op.io_advise(locked, 0, 1, 0),
                op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")],
               "close_child_io_advise_veto", NFS4ERR_BAD_STATEID)
        p.mark_closed(fh)
        require(p.read(fh, ANONYMOUS) == original, "closed child IO_ADVISE allowed suffix mutation")


def test_retry_lock_close(p):
    name, fh, initial = p.create("retry-lock-close-existing")
    original = b"retry-close-child"
    p.call([op.putfh(fh), op.write(initial, 0, FILE_SYNC4, original)])
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"retry-close-open"),
                  op.savefh(), new_lock(CURRENT, b"retry-close-lock", length=8),
                  op.read(CURRENT, 0, len(original)), op.restorefh(), op.close(0, CURRENT),
                  op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"retry-close-probe")),
                  op.test_stateid([initial, CURRENT])], "retry_open_lock_held_close")
    opened, locked = res.resarray[1].stateid, res.resarray[3].lock_stateid
    require(opened.seqid == 1 and locked.seqid == 1 and res.resarray[4].data == original and
            res.resarray[8].tsr_status_codes == [NFS4_OK, NFS4ERR_BAD_STATEID],
            "retry changed private OPEN/LOCK versions or lost post-CLOSE protocol state")
    res = p.call([op.test_stateid([opened, locked, stateid4(0, locked.other)])])
    require(res.resarray[0].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 3,
            "accepted private CLOSE published an abandoned parent or child state")

    # The current lock stateid remains usable after LOCKU, while RESTOREFH
    # restores the OPEN identity used by CLOSE in the same VFS submission.
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"unlock-close-open"),
                  op.savefh(), new_lock(CURRENT, b"unlock-close-lock", length=8),
                  op.locku(WRITE_LT, 0, CURRENT, 0, 8), op.restorefh(), op.close(0, CURRENT),
                  op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"retry-close-probe"))],
                 "open_lock_unlock_close")
    check_open_version(res.resarray[3].lock_stateid, res.resarray[4].lock_stateid,
                       "LOCKU before CLOSE advanced incorrectly")


def test_lock_close_reopen(p):
    name, fh, opened = p.create("lock-close-reopen")
    original = b"reopened-child"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, original)])
    lock_owner = b"same-reopened-lock-owner"
    res = p.call([op.putfh(fh), new_lock(opened, lock_owner, length=8)])
    locked = res.resarray[1].lock_stateid
    res = p.call([op.putfh(fh), op.close(0, opened), op.test_stateid([opened, locked]),
                  op.putfh(p.directory), p.open_op(name, create=False, owner=b"owner-" + name),
                  op.savefh(), new_lock(CURRENT, lock_owner, length=8),
                  op.read(CURRENT, 0, len(original)), op.restorefh(), op.close(0, CURRENT),
                  op.test_stateid([opened, locked]),
                  op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"reopen-probe"))],
                 "close_reopen_parent_and_lock_owner")
    p.mark_closed(fh)
    reopened, relocked = res.resarray[4].stateid, res.resarray[6].lock_stateid
    require(reopened.other != opened.other and relocked.other != locked.other and relocked.seqid == 1 and
            res.resarray[2].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2 and
            res.resarray[10].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2 and
            res.resarray[7].data == original,
            "same-owner reopen reused a closed identity or lost its parent/child mapping")
    res = p.call([op.test_stateid([opened, locked, reopened, relocked])])
    require(res.resarray[0].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 4,
            "close/reopen/close left one generation publicly alive")


def test_lock_downgrade_after(p):
    _, fh, opened, _ = downgrade_fixture(p, "downgrade-after-lock")
    res = p.call([op.putfh(fh), new_lock(opened, b"downgrade-child-owner", length=8)])
    locked = res.resarray[1].lock_stateid
    next_open = stateid4(opened.seqid + 1, opened.other)
    next_lock = stateid4(locked.seqid + 1, locked.other)
    res = p.call([op.putfh(fh), existing_lock(locked, offset=8, length=8),
                  op.open_downgrade(opened, 0, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.test_stateid([opened, next_open, locked, next_lock]),
                  op.read(stateid4(0, locked.other), 0, 6),
                  op.write(stateid4(0, locked.other), 0, FILE_SYNC4, b"BAD")],
                 "lock_then_downgrade_child_rights", NFS4ERR_OPENMODE)
    p.replace_stateid(fh, res.resarray[2].open_stateid)
    require(res.resarray[3].tsr_status_codes == [NFS4ERR_OLD_STATEID, NFS4_OK] * 2 and
            res.resarray[4].data == b"before" and p.read(fh, next_open) == b"before",
            "post-LOCK downgrade failed to narrow child I/O rights or stage both versions")
    p.call([op.putfh(fh), op.locku(WRITE_LT, 0, next_lock, 0, 16), op.close(0, next_open),
            op.test_stateid([next_open, next_lock])], "downgraded_lock_unlock_close")
    p.mark_closed(fh)


def test_lock_sibling_parents(p):
    _, a, sa = p.create("lock-sibling-a")
    _, b, sb = p.create("lock-sibling-b")
    owner = b"shared-sibling-lock-owner"
    p.call([op.putfh(a), op.write(sa, 0, FILE_SYNC4, b"alpha")])
    p.call([op.putfh(b), op.write(sb, 0, FILE_SYNC4, b"bravo")])
    res = p.call([op.putfh(a), new_lock(sa, owner, length=8)])
    la = res.resarray[1].lock_stateid
    res = p.call([op.putfh(b), new_lock(sb, owner, length=8)])
    lb = res.resarray[1].lock_stateid
    res = p.call([op.putfh(a), op.locku(WRITE_LT, 0, la, 0, 4),
                  op.lockt(WRITE_LT, 0, 4, lock_owner4(0, b"sibling-probe")),
                  op.putfh(b), op.lockt(READ_LT, 0, 8, lock_owner4(0, b"sibling-probe"))],
                 "lock_owner_multiple_parent_view", NFS4ERR_DENIED)
    la = res.resarray[1].lock_stateid
    check_lock_denied(res.resarray[4], owner, WRITE_LT, 0, 8)
    res = p.call([op.putfh(a), op.close(0, sa), op.test_stateid([sa, la, sb, lb]),
                  op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"sibling-probe")),
                  op.putfh(b), op.read(lb, 0, 5),
                  op.lockt(READ_LT, 0, 8, lock_owner4(0, b"sibling-probe"))],
                 "close_keeps_sibling_lock_parent", NFS4ERR_DENIED)
    p.mark_closed(a)
    require(res.resarray[2].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2 + [NFS4_OK] * 2 and
            res.resarray[5].data == b"bravo", "closing one parent invalidated another parent's child")
    check_lock_denied(res.resarray[6], owner, WRITE_LT, 0, 8)
    res = p.call([op.putfh(b), op.close(0, sb), op.test_stateid([sb, lb]),
                  op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"sibling-probe"))], "close_last_sibling_lock_parent")
    p.mark_closed(b)
    require(res.resarray[2].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2,
            "last sibling parent close retained lock identity")


def test_lock_reclaim_outside_grace(p):
    _, fh, opened = p.create("lock-reclaim-outside")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"unchanged")])
    reclaim = new_lock(opened, b"invalid-reclaim-owner", length=8)
    reclaim.oplock.reclaim = True
    res = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), reclaim,
                  op.write(opened, 0, FILE_SYNC4, b"BAD")], "lock_reclaim_outside_grace", NFS4ERR_NO_GRACE)
    require(len(res.resarray) == 3 and p.read(fh, opened) == b"unchanged",
            "reclaim rejection discarded its successful prefix or allowed suffix mutation")


def test_open_reclaim_outside_grace(p):
    _, fh, opened = p.create("open-reclaim-outside")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"unchanged")])
    reclaim = op.open(0, OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                      OPEN4_SHARE_DENY_NONE, open_owner4(0, b"invalid-open-reclaim-owner"),
                      openflag4(OPEN4_NOCREATE),
                      open_claim4(CLAIM_PREVIOUS, delegate_type=OPEN_DELEGATE_NONE))
    res = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), reclaim,
                  op.write(opened, 0, FILE_SYNC4, b"BAD")],
                 "open_reclaim_outside_grace", NFS4ERR_NO_GRACE, runs=[(1, 4)])
    require(len(res.resarray) == 3 and p.read(fh, opened) == b"unchanged",
            "OPEN reclaim rejection lost its prefix or permitted suffix mutation")


def test_exclusive_open_compounds(p):
    for mode, label in ((EXCLUSIVE4, "exclusive4"), (EXCLUSIVE4_1, "exclusive41")):
        name = label.encode() + b"-compound"
        verifier = b"excltest"
        initial_size = 7 if mode == EXCLUSIVE4_1 else 0

        def exclusive(owner, token, size):
            how = (createhow4(mode, createverf=token) if mode == EXCLUSIVE4 else
                   createhow4(mode, ch_createboth=creatverfattr(token, {FATTR4_MODE: 0o666, FATTR4_SIZE: size})))
            return op.open(0, OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                           OPEN4_SHARE_DENY_NONE, open_owner4(0, owner),
                           openflag4(OPEN4_CREATE, how), open_claim4(CLAIM_NULL, name))

        res = p.call([op.putfh(p.directory), exclusive(name + b"-first", verifier, initial_size),
                      op.getfh(), op.getattr(1 << FATTR4_SIZE)], label + "_create_suffix", runs=[(1, 4)])
        fh, sid = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((name, fh, sid))
        require(res.resarray[3].obj_attributes[FATTR4_SIZE] == initial_size,
                "exclusive create lost its initial attributes")
        res = p.call([op.putfh(p.directory), exclusive(name + b"-match", verifier, 0),
                      op.getfh(), op.read(CURRENT, 0, 16), op.close(0, CURRENT)],
                     label + "_matching_verifier", runs=[(1, 5)])
        require(res.resarray[2].object == fh and res.resarray[3].data == b"\0" * initial_size,
                "matching verifier changed object identity or reapplied create attributes")
        res = p.call([op.putfh(p.directory), exclusive(name + b"-mismatch", b"mismatch", 0),
                      op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                     label + "_mismatched_verifier_veto", NFS4ERR_EXIST, runs=[(1, 3)])
        require(len(res.resarray) == 2 and p.read(fh, sid) == b"\0" * initial_size,
                "mismatched verifier applied attributes or ran suffix WRITE")


def test_acl_verify(p):
    _, fh, sid = p.create("acl-verify")
    # More than 16 ACEs exceeds the former 4096-byte conservative ACL bound.
    acl = [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, 0x1F01FF, b"OWNER@")]
    acl += [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, ACE4_READ_DATA,
                   str(12000 + i).encode()) for i in range(20)]
    p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl})])
    res = p.call([op.putfh(fh), op.getattr(1 << FATTR4_ACL)])
    returned = res.resarray[-1].obj_attributes[FATTR4_ACL]
    names = {ace.who for ace in returned}
    require(all(str(12000 + i).encode() in names for i in range(20)),
            "ACL GETATTR lost named ACEs")
    expected = {FATTR4_ACL: returned}
    p.call([op.putfh(fh), op.verify(expected), op.write(sid, 0, FILE_SYNC4, b"accepted")],
           "acl_verify_match")
    require(p.read(fh, sid) == b"accepted", "matching ACL VERIFY did not run suffix")
    mismatch = copy.deepcopy(returned)
    mismatch[-1].access_mask ^= ACE4_WRITE_DATA
    p.call([op.putfh(fh), op.verify({FATTR4_ACL: mismatch}),
            op.write(sid, 0, FILE_SYNC4, b"BAD")], "acl_verify_mismatch", NFS4ERR_NOT_SAME)
    p.call([op.putfh(fh), op.nverify(expected), op.write(sid, 0, FILE_SYNC4, b"BAD")],
           "acl_nverify_same", NFS4ERR_SAME)
    require(p.read(fh, sid) == b"accepted", "ACL veto allowed a mutating suffix")
    p.call([op.putfh(fh), op.nverify({FATTR4_ACL: mismatch}),
            op.write(sid, 8, FILE_SYNC4, b"!")], "acl_nverify_different")
    require(p.read(fh, sid) == b"accepted!", "different ACL NVERIFY did not run suffix")


def check_named_acl(returned, expected):
    actual = {(ace.type, ace.flag, ace.access_mask, ace.who) for ace in returned}
    for ace in expected:
        require((ace.type, ace.flag, ace.access_mask, ace.who) in actual,
                f"ACL roundtrip lost or altered ACE {ace!r}")


def test_acl_inputs(p):
    acl = [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, 0x1F01FF, b"OWNER@"),
           nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, ACE4_READ_DATA, b"12501"),
           nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_IDENTIFIER_GROUP,
                   ACE4_READ_DATA, b"12502")]
    changed = copy.deepcopy(acl)
    changed[1].who = b"12503"
    changed[2].who = b"12504"
    _, fh, sid = p.create("acl-setattr")
    res = p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl}),
                  op.getattr(1 << FATTR4_ACL), op.setattr(sid, {FATTR4_ACL: changed}),
                  op.getattr(1 << FATTR4_ACL), op.verify({FATTR4_ACL: changed}),
                  op.write(sid, 0, FILE_SYNC4, b"acl-setattr")], "acl_two_setattrs_getattrs_verify")
    check_named_acl(res.resarray[2].obj_attributes[FATTR4_ACL], acl)
    check_named_acl(res.resarray[4].obj_attributes[FATTR4_ACL], changed)
    require(p.read(fh, sid) == b"acl-setattr", "ACL input sequence lost WRITE suffix")

    name = b"acl-open"
    res = p.call([op.putfh(p.directory), p.open_op(name, {FATTR4_MODE: 0o666, FATTR4_ACL: acl}),
                  op.getfh(), op.getattr(1 << FATTR4_ACL),
                  op.write(CURRENT, 0, FILE_SYNC4, b"acl-open")], "open_acl_input")
    fh, sid = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    check_named_acl(res.resarray[3].obj_attributes[FATTR4_ACL], acl)
    require(p.read(fh, sid) == b"acl-open", "OPEN ACL sequence lost WRITE suffix")

    name = b"acl-create-dir"
    res = p.call([op.putfh(p.directory),
                  op.create(createtype4(NF4DIR), name, {FATTR4_MODE: 0o777, FATTR4_ACL: acl}),
                  op.getattr(1 << FATTR4_ACL), op.verify({FATTR4_ACL: acl}), op.getfh()],
                 "create_acl_input")
    p.directories.append(name)
    check_named_acl(res.resarray[2].obj_attributes[FATTR4_ACL], acl)
    require(bool(res.resarray[-1].object), "CREATE ACL sequence lost GETFH suffix")


def test_acl_reply_budget(p):
    _, fh, sid = p.create("acl-reply-budget")
    acl = [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, 0x1F01FF, b"OWNER@")]
    acl += [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, ACE4_READ_DATA,
                   str(13000 + i).encode()) for i in range(179)]
    original = b"budget-veto-preserves-data"
    p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl}),
            op.write(sid, 0, FILE_SYNC4, original)])
    # Each staged ACL response reserves 4096 + 272 bytes per ACE. At 180 ACEs,
    # two fit alongside fixed successor reservations and the 8192-byte floor;
    # the third must fail before the following write is dispatched.
    res = p.call([op.putfh(fh), op.getattr(1 << FATTR4_ACL),
                  op.getattr(1 << FATTR4_ACL), op.getattr(1 << FATTR4_ACL),
                  op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "acl_reply_budget", NFS4ERR_RESOURCE)
    require(len(res.resarray) == 4 and res.resarray[-1].resop == OP_GETATTR,
            "oversized ACL result did not fail at the third GETATTR: "
            f"received {[nfs_opnum4[item.resop] for item in res.resarray]}")
    check_named_acl(res.resarray[1].obj_attributes[FATTR4_ACL], acl)
    check_named_acl(res.resarray[2].obj_attributes[FATTR4_ACL], acl)
    require(p.read(fh, sid) == original, "ACL reply budget failure allowed its WRITE suffix")


def check_read_plus(result, kind, offset, value, eof):
    require(result.rpr_eof == eof, f"READ_PLUS incorrect EOF: {result!r}")
    contents = result.rpr_contents
    if kind is None:
        require(contents == [], f"READ_PLUS at EOF returned segments: {contents!r}")
        return
    require(len(contents) == 1 and contents[0].rpc_content == kind,
            f"READ_PLUS returned incorrect segment kind: {contents!r}")
    if kind == NFS4_CONTENT_DATA:
        require(contents[0].rpc_data.d_offset == offset and contents[0].rpc_data.d_data == value,
                f"READ_PLUS data differs: {contents[0]!r}")
    else:
        require(contents[0].rpc_hole.di_offset == offset and contents[0].rpc_hole.di_length == value,
                f"READ_PLUS hole differs: {contents[0]!r}")


def test_read_plus(p):
    if p.args.minor < 2:
        return
    _, fh, sid = p.create("read-plus-sparse")
    # The wrapper uses default memfs (64 KiB blocks). The middle block has no
    # allocation; the last four bytes of the file are data in the third block.
    tail = 128 * 1024
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"DATA"),
            op.write(sid, tail, FILE_SYNC4, b"TAIL")])
    cases = [
        ("data", 0, 4, NFS4_CONTENT_DATA, b"DATA", False),
        ("hole", 64 * 1024, 32, NFS4_CONTENT_HOLE, 32, False),
        ("tail", tail, 4, NFS4_CONTENT_DATA, b"TAIL", True),
        ("eof", tail + 4, 16, None, None, True),
        ("zero", 0, 0, None, None, False),
    ]
    for name, offset, count, kind, value, eof in cases:
        res = p.call([op.putfh(fh), op.read_plus(sid, offset, count),
                      op.write(sid, 8, FILE_SYNC4, name.encode())], f"read_plus_{name}")
        check_read_plus(res.resarray[1], kind, offset, value, eof)
        require(p.read(fh, sid)[8:8 + len(name)] == name.encode(),
                f"READ_PLUS {name} did not run its WRITE suffix")

    res = p.call([op.putfh(fh), op.read_plus(sid, 0, 4),
                  op.read_plus(sid, 64 * 1024, 32), op.read_plus(sid, tail, 4),
                  op.read_plus(sid, tail + 4, 16), op.getfh()], "read_plus_multiple")
    for result, (_, offset, _, kind, value, eof) in zip(res.resarray[1:5], cases):
        check_read_plus(result, kind, offset, value, eof)
    require(res.resarray[-1].object == fh, "multiple READ_PLUS operations lost GETFH suffix")

    name = b"open-read-plus"
    res = p.call([op.putfh(p.directory), p.open_op(name),
                  op.write(CURRENT, 0, FILE_SYNC4, b"new"),
                  op.read_plus(CURRENT, 0, 3), op.getfh()], "open_read_plus")
    fh, sid = res.resarray[-1].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    check_read_plus(res.resarray[3], NFS4_CONTENT_DATA, 0, b"new", True)
    _, _, other_sid = p.create("read-plus-other")
    res = p.call([op.putfh(fh), op.read_plus(other_sid, 0, 3),
                  op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "read_plus_mismatched", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 2 and p.read(fh, sid) == b"new",
            "mismatched READ_PLUS permitted its WRITE suffix")


def test_copy_clone(p):
    if p.args.minor < 2:
        return
    _, source, ss = p.create("copy-clone-source")
    _, dest, ds = p.create("copy-clone-dest")
    payload = bytes(range(256)) * 16
    baseline = b"d" * 4096
    p.call([op.putfh(source), op.write(ss, 0, FILE_SYNC4, payload)])
    p.call([op.putfh(dest), op.write(ds, 0, FILE_SYNC4, baseline)])
    prefix = [op.putfh(source), op.savefh(), op.putfh(dest)]
    res = p.call(prefix + [op.copy(ss, ds, 13, 5, 37, True, True, []),
                           op.write(ds, 42, FILE_SYNC4, b"!")], "copy_saved_source")
    require(res.resarray[3].cr_response.wr_count == 37, "COPY count is incorrect")
    require(p.read(dest, ds) == baseline[:5] + payload[13:50] + b"!" + baseline[43:],
            "COPY offsets or following WRITE are incorrect")
    res = p.call(prefix + [op.copy(ss, ds, 0, 0, 0, True, True, []), op.getfh()], "copy_to_eof")
    require(res.resarray[3].cr_response.wr_count == len(payload) and p.read(dest, ds) == payload,
            "zero-count COPY did not copy through EOF")

    p.call(prefix + [op.clone(ss, ds, 0, 0, 4096),
                     op.write(ds, 7, FILE_SYNC4, b"!!")], "clone_saved_source")
    require(p.read(dest, ds) == payload[:7] + b"!!" + payload[9:],
            "CLONE or following WRITE produced incorrect bytes")
    require(p.read(source, ss) == payload, "CLONE destination write modified its source")

    stable = p.read(dest, ds)
    for kind in ("copy", "clone"):
        for side in ("source", "destination"):
            src_sid, dst_sid = (ds, ds) if side == "source" else (ss, ss)
            operation = (op.copy(src_sid, dst_sid, 0, 0, 4096, True, True, [])
                         if kind == "copy" else op.clone(src_sid, dst_sid, 0, 0, 4096))
            res = p.call(prefix + [operation, op.write(ds, 0, FILE_SYNC4, b"BAD")],
                         f"{kind}_mismatched_{side}", NFS4ERR_BAD_STATEID)
            require(len(res.resarray) == 4, f"{kind} mismatch did not stop before suffix")
            require(p.read(source, ss) == payload and p.read(dest, ds) == stable,
                    f"{kind} mismatched {side} changed data")

    anonymous = stateid4(0, b"\0" * 12)
    res = p.call(prefix + [op.copy(anonymous, anonymous, 1, 11, 17, True, True, []),
                           op.getfh()], "copy_anonymous_both")
    require(res.resarray[3].cr_response.wr_count == 17,
            "anonymous COPY did not resolve both runtime handles")
    stable = stable[:11] + payload[1:18] + stable[28:]
    require(p.read(dest, ds) == stable and res.resarray[-1].object == dest,
            "anonymous COPY selected the wrong file or lost the destination cursor")

    res = p.call([op.putfh(source), op.savefh(), op.putfh(source),
                  op.copy(ss, ss, 0, 0, 1, True, True, []),
                  op.write(ss, 0, FILE_SYNC4, b"BAD")], "copy_same_file", NFS4ERR_INVAL)
    require(len(res.resarray) == 4 and p.read(source, ss) == payload,
            "same-file COPY rejection allowed mutation")
    res = p.call(prefix + [op.copy(ss, ds, len(payload) + 1, 0, 1, True, True, []),
                           op.write(ds, 0, FILE_SYNC4, b"BAD")],
                 "copy_source_beyond_eof", NFS4ERR_INVAL)
    require(len(res.resarray) == 4 and p.read(dest, ds) == stable,
            "COPY beyond source EOF allowed mutation")

    _, third, ts = p.create("copy-switched-third")
    # COPY must preserve both cursors for a following SAVEFH. Then switch to
    # B->C and RESTOREFH to B; both dependencies remain in this same attempt.
    res = p.call(prefix + [op.copy(ss, ds, 0, 0, 32, True, True, []),
                           op.savefh(), op.putfh(third),
                           op.copy(ds, ts, 0, 0, 32, True, True, []),
                           op.restorefh(), op.write(ds, 32, FILE_SYNC4, b"B"), op.getfh()],
                 "copy_two_switched_pairs")
    require(res.resarray[3].cr_response.wr_count == 32 and
            res.resarray[6].cr_response.wr_count == 32,
            "switched COPY operations returned incorrect counts")
    require(p.read(third, ts) == payload[:32], "second COPY did not read its new saved source")
    require(p.read(dest, ds) == payload[:32] + b"B" + stable[33:] and
            res.resarray[-1].object == dest and p.read(source, ss) == payload,
            "COPY or RESTOREFH failed to preserve the switched current/saved cursors")


def test_close_and_prefix(p):
    name = b"open-write-close"
    res = p.call([op.putfh(p.directory), p.open_op(name),
                  op.write(CURRENT, 0, FILE_SYNC4, b"closed"), op.getfh(),
                  op.close(0, CURRENT), op.getattr(1 << FATTR4_SIZE)], "open_write_close")
    fh, sid = res.resarray[3].object, res.resarray[1].stateid
    p.files.append((name, fh, None))
    require(res.resarray[-1].obj_attributes[FATTR4_SIZE] == 6 and
            p.read(fh, ANONYMOUS) == b"closed", "OPEN+WRITE+CLOSE lost accepted data or current FH")
    p.call([op.putfh(fh), op.read(sid, 0, 6)], expected=NFS4ERR_BAD_STATEID)

    name = b"close-clears-current"
    res = p.call([op.putfh(p.directory), p.open_op(name), op.getfh(),
                  op.write(CURRENT, 0, FILE_SYNC4, b"kept"), op.close(0, CURRENT),
                  op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                 "close_invalidates_current", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 6, "current stateid did not fail at the WRITE after CLOSE")
    fh, sid = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, None))
    require(p.read(fh, ANONYMOUS) == b"kept", "post-CLOSE current-stateid WRITE changed data")
    p.call([op.putfh(fh), op.read(sid, 0, 4)], expected=NFS4ERR_BAD_STATEID)

    _, fh, sid = p.create("close-explicit-veto")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"explicit")])
    res = p.call([op.putfh(fh), op.close(0, sid), op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "close_invalidates_explicit", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 3, "explicit stateid did not fail immediately after CLOSE")
    p.mark_closed(fh)
    require(p.read(fh, ANONYMOUS) == b"explicit", "explicit closed-stateid WRITE changed data")

    _, fh, sid = p.create("close-getattr")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"metadata")])
    res = p.call([op.putfh(fh), op.close(0, sid), op.getattr(1 << FATTR4_SIZE)],
                 "close_existing_getattr")
    p.mark_closed(fh)
    require(res.resarray[-1].obj_attributes[FATTR4_SIZE] == 8,
            "CLOSE lost the FH required by the following GETATTR")

    name = b"open-accepted-prefix"
    res = p.call([op.putfh(p.directory), p.open_op(name), op.getfh(),
                  op.write(CURRENT, 0, FILE_SYNC4, b"prefix"),
                  op.verify({FATTR4_SIZE: 999}), op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                 "open_prefix_error", NFS4ERR_NOT_SAME)
    require(len(res.resarray) == 5, "VERIFY did not stop the OPEN prefix's suffix")
    fh, sid = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    require(p.read(fh, sid) == b"prefix", "later error discarded accepted OPEN state/data")

    res = p.call([op.putfh(fh), op.close(0, sid), op.verify({FATTR4_SIZE: 999}),
                  op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")],
                 "close_prefix_error", NFS4ERR_NOT_SAME)
    require(len(res.resarray) == 3, "VERIFY did not stop the CLOSE prefix's suffix")
    p.mark_closed(fh)
    p.call([op.putfh(fh), op.read(sid, 0, 6)], expected=NFS4ERR_BAD_STATEID)
    require(p.read(fh, ANONYMOUS) == b"prefix", "later error undid CLOSE or allowed suffix mutation")


def test_multiple_opens(p):
    for close_both in (False, True):
        suffix = b"closed" if close_both else b"live"
        a, b = b"multi-open-a-" + suffix, b"multi-open-b-" + suffix
        operations = [op.putfh(p.directory), p.open_op(a), op.getfh(), op.savefh(),
                      op.write(CURRENT, 0, FILE_SYNC4, b"A"),
                      op.putfh(p.directory), p.open_op(b), op.getfh(),
                      op.write(CURRENT, 0, FILE_SYNC4, b"B")]
        if close_both:
            operations.append(op.close(0, CURRENT))
        operations += [op.restorefh(), op.write(CURRENT, 1, FILE_SYNC4, b"+")]
        if close_both:
            operations.append(op.close(0, CURRENT))
        operations.append(op.getfh())
        res = p.call(operations, "multiple_opens_close_both" if close_both else "multiple_opens_saved_state")
        fa, sa = res.resarray[2].object, res.resarray[1].stateid
        fb, sb = res.resarray[7].object, res.resarray[6].stateid
        p.files.extend([(a, fa, None if close_both else sa), (b, fb, None if close_both else sb)])
        require(p.read(fa, ANONYMOUS if close_both else sa) == b"A+" and
                p.read(fb, ANONYMOUS if close_both else sb) == b"B" and
                res.resarray[-1].object == fa,
                "multiple OPENs lost a tentative handle, stateid, or saved cursor")
        if close_both:
            p.call([op.putfh(fa), op.read(sa, 0, 2)], expected=NFS4ERR_BAD_STATEID)
            p.call([op.putfh(fb), op.read(sb, 0, 1)], expected=NFS4ERR_BAD_STATEID)


def test_retryable_open_reads(p):
    # Only these following read-only compounds are eligible for the optional
    # finish-conflict interposer. Fixture creation/writes finish separately.
    name, fh, sid = p.create("retry-open-read-close")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"retry-safe")])
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"retry-reader"),
                  op.read(CURRENT, 0, 10), op.close(0, CURRENT),
                  op.getattr(1 << FATTR4_SIZE)], "retry_open_read_close")
    require(res.resarray[2].data == b"retry-safe" and
            res.resarray[-1].obj_attributes[FATTR4_SIZE] == 10,
            "read-only OPEN/CLOSE retry lost data or metadata")
    closed = res.resarray[1].stateid
    p.call([op.putfh(fh), op.read(closed, 0, 10)], expected=NFS4ERR_BAD_STATEID)

    a, fa, sa = p.create("retry-multiple-a")
    b, fb, sb = p.create("retry-multiple-b")
    p.call([op.putfh(fa), op.write(sa, 0, FILE_SYNC4, b"first")])
    p.call([op.putfh(fb), op.write(sb, 0, FILE_SYNC4, b"second")])
    res = p.call([op.putfh(p.directory), p.open_op(a, create=False, owner=b"retry-reader-a"),
                  op.savefh(), op.read(CURRENT, 0, 5), op.putfh(p.directory),
                  p.open_op(b, create=False, owner=b"retry-reader-b"), op.read(CURRENT, 0, 6),
                  op.close(0, CURRENT), op.restorefh(), op.read(CURRENT, 0, 5),
                  op.close(0, CURRENT), op.getfh()], "retry_multiple_opens")
    require(res.resarray[3].data == b"first" and res.resarray[6].data == b"second" and
            res.resarray[9].data == b"first" and res.resarray[-1].object == fa,
            "retry failed to reset multiple OPENs, CLOSE marks, read buffers, or saved state")
    p.call([op.putfh(fa), op.read(res.resarray[1].stateid, 0, 5)], expected=NFS4ERR_BAD_STATEID)
    p.call([op.putfh(fb), op.read(res.resarray[5].stateid, 0, 6)], expected=NFS4ERR_BAD_STATEID)

    _, fh, sid = p.create("retry-close-read-veto")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"closed-prefix")])
    # Execution fails after a successful CLOSE. A finish conflict must retry
    # the attempt before publishing that prefix, then retain the same wire
    # error and accepted CLOSE when the second finish succeeds.
    res = p.call([op.putfh(fh), op.close(0, sid), op.read(sid, 0, 13)],
                 "retry_close_read_veto", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 3, "read-only CLOSE prefix did not reject the closed-stateid READ")
    p.mark_closed(fh)
    require(p.read(fh, ANONYMOUS) == b"closed-prefix", "retry changed accepted CLOSE-prefix data")

    _, fh, sid = p.create("retry-read-before-close")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"read-before-close")])
    res = p.call([op.putfh(fh), op.read(sid, 0, 17), op.close(0, sid),
                  op.getattr(1 << FATTR4_SIZE)], "retry_read_before_close")
    p.mark_closed(fh)
    require(res.resarray[1].data == b"read-before-close" and
            res.resarray[-1].obj_attributes[FATTR4_SIZE] == 17,
            "a preheld CLOSE reservation blocked preceding READ or lost its reply on retry")
    p.call([op.putfh(fh), op.read(sid, 0, 17)], expected=NFS4ERR_BAD_STATEID)


def test_close_releases_deny(p):
    name = b"close-existing-deny"
    res = p.call([op.putfh(p.directory), p.open_op(name, deny=OPEN4_SHARE_DENY_WRITE),
                  op.getfh(), op.write(CURRENT, 0, FILE_SYNC4, b"before")])
    fh, old_sid = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, old_sid))
    p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"blocked-before-close")],
           expected=NFS4ERR_SHARE_DENIED)

    res = p.call([op.putfh(fh), op.close(0, old_sid), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=b"reopened-after-close"), op.getfh(),
                  op.write(CURRENT, 0, FILE_SYNC4, b"after!")], "close_existing_deny_then_open")
    new_sid = res.resarray[3].stateid
    p.replace_stateid(fh, new_sid)
    require(res.resarray[4].object == fh and p.read(fh, new_sid) == b"after!",
            "CLOSE did not remove an existing deny for the following fresh-owner OPEN")
    p.call([op.putfh(fh), op.read(old_sid, 0, 6)], expected=NFS4ERR_BAD_STATEID)

    name, fh, fixture_sid = p.create("close-private-deny")
    p.call([op.putfh(fh), op.write(fixture_sid, 0, FILE_SYNC4, b"before"),
            op.close(0, fixture_sid)])
    p.mark_closed(fh)
    res = p.call([op.putfh(p.directory),
                  p.open_op(name, create=False, owner=b"private-deny-owner", deny=OPEN4_SHARE_DENY_WRITE),
                  op.close(0, CURRENT), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=b"private-reopened-owner"), op.getfh(),
                  op.write(CURRENT, 0, FILE_SYNC4, b"after!")], "close_private_deny_then_open")
    closed_sid, new_sid = res.resarray[1].stateid, res.resarray[4].stateid
    p.replace_stateid(fh, new_sid)
    require(res.resarray[5].object == fh and p.read(fh, new_sid) == b"after!",
            "private OPEN deny survived CLOSE and blocked the following fresh-owner OPEN")
    p.call([op.putfh(fh), op.read(closed_sid, 0, 6)], expected=NFS4ERR_BAD_STATEID)


def test_unlinked_open_lifetime(p):
    for close_inside in (False, True):
        label = "private_close_unlinked_stale" if close_inside else "private_open_unlinked_pin"
        name, fh, fixture_sid = p.create(label)
        p.call([op.putfh(fh), op.write(fixture_sid, 0, FILE_SYNC4, b"unlinked"),
                op.close(0, fixture_sid)])
        p.mark_closed(fh)
        operations = [op.putfh(p.directory),
                      p.open_op(name, create=False, owner=b"unlinked-" + name), op.savefh(),
                      op.putfh(p.directory), op.remove(name), op.putfh(fh),
                      op.getattr(1 << FATTR4_SIZE), op.restorefh()]
        if close_inside:
            operations += [op.close(0, CURRENT), op.putfh(fh), op.getattr(1 << FATTR4_SIZE)]
        else:
            operations += [op.read(CURRENT, 0, 8)]
        res = p.call(operations, label, NFS4ERR_STALE if close_inside else NFS4_OK)
        p.files = [entry for entry in p.files if entry[1] != fh]
        require(res.resarray[6].obj_attributes[FATTR4_SIZE] == 8,
                "private OPEN did not pin the unlinked file for a following PUTFH")
        if close_inside:
            require(len(res.resarray) == 10, "last CLOSE did not make the following PUTFH stale")
        else:
            require(res.resarray[-1].data == b"unlinked", "private OPEN lost unlinked file data")
            p.call([op.putfh(fh), op.close(0, res.resarray[1].stateid)])
        p.call([op.putfh(fh)], expected=NFS4ERR_STALE)

    name, fh, sid = p.create("existing-close-unlinked-stale")
    p.call([op.putfh(p.directory), op.remove(name)])
    p.files = [entry for entry in p.files if entry[1] != fh]
    res = p.call([op.putfh(fh), op.close(0, sid), op.putfh(fh),
                  op.getattr(1 << FATTR4_SIZE)], "existing_close_unlinked_stale", NFS4ERR_STALE)
    require(len(res.resarray) == 3, "existing last CLOSE did not veto stale PUTFH's suffix")
    p.call([op.putfh(fh)], expected=NFS4ERR_STALE)


def test_anonymous_io_after_close(p):
    for write in (False, True):
        label = "anonymous_write_after_close" if write else "anonymous_read_after_close"
        name = label.encode()
        res = p.call([op.putfh(p.directory), p.open_op(name, deny=OPEN4_SHARE_DENY_BOTH),
                      op.getfh(), op.write(CURRENT, 0, FILE_SYNC4, b"before")])
        fh, sid = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((name, fh, sid))
        p.call([op.putfh(fh), op.read(ANONYMOUS, 0, 6)], expected=NFS4ERR_LOCKED)
        operations = [op.putfh(fh), op.close(0, sid)]
        if write:
            operations += [op.write(ANONYMOUS, 0, FILE_SYNC4, b"after!")]
        operations += [op.read(ANONYMOUS, 0, 6), op.getattr(1 << FATTR4_SIZE)]
        res = p.call(operations, label)
        p.mark_closed(fh)
        require(res.resarray[-2].data == (b"after!" if write else b"before") and
                res.resarray[-1].obj_attributes[FATTR4_SIZE] == 6,
                "accepted private CLOSE did not release its deny for anonymous I/O")

    for write in (False, True):
        label = "anonymous_write_unrelated_deny" if write else "anonymous_read_unrelated_deny"
        name = label.encode()
        access = OPEN4_SHARE_ACCESS_READ if write else OPEN4_SHARE_ACCESS_WRITE
        deny = OPEN4_SHARE_DENY_WRITE if write else OPEN4_SHARE_DENY_READ
        res = p.call([op.putfh(p.directory), p.open_op(name, access=access, deny=deny), op.getfh()])
        fh, closing_sid = res.resarray[2].object, res.resarray[1].stateid
        res = p.call([op.putfh(p.directory), p.open_op(name, create=False,
                      owner=b"remaining-" + name, access=access, deny=deny)])
        remaining_sid = res.resarray[-1].stateid
        p.files.append((name, fh, remaining_sid))
        io = op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD") if write else op.read(ANONYMOUS, 0, 6)
        res = p.call([op.putfh(fh), op.close(0, closing_sid), io,
                      op.getattr(1 << FATTR4_SIZE)], label, NFS4ERR_LOCKED)
        require(len(res.resarray) == 3, "CLOSE incorrectly excluded another owner's deny")
        check = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE)])
        require(check.resarray[-1].obj_attributes[FATTR4_SIZE] == 0,
                "denied anonymous I/O mutated the file")
        p.call([op.putfh(fh), op.read(closing_sid, 0, 1)], expected=NFS4ERR_BAD_STATEID)


def test_same_owner_closes(p):
    handles = []
    owner = b"same-owner-close-group"
    for label in (b"same-owner-close-a", b"same-owner-close-b"):
        res = p.call([op.putfh(p.directory), p.open_op(label, owner=owner),
                      op.getfh(), op.write(CURRENT, 0, FILE_SYNC4, label)])
        fh, sid = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((label, fh, sid))
        handles.append((label, fh, sid))
    (a, fa, sa), (b, fb, sb) = handles
    res = p.call([op.putfh(fa), op.read(sa, 0, len(a)), op.close(0, sa),
                  op.putfh(fb), op.read(sb, 0, len(b)), op.close(0, sb),
                  op.getattr(1 << FATTR4_SIZE)], "same_owner_multiple_closes")
    p.mark_closed(fa)
    p.mark_closed(fb)
    require(res.resarray[1].data == a and res.resarray[4].data == b and
            res.resarray[-1].obj_attributes[FATTR4_SIZE] == len(b),
            "grouped same-owner CLOSE reservations blocked preceding I/O or lost the current FH")
    p.call([op.putfh(fa), op.read(sa, 0, 1)], expected=NFS4ERR_BAD_STATEID)
    p.call([op.putfh(fb), op.read(sb, 0, 1)], expected=NFS4ERR_BAD_STATEID)

    _, fh, sid = p.create("metadata-after-close")
    res = p.call([op.putfh(fh), op.close(0, sid), op.setattr(ANONYMOUS, {FATTR4_MODE: 0o640}),
                  op.getattr(1 << FATTR4_MODE)], "metadata_setattr_after_close")
    p.mark_closed(fh)
    require(res.resarray[-1].obj_attributes[FATTR4_MODE] == 0o640,
            "CLOSE unnecessarily invalidated the filehandle for metadata SETATTR")


def test_anonymous_private_deny(p):
    for resize in (False, True):
        label = "private_deny_anonymous_size" if resize else "private_deny_anonymous_read"
        name, fh, fixture = p.create(label)
        p.call([op.putfh(fh), op.write(fixture, 0, FILE_SYNC4, b"unchanged"), op.close(0, fixture)])
        p.mark_closed(fh)
        deny = OPEN4_SHARE_DENY_WRITE if resize else OPEN4_SHARE_DENY_READ
        io = op.setattr(ANONYMOUS, {FATTR4_SIZE: 0}) if resize else op.read(ANONYMOUS, 0, 9)
        res = p.call([op.putfh(p.directory), p.open_op(name, create=False,
                      owner=b"private-" + name, deny=deny), io,
                      op.write(CURRENT, 0, FILE_SYNC4, b"BAD")], label, NFS4ERR_LOCKED)
        sid = res.resarray[1].stateid
        p.replace_stateid(fh, sid)
        require(len(res.resarray) == 3 and p.read(fh, sid) == b"unchanged",
                "anonymous I/O ignored private OPEN's deny or allowed a mutation suffix")


def test_anonymous_v42_after_close(p):
    if p.args.minor < 2:
        return
    handles = []
    for name in (b"anonymous-v42-source", b"anonymous-v42-dest"):
        data = b"source-data" if name.endswith(b"source") else b"destination"
        res = p.call([op.putfh(p.directory), p.open_op(name, deny=OPEN4_SHARE_DENY_BOTH),
                      op.getfh(), op.write(CURRENT, 0, FILE_SYNC4, data)])
        fh, sid = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((name, fh, sid))
        handles.append((name, fh, sid))
    (_, source, ss), (_, dest, ds) = handles
    res = p.call([op.putfh(source), op.close(0, ss), op.read_plus(ANONYMOUS, 0, 11),
                  op.getattr(1 << FATTR4_SIZE)], "anonymous_read_plus_after_close")
    p.mark_closed(source)
    check_read_plus(res.resarray[2], NFS4_CONTENT_DATA, 0, b"source-data", True)

    # Reopen the source so both source and destination denies are released
    # inside the COPY compound, including native-copy backend paths.
    res = p.call([op.putfh(p.directory), p.open_op(handles[0][0], create=False,
                  owner=b"anonymous-copy-source", deny=OPEN4_SHARE_DENY_BOTH)], retry_delay=True)
    ss = res.resarray[-1].stateid
    p.replace_stateid(source, ss)
    res = p.call([op.putfh(source), op.close(0, ss), op.savefh(), op.putfh(dest),
                  op.close(0, ds), op.copy(ANONYMOUS, ANONYMOUS, 0, 0, 11, True, True, []),
                  op.getattr(1 << FATTR4_SIZE)], "anonymous_copy_after_both_closes")
    p.mark_closed(source)
    p.mark_closed(dest)
    require(res.resarray[5].cr_response.wr_count == 11 and p.read(dest, ANONYMOUS) == b"source-data",
            "anonymous COPY did not exclude its privately closed source/destination claims")

    name, source, fixture = p.create("anonymous-copy-private-deny")
    p.call([op.putfh(source), op.write(fixture, 0, FILE_SYNC4, b"private-source"),
            op.close(0, fixture)])
    p.mark_closed(source)
    _, dest, ds = p.create("anonymous-copy-denied-dest")
    p.call([op.putfh(dest), op.write(ds, 0, FILE_SYNC4, b"unchanged")])
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False,
                  owner=b"anonymous-copy-private-owner", deny=OPEN4_SHARE_DENY_READ),
                  op.savefh(), op.putfh(dest),
                  op.copy(ANONYMOUS, ds, 0, 0, 9, True, True, []),
                  op.write(ds, 0, FILE_SYNC4, b"BAD")],
                 "anonymous_copy_private_deny", NFS4ERR_LOCKED)
    p.replace_stateid(source, res.resarray[1].stateid)
    require(len(res.resarray) == 5 and p.read(dest, ds) == b"unchanged",
            "anonymous COPY bypassed an unpublished OPEN deny or executed its WRITE suffix")


def test_secinfo_continuation(p):
    name, fh, sid = p.create("secinfo-continuation")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"security")])
    res = p.call([op.putfh(p.directory), op.secinfo(name), op.getattr(1 << FATTR4_SIZE),
                  op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "secinfo_consumed_fh_veto", NFS4ERR_NOFILEHANDLE)
    require(len(res.resarray) == 3 and p.read(fh, sid) == b"security",
            "SECINFO failed to consume the current FH or allowed a suffix mutation")
    res = p.call([op.putfh(p.directory), op.secinfo(name), op.putfh(fh), op.read(sid, 0, 8)],
                 "secinfo_putfh_continuation")
    require(res.resarray[-1].data == b"security", "PUTFH did not restore the cursor after SECINFO")
    res = p.call([op.putfh(fh), op.savefh(), op.putfh(p.directory), op.secinfo(name),
                  op.restorefh(), op.read(sid, 0, 8)], "secinfo_restorefh_continuation")
    require(res.resarray[-1].data == b"security", "SECINFO incorrectly consumed the saved FH")


def test_saved_fh_through_stateid(p):
    name, source, ss = p.create("inherited-saved-source")
    _, dest, ds = p.create("inherited-saved-dest")
    payload = b"inherited-source" * 256
    p.call([op.putfh(source), op.write(ss, 0, FILE_SYNC4, payload)])
    # A protocol-only checkpoint preserves both cursors in the same VFS run.
    res = p.call([op.putfh(source), op.savefh(), op.test_stateid([ss]),
                  op.putfh(dest), op.restorefh(), op.read(ss, 0, 32)],
                 "saved_fh_through_stateid")
    require(res.resarray[2].tsr_status_codes == [NFS4_OK] and
            res.resarray[-1].data == payload[:32], "TEST_STATEID lost the saved FH")
    res = p.call([op.putfh(p.directory), op.savefh(), op.secinfo(name), op.test_stateid([ss]),
                  op.restorefh(), op.getattr(1 << FATTR4_TYPE)],
                 "saved_fh_without_current_through_stateid")
    require(res.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4DIR,
            "TEST_STATEID lost saved FH when SECINFO consumed current FH")
    if p.args.minor == 2:
        for clone in (False, True):
            transfer = (op.clone(ss, ds, 0, 0, 4096) if clone else
                        op.copy(ss, ds, 0, 0, 4096, True, True, []))
            label = "clone_inherited_saved_fh" if clone else "copy_inherited_saved_fh"
            res = p.call([op.putfh(source), op.savefh(), op.test_stateid([ss]),
                          op.putfh(dest), transfer, op.getfh()], label)
            require(res.resarray[-1].object == dest and p.read(dest, ds) == payload,
                    "COPY/CLONE did not use the inherited source and preserve the current destination")


def coalesced_fixture(p, label, owner, access=OPEN4_SHARE_ACCESS_READ, deny=OPEN4_SHARE_DENY_NONE):
    name, fh, fixture = p.create(label)
    p.call([op.putfh(fh), op.write(fixture, 0, FILE_SYNC4, b"before"), op.close(0, fixture)])
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner, access=access, deny=deny)])
    sid = res.resarray[-1].stateid
    p.replace_stateid(fh, sid)
    return name, fh, sid


def check_open_version(previous, current, message):
    require(current.other == previous.other and current.seqid == previous.seqid + 1,
            f"{message}: previous={previous!r}, current={current!r}")


def test_coalesced_open_upgrade(p):
    owner = b"coalesced-upgrade-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-upgrade", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE), op.read(CURRENT, 0, 6),
                  op.write(CURRENT, 0, FILE_SYNC4, b"after!"), op.close(0, CURRENT),
                  op.getattr(1 << FATTR4_SIZE)], "coalesced_upgrade_io_close")
    p.mark_closed(fh)
    check_open_version(initial, res.resarray[1].stateid, "OPEN upgrade changed identity or advanced twice")
    require(res.resarray[2].data == b"before" and p.read(fh, ANONYMOUS) == b"after!",
            "coalesced OPEN failed to combine read/write access before I/O and CLOSE")

    owner = b"coalesced-repeat-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-repeat", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.read(CURRENT, 0, 6), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_WRITE),
                  op.write(CURRENT, 0, FILE_SYNC4, b"repeat"), op.read(CURRENT, 0, 6)],
                 "coalesced_repeated_opens")
    first, second = res.resarray[1].stateid, res.resarray[4].stateid
    p.replace_stateid(fh, second)
    check_open_version(initial, first, "first repeated OPEN version")
    check_open_version(first, second, "second repeated OPEN version")
    require(res.resarray[2].data == b"before" and res.resarray[-1].data == b"repeat",
            "repeated OPEN reply staging or upgraded handle lost per-op data")

    # WRITE/NONE was added by this compound. A subsequent downgrade to WRITE
    # must see that accepted event, rather than only the original READ event.
    down = p.call([op.putfh(fh), op.open_downgrade(second, 0, OPEN4_SHARE_ACCESS_WRITE,
                                                OPEN4_SHARE_DENY_NONE)])
    write_sid = down.resarray[-1].open_stateid
    p.replace_stateid(fh, write_sid)
    p.call([op.putfh(fh), op.write(write_sid, 0, FILE_SYNC4, b"write!")])
    p.call([op.putfh(fh), op.read(write_sid, 0, 6)], expected=NFS4ERR_OPENMODE)

    name, fh, fixture = p.create("coalesced-fresh-repeat")
    p.call([op.putfh(fh), op.close(0, fixture)])
    p.mark_closed(fh)
    owner = b"coalesced-fresh-repeat-owner"
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.getfh(), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_WRITE),
                  op.write(CURRENT, 0, FILE_SYNC4, b"fresh!"), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_READ),
                  op.read(CURRENT, 0, 6), op.close(0, CURRENT)], "coalesced_fresh_repeated_opens")
    first, second, third = [res.resarray[index].stateid for index in (1, 4, 7)]
    require(first.seqid == 1 and res.resarray[2].object == fh, "fresh coalesced OPEN initial identity")
    check_open_version(first, second, "fresh second OPEN version")
    check_open_version(second, third, "fresh third OPEN version")
    require(res.resarray[8].data == b"fresh!" and p.read(fh, ANONYMOUS) == b"fresh!",
            "fresh OPEN aliases failed to share upgraded access or CLOSE once")


def test_coalesced_downgrade_history(p):
    owner = b"coalesced-history-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-history", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE, deny=OPEN4_SHARE_DENY_WRITE),
                  op.read(CURRENT, 0, 6)], "coalesced_downgrade_combo_history")
    current = res.resarray[1].stateid
    p.replace_stateid(fh, current)
    check_open_version(initial, current, "combination-history OPEN version")
    require(res.resarray[2].data == b"before", "coalesced share union lost READ access")
    # READ/NONE + WRITE/WRITE can produce BOTH/WRITE, but READ/WRITE is
    # not the union of any subset of these complete (access, deny) events.
    p.call([op.putfh(fh), op.open_downgrade(current, 0, OPEN4_SHARE_ACCESS_READ,
                                          OPEN4_SHARE_DENY_WRITE)], expected=NFS4ERR_INVAL)
    res = p.call([op.putfh(fh), op.open_downgrade(current, 0, OPEN4_SHARE_ACCESS_WRITE,
                                                OPEN4_SHARE_DENY_WRITE)])
    downgraded = res.resarray[-1].open_stateid
    p.replace_stateid(fh, downgraded)
    check_open_version(current, downgraded, "accepted downgrade version after failed combination")
    p.call([op.putfh(fh), op.write(downgraded, 0, FILE_SYNC4, b"after!")])
    p.call([op.putfh(fh), op.read(downgraded, 0, 6)], expected=NFS4ERR_OPENMODE)


def test_coalesced_same_owner_files(p):
    owner = b"coalesced-files-owner"
    a, fa, sa = coalesced_fixture(p, "coalesced-files-a", owner)
    b, fb, sb = coalesced_fixture(p, "coalesced-files-b", owner)
    res = p.call([op.putfh(p.directory), p.open_op(a, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE), op.savefh(), op.write(CURRENT, 0, FILE_SYNC4, b"first!"),
                  op.putfh(p.directory), p.open_op(b, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE), op.write(CURRENT, 0, FILE_SYNC4, b"second"),
                  op.close(0, CURRENT), op.restorefh(), op.read(CURRENT, 0, 6), op.close(0, CURRENT)],
                 "coalesced_same_owner_files")
    p.mark_closed(fa)
    p.mark_closed(fb)
    check_open_version(sa, res.resarray[1].stateid, "same-owner first-file version")
    check_open_version(sb, res.resarray[5].stateid, "same-owner second-file version")
    require(res.resarray[9].data == b"first!" and p.read(fa, ANONYMOUS) == b"first!" and
            p.read(fb, ANONYMOUS) == b"second", "coalesced OPENs confused file-specific state or saved FH")


def test_coalesced_reopen_after_close(p):
    owner = b"coalesced-close-reopen-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-close-reopen", owner)
    res = p.call([op.putfh(fh), op.close(0, initial), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner),
                  op.write(CURRENT, 0, FILE_SYNC4, b"reopen"), op.read(CURRENT, 0, 6),
                  op.close(0, CURRENT)], "coalesced_existing_close_reopen")
    p.mark_closed(fh)
    reopened = res.resarray[3].stateid
    require(reopened.seqid == 1 and reopened.other != initial.other and
            res.resarray[5].data == b"reopen" and p.read(fh, ANONYMOUS) == b"reopen",
            "same-owner OPEN after public CLOSE reused a closed identity or lost I/O access")

    name, fh, fixture = p.create("coalesced-private-close-reopen")
    p.call([op.putfh(fh), op.close(0, fixture)])
    p.mark_closed(fh)
    owner = b"coalesced-private-close-reopen-owner"
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.getfh(), op.close(0, CURRENT),
                  op.putfh(p.directory), p.open_op(name, create=False, owner=owner),
                  op.write(CURRENT, 0, FILE_SYNC4, b"fresh!"), op.read(CURRENT, 0, 6),
                  op.close(0, CURRENT)], "coalesced_private_close_reopen")
    first, reopened = res.resarray[1].stateid, res.resarray[5].stateid
    require(first.seqid == 1 and reopened.seqid == 1 and reopened.other != first.other and
            res.resarray[2].object == fh and res.resarray[7].data == b"fresh!" and
            p.read(fh, ANONYMOUS) == b"fresh!",
            "same-owner OPEN after private CLOSE coalesced with a closed state")

    owner = b"coalesced-write-read-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-write-read", owner, OPEN4_SHARE_ACCESS_WRITE)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.write(CURRENT, 0, FILE_SYNC4, b"union!"),
                  op.read(CURRENT, 0, 6), op.close(0, CURRENT)], "coalesced_write_then_read_union")
    p.mark_closed(fh)
    check_open_version(initial, res.resarray[1].stateid, "WRITE-to-READ union OPEN version")
    require(res.resarray[3].data == b"union!" and p.read(fh, ANONYMOUS) == b"union!",
            "read-share coalescing discarded previously accepted WRITE access")


def test_coalesced_saved_version(p):
    owner = b"coalesced-saved-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-saved-version", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.savefh(), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_WRITE),
                  op.restorefh(), op.read(CURRENT, 0, 6), op.getattr(1 << FATTR4_SIZE)],
                 "coalesced_saved_preupgrade_stateid")
    first, second = res.resarray[1].stateid, res.resarray[4].stateid
    p.replace_stateid(fh, second)
    check_open_version(initial, first, "saved first OPEN version snapshot")
    check_open_version(first, second, "saved second OPEN version snapshot")
    require(res.resarray[6].data == b"before" and res.resarray[-1].obj_attributes[FATTR4_SIZE] == 6 and
            p.read(fh, second) == b"before",
            "saved CURRENT did not substitute sequence zero for latest-version READ")
    require(p.read(fh, stateid4(0, initial.other)) == b"before",
            "sequence-zero explicit stateid failed to resolve accepted coalesced state")

    owner = b"coalesced-explicit-owner"
    name, fh, initial = coalesced_fixture(p, "coalesced-explicit-version", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE), op.read(initial, 0, 6),
                  op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                 "coalesced_explicit_preupgrade_stateid", NFS4ERR_OLD_STATEID)
    current = res.resarray[1].stateid
    p.replace_stateid(fh, current)
    check_open_version(initial, current, "explicit stale reference OPEN version")
    require(len(res.resarray) == 3 and p.read(fh, current) == b"before",
            "explicit pre-upgrade stateid did not veto the mutation suffix")


def test_coalesced_self_conflict(p):
    for conflict in ("access", "deny"):
        owner = b"coalesced-self-" + conflict.encode()
        access = OPEN4_SHARE_ACCESS_READ if conflict == "access" else OPEN4_SHARE_ACCESS_WRITE
        deny = OPEN4_SHARE_DENY_WRITE if conflict == "access" else OPEN4_SHARE_DENY_NONE
        name, fh, initial = coalesced_fixture(p, "coalesced-self-" + conflict, owner, access, deny)
        new_access = OPEN4_SHARE_ACCESS_WRITE if conflict == "access" else OPEN4_SHARE_ACCESS_READ
        new_deny = OPEN4_SHARE_DENY_NONE if conflict == "access" else OPEN4_SHARE_DENY_WRITE
        res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                      access=access, deny=deny), op.putfh(p.directory),
                      p.open_op(name, create=False, owner=owner, access=new_access, deny=new_deny),
                      op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                     "coalesced_self_conflict_" + conflict, NFS4ERR_SHARE_DENIED)
        accepted = res.resarray[1].stateid
        p.replace_stateid(fh, accepted)
        check_open_version(initial, accepted, "self-conflict accepted prefix OPEN version")
        require(len(res.resarray) == 4 and
                p.read(fh, accepted if access == OPEN4_SHARE_ACCESS_READ else ANONYMOUS) == b"before",
                "coalesced OPEN ignored legacy self-conflict admission or changed file data")


def test_coalesced_prefix_failure(p):
    for failure in ("access", "deny", "truncate"):
        owner = b"coalesced-prefix-" + failure.encode()
        name, fh, initial = coalesced_fixture(p, "coalesced-fail-" + failure, owner)
        other_access = OPEN4_SHARE_ACCESS_WRITE if failure == "deny" else OPEN4_SHARE_ACCESS_READ
        other_deny = OPEN4_SHARE_DENY_NONE if failure == "deny" else OPEN4_SHARE_DENY_WRITE
        other = p.call([op.putfh(p.directory), p.open_op(name, create=False,
                       owner=b"conflict-" + owner, access=other_access, deny=other_deny)])
        other_sid = other.resarray[-1].stateid
        failing = p.open_op(name, create=failure == "truncate", owner=owner,
                            access=OPEN4_SHARE_ACCESS_READ if failure == "deny" else OPEN4_SHARE_ACCESS_WRITE,
                            deny=OPEN4_SHARE_DENY_WRITE if failure == "deny" else OPEN4_SHARE_DENY_NONE,
                            attrs={FATTR4_SIZE: 0}, create_mode=UNCHECKED4)
        res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                      access=OPEN4_SHARE_ACCESS_READ), op.putfh(p.directory), failing,
                      op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                     "coalesced_prefix_failed_" + failure, NFS4ERR_SHARE_DENIED)
        accepted = res.resarray[1].stateid
        p.replace_stateid(fh, accepted)
        check_open_version(initial, accepted, "accepted prefix OPEN version")
        require(len(res.resarray) == 4 and p.read(fh, accepted) == b"before",
                "failed coalesced OPEN lost its accepted prefix or changed bytes")
        following = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                           access=OPEN4_SHARE_ACCESS_READ)])
        current = following.resarray[-1].stateid
        p.replace_stateid(fh, current)
        check_open_version(accepted, current, "failed OPEN incorrectly advanced published stateid")
        p.call([op.putfh(fh), op.close(0, other_sid)])


def test_retry_coalesced_opens(p):
    owner = b"retry-coalesced-owner"
    name, fh, initial = coalesced_fixture(p, "retry-coalesced-existing", owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.read(CURRENT, 0, 6), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_READ),
                  op.read(CURRENT, 0, 6)], "retry_coalesced_existing_opens")
    first, second = res.resarray[1].stateid, res.resarray[4].stateid
    p.replace_stateid(fh, second)
    check_open_version(initial, first, "retried first coalesced OPEN advanced twice")
    check_open_version(first, second, "retried second coalesced OPEN advanced twice")
    require(res.resarray[2].data == b"before" and res.resarray[5].data == b"before",
            "retry failed to rebuild coalesced OPEN read buffers")
    following = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                       access=OPEN4_SHARE_ACCESS_READ)])
    current = following.resarray[-1].stateid
    p.replace_stateid(fh, current)
    check_open_version(second, current, "retry advanced published coalesced OPEN state twice")


def downgrade_fixture(p, label):
    owner = b"owner-" + label.encode()
    name, fh, sid = coalesced_fixture(p, label, owner)
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_WRITE)])
    current = res.resarray[-1].stateid
    check_open_version(sid, current, "downgrade fixture history")
    p.replace_stateid(fh, current)
    return name, fh, current, owner


def test_downgrade_checkpoints(p):
    _, fh, sid, _ = downgrade_fixture(p, "downgrade-public")
    res = p.call([op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.read(CURRENT, 0, 6), op.getattr(1 << FATTR4_SIZE)],
                 "downgrade_public_read")
    current = res.resarray[1].open_stateid
    p.replace_stateid(fh, current)
    check_open_version(sid, current, "public downgrade version")
    require(res.resarray[2].data == b"before", "public downgrade lost surviving READ access")
    p.call([op.putfh(fh), op.write(current, 0, FILE_SYNC4, b"BAD")], expected=NFS4ERR_OPENMODE)
    p.call([op.putfh(fh), op.setattr(sid, {FATTR4_SIZE: 0}),
            op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")], expected=NFS4ERR_OLD_STATEID)
    p.call([op.putfh(fh), op.setattr(current, {FATTR4_SIZE: 0}),
            op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")], expected=NFS4ERR_OPENMODE)
    require(p.read(fh, current) == b"before", "rejected post-downgrade size SETATTR changed data")

    name, fh, sid, owner = downgrade_fixture(p, "retry-downgrade-existing")
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.open_downgrade(CURRENT, 0,
                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE), op.read(CURRENT, 0, 6)],
                 "retry_open_downgrade_read")
    opened, current = res.resarray[1].stateid, res.resarray[2].open_stateid
    p.replace_stateid(fh, current)
    check_open_version(sid, opened, "retried OPEN before downgrade")
    check_open_version(opened, current, "retried downgrade must advance once")
    require(res.resarray[3].data == b"before", "OPEN+DOWNGRADE retry lost READ data")
    p.call([op.putfh(fh), op.write(current, 0, FILE_SYNC4, b"BAD")], expected=NFS4ERR_OPENMODE)
    p.call([op.putfh(fh), op.read(opened, 0, 6)], expected=NFS4ERR_OLD_STATEID)

    name, fh, fixture = p.create("retry-fresh-downgrade")
    p.call([op.putfh(fh), op.write(fixture, 0, FILE_SYNC4, b"before"), op.close(0, fixture)])
    p.mark_closed(fh)
    owner = b"fresh-downgrade-owner"
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.putfh(p.directory),
                  p.open_op(name, create=False, owner=owner, access=OPEN4_SHARE_ACCESS_WRITE),
                  op.open_downgrade(CURRENT, 0, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.read(CURRENT, 0, 6), op.close(0, CURRENT)], "downgrade_fresh_open_history")
    first, second, current = res.resarray[1].stateid, res.resarray[3].stateid, res.resarray[4].open_stateid
    require(first.seqid == 1 and res.resarray[5].data == b"before",
            "fresh private OPEN+DOWNGRADE history lost its initial version or READ data")
    check_open_version(first, second, "fresh private second OPEN version")
    check_open_version(second, current, "fresh private downgrade version")
    p.call([op.putfh(fh), op.read(current, 0, 6)], expected=NFS4ERR_BAD_STATEID)

    _, fh, sid, _ = downgrade_fixture(p, "downgrade-write-veto")
    res = p.call([op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                 "downgrade_write_veto", NFS4ERR_OPENMODE)
    current = res.resarray[1].open_stateid
    p.replace_stateid(fh, current)
    require(len(res.resarray) == 3 and p.read(fh, current) == b"before",
            "downgrade did not narrow attempt-local rights before WRITE")

    name, fh, sid, owner = downgrade_fixture(p, "downgrade-collapsed-history")
    res = p.call([op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_BOTH,
                  OPEN4_SHARE_DENY_NONE), op.open_downgrade(CURRENT, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                 "downgrade_collapsed_history", NFS4ERR_INVAL)
    current = res.resarray[1].open_stateid
    p.replace_stateid(fh, current)
    require(len(res.resarray) == 3 and p.read(fh, current) == b"before",
            "downgrade failed to collapse historical READ+WRITE events into BOTH")
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.open_downgrade(CURRENT, 0,
                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE), op.read(CURRENT, 0, 6)],
                 "downgrade_reopen_restores_history")
    opened, latest = res.resarray[1].stateid, res.resarray[2].open_stateid
    p.replace_stateid(fh, latest)
    check_open_version(current, opened, "reopen after failed downgrade version")
    check_open_version(opened, latest, "downgrade after restored READ event")


def test_downgrade_stateids(p):
    _, fh, sid, _ = downgrade_fixture(p, "downgrade-zero-stateid")
    res = p.call([op.putfh(fh), op.open_downgrade(stateid4(0, sid.other), 0,
                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE), op.read(CURRENT, 0, 6)],
                 "downgrade_zero_stateid")
    p.replace_stateid(fh, res.resarray[1].open_stateid)
    check_open_version(sid, res.resarray[1].open_stateid, "sequence-zero downgrade version")

    name, fh, sid, owner = downgrade_fixture(p, "downgrade-saved-stateid")
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.savefh(), op.open_downgrade(CURRENT, 0,
                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE), op.restorefh(),
                  op.read(CURRENT, 0, 6)], "downgrade_saved_current_read")
    latest = res.resarray[3].open_stateid
    p.replace_stateid(fh, latest)
    require(res.resarray[-1].data == b"before" and p.read(fh, latest) == b"before",
            "saved CURRENT READ did not use sequence zero after downgrade")
    p.call([op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.write(latest, 0, FILE_SYNC4, b"BAD")],
           "downgrade_explicit_old_stateid", NFS4ERR_OLD_STATEID)
    # CLOSE and OPEN_DOWNGRADE are the exceptions: CURRENT preserves the
    # saved sequence number for these operations (RFC 8881 section 8.2.3).
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                  access=OPEN4_SHARE_ACCESS_READ), op.savefh(), op.open_downgrade(CURRENT, 0,
                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE), op.restorefh(),
                  op.open_downgrade(CURRENT, 0, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.getattr(1 << FATTR4_SIZE)], "downgrade_saved_current_old_version", NFS4ERR_OLD_STATEID)
    latest = res.resarray[3].open_stateid
    p.replace_stateid(fh, latest)
    require(len(res.resarray) == 6 and p.read(fh, latest) == b"before",
            "OPEN_DOWNGRADE failed to preserve CURRENT's saved sequence number")

    _, a, sa, _ = downgrade_fixture(p, "downgrade-wrong-fh-a")
    _, b, sb = p.create("downgrade-wrong-fh-b")
    res = p.call([op.putfh(b), op.open_downgrade(sa, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.write(sb, 0, FILE_SYNC4, b"BAD")],
                 "downgrade_wrong_fh", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 2 and p.read(b, sb) == b"", "wrong-FH downgrade allowed suffix mutation")
    p.call([op.putfh(a), op.write(sa, 0, FILE_SYNC4, b"rights")])

    original = p.session
    foreign = p.client.new_client(f"foreign-downgrade-{os.getpid()}".encode()).create_session()
    try:
        p.session = foreign
        p.call([op.reclaim_complete(False)])
        p.call([op.putfh(a), op.open_downgrade(sa, 0, OPEN4_SHARE_ACCESS_READ,
                OPEN4_SHARE_DENY_NONE), op.getattr(1 << FATTR4_SIZE)],
               "downgrade_wrong_client", NFS4ERR_BAD_STATEID)
    finally:
        p.session = original
    p.call([op.putfh(a), op.write(sa, 0, FILE_SYNC4, b"rights")])


def test_downgrade_deny_shrink(p):
    for unrelated in (False, True):
        label = "downgrade_unrelated_deny" if unrelated else "downgrade_deny_shrink_open"
        owner = label.encode()
        name, fh, sid = coalesced_fixture(p, label, owner, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_WRITE)
        res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=owner,
                      access=OPEN4_SHARE_ACCESS_READ)])
        sid = res.resarray[-1].stateid
        p.replace_stateid(fh, sid)
        if unrelated:
            res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"other-" + owner,
                          access=OPEN4_SHARE_ACCESS_READ, deny=OPEN4_SHARE_DENY_WRITE)])
            other_sid = res.resarray[-1].stateid
        operations = [op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                      op.putfh(p.directory), p.open_op(name, create=False, owner=b"writer-" + owner,
                      access=OPEN4_SHARE_ACCESS_WRITE), op.write(CURRENT, 0, FILE_SYNC4, b"shared"),
                      op.close(0, CURRENT)]
        res = p.call(operations, label, NFS4ERR_SHARE_DENIED if unrelated else NFS4_OK)
        current = res.resarray[1].open_stateid
        p.replace_stateid(fh, current)
        check_open_version(sid, current, "deny-shrink downgrade version")
        require(p.read(fh, current) == (b"before" if unrelated else b"shared"),
                "downgrade did not distinguish its removed deny from an unrelated claim")
        if unrelated:
            require(len(res.resarray) == 4, "unrelated deny failed to stop the new OPEN")
            p.call([op.putfh(fh), op.close(0, other_sid)])

    _, fh, sid, _ = downgrade_fixture(p, "downgrade-cannot-expand")
    res = p.call([op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.open_downgrade(CURRENT, 0, OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_DENY_NONE),
                  op.write(CURRENT, 0, FILE_SYNC4, b"BAD")], "downgrade_cannot_expand", NFS4ERR_INVAL)
    current = res.resarray[1].open_stateid
    p.replace_stateid(fh, current)
    require(len(res.resarray) == 3 and p.read(fh, current) == b"before",
            "a second downgrade illegally restored discarded WRITE rights")


def test_stateid_checkpoints(p):
    _, fh, sid, _ = downgrade_fixture(p, "test-stateid-journal")
    predicted = stateid4(sid.seqid + 1, sid.other)
    res = p.call([op.test_stateid([sid, ANONYMOUS, CURRENT, stateid4(0xFFFFFFFF, b"\xff" * 12)]),
                  op.putfh(fh), op.open_downgrade(sid, 0, OPEN4_SHARE_ACCESS_READ,
                  OPEN4_SHARE_DENY_NONE), op.test_stateid([sid, predicted, stateid4(0, sid.other)]),
                  op.read(CURRENT, 0, 6)], "test_stateid_no_fh_and_downgrade")
    p.replace_stateid(fh, res.resarray[2].open_stateid)
    require(res.resarray[0].tsr_status_codes == [NFS4_OK] + [NFS4ERR_BAD_STATEID] * 3 and
            res.resarray[3].tsr_status_codes == [NFS4ERR_OLD_STATEID, NFS4_OK, NFS4_OK] and
            res.resarray[4].data == b"before", "TEST_STATEID did not use operation-time journal versions")

    res = p.call([op.test_stateid([predicted]), op.putfh(fh), op.close(0, predicted),
                  op.test_stateid([predicted, stateid4(0, sid.other), CURRENT]),
                  op.getattr(1 << FATTR4_SIZE)], "test_stateid_after_private_close")
    p.mark_closed(fh)
    require(res.resarray[0].tsr_status_codes == [NFS4_OK] and
            res.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 3,
            "TEST_STATEID resolved a privately closed state or current-stateid sentinel")

    _, fh, sid = p.create("test-stateid-wrong-client")
    original = p.session
    foreign = p.client.new_client(f"foreign-test-stateid-{os.getpid()}".encode()).create_session()
    try:
        p.session = foreign
        p.call([op.reclaim_complete(False)])
        res = p.call([op.test_stateid([sid]), op.putfh(fh), op.getattr(1 << FATTR4_SIZE)],
                     "test_stateid_wrong_client")
        require(res.resarray[0].tsr_status_codes == [NFS4ERR_BAD_STATEID],
                "TEST_STATEID accepted another session client's state")
    finally:
        p.session = original


def test_stateid_refused_build_budget(p):
    _, fh, sid = p.create("stateid-refused-build-budget")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"unchanged")])
    # A 16 KiB status array plus its 64 KiB input leaves room for response
    # structures and transport iovecs in the 128 KiB arena. Empty OWNER refuses
    # construction after the status allocation. Each leading PUTFH falls back
    # and retries construction; without rewind discarded arrays exhaust it.
    # OWNER avoids a separate large legacy ACL scratch allocation.
    ids = [sid] * 4000
    leading = [op.putfh(fh) for _ in range(8)]
    res = p.call(leading + [op.test_stateid(ids), op.setattr(sid, {FATTR4_OWNER: b""}),
                           op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "test_stateid_refused_build_budget", NFS4ERR_INVAL, runs=[])
    require(len(res.resarray) == len(leading) + 2 and
            res.resarray[len(leading)].tsr_status_codes == [NFS4_OK] * len(ids),
            "refused construction leaked reply budget or corrupted the accepted TEST_STATEID prefix")
    require(p.read(fh, sid) == b"unchanged", "malformed SETATTR allowed its mutation suffix")

    # A larger, valid request leaves too little shared decode/encode storage
    # for both the status array and transport iovecs. Return a protocol error
    # while preserving the successful prefix and stopping the WRITE suffix.
    res = p.call(leading + [op.test_stateid([sid] * 6000),
                           op.write(sid, 0, FILE_SYNC4, b"BAD")],
                 "test_stateid_transport_headroom", NFS4ERR_REP_TOO_BIG, runs=[])
    require(len(res.resarray) == len(leading) + 1,
            "oversized TEST_STATEID did not stop at the failing operation")
    require(p.read(fh, sid) == b"unchanged", "oversized TEST_STATEID allowed its WRITE suffix")


def test_secinfo_no_name_checkpoints(p):
    name, fh, sid = p.create("secinfo-no-name")
    for style, label in ((SECINFO_STYLE4_CURRENT_FH, "current"), (SECINFO_STYLE4_PARENT, "parent")):
        res = p.call([op.putfh(fh), op.savefh(), op.secinfo_no_name(style), op.test_stateid([CURRENT, sid]),
                      op.restorefh(), op.getattr(1 << FATTR4_TYPE)], "secinfo_no_name_restore_" + label)
        require(res.resarray[2].resok4 and res.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4REG,
                "SECINFO_NO_NAME lost its response or saved FH")
        require(res.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID, NFS4_OK],
                "no-FH TEST_STATEID failed to report special-ID error without stopping RESTOREFH")
    p.call([op.putfh(fh), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH),
            op.getattr(1 << FATTR4_SIZE), op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"BAD")],
           "secinfo_no_name_consumed_fh", NFS4ERR_NOFILEHANDLE)
    p.call([op.putfh(fh), op.secinfo_no_name(SECINFO_STYLE4_PARENT),
            op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"restored")], "secinfo_no_name_putfh")
    # The synthetic NFS namespace root remains a deliberate legacy boundary;
    # its missing parent is rejected before any VFS compound submission.
    p.call([op.putrootfh(), op.secinfo_no_name(SECINFO_STYLE4_PARENT),
            op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"BAD")],
           "secinfo_no_name_root_parent", NFS4ERR_NOENT, runs=[])
    p.call([op.putfh(fh), op.secinfo_no_name(99), op.write(sid, 0, FILE_SYNC4, b"BAD")],
           "secinfo_no_name_invalid_style", NFS4ERR_INVAL, checks=False)
    require(p.read(fh, sid) == b"restored", "failed SECINFO_NO_NAME allowed mutation suffix")
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"secinfo-no-name-private"),
                  op.savefh(), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH), op.restorefh(),
                  op.read(CURRENT, 0, 8), op.close(0, CURRENT)], "secinfo_no_name_saved_current_stateid")
    require(res.resarray[5].data == b"restored", "SECINFO_NO_NAME lost saved private current-stateid")


def test_io_advise_checkpoints(p):
    if p.args.minor < 2:
        return
    name, fh, sid = p.create("io-advise")
    hints = (1 << IO_ADVISE4_SEQUENTIAL) | (1 << IO_ADVISE4_WILLNEED)
    res = p.call([op.putfh(fh), op.io_advise(sid, 0, 4096, hints),
                  op.write(sid, 0, FILE_SYNC4, b"advised")], "io_advise_write_continuation")
    require(res.resarray[1].ior_hints == 0 and p.read(fh, sid) == b"advised",
            "IO_ADVISE did not return empty honored hints or continue")
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False, owner=b"io-advise-private"),
                  op.io_advise(CURRENT, 0, 7, hints), op.read(CURRENT, 0, 7), op.close(0, CURRENT)],
                 "io_advise_private_current")
    require(res.resarray[2].ior_hints == 0 and res.resarray[3].data == b"advised",
            "IO_ADVISE did not resolve private current state")
    _, other_fh, other_sid = p.create("io-advise-other-file")
    p.call([op.putfh(other_fh), op.io_advise(sid, 0, 7, hints),
            op.write(other_sid, 0, FILE_SYNC4, b"BAD")], "io_advise_wrong_fh", NFS4ERR_BAD_STATEID)
    require(p.read(other_fh, other_sid) == b"", "wrong-FH IO_ADVISE allowed suffix mutation")
    original = p.session
    foreign = p.client.new_client(f"foreign-io-advise-{os.getpid()}".encode()).create_session()
    try:
        p.session = foreign
        p.call([op.reclaim_complete(False)])
        p.call([op.putfh(fh), op.io_advise(sid, 0, 7, hints), op.getattr(1 << FATTR4_SIZE)],
               "io_advise_wrong_client", NFS4ERR_BAD_STATEID)
    finally:
        p.session = original
    res = p.call([op.putfh(fh), op.io_advise(ANONYMOUS, 0, 7, hints),
                  op.getattr(1 << FATTR4_SIZE)], "io_advise_anonymous")
    require(res.resarray[1].ior_hints == 0, "anonymous advisory checkpoint unexpectedly honored hints")
    _, closing_fh, closing_sid = p.create("io-advise-before-close")
    res = p.call([op.putfh(closing_fh), op.io_advise(closing_sid, 0, 4096, hints),
                  op.close(0, closing_sid), op.getattr(1 << FATTR4_SIZE)], "io_advise_before_close")
    p.mark_closed(closing_fh)
    require(res.resarray[1].ior_hints == 0 and res.resarray[-1].obj_attributes[FATTR4_SIZE] == 0,
            "preheld public CLOSE reservation blocked preceding IO_ADVISE")
    p.call([op.putfh(fh), op.close(0, sid), op.io_advise(sid, 0, 7, hints),
            op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")], "io_advise_closed_state", NFS4ERR_BAD_STATEID)
    p.mark_closed(fh)
    p.call([op.putfh(fh), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH),
            op.io_advise(ANONYMOUS, 0, 7, hints), op.putfh(fh),
            op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")],
           "io_advise_no_fh", NFS4ERR_NOFILEHANDLE)
    require(p.read(fh, ANONYMOUS) == b"advised", "failed IO_ADVISE allowed suffix mutation")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(1, 2), default=2)
    parser.add_argument("--server-log", required=True)
    probe = Probe(parser.parse_args())
    try:
        test_resolved_io(probe)
        test_open_suffix(probe)
        test_lockt_veto(probe)
        test_lock_open_current_retry(probe)
        test_lock_open_mode_parity(probe)
        test_access_execute_permissions(probe)
        test_lock_ranges(probe)
        test_lock_private_owners(probe)
        test_lock_prefix_and_denial(probe)
        test_close_lock_children(probe)
        test_retry_lock_close(probe)
        test_lock_close_reopen(probe)
        test_lock_downgrade_after(probe)
        test_lock_sibling_parents(probe)
        test_lock_reclaim_outside_grace(probe)
        test_open_reclaim_outside_grace(probe)
        test_exclusive_open_compounds(probe)
        test_acl_verify(probe)
        test_acl_inputs(probe)
        test_acl_reply_budget(probe)
        test_read_plus(probe)
        test_copy_clone(probe)
        test_close_and_prefix(probe)
        test_multiple_opens(probe)
        test_close_releases_deny(probe)
        test_unlinked_open_lifetime(probe)
        test_anonymous_io_after_close(probe)
        test_same_owner_closes(probe)
        test_anonymous_private_deny(probe)
        test_anonymous_v42_after_close(probe)
        test_secinfo_continuation(probe)
        test_saved_fh_through_stateid(probe)
        test_coalesced_open_upgrade(probe)
        test_coalesced_downgrade_history(probe)
        test_coalesced_same_owner_files(probe)
        test_coalesced_reopen_after_close(probe)
        test_coalesced_saved_version(probe)
        test_coalesced_self_conflict(probe)
        test_coalesced_prefix_failure(probe)
        test_retry_coalesced_opens(probe)
        test_downgrade_checkpoints(probe)
        test_downgrade_stateids(probe)
        test_downgrade_deny_shrink(probe)
        test_stateid_checkpoints(probe)
        test_stateid_refused_build_budget(probe)
        test_secinfo_no_name_checkpoints(probe)
        test_io_advise_checkpoints(probe)
        test_retryable_open_reads(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} wire/compound boundary checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
