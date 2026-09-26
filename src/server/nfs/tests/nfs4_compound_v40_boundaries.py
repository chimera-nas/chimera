#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""NFSv4.0 owner replay and compound boundaries, using fresh RPC XIDs.

Measured state operations must share one VFS submission with their prefix
and suffix, including fresh OPEN/CONFIRM and owner-seqid replay.
Run with the common namespace wrapper and minor version zero.
"""

from nfs4_compound_boundaries import *  # noqa: F401,F403
from xdrdef.nfs4_type import cb_client4, clientaddr4, nfs_client_id4


class Probe40(Probe):
    def __init__(self, args):
        self.args = args
        self.client = nfs4client.NFS4Client(args.host, args.port, 0)
        self.client.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"compound-v40-probe"))
        self.measured = []
        self.expected_runs = {}
        self.files = []
        self.directories = []
        self.owner_seqids = {}
        self.file_owners = {}
        identity = nfs_client_id4(b"v40probe", f"compound-v40-{os.getpid()}".encode())
        callback = cb_client4(0, clientaddr4(b"tcp", b"0.0.0.0.0.0"))
        res = self.call([op.setclientid(identity, callback, 0)])
        self.clientid = res.resarray[0].clientid
        self.call([op.setclientid_confirm(self.clientid, res.resarray[0].setclientid_confirm)])
        res = self.call([op.putrootfh(), op.lookup(args.export.encode()), op.getfh()])
        self.directory = res.resarray[-1].object

    def call(self, operations, name=None, expected=NFS4_OK, runs=None, **kwargs):
        tag = f"boundary_{name}".encode() if name else b"boundary_setup"
        res = self.client.compound(operations, tag=tag, version=0)
        require(res.status == expected,
                f"{name or 'setup'}: expected {nfsstat4[expected]}, got {res!r}")
        if name:
            self.measured.append(tag.decode())
            self.expected_runs[tag.decode()] = runs if runs is not None else [(0, len(operations))]
            print(f"PASS wire {name}: {nfsstat4[res.status]}", flush=True)
        return res

    def open_op(self, name, create=True, owner=None, access=OPEN4_SHARE_ACCESS_BOTH):
        owner = owner or b"owner-" + name
        how = (openflag4(OPEN4_CREATE, createhow4(GUARDED4, {FATTR4_MODE: 0o666}))
               if create else openflag4(OPEN4_NOCREATE))
        return op.open(self.owner_seqids.get(owner, 0), access, OPEN4_SHARE_DENY_NONE,
                       open_owner4(self.clientid, owner), how, open_claim4(CLAIM_NULL, name))

    def create(self, name, access=OPEN4_SHARE_ACCESS_BOTH):
        name = name.encode()
        owner = b"owner-" + name
        res = self.call([op.putfh(self.directory), self.open_op(name, access=access), op.getfh()])
        fh, sid = res.resarray[-1].object, res.resarray[-2].stateid
        self.owner_seqids[owner] = 1
        if res.resarray[-2].rflags & OPEN4_RESULT_CONFIRM:
            res = self.call([op.putfh(fh), op.open_confirm(sid, 1)])
            sid = res.resarray[-1].open_stateid
            self.owner_seqids[owner] = 2
        self.files.append((name, fh, sid))
        self.file_owners[fh] = owner
        return name, fh, sid

    def open_again(self, name, fh, access):
        owner = self.file_owners[fh]
        res = self.call([op.putfh(self.directory), self.open_op(name, create=False, access=access)])
        self.owner_seqids[owner] += 1
        sid = res.resarray[-1].stateid
        self.replace_stateid(fh, sid)
        return sid

    def close(self, fh, sid, name=None):
        owner = self.file_owners[fh]
        operation = op.close(self.owner_seqids[owner], sid)
        res = self.call([op.putfh(fh), operation, op.getattr(1 << FATTR4_SIZE)], name)
        self.owner_seqids[owner] += 1
        self.mark_closed(fh)
        return operation, res

    def cleanup(self):
        for name, fh, sid in reversed(self.files):
            if sid is not None:
                self.close(fh, sid)
            self.call([op.putfh(self.directory), op.remove(name)])


def same_sid(a, b):
    return a.seqid == b.seqid and a.other == b.other


def test_v40_secinfo_cursor(p):
    name, fh, sid = p.create("v40-secinfo-retains-directory")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"secinfo")])
    res = p.call([op.putfh(fh), op.savefh(), op.putfh(p.directory),
                  op.secinfo(name), op.getfh(), op.getattr(1 << FATTR4_TYPE),
                  op.restorefh(), op.getfh(), op.read(sid, 0, 7)],
                 "v40_secinfo_retains_cursors")
    require(res.resarray[4].object == p.directory and res.resarray[7].object == fh and
            res.resarray[8].data == b"secinfo", "SECINFO changed current or saved FH on v4.0")
    # Synthetic root takes the legacy handler; its cursor obeys the same rule.
    res = p.call([op.putrootfh(), op.getfh(), op.secinfo(p.args.export.encode()), op.getfh()])
    require(res.resarray[1].object == res.resarray[3].object,
            "legacy pseudo-root SECINFO consumed the v4.0 current FH")


def test_v40_fresh_open_replay(p):
    name = b"v40-open-confirm-replay"
    owner = b"owner-" + name
    operations = [op.putfh(p.directory), p.open_op(name), op.getfh(), op.getattr(1 << FATTR4_SIZE)]
    res = p.call(operations, "v40_fresh_open")
    fh, opened = res.resarray[2].object, res.resarray[1].stateid
    replay = p.call(operations, "v40_fresh_open_replay")
    require(replay.resarray[2].object == fh and repr(replay.resarray[1]) == repr(res.resarray[1]),
            f"fresh OPEN replay changed FH or response fields: first={res!r}, replay={replay!r}")
    require(res.resarray[1].rflags & OPEN4_RESULT_CONFIRM, "fixture needs an unconfirmed new owner")
    predicted = stateid4(opened.seqid + 1, opened.other)
    operations = [op.putfh(fh), op.open_confirm(opened, 1), op.read(predicted, 0, 16),
                  op.getattr(1 << FATTR4_SIZE)]
    res = p.call(operations, "v40_fresh_confirm_read")
    confirmed = res.resarray[1].open_stateid
    replay = p.call(operations, "v40_fresh_confirm_replay")
    require(same_sid(confirmed, predicted) and same_sid(replay.resarray[1].open_stateid, confirmed) and
            res.resarray[2].data == b"" and replay.resarray[2].data == b"",
            "OPEN_CONFIRM replay changed its stateid or did not authorize suffix READ")
    p.owner_seqids[owner] = 2
    p.file_owners[fh] = owner
    p.files.append((name, fh, confirmed))
    operation, res = p.close(fh, confirmed)
    replay = p.call([op.putfh(fh), operation, op.getattr(1 << FATTR4_SIZE)],
                    "v40_destroyed_close_replay")
    require(same_sid(replay.resarray[1].open_stateid, res.resarray[1].open_stateid),
            "destroyed-state CLOSE replay lost its cached stateid")


def test_v40_confirmed_open(p):
    name, fh, sid = p.create("retry-v40-confirmed-existing")
    owner = p.file_owners[fh]
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"confirmed")])
    operation = p.open_op(name, create=False)
    predicted = stateid4(sid.seqid + 1, sid.other)
    res = p.call([op.putfh(p.directory), operation, op.read(predicted, 0, 9), op.getfh()],
                 "v40_confirmed_open_read")
    opened = res.resarray[1].stateid
    require(same_sid(opened, predicted) and res.resarray[2].data == b"confirmed" and
            res.resarray[3].object == fh, "confirmed OPEN did not advance private state/FH")
    p.owner_seqids[owner] += 1
    p.replace_stateid(fh, opened)
    replay = p.call([op.putfh(p.directory), operation, op.read(opened, 0, 9), op.getfh()],
                    "v40_confirmed_open_replay")
    require(repr(replay.resarray[1]) == repr(res.resarray[1]) and replay.resarray[3].object == fh,
            "confirmed OPEN replay changed response fields or current FH")
    missing = p.open_op(b"v40-confirmed-missing", create=False, owner=owner)
    for case in ("v40_confirmed_open_missing", "v40_confirmed_open_missing_replay"):
        res = p.call([op.putfh(p.directory), missing, op.read(opened, 0, 9)],
                     case, NFS4ERR_NOENT)
        require(len(res.resarray) == 2, "missing OPEN did not stop its suffix")
    p.owner_seqids[owner] += 1
    invalid = p.open_op(name, create=False)
    invalid.opopen.seqid = 99
    p.call([op.putfh(p.directory), invalid, op.write(opened, 0, FILE_SYNC4, b"BAD")],
           "v40_confirmed_open_bad_seqid", NFS4ERR_BAD_SEQID)
    first = p.open_op(name, create=False)
    second = p.open_op(name, create=False)
    second.opopen.seqid += 1
    newest = stateid4(opened.seqid + 2, opened.other)
    res = p.call([op.putfh(p.directory), first, op.savefh(), op.putfh(p.directory), second,
                  op.restorefh(), op.read(newest, 0, 9)], "v40_two_confirmed_opens")
    require(res.resarray[1].stateid.seqid == opened.seqid + 1 and
            same_sid(res.resarray[4].stateid, newest) and res.resarray[6].data == b"confirmed",
            "multiple confirmed OPEN operations lost sequence or saved-FH state")
    p.owner_seqids[owner] += 2
    p.replace_stateid(fh, newest)
    operation = p.open_op(name, create=False)
    last = stateid4(newest.seqid + 1, newest.other)
    close = op.close(p.owner_seqids[owner] + 1, last)
    res = p.call([op.putfh(p.directory), operation, op.read(last, 0, 9), close,
                  op.getattr(1 << FATTR4_SIZE)], "v40_confirmed_open_close")
    require(same_sid(res.resarray[1].stateid, last) and res.resarray[2].data == b"confirmed",
            "confirmed OPEN/CLOSE did not retain intermediate result")
    p.owner_seqids[owner] += 2
    p.mark_closed(fh)
    replay = p.call([op.putfh(fh), close, op.getattr(1 << FATTR4_SIZE)],
                    "v40_confirmed_open_close_replay")
    require(same_sid(replay.resarray[1].open_stateid, res.resarray[3].open_stateid),
            "coalesced OPEN/CLOSE lost its final replay reply")


def test_v40_confirm_consumed_error(p):
    name = b"v40-unconfirmed-history"
    owner = b"owner-" + name
    res = p.call([op.putfh(p.directory), p.open_op(name), op.getfh()], "v40_unconfirmed_first_open")
    fh, first = res.resarray[2].object, res.resarray[1].stateid
    p.owner_seqids[owner] = 1
    p.file_owners[fh] = owner
    p.files.append((name, fh, first))
    res = p.call([op.putfh(p.directory), p.open_op(name, create=False), op.getfh()],
                 "v40_unconfirmed_second_open")
    second = res.resarray[1].stateid
    require(second.other == first.other and second.seqid == first.seqid + 1 and
            res.resarray[1].rflags & OPEN4_RESULT_CONFIRM,
            "repeated unconfirmed OPEN did not retain its state or confirmation requirement")
    p.owner_seqids[owner] = 2
    p.replace_stateid(fh, second)
    invalid = op.open_confirm(first, 2)
    for case in ("v40_confirm_old_stateid", "v40_confirm_old_stateid_replay"):
        res = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), invalid,
                      op.read(second, 0, 16)], case, NFS4ERR_OLD_STATEID)
        require(len(res.resarray) == 3, "OLD_STATEID confirm failed to stop its READ suffix")
    p.owner_seqids[owner] = 3
    predicted = stateid4(second.seqid + 1, second.other)
    res = p.call([op.putfh(fh), op.open_confirm(second, 3), op.read(predicted, 0, 16)],
                 "v40_confirm_after_consumed_error")
    confirmed = res.resarray[1].open_stateid
    require(same_sid(confirmed, predicted) and res.resarray[2].data == b"",
            "failed CONFIRM was not consumed once or advanced stateid before confirmation")
    p.owner_seqids[owner] = 4
    p.replace_stateid(fh, confirmed)


def test_v40_confirmed_create_replay(p):
    _, anchor, _ = p.create("v40-confirmed-owner-anchor")
    owner = p.file_owners[anchor]
    name = b"v40-confirmed-created"
    operation = p.open_op(name, owner=owner)
    res = p.call([op.putfh(p.directory), operation, op.getfh(), op.getattr(1 << FATTR4_SIZE)],
                 "v40_confirmed_create")
    fh, sid = res.resarray[2].object, res.resarray[1].stateid
    p.owner_seqids[owner] += 1
    p.file_owners[fh] = owner
    p.files.append((name, fh, sid))
    replay = p.call([op.putfh(p.directory), operation, op.getfh(), op.getattr(1 << FATTR4_SIZE)],
                    "v40_confirmed_create_replay")
    require(repr(replay.resarray[1]) == repr(res.resarray[1]) and replay.resarray[2].object == fh,
            "create replay lost cinfo, attrset, stateid, flags, delegation, or restored FH")
    require(res.resarray[1].attrset & (1 << FATTR4_MODE), "fixture needs a nonempty create attrset")
    p.close(fh, sid, "v40_confirmed_created_close")


def test_v40_downgrade(p):
    name, fh, sid = p.create("v40-downgrade", OPEN4_SHARE_ACCESS_READ)
    sid = p.open_again(name, fh, OPEN4_SHARE_ACCESS_BOTH)
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"unchanged")])
    owner = p.file_owners[fh]
    operation = op.open_downgrade(sid, p.owner_seqids[owner], OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE)
    predicted = stateid4(sid.seqid + 1, sid.other)
    res = p.call([op.putfh(fh), operation, op.read(predicted, 0, 9)], "v40_downgrade_read")
    narrowed = res.resarray[1].open_stateid
    require(same_sid(narrowed, predicted) and res.resarray[2].data == b"unchanged",
            "DOWNGRADE did not expose its new version to suffix READ")
    p.owner_seqids[owner] += 1
    p.replace_stateid(fh, narrowed)
    replay = p.call([op.putfh(fh), operation, op.read(narrowed, 0, 9)], "v40_downgrade_replay")
    require(same_sid(replay.resarray[1].open_stateid, narrowed), "DOWNGRADE replay advanced state version")
    p.call([op.putfh(fh), op.write(narrowed, 0, FILE_SYNC4, b"BAD")],
           "v40_downgrade_write_veto", NFS4ERR_OPENMODE)
    require(p.read(fh, narrowed) == b"unchanged", "narrowed OPEN allowed a WRITE")
    p.close(fh, narrowed, "v40_close_after_downgrade")


def test_v40_locks(p):
    _, fh, sid = p.create("v40-locks")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"lock-data")])
    owner = p.file_owners[fh]
    lock_owner = b"v40-range-owner"
    operation = op.lock(WRITE_LT, False, 0, 8,
                        locker4(True, open_to_lock_owner4(p.owner_seqids[owner], sid, 0,
                                                         lock_owner4(p.clientid, lock_owner))))
    res = p.call([op.putfh(fh), operation, op.read(sid, 0, 9)], "v40_new_lock_read")
    locked = res.resarray[1].lock_stateid
    p.owner_seqids[owner] += 1
    replay = p.call([op.putfh(fh), operation, op.read(locked, 0, 9)], "v40_new_lock_replay")
    require(same_sid(replay.resarray[1].lock_stateid, locked) and replay.resarray[2].data == b"lock-data",
            "new LOCK replay lost response or advanced its state")
    operation = op.lock(WRITE_LT, False, 8, 8, locker4(False, lock_owner=exist_lock_owner4(locked, 1)))
    res = p.call([op.putfh(fh), operation, op.getattr(1 << FATTR4_SIZE)], "v40_existing_lock")
    extended = res.resarray[1].lock_stateid
    replay = p.call([op.putfh(fh), operation, op.read(extended, 0, 9)], "v40_existing_lock_replay")
    require(same_sid(replay.resarray[1].lock_stateid, extended), "existing LOCK replay advanced state")
    operation = op.locku(WRITE_LT, 2, extended, 4, 8)
    res = p.call([op.putfh(fh), operation,
                  op.lockt(WRITE_LT, 4, 8, lock_owner4(p.clientid, b"v40-probe"))], "v40_unlock_hole")
    unlocked = res.resarray[1].lock_stateid
    replay = p.call([op.putfh(fh), operation,
                     op.lockt(WRITE_LT, 4, 8, lock_owner4(p.clientid, b"v40-probe"))], "v40_unlock_replay")
    require(same_sid(replay.resarray[1].lock_stateid, unlocked), "LOCKU replay advanced state")
    p.call([op.putfh(fh), op.locku(WRITE_LT, 99, unlocked, 0, 16), op.read(sid, 0, 9)],
           "v40_unlock_bad_seqid", NFS4ERR_BAD_SEQID)
    blocker = b"v40-other-range-owner"
    p.call([op.putfh(fh), op.lock(WRITE_LT, False, 32, 4,
            locker4(True, open_to_lock_owner4(p.owner_seqids[owner], sid, 0,
                                             lock_owner4(p.clientid, blocker))))])
    p.owner_seqids[owner] += 1
    conflict = op.lock(WRITE_LT, False, 32, 4,
                       locker4(False, lock_owner=exist_lock_owner4(unlocked, 3)))
    for case in ("v40_lock_denied", "v40_lock_denied_replay"):
        res = p.call([op.putfh(fh), conflict, op.read(sid, 0, 9)], case, NFS4ERR_DENIED)
        require(len(res.resarray) == 2, "denied LOCK did not stop its suffix")
        check_lock_denied(res.resarray[1], blocker, WRITE_LT, 32, 4)
    invalid = op.locku(WRITE_LT, 4, unlocked, 0, 0)
    for case in ("v40_unlock_invalid", "v40_unlock_invalid_replay"):
        p.call([op.putfh(fh), invalid, op.read(sid, 0, 9)], case, NFS4ERR_INVAL)
    denied = p.call([op.putfh(fh), op.lockt(READ_LT, 0, 4, lock_owner4(p.clientid, b"v40-probe")),
                     op.write(sid, 0, FILE_SYNC4, b"BAD")], "v40_lock_residual_veto", NFS4ERR_DENIED)
    check_lock_denied(denied.resarray[1], lock_owner, WRITE_LT, 0, 4)
    p.call([op.putfh(fh), op.lockt(WRITE_LT, 0, 4, lock_owner4(0, b"invalid-client")),
            op.write(sid, 0, FILE_SYNC4, b"BAD")], "v40_lockt_stale_client", NFS4ERR_STALE_CLIENTID)
    require(p.read(fh, sid) == b"lock-data", "LOCKT denial permitted suffix WRITE")
    res = p.call([op.putfh(fh), op.locku(WRITE_LT, 5, unlocked, 0, 16),
                  op.lockt(WRITE_LT, 0, 16, lock_owner4(p.clientid, b"v40-probe"))],
                 "v40_unlock_after_consumed_errors")
    require(res.resarray[1].lock_stateid.seqid == unlocked.seqid + 1,
            "consumed owner errors advanced the protocol stateid")
    p.close(fh, sid, "v40_close_held_lock")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(0,), default=0)
    parser.add_argument("--server-log", required=True)
    probe = Probe40(parser.parse_args())
    try:
        test_v40_secinfo_cursor(probe)
        test_v40_fresh_open_replay(probe)
        test_v40_confirm_consumed_error(probe)
        test_v40_confirmed_open(probe)
        test_v40_confirmed_create_replay(probe)
        test_v40_downgrade(probe)
        test_v40_locks(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} wire/compound boundary checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
