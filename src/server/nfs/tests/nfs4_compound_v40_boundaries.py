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
        for name in reversed(self.directories):
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


def test_v40_state_error_ordering(p):
    """Retain the direct-state fixture's version, typed replay and consuming errors."""
    _, fh, opened = p.create("v40-state-error-ordering")
    owner = p.file_owners[fh]
    create_lock = op.lock(WRITE_LT, False, 0, 1, locker4(True,
                         open_to_lock_owner4(p.owner_seqids[owner], opened, 0,
                                             lock_owner4(p.clientid, b"state-error-owner"))))
    res = p.call([op.putfh(fh), create_lock])
    p.owner_seqids[owner] += 1
    first = res.resarray[1].lock_stateid
    res = p.call([op.putfh(fh), op.lock(WRITE_LT, False, 2, 1,
                  locker4(False, lock_owner=exist_lock_owner4(first, 1)))])
    current = res.resarray[1].lock_stateid
    invalid = op.locku(WRITE_LT, 2, first, 0, 1)
    for label in ("v40_unlock_old_version", "v40_unlock_old_version_replay"):
        p.call([op.putfh(fh), invalid, op.getattr(1 << FATTR4_SIZE)], label, NFS4ERR_OLD_STATEID)
    p.call([op.putfh(fh), op.locku(WRITE_LT, 3, current, NFS4_UINT64_MAX - 1, 3),
            op.getattr(1 << FATTR4_SIZE)], "v40_unlock_overflow_consumed", NFS4ERR_INVAL)
    future = stateid4(current.seqid + 1, current.other)
    p.call([op.putfh(fh), op.locku(WRITE_LT, 4, future, 0, 3), op.getattr(1 << FATTR4_SIZE)],
           "v40_unlock_future_no_consume", NFS4ERR_BAD_STATEID)
    unlock = op.locku(WRITE_LT, 4, current, 0, 3)
    res = p.call([op.putfh(fh), unlock, op.getattr(1 << FATTR4_SIZE)], "v40_unlock_corrected_sequence")
    updated = res.resarray[1].lock_stateid
    replay = p.call([op.putfh(fh), unlock, op.getattr(1 << FATTR4_SIZE)], "v40_unlock_corrected_replay")
    require(updated.seqid == current.seqid + 1 and same_sid(updated, replay.resarray[1].lock_stateid),
            "failed LOCKU consumed the wrong sequence or replay advanced state")
    p.call([op.putfh(fh), op.lock(WRITE_LT, False, 0, 1,
            locker4(False, lock_owner=exist_lock_owner4(updated, 4))), op.getattr(1 << FATTR4_SIZE)],
           "v40_lock_wrong_replay_type", NFS4ERR_BAD_SEQID)
    reclaim = op.lock(WRITE_LT, True, 0, 1,
                      locker4(False, lock_owner=exist_lock_owner4(updated, 5)))
    for label in ("v40_lock_reclaim_consumed", "v40_lock_reclaim_replay"):
        p.call([op.putfh(fh), reclaim, op.getattr(1 << FATTR4_SIZE)], label, NFS4ERR_NO_GRACE)
    p.call([op.putfh(fh), op.lock(WRITE_LT, True, 0, 1,
            locker4(False, lock_owner=exist_lock_owner4(first, 6))), op.getattr(1 << FATTR4_SIZE)],
           "v40_lock_recovery_before_old_version", NFS4ERR_NO_GRACE)

    new_reclaim = op.lock(WRITE_LT, True, 8, 1, locker4(True,
                         open_to_lock_owner4(p.owner_seqids[owner], opened, 5,
                                             lock_owner4(p.clientid, b"state-reclaim-owner"))))
    for label in ("v40_new_lock_reclaim_consumed", "v40_new_lock_reclaim_replay"):
        p.call([op.putfh(fh), new_reclaim, op.getattr(1 << FATTR4_SIZE)], label, NFS4ERR_NO_GRACE)
    p.owner_seqids[owner] += 1
    corrected = op.lock(WRITE_LT, False, 8, 1, locker4(True,
                        open_to_lock_owner4(p.owner_seqids[owner], opened, 6,
                                            lock_owner4(p.clientid, b"state-reclaim-owner"))))
    res = p.call([op.putfh(fh), corrected, op.getattr(1 << FATTR4_SIZE)],
                 "v40_new_lock_after_consumed_reclaim")
    p.owner_seqids[owner] += 1
    require(res.resarray[1].lock_stateid.seqid == 1,
            "failed reclaim leaked a public lock state or advanced its version")

    name, owner = b"v40-confirm-typed-errors", b"owner-v40-confirm-typed-errors"
    res = p.call([op.putfh(p.directory), p.open_op(name), op.getfh()])
    fh, sid = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, sid))
    p.file_owners[fh] = owner
    p.owner_seqids[owner] = 1
    p.call([op.putfh(fh), op.open_confirm(sid, 0), op.getattr(1 << FATTR4_SIZE)],
           "v40_confirm_wrong_replay_type", NFS4ERR_BAD_SEQID)
    future = stateid4(sid.seqid + 1, sid.other)
    p.call([op.putfh(fh), op.open_confirm(future, 1), op.getattr(1 << FATTR4_SIZE)],
           "v40_confirm_future_no_consume", NFS4ERR_BAD_STATEID)
    p.call([op.putfh(p.directory), op.open_confirm(sid, 1), op.getattr(1 << FATTR4_SIZE)],
           "v40_confirm_wrong_fh_no_consume", NFS4ERR_BAD_STATEID)
    res = p.call([op.putfh(fh), op.open_confirm(sid, 1), op.getattr(1 << FATTR4_SIZE)],
                 "v40_confirm_corrected_sequence")
    confirmed = res.resarray[1].open_stateid
    p.replace_stateid(fh, confirmed)
    p.owner_seqids[owner] = 2
    replay = p.call([op.putfh(fh), op.open_confirm(sid, 1), op.getattr(1 << FATTR4_SIZE)],
                    "v40_confirm_corrected_replay")
    require(confirmed.seqid == sid.seqid + 1 and same_sid(confirmed, replay.resarray[1].open_stateid),
            "corrected CONFIRM or replay advanced state incorrectly")
    p.call([op.putfh(fh), op.open_confirm(confirmed, 2), op.getattr(1 << FATTR4_SIZE)],
           "v40_confirm_already_confirmed", NFS4ERR_BAD_STATEID)


def test_v40_transport_owner_admission(p):
    """Every state mutation journals correctly without an implicit session."""
    original = p.client
    transports = []

    def fresh():
        transport = nfs4client.NFS4Client(p.args.host, p.args.port, 0)
        transport.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"compound-v40-probe"))
        transports.append(transport)
        p.client = transport

    try:
        fresh()
        name, owner = b"v40-fresh-transport", b"owner-v40-fresh-transport"
        res = p.call([op.putfh(p.directory), p.open_op(name, owner=owner), op.getfh(),
                      op.getattr(1 << FATTR4_SIZE)], "v40_unbound_open_suffix")
        fh, opened = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((name, fh, opened))
        p.file_owners[fh] = owner
        p.owner_seqids[owner] = 1
        require(res.resarray[1].rflags & OPEN4_RESULT_CONFIRM, "fresh owner did not require CONFIRM")

        fresh()
        confirm = op.open_confirm(opened, 1)
        res = p.call([op.putfh(fh), confirm, op.getattr(1 << FATTR4_SIZE)], "v40_unbound_confirm_suffix")
        sid = res.resarray[1].open_stateid
        p.replace_stateid(fh, sid)
        p.owner_seqids[owner] = 2
        fresh()
        replay = p.call([op.putfh(fh), confirm, op.getattr(1 << FATTR4_SIZE)], "v40_unbound_confirm_replay")
        require(same_sid(replay.resarray[1].open_stateid, sid), "fresh-transport CONFIRM replay advanced twice")

        fresh()
        lock = op.lock(WRITE_LT, False, 0, 4,
                       locker4(True, open_to_lock_owner4(2, sid, 0,
                                                       lock_owner4(p.clientid, b"unbound-range-owner"))))
        res = p.call([op.putfh(fh), lock, op.getattr(1 << FATTR4_SIZE)], "v40_unbound_lock_suffix")
        locked = res.resarray[1].lock_stateid
        p.owner_seqids[owner] = 3
        fresh()
        res = p.call([op.putfh(fh), op.locku(WRITE_LT, 1, locked, 0, 4),
                      op.open_downgrade(sid, 3, OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_DENY_NONE),
                      op.getattr(1 << FATTR4_SIZE)], "v40_unbound_unlock_downgrade")
        sid = res.resarray[2].open_stateid
        p.replace_stateid(fh, sid)
        p.owner_seqids[owner] = 4
        fresh()
        closed, res = p.close(fh, sid, "v40_unbound_close_suffix")
        fresh()
        replay = p.call([op.putfh(fh), closed, op.getattr(1 << FATTR4_SIZE)], "v40_unbound_close_replay")
        require(same_sid(replay.resarray[1].open_stateid, res.resarray[1].open_stateid),
                "fresh-transport CLOSE tombstone replay changed the result")

        # Confirm a different client on this same connection, then OPEN with
        # the original clientid. Its prefix and suffix must stay coalesced.
        identity = nfs_client_id4(b"othercid", f"compound-v40-other-{os.getpid()}".encode())
        callback = cb_client4(0, clientaddr4(b"tcp", b"0.0.0.0.0.0"))
        res = p.call([op.setclientid(identity, callback, 0)])
        other_clientid = res.resarray[0].clientid
        p.call([op.setclientid_confirm(other_clientid, res.resarray[0].setclientid_confirm)])
        name, owner = b"v40-mismatched-transport", b"owner-v40-mismatched-transport"
        res = p.call([op.putfh(p.directory), p.open_op(name, owner=owner), op.getfh(),
                      op.getattr(1 << FATTR4_SIZE)], "v40_mismatched_client_open_suffix")
        fh, sid = res.resarray[2].object, res.resarray[1].stateid
        p.files.append((name, fh, sid))
        p.file_owners[fh] = owner
        p.owner_seqids[owner] = 1
        if res.resarray[1].rflags & OPEN4_RESULT_CONFIRM:
            res = p.call([op.putfh(fh), op.open_confirm(sid, 1)])
            sid = res.resarray[1].open_stateid
            p.replace_stateid(fh, sid)
            p.owner_seqids[owner] = 2
    finally:
        p.client = original


def test_v40_single_io(p):
    _, fh, sid = p.create("v40-single-io")
    large = lambda: op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)

    def single(operation, label, expected=NFS4_OK):
        result = p.call([op.putfh(fh), operation, large()], "v40_single_" + label,
                        NFS4ERR_NOTDIR if expected == NFS4_OK else expected,
                        runs=[(0, 3)])
        return result.resarray[1]

    require(single(op.write(sid, 0, FILE_SYNC4, b"v40-data"), "write").count == 8,
            "single v4.0 WRITE lost its count")
    require(single(op.read(sid, 0, 16), "read").data == b"v40-data", "single v4.0 READ lost data")
    single(op.setattr(sid, {FATTR4_SIZE: 3}), "setattr")
    require(p.read(fh, sid) == b"v40", "single v4.0 SETATTR lost truncation")
    single(op.lockt(READ_LT, 0, 4, lock_owner4(p.clientid, b"v40-single-lockt")), "lockt")
    _, other, wrong = p.create("v40-single-other")
    for label, operation in (("read", op.read(wrong, 0, 16)),
                              ("write", op.write(wrong, 0, FILE_SYNC4, b"BAD")),
                              ("setattr", op.setattr(wrong, {FATTR4_SIZE: 0}))):
        single(operation, label + "_wrong_fh", NFS4ERR_BAD_STATEID)
    require(p.read(fh, sid) == b"v40" and p.read(other, wrong) == b"", "wrong-FH v4.0 I/O changed data")


def test_v40_standalone_open(p):
    name, fh, original = p.create("v40-standalone-open")
    owner = p.file_owners[fh]
    operations = [op.putfh(p.directory), p.open_op(name, create=False),
                  op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)]
    result = p.call(operations, "v40_standalone_open", NFS4ERR_NOTDIR, runs=[(0, 3)])
    opened = result.resarray[1].stateid
    p.replace_stateid(fh, opened)
    p.owner_seqids[owner] += 1
    check_open_version(original, opened, "standalone v4.0 OPEN lost its identity")
    replay = p.call(operations, "v40_standalone_open_replay", NFS4ERR_NOTDIR, runs=[(0, 3)])
    require(repr(result.resarray[1]) == repr(replay.resarray[1]), "standalone v4.0 replay changed the OPEN result")

    denied = [op.putfh(p.directory), p.open_op(name, create=False), operations[-1]]
    cred = p.client.default_cred
    p.client.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"other-v40-open-principal"))
    try:
        p.call(denied, "v40_standalone_principal", NFS4ERR_ACCESS, runs=[(0, 3)])
        p.call(denied, "v40_standalone_principal_replay", NFS4ERR_ACCESS, runs=[(0, 3)])
    finally:
        p.client.set_cred(cred)
    p.owner_seqids[owner] += 1
    result = p.call([op.putfh(p.directory), p.open_op(name, create=False), operations[-1]],
                   "v40_open_after_consumed_access", NFS4ERR_NOTDIR, runs=[(0, 3)])
    p.owner_seqids[owner] += 1
    check_open_version(opened, result.resarray[1].stateid, "principal failure advanced the OPEN stateid")
    p.replace_stateid(fh, result.resarray[1].stateid)


def test_v40_stream_open(p):
    _, base, _ = p.create("v40-stream-base")
    p.call([op.putfh(base), op.openattr(True), op.getfh()], "v40_attrdir")
    name, owner = b"retry-v40-stream", b"v40-stream-owner"
    operations = [op.putfh(base), op.openattr(True), p.open_op(name, owner=owner), op.getfh()]
    result = p.call(operations, "v40_stream_create")
    fh, opened = result.resarray[-1].object, result.resarray[2].stateid
    replay = p.call(operations, "v40_stream_create_replay")
    require(repr(result.resarray[2]) == repr(replay.resarray[2]) and replay.resarray[-1].object == fh,
            "stream create replay changed OPEN or current FH")
    require(result.resarray[2].rflags & OPEN4_RESULT_CONFIRM, "stream owner did not require confirmation")
    confirmed = p.call([op.putfh(fh), op.open_confirm(opened, 1)]).resarray[-1].open_stateid
    p.owner_seqids[owner] = 2
    p.call([op.putfh(fh), op.write(confirmed, 0, FILE_SYNC4, b"stream-replay")])
    predicted = stateid4(confirmed.seqid + 1, confirmed.other)
    operations = [op.putfh(base), op.openattr(True), p.open_op(name, create=False, owner=owner),
                  op.read(predicted, 0, 64), op.getfh()]
    result = p.call(operations, "v40_stream_reopen")
    replay = p.call(operations, "v40_stream_reopen_replay")
    require(same_sid(result.resarray[2].stateid, predicted) and
            repr(result.resarray[2]) == repr(replay.resarray[2]) and
            result.resarray[3].data == replay.resarray[3].data == b"stream-replay" and
            result.resarray[-1].object == replay.resarray[-1].object == fh,
            "stream reopen/retry lost access, advanced state twice or restored the base FH")
    p.call([op.putfh(fh), op.close(3, predicted)])
    mask = ((1 << 56) - 1) & ~((1 << FATTR4_TIME_ACCESS_SET) | (1 << FATTR4_TIME_MODIFY_SET))
    result = p.call([op.putfh(base), op.openattr(False), op.readdir(0, b"", 4096, 4096, mask),
                    op.readdir(0, b"", 4096, 4096, mask), op.getfh()], "v40_stream_readdir")
    for listing in result.resarray[2:4]:
        require(listing.reply.eof and len(listing.reply.entries) == 1 and
                listing.reply.entries[0].name == name and
                listing.reply.entries[0].attrs[FATTR4_FILEHANDLE] == fh and
                listing.reply.entries[0].attrs[FATTR4_SIZE] == len(b"stream-replay"),
                "v4.0 named READDIR lost its stream identity or duplicated a retry page")
    p.call([op.putfh(base), op.openattr(False), op.readdir(0, b"", 16, 16, mask), op.getfh()],
           "v40_stream_readdir_too_small", NFS4ERR_TOOSMALL)


def test_v40_root_entry(p):
    sibling = p.directory
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    p.directory = root
    name, fh, sid = p.create("retry-v40-root-entry")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"root-entry")])
    for label, root_op in (("root", op.putrootfh()), ("public", op.putpubfh())):
        result = p.call([root_op, op.getfh(), op.lookup(name), op.read(sid, 0, 64), op.getfh()],
                        "v40_" + label + "_entry")
        require(result.resarray[1].object == root and result.resarray[3].data == b"root-entry" and
                result.resarray[-1].object == fh, "v4.0 root entry lost its cursor or data")
    operation = p.open_op(name, create=False, access=OPEN4_SHARE_ACCESS_READ)
    predicted = stateid4(sid.seqid + 1, sid.other)
    operations = [op.putrootfh(), operation, op.getfh(), op.read(predicted, 0, 64)]
    result = p.call(operations, "v40_root_open")
    sid = result.resarray[1].stateid
    p.owner_seqids[p.file_owners[fh]] += 1
    p.replace_stateid(fh, sid)
    replay = p.call(operations, "v40_root_open_replay")
    require(same_sid(sid, predicted) and repr(replay.resarray[1]) == repr(result.resarray[1]) and
            result.resarray[2].object == replay.resarray[2].object == fh and
            result.resarray[-1].data == replay.resarray[-1].data == b"root-entry",
            "root entry lost OPEN owner replay, restored cursor or suffix data")
    result = p.call([op.putrootfh(), op.lookup(b"share"), op.getfh()],
                    "v40_export_entry", runs=[(0, 3)])
    require(result.resarray[-1].object == sibling, "v4.0 junction selected the wrong export")
    p.call([op.putrootfh(), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE), op.getfh()],
           "v40_single_root", runs=[(0, 3)])
    p.call([op.putrootfh(), op.lookup(b"missing-v40-root-entry"), op.getfh()],
           "v40_root_failed_prefix", NFS4ERR_NOENT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(0,), default=0)
    parser.add_argument("--server-log", required=True)
    probe = Probe40(parser.parse_args())
    try:
        if os.environ.get("CHIMERA_COMPOUND_FEATURE", "").startswith("readdir"):
            from nfs4_readdir_sizing import readdir_sizing
            readdir_sizing(probe)
            probe.check_trace()
            return 0
        if os.environ.get("CHIMERA_COMPOUND_FEATURE") == "pseudo":
            from nfs4_compound_adoption import pseudo_root
            pseudo_root(probe)
            probe.check_trace()
            return 0
        if os.environ.get("CHIMERA_COMPOUND_FEATURE") == "cold_root":
            from nfs4_compound_adoption import cold_root
            cold_root(probe)
            probe.check_trace()
            return 0
        if os.environ.get("CHIMERA_COMPOUND_FEATURE") == "namespace":
            test_v40_root_entry(probe)
            from nfs4_compound_adoption import namespace_coalescing
            namespace_coalescing(probe)
            probe.check_trace()
            return 0
        test_v40_secinfo_cursor(probe)
        test_v40_fresh_open_replay(probe)
        test_v40_confirm_consumed_error(probe)
        test_v40_confirmed_open(probe)
        test_v40_confirmed_create_replay(probe)
        test_v40_downgrade(probe)
        test_v40_locks(probe)
        test_v40_transport_owner_admission(probe)
        test_v40_state_error_ordering(probe)
        test_v40_single_io(probe)
        test_v40_standalone_open(probe)
        if os.environ.get("CHIMERA_COMPOUND_FEATURE") == "metadata":
            test_v40_stream_open(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} wire/compound boundary checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
