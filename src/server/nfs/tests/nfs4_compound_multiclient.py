#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""NFSv4.0 compounds containing several independent state-owner clients.

Every measured call checks its full VFS submission span. The retry variant
uses only read-only backend operations; owner/state publication, replay and
connection rebinding must survive finish rejection without advancing twice.
"""

from nfs4_compound_v40_boundaries import *  # noqa: F401,F403


class ClientProbe40(Probe40):
    """Probe40 with a distinct client identity but reusable operation helpers."""

    def __init__(self, args, label, principal=b"compound-v40-probe"):
        self.args = args
        self.client = nfs4client.NFS4Client(args.host, args.port, 0)
        self.client.set_cred(AuthSys().init_cred(uid=0, gid=0, name=principal))
        self.measured = []
        self.expected_runs = {}
        self.files = []
        self.directories = []
        self.owner_seqids = {}
        self.file_owners = {}
        self.binding_expected = {}
        identity = nfs_client_id4(b"multicid", f"compound-multiclient-{os.getpid()}-{label}".encode())
        callback = cb_client4(0, clientaddr4(b"tcp", b"0.0.0.0.0.0"))
        res = self.call([op.setclientid(identity, callback, 0)])
        self.clientid = res.resarray[0].clientid
        self.call([op.setclientid_confirm(self.clientid, res.resarray[0].setclientid_confirm)])
        res = self.call([op.putrootfh(), op.lookup(args.export.encode()), op.getfh()])
        self.directory = res.resarray[-1].object

    def expect_binding(self, name, changes):
        self.binding_expected[f"boundary_{name}"] = changes

    def check_binding_trace(self):
        # These accepted-publication records also prove the final connection
        # binding: the last successful OPEN wins, even after another client's
        # state operation or a failed later OPEN. Retries must add no records.
        deadline = time.monotonic() + 3
        while True:
            text = Path(self.args.server_log).read_text(errors="replace")
            observed = {
                tag: [(int(index), int(clientid)) for index, clientid in re.findall(
                    rf"NFS4_VFS_CLIENT_BIND tag={re.escape(tag)} index=(\d+) clientid=(\d+)", text)]
                for tag in self.binding_expected
            }
            if all(len(observed[tag]) >= len(want) for tag, want in self.binding_expected.items()) or \
                    time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        for tag, want in self.binding_expected.items():
            require(observed[tag] == want,
                    f"{tag}: accepted client binding order {observed[tag]}, expected {want}")
            print(f"PASS binding {tag}: {want}", flush=True)


def remember(p, name, owner, fh, sid, next_seqid):
    p.files.append((name, fh, sid))
    p.file_owners[fh] = owner
    p.owner_seqids[owner] = next_seqid


def state(p):
    require(len(p.files) == 1, "mixed-client fixture expects one live file per client")
    return p.files[0]


def reopen(p):
    name, fh, sid = state(p)
    return p.open_op(name, create=False, owner=p.file_owners[fh], access=OPEN4_SHARE_ACCESS_READ)


def advanced(p, fh, old, new, owner_steps=1):
    require(new.other == old.other and new.seqid == old.seqid + 1,
            f"state advanced incorrectly: {old!r} -> {new!r}")
    p.replace_stateid(fh, new)
    p.owner_seqids[p.file_owners[fh]] += owner_steps


def fresh_clients_and_confirmation(p, q):
    owner = b"identical-open-owner-bytes"
    a, b = b"retry-multiclient-a", b"retry-multiclient-b"
    operations = [op.putfh(p.directory), p.open_op(a, owner=owner), op.getfh(), op.savefh(),
                  op.putfh(q.directory), q.open_op(b, owner=owner), op.getfh(), op.restorefh(), op.getfh()]
    res = p.call(operations, "multiclient_fresh_opens")
    af, ass = res.resarray[2].object, res.resarray[1].stateid
    bf, bs = res.resarray[6].object, res.resarray[5].stateid
    require(af != bf and ass.other != bs.other and res.resarray[-1].object == af,
            "different clientids aliased the same OPEN owner or damaged saved FH")
    require(res.resarray[1].rflags & OPEN4_RESULT_CONFIRM and res.resarray[5].rflags & OPEN4_RESULT_CONFIRM,
            "fresh client owners must both require confirmation")
    remember(p, a, owner, af, ass, 1)
    remember(q, b, owner, bf, bs, 1)
    p.expect_binding("multiclient_fresh_opens", [(5, q.clientid)])

    operations = [op.putfh(af), op.open_confirm(ass, 1), op.getattr(1 << FATTR4_SIZE),
                  op.putfh(bf), op.open_confirm(bs, 1), op.getfh()]
    res = p.call(operations, "multiclient_confirm")
    confirmed_a, confirmed_b = res.resarray[1].open_stateid, res.resarray[4].open_stateid
    advanced(p, af, ass, confirmed_a)
    advanced(q, bf, bs, confirmed_b)
    replay = p.call(operations, "multiclient_confirm_replay")
    require(same_sid(replay.resarray[1].open_stateid, confirmed_a) and
            same_sid(replay.resarray[4].open_stateid, confirmed_b), "mixed CONFIRM replay advanced state")
    p.expect_binding("multiclient_confirm", [])
    p.expect_binding("multiclient_confirm_replay", [])
    p.call([op.putfh(af), op.write(confirmed_a, 0, FILE_SYNC4, b"alpha")])
    q.call([op.putfh(bf), op.write(confirmed_b, 0, FILE_SYNC4, b"bravo")])


def mixed_open_replay_and_downgrade(p, q):
    _, af, ass = state(p)
    _, bf, bs = state(q)
    predicted_a, predicted_b = stateid4(ass.seqid + 1, ass.other), stateid4(bs.seqid + 1, bs.other)
    operations = [op.putfh(p.directory), reopen(p), op.read(predicted_a, 0, 5), op.getfh(), op.savefh(),
                  op.putfh(q.directory), reopen(q), op.read(predicted_b, 0, 5), op.getfh(),
                  op.restorefh(), op.getfh()]
    res = p.call(operations, "multiclient_reopen_read")
    opened_a, opened_b = res.resarray[1].stateid, res.resarray[6].stateid
    require(res.resarray[2].data == b"alpha" and res.resarray[7].data == b"bravo" and
            res.resarray[3].object == af and res.resarray[8].object == bf and res.resarray[-1].object == af,
            "mixed OPEN/READ lost operation identity or current/saved FH")
    advanced(p, af, ass, opened_a)
    advanced(q, bf, bs, opened_b)
    replay = p.call(operations, "multiclient_reopen_replay")
    require(repr(replay.resarray[1]) == repr(res.resarray[1]) and
            repr(replay.resarray[6]) == repr(res.resarray[6]) and replay.resarray[2].data == b"alpha" and
            replay.resarray[7].data == b"bravo", "mixed OPEN replay changed a result or used the wrong client")
    for name in ("multiclient_reopen_read", "multiclient_reopen_replay"):
        p.expect_binding(name, [(1, p.clientid), (6, q.clientid)])

    operations = [op.putfh(af), op.open_downgrade(opened_a, p.owner_seqids[p.file_owners[af]],
                                                OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.read(stateid4(opened_a.seqid + 1, opened_a.other), 0, 5),
                  op.putfh(bf), op.open_downgrade(opened_b, q.owner_seqids[q.file_owners[bf]],
                                                OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.read(stateid4(opened_b.seqid + 1, opened_b.other), 0, 5), op.getattr(1 << FATTR4_SIZE)]
    res = p.call(operations, "multiclient_downgrade_read")
    advanced(p, af, opened_a, res.resarray[1].open_stateid)
    advanced(q, bf, opened_b, res.resarray[4].open_stateid)
    require(res.resarray[2].data == b"alpha" and res.resarray[5].data == b"bravo",
            "mixed DOWNGRADE private state failed to authorize the corresponding READ")
    p.expect_binding("multiclient_downgrade_read", [])


def invalid_later_client_preserves_prefix(p, q):
    _, af, ass = state(p)
    _, bf, bs = state(q)
    invalid = reopen(q)
    invalid.opopen.owner.clientid = 0
    operations = [op.putfh(p.directory), reopen(p), op.getattr(1 << FATTR4_SIZE),
                  op.putfh(q.directory), invalid, op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")]
    res = p.call(operations, "multiclient_stale_later_open", NFS4ERR_STALE_CLIENTID)
    require(len(res.resarray) == 5 and res.resarray[1].status == NFS4_OK,
            "invalid later client rejected the earlier OPEN or executed its WRITE suffix")
    opened = res.resarray[1].stateid
    advanced(p, af, ass, opened)
    replay = p.call(operations, "multiclient_stale_later_replay", NFS4ERR_STALE_CLIENTID)
    require(same_sid(replay.resarray[1].stateid, opened), "failed-prefix replay advanced accepted OPEN twice")
    require(p.read(af, opened) == b"alpha" and q.read(bf, bs) == b"bravo",
            "invalid client changed either file or the other client's state")
    p.expect_binding("multiclient_stale_later_open", [(1, p.clientid)])
    p.expect_binding("multiclient_stale_later_replay", [])


def wrong_principal_preserves_prefix(p, r):
    _, af, ass = state(p)
    _, rf, rs = state(r)
    operations = [op.putfh(p.directory), reopen(p), op.getfh(), op.putfh(r.directory), reopen(r),
                  op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")]
    res = p.call(operations, "multiclient_wrong_principal", NFS4ERR_ACCESS)
    require(len(res.resarray) == 5 and res.resarray[2].object == af,
            "wrong-principal OPEN lost the accepted prefix or executed its suffix")
    advanced(p, af, ass, res.resarray[1].stateid)
    # ACCESS consumes the second OPEN owner's seqid, but its state version
    # and handle remain unchanged. The genuine principal can still use it.
    r.owner_seqids[r.file_owners[rf]] += 1
    require(r.read(rf, rs) == b"other principal", "principal failure changed the foreign client's state")
    p.expect_binding("multiclient_wrong_principal", [])


def wrong_fh_preserves_prefix(p, q):
    _, af, ass = state(p)
    _, bf, bs = state(q)
    operations = [op.putfh(af), op.open_downgrade(ass, p.owner_seqids[p.file_owners[af]],
                                                OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.getattr(1 << FATTR4_SIZE),
                  op.open_downgrade(bs, q.owner_seqids[q.file_owners[bf]],
                                    OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE),
                  op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")]
    res = p.call(operations, "multiclient_wrong_fh", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 4, "wrong-FH state operation did not suppress its suffix")
    advanced(p, af, ass, res.resarray[1].open_stateid)
    require(q.read(bf, bs) == b"bravo", "wrong FH advanced the second client's state")
    p.expect_binding("multiclient_wrong_fh", [])


def mixed_locks_and_close(p, q):
    _, af, ass = state(p)
    _, bf, bs = state(q)
    owner = b"identical-range-owner-bytes"
    wrong = op.lock(WRITE_LT, False, 0, 4,
                    locker4(True, open_to_lock_owner4(p.owner_seqids[p.file_owners[af]], ass, 0,
                                                     lock_owner4(q.clientid, owner))))
    res = p.call([op.putfh(af), op.getattr(1 << FATTR4_SIZE), wrong, op.getfh()],
                 "multiclient_lock_wrong_clientid", NFS4ERR_BAD_STATEID)
    require(len(res.resarray) == 3, "foreign new-lock-owner clientid was admitted")
    # BAD_STATEID must not consume the OPEN owner's sequence. Correcting the
    # clientid with the identical sequence below must succeed.
    lock_a = op.lock(WRITE_LT, False, 0, 4,
                     locker4(True, open_to_lock_owner4(p.owner_seqids[p.file_owners[af]], ass, 0,
                                                      lock_owner4(p.clientid, owner))))
    lock_b = op.lock(WRITE_LT, False, 0, 4,
                     locker4(True, open_to_lock_owner4(q.owner_seqids[q.file_owners[bf]], bs, 0,
                                                      lock_owner4(q.clientid, owner))))
    operations = [op.putfh(af), lock_a, op.getattr(1 << FATTR4_SIZE), op.putfh(bf), lock_b, op.getfh()]
    res = p.call(operations, "multiclient_locks")
    la, lb = res.resarray[1].lock_stateid, res.resarray[4].lock_stateid
    require(la.other != lb.other and res.resarray[-1].object == bf,
            "same lock-owner bytes for different clients aliased one lock journal")
    p.owner_seqids[p.file_owners[af]] += 1
    q.owner_seqids[q.file_owners[bf]] += 1
    replay = p.call(operations, "multiclient_locks_replay")
    require(same_sid(replay.resarray[1].lock_stateid, la) and same_sid(replay.resarray[4].lock_stateid, lb),
            "mixed LOCK replay advanced a lock state twice")
    operations = [op.putfh(af), op.lock(WRITE_LT, False, 8, 4,
                                       locker4(False, lock_owner=exist_lock_owner4(la, 1))),
                  op.getattr(1 << FATTR4_SIZE), op.putfh(bf), op.lock(WRITE_LT, False, 8, 4,
                                       locker4(False, lock_owner=exist_lock_owner4(lb, 1))), op.getfh()]
    res = p.call(operations, "multiclient_existing_locks")
    next_a, next_b = res.resarray[1].lock_stateid, res.resarray[4].lock_stateid
    require(next_a.other == la.other and next_a.seqid == la.seqid + 1 and
            next_b.other == lb.other and next_b.seqid == lb.seqid + 1,
            "mixed existing LOCK updated the wrong client or advanced twice")
    close_a = op.close(p.owner_seqids[p.file_owners[af]], ass)
    close_b = op.close(q.owner_seqids[q.file_owners[bf]], bs)
    res = p.call([op.putfh(af), op.locku(WRITE_LT, 2, next_a, 0, 16), close_a,
                  op.getattr(1 << FATTR4_SIZE), op.putfh(bf), op.locku(WRITE_LT, 2, next_b, 0, 16),
                  close_b, op.getattr(1 << FATTR4_SIZE)], "multiclient_unlock_close")
    require(res.resarray[1].lock_stateid.seqid == next_a.seqid + 1 and
            res.resarray[5].lock_stateid.seqid == next_b.seqid + 1,
            "mixed LOCKU state versions advanced incorrectly")
    p.owner_seqids[p.file_owners[af]] += 1
    q.owner_seqids[q.file_owners[bf]] += 1
    p.mark_closed(af)
    q.mark_closed(bf)
    replay = p.call([op.putfh(af), close_a, op.getattr(1 << FATTR4_SIZE), op.putfh(bf), close_b, op.getfh()],
                    "multiclient_close_tombstones")
    require(same_sid(replay.resarray[1].open_stateid, res.resarray[2].open_stateid) and
            same_sid(replay.resarray[4].open_stateid, res.resarray[6].open_stateid),
            "mixed CLOSE tombstone replay lost its client-independent result")
    p.call([op.release_lockowner(lock_owner4(p.clientid, owner)),
            op.release_lockowner(lock_owner4(q.clientid, owner)), op.putfh(af), op.getattr(1 << FATTR4_SIZE)],
           "multiclient_release_owners")
    p.call([op.release_lockowner(lock_owner4(p.clientid, owner)),
            op.release_lockowner(lock_owner4(0, owner)), op.putfh(af), op.getattr(1 << FATTR4_SIZE)],
           "multiclient_release_stale_suffix", NFS4ERR_STALE_CLIENTID)
    for name in ("multiclient_lock_wrong_clientid", "multiclient_locks", "multiclient_locks_replay",
                 "multiclient_existing_locks", "multiclient_unlock_close", "multiclient_close_tombstones",
                 "multiclient_release_owners", "multiclient_release_stale_suffix"):
        p.expect_binding(name, [])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(0,), default=0)
    parser.add_argument("--server-log", required=True)
    args = parser.parse_args()
    probes = []
    try:
        p = ClientProbe40(args, "first")
        probes.append(p)
        q = ClientProbe40(args, "second")
        probes.append(q)
        r = ClientProbe40(args, "foreign", principal=b"compound-foreign-principal")
        probes.append(r)
        require(len({p.clientid, q.clientid, r.clientid}) == 3, "fixture client identities collided")
        _, rf, rs = r.create("retry-multiclient-foreign")
        r.call([op.putfh(rf), op.write(rs, 0, FILE_SYNC4, b"other principal")])
        fresh_clients_and_confirmation(p, q)
        mixed_open_replay_and_downgrade(p, q)
        invalid_later_client_preserves_prefix(p, q)
        wrong_principal_preserves_prefix(p, r)
        wrong_fh_preserves_prefix(p, q)
        mixed_locks_and_close(p, q)
        p.check_trace()
        p.check_binding_trace()
        print(f"PASS: {len(p.measured)} mixed-client compound checks", flush=True)
    finally:
        for probe in reversed(probes):
            probe.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
