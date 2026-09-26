#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Real delegation and pNFS grants across VFS compound boundaries.

The wrapper selects CHIMERA_COMPOUND_FEATURE=delegation or pnfs. Every measured
request checks its complete wire span, not just the number of VFS submissions.
Delegations/layouts are actually granted, and callback traffic is observed.
"""

from nfs4_compound_boundaries import *  # noqa: F401,F403
import threading
from xdrdef.nfs4_type import (ff_layoutreturn4, layoutreturn4, layoutreturn_file4,
                            newoffset4, newtime4, layoutupdate4, nfstime4)
from xdrdef.nfs4_pack import NFS4Packer, NFS4Unpacker


class FeatureProbe(Probe):
    def __init__(self, args):
        self.args = args
        self.feature = os.environ["CHIMERA_COMPOUND_FEATURE"]
        self.client = nfs4client.NFS4Client(args.host, args.port, args.minor)
        self.client.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"compound-features"))
        flags = EXCHGID4_FLAG_USE_PNFS_MDS if self.feature.startswith("pnfs") else 0
        record = self.client.new_client(f"compound-features-{os.getpid()}".encode(), flags=flags)
        if flags:
            require(record.flags & flags, "metadata server did not advertise pNFS support")
        attrs = channel_attrs4(0, 1024 * 1024, 1024 * 1024, 1024 * 1024, 128, 8, [])
        self.session = record.create_session(fore_attrs=attrs)
        self.session.compound([op.reclaim_complete(False)])
        self.measured, self.files, self.directories = [], [], []
        self.expected_runs = {}
        self.delegations, self.extra_opens = [], []
        self.removed_names = set()
        self.layout = None
        res = self.call([op.putrootfh(), op.lookup(args.export.encode()), op.getfh()])
        self.directory = res.resarray[-1].object

    def call(self, operations, name=None, expected=NFS4_OK, **kwargs):
        if name and "runs" not in kwargs:
            kwargs["runs"] = [(1, len(operations))]
        return super().call(operations, name, expected, **kwargs)

    def call_as(self, session, operations, name=None, expected=NFS4_OK):
        previous, self.session = self.session, session
        try:
            return self.call(operations, name, expected)
        finally:
            self.session = previous

    def other_session(self, label):
        return self.client.new_client_session(f"feature-{label}-{os.getpid()}".encode())

    def sent_result(self, operations, name, result, expected=NFS4_OK):
        require(result.status == expected,
                f"{name}: expected {nfsstat4[expected]}, got {result!r}")
        tag = f"boundary_{name}"
        self.measured.append(tag)
        self.expected_runs[tag] = [(1, len(operations))]
        print(f"PASS wire {name}: {nfsstat4[result.status]}", flush=True)
        return result

    def return_delegation(self, fh, sid):
        self.call([op.putfh(fh), op.delegreturn(sid)])
        self.delegations = [(f, s) for f, s in self.delegations if s.other != sid.other]

    def return_layout(self, fh, sid):
        body = NFS4Packer()
        body.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
        self.call([op.putfh(fh), op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                  layoutreturn4(LAYOUTRETURN4_FILE,
                                layoutreturn_file4(0, NFS4_UINT64_MAX, sid, body.get_buffer())))])
        self.layout = None

    def cleanup(self):
        if self.layout:
            self.return_layout(*self.layout)
        for fh, sid in list(self.delegations):
            self.return_delegation(fh, sid)
        for session, fh, sid in reversed(self.extra_opens):
            self.call_as(session, [op.putfh(fh), op.close(0, sid)])
        for name, fh, sid in reversed(self.files):
            if sid is not None:
                self.call([op.putfh(fh), op.close(0, sid)])
            if name not in self.removed_names:
                self.call([op.putfh(self.directory), op.remove(name)])


def delegation_open(p, name, fh, label=None):
    operation = p.open_op(name, create=False, owner=b"deleg-owner-" + name)
    operation.opopen.share_access = OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG
    res = p.call([op.putfh(p.directory), operation, op.getfh(), op.getattr(1 << FATTR4_SIZE)], label)
    opened = res.resarray[1]
    p.extra_opens = [(session, handle, sid) for session, handle, sid in p.extra_opens
                     if sid.other != opened.stateid.other]
    p.extra_opens.append((p.session, fh, opened.stateid))
    if opened.delegation.delegation_type != OPEN_DELEGATE_WRITE:
        return None
    sid = opened.delegation.write.stateid
    p.delegations.append((fh, sid))
    require(res.resarray[2].object == fh, "delegation OPEN returned a different object")
    return sid


def warm_backchannel(p):
    name, fh, _ = p.create("feature-callback-warmup")
    for _ in range(10):
        sid = delegation_open(p, name, fh)
        if sid is not None:
            p.return_delegation(fh, sid)
            print("PASS setup: write delegation granted over live backchannel", flush=True)
            return
        # Only callback setup may retry; measured requests never hide DELAY
        # or a missing delegation grant behind a client-side retry loop.
        time.sleep(0.05)
    raise AssertionError("could not obtain a write delegation with callbacks enabled")


def test_delegation(p):
    warm_backchannel(p)
    name, fh, opened = p.create("retry-feature-delegated")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"delegated")])
    recalled = threading.Event()
    recalls = []

    def recall_hook(arg, env):
        recalls.append(arg.stateid)
        env.notify = recalled.set

    p.session.client.cb_pre_hook(OP_CB_RECALL, recall_hook)
    delegated = delegation_open(p, name, fh, "delegation_grant_open")
    require(delegated is not None, "measured OPEN did not grant a write delegation")
    res = p.call([op.putfh(fh), op.read(delegated, 0, 16), op.getattr(1 << FATTR4_SIZE)],
                 "delegation_stateid_read")
    require(res.resarray[1].data == b"delegated" and not recalls, "holder READ recalled its own delegation")
    p.call([op.putfh(fh), op.write(delegated, 0, FILE_SYNC4, b"DELEGATED"),
            op.read(delegated, 0, 16)], "delegation_stateid_write_read")
    require(not recalls and p.read(fh, delegated) == b"DELEGATED", "holder WRITE recalled its own delegation")
    if p.args.minor >= 2:
        res = p.call([op.putfh(fh), op.read_plus(delegated, 0, 9), op.getfh()],
                     "delegation_stateid_read_plus")
        require(res.resarray[1].rpr_contents and not recalls,
                "holder READ_PLUS lost data or recalled its own delegation")

    base = p.call([op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE))])
    attrs = base.resarray[1].obj_attributes
    callback_attrs = {FATTR4_SIZE: attrs[FATTR4_SIZE] + 5, FATTR4_CHANGE: attrs[FATTR4_CHANGE] + 1}
    queried = threading.Event()
    queries = []

    def getattr_hook(arg, env, result):
        require(arg.fh == fh, "CB_GETATTR queried the wrong file")
        queries.append(arg.fh)
        result.obj_attributes = dict(callback_attrs)
        env.notify = queried.set
        return result

    p.session.client.cb_post_hook(OP_CB_GETATTR, getattr_hook)
    other = p.other_session("delegation-peer")
    res = p.call_as(other, [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)),
                            op.getfh()], "delegation_foreign_getattr_callback")
    require(queried.wait(2) and res.resarray[1].obj_attributes == callback_attrs and len(queries) == 1,
            "foreign GETATTR did not merge the actual CB_GETATTR reply")
    print("PASS CB_GETATTR: exactly one query for the logical operation, including any finish retry", flush=True)
    last_change = res.resarray[1].obj_attributes[FATTR4_CHANGE]
    for attempt in range(2):
        before = len(queries)
        res = p.call_as(other, [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)),
                               op.getfh()], "delegation_identical_callback_%d" % attempt)
        actual = res.resarray[1].obj_attributes
        require(actual[FATTR4_CHANGE] == last_change + 1 and
                actual[FATTR4_SIZE] == callback_attrs[FATTR4_SIZE],
                "identical holder change lost dirty state or advanced twice on retry")
        require(len(queries) == before + 1, "finish retry repeated CB_GETATTR")
        last_change = actual[FATTR4_CHANGE]
    before = len(queries)
    res = p.call_as(other, [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)),
                           op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)), op.getfh()],
                    "delegation_identical_callbacks_same_compound")
    for index in (1, 2):
        actual = res.resarray[index].obj_attributes
        require(actual[FATTR4_CHANGE] == last_change + 1 and
                actual[FATTR4_SIZE] == callback_attrs[FATTR4_SIZE],
                "same-compound GETATTR lost the previous operation's dirty journal")
        last_change = actual[FATTR4_CHANGE]
    require(len(queries) == before + 2, "compound retry repeated captured callback inputs")
    saved_size = callback_attrs.pop(FATTR4_SIZE)
    before = len(queries)
    res = p.call_as(other, [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)),
                           op.getfh()], "delegation_incomplete_callback_veto", NFS4ERR_DELAY)
    require(len(res.resarray) == 2 and len(queries) == before + 1,
            "incomplete callback served stale attrs or executed its suffix")
    callback_attrs[FATTR4_SIZE] = saved_size
    res = p.call_as(other, [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE))],
                    "delegation_after_incomplete_callback")
    require(res.resarray[1].obj_attributes[FATTR4_CHANGE] == last_change + 1 and
            res.resarray[1].obj_attributes[FATTR4_SIZE] == saved_size,
            "failed callback published a combine version or lost dirty size")
    last_change = res.resarray[1].obj_attributes[FATTR4_CHANGE]
    p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), op.verify({FATTR4_SIZE: 9999}),
            op.read(delegated, 0, 9)], "delegation_verify_failed_prefix", NFS4ERR_NOT_SAME)

    operation = p.open_op(name, create=False, owner=b"delegation-conflicting-owner")
    operations = [op.putfh(p.directory), operation, op.getfh(),
                  op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE))]
    slot = other.compound_async(operations, tag=b"boundary_delegation_conflict_delay")
    require(recalled.wait(3), "conflicting OPEN did not send CB_RECALL")
    res = p.sent_result(operations, "delegation_conflict_delay", other.listen(slot), NFS4ERR_DELAY)
    require(len(res.resarray) == 2 and recalls[-1].other == delegated.other,
            "recall targeted the wrong state or permitted OPEN suffix")
    p.return_delegation(fh, delegated)
    res = p.call_as(other, operations, "delegation_conflict_after_return")
    p.extra_opens.append((other, fh, res.resarray[1].stateid))
    require(res.resarray[2].object == fh, "post-recall OPEN changed file identity")
    require(res.resarray[3].obj_attributes[FATTR4_CHANGE] >= last_change,
            "delegation return regressed the published CHANGE value")
    last_change = res.resarray[3].obj_attributes[FATTR4_CHANGE]
    peer_opened = res.resarray[1].stateid
    res = p.call_as(other, [op.putfh(fh), op.write(peer_opened, 0, FILE_SYNC4, b"post-return"),
                           op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE))],
                    "delegation_return_write_change")
    require(res.resarray[2].obj_attributes[FATTR4_CHANGE] > last_change,
            "retained delegation CHANGE hid a subsequent backend write")
    last_change = res.resarray[2].obj_attributes[FATTR4_CHANGE]
    p.call_as(other, [op.putfh(fh), op.verify({FATTR4_CHANGE: last_change}),
                     op.getfh()], "delegation_return_verify_change")
    res = p.call_as(other, [op.putfh(p.directory),
                           op.readdir(0, b"", 4096, 4096, 1 << FATTR4_CHANGE), op.getfh()],
                    "delegation_return_readdir_change")
    matching = [entry for entry in res.resarray[1].reply.entries if entry.name == name]
    require(len(matching) == 1 and matching[0].attrs[FATTR4_CHANGE] == last_change,
            "READDIR disagreed with GETATTR's retained CHANGE value")

    # The backend snapshot precedes CB_GETATTR. Returning a delegation while
    # the callback is pending must not expose that snapshot after a final flush.
    name, fh, opened = p.create("delegation-query-return")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"old")])
    delegated = delegation_open(p, name, fh)
    require(delegated is not None, "query-return fixture did not get a delegation")
    query_started, allow_reply = threading.Event(), threading.Event()
    return_queries = []

    def return_during_query(arg, env, result):
        require(arg.fh == fh, "pending query targeted the wrong file")
        return_queries.append(arg.fh)
        query_started.set()
        require(allow_reply.wait(3), "query-return fixture did not release callback")
        result.obj_attributes = {FATTR4_SIZE: 3, FATTR4_CHANGE: 4}
        return result

    p.session.client.cb_post_hook(OP_CB_GETATTR, return_during_query)
    operations = [op.putfh(fh), op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_CHANGE)), op.getfh()]
    slot = other.compound_async(operations, tag=b"boundary_delegation_return_during_query")
    try:
        require(query_started.wait(2), "peer GETATTR did not query the holder")
        p.call([op.putfh(fh), op.write(delegated, 0, FILE_SYNC4, b"flushed-before-return")])
        p.return_delegation(fh, delegated)
    finally:
        allow_reply.set()
    res = p.sent_result(operations, "delegation_return_during_query", other.listen(slot), NFS4ERR_DELAY)
    require(len(res.resarray) == 2 and len(return_queries) == 1,
            "returned holder exposed a stale snapshot or repeated callback on retry")
    res = p.call_as(other, operations, "delegation_query_after_return")
    require(res.resarray[1].obj_attributes[FATTR4_SIZE] == len(b"flushed-before-return"),
            "fresh query after holder return missed flushed data")


def test_delegated_namespace(p):
    p.session.client.cb_post_hook(OP_CB_GETATTR)
    other = p.other_session("namespace-peer")
    recalled = threading.Event()
    recalls = []

    def recall_hook(arg, env):
        recalls.append(arg.stateid)
        env.notify = recalled.set

    p.session.client.cb_pre_hook(OP_CB_RECALL, recall_hook)
    name, fh, opened = p.create("delegated-remove")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"removed-open-file")])
    delegated = delegation_open(p, name, fh)
    require(delegated is not None, "REMOVE fixture did not hold a delegation")
    operations = [op.putfh(p.directory), op.remove(name), op.getattr(1 << FATTR4_SIZE)]
    res = p.call_as(other, operations, "delegation_remove_recall_delay", NFS4ERR_DELAY)
    require(len(res.resarray) == 2 and recalled.wait(3) and recalls[-1].other == delegated.other,
            "REMOVE did not recall its target before stopping the suffix")
    present = p.call([op.putfh(p.directory), op.lookup(name), op.getfh()])
    require(present.resarray[2].object == fh, "REMOVE unlinked a still-delegated file")
    p.return_delegation(fh, delegated)
    p.call_as(other, operations, "delegation_remove_after_return")
    p.removed_names.add(name)
    require(p.read(fh, opened) == b"removed-open-file", "unlinked open file lost its contents")

    source, source_fh, source_sid = p.create("delegated-rename-source")
    target, target_fh, target_sid = p.create("delegated-rename-target")
    p.call([op.putfh(source_fh), op.write(source_sid, 0, FILE_SYNC4, b"source")])
    p.call([op.putfh(target_fh), op.write(target_sid, 0, FILE_SYNC4, b"target")])
    delegated = delegation_open(p, source, source_fh)
    require(delegated is not None, "RENAME fixture did not hold a source delegation")
    recalled.clear()
    operations = [op.putfh(p.directory), op.savefh(), op.rename(source, target),
                  op.lookup(target), op.getfh()]
    res = p.call_as(other, operations, "delegation_rename_recall_delay", NFS4ERR_DELAY)
    require(len(res.resarray) == 3 and recalled.wait(3) and recalls[-1].other == delegated.other,
            "RENAME did not recall its source before stopping the suffix")
    present = p.call([op.putfh(p.directory), op.lookup(source), op.getfh(),
                      op.putfh(p.directory), op.lookup(target), op.getfh()])
    require(present.resarray[2].object == source_fh and present.resarray[5].object == target_fh,
            "RENAME changed namespace before delegation return")
    p.return_delegation(source_fh, delegated)
    res = p.call_as(other, operations, "delegation_rename_after_return")
    p.removed_names.add(source)
    require(res.resarray[4].object == source_fh and p.read(source_fh, source_sid) == b"source" and
            p.read(target_fh, target_sid) == b"target", "RENAME lost source or displaced-open contents")

    source, source_fh, source_sid = p.create("rename-victim-source")
    target, target_fh, target_sid = p.create("rename-delegated-victim")
    p.call([op.putfh(source_fh), op.write(source_sid, 0, FILE_SYNC4, b"replacement")])
    p.call([op.putfh(target_fh), op.write(target_sid, 0, FILE_SYNC4, b"victim")])
    delegated = delegation_open(p, target, target_fh)
    require(delegated is not None, "RENAME fixture did not hold a victim delegation")
    recalled.clear()
    operations = [op.putfh(p.directory), op.savefh(), op.rename(source, target),
                  op.lookup(target), op.getfh()]
    res = p.call_as(other, operations, "delegation_rename_victim_recall_delay", NFS4ERR_DELAY)
    require(len(res.resarray) == 3 and recalled.wait(3) and recalls[-1].other == delegated.other,
            "RENAME did not recall its displaced target before stopping the suffix")
    present = p.call([op.putfh(p.directory), op.lookup(source), op.getfh(),
                      op.putfh(p.directory), op.lookup(target), op.getfh()])
    require(present.resarray[2].object == source_fh and present.resarray[5].object == target_fh,
            "RENAME displaced a still-delegated target")
    p.return_delegation(target_fh, delegated)
    res = p.call_as(other, operations, "delegation_rename_victim_after_return")
    p.removed_names.add(source)
    require(res.resarray[4].object == source_fh and p.read(source_fh, source_sid) == b"replacement" and
            p.read(target_fh, target_sid) == b"victim", "victim recall changed either open file's data")


def test_direct_data_server(p, layout, mds_fh, opened):
    decoder = NFS4Unpacker(layout.logr_layout[0].loc_body)
    flex = decoder.unpack_ff_layout4()
    decoder.done()
    endpoint = flex.ffl_mirrors[0].ffm_data_servers[0]
    require(endpoint.ffds_fh_vers, "granted layout did not contain a data-server filehandle")
    fh, advertised_sid = endpoint.ffds_fh_vers[0], endpoint.ffds_stateid
    # Chimera currently advertises anonymous ffds_stateid. Also use the real
    # MDS layout stateid, which is not a DS OPEN slot, to exercise its existing
    # MDS-authorized data-server validation policy rather than only anonymity.
    sid = layout.logr_stateid
    args = argparse.Namespace(host=p.args.host, port=2050, export="ds_export", minor=p.args.minor,
                              server_log=str(Path(p.args.server_log).with_name("ds.log")))
    ds = Probe(args)
    # The layout must name the same authoritative data as ordinary MDS I/O.
    # In particular, do not seed a second DS copy to hide a coherence defect.
    res = ds.call([op.putfh(fh), op.read(advertised_sid, 0, 16), op.read(sid, 0, 16),
                   op.getattr(1 << FATTR4_SIZE)], "ds_layout_stateid_read", runs=[(1, 4)])
    require(res.resarray[1].data == b"abcdefgh" and res.resarray[2].data == b"abcdefgh",
            "data-server READ lost the layout's backing data or required a local OPEN stateid")
    res = ds.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"AB"), op.read(sid, 0, 8),
                   op.write(sid, 0, FILE_SYNC4, b"ab"), op.getattr(1 << FATTR4_SIZE)],
                  "ds_layout_stateid_write_read", runs=[(1, 5)])
    require(res.resarray[2].data == b"ABcdefgh" and res.resarray[4].obj_attributes[FATTR4_SIZE] == 8,
            "data-server WRITE/READ did not retain the MDS-authorized stateid semantics")
    ds.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"DS")])
    require(p.read(mds_fh, opened) == b"DScdefgh", "MDS did not observe a DS write")
    p.call([op.putfh(mds_fh), op.write(opened, 0, FILE_SYNC4, b"ab")])
    require(ds.call([op.putfh(fh), op.read(sid, 0, 8)]).resarray[1].data == b"abcdefgh",
            "DS did not observe an MDS write")
    if p.args.minor >= 2:
        res = ds.call([op.putfh(fh), op.read_plus(sid, 0, 8), op.read_plus(sid, 8, 8), op.getfh()],
                      "ds_layout_stateid_read_plus", runs=[(1, 4)])
        check_read_plus(res.resarray[1], NFS4_CONTENT_DATA, 0, b"abcdefgh", True)
        check_read_plus(res.resarray[2], None, 8, None, True)
    ds.check_trace()
    print(f"PASS: {len(ds.measured)} direct data-server compound checks", flush=True)
    return ds, fh, sid


def test_pnfs(p):
    name = b"pnfs-no-layout"
    res = p.call([op.putfh(p.directory), p.open_op(name), op.getfh(), op.getattr(1 << FATTR4_SIZE)],
                 "pnfs_enabled_open")
    fh, opened = res.resarray[2].object, res.resarray[1].stateid
    p.files.append((name, fh, opened))
    res = p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"without-layout"),
                  op.read(opened, 0, 32), op.setattr(opened, {FATTR4_SIZE: 7}),
                  op.getattr(1 << FATTR4_SIZE)], "pnfs_enabled_without_layout")
    require(res.resarray[2].data == b"without-layout" and res.resarray[4].obj_attributes[FATTR4_SIZE] == 7,
            "pNFS enabled/no-layout I/O changed semantics")
    _, fh, opened = p.create("retry-pnfs-held-layout")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"abcdefgh")])
    res = p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                          0, NFS4_UINT64_MAX, 1, opened, 65536)])
    layout = res.resarray[1]
    require(layout.logr_layout and layout.logr_stateid.seqid > 0, "LAYOUTGET did not grant an actual layout")
    p.layout = (fh, layout.logr_stateid)
    ds, ds_fh, ds_sid = test_direct_data_server(p, layout, fh, opened)
    p.call([op.putfh(fh), op.read(layout.logr_stateid, 0, 8), op.getattr(1 << FATTR4_SIZE)],
           "pnfs_mds_rejects_layout_read", NFS4ERR_BAD_STATEID)
    recalled = threading.Event()
    recall_args = []

    def op_cb_layoutrecall(arg, env):
        # Acknowledge the recall, but leave the layout held until the test has
        # established that SETATTR and its suffix are parked before mutation.
        recall_args.append(arg.opcblayoutrecall)
        env.notify = recalled.set
        return nfs4client.encode_status(NFS4_OK)

    p.client.op_cb_layoutrecall = op_cb_layoutrecall
    res = p.call([op.putfh(fh), op.test_stateid([opened]), op.read(opened, 0, 8),
                  op.getattr(1 << FATTR4_SIZE)],
                 "pnfs_held_layout_owner_read")
    require(res.resarray[1].tsr_status_codes == [NFS4_OK] and
            res.resarray[2].data == b"abcdefgh" and not recalled.is_set(),
            "holder read recalled its layout or lost existing data")
    p.call([op.putfh(fh), op.test_stateid([opened]), op.verify({FATTR4_SIZE: 9999}),
            op.read(opened, 0, 8)],
           "pnfs_held_layout_failed_prefix", NFS4ERR_NOT_SAME)
    other = p.other_session("layout-peer")
    operations = [op.putfh(fh), op.setattr(ANONYMOUS, {FATTR4_SIZE: 3}),
                  op.write(ANONYMOUS, 3, FILE_SYNC4, b"!"), op.read(ANONYMOUS, 0, 16),
                  op.getattr(1 << FATTR4_SIZE)]
    slot = other.compound_async(operations, tag=b"boundary_pnfs_layout_recall_before_truncate")
    done = threading.Event()
    result = {}

    def await_result():
        try:
            result["reply"] = other.listen(slot)
        except BaseException as error:
            result["error"] = error
        finally:
            done.set()

    listener = threading.Thread(target=await_result, daemon=True)
    listener.start()
    require(recalled.wait(3), "size SETATTR did not recall the held writable layout")
    require(not done.wait(0.05), "SETATTR completed before the client returned its layout")
    before = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE)])
    require(before.resarray[1].obj_attributes[FATTR4_SIZE] == 8,
            "SETATTR changed size while the conflicting layout was still held")
    p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                     0, NFS4_UINT64_MAX, 1, opened, 65536)],
           expected=NFS4ERR_RECALLCONFLICT)
    print("PASS layout barrier: mutation parked and new layout rejected before explicit return", flush=True)
    recall = recall_args[-1].clora_recall
    require(recall.lor_recalltype == LAYOUTRECALL4_FILE and recall.lor_layout.lor_fh == fh,
            "layout recall did not identify the held file")
    p.return_layout(fh, recall.lor_layout.lor_stateid)
    require(done.wait(5), "parked SETATTR did not resume after LAYOUTRETURN")
    if "error" in result:
        raise result["error"]
    res = p.sent_result(operations, "pnfs_layout_recall_before_truncate", result["reply"])
    require(res.resarray[3].data == b"abc!" and res.resarray[4].obj_attributes[FATTR4_SIZE] == 4,
            "post-recall truncate/write/read suffix produced incorrect data")
    res = ds.call([op.putfh(ds_fh), op.read(ds_sid, 0, 16), op.getattr(1 << FATTR4_SIZE)])
    require(res.resarray[1].data == b"abc!" and res.resarray[2].obj_attributes[FATTR4_SIZE] == 4,
            "DS did not observe MDS truncate and post-recall write")
    print("PASS coherent backing: pre-layout data, bidirectional writes, and truncate agree", flush=True)
    test_layoutcommit_metadata(p)


def test_layoutcommit_metadata(p):
    _, fh, opened = p.create("pnfs-layoutcommit-metadata")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"data")])
    result = p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                             0, NFS4_UINT64_MAX, 1, opened, 65536)])
    sid = result.resarray[1].logr_stateid
    p.layout = (fh, sid)
    result = p.call([op.test_stateid([sid])])
    require(result.resarray[0].tsr_status_codes == [NFS4_OK],
            "new layout stateid does not resolve in the server state table")
    result = p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                             0, NFS4_UINT64_MAX, 1, sid, 65536)])
    newer = result.resarray[1].logr_stateid
    require(newer.other == sid.other and newer.seqid == sid.seqid + 1,
            "layout upgrade changed identity or failed to advance its version")
    sid = newer
    p.layout = (fh, sid)
    forged = stateid4(sid.seqid, sid.other[:-1] + bytes([sid.other[-1] ^ 1]))
    p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                     0, NFS4_UINT64_MAX, 1, forged, 65536)],
           expected=NFS4ERR_BAD_STATEID)
    result = p.call([op.test_stateid([sid])])
    require(result.resarray[0].tsr_status_codes == [NFS4_OK],
            "forged LAYOUTGET changed the real layout's version or identity")
    body = NFS4Packer()
    body.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
    p.call([op.putfh(fh), op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                layoutreturn4(LAYOUTRETURN4_FILE,
                              layoutreturn_file4(0, NFS4_UINT64_MAX, forged, body.get_buffer())))],
           expected=NFS4ERR_BAD_STATEID)
    result = p.call([op.test_stateid([sid])])
    require(result.resarray[0].tsr_status_codes == [NFS4_OK],
            "forged LAYOUTRETURN destroyed the real layout")

    def commit(token, high, timestamp=None):
        return op.layoutcommit(0, NFS4_UINT64_MAX, False, token,
                               newoffset4(high is not None, high or 0),
                               newtime4(timestamp is not None, timestamp),
                               layoutupdate4(LAYOUT4_FLEX_FILES, b""))

    # Matching sequence numbers do not authorize a fabricated layout identity.
    bad = stateid4(sid.seqid, b"forgedlayout")
    p.call([op.putfh(fh), commit(bad, 23), op.getattr(1 << FATTR4_SIZE)],
           expected=NFS4ERR_BAD_STATEID)
    result = p.call([op.putfh(fh), commit(sid, 23), op.getattr(1 << FATTR4_SIZE)])
    changed = result.resarray[1].locr_newsize
    require(changed.ns_sizechanged and changed.ns_size == 24 and
            result.resarray[2].obj_attributes[FATTR4_SIZE] == 24,
            "LAYOUTCOMMIT did not return the actual post-SETATTR size")
    result = p.call([op.putfh(fh), commit(sid, 7), op.getattr(1 << FATTR4_SIZE)])
    require(not result.resarray[1].locr_newsize.ns_sizechanged and
            result.resarray[2].obj_attributes[FATTR4_SIZE] == 24,
            "LAYOUTCOMMIT shrank the file or reported a nonexistent size change")
    timestamp = nfstime4(123456789, 987654321)
    result = p.call([op.putfh(fh), commit(sid, None, timestamp),
                     op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_TIME_MODIFY))])
    actual = result.resarray[2].obj_attributes
    require(not result.resarray[1].locr_newsize.ns_sizechanged and actual[FATTR4_SIZE] == 24 and
            actual[FATTR4_TIME_MODIFY].seconds == timestamp.seconds and
            actual[FATTR4_TIME_MODIFY].nseconds == timestamp.nseconds,
            "mtime-only LAYOUTCOMMIT lost its timestamp or reported a size change")
    p.return_layout(fh, sid)
    result = p.call([op.test_stateid([sid])])
    require(result.resarray[0].tsr_status_codes != [NFS4_OK],
            "valid LAYOUTRETURN left its grant alive")
    p.return_layout(fh, sid)  # No held layout remains an idempotent success.
    print("PASS LAYOUTCOMMIT: identity, extension/postattrs, no shrink, and mtime-only commit", flush=True)


def test_pnfs_unsupported(p):
    name, fh, opened = p.create("retry-pnfs-independent-backing")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"authoritative")])
    p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                     0, NFS4_UINT64_MAX, 1, opened, 65536)],
           expected=NFS4ERR_LAYOUTUNAVAILABLE)
    reopened = p.call([op.putfh(p.directory), p.open_op(name, create=False), op.getfh()],
                      "pnfs_unsupported_reopen")
    opened = reopened.resarray[1].stateid
    p.files[-1] = (name, fh, opened)
    res = p.call([op.putfh(fh), op.read(opened, 0, 32),
                  op.getattr((1 << FATTR4_SIZE) | (1 << FATTR4_FS_LAYOUT_TYPES))],
                 "pnfs_no_independent_backing")
    require(res.resarray[1].data == b"authoritative" and
            not res.resarray[2].obj_attributes.get(FATTR4_FS_LAYOUT_TYPES, []),
            "unsupported backing advertised a layout or changed authoritative bytes")
    failed = p.call([op.putfh(p.directory), p.open_op(name, create=False),
                     op.verify({FATTR4_SIZE: 999}), op.getfh()],
                    "pnfs_unsupported_failed_prefix", expected=NFS4ERR_NOT_SAME)
    p.replace_stateid(fh, failed.resarray[1].stateid)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(1, 2), default=2)
    parser.add_argument("--server-log", required=True)
    probe = FeatureProbe(parser.parse_args())
    try:
        if probe.feature == "delegation":
            test_delegation(probe)
            test_delegated_namespace(probe)
        elif probe.feature == "pnfs_unsupported":
            test_pnfs_unsupported(probe)
        else:
            test_pnfs(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} real {probe.feature} compound checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
