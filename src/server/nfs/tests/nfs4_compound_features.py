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
                            newoffset4, newtime4, layoutupdate4, nfstime4, open_claim_delegate_cur4, specdata4, settime4)
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


def test_delegated_open(p, name, fh, opened, delegated):
    other_name, other_fh, other_sid = p.create("delegated-open-other")
    p.call([op.putfh(other_fh), op.write(other_sid, 0, FILE_SYNC4, b"other")])

    def claim(named, sid=delegated, target=name, **kwargs):
        operation = p.open_op(target, owner=b"claim-" + str(named).encode() + target,
                              create=kwargs.pop("create", False), **kwargs)
        operation.opopen.claim = (open_claim4(CLAIM_DELEGATE_CUR,
            delegate_cur_info=open_claim_delegate_cur4(sid, target)) if named else
            open_claim4(CLAIM_DELEG_CUR_FH, oc_delegate_stateid=sid))
        return operation

    for named in (True, False):
        label = "name" if named else "fh"
        parent = p.directory if named else fh
        res = p.call([op.putfh(parent), claim(named), op.read(CURRENT, 0, 16),
                      op.getfh(), op.close(0, CURRENT)], "delegated_open_" + label)
        require(res.resarray[2].data == b"delegated" and res.resarray[3].object == fh,
                "delegated OPEN lost the resolved file or current stateid")
        require(res.resarray[1].delegation.delegation_type in (OPEN_DELEGATE_NONE, OPEN_DELEGATE_NONE_EXT),
                "delegated claim unexpectedly granted another delegation")
        # The requested READDIR bound no longer splits delegated OPEN;
        # its type failure must accept the preceding private OPEN result.
        res = p.call([op.putfh(parent), claim(named),
                      op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)],
                     "delegated_open_single_" + label, NFS4ERR_NOTDIR, runs=[(1, 3)])
        require(res.resarray[1].status == NFS4_OK, "single delegated OPEN did not complete")
        p.call([op.putfh(fh), op.close(0, res.resarray[1].stateid)])
        for failure, sid, target, cursor in (
                ("wrong_file", delegated, other_name, p.directory if named else other_fh),
                ("open_stateid", opened, name, parent),
                ("future_stateid", stateid4(delegated.seqid + 1, delegated.other), name, parent)):
            operations = [op.putfh(cursor), claim(named, sid, target),
                          op.write(CURRENT, 0, FILE_SYNC4, b"BAD")]
            res = p.call(operations, f"delegated_open_{label}_{failure}", NFS4ERR_BAD_STATEID)
            require(len(res.resarray) == 2, "invalid delegated OPEN executed its suffix")
        require(p.read(fh, delegated) == b"delegated" and p.read(other_fh, other_sid) == b"other",
                "invalid delegated OPEN changed file contents")

    p.call([op.putfh(p.directory), claim(True, create=True), op.getfh()],
           "delegated_open_guarded", NFS4ERR_EXIST)
    p.call([op.putfh(fh), claim(False, create=True), op.getfh()],
           "delegated_open_fh_create", NFS4ERR_INVAL)
    # A failed claim must not truncate before the delegation is validated.
    p.call([op.putfh(p.directory), claim(True, opened, create=True, create_mode=UNCHECKED4,
                                        attrs={FATTR4_SIZE: 0}), op.getfh()],
           "delegated_open_invalid_truncate", NFS4ERR_BAD_STATEID)
    require(p.read(fh, delegated) == b"delegated", "invalid delegated claim truncated the file")
    res = p.call([op.putfh(p.directory), claim(True, create=True, create_mode=UNCHECKED4,
                                             attrs={FATTR4_SIZE: 0}),
                  op.read(CURRENT, 0, 16), op.close(0, CURRENT)], "delegated_open_truncate")
    require(res.resarray[2].data == b"", "valid delegated UNCHECKED OPEN did not truncate")
    p.call([op.putfh(fh), op.write(delegated, 0, FILE_SYNC4, b"delegated")])


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
    test_delegated_open(p, name, fh, opened, delegated)
    require(not recalls, "holder's delegated OPEN recalled its own delegation")
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
        returned = p.call([op.putfh(fh), op.delegreturn(delegated),
                           op.test_stateid([delegated]), op.getfh()],
                          "delegation_return_with_pending_query")
        require(returned.resarray[2].tsr_status_codes == [NFS4ERR_BAD_STATEID] and
                returned.resarray[-1].object == fh,
                "pending query return kept its stateid valid or lost the cursor")
        p.delegations = [(f, s) for f, s in p.delegations if s.other != delegated.other]
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


def test_pnfs_resident_remove(p):
    ds_args = argparse.Namespace(host=p.args.host, port=2050, export="ds_export", minor=p.args.minor,
                                 server_log=str(Path(p.args.server_log).with_name("ds.log")))
    ds = Probe(ds_args)

    def backing_names():
        result = ds.call([op.putfh(ds.directory), op.readdir(0, b"", 4096, 65536, 1 << FATTR4_TYPE)])
        require(result.resarray[-1].reply.eof, "backing directory unexpectedly paginated")
        return {entry.name for entry in result.resarray[-1].reply.entries}

    empty_name, empty_fh, empty_sid = p.create("pnfs-layoutget-materialize")
    materialized = p.call([op.putfh(empty_fh), layoutget_op(empty_sid), op.getfh(),
                           op.getattr(1 << FATTR4_SIZE)], "pnfs_layoutget_materialize")
    require(materialized.resarray[2].object == empty_fh and
            materialized.resarray[3].obj_attributes[FATTR4_SIZE] == 0,
            "materializing LAYOUTGET lost the MDS cursor or changed its size")
    layout_sid = test_layoutget_reply_budget(p, empty_fh, materialized.resarray[1].logr_stateid)
    p.return_layout(empty_fh, layout_sid)
    before = backing_names()
    name, fh, sid = p.create("resident-remove")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"resident-data"), op.read(sid, 0, 64)],
           "pnfs_resident_write")
    created = backing_names() - before
    require(len(created) == 1, f"first WRITE did not create exactly one backing file: {created!r}")
    backing = created.pop()
    require(b"_" in backing, "backing filename lacks its mount identity")
    layout = p.call([op.putfh(fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                                              0, NFS4_UINT64_MAX, 1, sid, 65536)])
    layout_sid = layout.resarray[-1].logr_stateid
    p.layout = (fh, layout_sid)
    # A synthetic attribute directory must not authorize a layout for its base
    # file. This uses local memfs because the proxy export lacks OPENATTR.
    p.call([op.putfh(fh), op.openattr(False),
            op.layoutcommit(0, NFS4_UINT64_MAX, False, layout_sid, newoffset4(True, 99),
                            newtime4(False), layoutupdate4(LAYOUT4_FLEX_FILES, b""))],
           "pnfs_layoutcommit_attrdir", NFS4ERR_BAD_STATEID, runs=[(1, 3)])
    p.return_layout(fh, layout_sid)
    require(p.read(fh, sid) == b"resident-data", "attribute-directory LAYOUTCOMMIT changed its base file")
    p.call([op.putfh(fh), op.close(0, sid)])
    p.mark_closed(fh)
    alias = b"resident-alias"
    p.call([op.putfh(fh), op.savefh(), op.putfh(p.directory), op.link(alias)], "pnfs_resident_link")
    p.files.append((alias, fh, None))
    for victim, last in ((name, False), (alias, True)):
        result = p.call([op.putfh(p.directory), op.remove(victim), op.getfh()],
                        "pnfs_resident_remove_" + str(int(last)))
        p.removed_names.add(victim)
        require(result.resarray[-1].object == p.directory, "REMOVE lost the parent cursor")
        require((backing not in backing_names()) == last, "backing cleanup did not respect the last link")
        if not last:
            require(p.read(fh, ANONYMOUS) == b"resident-data", "removing one link lost resident data")
    ds.call([op.putfh(ds.directory), op.lookup(backing)], expected=NFS4ERR_NOENT)
    test_pnfs_remove_batch(p, ds, backing_names)


def layoutget_op(sid, mode=LAYOUTIOMODE4_RW, maxcount=65536):
    return op.layoutget(False, LAYOUT4_FLEX_FILES, mode, 0, NFS4_UINT64_MAX, 1, sid, maxcount)


def test_layoutget_compounds(p):
    name, fh, opened = p.create("retry-layoutget-coalesced")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"layout-bytes")])
    # A previously opened file is reopened inside this span, and its private
    # stateid authorizes the first grant before OPEN has become public.
    operations = [op.putfh(p.directory), p.open_op(name, create=False),
                  op.savefh(), layoutget_op(CURRENT), op.getattr(1 << FATTR4_SIZE),
                  layoutget_op(CURRENT), op.io_advise(CURRENT, 0, 1, 0), op.getfh(),
                  op.restorefh(), op.close(0, CURRENT)]
    result = p.call(operations, "pnfs_layoutget_open_current_repeated")
    first, second = result.resarray[3].logr_stateid, result.resarray[5].logr_stateid
    require(first.other == second.other and second.seqid == first.seqid + 1,
            "repeated private LAYOUTGET did not advance exactly once per operation")
    require(result.resarray[4].obj_attributes[FATTR4_SIZE] == len(b"layout-bytes") and
            result.resarray[7].object == fh, "LAYOUTGET lost its filehandle or metadata suffix")
    p.mark_closed(fh)
    p.layout = (fh, second)
    p.call([op.putfh(fh), op.io_advise(second, 0, 1, 0), op.getattr(1 << FATTR4_SIZE)],
           "pnfs_layout_advise")
    _, other_fh, _ = p.create("layoutget-other")
    p.call([op.putfh(other_fh), op.io_advise(second, 0, 1, 0), op.getfh()],
           "pnfs_layout_advise_wrong_fh", NFS4ERR_BAD_STATEID)
    p.call([op.putfh(fh), op.io_advise(stateid4(second.seqid + 1, second.other), 0, 1, 0), op.getfh()],
           "pnfs_layout_advise_future", NFS4ERR_BAD_STATEID)
    # TOOSMALL stops the suffix before journal publication and cannot bump the
    # old grant's version, even when finish rejects and repeats this attempt.
    result = p.call([op.putfh(fh), layoutget_op(second, maxcount=1), op.getfh()],
                    "pnfs_layoutget_too_small", NFS4ERR_TOOSMALL)
    require(len(result.resarray) == 2, "TOOSMALL LAYOUTGET executed its suffix")
    require(p.call([op.test_stateid([second])]).resarray[0].tsr_status_codes == [NFS4_OK],
            "failed LAYOUTGET advanced the public layout stateid")
    # LAYOUTCOMMIT may consume an earlier private layout stateid without
    # waiting for that state to be published in the server-wide slot table.
    result = p.call([op.putfh(fh), layoutget_op(second),
                     op.layoutcommit(0, NFS4_UINT64_MAX, False, CURRENT, newoffset4(False, 0),
                                     newtime4(False), layoutupdate4(LAYOUT4_FLEX_FILES, b"")),
                     op.getfh()], "pnfs_layoutget_commit_current")
    second = result.resarray[1].logr_stateid
    p.layout = (fh, second)
    require(not result.resarray[2].locr_newsize.ns_sizechanged and result.resarray[3].object == fh,
            "private LAYOUTCOMMIT changed size or lost the layout's cursor")
    decoder = NFS4Unpacker(result.resarray[1].logr_layout[0].loc_body)
    flex = decoder.unpack_ff_layout4()
    decoder.done()
    device = flex.ffl_mirrors[0].ffm_data_servers[0].ffds_deviceid
    result = p.call([op.putfh(fh), layoutget_op(second),
                     op.getdeviceinfo(device, LAYOUT4_FLEX_FILES, 65536, 0),
                     op.getattr(1 << FATTR4_SIZE), op.getfh()],
                    "pnfs_layoutget_deviceinfo_current")
    second = result.resarray[1].logr_stateid
    p.layout = (fh, second)
    require(result.resarray[2].gdir_device_addr.da_layout_type == LAYOUT4_FLEX_FILES and
            result.resarray[4].object == fh,
            "GETDEVICEINFO lost the preceding layout or changed its cursor")
    p.return_layout(fh, second)


def test_layoutget_reply_budget(p, fh, second):
    # The actual values and final layout fit in one arena and one attempt.
    # Never publish the grant and then reject it through a redundant floor.
    large_value = b"l" * 60360
    p.call([op.putfh(fh), op.setxattr(SETXATTR4_EITHER, b"layout-arena", large_value)])
    tight = p.call([op.putfh(fh), op.getxattr(b"layout-arena"),
                    op.getxattr(b"layout-arena"), op.getattr(1 << FATTR4_SIZE), layoutget_op(second)],
                   "pnfs_layoutget_last_reply", runs=[(1, 5)])
    require(tight.resarray[1].gxr_value == large_value and
            tight.resarray[2].gxr_value == large_value,
            "tight layout arena changed preceding xattr values")
    second = tight.resarray[4].logr_stateid
    p.layout = (fh, second)
    require(p.call([op.test_stateid([second])]).resarray[0].tsr_status_codes == [NFS4_OK],
            "fitting final layout reply did not publish its returned stateid")
    return second


def test_layoutget_finish_publication(p):
    gate = os.environ.get("CHIMERA_COMPOUND_LAYOUTGET_GATE")
    if not gate:
        return
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    for action, expected in (("a", NFS4_OK), ("r", NFS4_OK), ("e", NFS4ERR_IO)):
        ready.unlink(missing_ok=True)
        release.unlink(missing_ok=True)
        name, fh, opened = p.create("retry-layoutget-pending-" + action)
        p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"mapped-before-finish")])
        initial = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
        p.layout = (fh, initial)
        future = stateid4(initial.seqid + 1, initial.other)
        operations = [op.putfh(p.directory), op.lookup(name), layoutget_op(initial), op.getfh()]
        label = "pnfs_layoutget_pending_" + action
        slot = p.session.compound_async(operations, tag=("boundary_" + label).encode())
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "LAYOUTGET did not reach pending finish")
                time.sleep(0.01)
            states = p.call([op.test_stateid([initial, future])]).resarray[0].tsr_status_codes
            require(states[0] == NFS4_OK and states[1] == NFS4ERR_BAD_STATEID,
                    "pending finish exposed a layout version or invalidated the accepted one")
            body = NFS4Packer()
            body.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
            p.call([op.putfh(fh), op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                    layoutreturn4(LAYOUTRETURN4_FILE,
                        layoutreturn_file4(0, NFS4_UINT64_MAX, initial, body.get_buffer())))],
                   expected=NFS4ERR_DELAY)
        finally:
            release.write_text(action)
        result = p.sent_result(operations, label, p.session.listen(slot), expected)
        current = initial if expected != NFS4_OK else result.resarray[2].logr_stateid
        require(current.other == initial.other and current.seqid == initial.seqid + (expected == NFS4_OK),
                "accepted/retried/failed finish published the wrong layout version")
        require(p.call([op.test_stateid([current])]).resarray[0].tsr_status_codes == [NFS4_OK],
                "finish did not retain the expected public layout state")
        p.layout = (fh, current)
        p.return_layout(fh, current)
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)


def test_pnfs_remove_batch(p, ds, backing_names):
    before = backing_names()
    victims = [p.create("pnfs-batch-" + str(i)) for i in range(3)]
    for _, fh, sid in victims:
        p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"backing"), op.close(0, sid)])
        p.mark_closed(fh)
    created = backing_names() - before
    require(len(created) == 3, "batch setup did not create three separate backing files")
    names = [item[0] for item in victims]
    alias = b"pnfs-batch-hardlink"
    p.call([op.putfh(victims[0][1]), op.savefh(), op.putfh(p.directory), op.link(alias)])
    p.files.append((alias, victims[0][1], None))
    # The first two removes are separate hard links to the same MDS object;
    # only the final link schedules its DS backing for deletion.
    result = p.call([op.putfh(p.directory), op.remove(names[0]), op.remove(alias),
                     op.remove(names[1]), op.getfh(), op.remove(b"absent-batch-victim"),
                     op.remove(names[2])], "pnfs_remove_batch_accepted_prefix", NFS4ERR_NOENT)
    p.removed_names.update((names[0], names[1], alias))
    require(len(result.resarray) == 6 and result.resarray[4].object == p.directory,
            "batch REMOVE lost its parent cursor or executed after the failing victim")
    require(len(backing_names() & created) == 1,
            "accepted-prefix cleanup missed a victim or deleted the suppressed suffix")
    require(p.read(victims[2][1], ANONYMOUS) == b"backing", "failed REMOVE executed its suffix")
    p.call([op.putfh(p.directory), op.remove(names[2]), op.getfh()], "pnfs_remove_batch_final")
    p.removed_names.add(names[2])
    require(not (backing_names() & created), "final accepted REMOVE leaked its backing")
    # The victim's retained inode must prevent a same-span CREATE from
    # reusing its inode-based DS backing name before accepted cleanup runs.
    before = backing_names()
    name, fh, sid = p.create("pnfs-remove-recreate")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"old"), op.close(0, sid)])
    p.mark_closed(fh)
    old_backing = backing_names() - before
    require(len(old_backing) == 1, "recreate fixture did not materialize its first backing")
    result = p.call([op.putfh(p.directory), op.remove(name), p.open_op(name), op.getfh(),
                     op.write(CURRENT, 0, FILE_SYNC4, b"new-backed-data"),
                     op.read(CURRENT, 0, 64), op.close(0, CURRENT)], "pnfs_remove_recreate_write")
    new_fh = result.resarray[3].object
    p.files = [entry for entry in p.files if entry[1] != fh]
    p.files.append((name, new_fh, None))
    require(result.resarray[5].data == b"new-backed-data" and p.read(new_fh, ANONYMOUS) == b"new-backed-data",
            "accepted cleanup deleted a recreated file's backing")
    require(not (old_backing & backing_names()) and len(backing_names() - before) == 1,
            "remove/recreate leaked the old backing or lost the replacement")


def test_pnfs(p):
    test_layoutget_compounds(p)
    test_layoutget_finish_publication(p)
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
    name, fh, opened = p.create("retry-layoutcommit-return")
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

    # Every named request asserts a single VFS span unless it deliberately
    # forces the shared single-operation fallback with an oversized READDIR.
    bad = stateid4(sid.seqid, b"forgedlayout")
    p.call([op.putfh(fh), commit(bad, 23), op.getattr(1 << FATTR4_SIZE)],
           "pnfs_layoutcommit_forged", NFS4ERR_BAD_STATEID)
    result = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), commit(sid, 23),
                     op.getattr(1 << FATTR4_SIZE), op.getfh()], "pnfs_layoutcommit_extend")
    changed = result.resarray[2].locr_newsize
    require(result.resarray[1].obj_attributes[FATTR4_SIZE] == 4 and
            changed.ns_sizechanged and changed.ns_size == 24 and
            result.resarray[3].obj_attributes[FATTR4_SIZE] == 24 and result.resarray[-1].object == fh,
            "LAYOUTCOMMIT lost before/after attributes, newsize or the cursor")
    result = p.call([op.putfh(fh), commit(sid, 7), op.getattr(1 << FATTR4_SIZE)],
                    "pnfs_layoutcommit_no_shrink")
    require(not result.resarray[1].locr_newsize.ns_sizechanged and
            result.resarray[2].obj_attributes[FATTR4_SIZE] == 24,
            "LAYOUTCOMMIT shrank the file or reported a nonexistent size change")
    timestamp = nfstime4(123456789, 987654321)
    mask = (1 << FATTR4_SIZE) | (1 << FATTR4_TIME_MODIFY)
    result = p.call([op.putfh(fh), commit(sid, None, timestamp), op.getattr(mask)],
                    "pnfs_layoutcommit_mtime")
    actual = result.resarray[2].obj_attributes
    require(not result.resarray[1].locr_newsize.ns_sizechanged and actual[FATTR4_SIZE] == 24 and
            actual[FATTR4_TIME_MODIFY].seconds == timestamp.seconds and
            actual[FATTR4_TIME_MODIFY].nseconds == timestamp.nseconds,
            "mtime-only LAYOUTCOMMIT lost its timestamp or reported a size change")
    result = p.call([op.putfh(fh), commit(sid, 31), commit(sid, 47), commit(sid, 7),
                     op.getattr(1 << FATTR4_SIZE)], "pnfs_layoutcommit_repeated")
    require(result.resarray[1].locr_newsize.ns_size == 32 and
            result.resarray[2].locr_newsize.ns_size == 48 and
            not result.resarray[3].locr_newsize.ns_sizechanged and
            result.resarray[-1].obj_attributes[FATTR4_SIZE] == 48,
            "coalesced LAYOUTCOMMITs did not see their execution-time sizes")
    large = lambda: op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)
    for label, high, changed in (("extend", 55, True), ("noop", 7, False)):
        result = p.call([op.putfh(fh), commit(sid, high), large()],
                        "pnfs_layoutcommit_single_" + label, NFS4ERR_NOTDIR,
                        runs=[(1, 3)])
        require(result.resarray[1].locr_newsize.ns_sizechanged == changed and
                (not changed or result.resarray[1].locr_newsize.ns_size == 56),
                "single LAYOUTCOMMIT differs from the shared sequence")
    _, other_fh, other_sid = p.create("pnfs-layoutcommit-other")
    peer = p.other_session("layoutcommit-peer")
    p.call_as(peer, [op.putfh(fh), commit(sid, 99), op.getattr(mask)],
              "pnfs_layoutcommit_wrong_client", NFS4ERR_BAD_STATEID)
    p.call([op.putfh(other_fh), commit(sid, 99), op.write(other_sid, 0, FILE_SYNC4, b"BAD")],
           "pnfs_layoutcommit_wrong_fh", NFS4ERR_BAD_STATEID)
    before = p.call([op.putfh(fh), op.getattr(mask)]).resarray[-1].obj_attributes
    for label, token, high, stamp, error in (
            ("old", stateid4(sid.seqid - 1, sid.other), 99, None, NFS4ERR_OLD_STATEID),
            ("future", stateid4(sid.seqid + 1, sid.other), 99, None, NFS4ERR_BAD_STATEID),
            ("open", opened, 99, None, NFS4ERR_BAD_STATEID),
            ("anonymous", ANONYMOUS, 99, None, NFS4ERR_BAD_STATEID),
            ("overflow", sid, NFS4_UINT64_MAX, None, NFS4ERR_INVAL),
            ("timestamp", sid, 99, nfstime4(1, 1000000000), NFS4ERR_INVAL)):
        operations = [op.putfh(fh), commit(token, high, stamp)]
        # Read-only suffix permits finish rejection of the failed checkpoint.
        result = p.call(operations + [op.getattr(mask)], "pnfs_layoutcommit_bad_" + label, error)
        require(len(result.resarray) == 2, "rejected LAYOUTCOMMIT ran its suffix")
        p.call(operations + [op.write(opened, 0, FILE_SYNC4, b"BAD")], expected=error)
    after = p.call([op.putfh(fh), op.getattr(mask)]).resarray[-1].obj_attributes
    require(after[FATTR4_SIZE] == before[FATTR4_SIZE] and
            after[FATTR4_TIME_MODIFY].seconds == before[FATTR4_TIME_MODIFY].seconds and
            after[FATTR4_TIME_MODIFY].nseconds == before[FATTR4_TIME_MODIFY].nseconds,
            f"rejected LAYOUTCOMMIT changed metadata: {before!r} -> {after!r}")
    require(p.read(fh, opened)[:4] == b"data" and p.read(other_fh, other_sid) == b"",
            "rejected LAYOUTCOMMIT executed its WRITE suffix")
    # Seqid zero uses the current version without modifying the wire argument.
    zero = stateid4(0, sid.other)
    p.call([op.putfh(fh), commit(zero, None), commit(zero, 7), op.getfh()],
           "pnfs_layoutcommit_zero_seqid")
    p.call([commit(sid, 7)], "pnfs_layoutcommit_no_fh", NFS4ERR_NOFILEHANDLE, runs=[])
    p.call([op.putrootfh(), commit(sid, 7)], "pnfs_layoutcommit_root", NFS4ERR_BAD_STATEID, runs=[(1, 2)])
    gate = os.environ.get("CHIMERA_COMPOUND_LAYOUT_GATE")
    if gate:
        ready, release = Path(gate + ".ready"), Path(gate + ".release")
        operations = [op.putfh(p.directory), op.lookup(name), commit(sid, 7), op.getattr(mask)]
        slot = p.session.compound_async(operations, tag=b"boundary_pnfs_layoutcommit_return_during_finish")
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "LAYOUTCOMMIT never reached the pending-finish gate")
                time.sleep(0.01)
            p.return_layout(fh, sid)
        finally:
            release.write_text("reject")
        result = p.sent_result(operations, "pnfs_layoutcommit_return_during_finish", p.session.listen(slot),
                               NFS4ERR_BAD_STATEID)
        require(len(result.resarray) == 3, "retry failed to revalidate the returned layout before its suffix")
    else:
        p.return_layout(fh, sid)
    result = p.call([op.test_stateid([sid])])
    require(result.resarray[0].tsr_status_codes != [NFS4_OK], "LAYOUTRETURN left its grant alive")
    p.call([op.putfh(fh), commit(sid, 99), op.getattr(mask)],
           "pnfs_layoutcommit_returned", NFS4ERR_BAD_STATEID)
    # A removed identity must not be accepted merely because this client has
    # no layout on the current FH: that would also accept a foreign stateid.
    body = NFS4Packer()
    body.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
    p.call([op.putfh(fh), op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                layoutreturn4(LAYOUTRETURN4_FILE,
                              layoutreturn_file4(0, NFS4_UINT64_MAX, sid, body.get_buffer()))), op.getfh()],
           "pnfs_layoutreturn_retired", NFS4ERR_BAD_STATEID)
    result = p.call([op.putfh(other_fh), op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                                              0, NFS4_UINT64_MAX, 1, other_sid, 65536)])
    read_sid = result.resarray[-1].logr_stateid
    p.layout = (other_fh, read_sid)
    p.call([op.putfh(other_fh), commit(read_sid, 99), op.getattr(mask)],
           "pnfs_layoutcommit_read_layout", NFS4ERR_BADLAYOUT)
    p.return_layout(other_fh, read_sid)
    print("PASS LAYOUTCOMMIT shared/single spans, identity, metadata ordering and finish retry", flush=True)


def test_pnfs_unsupported(p):
    name, fh, opened = p.create("retry-pnfs-unsupported-backend")
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
            f"unsupported backing advertised a layout or changed authoritative bytes: {res!r}")
    failed = p.call([op.putfh(p.directory), p.open_op(name, create=False),
                     op.verify({FATTR4_SIZE: 999}), op.getfh()],
                    "pnfs_unsupported_failed_prefix", expected=NFS4ERR_NOT_SAME)
    p.replace_stateid(fh, failed.resarray[1].stateid)


def test_shared_builders(p):
    _, fh, sid = p.create("shared-builder-base")
    large = lambda: op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)

    result = p.call([op.putrootfh(), op.lookup(p.args.export.encode()), op.getfh(),
                     op.getattr(1 << FATTR4_TYPE)], "shared_pseudo_export_entry", runs=[(1, 4)])
    require(result.resarray[2].object == p.directory, "pseudo-root entry lost its export identity")
    p.call([op.putrootfh(), op.lookup(p.args.export.encode()), large(), op.getfh()],
           "shared_pseudo_export_single", runs=[(1, 4)])

    def single(operation, label, expected=NFS4ERR_NOTDIR):
        runs = [(1, 3)]
        return p.call([op.putfh(fh), operation, large()], "shared_single_" + label,
                      expected, runs=runs)

    disabled_commit = op.layoutcommit(0, NFS4_UINT64_MAX, False, ANONYMOUS,
                                     newoffset4(True, 99), newtime4(False),
                                     layoutupdate4(LAYOUT4_FLEX_FILES, b""))
    p.call([op.putfh(fh), disabled_commit, op.write(sid, 0, FILE_SYNC4, b"BAD")],
           "shared_layoutcommit_disabled", NFS4ERR_NOTSUPP)
    p.call([op.putfh(fh), disabled_commit, large()], "shared_single_layoutcommit_disabled",
           NFS4ERR_NOTSUPP, runs=[(1, 3)])
    p.call([disabled_commit], "shared_layoutcommit_disabled_no_fh", NFS4ERR_NOTSUPP, runs=[])
    require(p.read(fh, sid) == b"", "disabled LAYOUTCOMMIT changed data or ran its suffix")

    # Large READDIR requests now share the same attempt. Prefix errors must
    # still prevent that suffix from running; finite-limit standalone suffixes
    # have separate coverage in nfs4_compound_reply_budget.py.
    single(op.verify({FATTR4_SIZE: 0}), "verify_match")
    single(op.nverify({FATTR4_SIZE: 1}), "nverify_different")
    single(op.verify({FATTR4_SIZE: 1}), "verify_mismatch", NFS4ERR_NOT_SAME)
    single(op.nverify({FATTR4_SIZE: 0}), "nverify_same", NFS4ERR_SAME)
    single(op.readlink(), "readlink_type", NFS4ERR_INVAL)
    linkname, target = b"shared-builder-link", b"shared-builder-target"
    result = p.call([op.putfh(p.directory), op.create(createtype4(NF4LNK, linkdata=target),
                     linkname, {}), op.getfh()])
    link = result.resarray[-1].object
    p.files.append((linkname, link, None))
    result = p.call([op.putfh(link), op.readlink(), op.putfh(p.directory), large(), op.getfh()],
                    "shared_single_readlink", runs=[(1, 5)])
    require(result.resarray[1].link == target and result.resarray[-1].object == p.directory,
            "standalone READLINK lost its target or changed the cursor")

    key, value = b"shared-key", b"shared-value"
    single(op.setxattr(SETXATTR4_CREATE, key, value), "setxattr")
    single(op.setxattr(SETXATTR4_CREATE, key, b"wrong"), "setxattr_exists", NFS4ERR_EXIST)
    result = single(op.getxattr(key), "getxattr")
    require(result.resarray[1].gxr_value == value, "standalone GETXATTR lost its value")
    result = single(op.listxattrs(0, 4096), "listxattrs")
    require(result.resarray[1].lxr_value.lxr_names == [key],
            "standalone LISTXATTRS lost its names or exposed a namespace prefix")
    single(op.listxattrs(0, 1), "listxattrs_small", NFS4ERR_TOOSMALL)
    single(op.removexattr(key), "removexattr")
    single(op.getxattr(key), "getxattr_missing", NFS4ERR_NOXATTR)
    single(op.removexattr(key), "removexattr_missing", NFS4ERR_NOXATTR)

    # Metadata followed by xattrs must switch away from a PATH-only open.
    result = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE),
                    op.setxattr(SETXATTR4_EITHER, key, value), op.getxattr(key),
                    op.listxattrs(0, 4096), op.removexattr(key), op.getfh()],
                   "shared_xattrs_after_metadata")
    require(result.resarray[3].gxr_value == value and result.resarray[-1].object == fh,
            "coalesced xattrs lost their value or current filehandle")

    # Argument errors keep their original precedence and stop a mutation.
    for label, operation, expected in (
            ("get_empty", op.getxattr(b""), NFS4ERR_INVAL),
            ("set_option", op.setxattr(3, key, value), NFS4ERR_INVAL),
            ("set_long", op.setxattr(SETXATTR4_EITHER, b"x" * 251, value), NFS4ERR_NAMETOOLONG),
            ("remove_empty", op.removexattr(b""), NFS4ERR_INVAL),
            ("verify_mask", op.verify({FATTR4_RDATTR_ERROR: 0}), NFS4ERR_INVAL)):
        pack_option = NFS4Packer.pack_setxattr_option4
        try:
            # Send the invalid discriminant to the server rather than having
            # pynfs reject it while packing the test request.
            NFS4Packer.pack_setxattr_option4 = NFS4Packer.pack_uint
            result = p.call([op.putfh(fh), operation, op.write(sid, 0, FILE_SYNC4, b"BAD")], expected=expected)
        finally:
            NFS4Packer.pack_setxattr_option4 = pack_option
        require(len(result.resarray) == 2, label + " executed its mutation suffix")
    require(p.read(fh, sid) == b"", "rejected metadata operation changed file contents")

    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    attrdir = p.call([op.putfh(fh), op.openattr(False), op.getfh()]).resarray[-1].object
    for synthetic in (root, attrdir):
        for operation in (op.readlink(), op.verify({FATTR4_SIZE: 0}), op.nverify({FATTR4_SIZE: 1}),
                          op.getxattr(key), op.setxattr(SETXATTR4_EITHER, key, value),
                          op.listxattrs(0, 4096), op.removexattr(key)):
            p.call([op.putfh(synthetic), operation], expected=NFS4ERR_STALE)
    p.call([op.putfh(fh), op.getxattr(key)], expected=NFS4ERR_NOXATTR)


def test_commit_create_link_builders(p):
    name, fh, sid = p.create("shared-ccl-base")
    payload = b"shared commit payload"
    written = p.call([op.putfh(fh), op.write(sid, 0, UNSTABLE4, payload)])
    verifier = written.resarray[1].writeverf
    large = lambda: op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)

    def run(operations, label, forced, expected=NFS4_OK, stop=None, tail=NFS4_OK):
        if forced:
            operations = operations + [large()]
            if expected == NFS4_OK:
                expected = tail
            # The large-cap READDIR shares the attempt, including when an
            # earlier runtime error suppresses its execution.
            runs = [(1, len(operations))]
        else:
            runs = [(1, len(operations))]
        return p.call(operations, "shared_ccl_" + label + ("_single" if forced else "_coalesced"),
                      expected, runs=runs)

    def changed(info, label):
        require(info.atomic and info.after > info.before, label + " lost atomic directory change info")

    # Both paths must return the WRITE verifier and preserve the file cursor.
    for forced in (False, True):
        result = run([op.putfh(fh), op.commit(1, len(payload) - 1), op.getfh()],
                     "commit", forced, tail=NFS4ERR_NOTDIR)
        require(result.resarray[1].writeverf == verifier and result.resarray[2].object == fh,
                "COMMIT lost its verifier or changed the current filehandle")

    created = []
    for forced in (False, True):
        for kind in (NF4DIR, NF4LNK, NF4FIFO, NF4SOCK, NF4CHR, NF4BLK):
            leaf = f"shared-ccl-create-{kind}-{int(forced)}".encode()
            objtype = createtype4(kind)
            if kind == NF4LNK:
                objtype.linkdata = b"shared-ccl-target"
            elif kind in (NF4CHR, NF4BLK):
                objtype.devdata = specdata4(7, 11)
            mode = 0o777 if kind == NF4LNK else 0o710
            result = run([op.putfh(p.directory), op.savefh(),
                          op.create(objtype, leaf, {FATTR4_MODE: mode}), op.getfh(),
                          op.getattr((1 << FATTR4_TYPE) | (1 << FATTR4_MODE) | (1 << FATTR4_RAWDEV)),
                          op.restorefh(), op.getfh()], "create_" + str(kind), forced)
            made = result.resarray[3].object
            p.files.append((leaf, made, None))
            created.append((kind, made))
            require(made != p.directory and result.resarray[6].object == p.directory,
                    "CREATE/SAVEFH/RESTOREFH lost a cursor")
            attrs = result.resarray[4].obj_attributes
            require(attrs[FATTR4_TYPE] == kind and attrs[FATTR4_MODE] == mode,
                    "CREATE lost type or mode")
            if kind in (NF4CHR, NF4BLK):
                require(attrs[FATTR4_RAWDEV].specdata1 == 7 and attrs[FATTR4_RAWDEV].specdata2 == 11,
                        "CREATE lost device numbers")
            require(result.resarray[2].attrset & (1 << FATTR4_MODE), "CREATE lost applied attrset")
            changed(result.resarray[2].cinfo, "CREATE")
            failed = run([op.putfh(p.directory), op.create(objtype, leaf, {}), op.remove(leaf)],
                         "create_exists_" + str(kind), forced, NFS4ERR_EXIST, stop=2)
            require(len(failed.resarray) == 2, "failed CREATE ran its REMOVE suffix")
            p.call([op.putfh(p.directory), op.lookup(leaf)])

    # COMMIT's metadata type check must precede a potentially blocking data
    # open (especially FIFOs) and stop the subsequent WRITE.
    for kind, object_fh in created[:6]:
        expected = NFS4ERR_ISDIR if kind == NF4DIR else NFS4ERR_SYMLINK if kind == NF4LNK else NFS4ERR_INVAL
        for forced in (False, True):
            result = run([op.putfh(object_fh), op.commit(0, 0), op.putfh(fh),
                          op.write(sid, 0, FILE_SYNC4, b"BAD")], "commit_type_" + str(kind),
                         forced, expected, stop=2)
            require(len(result.resarray) == 2, "invalid COMMIT ran its WRITE suffix")
    require(p.read(fh, sid) == payload, "nonregular COMMIT changed file data")

    # Decoded attribute errors must reject before creating the name. The
    # checkpoint-only form is also eligible for synthetic finish rejection.
    for forced in (False, True):
        leaf = f"shared-ccl-bad-time-{int(forced)}".encode()
        attrs = {FATTR4_TIME_MODIFY_SET: settime4(SET_TO_CLIENT_TIME4, nfstime4(1, 1000000000))}
        result = run([op.putfh(p.directory), op.create(createtype4(NF4DIR), leaf, attrs), op.getfh()],
                     "create_bad_time", forced, NFS4ERR_INVAL, stop=2)
        require(len(result.resarray) == 2, "invalid CREATE continued past its error")
        p.call([op.putfh(p.directory), op.lookup(leaf)], expected=NFS4ERR_NOENT)

    for forced in (False, True):
        leaf = f"shared-ccl-link-{int(forced)}".encode()
        result = run([op.putfh(fh), op.savefh(), op.putfh(p.directory), op.link(leaf), op.getfh()],
                     "link", forced)
        p.files.append((leaf, fh, None))
        changed(result.resarray[3].cinfo, "LINK")
        require(result.resarray[4].object == p.directory, "LINK changed the current directory cursor")
        require(p.call([op.putfh(p.directory), op.lookup(leaf), op.getfh()]).resarray[-1].object == fh,
                "LINK named a different object")
        result = run([op.putfh(fh), op.savefh(), op.putfh(p.directory), op.link(leaf), op.remove(leaf)],
                     "link_exists", forced, NFS4ERR_EXIST, stop=4)
        require(len(result.resarray) == 4, "failed LINK ran its REMOVE suffix")
        result = run([op.putfh(fh), op.savefh(), op.link(leaf), op.getfh()],
                     "link_notdir", forced, NFS4ERR_NOTDIR, stop=3)
        require(len(result.resarray) == 3, "LINK ignored its non-directory target")

    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    attrdir = p.call([op.putfh(fh), op.openattr(False), op.getfh()]).resarray[-1].object
    for synthetic in (root, attrdir):
        for operation in (op.commit(0, 0), op.create(createtype4(NF4DIR), b"forbidden", {})):
            p.call([op.putfh(synthetic), operation], expected=NFS4ERR_STALE)
        p.call([op.putfh(fh), op.savefh(), op.putfh(synthetic), op.link(b"forbidden")], expected=NFS4ERR_STALE)
        # The raw synthetic source belongs to no backend mount; LINK's mount
        # check reports XDEV after opening the destination. An inferred open
        # need not check its type, so XDEV also precedes memfs's NOTDIR check.
        p.call([op.putfh(synthetic), op.savefh(), op.putfh(p.directory), op.link(b"forbidden")], expected=NFS4ERR_XDEV)
        p.call([op.putfh(synthetic), op.savefh(), op.putfh(fh), op.link(b"forbidden")], expected=NFS4ERR_XDEV)
    p.call([op.putfh(p.directory), op.lookup(b"forbidden")], expected=NFS4ERR_NOENT)

    # A different export ID need not mean a different filesystem. Preserve
    # LINK through a sibling export, real cross-filesystem EXDEV, and ROFS.
    alias = p.call([op.putrootfh(), op.lookup(b"alias"), op.getfh()]).resarray[-1].object
    other = p.call([op.putrootfh(), op.lookup(b"other"), op.getfh()]).resarray[-1].object
    readonly = p.call([op.putrootfh(), op.lookup(b"readonly"), op.lookup(name), op.getfh()]).resarray[-1].object
    leaf = b"shared-ccl-cross-export"
    result = p.call([op.putfh(fh), op.savefh(), op.putfh(alias), op.link(leaf), op.getfh()],
                    "shared_ccl_link_alias", runs=[(1, 5)])
    require(result.resarray[-1].object == alias, "sibling-export LINK changed export identity")
    p.files.append((leaf, fh, None))
    p.call([op.putfh(fh), op.savefh(), op.putfh(other), op.link(leaf)],
           "shared_ccl_link_xdev", NFS4ERR_XDEV, runs=[(1, 4)])
    p.call([op.putfh(readonly), op.savefh(), op.putfh(p.directory), op.link(b"forbidden")], expected=NFS4ERR_ROFS)
    p.call([op.putfh(p.directory), op.lookup(b"forbidden")], expected=NFS4ERR_NOENT)



def test_remaining_builders(p):
    large = lambda: op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)
    name, fh, sid = p.create("remaining-builders")

    def single(operation, label, expected=NFS4_OK, target=None, tail=NFS4ERR_NOTDIR):
        result = p.call([op.putfh(target or fh), operation, large()], "remaining_single_" + label,
                        tail if expected == NFS4_OK else expected,
                        runs=[(1, 3)])
        require(len(result.resarray) == (3 if expected == NFS4_OK else 2),
                label + " failed to stop at its error")
        return result.resarray[1]

    written = single(op.write(sid, 0, FILE_SYNC4, b"abcdefghijklmnop"), "write")
    require(written.count == 16 and written.committed == FILE_SYNC4, "WRITE lost count or stability")
    require(single(op.read(sid, 0, 32), "read").data == b"abcdefghijklmnop", "READ lost payload")
    check_read_plus(single(op.read_plus(sid, 0, 32), "read_plus"),
                    NFS4_CONTENT_DATA, 0, b"abcdefghijklmnop", True)
    single(op.allocate(sid, 0, 8), "allocate")
    single(op.deallocate(sid, 8, 8), "deallocate")
    require(p.read(fh, sid) == b"abcdefgh" + b"\0" * 8, "DEALLOCATE did not punch the requested range")
    require(single(op.seek(sid, 0, NFS4_CONTENT_DATA), "seek").sr_offset == 0, "SEEK lost offset")
    pattern = app_data_block4(0, 4, 2, NFS4_UINT64_MAX, 0, 0, b"abcd")
    single(op.write_same(sid, FILE_SYNC4, pattern), "write_same")
    single(op.setattr(sid, {FATTR4_SIZE: 4}), "setattr")
    require(p.read(fh, sid) == b"abcd", "WRITE_SAME/SETATTR changed the wrong bytes")
    single(op.write(sid, 0, FILE_SYNC4, b""), "write_empty")
    require(single(op.read(ANONYMOUS, 0, 8), "read_anonymous").data == b"abcd", "anonymous READ differs")
    _, other, other_sid = p.create("remaining-other")
    for label, operation in (
            ("read", op.read(other_sid, 0, 8)),
            ("read_plus", op.read_plus(other_sid, 0, 8)),
            ("write", op.write(other_sid, 0, FILE_SYNC4, b"BAD")),
            ("allocate", op.allocate(other_sid, 0, 8)),
            ("deallocate", op.deallocate(other_sid, 0, 8)),
            ("seek", op.seek(other_sid, 0, NFS4_CONTENT_DATA)),
            ("write_same", op.write_same(other_sid, FILE_SYNC4, pattern)),
            ("setattr", op.setattr(other_sid, {FATTR4_SIZE: 0}))):
        single(operation, label + "_wrong_fh", NFS4ERR_BAD_STATEID)
    require(p.read(fh, sid) == b"abcd" and p.read(other, other_sid) == b"", "rejected stateid mutated data")
    attrs = {FATTR4_TIME_MODIFY_SET: settime4(SET_TO_CLIENT_TIME4, nfstime4(1, 1000000000))}
    single(op.setattr(sid, attrs), "setattr_bad_time", NFS4ERR_INVAL)
    single(op.lockt(READ_LT, 0, 4, lock_owner4(0, b"remaining-probe")), "lockt")
    single(op.lockt(READ_LT, 0, 0, lock_owner4(0, b"remaining-probe")), "lockt_bad_range", NFS4ERR_INVAL)
    single(op.lookup(name), "lookup", target=p.directory)
    single(op.lookup(b"remaining-missing"), "lookup_missing", NFS4ERR_NOENT, target=p.directory)

    child_name = b"remaining-directory"
    child = p.call([op.putfh(p.directory), op.create(createtype4(NF4DIR), child_name, {}), op.getfh()]).resarray[-1].object
    p.files.append((child_name, child, None))
    single(op.lookupp(), "lookupp", target=child, tail=NFS4_OK)
    for forced in (False, True):
        renamed = b"remaining-renamed"
        operations = [op.putfh(p.directory), op.savefh(), op.rename(name, renamed), op.lookup(renamed), op.getfh()]
        runs = [(1, 5)]
        if forced:
            operations.append(large())
            runs = [(1, 6)]
        result = p.call(operations, "remaining_rename_" + str(int(forced)),
                        NFS4ERR_NOTDIR if forced else NFS4_OK, runs=runs)
        require(result.resarray[4].object == fh, "RENAME/LOOKUP changed file identity")
        info = result.resarray[2]
        require(info.source_cinfo.atomic and info.target_cinfo.atomic and
                info.source_cinfo.after > info.source_cinfo.before and
                info.target_cinfo.after > info.target_cinfo.before, "RENAME lost change_info")
        p.call([op.putfh(p.directory), op.savefh(), op.rename(renamed, name)])

    single(op.remove(child_name), "remove", target=p.directory, tail=NFS4_OK)
    p.removed_names.add(child_name)

    # Keep the range operation and its cursor setup in the fitting prefix.
    prefix = [op.putfh(fh), op.savefh(), op.putfh(other)]
    result = p.call(prefix + [op.copy(sid, other_sid, 0, 0, 0, True, True, []), large()],
                    "remaining_single_copy", NFS4ERR_NOTDIR, runs=[(1, 5)])
    require(result.resarray[3].cr_response.wr_count == 4 and p.read(other, other_sid) == b"abcd",
            "single COPY lost fallback data or count")
    payload = b"abcd" * 1024
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, payload)])
    p.call(prefix + [op.clone(sid, other_sid, 0, 0, 4096), large()],
           "remaining_single_clone", NFS4ERR_NOTDIR, runs=[(1, 5)])
    require(p.read(other, other_sid) == payload, "single CLONE lost data")
    for label, operation in (("copy_range", op.copy(sid, other_sid, 4097, 0, 1, True, True, [])),
                             ("clone_anonymous", op.clone(ANONYMOUS, ANONYMOUS, 0, 0, 4))):
        p.call(prefix + [operation, large()], "remaining_single_" + label,
               NFS4ERR_INVAL if label == "copy_range" else NFS4ERR_BAD_STATEID,
               runs=[(1, 5)])


def test_metadata(p):
    name, fh, sid = p.create("metadata-base")
    # Requested maxcount no longer determines the span. READDIR stages the
    # actual page before its suffix, together with the surrounding metadata.
    result = p.call([op.putfh(p.directory), op.access(ACCESS4_READ), op.getattr(1 << FATTR4_TYPE),
                    op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                    op.access(ACCESS4_READ), op.getattr(1 << FATTR4_TYPE)],
                  "metadata_large_readdir", runs=[(1, 6)])
    require(result.resarray[1].access == ACCESS4_READ and
            result.resarray[2].obj_attributes[FATTR4_TYPE] == NF4DIR,
            "standalone compound ACCESS/GETATTR lost their results")
    require(result.resarray[3].reply.entries and result.resarray[3].reply.eof,
            "standalone compound READDIR lost entries or EOF")
    require(result.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4DIR,
            "READDIR changed the current directory")
    p.call([op.putfh(fh), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
            op.write(sid, 0, FILE_SYNC4, b"BAD")], "metadata_readdir_type_failure",
           NFS4ERR_NOTDIR, runs=[(1, 3)])
    require(p.read(fh, sid) == b"", "failed READDIR executed its mutation suffix")

    result = p.call([op.putfh(fh), op.openattr(False),
                    op.getattr((1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) |
                               (1 << FATTR4_FILEHANDLE) | (1 << FATTR4_ACL)), op.getfh()],
                   "metadata_attrdir_getattr")
    attrdir = result.resarray[-1].object
    attrs = result.resarray[2].obj_attributes
    require(attrs[FATTR4_TYPE] == NF4ATTRDIR and attrs[FATTR4_SIZE] == 0 and
            attrs[FATTR4_FILEHANDLE] == attrdir and FATTR4_ACL in attrs,
            f"synthetic attribute directory lost its attributes: {attrs!r}")
    empty = p.call([op.putfh(fh), op.openattr(False),
                    op.readdir(0, b"", 16, 16, 1 << FATTR4_TYPE), op.getfh()],
                   "metadata_attrdir_empty")
    require(empty.resarray[2].reply.eof and not empty.resarray[2].reply.entries and
            empty.resarray[-1].object == attrdir, "empty stream directory was not EOF")
    names = {b"metadata-one", b"metadata-two", b"metadata-three"}
    stream_handles = {}
    for stream in sorted(names):
        result = p.call([op.putfh(attrdir), p.open_op(stream), op.getfh()],
                        "metadata_stream_create_" + stream.decode())
        stream_fh, stream_sid = result.resarray[-1].object, result.resarray[1].stateid
        stream_handles[stream] = stream_fh
        p.call([op.putfh(stream_fh), op.write(stream_sid, 0, FILE_SYNC4, stream),
                op.close(0, stream_sid)])

    seen, cookie, verifier = set(), 0, b""
    for page in range(4):
        result = p.call([op.putfh(attrdir), op.readdir(cookie, verifier, 512, 512,
                       (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE)), op.getfh()],
                      f"metadata_attrdir_page_{page}")
        listing = result.resarray[1]
        require(result.resarray[-1].object == attrdir, "READDIR exposed the base inode cursor")
        for entry in listing.reply.entries:
            require(entry.name in names and entry.name not in seen, "missing/duplicate stream page")
            require(entry.cookie > cookie and entry.cookie >= 3, "invalid stream continuation cookie")
            require(entry.attrs[FATTR4_TYPE] == NF4REG and
                    entry.attrs[FATTR4_SIZE] == len(entry.name), "stream attributes were not preserved")
            seen.add(entry.name)
            cookie = entry.cookie
        verifier = listing.cookieverf
        if listing.reply.eof:
            break
    require(seen == names and listing.reply.eof, "named-attribute pagination did not complete")
    p.call([op.putfh(attrdir), op.readdir(0, b"", 16, 16, 1 << FATTR4_TYPE)],
           "metadata_attrdir_too_small", NFS4ERR_TOOSMALL)


    # Two pages and staged ACL GETATTR must retain separate arena allocations
    # when finish rejects the entire read-only compound and replays it.
    mask = ((1 << 56) - 1) & ~((1 << FATTR4_TIME_ACCESS_SET) | (1 << FATTR4_TIME_MODIFY_SET))
    result = p.call([op.putfh(fh), op.openattr(False), op.readdir(0, b"", 4096, 4096, mask),
                    op.getattr((1 << FATTR4_ACL) | (1 << FATTR4_FILEHANDLE)),
                    op.readdir(0, b"", 4096, 4096, mask),
                    p.open_op(b"metadata-one", create=False, owner=b"list-then-open"),
                    op.read(CURRENT, 0, 64), op.close(0, CURRENT)], "metadata_attrdir_pages_retry")
    for listing in [result.resarray[2], result.resarray[4]]:
        require(listing.reply.eof and len(listing.reply.entries) == len(names) and
                {e.name for e in listing.reply.entries} == names, "retry lost or duplicated stream entries")
        for entry in listing.reply.entries:
            require(entry.attrs[FATTR4_FILEHANDLE] == stream_handles[entry.name] and
                    FATTR4_ACL in entry.attrs, "stream listing lost identity or inherited ACL")
    require(result.resarray[2].cookieverf == result.resarray[4].cookieverf == verifier and
            result.resarray[3].obj_attributes[FATTR4_FILEHANDLE] == attrdir and
            result.resarray[6].data == b"metadata-one", "READDIR corrupted a following operation")

    # Protocol errors are execution gates, including when a later operation
    # would mutate the directory. The read-only variants also encounter finish
    # rejection, proving that an unsuccessful page is rebuilt safely.
    bad_verifier = bytes([verifier[0] ^ 1]) + verifier[1:]
    invalid = [(0, b"", 0, 1 << FATTR4_TYPE, NFS4ERR_TOOSMALL),
               (0, b"", 16, 1 << FATTR4_TYPE, NFS4ERR_TOOSMALL),
               (1, verifier, 4096, 1 << FATTR4_TYPE, NFS4ERR_BAD_COOKIE),
               (2, verifier, 4096, 1 << FATTR4_TYPE, NFS4ERR_BAD_COOKIE),
               (cookie + 1, verifier, 4096, 1 << FATTR4_TYPE, NFS4ERR_BAD_COOKIE),
               (cookie, bad_verifier, 4096, 1 << FATTR4_TYPE, NFS4ERR_NOT_SAME),
               (0, b"", 4096, 1 << FATTR4_TIME_ACCESS_SET, NFS4ERR_INVAL)]
    for number, (bad_cookie, cv, size, attrs, error) in enumerate(invalid):
        operations = [op.putfh(attrdir), op.readdir(bad_cookie, cv, size, size, attrs)]
        failed = p.call(operations + [op.getfh()], f"metadata_attrdir_invalid_{number}", error)
        require(len(failed.resarray) == 2, "failed READDIR did not stop the wire compound")
        p.call(operations + [p.open_op(b"list-unexpected")],
               f"metadata_attrdir_invalid_create_{number}", error)
    p.call([op.putfh(attrdir), op.lookup(b"list-unexpected")], expected=NFS4ERR_NOENT)

    # Pages are snapshots at their wire position, even if CREATE/WRITE/REMOVE
    # follow them in the same VFS compound. A changed namespace invalidates the
    # earlier positional cookie, and a failed continuation must stop REMOVE.
    transient = b"list-transient"
    result = p.call([op.putfh(fh), op.openattr(False), op.savefh(),
                    op.readdir(0, b"", 8192, 8192, mask), p.open_op(transient),
                    op.write(CURRENT, 0, FILE_SYNC4, b"listed"), op.close(0, CURRENT), op.restorefh(),
                    op.readdir(0, b"", 8192, 8192, mask), op.remove(transient),
                    op.readdir(0, b"", 8192, 8192, mask), op.getfh()], "metadata_attrdir_mutation_pages")
    before, during, after = [result.resarray[i] for i in (3, 8, 10)]
    require({e.name for e in before.reply.entries} == {e.name for e in after.reply.entries} == names and
            {e.name for e in during.reply.entries} == names | {transient} and
            next(e for e in during.reply.entries if e.name == transient).attrs[FATTR4_SIZE] == 6 and
            before.cookieverf == verifier and after.cookieverf != during.cookieverf != verifier and
            result.resarray[-1].object == attrdir, f"stream pages were not staged at their wire position: {result!r}")
    p.call([op.putfh(attrdir), op.readdir(during.reply.entries[0].cookie, during.cookieverf,
            4096, 4096, mask), op.remove(b"metadata-one")],
           "metadata_attrdir_stale_stops_remove", NFS4ERR_NOT_SAME)
    p.call([op.putfh(attrdir), op.lookup(b"metadata-one")])

    # OPENATTR checks run before any suffix can create a stream. Both a real
    # directory and a second OPENATTR on an attribute directory are rejected.
    p.call([op.putfh(p.directory), op.openattr(True), p.open_op(b"attrdir-invalid-create")],
           "metadata_openattr_directory", NFS4ERR_NOTSUPP)
    p.call([op.putfh(p.directory), op.lookup(b"attrdir-invalid-create")], expected=NFS4ERR_NOENT)
    p.call([op.putfh(fh), op.openattr(False), op.openattr(False),
            p.open_op(b"attrdir-invalid-create")], "metadata_openattr_nested", NFS4ERR_NOTSUPP)

    # Resolve the base inside execution and carry its synthetic cursor across
    # SAVEFH/RESTOREFH, another base's OPENATTR, and three independent OPENs.
    _, other_base, _ = p.create("metadata-other-base")
    created = p.call([op.putfh(other_base), op.openattr(True), p.open_op(b"other-stream"),
                      op.write(CURRENT, 0, FILE_SYNC4, b"other-data"), op.getfh()],
                     "metadata_openattr_create_write")
    p.call([op.putfh(created.resarray[-1].object), op.close(0, created.resarray[2].stateid)])
    result = p.call([op.putfh(p.directory), op.lookup(name), op.openattr(False), op.savefh(),
                    p.open_op(b"metadata-one", create=False, owner=b"multi-one"),
                    op.read(CURRENT, 0, 64), op.getfh(),
                    op.putfh(other_base), op.openattr(False),
                    p.open_op(b"other-stream", create=False, owner=b"multi-other"),
                    op.read(CURRENT, 0, 64), op.getfh(), op.restorefh(), op.getfh(),
                    p.open_op(b"metadata-two", create=False, owner=b"multi-two"),
                    op.read(CURRENT, 0, 64), op.getfh()], "metadata_multiple_stream_bases")
    require(result.resarray[5].data == b"metadata-one" and
            result.resarray[10].data == b"other-data" and result.resarray[15].data == b"metadata-two" and
            result.resarray[13].object == attrdir, "compound lost a stream base or its saved synthetic cursor")
    for opened, handle in [(4, 6), (9, 11), (14, 16)]:
        p.call([op.putfh(result.resarray[handle].object), op.close(0, result.resarray[opened].stateid)])

    # Reopening the same new state by FH changes which OPEN supplies the
    # final handle, but must retain the base guard from its earlier stream OPEN.
    by_fh = p.open_op(b"metadata-one", create=False, owner=b"stream-claim-fh")
    by_fh.opopen.claim = open_claim4(CLAIM_FH)
    result = p.call([op.putfh(fh), op.openattr(False),
                    p.open_op(b"metadata-one", create=False, owner=b"stream-claim-fh"), by_fh,
                    op.read(CURRENT, 0, 64), op.getfh()], "metadata_stream_claim_fh")
    check_open_version(result.resarray[2].stateid, result.resarray[3].stateid,
                       "stream CLAIM_FH changed state identity")
    require(result.resarray[4].data == b"metadata-one", "stream CLAIM_FH changed the data cursor")
    p.call([op.putfh(result.resarray[-1].object), op.close(0, result.resarray[3].stateid)])

    # READDIR carries the saved synthetic cursor through the same compound.
    result = p.call([op.putfh(attrdir), op.savefh(),
                    op.readdir(0, b"", 4096, 4096, 1 << FATTR4_TYPE),
                    op.putfh(other_base), op.restorefh(), op.getfh()],
                   "metadata_saved_attrdir")
    require(result.resarray[-1].object == attrdir, "inherited SAVEFH leaked the base cursor")
    # A large requested cap preserves the same shared synthetic cursor.
    result = p.call([op.putfh(attrdir), op.savefh(),
                    op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                    op.putfh(other_base), op.restorefh(), op.getfh()],
                   "metadata_inherited_attrdir", runs=[(1, 6)])
    require(result.resarray[-1].object == attrdir and result.resarray[2].reply.eof,
            "standalone named READDIR lost its result or saved cursor")

    # Stream reopens must use the same owner journal as ordinary OPEN. A
    # read-only reopen is eligible for finish EAGAIN in the retry variant.
    stream = b"metadata-one"
    owner = b"metadata-stream-union"
    result = p.call([op.putfh(attrdir), p.open_op(stream, create=False, owner=owner,
                    access=OPEN4_SHARE_ACCESS_READ), op.read(CURRENT, 0, 64), op.getfh()],
                   "metadata_stream_read")
    stream_fh, initial = result.resarray[-1].object, result.resarray[1].stateid
    require(result.resarray[2].data == stream, "stream OPEN selected the base contents")
    prefix = p.call([op.putfh(fh), op.openattr(False), op.savefh(),
                     p.open_op(stream, create=False, owner=b"stream-prefix", access=OPEN4_SHARE_ACCESS_READ),
                     op.read(CURRENT, 0, 64), op.restorefh(),
                     p.open_op(b"stream-missing", create=False), op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                    "metadata_stream_failed_suffix", NFS4ERR_NOENT)
    prefix_sid = prefix.resarray[3].stateid
    require(p.read(stream_fh, prefix_sid) == stream,
            "failed suffix lost the accepted stream OPEN or executed the WRITE")
    p.call([op.putfh(stream_fh), op.close(0, prefix_sid)])

    result = p.call([op.putfh(attrdir), p.open_op(stream, create=False, owner=owner,
                    access=OPEN4_SHARE_ACCESS_WRITE), op.read(CURRENT, 0, 64),
                    op.write(CURRENT, 0, FILE_SYNC4, b"stream-data"), op.close(0, CURRENT)],
                   "metadata_stream_union_close")
    check_open_version(initial, result.resarray[1].stateid, "stream coalescing lost its stateid")
    require(result.resarray[2].data == stream, "stream union lost inherited read access")

    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"base-data")])
    result = p.call([op.putfh(attrdir), p.open_op(stream, create=False, owner=owner,
                    deny=OPEN4_SHARE_DENY_WRITE), op.getfh()])
    held = result.resarray[1].stateid
    before = p.read(stream_fh, held)
    p.call([op.putfh(attrdir), p.open_op(stream, owner=b"stream-denied", create_mode=UNCHECKED4,
                    attrs={FATTR4_SIZE: 0}), op.getfh()],
           "metadata_stream_denied_truncate", NFS4ERR_SHARE_DENIED)
    require(p.read(stream_fh, held) == before and p.read(fh, sid) == b"base-data",
            "rejected stream truncate changed stream or base data")
    p.call([op.putfh(stream_fh), op.close(0, held)])
    p.call([op.putfh(attrdir), p.open_op(stream), op.getfh()],
           "metadata_stream_guarded", NFS4ERR_EXIST)
    result = p.call([op.putfh(attrdir), p.open_op(stream, owner=b"stream-truncate",
                    create_mode=UNCHECKED4, attrs={FATTR4_SIZE: 0}),
                    op.read(CURRENT, 0, 64), op.close(0, CURRENT)],
                   "metadata_stream_truncate")
    require(result.resarray[2].data == b"" and p.read(fh, sid) == b"base-data",
            "accepted stream truncate failed or modified the base file")

    result = p.call([op.putfh(fh), op.openattr(False), op.savefh(), op.lookup(b"metadata-three"),
                    op.getattr((1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEHANDLE)),
                    op.getfh(), op.restorefh(), op.remove(b"metadata-three"), op.getfh()],
                   "metadata_stream_lookup_remove")
    attrs = result.resarray[4].obj_attributes
    change = result.resarray[7].cinfo
    require(attrs[FATTR4_TYPE] == NF4REG and attrs[FATTR4_SIZE] == len(b"metadata-three") and
            attrs[FATTR4_FILEHANDLE] == result.resarray[5].object and
            result.resarray[-1].object == attrdir and
            not change.atomic and change.before == change.after == 0,
            "stream LOOKUP/REMOVE lost metadata, change_info or the synthetic cursor")
    p.call([op.putfh(attrdir), op.lookup(b"metadata-three"), p.open_op(b"unexpected-stream")],
           "metadata_stream_missing_stops_create", NFS4ERR_NOENT)
    p.call([op.putfh(attrdir), op.lookup(b"unexpected-stream")], expected=NFS4ERR_NOENT)
    p.call([op.putfh(attrdir), op.lookup(b"metadata-one"),
            op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)],
           "metadata_stream_lookup_standalone", NFS4ERR_NOTDIR, runs=[(1, 3)])
    require(p.read(fh, sid) == b"base-data", "stream REMOVE changed the base data")



def test_large_stream_directory(p):
    _, base, _ = p.create("metadata-large-stream-base")
    attrdir = p.call([op.putfh(base), op.openattr(True), op.getfh()]).resarray[-1].object
    # Over 80 KiB of packed backend records, independently of NFS wire/arena
    # overhead. Names are long enough to cross the old cap with a small setup.
    names = {f"large-{i:04d}-".encode() + b"x" * 230 for i in range(300)}
    require(sum(32 + len(name) for name in names) > 64 * 1024, "fixture did not exceed the old limit")
    for name in sorted(names):
        p.call([op.putfh(attrdir), p.open_op(name, owner=b"large-stream-owner"), op.close(0, CURRENT)])
    mask = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEHANDLE)
    seen, cookie, verifier = set(), 0, b""
    for page in range(len(names) + 1):
        result = p.call([op.putfh(base), op.openattr(False),
                        op.readdir(cookie, verifier, 8192, 8192, mask), op.getfh()],
                       f"metadata_large_stream_page_{page}")
        listing = result.resarray[2]
        require(result.resarray[-1].object == attrdir, "large list lost its synthetic cursor")
        if cookie:
            require(listing.cookieverf == verifier, "unchanged stream namespace changed verifier")
        for entry in listing.reply.entries:
            require(entry.name in names and entry.name not in seen and entry.cookie > cookie,
                    "large stream listing skipped, duplicated or reordered a cookie")
            require(entry.attrs[FATTR4_FILEHANDLE] != base and entry.attrs[FATTR4_SIZE] == 0,
                    "large stream listing returned base attributes")
            seen.add(entry.name)
            cookie = entry.cookie
        verifier = listing.cookieverf
        if listing.reply.eof:
            break
        require(listing.reply.entries, "large listing made no progress")
    require(seen == names and listing.reply.eof and page > 1, "large stream pagination did not complete")
    result = p.call([op.putfh(attrdir), op.readdir(cookie, verifier, 16, 16, mask), op.getfh()],
                   "metadata_large_stream_eof")
    require(result.resarray[1].reply.eof and not result.resarray[1].reply.entries,
            "continuation after the final stream was not empty EOF")

    victim = min(names)
    p.call([op.putfh(attrdir), op.remove(victim)])
    survivor = max(names)
    p.call([op.putfh(attrdir), op.readdir(cookie, verifier, 8192, 8192, mask), op.remove(survivor)],
           "metadata_large_stream_stale", NFS4ERR_NOT_SAME)
    p.call([op.putfh(attrdir), op.lookup(survivor)])
    fresh = p.call([op.putfh(attrdir), op.readdir(0, verifier, 8192, 8192, mask)]).resarray[1]
    require(fresh.cookieverf != verifier and not fresh.reply.eof, "namespace mutation kept stale verifier")
    # Data writes do not invalidate positional cookies in an unchanged namespace.
    p.call([op.putfh(attrdir), p.open_op(survivor, create=False),
            op.write(CURRENT, 0, FILE_SYNC4, b"changed"), op.close(0, CURRENT)])
    p.call([op.putfh(attrdir), op.readdir(fresh.reply.entries[-1].cookie, fresh.cookieverf,
            8192, 8192, mask), op.getfh()], "metadata_large_stream_write_continuation")


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
        elif probe.feature == "pnfs_resident":
            test_pnfs_resident_remove(probe)
        elif probe.feature == "pnfs_unsupported":
            test_pnfs_unsupported(probe)
        elif probe.feature == "metadata":
            test_shared_builders(probe)
            test_commit_create_link_builders(probe)
            test_remaining_builders(probe)
            test_metadata(probe)
            test_large_stream_directory(probe)
        else:
            test_pnfs(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} real {probe.feature} compound checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
