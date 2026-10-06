#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Ordered protocol-state retirement with exact compound spans and finish gates.

The common wrapper selects none, delegation or pnfs. The fault-injection
variant rejects only compounds whose executed filesystem operations are
read-only; lock/delegation/layout publication must remain private until finish.
"""

from nfs4_compound_features import *  # noqa: F401,F403
from nfs4_compound_v40_boundaries import Probe40


def lock_op(sid, owner=b"retirement-lock"):
    return op.lock(WRITE_LT, False, 0, 8,
                   locker4(True, open_to_lock_owner4(0, sid, 0, lock_owner4(0, owner))))


def return_layout_op(sid):
    body = NFS4Packer()
    body.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
    return op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0, NFS4_UINT64_MAX, sid, body.get_buffer())))


def state_status(p, sid):
    return p.call([op.test_stateid([sid])]).resarray[0].tsr_status_codes[0]


def forget_delegation(p, sid):
    p.delegations = [(fh, old) for fh, old in p.delegations if old.other != sid.other]


def test_free(p):
    _, fh, opened = p.create("retirement-free-lock")
    locked = p.call([op.putfh(fh), lock_op(opened)]).resarray[1].lock_stateid
    result = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), op.free_stateid(locked),
                     op.write(opened, 0, FILE_SYNC4, b"BAD")],
                    "retirement_free_held", NFS4ERR_LOCKS_HELD)
    require(len(result.resarray) == 3 and p.read(fh, opened) == b"", "FREE with locks executed its suffix")
    other = p.other_session("free-foreign")
    p.call_as(other, [op.free_stateid(locked), op.putfh(fh), op.getfh()],
              "retirement_free_foreign_held", NFS4ERR_BAD_STATEID)
    predicted = stateid4(locked.seqid + 1, locked.other)
    result = p.call([op.putfh(fh), op.locku(WRITE_LT, 0, locked, 0, 8),
                     op.free_stateid(CURRENT), op.test_stateid([predicted, opened]),
                     op.lockt(WRITE_LT, 0, 8, lock_owner4(0, b"retirement-outsider")), op.getfh()],
                    "retirement_unlock_free_current")
    require(result.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID, NFS4_OK] and
            result.resarray[-1].object == fh, "FREE did not retire only the private lock identity")
    require(state_status(p, predicted) == NFS4ERR_BAD_STATEID, "accepted FREE kept its public slot")
    p.call([op.free_stateid(ANONYMOUS), op.putfh(fh), op.getfh()],
           "retirement_free_special", NFS4ERR_BAD_STATEID)
    # An empty lock still belongs only to its authenticated session client.
    locked = p.call([op.putfh(fh), lock_op(opened, b"retirement-empty-lock"),
                     op.locku(WRITE_LT, 0, CURRENT, 0, 8)]).resarray[2].lock_stateid
    p.call_as(other, [op.free_stateid(locked), op.putfh(fh), op.getfh()],
              "retirement_free_foreign_empty", NFS4ERR_BAD_STATEID)
    require(state_status(p, locked) == NFS4_OK, "foreign FREE destroyed an empty lock")
    # The session identity is valid, but owner admission must preserve its
    # principal error when no private OPEN/LOCK journal could be installed.
    previous = p.session.cred
    p.session.cred = AuthSys().init_cred(uid=0, gid=0, name=b"retirement-wrong-principal")
    try:
        for label, stateid in (("empty_lock", locked), ("open", opened)):
            result = p.call([op.putfh(fh), op.getattr(1 << FATTR4_SIZE), op.free_stateid(stateid),
                             op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")],
                            "retirement_free_principal_" + label, NFS4ERR_ACCESS)
            require(len(result.resarray) == 3 and result.resarray[1].obj_attributes[FATTR4_SIZE] == 0,
                    "FREE principal error lost the read-only prefix or ran its suffix")
    finally:
        p.session.cred = previous
    require(state_status(p, locked) == NFS4_OK and state_status(p, opened) == NFS4_OK and
            p.read(fh, opened) == b"", "FREE principal error changed public state or file data")
    result = p.call([op.free_stateid(locked), op.putfh(fh), op.read(locked, 0, 1),
                     op.write(opened, 0, FILE_SYNC4, b"BAD")],
                    "retirement_free_suppresses_io", NFS4ERR_BAD_STATEID)
    require(len(result.resarray) == 3 and p.read(fh, opened) == b"", "retired lock authorized suffix I/O")
    result = p.call([op.free_stateid(opened), op.putfh(fh), op.getfh()],
                    "retirement_free_open", NFS4ERR_LOCKS_HELD)
    require(len(result.resarray) == 1 and state_status(p, opened) == NFS4_OK,
            "FREE retired an OPEN state")


def test_delegreturn(p):
    warm_backchannel(p)
    name, fh, opened = p.create("retirement-delegation")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"delegation")])
    delegated = delegation_open(p, name, fh)
    require(delegated is not None, "retirement test needs a real write delegation")
    _, wrong_fh, _ = p.create("retirement-delegation-wrong")
    p.call([op.putfh(wrong_fh), op.delegreturn(delegated), op.getfh()],
           "retirement_deleg_wrong_fh", NFS4ERR_BAD_STATEID)
    p.call([op.putfh(fh), op.delegreturn(stateid4(delegated.seqid + 1, delegated.other)), op.getfh()],
           "retirement_deleg_future", NFS4ERR_BAD_STATEID)
    other = p.other_session("delegation-foreign")
    p.call_as(other, [op.putfh(fh), op.delegreturn(delegated), op.getfh()],
              "retirement_deleg_foreign", NFS4ERR_BAD_STATEID)
    p.call_as(other, [op.free_stateid(delegated), op.putfh(fh), op.getfh()],
              "retirement_free_foreign_delegation", NFS4ERR_BAD_STATEID)
    p.call([op.free_stateid(delegated), op.putfh(fh), op.getfh()],
           "retirement_free_live_delegation", NFS4ERR_LOCKS_HELD)
    require(state_status(p, delegated) == NFS4_OK, "rejected delegation return changed public state")
    recalls = []
    p.session.client.cb_pre_hook(OP_CB_RECALL, lambda arg, env: recalls.append(arg.stateid))
    result = p.call([op.putfh(fh), op.savefh(), op.delegreturn(delegated),
                     op.test_stateid([delegated, opened]), op.setattr(opened, {FATTR4_SIZE: 4}),
                     op.setattr(ANONYMOUS, {FATTR4_MODE: 0o640}), op.restorefh(), op.read(opened, 0, 16)],
                    "retirement_deleg_setattr_view")
    require(result.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID, NFS4_OK] and
            result.resarray[-1].data == b"dele" and not recalls,
            "private DELEGRETURN failed claim exclusions or cursor preservation")
    forget_delegation(p, delegated)
    delegated = delegation_open(p, name, fh)
    require(delegated is not None, "second write delegation was not granted")
    result = p.call([op.putfh(fh), op.delegreturn(delegated), op.read(delegated, 0, 1),
                     op.write(opened, 0, FILE_SYNC4, b"BAD")],
                    "retirement_deleg_suppresses_io", NFS4ERR_BAD_STATEID)
    forget_delegation(p, delegated)
    require(len(result.resarray) == 3 and p.read(fh, opened) == b"dele",
            "returned delegation authorized later I/O")



def test_delegreturn_namespace(p):
    recalls = []
    recalled = threading.Event()

    def recall(arg, env):
        recalls.append(arg.stateid)
        recalled.set()

    p.session.client.cb_pre_hook(OP_CB_RECALL, recall)
    for kind in ("remove", "rename_source", "rename_target", "link"):
        name, fh, opened = p.create("retirement-ns-" + kind)
        p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"source")])
        if kind.startswith("rename"):
            target, target_fh, target_open = p.create("retirement-ns-target-" + kind)
            p.call([op.putfh(target_fh), op.write(target_open, 0, FILE_SYNC4, b"victim")])
        delegated_name, delegated_fh = (target, target_fh) if kind == "rename_target" else (name, fh)
        delegated = delegation_open(p, delegated_name, delegated_fh)
        require(delegated is not None, "namespace retirement fixture did not receive delegation")
        recalls.clear()
        operations = [op.putfh(delegated_fh), op.delegreturn(delegated)]
        if kind == "remove":
            operations += [op.putfh(p.directory), op.remove(name), op.lookup(name), op.getfh()]
            result = p.call(operations, "retirement_namespace_remove", NFS4ERR_NOENT)
            p.removed_names.add(name)
            require(len(result.resarray) == 5, "REMOVE failed to delete or ran absent-name suffix")
        elif kind.startswith("rename"):
            operations += [op.putfh(p.directory), op.savefh(), op.rename(name, target),
                           op.lookup(target), op.getfh()]
            result = p.call(operations, "retirement_namespace_" + kind)
            p.removed_names.add(name)
            require(result.resarray[-1].object == fh and p.read(target_fh, target_open) == b"victim",
                    "private return/RENAME changed source identity or displaced open data")
        else:
            alias = name + b"-alias"
            operations += [op.savefh(), op.putfh(p.directory), op.link(alias), op.lookup(alias),
                           op.getfh(), op.restorefh(), op.read(opened, 0, 16)]
            result = p.call(operations, "retirement_namespace_link")
            p.files.append((alias, fh, None))
            require(result.resarray[6].object == fh and result.resarray[-1].data == b"source",
                    "private return/LINK lost saved source identity")
        forget_delegation(p, delegated)
        require(not recalls and state_status(p, delegated) == NFS4ERR_BAD_STATEID,
                "namespace mutation recalled a privately returned delegation or failed to retire it")
        require(p.read(fh, opened) == b"source", "namespace mutation lost existing open data")

    # A returned source does not exempt a different, still-live destination.
    source, source_fh, _ = p.create("retirement-ns-peer-source")
    target, target_fh, _ = p.create("retirement-ns-peer-target")
    source_deleg = delegation_open(p, source, source_fh)
    target_deleg = delegation_open(p, target, target_fh)
    require(source_deleg is not None and target_deleg is not None, "peer recall needs two actual delegations")
    recalls.clear()
    recalled.clear()
    result = p.call([op.putfh(source_fh), op.delegreturn(source_deleg), op.putfh(p.directory),
                     op.savefh(), op.rename(source, target), op.getfh()],
                    "retirement_namespace_preserves_peer", NFS4ERR_DELAY)
    forget_delegation(p, source_deleg)
    require(len(result.resarray) == 5 and recalled.wait(3) and
            all(sid.other != source_deleg.other for sid in recalls) and
            any(sid.other == target_deleg.other for sid in recalls),
            "private namespace view failed to recall only the remaining destination")
    present = p.call([op.putfh(p.directory), op.lookup(source), op.getfh(),
                      op.putfh(p.directory), op.lookup(target), op.getfh()])
    require(present.resarray[2].object == source_fh and present.resarray[-1].object == target_fh,
            "blocked namespace mutation changed either name")
    p.return_delegation(target_fh, target_deleg)
    result = p.call([op.putfh(p.directory), op.savefh(), op.rename(source, target), op.lookup(target), op.getfh()],
                    "retirement_namespace_peer_drained")
    p.removed_names.add(source)
    require(result.resarray[-1].object == source_fh, "namespace retry lost source identity")


def test_layoutreturn(p):
    name, fh, opened = p.create("retirement-layout")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"layout")])
    granted = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
    p.layout = (fh, granted)
    other = p.other_session("layout-foreign")
    p.call_as(other, [op.putfh(fh), return_layout_op(granted), op.getfh()],
              "retirement_layout_foreign", NFS4ERR_BAD_STATEID)
    p.call([op.putfh(fh), return_layout_op(stateid4(granted.seqid + 1, granted.other)), op.getfh()],
           "retirement_layout_future", NFS4ERR_BAD_STATEID)
    result = p.call([op.putfh(fh), op.savefh(), return_layout_op(granted),
                     op.test_stateid([granted, opened]), op.restorefh(), op.getfh()],
                    "retirement_layout_file")
    p.layout = None
    require(result.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID, NFS4_OK] and
            result.resarray[-1].object == fh, "FILE return lost its private tombstone or saved FH")
    result = p.call([op.putfh(fh), layoutget_op(opened), return_layout_op(CURRENT),
                     op.free_stateid(CURRENT), op.getfh()],
                    "retirement_layout_grant_return_current", NFS4ERR_BAD_STATEID)
    require(len(result.resarray) == 4 and state_status(p, result.resarray[1].logr_stateid) == NFS4ERR_BAD_STATEID,
            "new grant+return leaked a layout or accepted retired current stateid")
    # Same-file truncate cannot wait for a layout that its own unreturned reply
    # grants. DELAY is a protocol result in the same finish, with no split/wait.
    result = p.call([op.putfh(fh), layoutget_op(opened), op.setattr(opened, {FATTR4_SIZE: 0}),
                     op.getfh()], "retirement_layout_self_recall_delay", NFS4ERR_DELAY)
    granted = result.resarray[1].logr_stateid
    p.layout = (fh, granted)
    require(len(result.resarray) == 3 and p.read(fh, opened) == b"layout",
            "self-recall checkpoint waited or truncated before returning its grant")
    _, second_fh, second_open = p.create("retirement-layout-second")
    second = p.call([op.putfh(second_fh), layoutget_op(second_open)]).resarray[1].logr_stateid
    result = p.call([op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                                     layoutreturn4(LAYOUTRETURN4_ALL)),
                     op.test_stateid([granted, second]), op.putfh(fh), op.getfh()],
                    "retirement_layout_all_without_fh")
    p.layout = None
    require(result.resarray[1].tsr_status_codes == [NFS4ERR_BAD_STATEID] * 2,
            "ALL return failed to retire its frozen public layout set")
    result = p.call([op.putfh(fh), layoutget_op(opened), return_layout_op(CURRENT),
                     op.getattr(1 << FATTR4_SIZE)], "retirement_layout_cancel_new_grant")
    require(state_status(p, result.resarray[1].logr_stateid) == NFS4ERR_BAD_STATEID,
            "accepted grant+return published a canceled layout")


def test_release_owner(p):
    _, fh, opened = p.create("retirement-v40-owner")
    owner = p.file_owners[fh]
    wire_owner = lock_owner4(p.clientid, b"retirement-owner")

    def establish(seqid):
        operation = op.lock(WRITE_LT, False, 0, 8, locker4(True,
            open_to_lock_owner4(p.owner_seqids[owner], opened, seqid, wire_owner)))
        p.owner_seqids[owner] += 1
        return operation

    locked = p.call([op.putfh(fh), establish(0)]).resarray[1].lock_stateid
    result = p.call([op.putfh(fh), op.release_lockowner(wire_owner), op.getfh()],
                    "retirement_owner_held", NFS4ERR_LOCKS_HELD)
    require(len(result.resarray) == 2, "RELEASE_LOCKOWNER accepted live locks")
    predicted = stateid4(locked.seqid + 1, locked.other)
    result = p.call([op.putfh(fh), op.locku(WRITE_LT, 1, locked, 0, 8),
                     op.release_lockowner(wire_owner), op.read(predicted, 0, 1), op.getfh()],
                    "retirement_owner_unlock_release", NFS4ERR_BAD_STATEID)
    require(len(result.resarray) == 4, "released owner state still authorized I/O")
    # The owner string can be recreated in the same span with a fresh sequence.
    locked = p.call([op.putfh(fh), establish(73)]).resarray[1].lock_stateid
    result = p.call([op.putfh(fh), op.locku(WRITE_LT, 74, locked, 0, 8),
                     op.release_lockowner(wire_owner), establish(11), op.getfh()],
                    "retirement_owner_reuse_same_compound")
    replacement = result.resarray[3].lock_stateid
    require(replacement.other != locked.other, "owner reuse resurrected the retired state slot")
    p.call([op.putfh(fh), op.locku(WRITE_LT, 12, replacement, 0, 8), op.release_lockowner(wire_owner)])
    result = p.call([op.release_lockowner(wire_owner), op.putfh(fh), op.getfh()],
                    "retirement_owner_absent_without_fh")
    require(result.resarray[-1].object == fh, "absent owner release consumed filehandle state")


def test_finish_publication(p):
    gate = os.environ.get("CHIMERA_COMPOUND_RETIREMENT_GATE")
    if not gate:
        return
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    for decision in ("a", "r", "e"):
        ready.unlink(missing_ok=True)
        release.unlink(missing_ok=True)
        name, fh, opened = p.create(f"retirement-gate-target-{p.feature}-{decision}")
        marker, _, _ = p.create(f"retry-retirement-terminal-{p.feature}-{decision}")
        if p.feature == "delegation":
            sid = delegation_open(p, name, fh)
            require(sid is not None, "finish gate requires a granted delegation")
            retirement = op.delegreturn(sid)
        elif p.feature == "pnfs":
            sid = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
            p.layout = (fh, sid)
            retirement = return_layout_op(sid)
        else:
            sid = p.call([op.putfh(fh), lock_op(opened, b"gate-" + name),
                          op.locku(WRITE_LT, 0, CURRENT, 0, 8)]).resarray[2].lock_stateid
            retirement = op.free_stateid(sid)
        operations = [op.putfh(p.directory), p.open_op(marker, create=False, owner=b"marker-" + marker),
                      op.savefh(), op.putfh(fh), retirement, op.test_stateid([sid]),
                      op.restorefh(), op.close(0, CURRENT)]
        label = f"retirement_finish_{p.feature}_{decision}"
        slot = p.session.compound_async(operations, tag=f"boundary_{label}".encode())
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "retirement did not reach pending finish")
                time.sleep(0.01)
            require(state_status(p, sid) == NFS4_OK,
                    "pending retirement became visible before accepted finish")
        finally:
            release.write_text(decision)
        result = p.sent_result(operations, label, p.session.listen(slot),
                               NFS4ERR_IO if decision == "e" else NFS4_OK)
        require(state_status(p, sid) == (NFS4_OK if decision == "e" else NFS4ERR_BAD_STATEID),
                "retirement publication disagreed with final finish outcome")
        if decision == "e":
            p.call([op.putfh(fh), retirement])
        else:
            require(result.resarray[5].tsr_status_codes == [NFS4ERR_BAD_STATEID],
                    "retry did not rebuild private retirement tombstone")
        if p.feature == "delegation":
            forget_delegation(p, sid)
        elif p.feature == "pnfs":
            p.layout = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(0, 1, 2), default=2)
    parser.add_argument("--server-log", required=True)
    args = parser.parse_args()
    probe = Probe40(args) if args.minor == 0 else FeatureProbe(args)
    try:
        if args.minor == 0:
            test_release_owner(probe)
        elif probe.feature == "delegation":
            test_delegreturn(probe)
            test_delegreturn_namespace(probe)
        elif probe.feature == "pnfs":
            test_layoutreturn(probe)
        else:
            test_free(probe)
        if args.minor:
            test_finish_publication(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} retirement compound checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
