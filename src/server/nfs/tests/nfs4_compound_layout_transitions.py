#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Private layout replacement and returned-layout truncate in one VFS span.

Finish rejection only covers already materialized, read-only layout queries.
Mutating truncate cases run normally and do not claim backend rollback.
"""

from nfs4_compound_retirement import *  # noqa: F401,F403


def test_replacements(p):
    name, fh, opened = p.create("retry-layout-transition")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"layout-data")])
    original = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
    p.layout = (fh, original)
    result = p.call([op.putfh(p.directory), op.lookup(name), return_layout_op(original), layoutget_op(opened),
                     op.test_stateid([original]), op.getfh()], "layout_return_regrant")
    current = result.resarray[3].logr_stateid
    p.layout = (fh, current)
    require(current.other != original.other and result.resarray[4].tsr_status_codes == [NFS4ERR_BAD_STATEID],
            "return/regrant reused the retired identity or lost its private tombstone")
    require(state_status(p, original) == NFS4ERR_BAD_STATEID and state_status(p, current) == NFS4_OK,
            "accepted replacement published the wrong slot")
    result = p.call([op.putfh(p.directory), op.lookup(name), return_layout_op(current), layoutget_op(current),
                     op.write(opened, 0, FILE_SYNC4, b"BAD")],
                    "layout_return_regrant_stale", NFS4ERR_BAD_STATEID)
    p.layout = None
    require(len(result.resarray) == 4 and p.read(fh, opened) == b"layout-data",
            "retired layout state authorized regrant or executed its suffix")
    result = p.call([op.putfh(fh), layoutget_op(opened), return_layout_op(CURRENT),
                     layoutget_op(opened), return_layout_op(CURRENT), layoutget_op(opened),
                     op.getattr(1 << FATTR4_SIZE)], "layout_cancel_regrant_twice")
    first, second, current = [result.resarray[i].logr_stateid for i in (1, 3, 5)]
    p.layout = (fh, current)
    require(len({first.other, second.other, current.other}) == 3,
            "same-span canceled grants reused a private slot identity")
    require([state_status(p, sid) for sid in (first, second, current)] ==
            [NFS4ERR_BAD_STATEID, NFS4ERR_BAD_STATEID, NFS4_OK], "canceled replacement became public")
    result = p.call([op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                                     layoutreturn4(LAYOUTRETURN4_ALL)),
                     op.putfh(fh), layoutget_op(opened), op.test_stateid([current])],
                    "layout_all_regrant")
    next_sid = result.resarray[2].logr_stateid
    p.layout = (fh, next_sid)
    require(next_sid.other != current.other and result.resarray[3].tsr_status_codes == [NFS4ERR_BAD_STATEID],
            "ALL return reused or retained its old identity")
    p.return_layout(fh, next_sid)


def test_returned_truncate(p):
    _, fh, opened = p.create("layout-return-truncate")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"layout-data")])
    original = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
    p.layout = (fh, original)
    result = p.call([op.putfh(fh), return_layout_op(original), op.setattr(opened, {FATTR4_SIZE: 6}),
                     op.read(opened, 0, 32), op.test_stateid([original])], "layout_return_truncate")
    p.layout = None
    require(result.resarray[3].data == b"layout" and
            result.resarray[4].tsr_status_codes == [NFS4ERR_BAD_STATEID],
            "returned layout blocked truncate or remained usable")
    result = p.call([op.putfh(fh), layoutget_op(opened), return_layout_op(CURRENT),
                     op.setattr(opened, {FATTR4_SIZE: 3}), op.read(opened, 0, 32)],
                    "layout_cancel_grant_truncate")
    require(result.resarray[-1].data == b"lay" and
            state_status(p, result.resarray[1].logr_stateid) == NFS4ERR_BAD_STATEID,
            "canceled fresh grant blocked truncate or escaped publication")
    # A still-live private grant cannot be recalled from a client that has not
    # received it yet. Preserve the explicit successful-prefix DELAY.
    result = p.call([op.putfh(fh), layoutget_op(opened), op.setattr(opened, {FATTR4_SIZE: 0}),
                     op.getfh()], "layout_active_grant_truncate_delay", NFS4ERR_DELAY)
    p.layout = (fh, result.resarray[1].logr_stateid)
    require(len(result.resarray) == 3 and p.read(fh, opened) == b"lay",
            "active private layout bypassed truncate recall")
    p.return_layout(*p.layout)



def test_truncate_grant(p):
    _, fh, opened = p.create("layout-truncate-grant")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"layout-data")])
    result = p.call([op.putfh(fh), op.setattr(opened, {FATTR4_SIZE: 6}),
                     layoutget_op(opened), op.read(opened, 0, 32)], "layout_truncate_grant")
    original = result.resarray[2].logr_stateid
    p.layout = (fh, original)
    require(result.resarray[3].data == b"layout" and state_status(p, original) == NFS4_OK,
            "own truncate barrier rejected or lost the following layout grant")
    result = p.call([op.putfh(fh), return_layout_op(original),
                     op.setattr(opened, {FATTR4_SIZE: 5}), op.setattr(opened, {FATTR4_SIZE: 3}),
                     layoutget_op(opened), op.test_stateid([original]), op.read(opened, 0, 32)],
                    "layout_return_two_truncates_grant")
    current = result.resarray[4].logr_stateid
    p.layout = (fh, current)
    require(current.other != original.other and result.resarray[5].tsr_status_codes == [NFS4ERR_BAD_STATEID] and
            result.resarray[6].data == b"lay" and state_status(p, current) == NFS4_OK,
            "multiple own barriers rejected replacement or kept the retired identity")
    p.return_layout(*p.layout)
    result = p.call([op.putfh(fh), layoutget_op(opened), return_layout_op(CURRENT),
                     op.setattr(opened, {FATTR4_SIZE: 2}), layoutget_op(opened), op.read(opened, 0, 32)],
                    "layout_cancel_truncate_regrant")
    cancelled, current = result.resarray[1].logr_stateid, result.resarray[4].logr_stateid
    p.layout = (fh, current)
    require(current.other != cancelled.other and state_status(p, cancelled) == NFS4ERR_BAD_STATEID and
            state_status(p, current) == NFS4_OK and result.resarray[5].data == b"la",
            "canceled grant and own barrier prevented the final replacement")
    p.return_layout(*p.layout)


def test_open_truncate_layouts(p):
    name, fh, opened = p.create("layout-open-truncate")
    data = b"keep-until-layout-return"
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, data)])
    original = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
    p.layout = (fh, original)
    recalled = threading.Event()
    recalls = []
    previous = getattr(p.client, "op_cb_layoutrecall", None)

    def op_cb_layoutrecall(arg, env):
        recalls.append(arg.opcblayoutrecall)
        env.notify = recalled.set
        return nfs4client.encode_status(NFS4_OK)

    def truncate(owner):
        return p.open_op(name, attrs={FATTR4_SIZE: 0}, create_mode=UNCHECKED4, owner=owner)

    p.client.op_cb_layoutrecall = op_cb_layoutrecall
    try:
        other = p.other_session("open-layout-truncate")
        operations = [op.putfh(p.directory), truncate(b"peer-layout-truncate"),
                      op.read(CURRENT, 0, 64), op.close(0, CURRENT)]
        label = "layout_public_open_truncate"
        slot = other.compound_async(operations, tag=("boundary_" + label).encode())
        done = threading.Event()
        reply = {}

        def listen():
            try:
                reply["result"] = other.listen(slot)
            except BaseException as error:
                reply["error"] = error
            finally:
                done.set()

        listener = threading.Thread(target=listen, daemon=True)
        listener.start()
        try:
            require(recalled.wait(3), "truncating OPEN did not recall the held public layout")
            require(not done.wait(0.05) and p.read(fh, opened) == data,
                    "OPEN truncated or finished before the holder returned its layout")
            require(recalls[-1].clora_recall.lor_layout.lor_fh == fh,
                    "OPEN recalled a different object's layout")
        finally:
            p.return_layout(fh, original)
        require(done.wait(5), "truncating OPEN did not resume after LAYOUTRETURN")
        if "error" in reply:
            raise reply["error"]
        result = p.sent_result(operations, label, reply["result"])
        require(result.resarray[2].data == b"", "admitted OPEN failed to truncate its resolved object")

        p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, data)])
        original = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
        p.layout = (fh, original)
        recalls.clear()
        result = p.call([op.putfh(fh), return_layout_op(original), op.putfh(p.directory),
                         truncate(b"private-return-truncate"), op.savefh(), layoutget_op(CURRENT),
                         op.restorefh(), op.read(CURRENT, 0, 64), op.close(0, CURRENT)],
                        "layout_private_return_open_truncate_regrant")
        replacement = result.resarray[5].logr_stateid
        p.layout = (fh, replacement)
        require(result.resarray[7].data == b"" and not recalls and replacement.other != original.other and
                state_status(p, original) == NFS4ERR_BAD_STATEID,
                "private return blocked OPEN truncate, recalled itself, or reused the old layout")
        p.return_layout(*p.layout)

        p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, data)])
        recalls.clear()
        result = p.call([op.putfh(fh), layoutget_op(opened), op.putfh(p.directory),
                         truncate(b"active-layout-truncate"), op.write(CURRENT, 0, FILE_SYNC4, b"BAD")],
                        "layout_active_grant_open_truncate_delay", NFS4ERR_DELAY)
        p.layout = (fh, result.resarray[1].logr_stateid)
        require(len(result.resarray) == 4 and not recalls and p.read(fh, opened) == data,
                "private grant let OPEN truncate or recalled an unseen layout")
        p.return_layout(*p.layout)
    finally:
        if previous is None:
            del p.client.op_cb_layoutrecall
        else:
            p.client.op_cb_layoutrecall = previous


def test_replacement_finish(p):
    gate = os.environ.get("CHIMERA_COMPOUND_RETIREMENT_GATE")
    if not gate:
        return
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    for decision in ("a", "r", "e", "f"):
        ready.unlink(missing_ok=True)
        release.unlink(missing_ok=True)
        _, fh, opened = p.create("layout-replacement-finish-" + decision)
        # Materialize before the measured read-only finish-rejection attempt.
        original = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[1].logr_stateid
        p.layout = (fh, original)
        marker, marker_fh, _ = p.create("retry-retirement-terminal-layout-replace-" + decision)
        operations = [op.putfh(p.directory), p.open_op(marker, create=False, owner=b"marker-" + marker),
                      op.savefh(), op.putfh(fh), return_layout_op(original), layoutget_op(opened),
                      op.test_stateid([original])]
        if decision == "f":
            operations.append(layoutget_op(original))
        operations.extend([op.restorefh(), op.close(0, CURRENT)])
        label = "layout_replacement_finish_" + decision
        slot = p.session.compound_async(operations, tag=("boundary_" + label).encode())
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "replacement did not reach pending finish")
                time.sleep(0.01)
            require(state_status(p, original) == NFS4_OK,
                    "pending replacement retired its public predecessor")
        finally:
            release.write_text("r" if decision == "f" else decision)
        result = p.sent_result(operations, label, p.session.listen(slot),
                               NFS4ERR_IO if decision == "e" else NFS4ERR_BAD_STATEID if decision == "f" else NFS4_OK)
        if decision == "e":
            require(state_status(p, original) == NFS4_OK, "rejected replacement lost the original layout")
        else:
            current = result.resarray[5].logr_stateid
            p.layout = (fh, current)
            require(current.other != original.other and state_status(p, current) == NFS4_OK and
                    state_status(p, original) == NFS4ERR_BAD_STATEID and
                    result.resarray[6].tsr_status_codes == [NFS4ERR_BAD_STATEID],
                    "accepted/retried replacement changed identity or tombstone order")
            if decision == "f":
                require(len(result.resarray) == 8, "failed replacement suffix did not stop at stale layout")
                p.call([op.putfh(marker_fh), op.close(0, result.resarray[1].stateid)])
        p.return_layout(*p.layout)
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    parser.add_argument("--minor", type=int, choices=(1, 2), default=2)
    parser.add_argument("--server-log", required=True)
    probe = FeatureProbe(parser.parse_args())
    try:
        test_replacements(probe)
        test_returned_truncate(probe)
        test_truncate_grant(probe)
        test_open_truncate_layouts(probe)
        test_replacement_finish(probe)
        probe.check_trace()
    finally:
        probe.cleanup()
    print(f"PASS: {len(probe.measured)} layout transition compound checks", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
