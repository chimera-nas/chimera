#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Synthetic protocol cursors never borrow their base inode's state identity.

Run with synthetic, synthetic_delegation or pnfs_synthetic through the common
ctest wrapper. Every measured request checks its complete shared VFS span.
The retry fixture rejects only attempts without executed backend mutations.
"""

from nfs4_compound_features import *  # noqa: F401,F403
from nfs4_compound_retirement import return_layout_op, state_status
from nfs4_compound_v40_boundaries import Probe40


def measured(p, operations, name, expected=NFS4_OK, count=None):
    first = 0 if p.args.minor == 0 else 1
    return p.call(operations, name, expected,
                  runs=[(first, len(operations) if count is None else count)])


def lock_op(p, sid, owner, open_seqid=0):
    clientid = p.clientid if p.args.minor == 0 else 0
    return op.lock(WRITE_LT, False, 0, 8,
                   locker4(True, open_to_lock_owner4(open_seqid, sid, 0,
                                                     lock_owner4(clientid, owner))))


def failed_suffix(p, selector, operation, label, expected, fh, sid, count=None):
    operations = selector + [op.getfh(), operation, op.putfh(fh),
                             op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result = measured(p, operations, label, expected, count=count)
    require(len(result.resarray) == len(selector) + 2,
            f"{label}: synthetic failure did not stop the suffix")
    require(p.read(fh, sid) == b"base-data", f"{label}: synthetic operation changed its base")
    return result


def test_synthetic_state(p):
    name, fh, opened = p.create("synthetic-base")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"base-data")])
    attrdir = p.call([op.putfh(fh), op.openattr(False), op.getfh()]).resarray[-1].object
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    owner = p.file_owners[fh] if p.args.minor == 0 else None
    seqid = p.owner_seqids[owner] if owner else 0
    locked = p.call([op.putfh(fh), lock_op(p, opened, b"synthetic-held", seqid)]).resarray[-1].lock_stateid
    if owner:
        p.owner_seqids[owner] += 1
        seqid += 1

    selectors = (
        ("root", [op.putrootfh()]),
        ("runtime_root", [op.putfh(p.directory), op.lookupp()]),
        ("saved_root", [op.putrootfh(), op.savefh(), op.putfh(fh), op.restorefh()]),
        ("attrdir", [op.putfh(attrdir)]),
        ("runtime_attrdir", [op.putfh(fh), op.openattr(False)]),
        ("saved_attrdir", [op.putfh(attrdir), op.savefh(), op.putfh(p.directory), op.restorefh()]),
    )
    state_ops = [
        ("close", op.close(seqid, opened)),
        ("downgrade", op.open_downgrade(opened, seqid, OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE)),
        ("lock", lock_op(p, opened, b"synthetic-new-lock", seqid)),
        ("locku", op.locku(WRITE_LT, 1 if owner else 0, locked, 0, 8)),
    ]
    if p.args.minor >= 2:
        state_ops.append(("advise", op.io_advise(opened, 0, 8, 0)))
    for cursor, selector in selectors:
        for label, operation in state_ops:
            failed_suffix(p, selector, operation, f"synthetic_{cursor}_{label}",
                          NFS4ERR_BAD_STATEID, fh, opened)
        if p.args.minor >= 2:
            result = measured(p, selector + [op.io_advise(ANONYMOUS, 0, 8, 0), op.getfh(),
                                             op.putfh(fh), op.read(opened, 0, 16)],
                              f"synthetic_{cursor}_anonymous_advise")
            require(result.resarray[-3].object == (attrdir if "attrdir" in cursor else root) and
                    result.resarray[-1].data == b"base-data", "anonymous advice changed protocol cursor")
        failed_suffix(p, selector, op.setattr(opened, {FATTR4_SIZE: 0}),
                      f"synthetic_{cursor}_setattr", NFS4ERR_STALE, fh, opened)

    # No invalid state mutation may have removed a held range or changed the
    # OPEN's accepted version/mode, even after a rejected finish retries it.
    clientid = p.clientid if p.args.minor == 0 else 0
    p.call([op.putfh(fh), op.lockt(WRITE_LT, 0, 8, lock_owner4(clientid, b"synthetic-observer"))],
           expected=NFS4ERR_DENIED)
    p.call([op.putfh(fh), op.locku(WRITE_LT, 1 if owner else 0, locked, 0, 8)])
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"base-data")])
    if p.args.minor:
        require(state_status(p, opened) == NFS4_OK, "synthetic mutation changed the OPEN stateid")

    for cursor, selector in selectors:
        if "attrdir" in cursor:
            for label, operation in (("access", op.access(ACCESS4_READ | ACCESS4_LOOKUP)),
                                     ("secinfo", op.secinfo(b"missing-stream"))):
                failed_suffix(p, selector, operation, f"synthetic_{cursor}_{label}",
                              NFS4ERR_STALE, fh, opened)
            # Validation still precedes the synthetic-object backend error.
            failed_suffix(p, selector, op.secinfo(b""), f"synthetic_{cursor}_secinfo_empty",
                          NFS4ERR_INVAL, fh, opened, count=len(selector) + 2)
            invalid_open = p.open_op(name, create=False, owner=b"synthetic-claim-fh")
            invalid_open.opopen.claim = open_claim4(CLAIM_FH)
            if p.args.minor:
                failed_suffix(p, selector, invalid_open, f"synthetic_{cursor}_open_claim_fh",
                              NFS4ERR_NOTSUPP, fh, opened)
        else:
            failed_suffix(p, selector, p.open_op(b"synthetic-no-create", create=False,
                                                 owner=b"synthetic-root-open-" + cursor.encode()),
                          f"synthetic_{cursor}_open", NFS4ERR_STALE, fh, opened)

    if p.args.minor:
        for style in (SECINFO_STYLE4_CURRENT_FH, SECINFO_STYLE4_PARENT):
            result = measured(p, [op.putfh(fh), op.openattr(False), op.savefh(),
                                   op.secinfo_no_name(style), op.restorefh(), op.getfh()],
                              f"synthetic_attrdir_secinfo_no_name_{style}")
            require(result.resarray[3].resok4 and result.resarray[-1].object == attrdir,
                    "SECINFO_NO_NAME lost export policy or saved synthetic cursor")
            result = measured(p, [op.putfh(attrdir), op.secinfo_no_name(style), op.getfh(),
                                   op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"BAD")],
                              f"synthetic_attrdir_secinfo_consumes_{style}", NFS4ERR_NOFILEHANDLE)
            require(len(result.resarray) == 3 and p.read(fh, opened) == b"base-data",
                    "SECINFO_NO_NAME did not consume the current cursor")
        enabled = p.feature.startswith("pnfs")
        for cursor, selector in selectors:
            failed_suffix(p, selector, layoutget_op(opened), f"synthetic_{cursor}_layoutget",
                          NFS4ERR_BAD_STATEID if enabled else NFS4ERR_NOTSUPP, fh, opened)
            failed_suffix(p, selector, return_layout_op(opened), f"synthetic_{cursor}_layoutreturn",
                          NFS4ERR_BAD_STATEID if enabled else NFS4ERR_NOTSUPP, fh, opened)


def test_v40_confirm(p):
    name = b"synthetic-unconfirmed"
    owner = b"owner-" + name
    result = p.call([op.putfh(p.directory), p.open_op(name), op.getfh()])
    fh, opened = result.resarray[-1].object, result.resarray[-2].stateid
    require(result.resarray[-2].rflags & OPEN4_RESULT_CONFIRM, "fixture needs unconfirmed OPEN")
    attrdir = p.call([op.putfh(fh), op.openattr(False), op.getfh()]).resarray[-1].object
    for label, selector in (("root", [op.putrootfh()]),
                            ("attrdir", [op.putfh(fh), op.openattr(False)])):
        result = measured(p, selector + [op.open_confirm(opened, 1), op.putfh(fh), op.getfh()],
                          "synthetic_confirm_" + label, NFS4ERR_BAD_STATEID)
        require(len(result.resarray) == len(selector) + 1, "synthetic OPEN_CONFIRM ran its suffix")
    result = p.call([op.putfh(fh), op.open_confirm(opened, 1)])
    confirmed = result.resarray[-1].open_stateid
    p.owner_seqids[owner] = 2
    p.file_owners[fh] = owner
    p.files.append((name, fh, confirmed))
    close, _ = p.close(fh, confirmed)
    for label, synthetic in (("root", p.call([op.putrootfh(), op.getfh()]).resarray[-1].object),
                             ("attrdir", attrdir)):
        result = measured(p, [op.putfh(synthetic), close, op.putfh(fh), op.getfh()],
                          "synthetic_close_replay_" + label, NFS4ERR_BAD_STATEID)
        require(len(result.resarray) == 2, "CLOSE replay bypassed synthetic cursor identity")


def test_delegation_identity(p):
    warm_backchannel(p)
    name, fh, opened = p.create("synthetic-delegation")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"base-data")])
    delegated = delegation_open(p, name, fh)
    require(delegated is not None, "fixture requires a real write delegation")
    for label, selector in (("root", [op.putrootfh()]),
                            ("attrdir", [op.putfh(fh), op.openattr(False)])):
        failed_suffix(p, selector, op.delegreturn(delegated), "synthetic_delegreturn_" + label,
                      NFS4ERR_BAD_STATEID, fh, opened)
        require(state_status(p, delegated) == NFS4_OK,
                "synthetic DELEGRETURN retired its base inode's delegation")
        require(p.read(fh, delegated) == b"base-data", "base delegation no longer authorizes its own object")
    p.return_delegation(fh, delegated)


def test_layout_identity(p):
    _, fh, opened = p.create("synthetic-layout")
    p.call([op.putfh(fh), op.write(opened, 0, FILE_SYNC4, b"base-data")])
    granted = p.call([op.putfh(fh), layoutget_op(opened)]).resarray[-1].logr_stateid
    p.layout = (fh, granted)
    attrdir = p.call([op.putfh(fh), op.openattr(False), op.getfh()]).resarray[-1].object
    selectors = (
        ("root", [op.putrootfh()]),
        ("runtime_root", [op.putfh(p.directory), op.lookupp()]),
        ("saved_root", [op.putrootfh(), op.savefh(), op.putfh(fh), op.restorefh()]),
        ("attrdir", [op.putfh(attrdir)]),
        ("runtime_attrdir", [op.putfh(fh), op.openattr(False)]),
        ("saved_attrdir", [op.putfh(attrdir), op.savefh(), op.putfh(p.directory), op.restorefh()]),
    )
    for label, selector in selectors:
        unknown = return_layout_op(granted)
        unknown.oplayoutreturn.lora_layout_type = LAYOUT4_NFSV4_1_FILES
        failed_suffix(p, selector, unknown, "synthetic_layout_unknown_type_" + label,
                      NFS4ERR_UNKNOWN_LAYOUTTYPE, fh, opened)
        require(state_status(p, granted) == NFS4_OK,
                "invalid synthetic layout return changed the base inode's live layout")
        for opname, operation in (("return", return_layout_op(granted)),
                                  ("grant", layoutget_op(granted))):
            failed_suffix(p, selector, operation, "synthetic_layout_" + opname + "_" + label,
                          NFS4ERR_BAD_STATEID, fh, opened)
            require(state_status(p, granted) == NFS4_OK,
                    "synthetic layout operation changed the base inode's live layout")
    p.return_layout(fh, granted)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    args = parser.parse_args()
    p = Probe40(args) if args.minor == 0 else FeatureProbe(args)
    try:
        test_synthetic_state(p)
        if args.minor == 0:
            test_v40_confirm(p)
        elif p.feature == "synthetic_delegation":
            test_delegation_identity(p)
        elif p.feature.startswith("pnfs"):
            test_layout_identity(p)
        p.check_trace()
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
