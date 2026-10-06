#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""A new RPC XID must replay every field of a v4.0 delegated OPEN."""
import argparse
import os
import sys
import time

pynfs = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path[:0] = [os.path.join(pynfs, "nfs4.0"), os.path.join(pynfs, "nfs4.0", "lib")]
import nfs4lib
from nfs4lib import op4
from xdrdef.nfs4_const import *
import rpc.rpc as rpc
nfs4lib.SHOW_TRAFFIC = False


def checked(res):
    if res.status != NFS4_OK:
        raise AssertionError(repr(res))
    return res


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2049)
    parser.add_argument("--export", default="share")
    args = parser.parse_args()
    c = nfs4lib.NFS4Client(b"delegation-replay", args.host, args.port,
                          homedir=[args.export.encode()],
                          sec_list=[rpc.SecAuthSys(0, b"delegation-replay", 0, 0, [])])
    c.init_connection(cb_ident=0)
    parent = checked(c.compound(c.use_obj(c.homedir) + [op4.getfh()])).resarray[-1].switch.switch.object

    for access, wanted in ((OPEN4_SHARE_ACCESS_READ, OPEN_DELEGATE_READ),
                           (OPEN4_SHARE_ACCESS_BOTH, OPEN_DELEGATE_WRITE)):
        owner = f"delegation-owner-{access}".encode()
        # The first OPEN probes CB_NULL and confirms the owner. Later creates
        # should grant, but retry fixture setup while the asynchronous probe
        # completes; never retry or mask the measured replay itself.
        granted = False
        for attempt in range(8):
            name = f"delegation-replay-{access}-{attempt}".encode()
            operation = c.open(owner, name, OPEN4_CREATE, GUARDED4,
                               {FATTR4_MODE: 0o666}, None, access, OPEN4_SHARE_DENY_NONE)
            operations = [op4.putfh(parent), operation, op4.getfh(),
                          op4.putfh(parent), operation, op4.getfh()]
            res = checked(c.compound(operations, tag=b"delegation-original"))
            opened = res.resarray[1].switch.switch
            fh = res.resarray[2].switch.switch.object
            delegation = opened.delegation
            assert repr(res.resarray[4]) == repr(res.resarray[1]), res
            assert res.resarray[5].switch.switch.object == fh, res
            if delegation.delegation_type == wanted:
                original = repr(res.resarray[1])
                replay = checked(c.compound(operations, tag=b"delegation-new-xid-replay"))
                assert repr(replay.resarray[1]) == original, (res, replay)
                assert replay.resarray[2].switch.switch.object == fh, (res, replay)
                assert repr(replay.resarray[4]) == original, (res, replay)
                assert replay.resarray[5].switch.switch.object == fh, (res, replay)
                arm = delegation.read if wanted == OPEN_DELEGATE_READ else delegation.write
                assert arm.permissions.who, "delegation must carry its owned permission principal"
                claim_owner = b"claim-" + owner
                claim = c.open(claim_owner, name, OPEN4_NOCREATE, access=access,
                               deny=OPEN4_SHARE_DENY_NONE)
                claim.switch.claim = nfs4lib.open_claim4(CLAIM_DELEGATE_CUR,
                    delegate_cur_info=nfs4lib.open_claim_delegate_cur4(arm.stateid, name))
                claims = [op4.putfh(parent), claim, op4.getfh()]
                claimed = checked(c.compound(claims, tag=b"delegated-claim"))
                claimed_replay = checked(c.compound(claims, tag=b"delegated-claim-replay"))
                assert repr(claimed.resarray[1]) == repr(claimed_replay.resarray[1]), (claimed, claimed_replay)
                assert claimed.resarray[2].switch.switch.object == fh, claimed
                cinfo = claimed.resarray[1].switch.switch.cinfo
                assert cinfo.before == cinfo.after and cinfo.before != 0, cinfo
                c.advance_seqid(claim_owner, claimed)
                claim_fh, claim_sid = c.confirm(claim_owner, claimed)
                checked(c.close_file(claim_owner, claim_fh, claim_sid))
                checked(c.compound([op4.putfh(fh), op4.delegreturn(arm.stateid)]))
                granted = True
            c.advance_seqid(owner, res)
            fh, sid = c.confirm(owner, res)
            checked(c.close_file(owner, fh, sid))
            checked(c.compound([op4.putfh(parent), op4.remove(name)]))
            if granted:
                print(f"PASS v4.0 delegated OPEN replay type={wanted}: full reply and output FH", flush=True)
                break
            time.sleep(0.05)
        assert granted, f"server did not grant delegation type {wanted} with live callback channel"
    c.cb_command(0)


if __name__ == "__main__":
    main()
