#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
Regression test: an NFSv4.0 clientid minted before a server restart must be
NFS4ERR_STALE_CLIENTID after it, never a live client of the new boot
(issue #1377).

RFC 7530 9.1.1: a clientid "should be chosen so that it will not conflict
with a client ID previously assigned by the server.  This applies across
server restarts or reboots."  RFC 7530 16.33.5: the server "must take care to
ensure that these values [clientid and setclientid_confirm] are extremely
unlikely to ever be regenerated."  RFC 7530 9.6.2: NFS4ERR_STALE_CLIENTID is
how a client learns the server rebooted and that it must reclaim.

The server minted clientids as node_id<<48 | counter with a node_id that is
stable across restarts and a counter (and a setclientid_confirm counter) that
restarted at 1 every boot, so the first clients after a restart were handed
exactly the clientids of the first clients before it.  A 4.0 client that
missed the restart (a network partition, or just an idle lock holder whose
next RENEW lands after another client re-registered) then had its old
clientid resolve to that other client: RENEW answered NFS4_OK, so it never
reclaimed, and its new OPENs were filed under the other client's record.

The wrapper runs this script twice around a server crash + restart:

  --phase before : client A registers (SETCLIENTID + CONFIRM), creates a file
                   with a DENY_WRITE open and takes a whole-file write lock,
                   then walks away; its clientid / confirm verifier are saved.
  --phase after  : fresh clients register first (as clients remounting after
                   a restart would), then A's pre-restart identity is
                   presented on a new connection.  Checks:

     1. no clientid reuse     : no clientid minted after the restart equals
                                A's pre-restart clientid.
     2. no confirm reuse      : no setclientid_confirm verifier handed out
                                after the restart equals A's old one.
     3. RENEW                 : RENEW of A's old clientid is
                                NFS4ERR_STALE_CLIENTID.
     4. OPEN                  : an OPEN whose open-owner names A's old
                                clientid is NFS4ERR_STALE_CLIENTID (it must
                                not be filed under another client).
     5. LOCKT                 : a LOCKT whose lock-owner names A's old
                                clientid is NFS4ERR_STALE_CLIENTID.
     6. SETCLIENTID_CONFIRM   : a retransmitted confirm of A's old
                                {clientid, verifier} pair is not NFS4_OK.
     7. recovery              : A re-registers with its same id string and
                                verifier, gets a new clientid and can OPEN.

Before the fix checks 1-6 fail on a fresh server (the first post-restart
client is handed A's old clientid and confirm verifier).  Driven by
scripts/nfs4_v40_stale_clientid_test_wrapper.sh.
"""
import argparse
import json
import os
import sys

# Honour the wrapper's PYNFS_DIR so a non-default checkout is actually the one
# under test; /opt/pynfs is only the fallback.
_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.0"))
sys.path.insert(1, os.path.join(_PYNFS, "nfs4.0", "lib"))

import nfs4lib                                          # noqa: E402
from nfs4lib import BadCompoundRes, op4                 # noqa: E402
from xdrdef.nfs4_const import *                         # noqa: E402
from xdrdef.nfs4_type import lock_owner4                # noqa: E402
import rpc.rpc as rpc                                   # noqa: E402

nfs4lib.SHOW_TRAFFIC = False

# How many clients register after the restart before A comes back.  The first
# one alone collides with A on an unfixed server; a few more cover a server
# whose counter does not start where A's did.
SQUATTERS = 8


def name(status):
    return nfsstat4.get(status, str(status))


def status_of(fn):
    """Run fn; return NFS4_OK or the status that failed it."""
    try:
        res = fn()
        return getattr(res, "status", NFS4_OK)
    except BadCompoundRes as e:
        return e.errcode


def check(label, ok, detail):
    print("    %-36s %s  (%s)" % (label, "ok" if ok else "FAIL", detail))
    return ok


def new_client(args, who, tag):
    sec = rpc.SecAuthSys(0, b"chimstale", 0, 0, [])
    return nfs4lib.NFS4Client(("stale-%s-%s" % (who, tag)).encode(),
                              args.host, args.port,
                              homedir=[args.export.encode()],
                              sec_list=[sec])


def path(args, tag, fname):
    return [args.export.encode(), ("%s-%s" % (fname, tag)).encode()]


def phase_before(args):
    tag = "t%d" % (os.getpid() % 100000)
    a = new_client(args, "A", tag)
    clientid, confirm = a.init_connection()
    fh, open_stateid = a.create_confirm(b"owner-A", path(args, tag, "held"),
                                        access=OPEN4_SHARE_ACCESS_BOTH,
                                        deny=OPEN4_SHARE_DENY_WRITE)
    res = a.lock_file(b"owner-A", fh, open_stateid, type=WRITE_LT)
    if res.status != NFS4_OK:
        print("FAIL: A could not take its lock: %s" % name(res.status))
        return 1

    state = {
        "tag": tag,
        "id": a.id.decode(),
        "verifier": a.verifier.hex(),
        "clientid": clientid,
        "confirm": confirm.hex(),
    }
    with open(args.state, "w") as f:
        json.dump(state, f)
    print("A registered: clientid %#x confirm %s; holds a DENY_WRITE open "
          "and a write lock" % (clientid, confirm.hex()))
    return 0


def phase_after(args):
    with open(args.state) as f:
        st = json.load(f)
    a_clientid = st["clientid"]
    a_confirm = bytes.fromhex(st["confirm"])
    tag = st["tag"]
    print("A's pre-restart identity: clientid %#x confirm %s"
          % (a_clientid, a_confirm.hex()))

    # Clients remounting after the restart register before A comes back.
    squatters = []
    for i in range(SQUATTERS):
        c = new_client(args, "B%d" % i, tag)
        clientid, confirm = c.init_connection()
        squatters.append((c, clientid, confirm))
    print("post-restart clientids: %s"
          % ", ".join("%#x" % cid for _, cid, _ in squatters))
    b, b_clientid, _ = squatters[0]

    results = []

    print("\n[1] no clientid reuse")
    reused = [cid for _, cid, _ in squatters if cid == a_clientid]
    ok = check("A's clientid not re-issued", not reused,
               "re-issued to %d client(s)" % len(reused) if reused
               else "%#x unused" % a_clientid)
    results.append(("no clientid reuse", ok))

    print("\n[2] no confirm verifier reuse")
    reused = [conf for _, _, conf in squatters if conf == a_confirm]
    ok = check("A's confirm verifier not re-issued", not reused,
               "re-issued %d time(s)" % len(reused) if reused
               else "%s unused" % a_confirm.hex())
    results.append(("no confirm reuse", ok))

    # A comes back on a new connection (its old one died with the server).
    a = new_client(args, "A-back", tag)

    print("\n[3] RENEW")
    s = a.compound([op4.renew(a_clientid)]).status
    ok = check("RENEW(old clientid)", s == NFS4ERR_STALE_CLIENTID, name(s))
    results.append(("RENEW", ok))

    print("\n[4] OPEN")
    a.clientid = a_clientid
    s = status_of(lambda: a.create_file(b"owner-A2",
                                        path(args, tag, "after-A"),
                                        deny=OPEN4_SHARE_DENY_NONE))
    ok = check("OPEN(owner on old clientid)", s == NFS4ERR_STALE_CLIENTID,
               name(s))
    results.append(("OPEN", ok))

    print("\n[5] LOCKT")
    bfh, _ = b.create_confirm(b"owner-B", path(args, tag, "b-file"),
                              deny=OPEN4_SHARE_DENY_NONE)
    ops = a.use_obj(bfh)
    ops += [op4.lockt(WRITE_LT, 0, 0xffffffffffffffff,
                      lock_owner4(a_clientid, b"lockt-A"))]
    s = a.compound(ops).status
    ok = check("LOCKT(owner on old clientid)", s == NFS4ERR_STALE_CLIENTID,
               name(s))
    results.append(("LOCKT", ok))

    print("\n[6] SETCLIENTID_CONFIRM")
    s = a.compound([op4.setclientid_confirm(a_clientid, a_confirm)]).status
    ok = check("CONFIRM(old pair)", s != NFS4_OK, name(s))
    results.append(("SETCLIENTID_CONFIRM", ok))

    print("\n[7] recovery")
    r = nfs4lib.NFS4Client(st["id"].encode(), args.host, args.port,
                           homedir=[args.export.encode()],
                           sec_list=[rpc.SecAuthSys(0, b"chimstale", 0, 0,
                                                    [])])
    new_clientid, _ = r.init_connection(
        verifier=bytes.fromhex(st["verifier"]))
    ok = check("new clientid",
               new_clientid != a_clientid and
               new_clientid not in [cid for _, cid, _ in squatters],
               "%#x -> %#x" % (a_clientid, new_clientid))
    s = status_of(lambda: r.create_confirm(b"owner-A3",
                                           path(args, tag, "recovered-A")))
    ok &= check("OPEN + OPEN_CONFIRM", s == NFS4_OK, name(s))
    results.append(("recovery", ok))

    print("\n=== verdict ===")
    for label, ok in results:
        print("  %-22s : %s" % (label, "PASS" if ok else "FAIL"))
    passed = all(ok for _, ok in results)
    print("  overall %s" % ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


def main():
    p = argparse.ArgumentParser(
        description="NFSv4.0 stale-clientid-across-restart regression test")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", default="share",
                   help="export name without the leading slash")
    p.add_argument("--phase", choices=("before", "after"), required=True)
    p.add_argument("--state", required=True,
                   help="file carrying A's identity across the restart")
    args = p.parse_args()
    print("NFSv4.0 stale-clientid test (%s restart) against %s:%d /%s"
          % (args.phase, args.host, args.port, args.export))
    if args.phase == "before":
        return phase_before(args)
    return phase_after(args)


if __name__ == "__main__":
    sys.exit(main())
