#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
Regression test: an NFSv4.0 client whose lease was cancelled must be able to
register again under a new clientid.

RFC 7530 9.6.3.2: once the server cancels a lease, the clientid answers
NFS4ERR_EXPIRED and the client must use SETCLIENTID + SETCLIENTID_CONFIRM "in
order to establish a new clientid".  The server kept the dead record in its
by-owner table, so a client that re-registered with the same verifier (it did
not reboot) took the callback-update branch and got the dead clientid back.
OPEN then revived it just far enough to hand out a stateid that OPEN_CONFIRM
found expired, and the client was stuck until it rebooted or the server
restarted.

The lease is cancelled the usual way: client A holds a DENY_WRITE open, goes
silent past its lease into courtesy, and B's conflicting OPEN reclaims A's
share reservation.  Checks:

  1. re-register before the sweep : A re-registers at once, before the 1 Hz
                                    lease sweep has reaped it.  It must get a
                                    new clientid, and OPEN + OPEN_CONFIRM must
                                    work.
  2. re-register after the sweep  : the same for A2, once the sweep has torn
                                    its state down.
  3. old clientid                 : RENEW of a cancelled clientid answers
                                    NFS4ERR_EXPIRED, and a retransmitted
                                    SETCLIENTID_CONFIRM of the old pair cannot
                                    report success.
  4. live client keeps its id     : a same-verifier SETCLIENTID from a client
                                    whose lease is intact is a callback update
                                    and keeps the clientid.
  5. tombstone retired            : a lease after the cancellation the old
                                    clientid is forgotten: RENEW answers
                                    NFS4ERR_STALE_CLIENTID.

Needs a short lease (the wrapper sets 4 s) and the default 24 h courtesy, so A
is still in courtesy when B's OPEN arrives.  Driven by
scripts/nfs4_lease_cancel_test_wrapper.sh.
"""
import argparse
import os
import sys
import time

# Honour the wrapper's PYNFS_DIR so a non-default checkout is actually the one
# under test; /opt/pynfs is only the fallback.
_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.0"))
sys.path.insert(1, os.path.join(_PYNFS, "nfs4.0", "lib"))

import nfs4lib                                          # noqa: E402
from nfs4lib import BadCompoundRes, op4                 # noqa: E402
from xdrdef.nfs4_const import *                         # noqa: E402
import rpc.rpc as rpc                                   # noqa: E402

nfs4lib.SHOW_TRAFFIC = False


def name(status):
    return nfsstat4.get(status, str(status))


def status_of(fn):
    """Run fn; return NFS4_OK or the status that failed it."""
    try:
        fn()
        return NFS4_OK
    except BadCompoundRes as e:
        return e.errcode


class Run:
    def __init__(self, args):
        self.args = args
        self.tag = "t%d" % (os.getpid() % 100000)

    def client(self, who):
        sec = rpc.SecAuthSys(0, b"chimlease", 0, 0, [])
        return nfs4lib.NFS4Client(("lease-%s-%s" % (who, self.tag)).encode(),
                                  self.args.host, self.args.port,
                                  homedir=[self.args.export.encode()],
                                  sec_list=[sec])

    def path(self, fname):
        return [self.args.export.encode(),
                ("%s-%s" % (fname, self.tag)).encode()]


def renew(c, clientid):
    return c.compound([op4.renew(clientid)]).status


def check(label, ok, detail):
    print("    %-40s %s  (%s)" % (label, "ok" if ok else "FAIL", detail))
    return ok


def reregister(run, c, old_clientid, verifier, fname):
    """Same id, same verifier: the recovery a client does after
    NFS4ERR_EXPIRED.  Returns True if it got a new clientid and can open."""
    new_clientid, _ = c.init_connection(verifier=verifier)
    ok = check("new clientid", new_clientid != old_clientid,
               "%#x -> %#x" % (old_clientid, new_clientid))
    st = status_of(lambda: c.create_confirm(b"owner-2", run.path(fname)))
    ok &= check("OPEN + OPEN_CONFIRM", st == NFS4_OK, name(st))
    return ok


def main():
    p = argparse.ArgumentParser(
        description="NFSv4.0 lease-cancel re-registration regression test")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", default="share",
                   help="export name without the leading slash")
    args = p.parse_args()
    run = Run(args)
    print("NFSv4.0 lease-cancel test against %s:%d /%s   (run tag %s)"
          % (args.host, args.port, args.export, run.tag))

    # A and A2 each hold a DENY_WRITE open, then go silent.
    holders = []
    for who in ("A", "A2"):
        c = run.client(who)
        verifier = c.verifier
        clientid, confirm = c.init_connection()
        fh, stateid = c.create_confirm(b"owner-1", run.path("held-" + who),
                                       access=OPEN4_SHARE_ACCESS_BOTH,
                                       deny=OPEN4_SHARE_DENY_WRITE)
        holders.append((c, verifier, clientid, confirm, fh, stateid))
    lease = holders[0][0].getLeaseTime()
    print("A and A2 hold DENY_WRITE opens; lease %d s" % lease)

    # Live client C, kept renewed through the wait, for check 4.
    live = run.client("C")
    live_verifier = live.verifier
    live_clientid, _ = live.init_connection()

    deadline = time.time() + 2 * lease
    while time.time() < deadline:
        time.sleep(1)
        renew(live, live_clientid)

    # B's conflicting OPENs reclaim A's and A2's courtesy share reservations.
    b = run.client("B")
    b.init_connection()
    conflicts = [status_of(lambda w=who: b.open_confirm(
        ("owner-" + w).encode(), run.path("held-" + w),
        access=OPEN4_SHARE_ACCESS_WRITE, deny=OPEN4_SHARE_DENY_NONE))
        for who in ("A", "A2")]
    print("B's conflicting OPENs -> %s"
          % ", ".join(name(s) for s in conflicts))
    if any(s != NFS4_OK for s in conflicts):
        print("FAIL: B could not reclaim the courtesy share reservations")
        return 1

    results = []

    a, a_verifier, a_clientid, a_confirm, a_fh, a_stateid = holders[0]
    print("\n[1] re-register before the sweep")
    st = a.close_file(b"owner-1", a_fh, a_stateid).status
    ok = check("CLOSE of the old stateid", st == NFS4ERR_EXPIRED, name(st))
    ok &= reregister(run, a, a_clientid, a_verifier, "after-A")
    results.append(("re-register before sweep", ok))

    # Before any sleep: the tombstones must still be there.
    a2, a2_verifier, a2_clientid, a2_confirm, _, _ = holders[1]
    print("\n[3] old clientid")
    probe = run.client("probe")
    st = renew(probe, a_clientid)
    ok = check("RENEW of A's old clientid", st == NFS4ERR_EXPIRED, name(st))
    st = probe.compound([op4.setclientid_confirm(a2_clientid,
                                                 a2_confirm)]).status
    ok &= check("retransmitted SETCLIENTID_CONFIRM", st != NFS4_OK, name(st))
    results.append(("old clientid", ok))

    print("\n[2] re-register after the sweep")
    time.sleep(2.5)
    st = renew(a2, a2_clientid)
    ok = check("RENEW of the old clientid", st == NFS4ERR_EXPIRED, name(st))
    ok &= reregister(run, a2, a2_clientid, a2_verifier, "after-A2")
    results.append(("re-register after sweep", ok))

    print("\n[4] live client keeps its id")
    clientid, _ = live.init_connection(verifier=live_verifier)
    ok = check("same clientid", clientid == live_clientid,
               "%#x -> %#x" % (live_clientid, clientid))
    st = status_of(lambda: live.create_confirm(b"owner-1",
                                               run.path("live-C")))
    ok &= check("OPEN + OPEN_CONFIRM", st == NFS4_OK, name(st))
    results.append(("live client keeps its id", ok))

    print("\n[5] tombstone retired")
    # The sweep retires a tombstone a lease after it reaped the client, and
    # runs once a second.
    time.sleep(lease + 3)
    st = renew(probe, a_clientid)
    ok = check("RENEW of A's old clientid", st == NFS4ERR_STALE_CLIENTID,
               name(st))
    st = status_of(lambda: a.create_confirm(b"owner-3", run.path("late-A")))
    ok &= check("A still opens under its new clientid", st == NFS4_OK,
                name(st))
    results.append(("tombstone retired", ok))

    print("\n=== verdict ===")
    for label, ok in results:
        print("  %-26s : %s" % (label, "PASS" if ok else "FAIL"))
    passed = all(ok for _, ok in results)
    print("  overall %s" % ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
