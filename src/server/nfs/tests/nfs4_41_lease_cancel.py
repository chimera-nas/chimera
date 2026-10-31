#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
Regression test: an NFSv4.1 client whose lease was cancelled must be able to
register again under a new clientid.

The NFSv4.1 shape of nfs4_v40_lease_cancel.py.  Client A holds a DENY_WRITE
open, goes silent past its lease into courtesy, and B's conflicting OPEN
reclaims A's share reservation, which cancels A's lease.  A then does what a
client does when its state is gone: EXCHANGE_ID with the same owner and
verifier (it did not reboot), CREATE_SESSION, OPEN.

The server kept the dead record matchable by owner, so EXCHANGE_ID case 2
handed the dead clientid back with CONFIRMED_R ("no state was lost"),
CREATE_SESSION and OPEN revived it, and the next lease sweep reaped the
revived client again because the reclaim mark is never cleared.  Checks:

  1. re-register before the sweep : A re-registers at once, before the 1 Hz
                                    lease sweep has reaped it.  EXCHANGE_ID
                                    must issue a new, unconfirmed clientid,
                                    and an OPEN under it must survive the
                                    following sweeps.
  2. re-register after the sweep  : the same for A2, once the sweep has torn
                                    its state down.
  3. old clientid                 : CREATE_SESSION on a cancelled clientid is
                                    NFS4ERR_STALE_CLIENTID, not a revival.

Needs a short lease (the wrapper sets 4 s) and the default 24 h courtesy.
Driven by scripts/nfs4_lease_cancel_test_wrapper.sh.
"""
import argparse
import os
import sys
import time

_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.1"))
sys.path.insert(1, _PYNFS)

import nfs4client                                       # noqa: E402
import nfs_ops                                          # noqa: E402
import rpc                                              # noqa: E402
from xdrdef.nfs4_const import (                         # noqa: E402
    CLAIM_NULL, EXCHGID4_FLAG_CONFIRMED_R, FATTR4_MODE, GUARDED4, NFS4_OK,
    NFS4ERR_STALE_CLIENTID, OPEN4_CREATE, OPEN4_NOCREATE,
    OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
    OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, OPEN4_SHARE_DENY_WRITE,
    nfsstat4)
from xdrdef.nfs4_type import (                          # noqa: E402
    createhow4, open_claim4, open_owner4, openflag4)

op = nfs_ops.NFS4ops()

RUN = b"%d_%d" % (os.getpid(), int(time.time()))


def name(status):
    return nfsstat4.get(status, str(status))


def check(label, ok, detail):
    print("    %-40s %s  (%s)" % (label, "ok" if ok else "FAIL", detail))
    return ok


class Harness(object):
    def __init__(self, args):
        self.args = args
        self.client = nfs4client.NFS4Client(args.host.encode(), args.port,
                                            minorversion=1)
        self.client.set_cred(rpc.security.AuthSys().init_cred(
            uid=0, gid=0, name=b"chimlease41"))
        self.dirfh = None

    def register(self, who, verf=None):
        """EXCHANGE_ID + CREATE_SESSION + RECLAIM_COMPLETE.  Returns the
        client record and its session."""
        c = self.client.new_client(b"lease41_%s_%s" % (who.encode(), RUN),
                                   verf=verf)
        sess = c.create_session()
        sess.compound([op.reclaim_complete(False)])
        if self.dirfh is None:
            res = sess.compound([op.putrootfh(),
                                 op.lookup(self.args.export.encode()),
                                 op.getfh()])
            self.dirfh = res.resarray[-1].object
        return c, sess

    def open(self, sess, owner, fname, access, deny, create):
        if create:
            how = openflag4(OPEN4_CREATE,
                            createhow4(GUARDED4, {FATTR4_MODE: 0o644}))
        else:
            how = openflag4(OPEN4_NOCREATE)
        res = sess.compound([
            op.putfh(self.dirfh),
            op.open(0, access | OPEN4_SHARE_ACCESS_WANT_NO_DELEG, deny,
                    open_owner4(0, owner), how,
                    open_claim4(CLAIM_NULL, b"%s_%s" % (fname, RUN))),
            op.getfh(),
        ], handle_state_errors=False)
        if res.status != NFS4_OK:
            return res.status, None, None
        return NFS4_OK, res.resarray[-1].object, res.resarray[-2].stateid

    def close(self, sess, fh, sid):
        return sess.compound([op.putfh(fh), op.close(0, sid)],
                             handle_state_errors=False).status


def reregister(h, who, old, verifier, settle):
    """Same owner, same verifier: the recovery a client does once its state
    is gone.  Returns True if it got a new clientid whose state lasts."""
    c = h.client.new_client(b"lease41_%s_%s" % (who.encode(), RUN),
                            verf=verifier)
    ok = check("new clientid", c.clientid != old.clientid,
               "%#x -> %#x" % (old.clientid, c.clientid))
    ok &= check("not CONFIRMED_R",
                not (c.flags & EXCHGID4_FLAG_CONFIRMED_R),
                "flags %#x" % c.flags)
    res = c._create_session()
    if not check("CREATE_SESSION", res.status == NFS4_OK, name(res.status)):
        return False
    sess = c._add_session(res.resarray[0])
    sess.compound([op.reclaim_complete(False)])
    st, fh, sid = h.open(sess, b"owner_%s_2" % who.encode(),
                         b"after_%s" % who.encode(),
                         OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_DENY_NONE, True)
    ok &= check("OPEN", st == NFS4_OK, name(st))
    if st != NFS4_OK:
        return False
    # Two sweeps later the open must still be there: a revived client that
    # still carried the reclaim mark was reaped again by the next sweep.
    time.sleep(settle)
    st = h.close(sess, fh, sid)
    ok &= check("CLOSE after %.1f s" % settle, st == NFS4_OK, name(st))
    return ok


def main():
    p = argparse.ArgumentParser(
        description="NFSv4.1 lease-cancel re-registration regression test")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", default="share",
                   help="export name without the leading slash")
    args = p.parse_args()
    h = Harness(args)
    print("NFSv4.1 lease-cancel test against %s:%d /%s   (run %s)"
          % (args.host, args.port, args.export, RUN.decode()))

    # A and A2 each hold a DENY_WRITE open, then go silent.
    holders = {}
    for who in ("A", "A2"):
        verf = os.urandom(8)
        c, sess = h.register(who, verf)
        st, fh, sid = h.open(sess, b"owner_%s_1" % who.encode(),
                             b"held_%s" % who.encode(),
                             OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_DENY_WRITE,
                             True)
        if st != NFS4_OK:
            print("FAIL: %s could not create its file: %s" % (who, name(st)))
            return 1
        holders[who] = (c, verf)
    # The wrapper's nfs4_lease_time: two leases puts both clients in courtesy.
    lease = 4
    time.sleep(2 * lease)

    # B's conflicting OPENs reclaim A's and A2's courtesy share reservations.
    _, bsess = h.register("B")
    for who in ("A", "A2"):
        st, _, _ = h.open(bsess, b"owner_B_%s" % who.encode(),
                          b"held_%s" % who.encode(),
                          OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE,
                          False)
        print("B's conflicting OPEN of %s's file -> %s" % (who, name(st)))
        if st != NFS4_OK:
            print("FAIL: B could not reclaim the courtesy share reservation")
            return 1

    results = []

    print("\n[1] re-register before the sweep")
    old_a, verf_a = holders["A"]
    results.append(("re-register before sweep",
                    reregister(h, "A", old_a, verf_a, 2.5)))

    print("\n[3] old clientid")
    res = old_a._create_session(max_retries=1)
    results.append(("old clientid",
                    check("CREATE_SESSION on A's old clientid",
                          res.status == NFS4ERR_STALE_CLIENTID,
                          name(res.status))))

    print("\n[2] re-register after the sweep")
    old_a2, verf_a2 = holders["A2"]
    results.append(("re-register after sweep",
                    reregister(h, "A2", old_a2, verf_a2, 2.5)))

    print("\n=== verdict ===")
    for label, ok in results:
        print("  %-26s : %s" % (label, "PASS" if ok else "FAIL"))
    passed = all(ok for _, ok in results)
    print("  overall %s" % ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
