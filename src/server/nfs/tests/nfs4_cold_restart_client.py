#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
One NFSv4.1 client for the cold-restart test: establish itself, open a file,
and either tear down cleanly or walk away.

The pynfs testserver cannot play this part.  Its Environment.finish() sends
DESTROY_CLIENTID for every client it created, so a pynfs run leaves no
recovery record behind: the only record a pynfs run ever abandons is an
unconfirmed EXCHANGE_ID from an exchange_id test, and the lease sweeper drops
that within a lease.  The scenarios under test need a CONFIRMED client that
was alive when the server crashed, which is exactly a client that exits
without saying goodbye (--keep).

Three things are asserted on the way:

  * EXCHANGE_ID is NFS4_OK.  new_client() raises on any other status, DELAY
    included, so a client started right after the server reports ready proves
    the recovery load finished before the listener came up.
  * CREATE_SESSION + RECLAIM_COMPLETE succeed (new_client_session).
  * The OPEN of a fresh file is NFS4_OK (--expect-open ok) or NFS4ERR_GRACE
    (--expect-open grace), whichever the boot under test should produce.

Needs pynfs (nfs4.1 tree) and root (AUTH_SYS uid 0 against a root-owned export
root).  Driven by scripts/pynfs_cold_restart_test_wrapper.sh.
"""
import argparse
import os
import sys
import time

_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.1"))
sys.path.insert(1, _PYNFS)

import nfs4client  # noqa: E402
import nfs4lib  # noqa: E402
import nfs_ops  # noqa: E402
import rpc  # noqa: E402
from xdrdef.nfs4_const import (  # noqa: E402
    CLAIM_NULL, GUARDED4, NFS4ERR_GRACE, NFS4_OK, OPEN4_CREATE,
    OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
    OPEN4_SHARE_DENY_NONE, FATTR4_MODE)
from xdrdef.nfs4_type import (  # noqa: E402
    createhow4, open_claim4, open_owner4, openflag4)

op = nfs_ops.NFS4ops()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2049)
    ap.add_argument("--export", default="share",
                    help="export name under the pseudo-root")
    ap.add_argument("--name", required=True,
                    help="client owner string (co_ownerid)")
    ap.add_argument("--expect-open", choices=["ok", "grace"], default="ok",
                    help="status the OPEN must return")
    ap.add_argument("--keep", action="store_true",
                    help="exit without DESTROY_SESSION / DESTROY_CLIENTID, "
                         "abandoning the client as a crash would")
    args = ap.parse_args()

    name = args.name.encode()
    c = nfs4client.NFS4Client(args.host.encode(), args.port, minorversion=1)
    c.set_cred(rpc.security.AuthSys().init_cred(uid=0, gid=0,
                                                 name=b"coldrestart"))

    # EXCHANGE_ID + CREATE_SESSION + RECLAIM_COMPLETE; raises on anything but
    # NFS4_OK at each step.
    sess = c.new_client_session(name)
    print("ok: %s established (clientid 0x%x)" % (args.name,
                                                    sess.client.clientid))

    fname = b"%s_%d" % (name, int(time.time()))
    attrs = {FATTR4_MODE: 0o644}
    access = OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG
    res = sess.compound([
        op.putrootfh(),
        op.lookup(args.export.encode()),
        op.open(0, access, OPEN4_SHARE_DENY_NONE, open_owner4(0, name),
                openflag4(OPEN4_CREATE, createhow4(GUARDED4, attrs)),
                open_claim4(CLAIM_NULL, fname)),
        op.getfh(),
    ])
    want = NFS4_OK if args.expect_open == "ok" else NFS4ERR_GRACE
    nfs4lib.check(res, want, msg="OPEN of %s" % fname.decode())
    print("ok: %s OPEN -> %s" % (args.name, nfs4lib.nfsstat4[want]))

    if args.keep:
        print("ok: %s abandoned (no DESTROY_SESSION / DESTROY_CLIENTID)"
              % args.name)
        return 0

    # A clean exit: the open state must go first, or DESTROY_CLIENTID is
    # (correctly) refused NFS4ERR_CLIENTID_BUSY.
    if want == NFS4_OK:
        fh = res.resarray[-1].object
        stateid = res.resarray[-2].stateid
        nfs4lib.check(sess.compound([op.putfh(fh), op.close(0, stateid)]))
    nfs4lib.check(c.compound([op.destroy_session(sess.sessionid)]))
    nfs4lib.check(c.compound([op.destroy_clientid(sess.client.clientid)]))
    print("ok: %s destroyed" % args.name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
