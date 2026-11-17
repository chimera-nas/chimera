#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
Regression test: RENEW answers NFS4ERR_CB_PATH_DOWN to an NFSv4.0 client that
holds a delegation and re-registered with no callback program.

RFC 7530 Section 16.28.4: RENEW returns NFS4ERR_CB_PATH_DOWN when the client
holds delegations and the server cannot recall them.  A registration with
callback program zero is such a path, but no probe ever stores it as down, so
RENEW used to answer NFS4_OK.  Checks:

  1. path up         : a client registered with a callback program and holding
                       a write delegation renews with NFS4_OK.
  2. program zero    : the same client re-registers (same verifier, a
                       callback update) with program zero; RENEW answers
                       NFS4ERR_CB_PATH_DOWN.
  3. no delegations  : a client registered with program zero that holds no
                       delegation still renews with NFS4_OK.

Needs protocol delegations on.  Driven by
scripts/nfs4_lease_cancel_test_wrapper.sh with NFS4_TEST_DELEGATIONS=true.
"""
import argparse
import os
import sys
import time

_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.0"))
sys.path.insert(1, os.path.join(_PYNFS, "nfs4.0", "lib"))

import nfs4lib                                          # noqa: E402
from nfs4lib import op4                                 # noqa: E402
from xdrdef.nfs4_const import *                         # noqa: E402
from xdrdef.nfs4_type import (                          # noqa: E402
    cb_client4, clientaddr4, nfs_client_id4)
import rpc.rpc as rpc                                   # noqa: E402

nfs4lib.SHOW_TRAFFIC = False


def name(status):
    return nfsstat4.get(status, str(status))


def check(label, ok, detail):
    print("    %-44s %s  (%s)" % (label, "ok" if ok else "FAIL", detail))
    return ok


class Run:
    def __init__(self, args):
        self.args = args
        self.tag = "t%d" % (os.getpid() % 100000)

    def client(self, who):
        sec = rpc.SecAuthSys(0, b"chimcbpath", 0, 0, [])
        return nfs4lib.NFS4Client(("cbpath-%s-%s" % (who, self.tag)).encode(),
                                  self.args.host, self.args.port,
                                  homedir=[self.args.export.encode()],
                                  sec_list=[sec])

    def path(self, fname):
        return [self.args.export.encode(),
                ("%s-%s" % (fname, self.tag)).encode()]


def renew(c, clientid):
    return c.compound([op4.renew(clientid)]).status


def register_without_program(c):
    """SETCLIENTID + SETCLIENTID_CONFIRM with callback program zero and no
    callback address, under c's own id and verifier.  Returns the clientid."""
    callback = cb_client4(0, clientaddr4(b"tcp", b"0.0.0.0.0.0"))
    res = c.compound([op4.setclientid(nfs_client_id4(c.verifier, c.id),
                                      callback, 0)])
    nfs4lib.check_result(res)
    clientid = res.resarray[0].switch.switch.clientid
    confirm = res.resarray[0].switch.switch.setclientid_confirm
    nfs4lib.check_result(c.compound([op4.setclientid_confirm(clientid,
                                                             confirm)]))
    return clientid


def write_delegation(run, c, fname):
    """A write delegation on a fresh file.  The first eligible OPEN of a 4.0
    client only kicks the CB_NULL probe of its callback path, so open another
    file first and give the probe a moment.  Returns the delegation type."""
    res = c.create_file(b"owner-kick", run.path(fname + "-kick"),
                        access=OPEN4_SHARE_ACCESS_BOTH,
                        deny=OPEN4_SHARE_DENY_NONE)
    c.confirm(b"owner-kick", res)
    for _ in range(20):
        time.sleep(0.25)
        res = c.create_file(b"owner-1", run.path(fname),
                            access=OPEN4_SHARE_ACCESS_BOTH,
                            deny=OPEN4_SHARE_DENY_NONE)
        c.confirm(b"owner-1", res)
        dtype = res.resarray[-2].switch.switch.delegation.delegation_type
        if dtype != OPEN_DELEGATE_NONE:
            return dtype
        fname += "x"
    return OPEN_DELEGATE_NONE


def main():
    p = argparse.ArgumentParser(
        description="NFSv4.0 RENEW callback-path regression test")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", default="share",
                   help="export name without the leading slash")
    args = p.parse_args()
    run = Run(args)
    print("NFSv4.0 RENEW callback-path test against %s:%d /%s   (run tag %s)"
          % (args.host, args.port, args.export, run.tag))

    results = []

    print("\n[1] path up")
    a = run.client("A")
    clientid, _ = a.init_connection(cb_ident=0)
    dtype = write_delegation(run, a, "held-A")
    ok = check("write delegation granted", dtype == OPEN_DELEGATE_WRITE,
               "type %d" % dtype)
    st = renew(a, clientid)
    ok &= check("RENEW with the path up", st == NFS4_OK, name(st))
    results.append(("path up", ok))

    print("\n[2] program zero")
    new_clientid = register_without_program(a)
    ok = check("callback update keeps the clientid",
               new_clientid == clientid,
               "%#x -> %#x" % (clientid, new_clientid))
    st = renew(a, clientid)
    ok &= check("RENEW holding a delegation, program zero",
                st == NFS4ERR_CB_PATH_DOWN, name(st))
    results.append(("program zero", ok))

    print("\n[3] no delegations")
    b = run.client("B")
    b_clientid = register_without_program(b)
    st = renew(b, b_clientid)
    ok = check("RENEW with no delegation, program zero", st == NFS4_OK,
               name(st))
    results.append(("no delegations", ok))

    print("\n=== verdict ===")
    for label, ok in results:
        print("  %-18s : %s" % (label, "PASS" if ok else "FAIL"))
    passed = all(ok for _, ok in results)
    print("  overall %s" % ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
