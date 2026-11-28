#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
End-to-end checks for the NFSv4 client ID allocator against a real daemon,
driven by scripts/nfs4_clientid_allocator_test_wrapper.sh (one scenario per
daemon start):

  default    no supervisor grant and no allocator configuration: NFSv4 is
             not offered (fail closed), rather than minting IDs nobody
             granted.
  type1      CHIMERA_NODE_ID/INSTANCE/GENERATION set by a "supervisor": the
             first client ID of the incarnation is exactly
             Type 1 / node / instance / generation / sequence 1.
  notoffered an incomplete supervisor grant: NFSv4 is not offered at all
             (the NFSv4 program is not registered), rather than registering
             clients it cannot give IDs to.
  coalesce   the "test" backend delays every allocation: an identical
             SETCLIENTID that arrives on another connection while the first
             is pending (a retransmission after a reconnect) gets the SAME
             client ID and confirm verifier, with no NFS4ERR_DELAY; a
             SETCLIENTID for the same owner with a different verifier gets
             NFS4ERR_DELAY; once the first completes, the RFC cases apply
             unchanged.
  disconnect the "test" backend delays every allocation: a client sends a
             SETCLIENTID and an identical retransmission (a coalesced waiter)
             on two connections and is killed while the allocation is
             pending.  The server finishes the registration with nobody to
             answer, then serves the same owner and others normally.
  hang       helper for the wrapper's shutdown check: send a SETCLIENTID
             that the delayed allocator holds, and block on the reply.

Every scenario also asks the built-in portmapper for NFS version 4: it is
advertised (port 2049) only when NFSv4 is offered.

This test decodes client IDs only to check the layout from outside; the
server never does.
"""
import argparse
import os
import socket
import struct
import sys
import threading
import time

_PYNFS = os.environ.get("PYNFS_DIR", "/opt/pynfs")
sys.path.insert(0, os.path.join(_PYNFS, "nfs4.0"))
sys.path.insert(1, os.path.join(_PYNFS, "nfs4.0", "lib"))

import nfs4lib                                              # noqa: E402
from nfs4lib import op4                                     # noqa: E402
from xdrdef.nfs4_const import *                             # noqa: E402
from xdrdef.nfs4_type import (nfs_client_id4, cb_client4,   # noqa: E402
                              clientaddr4)
import rpc.rpc as rpc                                       # noqa: E402

nfs4lib.SHOW_TRAFFIC = False
FAILED = []


def check(label, ok, detail=""):
    print("    %-52s %s  %s" % (label, "ok" if ok else "FAIL", detail))
    if not ok:
        FAILED.append(label)


def new_client(args, who):
    sec = rpc.SecAuthSys(0, b"chimalloc", 0, 0, [])
    return nfs4lib.NFS4Client(("alloc-%s-%d" % (who, os.getpid())).encode(),
                              args.host, args.port,
                              homedir=[args.export.encode()], sec_list=[sec])


def setclientid_op(owner, verifier):
    # Fixed callback arguments, so two connections send byte-identical
    # SETCLIENTID arguments (pynfs gives each client its own cb program).
    cb = cb_client4(0x40000000, clientaddr4(b"tcp", b"0.0.0.0.0.0"))
    return op4.setclientid(nfs_client_id4(verifier, owner), cb, 0)


def compound_once(c, ops):
    """One COMPOUND, without pynfs's automatic retry on NFS4ERR_DELAY (which
    is what a real client does too, so the plain compound() would hide it)."""
    from xdrdef.nfs4_type import COMPOUND4args
    p, un_p = c.nfs4packer, c.nfs4unpacker
    p.reset()
    p.pack_COMPOUND4args(COMPOUND4args(argarray=ops, tag=c.create_tag(),
                                       minorversion=0))
    un_p.reset(c.call(NFSPROC4_COMPOUND, p.get_buffer()))
    res = un_p.unpack_COMPOUND4res()
    un_p.done()
    return res


def setclientid(c, owner, verifier):
    res = compound_once(c, [setclientid_op(owner, verifier)])
    if res.status != NFS4_OK:
        return res.status, None, None
    ok = res.resarray[0].switch.switch
    return NFS4_OK, ok.clientid, ok.setclientid_confirm


def portmap_getport(host, prog, vers, prot=6, pmport=111):
    """PMAPPROC_GETPORT (portmap v2) over TCP; returns the port, 0 if the
    program/version is not registered."""
    xid = os.getpid() & 0xffffffff
    body = struct.pack(">IIIIII", xid, 0, 2, 100000, 2, 3)   # CALL, rpcvers 2
    body += struct.pack(">IIII", 0, 0, 0, 0)                  # AUTH_NULL x2
    body += struct.pack(">IIII", prog, vers, prot, 0)
    with socket.create_connection((host, pmport), timeout=10) as so:
        so.sendall(struct.pack(">I", 0x80000000 | len(body)) + body)
        hdr = b""
        while len(hdr) < 4:
            hdr += so.recv(4 - len(hdr))
        n = struct.unpack(">I", hdr)[0] & 0x7fffffff
        reply = b""
        while len(reply) < n:
            chunk = so.recv(n - len(reply))
            if not chunk:
                break
            reply += chunk
    # xid, REPLY, MSG_ACCEPTED, verf(flavor, len=0), SUCCESS, port
    rxid, mtype, stat, _vf, _vl, astat, port = struct.unpack(">IIIIIII", reply[:28])
    if rxid != xid or mtype != 1 or stat != 0 or astat != 0:
        raise RuntimeError("bad portmap reply")
    return port


def check_portmap(args, offered):
    try:
        port = portmap_getport(args.host, 100003, 4)
    except Exception as e:
        check("portmapper reachable", False, repr(e))
        return
    if offered:
        check("portmapper advertises NFS v4", port == 2049, "port %d" % port)
    else:
        check("portmapper does not advertise NFS v4", port == 0, "port %d" % port)
    check("portmapper still advertises NFS v3",
          portmap_getport(args.host, 100003, 3) == 2049)


def fields(cid):
    return {"type": cid >> 60, "version": (cid >> 58) & 3}


def scenario_default(args):
    scenario_notoffered(args)


def scenario_type1(args):
    a = new_client(args, "A")
    cid, _ = a.init_connection()
    expect = int(args.expect, 0)
    print("client ID: %#x (expected %#x)" % (cid, expect))
    check("first ID is Type 1 / node / instance / generation / seq 1",
          cid == expect, "%#x" % cid)
    b = new_client(args, "B")
    cid2, _ = b.init_connection()
    check("second ID is the next sequence", cid2 == expect + 1, "%#x" % cid2)
    check_portmap(args, True)


def scenario_notoffered(args):
    try:
        a = new_client(args, "A")
        a.null()
        a.compound([op4.renew(1)])
        check("NFSv4 not offered", False, "an NFSv4 call succeeded")
    except Exception as e:  # RPC version/program mismatch
        check("NFSv4 not offered", True, type(e).__name__)
    check_portmap(args, False)


def scenario_coalesce(args):
    owner = ("alloc-coalesce-%d" % os.getpid()).encode()
    verifier = b"verif001"
    conns = [new_client(args, "C%d" % i) for i in range(3)]
    results = {}

    def run(key, c, verf, delay):
        time.sleep(delay)
        t0 = time.time()
        results[key] = setclientid(c, owner, verf) + (time.time() - t0,)

    threads = [
        threading.Thread(target=run, args=("original", conns[0], verifier, 0.0)),
        threading.Thread(target=run, args=("retransmit", conns[1], verifier, 0.15)),
        threading.Thread(target=run, args=("different", conns[2], b"verif002", 0.15)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    o, r, d = results["original"], results["retransmit"], results["different"]
    print("original   : %s %s %s %.2fs" % (o[0], o[1] and hex(o[1]), o[2] and o[2].hex(), o[3]))
    print("retransmit : %s %s %s %.2fs" % (r[0], r[1] and hex(r[1]), r[2] and r[2].hex(), r[3]))
    print("different  : %s %.2fs" % (d[0], d[3]))
    check("original succeeds after the allocator delay", o[0] == NFS4_OK and o[3] > 0.2)
    check("identical request while pending: same reply, no DELAY",
          r[0] == NFS4_OK and r[1] == o[1] and r[2] == o[2])
    check("different verifier while pending: NFS4ERR_DELAY",
          d[0] == NFS4ERR_DELAY, str(d[0]))

    # After completion nothing is coalesced: the RFC cases apply.  An
    # unconfirmed record with the same verifier is replaced (new ID).
    s, cid_after, conf_after = setclientid(conns[1], owner, verifier)
    check("after completion: RFC 7530 replacement (new ID)",
          s == NFS4_OK and cid_after not in (None, o[1]),
          "%s %s" % (s, cid_after and hex(cid_after)))
    if s == NFS4_OK:
        s = conns[1].compound([op4.setclientid_confirm(cid_after, conf_after)]).status
        check("and it confirms", s == NFS4_OK, str(s))


def _hang_requests(args, owner, verifier):
    """Send a SETCLIENTID and an identical one on a second connection, both
    pending in the delayed allocator; block until killed."""
    conns = [new_client(args, "hang%d" % i) for i in range(2)]
    for c in conns:
        c.null()
    ts = [threading.Thread(target=setclientid, args=(c, owner, verifier), daemon=True)
          for c in conns]
    for t in ts:
        t.start()
        time.sleep(0.05)
    print("hang: requests sent", flush=True)
    for t in ts:
        t.join()


def scenario_hang(args):
    _hang_requests(args, ("alloc-hang-%d" % os.getpid()).encode(), b"verifHNG")


def scenario_disconnect(args):
    import subprocess

    owner = ("alloc-disc-%d" % os.getpid()).encode()
    verifier = b"verifDSC"
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import sys; sys.argv=['x']; sys.path[:0]=%r; "
         "import importlib.util as u; "
         "s=u.spec_from_file_location('e2e', %r); m=u.module_from_spec(s); "
         "s.loader.exec_module(m); "
         "import argparse; a=argparse.Namespace(host=%r, port=%d, export=%r); "
         "m._hang_requests(a, %r, %r)"
         % (sys.path[:2], os.path.abspath(__file__), args.host, args.port,
            args.export, owner, verifier)],
        stdout=subprocess.PIPE)
    line = child.stdout.readline()
    check("pending requests sent", b"requests sent" in line, line.decode().strip())
    time.sleep(0.1)
    child.kill()        # both connections close while the allocation waits
    child.wait()

    # The allocator delay (400 ms) runs out with the requests' connections
    # gone; give it time, then the owner must be free again.
    time.sleep(1.0)
    c = new_client(args, "after")
    c.null()
    s, cid, conf = setclientid(c, owner, verifier)
    check("same owner after the disconnect: served", s == NFS4_OK, str(s))
    if s == NFS4_OK:
        s = c.compound([op4.setclientid_confirm(cid, conf)]).status
        check("and it confirms", s == NFS4_OK, str(s))
    d = new_client(args, "other")
    cid2, _ = d.init_connection()
    check("another client registers", cid2 is not None and cid2 != cid,
          cid2 and hex(cid2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2049)
    ap.add_argument("--export", default="share")
    ap.add_argument("--scenario", required=True,
                    choices=["default", "type1", "notoffered", "coalesce",
                             "disconnect", "hang"])
    ap.add_argument("--expect", default="0")
    args = ap.parse_args()

    print("scenario %s" % args.scenario)
    globals()["scenario_" + args.scenario](args)
    if FAILED:
        print("FAILED: %s" % ", ".join(FAILED))
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
