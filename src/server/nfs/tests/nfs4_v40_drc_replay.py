#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
Regression test: the NFSv4.0 duplicate-request cache must not answer one
client's request with another's reply.

The cache now keys a COMPOUND by {connection, xid, checksum of the request}, so
these symptoms are unrepresentable rather than guarded against.  They were real:
601a8d03 keyed it by {client address with the port stripped, xid, checksum},
nothing in that key separated two connections from one host, and every fresh
pynfs NFS4Client restarts its xid at 0 (rpc.py, RPCClient.__init__), so
byte-identical compounds from different clients collided.  Two things went wrong:

  * a read-only COMPOUND (PUTFH+READDIR, PUTROOTFH+LOOKUP) was cached, so a
    fresh client was served an arbitrarily old directory listing; and
  * a CREATE from a second, concurrently live connection was answered NFS4_OK
    straight from the cache without ever executing -- a silently dropped
    mutation.

Four checks.  The cache is always installed (it is bounded by the open
connections), so no server config flag is involved:

  1. read-only replay : fresh connections issuing byte-identical READDIRs must
                        each see the current directory, not the first one's.
  2. control          : the same loop with the request bytes made unique, so
                        the DRC key cannot match.  Proves the harness itself is
                        sound -- if this fails, the server's view is the
                        problem, not the cache.
  3. silent data loss : a byte-identical CREATE from a second live connection
                        must really create.
  4. real retransmit  : the cache must still do its job -- re-sending one
                        connection's own CREATE at the same xid replays
                        NFS4_OK instead of failing NFS4ERR_EXIST.

Every request the test cares about carries an explicit tag and an explicitly
forced xid, so byte-identity between two clients is asserted by construction
rather than left to pynfs's tag heuristics.

Trap worth knowing before editing this: READDIR request bytes do not vary with
directory contents, so a fixed maxcount repeats across *runs* as well as within
one, and a leftover entry from the previous run can poison a later one.  Hence
the per-run tag on every created name and the per-run maxcount base -- without
them the test ends up testing the cache against itself.

Needs pynfs and root (the AUTH_SYS cred is uid 0 against a root-owned export
root).  Driven by scripts/nfs4_v40_drc_replay_test_wrapper.sh.
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
from nfs4lib import op4                                 # noqa: E402
from xdrdef.nfs4_const import *                         # noqa: E402
from xdrdef.nfs4_type import createtype4                # noqa: E402
import rpc.rpc as rpc                                   # noqa: E402

nfs4lib.SHOW_TRAFFIC = False

ATTRS = nfs4lib.list2bitmap([FATTR4_TYPE, FATTR4_FILEID])

# xid every "colliding" request is forced onto.  Fixed and shared, so two
# clients present identical xids by construction.
COLLIDE_XID = 4242

COUNTER = 0


def client(args):
    """A brand-new connection, which is the point: on the wire it is
    indistinguishable from the previous one apart from its source port, and the
    DRC key does not carry the port."""
    global COUNTER
    COUNTER += 1
    sec = rpc.SecAuthSys(0, b"chimdrc", 0, 0, [])
    c = nfs4lib.NFS4Client(("drc%d" % COUNTER).encode(), args.host, args.port,
                           homedir=[args.export.encode()], sec_list=[sec])
    c.null()
    return c


def compound_at(c, ops, tag, xid=None):
    """Issue a COMPOUND, optionally forcing the RPC xid.  get_new_xid()
    pre-increments, so seed one below the value we want."""
    if xid is not None:
        c.xid = xid - 1
    return c.compound(ops, tag=tag)


def readdir(c, fh, maxcount, tag, xid=None):
    """Raw paged READDIR so the request bytes are exactly under our control."""
    names, cookie, verf = [], 0, b"\0" * 8
    for _ in range(8):
        res = compound_at(c,
                          [op4.putfh(fh),
                           op4.readdir(cookie, verf, maxcount // 2, maxcount,
                                       ATTRS)],
                          tag,
                          # Only the first page needs the forced xid: it is the
                          # one that must collide with the other client's.
                          xid if cookie == 0 else None)
        if res.status != NFS4_OK:
            return "status=%d" % res.status
        rd = res.resarray[-1].opreaddir.resok4
        e = rd.reply.entries
        while e:
            names.append(e[0].name.decode())
            cookie = e[0].cookie
            e = e[0].nextentry
        verf = rd.cookieverf
        if rd.reply.eof:
            break
    return sorted(names)


def create_ops(args, name):
    return ([op4.putrootfh(), op4.lookup(args.export.encode())] +
            [op4.create(createtype4(NF4DIR), name, {FATTR4_MODE: 0o755})])


def lookup(c, args, name):
    return c.compound([op4.putrootfh(), op4.lookup(args.export.encode()),
                       op4.lookup(name)], tag=b"verify").status


def remove(c, args, name):
    return c.compound([op4.putrootfh(), op4.lookup(args.export.encode()),
                       op4.remove(name)], tag=b"cleanup").status


def test_readonly_replay(args, writer, tag):
    """Fresh connections issuing byte-identical READDIRs must not be served the
    first one's listing."""
    print("\n[1] read-only replay: byte-identical READDIR, fresh connections")
    root = [args.export.encode()]
    bad = 0
    for i in range(5):
        name = ("%s-r%d" % (tag, i)).encode()
        writer.create_obj(root + [name])
        cur = readdir(writer, writer.do_getfh(root), 4096, b"writer")
        b = client(args)
        # Same bytes and same xid every iteration -- exactly what used to
        # collide in the DRC.
        got = readdir(b, b.do_getfh(root), 4096, b"reader", COLLIDE_XID)
        ok = got == cur
        bad += not ok
        print("    iter %d  writer=%-24s fresh=%-24s match=%s"
              % (i, cur, got, ok))
        remove(writer, args, name)
    print("    -> disagreements: %d/5 (expected 0)" % bad)
    return bad == 0


def test_control(args, writer, tag, mc_base):
    """The same loop with unique request bytes: the DRC key cannot match, so a
    failure here means the server's view is wrong, not the cache."""
    print("\n[2] control: unique maxcount per READDIR (DRC key cannot match)")
    root = [args.export.encode()]
    bad = 0
    for i in range(5):
        name = ("%s-c%d" % (tag, i)).encode()
        writer.create_obj(root + [name])
        cur = readdir(writer, writer.do_getfh(root), mc_base + 900 + i * 41,
                      b"writer")
        b = client(args)
        # Unique per iteration AND per run, so neither this loop nor a previous
        # run of it can supply a matching cache entry.
        got = readdir(b, b.do_getfh(root), mc_base + i * 37, b"reader")
        ok = got == cur
        bad += not ok
        print("    iter %d  writer=%-24s fresh=%-24s match=%s"
              % (i, cur, got, ok))
        remove(writer, args, name)
    print("    -> disagreements: %d/5 (expected 0)" % bad)
    return bad == 0


def test_data_loss(args, verifier, tag):
    """A byte-identical CREATE from a second live connection must execute, not
    be answered from the first connection's cache entry."""
    print("\n[3] silent data loss: byte-identical CREATE, second connection")
    name = ("%s-victim" % tag).encode()
    ops = create_ops(args, name)

    remove(verifier, args, name)
    print("    start:            exists=%s"
          % (lookup(verifier, args, name) == NFS4_OK))

    c1 = client(args)
    st1 = compound_at(c1, ops, b"victim", COLLIDE_XID).status
    e1 = lookup(verifier, args, name) == NFS4_OK
    print("    client1 CREATE -> %d  exists=%s" % (st1, e1))

    remove(verifier, args, name)
    print("    after delete:     exists=%s"
          % (lookup(verifier, args, name) == NFS4_OK))

    # Brand-new connection, byte-identical request, identical xid.  c1 is still
    # open, so its cache entry must stay its own.
    c2 = client(args)
    st2 = compound_at(c2, ops, b"victim", COLLIDE_XID).status
    e2 = lookup(verifier, args, name) == NFS4_OK
    print("    client2 CREATE -> %d  exists=%s" % (st2, e2))
    remove(verifier, args, name)

    ok = (st1 == NFS4_OK and e1 and st2 == NFS4_OK and e2)
    if not ok and st2 == NFS4_OK and not e2:
        print("    -> FAIL: CREATE returned NFS4_OK but nothing was created")
    else:
        print("    -> %s" % ("ok: the second CREATE really executed" if ok
                             else "FAIL: unexpected statuses"))
    return ok


def test_real_retransmit(args, verifier, tag):
    """The cache must still replay a genuine retransmit: the SAME connection
    re-presenting its own request at the same xid gets the original reply, not
    NFS4ERR_EXIST from a second execution."""
    print("\n[4] real retransmit: same connection, same xid, replayed")
    name = ("%s-rexmit" % tag).encode()
    ops = create_ops(args, name)

    remove(verifier, args, name)
    c = client(args)
    first = compound_at(c, ops, b"rexmit", COLLIDE_XID + 1).status
    second = compound_at(c, ops, b"rexmit", COLLIDE_XID + 1).status
    print("    first=%d  retransmit=%d (want %d/%d, not %d)"
          % (first, second, NFS4_OK, NFS4_OK, NFS4ERR_EXIST))
    remove(verifier, args, name)

    ok = (first == NFS4_OK and second == NFS4_OK)
    print("    -> %s" % ("ok: the retransmit replayed" if ok else
                         "FAIL: the retransmit re-executed"))
    return ok


def main():
    p = argparse.ArgumentParser(
        description="NFSv4.0 DRC cross-client replay regression test")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", default="share",
                   help="export name without the leading slash")
    args = p.parse_args()

    # Unique per run so the WRITER's own requests can never collide with a
    # previous run's cache entries -- only the reader's deliberately-identical
    # ones do.
    tag = "t%d" % (os.getpid() % 100000)
    print("NFSv4.0 DRC replay test against %s:%d /%s   (run tag %s)"
          % (args.host, args.port, args.export, tag))

    # Long-lived and always advancing its xid from a high base, so the ground
    # truth these tests compare against can never itself be a replay.
    writer = client(args)
    writer.xid = 100000 + (os.getpid() % 1000) * 100
    verifier = client(args)
    verifier.xid = 500000 + (os.getpid() % 1000) * 100

    # Unique per run: a fixed maxcount would collide with the previous run's
    # control requests, since READDIR bytes do not vary with directory contents.
    mc_base = 5000 + (int(time.time()) % 811) * 3

    results = [
        ("read-only replay", test_readonly_replay(args, writer, tag)),
        ("control", test_control(args, writer, tag, mc_base)),
        ("no silent data loss", test_data_loss(args, verifier, tag)),
        ("retransmit still replays", test_real_retransmit(args, verifier, tag)),
    ]

    print("\n=== verdict ===")
    for name, ok in results:
        print("  %-26s : %s" % (name, "PASS" if ok else "FAIL"))
    passed = all(ok for _, ok in results)
    print("  overall %s" % ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
