#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
pNFS: a layout stateid presented to I/O on the metadata server is refused.

A layout stateid names layout state, not an open, so it designates no state
that READ, WRITE, READ_PLUS or WRITE_SAME can use and must be answered
NFS4ERR_BAD_STATEID.  Once layout stateids carry the state table's epoch the
generic stateid lookup resolves them, so a handler that treats every stateid
that is neither OPEN nor DELEG as a lock state would read the layout object as
a lock state and dereference a wild pointer.  This test sends each of the four
operations with a live layout stateid, checks the error, and then checks that
the server still serves I/O with the open stateid.

Runs over a minorversion 2 session (READ_PLUS and WRITE_SAME are 4.2
operations).  Needs a pNFS metadata server (scripts/pynfs_pnfs_test_wrapper.sh brings one
up with a data server) and pynfs's nfs4.1 client.
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
    CLAIM_NULL, FATTR4_MODE, GUARDED4, LAYOUT4_FLEX_FILES,
    LAYOUTIOMODE4_ANY, LAYOUTIOMODE4_RW, LAYOUTRETURN4_FILE, NFS4_MAXFILELEN,
    NFS4_OK, NFS4ERR_BAD_STATEID, NFS4_UINT64_MAX, OPEN4_CREATE,
    OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
    OPEN4_SHARE_DENY_NONE, UNSTABLE4)
from xdrdef.nfs4_type import (                          # noqa: E402
    app_data_block4, createhow4, ff_layoutreturn4, layoutreturn4,
    layoutreturn_file4, open_claim4, open_owner4, openflag4)
from xdrdef.nfs4_pack import NFS4Packer                 # noqa: E402

op = nfs_ops.NFS4ops()
RUN = b"%d_%d" % (os.getpid(), int(time.time()))


def fatal(msg):
    print("FAIL: %s" % msg)
    sys.exit(1)


def expect(what, got, want):
    if got != want:
        fatal("%s: expected %d got %d" % (what, want, got))
    print("ok: %s -> %d" % (what, got))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2049)
    ap.add_argument("--export", default="share")
    args = ap.parse_args()

    client = nfs4client.NFS4Client(args.host.encode(), args.port,
                                   minorversion=2)
    client.set_cred(rpc.security.AuthSys().init_cred(
        uid=0, gid=0, name=b"lostateid"))
    sess = client.new_pnfs_client_session(b"lostateid_" + RUN)

    name = b"f_lostateid_%s" % RUN
    res = sess.compound([
        op.putrootfh(), op.lookup(args.export.encode()),
        op.open(0, OPEN4_SHARE_ACCESS_BOTH | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                OPEN4_SHARE_DENY_NONE, open_owner4(0, b"oo_" + RUN),
                openflag4(OPEN4_CREATE,
                          createhow4(GUARDED4, {FATTR4_MODE: 0o644})),
                open_claim4(CLAIM_NULL, name)),
        op.getfh()])
    if res.status != NFS4_OK:
        fatal("OPEN failed with %d" % res.status)
    fh = res.resarray[-1].object
    open_sid = res.resarray[-2].stateid

    res = sess.compound([
        op.putfh(fh),
        op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                     0, NFS4_MAXFILELEN, 4096, open_sid, 0xffff)])
    if res.status != NFS4_OK:
        fatal("LAYOUTGET failed with %d" % res.status)
    lo_sid = res.resarray[-1].logr_stateid

    adb = app_data_block4(0, 512, 2, NFS4_UINT64_MAX, 0, 0, b"x")
    cases = [
        ("WRITE", op.write(lo_sid, 0, UNSTABLE4, b"data")),
        ("READ", op.read(lo_sid, 0, 4)),
        ("READ_PLUS", op.read_plus(lo_sid, 0, 4)),
        ("WRITE_SAME", op.write_same(lo_sid, UNSTABLE4, adb)),
    ]
    for what, io in cases:
        res = sess.compound([op.putfh(fh), io])
        expect("%s with a layout stateid" % what, res.status,
               NFS4ERR_BAD_STATEID)

    # The server must still be serving, and the open stateid still valid.
    res = sess.compound([op.putfh(fh),
                         op.write(open_sid, 0, UNSTABLE4, b"data")])
    expect("WRITE with the open stateid", res.status, NFS4_OK)
    res = sess.compound([op.putfh(fh), op.read(open_sid, 0, 4)])
    expect("READ with the open stateid", res.status, NFS4_OK)

    packer = NFS4Packer()
    packer.pack_ff_layoutreturn4(ff_layoutreturn4([], []))
    res = sess.compound([
        op.putfh(fh),
        op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                        layoutreturn4(LAYOUTRETURN4_FILE,
                                      layoutreturn_file4(
                                          0, NFS4_MAXFILELEN, lo_sid,
                                          packer.get_buffer())))])
    expect("LAYOUTRETURN", res.status, NFS4_OK)
    res = sess.compound([op.putfh(fh), op.close(0, open_sid)])
    expect("CLOSE", res.status, NFS4_OK)

    print("PASS: layout stateids are refused for I/O on the metadata server")
    return 0


if __name__ == "__main__":
    sys.exit(main())
