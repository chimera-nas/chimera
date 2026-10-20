#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""
NFSv4.1 presented-stateid validation: seqid enforcement and error class.

RFC 8881 8.2.2 requires a stateid-modifying operation to compare the
presented seqid with the state's current one: an older seqid is
NFS4ERR_OLD_STATEID, a never-issued one NFS4ERR_BAD_STATEID, and zero means
"use the most recent".  Under 4.1 OPEN-upgrade pipelining a LOCK, LOCKU or
OPEN_DOWNGRADE built against a stateid a concurrent OPEN has since superseded
must be refused rather than acting on stale state.  (CLOSE enforcement is
part of the same rule but is deferred to a follow-up: rejecting a 4.1 CLOSE
reaches a separate latent async-completion bug in the pNFS/diskfs commit
path.)

Separately, NFS4ERR_STALE_STATEID is a 4.0-only code (RFC 8881 15.2 lists it
in no 4.1 operation's error set).  A 4.1+ client reads it as "the server
rebooted" and enters mount-wide lease recovery; since the server did not
reboot, reclaim fails and the client retries the op forever -- a livelock
that wedges the mount (~2.9M LOCK retries observed).  A wrong-epoch or
wrong-generation stateid must be NFS4ERR_BAD_STATEID on 4.1+.

Cases (one minorversion=1 session, each case on its own fresh file):

  old_seqid_lock    : LOCK (new lock-owner) citing a superseded open-stateid
                      seqid is OLD_STATEID; the current one is granted.
  old_seqid_locku   : LOCKU presenting a superseded lock-stateid seqid is
                      OLD_STATEID; the current one succeeds.
  old_seqid_downgrade : OPEN_DOWNGRADE presenting a superseded open-stateid
                      seqid is OLD_STATEID; the current one succeeds.
  stale_epoch_class : a wrong-epoch stateid (previous server instance) is
                      BAD_STATEID -- never STALE_STATEID -- on CLOSE, LOCK,
                      and WRITE.
  livelock_lock     : the degenerate recovery LOCK captured on the wire
                      (open_stateid seqid=0xffffffff, other=all-zero) is
                      BAD_STATEID, so the recovery loop can converge.

Needs pynfs (nfs4.1 tree) and root (AUTH_SYS uid 0 against a root-owned
export root).  Driven by scripts/nfs4_41_stateid_checks_test_wrapper.sh.
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
    CLAIM_NULL, GUARDED4, NFS4_OK, NFS4ERR_BAD_STATEID, NFS4ERR_OLD_STATEID,
    OPEN4_CREATE, OPEN4_NOCREATE, OPEN4_SHARE_ACCESS_BOTH,
    OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
    OPEN4_SHARE_DENY_NONE, FATTR4_MODE, WRITE_LT, UNSTABLE4)
from xdrdef.nfs4_type import (                          # noqa: E402
    createhow4, exist_lock_owner4, lock_owner4, locker4, open_claim4,
    open_owner4, open_to_lock_owner4, openflag4, stateid4)

op = nfs_ops.NFS4ops()

# Unique run tag so a rerun against a dirty export never collides.
RUN = b"%d_%d" % (os.getpid(), int(time.time()))


def fatal(case, msg):
    print("FAIL: %s: %s" % (case, msg))
    sys.exit(1)


def expect(case, what, got, want):
    if got != want:
        fatal(case, "%s: expected %d got %d" % (what, want, got))
    print("ok: %s: %s -> %d" % (case, what, got))


class Harness(object):
    def __init__(self, host, port, export):
        self.client = nfs4client.NFS4Client(host.encode(), port,
                                            minorversion=1)
        self.client.set_cred(rpc.security.AuthSys().init_cred(
            uid=0, gid=0, name=b"sidchecks"))
        self.sess = self.client.new_client_session(b"sidchecks_" + RUN)
        res = self.sess.compound([op.putrootfh(),
                                  op.lookup(export.encode()), op.getfh()])
        if res.status != NFS4_OK:
            fatal("setup", "export lookup failed with %d" % res.status)
        self.dirfh = res.resarray[-1].object

    def open_file(self, case, owner, name, access, create):
        """OPEN name under the export root; returns (fh, open_stateid)."""
        if create:
            how = openflag4(OPEN4_CREATE,
                            createhow4(GUARDED4, {FATTR4_MODE: 0o644}))
        else:
            how = openflag4(OPEN4_NOCREATE)
        res = self.sess.compound([
            op.putfh(self.dirfh),
            op.open(0, access | OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                    OPEN4_SHARE_DENY_NONE, open_owner4(0, owner),
                    how, open_claim4(CLAIM_NULL, name)),
            op.getfh(),
        ])
        if res.status != NFS4_OK:
            fatal(case, "OPEN %s failed with %d" % (name, res.status))
        return res.resarray[-1].object, res.resarray[-2].stateid

    def open_advance(self, case, tag):
        """Create a file and supersede its open stateid once: OPEN(create,
        rw) publishes seqid 1, a second OPEN by the same owner republishes
        the same slot with seqid 2.  Returns (fh, sid1, sid2)."""
        owner = b"owner_%s_%s" % (tag, RUN)
        name = b"f_%s_%s" % (tag, RUN)
        fh, sid1 = self.open_file(case, owner, name,
                                  OPEN4_SHARE_ACCESS_BOTH, create=True)
        if sid1.seqid != 1:
            fatal(case, "fresh open stateid seqid %d, expected 1" %
                  sid1.seqid)
        fh2, sid2 = self.open_file(case, owner, name,
                                   OPEN4_SHARE_ACCESS_READ, create=False)
        if sid2.other != sid1.other or sid2.seqid != 2:
            fatal(case, "re-OPEN did not republish the slot (seqid %d)" %
                  sid2.seqid)
        return fh, sid1, sid2

    def close(self, fh, sid):
        return self.sess.compound([op.putfh(fh), op.close(0, sid)])

    def lock_new_owner(self, fh, open_sid, lockowner, off=0, length=100):
        otl = open_to_lock_owner4(0, open_sid, 0, lock_owner4(0, lockowner))
        return self.sess.compound([
            op.putfh(fh),
            op.lock(WRITE_LT, False, off, length,
                    locker4(new_lock_owner=True, open_owner=otl)),
        ])

    def write(self, fh, sid, data):
        return self.sess.compound([op.putfh(fh),
                                   op.write(sid, 0, UNSTABLE4, data)])


def stale_epoch(sid):
    """The same slot as minted by a previous server instance: flip the top
    epoch byte (stateid.other bytes 0-3 carry the server-instance epoch)."""
    other = bytes(sid.other)
    return stateid4(sid.seqid, bytes([other[0] ^ 0xff]) + other[1:])


def case_old_seqid_lock(h):
    case = "old_seqid_lock"
    fh, sid1, sid2 = h.open_advance(case, b"osl")
    lockowner = b"lo_%s" % RUN
    expect(case, "LOCK citing superseded open seqid 1",
           h.lock_new_owner(fh, sid1, lockowner).status,
           NFS4ERR_OLD_STATEID)
    res = h.lock_new_owner(fh, sid2, lockowner)
    expect(case, "LOCK citing current open seqid 2", res.status, NFS4_OK)
    return fh, res.resarray[-1].lock_stateid


def case_old_seqid_locku(h, fh, lock_sid1):
    case = "old_seqid_locku"
    if lock_sid1.seqid != 1:
        fatal(case, "fresh lock stateid seqid %d, expected 1" %
              lock_sid1.seqid)
    # A second LOCK under the existing lock stateid advances its seqid.
    res = h.sess.compound([
        op.putfh(fh),
        op.lock(WRITE_LT, False, 200, 50,
                locker4(new_lock_owner=False,
                        lock_owner=exist_lock_owner4(lock_sid1, 0))),
    ])
    if res.status != NFS4_OK:
        fatal(case, "second LOCK failed with %d" % res.status)
    lock_sid2 = res.resarray[-1].lock_stateid
    if lock_sid2.seqid != 2:
        fatal(case, "second LOCK stateid seqid %d, expected 2" %
              lock_sid2.seqid)
    expect(case, "LOCKU with superseded lock seqid 1",
           h.sess.compound([op.putfh(fh),
                            op.locku(WRITE_LT, 0, lock_sid1, 200, 50)]
                           ).status, NFS4ERR_OLD_STATEID)
    expect(case, "LOCKU with current lock seqid 2",
           h.sess.compound([op.putfh(fh),
                            op.locku(WRITE_LT, 0, lock_sid2, 200, 50)]
                           ).status, NFS4_OK)


def case_stale_epoch_class(h):
    case = "stale_epoch_class"
    owner = b"owner_sec_%s" % RUN
    fh, sid = h.open_file(case, owner, b"f_sec_%s" % RUN,
                          OPEN4_SHARE_ACCESS_BOTH, create=True)
    forged = stale_epoch(sid)
    expect(case, "CLOSE with wrong-epoch stateid",
           h.close(fh, forged).status, NFS4ERR_BAD_STATEID)
    expect(case, "LOCK citing wrong-epoch open stateid",
           h.lock_new_owner(fh, forged, b"lo_sec_%s" % RUN).status,
           NFS4ERR_BAD_STATEID)
    expect(case, "WRITE with wrong-epoch stateid",
           h.write(fh, forged, b"x").status, NFS4ERR_BAD_STATEID)
    expect(case, "cleanup CLOSE", h.close(fh, sid).status, NFS4_OK)


def case_livelock_lock(h):
    case = "livelock_lock"
    owner = b"owner_ll_%s" % RUN
    fh, sid = h.open_file(case, owner, b"f_ll_%s" % RUN,
                          OPEN4_SHARE_ACCESS_BOTH, create=True)
    # The exact LOCK the wedged client retried ~2.9M times: a new lock-owner
    # citing open_stateid {seqid=0xffffffff, other=all-zero}.
    degenerate = stateid4(0xffffffff, bytes(12))
    expect(case, "LOCK citing the degenerate recovery stateid",
           h.lock_new_owner(fh, degenerate, b"lo_ll_%s" % RUN).status,
           NFS4ERR_BAD_STATEID)
    expect(case, "cleanup CLOSE", h.close(fh, sid).status, NFS4_OK)


def case_old_seqid_downgrade(h):
    case = "old_seqid_downgrade"
    fh, sid1, sid2 = h.open_advance(case, b"osd")
    expect(case, "OPEN_DOWNGRADE with superseded seqid 1",
           h.sess.compound([op.putfh(fh),
                            op.open_downgrade(sid1, 0,
                                              OPEN4_SHARE_ACCESS_READ,
                                              OPEN4_SHARE_DENY_NONE)]
                           ).status, NFS4ERR_OLD_STATEID)
    res = h.sess.compound([op.putfh(fh),
                           op.open_downgrade(sid2, 0,
                                             OPEN4_SHARE_ACCESS_READ,
                                             OPEN4_SHARE_DENY_NONE)])
    expect(case, "OPEN_DOWNGRADE with current seqid 2", res.status, NFS4_OK)
    expect(case, "cleanup CLOSE",
           h.close(fh, res.resarray[-1].open_stateid).status, NFS4_OK)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2049)
    ap.add_argument("--export", default="share")
    args = ap.parse_args()

    h = Harness(args.host, args.port, args.export)

    fh, lock_sid = case_old_seqid_lock(h)
    case_old_seqid_locku(h, fh, lock_sid)
    case_old_seqid_downgrade(h)
    case_stale_epoch_class(h)
    case_livelock_lock(h)

    print("PASS: nfs4.1 stateid seqid enforcement and error class")
    return 0


if __name__ == "__main__":
    sys.exit(main())
