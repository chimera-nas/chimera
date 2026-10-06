#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Negotiated wire limits must stop mutations and preserve cached errors."""
import argparse
import os
import re
import time
from pathlib import Path

from nfs4_compound_boundaries import Probe, op, require, channel_attrs4, CURRENT, ANONYMOUS
from nfs4lib import FancyNFS4Packer
from xdrdef.nfs4_const import *


def encoded(result):
    packer = FancyNFS4Packer()
    packer.pack_COMPOUND4res(result)
    return packer.get_buffer()


def call(session, operations, tag=b"wire", cache=False, expected=NFS4_OK):
    kwargs = dict(tag=tag, cache_this=cache)
    slot, sequence = session._prepare_compound(kwargs)
    args = [sequence] + operations
    result = session.c.compound(args, **kwargs)
    session.update_seq_state(result, slot)
    require(result.status == expected, f"{tag}: expected {nfsstat4[expected]}, got {result!r}")
    require(24 + len(encoded(result)) <= session.fore_channel.maxresponsesize,
            "reply exceeded negotiated limit including RPC header")
    return result, args, kwargs


def open_replies(p):
    normal = p.session
    name, fh, sid = p.create("retry-open-reply")
    p.call([op.putfh(fh), op.close(0, sid)])
    p.mark_closed(fh)
    enabled = os.environ.get("CHIMERA_COMPOUND_FEATURE") == "delegation"

    def opening(want):
        operation = p.open_op(name, create=False, owner=b"reply-owner")
        operation.opopen.share_access = OPEN4_SHARE_ACCESS_BOTH | want
        return operation

    def release_grant(result):
        delegation = result.delegation
        if delegation.delegation_type == OPEN_DELEGATE_WRITE:
            p.call([op.putfh(fh), op.delegreturn(delegation.write.stateid)])
            return True
        require(delegation.delegation_type in (OPEN_DELEGATE_NONE, OPEN_DELEGATE_NONE_EXT),
                "unexpected delegation type")
        return False

    # Prove that tight-budget declines are exercised with an otherwise usable
    # callback path, and that generous replies still receive real grants.
    if enabled:
        for attempt in range(10):
            result, _, _ = call(normal, [op.putfh(p.directory), opening(OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG)])
            granted = release_grant(result.resarray[2])
            p.call([op.putfh(fh), op.close(0, result.resarray[2].stateid)])
            if granted:
                break
            time.sleep(0.05)
        require(granted, "could not warm OPEN delegation callback path")

    # A later CLOSE, including one in a successful prefix, must not erase the
    # earlier OPEN's WANT-aware response. This also runs through finish retry.
    for want, reason in ((OPEN4_SHARE_ACCESS_WANT_NO_DELEG, WND4_NOT_WANTED),
                         (OPEN4_SHARE_ACCESS_WANT_CANCEL, WND4_CANCELLED)):
        for failed_suffix in (False, True):
            operations = [op.putfh(p.directory), opening(want), op.close(0, CURRENT)]
            if failed_suffix:
                operations += [op.lookup(b"not-a-directory"), op.write(ANONYMOUS, 0, FILE_SYNC4, b"BAD")]
            result, args, kwargs = call(normal, operations, cache=True,
                                        expected=NFS4ERR_NOTDIR if failed_suffix else NFS4_OK)
            delegation = result.resarray[2].delegation
            require(delegation.delegation_type == OPEN_DELEGATE_NONE_EXT and
                    delegation.od_whynone.ond_why == reason,
                    "coalesced CLOSE lost OPEN's delegation decline or reason")
            require(len(result.resarray) == (5 if failed_suffix else 4), "failed suffix executed its WRITE")
            require(encoded(normal.c.compound(args, **kwargs)) == encoded(result),
                    "cached OPEN/CLOSE replay changed its delegation decline")

    attrs = channel_attrs4(0, 1024 * 1024, 256, 256, 128, 8, [])
    small = normal.client.create_session(fore_attrs=attrs)
    attrs.ca_maxresponsesize = 1024 * 1024
    cached = normal.client.create_session(fore_attrs=attrs)
    for session, cache in ((small, False), (cached, True)):
        for want in (OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE, OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
                     OPEN4_SHARE_ACCESS_WANT_CANCEL, OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG):
            for close in (False, True):
                operations = [op.putfh(p.directory), opening(want), op.getattr(1 << FATTR4_SIZE)]
                if close:
                    operations.append(op.close(0, CURRENT))
                result, args, kwargs = call(session, operations, cache=cache)
                require(24 + len(encoded(result)) <= 256, "optional delegation exceeded the reply/cache budget")
                delegation = result.resarray[2].delegation
                expected_type = OPEN_DELEGATE_NONE_EXT if want else OPEN_DELEGATE_NONE
                require(delegation.delegation_type == expected_type,
                        "fitting OPEN did not preserve the required decline form")
                require(result.resarray[3].obj_attributes[FATTR4_SIZE] == 0,
                        "OPEN reply handling allowed a rejected WRITE suffix")
                if cache:
                    require(encoded(session.c.compound(args, **kwargs)) == encoded(result),
                            "cached tight-budget OPEN replay changed")
                if not close:
                    p.call([op.putfh(fh), op.close(0, result.resarray[2].stateid)])
        # Two OPEN replies share the budget and the final CLOSE retires both
        # uses of this owner. An optional grant must not displace either one.
        call(session, [op.putfh(p.directory), opening(OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG),
                       op.putfh(p.directory), opening(OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG),
                       op.close(0, CURRENT)], cache=cache)
        # Admission must also leave room for a real mutation after OPEN.
        result, _, _ = call(session, [op.putfh(p.directory), opening(OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE),
                                      op.write(CURRENT, 0, FILE_SYNC4, b"ok"), op.close(0, CURRENT)], cache=cache)
        require(result.resarray[3].count == 2 and p.read(fh, ANONYMOUS) == b"ok",
                "optional delegation reservation prevented a fitting WRITE suffix")
        p.call([op.putfh(fh), op.setattr(ANONYMOUS, {FATTR4_SIZE: 0})])
    print("PASS OPEN reply limits, optional grants, CLOSE declines, failed suffixes and replay", flush=True)


def exercise(p):
    normal = p.session
    _, fh, sid = p.create("wire-budget")
    original = b"x" * 2048
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, original)])

    def session(response, cached):
        attrs = channel_attrs4(0, 1024 * 1024, response, cached, 256, 8, [])
        return normal.client.create_session(fore_attrs=attrs)

    small = session(1024, 1024)
    operations = [op.putfh(fh), op.read(sid, 0, 2048), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result, _, _ = call(small, operations, expected=NFS4ERR_REP_TOO_BIG)
    require(len(result.resarray) == 3 and result.resarray[-1].resop == OP_READ,
            "oversized READ failed after its WRITE successor")
    require(p.read(fh, sid) == original, "oversized READ allowed WRITE")

    # Exact encoded size, including RPC header and odd payload padding. A
    # terminal WRITE needs 24 bytes; its failure needs only eight.
    base, _, _ = call(normal, [op.putfh(fh), op.read(sid, 0, 0),
                             op.write(sid, 0, FILE_SYNC4, b"x")])
    fixed = 24 + len(encoded(base))
    fitting = 1024 - fixed
    for count in (fitting, fitting - 1):
        result, _, _ = call(small, [op.putfh(fh), op.read(sid, 0, count),
                                    op.write(sid, 0, FILE_SYNC4, b"x")])
        require(result.resarray[2].data == original[:count], "exact-fit READ data changed")
        require(24 + len(encoded(result)) == 1024, "exact-fit reply did not reach its limit")
    result, _, _ = call(small, [op.putfh(fh), op.read(sid, 0, fitting + 16),
                                op.write(sid, 0, FILE_SYNC4, b"BAD")], expected=NFS4ERR_REP_TOO_BIG)
    require(len(result.resarray) == 4 and result.resarray[-1].resop == OP_WRITE,
            "a fitting READ did not preserve space for WRITE's size error")
    require(p.read(fh, sid) == original, "WRITE executed without room for its result")

    # A terminal SETATTR error includes an empty attribute bitmap.
    result, _, _ = call(small, [op.putfh(fh), op.read(sid, 0, fitting + 12),
                                op.setattr(sid, {FATTR4_SIZE: 0})], expected=NFS4ERR_REP_TOO_BIG)
    require(result.resarray[-1].resop == OP_SETATTR and result.resarray[-1].attrsset == 0,
            "SETATTR error bitmap was not initialized")
    require(p.read(fh, sid) == original, "SETATTR truncated the file before size admission")

    # Actual short reads fit even when their requested count would not.
    result, _, _ = call(small, [op.putfh(fh), op.read(sid, len(original) - 3, 1048576),
                                op.getattr(1 << FATTR4_SIZE)])
    require(result.resarray[2].data == b"xxx", "budget used READ capacity instead of actual bytes")
    result, _, _ = call(small, [op.putfh(fh)] + [op.getfh() for _ in range(12)])
    require(all(entry.object == fh for entry in result.resarray[2:]),
            "small filehandles were charged at maximum capacity")

    operations = [op.putfh(fh), op.read(sid, 0, 600), op.backchannel_ctl(0x40000000, []),
                  op.read(sid, 0, 600), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result, _, _ = call(small, operations, expected=NFS4ERR_REP_TOO_BIG)
    require(len(result.resarray) == 5 and result.resarray[-1].resop == OP_READ,
            "administrative boundary reset the reply budget")
    require(p.read(fh, sid) == original, "split response overflow allowed WRITE")

    cached = session(8192, 1024)
    # The accepted WRITE prefix must be replayed along with the size error.
    operations = [op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"OLD"),
                  op.read(sid, 0, 2048), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result, args, kwargs = call(cached, operations, cache=True, expected=NFS4ERR_REP_TOO_BIG_TO_CACHE)
    require(len(result.resarray) == 4 and result.resarray[-1].resop == OP_READ,
            "cache limit did not stop at READ")
    require(24 + len(encoded(result)) <= 1024, "size-error reply exceeded cache limit")
    require(p.read(fh, sid) == b"OLD" + original[3:], "successful prefix or failed suffix changed")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"NEW")])
    replay = cached.c.compound(args, **kwargs)
    require(encoded(replay) == encoded(result), "size error was not replayed byte-for-byte")
    require(p.read(fh, sid) == b"NEW" + original[3:], "cached request reexecuted its WRITE prefix")

    # Cache=false does not impose the cached-size limit, and zero cached size
    # rejects SEQUENCE without advancing the slot when caching is requested.
    call(cached, [op.putfh(fh), op.read(sid, 0, 2048)])
    zero = session(8192, 0)
    result, _, _ = call(zero, [], cache=True, expected=NFS4ERR_REP_TOO_BIG_TO_CACHE)
    require(result.resarray[0].sr_status == NFS4ERR_REP_TOO_BIG_TO_CACHE,
            "zero cache capacity was treated as unlimited")
    call(zero, [op.putfh(fh)])

    # GETATTR, READDIR, READ_PLUS and xattrs have distinct staging paths.
    if p.args.minor >= 2:
        p.call([op.putfh(fh), op.setxattr(SETXATTR4_EITHER, b"large", b"a" * 2048)])
        for operation in (op.getxattr(b"large"), op.read_plus(sid, 0, 2048)):
            result, _, _ = call(small, [op.putfh(fh), operation, op.write(sid, 0, FILE_SYNC4, b"BAD")],
                                 expected=NFS4ERR_REP_TOO_BIG)
            require(result.resarray[-1].resop == operation.argop, "variable result failed too late")
    require(p.read(fh, sid) == b"NEW" + original[3:], "variable reply overflow allowed WRITE")
    print("PASS wire reply admission, padding, boundaries, mutation suppression and cached replay", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    p = Probe(parser.parse_args())
    try:
        open_replies(p)
        exercise(p)
        if os.environ.get("CHIMERA_WIRE_RETRY") == "retry":
            log = Path(p.args.server_log).read_text()
            rejected = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+)", log)
            accepted = re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=(\d+)", log)
            require(len(rejected) >= 2 and sorted(rejected) == sorted(accepted),
                    "reply budgets were not reset across finish retries")
            require(re.search(r"NFS4_FINISH_RETRY injected [^\n]* execution=[1-9]\d* [^\n]* reads=[1-9]", log),
                    "size-error READ prefix did not undergo finish retry")
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
