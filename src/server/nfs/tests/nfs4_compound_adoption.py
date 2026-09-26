#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Runtime namespace and proxy COPY boundaries, including exact wire spans.

The optional finish fixture rejects only read-only requests, including COPY
whose source offset is EOF and whose prepared length remains zero. Nonempty
COPY exercises owned payloads across multiple upstream transfers without
pretending that current backends can roll back those writes.
"""
import argparse
import os
import json
from pathlib import Path
import threading
import time
import urllib.request

from nfs4_compound_boundaries import Probe, op, require, ANONYMOUS, createtype4
from xdrdef.nfs4_const import *


def measured(p, operations, name, expected=NFS4_OK):
    return p.call(operations, name, expected, runs=[(1, len(operations))])


def write_all(p, fh, sid, data):
    for offset in range(0, len(data), 65536):
        piece = data[offset:offset + 65536]
        result = p.call([op.putfh(fh), op.write(sid, offset, FILE_SYNC4, piece)])
        require(result.resarray[-1].count == len(piece), "short fixture WRITE")


def read_all(p, fh, sid, length):
    result = bytearray()
    while len(result) < length:
        reply = p.call([op.putfh(fh), op.read(sid, len(result), min(32768, length - len(result)))])
        piece = reply.resarray[-1].data
        require(piece, "unexpected EOF while checking copied bytes")
        result.extend(piece)
    return bytes(result)


def update_namespace_during_finish(p, root, secinfo):
    gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
    if not gate:
        return
    name = b"retry-junction-secinfo" if secinfo else b"retry-junction-lookup"
    _, shadow, sid = p.create(name.decode())
    ready = Path(gate + ".ready")
    release = Path(gate + ".release")
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)
    failures = []

    def add_export():
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "finish fixture never reached namespace gate")
                time.sleep(0.01)
            body = json.dumps({"name": "/" + name.decode(), "path": "/share",
                               "access": "ro", "squash": "root",
                               "anonuid": 65534, "anongid": 65534}).encode()
            request = urllib.request.Request("http://127.0.0.1:8080/api/v1/exports", body,
                                             {"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(request, timeout=3) as response:
                require(response.status in (200, 201), "export update was not accepted")
        except BaseException as error:
            failures.append(error)
        finally:
            release.touch()

    worker = threading.Thread(target=add_export)
    worker.start()
    try:
        operation = op.secinfo(name) if secinfo else op.lookup(name)
        result = measured(p, [op.putfh(root), operation, op.getfh()],
                          "junction_update_secinfo" if secinfo else "junction_update_lookup", NFS4ERR_DELAY)
        require(len(result.resarray) == 2, "new junction did not stop the retried operation")
    finally:
        worker.join(timeout=12)
    require(not worker.is_alive(), "namespace updater did not finish")
    if failures:
        raise failures[0]
    # Fresh client execution takes the ordinary junction path and its export
    # policy. It must not expose the file underneath or keep the old write ACL.
    result = p.call([op.putfh(root), op.lookup(name), op.getfh(), op.getattr(1 << FATTR4_TYPE)])
    require(result.resarray[-2].object != shadow and result.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4DIR,
            "new junction exposed its shadowed backend file")
    p.call([op.putfh(root), op.lookup(name), p.open_op(b"forbidden-create")], expected=NFS4ERR_ROFS)


def namespace(p):
    sibling = p.directory
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    p.directory = root
    name, fh, sid = p.create("retry-root-ordinary")
    payload = b"ordinary name in a real namespace root"
    write_all(p, fh, sid, payload)
    result = measured(p, [op.putfh(root), op.lookup(name), op.read(sid, 0, 128), op.getfh()],
                      "root_nonjunction_lookup_read")
    require(result.resarray[-2].data == payload and result.resarray[-1].object == fh,
            "nonjunction LOOKUP changed the resolved object")

    # A name absent from the export table is safe even after a preceding LOOKUP
    # moves a private cursor. The failed second lookup must terminate the suffix.
    dirname = b"ordinary-directory"
    result = p.call([op.putfh(root), op.create(createtype4(NF4DIR), dirname, {FATTR4_MODE: 0o777}),
                     op.getfh()])
    p.directories.append(dirname)
    directory = result.resarray[-1].object
    result = measured(p, [op.putfh(root), op.lookup(dirname), op.lookup(b"ordinary-missing"), op.getfh()],
                      "root_nested_missing_prefix", NFS4ERR_NOENT)
    require(len(result.resarray) == 3, "failed nested lookup did not stop its GETFH suffix")
    result = measured(p, [op.putfh(directory), op.lookup(b"ordinary-missing"), op.getfh()],
                      "root_descendant_missing", NFS4ERR_NOENT)
    require(len(result.resarray) == 2, "missing descendant lookup ran a suffix")

    result = measured(p, [op.putfh(root), op.secinfo(name), op.putfh(fh), op.read(sid, 0, 128)],
                      "root_nonjunction_secinfo_read")
    require(result.resarray[-1].data == payload and result.resarray[1].resok4,
            "nonjunction SECINFO returned no flavors or lost continuation")
    result = measured(p, [op.putfh(root), op.secinfo(name), op.getfh()],
                      "root_nonjunction_secinfo_consumed", NFS4ERR_NOFILEHANDLE)
    require(len(result.resarray) == 3, "SECINFO did not consume the protocol current FH")

    # OPEN creates a real backend entry with the same name as the sibling
    # export. LOOKUP at the namespace root must still choose the junction.
    _, shadow, shadow_sid = p.create("share")
    require(shadow != sibling, "shadow fixture unexpectedly names sibling export")
    result = p.call([op.putfh(root), op.lookup(b"share"), op.getfh()])
    require(result.resarray[-1].object == sibling, "LOOKUP exposed the shadowed backend entry")
    result = p.call([op.putfh(root), op.secinfo(b"share"), op.getfh()], expected=NFS4ERR_NOFILEHANDLE)
    require(result.resarray[1].resok4, "junction SECINFO lost the sibling policy")
    result = p.call([op.putfh(sibling), op.lookupp(), op.getfh()])
    require(result.resarray[-1].object == root, "sibling LOOKUPP did not return namespace root")
    p.call([op.putfh(root), op.lookupp(), op.getfh()], expected=NFS4ERR_NOENT)
    update_namespace_during_finish(p, root, False)
    update_namespace_during_finish(p, root, True)
    print("PASS namespace junction shadowing, parent semantics, and nonjunction spans", flush=True)


def proxy(p):
    _, source, ss = p.create("proxy-copy-source")
    _, dest, ds = p.create("proxy-copy-destination")
    # More than two generic 256 KiB COPY hops, with unaligned endpoints.
    payload = bytes(range(251)) * 2500
    baseline = b"d" * (len(payload) + 128)
    write_all(p, source, ss, payload)
    write_all(p, dest, ds, baseline)
    prefix = [op.putfh(source), op.savefh(), op.putfh(dest)]
    count = len(payload) - 45
    expected = baseline[:31] + payload[17:17 + count] + baseline[31 + count:]
    result = measured(p, prefix + [op.copy(ss, ds, 17, 31, count, True, True, []),
                                    op.read(ds, 0, 1024), op.getfh()], "proxy_multihop_copy_read")
    require(result.resarray[3].cr_response.wr_count == count and
            result.resarray[-2].data == expected[:1024] and result.resarray[-1].object == dest,
            "COPY lost count, destination cursor, or immediate read result")
    require(read_all(p, dest, ds, len(expected)) == expected, "multihop proxy COPY corrupted data")
    require(read_all(p, source, ss, len(payload)) == payload, "COPY changed its source")

    result = measured(p, prefix + [op.copy(ANONYMOUS, ANONYMOUS, 0, 0, 0, True, True, []),
                                    op.getfh()], "proxy_anonymous_copy_to_eof")
    expected = payload + baseline[len(payload):]
    require(result.resarray[3].cr_response.wr_count == len(payload) and
            read_all(p, dest, ds, len(expected)) == expected,
            "anonymous zero-count COPY failed to copy through EOF")

    # These requests are genuinely read-only and can have finish rejected. The
    # first zero-count COPY prepares length=0 at EOF; the second is vetoed
    # before any transfer. Successful-prefix replies and suffix suppression
    # must survive retry, while nonempty COPY above is never rejected.
    result = measured(p, prefix + [op.copy(ss, ds, len(payload), 0, 0, True, True, []),
                                    op.read(ds, 0, 128), op.getfh()], "proxy_empty_copy_retry")
    require(result.resarray[3].cr_response.wr_count == 0 and
            result.resarray[-2].data == expected[:128] and result.resarray[-1].object == dest,
            "empty COPY retry returned incorrect data or cursor")
    result = measured(p, prefix + [op.copy(ss, ds, len(payload) + 1, 0, 0, True, True, []),
                                    op.getfh()], "proxy_invalid_copy_retry", NFS4ERR_INVAL)
    require(len(result.resarray) == 4, "invalid COPY ran a suffix")

    for side in ("source", "destination"):
        src_sid, dst_sid = (ds, ds) if side == "source" else (ss, ss)
        result = measured(p, prefix + [op.copy(src_sid, dst_sid, 0, 0, 128, True, True, []),
                                        op.getfh()], "proxy_bad_" + side, NFS4ERR_BAD_STATEID)
        require(len(result.resarray) == 4, "mismatched stateid allowed COPY suffix")
    result = measured(p, [op.putfh(source), op.savefh(), op.putfh(source),
                          op.copy(ss, ss, 0, 0, 32, True, True, []), op.getfh()],
                      "proxy_same_file", NFS4ERR_INVAL)
    require(len(result.resarray) == 4, "same-file COPY allowed its suffix")
    require(read_all(p, dest, ds, len(expected)) == expected and
            read_all(p, source, ss, len(payload)) == payload,
            "vetoed or zero-byte COPY changed a file")
    print("PASS proxy bounded COPY ownership, EOF, anonymous access, and stateid validation", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    args = parser.parse_args()
    p = Probe(args)
    try:
        if os.environ["CHIMERA_COMPOUND_FEATURE"] == "namespace":
            namespace(p)
        else:
            proxy(p)
        p.check_trace()
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
