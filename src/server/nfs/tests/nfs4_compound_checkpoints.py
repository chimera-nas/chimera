#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Exact-span capacity, protocol and terminal checkpoints.

Run through the compound wrapper with checkpoints or pnfs_checkpoints. The
optional finish fixture rejects only attempts without executed backend mutation.
"""
import argparse
import os
from pathlib import Path
import threading
import time

from nfs4_compound_boundaries import Probe, op, require, ANONYMOUS, createtype4
from xdrdef.nfs4_const import *
from xdrdef.nfs4_pack import NFS4Packer
from xdrdef.nfs4_type import io_info4, layoutupdate4


def measured(p, operations, name, expected=NFS4_OK, count=None):
    return p.call(operations, name, expected, runs=[(1, len(operations) if count is None else count)])


def capacity(p):
    _, fh, sid = p.create("checkpoint-capacity")
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"capacity")])
    operations = [op.putfh(fh)] + [op.getattr(1 << FATTR4_SIZE) for _ in range(80)]
    operations += [op.delegpurge(0), op.getfh()]
    result = measured(p, operations, "checkpoint_many_small_attrs")
    require(all(item.obj_attributes[FATTR4_SIZE] == 8 for item in result.resarray[1:81]),
            "compacted GETATTR snapshots changed or overlapped")
    require(result.resarray[-1].object == fh, "long GETATTR run lost its cursor")

    # Repeated PUTFH really performs metadata open/stat work. Its expanded VFS
    # sequence exceeds 128 helpers while remaining below the wire-map limit.
    operations = []
    for _ in range(36):
        operations += [op.putfh(fh), op.getattr(1 << FATTR4_SIZE)]
    operations += [op.delegpurge(0), op.getfh()]
    result = measured(p, operations, "checkpoint_expanded_helper_capacity")
    require(result.resarray[-1].object == fh, "expanded helper run changed the current FH")


def terminal_inputs(p):
    suffix = b"checkpoint-forbidden-suffix"
    bad_option = op.setxattr(SETXATTR4_EITHER, b"attribute", b"value")
    bad_option.opsetxattr.sxa_option = 99
    cases = (
        ("empty_name", op.lookup(b""), NFS4ERR_INVAL),
        ("bad_character", op.lookup(b"a/b"), NFS4ERR_BADCHAR),
        ("long_name", op.remove(b"a" * 256), NFS4ERR_NAMETOOLONG),
        ("getattr_input", op.getattr(1 << FATTR4_TIME_ACCESS_SET), NFS4ERR_INVAL),
        ("xattr_option", bad_option, NFS4ERR_INVAL),
        ("xattr_name", op.getxattr(b""), NFS4ERR_INVAL),
        ("restore_missing", op.restorefh(), NFS4ERR_RESTOREFH),
        ("link_missing", op.link(b"linked"), NFS4ERR_NOFILEHANDLE),
        ("bad_handle", op.putfh(b"malformed"), NFS4ERR_BADHANDLE),
    )
    for label, invalid, expected in cases:
        operations = [op.putfh(p.directory), op.getattr(1 << FATTR4_TYPE), invalid,
                      op.create(createtype4(NF4DIR), suffix, {})]
        pack_option = NFS4Packer.pack_setxattr_option4
        try:
            if label == "xattr_option":
                # Exercise server validation instead of failing in pynfs's
                # enum packer before the request reaches the wire.
                NFS4Packer.pack_setxattr_option4 = NFS4Packer.pack_uint
            result = measured(p, operations, "checkpoint_terminal_" + label, expected, count=3)
        finally:
            NFS4Packer.pack_setxattr_option4 = pack_option
        require(len(result.resarray) == 3, "static terminal checkpoint allowed its suffix")
        p.call([op.putfh(p.directory), op.lookup(suffix)], expected=NFS4ERR_NOENT)

    result = measured(p, [op.putfh(p.directory), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH),
                          op.lookup(b""), op.create(createtype4(NF4DIR), suffix, {})],
                      "checkpoint_terminal_no_fh_precedence", NFS4ERR_NOFILEHANDLE, count=3)
    require(len(result.resarray) == 3, "NOFILEHANDLE did not suppress invalid-name suffix")
    measured(p, [op.delegpurge(0), op.putfh(b"malformed"), op.putfh(p.directory)],
             "checkpoint_terminal_after_handle_free", NFS4ERR_BADHANDLE, count=2)


def protocol(p):
    enabled = os.environ.get("CHIMERA_COMPOUND_FEATURE", "").startswith("pnfs")
    result = measured(p, [op.delegpurge(0), op.putfh(p.directory), op.getattr(1 << FATTR4_TYPE),
                          op.delegpurge(0), op.getfh()], "checkpoint_protocol_handle_free")
    require(result.resarray[-1].object == p.directory, "protocol checkpoint changed the current FH")

    result = measured(p, [op.putrootfh(), op.delegpurge(0), op.getfh(),
                          op.putfh(p.directory), op.getfh()], "checkpoint_protocol_synthetic")
    require(result.resarray[-1].object == p.directory, "synthetic protocol checkpoint lost real PUTFH")

    operations = (
        ("deviceinfo", op.getdeviceinfo(b"\x7f" * 16, LAYOUT4_FLEX_FILES, 4096, 0),
         NFS4ERR_NOENT if enabled else NFS4ERR_NOTSUPP),
        ("devicelist", op.getdevicelist(LAYOUT4_FLEX_FILES, 32, 0, b"\0" * 8), NFS4ERR_NOTSUPP),
        ("layoutstats", op.layoutstats(0, 0, ANONYMOUS, io_info4(0, 0), io_info4(0, 0),
                                       b"\0" * 16, layoutupdate4(LAYOUT4_FLEX_FILES, b"")),
         NFS4_OK if enabled else NFS4ERR_NOTSUPP),
        ("layouterror", op.layouterror(0, 0, ANONYMOUS, []),
         NFS4_OK if enabled else NFS4ERR_NOTSUPP),
    )
    for label, checkpoint, expected in operations:
        result = measured(p, [op.putfh(p.directory), op.getattr(1 << FATTR4_TYPE), checkpoint, op.getfh()],
                          "checkpoint_protocol_" + label, expected)
        require(len(result.resarray) == (4 if expected == NFS4_OK else 3),
                "protocol response did not stop or continue its suffix correctly")


def synthetic_errors(p):
    # The root is synthetic in both checkpoint fixtures. Runtime object-kind
    # failures must share the same accepted span as its namespace prefix.
    for label, operation, expected in (
        ("commit", op.commit(0, 0), NFS4ERR_STALE),
        ("readlink", op.readlink(), NFS4ERR_STALE),
        ("xattr", op.getxattr(b"some-key"), NFS4ERR_STALE),
        ("openattr", op.openattr(False), NFS4ERR_NOTSUPP),
    ):
        result = measured(p, [op.putrootfh(), op.getfh(), operation,
                              op.putfh(p.directory), op.getfh()],
                          "checkpoint_synthetic_" + label, expected)
        require(len(result.resarray) == 3, "synthetic checkpoint escaped to backend or ran a suffix")


def pending_finish(p):
    gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
    if not gate:
        return
    name, fh, sid = p.create("retry-junction-checkpoint-finish")
    payload = b"private-reply"
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, payload)])
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    for action, expected in (("accept", NFS4ERR_INVAL), ("reject", NFS4ERR_INVAL), ("error", NFS4ERR_IO)):
        ready.unlink(missing_ok=True)
        release.unlink(missing_ok=True)
        failures = []

        def finish():
            try:
                deadline = time.monotonic() + 5
                while not ready.exists():
                    require(time.monotonic() < deadline, "terminal checkpoint did not reach pending finish")
                    time.sleep(0.01)
                release.write_text(action)
            except BaseException as error:
                failures.append(error)
                release.write_text("error")

        worker = threading.Thread(target=finish)
        worker.start()
        try:
            result = measured(p, [op.putfh(p.directory), op.lookup(name), op.getattr(1 << FATTR4_SIZE),
                                  op.lookup(b""), op.write(sid, 0, FILE_SYNC4, b"BAD")],
                              "checkpoint_finish_" + action, expected, count=4)
            if action != "error":
                require(len(result.resarray) == 4 and result.resarray[2].obj_attributes[FATTR4_SIZE] == len(payload),
                        "accepted checkpoint lost its private GETATTR snapshot")
        finally:
            worker.join(timeout=12)
        require(not worker.is_alive(), "finish releaser did not exit")
        if failures:
            raise failures[0]
        require(p.read(fh, sid) == payload, "terminal checkpoint finish ran WRITE suffix")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    p = Probe(parser.parse_args())
    try:
        capacity(p)
        terminal_inputs(p)
        protocol(p)
        synthetic_errors(p)
        pending_finish(p)
        p.check_trace()
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
