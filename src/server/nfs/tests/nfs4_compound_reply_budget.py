#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Actual-size variable replies, exact shared spans, and mutation suppression.

Use the compound wrapper with FEATURE=checkpoints, with or without its finish
retry preload. Fixture mutation is separate from measured read-only attempts;
resource/TOOSMALL cases prove their WRITE suffix was never executed.
"""
import argparse

from nfs4_compound_boundaries import Probe, op, require, channel_attrs4
from xdrdef.nfs4_const import *


def measured(p, operations, name, expected=NFS4_OK):
    return p.call(operations, "reply_budget_" + name, expected, runs=[(1, len(operations))])


def entry_names(result):
    return [entry.name for entry in result.reply.entries]


def small_actual_results(p):
    name, fh, sid = p.create("reply-budget-small")
    data = b"a small actual extent"
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, data),
            op.setxattr(SETXATTR4_EITHER, b"first", b"one"),
            op.setxattr(SETXATTR4_EITHER, b"second", b"two")])
    operations = [op.putfh(fh), op.getattr(1 << FATTR4_SIZE),
                  op.getxattr(b"first"), op.listxattrs(0, 1024 * 1024),
                  op.getxattr(b"second"), op.read_plus(sid, 0, 1024 * 1024),
                  op.read_plus(sid, len(data), 1024 * 1024), op.getfh()]
    result = measured(p, operations, "small_values_large_caps")
    require(result.resarray[2].gxr_value == b"one" and result.resarray[4].gxr_value == b"two",
            "GETXATTR snapshots overlapped or lost their retry-owned bytes")
    require(set(result.resarray[3].lxr_value.lxr_names) == {b"first", b"second"},
            "LISTXATTRS name pointers did not survive accepted completion")
    read = result.resarray[5]
    require(read.rpr_eof and len(read.rpr_contents) == 1 and
            read.rpr_contents[0].rpc_content == NFS4_CONTENT_DATA and
            read.rpr_contents[0].rpc_data.d_data == data,
            "READ_PLUS copied capacity instead of actual returned data")
    require(result.resarray[6].rpr_eof and not result.resarray[6].rpr_contents,
            "READ_PLUS empty result reused earlier attempt contents")
    require(result.resarray[-1].object == fh, "variable staging lost the current filehandle")

    operations = [op.putfh(p.directory), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                  op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                  op.putfh(fh), op.getxattr(b"first"), op.getfh()]
    result = measured(p, operations, "two_large_directory_caps")
    require(name in entry_names(result.resarray[1]) and name in entry_names(result.resarray[2]),
            "multiple READDIR pages overlapped or changed on finish retry")
    require(result.resarray[4].gxr_value == b"one", "READDIR staging overwrote its xattr successor")

    operations = [op.putrootfh(), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                  op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                  op.lookup(p.args.export.encode()), op.getfh()]
    result = measured(p, operations, "two_synthetic_directory_caps")
    require(p.args.export.encode() in entry_names(result.resarray[1]) and
            p.args.export.encode() in entry_names(result.resarray[2]),
            "synthetic READDIR did not rebuild both pages independently")
    require(result.resarray[-1].object == p.directory, "synthetic page changed namespace continuation")

    # A successful page may precede mutation only after its resource decision.
    operations = [op.putfh(p.directory), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                  op.putfh(fh), op.write(sid, len(data), FILE_SYNC4, b"!")]
    measured(p, operations, "directory_then_write")
    require(p.read(fh, sid) == data + b"!", "accepted READDIR lost its WRITE successor")

    operations = [op.putfh(p.directory), op.readdir(0, b"", 4096, 16, 1 << FATTR4_TYPE),
                  op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result = measured(p, operations, "small_page_stops_write", NFS4ERR_TOOSMALL)
    require(len(result.resarray) == 2, "TOOSMALL page executed its suffix")
    require(p.read(fh, sid) == data + b"!", "TOOSMALL page mutated file contents")


def exhaustion(p):
    _, fh, sid = p.create("reply-budget-exhaustion")
    original = b"unchanged by a rejected suffix"
    value = b"x" * 60000
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, original),
            op.setxattr(SETXATTR4_EITHER, b"large", value)])
    operations = [op.putfh(fh)] + [op.getxattr(b"large") for _ in range(3)]
    operations += [op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result = measured(p, operations, "xattr_exhaustion_stops_write", NFS4ERR_RESOURCE)
    require(result.resarray[-1].resop == OP_GETXATTR and len(result.resarray) < len(operations),
            "reply exhaustion was discovered after its WRITE successor")
    require(result.resarray[1].gxr_value == value,
            "a later failed staging corrupted the accepted GETXATTR prefix")
    require(p.read(fh, sid) == original, "reply exhaustion allowed its WRITE successor")

    # Tight but fitting replies keep working without a redundant 8192 floor.
    result = measured(p, [op.putfh(fh), op.getxattr(b"large"), op.getxattr(b"large"),
                          op.getattr(1 << FATTR4_SIZE), op.getfh()], "tight_fitting_values")
    require(result.resarray[1].gxr_value == value and result.resarray[2].gxr_value == value and
            result.resarray[3].obj_attributes[FATTR4_SIZE] == len(original) and
            result.resarray[4].object == fh, "a fitting large response lost its suffix")

    # READ_PLUS owns copied response bytes even though its backend READ owns
    # iovecs. The second large response must fail before the WRITE executes.
    large_data = b"d" * 60000
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, large_data)])
    operations = [op.putfh(fh), op.read_plus(sid, 0, 1024 * 1024),
                  op.read_plus(sid, 0, 1024 * 1024), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result = measured(p, operations, "read_plus_exhaustion_stops_write", NFS4ERR_RESOURCE)
    require(result.resarray[-1].resop == OP_READ_PLUS and len(result.resarray) == 3 and
            result.resarray[1].rpr_contents[0].rpc_data.d_data == large_data,
            "READ_PLUS staging failed after its mutation or damaged the successful prefix")
    require(p.read(fh, sid) == large_data[:4096], "READ_PLUS resource failure allowed WRITE")

    # LISTXATTRS must account for both its persistent name bytes and the array
    # of pointers into them. A late result cannot allocate after acceptance.
    keys = [f"list-{i:03d}-".encode() + b"k" * 190 for i in range(80)]
    for start in range(0, len(keys), 16):
        p.call([op.putfh(fh)] + [op.setxattr(SETXATTR4_EITHER, key, b"v")
                                for key in keys[start:start + 16]])
    operations = [op.putfh(fh), op.getxattr(b"large"), op.getxattr(b"large"),
                  op.listxattrs(0, 1024 * 1024), op.write(sid, 0, FILE_SYNC4, b"BAD")]
    result = measured(p, operations, "list_exhaustion_stops_write", NFS4ERR_RESOURCE)
    require(result.resarray[-1].resop == OP_LISTXATTRS and len(result.resarray) == 4 and
            result.resarray[1].gxr_value == value and result.resarray[2].gxr_value == value,
            "LISTXATTRS failed outside its gate or corrupted preceding values")
    require(p.read(fh, sid) == large_data[:4096], "LISTXATTRS resource failure allowed WRITE")


def hard_limit(p):
    # The finite map limit remains intentional. Exercise a real independent
    # final map, rather than manufacture a split with an oversized READDIR cap.
    attrs = channel_attrs4(0, 1024 * 1024, 1024 * 1024, 1024 * 1024, 256, 8, [])
    p.session = p.session.client.create_session(fore_attrs=attrs)
    _, fh, sid = p.create("reply-budget-hard-limit")
    p.call([op.putfh(fh), op.setxattr(SETXATTR4_EITHER, b"key", b"last-map")])
    prefix = [op.putfh(fh)] + [op.delegpurge(0) for _ in range(127)]
    result = p.call(prefix + [op.getxattr(b"key")], "reply_budget_hard_limit_getxattr",
                    runs=[(1, 128), (129, 1)])
    require(result.resarray[-1].gxr_value == b"last-map", "standalone suffix lost xattr staging")
    result = p.call(prefix + [op.read_plus(sid, 0, 1024 * 1024)], "reply_budget_hard_limit_read_plus",
                    runs=[(1, 128), (129, 1)])
    require(result.resarray[-1].rpr_eof and not result.resarray[-1].rpr_contents,
            "standalone READ_PLUS changed an empty response")
    prefix[0] = op.putfh(p.directory)
    result = p.call(prefix + [op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE)],
                    "reply_budget_hard_limit_readdir", runs=[(1, 128), (129, 1)])
    require(b"reply-budget-hard-limit" in entry_names(result.resarray[-1]),
            "standalone READDIR lost entries after the wire-map boundary")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    p = Probe(parser.parse_args())
    try:
        small_actual_results(p)
        exhaustion(p)
        hard_limit(p)
        p.check_trace()
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
