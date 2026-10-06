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
import sys
from pathlib import Path
import threading
import time
import urllib.request

from nfs4_compound_boundaries import Probe, op, require, ANONYMOUS, CURRENT, createtype4, stateid4, nfs4client, AuthSys
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


def export_config(method, name, **options):
    url = "http://127.0.0.1:8080/api/v1/exports"
    data = None
    if method == "POST":
        data = json.dumps(dict(name=name, **options)).encode()
    else:
        # The REST route takes the literal remainder, including the leading /.
        url += "/" + name
    request = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(request, timeout=3) as response:
        require(response.status in (200, 201, 204), "export request failed")
        body = response.read()
        return json.loads(body) if body else None


def export_entries(p, root, sibling, directory, name, fh, sid, payload):
    for label, root_op in (("root", op.putrootfh()), ("public", op.putpubfh())):
        result = measured(p, [root_op, op.getfh(), op.lookup(name), op.read(sid, 0, 128), op.getfh()],
                          "export_" + label + "_lookup_read")
        require(result.resarray[1].object == root and result.resarray[3].data == payload and
                result.resarray[-1].object == fh, "root entry changed its cursor or read result")
    _, target, target_sid = p.create("export-copy-target")
    result = measured(p, [op.putrootfh(), op.lookup(name), op.savefh(), op.putfh(target),
                          op.copy(sid, target_sid, 0, 0, len(payload), True, True, []),
                          op.read(target_sid, 0, 128)], "export_root_copy")
    require(result.resarray[-1].data == payload, "root entry lost COPY endpoints or data")
    result = measured(p, [op.putrootfh(), op.lookup(name), op.savefh(), op.putfh(target),
                          op.copy(sid, target_sid, len(payload), 0, 0, True, True, []), op.getfh()],
                      "export_root_copy_eof")
    require(result.resarray[-2].cr_response.wr_count == 0 and result.resarray[-1].object == target,
            "root entry lost zero-length COPY or its final cursor")
    result = measured(p, [op.putrootfh(), op.savefh(), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH),
                          op.restorefh(), op.getfh()], "export_root_restore_consumed")
    require(result.resarray[-1].object == root, "RESTOREFH lost the newly resolved root")
    p.call([op.putfh(fh), op.putrootfh(), op.lookup(name), op.read(sid, 0, 128)],
           "export_root_after_file", runs=[(1, 4)])
    p.call([op.putrootfh(), op.getfh(), op.putrootfh(), op.getfh()],
           "export_repeated_root", runs=[(1, 4)])
    p.call([op.putrootfh(), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE), op.getfh()],
           "export_single_root", runs=[(1, 3)])
    result = p.call([op.putrootfh(), op.lookup(b"share"), op.getfh(), op.getattr(1 << FATTR4_TYPE)],
                    "export_sibling_suffix", runs=[(1, 4)])
    require(result.resarray[2].object == sibling, "sibling entry did not select its own export")
    p.call([op.putrootfh(), op.lookup(b"share"), op.getfh(), op.putfh(root), op.getfh()],
           "export_credential_boundary", runs=[(1, 5)])

    configurations = {
        "/entry-multi": {"path": "/rootfs/ordinary-directory"},
        "/entry-ro": {"path": "/rootfs/ordinary-directory", "access": "ro"},
        "/entry-squash": {"path": "/rootfs/ordinary-directory", "squash": "all",
                           "anonuid": 12345, "anongid": 23456},
        "/entry-krb": {"path": "/rootfs", "sec": ["krb5"]},
        "/entry-missing": {"path": "/rootfs/missing-export-target"},
    }
    for export, config in configurations.items():
        export_config("POST", export, **config)
    try:
        result = p.call([op.putrootfh(), op.lookup(b"entry-multi"), op.getattr(1 << FATTR4_FILEID),
                         op.getfh()], "export_multicomponent", runs=[(1, 4)])
        expected = p.call([op.putfh(directory), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
        require(result.resarray[2].obj_attributes == expected, "export path selected the wrong inode")
        p.call([op.putrootfh(), op.lookup(b"entry-multi"),
                op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE), op.getfh()],
               "export_single_lookup", runs=[(1, 4)])
        p.call([op.putrootfh(), op.lookup(b"entry-ro"), op.getfh(),
                op.create(createtype4(NF4DIR), b"forbidden-export", {})],
               "export_readonly_suffix", NFS4ERR_ROFS, runs=[(1, 4)])
        p.call([op.putfh(directory), op.lookup(b"forbidden-export")], expected=NFS4ERR_NOENT)
        p.call([op.putrootfh(), op.lookup(b"entry-krb"), op.getfh()],
               "export_wrongsec", NFS4ERR_WRONGSEC, runs=[(1, 3)])
        p.call([op.putrootfh(), op.lookup(b"entry-missing"),
                op.create(createtype4(NF4DIR), b"unexecuted-export-suffix", {})],
               "export_missing_prefix", NFS4ERR_NOENT, runs=[(1, 3)])
        p.call([op.putfh(root), op.lookup(b"unexecuted-export-suffix")], expected=NFS4ERR_NOENT)

        # Each entry recomputes squash from the original RPC identity. The
        # second OPEN crosses back to the unsquashed root in the same request.
        squashed, unsquashed = b"squashed-entry-file", b"root-entry-file"
        result = p.call([op.putrootfh(), op.lookup(b"entry-squash"),
                         p.open_op(squashed, attrs={FATTR4_MODE: 0o600}), op.getattr(1 << FATTR4_OWNER), op.getfh(),
                         op.putrootfh(), p.open_op(unsquashed), op.getattr(1 << FATTR4_OWNER), op.getfh()],
                        "export_squash_reset", runs=[(1, 9)])
        p.files.append((unsquashed, result.resarray[8].object, result.resarray[6].stateid))
        squashed_owner, root_owner = result.resarray[3].obj_attributes, result.resarray[7].obj_attributes
        p.call([op.putfh(result.resarray[4].object), op.close(0, result.resarray[2].stateid)])
        p.call([op.putfh(directory), op.remove(squashed)])
        require(squashed_owner[FATTR4_OWNER] == b"12345" and root_owner[FATTR4_OWNER] == b"0",
                f"export transition lost or stacked squash: {squashed_owner!r}, {root_owner!r}")
    finally:
        for export in configurations:
            export_config("DELETE", export)


def replace_export_during_finish(p, name, operations, runs, replacement, label=None, results=None, new_export=False):
    gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
    if not gate:
        return False
    if not new_export:
        replacement = dict(replacement, export_id=export_config("GET", name)["export_id"])
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)
    failures = []

    def replace():
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                require(time.monotonic() < deadline, "export entry never reached pending finish")
                time.sleep(0.01)
            if not new_export:
                export_config("DELETE", name)
            export_config("POST", name, **replacement)
        except BaseException as error:
            failures.append(error)
        finally:
            release.write_text("reject")

    worker = threading.Thread(target=replace)
    worker.start()
    try:
        result = p.call(operations, "export_replaced_" + (label or name.strip("/").replace("-", "_") or "root"),
                        NFS4ERR_DELAY, runs=runs)
        require(len(result.resarray) == (len(operations) - 1 if results is None else results),
                "replaced export ran its retried suffix")
    finally:
        worker.join(timeout=12)
    require(not worker.is_alive(), "export replacer did not finish")
    if failures:
        raise failures[0]
    return True


def root_export_reconfiguration(p, root, sibling):
    original = export_config("GET", "/")
    original.pop("name")
    marker = b"retry-export-entry"
    p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker, {FATTR4_MODE: 0o777})])
    p.directories.append(marker)
    entry_path = "/rootfs/" + marker.decode()
    link = b"export-root-symlink"
    result = p.call([op.putfh(root), op.create(createtype4(NF4LNK, linkdata=b"/share"), link, {}), op.getfh()])
    p.files.append((link, result.resarray[-1].object, None))
    for name, path in (("moving-entry", "/share"), ("moving-policy", entry_path)):
        export_config("POST", "/" + name, path=entry_path)
        try:
            changed = replace_export_during_finish(p, "/" + name,
                [op.putrootfh(), op.lookup(name.encode()), op.getfh()], [(1, 3)],
                {"path": path, "access": "ro", "squash": "all", "anonuid": 12345, "anongid": 23456})
            result = p.call([op.putrootfh(), op.lookup(name.encode()), op.getattr(1 << FATTR4_FILEID)],
                            "export_replacement_fresh_" + name, runs=[(1, 3)])
            target = sibling if changed and path == "/share" else p.call(
                [op.putfh(root), op.lookup(marker), op.getfh()]).resarray[-1].object
            expected = p.call([op.putfh(target), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
            require(result.resarray[-1].obj_attributes == expected, "new entry retained the old target")
            if changed:
                p.call([op.putrootfh(), op.lookup(name.encode()), p.open_op(b"denied-after-replacement")],
                       expected=NFS4ERR_ROFS)
        finally:
            export_config("DELETE", "/" + name)
    try:
        export_config("DELETE", "/")
        export_config("POST", "/", path=entry_path)
        replace_export_during_finish(p, "/", [op.putrootfh(), op.getfh()], [(1, 2)], {"path": "/share"})
        measured(p, [op.putrootfh(), op.getattr(1 << FATTR4_TYPE)], "export_root_replacement_fresh")

        # Empty VFS path uses the physical VFS root; ordinary configured paths
        # always resolve afresh, even with a previously populated root cache.
        export_config("DELETE", "/")
        export_config("POST", "/", **dict(original, path="/"))
        result = measured(p, [op.putrootfh(), op.lookup(b"rootfs"), op.getattr(1 << FATTR4_FILEID)],
                          "export_vfs_root_path")
        expected = p.call([op.putfh(root), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
        require(result.resarray[-1].obj_attributes == expected, "empty export path selected the wrong root")
        result = measured(p, [op.putfh(root), op.putrootfh(), op.getfh(), op.putpubfh(), op.getfh()],
                          "export_repeated_vfs_root")
        require(result.resarray[2].object == result.resarray[4].object,
                "repeated empty export path changed the physical VFS root")

        export_config("DELETE", "/")
        export_config("POST", "/", path="/rootfs/" + link.decode())
        result = measured(p, [op.putrootfh(), op.getattr(1 << FATTR4_TYPE)], "export_root_follows_symlink")
        require(result.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4DIR,
                "root path failed to follow its final symlink")

        export_config("DELETE", "/")
        export_config("POST", "/", **dict(original, path="/missing-root-path"))
        p.call([op.putrootfh(), op.getfh()], "export_root_missing_path", NFS4ERR_SERVERFAULT, runs=[(1, 2)])
        result = measured(p, [op.putfh(root), op.putpubfh(), op.getfh()],
                          "export_later_root_missing_path", NFS4ERR_SERVERFAULT)
        require(len(result.resarray) == 2, "failed root reset ran its suffix")

        # Reuse the configured id so the old signed root FH is also a valid
        # test of deferred WRONGSEC followed by PUTFH into that same export.
        export_config("DELETE", "/")
        export_config("POST", "/", **dict(original, sec=["krb5"]))
        p.call([op.putrootfh(), op.getfh()], "export_root_wrongsec", NFS4ERR_WRONGSEC, runs=[(1, 2)])
        p.call([op.putrootfh(), op.putfh(root), op.getattr(1 << FATTR4_TYPE)],
               "export_root_deferred_wrongsec", NFS4ERR_WRONGSEC, runs=[(1, 3)])
        p.call([op.putrootfh(), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH)],
               "export_root_security_info", runs=[(1, 2)])
        p.call([op.putrootfh(), op.putfh(sibling), op.getfh()],
               "export_root_security_replaced_fh", runs=[(1, 3)])
    finally:
        export_config("DELETE", "/")
        export_config("POST", "/", **original)
        p.directory = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object


def cold_root(p):
    """Cold resolution preserves cursors and rejects obsolete root snapshots."""
    base = int(p.args.minor != 0)
    original = export_config("GET", "/")
    original.pop("name")
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    sibling = p.directory

    def configure(**changes):
        export_config("DELETE", "/")
        export_config("POST", "/", **dict(original, **changes))

    def call(operations, label, expected=NFS4_OK, runs=None):
        return p.call(operations, "cold_root_" + label, expected,
                      runs=[(base + start, count) for start, count in runs])

    try:
        configure()
        result = call([op.putfh(root), op.lookup(b"share"), op.getfh()], "lookup",
                      runs=[(0, 3)])
        require(result.resarray[-1].object == sibling, "cold root failed to select its sibling junction")
        call([op.putfh(root), op.lookup(b"share"), op.getfh()], "warm_lookup",
             runs=[(0, 3)])

        configure()
        result = call([op.putfh(root), op.secinfo(b"share"), op.putfh(root), op.getfh()], "secinfo",
                      runs=[(0, 4)])
        require(result.resarray[-1].object == root, "root resolution changed the protocol cursor or export")

        configure()
        call([op.putfh(root), op.lookupp(), op.getfh()], "parentless", NFS4ERR_NOENT,
             runs=[(0, 3)])
        configure()
        result = call([op.putfh(sibling), op.lookupp(), op.getfh()], "sibling_parent",
                      runs=[(0, 3)])
        require(result.resarray[-1].object == root, "sibling LOOKUPP lost its namespace parent")

        configure(path="/missing-cold-root")
        call([op.putfh(sibling), op.lookupp(), op.getfh()], "missing", NFS4ERR_SERVERFAULT,
             runs=[(0, 3)])

        configure(path="/")
        result = call([op.putfh(sibling), op.lookupp(), op.getfh()], "physical",
                      runs=[(0, 3)])
        physical = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
        require(result.resarray[-1].object == physical, "cold empty path resolved the wrong root")

        configure()
        link = b"cold-root-symlink"
        p.call([op.putfh(root), op.create(createtype4(NF4LNK, linkdata=b"/share"), link, {})])
        configure(path="/rootfs/" + link.decode())
        result = call([op.putfh(root), op.lookupp(), op.getattr(1 << FATTR4_FILEID)], "symlink",
                      runs=[(0, 3)])
        expected = p.call([op.putfh(sibling), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
        require(result.resarray[-1].obj_attributes == expected, "cold root did not follow the final symlink")
        configure()
        p.call([op.putfh(root), op.remove(link)])

        nested_name = b"cold-root-ordinary"
        nested = p.call([op.putfh(root), op.create(createtype4(NF4DIR), nested_name,
                                                  {FATTR4_MODE: 0o777}), op.getfh()]).resarray[-1].object
        child = p.call([op.putfh(nested), op.create(createtype4(NF4DIR), b"share", {}),
                        op.getfh()]).resarray[-1].object
        configure()
        result = call([op.putfh(root), op.savefh(), op.lookup(nested_name), op.lookupp(),
                       op.getfh(), op.restorefh(), op.getfh()], "ordinary_parent_saved", runs=[(0, 7)])
        require(result.resarray[4].object == root and result.resarray[-1].object == root,
                "inline root resolution corrupted current or saved cursor")
        # Restoring a saved root must use the execution cursor rather than
        # the intervening ordinary object's physical namespace. The second
        # request uses the same sequence with a now-warm comparison cache.
        saved_lookup = [op.putfh(root), op.savefh(), op.putfh(nested), op.lookup(b"share"),
                        op.getfh(), op.restorefh(), op.lookup(b"share"), op.getfh()]
        configure()
        for label in ("cold_saved_lookup", "warm_saved_lookup"):
            result = call(saved_lookup, label, runs=[(0, 8)])
            require(result.resarray[4].object == child and result.resarray[-1].object == sibling,
                    "restored root confused a physical same-name entry with an export")

        # LOOKUPP discovers the root export at runtime. Planning still starts
        # in the sibling export, so subsequent saved-FH policy and junction
        # decisions must follow the private identity selected by execution.
        configure()
        result = call([op.putfh(sibling), op.lookupp(), op.savefh(), op.putfh(nested),
                       op.lookup(b"share"), op.getfh(), op.restorefh(), op.secinfo(b"share"),
                       op.restorefh(), op.lookup(b"share"), op.getfh()],
                      "runtime_parent_saved_policy", runs=[(0, 11)])
        require(result.resarray[5].object == child and result.resarray[7].resok4 and
                result.resarray[-1].object == sibling,
                "runtime parent lost its saved export identity or policy checkpoint")

        for label, changes in (("ordinary_name", {}), ("unavailable_view", {"path": "/missing-cold-root"})):
            configure(**changes)
            result = call([op.putfh(nested), op.lookup(b"share"), op.getfh()], label, runs=[(0, 3)])
            require(result.resarray[-1].object == child, "root probe changed an ordinary physical lookup")
        configure()
        p.call([op.putfh(nested), op.remove(b"share")])
        p.call([op.putfh(root), op.remove(nested_name)])

        gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
        if gate:
            marker = b"retry-export-cold"
            result = p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker,
                                                      {FATTR4_MODE: 0o777}), op.getfh()])
            old_root = result.resarray[-1].object
            # A swallowed failure would expose this physical entry instead of
            # rejecting the changed junction view before the mutation suffix.
            p.call([op.putfh(old_root), op.create(createtype4(NF4DIR), b"share", {})])
            ready, release = Path(gate + ".ready"), Path(gate + ".release")
            cases = (("lookup_path", op.lookup(b"share"), "reject", {"path": "/share"}),
                     ("secinfo_policy", op.secinfo(b"share"), "reject", {"access": "ro"}),
                     ("lookupp_path", op.lookupp(), "reject", {"path": "/share"}),
                     ("mount_parent_reject", op.lookupp(), "reject", {"path": "/share"}),
                     ("mount_parent_accept", op.lookupp(), "accept", {"path": "/share"}),
                     ("mount_parent_error", op.lookupp(), "error", {}),
                     ("accepted_path", op.lookup(b"share"), "accept", {"path": "/share"}),
                     ("secinfo_identity", op.secinfo(b"share"), "reject",
                      {"squash": "all", "anonuid": 12345, "anongid": 23456}),
                     ("finish_error", op.lookup(b"share"), "error", {}))
            for label, operation, finish, replacement in cases:
                configure(path="/rootfs/" + marker.decode())
                ready.unlink(missing_ok=True)
                release.unlink(missing_ok=True)
                failures = []

                def replace():
                    try:
                        deadline = time.monotonic() + 5
                        while not ready.exists():
                            require(time.monotonic() < deadline, "cold root never reached pending finish")
                            time.sleep(0.01)
                        configure(**dict({"path": "/rootfs/" + marker.decode()}, **replacement))
                    except BaseException as error:
                        failures.append(error)
                    finally:
                        pending = Path(gate + ".pending")
                        pending.write_text(finish)
                        pending.replace(release)

                worker = threading.Thread(target=replace)
                worker.start()
                try:
                    initial = sibling if label.startswith("mount_parent_") else old_root
                    operations = [op.putfh(initial), operation, op.putfh(sibling)]
                    # All namespace selections now finish inline. Stop before
                    # CREATE so this read-only fixture can safely reject finish.
                    operations.append(op.verify({FATTR4_TYPE: NF4REG}))
                    operations.append(op.create(createtype4(NF4DIR), b"forbidden-cold", {}))
                    result = call(operations, label,
                                  NFS4ERR_IO if finish == "error" else
                                  NFS4ERR_NOT_SAME if finish == "accept" else NFS4ERR_DELAY,
                                  runs=[(0, len(operations))])
                    require(len(result.resarray) == (4 if finish == "accept" else 1), "obsolete root resolution ran its mutation suffix")
                finally:
                    worker.join(timeout=12)
                require(not worker.is_alive(), "cold root replacer did not finish")
                if failures:
                    raise failures[0]
                p.call([op.putfh(sibling), op.lookup(b"forbidden-cold")], expected=NFS4ERR_NOENT)
                # A fresh request must not see a stale cache prime from the
                # rejected resolution, even when finish itself accepted.
                release.write_text("reject")
                result = call([op.putfh(root), op.lookupp(), op.getattr(1 << FATTR4_FILEID)],
                              label + "_fresh", runs=[(0, 3)])
                target = sibling if replacement.get("path") == "/share" else old_root
                expected = p.call([op.putfh(target), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
                require(result.resarray[-1].obj_attributes == expected, "stale root survived cache publication")
            configure()
            p.call([op.putfh(old_root), op.remove(b"share")])
            p.call([op.putfh(root), op.remove(marker)])
    finally:
        configure()
    print("PASS cold root snapshots, retry, cache publication and namespace continuation", flush=True)


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
            release.write_text("reject")

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


def pseudo_root(p):
    # pynfs decodes the XDR entry chain recursively, several frames per row.
    previous_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(previous_limit, 4096))
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    share_attrs = p.call([op.putfh(p.directory), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
    original = export_config("GET", "/share")
    original.pop("name")
    start = 1 if p.args.minor == 0 else 2
    mask = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEID) | (1 << FATTR4_FILEHANDLE)
    created = []
    share_removed = False

    def add(name, path="/share"):
        export_config("POST", name, path=path)
        created.append(name)

    def remove(name):
        export_config("DELETE", name)
        created.remove(name)

    def page(name, cookie=0, maxcount=65536, expected=NFS4_OK, attrmask=mask):
        result = p.call([op.putrootfh(), op.readdir(cookie, b"", maxcount, maxcount, attrmask), op.getfh()],
                        "pseudo_" + name, expected, runs=[(start - 1, 3)])
        if expected != NFS4_OK:
            require(len(result.resarray) == 2, "failed pseudo-root page executed its GETFH suffix")
            return None
        require(result.resarray[-1].object == root, "READDIR exposed a backing cursor as the pseudo-root")
        listing = result.resarray[1]
        require(listing.cookieverf == b"\0" * 8, "pseudo-root changed its positional-cookie verifier")
        return listing.reply

    try:
        operations = [op.putrootfh(), op.savefh(), op.getfh(), op.putpubfh(),
                      op.access(0xffffffff), op.getattr(mask | (1 << FATTR4_ACL)),
                      op.putfh(root), op.restorefh(), op.getfh()]
        result = p.call(operations, "pseudo_cursors_metadata", runs=[(start - 1, len(operations))])
        require(result.resarray[2].object == root and result.resarray[-1].object == root,
                "synthetic cursor was replaced by a backend handle")
        attrs = result.resarray[5].obj_attributes
        require(attrs[FATTR4_TYPE] == NF4DIR and attrs[FATTR4_FILEHANDLE] == root,
                "synthetic metadata describes a backing filesystem")
        access = result.resarray[4]
        require(access.supported == (ACCESS4_READ | ACCESS4_LOOKUP | ACCESS4_MODIFY | ACCESS4_EXTEND | ACCESS4_DELETE)
                and access.access == (ACCESS4_READ | ACCESS4_LOOKUP), "synthetic ACCESS changed its supported rights")
        operations = [op.putrootfh(), op.savefh(), op.lookup(b"share"), op.getfh(),
                      op.restorefh(), op.getattr(mask), op.getfh(), op.putfh(p.directory),
                      op.savefh(), op.putfh(root), op.restorefh(), op.getfh(),
                      op.lookupp(), op.getfh(), op.lookup(b"share"), op.getfh()]
        result = p.call(operations, "pseudo_export_roundtrip", runs=[(start - 1, len(operations))])
        require([result.resarray[i].object for i in (3, 11, 15)] == [p.directory] * 3 and
                [result.resarray[i].object for i in (6, 13)] == [root] * 2,
                "mixed real/synthetic cursor kinds or saved identity were lost")
        require(result.resarray[5].obj_attributes[FATTR4_FILEHANDLE] == root,
                "synthetic GETATTR retained an earlier real cursor")
        result = p.call([op.putfh(p.directory), op.lookupp(),
                         op.readdir(0, b"", 4096, 8192, mask), op.getfh(),
                         op.lookup(b"share"), op.getfh()], "pseudo_runtime_parent_page",
                        runs=[(start - 1, 6)])
        require(result.resarray[3].object == root and result.resarray[5].object == p.directory and
                any(e.name == b"share" for e in result.resarray[2].reply.entries),
                "runtime parent READDIR used backend cookie space or lost its export entry")
        result = p.call([op.putrootfh(), op.lookup(b"missing-synthetic-export"), op.getfh()],
                        "pseudo_missing_lookup", NFS4ERR_NOENT, runs=[(start - 1, 3)])
        require(len(result.resarray) == 2, "missing synthetic export executed its suffix")
        _, attr_base, _ = p.create("pseudo-attrdir-base")
        result = p.call([op.putfh(attr_base), op.openattr(False), op.putfh(root),
                         op.getattr(mask), op.getfh()], "pseudo_after_attrdir",
                        runs=[(start - 1, 5)])
        attrs = result.resarray[-2].obj_attributes
        require(attrs[FATTR4_TYPE] == NF4DIR and attrs[FATTR4_SIZE] == 4096 and result.resarray[-1].object == root,
                "synthetic PUTFH retained its predecessor's attribute-directory cursor")
        p.call([op.putrootfh(), op.lookupp(), op.getfh()], "pseudo_no_parent", NFS4ERR_NOENT,
               runs=[(start - 1, 3)])
        result = p.call([op.putrootfh(), op.savefh(), op.secinfo(b"share"), op.restorefh(), op.getfh()],
                        "pseudo_saved_across_boundary", runs=[(start - 1, 5)])
        require(result.resarray[-1].object == root, "inherited saved synthetic cursor was lost")
        p.call([op.putrootfh(), op.secinfo(b"missing-synthetic-export"), op.getfh()],
               "pseudo_missing_secinfo", NFS4ERR_NOENT, runs=[(start - 1, 3)])
        if p.args.minor:
            p.call([op.putrootfh(), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH), op.getfh()],
                   "pseudo_consumed", NFS4ERR_NOFILEHANDLE, runs=[(start - 1, 3)])
            result = p.call([op.putrootfh(), op.savefh(), op.secinfo_no_name(SECINFO_STYLE4_CURRENT_FH),
                             op.restorefh(), op.getfh()], "pseudo_restore_consumed", runs=[(start - 1, 5)])
            require(result.resarray[-1].object == root, "RESTOREFH did not recover a consumed pseudo-root")
            p.call([op.putrootfh(), op.secinfo_no_name(SECINFO_STYLE4_PARENT), op.getfh()],
                   "pseudo_secinfo_parent", NFS4ERR_NOENT, runs=[(start - 1, 3)])
        nested = b"pseudo-nested"
        result = p.call([op.putfh(p.directory), op.create(createtype4(NF4DIR), nested, {FATTR4_MODE: 0o777}),
                         op.getfh(), op.getattr(1 << FATTR4_FILEID)])
        nested_attrs = result.resarray[-1].obj_attributes
        p.directories.append(nested)
        link = b"pseudo-link"
        result = p.call([op.putfh(p.directory),
                         op.create(createtype4(NF4LNK, linkdata=b"/share"), link, {}), op.getfh()])
        p.files.append((link, result.resarray[-1].object, None))
        for index in range(40):
            add(f"/page{index:02d}")
        add("/physical", "/")
        add("/nested", "/share/" + nested.decode())
        add("/link", "/share/" + link.decode())
        expected_names = {b"share", b"physical", b"nested", b"link"} | {f"page{i:02d}".encode() for i in range(40)}
        listing = page("whole_page")
        require(listing.eof and {e.name for e in listing.entries} == expected_names,
                "one-compound page omitted, duplicated or invented an export")
        require(len(listing.entries) == len(expected_names), "pseudo-root repeated an entry")
        result = p.call([op.putrootfh(), op.savefh(), op.readdir(0, b"", 32768, 32768, mask),
                         op.getattr(mask), op.putpubfh(), op.readdir(0, b"", 32768, 32768, mask),
                         op.restorefh(), op.getfh()], "pseudo_two_pages", runs=[(start - 1, 8)])
        require({entry.name for entry in result.resarray[2].reply.entries} == expected_names and
                {entry.name for entry in result.resarray[5].reply.entries} == expected_names and
                result.resarray[3].obj_attributes[FATTR4_FILEHANDLE] == root and result.resarray[-1].object == root,
                "coalesced virtual pages leaked a backing cursor or lost their independent results")
        handles = set()
        for entry in listing.entries:
            require(FATTR4_TYPE in entry.attrs, f"export entry lacks requested attributes: {entry!r}")
            expected_type = NF4LNK if entry.name == b"link" else NF4DIR
            require(entry.attrs[FATTR4_TYPE] == expected_type, "export entry lost type or followed its final symlink")
            fh = entry.attrs[FATTR4_FILEHANDLE]
            handles.add(fh)
            attrs = p.call([op.putfh(fh), op.getattr(1 << FATTR4_FILEID)]).resarray[-1].obj_attributes
            require(attrs[FATTR4_FILEID] == entry.attrs[FATTR4_FILEID], "READDIR minted an unusable handle")
            if entry.name == b"nested":
                require(attrs == nested_attrs, "multi-component export selected its parent inode")
            elif entry.name not in (b"physical", b"link"):
                require(attrs == share_attrs, "alias entry resolved the wrong backing inode")
        require(len(handles) == len(expected_names), "export identities were lost from signed handles")

        broad_mask = (1 << 56) - 1
        broad_mask &= ~((1 << FATTR4_TIME_ACCESS_SET) | (1 << FATTR4_TIME_MODIFY_SET))
        seen, cookie = set(), 0
        while True:
            broad = page(f"broad_attributes_{cookie}", cookie, attrmask=broad_mask)
            require(broad.entries, "large attribute page stalled")
            for entry in broad.entries:
                require(entry.name in expected_names and entry.name not in seen and entry.cookie > cookie,
                        "large attribute replies corrupted the staged page")
                seen.add(entry.name)
                cookie = entry.cookie
                require(FATTR4_FILEHANDLE in entry.attrs and FATTR4_FILEID in entry.attrs,
                        "large attribute reply dropped the handle or file id")
                if entry.name != b"physical":
                    require(FATTR4_TIME_MODIFY in entry.attrs,
                            "large attribute reply dropped its trailing timestamp")
            if broad.eof:
                break
        require(seen == expected_names, "large attribute pages skipped an export")

        seen, cookie, pages = set(), 0, 0
        while True:
            listing = page(f"page_{pages}", cookie, 1024)
            require(listing.entries, "pseudo-root pagination stalled")
            for entry in listing.entries:
                require(entry.name in expected_names and entry.name not in seen and entry.cookie > cookie,
                        "paged enumeration repeated a name or regressed its cookie")
                seen.add(entry.name)
                cookie = entry.cookie
            pages += 1
            if listing.eof:
                break
            require(pages <= len(expected_names), "pseudo-root pagination failed to reach EOF")
        require(seen == expected_names and pages > 1, "pagination skipped an export or never filled a page")
        eof = page("after_eof", cookie, 16)
        require(eof.eof and not eof.entries, "continuation past the last export was not empty EOF")
        page("too_small", maxcount=16, expected=NFS4ERR_TOOSMALL)
        page("invalid_cookie", cookie=(1 << 64) - 1, expected=NFS4ERR_BAD_COOKIE)
        for reserved in (1, 2):
            p.call([op.putrootfh(), op.readdir(reserved, b"", 4096, 4096, mask), op.getfh()],
                   f"pseudo_reserved_{reserved}", NFS4ERR_BAD_COOKIE, runs=[(start - 1, 3)])

        # A generous wire maxcount must still leave the encoding arena's
        # reserve intact and return a resumable page, including its GETFH.
        wide = [f"/wide{i:03d}-" + "x" * 200 for i in range(400)]
        for name in wide:
            add(name)
        wide_expected = expected_names | {name[1:].encode() for name in wide}
        seen, cookie, pages = set(), 0, 0
        while True:
            listing = page(f"arena_{pages}", cookie, 1024 * 1024)
            require(listing.entries, "arena-limited page made no progress")
            for entry in listing.entries:
                require(entry.name in wide_expected and entry.name not in seen and entry.cookie > cookie,
                        "arena-limited page lost its continuation position")
                seen.add(entry.name)
                cookie = entry.cookie
            pages += 1
            if listing.eof:
                break
            require(pages < 10, "arena-limited pagination did not terminate")
        require(seen == wide_expected and pages > 1, "large fixture skipped exports or never filled the arena")
        for name in wide:
            remove(name)

        # The newer valid entry precedes the missing one in the export list.
        # A later failed lookup must discard the entire page and stop mutation.
        _, guard, guard_sid = p.create("pseudo-suffix-guard")
        p.call([op.putfh(guard), op.write(guard_sid, 0, FILE_SYNC4, b"unchanged")])
        add("/missing", "/share/missing-pseudo-target")
        add("/prefix")
        result = p.call([op.putrootfh(), op.readdir(0, b"", 65536, 65536, mask),
                         op.putfh(guard), op.write(guard_sid, 0, FILE_SYNC4, b"BAD")],
                        "pseudo_failed_prefix", NFS4ERR_NOENT, runs=[(start - 1, 4)])
        require(len(result.resarray) == 2 and p.read(guard, guard_sid) == b"unchanged",
                "failed page published a partial reply or executed its WRITE suffix")
        remove("/prefix")
        remove("/missing")

        marker = b"retry-export-pseudo"
        p.call([op.putfh(p.directory), op.create(createtype4(NF4DIR), marker, {FATTR4_MODE: 0o777})])
        p.directories.append(marker)
        add("/moving-page", "/share/" + marker.decode())
        changed = replace_export_during_finish(p, "/moving-page",
            [op.putrootfh(), op.readdir(0, b"", 65536, 65536, mask), op.getfh()], [(start - 1, 3)],
            {"path": "/share"})
        # The accepted variant has no gate; avoid leaving a gated path around
        # when later requests run under the retry fixture.
        if changed:
            entries = page("replacement_fresh").entries
            moved = next(e for e in entries if e.name == b"moving-page")
            require(moved.attrs[FATTR4_FILEID] == share_attrs[FATTR4_FILEID],
                    "fresh enumeration retained a replaced export's old inode")
        remove("/moving-page")

        # A real root installed while the virtual page awaits finish must
        # invalidate the entire retried prefix, including PUTROOTFH itself.
        add("/pending-root", "/share/" + marker.decode())
        changed = replace_export_during_finish(p, "/",
            [op.putrootfh(), op.readdir(0, b"", 65536, 65536, mask), op.getfh()],
            [(start - 1, 3)], {"path": "/share"}, label="pseudo_root_added", results=1, new_export=True)
        if changed:
            try:
                fresh = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
                require(fresh != root, "fresh root selection retained the obsolete virtual root")
                p.call([op.putfh(root), op.getfh()], expected=NFS4ERR_STALE)
            finally:
                export_config("DELETE", "/")
        remove("/pending-root")

        for name in list(created):
            remove(name)
        export_config("DELETE", "/share")
        share_removed = True
        empty = page("empty", maxcount=16)
        require(empty.eof and not empty.entries, "empty pseudo-root did not return EOF")
        page("empty_bad_cookie", cookie=3, expected=NFS4ERR_BAD_COOKIE)
    finally:
        if share_removed:
            export_config("POST", "/share", **original)
        for name in created:
            export_config("DELETE", name)
        sys.setrecursionlimit(previous_limit)


def explicit_export_transitions(p):
    """Credential groups preserve wire identity, saved cursors and policy fences."""
    base = int(p.args.minor != 0)
    root = p.directory
    dirname = b"group-exports"
    directory = p.call([op.putfh(root), op.create(createtype4(NF4DIR), dirname,
                       {FATTR4_MODE: 0o777}), op.getfh()]).resarray[-1].object
    p.directories.append(dirname)
    path = export_config("GET", "/")["path"].rstrip("/") + "/" + dirname.decode()
    configurations = {
        "/group-a": dict(path=path, squash="all", anonuid=12345, anongid=23456),
        "/group-b": dict(path=path, squash="all", anonuid=34567, anongid=45678),
        "/group-ro": dict(path=path, access="ro"),
    }
    handles = {}

    def call(operations, label, expected=NFS4_OK):
        return p.call(operations, "groups_" + label, expected, runs=[(base, len(operations))])

    try:
        for export, config in configurations.items():
            export_config("POST", export, **config)
            handles[export] = p.call([op.putrootfh(), op.lookup(export[1:].encode()), op.getfh()]).resarray[-1].object
        a, b, ro = (handles[key] for key in configurations)
        owner_mask = (1 << FATTR4_OWNER) | (1 << FATTR4_OWNER_GROUP)
        result = call([op.putfh(a), op.savefh(), op.create(createtype4(NF4DIR), b"a", {FATTR4_MODE: 0o700}),
                       op.getattr(owner_mask), op.getfh(), op.putfh(b),
                       op.create(createtype4(NF4DIR), b"b", {FATTR4_MODE: 0o755}), op.getattr(owner_mask),
                       op.putrootfh(), op.getfh(), op.restorefh(), op.getfh(),
                       op.putfh(directory), op.create(createtype4(NF4DIR), b"normal", {}),
                       op.getattr(owner_mask)], "squash_and_restore")
        for index, uid, gid in ((3, b"12345", b"23456"), (7, b"34567", b"45678"), (14, b"0", b"0")):
            attrs = result.resarray[index].obj_attributes
            require(attrs[FATTR4_OWNER] == uid and attrs[FATTR4_OWNER_GROUP] == gid,
                    "credential transition retained an earlier squash")
        require(result.resarray[9].object == root and result.resarray[11].object == a,
                "root selection or saved export lost its signed handle")
        runtime = call([op.putfh(root), op.lookup(b"group-a"),
                        op.create(createtype4(NF4DIR), b"runtime-a", {FATTR4_MODE: 0o700}),
                        op.getattr(owner_mask), op.savefh(), op.putfh(root), op.lookup(b"group-b"),
                        op.create(createtype4(NF4DIR), b"runtime-b", {}), op.getattr(owner_mask),
                        op.restorefh(), op.getattr(owner_mask), op.putfh(root), op.getattr(owner_mask)],
                       "runtime_squash_and_saved")
        for index, uid in ((3, b"12345"), (8, b"34567"), (10, b"12345"), (12, b"0")):
            require(runtime.resarray[index].obj_attributes[FATTR4_OWNER] == uid,
                    "runtime junction leaked another group's credential")
        call([op.putfh(root), op.lookup(b"group-ro"),
              op.create(createtype4(NF4DIR), b"forbidden-runtime", {})], "runtime_ro", NFS4ERR_ROFS)
        p.call([op.putfh(directory), op.lookup(b"forbidden-runtime")], expected=NFS4ERR_NOENT)
        private_a = result.resarray[4].object
        private_b = p.call([op.putfh(b), op.lookup(b"a"), op.getfh()]).resarray[-1].object
        mask = (1 << FATTR4_FILEHANDLE) | (1 << FATTR4_ACL)
        result = call([op.putfh(a), op.getattr(mask), op.verify({FATTR4_FILEHANDLE: a}),
                       op.readdir(0, b"", 4096, 8192, (1 << FATTR4_FILEHANDLE)),
                       op.putfh(b), op.getattr(mask), op.verify({FATTR4_FILEHANDLE: b}),
                       op.readdir(0, b"", 4096, 8192, (1 << FATTR4_FILEHANDLE)),
                       op.putpubfh(), op.getfh()], "staged_handles")
        require(result.resarray[1].obj_attributes[FATTR4_FILEHANDLE] == a and
                result.resarray[5].obj_attributes[FATTR4_FILEHANDLE] == b,
                "staged GETATTR used another group's export identity")
        for index, expected in ((3, private_a), (7, private_b)):
            entries = {entry.name: entry.attrs[FATTR4_FILEHANDLE] for entry in result.resarray[index].reply.entries}
            require(entries[b"a"] == expected, "staged READDIR used another group's export identity")
        result = call([op.putfh(private_a), op.access(ACCESS4_READ | ACCESS4_LOOKUP),
                       op.putfh(private_b), op.access(ACCESS4_READ | ACCESS4_LOOKUP),
                       op.putrootfh(), op.getfh()], "access_identity")
        require(result.resarray[1].access == ACCESS4_READ | ACCESS4_LOOKUP and result.resarray[3].access == 0,
                "ACCESS authorized a different export's anonymous user")
        _, streambase, _ = p.create("groups-stream-base")
        result = call([op.putfh(streambase), op.savefh(), op.openattr(True), op.savefh(), op.getfh(),
                       op.putfh(b), op.getfh(), op.restorefh(), op.getfh(), op.putpubfh(), op.getfh()],
                      "saved_attrdir")
        require(result.resarray[4].object == result.resarray[8].object and result.resarray[6].object == b and
                result.resarray[-1].object == root, "group reset lost the saved attribute-directory marker")
        for operation, label in ((op.link(b"forbidden"), "link"), (op.rename(b"a", b"forbidden"), "rename")):
            result = call([op.putfh(ro), op.savefh(), op.putfh(directory), operation,
                           op.create(createtype4(NF4DIR), b"forbidden-suffix", {})], "saved_ro_" + label, NFS4ERR_ROFS)
            require(len(result.resarray) == 4, "read-only saved source allowed a mutation suffix")
        call([op.putfh(directory), op.putfh(ro), op.create(createtype4(NF4DIR), b"forbidden", {}),
              op.getfh()], "current_ro", NFS4ERR_ROFS)
        p.call([op.putfh(directory), op.lookup(b"forbidden")], expected=NFS4ERR_NOENT)
        p.call([op.putfh(directory), op.lookup(b"forbidden-suffix")], expected=NFS4ERR_NOENT)
        if os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE"):
            marker = b"retry-junction-export-groups"
            p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker, {})])
            p.directories.append(marker)
            for label, changes in (("squash", dict(squash="all", anonuid=56789, anongid=67890)),
                                   ("security", dict(sec=["krb5"])), ("readonly", dict(access="ro"))):
                config = dict(export_config("GET", "/group-a"))
                config.pop("name")
                replacement = dict(config, **changes)
                replace_export_during_finish(p, "/group-a", [op.putfh(root), op.lookup(marker),
                    op.putfh(a), op.getattr(mask), op.putfh(root), op.getfh()], [(base, 6)], replacement,
                    "group_" + label, results=3)
                export_config("DELETE", "/group-a")
                export_config("POST", "/group-a", **config)
                replace_export_during_finish(p, "/group-a", [op.putfh(root), op.lookup(marker),
                    op.lookupp(), op.lookup(b"group-a"), op.getattr(mask), op.putfh(root), op.getfh()],
                    [(base, 7)], replacement, "runtime_group_" + label, results=4)
                export_config("DELETE", "/group-a")
                export_config("POST", "/group-a", **config)
        config = dict(export_config("GET", "/group-a"))
        config.pop("name")
        export_config("DELETE", "/group-a")
        export_config("POST", "/group-a", **dict(config, sec=["krb5"]))
        result = call([op.putfh(directory), op.getfh(), op.putfh(a), op.getfh()],
                      "wrongsec_prefix", NFS4ERR_WRONGSEC)
        require(len(result.resarray) == 3 and result.resarray[1].object == directory,
                "WRONGSEC changed the accepted prefix or allowed a suffix")
        call([op.putfh(root), op.lookup(b"group-a"),
              op.create(createtype4(NF4DIR), b"forbidden-security", {})],
             "runtime_wrongsec", NFS4ERR_WRONGSEC)
        p.call([op.putfh(directory), op.lookup(b"forbidden-security")], expected=NFS4ERR_NOENT)
    finally:
        for name in (b"a", b"b", b"normal", b"runtime-a", b"runtime-b"):
            p.call([op.putfh(directory), op.remove(name)], expected=NFS4_OK)
        for export in configurations:
            export_config("DELETE", export)


def junction_queries(p):
    """Known-root entries and runtime SECINFO share one accepted compound."""
    base = int(p.args.minor != 0)
    root = p.directory
    dirname, linkname = b"junction-fixtures", b"junction-final-link"
    directory = p.call([op.putfh(root), op.create(createtype4(NF4DIR), dirname,
                       {FATTR4_MODE: 0o777}), op.getfh()]).resarray[-1].object
    p.directories.append(dirname)
    link = p.call([op.putfh(root), op.create(createtype4(NF4LNK, linkdata=dirname), linkname, {}),
                   op.getfh()]).resarray[-1].object
    p.files.append((linkname, link, None))
    path = export_config("GET", "/")["path"].rstrip("/")
    configurations = {
        "/junction-a": dict(path=path + "/" + dirname.decode()),
        "/junction-b": dict(path=path + "/" + dirname.decode()),
        "/junction-krb": dict(path=path + "/missing-junction-target", sec=["krb5"]),
        "/junction-empty": dict(path="/"),
        "/junction-link": dict(path=path + "/" + linkname.decode()),
    }

    def call(operations, label, expected=NFS4_OK):
        return p.call(operations, "junctions_" + label, expected, runs=[(base, len(operations))])

    try:
        for export, config in configurations.items():
            export_config("POST", export, **config)
        result = call([op.putrootfh(), op.savefh(), op.lookup(b"junction-a"), op.getfh(),
                       op.restorefh(), op.getfh(), op.lookup(b"junction-b"), op.getfh(),
                       op.putpubfh(), op.lookup(b"junction-a"), op.getfh()], "saved_root_entries")
        require(result.resarray[3].object == result.resarray[10].object != result.resarray[7].object and
                result.resarray[5].object == root, "junction groups lost export identity or saved root")
        operations = [op.putrootfh(), op.savefh(), op.secinfo(b"junction-krb")]
        if p.args.minor:
            operations.append(op.restorefh())
        operations += [op.getfh(), op.lookup(b"junction-a"), op.getfh()]
        result = call(operations, "security_without_backing_lookup")
        require([item.flavor for item in result.resarray[2].resok4] == [6] and
                result.resarray[-3].object == root,
                "SECINFO entered an inaccessible export or lost its directory")
        if p.args.minor:
            call([op.putrootfh(), op.secinfo(b"junction-krb"), op.getfh()],
                 "security_consumes_cursor", NFS4ERR_NOFILEHANDLE)
        call([op.putrootfh(), op.lookup(b"junction-krb"),
              op.create(createtype4(NF4DIR), b"forbidden-junction", {})], "wrongsec", NFS4ERR_WRONGSEC)
        p.call([op.putfh(root), op.lookup(b"forbidden-junction")], expected=NFS4ERR_NOENT)
        call([op.putrootfh(), op.lookup(b"junction-empty"), op.getfh()], "empty_path", NFS4ERR_NOENT)
        result = call([op.putrootfh(), op.lookup(b"junction-link"), op.getattr(1 << FATTR4_TYPE)],
                      "final_symlink")
        require(result.resarray[-1].obj_attributes[FATTR4_TYPE] == NF4LNK,
                "junction entry unexpectedly followed its final symlink")
        p.call([op.putfh(directory), op.create(createtype4(NF4DIR), b"junction-krb", {})])
        try:
            result = call([op.putrootfh(), op.lookup(b"junction-a"), op.secinfo(b"junction-krb"),
                           op.putpubfh(), op.getfh()], "ordinary_name_policy")
            require([item.flavor for item in result.resarray[2].resok4] == [1],
                    "ordinary descendant SECINFO selected a same-named junction policy")
        finally:
            p.call([op.putfh(directory), op.remove(b"junction-krb")])
        if os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE"):
            marker = b"retry-junction-security-group"
            p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker, {})])
            p.directories.append(marker)
            replace_export_during_finish(p, "/junction-krb",
                [op.putrootfh(), op.lookup(marker), op.lookupp(), op.secinfo(b"junction-krb"),
                 op.putrootfh(), op.getfh()], [(base, 6)],
                dict(configurations["/junction-krb"], sec=["sys"]), "junction_security", results=4)
    finally:
        for export in configurations:
            export_config("DELETE", export)


def runtime_junction_retry(p):
    """A rejected attempt may resolve the same LOOKUP as a different branch."""
    gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
    if not gate:
        return
    base = int(p.args.minor != 0)
    parent = p.directory
    original = export_config("GET", "/")
    original.pop("name")
    name, retired, replacement = b"retry-export-runtime", b"runtime-retired", b"runtime-replacement"
    old_root = p.call([op.putfh(parent), op.create(createtype4(NF4DIR), name, {}),
                       op.getfh()]).resarray[-1].object
    physical = p.call([op.putfh(old_root), op.create(createtype4(NF4DIR), b"share", {}),
                       op.getfh()]).resarray[-1].object
    sibling = p.call([op.putrootfh(), op.lookup(b"share"), op.getfh()]).resarray[-1].object
    # A separate v4.0 transport can mutate directories without client/open
    # state, while the measured v4.0/v4.2 request waits at backend finish.
    peer = nfs4client.NFS4Client(p.args.host, p.args.port, 0)
    peer.set_cred(AuthSys().init_cred(uid=0, gid=0, name=b"runtime-root-peer"))
    ready, release = Path(gate + ".ready"), Path(gate + ".release")
    try:
        export_config("DELETE", "/")
        export_config("POST", "/", **dict(original, path=original["path"].rstrip("/") + "/" + name.decode()))
        for label, changes, expected in (
            ("junction_to_physical", [op.rename(name, retired), op.create(createtype4(NF4DIR), name, {})], physical),
            ("physical_to_junction", [op.rename(name, replacement), op.rename(retired, name)], sibling),
        ):
            ready.unlink(missing_ok=True)
            release.unlink(missing_ok=True)
            failures = []

            def replace_root():
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists():
                        require(time.monotonic() < deadline, "runtime LOOKUP never reached pending finish")
                        time.sleep(0.01)
                    result = peer.compound([op.putfh(parent), op.savefh()] + changes, version=0)
                    require(result.status == NFS4_OK, "could not replace the root's backing directory")
                except BaseException as error:
                    failures.append(error)
                finally:
                    release.write_text("reject")

            worker = threading.Thread(target=replace_root)
            worker.start()
            try:
                result = p.call([op.putfh(old_root), op.savefh(), op.putrootfh(), op.restorefh(),
                                 op.lookup(b"share"), op.getfh()], "runtime_" + label, runs=[(base, 6)])
                require(result.resarray[-1].object == expected,
                        "retry retained the first attempt's branch or export identity")
            finally:
                worker.join(timeout=12)
            require(not worker.is_alive(), "root replacement worker did not finish")
            if failures:
                raise failures[0]
    finally:
        export_config("DELETE", "/")
        export_config("POST", "/", **original)
        p.call([op.putfh(old_root), op.remove(b"share")])
        for entry in (name, retired, replacement):
            result = peer.compound([op.putfh(parent), op.remove(entry)], version=0)
            require(result.status in (NFS4_OK, NFS4ERR_NOENT), "runtime root fixture cleanup failed")


def namespace_coalescing(p):
    """Private cursor checks and accepted-prefix ownership across a crossing."""
    base = int(p.args.minor != 0)
    root = p.directory
    explicit_export_transitions(p)
    junction_queries(p)
    runtime_junction_retry(p)
    dirname = b"coalesced-parent"
    result = p.call([op.putfh(root), op.create(createtype4(NF4DIR), dirname, {}), op.getfh()])
    directory = result.resarray[-1].object
    p.directories.append(dirname)
    result = p.call([op.putfh(directory), op.create(createtype4(NF4DIR), b"share", {}), op.getfh()])
    child = result.resarray[-1].object
    name, fh, sid = p.create("coalesced-state")

    def call(operations, label, expected=NFS4_OK, runs=None):
        return p.call(operations, "coalesced_" + label, expected,
                      runs=[(base + start, count) for start, count in
                            (runs if runs is not None else [(0, len(operations))])])

    try:
        sibling = p.call([op.putrootfh(), op.lookup(b"share"), op.getfh()]).resarray[-1].object
        result = call([op.putfh(sibling), op.savefh(), op.lookupp(), op.getfh(),
                       op.getattr(1 << FATTR4_FILEHANDLE), op.lookup(b"share"), op.getfh(),
                       op.restorefh(), op.lookupp(), op.getfh()], "mount_parent_roundtrip")
        require(result.resarray[3].object == result.resarray[9].object == root and
                result.resarray[4].obj_attributes[FATTR4_FILEHANDLE] == root and
                result.resarray[6].object == sibling,
                "mount parent failed to update current/saved export identity")
        result = call([op.putrootfh(), op.getfh(), op.lookup(dirname), op.putpubfh(), op.getfh(),
                       op.lookup(name), op.putrootfh(), op.getfh()], "repeated_root")
        require([result.resarray[i].object for i in (1, 4, 7)] == [root] * 3,
                "repeated root entries lost the namespace root")
        result = call([op.putfh(fh), op.savefh(), op.putpubfh(), op.getfh(), op.restorefh(),
                       op.getfh(), op.read(sid, 0, 64), op.putrootfh(), op.getfh()], "root_saved_file")
        require(result.resarray[3].object == result.resarray[-1].object == root and
                result.resarray[5].object == fh, "root entry overwrote the saved filehandle")
        result = call([op.putfh(fh), op.openattr(True), op.savefh(), op.getfh(), op.putpubfh(),
                       op.getfh(), op.restorefh(), op.getfh(), op.putrootfh(), op.getfh()], "root_attrdir")
        require(result.resarray[3].object == result.resarray[7].object != fh and
                result.resarray[5].object == result.resarray[-1].object == root,
                "root reset lost the saved attribute directory or retained its marker")
        result = call([op.putfh(root), op.secinfo(dirname), op.putrootfh(), op.getfh(),
                       op.lookup(name), op.read(sid, 0, 64)], "root_after_secinfo")
        require(result.resarray[3].object == root, "root entry did not replace the SECINFO cursor")
        if p.args.minor:
            result = call([op.putrootfh(), p.open_op(name, create=False), op.savefh(), op.putpubfh(),
                           op.restorefh(), op.read(CURRENT, 0, 64), op.getfh()], "root_saved_stateid")
            sid = result.resarray[1].stateid
            p.replace_stateid(fh, sid)
            require(result.resarray[-1].object == fh, "root reset lost the saved current stateid")
            result = call([op.putrootfh(), p.open_op(name, create=False), op.putpubfh(),
                           op.read(CURRENT, 0, 64), op.getfh()], "root_clears_stateid", NFS4ERR_BAD_STATEID)
            sid = result.resarray[1].stateid
            p.replace_stateid(fh, sid)
            require(len(result.resarray) == 4, "root reset retained a current stateid or ran the suffix")
        result = call([op.putrootfh(), op.lookup(dirname), op.lookup(b"share"), op.getfh(),
                       op.lookupp(), op.getfh(), op.lookupp(), op.getfh()], "moving_parent")
        require([result.resarray[i].object for i in (3, 5, 7)] == [child, directory, root],
                "coalesced traversal confused a descendant name with an export junction")
        result = call([op.putfh(child), op.savefh(), op.lookupp(), op.getfh(), op.restorefh(),
                       op.lookupp(), op.lookupp(), op.getfh()], "saved_parent")
        require(result.resarray[3].object == directory and result.resarray[-1].object == root,
                "LOOKUPP did not follow the execution cursor after RESTOREFH")
        operations = [op.putrootfh(), op.lookup(dirname), op.savefh(), op.secinfo(b"share")]
        if p.args.minor:
            operations += [op.restorefh()]
        operations += [op.getfh(), op.lookupp(), op.getfh()]
        result = call(operations, "moving_secinfo")
        require(result.resarray[-3].object == directory and result.resarray[-1].object == root and
                result.resarray[3].resok4, "SECINFO lost its version-specific cursor semantics")
        # A large requested page must retain the runtime-discovered LOOKUP
        # in the same plan, without an empty deferred compound.
        result = call([op.putrootfh(), op.readdir(0, b"", 4096, 1024 * 1024, 1 << FATTR4_TYPE),
                       op.lookup(b"share"), op.getfh()], "empty_prefix",
                      runs=[(0, 4)])
        sibling = p.call([op.putrootfh(), op.lookup(b"share"), op.getfh()]).resarray[-1].object
        require(result.resarray[-1].object == sibling, "empty prefix failed to resume its junction")

        # OPEN, both WRITEs and the saved stateid cross the runtime junction
        # inside one compound. Publication occurs after the whole run finishes.
        opened = p.open_op(name, create=False)
        predicted = stateid4(sid.seqid + 1, sid.other) if not p.args.minor else CURRENT
        prefix, suffix = b"accepted-prefix/", b"shared-suffix"
        operations = [op.putrootfh(), opened, op.write(predicted, 0, FILE_SYNC4, prefix),
                      op.savefh(), op.putfh(root), op.lookup(dirname), op.lookupp(),
                      op.lookup(b"share"), op.restorefh(),
                      op.write(predicted, len(prefix), FILE_SYNC4, suffix),
                      op.read(predicted, 0, 128), op.getfh()]
        result = call(operations, "state_and_write_boundary", runs=[(0, 12)])
        sid = result.resarray[1].stateid
        p.replace_stateid(fh, sid)
        if not p.args.minor:
            p.owner_seqids[p.file_owners[fh]] += 1
            replay = call(operations, "owner_replay_boundary", runs=[(0, 12)])
            require(repr(replay.resarray[1]) == repr(result.resarray[1]),
                    "accepted namespace prefix lost its OPEN owner replay")
        require(result.resarray[2].count == len(prefix) and result.resarray[9].count == len(suffix) and
                result.resarray[10].data == prefix + suffix and result.resarray[11].object == fh,
                "namespace crossing lost its WRITE payload, stateid or saved filehandle")
        result = call([op.putrootfh(), op.lookup(dirname), op.lookupp(), op.lookupp(),
                       op.lookup(b"share"), op.putfh(fh), op.write(sid, 0, FILE_SYNC4, b"BAD")],
                      "parentless_veto", NFS4ERR_NOENT)
        require(len(result.resarray) == 4 and p.read(fh, sid) == prefix + suffix,
                "namespace-root LOOKUPP ran a junction or mutation suffix")
        call([op.putfh(fh), op.lookupp(), op.getfh()], "file_parent", NFS4ERR_NOTDIR)
        gate = os.environ.get("CHIMERA_COMPOUND_JUNCTION_GATE")
        if gate:
            marker = b"retry-junction-coalesced"
            p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker, {})])
            p.directories.append(marker)
            ready, release = Path(gate + ".ready"), Path(gate + ".release")
            for finish in ("reject", "error"):
                ready.unlink(missing_ok=True)
                release.unlink(missing_ok=True)
                failures = []

                def complete_finish():
                    try:
                        deadline = time.monotonic() + 5
                        while not ready.exists():
                            require(time.monotonic() < deadline, "deferred prefix never reached finish")
                            time.sleep(0.01)
                    except BaseException as error:
                        failures.append(error)
                    finally:
                        pending = Path(gate + ".pending")
                        pending.write_text(finish)
                        pending.replace(release)

                worker = threading.Thread(target=complete_finish)
                worker.start()
                try:
                    result = call([op.putfh(root), op.lookup(marker), op.lookupp(),
                                   op.lookup(b"share"), op.getfh()], "finish_" + finish,
                                  NFS4ERR_IO if finish == "error" else NFS4_OK,
                                  runs=[(0, 5)])
                    if finish == "error":
                        require(len(result.resarray) == 1, "rejected finish published its namespace prefix")
                finally:
                    worker.join(timeout=12)
                require(not worker.is_alive(), "deferred finish worker did not exit")
                if failures:
                    raise failures[0]
            release.write_text("reject")
        # Replacing the directory at the same export path does not change the
        # export configuration or invalidate its warm root cache. PUTROOTFH's
        # newly resolved cursor must take precedence within its own attempt.
        original = export_config("GET", "/")
        original.pop("name")
        cache_name, old_name = b"coalesced-cache-root", b"coalesced-cache-old"
        p.call([op.putfh(root), op.create(createtype4(NF4DIR), cache_name, {})])
        p.directories.append(cache_name)
        try:
            export_config("DELETE", "/")
            export_config("POST", "/", **dict(original, path=original["path"].rstrip("/") + "/" + cache_name.decode()))
            p.call([op.putrootfh(), op.getfh()])
            p.call([op.putfh(root), op.savefh(), op.rename(cache_name, old_name),
                    op.create(createtype4(NF4DIR), cache_name, {})])
            p.directories.append(old_name)
            call([op.putrootfh(), op.lookupp(), op.getfh()], "replaced_root_parent", NFS4ERR_NOENT)
            old_name = b"coalesced-cache-old-junction"
            p.call([op.putfh(root), op.savefh(), op.rename(cache_name, old_name),
                    op.create(createtype4(NF4DIR), cache_name, {})])
            p.directories.append(old_name)
            result = call([op.putrootfh(), op.lookup(b"share"), op.getfh()], "replaced_root_junction",
                          runs=[(0, 3)])
            require(result.resarray[-1].object == sibling, "new root exposed its physical junction entry")
            # The second root entry must resolve after the RENAME/CREATE,
            # and LOOKUPP must compare against that result, not the first one.
            old_name = b"coalesced-cache-old-in-compound"
            result = call([op.putrootfh(), op.getfh(), op.putfh(root), op.savefh(),
                           op.rename(cache_name, old_name), op.create(createtype4(NF4DIR), cache_name, {}),
                           op.getfh(), op.putpubfh(), op.getfh(), op.lookupp(), op.getfh()],
                          "root_replaced_in_compound", NFS4ERR_NOENT)
            p.directories.append(old_name)
            require(len(result.resarray) == 10 and
                    result.resarray[1].object != result.resarray[6].object == result.resarray[8].object,
                    "repeated root entry reused an earlier resolved path or allowed its parent")
            link_name = b"coalesced-root-alias"
            p.call([op.putfh(root), op.create(createtype4(NF4LNK, linkdata=cache_name), link_name, {})])
            export_config("DELETE", "/")
            export_config("POST", "/", **dict(original, path=original["path"].rstrip("/") + "/" + link_name.decode()))
            physical = p.call([op.putrootfh(), op.create(createtype4(NF4DIR), b"share", {}),
                               op.getfh()]).resarray[-1].object
            try:
                result = call([op.putrootfh(), op.savefh(), op.putfh(root), op.remove(link_name),
                               op.create(createtype4(NF4LNK, linkdata=old_name), link_name, {}),
                               op.putpubfh(), op.restorefh(), op.lookup(b"share"), op.getfh()],
                              "saved_older_root")
                require(result.resarray[-1].object == physical,
                        "saved older root was mistaken for the latest resolved namespace root")
            finally:
                p.call([op.putfh(root), op.remove(link_name)])
                p.call([op.putfh(root), op.lookup(cache_name), op.remove(b"share")])
        finally:
            export_config("DELETE", "/")
            export_config("POST", "/", **original)
            p.call([op.putrootfh(), op.getfh()])
        if gate:
            marker = b"retry-export-root-reset"
            p.call([op.putfh(root), op.create(createtype4(NF4DIR), marker, {FATTR4_MODE: 0o777}), op.getfh()])
            p.directories.append(marker)
            for label, replacement in (("path", {"path": "/share"}),
                                       ("identity", {"squash": "all", "anonuid": 12345, "anongid": 23456}),
                                       ("security", {"sec": ["krb5"]})):
                try:
                    export_config("DELETE", "/")
                    export_config("POST", "/", **dict(original, path=original["path"].rstrip("/") + "/" + marker.decode()))
                    replacement = dict(export_config("GET", "/"), **replacement)
                    replacement.pop("name")
                    replace_export_during_finish(p, "/", [op.putfh(fh), op.putpubfh(), op.getfh()],
                                                 [(base, 3)], replacement, "root_reset_" + label, results=1)
                finally:
                    export_config("DELETE", "/")
                    export_config("POST", "/", **original)
            p.call([op.putrootfh(), op.getfh()])
    finally:
        p.call([op.putfh(directory), op.remove(b"share")])


def namespace(p):
    sibling = p.directory
    root = p.call([op.putrootfh(), op.getfh()]).resarray[-1].object
    p.directory = root
    namespace_coalescing(p)
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
    result = p.call([op.putfh(directory), op.lookupp(), op.getfh()],
                    "root_descendant_lookupp", runs=[(1, 3)])
    require(result.resarray[-1].object == root, "ordinary LOOKUPP under a root export lost its parent")
    result = measured(p, [op.putfh(root), op.lookup(dirname), op.lookup(b"ordinary-missing"), op.getfh()],
                      "root_nested_missing_prefix", NFS4ERR_NOENT)
    require(len(result.resarray) == 3, "failed nested lookup did not stop its GETFH suffix")
    result = measured(p, [op.putfh(directory), op.lookup(b"ordinary-missing"), op.getfh()],
                      "root_descendant_missing", NFS4ERR_NOENT)
    require(len(result.resarray) == 2, "missing descendant lookup ran a suffix")

    # A descendant may contain a name that also names a root export. The
    # execution checkpoint must distinguish this directory from the root
    # without ending the compound to inspect its cursor.
    result = p.call([op.putfh(directory), p.open_op(b"share"), op.getfh()])
    nested_fh, nested_sid = result.resarray[-1].object, result.resarray[1].stateid
    result = p.call([op.putfh(directory), op.lookup(b"share"), op.getfh()],
                    "descendant_lookup_export_name", runs=[(1, 3)])
    require(result.resarray[-1].object == nested_fh, "descendant LOOKUP crossed an unrelated export")
    result = p.call([op.putfh(directory), op.secinfo(b"share"), op.getfh()],
                    "descendant_secinfo_export_name", NFS4ERR_NOFILEHANDLE,
                    runs=[(1, 3)])
    require(result.resarray[1].resok4, "descendant SECINFO lost its export policy")
    p.call([op.putfh(nested_fh), op.close(0, nested_sid)])
    p.call([op.putfh(directory), op.remove(b"share")])

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
    export_entries(p, root, sibling, directory, name, fh, sid, payload)
    root_export_reconfiguration(p, root, sibling)
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
        elif os.environ["CHIMERA_COMPOUND_FEATURE"] == "cold_root":
            cold_root(p)
        elif os.environ["CHIMERA_COMPOUND_FEATURE"] == "pseudo":
            pseudo_root(p)
        elif os.environ["CHIMERA_COMPOUND_FEATURE"].startswith("readdir"):
            from nfs4_readdir_sizing import readdir_sizing
            readdir_sizing(p)
        else:
            proxy(p)
        p.check_trace()
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
