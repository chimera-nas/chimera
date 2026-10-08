# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Validate daemon export configuration and its REST representation on every host."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def unused_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def main():
    daemon = sys.argv[1]
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    http_port, nfs_port = unused_ports(2)

    with tempfile.TemporaryDirectory(prefix="chimera-config-") as scratch:
        root = Path(scratch)
        state = root / "state"
        state.mkdir()
        env = os.environ.copy()
        env.update(TEMP=scratch, TMP=scratch)
        config_path = root / "config.json"

        def write_config(exports, extra_server=None, sections=None):
            config = {
                "common": {"huge_pages": False, "sync_delegation_threads": 2},
                "server": {
                    "threads": 2, "nfs_enabled": True, "nfs_port": nfs_port,
                    "data_server": True, "external_portmap": True,
                    "rest_http_port": http_port, "rest_auth_enabled": False,
                    "metrics_port": 0, "state_dir": state.as_posix(),
                },
                "filesystems": {"fs0": {"module": "memfs"}},
                "mounts": {"data": {"module": "memfs", "path": "fs0"}},
                "exports": exports,
            }
            config["server"].update(extra_server or {})
            # Whole top-level sections (filesystems, mounts) replaced by a case.
            config.update(sections or {})
            config_path.write_text(json.dumps(config), encoding="utf-8")

        def remove_certificates(pid):
            # On Windows TEMP points inside the test's temporary directory.
            if os.name != "nt":
                for suffix in ("crt", "key"):
                    Path(f"/tmp/chimera-rest-{pid}.{suffix}").unlink(missing_ok=True)

        # Preserve every malformed-config case from the original shell test.
        bad_exports = [
            ("legacy options", {"path": "/data", "options": "ro"}),
            ("invalid access", {"path": "/data", "access": "readonly"}),
            ("invalid squash", {"path": "/data", "squash": "rootsquash"}),
            ("string root_squash", {"path": "/data", "root_squash": "true"}),
            ("integer all_squash", {"path": "/data", "all_squash": 1}),
            ("string no_root_squash", {"path": "/data", "no_root_squash": "yes"}),
            ("export_id zero", {"path": "/data", "export_id": 0}),
            ("export_id overflow", {"path": "/data", "export_id": 65536}),
            ("non-integer export_id", {"path": "/data", "export_id": "abc"}),
            ("unknown sec flavor", {"path": "/data", "sec": ["krb5x"]}),
            ("non-array sec", {"path": "/data", "sec": "krb5"}),
            ("non-string sec entry", {"path": "/data", "sec": [5]}),
            ("missing path", {}),
            ("negative anonuid", {"path": "/data", "anonuid": -1}),
        ]
        # Each case: name, exports, server overrides, replaced top-level
        # sections, and the error line the rejection must log (None for the
        # export cases, which only check the exit status).
        cases = [(name, {"/e": export}, {}, None, None) for name, export in bad_exports]
        cases.extend([
            ("duplicate export_id", {
                "/e1": {"path": "/data", "export_id": 7},
                "/e2": {"path": "/data", "export_id": 7},
            }, {}, None, None),
            ("nfs_max_exports zero", {"/e": {"path": "/data"}},
             {"nfs_max_exports": 0}, None, None),
            # The mount names a memfs filesystem that was never declared, so
            # it fails; serving /e over the missing root must not be the
            # fallback.
            ("failed mount", {"/e": {"path": "/data"}}, {},
             {"mounts": {"data": {"module": "memfs", "path": "fs_missing"}}},
             "Failed to mount memfs://fs_missing to /data"),
            # Rejected before the mount is attempted, which would otherwise
            # dereference the missing string.
            ("mount without module", {"/e": {"path": "/data"}}, {},
             {"mounts": {"data": {"path": "fs0"}}},
             "Mount data missing module"),
            ("mount without path", {"/e": {"path": "/data"}}, {},
             {"mounts": {"data": {"module": "memfs"}}},
             "Mount data missing path"),
            # A filesystem that cannot be created is fatal, and the exit must
            # be a clean 1: plain exit() here runs libevpl's atexit cleanup
            # under live service threads and aborts (134).
            ("mkfs unknown module", {"/e": {"path": "/data"}}, {},
             {"filesystems": {"fs0": {"module": "memfs"}, "bad": {"module": "memf"}}},
             "Failed to create filesystem bad in module memf"),
            # nfs is registered on every platform and holds no named
            # filesystems (linux, the obvious choice, exists only on Linux).
            ("mkfs unsupported by module", {"/e": {"path": "/data"}}, {},
             {"filesystems": {"fs0": {"module": "memfs"}, "bad": {"module": "nfs"}}},
             "Failed to create filesystem bad in module nfs"),
            # Malformed even though no mount names it, so it is not caught
            # later as a failed mount.
            ("filesystem without module", {"/e": {"path": "/data"}}, {},
             {"filesystems": {"fs0": {"module": "memfs"}, "orphan": {}}},
             "Filesystem orphan missing module"),
            # smb_multichannel must be an array of interface objects. An object
            # here used to be ignored silently, so multichannel was never
            # advertised and nothing said why.
            ("smb_multichannel object", {"/e": {"path": "/data"}},
             {"smb_multichannel": {"address": "127.0.0.1", "speed": 10}}, None,
             "smb_multichannel must be an array"),
            ("smb_multichannel non-object entry", {"/e": {"path": "/data"}},
             {"smb_multichannel": ["127.0.0.1"]}, None,
             "interface 0 is not an object"),
            # The daemon keeps the interfaces in a fixed 16-entry array.
            ("smb_multichannel too many interfaces", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": "127.0.0.1", "speed": 10}] * 17},
             None, "at most 16 interfaces"),
            # rss and rdma are booleans; a string would silently read as false.
            ("smb_multichannel string rss", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": "127.0.0.1", "speed": 10, "rss": "true"}]},
             None, "rss and rdma on interface 0 must be booleans"),
            ("smb_multichannel integer rdma", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": "127.0.0.1", "speed": 10, "rdma": 1}]},
             None, "rss and rdma on interface 0 must be booleans"),
            # speed is an integer in Gbps, 1..1000000. The upper bound is a
            # sanity cap rather than an overflow limit: 10 GbE typed in bps
            # (10^10) is out of range even though 10^10 * 10^9 still fits
            # the 64-bit LinkSpeed.
            ("smb_multichannel speed in bps", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": "127.0.0.1", "speed": 10000000000}]},
             None, "speed on interface 0 must be an integer in Gbps"),
            ("smb_multichannel missing speed", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": "127.0.0.1"}]},
             None, "speed on interface 0 must be an integer in Gbps"),
        ])
        # Each side of the speed range and each non-integer form.
        for label, speed in [("zero", 0), ("above cap", 1000001), ("negative", -1),
                             ("real", 10.0), ("string", "10")]:
            cases.append((f"smb_multichannel speed {label}", {"/e": {"path": "/data"}},
                          {"smb_multichannel": [{"address": "127.0.0.1", "speed": speed}]},
                          None, "speed on interface 0 must be an integer in Gbps"))
        cases.extend([
            ("smb_multichannel missing address", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"speed": 10}]},
             None, "address on interface 0 must be a string"),
            ("smb_multichannel integer address", {"/e": {"path": "/data"}},
             {"smb_multichannel": [{"address": 5, "speed": 10}]},
             None, "address on interface 0 must be a string"),
        ])
        for name, exports, extra, sections, expect in cases:
            write_config(exports, extra, sections)
            with subprocess.Popen(
                [daemon, "-c", str(config_path)], stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, env=env, creationflags=flags,
            ) as proc:
                try:
                    try:
                        output, _ = proc.communicate(timeout=30)
                    except subprocess.TimeoutExpired as exc:
                        raise AssertionError(
                            f"{name}: expected configuration rejection (exit 1), "
                            f"but the daemon was still running after 30s"
                        ) from exc
                    text = output.decode(errors="replace")
                    if proc.returncode != 1:
                        raise AssertionError(
                            f"{name}: expected configuration rejection (exit 1), "
                            f"got {proc.returncode}\n{text}"
                        )
                    # A sanitizer report also exits 1 in Debug builds, so the
                    # status alone does not show the config was rejected.
                    if "Sanitizer" in text:
                        raise AssertionError(f"{name}: sanitizer report\n{text}")
                    if expect is not None and expect not in text:
                        raise AssertionError(
                            f"{name}: rejected without logging {expect!r}\n{text}"
                        )
                    print(f"PASS: {name} rejected", flush=True)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.communicate()
                    remove_certificates(proc.pid)

        # A full smb_multichannel list at every accepted boundary must start:
        # 16 interfaces, speeds 1 and 1000000, rss both ways and one IPv6
        # address. SMB stays disabled and rdma false, so nothing listens on
        # these addresses; only the parse is exercised.
        smb_nics = [
            {"address": f"192.0.2.{n + 1}", "speed": 10, "rss": n % 2 == 0, "rdma": False}
            for n in range(16)
        ]
        smb_nics[0]["speed"] = 1
        smb_nics[1]["speed"] = 1000000
        smb_nics[15]["address"] = "2001:db8::1"

        # Lexical ordering must not let an automatic ID steal the pinned ID.
        write_config({
            "/a_auto": {"path": "/data", "root_squash": True},
            "/b_pinned": {"path": "/data", "export_id": 1, "sec": ["krb5"]},
        }, {"smb_multichannel": smb_nics})
        with (root / "daemon.log").open("w+b") as log:
            proc = subprocess.Popen(
                [daemon, "-c", str(config_path)], stdout=log, stderr=log,
                env=env, creationflags=flags,
            )
            try:
                deadline = time.monotonic() + 60
                while True:
                    if proc.poll() is not None:
                        raise RuntimeError(f"valid config exited: {proc.returncode}")
                    try:
                        with opener.open(
                            f"http://127.0.0.1:{http_port}/api/v1/exports", timeout=2
                        ) as response:
                            body = json.load(response)
                        break
                    except (OSError, urllib.error.URLError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
                exports = {export["name"]: export for export in body}
                assert exports["/b_pinned"]["export_id"] == 1, body
                assert exports["/a_auto"]["export_id"] == 2, body
                assert exports["/a_auto"]["squash"] == "root", body
                assert exports["/b_pinned"]["sec"] == ["krb5"], body
                with opener.open(
                    f"http://127.0.0.1:{http_port}/api/v1/config", timeout=5
                ) as response:
                    config = json.load(response)
                assert config["exports"]["/b_pinned"]["sec"] == ["krb5"], config
                print("PASS: export IDs, squash and sec round-trip", flush=True)
                proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
                print("PASS: orderly daemon shutdown", flush=True)
                # Only the 1000000 Gbps interface reads like a Mbps-era value.
                log.seek(0)
                text = log.read().decode("utf-8", errors="replace")
                assert text.count("is above any current link rate") == 1, text
                assert "interface 1 speed 1000000 Gbps" in text, text
                print("PASS: smb_multichannel boundaries accepted", flush=True)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))
                remove_certificates(proc.pid)


if __name__ == "__main__":
    main()
