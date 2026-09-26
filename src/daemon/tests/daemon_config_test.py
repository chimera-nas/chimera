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

        def write_config(exports, extra_server=None):
            config = {
                "common": {"huge_pages": False, "sync_delegation_threads": 2},
                "server": {
                    "threads": 2, "nfs_enabled": True, "nfs_port": nfs_port,
                    "data_server": True, "external_portmap": True,
                    "rest_http_port": http_port, "rest_auth_enabled": False,
                    "rest_modules": [{"module": "core", "allow_public_routes": True}],
                    "metrics_port": 0, "state_dir": state.as_posix(),
                },
                "filesystems": {"fs0": {"module": "memfs"}},
                "mounts": {"data": {"module": "memfs", "path": "fs0"}},
                "exports": exports,
            }
            config["server"].update(extra_server or {})
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
        cases = [(name, {"/e": export}, {}) for name, export in bad_exports]
        cases.extend([
            ("duplicate export_id", {
                "/e1": {"path": "/data", "export_id": 7},
                "/e2": {"path": "/data", "export_id": 7},
            }, {}),
            ("nfs_max_exports zero", {"/e": {"path": "/data"}},
             {"nfs_max_exports": 0}),
        ])
        for name, exports, extra in cases:
            write_config(exports, extra)
            with subprocess.Popen(
                [daemon, "-c", str(config_path)], stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, env=env, creationflags=flags,
            ) as proc:
                try:
                    output, _ = proc.communicate(timeout=30)
                    if proc.returncode != 1:
                        raise AssertionError(
                            f"{name}: expected configuration rejection (exit 1), "
                            f"got {proc.returncode}\n{output.decode(errors='replace')}"
                        )
                    print(f"PASS: {name} rejected", flush=True)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.communicate()
                    remove_certificates(proc.pid)

        # Lexical ordering must not let an automatic ID steal the pinned ID.
        write_config({
            "/a_auto": {"path": "/data", "root_squash": True},
            "/b_pinned": {"path": "/data", "export_id": 1, "sec": ["krb5"]},
        })
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
                            f"http://127.0.0.1:{http_port}/api/core/v1/exports", timeout=2
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
                    f"http://127.0.0.1:{http_port}/api/core/v1/config", timeout=5
                ) as response:
                    config = json.load(response)
                assert config["exports"]["/b_pinned"]["sec"] == ["krb5"], config
                print("PASS: export IDs, squash and sec round-trip", flush=True)
                proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
                print("PASS: orderly daemon shutdown", flush=True)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))
                remove_certificates(proc.pid)


if __name__ == "__main__":
    main()
