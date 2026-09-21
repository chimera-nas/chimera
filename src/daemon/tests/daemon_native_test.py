# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Exercise the real daemon's startup, certificate creation and orderly shutdown."""
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


def ports(count):
    listeners = [socket.socket() for _ in range(count)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


with tempfile.TemporaryDirectory(prefix="chimera-daemon-") as scratch:
    root = Path(scratch)
    http_port, https_port, metrics_port = ports(3)
    config = {
        "common": {"huge_pages": False, "sync_delegation_threads": 2},
        "server": {
            "threads": 2, "nfs_enabled": False, "smb_enabled": False,
            "s3_enabled": False, "state_dir": root.as_posix(),
            "rest_http_port": http_port, "rest_https_port": https_port,
            "metrics_port": metrics_port, "rest_auth_enabled": False,
        },
    }
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    env = os.environ.copy()
    env.update(TEMP=scratch, TMP=scratch)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with (root / "daemon.log").open("w+b") as log:
        proc = subprocess.Popen(
            [sys.argv[1], "-c", str(config_path)], stdout=log, stderr=log,
            env=env, creationflags=flags,
        )
        try:
            deadline = time.monotonic() + 60
            while True:
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                try:
                    with opener.open(
                        f"http://127.0.0.1:{http_port}/api/v1/exports", timeout=2
                    ) as response:
                        assert response.status == 200
                        json.load(response)
                    break
                except (OSError, urllib.error.URLError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
            with opener.open(f"http://127.0.0.1:{metrics_port}/metrics", timeout=5) as response:
                assert response.status == 200
                response.read()
            certificate_dir = root if os.name == "nt" else Path("/tmp")
            for suffix in ("crt", "key"):
                assert (certificate_dir / f"chimera-rest-{proc.pid}.{suffix}").stat().st_size > 0
            proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
            assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            log.seek(0)
            print(log.read().decode("utf-8", errors="replace"))
            # Unix uses /tmp; Windows uses the per-test TEMP directory above.
            if os.name != "nt":
                for suffix in ("crt", "key"):
                    Path(f"/tmp/chimera-rest-{proc.pid}.{suffix}").unlink(missing_ok=True)
