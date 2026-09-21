# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Verify create-mount builds deep and sibling paths on an empty memfs."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


with tempfile.TemporaryDirectory(prefix="chimera-mount-create-") as scratch:
    root = Path(scratch)
    config = {
        "common": {"huge_pages": False, "sync_delegation_threads": 2},
        "server": {
            "nfs_enabled": True, "threads": 2, "nfs_port": 21049,
            "data_server": True, "external_portmap": True, "metrics_port": 0,
            "state_dir": root.as_posix(),
        },
        "filesystems": {"fs0": {"module": "memfs"}},
        "mounts": {
            "deep": {"module": "memfs", "path": "fs0/a/b/c/d",
                     "create": {"mode": "0750"}},
            "sibling": {"module": "memfs", "path": "fs0/a/b/e", "create": True},
        },
        "exports": {"/deep": {"path": "/deep"},
                    "/sibling": {"path": "/sibling"}},
    }
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    env = os.environ.copy()
    env.update(TEMP=scratch, TMP=scratch)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    with (root / "daemon.log").open("w+b") as log:
        proc = subprocess.Popen([sys.argv[1], "-c", str(config_path)],
                                stdout=log, stderr=log, env=env, creationflags=flags)
        try:
            deadline = time.monotonic() + 40
            while True:
                log.seek(0)
                output = log.read().decode("utf-8", errors="replace")
                if "Server is ready" in output:
                    break
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                if time.monotonic() >= deadline:
                    raise RuntimeError("daemon startup timed out")
                time.sleep(0.1)
            assert "failed to create mount path" not in output.lower()
            assert "Adding NFS export /deep" in output, "deep export missing"
            assert "Adding NFS export /sibling" in output, "sibling export missing"
            proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
            assert proc.wait(timeout=15) == 0, "daemon shutdown failed"
            print("PASS: create-mount built /a/b/c/d and /a/b/e on an empty memfs")
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            log.seek(0)
            print(log.read().decode("utf-8", errors="replace"))
            if os.name != "nt":
                for suffix in ("crt", "key"):
                    Path(f"/tmp/chimera-rest-{proc.pid}.{suffix}").unlink(missing_ok=True)
