# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Stop the real daemon with SIGQUIT and expect an orderly shutdown, not a core."""
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


with tempfile.TemporaryDirectory(prefix="chimera-daemon-signal-") as scratch:
    root = Path(scratch)
    config = {
        "common": {"huge_pages": False, "sync_delegation_threads": 1},
        "server": {
            "threads": 1, "nfs_enabled": False, "smb_enabled": False,
            "s3_enabled": False, "state_dir": root.as_posix(),
            "metrics_port": free_port(),
        },
    }
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    log_path = root / "daemon.log"
    with log_path.open("w+b") as log:
        proc = subprocess.Popen([sys.argv[1], "-c", str(config_path)],
                                stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 60
            while b"Server is ready." not in log_path.read_bytes():
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                if time.monotonic() >= deadline:
                    raise RuntimeError("daemon did not become ready")
                time.sleep(0.1)
            proc.send_signal(signal.SIGQUIT)
            returncode = proc.wait(timeout=30)
            text = log_path.read_text(encoding="utf-8", errors="replace")
            assert returncode == 0, f"SIGQUIT shutdown exited {returncode}"
            assert f"signal={int(signal.SIGQUIT)}" in text, "no orderly shutdown logged"
            assert "Server shutdown complete." in text, "shutdown did not complete"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            log.seek(0)
            print(log.read().decode("utf-8", errors="replace"))
