# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Run the real daemon with -l and check where its log lines land.

"""
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


def wait_for(path, needle, proc, timeout=60):
    deadline = time.monotonic() + timeout
    while needle not in path.read_bytes():
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited early: {proc.returncode}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{needle!r} never reached {path}")
        time.sleep(0.1)


chimera = sys.argv[1]

with tempfile.TemporaryDirectory(prefix="chimera-daemon-log-file-") as scratch:
    root = Path(scratch)

    # An unopenable log path must fail startup before anything else runs.
    bad = subprocess.run([chimera, "-l", str(root / "missing" / "chimera.log"),
                          "-c", str(root / "unused.json")],
                         capture_output=True, text=True, timeout=30)
    print(bad.stdout + bad.stderr)
    assert bad.returncode == 1, f"bad log path exited {bad.returncode}"
    assert "Failed to open log file" in bad.stderr, "bad log path not reported"

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

    log_path = root / "chimera.log"
    log_path.write_bytes(b"line from an earlier run\n")
    out_path = root / "stdout.txt"

    with out_path.open("w+b") as out:
        proc = subprocess.Popen([chimera, "-l", str(log_path), "-c", str(config_path)],
                                stdout=out, stderr=subprocess.STDOUT)
        try:
            wait_for(log_path, b"Server is ready.", proc)

            text = log_path.read_bytes()
            assert text.startswith(b"line from an earlier run\n"), \
                "-l truncated the existing log instead of appending"
            assert b"level=" not in out_path.read_bytes(), \
                "log lines still reached stdout with -l set"

            # What logrotate's copytruncate does: empty the file under the
            # running daemon.  With append mode the next write lands at
            # offset 0; without it the file grows a hole of NUL bytes.
            with log_path.open("r+b") as rotated:
                rotated.truncate(0)

            proc.send_signal(signal.SIGQUIT)
            returncode = proc.wait(timeout=30)
            assert returncode == 0, f"SIGQUIT shutdown exited {returncode}"

            text = log_path.read_bytes()
            assert b"Server shutdown complete." in text, "shutdown not logged after truncate"
            assert not text.startswith(b"\0"), "write after truncate left a hole"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            out.seek(0)
            print(out.read().decode("utf-8", errors="replace"))
            if log_path.exists():
                print(log_path.read_text(encoding="utf-8", errors="replace"))
