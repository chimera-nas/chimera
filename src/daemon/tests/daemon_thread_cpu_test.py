# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""The metrics endpoint exports CPU time for worker threads, and only those.

One daemon with one core thread and one sync thread: the scrape must carry
one chimera_thread_cpu_nanoseconds_total series for each, none for helper
threads, and the values must not go backwards between two scrapes.
"""
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

METRIC = "chimera_thread_cpu_nanoseconds_total"
HOST = "127.0.0.1"


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((HOST, 0))
        return listener.getsockname()[1]


def thread_cpu(opener, port):
    """{thread name: nanoseconds} from one scrape."""
    with opener.open(f"http://{HOST}:{port}/metrics", timeout=5) as response:
        assert response.status == 200
        text = response.read().decode("utf-8")
    values = {}
    for line in text.splitlines():
        if line.startswith(METRIC + "{"):
            labels, value = line.rsplit(" ", 1)
            values[labels.split('thread="')[1].split('"')[0]] = int(value)
    return values


def main(daemon):
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="chimera-daemon-thread-cpu-") as scratch:
        root = Path(scratch)
        config = {
            "common": {"huge_pages": False, "sync_delegation_threads": 1},
            "server": {
                "threads": 1, "nfs_enabled": False, "smb_enabled": False,
                "s3_enabled": False, "state_dir": root.as_posix(),
                "metrics_port": port, "metrics_hostname": HOST,
            },
        }
        config_path = root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        log_path = root / "daemon.log"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with log_path.open("w+b") as log:
            proc = subprocess.Popen([daemon, "-c", str(config_path)],
                                    stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 60
                while b"Server is ready." not in log_path.read_bytes():
                    if proc.poll() is not None:
                        raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("daemon did not become ready")
                    time.sleep(0.1)

                first = thread_cpu(opener, port)
                assert set(first) == {"chm_core_0", "chm_sync_0"}, \
                    f"expected one series per worker thread, got {sorted(first)}"

                second = thread_cpu(opener, port)
                assert set(second) == set(first), "the series set changed between scrapes"
                for name, value in first.items():
                    assert second[name] >= value, f"{name} went backwards"

                proc.send_signal(signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
                print("PASS")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))


main(sys.argv[1])
