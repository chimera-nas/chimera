# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""metrics_hostname binds the metrics endpoint to that address only.

Two cases, each a fresh daemon:
  - 127.0.0.2, which Linux routes to loopback without configuration;
  - ::1, an IPv6 literal, skipped when the host has no IPv6 loopback.
In both, a connection to 127.0.0.1 on the same port must be refused.
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

OTHER = "127.0.0.1"


def family(host):
    return socket.AF_INET6 if ":" in host else socket.AF_INET


def free_port(host):
    """A free port on host, or None when host cannot be bound (no IPv6)."""
    try:
        with socket.socket(family(host), socket.SOCK_STREAM) as listener:
            listener.bind((host, 0))
            return listener.getsockname()[1]
    except OSError:
        return None


def refused(host, port):
    try:
        with socket.create_connection((host, port), timeout=2):
            return False
    except ConnectionRefusedError:
        return True


def url(host, port):
    return f"http://[{host}]:{port}/metrics" if ":" in host else f"http://{host}:{port}/metrics"


def check(daemon, bind):
    metrics_port = free_port(bind)
    if metrics_port is None:
        print(f"SKIP {bind}: cannot bind it on this host")
        return
    with tempfile.TemporaryDirectory(prefix="chimera-daemon-metrics-") as scratch:
        root = Path(scratch)
        config = {
            "common": {"huge_pages": False, "sync_delegation_threads": 1},
            "server": {
                "threads": 1, "nfs_enabled": False, "smb_enabled": False,
                "s3_enabled": False, "state_dir": root.as_posix(),
                "metrics_port": metrics_port, "metrics_hostname": bind,
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

                with opener.open(url(bind, metrics_port), timeout=5) as response:
                    assert response.status == 200
                    assert b"evpl_" in response.read(), "scrape returned no metrics"

                assert refused(OTHER, metrics_port), \
                    f"metrics endpoint bound to {bind} also answers on {OTHER}:{metrics_port}"

                text = log_path.read_text(encoding="utf-8", errors="replace")
                assert f"Serving prometheus metrics on {bind} port {metrics_port}" in text, \
                    "startup log does not name the bound address"

                proc.send_signal(signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
                print(f"PASS {bind}")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))


for address in ("127.0.0.2", "::1"):
    check(sys.argv[1], address)
