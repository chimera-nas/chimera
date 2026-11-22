# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""rest_hostname binds the REST API listener to that address only.

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
import urllib.error
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
    return f"http://[{host}]:{port}/api/v1/exports" if ":" in host else f"http://{host}:{port}/api/v1/exports"


def check(daemon, bind):
    rest_port = free_port(bind)
    if rest_port is None:
        print(f"SKIP {bind}: cannot bind it on this host")
        return
    with tempfile.TemporaryDirectory(prefix="chimera-daemon-rest-") as scratch:
        root = Path(scratch)
        config = {
            "common": {"huge_pages": False, "sync_delegation_threads": 1},
            "server": {
                "threads": 1, "nfs_enabled": False, "smb_enabled": False,
                "s3_enabled": False, "state_dir": root.as_posix(),
                "rest_http_port": rest_port, "rest_hostname": bind,
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

                # Authentication is on by default, so an unauthenticated
                # request is answered 401: any HTTP answer shows the listener
                # is reachable on this address.
                try:
                    with opener.open(url(bind, rest_port), timeout=5) as response:
                        status = response.status
                except urllib.error.HTTPError as err:
                    status = err.code
                assert status in (200, 401), f"unexpected status {status} from {bind}"

                assert refused(OTHER, rest_port), \
                    f"REST listener bound to {bind} also answers on {OTHER}:{rest_port}"

                text = log_path.read_text(encoding="utf-8", errors="replace")
                assert f"REST API HTTP initialized on {bind} port {rest_port}" in text, \
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
