# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Start the daemon on a config the project ships and require it to come up.

A mount that fails is fatal at startup, so a shipped config whose mounts no
longer match what the VFS modules accept (for example memfs mounted at "/"
with no "filesystems" section) stops the daemon here instead of shipping a
server whose exports serve nothing.

Only what the test host needs is overridden: listening ports (so it runs
unprivileged and alongside other tests), the state directory, and the host
paths of passthrough mounts.  The daemon's own portmap is used, on an unused
port, so a host without a network namespace for the test neither needs port
111 nor has its rpcbind registrations changed.  Filesystems, mounts, exports,
shares and buckets are used as shipped.

Passthrough mounts need a backing filesystem that supports file handles
(name_to_handle_at), which /tmp in a container usually does not (overlayfs), so
they are created under the directory given as the third argument -- the build
tree, as the pynfs and S3 tests do.
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

PORT_KEYS = ("nfs_port", "mount_port", "lockmgr_port", "portmap_port", "smb_port",
             "s3_port", "rest_http_port", "rest_https_port")
PASSTHROUGH_MODULES = ("linux", "io_uring")


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
    daemon, shipped, backing_base = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0

    backing_base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="chimera-shipped-") as scratch, \
            tempfile.TemporaryDirectory(prefix="shipped-backing-", dir=backing_base) as backing_root:
        root = Path(scratch)
        state = root / "state"
        state.mkdir()
        env = os.environ.copy()
        env.update(TEMP=scratch, TMP=scratch)

        config = json.loads(shipped.read_text(encoding="utf-8"))
        server = config.setdefault("server", {})
        server.update(zip(PORT_KEYS, unused_ports(len(PORT_KEYS))))
        server.update(metrics_port=0, external_portmap=False, state_dir=state.as_posix())
        for name, mount in config.get("mounts", {}).items():
            if mount.get("module") in PASSTHROUGH_MODULES:
                backing = Path(backing_root) / name
                backing.mkdir()
                mount["path"] = backing.as_posix()

        config_path = root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        with (root / "daemon.log").open("w+b") as log:
            proc = subprocess.Popen([daemon, "-c", str(config_path)],
                                    stdout=log, stderr=log, env=env, creationflags=flags)
            try:
                deadline = time.monotonic() + 60
                while True:
                    log.seek(0)
                    if b"Server is ready" in log.read():
                        break
                    if proc.poll() is not None:
                        raise AssertionError(
                            f"{shipped.name}: daemon exited during startup "
                            f"({proc.returncode})")
                    if time.monotonic() >= deadline:
                        raise AssertionError(f"{shipped.name}: daemon startup timed out")
                    time.sleep(0.1)
                print(f"PASS: {shipped.name} starts", flush=True)
                proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, f"{shipped.name}: daemon shutdown failed"
                print(f"PASS: {shipped.name} orderly shutdown", flush=True)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))
                if os.name != "nt":
                    for suffix in ("crt", "key"):
                        Path(f"/tmp/chimera-rest-{proc.pid}.{suffix}").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
