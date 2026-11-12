# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Run the real daemon with nfs3_disabled and expect NFSv4 alone.

No MOUNT, NLM or NSM listener, no portmap traffic, and an NFSv3 call answered
PROG_MISMATCH.  Runs in its own network namespace, so the well-known ports it
checks are free.  The control runs with the option off show each check can fail.

Run 1, nfs3_disabled true (a fake portmap on 127.0.0.1:111 that records and
closes any connection):

  check               how                                  proves
  NFSv4 served        v4 NULL -> SUCCESS, v4.0 COMPOUND    NFS still starts and
                      PUTROOTFH+GETATTR -> NFS4_OK         serves COMPOUNDs
  NFSv3 rejected      v3 NULL -> PROG_MISMATCH 4-4         v3 program not
                                                           registered
  no MOUNT/NLM/NSM    connect to 20048/32803/32765         auxiliary servers
                      refused                              not started
  clean shutdown      SIGTERM -> exit 0                    stop path without
                                                           unregister is safe
  no portmap traffic  fake portmap never contacted         no SET at startup,
                                                           no UNSET at stop
  option applied      "NFSv3 disabled" in the log          JSON key parsed

Run 2, nfs3_disabled true, built-in portmap (external_portmap left out):

  check               how                                  proves
  as run 1, and       connect to 111 refused               built-in portmapper
  no portmapper                                            not started

Run 3, data_server true (fake portmap on 111):

  check               how                                  proves
  NFSv3 still served  v3 NULL -> SUCCESS                   DS I/O over v3 works
  no MOUNT/NLM/NSM    connect to 20048/32803/32765         auxiliary servers
                      refused                              not started
  no portmap traffic  fake portmap never contacted         stop sends no UNSET
                                                           for a data server

Run 4, control, nfs3_disabled false (nothing on 111):

  check               how                                  proves
  NFSv4 and v3        v4 and v3 NULL -> SUCCESS            run 1's v3 check
  served                                                   can fail
  MOUNT/NLM/NSM up    connect to 20048/32803/32765         run 1's port checks
                      accepted                             can fail
  clean shutdown      SIGTERM -> exit 0                    the refused portmap
                                                           connect is not fatal
  registration tried  "not reachable" in the log           runs 1 and 3 portmap
                                                           checks can fail

Run 5, control, nfs3_disabled false, built-in portmap:

  check               how                                  proves
  portmapper up       connect to 111 accepted              run 2's 111 check
                                                           can fail
"""
import json
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

AUX_PORTS = (20048, 32803, 32765)  # MOUNT, NLM, NSM defaults


class FakePortmap:
    """Listens on 127.0.0.1:111 and records any connection, closing it at once
    so a SET or UNSET fails fast instead of waiting for a reply."""

    def __init__(self):
        self.sock = socket.create_server(("127.0.0.1", 111))
        self.contacted = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.contacted = True
            conn.close()

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def accepts(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
        return True
    except ConnectionRefusedError:
        return False


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("short RPC reply")
        buf += chunk
    return buf


def nfs_call(port, vers, proc, args=b""):
    """One AUTH_NONE call to the NFS program; returns the reply words after the
    verifier: the accept_stat, then its body."""
    call = struct.pack(">10I", 1, 0, 2, 100003, vers, proc, 0, 0, 0, 0) + args
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(struct.pack(">I", 0x80000000 | len(call)) + call)
        mark = struct.unpack(">I", recv_exact(sock, 4))[0]
        reply = recv_exact(sock, mark & 0x7fffffff)
    # xid, REPLY, MSG_ACCEPTED, AUTH_NONE verifier (flavor, 0 length)
    return struct.unpack(f">{len(reply) // 4}I", reply)[5:]


def nfs_null(port, vers):
    """NFS NULL call; returns the accept_stat and, for PROG_MISMATCH, the
    version range: (0,) or (2, low, high)."""
    return nfs_call(port, vers, 0)


def nfs4_compound(port):
    """Minor-version-0 COMPOUND { PUTROOTFH; GETATTR(type) }; returns the
    accept_stat and the COMPOUND status."""
    # tag "", minorversion 0, 2 ops: PUTROOTFH (24), GETATTR (9) bitmap {TYPE}
    args = struct.pack(">7I", 0, 0, 2, 24, 9, 1, 1 << 1)
    return nfs_call(port, 4, 1, args)[:2]


def run_daemon(daemon, nfs3_disabled, check, external_portmap=True, data_server=False):
    """Start the daemon, run check(nfs_port) once it is ready, stop it; return its log."""
    with tempfile.TemporaryDirectory(prefix="chimera-nfs3-disabled-") as scratch:
        root = Path(scratch)
        nfs_port = free_port()
        config = {
            "common": {"huge_pages": False, "sync_delegation_threads": 2},
            "server": {
                "threads": 2, "nfs_enabled": True, "nfs_port": nfs_port,
                "external_portmap": external_portmap, "nfs3_disabled": nfs3_disabled,
                "data_server": data_server,
                "metrics_port": 0, "state_dir": root.as_posix(),
            },
            "filesystems": {"fs0": {"module": "memfs"}},
            "mounts": {"data": {"module": "memfs", "path": "fs0"}},
            "exports": {"/share": {"path": "/data"}},
        }
        config_path = root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        log_path = root / "daemon.log"
        with log_path.open("w+b") as log:
            proc = subprocess.Popen([daemon, "-c", str(config_path)],
                                    stdout=log, stderr=log)
            try:
                # Logged once every protocol has started, so NFS has bound
                # everything it is going to bind.
                deadline = time.monotonic() + 60
                while b"Server is ready." not in log_path.read_bytes():
                    if proc.poll() is not None:
                        raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("daemon did not become ready")
                    time.sleep(0.1)
                check(nfs_port)
                proc.send_signal(signal.SIGTERM)
                rc = proc.wait(timeout=30)
                assert rc == 0, f"daemon shutdown failed: {rc}"
                return log_path.read_text(encoding="utf-8", errors="replace")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))


def check_v4(nfs_port):
    assert nfs_null(nfs_port, 4) == (0,), "NFSv4 NULL failed"
    assert nfs4_compound(nfs_port) == (0, 0), "NFSv4 COMPOUND failed"


def check_disabled(nfs_port):
    check_v4(nfs_port)
    assert nfs_null(nfs_port, 3) == (2, 4, 4), "NFSv3 NULL not PROG_MISMATCH 4-4"
    for port in AUX_PORTS:
        assert not accepts(port), f"something is listening on {port}"


def check_enabled(nfs_port):
    check_v4(nfs_port)
    assert nfs_null(nfs_port, 3) == (0,), "NFSv3 NULL failed"
    for port in AUX_PORTS:
        assert accepts(port), f"nothing is listening on {port}"


def check_disabled_builtin(nfs_port):
    check_disabled(nfs_port)
    assert not accepts(111), "built-in portmapper is listening on 111"


def check_data_server(nfs_port):
    check_v4(nfs_port)
    assert nfs_null(nfs_port, 3) == (0,), "NFSv3 NULL failed"
    for port in AUX_PORTS:
        assert not accepts(port), f"something is listening on {port}"


def check_enabled_builtin(nfs_port):
    check_enabled(nfs_port)
    assert accepts(111), "built-in portmapper is not listening on 111"


def main():
    daemon = sys.argv[1]

    portmap = FakePortmap()
    try:
        text = run_daemon(daemon, True, check_disabled)
    finally:
        portmap.close()
        # Overrides any startup failure a contact caused, so a regression
        # reports the real cause.
        assert not portmap.contacted, "daemon contacted portmap"
    assert "NFSv3 disabled: serving only NFSv4" in text, "nfs3_disabled not applied"
    print("PASS: nfs3_disabled serves NFSv4 only", flush=True)

    run_daemon(daemon, True, check_disabled_builtin, external_portmap=False)
    print("PASS: nfs3_disabled starts no built-in portmapper", flush=True)

    portmap = FakePortmap()
    try:
        run_daemon(daemon, False, check_data_server, data_server=True)
    finally:
        portmap.close()
        assert not portmap.contacted, "data server contacted portmap"
    print("PASS: data server neither registers nor unregisters", flush=True)

    # Control: no portmap listens, so the refused connect leaves the server
    # running unregistered, with NFSv3 and its auxiliary services up.  The log
    # shows registration was attempted, so runs 1 and 3 would see a contact.
    text = run_daemon(daemon, False, check_enabled)
    assert "External portmap at 127.0.0.1:111 is not reachable" in text, \
        "control run did not attempt portmap registration"
    assert "NFSv3 disabled: serving only NFSv4" not in text, "control run disabled NFSv3"
    print("PASS: control run serves NFSv3, MOUNT, NLM and NSM", flush=True)

    run_daemon(daemon, False, check_enabled_builtin, external_portmap=False)
    print("PASS: control run starts the built-in portmapper", flush=True)


if __name__ == "__main__":
    main()
