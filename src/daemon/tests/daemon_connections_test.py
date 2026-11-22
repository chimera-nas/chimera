# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""GET /api/v1/connections lists the NFS connections the server holds.

One daemon serving NFSv4.  The test opens TCP connections to it, runs RPC calls
on some, and checks that the endpoint reports each connection with its address,
counters and the NFSv4 minor version used; that filters narrow the answer and
bad ones are refused; that a closed connection disappears; and that requests
abandoned halfway or sent in a burst neither crash the daemon nor leave it
unable to answer afterwards.
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
import urllib.error
import urllib.request

HOST = "127.0.0.1"
NFS_PROGRAM = 100003


def unused_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind((HOST, 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def rpc_call(xid, procedure, body=b""):
    """An ONC RPC call to NFS version 4 with AUTH_NONE, as one record."""
    message = struct.pack(">IIIIII", xid, 0, 2, NFS_PROGRAM, 4, procedure)
    message += struct.pack(">IIII", 0, 0, 0, 0) + body
    return struct.pack(">I", 0x80000000 | len(message)) + message


def read_record(sock):
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        assert chunk, "connection closed while waiting for a reply"
        header += chunk
    length = struct.unpack(">I", header)[0] & 0x7FFFFFFF
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        assert chunk, "connection closed inside a reply"
        data += chunk
    return data


def call(sock, xid, procedure, body=b""):
    sock.sendall(rpc_call(xid, procedure, body))
    reply = read_record(sock)
    assert struct.unpack(">I", reply[:4])[0] == xid, "reply for a different call"


def compound(minor):
    """A COMPOUND with an empty tag and no operations."""
    return struct.pack(">III", 0, minor, 0)


def main(daemon):
    rest_port, nfs_port = unused_ports(2)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def get(path):
        try:
            with opener.open(f"http://{HOST}:{rest_port}{path}", timeout=10) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as err:
            return err.code, None

    with tempfile.TemporaryDirectory(prefix="chimera-daemon-connections-") as scratch:
        root = Path(scratch)
        config = {
            "common": {"huge_pages": False, "sync_delegation_threads": 1},
            "server": {
                "threads": 2, "nfs_enabled": True, "nfs_port": nfs_port,
                "nfs3_disabled": True, "data_server": True, "external_portmap": True,
                "smb_enabled": False, "s3_enabled": False,
                "rest_http_port": rest_port, "rest_auth_enabled": False,
                "metrics_port": 0, "state_dir": root.as_posix(),
            },
            "filesystems": {"fs0": {"module": "memfs"}},
            "mounts": {"data": {"module": "memfs", "path": "fs0"}},
            "exports": {"/e": {"path": "/data"}},
        }
        config_path = root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        log_path = root / "daemon.log"
        sockets = []
        with log_path.open("w+b") as log:
            proc = subprocess.Popen([daemon, "-c", str(config_path)], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 60
                while b"Server is ready." not in log_path.read_bytes():
                    if proc.poll() is not None:
                        raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("daemon did not become ready")
                    time.sleep(0.1)

                # Three connections: one idle, one that ran NULL, one that ran
                # a COMPOUND as NFSv4.2.
                for _ in range(3):
                    sock = socket.create_connection((HOST, nfs_port), timeout=10)
                    sockets.append(sock)
                idle, nulled, minor2 = sockets
                call(nulled, 1, 0)
                call(minor2, 1, 1, compound(2))
                ports = [sock.getsockname()[1] for sock in sockets]

                # A connection is listed once its worker has accepted it, which
                # can lag the client's connect, so wait for all three.
                deadline = time.monotonic() + 10
                while True:
                    status, body = get("/api/v1/connections")
                    assert status == 200, f"status {status}"
                    listed = {c["remote"]["port"] for c in body["connections"]}
                    if set(ports) <= listed:
                        break
                    assert time.monotonic() < deadline, f"not all of {ports} listed: {listed}"
                    time.sleep(0.1)
                assert body["complete"] and not body["truncated"] and not body["missing_workers"]
                assert body["server_instance_id"]
                by_port = {c["remote"]["port"]: c for c in body["connections"]}
                for port in ports:
                    assert port in by_port, f"connection from port {port} not listed"
                for connection in by_port.values():
                    assert connection["service"] == "nfs"
                    assert connection["transport"] == "tcp"
                    assert connection["state"] == "open"
                    assert connection["remote"]["address"] == HOST
                    assert connection["local"]["port"] == nfs_port
                    assert connection["connection_id"].count(":") == 1

                assert by_port[ports[0]]["stats"]["rx_messages"] == "0"
                assert by_port[ports[0]]["observed_rpc_programs"] == []
                assert by_port[ports[1]]["stats"]["requests_completed"] == "1"
                assert by_port[ports[1]]["stats"]["rx_messages"] == "1"
                assert by_port[ports[1]]["stats"]["tx_messages"] == "1"
                assert by_port[ports[1]]["nfs_minor_versions_seen"] == []
                assert {"program": NFS_PROGRAM, "version": 4} in \
                    by_port[ports[1]]["observed_rpc_programs"]
                assert by_port[ports[2]]["nfs_minor_versions_seen"] == [2]
                assert int(by_port[ports[2]]["stats"]["rx_bytes"]) > 0

                # Filters narrow the answer; all of them are ANDed.
                status, body = get(f"/api/v1/connections?remote_port={ports[1]}")
                assert status == 200 and len(body["connections"]) == 1
                status, body = get(f"/api/v1/connections?remote_ip={HOST}&remote_port={ports[1]}"
                                   f"&local_port={nfs_port}&transport=tcp&service=nfs")
                assert status == 200 and len(body["connections"]) == 1
                status, body = get(f"/api/v1/connections?remote_port={ports[1]}&transport=rdma")
                assert status == 200 and body["connections"] == []
                status, body = get("/api/v1/connections?service=mount")
                assert status == 200 and body["connections"] == []
                worker = by_port[ports[1]]["worker_id"]
                status, body = get(f"/api/v1/connections?worker_id={worker}")
                assert status == 200 and ports[1] in {c["remote"]["port"] for c in body["connections"]}

                for bad in ("worker_id=99", "worker_id=x", "remote_port=70000", "remote_ip=nothost",
                            "service=bogus", "transport=carrier", "bogus=1", "worker_id=0&worker_id=0",
                            "remote_port=", "remote_port=1&"):
                    status, _ = get(f"/api/v1/connections?{bad}")
                    assert status == 400, f"{bad!r} gave {status}"

                request = urllib.request.Request(f"http://{HOST}:{rest_port}/api/v1/connections",
                                                 data=b"", method="POST")
                try:
                    opener.open(request, timeout=10)
                    raise AssertionError("POST was accepted")
                except urllib.error.HTTPError as err:
                    assert err.code == 405

                # A closed connection leaves the list.
                gone = ports[0]
                idle.close()
                deadline = time.monotonic() + 10
                while True:
                    status, body = get("/api/v1/connections")
                    assert status == 200
                    if gone not in {c["remote"]["port"] for c in body["connections"]}:
                        break
                    assert time.monotonic() < deadline, "closed connection still listed"
                    time.sleep(0.1)

                # Requests abandoned halfway and a burst from many clients: each
                # answer is a 200 or a 429, and the daemon is still healthy.
                statuses = []

                def burst():
                    for _ in range(10):
                        try:
                            statuses.append(get("/api/v1/connections")[0])
                        except OSError as err:
                            statuses.append(repr(err))

                workers = [threading.Thread(target=burst) for _ in range(8)]
                for thread in workers:
                    thread.start()
                for _ in range(40):
                    raw = socket.create_connection((HOST, rest_port), timeout=5)
                    raw.sendall(b"GET /api/v1/connections HTTP/1.1\r\nHost: x\r\n\r\n")
                    raw.close()
                for thread in workers:
                    thread.join()
                assert statuses and set(statuses) <= {200, 429}, set(statuses)
                assert 200 in statuses
                assert proc.poll() is None, "daemon died under concurrent requests"
                for _ in range(50):
                    status, body = get("/api/v1/connections")
                    if status == 200:
                        break
                    time.sleep(0.1)
                assert status == 200 and body["complete"]

                for sock in sockets:
                    sock.close()
                proc.send_signal(signal.SIGTERM)
                assert proc.wait(timeout=30) == 0, "daemon shutdown failed"
                print("PASS")
            finally:
                for sock in sockets:
                    sock.close()
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                log.seek(0)
                print(log.read().decode("utf-8", errors="replace"))


main(sys.argv[1])
