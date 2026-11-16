# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Inspection SDK/CLI against the real daemon used by the admin API suite."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from chimera_admin import ChimeraAdminError


def test_inspection_sdk_live(client):
    threads = client.get_threads()
    assert threads["complete"] is True
    assert len(threads["threads"]) == 2
    selected = client.get_threads(worker_id=1)
    assert [row["worker_id"] for row in selected["threads"]] == [1]
    connections = client.get_connections(service="nfs", transport="tcp")
    assert connections["server_instance_id"] == threads["server_instance_id"]
    assert connections["complete"] is True
    for row in threads["threads"]:
        assert int(row["sampled_monotonic_ns"]) > 0
        assert row["worker_name"] == "chm_core_{}".format(row["worker_id"])
        assert "connection_counts" in row


def test_inspection_sdk_live_invalid_filter(client):
    with pytest.raises(ChimeraAdminError) as error:
        client.get_connections(remote_ip="localhost")
    assert error.value.status_code == 400


def test_inspection_sdk_nfs_sessions(client):
    result = client.get_nfs_sessions(limit=8, offset=0)
    assert result["total"] == 0
    assert result["sessions"] == []
    assert result["truncated"] is False
    with pytest.raises(ChimeraAdminError) as error:
        client._inspection_get("/api/v1/nfs/sessions", {"unexpected": 1})
    assert error.value.status_code == 400
    with pytest.raises(ChimeraAdminError) as missing:
        client.get_nfs_session_slots("00000000000000000000000000000000")
    assert missing.value.status_code == 404


def test_inspection_cli_live_dashboard(client):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    for name in ("CHIMERA_ADMIN_TOKEN", "CHIMERA_ADMIN_USER", "CHIMERA_ADMIN_PASSWORD"):
        env.pop(name, None)
    result = subprocess.run(
        [sys.executable, "-m", "chimera_admin", "--host", client.host,
         "--port", str(client.port), "inspect", "dashboard", "--json"],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    snapshots = json.loads(result.stdout)
    assert snapshots["threads_snapshot"]["complete"] is True
    assert snapshots["connections_snapshot"]["complete"] is True
    assert "\x1b" not in result.stdout


def test_inspection_live_smb_negotiation(client):
    import socket
    import struct
    import time

    def receive_exact(sock, length):
        data = bytearray()
        while len(data) < length:
            part = sock.recv(length - len(data))
            assert part, "SMB connection closed before negotiation reply"
            data.extend(part)
        return data

    with socket.create_connection((client.host, 445), timeout=3) as sock:
        source_port = sock.getsockname()[1]
        until = time.monotonic() + 3
        while True:
            snapshot = client.get_connections(service="smb", remote_port=source_port)
            if snapshot["connections"]:
                break
            assert time.monotonic() < until
            time.sleep(0.01)
        row = snapshot["connections"][0]
        assert row["negotiated_dialect"] is None
        assert row["local"]["address"] == client.host
        assert row["local"]["port"] == 445
        assert row["remote"]["port"] == source_port
        assert row["per_connection_stats"]["messages_received"] == 0
        identity = row["connection_id"]
        header = struct.pack("<4sHHIHHIIQIIQ16s", b"\xfeSMB", 64, 0, 0, 0, 1,
                             0, 0, 0, 0, 0, 0, b"\0" * 16)
        negotiate = struct.pack("<HHHHI16sIHHH", 36, 1, 1, 0, 0,
                                b"inspection-probe", 0, 0, 0, 0x0202)
        payload = header + negotiate
        sock.sendall(struct.pack(">I", len(payload)) + payload)
        length = struct.unpack(">I", receive_exact(sock, 4))[0]
        assert length < 1024 * 1024
        reply = receive_exact(sock, length)
        assert struct.unpack_from("<I", reply, 8)[0] == 0
        assert struct.unpack_from("<H", reply, 68)[0] == 0x0202
        row = client.get_connections(service="smb", remote_port=source_port)["connections"][0]
        assert row["connection_id"] == identity
        assert row["negotiated_dialect"] == 0x0202
        assert row["per_connection_stats"] == {"messages_received": 1, "counter_bits": 32}
