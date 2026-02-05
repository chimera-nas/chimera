# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Pytest fixtures for Chimera Admin SDK tests."""

import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import requests


def find_chimera_binary():
    """Find the chimera daemon binary."""
    # Look in common build directories
    script_dir = Path(__file__).parent.parent.parent.parent
    candidates = [
        script_dir / "build" / "Debug" / "src" / "daemon" / "chimera",
        script_dir / "build" / "Release" / "src" / "daemon" / "chimera",
        Path("/build/Debug/src/daemon/chimera"),
        Path("/build/Release/src/daemon/chimera"),
    ]

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError(
        f"Could not find chimera binary. Searched: {candidates}"
    )


def wait_for_rest_api(host: str, port: int, timeout: float = 30.0) -> bool:
    """Wait for the REST API to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(
                f"http://{host}:{port}/version",
                timeout=1.0,
            )
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def chimera_server():
    """Start a Chimera server with REST API enabled.

    Yields:
        tuple: (host, port) of the REST API endpoint
    """
    host = "127.0.0.1"
    rest_port = 18080  # Use a high port to avoid conflicts

    # Create a temporary config file
    config = {
        "server": {
            "threads": 2,
            "delegation_threads": 4,
            "rest_http_port": rest_port,
        },
        "mounts": {
            "testshare": {
                "module": "memfs",
                "path": "/",
            },
        },
    }

    config_dir = tempfile.mkdtemp(prefix="chimera_test_")
    config_path = os.path.join(config_dir, "config.json")

    with open(config_path, "w") as f:
        json.dump(config, f)

    # Find and start the daemon
    chimera_binary = find_chimera_binary()

    env = os.environ.copy()
    # Set library path to find shared libraries
    lib_dirs = [
        str(Path(chimera_binary).parent.parent / "src" / "server"),
        str(Path(chimera_binary).parent.parent / "src" / "server" / "rest"),
        str(Path(chimera_binary).parent.parent / "src" / "server" / "nfs"),
        str(Path(chimera_binary).parent.parent / "src" / "server" / "smb"),
        str(Path(chimera_binary).parent.parent / "src" / "server" / "s3"),
        str(Path(chimera_binary).parent.parent / "src" / "vfs"),
        str(Path(chimera_binary).parent.parent / "src" / "common"),
        str(Path(chimera_binary).parent.parent / "ext" / "libevpl" / "src" / "core"),
        str(Path(chimera_binary).parent.parent / "ext" / "libevpl" / "src" / "http"),
    ]
    existing_path = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(lib_dirs) + (
        f":{existing_path}" if existing_path else ""
    )

    process = subprocess.Popen(
        [chimera_binary, "-c", config_path, "-d"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for the REST API to become available
        if not wait_for_rest_api(host, rest_port, timeout=30.0):
            # Capture any error output
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(
                f"REST API did not become available.\n"
                f"stdout: {stdout.decode()}\n"
                f"stderr: {stderr.decode()}"
            )

        yield host, rest_port

    finally:
        # Shutdown the server
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

        # Clean up config
        os.unlink(config_path)
        os.rmdir(config_dir)
