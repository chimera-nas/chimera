# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end SMB pipeline test.

Drives the full multi-protocol lifecycle against a live daemon: create a user
with an SMB password, mount a memfs backend, expose it as an SMB share, connect
over SMB with the real smbclient binary, and verify the mount-in-use (409)
guard before tearing everything down.

Management operations go through the Python SDK client (what the CLI wraps);
the data path uses the system smbclient.
"""

import os
import shutil
import subprocess

import pytest
from chimera_admin import ChimeraAdminError

USER = "testuser1"
PASSWORD = "systest"
MOUNT = "smbmount"
SHARE = "smb1"
SHARE_PATH = "/smbmount"

# The daemon binds the privileged SMB port 445, and smbclient must be present.
# Skip cleanly elsewhere rather than fail.
pytestmark = [
    pytest.mark.skipif(
        shutil.which("smbclient") is None, reason="smbclient not installed"
    ),
    pytest.mark.skipif(
        os.geteuid() != 0,
        reason="daemon must bind privileged SMB port 445 (needs root)",
    ),
]


def _smbclient(host, share, user, password, command, timeout=30):
    """Run a single smbclient command against the server.

    Mirrors the invocation proven in the C smbclient_auth_test.c NTLM test:
    ``smbclient //host/share -U user%password -c '<command>'`` with no extra
    flags. Returns the CompletedProcess.
    """
    return subprocess.run(
        [
            "smbclient",
            f"//{host}/{share}",
            "-U",
            f"{user}%{password}",
            "-c",
            command,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class TestSmbPipeline:
    """Exercise the user -> mount -> share -> SMB access -> teardown flow."""

    def test_full_smb_pipeline(self, client, chimera_server):
        """Walk the entire pipeline end to end against the session daemon."""
        host, _ = chimera_server

        created = {"user": False, "mount": False, "share": False}
        try:
            # 1. User with an SMB password (stored plaintext; the server
            #    computes the NTLM hash on the fly).
            client.create_user(USER, uid=1000, gid=1000, smbpasswd=PASSWORD)
            created["user"] = True

            # 2. memfs mount; appears in the VFS namespace at /smbmount.
            client.create_mount(MOUNT, module="memfs", path="/")
            created["mount"] = True

            # 3. SMB share backed by that mount.
            client.create_share(SHARE, SHARE_PATH)
            created["share"] = True

            # 4. Data path: authenticate over SMB and list the share root.
            result = _smbclient(host, SHARE, USER, PASSWORD, "ls")
            assert result.returncode == 0, (
                "smbclient ls failed (rc="
                f"{result.returncode})\nstdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

            # 5. Deleting the mount must fail while the share references it.
            with pytest.raises(ChimeraAdminError) as exc_info:
                client.delete_mount(MOUNT)
            assert exc_info.value.status_code == 409

            # 6. Remove the share, then the mount succeeds.
            client.delete_share(SHARE)
            created["share"] = False

            client.delete_mount(MOUNT)
            created["mount"] = False

            client.delete_user(USER)
            created["user"] = False
        finally:
            # Best-effort cleanup so a mid-test failure does not leak resources
            # into the session daemon shared with the other test modules.
            if created["share"]:
                try:
                    client.delete_share(SHARE)
                except ChimeraAdminError:
                    pass
            if created["mount"]:
                try:
                    client.delete_mount(MOUNT)
                except ChimeraAdminError:
                    pass
            if created["user"]:
                try:
                    client.delete_user(USER)
                except ChimeraAdminError:
                    pass
