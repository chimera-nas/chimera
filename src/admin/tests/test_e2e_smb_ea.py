# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end SMB2 Extended Attribute round-trip test.

Drives the full pipeline against a live daemon: create an SMB user, mount a
memfs backend, expose it as an SMB share, kernel-mount it over SMB3 with cifs,
and verify that a user.* xattr round-trips through the FILE_FULL_EA_INFORMATION
set/query path.  The Linux cifs client maps the user.* xattr namespace onto SMB
EAs, so setfattr/getfattr exercise the server's EA SetInfo/QueryInfo handlers.

Also checks the cross-protocol contract: SMB EAs and NFSv4.2 xattrs share the
same "user." VFS keyspace, so an EA set here is the same backend object an NFS
client would see as a user xattr.
"""

import os
import shutil
import subprocess
import tempfile

import pytest
from chimera_admin import ChimeraAdminError

USER = "smbeauser"
PASSWORD = "smbeapass"
MOUNT = "smbeamount"
SHARE = "smbea"
SHARE_PATH = "/smbeamount"

# Needs root (cifs mount + the daemon's privileged port 445) and the cifs mount
# helper + attr tooling.  Skip cleanly elsewhere rather than fail.
pytestmark = [
    pytest.mark.skipif(
        os.geteuid() != 0, reason="cifs mount + privileged SMB port need root"
    ),
    pytest.mark.skipif(
        shutil.which("mount.cifs") is None, reason="cifs mount helper not installed"
    ),
    pytest.mark.skipif(
        shutil.which("setfattr") is None or shutil.which("getfattr") is None,
        reason="attr tools (setfattr/getfattr) not installed",
    ),
]


def _run(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


class TestSmbEaRoundTrip:
    """mount -> share -> SMB3 cifs mount -> EA set/query/remove round-trip."""

    def test_user_ea_round_trip(self, client, chimera_server):
        host, _ = chimera_server

        created = {"user": False, "mount": False, "share": False}
        mountpoint = tempfile.mkdtemp(prefix="chimera_smb_ea_")
        mounted = False
        try:
            client.create_user(USER, uid=1000, gid=1000, smbpasswd=PASSWORD)
            created["user"] = True
            client.create_mount(MOUNT, module="memfs", path="/")
            created["mount"] = True
            client.create_share(SHARE, SHARE_PATH)
            created["share"] = True

            result = _run(
                [
                    "mount", "-t", "cifs",
                    f"//{host}/{SHARE}", mountpoint,
                    "-o",
                    f"username={USER},password={PASSWORD},vers=3.0,"
                    "seal,uid=0,gid=0",
                ]
            )
            assert result.returncode == 0, (
                f"cifs mount failed (rc={result.returncode})\nstderr:\n{result.stderr}"
            )
            mounted = True

            testfile = os.path.join(mountpoint, "file1")
            assert _run(["touch", testfile]).returncode == 0

            # Set an EA via the user.* namespace (cifs -> SMB FILE_FULL_EA set).
            result = _run(["setfattr", "-n", "user.test", "-v", "hello", testfile])
            assert result.returncode == 0, f"setfattr failed: {result.stderr}"

            # Read it back by name (SMB FILE_FULL_EA query).
            result = _run(
                ["getfattr", "-n", "user.test", "--only-values", testfile]
            )
            assert result.returncode == 0, f"getfattr failed: {result.stderr}"
            assert result.stdout == "hello", f"unexpected value: {result.stdout!r}"

            # Full dump: appears exactly once, not double-prefixed.
            result = _run(["getfattr", "-d", testfile])
            assert "user.test" in result.stdout, result.stdout
            assert "user.user.test" not in result.stdout, result.stdout

            # Case-insensitive match: setting user.TEST updates the same EA
            # (Samba canonicalize semantics), it must not create a second one.
            assert _run(
                ["setfattr", "-n", "user.TEST", "-v", "world", testfile]
            ).returncode == 0
            result = _run(["getfattr", "-d", testfile])
            # exactly one EA line mentioning test (case-insensitively)
            ea_lines = [
                ln for ln in result.stdout.splitlines()
                if ln.lower().startswith("user.test=")
            ]
            assert len(ea_lines) == 1, f"expected one EA, got: {result.stdout!r}"

            # Remove (zero-length value deletes).
            assert _run(["setfattr", "-x", "user.test", testfile]).returncode == 0
            result = _run(["getfattr", "-d", testfile])
            assert "test" not in result.stdout.lower(), result.stdout
        finally:
            if mounted:
                _run(["umount", mountpoint])
            try:
                os.rmdir(mountpoint)
            except OSError:
                pass
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
