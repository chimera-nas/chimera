# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end NFSv4.2 extended-attribute round-trip test.

Drives the full pipeline against a live daemon: mount a memfs backend, expose it
as an NFS export, kernel-mount it over NFSv4.2, and verify that a user.* xattr
round-trips through the RFC 8276 SETXATTR/GETXATTR/LISTXATTRS/REMOVEXATTR ops.

This is the regression guard for the namespace-translation bug where names were
passed verbatim to the backend, so a listing showed the attribute double-prefixed
as ``user.user.test`` and reads/writes of ``user.test`` failed.

Management operations go through the Python SDK client; the data path uses the
system mount/setfattr/getfattr binaries.
"""

import os
import shutil
import subprocess
import tempfile

import pytest
from chimera_admin import ChimeraAdminError

MOUNT = "xattrmount"
# Export name doubles as the NFS pseudo-path clients mount; keep the leading
# slash so the server's export matcher resolves "host:/xattr1".
EXPORT = "/xattr1"
EXPORT_VFS_PATH = "/xattrmount"

# Needs root to mount NFS, plus the NFS client and attr tooling.  Skip cleanly
# elsewhere rather than fail.
pytestmark = [
    pytest.mark.skipif(
        os.geteuid() != 0, reason="kernel NFS mount needs root"
    ),
    pytest.mark.skipif(
        shutil.which("mount") is None, reason="mount(8) not installed"
    ),
    pytest.mark.skipif(
        shutil.which("setfattr") is None or shutil.which("getfattr") is None,
        reason="attr tools (setfattr/getfattr) not installed",
    ),
]


def _run(cmd, timeout=30):
    """Run a command, capturing output."""
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout
    )


class TestNfsXattrRoundTrip:
    """Exercise mount -> export -> NFSv4.2 mount -> xattr round-trip."""

    def test_user_xattr_round_trip(self, client, chimera_server):
        """A user.* xattr set over NFSv4.2 round-trips without prefix doubling."""
        host, _ = chimera_server

        created = {"mount": False, "export": False}
        mountpoint = tempfile.mkdtemp(prefix="chimera_nfs_xattr_")
        mounted = False
        try:
            # memfs mount in the VFS namespace, exposed as an NFS export.
            client.create_mount(MOUNT, module="memfs", path="/")
            created["mount"] = True
            client.create_export(EXPORT, EXPORT_VFS_PATH)
            created["export"] = True

            # Kernel-mount over NFSv4.2.
            result = _run(
                [
                    "mount",
                    "-t",
                    "nfs4",
                    "-o",
                    "vers=4.2,port=2049,proto=tcp",
                    f"{host}:{EXPORT}",
                    mountpoint,
                ]
            )
            assert result.returncode == 0, (
                f"NFSv4.2 mount failed (rc={result.returncode})\n"
                f"stderr:\n{result.stderr}"
            )
            mounted = True

            testfile = os.path.join(mountpoint, "file1")
            result = _run(["touch", testfile])
            assert result.returncode == 0, f"touch failed: {result.stderr}"

            # Set user.test -- this is what failed before the fix.
            result = _run(["setfattr", "-n", "user.test", "-v", "hello", testfile])
            assert result.returncode == 0, f"setfattr failed: {result.stderr}"

            # Read it back by name.
            result = _run(
                ["getfattr", "-n", "user.test", "--only-values", testfile]
            )
            assert result.returncode == 0, f"getfattr failed: {result.stderr}"
            assert result.stdout == "hello", f"unexpected value: {result.stdout!r}"

            # Dump all: the attribute must appear exactly once and NOT be
            # double-prefixed as user.user.test.
            result = _run(["getfattr", "-d", testfile])
            assert result.returncode == 0, f"getfattr -d failed: {result.stderr}"
            assert "user.test" in result.stdout, result.stdout
            assert "user.user.test" not in result.stdout, result.stdout

            # Remove it.
            result = _run(["setfattr", "-x", "user.test", testfile])
            assert result.returncode == 0, f"remove failed: {result.stderr}"

            result = _run(["getfattr", "-d", testfile])
            assert result.returncode == 0, f"getfattr -d failed: {result.stderr}"
            assert "user.test" not in result.stdout, result.stdout
        finally:
            # Best-effort cleanup so a mid-test failure does not leak the mount
            # or the resources in the session daemon shared with other modules.
            if mounted:
                _run(["umount", mountpoint])
            try:
                os.rmdir(mountpoint)
            except OSError:
                pass
            if created["export"]:
                try:
                    client.delete_export(EXPORT)
                except ChimeraAdminError:
                    pass
            if created["mount"]:
                try:
                    client.delete_mount(MOUNT)
                except ChimeraAdminError:
                    pass
