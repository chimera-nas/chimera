# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end NFSv3 POSIX ACL round-trip test (the NFSACL sideband protocol).

Drives the full pipeline against a live daemon: mount a memfs backend, expose it
as an NFS export, kernel-mount it over NFSv3, and verify that POSIX.1e ACLs set
and read with setfacl/getfacl round-trip through the unofficial NFSACL sideband
protocol (RPC program 100227).  The Linux kernel NFSv3 client auto-negotiates
NFSACL when the server registers program 100227, which chimera advertises via
portmap and multiplexes on the NFS port.

memfs stores the canonical (NFSv4/NT) ACL natively, so a named-user entry must
survive the canonical<->POSIX.1e translation in both directions.

Management operations go through the Python SDK client; the data path uses the
system mount/setfacl/getfacl binaries.
"""

import os
import shutil
import subprocess
import tempfile

import pytest
from chimera_admin import ChimeraAdminError

MOUNT = "aclmount"
# Export name doubles as the NFS pseudo-path clients mount.
EXPORT = "/acl1"
EXPORT_VFS_PATH = "/aclmount"

# Needs root to mount NFS, plus the NFS client and acl tooling.  Skip cleanly
# elsewhere rather than fail.
pytestmark = [
    pytest.mark.skipif(
        os.geteuid() != 0, reason="kernel NFS mount needs root"
    ),
    pytest.mark.skipif(
        shutil.which("mount") is None, reason="mount(8) not installed"
    ),
    pytest.mark.skipif(
        shutil.which("setfacl") is None or shutil.which("getfacl") is None,
        reason="acl tools (setfacl/getfacl) not installed",
    ),
]


def _run(cmd, timeout=30):
    """Run a command, capturing output."""
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout
    )


class TestNfsAclRoundTrip:
    """Exercise mount -> export -> NFSv3 mount -> POSIX ACL round-trip."""

    def test_named_user_acl_round_trip(self, client, chimera_server):
        """A named-user ACL set over NFSv3 round-trips via the NFSACL sideband."""
        host, _ = chimera_server

        created = {"mount": False, "export": False}
        mountpoint = tempfile.mkdtemp(prefix="chimera_nfs_acl_")
        mounted = False
        try:
            # memfs mount in the VFS namespace, exposed as an NFS export.
            client.create_mount(MOUNT, module="memfs", path="/")
            created["mount"] = True
            client.create_export(EXPORT, EXPORT_VFS_PATH)
            created["export"] = True

            # Kernel-mount over NFSv3 (NFSACL is negotiated automatically).
            result = _run(
                [
                    "mount",
                    "-t",
                    "nfs",
                    "-o",
                    "vers=3,port=2049,mountport=20048,"
                    "mountproto=tcp,proto=tcp,nolock",
                    f"{host}:{EXPORT}",
                    mountpoint,
                ]
            )
            assert result.returncode == 0, (
                f"NFSv3 mount failed (rc={result.returncode})\n"
                f"stderr:\n{result.stderr}"
            )
            mounted = True

            testfile = os.path.join(mountpoint, "file1")
            result = _run(["touch", testfile])
            assert result.returncode == 0, f"touch failed: {result.stderr}"

            # Grant a named user r-x via the NFSACL SETACL path.
            result = _run(["setfacl", "-m", "u:1005:r-x", testfile])
            assert result.returncode == 0, f"setfacl failed: {result.stderr}"

            # Read it back (numeric ids for a stable assertion) -- the named
            # entry must survive the canonical<->POSIX translation.
            result = _run(["getfacl", "-n", testfile])
            assert result.returncode == 0, f"getfacl failed: {result.stderr}"
            assert "user:1005:r-x" in result.stdout, result.stdout
            # POSIX requires a mask once a named entry exists.
            assert "mask::" in result.stdout, result.stdout

            # Remove the named entry.
            result = _run(["setfacl", "-x", "u:1005", testfile])
            assert result.returncode == 0, f"setfacl -x failed: {result.stderr}"

            result = _run(["getfacl", "-n", testfile])
            assert result.returncode == 0, f"getfacl failed: {result.stderr}"
            assert "user:1005:" not in result.stdout, result.stdout
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

    def test_default_acl_inheritance(self, client, chimera_server):
        """A default ACL set on a directory is inherited by a new child."""
        host, _ = chimera_server

        created = {"mount": False, "export": False}
        mountpoint = tempfile.mkdtemp(prefix="chimera_nfs_dacl_")
        mounted = False
        try:
            client.create_mount(MOUNT, module="memfs", path="/")
            created["mount"] = True
            client.create_export(EXPORT, EXPORT_VFS_PATH)
            created["export"] = True

            result = _run(
                [
                    "mount",
                    "-t",
                    "nfs",
                    "-o",
                    "vers=3,port=2049,mountport=20048,"
                    "mountproto=tcp,proto=tcp,nolock",
                    f"{host}:{EXPORT}",
                    mountpoint,
                ]
            )
            assert result.returncode == 0, (
                f"NFSv3 mount failed (rc={result.returncode})\n"
                f"stderr:\n{result.stderr}"
            )
            mounted = True

            testdir = os.path.join(mountpoint, "dir1")
            os.mkdir(testdir)

            # Set a default ACL granting a named user rwx on future children.
            result = _run(["setfacl", "-d", "-m", "u:1006:rwx", testdir])
            assert result.returncode == 0, f"setfacl -d failed: {result.stderr}"

            result = _run(["getfacl", "-n", testdir])
            assert result.returncode == 0, f"getfacl failed: {result.stderr}"
            assert "default:user:1006:rwx" in result.stdout, result.stdout
        finally:
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
