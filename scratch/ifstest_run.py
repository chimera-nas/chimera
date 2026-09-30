# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Experiment: run IFSTest headlessly against one drive.

Usage: ifstest_run.py <ifs_test_kit dir> <drive, e.g. T:> <log file> <timeout s>

IFSTest is a GUI-era tool: a missing DLL or a hard error raises a modal dialog
that nobody on a CI runner can dismiss.  The error mode set here is inherited
by the child, so such a failure becomes an exit code instead of a hang.
"""
import ctypes
from pathlib import Path
import secrets
import subprocess
import sys
import time

SEM_FAILCRITICALERRORS = 0x0001
SEM_NOGPFAULTERRORBOX = 0x0002
SEM_NOOPENFILEERRORBOX = 0x8000


def run(kit, drive, log, timeout):
    kit = Path(kit)
    ctypes.windll.kernel32.SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX |
                                        SEM_NOOPENFILEERRORBOX)
    password = "Ifs-" + secrets.token_urlsafe(18)
    # The command Microsoft's RunIfsTests.cmd uses for a third-party NTFS-style
    # file system (skipping only the antivirus group).
    args = [str(kit / "ifstest.exe"), drive, "-g", "Virus", "/n", str(log), "/N", "356789AB",
            "/T", "/p", "/m", "/E", "/j", "/r", "c:", "-d", "\\Ntfs",
            "-a", "\\datacoh.exe", "/u", "ifstest", "/U", password]
    print("running:", " ".join(a if a != password else "<password>" for a in args), flush=True)
    start = time.monotonic()
    proc = subprocess.Popen(args, cwd=kit)
    try:
        rc = proc.wait(timeout=timeout)
        print(f"ifstest exited {rc} ({rc & 0xffffffff:#010x}) after {time.monotonic() - start:.0f}s",
              flush=True)
    except subprocess.TimeoutExpired:
        print(f"ifstest still running after {timeout}s; killing", flush=True)
        proc.kill()
        proc.wait()
    log = Path(log)
    print(f"log {log}: {log.stat().st_size if log.exists() else 'not written'}", flush=True)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
