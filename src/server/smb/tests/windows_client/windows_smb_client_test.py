# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Drive a native chimera SMB server with the real Windows SMB client.

The daemon runs on this machine and the Windows redirector (LanmanWorkstation)
connects to it over loopback, so every operation below goes through the same
client stack Explorer, robocopy and every Win32 application use: NTLMv2 logon,
SMB 3.1.1 negotiation with signing (required by default on Windows 11 24H2 and
Server 2025), leases, the client's own caching, and whatever FSCTLs the Win32
API decides to issue.

Port 445 belongs to the kernel's own SMB server and cannot be released without
a reboot, so the daemon listens elsewhere and the drive is mapped with
`net use /TCPPORT`, which the client supports from Windows 11 24H2 and Windows
Server 2025.  On an older client the test reports itself skipped (exit 77).

Every check runs even when an earlier one fails, so one run reports the whole
compatibility picture; the exit status is non-zero when any check failed.

Usage: windows_smb_client_test.py <chimera.exe> <memfs|diskfs|cairn>... \
           [--junit FILE] [--duration FILE]
"""
import argparse
import ctypes
import hashlib
import json
import mmap
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import string
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from xml.etree import ElementTree

if os.name == "nt":
    import msvcrt

SKIP = 77
USER = "smbclient"
PASSWORD = "Chimera-Test-1"
SHARE = "share"

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True) if os.name == "nt" else None


def free_ports(count):
    listeners = [socket.socket() for _ in range(count)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def client_supports_alternate_port():
    usage = subprocess.run(["net", "use", "/?"], capture_output=True, text=True)
    return "TCPPORT" in (usage.stdout + usage.stderr).upper()


def daemon_config(backend, scratch, smb_port, metrics_port):
    config = {
        "common": {"huge_pages": False, "sync_delegation_threads": 2},
        "server": {
            "threads": 2,
            "nfs_enabled": False,
            "s3_enabled": False,
            "smb_enabled": True,
            "smb_port": smb_port,
            "metrics_port": metrics_port,
            "state_dir": (scratch / "state").as_posix(),
        },
        "users": [{
            "username": USER, "password": PASSWORD, "smbpasswd": PASSWORD,
            "uid": 1000, "gid": 1000,
        }],
        "filesystems": {"fs0": {"module": backend}},
        "mounts": {"share": {"module": backend, "path": "fs0"}},
        "shares": {SHARE: {"path": "/share"}},
    }
    if backend == "diskfs":
        devices = []
        for i in range(2):
            device = scratch / f"device-{i}.img"
            with device.open("wb") as f:
                f.truncate(1024 * 1024 * 1024)
            devices.append({"type": "pread", "size": 1, "path": device.as_posix()})
        config["server"]["vfs"] = {"diskfs": {"path": None, "config": {
            "initialize": True, "devices": devices, "intent_log_size": 67108864}}}
    elif backend == "cairn":
        (scratch / "cairn").mkdir()
        config["server"]["vfs"] = {"cairn": {"path": None, "config": {
            "initialize": True, "path": (scratch / "cairn").as_posix()}}}
    elif backend != "memfs":
        raise SystemExit(f"unknown backend {backend}")
    return config


def wait_for_port(proc, port, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited during startup: {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"daemon did not listen on {port} within {timeout}s")
            time.sleep(0.2)


def free_drive_letter():
    for letter in reversed(string.ascii_uppercase[7:]):
        if not os.path.exists(f"{letter}:\\"):
            return letter
    raise RuntimeError("no free drive letter")


def net_use(drive, port):
    """Map the share, retrying: the first logon can race the listener."""
    command = ["net", "use", f"{drive}:", f"\\\\127.0.0.1\\{SHARE}", f"/TCPPORT:{port}",
               f"/USER:{USER}", PASSWORD, "/PERSISTENT:NO"]
    last = None
    for _ in range(5):
        last = subprocess.run(command, capture_output=True, text=True)
        if last.returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError(f"net use failed ({last.returncode}):\n{last.stdout}{last.stderr}")


def net_use_delete(drive):
    subprocess.run(["net", "use", f"{drive}:", "/DELETE", "/Y"], capture_output=True)


# --- Win32 helpers ----------------------------------------------------------

GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
CREATE_ALWAYS = 2
FILE_ATTRIBUTE_NORMAL = 0x80
FILE_ATTRIBUTE_HIDDEN = 0x2
FILE_FLAG_DELETE_ON_CLOSE = 0x04000000
INVALID_HANDLE = ctypes.c_void_p(-1).value
ERROR_SHARING_VIOLATION = 32
ERROR_LOCK_VIOLATION = 33

if kernel32:
    kernel32.CreateFileW.restype = ctypes.c_void_p
    kernel32.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                                     ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                                     ctypes.c_void_p]
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.GetFileAttributesW.restype = ctypes.c_uint32
    kernel32.GetFileAttributesW.argtypes = [ctypes.c_wchar_p]
    kernel32.SetFileAttributesW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32]
    kernel32.CopyFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_int]
    kernel32.FindFirstChangeNotificationW.restype = ctypes.c_void_p
    kernel32.FindFirstChangeNotificationW.argtypes = [ctypes.c_wchar_p, ctypes.c_int,
                                                      ctypes.c_uint32]
    kernel32.FindCloseChangeNotification.argtypes = [ctypes.c_void_p]
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.WaitForSingleObject.restype = ctypes.c_uint32


def create_file(path, access, share, disposition, flags=FILE_ATTRIBUTE_NORMAL):
    handle = kernel32.CreateFileW(str(path), access, share, None, disposition, flags, None)
    if handle == INVALID_HANDLE:
        return None, ctypes.get_last_error()
    return handle, 0


def win_check(ok, what):
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error(), what)


# --- checks -----------------------------------------------------------------
#
# Each takes a fresh directory on the mapped drive.  An assertion failure or an
# exception fails that check alone.

def check_directories(d):
    nested = d / "a" / "b" / "c"
    nested.mkdir(parents=True)
    assert nested.is_dir()
    assert sorted(p.name for p in (d / "a").iterdir()) == ["b"]
    nested.rmdir()
    (d / "a" / "b").rmdir()
    assert not (d / "a" / "b").exists()


def check_small_file(d):
    text = "hello from the Windows SMB client\r\nsecond line\r\n"
    (d / "small.txt").write_text(text, encoding="utf-8", newline="")
    assert (d / "small.txt").read_text(encoding="utf-8", newline="") == text
    assert (d / "small.txt").stat().st_size == len(text.encode())


def check_large_file(d):
    chunk = os.urandom(1024 * 1024)
    digest = hashlib.sha256()
    with (d / "large.bin").open("wb") as f:
        for i in range(48):
            block = bytes([i]) + chunk[1:]
            f.write(block)
            digest.update(block)
    assert (d / "large.bin").stat().st_size == 48 * 1024 * 1024
    readback = hashlib.sha256()
    with (d / "large.bin").open("rb") as f:
        while block := f.read(4 * 1024 * 1024):
            readback.update(block)
    assert readback.hexdigest() == digest.hexdigest(), "content mismatch"


def check_random_access(d):
    path = d / "random.bin"
    with path.open("wb") as f:
        f.write(b"A" * 8192)
    with path.open("r+b") as f:
        f.seek(100)
        f.write(b"middle")
        f.seek(20000)  # past EOF: the gap must read back as zeros
        f.write(b"tail")
    data = path.read_bytes()
    assert len(data) == 20004
    assert data[100:106] == b"middle"
    assert data[8192:20000] == bytes(20000 - 8192)
    assert data[-4:] == b"tail"


def check_append(d):
    path = d / "append.log"
    for i in range(20):
        with path.open("a", encoding="ascii", newline="") as f:
            f.write(f"line {i}\n")
    assert path.read_text(encoding="ascii").splitlines() == [f"line {i}" for i in range(20)]


def check_truncate(d):
    path = d / "truncate.bin"
    path.write_bytes(b"x" * 10000)
    os.truncate(path, 1234)
    assert path.stat().st_size == 1234
    os.truncate(path, 50000)
    data = path.read_bytes()
    assert len(data) == 50000 and data[:1234] == b"x" * 1234 and not any(data[1234:])


def check_rename(d):
    (d / "src.txt").write_text("one")
    os.rename(d / "src.txt", d / "dst.txt")
    assert not (d / "src.txt").exists() and (d / "dst.txt").read_text() == "one"
    (d / "other.txt").write_text("two")
    os.replace(d / "other.txt", d / "dst.txt")  # over an existing file
    assert (d / "dst.txt").read_text() == "two" and not (d / "other.txt").exists()
    (d / "dir1").mkdir()
    (d / "dir1" / "f").write_text("inside")
    os.rename(d / "dir1", d / "dir2")
    assert (d / "dir2" / "f").read_text() == "inside"
    (d / "dir3").mkdir()
    os.rename(d / "dst.txt", d / "dir3" / "moved.txt")  # across directories
    assert (d / "dir3" / "moved.txt").read_text() == "two"


def check_delete(d):
    (d / "gone.txt").write_text("bye")
    (d / "gone.txt").unlink()
    assert not (d / "gone.txt").exists()
    tree = d / "tree"
    for i in range(5):
        (tree / f"sub{i}").mkdir(parents=True)
        (tree / f"sub{i}" / "file").write_bytes(b"z" * i)
    shutil.rmtree(tree)
    assert not tree.exists()


def check_timestamps(d):
    path = d / "times.txt"
    path.write_text("t")
    when = 1_600_000_000.5
    os.utime(path, (when, when))
    st = path.stat()
    assert abs(st.st_mtime - when) < 0.01, f"mtime {st.st_mtime} != {when}"
    assert abs(st.st_atime - when) < 0.01, f"atime {st.st_atime} != {when}"


def check_readonly_attribute(d):
    path = d / "readonly.txt"
    path.write_text("locked")
    os.chmod(path, stat.S_IREAD)
    try:
        assert not os.access(path, os.W_OK)
        try:
            path.open("w").close()
        except PermissionError:
            pass
        else:
            raise AssertionError("opened a read-only file for writing")
    finally:
        os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
    path.write_text("unlocked")


def check_hidden_attribute(d):
    path = d / "hidden.txt"
    path.write_text("h")
    win_check(kernel32.SetFileAttributesW(str(path), FILE_ATTRIBUTE_HIDDEN), "SetFileAttributes")
    attrs = kernel32.GetFileAttributesW(str(path))
    assert attrs != 0xFFFFFFFF and attrs & FILE_ATTRIBUTE_HIDDEN, f"attributes {attrs:#x}"
    win_check(kernel32.SetFileAttributesW(str(path), FILE_ATTRIBUTE_NORMAL), "SetFileAttributes")
    assert not kernel32.GetFileAttributesW(str(path)) & FILE_ATTRIBUTE_HIDDEN


def check_enumeration(d):
    expected = {}
    for i in range(600):
        name = f"entry-{i:04d}.dat"
        (d / name).write_bytes(b"e" * (i % 97))
        expected[name] = i % 97
    seen = {e.name: e.stat().st_size for e in os.scandir(d)}
    assert seen == expected, f"{len(seen)} entries listed, {len(expected)} created"


def check_unicode_names(d):
    names = ["naïve café.txt", "файл.txt", "日本語のファイル.txt", "emoji-🙂.txt"]
    for name in names:
        (d / name).write_text(name, encoding="utf-8")
    assert sorted(os.listdir(d)) == sorted(names)
    for name in names:
        assert (d / name).read_text(encoding="utf-8") == name


def check_long_name(d):
    name = ("L" * 240) + ".txt"
    (d / name).write_text("long")
    assert os.listdir(d) == [name]


def check_case_insensitive_lookup(d):
    (d / "MixedCase.txt").write_text("case")
    assert (d / "mixedcase.TXT").read_text() == "case"


def check_sharing_violation(d):
    path = d / "shared.bin"
    path.write_bytes(b"s")
    first, err = create_file(path, GENERIC_READ | GENERIC_WRITE, 0, OPEN_EXISTING)
    assert first, f"first open failed: {err}"
    try:
        second, err = create_file(path, GENERIC_READ, 7, OPEN_EXISTING)
        if second:
            kernel32.CloseHandle(second)
            raise AssertionError("second open succeeded against a share-mode 0 handle")
        assert err == ERROR_SHARING_VIOLATION, f"expected sharing violation, got {err}"
    finally:
        kernel32.CloseHandle(first)


def check_byte_range_lock(d):
    path = d / "locked.bin"
    path.write_bytes(b"0123456789" * 10)
    holder = os.open(path, os.O_RDWR | os.O_BINARY)
    other = os.open(path, os.O_RDWR | os.O_BINARY)
    try:
        os.lseek(holder, 10, os.SEEK_SET)
        msvcrt.locking(holder, msvcrt.LK_NBLCK, 20)
        os.lseek(other, 15, os.SEEK_SET)
        try:
            os.read(other, 5)
        except OSError as e:
            assert getattr(e, "winerror", None) == ERROR_LOCK_VIOLATION, e
        else:
            raise AssertionError("read inside a locked range succeeded")
        os.lseek(other, 50, os.SEEK_SET)
        assert os.read(other, 5) == b"01234"  # outside the range
        os.lseek(holder, 10, os.SEEK_SET)
        msvcrt.locking(holder, msvcrt.LK_UNLCK, 20)
        os.lseek(other, 15, os.SEEK_SET)
        assert os.read(other, 5) == b"56789"
    finally:
        os.close(holder)
        os.close(other)


def check_delete_on_close(d):
    path = d / "temporary.bin"
    handle, err = create_file(path, GENERIC_READ | GENERIC_WRITE, 7, CREATE_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE)
    assert handle, f"create failed: {err}"
    assert path.exists()
    kernel32.CloseHandle(handle)
    assert not path.exists()


def check_server_side_copy(d):
    """CopyFileW between two files on one share uses the server's copy offload."""
    data = os.urandom(3 * 1024 * 1024 + 17)
    (d / "copy-src.bin").write_bytes(data)
    win_check(kernel32.CopyFileW(str(d / "copy-src.bin"), str(d / "copy-dst.bin"), True),
              "CopyFileW")
    assert (d / "copy-dst.bin").read_bytes() == data


def check_robocopy_roundtrip(d):
    local = Path(tempfile.mkdtemp(prefix="smbclient-robocopy-"))
    back = Path(tempfile.mkdtemp(prefix="smbclient-robocopy-back-"))
    try:
        for i in range(4):
            sub = local / f"dir{i}" / "nested"
            sub.mkdir(parents=True)
            for j in range(10):
                (sub / f"f{j}.bin").write_bytes(os.urandom(1000 * (j + 1)))
        for src, dst in ((local, d / "robo"), (d / "robo", back)):
            result = subprocess.run(["robocopy", str(src), str(dst), "/E", "/COPY:DAT",
                                     "/R:0", "/W:0", "/NP", "/NFL", "/NDL"],
                                    capture_output=True, text=True)
            assert result.returncode < 8, f"robocopy {result.returncode}:\n{result.stdout}"
        for f in local.rglob("*.bin"):
            assert (back / f.relative_to(local)).read_bytes() == f.read_bytes(), f
    finally:
        shutil.rmtree(local, ignore_errors=True)
        shutil.rmtree(back, ignore_errors=True)


def check_memory_mapped_file(d):
    path = d / "mapped.bin"
    path.write_bytes(bytes(65536))
    with path.open("r+b") as f, mmap.mmap(f.fileno(), 0) as m:
        m[1000:1005] = b"hello"
        m[65530:65536] = b"ending"
        m.flush()
    data = path.read_bytes()
    assert data[1000:1005] == b"hello" and data[-6:] == b"ending"


def check_change_notify(d):
    FILE_NOTIFY_CHANGE_FILE_NAME = 0x1
    handle = kernel32.FindFirstChangeNotificationW(str(d), False, FILE_NOTIFY_CHANGE_FILE_NAME)
    assert handle not in (None, INVALID_HANDLE), f"FindFirstChangeNotification {ctypes.get_last_error()}"
    try:
        timer = threading.Timer(0.5, lambda: (d / "notify.txt").write_text("n"))
        timer.start()
        result = kernel32.WaitForSingleObject(handle, 15000)
        timer.join()
        assert result == 0, f"no change notification (wait returned {result:#x})"
    finally:
        kernel32.FindCloseChangeNotification(handle)


def check_parallel_writers(d):
    errors = []

    def writer(n):
        try:
            payload = bytes([n]) * (256 * 1024)
            for i in range(8):
                (d / f"w{n}-{i}.bin").write_bytes(payload)
                assert (d / f"w{n}-{i}.bin").read_bytes() == payload
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(f"writer {n}: {e!r}")

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert len(os.listdir(d)) == 64


def check_disk_usage(d):
    usage = shutil.disk_usage(d)
    assert usage.total > 0 and usage.free <= usage.total, usage


def check_security_descriptor(d):
    (d / "acl.txt").write_text("acl")
    result = subprocess.run(["icacls", str(d / "acl.txt")], capture_output=True, text=True)
    assert result.returncode == 0, f"icacls {result.returncode}:\n{result.stdout}{result.stderr}"


CHECKS = [
    check_directories,
    check_small_file,
    check_large_file,
    check_random_access,
    check_append,
    check_truncate,
    check_rename,
    check_delete,
    check_timestamps,
    check_readonly_attribute,
    check_hidden_attribute,
    check_enumeration,
    check_unicode_names,
    check_long_name,
    check_case_insensitive_lookup,
    check_sharing_violation,
    check_byte_range_lock,
    check_delete_on_close,
    check_server_side_copy,
    check_robocopy_roundtrip,
    check_memory_mapped_file,
    check_change_notify,
    check_parallel_writers,
    check_disk_usage,
    check_security_descriptor,
]


def run_checks(root, backend):
    results = []
    for check in CHECKS:
        name = check.__name__[len("check_"):]
        workdir = root / name
        start = time.monotonic()
        try:
            workdir.mkdir()
            check(workdir)
            failure = None
        except Exception:  # noqa: BLE001 - every failure is reported, none aborts the run
            failure = traceback.format_exc()
        elapsed = time.monotonic() - start
        results.append((f"{backend}/{name}", elapsed, failure))
        print(f"[{'PASS' if failure is None else 'FAIL'}] {backend}/{name} ({elapsed:.1f}s)",
              flush=True)
        if failure:
            print(failure, flush=True)
    return results


def write_junit(path, results, os_label):
    suites = ElementTree.Element("testsuites")
    suite = ElementTree.SubElement(suites, "testsuite", name="windows_smb_client",
                                   tests=str(len(results)),
                                   failures=str(sum(1 for r in results if r[2])))
    for name, elapsed, failure in results:
        case = ElementTree.SubElement(suite, "testcase", classname=f"windows_smb_client.{os_label}",
                                      name=f"windows_smb_client/{name}", time=f"{elapsed:.3f}")
        if failure:
            ElementTree.SubElement(case, "failure",
                                   message=failure.strip().splitlines()[-1]).text = failure
    ElementTree.ElementTree(suites).write(path, encoding="utf-8", xml_declaration=True)


def run_backend(chimera, backend):
    """Serve one backend, map it, run every check, tear it all down."""
    scratch = Path(tempfile.mkdtemp(prefix=f"chimera-smbclient-{backend}-"))
    smb_port, metrics_port = free_ports(2)
    config_path = scratch / "config.json"
    config_path.write_text(json.dumps(daemon_config(backend, scratch, smb_port, metrics_port)),
                           encoding="utf-8")
    env = os.environ.copy()
    env.update(TEMP=str(scratch), TMP=str(scratch))
    drive = None
    results = []
    with (scratch / "daemon.log").open("w+b") as log:
        proc = subprocess.Popen([chimera, "-c", str(config_path)], stdout=log, stderr=log,
                                env=env, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        try:
            wait_for_port(proc, smb_port)
            drive = free_drive_letter()
            net_use(drive, smb_port)
            print(f"{backend}: mapped {drive}: to \\\\127.0.0.1\\{SHARE} on port {smb_port}",
                  flush=True)
            root = Path(f"{drive}:\\") / f"run-{os.getpid()}"
            root.mkdir()
            results = run_checks(root, backend)
        except Exception:  # noqa: BLE001 - a setup failure is one failed result
            results.append((f"{backend}/setup", 0.0, traceback.format_exc()))
            print(results[-1][2], flush=True)
        finally:
            if drive:
                net_use_delete(drive)
            if proc.poll() is None:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            if proc.returncode != 0:
                results.append((f"{backend}/shutdown", 0.0,
                                f"daemon exited with status {proc.returncode}"))
            if any(r[2] for r in results):
                log.seek(0)
                tail = log.read().decode("utf-8", errors="replace").splitlines()[-200:]
                print(f"---- {backend} daemon log (last 200 lines) ----")
                print("\n".join(tail), flush=True)
    shutil.rmtree(scratch, ignore_errors=True)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("chimera")
    parser.add_argument("backends", nargs="+", choices=["memfs", "diskfs", "cairn"])
    parser.add_argument("--junit", help="write JUnit XML here")
    parser.add_argument("--duration", help="write the wall-clock seconds here")
    args = parser.parse_args()

    if os.name != "nt":
        print("the Windows SMB client test runs on Windows only")
        return SKIP
    if not client_supports_alternate_port():
        print("this SMB client cannot connect to an alternate port (needs Windows 11 24H2 "
              "or Windows Server 2025); skipping")
        return SKIP

    ver = sys.getwindowsversion()
    os_label = f"windows-{ver.major}.{ver.minor}.{ver.build}"
    print(f"client: {os_label}", flush=True)

    start = time.monotonic()
    results = []
    for backend in args.backends:
        results.extend(run_backend(args.chimera, backend))
    if args.junit:
        write_junit(args.junit, results, os_label)
    if args.duration:
        Path(args.duration).write_text(f"{round(time.monotonic() - start)}\n")
    failed = [r[0] for r in results if r[2]]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed"
          + (f"; failed: {', '.join(failed)}" if failed else ""), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
