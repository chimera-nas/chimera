#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Replay a Quint-generated ITF trace against chimera's POSIX client.

Each state of the trace carries a `lastOp` label naming the syscall the
model issued and the result the implementation must produce (see posix.qnt).
This harness spawns posix_driver (an in-process memfs mount behind the
chimera_posix_* API, speaking line-delimited JSON), replays every step, and
compares the driver's actual result against the model's expectation.  Any
mismatch not covered by the known-deviation registry (posix_deviations.py)
is reported as a divergence with full context and fails the run.

Model-to-real mapping maintained here (DESIGN-POSIX.md "Step and trace
contract"):
  - model pid      -> per-operation credential/umask switch in the driver
  - model (pid,fd) -> real chimera fd, learned from open/dup replies
  - model sid      -> driver directory-stream id
  - model Ino      -> real (st_dev, st_ino), learned from stat replies
  - model block i  -> BLOCK_SIZE bytes at offset i * BLOCK_SIZE; block
                      symbol 0 is a hole (zero bytes), symbol s > 0 is
                      BLOCK_SIZE repetitions of byte 0x40 + s
  - timestamps     -> abstract instants checked for monotonic consistency,
                      never predicted; explicit utimensat values map to
                      fixed wall-clock times (XTIME) checked exactly
"""

import argparse
import base64
import errno
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import posix_deviations  # noqa: E402

# The Quint model (posix_ops.qnt) and the deviation registry (posix_deviations)
# both encode Linux errno numbers.  The driver returns the host libc's errno,
# which is identical on Linux but diverges on macOS/BSD for the higher codes --
# and even swaps a pair: macOS has EAGAIN=35/EDEADLK=11 where Linux has 11/35,
# ENOTSUP=45 vs 95, ENOTEMPTY=66 vs 39, ELOOP=62 vs 40, ENAMETOOLONG=63 vs 36,
# ENOSYS=78 vs 38.  Translate host->Linux *by name* (never by number) so every
# comparison and every reconcile() stays in the model's errno space no matter
# which platform runs the suite.
_LINUX_ERRNO = {
    "EPERM": 1, "ENOENT": 2, "EIO": 5, "ENXIO": 6, "EBADF": 9, "EAGAIN": 11,
    "EWOULDBLOCK": 11, "EACCES": 13, "EBUSY": 16, "EEXIST": 17, "EXDEV": 18,
    "ENOTDIR": 20, "EISDIR": 21, "EINVAL": 22, "EMFILE": 24, "EFBIG": 27,
    "ENOSPC": 28, "ESPIPE": 29, "EROFS": 30, "EMLINK": 31, "EDEADLK": 35,
    "ENAMETOOLONG": 36, "ENOSYS": 38, "ENOTEMPTY": 39, "ELOOP": 40,
    "ENOTSUP": 95, "EOPNOTSUPP": 95,
}


def host_errno_to_linux(err):
    """Map a host errno number to the Linux value the model/registry encode.

    Identity on Linux.  Elsewhere the host's own errno table names the number
    and we look the name up in the Linux table; an unrecognized number passes
    through unchanged so a genuinely unexpected errno still shows as a mismatch.
    """
    if not err:
        return err
    name = errno.errorcode.get(err)
    if name is None:
        return err
    return _LINUX_ERRNO.get(name, err)


MOUNT = "/test"
BADFD = 999999

# Explicit utimensat instants: model reserved value -> (sec, nsec).  Kept in
# the past so a later "mark to now" still satisfies the >= monotonic check.
XTIME = {-1: (1000000, 0), -2: (2000000, 0)}

FTYPE_MAP = {"FReg": "reg", "FDir": "dir", "FLnk": "lnk", "FFifo": "fifo",
             "FSock": "sock", "FBlk": "blk", "FChr": "chr"}

ACC_FLAGS = {"AccR": os.O_RDONLY, "AccW": os.O_WRONLY, "AccRW": os.O_RDWR}

WHENCE_MAP = {"WSet": "set", "WCur": "cur", "WEnd": "end",
              "WData": "data", "WHole": "hole"}

LOCK_CMD = {"CSetlk": "setlk", "CSetlkw": "setlkw", "CGetlk": "getlk"}
LOCK_TYPE = {"LkRd": "rd", "LkWr": "wr", "LkUn": "un"}
LOCKF_CMD = {"LfLock": "lock", "LfTlock": "tlock", "LfUlock": "ulock",
             "LfTst": "test"}

# The capability/policy profile of chimera's POSIX client over memfs,
# established empirically by the probe below (run with --probe) and pinned
# here as a regression check; posix_run.qnt's posixMemfs instance pins trace
# generation to the same profile.  A trace whose LInit profile disagrees is
# skipped (exit 77); --check-profile re-measures and diffs against this.
# None = not measurable / any value accepted (withRoot is harness-chosen;
# errLockAgain is unobservable while memfs lacks lock support, see PD1).
# Probed 2026-08-10 against memfs (block_size 4096):
PROFILE = {
    "copyRange": True,
    "cloneRange": True,
    "seekHole": True,
    "withRoot": None,
    "gidFromParent": False,
    "sgidInherit": False,
    "writeClearsSets": True,
    "pwriteAppends": False,
    "renameCtime": True,
    "strictAtime": False,
    "stickyWriteArm": False,
    "errNotempty": True,
    "errStickyAcces": True,
    "errUnlinkDirIsdir": True,
    "errLockAgain": None,
}

# Per-backend live profiles.  diskfs and cairn probed identically on
# 2026-08-11: the only drift from memfs is that clone_range is unsupported.
# Trace generation for them uses posix_run.qnt's posixDisk instance, which
# pins the same profile, so their traces never skip either.
DISK_PROFILE = dict(PROFILE, cloneRange=False)

# NFS loopback paths (client -> in-process server -> backend) probed
# 2026-08-12; identical across the three backends behind each version, so
# the profiles are version-keyed (posixNfs3/posixNfs4 instances match).
NFS3_PROFILE = dict(PROFILE, copyRange=False, cloneRange=False,
                    seekHole=False, strictAtime=True)
NFS4_PROFILE = dict(PROFILE, copyRange=False, cloneRange=False,
                    seekHole=True, strictAtime=False)

PROFILES = {
    "memfs": PROFILE,
    "diskfs": DISK_PROFILE,
    "cairn": DISK_PROFILE,
    "nfs3_memfs": NFS3_PROFILE,
    "nfs3_diskfs": NFS3_PROFILE,
    "nfs3_cairn": NFS3_PROFILE,
    "nfs4_memfs": NFS4_PROFILE,
    "nfs4_diskfs": NFS4_PROFILE,
    "nfs4_cairn": NFS4_PROFILE,
}


class TraceFormatError(Exception):
    pass


class Divergence(Exception):
    def __init__(self, step, op, mismatches):
        self.step = step
        self.op = op
        self.mismatches = mismatches
        super().__init__(f"step {step}: " + "; ".join(mismatches))


def itf_decode(v):
    """Decode one ITF-encoded Quint value into plain Python data."""
    if isinstance(v, dict):
        special = [k for k in v if k.startswith("#")]
        if special == ["#bigint"]:
            return int(v["#bigint"])
        if special == ["#map"]:
            return {itf_decode(k): itf_decode(val) for k, val in v["#map"]}
        if special == ["#set"]:
            return [itf_decode(x) for x in v["#set"]]
        if special == ["#tup"]:
            return tuple(itf_decode(x) for x in v["#tup"])
        if special:
            raise TraceFormatError(f"unrecognized ITF encoding {special}")
        if set(v.keys()) == {"tag", "value"}:
            return {"tag": v["tag"], "value": itf_decode(v["value"])}
        return {k: itf_decode(val) for k, val in v.items()}
    if isinstance(v, list):
        return [itf_decode(x) for x in v]
    if isinstance(v, (str, bool, int)):
        return v
    raise TraceFormatError(f"unrecognized ITF value {v!r}")


def load_trace(path):
    with open(path) as f:
        raw = json.load(f)
    if "states" not in raw or "vars" not in raw:
        raise TraceFormatError(f"{path}: not an ITF trace")
    states = []
    for st in raw["states"]:
        # Instance mains namespace the state variables
        # (posixMemfs::posix::fs); keep the base name.
        states.append({k.rsplit("::", 1)[-1]: itf_decode(v)
                       for k, v in st.items()
                       if k != "#meta" and not k.startswith("mbt::")})
    for st in states:
        if "lastOp" not in st or "fs" not in st:
            raise TraceFormatError(f"{path}: state missing lastOp/fs")
    return states


class Driver:
    """posix_driver process wrapper: one JSON request line per call."""

    def __init__(self, driver_path, backend="memfs"):
        self.storage = None
        argv = [driver_path]
        if backend != "memfs":
            # Real-storage backends get a private scratch dir, removed on
            # close (diskfs writes a sparse device image, cairn a rocksdb).
            self.storage = tempfile.mkdtemp(prefix=f"pq_{backend}_",
                                            dir=os.getcwd())
            argv = [driver_path, backend, self.storage]
        self.stderr_file = tempfile.NamedTemporaryFile(
            prefix="posix_mbt_", suffix=".stderr", delete=False)
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self.stderr_file, text=True)
        ready = self.proc.stdout.readline()
        try:
            ready_obj = json.loads(ready)
        except json.JSONDecodeError:
            raise RuntimeError(
                f"driver failed to start: {ready!r}\n{self.stderr_tail()}")
        if not ready_obj.get("ready"):
            raise RuntimeError(f"driver not ready: {ready_obj}")
        self.block_size = ready_obj["blocksize"]

    def request(self, **req):
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(
                f"driver died on request {req}\n{self.stderr_tail()}")
        resp = json.loads(line)
        # Normalize the driver's host errno into the model's Linux errno space
        # so the comparison is platform-independent (see host_errno_to_linux).
        if isinstance(resp, dict) and "err" in resp:
            resp["err"] = host_errno_to_linux(resp["err"])
        return resp

    def stderr_tail(self, lines=50):
        try:
            with open(self.stderr_file.name, errors="replace") as f:
                return "".join(f.readlines()[-lines:])
        except OSError:
            return "<no driver stderr>"

    def close(self):
        try:
            if self.proc.poll() is None:
                self.proc.stdin.write(json.dumps({"op": "shutdown"}) + "\n")
                self.proc.stdin.flush()
                self.proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired, BrokenPipeError):
            self.proc.kill()
            self.proc.wait()
        finally:
            self.stderr_file.close()
            os.unlink(self.stderr_file.name)
            if self.storage is not None:
                shutil.rmtree(self.storage, ignore_errors=True)


# Over-long component sentinels (posix_ops NLONG/PLONG): materialize to real
# strings that exceed NAME_MAX (256) / make the whole path exceed PATH_MAX
# (4096), so the ENAMETOOLONG the model predicts actually fires in chimera.
_NLONG = "@nlong"
_PLONG = "@plong"


def _expand(c):
    if c == _NLONG:
        return "n" * 300     # one component > NAME_MAX (256)
    if c == _PLONG:
        return "p" * 5000    # makes the whole path > PATH_MAX (4096)
    return c


def real_path(pth):
    comps = [_expand(c) for c in pth["comps"]]
    if pth["abs"]:
        p = MOUNT + "".join("/" + c for c in comps)
        if pth["slash"] and comps:
            p += "/"
        return p
    p = "/".join(comps)
    if pth["slash"] and comps:
        p += "/"
    return p


def real_target(tgt):
    comps = [_expand(c) for c in tgt["comps"]]
    if tgt["abs"]:
        return MOUNT + "".join("/" + c for c in comps)
    return "/".join(comps)


def creds_for(with_root):
    return {
        0: {"uid": 0 if with_root else 100, "gid": 10, "gids": [10, 30]},
        1: {"uid": 200, "gid": 20, "gids": [20, 30]},
    }


class Replayer:
    def __init__(self, driver, caps, verbose=False):
        self.drv = driver
        self.bs = driver.block_size
        self.caps = caps
        self.verbose = verbose
        self.fdmap = {}       # (pid, model fd) -> real fd
        self.sidmap = {}      # model sid -> driver sid
        self.inomap = {}      # model ino -> (dev, real ino)
        self.shadow = {}      # model ino -> bytearray (fsx-style byte content)
        self.timemap = {}     # (model ino, field) -> (abstract, (sec, ns))
        self.history = []
        self.deviations_hit = {}
        self.audit_exempt = set()  # model paths of PD24 residue nodes
        self._cur_tag = None
        self._cur_req = None
        self._cur_fs = None
        self._cur_ps = None

        for pid, cred in creds_for(caps["withRoot"]).items():
            self.drv.request(op="setcred", pid=pid, **cred)

    # -- helpers ----------------------------------------------------------

    # --- fsx-style byte shadow ------------------------------------------
    # Exact file content is verified against a byte-accurate shadow (keyed by
    # the immutable model ino, which is monotonic and never reused, so dup /
    # hardlink / rename / unlink-with-open-fd share content for free and no
    # clear-on-create is needed).  A write stamps offset-dependent, always-
    # nonzero bytes so a hole (byte 0) is distinguishable and any off-by-N is
    # caught.
    @staticmethod
    def pat_byte(pat, pos):
        return 1 + ((31 * pat + pos) % 255)

    def write_bytes(self, pat, off, length):
        return bytes(self.pat_byte(pat, off + i) for i in range(length))

    def shadow_apply(self, ino, off, data):
        sh = self.shadow.setdefault(ino, bytearray())
        end = off + len(data)
        if len(sh) < end:
            sh.extend(b"\0" * (end - len(sh)))   # sparse gap reads as zero
        sh[off:end] = data

    def shadow_read(self, ino, off, count):
        sh = self.shadow.get(ino, b"")
        chunk = bytes(sh[off:off + count])
        if len(chunk) < count:                   # in-size hole past written end
            chunk += b"\0" * (count - len(chunk))
        return chunk

    def shadow_resize(self, ino, n):
        sh = self.shadow.setdefault(ino, bytearray())
        if n < len(sh):
            del sh[n:]
        else:
            sh.extend(b"\0" * (n - len(sh)))

    def shadow_punch(self, ino, off, length):
        sh = self.shadow.get(ino)
        if sh is None:
            return
        hi = min(off + length, len(sh))
        if hi > off:
            sh[off:hi] = b"\0" * (hi - off)

    def model_ino_of_fd(self, pid, mfd):
        ps = self._cur_ps
        if ps is None:
            return None
        ofd = ps["fds"].get((pid, mfd))
        if ofd is None:
            return None
        return ps["ofds"][ofd]["ino"]

    def path_ino(self, post_fs, comps):
        ino = 0  # ROOT
        for name in comps:
            node = post_fs["inodes"].get(ino)
            if node is None:
                return None
            ino = node["ents"].get(name)
            if ino is None:
                return None
        return ino

    def rfd(self, pid, mfd):
        return self.fdmap.get((pid, mfd), BADFD)

    def rsid(self, msid):
        return self.sidmap.get(msid, -1)

    def check_status(self, expected, actual, mism):
        """True if the errno matches (proceed with success-path checks)."""
        if actual == expected:
            return True
        dev = posix_deviations.reconcile(self._cur_tag, self._cur_req,
                                         expected, actual, self._cur_fs)
        if dev is not None:
            self.deviations_hit[dev.id] = self.deviations_hit.get(dev.id,
                                                                  0) + 1
            return False
        mism.append(f"errno: expected {expected}, got {actual}")
        return False

    def check_time(self, mino, field, abstract, wire, mism):
        if field == "atime" and not self.caps["strictAtime"]:
            return
        wire = tuple(wire)
        if abstract < 0:
            want = XTIME.get(abstract)
            if want is None:
                mism.append(f"{field}: unmapped explicit instant {abstract}")
            elif wire != want:
                mism.append(f"{field}: explicit instant {abstract}: "
                            f"expected {want}, got {wire}")
            self.timemap[(mino, field)] = (abstract, wire)
            return
        key = (mino, field)
        prev = self.timemap.get(key)
        if prev is None or prev[0] < 0:
            self.timemap[key] = (abstract, wire)
        elif abstract == prev[0]:
            if wire != prev[1]:
                mism.append(f"{field}: model instant unchanged ({abstract}) "
                            f"but wire value moved {prev[1]} -> {wire}")
        elif abstract > prev[0]:
            if wire < prev[1]:
                mism.append(f"{field}: model instant advanced "
                            f"{prev[0]} -> {abstract} but wire value went "
                            f"backwards {prev[1]} -> {wire}")
            self.timemap[key] = (abstract, wire)
        else:
            mism.append(f"{field}: model instant went backwards "
                        f"{prev[0]} -> {abstract} (harness bug?)")

    def check_statres(self, rv, res, post_fs, mism):
        """Compare a driver stat reply against the model's SStatR payload."""
        want_ftype = FTYPE_MAP[rv["ftype"]["tag"]]
        if res.get("ftype") != want_ftype:
            mism.append(f"ftype: expected {want_ftype}, got "
                        f"{res.get('ftype')}")
        if rv["ftype"]["tag"] == "FLnk":
            # PD16: memfs creates symlinks with mode 0755; POSIX/Linux use
            # 0777 (and never consult it).  Skip the mode check for links.
            pass
        elif res.get("mode") != rv["mode"]:
            mism.append(f"mode: expected {rv['mode']:#o}, "
                        f"got {res.get('mode', 0):#o}")
        if res.get("uid") != rv["uid"]:
            mism.append(f"uid: expected {rv['uid']}, got {res.get('uid')}")
        if res.get("gid") != rv["gid"]:
            mism.append(f"gid: expected {rv['gid']}, got {res.get('gid')}")
        if res.get("nlink") != rv["nlink"]:
            mism.append(f"nlink: expected {rv['nlink']}, "
                        f"got {res.get('nlink')}")
        if rv["ftype"]["tag"] == "FReg":
            want = rv["sizeB"]
            if res.get("size") != want:
                mism.append(f"size: expected {want}, got {res.get('size')}")
        elif rv["ftype"]["tag"] == "FLnk":
            node = post_fs["inodes"].get(rv["ino"])
            if node is not None:
                want = len(real_target(node["target"]))
                if res.get("size") != want:
                    mism.append(f"symlink size: expected {want}, "
                                f"got {res.get('size')}")
        mino = rv["ino"]
        ident = (res.get("dev"), res.get("ino"))
        known = self.inomap.get(mino)
        if known is None:
            for other, oident in self.inomap.items():
                if oident == ident and other != mino \
                        and other in post_fs["inodes"]:
                    mism.append(f"st_ino {ident} of model ino {mino} "
                                f"collides with live model ino {other}")
            self.inomap[mino] = ident
        elif known != ident:
            mism.append(f"identity: model ino {mino} previously "
                        f"{known}, now {ident}")
        self.check_time(mino, "atime", rv["atime"], res["atime"], mism)
        self.check_time(mino, "mtime", rv["mtime"], res["mtime"], mism)
        self.check_time(mino, "ctime", rv["ctime"], res["ctime"], mism)

    # -- per-request handlers ---------------------------------------------

    def op_open(self, pid, rv, res_v, post_fs, mism):
        fl = rv["fl"]
        flags = ACC_FLAGS[fl["acc"]["tag"]]
        if fl["creat"]:
            flags |= os.O_CREAT
        if fl["excl"]:
            flags |= os.O_EXCL
        if fl["trunc"]:
            flags |= os.O_TRUNC
        if fl["appendF"]:
            flags |= os.O_APPEND
        if fl["directory"]:
            flags |= os.O_DIRECTORY
        if fl["nofollow"]:
            flags |= os.O_NOFOLLOW
        if rv["dfd"] == -1:
            r = self.drv.request(op="open", pid=pid,
                                 path=real_path(rv["pth"]), flags=flags,
                                 mode=fl["mode"])
        else:
            r = self.drv.request(op="openat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]), flags=flags,
                                 mode=fl["mode"])
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.fdmap[(pid, res_v["fd"])] = r["ret"]
            if fl["trunc"]:
                # O_TRUNC zeroed an existing regular file (harmless for a
                # freshly created one): drop its shadow content.
                ino = self.model_ino_of_fd(pid, res_v["fd"])
                if ino is not None:
                    self.shadow_resize(ino, 0)
        elif res_v["e"] == 24 and r["err"] == 0 and r["ret"] >= 0:
            # PD24 (EMFILE): the model's 16-slot table was full but
            # chimera's larger table let the open succeed.  Close the
            # stray descriptor to restore parity, and when O_CREAT minted
            # a node the model never created, exempt it from the final
            # audit (the state divergence is the accepted PD24 residue).
            self.drv.request(op="close", pid=pid, fd=r["ret"])
            if fl["creat"]:
                self.audit_exempt.add("/" + "/".join(rv["pth"]["comps"]))
        return r

    def op_close(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="close", pid=pid, fd=self.rfd(pid, rv["fd"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.fdmap.pop((pid, rv["fd"]), None)
        return r

    def op_dup(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="dup", pid=pid, fd=self.rfd(pid, rv["fd"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.fdmap[(pid, res_v["fd"])] = r["ret"]
        return r

    def op_dup2(self, pid, rv, res_v, post_fs, mism):
        target = self.fdmap.get((pid, rv["nfd"]))
        if rv["fd"] == rv["nfd"] or target is not None:
            # A live target (or self-dup): real dup2 exercises the implicit
            # close of the old description.
            r = self.drv.request(op="dup2", pid=pid,
                                 fd=self.rfd(pid, rv["fd"]),
                                 nfd=self.rfd(pid, rv["nfd"]))
        else:
            # The model's nfd names a free slot; chimera fd numbers are its
            # own, so plain dup() is observationally identical here.
            r = self.drv.request(op="dup", pid=pid,
                                 fd=self.rfd(pid, rv["fd"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.fdmap[(pid, rv["nfd"])] = r["ret"]
        return r

    def _fd_is_model_dir(self, pid, mfd, post_fs):
        """True when the model maps (pid, fd) to a directory inode."""
        ps = self._cur_ps
        if ps is None:
            return False
        ofd = ps["fds"].get((pid, mfd))
        if ofd is None:
            return False
        ino = ps["ofds"][ofd]["ino"]
        node = post_fs["inodes"].get(ino)
        return node is not None and node["ftype"]["tag"] == "FDir"

    def op_lseek(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="lseek", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             off=rv["off"],
                             whence=WHENCE_MAP[rv["wh"]["tag"]])
        local = []
        if self.check_status(res_v["e"], r["err"], local) \
                and res_v["e"] == 0:
            want = res_v["off"]
            if r["ret"] != want:
                local.append(f"lseek: expected offset {want}, "
                             f"got {r['ret']}")
        if local and rv["wh"]["tag"] in ("WEnd", "WCur", "WData", "WHole") \
                and self._fd_is_model_dir(pid, rv["fd"], post_fs):
            # PD25: POSIX leaves a directory's st_size unspecified; the
            # model abstracts it as 0 while memfs reports a block, so
            # size-relative seeks (and SEEK_DATA/SEEK_HOLE, whose ENXIO
            # boundary is the size) on directory descriptors legitimately
            # disagree.  Accepted, recorded, never fatal.
            self.deviations_hit["PD25"] = \
                self.deviations_hit.get("PD25", 0) + 1
        else:
            mism.extend(local)
        return r

    def op_read(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="read", pid=pid, fd=self.rfd(pid, rv["fd"]),
                             len=rv["len"])
        self._check_read(r, res_v, mism)
        return r

    def op_pread(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="pread", pid=pid, fd=self.rfd(pid, rv["fd"]),
                             off=rv["off"], len=rv["len"])
        self._check_read(r, res_v, mism)
        return r

    def _check_read(self, r, res_v, mism):
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            count = res_v["n"]
            if r["ret"] != count:
                mism.append(f"read count: expected {count}, got {r['ret']}")
            data = base64.b64decode(r.get("data", ""))
            expect = self.shadow_read(res_v["ino"], res_v["off"], count)
            if data != expect:
                mism.append("read data mismatch"
                            + diff_bytes(expect, data, self.bs))

    def _write_and_shadow(self, op, pid, rv, res_v, **kw):
        # Stamp offset-dependent bytes at the model's landing offset, issue the
        # write, and (on success) record them in the shadow.
        off = res_v.get("off", 0)
        data = self.write_bytes(rv["pat"], off, rv["len"])
        r = self.drv.request(op=op, pid=pid, fd=self.rfd(pid, rv["fd"]),
                             data=base64.b64encode(data).decode(), **kw)
        if res_v["e"] == 0:
            self.shadow_apply(res_v["ino"], res_v["off"], data)
        return r

    def op_write(self, pid, rv, res_v, post_fs, mism):
        r = self._write_and_shadow("write", pid, rv, res_v)
        self._check_write(r, res_v, mism)
        return r

    def op_pwrite(self, pid, rv, res_v, post_fs, mism):
        r = self._write_and_shadow("pwrite", pid, rv, res_v, off=rv["off"])
        self._check_write(r, res_v, mism)
        return r

    def _check_write(self, r, res_v, mism):
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            if r["ret"] != res_v["n"]:
                mism.append(f"write count: expected {res_v['n']}, "
                            f"got {r['ret']}")

    # Vectored I/O.  POSIX-observably identical to the scalar forms (the
    # split_iovec buffer split is a driver-internal detail that must not leak
    # into the shadow), so the checks and shadow update are shared.
    def op_readv(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="readv", pid=pid, fd=self.rfd(pid, rv["fd"]),
                             len=rv["len"])
        self._check_read(r, res_v, mism)
        return r

    def op_preadv(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="preadv", pid=pid, fd=self.rfd(pid, rv["fd"]),
                             off=rv["off"], len=rv["len"])
        self._check_read(r, res_v, mism)
        return r

    def op_writev(self, pid, rv, res_v, post_fs, mism):
        r = self._write_and_shadow("writev", pid, rv, res_v)
        self._check_write(r, res_v, mism)
        return r

    def op_pwritev(self, pid, rv, res_v, post_fs, mism):
        r = self._write_and_shadow("pwritev", pid, rv, res_v, off=rv["off"])
        self._check_write(r, res_v, mism)
        return r

    # Filesystem-wide stat.  The capacity fields are backend-specific and
    # untracked, so only the errno is asserted.
    def op_statfs(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="statfs", pid=pid, path=real_path(rv["pth"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_statvfs(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="statvfs", pid=pid, path=real_path(rv["pth"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fstatfs(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fstatfs", pid=pid, fd=self.rfd(pid, rv["fd"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fstatvfs(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fstatvfs", pid=pid, fd=self.rfd(pid, rv["fd"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_truncate(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="truncate", pid=pid,
                             path=real_path(rv["pth"]),
                             len=rv["len"])
        self.check_status(res_v["e"], r["err"], mism)
        if res_v["e"] == 0:
            ino = self.path_ino(post_fs, rv["pth"]["comps"])
            if ino is not None:
                self.shadow_resize(ino, rv["len"])
        return r

    def op_ftruncate(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="ftruncate", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             len=rv["len"])
        self.check_status(res_v["e"], r["err"], mism)
        if res_v["e"] == 0:
            ino = self.model_ino_of_fd(pid, rv["fd"])
            if ino is not None:
                self.shadow_resize(ino, rv["len"])
        return r

    def op_stat(self, pid, rv, res_v, post_fs, mism):
        if rv["dfd"] == -1:
            r = self.drv.request(op="stat", pid=pid,
                                 path=real_path(rv["pth"]),
                                 follow=rv["follow"])
        else:
            r = self.drv.request(op="fstatat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]),
                                 follow=rv["follow"])
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.check_statres(res_v, r, post_fs, mism)
        return r

    def op_fstat(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fstat", pid=pid, fd=self.rfd(pid, rv["fd"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.check_statres(res_v, r, post_fs, mism)
        return r

    def op_chmod(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="chmod", pid=pid, path=real_path(rv["pth"]),
                             mode=rv["mode"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fchmod(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fchmod", pid=pid,
                             fd=self.rfd(pid, rv["fd"]), mode=rv["mode"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_chown(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="chown", pid=pid, path=real_path(rv["pth"]),
                             uid=rv["u"], gid=rv["g"], follow=rv["follow"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fchown(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fchown", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             uid=rv["u"], gid=rv["g"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def _ts_args(self, prefix, ts):
        tag = ts["tag"]
        if tag == "TsNow":
            return {prefix + "type": "now"}
        if tag == "TsOmit":
            return {prefix + "type": "omit"}
        sec, nsec = XTIME[ts["value"]]
        return {prefix + "type": "val", prefix + "sec": sec,
                prefix + "nsec": nsec}

    def op_utimens(self, pid, rv, res_v, post_fs, mism):
        args = {}
        args.update(self._ts_args("a", rv["ta"]))
        args.update(self._ts_args("m", rv["tm"]))
        if rv["dfd"] == -1:
            r = self.drv.request(op="utimens", pid=pid,
                                 path=real_path(rv["pth"]), **args)
        else:
            r = self.drv.request(op="utimensat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]), **args)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_futimens(self, pid, rv, res_v, post_fs, mism):
        args = {}
        args.update(self._ts_args("a", rv["ta"]))
        args.update(self._ts_args("m", rv["tm"]))
        r = self.drv.request(op="futimens", pid=pid,
                             fd=self.rfd(pid, rv["fd"]), **args)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_access(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="access", pid=pid,
                             path=real_path(rv["pth"]),
                             r=rv["r"], w=rv["w"], x=rv["x"], eff=rv["eff"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_umask(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="umask", pid=pid, mask=rv["mask"])
        # The driver's per-pid table mirrors the model's umask state; a
        # disagreement means a harness bug, not a chimera one.
        if r["ret"] != res_v["old"]:
            mism.append(f"umask bookkeeping: expected old {res_v['old']}, "
                        f"driver had {r['ret']} (harness bug)")
        return r

    def op_mkdir(self, pid, rv, res_v, post_fs, mism):
        if rv["dfd"] == -1:
            r = self.drv.request(op="mkdir", pid=pid,
                                 path=real_path(rv["pth"]), mode=rv["mode"])
        else:
            r = self.drv.request(op="mkdirat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]), mode=rv["mode"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_mknod(self, pid, rv, res_v, post_fs, mism):
        # FReg/FFifo/FBlk/FChr -> the driver's ftype tag (blk/chr carry a dev).
        ft = FTYPE_MAP.get(rv["ft"]["tag"], "reg")
        r = self.drv.request(op="mknod", pid=pid, path=real_path(rv["pth"]),
                             mode=rv["mode"], ftype=ft)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_symlink(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="symlink", pid=pid,
                             target=real_target(rv["tgt"]),
                             path=real_path(rv["pth"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_link(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="link", pid=pid,
                             old=real_path(rv["pthOld"]),
                             new=real_path(rv["pthNew"]),
                             follow=rv["followOld"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_unlink(self, pid, rv, res_v, post_fs, mism):
        if rv["dfd"] == -1:
            r = self.drv.request(op="unlink", pid=pid,
                                 path=real_path(rv["pth"]))
        else:
            r = self.drv.request(op="unlinkat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]), rmdir=False)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_rmdir(self, pid, rv, res_v, post_fs, mism):
        if rv["dfd"] == -1:
            r = self.drv.request(op="rmdir", pid=pid,
                                 path=real_path(rv["pth"]))
        else:
            r = self.drv.request(op="unlinkat", pid=pid,
                                 dirfd=self.rfd(pid, rv["dfd"]),
                                 path=real_path(rv["pth"]), rmdir=True)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_rename(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="rename", pid=pid,
                             old=real_path(rv["pthOld"]),
                             new=real_path(rv["pthNew"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_readlink(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="readlink", pid=pid,
                             path=real_path(rv["pth"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            want = real_target(res_v["tgt"])
            if r.get("target") != want:
                mism.append(f"readlink: expected {want!r}, "
                            f"got {r.get('target')!r}")
        return r

    def op_opendir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="opendir", pid=pid,
                             path=real_path(rv["pth"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.sidmap[res_v["sid"]] = r["ret"]
        return r

    def op_readdir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="readdir", pid=pid,
                             sid=self.rsid(rv["sid"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            names = r.get("names", [])
            if len(names) != len(set(names)):
                mism.append(f"readdir: duplicate entries in {sorted(names)}")
            got = set(names) - {".", ".."}
            want = set(res_v["names"])
            if got != want:
                mism.append(f"readdir: expected {sorted(want)}, "
                            f"got {sorted(got)}")
        return r

    def op_rewinddir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="rewinddir", pid=pid,
                             sid=self.rsid(rv["sid"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_telldir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="telldir", pid=pid, sid=self.rsid(rv["sid"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_seekdir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="seekdir", pid=pid, sid=self.rsid(rv["sid"]),
                             loc=rv["loc"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_closedir(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="closedir", pid=pid,
                             sid=self.rsid(rv["sid"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            self.sidmap.pop(rv["sid"], None)
        return r

    def op_fcntl_dupfd(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fcntl_dupfd", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             atleast=rv["atLeast"])
        ok = self.check_status(res_v["e"], r["err"], mism)
        if res_v["e"] == 0:
            if ok:
                self.fdmap[(pid, res_v["fd"])] = r["ret"]
            elif r["err"] == 22:
                # PD2: F_DUPFD is unimplemented (EINVAL).  Emulate with
                # dup() -- identical semantics except the (never checked)
                # descriptor number -- so the model's new descriptor exists
                # on the real side and the trace keeps replaying in sync.
                r2 = self.drv.request(op="dup", pid=pid,
                                      fd=self.rfd(pid, rv["fd"]))
                if r2["err"] == 0:
                    self.fdmap[(pid, res_v["fd"])] = r2["ret"]
        return r

    def op_fcntl_getfl(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fcntl_getfl", pid=pid,
                             fd=self.rfd(pid, rv["fd"]))
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            want_acc = ACC_FLAGS[res_v["acc"]["tag"]]
            if (r["ret"] & os.O_ACCMODE) != want_acc:
                mism.append(f"F_GETFL access mode: expected {want_acc}, "
                            f"got {r['ret'] & os.O_ACCMODE}")
            if bool(r["ret"] & os.O_APPEND) != res_v["appendF"]:
                mism.append(f"F_GETFL O_APPEND: expected {res_v['appendF']},"
                            f" flags {r['ret']:#x}")
        return r

    def op_fcntl_setfl(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="fcntl_setfl", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             flags=os.O_APPEND if rv["appendF"] else 0)
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fcntl_lock(self, pid, rv, res_v, post_fs, mism):
        cmd = LOCK_CMD[rv["cmd"]["tag"]]
        r = self.drv.request(op="fcntl_lock", pid=pid,
                             fd=self.rfd(pid, rv["fd"]), cmd=cmd,
                             type=LOCK_TYPE[rv["lk"]["tag"]],
                             start=rv["lo"],
                             len=rv["hi"] - rv["lo"])
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0 and cmd == "getlk":
            conflict = r.get("l_type", "un") != "un"
            if conflict != res_v["conflict"]:
                mism.append(f"F_GETLK: expected conflict="
                            f"{res_v['conflict']}, got l_type "
                            f"{r.get('l_type')}")
        return r

    def op_lockf(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="lockf", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             cmd=LOCKF_CMD[rv["cmd"]["tag"]],
                             len=rv["len"])
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_fsync(self, pid, rv, res_v, post_fs, mism):
        op = "fdatasync" if rv["dataOnly"] else "fsync"
        r = self.drv.request(op=op, pid=pid, fd=self.rfd(pid, rv["fd"]))
        self.check_status(res_v["e"], r["err"], mism)
        return r

    def op_copy_range(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="copy_range", pid=pid,
                             fd_in=self.rfd(pid, rv["fdIn"]),
                             off_in=rv["offIn"],
                             fd_out=self.rfd(pid, rv["fdOut"]),
                             off_out=rv["offOut"],
                             len=rv["len"])
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            n = res_v["n"]
            if r["ret"] != n:
                mism.append(f"copy_file_range: expected {n}, got {r['ret']}")
            if n > 0:
                si = self.model_ino_of_fd(pid, rv["fdIn"])
                di = self.model_ino_of_fd(pid, rv["fdOut"])
                if si is not None and di is not None:
                    self.shadow_apply(di, rv["offOut"],
                                      self.shadow_read(si, rv["offIn"], n))
        return r

    def op_clone_range(self, pid, rv, res_v, post_fs, mism):
        r = self.drv.request(op="clone_range", pid=pid,
                             dst_fd=self.rfd(pid, rv["fdDst"]),
                             dst_off=rv["offDst"],
                             src_fd=self.rfd(pid, rv["fdSrc"]),
                             src_off=rv["offSrc"],
                             len=rv["len"])
        if self.check_status(res_v["e"], r["err"], mism) \
                and res_v["e"] == 0:
            si = self.model_ino_of_fd(pid, rv["fdSrc"])
            di = self.model_ino_of_fd(pid, rv["fdDst"])
            if si is not None and di is not None:
                self.shadow_apply(di, rv["offDst"],
                                  self.shadow_read(si, rv["offSrc"], rv["len"]))
        return r

    def op_fallocate(self, pid, rv, res_v, post_fs, mism):
        # mode 0 grows to off+len (materialized bytes read zero); mode 1
        # punches a hole (KEEP_SIZE).  Byte offsets; len 0 stays 0 for EINVAL.
        r = self.drv.request(op="fallocate", pid=pid,
                             fd=self.rfd(pid, rv["fd"]),
                             mode=rv["mode"],
                             off=rv["off"], len=rv["len"])
        self.check_status(res_v["e"], r["err"], mism)
        if res_v["e"] == 0:
            ino = self.model_ino_of_fd(pid, rv["fd"])
            if ino is not None:
                if rv["mode"] == 0:
                    end = rv["off"] + rv["len"]
                    if len(self.shadow.setdefault(ino, bytearray())) < end:
                        self.shadow_resize(ino, end)
                else:
                    self.shadow_punch(ino, rv["off"], rv["len"])
        return r

    HANDLERS = {
        "ROpen": op_open,
        "RClose": op_close,
        "RDup": op_dup,
        "RDup2": op_dup2,
        "RLseek": op_lseek,
        "RRead": op_read,
        "RWrite": op_write,
        "RPread": op_pread,
        "RPwrite": op_pwrite,
        "RReadv": op_readv,
        "RWritev": op_writev,
        "RPreadv": op_preadv,
        "RPwritev": op_pwritev,
        "RStatfs": op_statfs,
        "RStatvfs": op_statvfs,
        "RFstatfs": op_fstatfs,
        "RFstatvfs": op_fstatvfs,
        "RTruncate": op_truncate,
        "RFtruncate": op_ftruncate,
        "RStat": op_stat,
        "RFstat": op_fstat,
        "RChmod": op_chmod,
        "RFchmod": op_fchmod,
        "RChown": op_chown,
        "RFchown": op_fchown,
        "RUtimens": op_utimens,
        "RFutimens": op_futimens,
        "RAccess": op_access,
        "RUmask": op_umask,
        "RMkdir": op_mkdir,
        "RMknod": op_mknod,
        "RSymlink": op_symlink,
        "RLink": op_link,
        "RUnlink": op_unlink,
        "RRmdir": op_rmdir,
        "RRename": op_rename,
        "RReadlink": op_readlink,
        "ROpendir": op_opendir,
        "RReaddir": op_readdir,
        "RRewinddir": op_rewinddir,
        "RTelldir": op_telldir,
        "RSeekdir": op_seekdir,
        "RClosedir": op_closedir,
        "RFcntlDupfd": op_fcntl_dupfd,
        "RFcntlGetfl": op_fcntl_getfl,
        "RFcntlSetfl": op_fcntl_setfl,
        "RFcntlLock": op_fcntl_lock,
        "RLockf": op_lockf,
        "RFsync": op_fsync,
        "RCopyRange": op_copy_range,
        "RCloneRange": op_clone_range,
        "RFallocate": op_fallocate,
    }

    def final_audit(self, final_state, nsteps):
        """End-of-trace sweep: walk the final model tree and verify every
        reachable object's identity, attributes, directory contents, file
        data and link targets against the live filesystem.  Catches silent
        state drift the per-operation checks never observed.  Runs as an
        out-of-band root credential (pid 3) so permission bits cannot mask
        the comparison."""
        fs = final_state["fs"]
        mism = []
        audited = 0

        self.drv.request(op="setcred", pid=3, uid=0, gid=0, gids=[])

        stack = [(0, "")]
        while stack and len(mism) < 20:
            ino, rpath = stack.pop()
            node = fs["inodes"][ino]

            r = self.drv.request(op="opendir", pid=3, path=MOUNT + rpath)
            if r["err"] != 0:
                mism.append(f"audit: opendir {rpath or '/'}: errno "
                            f"{r['err']}")
                continue
            sid = r["ret"]
            names = set(self.drv.request(op="readdir", pid=3,
                                         sid=sid).get("names", []))
            names -= {".", ".."}
            self.drv.request(op="closedir", pid=3, sid=sid)

            want_names = set(node["ents"].keys())

            # PD24 residue: nodes chimera legitimately created where the
            # model's descriptor table predicted EMFILE do not exist in the
            # model tree; ignore exactly those chimera-only entries (an
            # O_CREAT open of a file that exists in the model created
            # nothing, so entries the model also has are never dropped).
            names -= {n for n in names - want_names
                      if rpath + "/" + n in self.audit_exempt}
            if names != want_names:
                mism.append(f"audit: dir {rpath or '/'}: entries "
                            f"{sorted(names)} != model "
                            f"{sorted(want_names)}")

            for name in sorted(want_names & names):
                cino = node["ents"][name]
                cnode = fs["inodes"][cino]
                cpath = rpath + "/" + name
                ftag = cnode["ftype"]["tag"]
                audited += 1

                st = self.drv.request(op="stat", pid=3, path=MOUNT + cpath,
                                      follow=False)
                if st["err"] != 0:
                    mism.append(f"audit: lstat {cpath}: errno {st['err']}")
                    continue
                if st.get("ftype") != FTYPE_MAP[ftag]:
                    mism.append(f"audit: {cpath}: ftype "
                                f"{st.get('ftype')} != {FTYPE_MAP[ftag]}")
                    continue
                if ftag != "FLnk" and st.get("mode") != cnode["mode"]:
                    mism.append(f"audit: {cpath}: mode "
                                f"{st.get('mode', 0):#o} != "
                                f"{cnode['mode']:#o}")
                if st.get("uid") != cnode["uid"] or \
                        st.get("gid") != cnode["gid"]:
                    mism.append(f"audit: {cpath}: owner "
                                f"{st.get('uid')}:{st.get('gid')} != "
                                f"{cnode['uid']}:{cnode['gid']}")
                if st.get("nlink") != cnode["nlink"]:
                    mism.append(f"audit: {cpath}: nlink "
                                f"{st.get('nlink')} != {cnode['nlink']}")

                ident = (st.get("dev"), st.get("ino"))
                known = self.inomap.get(cino)
                if known is not None and known != ident:
                    mism.append(f"audit: {cpath}: identity {ident} != "
                                f"learned {known}")

                if ftag == "FDir":
                    stack.append((cino, cpath))
                elif ftag == "FLnk":
                    r = self.drv.request(op="readlink", pid=3,
                                         path=MOUNT + cpath)
                    want = real_target(cnode["target"])
                    if r["err"] != 0 or r.get("target") != want:
                        mism.append(f"audit: readlink {cpath}: "
                                    f"{r.get('target')!r} != {want!r}")
                elif ftag == "FReg":
                    want_size = cnode["size"]
                    if st.get("size") != want_size:
                        mism.append(f"audit: {cpath}: size "
                                    f"{st.get('size')} != {want_size}")
                        continue
                    if want_size == 0:
                        continue
                    fd = self.drv.request(op="open", pid=3,
                                          path=MOUNT + cpath,
                                          flags=os.O_RDONLY, mode=0)
                    if fd["err"] != 0:
                        mism.append(f"audit: open {cpath}: errno "
                                    f"{fd['err']}")
                        continue
                    # Read in chunks: a whole-file read of a large (sparse)
                    # file would exceed the driver's single-response size.
                    CHUNK = 65536
                    for off in range(0, want_size, CHUNK):
                        n = min(CHUNK, want_size - off)
                        r = self.drv.request(op="pread", pid=3, fd=fd["ret"],
                                             off=off, len=n)
                        data = base64.b64decode(r.get("data", ""))
                        expect = self.shadow_read(cino, off, n)
                        if data != expect:
                            mism.append(
                                f"audit: {cpath}: content mismatch at +{off}"
                                + diff_bytes(expect, data, self.bs))
                            break
                    self.drv.request(op="close", pid=3, fd=fd["ret"])

        if mism:
            raise Divergence(nsteps, ("final-audit", {}), mism)
        return audited

    def cleanup(self):
        """Best-effort close of everything still open, so driver shutdown
        does not trip chimera's shutdown-with-open-descriptors hang (PD9)."""
        try:
            for sid in list(self.sidmap.values()):
                self.drv.request(op="closedir", pid=0, sid=sid)
            for real in list(self.fdmap.values()):
                self.drv.request(op="close", pid=0, fd=real)
        except (RuntimeError, OSError, json.JSONDecodeError):
            pass

    def replay(self, states):
        for idx, state in enumerate(states[1:], start=1):
            label = state["lastOp"]
            if label["tag"] != "LCall":
                raise TraceFormatError(
                    f"step {idx}: unexpected label {label['tag']}")
            pid = label["value"]["pid"]
            req = label["value"]["req"]
            res = label["value"]["res"]
            tag = req["tag"]
            handler = self.HANDLERS.get(tag)
            if handler is None:
                raise TraceFormatError(f"step {idx}: no handler for {tag}")
            signal.alarm(60)
            mism = []
            self._cur_tag = tag
            self._cur_req = req["value"]
            self._cur_fs = state["fs"]
            self._cur_ps = state.get("ps")
            r = handler(self, pid, req["value"], res["value"],
                        state["fs"], mism)
            self.history.append((idx, pid, tag, req["value"],
                                 res["value"], r))
            if self.verbose:
                print(f"  [{idx:4d}] pid{pid} {tag} {req['value']} "
                      f"-> {r}")
            if mism:
                raise Divergence(idx, (tag, req["value"], res["value"]),
                                 mism)
        signal.alarm(0)


def diff_bytes(expect, actual, block_size):
    n = min(len(expect), len(actual))
    for i in range(0, n, block_size):
        if expect[i:i + block_size] != actual[i:i + block_size]:
            return (f"; first differing block {i // block_size}: "
                    f"expected byte {expect[i]:#x}, "
                    f"got byte {actual[i] if i < len(actual) else -1:#x}")
    return "; lengths differ only"


# ---------------------------------------------------------------------------
# Live-profile probe: measures the capability/policy profile of the backend
# behind posix_driver, for pinning PROFILE and posix_run.qnt's posixMemfs.
# ---------------------------------------------------------------------------

def probe(driver_path, backend="memfs"):
    drv = Driver(driver_path, backend)
    bs = drv.block_size
    out = {}
    root = {"uid": 0, "gid": 10, "gids": [10, 30]}
    user1 = {"uid": 100, "gid": 10, "gids": [10, 30]}
    user2 = {"uid": 200, "gid": 20, "gids": [20, 30]}
    drv.request(op="setcred", pid=0, **root)
    drv.request(op="setcred", pid=1, **user2)
    drv.request(op="setcred", pid=2, **user1)
    blk = base64.b64encode(b"A" * bs).decode()

    def mk(path, pid=0, mode=0o777):
        drv.request(op="mkdir", pid=pid, path=path, mode=mode)

    def touch(path, pid=0, mode=0o666, data=None):
        r = drv.request(op="open", pid=pid, path=path,
                        flags=os.O_CREAT | os.O_WRONLY, mode=mode)
        if data:
            drv.request(op="write", pid=pid, fd=r["ret"], data=data)
        drv.request(op="close", pid=pid, fd=r["ret"])

    # copy_file_range / clone_file_range / SEEK_HOLE
    touch("/test/p_src", data=blk)
    touch("/test/p_dst")
    fin = drv.request(op="open", pid=0, path="/test/p_src",
                      flags=os.O_RDONLY, mode=0)["ret"]
    fout = drv.request(op="open", pid=0, path="/test/p_dst",
                       flags=os.O_WRONLY, mode=0)["ret"]
    r = drv.request(op="copy_range", pid=0, fd_in=fin, off_in=0,
                    fd_out=fout, off_out=0, len=bs)
    out["copyRange"] = r["ret"] >= 0
    r = drv.request(op="clone_range", pid=0, dst_fd=fout, dst_off=0,
                    src_fd=fin, src_off=0, len=bs)
    out["cloneRange"] = r["ret"] >= 0
    drv.request(op="ftruncate", pid=0, fd=fout, len=0)
    drv.request(op="pwrite", pid=0, fd=fout, off=0, data=blk)
    drv.request(op="ftruncate", pid=0, fd=fout, len=3 * bs)
    r = drv.request(op="lseek", pid=0, fd=fout, off=0, whence="hole")
    out["seekHole"] = r["ret"] == bs
    out["seekHoleRaw"] = r["ret"]
    drv.request(op="close", pid=0, fd=fin)
    drv.request(op="close", pid=0, fd=fout)

    # gidFromParent: dir gid 77, creator (root, egid 10) makes a file
    mk("/test/p_gid")
    drv.request(op="chown", pid=0, path="/test/p_gid", uid=0, gid=77,
                follow=True)
    touch("/test/p_gid/f")
    r = drv.request(op="stat", pid=0, path="/test/p_gid/f", follow=True)
    out["gidFromParent"] = r.get("gid") == 77
    out["gidFromParentRaw"] = r.get("gid")

    # sgidInherit: subdir of a setgid dir
    mk("/test/p_sgid")
    drv.request(op="chmod", pid=0, path="/test/p_sgid", mode=0o2777)
    mk("/test/p_sgid/sub", mode=0o755)
    r = drv.request(op="stat", pid=0, path="/test/p_sgid/sub", follow=True)
    out["sgidInherit"] = bool(r.get("mode", 0) & 0o2000)

    # writeClearsSets: unprivileged owner writes a setuid file
    touch("/test/p_setid", pid=1, mode=0o700)
    drv.request(op="chmod", pid=1, path="/test/p_setid", mode=0o4755)
    fd = drv.request(op="open", pid=1, path="/test/p_setid",
                     flags=os.O_WRONLY, mode=0)["ret"]
    drv.request(op="write", pid=1, fd=fd, data=blk)
    drv.request(op="close", pid=1, fd=fd)
    r = drv.request(op="stat", pid=1, path="/test/p_setid", follow=True)
    out["writeClearsSets"] = not (r.get("mode", 0) & 0o4000)

    # pwriteAppends: pwrite at 0 through an O_APPEND descriptor
    touch("/test/p_app", data=blk)
    fd = drv.request(op="open", pid=0, path="/test/p_app",
                     flags=os.O_WRONLY | os.O_APPEND, mode=0)["ret"]
    drv.request(op="pwrite", pid=0, fd=fd, off=0,
                data=base64.b64encode(b"B" * bs).decode())
    drv.request(op="close", pid=0, fd=fd)
    r = drv.request(op="stat", pid=0, path="/test/p_app", follow=True)
    out["pwriteAppends"] = r.get("size") == 2 * bs

    # renameCtime
    touch("/test/p_ren")
    r1 = drv.request(op="stat", pid=0, path="/test/p_ren", follow=True)
    import time
    time.sleep(0.02)
    drv.request(op="rename", pid=0, old="/test/p_ren", new="/test/p_ren2")
    r2 = drv.request(op="stat", pid=0, path="/test/p_ren2", follow=True)
    out["renameCtime"] = tuple(r2["ctime"]) > tuple(r1["ctime"])

    # strictAtime: read marks atime
    touch("/test/p_at", data=blk)
    r1 = drv.request(op="stat", pid=0, path="/test/p_at", follow=True)
    time.sleep(0.02)
    fd = drv.request(op="open", pid=0, path="/test/p_at",
                     flags=os.O_RDONLY, mode=0)["ret"]
    drv.request(op="read", pid=0, fd=fd, len=bs)
    drv.request(op="close", pid=0, fd=fd)
    r2 = drv.request(op="stat", pid=0, path="/test/p_at", follow=True)
    out["strictAtime"] = tuple(r2["atime"]) > tuple(r1["atime"])

    # sticky arm + errno: sticky dir owned by root; victim owned by uid 100
    mk("/test/p_sticky")
    drv.request(op="chmod", pid=0, path="/test/p_sticky", mode=0o1777)
    touch("/test/p_sticky/w", mode=0o666)
    drv.request(op="chown", pid=0, path="/test/p_sticky/w", uid=100,
                gid=10, follow=True)
    r = drv.request(op="unlink", pid=1, path="/test/p_sticky/w")
    out["stickyWriteArm"] = r["ret"] == 0
    touch("/test/p_sticky/s", mode=0o600)
    drv.request(op="chown", pid=0, path="/test/p_sticky/s", uid=100,
                gid=10, follow=True)
    r = drv.request(op="unlink", pid=1, path="/test/p_sticky/s")
    out["stickyDenyErrno"] = r["err"]
    out["errStickyAcces"] = r["err"] == 13 if r["ret"] < 0 else None

    # errNotempty / errUnlinkDirIsdir
    mk("/test/p_ne")
    mk("/test/p_ne/x")
    r = drv.request(op="rmdir", pid=0, path="/test/p_ne")
    out["errNotempty"] = r["err"] == 39
    out["rmdirNonemptyErrno"] = r["err"]
    r = drv.request(op="unlink", pid=0, path="/test/p_ne")
    out["errUnlinkDirIsdir"] = r["err"] == 21
    out["unlinkDirErrno"] = r["err"]

    # record locks (expected EOPNOTSUPP on memfs, see PD1)
    fd = drv.request(op="open", pid=0, path="/test/p_src",
                     flags=os.O_RDWR, mode=0)["ret"]
    r = drv.request(op="fcntl_lock", pid=0, fd=fd, cmd="setlk", type="wr",
                    start=0, len=bs)
    out["lockErrno"] = r["err"]
    out["errLockAgain"] = None
    drv.request(op="close", pid=0, fd=fd)

    drv.close()
    return out


def report_divergence(trace_path, div, replayer, driver):
    print(f"\n=== DIVERGENCE in {trace_path} ===", file=sys.stderr)
    print(f"step {div.step}: {div.op[0]} req: {div.op[1]}", file=sys.stderr)
    if len(div.op) > 2:
        print(f"  model expectation: {div.op[2]}", file=sys.stderr)
    for m in div.mismatches:
        print(f"  MISMATCH: {m}", file=sys.stderr)
    print("\nlast operations before failure:", file=sys.stderr)
    for idx, pid, tag, req, res, r in replayer.history[-10:]:
        print(f"  [{idx:4d}] pid{pid} {tag} {req} expect {res} -> {r}",
              file=sys.stderr)
    print("\n(pid, model fd) -> real fd map:", file=sys.stderr)
    for k, v in sorted(replayer.fdmap.items()):
        print(f"  {k}: {v}", file=sys.stderr)
    print(f"\ndriver stderr tail:\n{driver.stderr_tail()}", file=sys.stderr)


def run_trace(trace_path, args):
    # diskfs drives a libaio block device, which exists only on Linux; on other
    # platforms the backend aborts at startup ("liburcu: set CPU # out of
    # range").  Skip rather than fail so the suite is meaningful off Linux --
    # the Linux CI hosts still run the full diskfs matrix.
    if "diskfs" in args.backend and not sys.platform.startswith("linux"):
        print(f"{trace_path}: SKIP: diskfs requires Linux (libaio); "
              f"unavailable on {sys.platform}")
        sys.exit(77)

    states = load_trace(trace_path)
    if args.dry_run:
        print(f"{trace_path}: {len(states) - 1} steps, format OK")
        return True

    init = states[0]["lastOp"]
    if init["tag"] != "LInit":
        raise TraceFormatError(f"{trace_path}: first label is not LInit")
    caps = init["value"]["caps"]

    for key, want in PROFILES[args.backend].items():
        if want is not None and caps.get(key) != want:
            print(f"{trace_path}: SKIP: trace profile {key}="
                  f"{caps.get(key)} does not match live profile {want}")
            sys.exit(77)

    driver = Driver(args.driver, args.backend)
    try:
        replayer = Replayer(driver, caps, verbose=args.verbose)
        audited = 0
        try:
            replayer.replay(states)
            audited = replayer.final_audit(states[-1], len(states) - 1)
        except Divergence as div:
            report_divergence(trace_path, div, replayer, driver)
            replayer.cleanup()
            return False
        replayer.cleanup()

        dev_summary = ""
        if replayer.deviations_hit:
            parts = ", ".join(
                f"{k}x{v}"
                for k, v in sorted(replayer.deviations_hit.items()))
            dev_summary = f"; known deviations: {parts}"
        print(f"{trace_path}: {len(states) - 1} steps replayed, "
              f"{audited} objects audited{dev_summary}")
        return True
    finally:
        driver.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trace", action="append", default=[],
                    help="ITF trace file (repeatable; fresh driver per "
                         "trace)")
    ap.add_argument("--driver", help="path to the posix_quint_driver binary")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and validate traces without a driver")
    ap.add_argument("--backend", default="memfs",
                    choices=sorted(PROFILES),
                    help="VFS backend behind the driver (default memfs)")
    ap.add_argument("--probe", action="store_true",
                    help="measure the live capability/policy profile")
    ap.add_argument("--check-profile", action="store_true",
                    help="measure the live profile and diff against the "
                         "pinned PROFILE")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    def on_alarm(sig, frame):
        print("FATAL: driver request timed out (possible deadlock)",
              file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGALRM, on_alarm)

    if args.probe or args.check_profile:
        if not args.driver:
            ap.error("--driver is required for probing")
        measured = probe(args.driver, args.backend)
        print(json.dumps(measured, indent=2))
        if args.check_profile:
            bad = [k for k, v in PROFILES[args.backend].items()
                   if v is not None and measured.get(k) != v]
            if bad:
                print(f"PROFILE drift on: {bad}", file=sys.stderr)
                sys.exit(1)
        return

    if not args.trace:
        ap.error("--trace is required unless --probe")
    if not args.dry_run and not args.driver:
        ap.error("--driver is required unless --dry-run")

    failures = 0
    for trace in args.trace:
        if not run_trace(trace, args):
            failures += 1
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
