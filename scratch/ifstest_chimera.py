# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Experiment: run IFSTest against chimera shares mapped by the Windows SMB client.

Usage: ifstest_chimera.py <chimera.exe> <ifstest dir> <log dir> <timeout s> <backend>...

One daemon serves every backend as its own share (see run_all in the Windows
SMB client test for why), and IFSTest runs against each mapped drive in turn.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src/server/smb/tests/windows_client"))
import windows_smb_client_test as t  # noqa: E402
import ifstest_run  # noqa: E402

chimera, kit, logdir, timeout = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]).resolve(), int(sys.argv[4])
backends = sys.argv[5:]
logdir.mkdir(parents=True, exist_ok=True)
scratch = Path(tempfile.mkdtemp(prefix="ifstest-chimera-"))
(scratch / "state").mkdir()
port, metrics = t.free_ports(2)
config = t.daemon_config(backends, scratch, port, metrics)
# IFSTest's stream tests need named streams, which the daemon leaves off.
config["server"]["smb_named_streams"] = True
(scratch / "config.json").write_text(json.dumps(config))
daemon_log = logdir / "daemon.log"
log = daemon_log.open("w+b")
# IFSTEST_CHIMERA_DEBUG=1 runs the daemon with -d, which dumps every SMB
# request and reply into daemon.log.
debug = ["-d"] if os.environ.get("IFSTEST_CHIMERA_DEBUG") == "1" else []
proc = subprocess.Popen([chimera, *debug, "-c", str(scratch / "config.json")], stdout=log, stderr=log,
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
drives = []
try:
    t.wait_until_ready(proc, daemon_log)
    for backend in backends:
        drive = t.free_drive_letter()
        t.net_use(drive, backend, port)
        drives.append(drive)
        print(f"==== {backend}: mapped {drive}: to \\\\127.0.0.1\\{backend} on port {port}", flush=True)
        ifstest_run.run(kit, f"{drive}:", logdir / f"ifstest-chimera-{backend}.log", timeout)
finally:
    for drive in drives:
        t.net_use_delete(drive)
    if proc.poll() is None:
        proc.send_signal(signal.CTRL_BREAK_EVENT)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.close()
