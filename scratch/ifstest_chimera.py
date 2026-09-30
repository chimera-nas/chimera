# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Experiment: run IFSTest against a chimera share mapped by the Windows SMB client.

Usage: ifstest_chimera.py <chimera.exe> <backend> <ifstest dir> <log dir> <timeout s>
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src/server/smb/tests/windows_client"))
import windows_smb_client_test as t  # noqa: E402

chimera, backend, kit, logdir, timeout = sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), int(sys.argv[5])
logdir.mkdir(parents=True, exist_ok=True)
scratch = Path(tempfile.mkdtemp(prefix="ifstest-chimera-"))
(scratch / "state").mkdir()
port, metrics = t.free_ports(2)
(scratch / "config.json").write_text(json.dumps(t.daemon_config([backend], scratch, port, metrics)))
log = (logdir / f"daemon-{backend}.log").open("w+b")
proc = subprocess.Popen([chimera, "-c", str(scratch / "config.json")], stdout=log, stderr=log,
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
rc = 1
try:
    t.wait_until_ready(proc, logdir / f"daemon-{backend}.log")
    drive = t.free_drive_letter()
    t.net_use(drive, backend, port)
    print(f"mapped {drive}: to \\\\127.0.0.1\\{backend} on port {port}", flush=True)
    import ifstest_run
    ifstest_run.run(kit, f"{drive}:", logdir / f"ifstest-chimera-{backend}.log", timeout)
    rc = 0
    t.net_use_delete(drive)
finally:
    if proc.poll() is None:
        proc.send_signal(signal.CTRL_BREAK_EVENT)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.close()
sys.exit(0)
