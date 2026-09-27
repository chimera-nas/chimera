#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

"""Exercise guest completion and process cleanup without requiring KVM."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

RUNNER = Path(__file__).with_name("run_guest.py")


class GuestCompletionTest(unittest.TestCase):
    def command(self, guest):
        return [sys.executable, str(RUNNER), "--shutdown-grace", "0.1", "--",
                sys.executable, "-u", "-c", guest]

    def run_guest(self, guest):
        return subprocess.run(self.command(guest), capture_output=True, text=True, timeout=8)

    def test_normal_completion(self):
        result = self.run_guest("print('[    1.142904] CHIMERA_KVM_EXIT_CODE=0')")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("stopping", result.stdout)

    def test_shutdown_panic_after_success(self):
        result = self.run_guest("""
import time
print('[    1.142904] CHIMERA_KVM_EXIT_CODE=0')
print('I/O error, dev vda')
print('Kernel panic - not syncing: Attempted to kill init!')
time.sleep(60)
""")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("stopping its process group", result.stdout)
        self.assertIn("Kernel panic", result.stdout)

    def test_failed_workload_stays_failed(self):
        result = self.run_guest("import time; print('CHIMERA_KVM_EXIT_CODE=7'); time.sleep(60)")
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_exit_without_result_is_failure(self):
        result = self.run_guest("print('Kernel panic before workload completion')")
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_embedded_marker_is_not_completion(self):
        result = self.run_guest("print('Executing: echo CHIMERA_KVM_EXIT_CODE=0')")
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_split_result(self):
        result = self.run_guest("""
import sys, time
sys.stdout.write('[ 2.123] CHIMERA_KVM_EXIT_')
sys.stdout.flush()
time.sleep(0.02)
print('CODE=3')
time.sleep(60)
""")
        self.assertEqual(result.returncode, 3, result.stderr)

    def test_no_result_keeps_waiting_and_cancellation_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / 'pid'
            guest = f"import os,time; open({str(pidfile)!r},'w').write(str(os.getpid())); time.sleep(60)"
            child = subprocess.Popen(self.command(guest), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                for _ in range(100):
                    if pidfile.exists():
                        break
                    time.sleep(0.01)
                self.assertTrue(pidfile.exists())
                time.sleep(0.3)  # Longer than the post-result shutdown grace.
                self.assertIsNone(child.poll())
                child.terminate()
                child.communicate(timeout=5)
                self.assertEqual(child.returncode, 128 + signal.SIGTERM)
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(pidfile.read_text()), 0)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()

    def test_stubborn_descendant_stopped_but_sibling_survives(self):
        sibling = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        try:
            with tempfile.TemporaryDirectory() as tmp:
                pidfile = Path(tmp) / 'descendant'
                guest = f"""
import os, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
pid = os.fork()
if pid == 0:
    open({str(pidfile)!r}, 'w').write(str(os.getpid()))
    time.sleep(60)
else:
    print('CHIMERA_KVM_EXIT_CODE=0')
    time.sleep(60)
"""
                result = self.run_guest(guest)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIsNone(sibling.poll())
                pid = int(pidfile.read_text())
                for _ in range(100):
                    stat = Path(f'/proc/{pid}/stat')
                    if not stat.exists() or stat.read_text().split()[2] == 'Z':
                        break
                    time.sleep(0.01)
                else:
                    self.fail('guest descendant survived cleanup')
        finally:
            sibling.terminate()
            sibling.wait()


if __name__ == '__main__':
    unittest.main()
