# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

"""Check loader diagnostics even when Chimera's fatal path terminates by signal."""
import subprocess
import sys

result = subprocess.run(sys.argv[1:3], capture_output=True, text=True, timeout=30)
if result.returncode == 0 or sys.argv[3] not in result.stdout + result.stderr:
    print(result.stdout)
    print(result.stderr)
    raise SystemExit(f"Expected failure containing {sys.argv[3]!r}, got {result.returncode}")
