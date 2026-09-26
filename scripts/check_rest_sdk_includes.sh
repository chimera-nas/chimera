#!/bin/sh
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -eu
rest_repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python3 - "$rest_repo_root" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
sdk = (root / "src/server/rest/sdk").resolve()
allowed = {"stddef.h", "stdint.h"}
for header in sdk.glob("*.h"):
    for include in re.findall(r'^\s*#\s*include\s*[<"]([^>"]+)[>"]', header.read_text(), re.M):
        dependency = (sdk / include).resolve()
        if include not in allowed and not (dependency.parent == sdk and dependency.is_file()):
            raise SystemExit(f"{header}: private or external dependency {include}")
print("REST SDK header boundary passed")
PY
