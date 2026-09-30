# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Experiment: compare IFSTest logs.

Usage: ifstest_compare.py <log dir>

Reads ifstest-ntfs.log (local NTFS), ifstest-winsmb.log (Windows' own SMB
server over the loopback redirector) and ifstest-chimera-<backend>.log, and
prints per-group pass counts plus the tests Windows' SMB server passes that a
chimera backend does not -- the ones that point at chimera rather than at
the redirector.
"""
import collections
from pathlib import Path
import re
import sys

logdir = Path(sys.argv[1])
order = ["ntfs", "winsmb"] + sorted(p.stem[len("ifstest-chimera-"):]
                                    for p in logdir.glob("ifstest-chimera-*.log"))
files = {"ntfs": "ifstest-ntfs.log", "winsmb": "ifstest-winsmb.log"}
files.update({b: f"ifstest-chimera-{b}.log" for b in order[2:]})

pattern = re.compile(r"\+TEST\+(PASS|SEV[123]|BLOCK|WARN|ABORT)\s*:\s*Test\s*:(\S+)\s*\n"
                     r"Group\s*:(\S+)\s*\nStatus\s*:(\S+)\s*(\([^)]*\))?")
res, why = {}, {}
for name in order:
    path = logdir / files[name]
    if not path.exists():
        continue
    r, w = {}, {}
    for m in pattern.finditer(path.read_text(encoding="latin-1")):
        key = f"{m.group(3)}:{m.group(2)}"
        if r.get(key) in (None, "PASS"):
            r[key] = m.group(1)
            w[key] = (m.group(5) or m.group(4)).strip("()")
    res[name], why[name] = r, w
names = [n for n in order if n in res]

print(f"{'':15}" + "".join(f"{n:>10}" for n in names))
print(f"{'passed':15}" + "".join(f"{sum(v == 'PASS' for v in res[n].values()):>10}" for n in names))
print(f"{'reported':15}" + "".join(f"{len(res[n]):>10}" for n in names))

tests = sorted(set().union(*[set(r) for r in res.values()]))
groups = collections.OrderedDict()
for t in tests:
    groups.setdefault(t.split(":")[0], []).append(t)
print(f"\n{'group (passes)':28}{'tests':>6}" + "".join(f"{n:>9}" for n in names))
for g, ts in groups.items():
    print(f"{g:28}{len(ts):6}" + "".join(
        f"{sum(1 for t in ts if res[n].get(t) == 'PASS'):9}" for n in names))

if "winsmb" in res:
    for backend in names[2:]:
        gaps = [t for t in tests if res["winsmb"].get(t) == "PASS" and res[backend].get(t) != "PASS"]
        print(f"\n{backend}: {len(gaps)} tests pass over Windows SMB but not on chimera")
        for t in gaps:
            print(f"  {t:60} {res[backend].get(t, 'not reached'):6} {why[backend].get(t, '')}")
