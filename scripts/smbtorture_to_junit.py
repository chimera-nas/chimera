#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Convert smbtorture text output into JUnit XML (one <testcase> per subtest).

Usage: smbtorture_to_junit.py <smbtorture-output> <output-junit.xml> [suite]

A consolidated smbtorture CTest entry runs a whole suite in one process, so
CTest's --output-junit only sees one test.  smbtorture prints a per-subtest
stream (`test:` / `success:` / `failure: NAME [ ... ]` / `skip:`); parse it into
a per-subtest JUnit so the CI report keeps individual-test visibility -- the
smbtorture analogue of the WPTS TRX->JUnit and the pynfs per-code JUnit.
"""
import os
import re
import sys

RESULT = re.compile(r'^(success|failure|skip|error|xfail): (.*?)( \[)?$')


def esc(s):
    return ((s or "")
            .replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def parse(path):
    cases = []                      # (name, outcome, detail)
    name = outcome = None
    detail = []
    collecting = False
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if collecting:
                if line == "]":
                    cases.append((name, outcome, "\n".join(detail)))
                    collecting = False
                    name = None
                    detail = []
                else:
                    detail.append(line)
                continue
            m = RESULT.match(line)
            if not m:
                continue
            kind, tname, bracket = m.group(1), m.group(2), m.group(3)
            if bracket:                 # multi-line detail block follows
                name, outcome, detail, collecting = tname, kind, [], True
            else:
                cases.append((tname, kind, ""))
    return cases


def main():
    inp, out = sys.argv[1], sys.argv[2]
    suite = sys.argv[3] if len(sys.argv) > 3 else "smbtorture"

    cases = parse(inp)
    fails = sum(1 for _, o, _ in cases if o in ("failure", "error"))
    skips = sum(1 for _, o, _ in cases if o in ("skip", "xfail"))

    lines = [f'<testsuite name="{esc(suite)}" tests="{len(cases)}" '
             f'failures="{fails}" errors="0" skipped="{skips}">']
    for name, outcome, detail in cases:
        attr = f'name="{esc(name)}" classname="{esc(suite)}"'
        if outcome in ("failure", "error"):
            lines.append(f'  <testcase {attr}><failure>{esc(detail)}'
                         f'</failure></testcase>')
        elif outcome in ("skip", "xfail"):
            lines.append(f'  <testcase {attr}><skipped/></testcase>')
        else:
            lines.append(f'  <testcase {attr}/>')
    lines.append("</testsuite>")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
