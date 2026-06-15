#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Convert a WPTS (MS-SMB2) vstest TRX result into JUnit XML (one <testcase>
per case).

A consolidated WPTS CTest entry runs a whole config-group's case list in one
`dotnet vstest` invocation, so CTest's --output-junit only sees one test.
vstest writes a TRX with one <UnitTestResult> per case (outcome Passed |
Failed | NotExecuted); parse it into a per-case JUnit so the CI report keeps
individual-test visibility -- the WPTS analogue of smbtorture_to_junit.py.

Usage: trx_to_junit.py <result.trx> <output-junit.xml> [suite]
"""
import os
import sys
import xml.etree.ElementTree as ET

NS = "{http://microsoft.com/schemas/VisualStudio/TeamTest/2010}"


def esc(s):
    return ((s or "")
            .replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def parse(path):
    cases = []                                  # (name, outcome, detail)
    root = ET.parse(path).getroot()
    for u in root.iter(NS + "UnitTestResult"):
        name = u.get("testName")
        outcome = u.get("outcome")              # Passed | Failed | NotExecuted
        m = u.find(".//" + NS + "Message")
        detail = " ".join(m.text.split()) if (m is not None and m.text) else ""
        cases.append((name, outcome, detail))
    return cases


def main():
    inp, out = sys.argv[1], sys.argv[2]
    suite = sys.argv[3] if len(sys.argv) > 3 else "wpts"

    cases = parse(inp)
    fails = sum(1 for _, o, _ in cases if o == "Failed")
    skips = sum(1 for _, o, _ in cases if o == "NotExecuted")

    lines = [f'<testsuite name="{esc(suite)}" tests="{len(cases)}" '
             f'failures="{fails}" errors="0" skipped="{skips}">']
    for name, outcome, detail in cases:
        attr = f'name="{esc(name)}" classname="{esc(suite)}"'
        if outcome == "Failed":
            lines.append(f'  <testcase {attr}><failure>{esc(detail)}'
                         f'</failure></testcase>')
        elif outcome == "NotExecuted":
            lines.append(f'  <testcase {attr}><skipped/></testcase>')
        else:
            lines.append(f'  <testcase {attr}/>')
    lines.append("</testsuite>")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
