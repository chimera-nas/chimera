#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Convert a VSTest TRX result file to JUnit XML (one <testcase> per case).

Usage: trx_to_junit.py <input.trx> <output-junit.xml>

The WPTS suites run under `dotnet vstest`, which writes a TRX (Visual Studio
TeamTest XML).  ctest --output-junit only sees the consolidated run as a single
test, so -- exactly like the pynfs per-code JUnit -- we emit a per-case JUnit so
the CI report keeps WPTS per-case visibility.
"""
import sys
import xml.etree.ElementTree as ET


def localname(tag):
    return tag.rsplit("}", 1)[-1]


def esc(s):
    return ((s or "")
            .replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def duration_secs(d):
    # TRX duration looks like "00:00:01.2340000"
    if not d:
        return 0.0
    try:
        h, m, s = d.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        return 0.0


def main():
    trx, out = sys.argv[1], sys.argv[2]
    root = ET.parse(trx).getroot()

    cases = []
    fails = skips = 0
    for el in root.iter():
        if localname(el.tag) != "UnitTestResult":
            continue
        name = el.get("testName") or "?"
        outcome = el.get("outcome") or ""
        dur = duration_secs(el.get("duration"))
        msg = ""
        for e in el.iter():
            if localname(e.tag) == "Message" and e.text:
                msg = e.text.strip()
                break
        cases.append((name, outcome, dur, msg))
        if outcome == "Failed":
            fails += 1
        elif outcome != "Passed":
            skips += 1

    lines = [f'<testsuite name="wpts" tests="{len(cases)}" '
             f'failures="{fails}" errors="0" skipped="{skips}">']
    for name, outcome, dur, msg in cases:
        attr = f'name="{esc(name)}" classname="wpts" time="{dur:.3f}"'
        if outcome == "Failed":
            lines.append(f'  <testcase {attr}>'
                         f'<failure message="{esc(msg)}"></failure></testcase>')
        elif outcome == "Passed":
            lines.append(f'  <testcase {attr}/>')
        else:
            lines.append(f'  <testcase {attr}><skipped/></testcase>')
    lines.append("</testsuite>")
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
