# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Judge IFSTest results against the known failures.

Usage: ifstest_check.py <log dir> <expected.csv> [--junit <file>] <backend>...

Reads ifstest-chimera-<backend>.log for each backend.  Every test IFSTest
reports is expected to pass unless ifstest_expected.csv lists it for that
backend.  The check fails when

  - a test not listed fails (a regression), or
  - fewer tests are reported than EXPECTED_REPORTED (IFSTest stopped early).

A listed test that passes is reported, not failed -- several IFSTest checks are
timing-sensitive -- but it should be taken off the list.
"""
import argparse
import csv
from pathlib import Path
import re
import sys
from xml.sax.saxutils import quoteattr

# Tests IFSTest reports for a share, with the Virus group excluded as
# ifstest_run.py runs it, for the kit that fetch_kit.ps1 fetches.
EXPECTED_REPORTED = 245

PATTERN = re.compile(r"\+TEST\+(PASS|SEV[123]|BLOCK|WARN|ABORT)\s*:\s*Test\s*:(\S+)\s*\n"
                     r"Group\s*:(\S+)\s*\nStatus\s*:(\S+)\s*(\([^)]*\))?")


def results(log):
    """Map "Group:Test" to (outcome, status) for every test the log reports."""
    out = {}
    for m in PATTERN.finditer(log.read_text(encoding="latin-1")):
        key = f"{m.group(3)}:{m.group(2)}"
        # A test reports once; keep the first failure if it reports again.
        if out.get(key, ("PASS",))[0] == "PASS":
            out[key] = (m.group(1), (m.group(5) or m.group(4)).strip("()"))
    return out


def expected(path):
    """Map backend to {"Group:Test": reason} for the tests known not to pass."""
    out = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(line for line in f if not line.startswith("#")):
            for backend in row["backends"].split():
                out.setdefault(backend, {})[row["test"]] = row["reason"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("logdir", type=Path)
    ap.add_argument("expected", type=Path)
    ap.add_argument("backends", nargs="+")
    ap.add_argument("--junit", type=Path)
    args = ap.parse_args()

    known = expected(args.expected)
    failed = False
    cases = []
    for backend in args.backends:
        log = args.logdir / f"ifstest-chimera-{backend}.log"
        res = results(log) if log.exists() else {}
        xfail = {**known.get("*", {}), **known.get(backend, {})}
        passed = sum(1 for o, _ in res.values() if o == "PASS")
        regressions = sorted(t for t, (o, _) in res.items() if o != "PASS" and t not in xfail)
        fixed = sorted(t for t, (o, _) in res.items() if o == "PASS" and t in xfail)
        print(f"{backend}: {passed} of {len(res)} reported tests pass, "
              f"{len(res) - passed} fail ({len(res) - passed - len(regressions)} known)")
        if len(res) < EXPECTED_REPORTED:
            print(f"  FAIL: IFSTest reported {len(res)} tests, expected {EXPECTED_REPORTED}")
            failed = True
        for t in regressions:
            print(f"  FAIL: {t}: {res[t][0]} {res[t][1]}")
            failed = True
        for t in fixed:
            print(f"  now passes, take it off the known failures: {t}")
        for t, (o, status) in sorted(res.items()):
            cases.append((backend, t, o, status, xfail.get(t)))

    if args.junit:
        with open(args.junit, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n<testsuites>\n')
            f.write(f'<testsuite name="ifstest" tests="{len(cases)}">\n')
            for backend, t, o, status, reason in cases:
                f.write(f'<testcase classname={quoteattr("ifstest." + backend)} name={quoteattr(t)}>')
                if o != "PASS" and reason:
                    f.write(f'<skipped message={quoteattr("known failure: " + reason)}/>')
                elif o != "PASS":
                    f.write(f'<failure message={quoteattr(o + " " + status)}/>')
                f.write('</testcase>\n')
            f.write('</testsuite>\n</testsuites>\n')

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
