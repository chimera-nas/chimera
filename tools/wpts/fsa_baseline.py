#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Gate an MS-FSAModel full-suite run against a recorded baseline.

MS-FSAModel is a model-based suite: its cases are NOT independent (outcomes
depend on the full ordered S0,S2,... sequence run against one server), so we
cannot gate on a curated green subset the way the MS-SMB2 suite does -- a
subset behaves differently from the full run.  Instead the whole suite is run
every time and each case's outcome is compared to a recorded baseline
(wpts_fsa_cases.csv: <case,baseline,area,detail> with baseline in pass|fail|skip).

Exit non-zero (CI failure) ONLY on a regression: a case the baseline records as
`pass` that is no longer Passed (now Failed, NotExecuted, or absent -- the latter
catches a mid-run daemon crash).  Known failures (fail->fail) and skips
(skip->NotExecuted) are tolerated.  Improvements (fail/skip -> pass) and newly
appearing cases are reported as baseline drift to fold back in, but do NOT fail
the run.

Usage: fsa_baseline.py <result.trx> <baseline.csv>
"""
import csv
import sys
import xml.etree.ElementTree as ET

NS = "{http://microsoft.com/schemas/VisualStudio/TeamTest/2010}"
# TRX outcome -> normalized
_PASS = "Passed"


def load_trx(path):
    outcomes = {}
    root = ET.parse(path).getroot()
    for u in root.iter(NS + "UnitTestResult"):
        outcomes[u.get("testName")] = u.get("outcome")
    return outcomes


def load_baseline(path):
    base = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            base[row["case"]] = row["baseline"].strip()
    return base


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    trx, csvp = sys.argv[1], sys.argv[2]
    cur = load_trx(trx)
    base = load_baseline(csvp)

    regressions = []          # baseline=pass, now not Passed (or absent)
    new_pass = []             # baseline in {fail,skip}, now Passed -> promote
    skip_now_fail = []        # baseline=skip, now Failed -> drift (warn)
    unknown = []              # ran but not in baseline -> suite changed

    for case, want in base.items():
        got = cur.get(case)
        if want == "pass":
            if got != _PASS:
                regressions.append((case, got or "ABSENT"))
        elif want in ("fail", "skip"):
            if got == _PASS:
                new_pass.append(case)
            elif want == "skip" and got == "Failed":
                skip_now_fail.append(case)

    for case in cur:
        if case not in base:
            unknown.append(case)

    n_base_pass = sum(1 for v in base.values() if v == "pass")
    n_now_pass = sum(1 for v in cur.values() if v == _PASS)
    print(f"FSAModel baseline gate: baseline pass={n_base_pass}, "
          f"this run pass={n_now_pass}, total cases={len(cur)}")

    if new_pass:
        print(f"  [drift] {len(new_pass)} case(s) now PASS that the baseline "
              f"records as fail/skip -- promote them in wpts_fsa_cases.csv:")
        for c in sorted(new_pass):
            print(f"            + {c}")
    if skip_now_fail:
        print(f"  [drift] {len(skip_now_fail)} baseline=skip case(s) now FAIL "
              f"(model/env shift, not gated):")
        for c in sorted(skip_now_fail):
            print(f"            ~ {c}")
    if unknown:
        print(f"  [drift] {len(unknown)} case(s) ran but are absent from the "
              f"baseline (suite version changed?):")
        for c in sorted(unknown):
            print(f"            ? {c}")

    if regressions:
        print(f"  [FAIL] {len(regressions)} REGRESSION(S) -- baseline=pass case "
              f"no longer passes:")
        for c, got in sorted(regressions):
            print(f"            - {c}: {got}")
        return 1

    print("  [OK] no regressions against the baseline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
