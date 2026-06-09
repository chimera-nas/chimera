#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Render a Markdown CI test report from the per-job CTest artifacts.

Usage: ci_test_report.py <results-dir> > report.md

<results-dir> holds one subdirectory per test job (the downloaded
`ctest-junit-<image>-<arch>-<build_type>` artifacts), each containing:
  - results.xml        : CTest --output-junit (one <testcase> per CTest test)
  - duration_seconds   : wall-clock of the whole ctest run (parallel-reduced)
  - pynfs-junit/*.xml   : pynfs per-code JUnit; a combined pynfs run is a single
                         CTest test, so its ~500 codes only appear here.

The report is a per-run table (wall clock, summed test time, pass/fail counts)
followed by every failed test named -- with failed `combined_*` pynfs CTest
entries expanded to the specific failing codes from their pynfs JUnit.
"""
import os
import sys
import glob
import xml.etree.ElementTree as ET

MARKER = "<!-- ctest-report -->"


def fmt_dur(seconds):
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def testcases(path):
    """Yield (name, failed) for each <testcase> in a JUnit file."""
    try:
        root = ET.parse(path).getroot()
    except Exception:
        return
    for tc in root.iter("testcase"):
        name = tc.get("name") or "?"
        failed = tc.find("failure") is not None or tc.find("error") is not None
        yield name, failed, float(tc.get("time") or 0.0)


def run_label(dirname):
    # ctest-junit-<image>-<arch>-<build_type> -> "image · arch · build_type"
    base = os.path.basename(dirname)
    if base.startswith("ctest-junit-"):
        base = base[len("ctest-junit-"):]
    parts = base.rsplit("-", 2)
    return " · ".join(parts)


def collect(run_dir):
    """Return a summary dict for one run directory."""
    results_xml = os.path.join(run_dir, "results.xml")
    pynfs_dir = os.path.join(run_dir, "pynfs-junit")

    passed = failed = 0
    sum_time = 0.0
    failures = []  # list of named failures (codes, not the pynfs rollup)
    have_results = os.path.exists(results_xml)

    for name, is_fail, t in testcases(results_xml):
        sum_time += t
        # Expand a failed combined pynfs CTest entry to its specific codes.
        if is_fail and name.startswith("chimera/pynfs/combined_"):
            suite = name.split("/")[-1]  # combined_<backend>_nfs4.<minor>
            pj = os.path.join(pynfs_dir, suite + ".xml")
            codes = [n for n, f, _ in testcases(pj) if f]
            if codes:
                failures.extend(f"{name}  →  {c}" for c in codes)
            else:
                failures.append(name)
            failed += 1
        elif is_fail:
            failures.append(name)
            failed += 1
        else:
            passed += 1

    wall = None
    dpath = os.path.join(run_dir, "duration_seconds")
    if os.path.exists(dpath):
        try:
            wall = float(open(dpath).read().strip())
        except Exception:
            wall = None

    return {
        "label": run_label(run_dir),
        "passed": passed,
        "failed": failed,
        "sum_time": sum_time,
        "wall": wall,
        "failures": failures,
        "missing": not have_results,
    }


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    run_dirs = sorted(
        d for d in glob.glob(os.path.join(results_dir, "*")) if os.path.isdir(d)
    )
    runs = [collect(d) for d in run_dirs]

    out = [MARKER, "## CTest results", ""]

    total_failed = sum(r["failed"] for r in runs)
    if not runs:
        out.append("_No test-result artifacts found._")
        print("\n".join(out))
        return

    head = "✅ all green" if total_failed == 0 else f"❌ {total_failed} failed test(s)"
    out.append(f"**{head}** across {len(runs)} run(s).")
    out.append("")
    out.append("| Run | Wall | Σ test-time | Passed | Failed |")
    out.append("|---|--:|--:|--:|--:|")
    for r in runs:
        if r["missing"]:
            out.append(f"| {r['label']} | — | — | — | ⚠️ no results |")
            continue
        wall = fmt_dur(r["wall"]) if r["wall"] is not None else "—"
        fail_cell = "0" if r["failed"] == 0 else f"**{r['failed']}**"
        out.append(
            f"| {r['label']} | {wall} | {fmt_dur(r['sum_time'])} "
            f"| {r['passed']} | {fail_cell} |"
        )
    out.append("")

    if total_failed:
        out.append("### Failed tests")
        out.append("")
        for r in runs:
            if not r["failures"]:
                continue
            out.append(f"<details><summary><b>{r['label']}</b> — "
                       f"{len(r['failures'])} failed</summary>")
            out.append("")
            for name in r["failures"]:
                out.append(f"- `{name}`")
            out.append("")
            out.append("</details>")
            out.append("")

    print("\n".join(out))


if __name__ == "__main__":
    main()
