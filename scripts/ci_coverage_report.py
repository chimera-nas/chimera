#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Render llvm-cov's per-file summary as a markdown coverage report.

Usage: ci_coverage_report.py <export.json> <repo_root> [<run_url>]

<export.json> is what `COVERAGE_JSON=... etc/coverage-report.sh` writes, i.e.
`llvm-cov export -summary-only`.  The output is a sticky-comment body for
scripts/ci_pr_comment.py (CI_COMMENT_MARKER must match MARKER below) and doubles
as a run-summary fragment.

Test sources are excluded from the totals.  The harness that drives a suite is
covered by definition, and folding it in would flatter every number here; what
this report is for is how much of the *product* the model-based suites reach.
"""
import json
import os
import sys

MARKER = "<!-- quint-coverage-report -->"

# How many "biggest gap" files to name.  Enough to aim the next round of model
# work at, short enough to stay a comment rather than a document.
TOP_GAPS = 12


def component(rel):
    """Bucket a repo-relative path into the unit we report on.

    src/server/ is split one level deeper than everything else: "the NFS server"
    and "the SMB server" are the units someone actually reasons about, whereas
    "src/server" as a whole is not.
    """
    parts = rel.split("/")
    if len(parts) >= 4 and parts[0] == "src" and parts[1] == "server":
        return "/".join(parts[:3])
    if len(parts) >= 3:
        return "/".join(parts[:2])
    return parts[0]


def is_test_source(rel):
    return rel.startswith("tests/") or "/tests/" in rel


def pct(covered, count):
    return 100.0 * covered / count if count else 0.0


def bar(percent, width=20):
    filled = int(round(percent / 100.0 * width))
    return "█" * filled + "░" * (width - filled)


def main():
    export_path, root = sys.argv[1], os.path.realpath(sys.argv[2])
    run_url = sys.argv[3] if len(sys.argv) > 3 else ""

    with open(export_path) as f:
        data = json.load(f)

    comps = {}
    files = []
    skipped = 0

    for entry in data.get("data", [{}])[0].get("files", []):
        rel = os.path.relpath(os.path.realpath(entry["filename"]), root)
        if rel.startswith(".."):
            continue
        if is_test_source(rel):
            skipped += 1
            continue
        lines = entry.get("summary", {}).get("lines", {})
        count, covered = lines.get("count", 0), lines.get("covered", 0)
        if count == 0:
            continue
        c = comps.setdefault(component(rel), [0, 0])
        c[0] += count
        c[1] += covered
        files.append((rel, count, covered))

    total_count = sum(c[0] for c in comps.values())
    total_covered = sum(c[1] for c in comps.values())

    out = [MARKER, "", "## Quint model-based test coverage", ""]
    if total_count == 0:
        out += ["No instrumented chimera source in the coverage export — "
                "the report step ran, but nothing it measured belongs to this "
                "tree. Treat this as a broken report, not as 0% coverage."]
        print("\n".join(out))
        return

    out += [
        f"`ctest -L quint` alone covers **{pct(total_covered, total_count):.1f}%** "
        f"of chimera ({total_covered:,} / {total_count:,} lines).",
        "",
        "This is the model-based suites on their own — not the full ctest run. "
        "The number going up is the point: the more the models reach, the less "
        "the expensive merge-queue suites are the only thing standing between a "
        "regression and main.",
        "",
        "| Component | Lines | Covered | % | |",
        "|---|---:|---:|---:|---|",
    ]

    for name, (count, covered) in sorted(comps.items(),
                                         key=lambda kv: -kv[1][0]):
        p = pct(covered, count)
        out.append(f"| `{name}` | {count:,} | {covered:,} | {p:.1f}% | "
                   f"`{bar(p)}` |")

    p = pct(total_covered, total_count)
    out.append(f"| **Total** | **{total_count:,}** | **{total_covered:,}** | "
               f"**{p:.1f}%** | `{bar(p)}` |")

    gaps = sorted(files, key=lambda f: -(f[1] - f[2]))[:TOP_GAPS]
    if gaps:
        out += ["", "<details><summary>Biggest gaps — the files with the most "
                "lines the models never reach</summary>", "",
                "| File | Uncovered | Lines | % |", "|---|---:|---:|---:|"]
        for rel, count, covered in gaps:
            out.append(f"| `{rel}` | {count - covered:,} | {count:,} | "
                       f"{pct(covered, count):.1f}% |")
        out += ["", "</details>"]

    note = f"{skipped} test source(s) excluded"
    if run_url:
        note += f" · [run]({run_url})"
    out += ["", f"<sub>{note}</sub>"]

    print("\n".join(out))


if __name__ == "__main__":
    main()
