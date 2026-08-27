#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Render llvm-cov's per-file summary as a markdown coverage report.

Usage: ci_coverage_report.py <export.json> <repo_root> [<run_url>] [<build_root>]

<export.json> is what `COVERAGE_JSON=... etc/coverage-report.sh` writes, i.e.
`llvm-cov export -summary-only`.  The output is a sticky-comment body for
scripts/ci_pr_comment.py (CI_COMMENT_MARKER must match MARKER below) and doubles
as a run-summary fragment.

The report is a table and the command that reproduces it, and nothing else.  It
appears on every PR, usually read by someone checking whether their change moved
the number, and prose does not survive that kind of repetition.

Three metrics rather than one, because where they disagree is the useful part.
Lines say how much of the code ran; functions say how much of the API surface was
entered at all; branches say how many of the decisions inside the code that did
run were taken both ways.  A component high on lines and low on branches has been
walked down its happy path and never off it -- which is the gap a model-based
suite exists to close.

What counts is first-party, hand-written, non-test source.  Test sources are out
because a suite cannot meaningfully cover the other suites; generated marshallers
and the bundled ext/ projects are out because neither is code this repository is
asking the models to reach -- one is emitted from a .x file and the other has its
own repository and its own tests.  etc/coverage-report.sh applies the latter two
(COVERAGE_INCLUDE_GENERATED / COVERAGE_INCLUDE_EXT fold them back in); this only
has to drop the tests.

<build_root> is optional, and only useful alongside COVERAGE_INCLUDE_GENERATED:
generated sources are compiled from under the build tree, which mirrors the
source layout, so naming it puts a generated marshaller in the same component as
the hand-written code it belongs to instead of dropping it as foreign.
"""
import json
import os
import sys

MARKER = "<!-- quint-coverage-report -->"

# The selection this report measures, as something to paste.  The make target
# builds Coverage, runs the label with LLVM_PROFILE_FILE set, and then runs
# etc/coverage-report.sh over the result -- exactly what CI does.
REPRO = 'make coverage CTEST_ARGS="-L quint --output-on-failure"'

METRICS = ("functions", "lines", "branches")

BAR_WIDTH = 10


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


def relative(path, roots):
    """Path relative to whichever root contains it, or None if none does.

    Longest root wins, so an in-tree build directory (build/ inside the
    checkout) attributes its generated sources to the build tree rather than
    to a "build/..." component of the source tree.
    """
    for root in sorted((r for r in roots if r), key=len, reverse=True):
        rel = os.path.relpath(path, root)
        if not rel.startswith(".."):
            return rel
    return None


def cell(covered, count):
    """One metric as a bar, a percentage and the raw pair.

    Two lines rather than one: three of these per row on a single line forces a
    horizontal scrollbar onto the comment, which is where a wide table stops
    being read at all.
    """
    if not count:
        return "—"
    percent = 100.0 * covered / count
    filled = int(round(percent / 100.0 * BAR_WIDTH))
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
    return f"`{bar}` {percent:.0f}%<br>{covered:,}/{count:,}"


def main():
    export_path, root = sys.argv[1], os.path.realpath(sys.argv[2])
    run_url = sys.argv[3] if len(sys.argv) > 3 else ""
    build_root = os.path.realpath(sys.argv[4]) if len(sys.argv) > 4 else ""

    with open(export_path) as f:
        data = json.load(f)

    comps = {}
    for entry in data.get("data", [{}])[0].get("files", []):
        rel = relative(os.path.realpath(entry["filename"]), (root, build_root))
        # None: compiled from outside the checkout entirely -- the container's
        # own /fio clone, say.  Not code this repository can be measured on.
        if rel is None or is_test_source(rel):
            continue
        summary = entry.get("summary", {})
        if summary.get("lines", {}).get("count", 0) == 0:
            continue
        totals = comps.setdefault(component(rel),
                                  {m: [0, 0] for m in METRICS})
        for metric in METRICS:
            got = summary.get(metric, {})
            totals[metric][0] += got.get("count", 0)
            totals[metric][1] += got.get("covered", 0)

    title = "Quint model-based test coverage"
    out = [MARKER, "",
           f"## [{title}]({run_url})" if run_url else f"## {title}", ""]

    if not comps:
        out += ["No instrumented chimera source in the coverage export — the "
                "report step ran, but nothing it measured belongs to this tree. "
                "Treat this as a broken report, not as 0% coverage.",
                "", "```sh", REPRO, "```"]
        print("\n".join(out))
        return

    out += ["| Component | Functions | Lines | Branches |",
            "|---|---|---|---|"]

    grand = {m: [0, 0] for m in METRICS}
    for name, totals in sorted(comps.items(),
                               key=lambda kv: -kv[1]["lines"][0]):
        cells = []
        for metric in METRICS:
            count, covered = totals[metric]
            grand[metric][0] += count
            grand[metric][1] += covered
            cells.append(cell(covered, count))
        out.append(f"| `{name}` | " + " | ".join(cells) + " |")

    out.append("| **Total** | "
               + " | ".join(cell(grand[m][1], grand[m][0]) for m in METRICS)
               + " |")

    out += ["", "```sh", REPRO, "```"]
    print("\n".join(out))


if __name__ == "__main__":
    main()
