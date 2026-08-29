#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Report how much of what a pull request changed the model suites executed.

Two modes, because the llvm-cov export in between needs to know which files to
bother with:

    ci_patch_coverage.py --sources <diff>
        Print the repo-relative paths worth measuring, one per line, for
        COVERAGE_LCOV_SOURCES.  Empty output means the PR touched no measurable
        source, and etc/coverage-report.sh then skips the export entirely.

    ci_patch_coverage.py <diff> <lcov> <repo_root> [<repo> <sha>]
        Render the markdown section.  <repo>/<sha> are optional and only make
        the uncovered lines clickable.

The overall table answers "how much of chimera do the models reach".  This
answers "how much of what this PR wrote did they reach", which is the question a
reviewer actually has and the one the aggregate cannot show: a 300-line addition
that nothing executes moves the total by a fraction of a point.

Line counts come from `llvm-cov export --format=lcov`, whose DA records give a
hit count per instrumented line.  A changed line with no DA record is not
executable -- a declaration, a brace, a comment, a blank -- and is left out of
the denominator rather than counted as a miss.  Only added and modified lines
count; deleting code is not a coverage regression.

The exclusions are imported rather than restated.  A patch number computed over
a different set of files than the aggregate would be a second number in the same
comment quietly meaning something else.
"""
import collections
import os
import re
import sys

from ci_coverage_report import is_model_unreachable, is_test_source

# Sources llvm-cov can have line counts for.  chimera is C; anything else in a
# diff (CMake, YAML, .qnt models, documentation) has no coverage to report and
# only lengthens the export command.
MEASURABLE_SUFFIXES = (".c", ".h")

# Enough uncovered lines to point a reviewer at the gap; past that the list
# stops being read and the file-level percentage is the signal.
MAX_UNCOVERED_RUNS = 12

BAR_WIDTH = 10


def measurable(rel):
    return (rel.endswith(MEASURABLE_SUFFIXES)
            and not is_test_source(rel)
            and not is_model_unreachable(rel))


def changed_lines(diff_path):
    """{path: set(line)} -- lines a diff adds or modifies, on the new side.

    Reads `git diff -U0`, so each hunk header names exactly the changed run
    with no context lines to subtract back out.
    """
    changed, cur = collections.defaultdict(set), None
    with open(diff_path, errors="replace") as f:
        for line in f:
            if line.startswith("+++ b/"):
                cur = line[6:].strip()
            elif line.startswith("+++ /dev/null"):
                cur = None                      # deletion; nothing to cover
            elif line.startswith("@@") and cur:
                m = re.match(r"@@ -\S+ \+(\d+)(?:,(\d+))? @@", line)
                if m:
                    start, count = int(m.group(1)), int(m.group(2) or 1)
                    # count 0 is a pure deletion hunk: no new-side lines.
                    changed[cur].update(range(start, start + count))
    return changed


def lcov_hits(lcov_path, root):
    """{path: {line: hits}} from an lcov tracefile, paths relative to root."""
    hits, cur = collections.defaultdict(dict), None
    with open(lcov_path, errors="replace") as f:
        for line in f:
            if line.startswith("SF:"):
                path = os.path.realpath(line[3:].strip())
                rel = os.path.relpath(path, root)
                cur = None if rel.startswith("..") else rel
            elif line.startswith("DA:") and cur is not None:
                num, _, rest = line[3:].strip().partition(",")
                count = int(rest.split(",")[0])
                # One line can carry several regions (a && b, a macro); it ran
                # if any of them did.
                prev = hits[cur].get(int(num))
                hits[cur][int(num)] = count if prev is None else max(prev, count)
    return hits


def runs(numbers):
    """[1,2,3,7,8] -> [(1,3),(7,8)] -- consecutive lines as ranges."""
    out = []
    for n in sorted(numbers):
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return [tuple(r) for r in out]


def bar(covered, total):
    filled = int(round(BAR_WIDTH * covered / total))
    return "`" + "█" * filled + "░" * (BAR_WIDTH - filled) + "`"


def link(repo, sha, rel, lo, hi):
    text = f"{lo}" if lo == hi else f"{lo}–{hi}"
    if not (repo and sha):
        return text
    frag = f"#L{lo}" if lo == hi else f"#L{lo}-L{hi}"
    return f"[{text}](https://github.com/{repo}/blob/{sha}/{rel}{frag})"


def main():
    if sys.argv[1:2] == ["--sources"]:
        for rel in sorted(changed_lines(sys.argv[2])):
            if measurable(rel):
                print(rel)
        return

    diff_path, lcov_path = sys.argv[1], sys.argv[2]
    root = os.path.realpath(sys.argv[3])
    repo = sys.argv[4] if len(sys.argv) > 4 else ""
    sha = sys.argv[5] if len(sys.argv) > 5 else ""

    changed = {rel: lines for rel, lines in changed_lines(diff_path).items()
               if measurable(rel)}
    out = ["", "### Coverage of this pull request's changes", ""]

    if not changed:
        print("\n".join(out + ["This pull request changes no instrumented "
                               "source, so there is nothing to measure."]))
        return

    # No lcov at all means the export was skipped or failed.  Say which, rather
    # than render 0% and let it read as a coverage collapse.
    if not os.path.exists(lcov_path):
        print("\n".join(out + [
            f"{len(changed)} changed source file(s), but no coverage export to "
            "measure them against — treat this as a broken report, not as 0%."]))
        return

    hits = lcov_hits(lcov_path, root)

    rows, uncovered, tot_cov, tot_exec = [], [], 0, 0
    for rel in sorted(changed):
        file_hits = hits.get(rel, {})
        executable = sorted(changed[rel] & file_hits.keys())
        if not executable:
            # Instrumented file, but the change was to comments, declarations
            # or whitespace.  A row saying 0/0 would read as a miss.
            rows.append((rel, None, None))
            continue
        missed = [n for n in executable if file_hits[n] == 0]
        covered = len(executable) - len(missed)
        tot_cov += covered
        tot_exec += len(executable)
        rows.append((rel, covered, len(executable)))
        if missed:
            uncovered.append((rel, runs(missed)))

    if not tot_exec:
        print("\n".join(out + ["This pull request changes no executable lines "
                               "(comments, declarations or build files only)."]))
        return

    out += [f"**{bar(tot_cov, tot_exec)} {100.0 * tot_cov / tot_exec:.0f}% "
            f"({tot_cov:,}/{tot_exec:,} changed lines executed)**", "",
            "| File | Changed lines executed |", "|---|---|"]
    for rel, covered, total in rows:
        if total is None:
            out.append(f"| `{rel}` | no executable lines changed |")
        else:
            out.append(f"| `{rel}` | {bar(covered, total)} "
                       f"{100.0 * covered / total:.0f}% ({covered}/{total}) |")

    if uncovered:
        shown = 0
        items = []
        for rel, spans in uncovered:
            take = spans[:max(0, MAX_UNCOVERED_RUNS - shown)]
            if not take:
                break
            shown += len(take)
            items.append("`" + rel + "` "
                         + ", ".join(link(repo, sha, rel, lo, hi)
                                     for lo, hi in take))
        total_runs = sum(len(s) for _, s in uncovered)
        more = (f" …and {total_runs - shown} more"
                if total_runs > shown else "")
        out += ["", "Changed lines the model suites never executed: "
                + "; ".join(items) + more + "."]

    out += ["", "<sub>Executed by the quint model suites only "
            "(`ctest -L quint`) — code reached solely by the other suites "
            "counts as unexecuted here. Lines with no coverage mapping "
            "(declarations, braces, comments) are excluded rather than counted "
            "as misses.</sub>"]
    print("\n".join(out))


if __name__ == "__main__":
    main()
