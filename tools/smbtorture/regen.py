#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Generate or diff the SMBTORTURE_SUITES + SMBTORTURE_ENABLED_<backend> blocks
   in src/server/smb/tests/CMakeLists.txt from a smbtorture matrix run.

   Modes:
       generate   splice a freshly measured matrix into the CMake block
       compare    diff a matrix result against the checked-in baseline

   Inputs shared by both modes:
       --results    path to a `PASS|FAIL|TIME|SKIP|<backend>|<subtest>` log as
                    written by tools/smbtorture/regen.sh.  compare accepts the
                    flag more than once; cells whose status disagrees between
                    runs are reported as flaky.
       --cmake      path to src/server/smb/tests/CMakeLists.txt

   generate splices a single contiguous block containing the suite list and all
   six per-backend allowlists, replacing whatever lies between the anchor markers
   `# >>> SMBTORTURE_BLOCK_BEGIN` and `# >>> SMBTORTURE_BLOCK_END`.  Comments
   hand-written against an individual entry are carried across the rewrite (see
   split_entries); the hand-curated SMBTORTURE_ISOLATE_SUITES and
   SMBTORTURE_DISABLED_SUBTESTS blocks live outside the markers and are never
   touched.
"""

import argparse, collections, re, sys

BACKENDS = ("memfs","linux","io_uring","cairn","diskfs_io_uring","diskfs_aio")
BEGIN = "# >>> SMBTORTURE_BLOCK_BEGIN (regenerate with tools/smbtorture/regen.sh)"
END   = "# >>> SMBTORTURE_BLOCK_END"

# A comment regen.py itself emits: the `# smb2.<suite>` group header.  Anything
# else inside a list is hand-written prose about the entry that follows it.
GROUP_COMMENT = re.compile(r"#\s*smb2\.[\w-]+\s*$")

STATUSES = ("PASS","FAIL","TIME","SKIP")


def parse_results(paths):
    """Map (backend, subtest) -> [status, ...] in the order the runs were given."""
    res = collections.defaultdict(list)
    for path in paths:
        seen = {}
        with open(path) as f:
            for line in f:
                parts = line.rstrip().split("|")
                if len(parts) < 3 or parts[0] not in STATUSES:
                    continue
                seen[(parts[1], parts[2])] = parts[0]
        for cell, status in seen.items():
            res[cell].append(status)
    return res


LIST_OPEN = re.compile(r"set\(SMBTORTURE_(\w+)\s*$")


def parse_cmake_lists(txt):
    """Extract every multi-line `set(SMBTORTURE_<name>\\n ... \\n)` list.

       Returns {name: [line, ...]} covering both the generated lists and the
       hand-curated ones (DISABLED_SUBTESTS, ISOLATE_SUITES).  Scanned line by
       line rather than with one regex: the file also holds single-line and
       `CACHE`-decorated SMBTORTURE_* sets, and a non-greedy body pattern
       mis-pairs their parens with a later list's closing one.
    """
    out, name, body = {}, None, []
    for line in txt.splitlines():
        if name is None:
            m = LIST_OPEN.match(line.strip())
            if m:
                name, body = m.group(1), []
            continue
        # A list may close on its own line or on the last entry's line
        # (`    rpc.lsalookup)`), which must still terminate the scan.  Comments
        # are exempt: the curated prose ends in ")" often enough
        # ("...(lock-vs-caching conflict)") to close the list 400 entries early.
        stripped = line.rstrip()
        if stripped.endswith(")") and not stripped.lstrip().startswith("#"):
            last = stripped[:-1]
            if last.strip():
                body.append(last)
            out[name] = body
            name = None
        else:
            body.append(line)
    return out


def split_entries(lines):
    """Split raw list body lines into (entries, comments-attached-to-entry).

       A hand-written comment attaches to exactly the entry below it.  Prose that
       covers several entries has to be repeated above each of them, because
       generate re-sorts the list and any adjacency-based grouping is lost -- one
       comment per entry is the only association that survives a rewrite (and
       makes this parse a fixed point, so a regen of a generated block is a
       no-op).

       Group headers (`# smb2.<suite>`) are dropped; generate re-emits those.
    """
    entries, attached, pending = [], {}, []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            if not GROUP_COMMENT.match(s):
                pending.append(s)
            continue
        entry = s.strip('"')
        entries.append(entry)
        if pending:
            attached[entry] = pending
        pending = []
    return entries, attached


def cmake_quote(s):
    return f'"{s}"' if any(c in s for c in ' ()') else s


def parent_of(subtest):
    bits = subtest.split(".")
    return bits[0] + "." + bits[1]


def emit_block(subs, res, attached):
    by_suite = collections.defaultdict(list)
    for s in subs:
        by_suite[parent_of(s)].append(s)

    out = []
    w = out.append
    w(BEGIN)
    w("# Every smb2.* atomic subtest the Samba smbtorture client knows about,")
    w("# one ctest per (subtest, backend).  Generated by tools/smbtorture/regen.sh.")
    w("set(SMBTORTURE_SUITES")
    for parent in sorted(by_suite):
        w(f"    # {parent}")
        for s in sorted(by_suite[parent]):
            w(f"    {cmake_quote(s)}")
    w(")")
    w("")
    for b in BACKENDS:
        enabled = sorted([s for s in subs if res.get((b,s), [None])[-1] == "PASS"])
        w(f"set(SMBTORTURE_ENABLED_{b}")
        cur = None
        for s in enabled:
            parent = parent_of(s)
            if parent != cur:
                cur = parent
                w(f"    # {parent}")
            # Carry across any rationale hand-written against this entry.
            for c in attached.get(b, {}).get(s, ()):
                w(f"    {c}")
            w(f"    {cmake_quote(s)}")
        w(")")
        w("")
    w(END)
    return "\n".join(out)


def log_path(results_path, backend, subtest):
    import os
    sid = re.sub(r"[^A-Za-z0-9]", "_", subtest)
    return os.path.join(os.path.dirname(os.path.abspath(results_path)),
                        f"smbt-fail.{backend}.{sid}.log")


def do_generate(args):
    subs = [l.strip() for l in open(args.subs) if l.strip()]
    res = parse_results(args.results)

    txt = open(args.cmake).read()
    if BEGIN not in txt or END not in txt:
        print(f"error: anchor markers not found in {args.cmake}", file=sys.stderr)
        return 1

    # Preserve rationale comments hand-written against individual entries.
    lists = parse_cmake_lists(txt)
    attached, kept = {}, 0
    for b in BACKENDS:
        raw = lists.get(f"ENABLED_{b}")
        if raw is None:
            continue
        _, att = split_entries(raw)
        attached[b] = att
        kept += sum(len(v) for v in att.values())

    block = emit_block(subs, res, attached)
    head, _, rest = txt.partition(BEGIN)
    _, _, tail = rest.partition(END)
    new = head + block + tail

    if args.block_only:
        new = block + "\n"
    dest = args.output or args.cmake
    open(dest, "w").write(new)

    n_pass = sum(1 for v in res.values() if v[-1] == "PASS")
    print(f"wrote {dest}: {len(subs)} subtests, {n_pass} pass cells, "
          f"{kept} hand comments preserved")
    return 0


def do_compare(args):
    txt = open(args.cmake).read()
    lists = parse_cmake_lists(txt)
    if "SUITES" not in lists:
        print(f"error: SMBTORTURE_SUITES not found in {args.cmake}", file=sys.stderr)
        return 1

    catalog, _ = split_entries(lists["SUITES"])
    if args.subs:
        catalog = [l.strip() for l in open(args.subs) if l.strip()]
    disabled = set(split_entries(lists.get("DISABLED_SUBTESTS", []))[0])
    res = parse_results(args.results)

    report, totals = [], collections.Counter()
    for b in BACKENDS:
        raw = lists.get(f"ENABLED_{b}")
        if raw is None:
            continue
        baseline = set(split_entries(raw)[0])
        # The gated set is what add_smbtorture_backend_tests() actually registers.
        gated = baseline - disabled
        buckets = collections.defaultdict(list)
        for s in catalog:
            statuses = res.get((b, s))
            if not statuses:
                buckets["not measured"].append(s)
                continue
            if len(set(statuses)) > 1:
                buckets["flaky"].append(f"{s} [{'/'.join(statuses)}]")
                continue
            status = statuses[-1]
            if status == "PASS":
                if s not in baseline:
                    buckets["newly passing"].append(s)
            elif status == "SKIP":
                buckets["skipped"].append(s)
            elif s in gated:
                buckets["newly failing"].append(
                    f"{s} [{status}] {log_path(args.results[0], b, s)}")
            elif s in disabled:
                buckets["failing (disabled)"].append(f"{s} [{status}]")
            elif status == "TIME":
                buckets["timing out"].append(
                    f"{s} {log_path(args.results[0], b, s)}")
            else:
                buckets["still failing"].append(
                    f"{s} {log_path(args.results[0], b, s)}")
        report.append((b, len(baseline), buckets))
        for k, v in buckets.items():
            totals[k] += len(v)

    md = args.format == "markdown"
    for b, n_baseline, buckets in report:
        counts = ", ".join(f"{k} {len(buckets[k])}" for k in sorted(buckets)
                           if k != "not measured")
        head = f"{b}: baseline {n_baseline}/{len(catalog)}"
        print(f"\n### {head}\n" if md else f"\n== {head}")
        print(f"{counts or 'no cells measured'}\n")
        for k in ("newly failing", "newly passing", "flaky", "timing out",
                  "still failing", "failing (disabled)", "skipped"):
            items = buckets.get(k)
            if not items or (k in args.quiet_buckets):
                continue
            print(f"{'#### ' if md else '-- '}{k} ({len(items)})")
            for s in items:
                print(f"  {'- ' if md else ''}{s}")
            print()

    print("== totals: " + (", ".join(f"{k} {v}" for k, v in sorted(totals.items()))
                           or "nothing measured"))
    if totals["newly failing"] and args.fail_on_newly_failing:
        print(f"\nerror: {totals['newly failing']} newly failing cell(s) -- "
              "a gated test regressed", file=sys.stderr)
        return 1
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)

    g = sub.add_parser("generate", help="splice a measured matrix into CMakeLists")
    g.add_argument("--subs", required=True, help="one-per-line subtest catalog")
    g.add_argument("--results", required=True, action="append",
                   help="matrix results.txt")
    g.add_argument("--cmake", required=True, help="CMakeLists.txt to read/rewrite")
    g.add_argument("--output", help="write here instead of --cmake (leaves the "
                                    "tree untouched)")
    g.add_argument("--block-only", action="store_true",
                   help="with --output, emit just the generated block")
    g.set_defaults(func=do_generate)

    c = sub.add_parser("compare", help="diff a matrix against the baseline")
    c.add_argument("--results", required=True, action="append",
                   help="matrix results.txt; repeat to detect flaky cells")
    c.add_argument("--cmake", required=True, help="CMakeLists.txt holding the baseline")
    c.add_argument("--subs", help="restrict the catalog to this list")
    c.add_argument("--format", choices=("text","markdown"), default="text")
    c.add_argument("--fail-on-newly-failing", action="store_true",
                   help="exit 1 if a gated test regressed (nightly gate)")
    c.add_argument("--quiet-buckets", default="", type=lambda s: set(filter(None, s.split(","))),
                   help="comma separated bucket names to summarize but not list")
    c.set_defaults(func=do_compare)

    args = ap.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
