#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Run the clang static analyzer over a compile_commands.json, one cacheable
# invocation per translation unit.
#
# Why not scan-build: scan-build sets CC to ccc-analyzer, which compiles the TU
# *and*, as a side effect, writes a report into a directory named by an
# environment variable.  ccache knows nothing about that side effect -- it
# caches the object and the stderr -- so a cache hit replays a compile that
# looks identical while producing no report at all, and `--status-bugs` then
# sees a clean run.  That is not a hypothetical: the same source goes from
# "1 bug found" to "No bugs found" once the cache is warm, with the warning
# text still printed from the replayed stderr.  So scan-build must run with
# CCACHE_DISABLE=1, and pays a full uncached compile of the tree every time.
#
# Driving the analyzer directly avoids the whole problem.  `clang --analyze
# -o report.plist` makes the report the *primary* output -- the thing -o names
# -- so it is exactly what ccache already knows how to cache.  A hit restores
# the plist with its findings intact, and a real source change invalidates it
# like any other compile.
#
# Usage: analyze_cached.py --build-dir DIR --report-dir DIR [-j N]
#                          [--exclude SUBSTR ...] [--checker NAME ...]

import argparse
import concurrent.futures
import hashlib
import os
import plistlib
import shlex
import subprocess
import sys

# Flags that make no sense for, or actively fight with, an --analyze run:
# -c asks for an object, the -o we care about is our own, and the dependency
# flags would scribble .d files over the ones the real build produced.
DROP_FLAGS = {"-c", "-MD", "-MMD", "-MP"}
DROP_WITH_ARG = {"-o", "-MT", "-MF", "-MQ"}


def analyzer_argv(entry, plist_path, checkers):
    argv = entry["arguments"] if "arguments" in entry else shlex.split(entry["command"])
    out, skip = [], False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg in DROP_WITH_ARG:
            skip = True
            continue
        if arg in DROP_FLAGS:
            continue
        out.append(arg)
    # ccache sees a distinct command line from the real compile, so the analyzer
    # results occupy their own cache entries and never collide with objects.
    argv = ["ccache"] + out + ["--analyze", "-Xclang", "-analyzer-output=plist"]
    for c in checkers:
        argv += ["-Xclang", "-analyzer-checker=" + c]
    return argv + ["-o", plist_path]


def run_one(entry, report_dir, checkers):
    src = entry["file"]
    # The plist name has to be unique per TU: the same basename appears in more
    # than one directory here, and two entries can even share a source path with
    # different flags.
    key = hashlib.sha256(
        (src + "\0" + entry.get("command", "") + "".join(entry.get("arguments", []))).encode()
    ).hexdigest()[:16]
    stem = os.path.basename(src).replace(os.sep, "_")
    plist = os.path.join(report_dir, f"{stem}.{key}.plist")
    argv = analyzer_argv(entry, plist, checkers)
    proc = subprocess.run(argv, cwd=entry["directory"], capture_output=True, text=True)
    return src, plist, proc.returncode, proc.stderr


def findings(plist_path):
    """Return (description, file, line) for each diagnostic the analyzer emitted."""
    try:
        with open(plist_path, "rb") as fh:
            doc = plistlib.load(fh)
    except Exception:
        return []
    files = doc.get("files", [])
    out = []
    for d in doc.get("diagnostics", []):
        loc = d.get("location", {})
        idx = loc.get("file")
        name = files[idx] if isinstance(idx, int) and idx < len(files) else "?"
        out.append((d.get("description", "?"), name, loc.get("line", 0)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", required=True)
    ap.add_argument("--report-dir", required=True)
    ap.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--exclude", action="append", default=[],
                    help="skip TUs whose path contains this substring (repeatable)")
    ap.add_argument("--checker", action="append", default=[],
                    help="additionally enable this analyzer checker (repeatable)")
    args = ap.parse_args()

    cc = os.path.join(args.build_dir, "compile_commands.json")
    with open(cc) as fh:
        import json
        entries = json.load(fh)

    kept = [e for e in entries if not any(x in e["file"] for x in args.exclude)]

    # The analyzer lives in clang; gcc rejects --analyze outright.  Whatever
    # configured this build tree has to have picked a clang, and if it did not,
    # say so once instead of failing every translation unit identically -- the
    # first time this ran in CI it produced 838 copies of the same gcc error.
    if kept:
        probe = kept[0]
        argv = probe["arguments"] if "arguments" in probe else shlex.split(probe["command"])
        cc = argv[0]
        ok = subprocess.run([cc, "--analyze", "-xc", "-", "-o", os.devnull],
                            input="int main(void){return 0;}", text=True,
                            capture_output=True, cwd=probe["directory"])
        if ok.returncode != 0:
            print(f"error: {cc} does not support --analyze; configure the build "
                  f"with -DCMAKE_C_COMPILER=clang\n{ok.stderr.strip()[:500]}",
                  file=sys.stderr)
            return 2
    os.makedirs(args.report_dir, exist_ok=True)
    print(f"analyzing {len(kept)} of {len(entries)} translation units "
          f"with -j{args.jobs}", flush=True)

    failed_runs, all_findings = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for src, plist, rc, stderr in pool.map(
                lambda e: run_one(e, args.report_dir, args.checker), kept):
            if rc != 0:
                # The analyzer itself failed -- a broken command line, a missing
                # generated header.  Loud, because a TU that never ran is not a
                # TU with no findings.
                failed_runs.append((src, stderr.strip()))
                continue
            for desc, f, line in findings(plist):
                all_findings.append((f, line, desc))

    for src, err in failed_runs:
        print(f"::error::analyzer failed on {src}\n{err[:2000]}", file=sys.stderr)

    for f, line, desc in sorted(all_findings):
        print(f"{f}:{line}: warning: {desc}")

    print(f"\n{len(all_findings)} finding(s), {len(failed_runs)} analyzer failure(s)")
    return 1 if (all_findings or failed_runs) else 0


if __name__ == "__main__":
    sys.exit(main())
