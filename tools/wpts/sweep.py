#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Feature-aware WPTS (MS-SMB2) test runner + reporter.

`src/server/smb/tests/wpts/wpts_cases.csv` is the single source of truth: one
row per case mapping it to the server config where its features are ON (its
"home"), and a status:

  green  -- passes today; CI gates on this subset.
  xfail  -- applicable but currently FAILING (a real chimera defect); tracked,
            never gates CI.
  skip   -- never-applicable today (needs an unimplemented capability or a
            test-environment feature); documented with a reason, never run.

`enc_path=1` (only valid on home=base rows) marks a feature-agnostic case that
ALSO runs under encryption -- the curated transport-sensitive subset that
surfaced the #797 interim-encryption bug.

This tool derives the per-config case sets from that CSV (the SAME derivation
the CMake green-gate uses, kept here so they cannot diverge in intent), runs
each config's set against a chimera daemon via scripts/wpts_smb_test_wrapper.sh,
and classifies every result against its CSV expectation.

Modes:
  --lint        validate the CSV invariants and exit.
  (default)     run each config's APPLICABLE (green+xfail) set in parallel,
                report green-pass / green-FAIL(regression) / xfail-fail(known) /
                xfail-PASS(promote) / skip-ledger.
  --check       like default but exit non-zero on any regression or feature-off
                leak (for gating use).
  --green-only  run only the green set per config (what CI runs).
  --discover    run each config UNFILTERED (all cases) and reconcile reality vs
                the CSV (new/moved/stale-skip cases). On-demand only.
  --update      with the above, emit a proposed CSV diff (promotions /
                regressions) for human review -- never writes the CSV.
"""
import argparse
import csv
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
CSV_PATH = os.path.join(REPO, "src/server/smb/tests/wpts/wpts_cases.csv")
WRAPPER = os.path.join(REPO, "scripts/wpts_smb_test_wrapper.sh")
TRX_NS = "{http://microsoft.com/schemas/VisualStudio/TeamTest/2010}"

CONFIGS = ["base", "persistent", "compression", "multichannel",
           "multichannel_persistent", "multichannel_persistent_encryption",
           "encryption", "compression_encryption", "multichannel_encryption",
           "dirlease", "dirlease_persistent", "dirlease_appinstance"]

# config -> CHIMERA_SMB_* env that the wrapper reads (drives daemon config +
# ptfconfig capability flips).  base sets nothing.
CONFIG_ENV = {
    "base":                    {},
    "persistent":              {"CHIMERA_SMB_PERSISTENT": "1"},
    "compression":             {"CHIMERA_SMB_COMPRESSION": "1"},
    "multichannel":            {"CHIMERA_SMB_MULTICHANNEL": "1"},
    "multichannel_persistent": {"CHIMERA_SMB_MULTICHANNEL": "1",
                                "CHIMERA_SMB_PERSISTENT": "1"},
    # Per-share encryption (not global encrypt-all): the encrypted share rejects
    # unencrypted access while the session stays cleartext, which the
    # AppInstanceId encryption cases require (the negatives only run when
    # IsGlobalEncryptDataEnabled is false).
    "multichannel_persistent_encryption": {"CHIMERA_SMB_MULTICHANNEL": "1",
                                           "CHIMERA_SMB_PERSISTENT": "1",
                                           "CHIMERA_SMB_PERSHARE_ENCRYPTION": "1"},
    "encryption":              {"CHIMERA_SMB_ENCRYPTION": "1"},
    "compression_encryption":  {"CHIMERA_SMB_COMPRESSION": "1",
                                "CHIMERA_SMB_ENCRYPTION": "1"},
    "multichannel_encryption": {"CHIMERA_SMB_MULTICHANNEL": "1",
                                "CHIMERA_SMB_ENCRYPTION": "1"},
    "dirlease":                {"CHIMERA_SMB_DIRLEASE": "1"},
    "dirlease_persistent":     {"CHIMERA_SMB_DIRLEASE": "1",
                                "CHIMERA_SMB_PERSISTENT": "1"},
    "dirlease_appinstance":    {"CHIMERA_SMB_DIRLEASE": "1",
                                "CHIMERA_SMB_PERSISTENT": "1",
                                "CHIMERA_SMB_MULTICHANNEL": "1"},
}

STATUSES = {"green", "xfail", "skip"}


# --------------------------------------------------------------------------
# CSV model
# --------------------------------------------------------------------------
def load_csv(path=CSV_PATH):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            r["enc_path"] = r["enc_path"].strip()
            rows.append(r)
    return rows


def lint(rows):
    """Return a list of invariant violations (empty == OK)."""
    errs = []
    seen = set()
    for r in rows:
        c = r["case"]
        if c in seen:
            errs.append(f"duplicate case: {c}")
        seen.add(c)
        if r["status"] not in STATUSES:
            errs.append(f"{c}: bad status {r['status']!r}")
        if r["home_config"] not in CONFIGS:
            errs.append(f"{c}: bad home_config {r['home_config']!r}")
        skip = r["status"] == "skip"
        has_reason = bool(r["skip_reason"].strip())
        if skip != has_reason:
            errs.append(f"{c}: skip<->skip_reason mismatch")
        if r["enc_path"] not in ("0", "1"):
            errs.append(f"{c}: bad enc_path {r['enc_path']!r}")
        if r["enc_path"] == "1" and r["home_config"] != "base":
            errs.append(f"{c}: enc_path=1 requires home_config=base")
    return errs


def derive(rows, statuses):
    """config -> sorted list of cases whose status is in `statuses` and whose
    home is that config, plus enc_path=base rows mirrored into encryption.
    This is the ONE derivation shared by the green-gate and the sweep."""
    out = {c: [] for c in CONFIGS}
    for r in rows:
        if r["status"] not in statuses:
            continue
        out[r["home_config"]].append(r["case"])
        if r["enc_path"] == "1" and r["home_config"] == "base":
            out["encryption"].append(r["case"])
    return {c: sorted(set(v)) for c, v in out.items()}


# --------------------------------------------------------------------------
# Running
# --------------------------------------------------------------------------
def parse_trx(path):
    """case -> outcome (Passed|Failed|NotExecuted)."""
    res = {}
    if not os.path.exists(path):
        return res
    for u in ET.parse(path).getroot().iter(TRX_NS + "UnitTestResult"):
        res[u.get("testName")] = u.get("outcome")
    return res


def run_config(cfg, cases, binary, bin_dir, dotnet, results_root):
    """Run one config's case list via the wrapper; return path to its TRX dir."""
    outdir = os.path.join(results_root, cfg)
    os.makedirs(outdir, exist_ok=True)
    env = dict(os.environ)
    env["WPTS_BIN_DIR"] = bin_dir
    env["DOTNET"] = dotnet
    env["WPTS_RESULT_DIR"] = outdir
    env.update(CONFIG_ENV[cfg])
    flt = ",".join(cases)  # empty == run all (discover mode)
    with open(os.path.join(outdir, "wrapper.log"), "w") as log:
        p = subprocess.Popen([WRAPPER, binary, "memfs", flt],
                             env=env, stdout=log, stderr=subprocess.STDOUT)
    return p, outdir


def run_all(per_cfg, args):
    """Launch every non-empty config concurrently; return cfg -> TRX outcomes."""
    procs = {}
    for cfg in CONFIGS:
        if not per_cfg.get(cfg):
            continue
        p, outdir = run_config(cfg, per_cfg[cfg], args.binary, args.bin_dir,
                               args.dotnet, args.results_root)
        procs[cfg] = (p, outdir)
    outcomes = {}
    for cfg, (p, outdir) in procs.items():
        p.wait()
        outcomes[cfg] = parse_trx(os.path.join(outdir, "SMB2TestResult.trx"))
    return outcomes


# --------------------------------------------------------------------------
# Classification + report
# --------------------------------------------------------------------------
def classify(rows, outcomes):
    """Bucket every applicable run result against its CSV expectation."""
    by_case = {r["case"]: r for r in rows}
    b = defaultdict(list)   # bucket -> [(cfg, case)]
    # which (cfg, case) we actually ran
    for cfg, res in outcomes.items():
        for case, outcome in res.items():
            r = by_case.get(case)
            exp = r["status"] if r else "UNMAPPED"
            if exp == "green":
                if outcome == "Passed":
                    b["green-pass"].append((cfg, case))
                elif outcome == "NotExecuted":
                    b["feature-off"].append((cfg, case))   # mis-homed: CSV bug
                else:
                    b["green-FAIL"].append((cfg, case))     # REGRESSION
            elif exp == "xfail":
                if outcome == "Passed":
                    b["xfail-PASS"].append((cfg, case))     # promote candidate
                elif outcome == "NotExecuted":
                    b["feature-off"].append((cfg, case))
                else:
                    b["xfail-fail"].append((cfg, case))     # known defect
            else:
                b["unexpected"].append((cfg, case, exp, outcome))
    return b


def report(rows, buckets):
    skips = defaultdict(list)
    for r in rows:
        if r["status"] == "skip":
            skips[r["skip_reason"]].append(r["case"])

    def section(title, items, fmt=lambda x: x):
        print(f"\n## {title} ({len(items)})")
        for it in sorted(items):
            print("  " + fmt(it))

    print("# WPTS feature-aware sweep report")
    g = len(buckets["green-pass"])
    print(f"\ngreen-pass={g}  green-FAIL={len(buckets['green-FAIL'])}  "
          f"xfail-fail={len(buckets['xfail-fail'])}  "
          f"xfail-PASS={len(buckets['xfail-PASS'])}  "
          f"feature-off={len(buckets['feature-off'])}  "
          f"skip={sum(len(v) for v in skips.values())}")

    if buckets["green-FAIL"]:
        section("green-FAIL  ===  REGRESSIONS (must fix or reclassify)",
                buckets["green-FAIL"], lambda t: f"[{t[0]}] {t[1]}")
    if buckets["feature-off"]:
        section("feature-off  ===  MIS-HOMED (CSV bug: case not applicable in its home)",
                buckets["feature-off"], lambda t: f"[{t[0]}] {t[1]}")
    if buckets["xfail-PASS"]:
        section("xfail-PASS  ===  promote to green",
                buckets["xfail-PASS"], lambda t: f"[{t[0]}] {t[1]}")
    if buckets["xfail-fail"]:
        section("xfail-fail  ===  known defects (tracked)",
                buckets["xfail-fail"], lambda t: f"[{t[0]}] {t[1]}")
    if buckets["unexpected"]:
        section("unexpected", buckets["unexpected"], str)
    print("\n## skip-ledger (never-applicable today)")
    for reason in sorted(skips):
        print(f"  {reason} ({len(skips[reason])}): "
              + ", ".join(sorted(skips[reason])[:6])
              + (" ..." if len(skips[reason]) > 6 else ""))


def discover_report(rows, outcomes):
    """Reconcile an unfiltered run vs the CSV."""
    mapped = {r["case"]: r for r in rows}
    seen = set()
    new = []
    moved = []
    stale = []
    for cfg, res in outcomes.items():
        for case, outcome in res.items():
            seen.add(case)
            r = mapped.get(case)
            if r is None:
                new.append((cfg, case, outcome))
                continue
            if outcome == "Passed":
                if r["status"] == "skip":
                    stale.append((cfg, case))
                elif r["home_config"] != cfg and r["enc_path"] != "1":
                    moved.append((cfg, case, r["home_config"]))
    print("# WPTS discovery reconciliation")
    print(f"\nnew (unmapped) cases: {len(set(c for _, c, _ in new))}")
    for cfg, case, outcome in sorted(set(new)):
        print(f"  [{cfg}] {case} = {outcome}  (add a CSV row)")
    print(f"\nstale skips (now Passed): {len(set(c for _, c in stale))}")
    for cfg, case in sorted(set(stale)):
        print(f"  [{cfg}] {case}")
    print(f"\nmoved (Passed outside home): {len(moved)}")
    for cfg, case, home in sorted(set(moved)):
        print(f"  [{cfg}] {case}  (home={home})")
    missing = [r["case"] for r in rows if r["case"] not in seen and r["status"] != "skip"]
    print(f"\nmapped non-skip cases never seen in any TRX: {len(missing)}")
    for c in sorted(missing):
        print(f"  {c}")


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lint", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--green-only", action="store_true")
    ap.add_argument("--discover", action="store_true")
    ap.add_argument("--csv", default=CSV_PATH)
    ap.add_argument("--binary", default=os.environ.get(
        "BINARY", os.path.join(REPO, "build/Release/src/daemon/chimera")))
    ap.add_argument("--bin-dir", default=os.environ.get("WPTS_BIN_DIR", "/opt/wpts/Bin"))
    ap.add_argument("--dotnet", default=os.environ.get("DOTNET", "/opt/dotnet/dotnet"))
    ap.add_argument("--results-root", default=os.path.join(REPO, "build/wpts_sweep"))
    args = ap.parse_args()

    rows = load_csv(args.csv)
    errs = lint(rows)
    if errs:
        print("CSV lint FAILED:", file=sys.stderr)
        for e in errs:
            print("  " + e, file=sys.stderr)
        return 2
    if args.lint:
        print(f"CSV OK: {len(rows)} cases "
              f"({sum(r['status']=='green' for r in rows)} green, "
              f"{sum(r['status']=='xfail' for r in rows)} xfail, "
              f"{sum(r['status']=='skip' for r in rows)} skip)")
        return 0

    if args.discover:
        outcomes = run_all({c: [""] for c in CONFIGS}, args)  # "" filter == run all
        discover_report(rows, outcomes)
        return 0

    statuses = {"green"} if args.green_only else {"green", "xfail"}
    per_cfg = derive(rows, statuses)
    outcomes = run_all(per_cfg, args)
    buckets = classify(rows, outcomes)
    report(rows, buckets)

    if args.check:
        bad = len(buckets["green-FAIL"]) + len(buckets["feature-off"])
        if bad:
            print(f"\nFAIL: {len(buckets['green-FAIL'])} regression(s), "
                  f"{len(buckets['feature-off'])} feature-off leak(s)", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
