#!/bin/bash
# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Re-discover the smbtorture smb2 subtest catalog and re-run the per-backend
# matrix, then regenerate the SMBTORTURE_SUITES + SMBTORTURE_ENABLED_<backend>
# blocks in src/server/smb/tests/CMakeLists.txt.
#
# Run after upgrading the Samba devcontainer image (which changes the subtest
# catalog) or after adding a new backend to the smbtorture_test driver.
#
# With --report-only the matrix runs but the splice step is skipped, so the run
# is a pure measurement: results.txt plus one log per failing cell, retained in
# --out-dir for inspection or for `regen.py compare`.  Use --backends/--filter to
# narrow a targeted investigation to a subset of the ~3900 cells.
#
# Pre-requisites:
#   - smbtorture client present in PATH (e.g. apt: samba-testsuite)
#   - chimera built with smbtorture_test (see --build-dir)
#   - run as root (the netns wrapper needs CAP_NET_ADMIN)
#
# Output:
#   - <out-dir>/results.txt              PASS|SKIP|TIME|FAIL cell classifications
#   - <out-dir>/subtests.txt             the full discovered catalog
#   - <out-dir>/measured-catalog.txt     the subset this run measured (== the
#                                        full catalog unless --filter narrowed
#                                        it); pass to `regen.py compare --subs`
#   - <out-dir>/smbt-fail.<b>.<s>.log    one log per non-passing cell
#   - src/server/smb/tests/CMakeLists.txt rewritten in place (unless
#     --report-only)
#
# The out-dir is reusable: everything this script generates in it is removed at
# startup, so a later run can never be read against a leftover catalog or a
# leftover failure log from an earlier, differently-scoped one.

set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
ALL_BACKENDS="memfs linux io_uring cairn diskfs_io_uring diskfs_aio"

BUILD_DIR=${SMBTORTURE_BUILD_DIR:-}
OUT_DIR=${SMBTORTURE_OUT_DIR:-/tmp}
PARALLEL=${SMBTORTURE_PARALLEL:-12}
# Env fallbacks so a caller that cannot easily append flags -- a CI job passing
# `docker run -e` -- can scope a run without interpolating anything into a shell
# command line.  Unset or empty means "the whole matrix".
BACKENDS=${SMBTORTURE_BACKENDS:-$ALL_BACKENDS}
FILTER=${SMBTORTURE_FILTER:-}
REPORT_ONLY=0

usage()
{
    cat <<'USAGE'
usage: regen.sh [options]

  --report-only        run the matrix and stop; do not splice CMakeLists.txt
  --out-dir <dir>      where results.txt and per-failure logs land (default /tmp)
                       ($SMBTORTURE_OUT_DIR)
  --backends <list>    comma/space separated subset of backends to run
                       ($SMBTORTURE_BACKENDS)
  --filter <regex>     only run catalog subtests matching this extended regex
                       ($SMBTORTURE_FILTER)
  --build-dir <dir>    build tree holding src/server/smb/tests/smbtorture_test
                       (default: first of $SMBTORTURE_BUILD_DIR, ./build/Release,
                       /build/Release that exists)
  --parallel <n>       concurrent cells (default 12, $SMBTORTURE_PARALLEL)
  -h, --help           this message

Known backends: memfs linux io_uring cairn diskfs_io_uring diskfs_aio
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --report-only) REPORT_ONLY=1; shift;;
        --out-dir)     OUT_DIR=$2; shift 2;;
        --backends)    BACKENDS=${2//,/ }; shift 2;;
        --filter)      FILTER=$2; shift 2;;
        --build-dir)   BUILD_DIR=$2; shift 2;;
        --parallel)    PARALLEL=$2; shift 2;;
        -h|--help)     usage; exit 0;;
        *)             echo "error: unknown option '$1'" >&2; usage >&2; exit 2;;
    esac
done

# Normalize commas once every input source has been consumed.  --backends did it
# on the way in, but $SMBTORTURE_BACKENDS handed its text straight to the
# validation below, where "memfs,linux" is a single unknown backend.
BACKENDS=${BACKENDS//,/ }

# Canonical form for a backend set, so "is this the whole matrix" is a question
# about the set and not about the order it happened to be typed in.
canon_backends()
{
    # $1 is deliberately unquoted: the word split is what turns the list into
    # one entry per line for sort.
    printf '%s\n' $1 | sort -u | tr '\n' ' '
}

for b in $BACKENDS; do
    case " $ALL_BACKENDS " in
        *" $b "*) ;;
        *) echo "error: unknown backend '$b' (known: $ALL_BACKENDS)" >&2; exit 2;;
    esac
done

# xargs reads -P 0 as "unbounded", which across ~3900 cells is ~3900 concurrent
# daemon+netns+client invocations rather than a faster run.
case $PARALLEL in
    ''|*[!0-9]*)
        echo "error: --parallel wants a positive integer (got '$PARALLEL')" >&2
        exit 2;;
esac
if [ "$PARALLEL" -lt 1 ]; then
    echo "error: --parallel wants a positive integer (got '$PARALLEL'); 0 tells" >&2
    echo "       xargs to run every cell at once" >&2
    exit 2
fi

# Reject a scoped splice up front rather than after the matrix: the run is the
# expensive part and the verdict does not depend on any of it.
if [ "$REPORT_ONLY" -eq 0 ] &&
   { [ -n "$FILTER" ] ||
     [ "$(canon_backends "$BACKENDS")" != "$(canon_backends "$ALL_BACKENDS")" ]; }; then
    echo "error: refusing to splice from a partial run (--backends/--filter)" >&2
    echo "       a partial matrix would drop every unmeasured subtest from the" >&2
    echo "       allowlists; re-run without them, or use --report-only" >&2
    exit 1
fi

cd "$REPO"

# Build trees live in ./build/Release for worktrees and /build/Release for the
# main tree, so there is no single relative path that works in both.
if [ -z "$BUILD_DIR" ]; then
    for d in build/Release /build/Release; do
        if [ -x "$d/src/server/smb/tests/smbtorture_test" ]; then
            BUILD_DIR=$d
            break
        fi
    done
fi
TEST_BIN=$BUILD_DIR/src/server/smb/tests/smbtorture_test
if [ -z "$BUILD_DIR" ] || [ ! -x "$TEST_BIN" ]; then
    echo "error: smbtorture_test not found (looked for $TEST_BIN)" >&2
    echo "       run 'make release' first, or pass --build-dir" >&2
    exit 1
fi
if ! command -v smbtorture >/dev/null; then
    echo "error: smbtorture client not in PATH (install samba-testsuite)" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
RESULTS=$OUT_DIR/results.txt
SUBS=$OUT_DIR/subtests.txt
MEASURED=$OUT_DIR/measured-catalog.txt
PAIRS=$OUT_DIR/pairs.bin

echo "build dir: $BUILD_DIR"
echo "out dir:   $OUT_DIR"

# Clear what a previous run left here before writing any of it.  The default
# out-dir is /tmp and the documented workflow retains the directory, so stale
# state is the normal case, not the exotic one: a measured-catalog.txt from an
# earlier --filter run would silently become this run's denominator, and old
# failure logs would sit in the artifact looking like this run's failures.
# smbt.*.log and smbt-runner.*.sh are the temporaries a killed run leaves.
# subtests.filtered.txt is the pre-measured-catalog name for the same thing;
# nothing reads it any more, so clear it rather than leave a file that looks
# authoritative sitting in an out-dir that predates the rename.
rm -f "$SUBS" "$MEASURED" "$RESULTS" "${RESULTS}.lock" "$PAIRS" \
    "$OUT_DIR"/subtests.filtered.txt \
    "$OUT_DIR"/smbt-fail.*.log "$OUT_DIR"/smbt.*.log "$OUT_DIR"/smbt-runner.*.sh

# 1) Discover the catalog.
smbtorture --list 2>&1 | grep -E '^smb2\.' \
    | python3 -c '
import sys
seen = set()
for line in sys.stdin:
    a = line.strip().rsplit(".",1)[0]   # drop the trailing display-name dup
    if a:
        seen.add(a)
for a in sorted(seen):
    print(a)
' > "$SUBS"
echo "catalog: $(wc -l < "$SUBS") subtests"

# The filter narrows only what this run measures; the catalog written to $SUBS
# stays complete so a spliced block never drops the unmeasured subtests.  What
# the run did measure is always written to $MEASURED, filtered or not, so a
# consumer never has to infer the scope from which files happen to exist.
if [ -n "$FILTER" ]; then
    # Distinguish grep's "no lines selected" (1) from "bad regex" (2): both used
    # to surface as "filter matched no subtests", which sends you looking for a
    # catalog that does not contain what you asked for instead of at your regex.
    grep -E "$FILTER" "$SUBS" > "$MEASURED" && grc=0 || grc=$?
    if [ "$grc" -gt 1 ]; then
        echo "error: --filter '$FILTER' is not a valid extended regex" >&2
        exit 2
    fi
    echo "filter '$FILTER': $(wc -l < "$MEASURED") subtests match"
    if [ ! -s "$MEASURED" ]; then
        echo "error: filter matched no subtests" >&2
        exit 1
    fi
else
    cp "$SUBS" "$MEASURED"
fi
RUN_SUBS=$MEASURED

# 2) Build (backend, subtest) pairs as null-delimited records.
> "$PAIRS"
NCELLS=0
for b in $BACKENDS; do
    while IFS= read -r s; do
        printf '%s\0%s\0' "$b" "$s" >> "$PAIRS"
        NCELLS=$((NCELLS + 1))
    done < "$RUN_SUBS"
done
echo "cells: $NCELLS ($(echo "$BACKENDS" | wc -w) backends)"

# 3) Per-pair runner: flock-serialized append to results.
RUNNER=$(mktemp "$OUT_DIR/smbt-runner.XXXXXX.sh")
cat > "$RUNNER" <<'RUNNER_EOF'
#!/bin/bash
# Paths arrive as exported SMBT_* variables rather than being substituted into
# this text.  A repository path containing "&" or the sed delimiter would
# otherwise corrupt every cell of the run, and the failure would look like a
# test failure rather than a quoting bug.
set -u
BACKEND="$1"; SUITE="$2"
LOG=$(mktemp "$SMBT_OUTDIR/smbt.XXXXXX.log")
# mktemp creates 0600 regardless of umask, and this runs as root inside the
# container while the out-dir is a bind mount the CI runner reads as an
# unprivileged user.  Without this the kept failure logs are unreadable outside
# the container and artifact upload dies with EACCES.  Done before the run, not
# after the mv, so the temporaries an aborted matrix leaves behind are readable
# too -- upload happens even when the matrix never finished.
chmod 644 "$LOG"
cd "$SMBT_REPO"
# Most cells finish in seconds.  smb2.maxfid opens 65520 files and then closes
# them all, which cairn takes about 341s to do -- inside the budget on every
# other backend, and just over it on that one, so it flapped as a regression
# while smbtorture itself reported success.  Give that one subtest room rather
# than raising the default: four subtests (hold-oplock, hold-sharemode,
# lease.breaking4, aio_delay.aio_cancel) block on purpose and time out on every
# backend by design, and a larger default would only make them wait longer.
case "$SUITE" in
    smb2.maxfid) CELL_TIMEOUT=600;;
    *)           CELL_TIMEOUT=300;;
esac

timeout --signal=KILL "$CELL_TIMEOUT" \
    scripts/netns_test_wrapper.sh \
    "$SMBT_TESTBIN" \
    -b "$BACKEND" "$SUITE" > "$LOG" 2>&1
RC=$?
# 124 is what GNU timeout reports when the command timed out; 137 is the child
# seen as killed by the SIGKILL it was sent.  Which of the two surfaces depends
# on the coreutils version and on what the wrapper does with the signal, and
# only 137 was handled -- so in practice every timeout was landing in the FAIL
# arm and being reported as a functional failure.  Of the 25 timeouts in the
# 2026-08-30 nightly, not one was classified TIME.
case $RC in
    0)        STATUS="PASS|$BACKEND|$SUITE";;
    77)       STATUS="SKIP|$BACKEND|$SUITE";;
    124|137)  STATUS="TIME|$BACKEND|$SUITE";;
    *)        STATUS="FAIL|$BACKEND|$SUITE|rc=$RC";;
esac
{ flock -x 9; echo "$STATUS" >> "$SMBT_RESULTS"; } 9>"$SMBT_RESULTS.lock"
if [ "$RC" -ne 0 ] && [ "$RC" -ne 77 ]; then
    mv "$LOG" "$SMBT_OUTDIR/smbt-fail.${BACKEND}.${SUITE//[^A-Za-z0-9]/_}.log" 2>/dev/null || rm -f "$LOG"
else
    rm -f "$LOG"
fi
# A cell's outcome belongs in results.txt, never in this script's exit status:
# xargs returns 123 if any invocation exits non-zero, and the caller's `set -e`
# would discard a multi-hour matrix over one unlink.
exit 0
RUNNER_EOF
# 755 rather than +x for the same reason the per-cell log is 644: mktemp made it
# 0600, and a matrix that dies before the cleanup below leaves it in an out-dir
# that CI uploads as an unprivileged user.
chmod 755 "$RUNNER"
export SMBT_REPO=$REPO
export SMBT_RESULTS=$RESULTS
export SMBT_OUTDIR=$OUT_DIR
export SMBT_TESTBIN=$TEST_BIN

# 4) Drive the matrix.
rm -f "$RESULTS" "${RESULTS}.lock"
: > "$RESULTS"
echo "running $NCELLS cells at parallel=$PARALLEL"
xargs -0 -P "$PARALLEL" -n 2 -a "$PAIRS" "$RUNNER"
rm -f "$RUNNER" "${RESULTS}.lock" "$PAIRS"

if [ "$REPORT_ONLY" -eq 1 ]; then
    echo "report-only: CMakeLists.txt untouched"
    echo "results: $RESULTS"
    # --subs keeps the comparison honest about its denominator: without it a
    # scoped run is diffed against the whole checked-in catalog.
    echo "compare with: python3 tools/smbtorture/regen.py compare \\"
    echo "                  --results $RESULTS \\"
    echo "                  --subs $MEASURED \\"
    echo "                  --cmake src/server/smb/tests/CMakeLists.txt"
    exit 0
fi

# 5) Regenerate the CMakeLists block.  A scoped run was rejected before the
#    matrix ran, so what reached here measured the whole catalog.
python3 "$(dirname "$0")/regen.py" generate \
    --subs "$SUBS" \
    --results "$RESULTS" \
    --cmake "$REPO/src/server/smb/tests/CMakeLists.txt"

echo "done; review with: git diff -- src/server/smb/tests/CMakeLists.txt"
