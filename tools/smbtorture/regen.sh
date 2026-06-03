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
# Pre-requisites:
#   - smbtorture client present in PATH (e.g. apt: samba-testsuite)
#   - chimera built in build/Release (with smbtorture_test)
#   - run as root (the netns wrapper needs CAP_NET_ADMIN)
#
# Output:
#   - rewrites src/server/smb/tests/CMakeLists.txt in place
#   - leaves /tmp/smbtorture_results.txt for inspection

set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
RESULTS=/tmp/smbtorture_results.txt
SUBS=/tmp/smbtorture_subtests.txt
PAIRS=/tmp/smbtorture_pairs.bin
PARALLEL=${SMBTORTURE_PARALLEL:-12}

cd "$REPO"

if [ ! -x build/Release/src/server/smb/tests/smbtorture_test ]; then
    echo "error: build/Release/src/server/smb/tests/smbtorture_test not built" >&2
    echo "       run 'make release' first" >&2
    exit 1
fi
if ! command -v smbtorture >/dev/null; then
    echo "error: smbtorture client not in PATH (install samba-testsuite)" >&2
    exit 1
fi

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

# 2) Build (backend, subtest) pairs as null-delimited records.
> "$PAIRS"
for b in memfs linux io_uring cairn diskfs_io_uring diskfs_aio; do
    while IFS= read -r s; do
        printf '%s\0%s\0' "$b" "$s" >> "$PAIRS"
    done < "$SUBS"
done
echo "pairs: $(( $(stat -c%s "$PAIRS") )) bytes"

# 3) Per-pair runner: flock-serialized append to results.
RUNNER=$(mktemp /tmp/smbt-runner.XXXXXX.sh)
cat > "$RUNNER" <<'RUNNER_EOF'
#!/bin/bash
set -u
BACKEND="$1"; SUITE="$2"
LOG=$(mktemp /var/tmp/smbt.XXXXXX.log)
cd %REPO%
timeout --signal=KILL 300 \
    scripts/netns_test_wrapper.sh \
    build/Release/src/server/smb/tests/smbtorture_test \
    -b "$BACKEND" "$SUITE" > "$LOG" 2>&1
RC=$?
case $RC in
    0)   STATUS="PASS|$BACKEND|$SUITE";;
    77)  STATUS="SKIP|$BACKEND|$SUITE";;
    137) STATUS="TIME|$BACKEND|$SUITE";;
    *)   STATUS="FAIL|$BACKEND|$SUITE|rc=$RC";;
esac
{ flock -x 9; echo "$STATUS" >> %RESULTS%; } 9>%RESULTS%.lock
if [ "$RC" -ne 0 ] && [ "$RC" -ne 77 ]; then
    mv "$LOG" "/var/tmp/smbt-fail.${BACKEND}.${SUITE//[^A-Za-z0-9]/_}.log" 2>/dev/null || rm -f "$LOG"
else
    rm -f "$LOG"
fi
RUNNER_EOF
sed -i "s|%REPO%|$REPO|g; s|%RESULTS%|$RESULTS|g" "$RUNNER"
chmod +x "$RUNNER"

# 4) Drive the matrix.
rm -f "$RESULTS" "${RESULTS}.lock"
: > "$RESULTS"
echo "running $(( $(stat -c%s "$PAIRS") / 2 )) cells at parallel=$PARALLEL"
xargs -0 -P "$PARALLEL" -n 2 -a "$PAIRS" "$RUNNER"
rm -f "$RUNNER" "${RESULTS}.lock"

# 5) Regenerate the CMakeLists block.
python3 "$(dirname "$0")/regen.py" \
    --subs "$SUBS" \
    --results "$RESULTS" \
    --cmake "$REPO/src/server/smb/tests/CMakeLists.txt"

echo "done; review with: git diff -- src/server/smb/tests/CMakeLists.txt"
