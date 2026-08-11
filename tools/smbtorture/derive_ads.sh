#!/bin/bash
# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Rediscover which smbtorture subtests need named-stream (ADS) support, and
# report any that are missing from src/server/smb/tests/smbtorture_ads_subtests.txt.
#
# Why this exists: that list used to be a hand-maintained strcmp chain in
# smbtorture_test.c, grown one entry at a time whenever somebody noticed a
# failure.  Eleven subtests were never noticed -- they open a stream as
# incidental setup (a second handle, an elaborate file to query, a directory
# reached via "::$INDEX_ALLOCATION") and died in that setup with
# NT_STATUS_OBJECT_NAME_INVALID, looking exactly like a server bug.
#
# The set cannot be derived statically: there is no Samba source in this tree,
# and smbtorture builds its paths at run time from format strings, so scanning
# the binary yields noise.  So derive it empirically -- sweep the catalog with
# the feature forced OFF (SMBTORTURE_ADS_OFF=1) and collect every subtest whose
# server log reports hitting the named-streams-off gate.  The gate logs at debug,
# so the sweep raises the driver's level with SMBTORTURE_LOG_LEVEL=debug.
#
# Runs against memfs only: it is the sole backend advertising
# CHIMERA_VFS_CAP_NAMED_STREAMS, so it is the only one where the gate that fires
# is unambiguously gate 1 ("feature off") rather than gate 2 ("backend cannot").
#
# Pre-requisites: same as regen.sh -- smbtorture in PATH, chimera built in
# build/Release, run as root (the netns wrapper needs CAP_NET_ADMIN).
#
# Output: prints the derived list, and diffs it against the checked-in file.
# Does not rewrite the file: the list carries hand-written commentary (which
# subtests still fail for real reasons, and why), and clobbering that is worse
# than the drift it would fix.  Add new entries by hand.
#
# Exit status, so CI can gate on it:
#   0  the checked-in list covers every subtest that hit the gate
#   1  the list is missing an entry (drift; the sweep itself was sound)
#   2  the sweep is untrustworthy and NO drift verdict was reached -- it observed
#      the gate zero times, or missed the subtests that unambiguously open a
#      stream, or some subtest timed out / was killed / produced no log.  A
#      verdict is only meaningful if the sweep actually ran, so this case must
#      never be reported as success.

set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
LIST=$REPO/src/server/smb/tests/smbtorture_ads_subtests.txt
WORK=${SMBTORTURE_ADS_WORK:-/tmp/smbtorture-ads}
PARALLEL=${SMBTORTURE_PARALLEL:-12}
DRIVER=build/Release/src/server/smb/tests/smbtorture_test

# The log line emitted by gate 1 in chimera_smb_create().  Threaded into both the
# sweep and the harvest below rather than duplicated -- a stale copy would make
# the sweep find nothing, which used to look exactly like success.
GATE1='named streams: disabled by config'

# Subtests that unambiguously open a named stream, used as a sanity floor: if the
# sweep does not see the gate fire for these, it is broken and its "nothing is
# missing" verdict is worthless.  Any of them may vanish from the catalog on a
# Samba upgrade, so the floor requires at least one, not all.
FLOOR='smb2.streams.'

# WORK is recursively deleted below, so refuse an obviously wrong override
# instead of eating someone's home directory.
case $WORK in
    /tmp/*) ;;
    *)
        echo "error: SMBTORTURE_ADS_WORK must be under /tmp (got '$WORK')" >&2
        exit 1
        ;;
esac

cd "$REPO"

if [ ! -x "$DRIVER" ]; then
    echo "error: $DRIVER not built; run 'make release' first" >&2
    exit 1
fi
if ! command -v smbtorture >/dev/null; then
    echo "error: smbtorture client not in PATH (install samba-testsuite)" >&2
    exit 1
fi

rm -rf "$WORK"
mkdir -p "$WORK/logs"

# 1) Catalog, same derivation as regen.sh.
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
' > "$WORK/subtests.txt"
CATALOG=$(wc -l < "$WORK/subtests.txt")
echo "catalog: $CATALOG subtests"
if [ "$CATALOG" -eq 0 ]; then
    echo "error: 'smbtorture --list' yielded no smb2.* subtests; nothing to sweep" >&2
    exit 1
fi

# 2) Sweep with the feature forced off, one subtest per server process so a hit
#    is attributable to exactly one subtest.  Sweep every subtest, pass or fail:
#    a subtest can hit the gate on a path it does not assert on and still pass,
#    and that still means it needs the feature.
mkdir -p "$WORK/full"
RUNNER=$(mktemp "$WORK/runner.XXXXXX.sh")
cat > "$RUNNER" <<'RUNNER_EOF'
#!/bin/bash
set -u
SUITE="$1"
SAFE="${SUITE//[^A-Za-z0-9]/_}"
FULL="%WORK%/full/$SAFE.out"
cd %REPO%
SMBTORTURE_LOG_LEVEL=debug SMBTORTURE_ADS_OFF=1 timeout --signal=KILL 300 \
    scripts/netns_test_wrapper.sh %DRIVER% -b memfs "$SUITE" \
    > "$FULL" 2>&1
rc=$?
# Distil rather than keep: the exit status, so a killed or crashed run is not
# silently read as "this subtest does not need ADS", plus the gate lines only.
# Debug-level logs for the whole catalog would otherwise run to many GB.
{
    echo "runner-exit=$rc"
    grep -F "%GATE1%" "$FULL" || true
} > "%WORK%/logs/$SAFE.log"
rm -f "$FULL"
exit 0
RUNNER_EOF
sed -i "s|%REPO%|$REPO|g; s|%WORK%|$WORK|g; s|%DRIVER%|$DRIVER|g; s|%GATE1%|$GATE1|g" \
    "$RUNNER"
chmod +x "$RUNNER"

echo "sweeping with SMBTORTURE_ADS_OFF=1 SMBTORTURE_LOG_LEVEL=debug at parallel=$PARALLEL"
# One subtest per line, and -d '\n' so xargs honours that.  Eight catalog names
# contain spaces -- "smb2.ioctl.copy-chunk streams" and the smb2.charset /
# delete-on-close-perms display names -- and xargs' default blank-and-newline
# splitting would dispatch each of them as two nonexistent subtests, so the real
# name would never be swept and could never show up as missing -- leaving the
# sweep blind to exactly the kind of name it exists to find.  -d also turns off
# xargs' quote and backslash processing, which those same names would otherwise
# be subject to.
xargs -d '\n' -P "$PARALLEL" -n 1 -a "$WORK/subtests.txt" "$RUNNER"
rmdir "$WORK/full" 2>/dev/null || true

# 3) Harvest. Map log file back to its subtest via the same name mangling.
python3 - "$WORK" "$LIST" "$GATE1" "$FLOOR" "$DRIVER" <<'PY'
import os, re, sys

work, listfile, gate1, floor, DRIVER = sys.argv[1:6]

subs = [l.strip() for l in open(f"{work}/subtests.txt") if l.strip()]
by_mangled = {re.sub(r"[^A-Za-z0-9]", "_", s): s for s in subs}

# A run that timed out, was killed, or skipped never got far enough for the
# absence of a gate line to mean anything.  Counting it as "does not need ADS"
# is how a broken sweep produces a clean bill of health.
INCONCLUSIVE_RC = {77, 124, 137}

hits, inconclusive, nolog = set(), [], []
for mangled, name in by_mangled.items():
    path = f"{work}/logs/{mangled}.log"
    if not os.path.exists(path):
        nolog.append(name)
        continue
    with open(path, errors="replace") as fp:
        body = fp.read()
    m = re.search(r"^runner-exit=(-?\d+)$", body, re.M)
    if m is None or int(m.group(1)) in INCONCLUSIVE_RC:
        inconclusive.append(name)
        continue
    if gate1 in body:
        hits.add(name)

# Checked-in list, with the same match semantics as ads_entry_matches() in
# smbtorture_test.c and _smbtorture_needs_ads() in CMakeLists.txt.
patterns = []
for line in open(listfile):
    line = line.split("#", 1)[0].strip()
    if line:
        patterns.append(line)

def covered(name):
    return any(name == p or name.startswith(p + ".") for p in patterns)

print(f"\nsubtests hitting the named-streams-off gate: {len(hits)}")
for h in sorted(hits):
    print(f"  {'   ' if covered(h) else '+++'} {h}")

# Sanity floor.  Everything below this point only means something if the sweep
# actually observed the gate; without these checks an empty harvest -- wrong log
# level, driver aborting at startup, netns wrapper broken, drifted log string --
# reports "covers every subtest" and exits 0.
fatal = []
if nolog:
    fatal.append(f"{len(nolog)} subtest(s) produced no log at all, e.g. "
                 + ", ".join(sorted(nolog)[:5]))
if inconclusive:
    fatal.append(f"{len(inconclusive)} subtest(s) timed out, were killed, or "
                 "skipped, so their result is unknown, e.g. "
                 + ", ".join(sorted(inconclusive)[:5]))
if not hits:
    fatal.append("the sweep observed the gate ZERO times; it tested nothing")
elif not any(h.startswith(floor) for h in hits):
    fatal.append(f"no '{floor}*' subtest hit the gate; those unambiguously open "
                 "a stream, so the sweep is not observing the gate correctly")

if fatal:
    print("\nSWEEP IS NOT TRUSTWORTHY -- not reporting drift:")
    for f in fatal:
        print(f"  ! {f}")
    print("\nFix the cause and re-run the sweep.  A single subtest can be checked\n"
          "by hand without a full sweep:\n"
          "  SMBTORTURE_LOG_LEVEL=debug SMBTORTURE_ADS_OFF=1 \\\n"
          "    scripts/netns_test_wrapper.sh " + DRIVER + " -b memfs SUBTEST \\\n"
          "    2>&1 | grep -F '" + gate1 + "'")
    sys.exit(2)

missing = sorted(h for h in hits if not covered(h))
if missing:
    print(f"\n{len(missing)} subtest(s) MISSING from {listfile}:")
    for m in missing:
        print(f"    {m}")
    print("\nAdd them by hand (the file carries commentary worth keeping).")
    sys.exit(1)

print(f"\n{listfile} covers every subtest that hit the gate.")
PY
