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
# server log reports hitting the named-streams-off gate.
#
# Runs against memfs only: it is the sole backend advertising
# CHIMERA_VFS_CAP_NAMED_STREAMS, so it is the only one where the gate that fires
# is unambiguously gate 1 ("feature off") rather than gate 2 ("backend cannot").
#
# Pre-requisites: same as regen.sh -- smbtorture in PATH, chimera built in
# build/Release, run as root (the netns wrapper needs CAP_NET_ADMIN).
#
# Output: prints the derived list, and diffs it against the checked-in file.
# Exits non-zero if the checked-in list is missing an entry, so CI can gate on
# it.  Does not rewrite the file: the list carries hand-written commentary
# (which subtests still fail for real reasons, and why), and clobbering that is
# worse than the drift it would fix.  Add new entries by hand.

set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
LIST=$REPO/src/server/smb/tests/smbtorture_ads_subtests.txt
WORK=${SMBTORTURE_ADS_WORK:-/tmp/smbtorture-ads}
PARALLEL=${SMBTORTURE_PARALLEL:-12}
DRIVER=build/Release/src/server/smb/tests/smbtorture_test

# The log line emitted by gate 1 in chimera_smb_create_start().
GATE1='named streams: disabled by config'

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
echo "catalog: $(wc -l < "$WORK/subtests.txt") subtests"

# 2) Sweep with the feature forced off, one subtest per server process so a hit
#    is attributable to exactly one subtest.  Keep every log, pass or fail: a
#    subtest can hit the gate on a path it does not assert on and still pass,
#    and that still means it needs the feature.
RUNNER=$(mktemp "$WORK/runner.XXXXXX.sh")
cat > "$RUNNER" <<'RUNNER_EOF'
#!/bin/bash
set -u
SUITE="$1"
cd %REPO%
SMBTORTURE_ADS_OFF=1 timeout --signal=KILL 300 \
    scripts/netns_test_wrapper.sh %DRIVER% -b memfs "$SUITE" \
    > "%WORK%/logs/${SUITE//[^A-Za-z0-9]/_}.log" 2>&1
exit 0
RUNNER_EOF
sed -i "s|%REPO%|$REPO|g; s|%WORK%|$WORK|g; s|%DRIVER%|$DRIVER|g" "$RUNNER"
chmod +x "$RUNNER"

echo "sweeping with SMBTORTURE_ADS_OFF=1 at parallel=$PARALLEL"
# One subtest per line, and -d '\n' so xargs honours that.  Eight catalog names
# contain spaces -- "smb2.ioctl.copy-chunk streams" and the smb2.charset /
# delete-on-close-perms display names -- and xargs' default blank-and-newline
# splitting would dispatch each of them as two nonexistent subtests, so the real
# name would never be swept and could never show up as missing -- leaving the
# sweep blind to exactly the kind of name it exists to find.  -d also turns off
# xargs' quote and backslash processing, which those same names would otherwise
# be subject to.
xargs -d '\n' -P "$PARALLEL" -n 1 -a "$WORK/subtests.txt" "$RUNNER"

# 3) Harvest. Map log file back to its subtest via the same name mangling.
python3 - "$WORK" "$LIST" <<'PY'
import os, re, sys

work, listfile = sys.argv[1], sys.argv[2]
GATE1 = "named streams: disabled by config"

subs = [l.strip() for l in open(f"{work}/subtests.txt") if l.strip()]
by_mangled = {re.sub(r"[^A-Za-z0-9]", "_", s): s for s in subs}

hits = set()
for fn in os.listdir(f"{work}/logs"):
    with open(f"{work}/logs/{fn}", errors="replace") as fp:
        if GATE1 in fp.read():
            name = by_mangled.get(fn[:-4])
            if name:
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

missing = sorted(h for h in hits if not covered(h))

print(f"\nsubtests hitting the named-streams-off gate: {len(hits)}")
for h in sorted(hits):
    print(f"  {'   ' if covered(h) else '+++'} {h}")

if missing:
    print(f"\n{len(missing)} subtest(s) MISSING from {listfile}:")
    for m in missing:
        print(f"    {m}")
    print("\nAdd them by hand (the file carries commentary worth keeping).")
    sys.exit(1)

print(f"\n{listfile} covers every subtest that hit the gate.")
PY
