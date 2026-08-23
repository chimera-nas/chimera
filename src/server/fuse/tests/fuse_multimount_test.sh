#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Stands up a daemon exporting ONE memfs share through TWO FUSE mounts and
# runs a test binary against the pair.  Two mounts are two independent FUSE
# connections: distinct lock-owner namespaces (real fcntl conflicts in one
# process) and distinct kernel caches (invalidation observable from the
# sibling mount).  The 60-second attr/entry timeouts make the coherence
# assertions unambiguous: prompt visibility can only come from invalidation.

BIN=${1:?usage: fuse_multimount_test.sh <chimera_binary> <test_binary>}
TESTBIN=${2:?usage: fuse_multimount_test.sh <chimera_binary> <test_binary>}

set -u

SESSION=$(mktemp -d)
CFG="$SESSION/config.json"
LOG="$SESSION/daemon.log"
MNT_A="$SESSION/mnt_a"
MNT_B="$SESSION/mnt_b"
PID=""

cleanup() {
    umount -l "$MNT_A" 2> /dev/null
    umount -l "$MNT_B" 2> /dev/null
    [ -n "$PID" ] && kill -9 "$PID" 2> /dev/null
    rm -rf "$SESSION"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1"
    echo "--- daemon log ---"
    cat "$LOG"
    exit 1
}

mkdir -p "$MNT_A" "$MNT_B"

cat > "$CFG" <<EOF
{
    "server": {
        "fuse_enabled": true,
        "threads": 2,
        "metrics_port": 0
    },
    "filesystems": {
        "fs0": { "module": "memfs" }
    },
    "mounts": {
        "data": { "module": "memfs", "path": "fs0" }
    },
    "fuse_mounts": {
        "$MNT_A": { "path": "/data", "options": "attr_timeout_ms=60000,entry_timeout_ms=60000" },
        "$MNT_B": { "path": "/data", "options": "attr_timeout_ms=60000,entry_timeout_ms=60000" }
    }
}
EOF

"$BIN" -c "$CFG" > "$LOG" 2>&1 &
PID=$!

for _ in $(seq 1 100); do
    grep -q "Server is ready" "$LOG" 2> /dev/null && break
    kill -0 "$PID" 2> /dev/null || break
    sleep 0.1
done

grep -q "Server is ready" "$LOG" || fail "daemon did not start"

mountpoint -q "$MNT_A" || fail "$MNT_A is not a mountpoint"
mountpoint -q "$MNT_B" || fail "$MNT_B is not a mountpoint"

"$TESTBIN" "$MNT_A" "$MNT_B" || fail "$(basename "$TESTBIN") reported failures"

umount "$MNT_A" || fail "clean unmount of $MNT_A failed"
umount "$MNT_B" || fail "clean unmount of $MNT_B failed"

kill -TERM "$PID" 2> /dev/null

for _ in $(seq 1 100); do
    kill -0 "$PID" 2> /dev/null || break
    sleep 0.1
done

kill -0 "$PID" 2> /dev/null && fail "daemon did not shut down after SIGTERM"
PID=""

echo "PASS: $(basename "$TESTBIN") across two FUSE mounts of one share"
exit 0
