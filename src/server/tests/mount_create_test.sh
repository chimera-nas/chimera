#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Exercises the per-mount "create" option: the daemon creates a not-yet-existing
# backend directory path (and any intermediate directories) before mounting it.
#
# memfs initializes empty, so mounting /a/b/c/d only succeeds if mkpath created
# the path; the second mount of /a/b/e additionally exercises descending into
# the already-created /a/b.  Both create-mounts are exported, so the daemon only
# reaches "Server is ready" with both exports registered if the paths were
# created and mounted successfully.

set -u

BIN=${1:?usage: mount_create_test.sh <chimera_binary>}

SESSION=$(mktemp -d)
CFG="$SESSION/config.json"
LOG="$SESSION/daemon.log"
PID=""

cleanup() {
    [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null
    rm -rf "$SESSION"
}
trap cleanup EXIT

cat > "$CFG" <<'EOF'
{
    "server": {
        "threads": 2,
        "nfs_port": 21049,
        "data_server": true,
        "external_portmap": true,
        "metrics_port": 0
    },
    "mounts": {
        "deep":    { "module": "memfs", "path": "/a/b/c/d", "create": { "mode": "0750" } },
        "sibling": { "module": "memfs", "path": "/a/b/e",   "create": true }
    },
    "exports": {
        "/deep":    { "path": "/deep" },
        "/sibling": { "path": "/sibling" }
    }
}
EOF

"$BIN" -c "$CFG" > "$LOG" 2>&1 &
PID=$!

for _ in $(seq 1 100); do
    grep -q "Server is ready" "$LOG" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

kill -TERM "$PID" 2>/dev/null

fail() {
    echo "FAIL: $1"
    echo "--- daemon log ---"
    cat "$LOG"
    exit 1
}

grep -q "Server is ready" "$LOG"              || fail "daemon did not start (create-mount failed?)"
grep -qi "Failed to create mount path" "$LOG" && fail "mount-path creation reported failure"
grep -q "Adding NFS export /deep" "$LOG"      || fail "/deep export missing (deep create-mount failed)"
grep -q "Adding NFS export /sibling" "$LOG"   || fail "/sibling export missing (sibling create-mount failed)"

echo "PASS: create-mount built /a/b/c/d and /a/b/e on an empty memfs backend"
exit 0
