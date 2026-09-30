#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: pynfs_cold_restart_test_wrapper.sh <chimera_binary> <pynfs_dir> <client.py>
#
# Cold restarts against a PERSISTENT KV store (server.kv_module = cairn,
# nfs4_drc = true).  Every other pynfs run uses memkv, where the recovery load
# is skipped outright, so this is the only client-visible coverage of:
#
#   1. the load finishing before the listeners start -- the first EXCHANGE_ID
#      after a restart is NFS4_OK, never NFS4ERR_DELAY (the client driver
#      raises on any other status, so a client started right after "Server is
#      ready." proves it);
#   2. a client that was alive at the crash reopening grace once (a fresh
#      client's OPEN is NFS4ERR_GRACE), and its record being purged when the
#      window closes on the deadline;
#   3. a client whose lease had lapsed before the crash being skipped and
#      purged at load, so the next boot skips grace entirely.
#
# Three boots of one daemon over one cairn store, in a network namespace.  The
# clients are driven by src/server/nfs/tests/nfs4_cold_restart_client.py on top
# of pynfs's nfs4.1 library, because the pynfs testserver destroys every client
# it creates at exit and so cannot leave a confirmed client behind.

set -u

SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
PYNFS_DIR=$1; shift
CLIENT_PY=$1; shift

# lease 4 s: the stale bound is 1.5 leases = 6 s and the refresh/heartbeat
# interval is 2 s.  grace 6 s: the in-grace probe has to get through netns
# exec, Python start-up and four round trips before the window closes, and a
# loaded CI runner can spend a couple of seconds on that; nothing else in the
# scenario depends on the grace length.
LEASE_S=4
GRACE_S=6

NETNS_NAME="pynfs_cold_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/pynfs_cold_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CAIRN_DIR="${SESSION_DIR}/cairn"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
CHIMERA_PID=""
CHIMERA_LOG=""
BOOT_N=0

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    for f in "${SESSION_DIR}"/chimera_*.log; do
        [ -f "$f" ] || continue
        echo "=== $(basename "$f") (last 60 lines) ==="
        tail -60 "$f"
    done
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*"
    exit 1
}

# $1 = cairn initialize flag (true on the first boot, false afterwards).
generate_config() {
    cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "nfs_enabled": true,
        "threads": 4,
        "delegation_threads": 4,
        "kv_module": "cairn",
        "nfs4_drc": true,
        "nfs4_lease_time": ${LEASE_S},
        "nfs4_grace_time": ${GRACE_S},
        "vfs": {
            "cairn": {
                "config": {"initialize":$1,"path":"${CAIRN_DIR}"}
            }
        },
        "external_portmap": false
    },
    "filesystems": {
        "fs0": { "module": "cairn" }
    },
    "mounts": {
        "share": { "module": "cairn", "path": "fs0" }
    },
    "exports": {
        "/share": { "path": "/share" }
    }
}
EOF
}

start_chimera() {
    BOOT_N=$((BOOT_N + 1))
    CHIMERA_LOG="${SESSION_DIR}/chimera_${BOOT_N}.log"
    if [ -n "${SAVED_LD_PRELOAD}" ]; then
        ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
            "$CHIMERA_BINARY" -c "$CONFIG_FILE" >"$CHIMERA_LOG" 2>&1 &
    else
        ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" \
            >"$CHIMERA_LOG" 2>&1 &
    fi
    CHIMERA_PID=$!
    for _ in $(seq 1 500); do
        if grep -q "Server is ready." "$CHIMERA_LOG" &&
           ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
            return 0
        fi
        kill -0 "$CHIMERA_PID" 2>/dev/null || fail "boot ${BOOT_N}: daemon exited during startup"
        sleep 0.02
    done
    fail "boot ${BOOT_N}: NFS port never became ready"
}

# A crash, not a shutdown: the client state must be abandoned, not torn down.
crash_chimera() {
    kill -9 "$CHIMERA_PID" 2>/dev/null || true
    wait "$CHIMERA_PID" 2>/dev/null || true
    CHIMERA_PID=""
}

# run_client <label> <client.py args...>: one NFSv4.1 client, see the driver.
run_client() {
    local label=$1; shift
    timeout --foreground -k 5 60 \
        ip netns exec "${NETNS_NAME}" env PYNFS_DIR="${PYNFS_DIR}" \
        python3 "${CLIENT_PY}" --host 127.0.0.1 --export share "$@"
    local rc=$?
    [ "$rc" -eq 0 ] || fail "client ${label} exited ${rc}"
    echo "ok: client ${label}"
}

# expect_log <regex>: the current boot's log must match.
expect_log() {
    grep -qE "$1" "$CHIMERA_LOG" || fail "boot ${BOOT_N}: log lacks /$1/"
}

export PYTHONPATH="${SCRIPT_DIR}/pynfs_pycompat:${PYNFS_DIR}:${PYTHONPATH:-}"

ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ulimit -l unlimited
mkdir -p "$CAIRN_DIR"

# --- boot 1: fresh store; a client confirms itself and opens a file, then the
# server crashes under it -------------------------------------------------
generate_config true
start_chimera
expect_log "cold-start load complete: 0 client record\(s\) reloaded, 0 stale record\(s\) purged, grace skipped"
run_client boot1 --name cold_boot1 --keep
crash_chimera

# --- boot 2: the record from boot 1 is reloaded (its client was alive at the
# crash), the load finishes before the listener starts, and the window closes
# on the deadline and drops the record nobody reclaimed ---------------------
generate_config false
start_chimera
expect_log "cold-start load complete: [1-9][0-9]* client record\(s\) reloaded, 0 stale record\(s\) purged, grace active"
LOAD_LINE=$(grep -nE "cold-start load complete:" "$CHIMERA_LOG" | head -1 | cut -d: -f1)
READY_LINE=$(grep -n "Server is ready." "$CHIMERA_LOG" | head -1 | cut -d: -f1)
[ "$LOAD_LINE" -lt "$READY_LINE" ] || fail "boot 2: load (line $LOAD_LINE) did not complete before readiness (line $READY_LINE)"
# Straight into the grace window: EXCHANGE_ID must be NFS4_OK, not DELAY, and
# a fresh client's OPEN is refused NFS4ERR_GRACE while boot 1's client is owed
# its reclaim.
run_client boot2_in_grace --name cold_boot2_probe --expect-open grace --keep
sleep $((GRACE_S + 1))
# Only boot 1's client was owed a reclaim and it never came back.
expect_log "grace window closed \(deadline\): 1 unreclaimed client record\(s\) purged"
run_client boot2_after_grace --name cold_boot2_open --keep
# Let both boot-2 clients' leases lapse (courtesy keeps their state in memory,
# but their records are now provably stale) while the heartbeat keeps running.
sleep $(( 3 * LEASE_S ))
crash_chimera

# --- boot 3: nothing alive at the crash -> stale records purged, no grace ---
generate_config false
start_chimera
expect_log "cold-start load complete: 0 client record\(s\) reloaded, 2 stale record\(s\) purged, grace skipped"
run_client boot3 --name cold_boot3
crash_chimera

echo "PASS: pynfs cold restart"
exit 0
