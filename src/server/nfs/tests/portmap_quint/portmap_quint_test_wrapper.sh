#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: portmap_quint_test_wrapper.sh <chimera_binary> <test_dir>
#
# Model-based conformance test for chimera's portmap/rpcbind server.
# Starts a chimera daemon (memfs backend) in a private network namespace and
# replays the checked-in Quint model traces (<test_dir>/traces/*.itf.json)
# against it with portmap_quint_replay.py, diffing every RPC reply against
# the reply the model predicted.  See <test_dir>/README.md.
#
# Set CHIMERA_PORTMAP_QUINT_LIVE=1 to additionally generate and replay a
# fresh randomized trace with the quint CLI (skipped when quint/npx is not
# available).
#
# Exits 77 (ctest SKIP_RETURN_CODE) when the environment cannot run the
# test at all (not root / no netns support / no python3).

set -u

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with
# system binaries (ip, python3, etc.) which exit non-zero under ASAN.
# LD_PRELOAD is restored only for the chimera daemon.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1
TEST_DIR=$2

if [ "$(id -u)" != "0" ] || ! command -v ip > /dev/null 2>&1; then
    echo "SKIP: network namespace setup requires root and iproute2"
    exit 77
fi

if ! command -v python3 > /dev/null 2>&1; then
    echo "SKIP: python3 not available"
    exit 77
fi

NETNS_NAME="pmapq_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/portmap_quint_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
CHIMERA_PID=""
FAILED=0

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ "$FAILED" != "0" ] && [ -f "$CHIMERA_LOG" ]; then
        echo "=== Chimera log (last 100 lines) ==="
        tail -100 "$CHIMERA_LOG"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# The default lockmgr_port (32803) and NSM port (32765) must stay in effect:
# the model's service table mirrors them (see portmap.qnt).
cat > "$CONFIG_FILE" << 'EOF'
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 2,
        "delegation_threads": 2,
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "memfs",
            "path": "/"
        }
    },
    "exports": {
        "/share": {
            "path": "/share"
        }
    }
}
EOF

if ! ip netns add "${NETNS_NAME}" 2>/dev/null; then
    echo "SKIP: unable to create network namespace"
    exit 77
fi
ip netns exec "${NETNS_NAME}" ip link set lo up

# Start the chimera daemon in the netns (restore LD_PRELOAD for chimera only).
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
else
    ip netns exec "${NETNS_NAME}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
fi
CHIMERA_PID=$!

READY=0
for i in $(seq 1 500); do
    if grep -q "Server is ready." "$CHIMERA_LOG" 2>/dev/null &&
       ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/111" 2>/dev/null; then
        READY=1
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        FAILED=1
        exit 1
    fi
    sleep 0.02
done

if [ "$READY" != "1" ]; then
    echo "chimera portmap port never became ready"
    FAILED=1
    exit 1
fi

TRACES=("$TEST_DIR"/traces/*.itf.json)
if [ ! -f "${TRACES[0]}" ]; then
    echo "no checked-in traces found under $TEST_DIR/traces"
    FAILED=1
    exit 1
fi

# Optionally mint one fresh randomized trace straight from the model, so a
# developer run also exercises quint itself (CI relies on the checked-in
# traces alone and never needs quint).
if [ "${CHIMERA_PORTMAP_QUINT_LIVE:-0}" = "1" ]; then
    QUINT=()
    if command -v quint > /dev/null 2>&1; then
        QUINT=(quint)
    elif command -v npx > /dev/null 2>&1; then
        QUINT=(npx -y @informalsystems/quint)
    fi
    if [ "${#QUINT[@]}" -gt 0 ] &&
       "${QUINT[@]}" run "$TEST_DIR/portmap.qnt" --max-steps=50 \
           --out-itf="${SESSION_DIR}/live.itf.json" > "${SESSION_DIR}/quint.log" 2>&1; then
        TRACES+=("${SESSION_DIR}/live.itf.json")
        echo "generated fresh trace with quint ($(grep -om1 -- '--seed=0x[0-9a-f]*' "${SESSION_DIR}/quint.log" || echo 'seed unknown'))"
    else
        echo "quint unavailable or failed; replaying checked-in traces only"
    fi
fi

ip netns exec "${NETNS_NAME}" python3 "$TEST_DIR/portmap_quint_replay.py" \
    127.0.0.1 "${TRACES[@]}"
REPLAY_EXIT=$?

# A daemon crash during the replay is a failure even if the replies matched.
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    echo "=== Chimera daemon DIED during test (exit code: $?) ==="
    CHIMERA_PID=""
    FAILED=1
    exit 1
fi

if [ "$REPLAY_EXIT" -ne 0 ]; then
    FAILED=1
fi
exit "$REPLAY_EXIT"
