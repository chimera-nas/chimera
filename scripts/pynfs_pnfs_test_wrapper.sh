#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: pynfs_pnfs_test_wrapper.sh <chimera_binary> <pynfs_dir> <flag_args...>
#
# Runs the pynfs NFSv4.1 flex-files (pNFS) suite against a chimera pNFS setup:
#   1. A chimera *data server* (plain memfs, data_server mode) on 127.0.0.1:2050
#   2. A chimera *metadata server* (pNFS enabled) on 127.0.0.1:2049 that
#      nfs-mounts the data server and steers file data to it.
#   3. pynfs testserver.py (minorversion 1) in the netns, with the given flags.

set -u

SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
PYNFS_DIR=$1; shift
FLAG_ARGS="$*"

NETNS_NAME="pnfs_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/pnfs_session_XXXXXX")
DS_CONFIG="${SESSION_DIR}/ds.json"
MDS_CONFIG="${SESSION_DIR}/mds.json"
RESULTS_FILE="${SESSION_DIR}/results.json"
DS_LOG="${SESSION_DIR}/ds.log"
MDS_LOG="${SESSION_DIR}/mds.log"
DS_PID=""
MDS_PID=""

MDS_PORT=2049
DS_PORT=2050
DS_UADDR="127.0.0.1.8.2"   # RFC 5665 uaddr for 127.0.0.1:2050 (2050 = 8*256+2)
PYNFS_TIMEOUT="${PYNFS_TIMEOUT:-60}"
PYNFS_NFS4_LEASE_TIME="${PYNFS_NFS4_LEASE_TIME:-5}"
PYNFS_NFS4_GRACE_TIME="${PYNFS_NFS4_GRACE_TIME:-10}"

run_chimera() {
    local cfg="$1" log="$2"
    if [ -n "${SAVED_LD_PRELOAD}" ]; then
        ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
            "$CHIMERA_BINARY" -c "$cfg" >"$log" 2>&1 &
    else
        ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$cfg" >"$log" 2>&1 &
    fi
    echo $!
}

cleanup() {
    for pid in "$MDS_PID" "$DS_PID"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            for i in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    [ -f "$DS_LOG" ]  && { echo "=== Data server log (tail) ===";     tail -25 "$DS_LOG"; }
    [ -f "$MDS_LOG" ] && { echo "=== Metadata server log (tail) ==="; tail -40 "$MDS_LOG"; }
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

cat > "$DS_CONFIG" << EOF
{
    "server": { "threads": 2, "nfs_port": ${DS_PORT}, "data_server": true,
                "nfs4_lease_time": ${PYNFS_NFS4_LEASE_TIME},
                "nfs4_grace_time": ${PYNFS_NFS4_GRACE_TIME},
                "external_portmap": true, "metrics_port": 9001 },
    "mounts": { "ds_data": { "module": "memfs", "path": "/" } },
    "exports": { "/ds_export": { "path": "/ds_data" } }
}
EOF

cat > "$MDS_CONFIG" << EOF
{
    "server": {
        "threads": 4, "external_portmap": false,
        "nfs4_lease_time": ${PYNFS_NFS4_LEASE_TIME},
        "nfs4_grace_time": ${PYNFS_NFS4_GRACE_TIME},
        "pnfs": { "enabled": true,
                  "data_servers": [ { "netid": "tcp", "uaddr": "${DS_UADDR}", "backing_path": "/ds0" } ] }
    },
    "mounts": {
        "share": { "module": "memfs", "path": "/" },
        "ds0":   { "module": "nfs", "path": "127.0.0.1:/ds_export", "options": "vers=4,port=${DS_PORT}" }
    },
    "exports": { "/share": { "path": "/share" } }
}
EOF

wait_for_port() {
    local port="$1" pid="$2"
    for i in $(seq 1 50); do
        ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/${port}" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || { echo "chimera (pid $pid) exited early" >&2; return 1; }
        sleep 0.1
    done
    echo "timed out waiting for 127.0.0.1:${port}" >&2; return 1
}

ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

DS_PID=$(run_chimera "$DS_CONFIG" "$DS_LOG")
wait_for_port "$DS_PORT" "$DS_PID" || exit 1
echo "=== pNFS data server up on 127.0.0.1:${DS_PORT} ==="

MDS_PID=$(run_chimera "$MDS_CONFIG" "$MDS_LOG")
wait_for_port "$MDS_PORT" "$MDS_PID" || exit 1
grep -q "backing root resolved" "$MDS_LOG" || {
    echo "MDS did not resolve its data-server backing root:" >&2
    grep -iE "pNFS|backing|nfs|error" "$MDS_LOG" | tail -10 >&2; exit 1
}
echo "=== pNFS metadata server up on 127.0.0.1:${MDS_PORT} ==="

# PYTHONPATH includes pynfs root for shared modules (rpc, xdr).
export PYTHONPATH="${PYNFS_DIR}:${PYTHONPATH:-}"

TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"
timeout --foreground -k 5 "${PYNFS_TIMEOUT}" \
    ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
    --minorversion=1 --maketree --force -v --json="${RESULTS_FILE}" $FLAG_ARGS
RC=$?

# Propagate a pynfs crash / connection error directly.
if [ "$RC" -ne 0 ]; then
    echo "=== pynfs exited non-zero (${RC}) ==="
    exit "$RC"
fi

# Otherwise fail iff the JSON results report any test failures.
if [ -f "$RESULTS_FILE" ]; then
    FAILURES=$(jq '.failures' "$RESULTS_FILE" 2>/dev/null)
    if [ "$FAILURES" != "0" ] && [ -n "$FAILURES" ]; then
        echo "=== pynfs reported ${FAILURES} flex test failure(s) ==="
        exit 1
    fi
else
    echo "=== pynfs results file not found ==="
    exit 1
fi

exit 0
