#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: pynfs_test_wrapper.sh <chimera_binary> <backend> <nfs_minor_version> <pynfs_dir> <flag_args...>
#
# Orchestrates a chimera NFS server + pynfs test suite in a network namespace.
# 1. Generates chimera config for the given backend
# 2. Creates a network namespace with loopback
# 3. Starts chimera daemon in the netns (127.0.0.1:2049)
# 4. Runs pynfs testserver.py with the given flags
# 5. Checks JSON results for failures
# 6. Captures exit code and cleans up

set -u

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with
# system binaries (ip, sysctl, etc.) which exit non-zero under ASAN.
# LD_PRELOAD is restored only for the chimera daemon.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
NFS_MINOR_VERSION=$1; shift
PYNFS_DIR=$1; shift
FLAG_ARGS="$*"

NETNS_NAME="pynfs_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/pynfs_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
RESULTS_FILE="${SESSION_DIR}/results.json"
CHIMERA_PID=""
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
CHIMERA_DIED_DURING_TEST=0
PYNFS_NFS4_LEASE_TIME="${PYNFS_NFS4_LEASE_TIME:-5}"
PYNFS_NFS4_GRACE_TIME="${PYNFS_NFS4_GRACE_TIME:-10}"

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
            CHIMERA_DIED_DURING_TEST=1
        fi
        kill "$CHIMERA_PID" 2>/dev/null || true
        for i in $(seq 1 30); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "=== Chimera shutdown hung, force killing ==="
            kill -9 "$CHIMERA_PID" 2>/dev/null || true
        fi
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ "$CHIMERA_DIED_DURING_TEST" = "1" ]; then
        echo "=== Chimera daemon DIED during test ==="
    fi
    if [ -f "$CHIMERA_LOG" ]; then
        echo "=== Chimera log (last 150 lines) ==="
        tail -150 "$CHIMERA_LOG"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# Enable NFSv4 protocol delegations only for delegation test runs, so the
# other suites exercise the default (delegations-off) behavior.
DELEG_ENABLE="false"
case " $FLAG_ARGS " in
    *" deleg"* | *" delegations"* | *DELEG* | *"delegations "* | *"deleg "*)
        DELEG_ENABLE="true"
        ;;
esac

# Generate chimera config based on backend
generate_config() {
    local mount_path="/"
    local vfs_section=""

    case "$BACKEND" in
        linux|io_uring)
            mount_path="$SESSION_DIR/data"
            mkdir -p "$SESSION_DIR/data"
            ;;
        memfs)
            mount_path="/"
            ;;
        diskfs_io_uring|diskfs_aio)
            local device_type="io_uring"
            if [ "$BACKEND" = "diskfs_aio" ]; then
                device_type="libaio"
            fi
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 1G "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="diskfs"
            vfs_section="\"vfs\": {
                \"diskfs\": {
                    \"config\": {\"initialize\":true,\"devices\":[$devices_json],\"unsafe_async\":true}
                }
            },"
            ;;
        cairn)
            mount_path="/"
            vfs_section="\"vfs\": {
                \"cairn\": {
                    \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"}
                }
            },"
            ;;
    esac

    cat > "$CONFIG_FILE" << EOF
{
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        "nfs4_delegations": $DELEG_ENABLE,
        "nfs4_lease_time": $PYNFS_NFS4_LEASE_TIME,
        "nfs4_grace_time": $PYNFS_NFS4_GRACE_TIME,
        $vfs_section
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "$BACKEND",
            "path": "$mount_path"
        }
    },
    "exports": {
        "/share": {
            "path": "/share"
        }
    }
}
EOF
}

generate_config

# Create network namespace
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

ulimit -l unlimited
sysctl -q -w fs.aio-max-nr=2097152 2>/dev/null || true

# Start chimera daemon in the netns (restore LD_PRELOAD for chimera only).
# Chimera writes log lines to stdout and ASAN/signal-handler output to stderr,
# so capture both into the same log file for post-mortem on crash.
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" >"$CHIMERA_LOG" 2>&1 &
else
    ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" >"$CHIMERA_LOG" 2>&1 &
fi
CHIMERA_PID=$!

# Wait for the daemon to finish startup and for NFS to accept connections.
# On slower arm64 runners, the NFS socket can briefly accept before the server
# has finished its own readiness path; starting pynfs in that window produces
# connection-refused initialization failures.
READY=0
for i in $(seq 1 100); do
    if grep -q "Server is ready." "$CHIMERA_LOG" &&
       ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
        READY=1
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.1
done

if [ "$READY" != "1" ]; then
    echo "chimera NFS port never became ready"
    exit 1
fi

# Run pynfs tests based on NFS minor version
# Flags are passed as positional arguments to testserver.py
# --jsonout writes structured results for failure detection
# PYTHONPATH includes pynfs root for shared modules (rpc, xdr)
export PYTHONPATH="${PYNFS_DIR}:${PYTHONPATH:-}"

# Hard wall-clock timeout per ctest entry. Healthy flag groups complete in
# under a second; anything past this is hung. `timeout` returns 124 when it
# fires, which we surface explicitly below.
PYNFS_TIMEOUT="${PYNFS_TIMEOUT:-60}"
PYNFS_DEP_ARGS=(--force)
if [ "${PYNFS_RUNDEPS:-0}" = "1" ]; then
    PYNFS_DEP_ARGS=(--rundeps --force)
fi

if [ "$NFS_MINOR_VERSION" = "0" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.0/testserver.py"
    # shellcheck disable=SC2086
    timeout --foreground -k 5 "${PYNFS_TIMEOUT}" \
        ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --maketree "${PYNFS_DEP_ARGS[@]}" -v --json="${RESULTS_FILE}" $FLAG_ARGS
elif [ "$NFS_MINOR_VERSION" = "1" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"
    # shellcheck disable=SC2086
    timeout --foreground -k 5 "${PYNFS_TIMEOUT}" \
        ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --minorversion=1 --maketree "${PYNFS_DEP_ARGS[@]}" -v --json="${RESULTS_FILE}" $FLAG_ARGS
elif [ "$NFS_MINOR_VERSION" = "2" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"
    # shellcheck disable=SC2086
    timeout --foreground -k 5 "${PYNFS_TIMEOUT}" \
        ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --minorversion=2 --maketree "${PYNFS_DEP_ARGS[@]}" -v --json="${RESULTS_FILE}" $FLAG_ARGS
else
    echo "Unsupported NFS minor version: $NFS_MINOR_VERSION"
    exit 1
fi
PYNFS_EXIT=$?

if [ "$PYNFS_EXIT" -eq 124 ] || [ "$PYNFS_EXIT" -eq 137 ]; then
    echo "=== PyNFS wall-clock timeout (${PYNFS_TIMEOUT}s) — killed ==="
fi

# Check if chimera is still alive after tests
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    CHIMERA_EXIT=$?
    echo "=== Chimera daemon DIED during test (exit code: $CHIMERA_EXIT) ==="
    CHIMERA_PID=""
fi

# If pynfs itself failed (crash, connection error, etc.), propagate that
if [ "$PYNFS_EXIT" -ne 0 ]; then
    exit "$PYNFS_EXIT"
fi

# Check JSON results for test failures
if [ -f "$RESULTS_FILE" ]; then
    FAILURES=$(jq '.failures' "$RESULTS_FILE" 2>/dev/null)
    if [ "$FAILURES" != "0" ] && [ -n "$FAILURES" ]; then
        echo "=== PyNFS reported $FAILURES test failure(s) ==="
        exit 1
    fi
else
    echo "=== PyNFS results file not found ==="
    exit 1
fi

exit 0
