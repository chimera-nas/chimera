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
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
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
    if [ -f "$CHIMERA_LOG" ]; then
        echo "=== Chimera stderr (last 100 lines) ==="
        tail -100 "$CHIMERA_LOG"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

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
        demofs_io_uring|demofs_aio)
            local device_type="io_uring"
            if [ "$BACKEND" = "demofs_aio" ]; then
                device_type="libaio"
            fi
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 256G "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="demofs"
            vfs_section="\"vfs\": {
                \"demofs\": {
                    \"config\": {\"devices\":[$devices_json]}
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

# Start chimera daemon in the netns (restore LD_PRELOAD for chimera only)
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" 2>"$CHIMERA_LOG" &
else
    ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" 2>"$CHIMERA_LOG" &
fi
CHIMERA_PID=$!

# Wait for NFS port to be ready
for i in $(seq 1 30); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.1
done

# Run pynfs tests based on NFS minor version
# Flags are passed as positional arguments to testserver.py
# --jsonout writes structured results for failure detection
# PYTHONPATH includes pynfs root for shared modules (rpc, xdr)
export PYTHONPATH="${PYNFS_DIR}:${PYTHONPATH:-}"

if [ "$NFS_MINOR_VERSION" = "0" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.0/testserver.py"
    # shellcheck disable=SC2086
    ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --maketree --force -v --json="${RESULTS_FILE}" $FLAG_ARGS
elif [ "$NFS_MINOR_VERSION" = "1" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"
    # shellcheck disable=SC2086
    ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --minorversion=1 --maketree --force -v --json="${RESULTS_FILE}" $FLAG_ARGS
elif [ "$NFS_MINOR_VERSION" = "2" ]; then
    TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"
    # shellcheck disable=SC2086
    ip netns exec "${NETNS_NAME}" python3 "${TESTSERVER}" 127.0.0.1:/share \
        --minorversion=2 --maketree --force -v --json="${RESULTS_FILE}" $FLAG_ARGS
else
    echo "Unsupported NFS minor version: $NFS_MINOR_VERSION"
    exit 1
fi
PYNFS_EXIT=$?

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
