#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
#
# Wrapper script to run FSX against different Chimera backends
# Generates the JSON config and runs fsx with appropriate settings

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-/build/Debug}"
FSX_BINARY="${BUILD_DIR}/src/posix/tests/posix_fsx"

# Default values
BACKEND="memfs"
NUM_OPS=1000
MAX_FILE_LEN=262144
SEED=$RANDOM
QUIET=0
EXTRA_ARGS=""

usage() {
    echo "Usage: $0 -b <backend> [options]"
    echo ""
    echo "Direct backends:    memfs, demofs, cairn, linux, io_uring"
    echo "NFS3 backends:      nfs3_memfs, nfs3_demofs, nfs3_cairn, nfs3_linux, nfs3_io_uring"
    echo "NFS3 RDMA backends: nfs3rdma_memfs, nfs3rdma_demofs, nfs3rdma_cairn, nfs3rdma_linux, nfs3rdma_io_uring"
    echo ""
    echo "Options:"
    echo "  -b <backend>   VFS backend to use (required)"
    echo "  -N <numops>    Number of operations (default: $NUM_OPS)"
    echo "  -l <len>       Max file length (default: $MAX_FILE_LEN)"
    echo "  -S <seed>      Random seed (default: random)"
    echo "  -q             Quiet mode"
    echo "  -h             Show this help"
    echo ""
    echo "Any additional arguments are passed directly to fsx."
    exit 1
}

while getopts "b:N:l:S:qh" opt; do
    case $opt in
        b) BACKEND="$OPTARG" ;;
        N) NUM_OPS="$OPTARG" ;;
        l) MAX_FILE_LEN="$OPTARG" ;;
        S) SEED="$OPTARG" ;;
        q) QUIET=1 ;;
        h) usage ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

# Remaining arguments passed to fsx
EXTRA_ARGS="$@"

# Validate backend and check if NFS
IS_NFS=0
case "$BACKEND" in
    memfs|demofs|cairn|linux|io_uring) ;;
    nfs3_memfs|nfs3_demofs|nfs3_cairn|nfs3_linux|nfs3_io_uring) IS_NFS=1 ;;
    nfs3rdma_memfs|nfs3rdma_demofs|nfs3rdma_cairn|nfs3rdma_linux|nfs3rdma_io_uring) IS_NFS=1 ;;
    *) echo "Error: Unknown backend '$BACKEND'"; usage ;;
esac

# Create session directory
SESSION_DIR="/build/test/fsx_session_$$_$(date +%s)"
mkdir -p "$SESSION_DIR"
CONFIG_FILE="${SESSION_DIR}/fsx_config.json"
TEST_FILE="/fsx/testfile"

cleanup() {
    if [ -d "$SESSION_DIR" ]; then
        rm -rf "$SESSION_DIR"
    fi
}
trap cleanup EXIT

# Generate config based on backend
generate_config() {
    local mount_path="/"
    local modules_section=""

    case "$BACKEND" in
        linux|io_uring)
            # linux and io_uring need the session dir as mount path
            mount_path="$SESSION_DIR"
            mkdir -p "$SESSION_DIR/fsx"
            ;;
        memfs)
            # memfs uses "/" as mount path, no special config needed
            mount_path="/"
            ;;
        demofs)
            # Create demofs devices and build inline config
            DEVICES_JSON=""
            for i in $(seq 0 9); do
                DEVICE_PATH="${SESSION_DIR}/device-${i}.img"
                truncate -s 256G "$DEVICE_PATH"
                if [ $i -gt 0 ]; then
                    DEVICES_JSON="${DEVICES_JSON},"
                fi
                DEVICES_JSON="${DEVICES_JSON}{\"type\":\"io_uring\",\"size\":1,\"path\":\"$DEVICE_PATH\"}"
            done
            mount_path="/"
            modules_section="\"modules\": {
        \"demofs\": {
            \"path\": \"/build/test/demofs\",
            \"config\": {\"devices\":[$DEVICES_JSON]}
        }
    },"
            ;;
        cairn)
            mount_path="/"
            modules_section="\"modules\": {
        \"cairn\": {
            \"path\": \"/build/test/cairn\",
            \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"}
        }
    },"
            ;;
    esac

    # Generate main config file
    # Note: modules section only needed for backends requiring config files
    cat > "$CONFIG_FILE" << EOF
{
    $modules_section
    "mounts": {
        "/fsx": {
            "module": "$BACKEND",
            "path": "$mount_path"
        }
    }
}
EOF
}

# Build fsx arguments
FSX_ARGS="-N $NUM_OPS"
FSX_ARGS="$FSX_ARGS -l $MAX_FILE_LEN"

if [ "$SEED" != "0" ]; then
    FSX_ARGS="$FSX_ARGS -S $SEED"
fi

if [ "$QUIET" = "1" ]; then
    FSX_ARGS="$FSX_ARGS -q"
fi

# Disable features not supported by Chimera POSIX API
# (mmap is already disabled in fsx.c, but we also disable fallocate variants
# which may not be supported by all backends)
FSX_ARGS="$FSX_ARGS -P $SESSION_DIR/"  # Put auxiliary files in session dir (local filesystem)
FSX_ARGS="$FSX_ARGS -R -W"  # Disable mapped reads/writes (already done internally, but be explicit)
FSX_ARGS="$FSX_ARGS -F"     # Disable fallocate
FSX_ARGS="$FSX_ARGS -H"     # Disable punch hole
FSX_ARGS="$FSX_ARGS -z"     # Disable zero range
FSX_ARGS="$FSX_ARGS -C"     # Disable collapse range
FSX_ARGS="$FSX_ARGS -I"     # Disable insert range
FSX_ARGS="$FSX_ARGS -J"     # Disable clone range
FSX_ARGS="$FSX_ARGS -B"     # Disable dedupe range
FSX_ARGS="$FSX_ARGS -E"     # Disable copy range
FSX_ARGS="$FSX_ARGS -d"

# Add extra args
if [ -n "$EXTRA_ARGS" ]; then
    FSX_ARGS="$FSX_ARGS $EXTRA_ARGS"
fi

if [ "$IS_NFS" = "1" ]; then
    # NFS backend: use --backend option and network namespace wrapper
    NETNS_WRAPPER="${SCRIPT_DIR}/../../../scripts/netns_test_wrapper.sh"
    FSX_ARGS="--backend=$BACKEND $FSX_ARGS"

    if [ "$QUIET" != "1" ]; then
        echo "Running FSX test (NFS mode):"
        echo "  Backend: $BACKEND"
        echo "  Operations: $NUM_OPS"
        echo "  Max file length: $MAX_FILE_LEN"
        echo "  Command: $NETNS_WRAPPER $FSX_BINARY $FSX_ARGS $TEST_FILE"
        echo ""
    fi

    # Run fsx in network namespace
    exec "$NETNS_WRAPPER" "$FSX_BINARY" $FSX_ARGS "$TEST_FILE"
else
    # Direct backend: use --chimera-config with generated config
    generate_config
    FSX_ARGS="--chimera-config=$CONFIG_FILE $FSX_ARGS"

    if [ "$QUIET" != "1" ]; then
        echo "Running FSX test:"
        echo "  Backend: $BACKEND"
        echo "  Config: $CONFIG_FILE"
        echo "  Operations: $NUM_OPS"
        echo "  Max file length: $MAX_FILE_LEN"
        echo "  Command: $FSX_BINARY $FSX_ARGS $TEST_FILE"
        echo ""
    fi

    # Run fsx
    exec "$FSX_BINARY" $FSX_ARGS "$TEST_FILE"
fi
