#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <nfsver> <test_cmd>
#
# Orchestrates a chimera NFS server + QEMU VM to run tests over NFS.
# 1. Generates chimera config for the given backend
# 2. Creates a network namespace with TAP device
# 3. Starts chimera daemon in the netns (NFS on 10.0.0.1:2049)
# 4. Boots QEMU VM which mounts NFS and runs the test command
# 5. Captures exit code and cleans up

set -u

# Detect architecture for QEMU configuration
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    QEMU_BIN="qemu-system-aarch64"
    QEMU_MACHINE="-machine virt"
    QEMU_CONSOLE="ttyAMA0"
else
    QEMU_BIN="qemu-system-x86_64"
    QEMU_MACHINE="-machine q35,usb=off"
    QEMU_CONSOLE="ttyS0"
fi

VMLINUZ=$1; shift
ROOTFS=$1; shift
CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
NFS_VERSION=$1; shift
TEST_CMD_ARG="$*"

# Use initrd if present alongside vmlinuz
INITRD="$(dirname "$VMLINUZ")/initrd"
if [ -f "$INITRD" ]; then
    QEMU_INITRD="-initrd $INITRD"
else
    QEMU_INITRD=""
fi

NETNS_NAME="kvm_nfs_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_nfs_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfs_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""
TCPDUMP_PID=""
PCAP_DIR="${KVM_PCAP_DIR:-}"
if [ -n "$PCAP_DIR" ]; then
    PCAP_FILE="${PCAP_DIR}/${BACKEND}_nfs${NFS_VERSION}_$$.pcap"
else
    PCAP_FILE="${KVM_PCAP_FILE:-}"
fi

cleanup() {
    if [ -n "$TCPDUMP_PID" ]; then
        # Use SIGINT so tcpdump flushes its capture buffer before exiting
        kill -INT "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        # Give chimera up to 3 seconds to shut down cleanly
        for i in $(seq 1 30); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.1
        done
        # Force kill if still alive
        if kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "=== Chimera shutdown hung, force killing ===" >&2
            kill -9 "$CHIMERA_PID" 2>/dev/null || true
        fi
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ -f "$LOG_FILE" ]; then
        echo "=== Guest serial log ==="
        cat "$LOG_FILE"
    fi
    # Chimera stderr goes directly to wrapper stderr (captured by ctest)
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
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

# Raise system limits for high-parallelism testing
ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr

# Create network namespace
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

# Create TAP device inside the netns
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# Optionally start tcpdump to capture traffic (set KVM_PCAP_FILE to enable)
if [ -n "$PCAP_FILE" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -U -i "${TAP_NAME}" -w "$PCAP_FILE" -s 0 &
    TCPDUMP_PID=$!
    sleep 0.5
fi

# Start chimera daemon in the netns
# Let chimera's stderr flow directly to the wrapper's stderr so ctest captures it
# immediately with no buffering risk on timeout kills.
CHIMERA_LOG=""
ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" &
CHIMERA_PID=$!

# Wait for NFS port to be ready
for i in $(seq 1 30); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/2049" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.1
done

# Build the test command to run inside the VM
# Build mount options based on NFS version
NFS_MOUNT_OPTS="vers=${NFS_VERSION},tcp,nconnect=16"
if [ "$NFS_VERSION" = "3" ]; then
    NFS_MOUNT_OPTS="${NFS_MOUNT_OPTS},nolock"
fi

TEST_CMD="mount -t nfs -o ${NFS_MOUNT_OPTS} 10.0.0.1:/share /mnt && ${TEST_CMD_ARG}"

# Boot QEMU inside the netns
# Use -serial stdio so serial output goes to stdout in real-time (captured by ctest).
# Pipe through tee to also write to LOG_FILE for exit code parsing.
# This ensures guest output is visible even when ctest kills the process on timeout.
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_INITRD \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial stdio \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet panic=-1 test_cmd=\"${TEST_CMD}\" init=/init.sh" \
    2>/dev/null | tee "$LOG_FILE"

# Check if chimera is still alive after QEMU exits
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    CHIMERA_EXIT=$?
    echo "=== Chimera daemon DIED during test (exit code: $CHIMERA_EXIT) ==="
    CHIMERA_PID=""
fi

# Chimera stderr is already flowing to wrapper stderr (captured by ctest)

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
