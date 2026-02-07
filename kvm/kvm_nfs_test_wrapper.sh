#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <cthon_flags...>
#
# Orchestrates a chimera NFS3 server + QEMU VM to run cthon04 tests.
# 1. Generates chimera config for the given backend
# 2. Creates a network namespace with TAP device
# 3. Starts chimera daemon in the netns (NFS3 on 10.0.0.1:2049)
# 4. Boots QEMU VM which mounts NFS and runs cthon04 runtests
# 5. Captures exit code and cleans up

set -u

VMLINUZ=$1; shift
ROOTFS=$1; shift
CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
CTHON_FLAGS="$*"

NETNS_NAME="kvm_nfs_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_nfs_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfs_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
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
        demofs)
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 256G "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"io_uring\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
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
        "delegation_threads": 1,
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

# Create TAP device inside the netns
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# Start chimera daemon in the netns
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
    sleep 0.5
done

# Build the test command to run inside the VM
TEST_CMD="mount -t nfs -o vers=3,nolock,tcp,port=2049,mountport=20048 10.0.0.1:/share /mnt && cd /opt/cthon04 && ./runtests ${CTHON_FLAGS} -f /mnt/test"

# Boot QEMU inside the netns
ip netns exec "${NETNS_NAME}" qemu-system-x86_64 \
    -enable-kvm -smp 2 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    -machine q35,usb=off \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0 \
    -serial file:"$LOG_FILE" \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=ttyS0 panic=-1 test_cmd=\"${TEST_CMD}\" init=/bin/sh -- /init.sh"

cat "$LOG_FILE"

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
