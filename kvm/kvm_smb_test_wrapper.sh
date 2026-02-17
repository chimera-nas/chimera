#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_smb_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <test_cmd>
#
# Orchestrates a chimera SMB server + QEMU VM to run tests over CIFS.
# 1. Generates chimera config for the given backend
# 2. Creates a network namespace with TAP device
# 3. Starts chimera daemon in the netns (SMB on 10.0.0.1:445)
# 4. Boots QEMU VM which mounts CIFS and runs the test command
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
TEST_CMD_ARG="$*"

NETNS_NAME="kvm_smb_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_smb_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_smb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""
TCPDUMP_PID=""
PCAP_FILE="${KVM_PCAP_FILE:-}"

cleanup() {
    if [ -n "$TCPDUMP_PID" ]; then
        kill "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
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
    "shares": {
        "share": {
            "path": "/share"
        }
    },
    "users": [
        {
            "username": "root",
            "smbpasswd": "secret",
            "uid": 0,
            "gid": 0
        }
    ]
}
EOF
}

generate_config

# Raise system limits for high-parallelism testing
ulimit -l unlimited
echo 2097152 > /proc/sys/fs/aio-max-nr

# Create network namespace
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

# Create TAP device inside the netns
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# Optionally start tcpdump to capture traffic (set KVM_PCAP_FILE to enable)
if [ -n "$PCAP_FILE" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -i "${TAP_NAME}" -w "$PCAP_FILE" -s 0 &
    TCPDUMP_PID=$!
    sleep 0.5
fi

# Start chimera daemon in the netns
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
ip netns exec "${NETNS_NAME}" env \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    2>"$CHIMERA_LOG" &
CHIMERA_PID=$!

# Wait for SMB port to be ready
for i in $(seq 1 30); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/445" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.1
done

# Build the test command to run inside the VM
TEST_CMD="mount -t cifs //10.0.0.1/share /mnt -o username=root,password=secret,vers=2.1,nobrl,modefromsid,cache=loose && ${TEST_CMD_ARG}"

# Boot QEMU inside the netns
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial file:"$LOG_FILE" \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} quiet panic=-1 test_cmd=\"${TEST_CMD}\" init=/bin/sh -- /init.sh"

cat "$LOG_FILE"

# Show chimera debug output if present
if [ -f "$CHIMERA_LOG" ]; then
    echo "=== Chimera stderr (last 100 lines) ==="
    tail -100 "$CHIMERA_LOG"
    cp "$CHIMERA_LOG" /tmp/chimera_stderr_last.log 2>/dev/null || true
fi

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
