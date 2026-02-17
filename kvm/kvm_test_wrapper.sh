#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <test_cmd>
#
# Boots a QEMU VM connected to a network namespace via TAP.
# The host side of the TAP gets 10.0.0.1, the VM gets 10.0.0.2.
# The test_cmd runs inside the VM and can reach 10.0.0.1 (host).
# Chimera (or any host process) can be started in the same netns
# to communicate with the VM.

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
TEST_CMD="$*"

NETNS_NAME="kvm_test_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_test_XXXXXX.log)

cleanup() {
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
}
trap cleanup EXIT

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

# Boot QEMU inside the netns
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 2 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial file:"$LOG_FILE" \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} panic=-1 test_cmd=\"${TEST_CMD}\" init=/bin/sh -- /init.sh"

cat "$LOG_FILE"

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
