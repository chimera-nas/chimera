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
    # microvm machine: skips legacy PCI/ACPI device probing for a faster boot
    # (~0.1s/test).  pcie=on keeps a PCIe bus so the existing virtio-pci and
    # virtio-scsi-pci devices attach unchanged; rtc/pit on so the guest kernel
    # uses normal timers (without them it falls back to slow calibration paths).
    QEMU_MACHINE="-M microvm,acpi=on,rtc=on,pit=on,pcie=on"
    QEMU_CONSOLE="ttyS0"
fi

# Upper bound on a single guest, enforced by a process that is not this shell.
#
# ctest kills this wrapper when a test times out, and bash does run the EXIT
# trap on SIGTERM -- but not on SIGKILL, and a foreground qemu is not a job bash
# tears down either way.  An orphaned qemu keeps the stdout pipe open, ctest
# blocks forever waiting for EOF on it, and the whole shard wedges until the
# six-hour job limit: that is how one hung guest has been costing twelve runner
# hours a night while reporting nothing.
#
# timeout(1) is a separate process, so it keeps enforcing after this shell is
# gone, whatever killed it.  Generous by design -- it is a backstop, not a test
# budget; ctest's own per-test TIMEOUT is what should normally fire first.
KVM_QEMU_DEADLINE="${KVM_QEMU_DEADLINE:-2400}"

VMLINUZ=$1; shift
ROOTFS=$1; shift
TEST_CMD="$*"

# Boot with no initrd: every kernel in the KVM image matrix builds the virtio
# block/net drivers in, so the kernel mounts the virtio root disk directly.
# Skipping the ~63MB initrd unpack saves ~0.9s/test.  (See kvm/CMakeLists.txt:
# the 22.04 generic kernel, which needs an initrd, was dropped from the matrix
# in favor of its HWE kernel for exactly this reason.)
QEMU_INITRD=""

NETNS_NAME="kvm_test_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_test_XXXXXX.log)

cleanup() {
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
    # Reclaim the guest.  Everything above kills what this shell started by
    # PID; qemu runs in the foreground -- and in a pipeline, so $! never named
    # it -- and nothing was killing it at all.
    #
    # Walk our own descendants rather than signalling direct children only:
    # qemu sits under timeout(1) now, so it is a grandchild, and reaching it
    # that way would depend on timeout forwarding the signal.  It does, but a
    # reclaim that silently degrades to a 40-minute wait if that ever changes
    # is not worth the two saved lines.  Scoped to this shell's own tree, so a
    # sibling test's guest is never touched -- these run under ctest -j.
    #
    # Collect the whole tree before killing any of it: killing a parent first
    # reparents its children away and loses them.
    kvm_tree=""
    kvm_frontier="$(pgrep -P $$ 2>/dev/null)"
    while [ -n "${kvm_frontier}" ]; do
        kvm_tree="${kvm_tree} ${kvm_frontier}"
        kvm_next=""
        for kvm_p in ${kvm_frontier}; do
            kvm_next="${kvm_next} $(pgrep -P "${kvm_p}" 2>/dev/null)"
        done
        kvm_frontier="${kvm_next}"
    done
    for kvm_p in ${kvm_tree}; do
        kill -TERM "${kvm_p}" 2>/dev/null || true
    done
}
trap cleanup EXIT

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

# Boot QEMU inside the netns
ip netns exec "${NETNS_NAME}" timeout --foreground --kill-after=10s "${KVM_QEMU_DEADLINE}" "$QEMU_BIN" \
    -enable-kvm -smp 2 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_INITRD \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial file:"$LOG_FILE" \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 mitigations=off tsc=reliable panic=-1 test_cmd=\"${TEST_CMD}\" init=/init.sh"

cat "$LOG_FILE"

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
