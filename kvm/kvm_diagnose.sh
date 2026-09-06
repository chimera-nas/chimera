#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Standing diagnostic for the KVM suites.
#
# The KVM shard has never produced a passing run on the GitHub-hosted runners,
# in two different ways: guests that never boot (no rootfs where the registered
# test looks for it) and guests that boot and then hang, which wedge ctest for
# the job's whole six-hour budget.  Both take six hours to observe and neither
# leaves a usable log.
#
# This runs one guest, once, with everything named up front and a hard bound on
# the boot, so the answer arrives in minutes.  It is deliberately independent of
# ctest: no test registration, no parallelism, no output capture in the way.

set -u

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="${1:-/build/Release}"
IMAGE_NAME="${2:-ubuntu2404}"
BOOT_TIMEOUT="${KVM_DIAG_BOOT_TIMEOUT:-120}"

section() { printf '\n=== %s ===\n' "$1"; }

section "host"
echo "uname:      $(uname -srm)"
echo "container:  $( [ -f /.dockerenv ] && echo yes || echo no )"
echo "uid:        $(id -u)"
echo "nproc:      $(nproc 2>/dev/null || echo "?")"
echo "cpu:        $(grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
for f in vmx svm hypervisor; do
    grep -qm1 "^flags.*\b${f}\b" /proc/cpuinfo 2>/dev/null \
        && echo "cpuflag:    ${f} present" || echo "cpuflag:    ${f} ABSENT"
done

section "kvm device"
if [ -e /dev/kvm ]; then
    ls -l /dev/kvm
    [ -r /dev/kvm ] && [ -w /dev/kvm ] && echo "access:     rw ok" || echo "access:     NOT rw for uid $(id -u)"
else
    echo "/dev/kvm:   ABSENT"
fi

section "qemu"
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then QEMU_BIN=qemu-system-aarch64; else QEMU_BIN=qemu-system-x86_64; fi
if command -v "$QEMU_BIN" >/dev/null 2>&1; then
    echo "binary:     $(command -v "$QEMU_BIN")"
    "$QEMU_BIN" --version | head -1
    echo "accelerators:"
    "$QEMU_BIN" -accel help 2>/dev/null | sed 's/^/  /'
else
    echo "binary:     $QEMU_BIN NOT INSTALLED"
fi

# The registered tests take their image path from CMake's IMAGE_DIR, which is
# <parent of CMAKE_BINARY_DIR>/kvm/<image>.  That makes the location depend on
# how deep the build directory is: /build/Release yields /build/kvm/..., but a
# build configured directly in /build yields //kvm/... instead.  If the shard
# fetches images against one build directory and runs tests registered by
# another, the fetch succeeds and every guest still fails to open its rootfs.
section "image location"
BUILD_ROOT="$(dirname "$BUILD_DIR")"
IMAGE_DIR="${BUILD_ROOT}/kvm/${IMAGE_NAME}"
echo "build dir:  $BUILD_DIR"
echo "build root: $BUILD_ROOT"
echo "image dir:  $IMAGE_DIR"
for f in vmlinuz initrd rootfs.qcow2; do
    if [ -f "${IMAGE_DIR}/${f}" ]; then
        echo "  present   ${f} ($(stat -c %s "${IMAGE_DIR}/${f}" 2>/dev/null) bytes)"
    else
        echo "  MISSING   ${f}"
    fi
done
echo "anything under a 'kvm' directory nearby:"
find / -maxdepth 4 -type d -name kvm -not -path '*/proc/*' 2>/dev/null | sed 's/^/  /' | head

section "boot one guest (bounded at ${BOOT_TIMEOUT}s)"
if [ ! -f "${IMAGE_DIR}/rootfs.qcow2" ] || [ ! -f "${IMAGE_DIR}/vmlinuz" ]; then
    echo "SKIP: no image to boot -- the location above is the thing to fix first."
    exit 1
fi

QEMU_MACHINE="-M microvm,acpi=on,rtc=on,pit=on,pcie=on"
QEMU_CONSOLE="ttyS0"
if [ "$ARCH" = "aarch64" ]; then QEMU_MACHINE="-machine virt"; QEMU_CONSOLE="ttyAMA0"; fi
QEMU_INITRD=""
[ -f "${IMAGE_DIR}/initrd" ] && QEMU_INITRD="-initrd ${IMAGE_DIR}/initrd"

# No network and no chimera: this asks one question -- does a guest reach
# userspace and run a command -- so anything else that could fail is left out.
set +e
timeout --kill-after=10s "$BOOT_TIMEOUT" \
    "$QEMU_BIN" -enable-kvm -smp 2 -m 1G -cpu host \
    -kernel "${IMAGE_DIR}/vmlinuz" \
    $QEMU_INITRD \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="${IMAGE_DIR}/rootfs.qcow2",if=virtio,format=qcow2,snapshot=on \
    -serial stdio \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 mitigations=off tsc=reliable panic=-1 test_cmd=\"echo KVM_DIAG_GUEST_ALIVE; true\" init=/init.sh" \
    2>&1
rc=$?
set -e

section "result"
if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "TIMED OUT after ${BOOT_TIMEOUT}s -- the guest did not finish."
    echo "The serial output above is where it stopped; that is the hang."
elif [ "$rc" -ne 0 ]; then
    echo "qemu exited $rc"
else
    echo "qemu exited cleanly"
fi
exit "$rc"
