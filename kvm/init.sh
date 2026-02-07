#!/bin/sh
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

mount -t proc none /proc
mount -t sysfs none /sys
mount -t devtmpfs udev /dev
mount -t tmpfs none /tmp

# Configure networking (VM side of the TAP link)
# virtio_net, virtio_blk, virtio_pci are built-in to the Ubuntu kernel
ip link set lo up
ip link set eth0 up 2>/dev/null || true
ip addr add 10.0.0.2/24 dev eth0 2>/dev/null || true

# Re-enable kernel console output (quiet suppressed it during boot)
echo 7 > /proc/sys/kernel/printk

# Parse test_cmd="..." from kernel cmdline
TEST_CMD=`cat /proc/cmdline | sed -e 's/^.*test_cmd="//' -e 's/".*$//'`

echo "Executing: $TEST_CMD"

eval "$TEST_CMD"
EXIT_CODE=$?

echo "CHIMERA_KVM_EXIT_CODE=${EXIT_CODE}"

# Flush all kernel buffers (including serial console) before power off
sync

# Tell kernel to power off; QEMU with -no-reboot will exit
echo o > /proc/sysrq-trigger

# Don't let init terminate before kernel has a chance to act
sleep 60
