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

# Run the test command in background so we can monitor it
eval "$TEST_CMD" &
TEST_PID=$!

# Background watchdog: if test runs longer than 10s, dump diagnostics
(
    ELAPSED=0
    while kill -0 $TEST_PID 2>/dev/null; do
        sleep 5
        ELAPSED=$((ELAPSED + 5))
        if [ $ELAPSED -ge 10 ]; then
            echo "=== WATCHDOG: test PID $TEST_PID still running after ${ELAPSED}s ==="
            echo "--- All processes ---"
            for pid in $(ls -d /proc/[0-9]* 2>/dev/null | cut -d/ -f3); do
                COMM=$(cat /proc/$pid/comm 2>/dev/null) || continue
                WCHAN=$(cat /proc/$pid/wchan 2>/dev/null)
                STATE=$(cat /proc/$pid/stat 2>/dev/null | cut -d' ' -f3)
                echo "PID $pid ($COMM) state=$STATE wchan=$WCHAN"
                if [ "$STATE" = "D" ]; then
                    cat /proc/$pid/stack 2>/dev/null
                fi
            done
            echo "--- NFS mount stats ---"
            cat /proc/self/mountstats 2>/dev/null | head -60
            echo "--- NFS RPC stats ---"
            cat /proc/net/rpc/nfs 2>/dev/null
            echo "--- dmesg (last 30 lines) ---"
            dmesg 2>/dev/null | tail -30
            echo "=== END WATCHDOG ==="
        fi
    done
) &
WATCHDOG_PID=$!

wait $TEST_PID
EXIT_CODE=$?

# Stop the watchdog
kill $WATCHDOG_PID 2>/dev/null
wait $WATCHDOG_PID 2>/dev/null

# Write exit code via kmsg so it goes through the kernel console driver
# synchronously, guaranteeing it reaches the serial log before power off
echo "CHIMERA_KVM_EXIT_CODE=${EXIT_CODE}" > /dev/kmsg

# Tell kernel to power off; QEMU with -no-reboot will exit
echo o > /proc/sysrq-trigger

# Don't let init terminate before kernel has a chance to act
sleep 60
