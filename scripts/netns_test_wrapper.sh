#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -e

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with
# system binaries (date, ip, sysctl, etc.) which exit non-zero under ASAN.
# LD_PRELOAD is restored only for the actual test command.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

if [ $# -lt 1 ]; then
    echo "Usage: $0 <test_command> [args...]"
    echo "Runs a test command in an isolated network namespace"
    exit 1
fi

TEST_NAME="chimera_test_$$_$(date +%s%N)"
NETNS_NAME="netns_${TEST_NAME}"

cleanup() {
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
}

trap cleanup EXIT

ip netns add "${NETNS_NAME}"

ip netns exec "${NETNS_NAME}" ip link set lo up

ulimit -l unlimited
sysctl -q -w fs.aio-max-nr=2097152 2>/dev/null || true

# Run the test command inside the namespace, restoring LD_PRELOAD only for
# the test process (not for the ip binary itself, which ASAN would break)
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" "$@"
else
    ip netns exec "${NETNS_NAME}" "$@"
fi
