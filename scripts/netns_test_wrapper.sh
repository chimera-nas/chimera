#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -e

# The preload a test needs arrives as CHIMERA_TEST_LD_PRELOAD, not LD_PRELOAD,
# and is applied to the test command alone (see the exec at the end).
#
# It has to be a separate variable.  Clearing LD_PRELOAD here cleans what the
# helpers below inherit -- date, ip -- but cannot unload anything from THIS
# shell: bash was already exec'd with it by then.  A bash running under ASAN
# then reports its own parser allocations at exit and fails the test with them:
#
#     ==NNN==ERROR: LeakSanitizer: detected memory leaks
#         #1 make_if_command (/usr/bin/bash+0x449f3)
#         #2 yyparse ... #6 main (/usr/bin/bash+0x351f5)
#
# which is a leak in bash, not in anything under test.  Never putting the
# preload in this process's environment is the only way to avoid it; a
# suppression would have to name bash's parser internals, and disabling leak
# detection outright would throw away the coverage the suppressions file exists
# to keep.
#
# LD_PRELOAD is still honoured for callers that have not moved over, and still
# cleared for the helpers, so behaviour there is unchanged.
SAVED_LD_PRELOAD="${CHIMERA_TEST_LD_PRELOAD:-${LD_PRELOAD:-}}"
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
echo 16777216 > /proc/sys/fs/aio-max-nr

# Run the test command inside the namespace, restoring LD_PRELOAD only for
# the test process (not for the ip binary itself, which ASAN would break)
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" "$@"
else
    ip netns exec "${NETNS_NAME}" "$@"
fi
