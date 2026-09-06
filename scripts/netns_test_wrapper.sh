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
# the test process (not for the ip binary itself, which ASAN would break).
#
# set -e is lifted around the run so a signal death reaches the report below
# rather than killing the shell with the status unexamined.
set +e
if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" "$@"
else
    ip netns exec "${NETNS_NAME}" "$@"
fi
STATUS=$?
set -e

# A command killed by a signal leaves only bash's own one-line "Illegal
# instruction" on stderr, which says nothing about which binary died or why.
# That is exactly how the fio SIGILL has stayed unexplained: the tests run with
# chimera logging off, so the process produces no output at all before it dies.
# Report what is knowable here, while the namespace and the environment that
# produced it are still the ones the command saw.
if [ "${STATUS}" -gt 128 ]; then
    SIG=$((STATUS - 128))
    echo "=== netns test died on signal ${SIG} ($(kill -l "${SIG}" 2>/dev/null || echo unknown)) ===" >&2
    echo "  command: $*" >&2
    echo "  uname:   $(uname -m) $(uname -r)" >&2

    # SIGILL is the one that needs hardware context: an instruction the build
    # emitted that this particular runner cannot execute looks exactly like a
    # crash in the loaded plugin.  Name the CPU and the ISA levels it claims.
    if [ "${SIG}" -eq 4 ]; then
        echo "  cpu:     $(grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')" >&2
        for f in avx2 bmi2 fma avx512f sse4_2; do
            if grep -qm1 "^flags.*\b${f}\b" /proc/cpuinfo 2>/dev/null; then
                printf '  isa:     %s present\n' "${f}" >&2
            else
                printf '  isa:     %s ABSENT\n' "${f}" >&2
            fi
        done
        # The ISA data has since ruled the hardware out -- avx2/bmi2/fma are all
        # present on the runners that fail, so -march=x86-64-v3 is satisfied.
        # What is left is the binary and the plugin it dlopens, which come from
        # the container image and are built against each other's headers
        # (src/fio builds with include_directories(/fio)).  Name the version of
        # the binary that died, so a drift between the image's fio and the one
        # the plugin was compiled for is visible rather than inferred.
        echo "  binary:  $1" >&2
        # Bounded: this runs on an already-failing path and must not be the
        # thing that hangs.  Falls back when timeout(1) is unavailable rather
        # than reporting its own absence as the version.
        if command -v timeout >/dev/null 2>&1; then
            timeout 5 "$1" --version 2>&1 | head -2 | sed 's/^/  version: /' >&2 || true
        else
            "$1" --version 2>&1 | head -2 | sed 's/^/  version: /' >&2 || true
        fi

        # The plugin is dlopened, so a mismatch there is invisible to ldd on the
        # host binary; name it explicitly when the job file points at one.
        for arg in "$@"; do
            case "${arg}" in
                *.fio)
                    plugin=$(sed -n 's/^ioengine=external://p' "${arg}" 2>/dev/null | head -1)
                    if [ -n "${plugin}" ]; then
                        echo "  plugin:  ${plugin}" >&2
                        ls -l "${plugin}" >&2 2>/dev/null || echo "  plugin:  MISSING" >&2
                    fi
                    ;;
            esac
        done
    fi
fi

exit "${STATUS}"
