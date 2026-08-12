#!/bin/bash

# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Raise the resource limits some VFS backends need, then run the test.
#
# This is netns_test_wrapper.sh with the network namespace taken out.  Tests
# that reach their server over the in-process transport have no port to
# conflict over, so they need no namespace to isolate them -- but they still
# need the limits the namespace wrapper happened to be raising along the way:
# unlimited locked memory for the io_uring module's queues, and an aio-max-nr
# large enough for the libaio backend's contexts.
#
# Losing the namespace is the point: creating one needs CAP_NET_ADMIN, and
# where that is unavailable the tests using it could not run at all.  These can.

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <test_command> [args...]"
    echo "Runs a test command with the resource limits the backends need"
    exit 1
fi

# Both are best-effort.  A backend that genuinely needs them fails loudly on
# its own; a backend that does not (memfs, cairn, linux) should still run in an
# environment that grants neither, which is precisely the case this wrapper
# exists to support.
ulimit -l unlimited 2>/dev/null || true
# The braces matter: a failed redirection is reported by the shell before the
# command's own 2>/dev/null takes effect, so the suppression must wrap the
# whole command to keep hosts without /proc (macOS) quiet.
{ echo 16777216 > /proc/sys/fs/aio-max-nr; } 2>/dev/null || true

exec "$@"
