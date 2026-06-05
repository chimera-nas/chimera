#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only
#
# Run elbencho with the chimera backend plugin and assert that it (a) succeeded and
# (b) drove chimera fully asynchronously, i.e. kept at least <min_peak> ops in flight
# concurrently. The plugin reports its peak via the "peak-async-inflight=N" token.
#
# Usage: verify_async.sh <min_peak> <elbencho> [elbencho args...]

set -o pipefail

min_peak="$1"
shift

out="$("$@" 2>&1)"
rc=$?

echo "$out"

if [ "$rc" -ne 0 ]; then
    echo "FAIL: elbencho exited with code $rc"
    exit 1
fi

peak="$(printf '%s\n' "$out" | sed -n 's/.*peak-async-inflight=\([0-9][0-9]*\).*/\1/p' | tail -1)"

if [ -z "$peak" ]; then
    echo "FAIL: plugin did not report peak-async-inflight"
    exit 1
fi

if [ "$peak" -lt "$min_peak" ]; then
    echo "FAIL: peak concurrent async ops = $peak, expected >= $min_peak (not fully async)"
    exit 1
fi

echo "PASS: peak concurrent async ops = $peak (>= $min_peak)"
exit 0
