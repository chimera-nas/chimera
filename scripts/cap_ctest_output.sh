#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Cap command output to ~4 MB total for CI log readability.
#
# Streams the first 2 MB of output in real time, then keeps only
# the last 2 MB of any remaining output using tail's internal ring
# buffer.  Memory usage is O(2 MB) regardless of total output size.
# Preserves the wrapped command's exit code.
#
# Usage: cap_ctest_output.sh <command> [args...]

set -uo pipefail

LIMIT=2097152  # 2 MB
BLOCK_SIZE=4096
BLOCK_COUNT=$((LIMIT / BLOCK_SIZE))

TAIL_FILE=$(mktemp)
trap 'rm -f "$TAIL_FILE"' EXIT

"$@" 2>&1 | {
    # Phase 1: stream first 2 MB in real time.
    # dd uses raw I/O so output appears immediately.
    dd bs="$BLOCK_SIZE" count="$BLOCK_COUNT" iflag=fullblock 2>/dev/null

    # Phase 2: keep only the last 2 MB of any overflow.
    # tail -c uses an internal ring buffer so this is O(LIMIT) memory.
    tail -c "$LIMIT" > "$TAIL_FILE"

    if [ -s "$TAIL_FILE" ]; then
        echo ""
        echo "=== Output truncated. Showing last 2 MB of remaining output ==="
        cat "$TAIL_FILE"
    fi
}

exit "${PIPESTATUS[0]}"
