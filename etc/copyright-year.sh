#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# copyright-year.sh - Check or update copyright years in modified files
#
# Usage: copyright-year.sh [--check] [--base BRANCH]
#   --check       Report out-of-date files and exit non-zero if any found
#   --base BRANCH Base branch for diff (default: main)

set -euo pipefail

CHECK_MODE=0
BASE="main"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)
            CHECK_MODE=1
            shift
            ;;
        --base)
            BASE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

CURRENT_YEAR=$(date +%Y)

# Get list of changed files relative to base branch, plus uncommitted changes
# Exclude deleted files with --diff-filter=d
MERGE_BASE=$(git merge-base HEAD "$BASE" 2>/dev/null || echo HEAD)

{
    git diff --name-only --diff-filter=d "$MERGE_BASE"
    git diff --name-only --diff-filter=d
    git diff --name-only --diff-filter=d --cached
} | sort -u | while IFS= read -r file; do
    # Skip empty lines and files that don't exist
    [[ -z "$file" ]] && continue
    [[ -f "$file" ]] || continue

    # Check for SPDX copyright lines or dep5-style Copyright lines
    if [[ "$file" == ".reuse/dep5" ]]; then
        PATTERN="Copyright:"
    else
        PATTERN="SPDX-FileCopyrightText:"
    fi

    grep -q "$PATTERN" "$file" 2>/dev/null || continue

    # Check if the file already contains the current year in a copyright line
    if grep "$PATTERN" "$file" | grep -q "$CURRENT_YEAR"; then
        continue
    fi

    if [[ "$CHECK_MODE" -eq 1 ]]; then
        echo "Copyright year not current: $file"
        # Use a temp file to signal failure since we're in a pipeline
        echo "$file" >> /tmp/copyright-year-failures.$$
    else
        # Fix mode: update copyright years
        # Handle: "2025" -> "2025-2026" (single year to range)
        # Handle: "2024-2025" -> "2024-2026" (extend range)
        # Handle: "2024 - 2025" -> "2024-2026" (normalize and extend)
        sed -i -E "/${PATTERN}/s/([0-9]{4}) *- *[0-9]{4}/\1-${CURRENT_YEAR}/g; /${PATTERN}/{ /[0-9]{4}-/!s/([0-9]{4})/\1-${CURRENT_YEAR}/; }" "$file"
        echo "Updated: $file"
    fi
done

if [[ "$CHECK_MODE" -eq 1 ]]; then
    FAIL_FILE="/tmp/copyright-year-failures.$$"
    if [[ -f "$FAIL_FILE" ]]; then
        COUNT=$(wc -l < "$FAIL_FILE")
        rm -f "$FAIL_FILE"
        echo "$COUNT file(s) have out-of-date copyright years."
        exit 1
    fi
    echo "All copyright years are current."
fi
