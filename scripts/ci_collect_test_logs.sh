#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Called by the coverage container's EXIT trap, before --rm removes /build.
# Keep the paths so identically named logs from different tests stay distinct.
set -euo pipefail

build_dir=$1
output_dir=$2
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
cd "$build_dir"
find . -type f \( -name '*.debug.log' -o -name '*.metrics.prom' \
    -o -path './Testing/Temporary/*' \) -print0 |
    tar --null -czf "$output_dir/logs.tar.gz" --files-from=-
