#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
set -eu
binary=$1
fixture=$2
preload=${LD_PRELOAD:-}
unset LD_PRELOAD
asan_runtime=$(ldd "$binary" | awk '/libasan[.]so/ && $2 == "=>" { print $3; exit }')
if [ -n "$asan_runtime" ]; then
    preload="$asan_runtime${preload:+:$preload}"
fi
exec env LD_PRELOAD="${preload:+$preload:}$fixture" "$binary"
