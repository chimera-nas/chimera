#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Regenerate the checked-in conformance traces under traces/ from the Quint
# model.  Run this after changing portmap.qnt (or to widen coverage), then
# commit the results.  Fixed seeds keep regeneration reproducible for a given
# quint version; the exact seed values are arbitrary.
#
# Requires the quint CLI (https://quint-lang.org), either installed as
# `quint` or fetchable via `npx @informalsystems/quint`.

set -eu

cd "$(dirname "$(readlink -f "$0")")"

if command -v quint > /dev/null 2>&1; then
    QUINT=(quint)
elif command -v npx > /dev/null 2>&1; then
    QUINT=(npx -y @informalsystems/quint)
else
    echo "quint not found (install quint or npm/npx)" >&2
    exit 1
fi

# The model must hold its own invariant before we mint oracle traces from it.
"${QUINT[@]}" test portmap.qnt
"${QUINT[@]}" run portmap.qnt --invariant=repliesExact \
    --max-samples=500 --max-steps=50

mkdir -p traces

SEEDS=(0xc1 0xc2 0xc3)

for i in "${!SEEDS[@]}"; do
    n=$((i + 1))
    "${QUINT[@]}" run portmap.qnt \
        --seed="${SEEDS[$i]}" \
        --max-steps=50 \
        --out-itf="traces/trace-${n}.itf.json" > /dev/null
    echo "traces/trace-${n}.itf.json (seed ${SEEDS[$i]})"
done
