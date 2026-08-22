#!/bin/bash

# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Gate a FUSE test on the environment actually being able to serve FUSE
# mounts (open /dev/fuse + mount capability), probed at test time by
# fuse_probe because the configure environment and the test environment are
# different containers in CI.  Skips (ctest SKIP_RETURN_CODE 77) where the
# capability is missing, unless CHIMERA_REQUIRE_FUSE=1 turns that into a
# hard failure.

set -u

# Keep ASAN preloads away from the probe; restore for the real test command.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

if ! "${FUSE_PROBE:?FUSE_PROBE not set}"; then
    if [ "${CHIMERA_REQUIRE_FUSE:-0}" = "1" ]; then
        echo "FAIL: FUSE capability required (CHIMERA_REQUIRE_FUSE=1) but unavailable"
        exit 1
    fi
    echo "SKIP: FUSE mounts not available in this environment"
    exit 77
fi

if [ -n "$SAVED_LD_PRELOAD" ]; then
    export LD_PRELOAD="$SAVED_LD_PRELOAD"
fi

exec "$@"
