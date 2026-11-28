#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Source this, then call chimera_clientid_test_env before every chimera start.
#
# Tests stand in for the supervisor: NFSv4 is offered only with a granted
# client ID namespace (src/server/nfs/clientid_allocator.h).  This exports
# CHIMERA_NODE_ID=1, CHIMERA_INSTANCE=0 and a CHIMERA_GENERATION taken from the
# millisecond clock (1..1048575), so a test that restarts the daemon gets a
# fresh Generation each time, as a real supervisor guarantees.
# src/common/test_clientid_env.h is the C counterpart.

chimera_clientid_test_env() {
    local ms
    if [ -n "${EPOCHREALTIME:-}" ]; then
        ms=$(( ${EPOCHREALTIME//[.,]/} / 1000 ))
    else
        ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    fi
    export CHIMERA_NODE_ID=1
    export CHIMERA_INSTANCE=0
    export CHIMERA_GENERATION=$(( (ms % 1048575) + 1 ))
}
