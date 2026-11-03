#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# CTest creates its persistent Testing/ logs in the current build directory.
# Running CTest as root is required by privileged integration tests, but leaving
# those logs root-owned prevents the Dev Container user from running tests or
# cleaning the build tree later.
if [ "$(id -u)" -eq 0 ]; then
    exec ctest "$@"
fi

sudo --preserve-env ctest "$@"
status=$?

if [ -e Testing ]; then
    if ! sudo chown -R "$(id -u):$(id -g)" Testing; then
        echo "failed to restore ownership of CTest Testing/ results" >&2
        [ "${status}" -ne 0 ] || status=1
    fi
fi

exit "${status}"