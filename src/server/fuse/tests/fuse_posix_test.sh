#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Drives fuse_posix_test (exact-errno POSIX assertions with plain syscalls)
# against a live FUSE mountpoint served by a chimera daemon.

BIN=${1:?usage: fuse_posix_test.sh <chimera_binary> <backend> <test_binary> [options]}
BACKEND=${2:?usage: fuse_posix_test.sh <chimera_binary> <backend> <test_binary> [options]}
TESTBIN=${3:?usage: fuse_posix_test.sh <chimera_binary> <backend> <test_binary> [options]}
OPTIONS=${4:-}

. "$(dirname "$0")/fuse_test_common.sh"

fuse_test_start "$BIN" "$BACKEND" "/" "$OPTIONS"

"$TESTBIN" "$MNT" || fail "fuse_posix_test reported failures"

fuse_test_stop

echo "PASS: POSIX semantics over FUSE on $BACKEND ${OPTIONS:+($OPTIONS)}"
exit 0
