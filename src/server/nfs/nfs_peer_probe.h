// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>

/* Probe a client-supplied peer address with a throwaway non-blocking
 * connect.  addr must be an IPv4 or IPv6 literal.  With timeout_ms == 0
 * the call never blocks: it reports a refusal the kernel already knows
 * (a closed loopback port, on Linux) as unreachable, and a connect still
 * in progress as reachable, so a dead or blackholed address is not
 * detected.
 * With timeout_ms > 0 an in-progress connect is waited on for up to that
 * long and counts only if it then completes without error. */
bool
nfs_peer_reachable(
    const char *addr,
    int         port,
    int         timeout_ms);
