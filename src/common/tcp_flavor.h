// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "evpl/evpl.h"

/*
 * TCP transport "flavor" selects which evpl stream backend is used for
 * plain (non-RDMA) TCP connections.  It is a process-wide / common setting
 * honored by both the server (listen sockets) and the client (outbound NFS
 * connections), so it lives here rather than in the server config.
 */
enum chimera_tcp_flavor {
    CHIMERA_TCP_FLAVOR_PLAIN    = 0,
    CHIMERA_TCP_FLAVOR_IO_URING = 1,
    CHIMERA_TCP_FLAVOR_XLIO     = 2,
};

/* Map a TCP flavor to the corresponding evpl stream protocol. */
static inline enum evpl_protocol_id
chimera_tcp_flavor_to_protocol(enum chimera_tcp_flavor flavor)
{
    switch (flavor) {
        case CHIMERA_TCP_FLAVOR_IO_URING:
            return EVPL_STREAM_IO_URING_TCP;
        case CHIMERA_TCP_FLAVOR_XLIO:
            return EVPL_STREAM_XLIO_TCP;
        case CHIMERA_TCP_FLAVOR_PLAIN:
        default:
            return EVPL_STREAM_SOCKET_TCP;
    } /* switch */
} /* chimera_tcp_flavor_to_protocol */
