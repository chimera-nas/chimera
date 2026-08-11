// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdio.h>

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

    /* Not TCP at all: libevpl's in-process transport, where a "connection" is
     * a queue between two threads and never reaches the kernel.  It occupies
     * this enum because it answers the same question the others do -- which
     * transport do the server and the client agree on -- and that agreement is
     * already plumbed process-wide to both ends.
     *
     * Its use is the test suite.  An inproc name is private to the process, so
     * a server and client in one test binary can talk without binding a port,
     * and any number of those binaries can run at once without colliding.
     * That is what lets the tests run in parallel with no network namespace
     * to isolate them.  It is not a transport a real deployment can use: the
     * peer has to be in the same process. */
    CHIMERA_TCP_FLAVOR_INPROC   = 3,
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
        case CHIMERA_TCP_FLAVOR_INPROC:
            return EVPL_STREAM_INPROC;
        case CHIMERA_TCP_FLAVOR_PLAIN:
        default:
            return EVPL_STREAM_SOCKET_TCP;
    } /* switch */
} /* chimera_tcp_flavor_to_protocol */

/*
 * The RPC-over-RDMA counterpart of the above: which protocol carries the
 * chunk-bearing (DDP) framing.  tcp_rdma selects the emulation over a TCP
 * socket rather than real RDMA hardware.
 *
 * DATAGRAM_INPROC reports itself as RDMA-capable to rpc2, so the read and
 * write chunk paths are exercised exactly as they are over the wire
 * transports -- an inproc run is not quietly a plain-RPC run.
 */
static inline enum evpl_protocol_id
chimera_tcp_flavor_to_rdma_protocol(
    enum chimera_tcp_flavor flavor,
    int                     tcp_rdma)
{
    if (flavor == CHIMERA_TCP_FLAVOR_INPROC) {
        return EVPL_DATAGRAM_INPROC;
    }

    return tcp_rdma ? EVPL_DATAGRAM_TCP_RDMA : EVPL_DATAGRAM_RDMACM_RC;
} /* chimera_tcp_flavor_to_rdma_protocol */

/*
 * Build the endpoint for a service that the socket transports identify by
 * (host, port).
 *
 * Under inproc there is neither: the name is the whole address.  The port is
 * still what distinguishes one service from another -- NFS from mountd from
 * NLM -- so it names the endpoint, which keeps the portmap exchange working
 * unchanged: the server registers a port, the client asks for it, and both
 * sides derive the same name from it.
 *
 * No pid or other per-run token belongs in that name.  The registry is
 * private to the process, so two chimera processes using "chimera-inproc-2049"
 * simultaneously do not collide the way two binds to port 2049 would.  That
 * is precisely the property being bought here.
 */
static inline struct evpl_endpoint *
chimera_tcp_flavor_endpoint_create(
    enum chimera_tcp_flavor flavor,
    const char             *host,
    int                     port)
{
    char name[64];

    if (flavor != CHIMERA_TCP_FLAVOR_INPROC) {
        return evpl_endpoint_create(host, port);
    }

    snprintf(name, sizeof(name), "chimera-inproc-%d", port);

    return evpl_endpoint_create_inproc(name);
} /* chimera_tcp_flavor_endpoint_create */
