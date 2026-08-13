// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * TCP transport "flavor" selects which evpl stream backend is used for
 * plain (non-RDMA) TCP connections.  It is a process-wide / common setting
 * honored by both the server (listen sockets) and the client (outbound NFS
 * connections).
 *
 * The vocabulary lives in the SDK because it crosses the module boundary:
 * chimera_vfs_request_tcp_flavor() (vfs_utils.h) hands a backend the flavor
 * the process agreed on, and a proxying backend that opens its own
 * connections -- the in-tree nfs and smb clients, or any out-of-tree
 * equivalent -- has to interpret the value to honor it.
 *
 * common/tcp_flavor.h includes this header and adds the evpl-facing helpers
 * (flavor -> evpl_protocol_id, endpoint construction) used by core and the
 * protocol servers.
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
