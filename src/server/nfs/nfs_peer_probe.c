// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdbool.h>
#ifndef _WIN32
#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */

#include "common/socket.h"
#include "nfs_peer_probe.h"

bool
nfs_peer_reachable(
    const char *addr,
    int         port,
    int         timeout_ms)
{
    struct sockaddr_storage ss;
    struct sockaddr_in     *sin  = (struct sockaddr_in *) &ss;
    struct sockaddr_in6    *sin6 = (struct sockaddr_in6 *) &ss;
    chimera_socket_t        fd;
    int                     family, rc;
    int                     salen;
    bool                    reachable;

    memset(&ss, 0, sizeof(ss));

    if (inet_pton(AF_INET, addr, &sin->sin_addr) == 1) {
        family          = AF_INET;
        sin->sin_family = AF_INET;
        sin->sin_port   = htons(port);
        salen           = sizeof(*sin);
    } else if (inet_pton(AF_INET6, addr, &sin6->sin6_addr) == 1) {
        family            = AF_INET6;
        sin6->sin6_family = AF_INET6;
        sin6->sin6_port   = htons(port);
        salen             = sizeof(*sin6);
    } else {
        return false;
    }

    fd = socket(family, SOCK_STREAM, 0);

    if (fd == CHIMERA_INVALID_SOCKET) {
        return false;
    }

    if (chimera_socket_nonblocking(fd) != 0) {
        chimera_socket_close(fd);
        return false;
    }

    do {
        rc = connect(fd, (struct sockaddr *) &ss, salen);
    } while (rc != 0 && chimera_socket_interrupted());

    if (rc == 0) {
        reachable = true;
    } else if (!chimera_socket_connect_pending()) {
        reachable = false;
    } else if (timeout_ms == 0) {
        reachable = true;
    } else {
        reachable = chimera_socket_wait_writable(fd, timeout_ms) > 0 &&
            chimera_socket_error(fd) == 0;
    }

    chimera_socket_close(fd);
    return reachable;
} /* nfs_peer_reachable */
