// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#ifdef _WIN32
#include "common/platform.h"
typedef SOCKET chimera_socket_t;
#define CHIMERA_INVALID_SOCKET INVALID_SOCKET
#define chimera_socket_close   closesocket
static inline int
chimera_socket_nonblocking(chimera_socket_t fd)
{
    u_long enabled = 1;

    return ioctlsocket(fd, FIONBIO, &enabled);
} // chimera_socket_nonblocking
static inline int
chimera_socket_interrupted(void)
{
    return WSAGetLastError() == WSAEINTR;
} // chimera_socket_interrupted
static inline int
chimera_socket_connect_pending(void)
{
    int error = WSAGetLastError();

    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS || error == WSAEALREADY;
} // chimera_socket_connect_pending
static inline int
chimera_socket_wait_writable(
    chimera_socket_t fd,
    int              timeout_ms)
{
    WSAPOLLFD pfd;

    pfd.fd      = fd;
    pfd.events  = POLLWRNORM;
    pfd.revents = 0;
    return WSAPoll(&pfd, 1, timeout_ms);
} // chimera_socket_wait_writable
static inline int
chimera_socket_error(chimera_socket_t fd)
{
    int err = 0;
    int len = sizeof(err);

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *) &err, &len) != 0) {
        return -1;
    }
    return err;
} // chimera_socket_error
#else // ifdef _WIN32
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
typedef int chimera_socket_t;
#define CHIMERA_INVALID_SOCKET (-1)
#define chimera_socket_close   close
static inline int
chimera_socket_nonblocking(chimera_socket_t fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
} // chimera_socket_nonblocking
static inline int
chimera_socket_interrupted(void)
{
    return errno == EINTR;
} // chimera_socket_interrupted
static inline int
chimera_socket_connect_pending(void)
{
    return errno == EINPROGRESS || errno == EALREADY;
} // chimera_socket_connect_pending
static inline int
chimera_socket_wait_writable(
    chimera_socket_t fd,
    int              timeout_ms)
{
    struct pollfd pfd;
    int           rc;

    pfd.fd      = fd;
    pfd.events  = POLLOUT;
    pfd.revents = 0;
    do {
        rc = poll(&pfd, 1, timeout_ms);
    } while (rc < 0 && errno == EINTR);
    return rc;
} // chimera_socket_wait_writable
static inline int
chimera_socket_error(chimera_socket_t fd)
{
    int       err = 0;
    socklen_t len = sizeof(err);

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *) &err, &len) != 0) {
        return -1;
    }
    return err;
} // chimera_socket_error
#endif // ifdef _WIN32
