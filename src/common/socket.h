// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#ifdef _WIN32
#include "common/platform.h"
typedef SOCKET chimera_socket_t;
#define CHIMERA_INVALID_SOCKET INVALID_SOCKET
#define chimera_socket_close closesocket
static inline int chimera_socket_nonblocking(chimera_socket_t fd)
{
    u_long enabled = 1;
    return ioctlsocket(fd, FIONBIO, &enabled);
}
static inline int chimera_socket_interrupted(void)
{
    return WSAGetLastError() == WSAEINTR;
}
static inline int chimera_socket_connect_pending(void)
{
    int error = WSAGetLastError();
    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS || error == WSAEALREADY;
}
#else
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
typedef int chimera_socket_t;
#define CHIMERA_INVALID_SOCKET (-1)
#define chimera_socket_close close
static inline int chimera_socket_nonblocking(chimera_socket_t fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
static inline int chimera_socket_interrupted(void)
{
    return errno == EINTR;
}
static inline int chimera_socket_connect_pending(void)
{
    return errno == EINPROGRESS || errno == EALREADY;
}
#endif
