// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* The reachability probe must answer without ever aborting or blocking the
 * caller beyond its budget: refused, dead and malformed addresses all
 * report false; a listening peer reports true. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */

#include "common/socket.h"
#include "common/mbt_watchdog.h"
#include "nfs_peer_probe.h"

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

static int
listen_on(chimera_socket_t *fd)
{
    struct sockaddr_in sin;
    socklen_t          len = sizeof(sin);

    *fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(*fd != CHIMERA_INVALID_SOCKET);
    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(*fd, (struct sockaddr *) &sin, sizeof(sin)) == 0);
    CHECK(listen(*fd, 8) == 0);
    CHECK(getsockname(*fd, (struct sockaddr *) &sin, &len) == 0);
    return ntohs(sin.sin_port);
} /* listen_on */

static int
closed_port(void)
{
    chimera_socket_t fd;
    int              port = listen_on(&fd);

    chimera_socket_close(fd);
    return port;
} /* closed_port */

static long
now_ms(void)
{
#ifdef _WIN32
    return (long) GetTickCount64();
#else  /* ifdef _WIN32 */
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long) (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
#endif /* ifdef _WIN32 */
} /* now_ms */

static void
test_refused_port(void)
{
    int port = closed_port();

#ifdef __linux__
    /* Only Linux knows a refused loopback connect by the time connect()
     * returns (Windows retries the SYN, macOS takes the RST later), so only
     * there must even the zero-timeout probe see the refusal. */
    CHECK(!nfs_peer_reachable("127.0.0.1", port, 0));
#endif /* ifdef __linux__ */
    CHECK(!nfs_peer_reachable("127.0.0.1", port, 500));
} /* test_refused_port */

static void
test_listener(void)
{
    chimera_socket_t fd;
    int              port = listen_on(&fd);

    CHECK(nfs_peer_reachable("127.0.0.1", port, 0));
    CHECK(nfs_peer_reachable("127.0.0.1", port, 500));
    chimera_socket_close(fd);
} /* test_listener */

static void
test_ipv6_listener(void)
{
    struct sockaddr_in6 sin6;
    socklen_t           len = sizeof(sin6);
    chimera_socket_t    fd;

    fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd == CHIMERA_INVALID_SOCKET) {
        printf("SKIP: ipv6\n");
        return;
    }
    memset(&sin6, 0, sizeof(sin6));
    sin6.sin6_family = AF_INET6;
    sin6.sin6_addr   = in6addr_loopback;
    if (bind(fd, (struct sockaddr *) &sin6, sizeof(sin6)) != 0 ||
        listen(fd, 8) != 0 ||
        getsockname(fd, (struct sockaddr *) &sin6, &len) != 0) {
        chimera_socket_close(fd);
        printf("SKIP: ipv6\n");
        return;
    }
    CHECK(nfs_peer_reachable("::1", ntohs(sin6.sin6_port), 500));
    chimera_socket_close(fd);
} /* test_ipv6_listener */

static void
test_dead_address(void)
{
    long t0 = now_ms();

    /* RFC 5737 TEST-NET-1 never answers (or fails at once). */
    CHECK(!nfs_peer_reachable("192.0.2.1", 111, 300));
    CHECK(now_ms() - t0 < 2000);
} /* test_dead_address */

static void
test_bad_literal(void)
{
    CHECK(!nfs_peer_reachable("not-an-address", 111, 0));
    CHECK(!nfs_peer_reachable("", 111, 0));
} /* test_bad_literal */

int
main(void)
{
#ifdef _WIN32
    WSADATA wsa;

    CHECK(WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
#endif /* ifdef _WIN32 */
    mbt_watchdog_arm(15);
    test_refused_port();
    test_listener();
    test_ipv6_listener();
    test_dead_address();
    test_bad_literal();
    mbt_watchdog_disarm();
    printf("PASS: peer probe\n");
    return 0;
} /* main */
