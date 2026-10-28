// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* A host without rpcbind must run unregistered: a refused loopback connect
 * to the local portmapper must neither abort the server at startup
 * (register) nor at shutdown (unregister), and a listening portmapper must
 * still be found. */
static int test_portmap_port;
#define NFS_EXTERNAL_PORTMAP_PORT test_portmap_port
#include "../nfs_external_portmap.c"
#include "common/mbt_watchdog.h"
#include <time.h>
#ifndef _WIN32
#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */
#include "common/socket.h"

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

/* Bind a loopback listener, report its port and return the socket. */
static chimera_socket_t
listen_on(int *port)
{
    struct sockaddr_in sin;
    socklen_t          len = sizeof(sin);
    chimera_socket_t   fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd != CHIMERA_INVALID_SOCKET);
    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(fd, (struct sockaddr *) &sin, sizeof(sin)) == 0);
    CHECK(listen(fd, 8) == 0);
    CHECK(getsockname(fd, (struct sockaddr *) &sin, &len) == 0);
    *port = ntohs(sin.sin_port);
    return fd;
} /* listen_on */

static int
closed_port(void)
{
    int              port;
    chimera_socket_t fd = listen_on(&port);

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
test_init_refused(void)
{
    struct portmap_reg_ctx ctx;
    long                   t0 = now_ms();

    /* The refusal is immediate on Linux but takes a SYN retry or two on
     * Windows; either way the probe's own budget bounds it. */
    test_portmap_port = closed_port();
    CHECK(portmap_init_context(&ctx) == -1);
    CHECK(now_ms() - t0 < NFS_EXTERNAL_PORTMAP_CONNECT_TIMEOUT_MS + 1000);
} /* test_init_refused */

static void
test_register_refused(void)
{
    /* The startup path: logs and continues unregistered. */
    test_portmap_port = closed_port();
    register_nfs_rpc_services(0, 0);
} /* test_register_refused */

static void
test_unregister_refused(void)
{
    /* The shutdown path every server without its own portmapper takes. */
    test_portmap_port = closed_port();
    unregister_nfs_rpc_services(0, 0);
} /* test_unregister_refused */

static void
test_init_listening(void)
{
    struct portmap_reg_ctx ctx;
    chimera_socket_t       fd = listen_on(&test_portmap_port);

    CHECK(portmap_init_context(&ctx) == 0);
    CHECK(ctx.portmap_conn != NULL);
    portmap_cleanup_context(&ctx);
    chimera_socket_close(fd);
} /* test_init_listening */

int
main(void)
{
#ifdef _WIN32
    WSADATA wsa;

    CHECK(WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
#endif /* ifdef _WIN32 */
    mbt_watchdog_arm(15);
    test_init_refused();
    test_register_refused();
    test_unregister_refused();
    test_init_listening();
    mbt_watchdog_disarm();
    puts("PASS: external portmap without rpcbind runs unregistered");
    return 0;
} /* main */
