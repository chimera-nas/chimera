// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <time.h>
#include "nfs_portmap.h"
#include "nfs_internal.h"
#include "nfs_common.h"

/*
 * Service registration table for portmap/rpcbind responses.
 * Lists all RPC services we advertise.
 */
struct portmap_service {
    uint32_t prog;    /* RPC program number */
    uint32_t vers;    /* Program version */
    uint32_t prot;    /* Protocol: 6 = TCP, 17 = UDP */
    uint32_t port;    /* Port number */
};

static const struct portmap_service portmap_services[] = {
    /* Portmap/rpcbind - program 100000 */
    { 100000, 2, 6, 111   },
    { 100000, 3, 6, 111   },
    { 100000, 4, 6, 111   },
    /* NFS - program 100003 */
    { 100003, 3, 6, 2049  },
    { 100003, 4, 6, 2049  },
    /* Mount - program 100005 */
    { 100005, 3, 6, 20048 },
};

#define NUM_PORTMAP_SERVICES (sizeof(portmap_services) / sizeof(portmap_services[0]))

/*
 * Convert local address from "ip:port" format to universal address format.
 * Universal address format for IPv4: "a.b.c.d.port_hi.port_lo"
 * where port = port_hi * 256 + port_lo
 *
 * Parameters:
 *   conn       - RPC connection to get local address from
 *   port       - Port number to encode in universal format
 *   uaddr      - Output buffer for universal address string
 *   uaddr_size - Size of output buffer
 *
 * Returns the length of the universal address string, or 0 on error.
 */
static int
portmap_make_uaddr(
    struct evpl_rpc2_conn *conn,
    unsigned int           port,
    char                  *uaddr,
    size_t                 uaddr_size)
{
    char  local_addr[128];
    char *colon;

    evpl_bind_get_local_address(conn->bind, local_addr, sizeof(local_addr));

    /* Find and terminate at the colon to get just the IP */
    colon = strchr(local_addr, ':');
    if (colon) {
        *colon = '\0';
    }

    return snprintf(uaddr, uaddr_size, "%s.%u.%u",
                    local_addr, port >> 8, port & 0xff);
} /* portmap_make_uaddr */

/*
 * Look up port number for a given program.
 * Returns the port number, or 0 if not found.
 */
static unsigned int
portmap_lookup_port(uint32_t prog)
{
    for (unsigned int i = 0; i < NUM_PORTMAP_SERVICES; i++) {
        if (portmap_services[i].prog == prog) {
            return portmap_services[i].port;
        }
    }
    return 0;
} /* portmap_lookup_port */

/*
 * Build a V2 pmaplist linked list from the service table.
 * Allocates structures from the RPC message dbuf.
 */
static struct pmaplist *
portmap_build_pmaplist(struct evpl_rpc2_msg *msg)
{
    struct pmaplist *head = NULL;
    struct pmaplist *tail = NULL;
    struct pmaplist *entry;

    for (unsigned int i = 0; i < NUM_PORTMAP_SERVICES; i++) {
        entry = xdr_dbuf_alloc_space(sizeof(*entry), msg->dbuf);
        if (!entry) {
            return head;
        }

        entry->map.prog = portmap_services[i].prog;
        entry->map.vers = portmap_services[i].vers;
        entry->map.prot = portmap_services[i].prot;
        entry->map.port = portmap_services[i].port;
        entry->next     = NULL;

        if (tail) {
            tail->next = entry;
        } else {
            head = entry;
        }
        tail = entry;
    }

    return head;
} /* portmap_build_pmaplist */

/*
 * Build a V3/V4 rp__list linked list from the service table.
 * Allocates structures and uaddr strings from the RPC message dbuf.
 */
static struct rp__list *
portmap_build_rpcblist(
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg)
{
    struct rp__list *head = NULL;
    struct rp__list *tail = NULL;
    struct rp__list *entry;
    char            *uaddr;

    for (unsigned int i = 0; i < NUM_PORTMAP_SERVICES; i++) {
        entry = xdr_dbuf_alloc_space(sizeof(*entry), msg->dbuf);
        if (!entry) {
            return head;
        }

        uaddr = xdr_dbuf_alloc_space(64, msg->dbuf);
        if (!uaddr) {
            return head;
        }

        entry->rpcb_map.r_prog      = portmap_services[i].prog;
        entry->rpcb_map.r_vers      = portmap_services[i].vers;
        entry->rpcb_map.r_netid.str = "tcp";
        entry->rpcb_map.r_netid.len = 3;
        entry->rpcb_map.r_addr.len  = portmap_make_uaddr(conn,
                                                         portmap_services[i].port,
                                                         uaddr, 64);
        entry->rpcb_map.r_addr.str  = uaddr;
        entry->rpcb_map.r_owner.str = "";
        entry->rpcb_map.r_owner.len = 0;
        entry->next                 = NULL;

        if (tail) {
            tail->next = entry;
        } else {
            head = entry;
        }
        tail = entry;
    }

    return head;
} /* portmap_build_rpcblist */

void
chimera_portmap_null_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->portmap_v2.send_reply_PMAPPROC_NULL(evpl, NULL, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_null */

void
chimera_portmap_getport_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct mapping        *mapping,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    unsigned int                      port;
    int                               rc;

    port = portmap_lookup_port(mapping->prog);

    if (port == 0) {
        chimera_nfs_error("portmap request for unknown program %u",
                          mapping->prog);
    }

    rc = shared->portmap_v2.send_reply_PMAPPROC_GETPORT(evpl, NULL, port, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_getport */

void
chimera_portmap_dump_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct pmaplist                  *list;
    int                               rc;

    list = portmap_build_pmaplist(msg);

    rc = shared->portmap_v2.send_reply_PMAPPROC_DUMP(evpl, NULL, list, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_dump */

/*
 * Common implementation for V3/V4 GETADDR.
 * Returns universal address for known programs.
 */
static void
portmap_getaddr_common(
    struct evpl *evpl,
    struct evpl_rpc2_conn *conn,
    struct rpcb *args,
    struct evpl_rpc2_msg *msg,
    int ( *send_reply )(struct evpl *, const struct evpl_rpc2_verf *, xdr_string *, void *))
{
    xdr_string   addr;
    char         uaddr[64];
    unsigned int port;
    int          rc;

    port = portmap_lookup_port(args->r_prog);

    if (port == 0) {
        chimera_nfs_error("rpcbind getaddr request for unknown program %u",
                          args->r_prog);
        addr.str = "";
        addr.len = 0;
    } else {
        addr.len = portmap_make_uaddr(conn, port, uaddr, sizeof(uaddr));
        addr.str = uaddr;
    }

    rc = send_reply(evpl, NULL, &addr, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* portmap_getaddr_common */

void
chimera_portmap_getaddr_v3(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct rpcb           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    portmap_getaddr_common(evpl, conn, args, msg,
                           shared->portmap_v3.send_reply_rpcbproc_getaddr);
} /* chimera_portmap_getaddr_v3 */

/*
 * Common implementation for V3/V4 DUMP.
 * Returns list of all registered services.
 */
static void
portmap_dump_common(
    struct evpl *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg *msg,
    int ( *send_reply )(struct evpl *, const struct evpl_rpc2_verf *, struct rp__list *, void *))
{
    struct rp__list *list;
    int              rc;

    list = portmap_build_rpcblist(conn, msg);

    rc = send_reply(evpl, NULL, list, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* portmap_dump_common */

void
chimera_portmap_dump_v3(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    portmap_dump_common(evpl, conn, msg,
                        shared->portmap_v3.send_reply_rpcbproc_dump);
} /* chimera_portmap_dump_v3 */

void
chimera_portmap_dump_v4(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    portmap_dump_common(evpl, conn, msg,
                        shared->portmap_v4.send_reply_RPCBPROC_DUMP);
} /* chimera_portmap_dump_v4 */

void
chimera_portmap_getaddr_v4(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_cred *cred,
    struct rpcb           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    portmap_getaddr_common(evpl, conn, args, msg,
                           shared->portmap_v4.send_reply_RPCBPROC_GETADDR);
} /* chimera_portmap_getaddr_v4 */
