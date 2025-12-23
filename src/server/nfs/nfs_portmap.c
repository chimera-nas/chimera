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
}

void
chimera_portmap_null_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->portmap_v2.send_reply_PMAPPROC_NULL(evpl, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_null */

void
chimera_portmap_getport_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mapping        *mapping,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    unsigned int                      port;
    int                               rc;

    switch (mapping->prog) {
        case 100003:
            port = 2049;
            break;
        case 100005:
            port = 20048;
            break;
        default:
            chimera_nfs_error("portmap request for unknown program %u",
                              mapping->prog);
            port = 0;
    } /* switch */

    rc = shared->portmap_v2.send_reply_PMAPPROC_GETPORT(evpl, port, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

} /* chimera_portmap_getport */

void
chimera_portmap_dump_v2(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;
    struct pmaplist                   pmap_mountd, pmap_nfsd;

    pmap_mountd.map.prog = 100005;
    pmap_mountd.map.vers = 3;
    pmap_mountd.map.prot = 6;
    pmap_mountd.map.port = 20048;
    pmap_mountd.next     = &pmap_nfsd;

    pmap_nfsd.map.prog = 100003;
    pmap_nfsd.map.vers = 3;
    pmap_nfsd.map.prot = 6;
    pmap_nfsd.map.port = 2049;
    pmap_nfsd.next     = NULL;

    rc = shared->portmap_v2.send_reply_PMAPPROC_DUMP(evpl, &pmap_mountd, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_dump */

void
chimera_portmap_getaddr_v3(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct rpcb           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    xdr_string                        addr;
    char                              uaddr[64];
    unsigned int                      port;
    int                               rc;

    /* Return universal address for known programs */
    switch (args->r_prog) {
        case 100000: /* PORTMAP */
            port = 111;
            break;
        case 100003: /* NFS */
            port = 2049;
            break;
        case 100005: /* MOUNT */
            port = 20048;
            break;
        default:
            chimera_nfs_error("rpcbind getaddr request for unknown program %u",
                              args->r_prog);
            addr.str = "";
            addr.len = 0;
            goto send_reply;
    } /* switch */

    addr.len = portmap_make_uaddr(conn, port, uaddr, sizeof(uaddr));
    addr.str = uaddr;

 send_reply:
    rc = shared->portmap_v3.send_reply_rpcbproc_getaddr(evpl, &addr, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_rpcbind_getaddr */

void
chimera_portmap_dump_v3(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;
    struct rp__list                   rpcb_mountd, rpcb_nfsd;
    char                              uaddr_mountd[64], uaddr_nfsd[64];

    rpcb_mountd.rpcb_map.r_prog      = 100005;
    rpcb_mountd.rpcb_map.r_vers      = 3;
    rpcb_mountd.rpcb_map.r_netid.str = "tcp";
    rpcb_mountd.rpcb_map.r_netid.len = 3;
    rpcb_mountd.rpcb_map.r_addr.len  = portmap_make_uaddr(conn, 20048, uaddr_mountd, sizeof(uaddr_mountd));
    rpcb_mountd.rpcb_map.r_addr.str  = uaddr_mountd;
    rpcb_mountd.rpcb_map.r_owner.str = "";
    rpcb_mountd.rpcb_map.r_owner.len = 0;
    rpcb_mountd.next                 = &rpcb_nfsd;

    rpcb_nfsd.rpcb_map.r_prog      = 100003;
    rpcb_nfsd.rpcb_map.r_vers      = 3;
    rpcb_nfsd.rpcb_map.r_netid.str = "tcp";
    rpcb_nfsd.rpcb_map.r_netid.len = 3;
    rpcb_nfsd.rpcb_map.r_addr.len  = portmap_make_uaddr(conn, 2049, uaddr_nfsd, sizeof(uaddr_nfsd));
    rpcb_nfsd.rpcb_map.r_addr.str  = uaddr_nfsd;
    rpcb_nfsd.rpcb_map.r_owner.str = "";
    rpcb_nfsd.rpcb_map.r_owner.len = 0;
    rpcb_nfsd.next                 = NULL;

    rc = shared->portmap_v3.send_reply_rpcbproc_dump(evpl, &rpcb_mountd, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_rpcbind_dump */

void
chimera_portmap_dump_v4(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;
    struct rp__list                   rpcb_mountd, rpcb_nfsd;
    char                              uaddr_mountd[64], uaddr_nfsd[64];

    rpcb_mountd.rpcb_map.r_prog      = 100005;
    rpcb_mountd.rpcb_map.r_vers      = 3;
    rpcb_mountd.rpcb_map.r_netid.str = "tcp";
    rpcb_mountd.rpcb_map.r_netid.len = 3;
    rpcb_mountd.rpcb_map.r_addr.len  = portmap_make_uaddr(conn, 20048, uaddr_mountd, sizeof(uaddr_mountd));
    rpcb_mountd.rpcb_map.r_addr.str  = uaddr_mountd;
    rpcb_mountd.rpcb_map.r_owner.str = "";
    rpcb_mountd.rpcb_map.r_owner.len = 0;
    rpcb_mountd.next                 = &rpcb_nfsd;

    rpcb_nfsd.rpcb_map.r_prog      = 100003;
    rpcb_nfsd.rpcb_map.r_vers      = 3;
    rpcb_nfsd.rpcb_map.r_netid.str = "tcp";
    rpcb_nfsd.rpcb_map.r_netid.len = 3;
    rpcb_nfsd.rpcb_map.r_addr.len  = portmap_make_uaddr(conn, 2049, uaddr_nfsd, sizeof(uaddr_nfsd));
    rpcb_nfsd.rpcb_map.r_addr.str  = uaddr_nfsd;
    rpcb_nfsd.rpcb_map.r_owner.str = "";
    rpcb_nfsd.rpcb_map.r_owner.len = 0;
    rpcb_nfsd.next                 = NULL;

    rc = shared->portmap_v4.send_reply_RPCBPROC_DUMP(evpl, &rpcb_mountd, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_dump_v4 */

void
chimera_portmap_getaddr_v4(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct rpcb           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    xdr_string                        addr;
    char                              uaddr[64];
    unsigned int                      port;
    int                               rc;

    /* Return universal address for known programs */
    switch (args->r_prog) {
        case 100000: /* PORTMAP */
            port = 111;
            break;
        case 100003: /* NFS */
            port = 2049;
            break;
        case 100005: /* MOUNT */
            port = 20048;
            break;
        default:
            chimera_nfs_error("rpcbind getaddr request for unknown program %u",
                              args->r_prog);
            addr.str = "";
            addr.len = 0;
            goto send_reply;
    } /* switch */

    addr.len = portmap_make_uaddr(conn, port, uaddr, sizeof(uaddr));
    addr.str = uaddr;

 send_reply:
    rc = shared->portmap_v4.send_reply_RPCBPROC_GETADDR(evpl, &addr, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_portmap_getaddr_v4 */