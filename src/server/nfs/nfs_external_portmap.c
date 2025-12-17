// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <rpc/rpc.h>
#include <rpc/types.h>
#include <rpc/rpcb_prot.h>
#include <netconfig.h>

#include "nfs_internal.h"
#include "nfs_external_portmap.h"

#define NFS_VERSION       3
#define NFS_MOUNT_VERSION 3

static void
register_service(
    const char    *netid,
    unsigned long  prog,
    unsigned long  vers,
    unsigned short port,
    const char    *desc)
{
    struct netconfig  *nconf = getnetconfigent(netid);
    struct sockaddr_in sin;
    struct netbuf      nb;
    bool_t             rc;

    if (!nconf) {
        chimera_nfs_error("Failed to get netconfig for %s", netid);
        return;
    }

    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons(port);
    sin.sin_addr.s_addr = INADDR_ANY;

    nb.len    = sizeof(sin);
    nb.maxlen = sizeof(sin);
    nb.buf    = &sin;

    rc = rpcb_set(prog, vers, nconf, &nb);
    chimera_nfs_abort_if(rc != TRUE, "Failed to register %s with rpcbind", desc);
    chimera_nfs_info("Registered %s with rpcbind", desc);
    freenetconfigent(nconf);
} /* register_service */

static void
unregister_service(
    const char   *netid,
    unsigned long prog,
    unsigned long vers,
    const char   *desc)
{
    struct netconfig *nconf = getnetconfigent(netid);
    bool_t            rc;

    if (!nconf) {
        chimera_nfs_error("Failed to get netconfig for %s", netid);
        return;
    }
    rc = rpcb_unset(prog, vers, nconf);
    if (rc != TRUE) {
        chimera_nfs_error("Failed to unregister %s from rpcbind", desc);
    } else {
        // Optional: log success
        // chimera_nfs_info("Unregistered %s from rpcbind", desc);
    }
    freenetconfigent(nconf);
} /* unregister_service */

void
register_nfs_rpc_services(void)
{
    register_service("tcp", NFS_RPC_PROGRAM, NFS_VERSION, NFS_PORT, "NFS over TCP");
    register_service("tcp", NFS_MOUNT_PROGRAM, NFS_MOUNT_VERSION, NFS_MOUNT_PORT, "NFS mountd over TCP");
} /* register_rpc_services */

void
unregister_nfs_rpc_services(void)
{
    unregister_service("tcp", NFS_RPC_PROGRAM, NFS_VERSION, "NFS over TCP");
    unregister_service("tcp", NFS_MOUNT_PROGRAM, NFS_MOUNT_VERSION, "NFS mountd over TCP");
} /* unregister_rpc_services */
