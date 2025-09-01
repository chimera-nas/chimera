// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "nfs_portmap.h"
#include "nfs_internal.h"
#include "nfs_common.h"

void
chimera_portmap_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->portmap_v2.send_reply_PMAPPROC_NULL(evpl, msg);
} /* chimera_portmap_null */

void
chimera_portmap_getport(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mapping        *mapping,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct port                       port;

    switch (mapping->prog) {
        case 100003:
            port.port = 2049;
            break;
        case 100005:
            port.port = 20048;
            break;
        default:
            chimera_nfs_error("portmap request for unknown program %u",
                              mapping->prog);
            port.port = 0;
    } /* switch */

    shared->portmap_v2.send_reply_PMAPPROC_GETPORT(evpl, &port, msg);

} /* chimera_portmap_getport */