// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <pthread.h>

#include "nfs_internal.h"
#include "nfs_common.h"
#include "nfs_external_portmap.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

#define NFS_VERSION       3
#define NFS_MOUNT_VERSION 3

struct portmap_reg_ctx {
    struct evpl             *evpl;
    struct evpl_rpc2_thread *rpc2_thread;
    struct NFS_PORTMAP_V2    portmap_v2;
    struct evpl_rpc2_conn   *portmap_conn;
    struct evpl_endpoint    *portmap_endpoint;
    int                      complete;
    int                      success;
    int                      pending_ops;
};

static void
portmap_set_callback(
    struct evpl *evpl,
    xdr_bool     reply,
    int          status,
    void        *private_data)
{
    struct portmap_reg_ctx *ctx = private_data;

    if (status != 0 || !reply || reply == 0) {
        chimera_nfs_error("Failed to register service with external portmap (status=%d)", status);
        ctx->success = 0;
    } else {
        chimera_nfs_debug("Successfully registered service with external portmap");
    }

    ctx->pending_ops--;
    if (ctx->pending_ops == 0) {
        ctx->complete = 1;
    }
} /* portmap_set_callback */

static void
portmap_unset_callback(
    struct evpl *evpl,
    xdr_bool     reply,
    int          status,
    void        *private_data)
{
    struct portmap_reg_ctx *ctx = private_data;

    if (status != 0 || !reply || reply == 0) {
        chimera_nfs_debug(
            "Failed to unregister service from external portmap (status=%d, may not have been registered)", status);
    } else {
        chimera_nfs_debug("Successfully unregistered service from external portmap");
    }

    ctx->pending_ops--;
    if (ctx->pending_ops == 0) {
        ctx->complete = 1;
    }
} /* portmap_unset_callback */

static int
portmap_init_context(struct portmap_reg_ctx *ctx)
{
    struct evpl_rpc2_program *programs[1];

    memset(ctx, 0, sizeof(*ctx));

    ctx->evpl = evpl_create(NULL);
    if (!ctx->evpl) {
        chimera_nfs_error("Failed to create evpl instance for portmap registration");
        return -1;
    }

    NFS_PORTMAP_V2_init(&ctx->portmap_v2);

    programs[0]      = &ctx->portmap_v2.rpc2;
    ctx->rpc2_thread = evpl_rpc2_thread_init(ctx->evpl, programs, 1, NULL, NULL);
    if (!ctx->rpc2_thread) {
        chimera_nfs_error("Failed to create RPC2 client thread for portmap registration");
        evpl_destroy(ctx->evpl);
        return -1;
    }

    ctx->portmap_endpoint = evpl_endpoint_create("127.0.0.1", 111);
    if (!ctx->portmap_endpoint) {
        chimera_nfs_error("Failed to create portmap endpoint");
        evpl_rpc2_thread_destroy(ctx->rpc2_thread);
        evpl_destroy(ctx->evpl);
        return -1;
    }

    ctx->portmap_conn = evpl_rpc2_client_connect(ctx->rpc2_thread,
                                                 EVPL_STREAM_SOCKET_TCP,
                                                 ctx->portmap_endpoint,
                                                 NULL, 0, NULL);
    if (!ctx->portmap_conn) {
        chimera_nfs_error("Failed to connect to external portmap at 127.0.0.1:111");
        evpl_rpc2_thread_destroy(ctx->rpc2_thread);
        evpl_destroy(ctx->evpl);
        return -1;
    }

    ctx->complete = 0;
    ctx->success  = 1;

    return 0;
} /* portmap_init_context */

static void
portmap_cleanup_context(struct portmap_reg_ctx *ctx)
{
    if (ctx->portmap_conn) {
        evpl_rpc2_client_disconnect(ctx->rpc2_thread, ctx->portmap_conn);
    }
    if (ctx->rpc2_thread) {
        evpl_rpc2_thread_destroy(ctx->rpc2_thread);
    }
    if (ctx->evpl) {
        evpl_destroy(ctx->evpl);
    }
} /* portmap_cleanup_context */

static void
register_service(
    struct portmap_reg_ctx *ctx,
    unsigned int            prog,
    unsigned int            vers,
    unsigned int            port,
    const char             *desc)
{
    struct mapping mapping;

    mapping.prog = prog;
    mapping.vers = vers;
    mapping.prot = 6;  /* TCP = 6 */
    mapping.port = port;

    chimera_nfs_info("Registering %s (program %u, version %u, port %u) with external portmap",
                     desc, prog, vers, port);

    ctx->pending_ops++;

    ctx->portmap_v2.send_call_PMAPPROC_SET(&ctx->portmap_v2.rpc2,
                                           ctx->evpl,
                                           ctx->portmap_conn,
                                           &mapping,
                                           0, 0, 0,
                                           portmap_set_callback, ctx);
} /* register_service */

static void
unregister_service(
    struct portmap_reg_ctx *ctx,
    unsigned int            prog,
    unsigned int            vers,
    const char             *desc)
{
    struct mapping mapping;

    mapping.prog = prog;
    mapping.vers = vers;
    mapping.prot = 6;  /* TCP = 6 */
    mapping.port = 0;  /* port is ignored for UNSET */

    chimera_nfs_debug("Unregistering %s (program %u, version %u) from external portmap",
                      desc, prog, vers);

    ctx->pending_ops++;

    ctx->portmap_v2.send_call_PMAPPROC_UNSET(&ctx->portmap_v2.rpc2,
                                             ctx->evpl,
                                             ctx->portmap_conn,
                                             &mapping,
                                             0, 0, 0,
                                             portmap_unset_callback, ctx);
} /* unregister_service */

void
register_nfs_rpc_services(void)
{
    struct portmap_reg_ctx ctx;

    if (portmap_init_context(&ctx) != 0) {
        chimera_nfs_fatal("Failed to initialize portmap registration context");
        return;
    }

    /* Register NFS and MOUNT services */
    register_service(&ctx, NFS_RPC_PROGRAM, NFS_VERSION, NFS_PORT, "NFS over TCP");
    register_service(&ctx, NFS_MOUNT_PROGRAM, NFS_MOUNT_VERSION, NFS_MOUNT_PORT, "NFS mountd over TCP");

    /* Wait for all registrations to complete */
    while (!ctx.complete) {
        evpl_continue(ctx.evpl);
    }

    if (!ctx.success) {
        chimera_nfs_fatal("Failed to register NFS services with external portmap");
    } else {
        chimera_nfs_info("Successfully registered all NFS services with external portmap");
    }

    portmap_cleanup_context(&ctx);
} /* register_nfs_rpc_services */

void
unregister_nfs_rpc_services(void)
{
    struct portmap_reg_ctx ctx;

    if (portmap_init_context(&ctx) != 0) {
        chimera_nfs_error("Failed to initialize portmap unregistration context");
        return;
    }

    /* Unregister NFS and MOUNT services */
    unregister_service(&ctx, NFS_RPC_PROGRAM, NFS_VERSION, "NFS over TCP");
    unregister_service(&ctx, NFS_MOUNT_PROGRAM, NFS_MOUNT_VERSION, "NFS mountd over TCP");

    /* Wait for all unregistrations to complete */
    while (!ctx.complete) {
        evpl_continue(ctx.evpl);
    }

    portmap_cleanup_context(&ctx);
} /* unregister_nfs_rpc_services */
