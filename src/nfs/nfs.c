#include <stdio.h>
#include "nfs.h"
#include "vfs/protocol.h"
#include "rpc2/rpc2.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"

struct chimera_server_nfs_shared
{
    struct NFS_V3 nfs3;
    struct NFS_V4 nfs4;
};

struct chimera_server_nfs_thread
{
    struct chimera_server_nfs_shared *shared;
    struct evpl_rpc2_agent *rpc2_agent;
    struct evpl_bind *bind;
    struct evpl_endpoint *endpoint;
};

static void *
nfs_server_init(void)
{
    struct chimera_server_nfs_shared *shared;

    shared = calloc(1, sizeof(*shared));

    shared->nfs3 = NFS_V3;
    shared->nfs4 = NFS_V4;

    return shared;
}

static void
nfs_server_destroy(void *data)
{
    struct chimera_server_nfs_shared *shared = data;

    free(shared);
}

static void
nfs_server_rpc2_dispatch(struct evpl_rpc2_agent *agent, struct evpl_rpc2_request *request, void *private_data)
{
    fprintf(stderr, "nfs_server_rpc2_dispatch\n");
}

static void *
nfs_server_thread_init(struct evpl *evpl, void *data)
{
    struct chimera_server_nfs_thread *thread;

    thread = calloc(1, sizeof(*thread));
    thread->shared = data;
    thread->rpc2_agent = evpl_rpc2_init(evpl);

    thread->endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 2049);

    thread->bind = evpl_rpc2_listen(thread->rpc2_agent,
                                    EVPL_STREAM_SOCKET_TCP,
                                    thread->endpoint,
                                    nfs_server_rpc2_dispatch,
                                    thread);
    return thread;
}

static void
nfs_server_thread_destroy(struct evpl *evpl, void *data)
{
    struct chimera_server_nfs_thread *thread = data;

    evpl_rpc2_destroy(thread->rpc2_agent);
    free(thread);
}

struct chimera_server_protocol nfs_protocol = {
    .init = nfs_server_init,
    .destroy = nfs_server_destroy,
    .thread_init = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
