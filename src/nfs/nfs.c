#include <stdio.h>
#include "nfs.h"
#include "server/protocol.h"
#include "vfs/vfs.h"
#include "rpc2/rpc2.h"
#include "nfs4_procs.h"
#include "nfs_common.h"

#include "common/logging.h"

static void *
nfs_server_init(struct chimera_vfs *vfs)
{
    struct chimera_server_nfs_shared *shared;

    shared = calloc(1, sizeof(*shared));

    shared->vfs = vfs;

    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    shared->nfs_v4.recv_call_NFSPROC4_NULL     = chimera_nfs4_null;
    shared->nfs_v4.recv_call_NFSPROC4_COMPOUND = chimera_nfs4_compound;

    nfs4_client_table_init(&shared->nfs4_shared_clients);

    return shared;
} /* nfs_server_init */

static void
nfs_server_destroy(void *data)
{
    struct chimera_server_nfs_shared *shared = data;

    nfs4_client_table_free(&shared->nfs4_shared_clients);

    free(shared);
} /* nfs_server_destroy */

static void *
nfs_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server_nfs_shared *shared = data;
    struct chimera_server_nfs_thread *thread;
    struct evpl_rpc2_program         *programs[3];

    programs[0] = &shared->nfs_v3.rpc2;
    programs[1] = &shared->nfs_v4.rpc2;
    programs[2] = &shared->nfs_v4_cb.rpc2;

    thread             = calloc(1, sizeof(*thread));
    thread->evpl       = evpl;
    thread->shared     = data;
    thread->rpc2_agent = evpl_rpc2_init(evpl);

    thread->endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 2049);

    thread->server = evpl_rpc2_listen(thread->rpc2_agent,
                                      EVPL_STREAM_SOCKET_TCP,
                                      thread->endpoint,
                                      programs,
                                      3,
                                      thread);
    return thread;
} /* nfs_server_thread_init */

static void
nfs_server_thread_destroy(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server_nfs_thread *thread = data;

    evpl_rpc2_destroy(thread->rpc2_agent);
    free(thread);
} /* nfs_server_thread_destroy */

struct chimera_server_protocol nfs_protocol = {
    .init           = nfs_server_init,
    .destroy        = nfs_server_destroy,
    .thread_init    = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
