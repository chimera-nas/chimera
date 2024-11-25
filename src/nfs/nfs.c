#include <stdio.h>
#include "nfs.h"
#include "server/protocol.h"
#include "vfs/vfs.h"
#include "rpc2/rpc2.h"
#include "nfs3_procs.h"
#include "nfs4_procs.h"
#include "nfs_mount.h"
#include "nfs_portmap.h"
#include "nfs_common.h"
#include "nfs_internal.h"

#include "common/logging.h"
#include "uthash/utlist.h"

static void *
nfs_server_init(struct chimera_vfs *vfs)
{
    struct chimera_server_nfs_shared *shared;

    shared = calloc(1, sizeof(*shared));

    shared->vfs = vfs;

    NFS_PORTMAP_V2_init(&shared->portmap_v2);
    NFS_MOUNT_V3_init(&shared->mount_v3);
    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    shared->mount_v3.recv_call_MOUNTPROC3_NULL    = chimera_nfs_mount_null;
    shared->mount_v3.recv_call_MOUNTPROC3_MNT     = chimera_nfs_mount_mnt;
    shared->mount_v3.recv_call_MOUNTPROC3_DUMP    = chimera_nfs_mount_dump;
    shared->mount_v3.recv_call_MOUNTPROC3_UMNT    = chimera_nfs_mount_umnt;
    shared->mount_v3.recv_call_MOUNTPROC3_UMNTALL = chimera_nfs_mount_umntall;
    shared->mount_v3.recv_call_MOUNTPROC3_EXPORT  = chimera_nfs_mount_export;

    shared->portmap_v2.recv_call_PMAPPROC_NULL    = chimera_portmap_null;
    shared->portmap_v2.recv_call_PMAPPROC_GETPORT = chimera_portmap_getport;

    shared->nfs_v3.recv_call_NFSPROC3_NULL        = chimera_nfs3_null;
    shared->nfs_v3.recv_call_NFSPROC3_GETATTR     = chimera_nfs3_getattr;
    shared->nfs_v3.recv_call_NFSPROC3_SETATTR     = chimera_nfs3_setattr;
    shared->nfs_v3.recv_call_NFSPROC3_LOOKUP      = chimera_nfs3_lookup;
    shared->nfs_v3.recv_call_NFSPROC3_ACCESS      = chimera_nfs3_access;
    shared->nfs_v3.recv_call_NFSPROC3_READLINK    = chimera_nfs3_readlink;
    shared->nfs_v3.recv_call_NFSPROC3_READ        = chimera_nfs3_read;
    shared->nfs_v3.recv_call_NFSPROC3_WRITE       = chimera_nfs3_write;
    shared->nfs_v3.recv_call_NFSPROC3_CREATE      = chimera_nfs3_create;
    shared->nfs_v3.recv_call_NFSPROC3_REMOVE      = chimera_nfs3_remove;
    shared->nfs_v3.recv_call_NFSPROC3_RENAME      = chimera_nfs3_rename;
    shared->nfs_v3.recv_call_NFSPROC3_LINK        = chimera_nfs3_link;
    shared->nfs_v3.recv_call_NFSPROC3_SYMLINK     = chimera_nfs3_symlink;
    shared->nfs_v3.recv_call_NFSPROC3_READDIR     = chimera_nfs3_readdir;
    shared->nfs_v3.recv_call_NFSPROC3_READDIRPLUS = chimera_nfs3_readdirplus;
    shared->nfs_v3.recv_call_NFSPROC3_FSSTAT      = chimera_nfs3_fsstat;
    shared->nfs_v3.recv_call_NFSPROC3_FSINFO      = chimera_nfs3_fsinfo;
    shared->nfs_v3.recv_call_NFSPROC3_PATHCONF    = chimera_nfs3_pathconf;
    shared->nfs_v3.recv_call_NFSPROC3_COMMIT      = chimera_nfs3_commit;

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

    thread->vfs = chimera_vfs_thread_init(evpl, shared->vfs);

    thread->nfs_endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 2049);

    thread->nfs_server = evpl_rpc2_listen(thread->rpc2_agent,
                                          EVPL_STREAM_SOCKET_TCP,
                                          thread->nfs_endpoint,
                                          programs,
                                          3,
                                          thread);

    programs[0] = &shared->mount_v3.rpc2;

    thread->mount_endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 20048);

    thread->mount_server = evpl_rpc2_listen(thread->rpc2_agent,
                                            EVPL_STREAM_SOCKET_TCP,
                                            thread->mount_endpoint,
                                            programs,
                                            1,
                                            thread);

    programs[0] = &shared->portmap_v2.rpc2;

    thread->portmap_endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 111);

    thread->portmap_server = evpl_rpc2_listen(thread->rpc2_agent,
                                              EVPL_STREAM_SOCKET_TCP,
                                              thread->portmap_endpoint,
                                              programs,
                                              1,
                                              thread);

    return thread;
} /* nfs_server_thread_init */

static void
nfs_server_thread_destroy(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server_nfs_thread *thread = data;
    struct nfs_request               *req;

    chimera_vfs_thread_destroy(thread->vfs);

    evpl_rpc2_server_destroy(thread->rpc2_agent, thread->nfs_server);
    evpl_rpc2_server_destroy(thread->rpc2_agent, thread->mount_server);
    evpl_rpc2_server_destroy(thread->rpc2_agent, thread->portmap_server);
    evpl_rpc2_destroy(thread->rpc2_agent);

    while (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
        free(req);
    }

    free(thread);
} /* nfs_server_thread_destroy */

struct chimera_server_protocol nfs_protocol = {
    .init           = nfs_server_init,
    .destroy        = nfs_server_destroy,
    .thread_init    = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
