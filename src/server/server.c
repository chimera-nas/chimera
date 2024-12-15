#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/resource.h>

#include "core/evpl.h"
#include "thread/thread.h"
#include "server.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "vfs/vfs.h"

struct chimera_server_config {
    int  rdma;
    char rdma_hostname[256];
    int  core_threads;
};

struct chimera_server {
    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;
    struct evpl_threadpool             *pool;
    struct chimera_server_protocol     *protocols[2];
    void                               *protocol_private[2];
    int                                 num_protocols;
    int                                 threads_online;
    pthread_mutex_t                     lock;
};

struct chimera_thread {
    struct chimera_server     *server;
    struct chimera_vfs_thread *vfs_thread;
    void                      *protocol_private[2];
};

struct chimera_server_config *
chimera_server_config_init(void)
{
    struct chimera_server_config *config;

    config = calloc(1, sizeof(struct chimera_server_config));

    config->core_threads = 16;

    return config;
} /* chimera_server_config_init */

void
chimera_server_config_set_rdma(
    struct chimera_server_config *config,
    const char                   *hostname)
{
    config->rdma = 1;
    strncpy(config->rdma_hostname, hostname, sizeof(config->rdma_hostname));
} /* chimera_server_config_set_rdma */

const char *
chimera_server_config_get_rdma(const struct chimera_server_config *config)
{
    if (!config->rdma) {
        return NULL;
    }

    return config->rdma_hostname;
} /* chimera_server_config_get_rdma */

static void *
chimera_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server *server = data;
    struct chimera_thread *thread;

    thread = calloc(1, sizeof(*thread));

    thread->server = server;

    thread->vfs_thread = chimera_vfs_thread_init(evpl, server->vfs);

    for (int i = 0; i < server->num_protocols; i++) {
        thread->protocol_private[i] = server->protocols[i]->thread_init(evpl,
                                                                        server->
                                                                        protocol_private
                                                                        [i]);
    }

    pthread_mutex_lock(&server->lock);
    server->threads_online++;
    pthread_mutex_unlock(&server->lock);

    return thread;
} /* chimera_server_thread_init */

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *module_name,
    const char            *share_path,
    const char            *module_path)
{
    return chimera_vfs_create_share(server->vfs, module_name, share_path,
                                    module_path);
} /* chimera_server_create_share */

static void
chimera_server_thread_destroy(void *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->thread_destroy(thread->protocol_private[i]);
    }

    chimera_vfs_thread_destroy(thread->vfs_thread);
    free(thread);
} /* chimera_server_thread_destroy */

struct chimera_server *
chimera_server_init(const struct chimera_server_config *config)
{
    struct chimera_server *server;
    int                    i;
    struct rlimit          rl;

    if (!config) {
        config = chimera_server_config_init();
    }

    chimera_log_init();

    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        chimera_server_info("Effective file descriptor limit: %ld", rl.rlim_cur);
    } else {
        chimera_server_error("Failed to get file descriptor limit");
    }

    server = calloc(1, sizeof(*server));

    server->config = config;

    pthread_mutex_init(&server->lock, NULL);

    chimera_server_info("Initializing VFS...");
    server->vfs = chimera_vfs_init();

    chimera_server_info("Initializing NFS protocol...");
    server->protocols[server->num_protocols++] = &nfs_protocol;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(config, server->vfs);
    }

    server->pool = evpl_threadpool_create(config->core_threads,
                                          chimera_server_thread_init, NULL,
                                          NULL,
                                          chimera_server_thread_destroy,
                                          -1,
                                          server);

    chimera_server_info("Waiting for threads to start...");
    while (server->threads_online < config->core_threads) {
        usleep(100);
    }

    chimera_server_info("Server is ready.");
    return server;
} /* chimera_server_init */

void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    evpl_threadpool_destroy(server->pool);

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    chimera_vfs_destroy(server->vfs);

    free((void *) server->config);
    free(server);
} /* chimera_server_destroy */
