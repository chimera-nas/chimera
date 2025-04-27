#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/resource.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "s3/s3.h"
#include "vfs/vfs.h"
#include "common/macros.h"
#include "server/server.h"

#define CHIMERA_SERVER_MAX_MODULES 64

struct chimera_server_config {
    int                           nfs_rdma;
    char                          nfs_rdma_hostname[256];
    int                           nfs_rdma_port;
    int                           core_threads;
    int                           delegation_threads;
    int                           cache_ttl;
    struct chimera_vfs_module_cfg modules[CHIMERA_SERVER_MAX_MODULES];
    int                           num_modules;
    int                           metrics_port;
};

struct chimera_server {
    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;
    struct evpl_threadpool             *pool;
    struct chimera_server_protocol     *protocols[2];
    void                               *protocol_private[2];
    void                               *s3_shared;
    int                                 num_protocols;
    int                                 threads_online;
    pthread_mutex_t                     lock;
};

struct chimera_thread {
    struct chimera_server     *server;
    struct chimera_vfs_thread *vfs_thread;
    void                      *protocol_private[2];
    struct evpl_timer          watchdog;
};

SYMBOL_EXPORT struct chimera_server_config *
chimera_server_config_init(void)
{
    struct chimera_server_config *config;

    config = calloc(1, sizeof(struct chimera_server_config));

    config->core_threads       = 16;
    config->delegation_threads = 64;
    config->nfs_rdma           = 0;

    config->cache_ttl = 60;

    strncpy(config->nfs_rdma_hostname, "0.0.0.0", sizeof(config->nfs_rdma_hostname));
    config->nfs_rdma_port = 20049;

    strncpy(config->modules[0].module_name, "root", sizeof(config->modules[0].module_name));
    config->modules[0].config_path[0] = '\0';
    config->modules[0].module_path[0] = '\0';

    strncpy(config->modules[1].module_name, "memfs", sizeof(config->modules[1].module_name));
    config->modules[1].config_path[0] = '\0';
    config->modules[1].module_path[0] = '\0';

    strncpy(config->modules[2].module_name, "linux", sizeof(config->modules[2].module_name));
    config->modules[2].config_path[0] = '\0';
    config->modules[2].module_path[0] = '\0';

    strncpy(config->modules[3].module_name, "io_uring", sizeof(config->modules[3].module_name));
    config->modules[3].config_path[0] = '\0';
    config->modules[3].module_path[0] = '\0';
    config->num_modules               = 4;

    config->metrics_port = 0;

    return config;
} /* chimera_server_config_init */

SYMBOL_EXPORT void
chimera_server_config_set_core_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->core_threads = threads;
} /* chimera_server_config_set_core_threads */

SYMBOL_EXPORT void
chimera_server_config_set_delegation_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->delegation_threads = threads;
} /* chimera_server_config_set_delegation_threads */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs_rdma = enable;
} /* chimera_server_config_set_nfs_rdma */

SYMBOL_EXPORT void
chimera_server_config_set_cache_ttl(
    struct chimera_server_config *config,
    int                           ttl)
{
    config->cache_ttl = ttl;
} /* chimera_server_config_set_cache_ttl */

SYMBOL_EXPORT int
chimera_server_config_get_cache_ttl(const struct chimera_server_config *config)
{
    return config->cache_ttl;
} /* chimera_server_config_get_cache_ttl */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_rdma(const struct chimera_server_config *config)
{
    return config->nfs_rdma;
} /* chimera_server_config_get_nfs_rdma */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma_hostname(
    struct chimera_server_config *config,
    const char                   *hostname)
{
    config->nfs_rdma = 1;
    strncpy(config->nfs_rdma_hostname, hostname, sizeof(config->nfs_rdma_hostname) - 1);
} /* chimera_server_config_set_nfs_rdma_hostname */

SYMBOL_EXPORT const char *
chimera_server_config_get_nfs_rdma_hostname(const struct chimera_server_config *config)
{
    if (!config->nfs_rdma) {
        return NULL;
    }

    return config->nfs_rdma_hostname;
} /* chimera_server_config_get_nfs_rdma_hostname */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_rdma_port = port;
} /* chimera_server_config_set_nfs_rdma_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_rdma_port(const struct chimera_server_config *config)
{
    return config->nfs_rdma_port;
} /* chimera_server_config_get_nfs_rdma_port */

SYMBOL_EXPORT void
chimera_server_config_add_module(
    struct chimera_server_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_path)
{
    struct chimera_vfs_module_cfg *module_cfg;

    module_cfg = &config->modules[config->num_modules];

    strncpy(module_cfg->module_name, module_name, sizeof(module_cfg->module_name));
    strncpy(module_cfg->module_path, module_path, sizeof(module_cfg->module_path));
    strncpy(module_cfg->config_path, config_path, sizeof(module_cfg->config_path));

    config->num_modules++;
} /* chimera_server_config_add_module */

SYMBOL_EXPORT void
chimera_server_config_set_metrics_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->metrics_port = port;
} /* chimera_server_config_set_metrics_port */

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_thread *thread = container_of(timer, struct chimera_thread, watchdog);

    chimera_vfs_watchdog(thread->vfs_thread);
} /* chimera_server_thread_wake */

static void *
chimera_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server *server = data;
    struct chimera_thread *thread;

    thread = calloc(1, sizeof(*thread));

    evpl_add_timer(evpl, &thread->watchdog, chimera_server_thread_wake, 1000000);

    thread->server = server;

    thread->vfs_thread = chimera_vfs_thread_init(evpl, server->vfs);

    for (int i = 0; i < server->num_protocols; i++) {
        thread->protocol_private[i] = server->protocols[i]->thread_init(evpl,
                                                                        thread->vfs_thread,
                                                                        server->
                                                                        protocol_private
                                                                        [i]);
    }

    pthread_mutex_lock(&server->lock);
    server->threads_online++;
    pthread_mutex_unlock(&server->lock);

    return thread;
} /* chimera_server_thread_init */

SYMBOL_EXPORT int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_path,
    const char            *module_name,
    const char            *module_path)
{
    return chimera_vfs_mount(server->vfs, share_path, module_name, module_path);
} /* chimera_server_create_share */

SYMBOL_EXPORT int
chimera_server_create_bucket(
    struct chimera_server *server,
    const char            *bucket_name,
    const char            *bucket_path)
{
    if (!server->s3_shared) {
        return -1;
    }

    chimera_s3_add_bucket(server->s3_shared, bucket_name, bucket_path);

    return 0;
} /* chimera_server_create_bucket */

static void
chimera_server_thread_shutdown(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->thread_destroy(thread->protocol_private[i]);
    }

    evpl_remove_timer(evpl, &thread->watchdog);
    chimera_vfs_thread_destroy(thread->vfs_thread);
    free(thread);
} /* chimera_server_create_share */

SYMBOL_EXPORT struct chimera_server *
chimera_server_init(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics)
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
    server->vfs = chimera_vfs_init(config->delegation_threads,
                                   config->modules,
                                   config->num_modules,
                                   config->cache_ttl,
                                   metrics);

    chimera_server_info("Initializing protocols...");
    server->protocols[server->num_protocols++] = &nfs_protocol;
    server->protocols[server->num_protocols++] = &s3_protocol;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(config, server->vfs, metrics);
    }

    server->s3_shared = server->protocol_private[1];

    return server;
} /* chimera_server_init */

SYMBOL_EXPORT void
chimera_server_start(struct chimera_server *server)
{
    int i;

    server->pool = evpl_threadpool_create(NULL,
                                          server->config->core_threads,
                                          chimera_server_thread_init,
                                          chimera_server_thread_shutdown,
                                          server);

    chimera_server_info("Waiting for %d threads to start...", server->config->core_threads);
    while (server->threads_online < server->config->core_threads) {
        usleep(100);
    }

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->start(server->protocol_private[i]);
    }

    chimera_server_info("Server is ready.");
} /* chimera_server_start */

SYMBOL_EXPORT void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    evpl_threadpool_destroy(server->pool);
    chimera_vfs_destroy(server->vfs);

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    free((void *) server->config);
    free(server);
} /* chimera_server_destroy */
