#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>

#include "client.h"
#include "client_internal.h"
#include "vfs/vfs.h"

#define CHIMERA_CLIENT_MAX_MODULES 64

struct chimera_client_config {
    int                           core_threads;
    int                           delegation_threads;
    struct chimera_vfs_module_cfg modules[CHIMERA_CLIENT_MAX_MODULES];
    int                           num_modules;
};

struct chimera_client {
    const struct chimera_client_config *config;
    struct chimera_vfs                 *vfs;
    struct evpl_threadpool             *pool;
    int                                 threads_online;
    pthread_mutex_t                     lock;
};

struct chimera_client_thread {
    struct chimera_client     *client;
    struct chimera_vfs_thread *vfs_thread;
};


struct chimera_client_config *
chimera_client_config_init(void)
{
    struct chimera_client_config *config;

    config = calloc(1, sizeof(struct chimera_client_config));

    config->core_threads       = 16;
    config->delegation_threads = 64;

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

    return config;
} /* chimera_server_config_init */


void *
chimera_client_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_client        *client = data;
    struct chimera_client_thread *thread;

    thread = calloc(1, sizeof(struct chimera_client_thread));

    thread->client = client;

    return thread;
} /* chimera_client_thread_init */

void
chimera_client_thread_shutdown(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_client_thread *thread = data;

    free(thread);
} /* chimera_client_thread_shutdown */

struct chimera_client *
chimera_client_init(const struct chimera_client_config *config)
{
    struct chimera_client *client;
    struct rlimit          rl;

    client = calloc(1, sizeof(struct chimera_client));

    chimera_log_init();

    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        chimera_client_info("Effective file descriptor limit: %ld", rl.rlim_cur);
    } else {
        chimera_client_error("Failed to get file descriptor limit");
    }

    client->config = config;

    pthread_mutex_init(&client->lock, NULL);

    chimera_client_info("Initializing VFS...");

    client->vfs = chimera_vfs_init(config->delegation_threads,
                                   config->modules,
                                   config->num_modules);

    client->pool = evpl_threadpool_create(NULL,
                                          client->config->core_threads,
                                          chimera_client_thread_init,
                                          chimera_client_thread_shutdown,
                                          client);

    chimera_client_info("Waiting for %d threads to start...", client->config->core_threads);

    while (client->threads_online < client->config->core_threads) {
        usleep(100);
    }

    chimera_client_info("Client is ready.");
    return client;
} /* chimera_client_init */

void
chimera_client_destroy(struct chimera_client *client)
{
    free(client);
} /* chimera_client_destroy */
