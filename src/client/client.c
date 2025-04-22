#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>

#include "uthash/utlist.h"

#include "client.h"
#include "client_internal.h"
#include "common/macros.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "prometheus-c.h"
const uint8_t root_fh[1] = { CHIMERA_VFS_FH_MAGIC_ROOT };

SYMBOL_EXPORT struct chimera_client_config *
chimera_client_config_init(void)
{
    struct chimera_client_config *config;

    config = calloc(1, sizeof(struct chimera_client_config));

    config->core_threads       = 16;
    config->delegation_threads = 64;
    config->cache_ttl          = 60;

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


SYMBOL_EXPORT struct chimera_client_thread *
chimera_client_thread_init(
    struct evpl           *evpl,
    struct chimera_client *client)
{
    struct chimera_client_thread *thread;

    thread = calloc(1, sizeof(struct chimera_client_thread));

    thread->client = client;

    thread->vfs_thread = chimera_vfs_thread_init(evpl, client->vfs);

    return thread;
} /* chimera_client_thread_init */

SYMBOL_EXPORT void
chimera_client_thread_shutdown(
    struct evpl                  *evpl,
    struct chimera_client_thread *thread)
{
    struct chimera_client_request *request;

    while (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
        free(request);
    }

    chimera_vfs_thread_destroy(thread->vfs_thread);

    free(thread);
} /* chimera_client_thread_shutdown */


SYMBOL_EXPORT struct chimera_client *
chimera_client_init(
    const struct chimera_client_config *config,
    struct prometheus_metrics          *metrics)
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

    chimera_client_info("Initializing VFS...");

    client->vfs = chimera_vfs_init(config->delegation_threads,
                                   config->modules,
                                   config->num_modules,
                                   config->cache_ttl,
                                   metrics);

    return client;
} /* chimera_client_init */

SYMBOL_EXPORT void
chimera_client_destroy(struct chimera_client *client)
{
    chimera_vfs_destroy(client->vfs);

    free((void *) client->config);

    free(client);
} /* chimera_client_destroy */

SYMBOL_EXPORT int
chimera_client_mount(
    struct chimera_client *client,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path)
{
    return chimera_vfs_mount(client->vfs, mount_path, module_name, module_path);
} /* chimera_client_mount */

SYMBOL_EXPORT int
chimera_client_umount(
    struct chimera_client *client,
    const char            *mount_path)
{
    return chimera_vfs_umount(client->vfs, mount_path);
} /* chimera_client_umount */