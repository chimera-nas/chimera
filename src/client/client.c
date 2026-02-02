// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>
#include <utlist.h>

#include "client.h"
#include "client_internal.h"
#include "common/macros.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "prometheus-c.h"

SYMBOL_EXPORT struct chimera_client_config *
chimera_client_config_init(void)
{
    struct chimera_client_config *config;

    config = calloc(1, sizeof(struct chimera_client_config));

    config->core_threads       = 16;
    config->delegation_threads = 64;
    config->cache_ttl          = 60;
    config->max_fds            = 1024;

    strncpy(config->modules[0].module_name, "root", sizeof(config->modules[0].module_name));
    config->modules[0].config_data[0] = '\0';
    config->modules[0].module_path[0] = '\0';

    strncpy(config->modules[1].module_name, "nfs", sizeof(config->modules[1].module_name));
    config->modules[1].config_data[0] = '\0';
    config->modules[1].module_path[0] = '\0';

    strncpy(config->modules[2].module_name, "memfs", sizeof(config->modules[2].module_name));
    config->modules[2].config_data[0] = '\0';
    config->modules[2].module_path[0] = '\0';

    strncpy(config->modules[3].module_name, "linux", sizeof(config->modules[3].module_name));
    config->modules[3].config_data[0] = '\0';
    config->modules[3].module_path[0] = '\0';

    config->num_modules = 4;

#ifdef HAVE_IO_URING
    strncpy(config->modules[4].module_name, "io_uring", sizeof(config->modules[4].module_name));
    config->modules[4].config_data[0] = '\0';
    config->modules[4].module_path[0] = '\0';

    config->num_modules = 5;
#endif /* ifdef HAVE_IO_URING */

    return config;
} /* chimera_server_config_init */


SYMBOL_EXPORT void
chimera_client_config_add_module(
    struct chimera_client_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_data)
{
    struct chimera_vfs_module_cfg *module_cfg;

    module_cfg = &config->modules[config->num_modules];

    strncpy(module_cfg->module_name, module_name, sizeof(module_cfg->module_name));
    strncpy(module_cfg->module_path, module_path, sizeof(module_cfg->module_path));
    strncpy(module_cfg->config_data, config_data, sizeof(module_cfg->config_data));

    config->num_modules++;
} /* chimera_client_config_add_module */

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
    const struct chimera_vfs_cred      *cred,
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

    /* Copy the credentials into the client structure */
    client->cred = *cred;

    chimera_client_info("Initializing VFS...");

    client->vfs = chimera_vfs_init(config->delegation_threads,
                                   config->modules,
                                   config->num_modules,
                                   config->cache_ttl,
                                   metrics);

    /* Initialize the root file handle after VFS is initialized */
    chimera_vfs_get_root_fh(client->root_fh, &client->root_fh_len);

    return client;
} /* chimera_client_init */

SYMBOL_EXPORT void
chimera_destroy(struct chimera_client *client)
{
    chimera_vfs_destroy(client->vfs);

    free((void *) client->config);

    free(client);
} /* chimera_client_destroy */


SYMBOL_EXPORT void
chimera_drain(struct chimera_client_thread *thread)
{
    chimera_vfs_thread_drain(thread->vfs_thread);
} /* chimera_drain */
