// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>
#include <utlist.h>

#include <jansson.h>

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
                                   config->kv_module,
                                   config->cache_ttl,
                                   metrics);

    /* Initialize the root file handle after VFS is initialized */
    chimera_vfs_get_root_fh(client->root_fh, &client->root_fh_len);

    return client;
} /* chimera_client_init */

SYMBOL_EXPORT int
chimera_client_add_user(
    struct chimera_client *client,
    const char            *username,
    const char            *password,
    const char            *smbpasswd,
    const char            *sid,
    uint32_t               uid,
    uint32_t               gid,
    uint32_t               ngids,
    const uint32_t        *gids,
    int                    pinned)
{
    return chimera_vfs_add_user(client->vfs, username, password, smbpasswd, sid,
                                uid, gid, ngids, gids, pinned);
} /* chimera_client_add_user */

SYMBOL_EXPORT struct chimera_client *
chimera_client_init_json(
    const char                    *config_path,
    const struct chimera_vfs_cred *cred,
    struct prometheus_metrics     *metrics)
{
    struct chimera_client_config *config;
    struct chimera_client        *client;
    json_t                       *root, *config_section, *vfs_modules, *users;
    json_error_t                  error;

    config = chimera_client_config_init();

    if (!config) {
        return NULL;
    }

    root = json_load_file(config_path, 0, &error);

    if (!root) {
        fprintf(stderr, "Failed to load client config: %s\n", error.text);
        free(config);
        return NULL;
    }

    config_section = json_object_get(root, "config");

    if (config_section && json_is_object(config_section)) {
        json_t *val;

        val = json_object_get(config_section, "core_threads");
        if (val && json_is_integer(val)) {
            config->core_threads = json_integer_value(val);
        }

        val = json_object_get(config_section, "delegation_threads");
        if (val && json_is_integer(val)) {
            config->delegation_threads = json_integer_value(val);
        }

        val = json_object_get(config_section, "cache_ttl");
        if (val && json_is_integer(val)) {
            config->cache_ttl = json_integer_value(val);
        }

        val = json_object_get(config_section, "max_fds");
        if (val && json_is_integer(val)) {
            config->max_fds = json_integer_value(val);
        }

        val = json_object_get(config_section, "kv_module");
        if (val && json_is_string(val)) {
            strncpy(config->kv_module, json_string_value(val), sizeof(config->kv_module) - 1);
            config->kv_module[sizeof(config->kv_module) - 1] = '\0';
        }

        vfs_modules = json_object_get(config_section, "vfs");
        if (vfs_modules && json_is_object(vfs_modules)) {
            const char *module_name;
            json_t     *module;
            json_object_foreach(vfs_modules, module_name, module)
            {
                const char *path       = json_string_value(json_object_get(module, "path"));
                const char *mod_config = json_string_value(json_object_get(module, "config"));

                chimera_client_config_add_module(config, module_name,
                                                 path ? path : "",
                                                 mod_config ? mod_config : "");
            }
        }
    }

    client = chimera_client_init(config, cred, metrics);

    if (!client) {
        free(config);
        json_decref(root);
        return NULL;
    }

    users = json_object_get(root, "users");
    if (users && json_is_array(users)) {
        json_t *user_entry;
        size_t  user_idx;

        json_array_foreach(users, user_idx, user_entry)
        {
            const char *username  = json_string_value(json_object_get(user_entry, "username"));
            const char *password  = json_string_value(json_object_get(user_entry, "password"));
            const char *smbpasswd = json_string_value(json_object_get(user_entry, "smbpasswd"));
            int         u_uid     = json_integer_value(json_object_get(user_entry, "uid"));
            int         u_gid     = json_integer_value(json_object_get(user_entry, "gid"));
            uint32_t    user_gids[CHIMERA_VFS_CRED_MAX_GIDS];
            uint32_t    ngids      = 0;
            json_t     *gids_array = json_object_get(user_entry, "gids");

            if (gids_array && json_is_array(gids_array)) {
                json_t *gid_val;
                size_t  gid_idx;
                json_array_foreach(gids_array, gid_idx, gid_val)
                {
                    if (ngids < CHIMERA_VFS_CRED_MAX_GIDS) {
                        user_gids[ngids++] = json_integer_value(gid_val);
                    }
                }
            }

            if (!username) {
                continue;
            }

            chimera_client_add_user(client, username,
                                    password ? password : "",
                                    smbpasswd ? smbpasswd : "",
                                    NULL,  // SID - synthesized for builtin users
                                    u_uid, u_gid, ngids, user_gids, 1);
        }
    }

    json_decref(root);

    return client;
} /* chimera_client_init_json */

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
