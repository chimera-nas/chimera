// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <jansson.h>
#include "client/client.h"
#include "server/server.h"
#include "common/logging.h"
#include "prometheus-c.h"
#include "evpl/evpl.h"

struct test_env {
    struct chimera_server        *server;
    struct chimera_client        *client;
    struct chimera_client_thread *client_thread;
    struct evpl                  *evpl;
    char                          session_dir[256];
    struct prometheus_metrics    *server_metrics;
    struct prometheus_metrics    *client_metrics;
    const char                   *backend;
    int                           use_nfs;
    int                           nfsvers;
};

static inline void
client_test_init(
    struct test_env *env,
    char           **argv,
    int              argc)
{
    int                           opt, rc;
    extern char                  *optarg;
    int                           nfsvers = 0;
    const char                   *backend = "memfs";
    struct chimera_server_config *server_config;
    struct chimera_client_config *client_config;
    struct timespec               tv;

    env->use_nfs = 0;
    env->nfsvers = 0;

    env->server_metrics = prometheus_metrics_create(NULL, NULL, 0);

    clock_gettime(
        CLOCK_MONOTONIC,
        &tv);

    env->session_dir[0] = '\0';

    while ((opt = getopt(argc, argv, "b:v:")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
            case 'v':
                nfsvers      = atoi(optarg);
                env->use_nfs = 1;
                env->nfsvers = nfsvers;
                break;
        } /* switch */
    }

    env->backend = backend;

    chimera_log_init();

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

#ifndef CHIMERA_SANITIZE
    chimera_enable_crash_handler();
#endif /* ifndef CHIMERA_SANITIZE */

    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/build/test/session_%d_%lu_%lu",
             getpid(), tv.tv_sec, tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env->session_dir);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    if (env->use_nfs) {
        server_config = chimera_server_config_init();

        if (strcmp(backend, "demofs") == 0) {
            char    demofs_cfg[4096];
            char    device_path[300];
            char   *json_str;
            json_t *cfg, *devices, *device;

            cfg     = json_object();
            devices = json_array();

            for (int i = 0; i < 10; ++i) {
                device = json_object();
                snprintf(device_path, sizeof(device_path), "%s/device-%d.img", env->session_dir, i);
                json_object_set_new(device, "type", json_string("io_uring"));
                json_object_set_new(device, "size", json_integer(1));
                json_object_set_new(device, "path", json_string(device_path));
                json_array_append_new(devices, device);

                int fd = open(device_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
                if (fd < 0) {
                    fprintf(stderr, "Failed to create device %s: %s\n", device_path, strerror(errno));
                    exit(EXIT_FAILURE);
                }

                rc = ftruncate(fd, 1024 * 1024 * 1024);

                if (rc < 0) {
                    fprintf(stderr, "Failed to truncate device %s: %s\n", device_path, strerror(errno));
                    exit(EXIT_FAILURE);
                }

                close(fd);
            }

            json_object_set_new(cfg, "devices", devices);
            json_str = json_dumps(cfg, JSON_COMPACT);
            snprintf(demofs_cfg, sizeof(demofs_cfg), "%s", json_str);
            free(json_str);
            json_decref(cfg);

            chimera_server_config_add_module(server_config, "demofs", "/build/test/demofs", demofs_cfg);
        } else if (strcmp(backend, "cairn") == 0) {
            char    cairn_cfg[4096];
            char   *json_str;
            json_t *cfg;

            cfg = json_object();
            json_object_set_new(cfg, "initialize", json_true());
            json_object_set_new(cfg, "path", json_string(env->session_dir));
            json_str = json_dumps(cfg, JSON_COMPACT);
            snprintf(cairn_cfg, sizeof(cairn_cfg), "%s", json_str);
            free(json_str);
            json_decref(cfg);

            chimera_server_config_add_module(server_config, "cairn", "/build/test/cairn", cairn_cfg);
        }

        env->server = chimera_server_init(server_config, env->server_metrics);

        if (strcmp(backend, "linux") == 0) {
            chimera_server_mount(env->server, "share", "linux", env->session_dir);

        } else if (strcmp(backend, "io_uring") == 0) {
            chimera_server_mount(env->server, "share", "io_uring", env->session_dir);

        } else if (strcmp(backend, "memfs") == 0) {
            chimera_server_mount(env->server, "share", "memfs", "/");

        } else if (strcmp(backend, "demofs") == 0) {
            chimera_server_mount(env->server, "share", "demofs", "/");

        } else if (strcmp(backend, "cairn") == 0) {
            chimera_server_mount(env->server, "share", "cairn", "/");
        } else {
            fprintf(stderr, "Unknown backend: %s\n", backend);
            exit(EXIT_FAILURE);
        }

        // Create NFSv3 server export entry
        chimera_server_create_export(env->server, "/share", "/share");

        chimera_server_start(env->server);
    } else {
        env->server = NULL;
    }

    env->client_metrics = prometheus_metrics_create(NULL, NULL, 0);

    env->evpl = evpl_create(NULL);

    client_config = chimera_client_config_init();

    if (!env->use_nfs) {
        if (strcmp(backend, "demofs") == 0) {
            char    demofs_cfg[4096];
            char    device_path[300];
            char   *json_str;
            json_t *cfg, *devices, *device;

            cfg     = json_object();
            devices = json_array();

            for (int i = 0; i < 10; ++i) {
                device = json_object();
                snprintf(device_path, sizeof(device_path), "%s/device-%d.img", env->session_dir, i);
                json_object_set_new(device, "type", json_string("io_uring"));
                json_object_set_new(device, "size", json_integer(1));
                json_object_set_new(device, "path", json_string(device_path));
                json_array_append_new(devices, device);

                int fd = open(device_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
                if (fd < 0) {
                    fprintf(stderr, "Failed to create device %s: %s\n", device_path, strerror(errno));
                    exit(EXIT_FAILURE);
                }

                rc = ftruncate(fd, 1024 * 1024 * 1024);

                if (rc < 0) {
                    fprintf(stderr, "Failed to truncate device %s: %s\n", device_path, strerror(errno));
                    exit(EXIT_FAILURE);
                }

                close(fd);
            }

            json_object_set_new(cfg, "devices", devices);
            json_str = json_dumps(cfg, JSON_COMPACT);
            snprintf(demofs_cfg, sizeof(demofs_cfg), "%s", json_str);
            free(json_str);
            json_decref(cfg);

            chimera_client_config_add_module(client_config, "demofs", "/build/test/demofs", demofs_cfg);
        } else if (strcmp(backend, "cairn") == 0) {
            char    cairn_cfg[4096];
            char   *json_str;
            json_t *cfg;

            cfg = json_object();
            json_object_set_new(cfg, "initialize", json_true());
            json_object_set_new(cfg, "path", json_string(env->session_dir));
            json_str = json_dumps(cfg, JSON_COMPACT);
            snprintf(cairn_cfg, sizeof(cairn_cfg), "%s", json_str);
            free(json_str);
            json_decref(cfg);

            chimera_client_config_add_module(client_config, "cairn", "/build/test/cairn", cairn_cfg);
        }
    }

    struct chimera_vfs_cred root_cred;
    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);
    env->client = chimera_client_init(client_config, &root_cred, env->client_metrics);

    env->client_thread = chimera_client_thread_init(env->evpl, env->client);

} /* client_test_init */

static inline void
client_test_cleanup(
    struct test_env *env,
    int              remove_session)
{
    int rc;

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        rc = system(cmd);
        if (rc < 0) {
            fprintf(stderr, "Failed to remove session directory %s: %s\n", env->session_dir, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    chimera_client_thread_shutdown(env->evpl, env->client_thread);
    chimera_destroy(env->client);
    if (env->server) {
        chimera_server_destroy(env->server);
    }
    evpl_destroy(env->evpl);

    prometheus_metrics_destroy(env->server_metrics);
    prometheus_metrics_destroy(env->client_metrics);
} /* client_test_cleanup */

static inline void
client_test_fail(struct test_env *env)
{
    fprintf(stderr, "Test failed\n");

    client_test_cleanup(env, 0);

    exit(EXIT_FAILURE);
} /* client_test_fail */

static inline void
client_test_success(struct test_env *env)
{
    client_test_cleanup(env, 1);
} /* client_test_success */

static inline void
client_test_mount(
    struct test_env         *env,
    const char              *mount_path,
    chimera_mount_callback_t callback,
    void                    *private_data)
{
    if (env->use_nfs) {
        char nfs_path[256];
        snprintf(nfs_path, sizeof(nfs_path), "127.0.0.1:/share");
        chimera_mount(env->client_thread, mount_path, "nfs", nfs_path, NULL, callback, private_data);
    } else {
        const char *module_path = "/";
        if (strcmp(env->backend, "linux") == 0 || strcmp(env->backend, "io_uring") == 0) {
            module_path = env->session_dir;
        }
        chimera_mount(env->client_thread, mount_path, env->backend, module_path, NULL, callback, private_data);
    }
} /* client_test_mount */

