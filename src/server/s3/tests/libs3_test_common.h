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
#include <sys/statvfs.h>
#include <errno.h>
#include <jansson.h>
#include "server/server.h"
#include "common/logging.h"
#include "prometheus-c.h"

struct test_env {
    struct chimera_server     *server;
    char                       session_dir[256];
    int                        path_style;
    struct prometheus_metrics *metrics;
};

static inline void
libs3_test_init(
    struct test_env *env,
    char           **argv,
    int              argc)
{
    int                           opt, rc;
    extern char                  *optarg;
    const char                   *backend = "demofs";
    struct chimera_server_config *config;
    struct timespec               tv;

    chimera_log_init();

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

#ifndef CHIMERA_SANITIZE
    chimera_enable_crash_handler();
#endif /* ifndef CHIMERA_SANITIZE */

    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);

    clock_gettime(
        CLOCK_MONOTONIC,
        &tv);

    env->session_dir[0] = '\0';
    env->path_style     = 0;
    while ((opt = getopt(argc, argv, "b:p")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
            case 'p':
                env->path_style = 1;
                break;
        } /* switch */
    }

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/build/test/session_%d_%lu_%lu",
             getpid(), tv.tv_sec, tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env->session_dir);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    config = chimera_server_config_init();

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

        chimera_server_config_add_module(config, "demofs", "/build/test/demofs", demofs_cfg);
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

        chimera_server_config_add_module(config, "cairn", "/build/test/cairn", cairn_cfg);
    }

    env->server = chimera_server_init(config, env->metrics);

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

    chimera_server_create_bucket(env->server, "mybucket", "/share");

    chimera_server_start(env->server);
} /* libnfs_test_init */

static inline void
libs3_test_cleanup(
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

    chimera_server_destroy(env->server);

    prometheus_metrics_destroy(env->metrics);
} /* libnfs_test_cleanup */

static inline void
libs3_test_fail(struct test_env *env)
{
    fprintf(stderr, "Test failed\n");

    libs3_test_cleanup(env, 0);

    exit(EXIT_FAILURE);
} /* libnfs_test_fail */

static inline void
libs3_test_success(struct test_env *env)
{
    libs3_test_cleanup(env, 1);
} /* libs3_test_cleanup */