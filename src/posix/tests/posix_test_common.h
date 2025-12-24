// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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
#include "posix/posix.h"
#include "client/client.h"
#include "common/logging.h"
#include "prometheus-c.h"

struct posix_test_env {
    struct chimera_posix_client *posix;
    struct prometheus_metrics   *metrics;
    char                         session_dir[256];
    const char                  *backend;
};

static inline void
posix_test_init(
    struct posix_test_env *env,
    char                 **argv,
    int                    argc)
{
    int                           opt, rc;
    extern char                  *optarg;
    const char                   *backend = "memfs";
    struct chimera_client_config *config;
    struct timespec               tv;

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);

    clock_gettime(CLOCK_MONOTONIC, &tv);

    env->session_dir[0] = '\0';

    while ((opt = getopt(argc, argv, "b:")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
        } /* switch */
    }

    env->backend = backend;

    chimera_log_init();

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

#ifndef CHIMERA_SANITIZE
    chimera_enable_crash_handler();
#endif /* ifndef CHIMERA_SANITIZE */

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/build/test/posix_session_%d_%lu_%lu",
             getpid(), tv.tv_sec, tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env->session_dir);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    config = chimera_client_config_init();

    if (strcmp(backend, "demofs") == 0) {
        char    demofs_cfg[300];
        char    device_path[300];
        json_t *cfg, *devices, *device;
        snprintf(demofs_cfg, sizeof(demofs_cfg), "%s/demofs.json", env->session_dir);

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

            rc = ftruncate(fd, 256 * 1024 * 1024 * 1024UL);

            if (rc < 0) {
                fprintf(stderr, "Failed to truncate device %s: %s\n", device_path, strerror(errno));
                exit(EXIT_FAILURE);
            }

            close(fd);
        }

        json_object_set_new(cfg, "devices", devices);
        json_dump_file(cfg, demofs_cfg, 0);
        json_decref(cfg);

        chimera_client_config_add_module(config, "demofs", "/build/test/demofs", demofs_cfg);
    } else if (strcmp(backend, "cairn") == 0) {
        char    cairn_cfgfile[300];
        json_t *cfg;

        snprintf(cairn_cfgfile, sizeof(cairn_cfgfile),
                 "%s/cairn.cfg", env->session_dir);

        cfg = json_object();
        json_object_set_new(cfg, "initialize", json_true());
        json_object_set_new(cfg, "path", json_string(env->session_dir));
        json_dump_file(cfg, cairn_cfgfile, 0);
        json_decref(cfg);

        chimera_client_config_add_module(config, "cairn", "/build/test/cairn", cairn_cfgfile);
    }

    env->posix = chimera_posix_init(config, env->metrics);

    if (!env->posix) {
        fprintf(stderr, "Failed to initialize POSIX client\n");
        exit(EXIT_FAILURE);
    }
} /* posix_test_init */

static inline void
posix_test_cleanup(
    struct posix_test_env *env,
    int                    remove_session)
{
    int rc;

    chimera_posix_shutdown();

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        rc = system(cmd);
        if (rc < 0) {
            fprintf(stderr, "Failed to remove session directory %s: %s\n", env->session_dir, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    prometheus_metrics_destroy(env->metrics);
} /* posix_test_cleanup */

static inline void
posix_test_fail(struct posix_test_env *env)
{
    fprintf(stderr, "Test failed\n");

    posix_test_cleanup(env, 0);

    exit(EXIT_FAILURE);
} /* posix_test_fail */

static inline void
posix_test_success(struct posix_test_env *env)
{
    posix_test_cleanup(env, 1);
} /* posix_test_success */

static inline int
posix_test_mount(struct posix_test_env *env)
{
    const char *module_path = "/";

    if (strcmp(env->backend, "linux") == 0 || strcmp(env->backend, "io_uring") == 0) {
        module_path = env->session_dir;
    }

    return chimera_posix_mount("/test", env->backend, module_path);
} /* posix_test_mount */

static inline int
posix_test_umount(void)
{
    return chimera_posix_umount("/test");
} /* posix_test_umount */
