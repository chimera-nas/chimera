// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "smb2/smb2.h"
#include "smb2/libsmb2.h"
#include <errno.h>
#include <fcntl.h>
#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

static void
auth_fn(
    const char *server,
    const char *share,
    char       *workgroup,
    int         maxlen_workgroup,
    char       *username,
    int         maxlen_username,
    char       *password,
    int         maxlen_password)
{
    strncpy(workgroup, "", maxlen_workgroup);
    strncpy(username, "anonymous", maxlen_username);
    strncpy(password, "", maxlen_password);
} /* auth_fn */

struct test_env {
    struct smb2_context       *ctx;
    struct chimera_server     *server;
    char                       session_dir[256];
    struct prometheus_metrics *metrics;
};

static inline void
libsmb2_test_init(
    struct test_env *env,
    char           **argv,
    int              argc)
{
    int                           opt, rc;
    extern char                  *optarg;
    const char                   *backend = "linux";
    struct chimera_server_config *config;
    struct timespec               tv;
    char                          ntlm_path[1024];
    FILE                         *ntlm_pass;

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

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    chimera_enable_crash_handler();

    evpl_set_log_fn(chimera_vlog);

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/build/test/session_%d_%lu_%lu", getpid(), tv.tv_sec, tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env->session_dir);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    snprintf(ntlm_path, sizeof(ntlm_path), "%s/ntlm_pass.txt", env->session_dir);

    ntlm_pass = fopen(ntlm_path, "w");

    fprintf(ntlm_pass, "WORKGROUP:myuser:mypassword\n");

    fclose(ntlm_pass);

    setenv("NTLM_USER_FILE", ntlm_path, 1);

    config = chimera_server_config_init();

    if (strcmp(backend, "demofs") == 0) {
        char    demofs_cfg[300];
        char    device_path[300];
        json_t *cfg, *devices, *device;
        snprintf(demofs_cfg, sizeof(demofs_cfg), "%s/demofs.json",
                 env->session_dir);

        cfg     = json_object();
        devices = json_array();

        for (int i = 0; i < 10; ++i) {
            device = json_object();
            snprintf(device_path, sizeof(device_path), "%s/device-%d.img",
                     env->session_dir, i);
            json_object_set_new(device, "type", json_string("io_uring"));
            json_object_set_new(device, "size", json_integer(1));
            json_object_set_new(device, "path", json_string(device_path));
            json_array_append_new(devices, device);

            int fd = open(device_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
            if (fd < 0) {
                fprintf(stderr, "Failed to create device %s: %s\n", device_path,
                        strerror(errno));
                exit(EXIT_FAILURE);
            }

            rc = ftruncate(fd, 1024 * 1024 * 1024);

            if (rc < 0) {
                fprintf(stderr, "Failed to truncate device %s: %s\n", device_path,
                        strerror(errno));
                exit(EXIT_FAILURE);
            }

            close(fd);
        }

        json_object_set_new(cfg, "devices", devices);
        json_dump_file(cfg, demofs_cfg, 0);
        json_decref(cfg);

        chimera_server_config_add_module(config, "demofs", "/build/test/demofs",
                                         demofs_cfg);
    } else if (strcmp(backend, "cairn") == 0) {
        char    cairn_cfgfile[300];
        json_t *cfg;

        snprintf(cairn_cfgfile, sizeof(cairn_cfgfile), "%s/cairn.cfg",
                 env->session_dir);

        cfg = json_object();
        json_object_set_new(cfg, "initialize", json_true());
        json_object_set_new(cfg, "path", json_string(env->session_dir));
        json_dump_file(cfg, cairn_cfgfile, 0);
        json_decref(cfg);

        chimera_server_config_add_module(config, "cairn", "/build/test/cairn",
                                         cairn_cfgfile);
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

    chimera_server_start(env->server);

    chimera_server_create_share(env->server, "share", "share");

    env->ctx = smb2_init_context();

    if (env->ctx == NULL) {
        fprintf(stderr, "Failed to init context\n");
        exit(EXIT_FAILURE);
    }

    smb2_set_security_mode(env->ctx, SMB2_NEGOTIATE_SIGNING_ENABLED);
    smb2_set_authentication(env->ctx, SMB2_SEC_NTLMSSP);

    smb2_set_user(env->ctx, "myuser");
    smb2_set_password(env->ctx, "mypassword");
    smb2_set_domain(env->ctx, "WORKGROUP");

    if (smb2_connect_share(env->ctx, "localhost", "share", "myuser") != 0) {
        fprintf(stderr, "smb2_connect_share failed. %s\n",
                smb2_get_error(env->ctx));
        exit(EXIT_FAILURE);
    }

} /* libsmbclient_test_init */

static inline void
libsmb2_test_cleanup(
    struct test_env *env,
    int              remove_session)
{
    int rc;

    rc = smb2_disconnect_share(env->ctx);

    if (rc < 0) {
        fprintf(stderr, "Failed to disconnect share: %s\n",
                smb2_get_error(env->ctx));
    }

    smb2_destroy_context(env->ctx);

    chimera_server_destroy(env->server);

    prometheus_metrics_destroy(env->metrics);

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);

        rc = system(cmd);

        if (rc < 0) {
            fprintf(stderr, "Failed to remove session directory %s: %s\n",
                    env->session_dir, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
} /* libsmbclient_test_cleanup */

static inline void
libsmb2_test_fail(struct test_env *env)
{
    fprintf(stderr, "Test failed\n");

    libsmb2_test_cleanup(env, 0);

    exit(EXIT_FAILURE);
} /* libsmbclient_test_fail */

static inline void
libsmb2_test_success(struct test_env *env)
{
    libsmb2_test_cleanup(env, 1);
} /* libsmb2_test_cleanup */