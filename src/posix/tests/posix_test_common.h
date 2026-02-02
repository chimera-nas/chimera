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
#include "posix/posix.h"
#include "client/client.h"
#include "server/server.h"
#include "common/logging.h"
#include "prometheus-c.h"

struct posix_test_env {
    struct chimera_posix_client *posix;
    struct chimera_server       *server;       // For NFS backend tests
    struct prometheus_metrics   *metrics;
    char                         session_dir[256];
    const char                  *backend;
    const char                  *nfs_backend;  // Actual backend behind NFS (e.g., "memfs")
    int                          nfs_version;  // 3 or 4, 0 if not NFS
    int                          use_nfs_rdma; // 1 if using NFS over RDMA (TCP-RDMA)
};

// Helper to parse backend string for NFS backends (e.g., "nfs3_memfs" -> version=3, backend="memfs")
// Returns 1 if NFS backend, 0 otherwise. Also sets use_rdma if backend is nfs3rdma_*
static inline int
posix_test_parse_nfs_backend(
    const char  *backend,
    int         *nfs_version,
    const char **nfs_backend,
    int         *use_rdma)
{
    *use_rdma = 0;
    if (strncmp(backend, "nfs3rdma_", 9) == 0) {
        *nfs_version = 3;
        *nfs_backend = backend + 9;
        *use_rdma    = 1;
        return 1;
    } else if (strncmp(backend, "nfs3_", 5) == 0) {
        *nfs_version = 3;
        *nfs_backend = backend + 5;
        return 1;
    } else if (strncmp(backend, "nfs4_", 5) == 0) {
        *nfs_version = 4;
        *nfs_backend = backend + 5;
        return 1;
    }
    *nfs_version = 0;
    *nfs_backend = NULL;
    return 0;
} // posix_test_parse_nfs_backend

// Helper to configure demofs backend
static inline void
posix_test_configure_demofs(
    const char *session_dir,
    char       *demofs_cfg,
    size_t      demofs_cfg_size)
{
    char    device_path[300];
    json_t *cfg, *devices, *device;
    int     rc;
    char   *json_str;

    cfg     = json_object();
    devices = json_array();

    for (int i = 0; i < 10; ++i) {
        device = json_object();
        snprintf(device_path, sizeof(device_path), "%s/device-%d.img", session_dir, i);
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
    json_str = json_dumps(cfg, JSON_COMPACT);
    snprintf(demofs_cfg, demofs_cfg_size, "%s", json_str);
    free(json_str);
    json_decref(cfg);
} // posix_test_configure_demofs // posix_test_configure_demofs

// Helper to configure cairn backend
static inline void
posix_test_configure_cairn(
    const char *session_dir,
    char       *cairn_cfg,
    size_t      cairn_cfg_size)
{
    json_t *cfg;
    char   *json_str;

    cfg = json_object();
    json_object_set_new(cfg, "initialize", json_true());
    json_object_set_new(cfg, "path", json_string(session_dir));
    json_str = json_dumps(cfg, JSON_COMPACT);
    snprintf(cairn_cfg, cairn_cfg_size, "%s", json_str);
    free(json_str);
    json_decref(cfg);
} // posix_test_configure_cairn // posix_test_configure_cairn

static inline void
posix_test_init(
    struct posix_test_env *env,
    char                 **argv,
    int                    argc)
{
    int                           opt;
    extern char                  *optarg;
    const char                   *backend = "memfs";
    struct chimera_client_config *client_config;
    struct chimera_server_config *server_config;
    struct timespec               tv;
    int                           is_nfs;
    const char                   *nfs_backend_name;
    int                           nfs_version;
    int                           use_nfs_rdma;

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);
    env->server  = NULL;

    clock_gettime(CLOCK_MONOTONIC, &tv);

    env->session_dir[0] = '\0';

    /* Suppress error messages for options we don't handle here -
     * individual tests may have additional options they parse after.
     * Use "+" prefix to stop at first non-option (POSIX behavior),
     * preventing argv permutation that would break subsequent getopt calls. */
    opterr = 0;
    while ((opt = getopt(argc, argv, "+b:")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
        } /* switch */
    }
    opterr = 1;  /* Re-enable for subsequent getopt calls */
    optind = 1;  /* Reset for next getopt caller */

    env->backend = backend;

    // Check if this is an NFS backend (e.g., "nfs3_memfs" or "nfs3rdma_memfs")
    is_nfs            = posix_test_parse_nfs_backend(backend, &nfs_version, &nfs_backend_name, &use_nfs_rdma);
    env->nfs_version  = nfs_version;
    env->nfs_backend  = nfs_backend_name;
    env->use_nfs_rdma = use_nfs_rdma;

    chimera_log_init();

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

#ifndef CHIMERA_SANITIZE
    chimera_enable_crash_handler();
#endif /* ifndef CHIMERA_SANITIZE */

    if (is_nfs) {
        evpl_set_log_fn(chimera_vlog, chimera_log_flush);
    }

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/build/test/posix_session_%d_%lu_%lu",
             getpid(), tv.tv_sec, tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env->session_dir);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    if (is_nfs) {
        // NFS backend: Start server with the actual backend, then connect via NFS client
        char config_data[4096];

        server_config = chimera_server_config_init();

        if (strcmp(nfs_backend_name, "demofs") == 0) {
            posix_test_configure_demofs(env->session_dir, config_data, sizeof(config_data));
            chimera_server_config_add_module(server_config, "demofs", NULL, config_data);
        } else if (strcmp(nfs_backend_name, "cairn") == 0) {
            posix_test_configure_cairn(env->session_dir, config_data, sizeof(config_data));
            chimera_server_config_add_module(server_config, "cairn", NULL, config_data);
        }

        // Enable TCP-RDMA if using RDMA backend
        if (use_nfs_rdma) {
            fprintf(stderr, "Enabling NFS3 over TCP-RDMA on port 20049\n");
            chimera_server_config_set_nfs_rdma_hostname(server_config, "127.0.0.1");
            chimera_server_config_set_nfs_tcp_rdma_port(server_config, 20049);
        }

        env->server = chimera_server_init(server_config, env->metrics);

        // Mount the backend on the server as "share"
        if (strcmp(nfs_backend_name, "linux") == 0) {
            chimera_server_mount(env->server, "share", "linux", env->session_dir);
        } else if (strcmp(nfs_backend_name, "io_uring") == 0) {
            chimera_server_mount(env->server, "share", "io_uring", env->session_dir);
        } else if (strcmp(nfs_backend_name, "memfs") == 0) {
            chimera_server_mount(env->server, "share", "memfs", "/");
        } else if (strcmp(nfs_backend_name, "demofs") == 0) {
            chimera_server_mount(env->server, "share", "demofs", "/");
        } else if (strcmp(nfs_backend_name, "cairn") == 0) {
            chimera_server_mount(env->server, "share", "cairn", "/");
        } else {
            fprintf(stderr, "Unknown NFS backend: %s\n", nfs_backend_name);
            exit(EXIT_FAILURE);
        }

        // Create NFSv3 server export entry
        chimera_server_create_export(env->server, "/share", "/share");

        chimera_server_start(env->server);

        // Initialize the POSIX client (NFS client module is already registered by default)
        client_config = chimera_client_config_init();
    } else {
        // Direct backend: No server needed
        client_config = chimera_client_config_init();

        if (strcmp(backend, "demofs") == 0) {
            char demofs_cfg[4096];
            posix_test_configure_demofs(env->session_dir, demofs_cfg, sizeof(demofs_cfg));
            chimera_client_config_add_module(client_config, "demofs", "/build/test/demofs", demofs_cfg);
        } else if (strcmp(backend, "cairn") == 0) {
            char cairn_cfg[4096];
            posix_test_configure_cairn(env->session_dir, cairn_cfg, sizeof(cairn_cfg));
            chimera_client_config_add_module(client_config, "cairn", "/build/test/cairn", cairn_cfg);
        }
    }

    struct chimera_vfs_cred root_cred;
    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);
    env->posix = chimera_posix_init(client_config, &root_cred, env->metrics);

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

    // Destroy the server if it was created (NFS backend)
    if (env->server) {
        chimera_server_destroy(env->server);
    }

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

__attribute__((noreturn)) static inline void
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
    const char *module_name;
    char        nfs_mount_path[256];
    char        nfs_mount_options[128];

    if (env->nfs_version > 0) {
        // NFS backend: mount via NFS client
        // Mount path format: hostname:path
        snprintf(nfs_mount_path, sizeof(nfs_mount_path), "127.0.0.1:/share");
        if (env->use_nfs_rdma) {
            // Use RDMA protocol and port for NFS3 over TCP-RDMA
            snprintf(nfs_mount_options, sizeof(nfs_mount_options), "vers=%d,rdma=tcp,port=20049", env->nfs_version);
        } else {
            snprintf(nfs_mount_options, sizeof(nfs_mount_options), "vers=%d", env->nfs_version);
        }
        return chimera_posix_mount_with_options("/test", "nfs", nfs_mount_path, nfs_mount_options);
    }

    // Direct backend
    module_name = env->backend;
    if (strcmp(env->backend, "linux") == 0 || strcmp(env->backend, "io_uring") == 0) {
        module_path = env->session_dir;
    }

    return chimera_posix_mount("/test", module_name, module_path);
} /* posix_test_mount */

static inline int
posix_test_umount(void)
{
    return chimera_posix_umount("/test");
} /* posix_test_umount */
