// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB smbtorture Integration Test
 *
 * Starts a Chimera SMB server in-process and runs the Samba smbtorture
 * test suite against it.  Individual smbtorture test names are passed
 * as positional arguments so that each ctest entry can exercise a
 * different subset of the suite.
 *
 * Usage:
 *   smbtorture_test [options] TEST1 [TEST2 ...]
 *
 * Options:
 *   -b <backend>   VFS backend (memfs, linux, io_uring, demofs_io_uring,
 *                  demofs_aio, cairn) - default: memfs
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "common/test_users.h"
#include <errno.h>
#include <fcntl.h>
#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

struct test_env {
    struct chimera_server     *server;
    char                       session_dir[256];
    struct prometheus_metrics *metrics;
};

static void
test_cleanup(
    struct test_env *env,
    int              remove_session)
{
    if (env->server) {
        chimera_server_destroy(env->server);
        env->server = NULL;
    }

    if (env->metrics) {
        prometheus_metrics_destroy(env->metrics);
        env->metrics = NULL;
    }

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        if (system(cmd) != 0) {
            fprintf(stderr, "Warning: failed to clean up session dir\n");
        }
    }
} /* test_cleanup */

static int
run_smbtorture(
    int         num_tests,
    const char *tests[])
{
    char cmd[8192];
    int  off, i, rc;

    off = snprintf(cmd, sizeof(cmd),
                   "smbtorture //localhost/share"
                   " -U myuser%%mypassword"
                   " --option=torture:samba3=yes"
                   " --option=torture:resume_key_support=no"
                   " --fullname");

    for (i = 0; i < num_tests; i++) {
        off += snprintf(cmd + off, sizeof(cmd) - off, " %s", tests[i]);
    }

    /* Capture combined output so it shows in ctest logs */
    off += snprintf(cmd + off, sizeof(cmd) - off, " 2>&1");

    fprintf(stderr, "Running: %s\n", cmd);

    rc = system(cmd);

    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }

    return -1;
} /* run_smbtorture */

static void
print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [options] TEST1 [TEST2 ...]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -b <backend>   VFS backend (memfs, linux, io_uring,\n");
    fprintf(stderr, "                 demofs_io_uring, demofs_aio, cairn)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Positional arguments are smbtorture test names, e.g.:\n");
    fprintf(stderr, "  smb2.connect  smb2.create.open  smb2.rw\n");
} /* print_usage */

int
main(
    int    argc,
    char **argv)
{
    struct test_env               env = { 0 };
    struct chimera_server_config *config;
    struct timespec               tv;
    const char                   *backend   = "memfs";
    const char                  **tests     = NULL;
    int                           num_tests = 0;
    int                           rc;
    int                           i;

    /* Parse arguments - options first, then positional test names */
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            backend = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (argv[i][0] != '-') {
            /* First positional arg - rest are test names */
            tests     = (const char **) &argv[i];
            num_tests = argc - i;
            break;
        }
    }

    if (num_tests == 0) {
        fprintf(stderr, "ERROR: No smbtorture tests specified\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "SMB smbtorture Integration Test\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "Backend: %s\n", backend);
    fprintf(stderr, "Tests:  ");
    for (i = 0; i < num_tests; i++) {
        fprintf(stderr, " %s", tests[i]);
    }
    fprintf(stderr, "\n");

    /* Check if smbtorture is available */
    if (system("which smbtorture >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nERROR: smbtorture not found in PATH\n");
        fprintf(stderr, "Install with: apt-get install samba-testsuite\n");
        return EXIT_FAILURE;
    }

    /* Initialize logging */
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    env.metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!env.metrics) {
        fprintf(stderr, "Failed to create metrics\n");
        return EXIT_FAILURE;
    }

    /* Create session directory */
    clock_gettime(CLOCK_MONOTONIC, &tv);
    snprintf(env.session_dir, sizeof(env.session_dir),
             "/tmp/smbtorture_test_%d_%lu", getpid(), tv.tv_sec);

    if (mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create session directory: %s\n", strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(stderr, "Session directory: %s\n", env.session_dir);

    /* Initialize server configuration */
    config = chimera_server_config_init();

    /* Configure backend-specific modules */
    if (strcmp(backend, "demofs_io_uring") == 0 ||
        strcmp(backend, "demofs_aio") == 0) {
        char        demofs_cfg[4096];
        char        device_path[300];
        char       *json_str;
        json_t     *cfg, *devices, *device;
        const char *device_type;

        device_type = strcmp(backend, "demofs_aio") == 0
                      ? "libaio" : "io_uring";

        cfg     = json_object();
        devices = json_array();

        for (i = 0; i < 10; ++i) {
            int fd;

            device = json_object();
            snprintf(device_path, sizeof(device_path),
                     "%s/device-%d.img", env.session_dir, i);
            json_object_set_new(device, "type", json_string(device_type));
            json_object_set_new(device, "size", json_integer(1));
            json_object_set_new(device, "path", json_string(device_path));
            json_array_append_new(devices, device);

            fd = open(device_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
            if (fd < 0) {
                fprintf(stderr, "Failed to create device %s: %s\n",
                        device_path, strerror(errno));
                test_cleanup(&env, 0);
                return EXIT_FAILURE;
            }
            if (ftruncate(fd, 1024 * 1024 * 1024) < 0) {
                fprintf(stderr, "Failed to truncate device %s: %s\n",
                        device_path, strerror(errno));
                close(fd);
                test_cleanup(&env, 0);
                return EXIT_FAILURE;
            }
            close(fd);
        }

        json_object_set_new(cfg, "devices", devices);
        json_str = json_dumps(cfg, JSON_COMPACT);
        snprintf(demofs_cfg, sizeof(demofs_cfg), "%s", json_str);
        free(json_str);
        json_decref(cfg);

        chimera_server_config_add_module(config, "demofs",
                                         "/build/test/demofs", demofs_cfg);
    } else if (strcmp(backend, "cairn") == 0) {
        char    cairn_cfg[4096];
        char   *json_str;
        json_t *cfg;

        cfg = json_object();
        json_object_set_new(cfg, "initialize", json_true());
        json_object_set_new(cfg, "path", json_string(env.session_dir));
        json_str = json_dumps(cfg, JSON_COMPACT);
        snprintf(cairn_cfg, sizeof(cairn_cfg), "%s", json_str);
        free(json_str);
        json_decref(cfg);

        chimera_server_config_add_module(config, "cairn",
                                         "/build/test/cairn", cairn_cfg);
    }

    /* Initialize server */
    env.server = chimera_server_init(config, env.metrics);
    if (!env.server) {
        fprintf(stderr, "Failed to initialize server\n");
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    /* Mount filesystem */
    if (strcmp(backend, "memfs") == 0) {
        chimera_server_mount(env.server, "share", "memfs", "/");
    } else if (strcmp(backend, "linux") == 0) {
        chimera_server_mount(env.server, "share", "linux", env.session_dir);
    } else if (strcmp(backend, "io_uring") == 0) {
        chimera_server_mount(env.server, "share", "io_uring", env.session_dir);
    } else if (strcmp(backend, "demofs_io_uring") == 0 ||
               strcmp(backend, "demofs_aio") == 0) {
        chimera_server_mount(env.server, "share", "demofs", "/");
    } else if (strcmp(backend, "cairn") == 0) {
        chimera_server_mount(env.server, "share", "cairn", "/");
    } else {
        fprintf(stderr, "Unknown backend: %s\n", backend);
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share");

    fprintf(stderr, "Server started\n");

    /* Give server a moment to be ready */
    usleep(100000);

    /* Run smbtorture */
    rc = run_smbtorture(num_tests, tests);

    fprintf(stderr, "\n========================================\n");
    if (rc == 0) {
        fprintf(stderr, "smbtorture: PASSED\n");
    } else {
        fprintf(stderr, "smbtorture: FAILED (exit code %d)\n", rc);
    }
    fprintf(stderr, "========================================\n\n");

    test_cleanup(&env, rc == 0);
    return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
} /* main */
