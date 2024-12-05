#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-nfs4.h>
#include "server/server.h"
#include "common/logging.h"

struct test_env {
    struct nfs_context    *nfs;
    struct chimera_server *server;
    char                   session_dir[256];
};

static inline void
libnfs_test_init(
    struct test_env *env,
    char           **argv,
    int              argc)
{
    int             opt;
    extern char    *optarg;
    int             nfsvers = 3;
    const char     *backend = "linux";
    struct timespec tv;

    clock_gettime(CLOCK_MONOTONIC, &tv);

    env->session_dir[0] = '\0';

    while ((opt = getopt(argc, argv, "b:v:")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
            case 'v':
                nfsvers = atoi(optarg);
                break;
        } /* switch */
    }

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    env->server = chimera_server_init(NULL);

    if (strcmp(backend, "linux") == 0) {
        snprintf(env->session_dir, sizeof(env->session_dir),
                 "/build/test/session_%d_%lu_%lu",
                 getpid(), tv.tv_sec, tv.tv_nsec);

        fprintf(stderr, "Creating session directory %s\n", env->session_dir);

        (void) mkdir("/build/test", 0755);
        (void) mkdir(env->session_dir, 0755);

        chimera_server_create_share(env->server, "linux", "share",
                                    env->session_dir);
    } else if (strcmp(backend, "memfs") == 0) {
        chimera_server_create_share(env->server, "memfs", "share", "/");
    } else {
        fprintf(stderr, "Unknown backend: %s\n", backend);
        exit(EXIT_FAILURE);
    }

    env->nfs = nfs_init_context();

    if (!env->nfs) {
        fprintf(stderr, "Failed to initialize NFS context\n");
        exit(EXIT_FAILURE);
    }

    if (nfsvers == 3) {
        nfs_set_version(env->nfs, NFS_V3);
    } else {
        nfs_set_version(env->nfs, NFS_V4);
    }
} /* libnfs_test_init */

static inline void
libnfs_test_cleanup(
    struct test_env *env,
    int              remove_session)
{

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        system(cmd);
    }

    nfs_destroy_context(env->nfs);

    chimera_server_destroy(env->server);
} /* libnfs_test_cleanup */

static inline void
libnfs_test_fail(struct test_env *env)
{
    fprintf(stderr, "Test failed\n");

    libnfs_test_cleanup(env, 0);

    exit(EXIT_FAILURE);
} /* libnfs_test_fail */

static inline void
libnfs_test_success(struct test_env *env)
{
    libnfs_test_cleanup(env, 1);
} /* libnfs_test_cleanup */