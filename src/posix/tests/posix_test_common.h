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
#include <signal.h>
#include <dirent.h>
#include <execinfo.h>
#include <jansson.h>
#include "posix/posix.h"
#include "server/server.h"
#include "common/logging.h"
#include "common/test_users.h"
#include "prometheus-c.h"

/*
 * Self-dumping watchdog for the forked child of a cross-process test.
 *
 * The lock-family tests (lockf, fcntl, cthon tlock) fork a child that runs
 * its own chimera_posix init/mount/open.  A rare, so-far-unreproduced CI
 * flake strands that child before it signals the parent, and the test dies
 * as a silent 300s ctest Timeout with no stack -- four tests have hit it
 * (lockf_linux, lockf_io_uring, fcntl_io_uring, cthon_lock_tlock_linux).
 *
 * Arm SIGALRM in the child well below the ctest timeout.  On fire, dump
 * every thread's kernel sleep location (procfs wchan -- localizes a hang to
 * its blocking syscall even for threads this handler cannot backtrace) plus
 * the signaled thread's userspace backtrace, then abort().  The SIGABRT
 * unblocks the parent's waitpid, so the test fails fast WITH diagnostics
 * instead of timing out silently.  The handler knowingly calls
 * async-signal-unsafe functions: the process is wedged and about to abort,
 * so a lost dump costs nothing over the status quo.
 */
static void
posix_test_child_watchdog_fire(int sig)
{
    void *frames[64];
    int   nframes;
    DIR  *taskdir;

    (void) sig;

    dprintf(STDERR_FILENO,
            "child-watchdog: pid %d stuck; dumping thread states\n",
            (int) getpid());

    taskdir = opendir("/proc/self/task");
    if (taskdir) {
        struct dirent *de;

        while ((de = readdir(taskdir)) != NULL) {
            char    path[280], wchan[128];
            int     wfd;
            ssize_t m = 0;

            if (de->d_name[0] == '.') {
                continue;
            }
            snprintf(path, sizeof(path), "/proc/self/task/%s/wchan",
                     de->d_name);
            wfd = open(path, O_RDONLY);
            if (wfd >= 0) {
                m = read(wfd, wchan, sizeof(wchan) - 1);
                close(wfd);
            }
            wchan[m > 0 ? m : 0] = '\0';
            dprintf(STDERR_FILENO, "child-watchdog: tid %s wchan %s\n",
                    de->d_name, wchan);
        }
        closedir(taskdir);
    }

    dprintf(STDERR_FILENO, "child-watchdog: signaled thread backtrace:\n");
    nframes = backtrace(frames, 64);
    backtrace_symbols_fd(frames, nframes, STDERR_FILENO);

    signal(SIGABRT, SIG_DFL);
    abort();
} /* posix_test_child_watchdog_fire */

static inline void
posix_test_child_watchdog(unsigned int seconds)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = posix_test_child_watchdog_fire;
    sigaction(SIGALRM, &sa, NULL);
    alarm(seconds);
} /* posix_test_child_watchdog */

struct posix_test_env {
    struct chimera_posix_client *posix;
    struct chimera_server       *server;       // For NFS backend tests
    struct prometheus_metrics   *metrics;
    char                         session_dir[256];
    const char                  *backend;
    const char                  *nfs_backend;  // Actual backend behind NFS (e.g., "memfs")
    int                          nfs_version;  // 3 or 4, 0 if not NFS
    int                          use_nfs_rdma; // 1 if using NFS over RDMA (TCP-RDMA)
    struct chimera_vfs_cred      cred;         // Credential used to initialize the POSIX client
};

// Helper to parse env->backend for NFS backends (e.g., "nfs3_memfs" -> version=3, backend="memfs")
// Sets env->nfs_version, env->nfs_backend, env->use_nfs_rdma. Returns 1 if NFS backend, 0 otherwise.
static inline int
posix_test_parse_nfs_backend(struct posix_test_env *env)
{
    const char *backend = env->backend;

    env->use_nfs_rdma = 0;
    if (strncmp(backend, "nfs3rdma_", 9) == 0) {
        env->nfs_version  = 3;
        env->nfs_backend  = backend + 9;
        env->use_nfs_rdma = 1;
        return 1;
    } else if (strncmp(backend, "nfs3_", 5) == 0) {
        env->nfs_version = 3;
        env->nfs_backend = backend + 5;
        return 1;
    } else if (strncmp(backend, "nfs4_", 5) == 0) {
        env->nfs_version = 4;
        env->nfs_backend = backend + 5;
        return 1;
    }
    env->nfs_version = 0;
    env->nfs_backend = NULL;
    return 0;
} // posix_test_parse_nfs_backend

// Helper to get device type for a diskfs variant backend name
static inline const char *
posix_test_diskfs_device_type(const char *backend)
{
    if (strcmp(backend, "diskfs_aio") == 0) {
        return "libaio";
    }
    return "io_uring";
} // posix_test_diskfs_device_type

// Helper to check if a backend name is a diskfs variant
static inline int
posix_test_is_diskfs(const char *backend)
{
    return strcmp(backend, "diskfs") == 0 ||
           strcmp(backend, "diskfs_io_uring") == 0 ||
           strcmp(backend, "diskfs_aio") == 0;
} // posix_test_is_diskfs

/* Optional diskfs-config overrides for tests that need a non-default setup.
 * posix_test_diskfs_extra_cfg is a JSON object (text) merged into the
 * generated diskfs config (e.g. to shrink the caches for an eviction stress);
 * posix_test_diskfs_reuse_devices reuses the existing device images without
 * re-initializing the filesystem (cold remount).  Set before posix_test_init
 * / posix_test_configure_diskfs. */
static const char *posix_test_diskfs_extra_cfg = NULL;
static int         posix_test_diskfs_reuse_devices __attribute__ ((unused)) = 0;

/* When non-zero (set before posix_test_init), posix_test_start_nfs_server also
 * mounts the SAME NFS backend a second time, read-only, under a subdirectory
 * and exposes it via a second export "/share_ro".  The read-write export
 * "/share" mounts the backend root, so a file created under "/share/ro/<name>"
 * is the same inode the read-only mount exposes as "/<name>".  This lets an
 * EROFS test create fixtures through the writable export and assert that every
 * mutation through the read-only export fails with EROFS.  The read-only mount
 * carries a distinct mount_id (its root_fh is encoded from the subdirectory
 * inode rather than copied from the root mount), which is what the VFS
 * read-only gate keys on. */
static int posix_test_ro_export __attribute__ ((unused)) = 0;

/* Name of the subdirectory (relative to the backend root) that the read-only
 * export is mounted at; the read-write export sees it as "/share/ro". */
#define POSIX_TEST_RO_SUBDIR "ro"

// Helper to configure diskfs backend
static inline void
posix_test_configure_diskfs(
    const char *session_dir,
    const char *device_type,
    char       *diskfs_cfg,
    size_t      diskfs_cfg_size)
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
        json_object_set_new(device, "type", json_string(device_type));
        json_object_set_new(device, "size", json_integer(1));
        json_object_set_new(device, "path", json_string(device_path));
        json_array_append_new(devices, device);

        if (posix_test_diskfs_reuse_devices) {
            continue;
        }

        int fd = open(device_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0) {
            fprintf(stderr, "Failed to create device %s: %s\n", device_path, strerror(errno));
            exit(EXIT_FAILURE);
        }

        rc = ftruncate(fd, 1024 * 1024 * 1024UL);

        if (rc < 0) {
            fprintf(stderr, "Failed to truncate device %s: %s\n", device_path, strerror(errno));
            exit(EXIT_FAILURE);
        }

        close(fd);
    }

    /* diskfs keys initialization on the presence of the key, so a remount
     * must omit it entirely rather than pass false. */
    if (!posix_test_diskfs_reuse_devices) {
        json_object_set_new(cfg, "initialize", json_true());
    }
    json_object_set_new(cfg, "devices", devices);
    json_object_set_new(cfg, "unsafe_async", json_true());
    if (posix_test_diskfs_extra_cfg) {
        json_t *extra = json_loads(posix_test_diskfs_extra_cfg, 0, NULL);

        if (!extra) {
            fprintf(stderr, "Bad posix_test_diskfs_extra_cfg JSON: %s\n",
                    posix_test_diskfs_extra_cfg);
            exit(EXIT_FAILURE);
        }
        json_object_update(cfg, extra);
        json_decref(extra);
    }
    json_str = json_dumps(cfg, JSON_COMPACT);
    snprintf(diskfs_cfg, diskfs_cfg_size, "%s", json_str);
    free(json_str);
    json_decref(cfg);
} // posix_test_configure_diskfs

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
} // posix_test_configure_cairn

// Helper to start NFS server with the given backend and mount it as "share"
static inline void
posix_test_start_nfs_server(struct posix_test_env *env)
{
    char                          config_data[4096];
    struct chimera_server_config *server_config;
    const char                   *nfs_backend_name = env->nfs_backend;
    int                           use_nfs_rdma     = env->use_nfs_rdma;

    server_config = chimera_server_config_init();
    chimera_server_config_set_state_dir(server_config, env->session_dir);

    if (posix_test_is_diskfs(nfs_backend_name)) {
        posix_test_configure_diskfs(env->session_dir,
                                    posix_test_diskfs_device_type(nfs_backend_name),
                                    config_data, sizeof(config_data));
        chimera_server_config_add_module(server_config, "diskfs", NULL, config_data);
    } else if (strcmp(nfs_backend_name, "cairn") == 0) {
        posix_test_configure_cairn(env->session_dir, config_data, sizeof(config_data));
        chimera_server_config_add_module(server_config, "cairn", NULL, config_data);
    }

    if (use_nfs_rdma) {
        fprintf(stderr, "Enabling NFS3 over TCP-RDMA on port 20049\n");
        chimera_server_config_set_nfs_rdma_hostname(server_config, "127.0.0.1");
        chimera_server_config_set_nfs_tcp_rdma_port(server_config, 20049);
    }

    env->server = chimera_server_init(server_config, env->metrics);

    /* Backend module name + root module path used for the writable "/share"
     * mount (passthrough backends root at the host session dir). */
    const char *share_module      = nfs_backend_name;
    const char *share_module_path = "/";

    if (strcmp(nfs_backend_name, "linux") == 0 ||
        strcmp(nfs_backend_name, "io_uring") == 0) {
        share_module_path = env->session_dir;
    } else if (posix_test_is_diskfs(nfs_backend_name)) {
        share_module = "diskfs";
    } else if (strcmp(nfs_backend_name, "memfs") != 0 &&
               strcmp(nfs_backend_name, "cairn") != 0) {
        fprintf(stderr, "Unknown NFS backend: %s\n", nfs_backend_name);
        exit(EXIT_FAILURE);
    }

    /* For the read-only test, create the subdirectory the read-only mount will
     * be rooted at BEFORE any other mount exists (chimera_server_mkpath does a
     * transient mount of the backend root, which would otherwise collide in the
     * mount table with the writable "/share" mount of the same backend). */
    char ro_module_path[400] = { 0 };

    if (posix_test_ro_export) {
        if (strcmp(nfs_backend_name, "linux") == 0 ||
            strcmp(nfs_backend_name, "io_uring") == 0) {
            snprintf(ro_module_path, sizeof(ro_module_path), "%s/%s",
                     env->session_dir, POSIX_TEST_RO_SUBDIR);
        } else {
            snprintf(ro_module_path, sizeof(ro_module_path), "/%s",
                     POSIX_TEST_RO_SUBDIR);
        }

        if (chimera_server_mkpath(env->server, share_module, ro_module_path,
                                  0777) != 0) {
            fprintf(stderr, "Failed to create read-only subdir %s in %s\n",
                    ro_module_path, share_module);
            exit(EXIT_FAILURE);
        }
    }

    chimera_server_mount(env->server, "share", share_module, share_module_path,
                         NULL);

    chimera_server_create_export(env->server, "/share", "/share");

    if (posix_test_ro_export) {
        /* Second, read-only mount of the same backend rooted at the subdir.
         * Its root_fh is encoded from the subdir inode, giving it a distinct
         * mount_id that the VFS read-only gate keys on, even though it exposes
         * the same inodes as "/share/ro". */
        /* The mount/export name must not be a string prefix of "share" nor
         * have "share" as its prefix: the pseudo-root mount lookup matches by
         * strncmp prefix, so "share_ro" would collide with "share". */
        if (chimera_server_mount(env->server, "roshare", share_module,
                                 ro_module_path, "ro") != 0) {
            fprintf(stderr, "Failed to mount read-only export\n");
            exit(EXIT_FAILURE);
        }

        chimera_server_create_export(env->server, "/roshare", "/roshare");
    }

    chimera_server_start(env->server);

    chimera_test_add_server_users(env->server);
} /* posix_test_start_nfs_server */

static inline void
posix_test_init(
    struct posix_test_env *env,
    char                 **argv,
    int                    argc)
{
    int             opt;
    extern char    *optarg;
    const char     *backend = "memfs";
    struct timespec tv;
    int             is_nfs;

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);
    env->server  = NULL;

    chimera_vfs_cred_init_unix(&env->cred,
                               CHIMERA_TEST_USER_ROOT_UID,
                               CHIMERA_TEST_USER_ROOT_GID,
                               0, NULL);

    clock_gettime(CLOCK_MONOTONIC, &tv);

    env->session_dir[0] = '\0';

    /* Suppress error messages for options we don't handle here -
     * individual tests may have additional options they parse after.
     * Use "+" prefix to stop at first non-option (POSIX behavior),
     * preventing argv permutation that would break subsequent getopt calls. */
    opterr = 0;
    while ((opt = getopt(argc, argv, "+b:U:")) != -1) {
        switch (opt) {
            case 'b':
                backend = optarg;
                break;
            case 'U':
                if (!chimera_test_parse_user(optarg, &env->cred)) {
                    fprintf(stderr, "Unknown user spec '%s'. "
                            "Use: root, johndoe, myuser, or uid:gid\n", optarg);
                    exit(EXIT_FAILURE);
                }
                break;
        } /* switch */
    }
    opterr = 1;  /* Re-enable for subsequent getopt calls */
    optind = 1;  /* Reset for next getopt caller */

    env->backend = backend;

    // Check if this is an NFS backend (e.g., "nfs3_memfs" or "nfs3rdma_memfs")
    is_nfs = posix_test_parse_nfs_backend(env);

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

    int rc;

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env->session_dir, 0755);

    rc = chown(env->session_dir, env->cred.uid, env->cred.gid);
    if (rc < 0) {
        fprintf(stderr, "Failed to set session_dir uid/gid: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    if (is_nfs) {
        posix_test_start_nfs_server(env);
    }

    {
        char    posix_json_path[300];
        json_t *posix_json_root, *posix_json_config;

        posix_json_root   = json_object();
        posix_json_config = json_object();

        if (!is_nfs) {
            if (posix_test_is_diskfs(backend)) {
                char    diskfs_cfg[4096];
                json_t *vfs, *vfs_entry;
                posix_test_configure_diskfs(env->session_dir,
                                            posix_test_diskfs_device_type(backend),
                                            diskfs_cfg, sizeof(diskfs_cfg));
                vfs       = json_object();
                vfs_entry = json_object();
                json_object_set_new(vfs_entry, "path", json_string("/build/test/diskfs"));
                json_object_set_new(vfs_entry, "config", json_string(diskfs_cfg));
                json_object_set_new(vfs, "diskfs", vfs_entry);
                json_object_set_new(posix_json_config, "vfs", vfs);
            } else if (strcmp(backend, "cairn") == 0) {
                char    cairn_cfg[4096];
                json_t *vfs, *vfs_entry;
                posix_test_configure_cairn(env->session_dir, cairn_cfg, sizeof(cairn_cfg));
                vfs       = json_object();
                vfs_entry = json_object();
                json_object_set_new(vfs_entry, "path", json_string("/build/test/cairn"));
                json_object_set_new(vfs_entry, "config", json_string(cairn_cfg));
                json_object_set_new(vfs, "cairn", vfs_entry);
                json_object_set_new(posix_json_config, "vfs", vfs);
            }
        }

        json_object_set_new(posix_json_root, "config", posix_json_config);
        chimera_test_write_users_json(posix_json_root);

        snprintf(posix_json_path, sizeof(posix_json_path), "%s/posix.json", env->session_dir);
        json_dump_file(posix_json_root, posix_json_path, 0);
        json_decref(posix_json_root);

        env->posix = chimera_posix_init_json(posix_json_path, &env->cred, env->metrics);
    }

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
        fprintf(stderr, "Mounting NFS backend path %s with options: %s\n", nfs_mount_path, nfs_mount_options);
        return chimera_posix_mount_with_options("/test", "nfs", nfs_mount_path, nfs_mount_options);
    }

    // Direct backend
    module_name = env->backend;
    if (posix_test_is_diskfs(env->backend)) {
        module_name = "diskfs";
    } else if (strcmp(env->backend, "linux") == 0 || strcmp(env->backend, "io_uring") == 0) {
        module_path = env->session_dir;
    }

    return chimera_posix_mount("/test", module_name, module_path);
} /* posix_test_mount */

static inline int
posix_test_umount(void)
{
    return chimera_posix_umount("/test");
} /* posix_test_umount */
