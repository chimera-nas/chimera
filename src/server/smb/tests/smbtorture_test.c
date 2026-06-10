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
 *   -b <backend>   VFS backend (memfs, linux, io_uring, diskfs_io_uring,
 *                  diskfs_aio, cairn) - default: memfs
 */

#define _GNU_SOURCE
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
    const char *tests[],
    const char *backend,
    const char *session_dir)
{
    char cmd[8192];
    int  off, i, rc;

    off = snprintf(cmd, sizeof(cmd),
                   "smbtorture //127.0.0.1/share"
                   " -U myuser%%mypassword"
                   " --option=torture:samba3=yes"
                   " --option=torture:resume_key_support=no"
                   /* Provide the parameters that several suites abort
                    * without, so the real protocol logic runs instead of
                    * a "missing option" bail-out:
                    *   set-sparse-ioctl / zero-data-ioctl -> filename,
                    *       offset, beyond_final_zero
                    *   ea.acl_xattr                       -> acl_xattr_name */
                   " --option=torture:filename=torture_test_file"
                   " --option=torture:offset=0"
                   " --option=torture:beyond_final_zero=4096"
                   " --option=torture:acl_xattr_name=user.NTACL"
                   " --option='client smb3 signing algorithms=aes-128-cmac'"
                   " --fullname");

    /* samba3misc's localposixlock test needs the share's on-disk path so
     * it can take a POSIX lock directly.  Only the passthrough backends
     * (linux, io_uring) expose the share root as a real local directory. */
    if (strcmp(backend, "linux") == 0 || strcmp(backend, "io_uring") == 0) {
        off += snprintf(cmd + off, sizeof(cmd) - off,
                        " --option=torture:localdir=%s", session_dir);
    }

    for (i = 0; i < num_tests; i++) {
        off += snprintf(cmd + off, sizeof(cmd) - off, " %s", tests[i]);
    }

    /* Capture combined output so it shows in ctest logs */
    snprintf(cmd + off, sizeof(cmd) - off, " 2>&1");

    fprintf(stderr, "Running: %s\n", cmd);

    rc = system(cmd);

    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }

    return -1;
} /* run_smbtorture */

/* The linux and io_uring passthrough backends derive their VFS file handles
* from name_to_handle_at(2).  A container/overlay rootfs (and the /tmp that
* sits on it) returns EOPNOTSUPP for that call, which makes the passthrough
* mount fail -- after which every CREATE against the share root resolves to
* nothing and the server replies NETWORK_NAME_DELETED.  Such backends need
* their share root on a filesystem that supports file handles (e.g. tmpfs).
* Backends that store their own data (memfs/diskfs/cairn) are unaffected. */
static int
fs_supports_file_handles(const char *dir)
{
    struct {
        struct file_handle fh;
        unsigned char      buf[MAX_HANDLE_SZ];
    } h;
    int mount_id;

    h.fh.handle_bytes = MAX_HANDLE_SZ;

    return name_to_handle_at(AT_FDCWD, dir, &h.fh, &mount_id, 0) == 0;
} /* fs_supports_file_handles */

/* Pick a base directory for the session.  Passthrough backends require one
 * whose filesystem supports name_to_handle_at(2); everything else keeps the
 * historical /tmp.  Returns NULL if no suitable directory is available. */
static const char *
pick_session_base(int needs_file_handles)
{
    static const char *candidates[] = { "/tmp", "/dev/shm" };
    unsigned           i;

    if (!needs_file_handles) {
        return "/tmp";
    }

    for (i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
        if (fs_supports_file_handles(candidates[i])) {
            return candidates[i];
        }
    }

    return NULL;
} /* pick_session_base */

static void
print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [options] TEST1 [TEST2 ...]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -b <backend>   VFS backend (memfs, linux, io_uring,\n");
    fprintf(stderr, "                 diskfs_io_uring, diskfs_aio, cairn)\n");
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

    /* Check if smbtorture is available.  Exit 77 (the CTest SKIP_RETURN_CODE
     * used by these tests) so environments without the smbtorture client
     * report Skipped rather than a hard failure. */
    if (system("which smbtorture >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nsmbtorture not found in PATH; skipping.\n");
        fprintf(stderr, "Install with: apt-get install samba-testsuite\n");
        return 77;
    }

    /* Initialize logging */
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    env.metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!env.metrics) {
        fprintf(stderr, "Failed to create metrics\n");
        return EXIT_FAILURE;
    }

    /* Create session directory.  Passthrough backends need it on a
     * file-handle-capable filesystem (see pick_session_base). */
    int         needs_file_handles = strcmp(backend, "linux") == 0 ||
        strcmp(backend, "io_uring") == 0;
    const char *session_base = pick_session_base(needs_file_handles);

    if (!session_base) {
        fprintf(stderr,
                "No filesystem supporting name_to_handle_at(2) found for the "
                "'%s' passthrough backend (tried /tmp, /dev/shm)\n", backend);
        return EXIT_FAILURE;
    }

    clock_gettime(CLOCK_MONOTONIC, &tv);
    snprintf(env.session_dir, sizeof(env.session_dir),
             "%s/smbtorture_test_%d_%lu", session_base, getpid(), tv.tv_sec);

    if (mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create session directory: %s\n", strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(stderr, "Session directory: %s\n", env.session_dir);

    /* Initialize server configuration */
    config = chimera_server_config_init();

    /* Exercise the real SMB3 durable/persistent-handle path (as the WPTS
     * harness does via CHIMERA_SMB_PERSISTENT).  Without it the server refuses
     * every durable grant, and the durable smbtorture suites take their
     * not-granted failure branch -- which in several tests leaves a tree/session
     * pointer NULL and then dereferences it in cleanup, crashing the client. */
    chimera_server_config_set_smb_persistent_handles(config, 1);

    /* SMB2 leases and legacy oplocks are off by default in the server; enable
     * both here so the leasing/oplock smbtorture suites (smb2.lease*,
     * smb2.oplock*, plus durable/multichannel cases that ride a lease) exercise
     * the caching grant + break path.  Suites that request neither are
     * unaffected by the grant being available. */
    chimera_server_config_set_smb_leases(config, 1);
    chimera_server_config_set_smb_oplocks(config, 1);

    /* SMB3 multichannel: advertise interfaces so FSCTL_QUERY_NETWORK_INTERFACE_
     * INFO returns a non-empty list and the smb2.multichannel suites run instead
     * of bailing ("no interface info returned").  Everything in the test netns
     * lives on loopback (127.0.0.0/8 is all local), so the smbtorture client
     * opens every additional channel back to 127.0.0.1 regardless of which
     * addresses are advertised; two RSS-capable NICs mirror the WPTS setup and
     * let num_channels exercise binding.  The capability bit itself is already
     * advertised globally by the server. */
    {
        struct chimera_server_config_smb_nic nics[2] = {
            { .address = "127.0.0.1", .speed = 10, .rdma = 0 },
            { .address = "127.0.0.2", .speed = 10, .rdma = 0 },
        };
        chimera_server_config_set_smb_nic_info(config, 2, nics);
    }

    /* CTest invokes this binary once per suite.  Enable named-stream (ADS)
     * support only for the stream suites, so the negative smb2.create_no_streams
     * suite still runs with the feature off on the same backend.  Also match
     * individual smb2.streams.* subtests so the per-subtest harvest variant
     * (run one stream subtest in its own server process) gets the same
     * capability the combined smb2.streams suite needs. */
    for (i = 0; i < num_tests; i++) {
        if (strcmp(tests[i], "smb2.streams") == 0 ||
            strncmp(tests[i], "smb2.streams.", 13) == 0 ||
            strcmp(tests[i], "smb2.ioctl-on-stream") == 0 ||
            strcmp(tests[i], "smb2.sdread") == 0) {
            chimera_server_config_set_smb_named_streams(config, 1);
            break;
        }
    }

    /* The smb2.acls_non_canonical suite asserts Samba's
     * "acl flag inherited canonicalization = no" behaviour: the
     * DACL_AUTO_INHERITED bit round-trips verbatim even when the client
     * sets it without AUTO_INHERIT_REQ.  The smb2.acls.INHERITFLAGS suite
     * asserts the opposite (Windows-canonical) behaviour, so the knob is
     * flipped only for the non-canonical suite. */
    for (i = 0; i < num_tests; i++) {
        if (strcmp(tests[i], "smb2.acls_non_canonical") == 0) {
            chimera_server_config_set_smb_acl_inherited_canonicalize(config, 0);
            break;
        }
    }

    /* Configure backend-specific modules */
    if (strcmp(backend, "diskfs_io_uring") == 0 ||
        strcmp(backend, "diskfs_aio") == 0) {
        char        diskfs_cfg[4096];
        char        device_path[300];
        char       *json_str;
        json_t     *cfg, *devices, *device;
        const char *device_type;

        device_type = strcmp(backend, "diskfs_aio") == 0
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

        json_object_set_new(cfg, "initialize", json_true());
        json_object_set_new(cfg, "devices", devices);
        /* unsafe_async: this test does not exercise crash recovery, so skip FUA/sync
         * on writes to run lighter. */
        json_object_set_new(cfg, "unsafe_async", json_true());
        /* smb2.maxfid keeps 65520 file handles open across ~256 inode-cache
         * shards; the inode home blocks stay pinned by the open handles and
         * concurrent close traffic also pins LOGGED blocks (held until tail
         * push).  The default cache (32768 blocks across 256 shards ≈ 128 per
         * shard) is too small for this skew and aborts with either
         *   - "b+tree lookup suspended on a cache miss"  (sync wrapper hits a
         *     non-resident block on the ACL descent), or
         *   - "block cache shard exhausted: every buffer pinned"
         *     (recycle finds no clean victim in a hot shard).
         * Both are diskfs cache-sizing issues, not protocol bugs.  Bump the
         * cache for these tests so the working set fits.  TODO: replace this
         * test-side workaround with a runtime fix -- either a cross-shard
         * victim search (so pinned skew in one shard can draw from another)
         * or making diskfs_map_attrs's ACL lookup tolerate suspension.  See
         * diskfs.c:5665 and :2542 for the abort sites. */
        json_object_set_new(cfg, "block_cache_blocks", json_integer(262144));
        json_str = json_dumps(cfg, JSON_COMPACT);
        snprintf(diskfs_cfg, sizeof(diskfs_cfg), "%s", json_str);
        free(json_str);
        json_decref(cfg);

        chimera_server_config_add_module(config, "diskfs",
                                         "/build/test/diskfs", diskfs_cfg);
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

    /* The smb2.session-require-signing suite checks that the server advertises
     * mandatory signing (security_mode SIGNING_REQUIRED|ENABLED).  That is a
     * per-server policy, so enable it only when running that suite to avoid
     * forcing signing on every other suite's connections. */
    for (i = 0; i < num_tests; i++) {
        if (strstr(tests[i], "session-require-signing") != NULL) {
            chimera_server_config_set_smb_signing_required(config, 1);
            break;
        }
    }

    /* The change_notify_disabled suite expects the server to reject
     * CHANGE_NOTIFY with STATUS_NOT_IMPLEMENTED (the "change notify = no"
     * share policy).  Enable that policy only for that suite — every other
     * suite needs CHANGE_NOTIFY working. */
    for (i = 0; i < num_tests; i++) {
        if (strstr(tests[i], "change_notify_disabled") != NULL) {
            chimera_server_config_set_smb_notify_disabled(config, 1);
            break;
        }
    }

    /* Initialize server */
    env.server = chimera_server_init(config, env.metrics);
    if (!env.server) {
        fprintf(stderr, "Failed to initialize server\n");
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    /* Mount filesystem */
    int mount_rc;

    if (strcmp(backend, "memfs") == 0) {
        mount_rc = chimera_server_mount(env.server, "share", "memfs", "/", NULL);
    } else if (strcmp(backend, "linux") == 0) {
        mount_rc = chimera_server_mount(env.server, "share", "linux", env.session_dir, NULL);
    } else if (strcmp(backend, "io_uring") == 0) {
        mount_rc = chimera_server_mount(env.server, "share", "io_uring", env.session_dir, NULL);
    } else if (strcmp(backend, "diskfs_io_uring") == 0 ||
               strcmp(backend, "diskfs_aio") == 0) {
        mount_rc = chimera_server_mount(env.server, "share", "diskfs", "/", NULL);
    } else if (strcmp(backend, "cairn") == 0) {
        mount_rc = chimera_server_mount(env.server, "share", "cairn", "/", NULL);
    } else {
        fprintf(stderr, "Unknown backend: %s\n", backend);
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    /* A failed mount leaves the share pointing at nothing, so every CREATE
     * would come back as NETWORK_NAME_DELETED.  Fail loudly instead. */
    if (mount_rc != 0) {
        fprintf(stderr, "Failed to mount '%s' backend at %s: vfs error %d\n",
                backend, env.session_dir, mount_rc);
        test_cleanup(&env, 1);
        return EXIT_FAILURE;
    }

    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share", 0);
    /* Second view of the same tree with access-based enumeration enabled, for
     * smb2.acls.ACCESSBASED. */
    chimera_server_create_share(env.server, "hideunread", "share", 0);
    chimera_server_share_set_access_based_enum(env.server, "hideunread");

    fprintf(stderr, "Server started\n");

    /* Give server a moment to be ready */
    usleep(100000);

    /* smb2.zero-data-ioctl (test_ioctl_zero_data) opens
     * --option=torture:filename with NTCREATEX_DISP_OPEN, so the file MUST
     * pre-exist on the share or the test bails with NT_STATUS_OBJECT_NAME_NOT_FOUND
     * before it ever issues FSCTL_SET_ZERO_DATA.  Pre-create it via smbclient,
     * which works across every backend without backend-specific paths. */
    for (i = 0; i < num_tests; i++) {
        if (strcmp(tests[i], "smb2.zero-data-ioctl") == 0) {
            char seed_path[512];
            char cmd[1024];
            int  seed_fd, sc_rc;

            snprintf(seed_path, sizeof(seed_path),
                     "%s/empty_seed", env.session_dir);
            seed_fd = open(seed_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
            if (seed_fd >= 0) {
                close(seed_fd);
            }
            snprintf(cmd, sizeof(cmd),
                     "smbclient //127.0.0.1/share -U myuser%%mypassword"
                     " -c 'put %s torture_test_file' >/dev/null 2>&1",
                     seed_path);
            sc_rc = system(cmd);
            unlink(seed_path);
            if (sc_rc != 0) {
                fprintf(stderr, "Failed to pre-create torture_test_file via"
                        " smbclient (rc=%d)\n", sc_rc);
            }
            break;
        }
    }

    /* Run smbtorture */
    rc = run_smbtorture(num_tests, tests, backend, env.session_dir);

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
