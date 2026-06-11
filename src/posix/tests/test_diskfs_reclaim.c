// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs space-reclaim + AG-log condensation test.
 *
 * Verifies that deleted-inode space actually returns to the filesystem:
 * create+write+delete cycles must converge back to the baseline free space
 * (data extents, b+tree nodes, AND the 4 KiB inode home blocks -- nothing may
 * bleed), unlink-while-open must hold the space until the last close and
 * reclaim it afterwards, and a reclaim backlog must survive a cold remount
 * (DISKFS_DRAIN_DISABLE suppresses the in-session drain so the next mount's
 * orphan scan does the work).  DISKFS_AG_CONDENSE_DELTAS lowers the AG-log
 * condensation trigger so the churn above exercises runtime condensation
 * (park -> snapshot -> slot flip -> resume) many times over.
 */

#include <sys/vfs.h>

#include "posix_test_common.h"

#define RECLAIM_CYCLES \
        (getenv("RECLAIM_CYCLES") ? atoi(getenv("RECLAIM_CYCLES")) : 1500)
#define RECLAIM_FILE_BYTES (256 * 1024)

/* Free-space convergence slack: steady-state metadata that legitimately
 * stays allocated (directory tree nodes, AG-log churn) is far below this. */
#define RECLAIM_SLACK      (8ULL << 20)

static uint64_t
free_bytes(struct posix_test_env *env)
{
    struct statfs sf;

    if (chimera_posix_statfs("/test", &sf) != 0) {
        fprintf(stderr, "statfs failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }
    return (uint64_t) sf.f_bfree * (uint64_t) sf.f_bsize;
} /* free_bytes */

/* Reclaim is asynchronous: poll until free space is within slack of the
 * baseline (or time out). */
static void
expect_convergence(
    struct posix_test_env *env,
    uint64_t               baseline,
    const char            *phase)
{
    uint64_t now_free = 0;
    int      i;

    for (i = 0; i < 600; i++) {
        now_free = free_bytes(env);
        if (now_free + RECLAIM_SLACK >= baseline) {
            fprintf(stderr, "%s: converged (baseline=%lu free=%lu)\n",
                    phase, baseline, now_free);
            return;
        }
        usleep(100000);
    }

    fprintf(stderr, "%s: space did not converge: baseline=%lu free=%lu "
            "(leaked ~%lu bytes)\n",
            phase, baseline, now_free, baseline - now_free);
    posix_test_fail(env);
} /* expect_convergence */

static void
write_file(
    struct posix_test_env *env,
    const char            *path,
    int                    keep_open,
    int                   *r_fd)
{
    static char buf[64 * 1024];
    int         fd, i, rc;

    fd = chimera_posix_open(path, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "create %s failed: %s\n", path, strerror(errno));
        posix_test_fail(env);
    }

    memset(buf, 0xa5, sizeof(buf));
    for (i = 0; i < (int) (RECLAIM_FILE_BYTES / sizeof(buf)); i++) {
        rc = (int) chimera_posix_write(fd, buf, sizeof(buf));
        if (rc != (int) sizeof(buf)) {
            fprintf(stderr, "write %s failed: %s\n", path, strerror(errno));
            posix_test_fail(env);
        }
    }

    if (keep_open) {
        *r_fd = fd;
    } else {
        chimera_posix_close(fd);
    }
} /* write_file */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    char                  path[128];
    uint64_t              baseline;
    int                   fd = -1, rc, i;

    /* Exercise AG-log runtime condensation continuously: condense every 64
     * deltas instead of at the 4 MiB slot high-water. */
    setenv("DISKFS_AG_CONDENSE_DELTAS", "64", 1);

    posix_test_init(&env, argv, argc);
    ChimeraLogLevel = CHIMERA_LOG_INFO;

    if (!posix_test_is_diskfs(env.backend)) {
        fprintf(stderr, "diskfs-only test, nothing to do for %s\n", env.backend);
        posix_test_success(&env);
        return 0;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_mkdir("/test/d", 0755);
    if (rc != 0) {
        fprintf(stderr, "mkdir failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Warm-up: every client worker that ever allocates retains up to 1 MiB
     * of space-map reservation as working capital (by design, not a leak).
     * Touch them all with a round of churn BEFORE taking the baseline, so
     * the convergence checks below measure true bleed. */
    fprintf(stderr, "warm-up...\n");
    for (i = 0; i < 200; i++) {
        snprintf(path, sizeof(path), "/test/d/w%04d", i % 32);
        write_file(&env, path, 0, NULL);
        if (chimera_posix_unlink(path) != 0) {
            fprintf(stderr, "unlink %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }
    /* Let the warm-up's reclaim settle so the baseline is steady. */
    sleep(3);

    baseline = free_bytes(&env);
    fprintf(stderr, "baseline free: %lu bytes\n", baseline);

    /* Phase 1: create+write+delete churn must not bleed space.  This also
     * recycles inums (home blocks return to the allocator), so stale-handle
     * safety rides on the generation epoch throughout. */
    fprintf(stderr, "phase 1: %d create/write/delete cycles...\n",
            RECLAIM_CYCLES);
    for (i = 0; i < RECLAIM_CYCLES; i++) {
        snprintf(path, sizeof(path), "/test/d/f%04d", i % 32);
        write_file(&env, path, 0, NULL);
        if (chimera_posix_unlink(path) != 0) {
            fprintf(stderr, "unlink %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }
    expect_convergence(&env, baseline, "phase 1 (churn)");

    /* Phase 2: unlink-while-open defers reclaim to the final close.  The
     * open handle must keep the (now nameless) file fully readable -- if the
     * extents were reclaimed early this read returns wrong data or fails --
     * and the close must then hand it to the reclaim workers. */
    fprintf(stderr, "phase 2: unlink-while-open...\n");
    write_file(&env, "/test/d/held", 1, &fd);
    if (chimera_posix_unlink("/test/d/held") != 0) {
        fprintf(stderr, "unlink held failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    {
        static char rbuf[64 * 1024];
        int         k, r;

        for (k = 0; k < (int) (RECLAIM_FILE_BYTES / sizeof(rbuf)); k++) {
            r = (int) chimera_posix_pread(fd, rbuf, sizeof(rbuf),
                                          (off_t) k * (off_t) sizeof(rbuf));
            if (r != (int) sizeof(rbuf) ||
                memchr(rbuf, 0, sizeof(rbuf)) != NULL) {
                fprintf(stderr, "phase 2: unlinked-but-open file unreadable "
                        "at %d (r=%d)\n", k, r);
                posix_test_fail(&env);
            }
        }
    }
    chimera_posix_close(fd);
    expect_convergence(&env, baseline, "phase 2 (close reclaim)");

    /* Phase 3: a reclaim backlog survives a cold remount.  Suppress the
     * in-session drain entirely; the deletes only leave durable orphan
     * records, and the next mount's scan must reclaim everything. */
    fprintf(stderr, "phase 3: backlog across cold remount...\n");
    setenv("DISKFS_DRAIN_DISABLE", "1", 1);
    for (i = 0; i < 64; i++) {
        snprintf(path, sizeof(path), "/test/d/b%04d", i);
        write_file(&env, path, 0, NULL);
        if (chimera_posix_unlink(path) != 0) {
            fprintf(stderr, "unlink %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }
    unsetenv("DISKFS_DRAIN_DISABLE");

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "umount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_shutdown();

    {
        char                       diskfs_cfg[4096];
        char                       posix_json_path[300];
        json_t                    *root, *config, *vfs, *vfs_entry;
        struct prometheus_metrics *metrics2;

        posix_test_diskfs_reuse_devices = 1;
        posix_test_configure_diskfs(env.session_dir,
                                    posix_test_diskfs_device_type(env.backend),
                                    diskfs_cfg, sizeof(diskfs_cfg));

        root      = json_object();
        config    = json_object();
        vfs       = json_object();
        vfs_entry = json_object();
        json_object_set_new(vfs_entry, "path", json_string("/build/test/diskfs"));
        json_object_set_new(vfs_entry, "config", json_string(diskfs_cfg));
        json_object_set_new(vfs, "diskfs", vfs_entry);
        json_object_set_new(config, "vfs", vfs);
        json_object_set_new(root, "config", config);
        chimera_test_write_users_json(root);

        snprintf(posix_json_path, sizeof(posix_json_path),
                 "%s/posix_remount.json", env.session_dir);
        json_dump_file(root, posix_json_path, 0);
        json_decref(root);

        metrics2  = prometheus_metrics_create(NULL, NULL, 0);
        env.posix = chimera_posix_init_json(posix_json_path, &env.cred, metrics2);
        if (!env.posix) {
            fprintf(stderr, "Failed to re-initialize POSIX client\n");
            posix_test_fail(&env);
        }
        prometheus_metrics_destroy(env.metrics);
        env.metrics = metrics2;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to re-mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    expect_convergence(&env, baseline, "phase 3 (remount re-drain)");

    if (chimera_posix_rmdir("/test/d") != 0) {
        fprintf(stderr, "rmdir failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "umount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
