// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs cache-eviction / cold-remount stress.
 *
 * Creates more metadata (one inode home block per file plus the directories'
 * b+tree nodes) than the block cache can hold, with the inode cache shrunk so
 * inode structs recycle constantly.  Every sweep below therefore faults
 * evicted b+tree nodes and inodes back in through the async suspend/resume
 * path -- the workload shape that used to abort the daemon in the sync
 * b+tree wrappers ("suspended on a cache miss").  A final cold remount
 * (fresh process state, empty caches, same devices) re-walks everything from
 * disk.
 */

#include <dirent.h>

#include "posix_test_common.h"

#define EVICT_NDIRS  8
#define EVICT_NFILES 40000

static void
evict_path(
    char  *buf,
    size_t len,
    int    i)
{
    snprintf(buf, len, "/test/d%d/f%06d", i % EVICT_NDIRS, i);
} /* evict_path */

static void
evict_sweep(
    struct posix_test_env *env,
    int                    removed_stride)
{
    char        path[128];
    struct stat st;
    int         i, rc;
    long        expect_files = 0, found_files = 0;

    for (i = 0; i < EVICT_NFILES; i++) {
        int removed = removed_stride && (i % removed_stride) == 0;

        evict_path(path, sizeof(path), i);
        rc = chimera_posix_stat(path, &st);

        if (removed) {
            if (rc == 0 || errno != ENOENT) {
                fprintf(stderr, "stat %s: expected ENOENT, got rc=%d errno=%d\n",
                        path, rc, errno);
                posix_test_fail(env);
            }
            continue;
        }

        expect_files++;
        if (rc != 0) {
            fprintf(stderr, "stat %s failed: %s\n", path, strerror(errno));
            posix_test_fail(env);
        }
        if ((st.st_mode & 07777) != (((i & 63) == 0) ? 0750 : 0644)) {
            fprintf(stderr, "stat %s: unexpected mode %o\n", path,
                    st.st_mode & 07777);
            posix_test_fail(env);
        }
    }

    for (i = 0; i < EVICT_NDIRS; i++) {
        CHIMERA_DIR   *dir;
        struct dirent *de;

        snprintf(path, sizeof(path), "/test/d%d", i);
        dir = chimera_posix_opendir(path);
        if (!dir) {
            fprintf(stderr, "opendir %s failed: %s\n", path, strerror(errno));
            posix_test_fail(env);
        }
        while ((de = chimera_posix_readdir(dir)) != NULL) {
            if (de->d_name[0] == 'f') {
                found_files++;
            }
        }
        chimera_posix_closedir(dir);
    }

    if (found_files != expect_files) {
        fprintf(stderr, "readdir found %ld files, expected %ld\n",
                found_files, expect_files);
        posix_test_fail(env);
    }

    fprintf(stderr, "sweep ok: %ld files\n", found_files);
} /* evict_sweep */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    char                  path[128];
    int                   fd, rc, i;

    /* Shrink the inode cache so the working set recycles constantly (each
     * re-fault loads the inode's ACL/pNFS record mirrors through the async
     * b+tree path).  The block cache is already floored at ~24K buffers; the
     * ~40K inode blocks plus directory b+tree nodes created below overflow
     * it, so metadata blocks evict and re-fault under the storm. */
    posix_test_diskfs_extra_cfg = "{\"inode_cache_inodes\":2048}";

    posix_test_init(&env, argv, argc);

    /* The harness defaults to debug logging, which at this op count swamps
     * the run (hundreds of MB of per-request dumps); this is a throughput
     * stress, so keep the log quiet. */
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

    for (i = 0; i < EVICT_NDIRS; i++) {
        snprintf(path, sizeof(path), "/test/d%d", i);
        rc = chimera_posix_mkdir(path, 0755);
        if (rc != 0) {
            fprintf(stderr, "mkdir %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }

    fprintf(stderr, "creating %d files...\n", EVICT_NFILES);
    for (i = 0; i < EVICT_NFILES; i++) {
        evict_path(path, sizeof(path), i);
        fd = chimera_posix_open(path, O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            fprintf(stderr, "create %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
        chimera_posix_close(fd);

        if ((i & 63) == 0) {
            rc = chimera_posix_chmod(path, 0750);
            if (rc != 0) {
                fprintf(stderr, "chmod %s failed: %s\n", path, strerror(errno));
                posix_test_fail(&env);
            }
        }
    }

    evict_sweep(&env, 0);

    fprintf(stderr, "unlinking every 3rd file...\n");
    for (i = 0; i < EVICT_NFILES; i += 3) {
        evict_path(path, sizeof(path), i);
        rc = chimera_posix_unlink(path);
        if (rc != 0) {
            fprintf(stderr, "unlink %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
        if ((i % 6000) == 0) {
            fprintf(stderr, "unlinked through %d\n", i);
        }
    }

    evict_sweep(&env, 3);

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /*
     * Cold remount: tear the client (and the in-process diskfs) down, then
     * bring it back up on the same device images without re-initializing.
     * Every cache starts empty, so the sweep below faults everything --
     * dinodes, record mirrors, directory b+tree nodes -- in from disk
     * through the async path.
     */
    fprintf(stderr, "cold remount...\n");
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
        fprintf(stderr, "Failed to re-mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    evict_sweep(&env, 3);

    fprintf(stderr, "unlinking the rest...\n");
    for (i = 0; i < EVICT_NFILES; i++) {
        if ((i % 3) == 0) {
            continue;
        }
        evict_path(path, sizeof(path), i);
        rc = chimera_posix_unlink(path);
        if (rc != 0) {
            fprintf(stderr, "unlink %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }

    for (i = 0; i < EVICT_NDIRS; i++) {
        snprintf(path, sizeof(path), "/test/d%d", i);
        rc = chimera_posix_rmdir(path);
        if (rc != 0) {
            fprintf(stderr, "rmdir %s failed: %s\n", path, strerror(errno));
            posix_test_fail(&env);
        }
    }

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
