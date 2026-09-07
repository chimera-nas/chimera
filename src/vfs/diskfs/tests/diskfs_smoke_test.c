// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs white-box smoke test.  Drives diskfs directly through the VFS on a
 * small pread-backed device and exercises the test SDK (diskfs_test.h): it
 * confirms the space-allocator invariants hold across a directory fan-out (which
 * grows the namespace b+tree), a fragmenting create/delete churn, reclaim
 * quiescence, and a crash + intent-log-replay recovery in which acknowledged
 * data survives.  It needs no trace corpus, so it always runs -- it is the
 * ground truth the SDK the MBT model depends on is built on.
 */

#include "diskfs_test_harness.h"

#define CHECK(dh) do { \
            char _e[256]; \
            if (diskfs_test_check((dh)->vfs, _e, sizeof(_e)) != 0) { \
                fprintf(stderr, "INVARIANT VIOLATION at %s:%d: %s\n", \
                        __func__, __LINE__, _e); \
                exit(1); \
            } \
} while (0)

int
main(
    int    argc,
    char **argv)
{
    struct dh                       dh;
    struct chimera_vfs_open_handle *root, *dirh, *fh;
    struct diskfs_test_space        sp;
    struct diskfs_test_inode        di;
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        dir_fhlen;
    uint64_t                        dir_ino;
    uint8_t                         got[65536];
    char                            name[64];
    int                             i, r;

    (void) argc;
    (void) argv;
    setvbuf(stdout, NULL, _IONBF, 0);

    /* 64 MiB device, 4 MiB (floor) intent log, deliberately small block cache
     * (512 * 4 KiB = 2 MiB) so a large tree evicts nodes and later structural
     * ops fault cold siblings back in. */
    dh_init(&dh, 1, 64ULL * 1024 * 1024, 4ULL * 1024 * 1024, 16384);

    root = dh_root_handle(&dh);
    CHECK(&dh);

    r = dh_mkdir(&dh, root, "d", dir_fh, &dir_fhlen);
    assert(r == CHIMERA_VFS_OK);

    dirh = dh_open_handle(&dh, dir_fh, dir_fhlen);
    assert(dirh != NULL);

    /* Recover the directory's inode number (a lookup fills dh.ino) so we can
     * query its tree geometry through the SDK. */
    dir_ino = 0;
    if (dh_lookup(&dh, dir_fh, dir_fhlen, ".") == CHIMERA_VFS_OK) {
        dir_ino = dh.ino;
    }
    printf("dir inode = %lu\n", (unsigned long) dir_ino);

    /* ---- fan out one directory to grow the namespace b+tree ---- */
    for (i = 0; i < 4000; i++) {
        snprintf(name, sizeof(name), "file_%06d", i);
        r = dh_create(&dh, dirh, name, NULL, NULL, NULL);
        assert(r == CHIMERA_VFS_OK);
        if ((i + 1) % 1000 == 0) {
            CHECK(&dh);
            if (dir_ino && diskfs_test_inode(dh.vfs, dir_ino, &di) == 0) {
                printf("  after %5d dirents: tree_height=%u root_nitems=%u\n",
                       i + 1, di.tree_height, di.root_nitems);
            }
            diskfs_test_space(dh.vfs, &sp);
            printf("  free=%lu/%lu largest_extent=%lu frags=%lu\n",
                   (unsigned long) sp.ag_free_sum, (unsigned long) sp.usable_capacity,
                   (unsigned long) sp.largest_free_extent,
                   (unsigned long) sp.total_free_extents);
        }
    }
    if (dir_ino && diskfs_test_inode(dh.vfs, dir_ino, &di) == 0) {
        printf("final dir tree_height=%u (expect >= 2 after 4000 dirents)\n",
               di.tree_height);
        assert(di.tree_height >= 2);
    }

    /* ---- a fragmented file: scattered holey 4 KiB writes grow its extent map ---- */
    r = dh_create(&dh, dirh, "frag", NULL, NULL, &fh);
    assert(r == CHIMERA_VFS_OK);
    for (i = 0; i < 200; i++) {
        /* stride by 8 KiB so consecutive writes never coalesce */
        r = dh_write(&dh, fh, (uint64_t) i * 8192, 4096, (uint8_t) (i % 251 + 1));
        assert(r == CHIMERA_VFS_OK);
    }
    CHECK(&dh);
    dh_release(&dh, fh);

    /* ---- fragmenting churn: create + delete many small files ---- */
    for (r = 0; r < 6; r++) {
        for (i = 0; i < 400; i++) {
            snprintf(name, sizeof(name), "churn_%03d", i);
            assert(dh_create(&dh, dirh, name, NULL, NULL, NULL) == CHIMERA_VFS_OK);
        }
        for (i = 0; i < 400; i++) {
            snprintf(name, sizeof(name), "churn_%03d", i);
            assert(dh_remove(&dh, dirh, name) == CHIMERA_VFS_OK);
        }
        CHECK(&dh);
    }

    diskfs_test_await_reclaim(dh.vfs, dh.evpl, 5000);
    CHECK(&dh);
    diskfs_test_space(dh.vfs, &sp);
    printf("after churn+reclaim: free=%lu frags=%lu largest=%lu\n",
           (unsigned long) sp.ag_free_sum, (unsigned long) sp.total_free_extents,
           (unsigned long) sp.largest_free_extent);

    /* ---- durable survivors, then crash + recovery ---- */
    {
        struct chimera_vfs_open_handle *sh;

        for (i = 0; i < 4; i++) {
            snprintf(name, sizeof(name), "surv_%d", i);
            assert(dh_create(&dh, dirh, name, NULL, NULL, &sh) == CHIMERA_VFS_OK);
            assert(dh_write(&dh, sh, 0, 4096, (uint8_t) (0xA0 + i)) == CHIMERA_VFS_OK);
            dh_release(&dh, sh);
        }
    }
    dh_release(&dh, dirh);
    dh_release(&dh, root);

    printf("crash + recover...\n");
    dh_remount_crash(&dh);
    CHECK(&dh);

    /* survivors must read back with the exact bytes acknowledged before crash */
    {
        uint8_t  root_fh[CHIMERA_VFS_FH_SIZE], tfh[CHIMERA_VFS_FH_SIZE];
        uint32_t root_len, tlen;

        chimera_vfs_get_root_fh(root_fh, &root_len);
        assert(dh_lookup(&dh, root_fh, root_len, "test") == CHIMERA_VFS_OK);
        tlen = dh.fh_len;
        memcpy(tfh, dh.fh, tlen);
        assert(dh_lookup(&dh, tfh, tlen, "d") == CHIMERA_VFS_OK);
        memcpy(dir_fh, dh.fh, dh.fh_len);
        dir_fhlen = dh.fh_len;

        for (i = 0; i < 4; i++) {
            struct chimera_vfs_open_handle *sh;

            snprintf(name, sizeof(name), "surv_%d", i);
            assert(dh_lookup(&dh, dir_fh, dir_fhlen, name) == CHIMERA_VFS_OK);
            sh = dh_open_handle(&dh, dh.fh, dh.fh_len);
            assert(sh != NULL);
            assert(dh_read(&dh, sh, 0, 4096, got) == CHIMERA_VFS_OK);
            if (got[0] != (uint8_t) (0xA0 + i) || got[4095] != (uint8_t) (0xA0 + i)) {
                fprintf(stderr, "surv_%d corrupt after recovery: got 0x%02x\n",
                        i, got[0]);
                exit(1);
            }
            dh_release(&dh, sh);
        }
    }
    printf("survivors intact after recovery\n");

    dh_fini(&dh);
    printf("PASS\n");
    return 0;
} /* main */
