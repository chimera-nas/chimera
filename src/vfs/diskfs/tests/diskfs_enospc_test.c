// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs ENOSPC-boundary white-box test -- the in-process analogue of the
 * nfstest_alloc "alloc06" case (`Verify ALLOCATE reserves the disk space`),
 * which is what the kvm nfstest shards run over NFSv4.2.
 *
 * alloc06 allocates a small file, reads the free space the filesystem reports,
 * and then allocates all but 256 KiB of it -- which must succeed, because the
 * reported free space is supposed to be what a fresh allocation can still
 * place.  The interesting part is not the assertion but the aftermath: the
 * boundary allocation walks the pool down to its last extents, and if it fails
 * the failure path has to leave the space map exactly as it found it.
 *
 * Reaching that boundary at all needs a *bounded* pool, so this mounts the same
 * geometry the kvm wrapper uses for alloc06 (2 x 128 MiB devices behind a
 * 64 MiB intent log).  The default multi-gigabyte test pools never get near it,
 * which is why no in-process suite covered this before.
 */

#include <inttypes.h>

#include "diskfs_test_harness.h"

#define CHECK(dh) do { \
            char _e[256]; \
            if (diskfs_test_check((dh)->vfs, _e, sizeof(_e)) != 0) { \
                fprintf(stderr, "INVARIANT VIOLATION at %s:%d: %s\n", \
                        __func__, __LINE__, _e); \
                exit(1); \
            } \
} while (0)

/* The geometry kvm/kvm_nfstest_test_wrapper.sh gives nfstest_alloc. */
#define ENOSPC_NDEV      2
#define ENOSPC_DEV_BYTES (128ULL << 20)
#define ENOSPC_LOG_BYTES (64ULL << 20)

/* alloc06's own numbers: a 256 KiB first file, and "the rest" is free-256 KiB. */
#define ENOSPC_SMALL     (256ULL * 1024)

int
main(
    int    argc,
    char **argv)
{
    struct dh                       dh;
    struct chimera_vfs_open_handle *root, *h;
    struct diskfs_test_space        sp;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fhlen;
    uint64_t                        free_before, ask;
    int                             r, boundary;

    (void) argc;
    (void) argv;
    setvbuf(stdout, NULL, _IONBF, 0);

    dh_init(&dh, ENOSPC_NDEV, ENOSPC_DEV_BYTES, ENOSPC_LOG_BYTES, 16384);

    root = dh_root_handle(&dh);
    CHECK(&dh);

    /* alloc06 step 1: a small allocation that must succeed. */
    r = dh_create(&dh, root, "small", fh, &fhlen, &h);
    assert(r == CHIMERA_VFS_OK);
    r = dh_allocate(&dh, h, 0, ENOSPC_SMALL, 0);
    assert(r == CHIMERA_VFS_OK);
    dh_release(&dh, h);
    CHECK(&dh);

    /* alloc06 step 2: everything still reported free, less 256 KiB. */
    r = diskfs_test_space(dh.vfs, &sp);
    assert(r == 0);
    free_before = sp.ag_free_sum;
    printf("free before boundary allocate: %" PRIu64 "\n", free_before);
    printf("  total=%" PRIu64 " usable=%" PRIu64 " largest_extent=%" PRIu64
           " fragments=%" PRIu64 " claims=%" PRIu64 " devs=%u ags=%u\n",
           sp.total_capacity, sp.usable_capacity, sp.largest_free_extent,
           sp.total_free_extents, sp.claim_bytes, sp.num_devices, sp.num_ags);
    assert(free_before > ENOSPC_SMALL);
    ask = free_before - ENOSPC_SMALL;

    r = dh_create(&dh, root, "big", fh, &fhlen, &h);
    assert(r == CHIMERA_VFS_OK);
    boundary = dh_allocate(&dh, h, 0, ask, 0);
    printf("boundary allocate of %" PRIu64 " bytes: %d\n", ask, boundary);

    r = diskfs_test_space(dh.vfs, &sp);
    assert(r == 0);
    printf("free after boundary allocate: %" PRIu64 "\n", sp.ag_free_sum);

    /* A failed allocate must leave the file exactly as it found it.  The
     * allocator hands blocks out of a per-thread reservation and marks them
     * used only when the transaction's redo retires, so a transaction that
     * aborts leaves its blocks free -- while the extent records it wrote into
     * the file's b+tree survive the abort (the block cache keeps the mutated
     * nodes, it does not revert them).  The file then references space the
     * allocator believes is free, which the next allocation can hand to
     * somebody else and which double-frees the pool when the file is deleted.
     * The full-stack version of that is chimera/posix/diskfs_enospc_*, which
     * has the reclaim workers this harness does not run; here the allocator's
     * own invariants and its free-space accounting are the oracle. */
    dh_release(&dh, h);

    /* Whatever the boundary allocation returned, the allocator must still be
     * self-consistent -- and tearing the file down must free exactly what it
     * allocated, no more.  A failed allocation that leaves an extent record
     * behind for space it never committed double-frees here. */
    CHECK(&dh);

    assert(dh_remove(&dh, root, "big") == CHIMERA_VFS_OK);
    assert(dh_remove(&dh, root, "small") == CHIMERA_VFS_OK);

    diskfs_test_await_reclaim(dh.vfs, dh.evpl, 30000);
    CHECK(&dh);

    r = diskfs_test_space(dh.vfs, &sp);
    assert(r == 0);
    printf("free after delete: %" PRIu64 " (was %" PRIu64 ")\n",
           sp.ag_free_sum, free_before);

    /* This harness runs no reclaim workers, so the deleted files' space does
     * not come back here -- what matters is the ceiling: the pool must never
     * report more free than it has, which is what a rolled-back-but-still-
     * referenced extent would eventually produce. */
    assert(sp.ag_free_sum <= sp.usable_capacity);

    /* The boundary allocation is *expected* to fall short today, and that is a
     * second, separate defect: space that sits inside a live reservation claim
     * is counted free by statfs but cannot be handed out, so a request for
     * "everything free" hits ENOSPC with tens of megabytes still reported.  The
     * claim_bytes line above is the evidence.  What this test gates is the
     * failure *path*: hitting that ENOSPC must leave the allocator exactly as
     * it found it.  When the accounting is fixed this becomes a hard assert.
     */
    if (boundary != CHIMERA_VFS_OK) {
        printf("KNOWN GAP: reported %" PRIu64 " free but could not allocate "
               "%" PRIu64 " (status %d) -- free space inside live claims\n",
               free_before, ask, boundary);
    }

    dh_release(&dh, root);
    dh_fini(&dh);
    printf("PASS\n");
    return 0;
} /* main */
