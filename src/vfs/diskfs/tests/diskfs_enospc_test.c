// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* The bounded-pool alloc06/dealloc06 sequence from nfstest_alloc. Exercise
 * both sides of the reported capacity, before and after journal retirement.
 * The quick test must reject over-allocation as well as accept the initial
 * fill; checking only the latter missed an unenforced internal reserve. */
#include <inttypes.h>
#include "diskfs_test_harness.h"

#define SMALL (256ULL * 1024)
#define BLOCK 4096ULL

struct attr_result {
    struct dh               *dh;
    struct chimera_vfs_attrs attr;
};

static void
attrs_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    void                     *pd)
{
    struct attr_result *result = pd;

    assert(status == CHIMERA_VFS_OK);
    result->attr     = *attr;
    result->dh->done = 1;
} /* attrs_cb */

static struct chimera_vfs_attrs
attrs(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h)
{
    struct attr_result result = { .dh = dh };

    /* STATFS forces dispatch, so size is not answered from the attr cache. */
    chimera_vfs_getattr(dh->thread, &dh->cred, h,
                        CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MASK_STATFS,
                        attrs_cb, &result);
    dh_wait(dh);
    return result.attr;
} /* attrs */

static uint64_t
snapshot(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *phase)
{
    struct diskfs_test_space sp;
    struct chimera_vfs_attrs attr = attrs(dh, h);
    char                     error[256];

    assert(diskfs_test_space(dh->vfs, &sp) == 0);
    printf("%s: reported=%" PRIu64 " live=%" PRIu64 " committed_free=%" PRIu64
           " reserve=%" PRIu64 " claims=%" PRIu64 "\n", phase,
           attr.va_fs_space_avail, sp.available_bytes, sp.ag_free_sum,
           sp.reserve_bytes, sp.claim_bytes);
    assert(attr.va_fs_space_free == attr.va_fs_space_avail);
    assert(sp.available_bytes <= sp.ag_free_sum);
    if (diskfs_test_check(dh->vfs, error, sizeof(error))) {
        fprintf(stderr, "%s: %s\n", phase, error);
        abort();
    }
    return attr.va_fs_space_avail;
} /* snapshot */

static void
check_data(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        off,
    uint8_t                         value)
{
    uint8_t buf[4096];

    assert(dh_read(dh, h, off, sizeof(buf), buf) == CHIMERA_VFS_OK);
    for (unsigned i = 0; i < sizeof(buf); i++) {
        assert(buf[i] == value);
    }
} /* check_data */

int
main(
    int    argc,
    char **argv)
{
    struct dh                       dh;
    struct chimera_vfs_open_handle *root, *small, *big, *other;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fhlen;
    uint64_t                        available, ask, before, fill;

    (void) argc;
    (void) argv;
    setvbuf(stdout, NULL, _IONBF, 0);

    dh_init(&dh, 2, 128ULL << 20, 64ULL << 20, 16384);
    root = dh_root_handle(&dh);
    assert(dh_create(&dh, root, "small", fh, &fhlen, &small) == CHIMERA_VFS_OK);
    assert(dh_allocate(&dh, small, 0, SMALL, 0) == CHIMERA_VFS_OK);
    assert(dh_write(&dh, small, 0, BLOCK, 0x5a) == CHIMERA_VFS_OK);
    /* Do not drain the journal before asking what can still be allocated. */
    available = snapshot(&dh, small, "after small allocation");
    assert(available > SMALL);
    ask = (available - SMALL) & ~(BLOCK - 1);
    assert(dh_create(&dh, root, "big", fh, &fhlen, &big) == CHIMERA_VFS_OK);
    assert(dh_allocate(&dh, big, 0, ask, 0) == CHIMERA_VFS_OK);
    snapshot(&dh, big, "after boundary allocation");

    /* An over-allocation may fail after several successful internal bumps.
     * It must roll back every extent and charge, including before retirement. */
    for (int settled = 0; settled < 2; settled++) {
        if (settled) {
            diskfs_test_await_reclaim(dh.vfs, dh.evpl, 30000);
        }
        before = snapshot(&dh, small, settled ? "settled" : "immediate");
        assert(dh_allocate(&dh, small, SMALL, before + SMALL, 0) == CHIMERA_VFS_ENOSPC);
        assert(attrs(&dh, small).va_size == SMALL);
        assert(snapshot(&dh, small, "after rejected allocation") == before);
        check_data(&dh, small, 0, 0x5a);
    }

    assert(dh_create(&dh, root, "other", fh, &fhlen, &other) == CHIMERA_VFS_OK);
    fill = snapshot(&dh, other, "before final fill") & ~(BLOCK - 1);
    assert(fill > 0);
    assert(dh_allocate(&dh, other, 0, fill, 0) == CHIMERA_VFS_OK);
    assert(snapshot(&dh, other, "full") < BLOCK);
    assert(dh_write(&dh, other, fill, BLOCK, 0x22) == CHIMERA_VFS_ENOSPC);
    assert(dh_write(&dh, small, SMALL, BLOCK, 0x33) == CHIMERA_VFS_ENOSPC);
    /* ALLOCATE's guarantee: existing backing remains writable at ENOSPC. */
    assert(dh_write(&dh, small, 0, BLOCK, 0x6b) == CHIMERA_VFS_OK);
    check_data(&dh, small, 0, 0x6b);
    assert(attrs(&dh, small).va_size == SMALL);

    assert(dh_allocate(&dh, small, 0, BLOCK, CHIMERA_VFS_ALLOCATE_DEALLOCATE) == CHIMERA_VFS_OK);
    diskfs_test_await_reclaim(dh.vfs, dh.evpl, 30000);
    assert(snapshot(&dh, small, "after deallocate") >= BLOCK);
    check_data(&dh, small, 0, 0);
    /* Another file consumes the released backing; writing the hole must fail. */
    assert(dh_allocate(&dh, other, fill, BLOCK, 0) == CHIMERA_VFS_OK);
    assert(dh_write(&dh, small, 0, BLOCK, 0x77) == CHIMERA_VFS_ENOSPC);
    check_data(&dh, small, 0, 0);

    dh_release(&dh, small);
    dh_release(&dh, big);
    dh_release(&dh, other);
    dh_release(&dh, root);
    diskfs_test_await_reclaim(dh.vfs, dh.evpl, 30000);
    /* Both clean reload and redo recovery must seed the same live accounting. */
    for (int crash = 0; crash < 2; crash++) {
        if (crash) {
            dh_remount_crash(&dh);
        } else {
            dh_remount_clean(&dh);
        }
        root = dh_root_handle(&dh);
        assert(dh_lookup(&dh, root->fh, root->fh_len, "small") == CHIMERA_VFS_OK);
        small = dh_open_handle(&dh, dh.fh, dh.fh_len);
        assert(small);
        assert(snapshot(&dh, small, crash ? "recovered" : "remounted") < BLOCK);
        assert(dh_write(&dh, small, 0, BLOCK, 0x77) == CHIMERA_VFS_ENOSPC);
        check_data(&dh, small, 0, 0);
        dh_release(&dh, small);
        dh_release(&dh, root);
    }
    root = dh_root_handle(&dh);
    assert(dh_remove(&dh, root, "big") == CHIMERA_VFS_OK);
    assert(dh_remove(&dh, root, "small") == CHIMERA_VFS_OK);
    assert(dh_remove(&dh, root, "other") == CHIMERA_VFS_OK);
    diskfs_test_await_reclaim(dh.vfs, dh.evpl, 30000);
    snapshot(&dh, root, "after delete");
    dh_release(&dh, root);
    dh_fini(&dh);
    puts("PASS");
    return 0;
} /* main */
