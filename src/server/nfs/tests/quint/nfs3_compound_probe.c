// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense

/* Real RPC regression with a test-only interposed compound submit. Rejections
 * are confined to read-only attempts (or SETATTR rejected by its guard), so
 * this fixture makes no backend rollback claim. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "vfs/vfs_compound.h"

static atomic_int                        armed;
static atomic_uint                       submissions, finishes;
static unsigned                          rejection;
static enum chimera_vfs_compound_op_type expected_last;

struct fixture {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned                        attempts;
};

static void
finish_attempt(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct fixture *f = private_data;

    f->attempts++;
    atomic_fetch_add(&finishes, 1);
    if (rejection) {
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            switch (op->type) {
                case CHIMERA_VFS_COMPOUND_OP_PUTFH:
                case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
                case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
                case CHIMERA_VFS_COMPOUND_OP_GETATTR:
                case CHIMERA_VFS_COMPOUND_OP_READ:
                case CHIMERA_VFS_COMPOUND_OP_READDIR:
                    break;
                case CHIMERA_VFS_COMPOUND_OP_SETATTR:
                    /* A failed guard must stop before any mutation. */
                    assert(op->status == CHIMERA_VFS_UNSET);
                    break;
                default:
                    assert(!"unsafe operation in finish rejection fixture");
            } /* switch */
        }
    }
    enum chimera_vfs_error status = CHIMERA_VFS_OK;
    if (rejection == 2 || (rejection == 1 && f->attempts == 1)) {
        status = CHIMERA_VFS_EAGAIN;
    } else if (rejection == 3) {
        status = CHIMERA_VFS_EIO;
    }
    chimera_vfs_compound_finish_result(compound, status);
} /* finish_attempt */

static void
complete_attempt(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct fixture                 *f        = private_data;
    chimera_vfs_compound_callback_t callback = f->callback;
    void                           *caller   = f->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN || f->attempts == 9) {
        free(f);
    }
    callback(compound, caller);
} /* complete_attempt */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn       next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (!atomic_load(&armed)) {
        next(compound, callback, private_data);
        return;
    }
    atomic_fetch_add(&submissions, 1);
    assert(chimera_vfs_compound_op(compound, chimera_vfs_compound_num_ops(compound) - 1)->type == expected_last);
    struct fixture *f = calloc(1, sizeof(*f));
    assert(f);
    f->callback     = callback;
    f->private_data = private_data;
    chimera_vfs_compound_set_finish_handler(compound, finish_attempt, f);
    next(compound, complete_attempt, f);
} /* chimera_vfs_compound_submit */

static void
begin(
    enum chimera_vfs_compound_op_type last,
    unsigned                          reject)
{
    expected_last = last;
    rejection     = reject;
    atomic_store(&submissions, 0);
    atomic_store(&finishes, 0);
    atomic_store(&armed, 1);
} /* begin */

static void
check(
    struct mbt_result *res,
    unsigned           status,
    unsigned           attempts)
{
    atomic_store(&armed, 0);
    assert(!res->rpc_err && res->status == status);
    assert(atomic_load(&submissions) == 1);
    assert(atomic_load(&finishes) == attempts);
} /* check */

static void
check_entries(
    struct mbt_result *res,
    const char        *name)
{
    int found = 0;

    assert(!res->entries_overflow && res->eof);
    for (int i = 0; i < res->nentries; i++) {
        if (!strcmp(res->entries[i].name, name)) {
            found++;
        }
        for (int j = 0; j < i; j++) {
            assert(strcmp(res->entries[i].name, res->entries[j].name));
        }
    }
    assert(found == 1);
} /* check_entries */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env     *env     = malloc(sizeof(*env));
    struct mbt_env_opts opts    = { 0 };
    const char         *backend = "memfs";

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-b") && i + 1 < argc) {
            backend = argv[++i];
        }
    }
    assert(env);
    opts.module = backend;
    opts.sec    = mbt_sec_scan_argv(argc, argv);
    mbt_watchdog_arm(90);
    mbt_env_start_opts(env, &opts);
    struct mbt_result *res = mbt_mnt(env, "/fs0");
    assert(!res->rpc_err && res->status == MNT3_OK);
    struct mbt_fh      root      = res->obj_fh;
    const uint8_t      bytes[]   = "retained request and reply payload";
    const uint8_t      verf_a[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    const uint8_t      verf_b[8] = { 1, 2, 3, 4, 5, 6, 7, 9 };

    begin(CHIMERA_VFS_COMPOUND_OP_OPEN, 0);
    res = mbt_create(env, &root, "file", 4, EXCLUSIVE, 0600, verf_a);
    check(res, NFS3_OK, 1);
    struct mbt_fh      file = res->obj_fh;
    assert(file.has);
    begin(CHIMERA_VFS_COMPOUND_OP_OPEN, 0);
    check(mbt_create(env, &root, "file", 4, EXCLUSIVE, 0600, verf_a), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_OPEN, 0);
    check(mbt_create(env, &root, "file", 4, EXCLUSIVE, 0600, verf_b), NFS3ERR_EXIST, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_LOOKUP, 1);
    res = mbt_lookup(env, &root, "file", 4);
    check(res, NFS3_OK, 2);
    assert(res->obj_fh.len == file.len && !memcmp(res->obj_fh.data, file.data, file.len));

    begin(CHIMERA_VFS_COMPOUND_OP_WRITE, 0);
    res = mbt_write(env, &file, 0, bytes, sizeof(bytes), FILE_SYNC);
    check(res, NFS3_OK, 1);
    assert(res->count == sizeof(bytes) && res->wcc_after.has);
    begin(CHIMERA_VFS_COMPOUND_OP_READ, 1);
    res = mbt_read(env, &file, 0, sizeof(bytes));
    check(res, NFS3_OK, 2);
    assert(res->data_len == sizeof(bytes) && !memcmp(res->data, bytes, sizeof(bytes)));
    begin(CHIMERA_VFS_COMPOUND_OP_READ, 3);
    res = mbt_read(env, &file, 0, sizeof(bytes));
    check(res, NFS3ERR_IO, 1);
    assert(!res->obj_attrs.has && !res->data_len);
    begin(CHIMERA_VFS_COMPOUND_OP_GETATTR, 2);
    check(mbt_getattr(env, &file), NFS3ERR_JUKEBOX, 9);

    struct nfstime3 guard = { 0, 0 };
    begin(CHIMERA_VFS_COMPOUND_OP_SETATTR, 1);
    check(mbt_setattr(env, &file, -1, 1, &guard), NFS3ERR_NOT_SYNC, 2);
    res = mbt_read(env, &file, 0, sizeof(bytes));
    assert(res->status == NFS3_OK && res->data_len == sizeof(bytes));
    res = mbt_getattr(env, &file);
    assert(res->status == NFS3_OK && res->obj_attrs.has);
    guard = res->obj_attrs.a.ctime;
    begin(CHIMERA_VFS_COMPOUND_OP_SETATTR, 0);
    res = mbt_setattr(env, &file, 0644, -1, &guard);
    check(res, NFS3_OK, 1);
    assert(res->wcc_after.has && res->wcc_after.a.mode == 0644);
    begin(CHIMERA_VFS_COMPOUND_OP_COMMIT, 0);
    check(mbt_commit(env, &file), NFS3_OK, 1);

    begin(CHIMERA_VFS_COMPOUND_OP_CREATE, 0);
    res = mbt_mkdir(env, &root, "dir", 3, 0755);
    check(res, NFS3_OK, 1);
    struct mbt_fh dir = res->obj_fh;
    begin(CHIMERA_VFS_COMPOUND_OP_CREATE, 0);
    check(mbt_symlink(env, &root, "symlink", 7, "file", 0777), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_CREATE, 0);
    check(mbt_mknod(env, &root, "fifo", 4, NF3FIFO, 0644), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_LINK, 0);
    check(mbt_link(env, &file, &dir, "linked", 6), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_RENAME, 0);
    check(mbt_rename(env, &dir, "linked", 6, &dir, "renamed", 7), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_READDIR, 1);
    res = mbt_readdir(env, &dir);
    check(res, NFS3_OK, 2);
    check_entries(res, "renamed");
    begin(CHIMERA_VFS_COMPOUND_OP_READDIR, 1);
    res = mbt_readdirplus(env, &dir);
    check(res, NFS3_OK, 2);
    check_entries(res, "renamed");
    for (int i = 0; i < res->nentries; i++) {
        assert(res->entries[i].attrs.has && res->entries[i].fh.has);
    }
    begin(CHIMERA_VFS_COMPOUND_OP_REMOVE, 0);
    check(mbt_remove(env, &dir, "renamed", 7), NFS3_OK, 1);
    begin(CHIMERA_VFS_COMPOUND_OP_REMOVE, 0);
    check(mbt_rmdir(env, &root, "dir", 3), NFS3_OK, 1);
    /* Compound construction must preserve the VFS's overlong-name error. */
    char long_name[257];
    memset(long_name, 'x', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = 0;
    assert(mbt_lookup(env, &root, long_name, 256)->status == NFS3ERR_NAMETOOLONG);
    assert(mbt_create(env, &root, long_name, 256, UNCHECKED, 0600, NULL)->status == NFS3ERR_NAMETOOLONG);
    assert(mbt_mkdir(env, &root, long_name, 256, 0700)->status == NFS3ERR_NAMETOOLONG);
    assert(mbt_remove(env, &root, long_name, 256)->status == NFS3ERR_NAMETOOLONG);
    assert(mbt_rename(env, &root, "file", 4, &root, long_name, 256)->status == NFS3ERR_NAMETOOLONG);
    assert(mbt_link(env, &file, &root, long_name, 256)->status == NFS3ERR_NAMETOOLONG);
    mbt_env_stop(env);
    free(env);
    printf("NFS3 one-compound RPCs, EXCLUSIVE, guarded SETATTR, payload and directory retries passed (%s)\n", backend);
    return 0;
} /* main */
