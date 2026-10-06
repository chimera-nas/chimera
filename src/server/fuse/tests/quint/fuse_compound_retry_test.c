// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Real FUSE requests exercise finish retry and streaming-result reset. Only
 * read-only operations are rejected: this fixture supplies no FS rollback. */
#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <stdatomic.h>
#include "fuse_sim.h"
#include "common/compound_retry.h"

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

static atomic_bool armed;
static atomic_uint finishes, submissions;
static unsigned    rejections;

static void
reject_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    (void) private_data;
    unsigned attempt = atomic_fetch_add(&finishes, 1);
    chimera_vfs_compound_finish_result(compound, attempt < rejections ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* reject_finish */

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
    submit_fn real_submit = dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    CHECK(real_submit);
    if (atomic_load(&armed)) {
        CHECK(atomic_fetch_add(&submissions, 1) == 0);
        for (unsigned i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            switch (op->type) {
                case CHIMERA_VFS_COMPOUND_OP_PUTFH:
                case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
                case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
                case CHIMERA_VFS_COMPOUND_OP_ACCESS:
                case CHIMERA_VFS_COMPOUND_OP_GETATTR:
                case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
                case CHIMERA_VFS_COMPOUND_OP_READ:
                case CHIMERA_VFS_COMPOUND_OP_READDIR:
                    break;
                case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
                    CHECK(!(op->open_flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE)));
                    break;
                default: CHECK(false);
            } /* switch */
        }
        chimera_vfs_compound_set_finish_handler(compound, reject_finish, NULL);
    }
    real_submit(compound, callback, private_data);
} /* chimera_vfs_compound_submit */

static void
arm(unsigned count)
{
    rejections = count;
    atomic_store(&finishes, 0);
    atomic_store(&submissions, 0);
    atomic_store(&armed, true);
} /* arm */

static void
check_attempts(unsigned count)
{
    atomic_store(&armed, false);
    CHECK(atomic_load(&submissions) == 1 && atomic_load(&finishes) == count);
} /* check_attempts */

int
main(void)
{
    struct fuse_sim       sim;
    struct fuse_entry_out entry = { 0 }, found = { 0 };
    struct fuse_open_out  file = { 0 }, reopened = { 0 }, directory = { 0 };
    struct fuse_attr_out  attr = { 0 };
    uint32_t              written;
    uint64_t              storage[512];
    size_t                length;

    fuse_sim_open(&sim, "retryfs");
    CHECK(fuse_sim_create(&sim, FUSE_ROOT_ID, "retry-file", 0644, &entry, &file) == 0);
    CHECK(fuse_sim_write(&sim, entry.nodeid, file.fh, 0, "content", 7, &written) == 0 && written == 7);

    arm(2);
    CHECK(fuse_sim_lookup(&sim, FUSE_ROOT_ID, "retry-file", &found) == 0 && found.nodeid == entry.nodeid);
    check_attempts(3);
    arm(1);
    CHECK(fuse_sim_getattr(&sim, entry.nodeid, &attr) == 0 && attr.attr.size == 7);
    check_attempts(2);
    arm(1);
    CHECK(fuse_sim_open_file(&sim, entry.nodeid, O_RDONLY, &reopened) == 0 && reopened.fh);
    check_attempts(2);
    arm(2);
    CHECK(fuse_sim_read(&sim, entry.nodeid, reopened.fh, 0, 7, storage, &length) == 0);
    CHECK(length == 7 && !memcmp(storage, "content", 7));
    check_attempts(3);
    CHECK(fuse_sim_release(&sim, entry.nodeid, reopened.fh) == 0);
    arm(1);
    CHECK(fuse_sim_opendir(&sim, FUSE_ROOT_ID, &directory) == 0 && directory.fh);
    check_attempts(2);

    struct fuse_read_in read = { .fh = directory.fh, .size = sizeof(storage) };
    arm(2);
    CHECK(fuse_sim_call(&sim, FUSE_READDIRPLUS, FUSE_ROOT_ID, &read, sizeof(read), NULL,
                        NULL, 0, storage, sizeof(storage), &length) == 0);
    check_attempts(3);
    unsigned            children = 0;
    for (size_t offset = 0; offset < length;) {
        const struct fuse_direntplus *plus = (const struct fuse_direntplus *) ((const char *) storage + offset);
        size_t                        size = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS + plus->dirent.namelen);
        CHECK(size && size <= length - offset);
        if (plus->dirent.namelen == 10 && !memcmp(plus->dirent.name, "retry-file", 10)) {
            CHECK(plus->entry_out.nodeid == entry.nodeid);
            children++;
        }
        offset += size;
    }
    CHECK(children == 1);

    arm(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(fuse_sim_read(&sim, entry.nodeid, file.fh, 0, 7, storage, &length) == EAGAIN);
    check_attempts(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(fuse_sim_read(&sim, entry.nodeid, file.fh, 0, 7, storage, &length) == 0 && length == 7);
    arm(1);
    memset(&found, 0, sizeof(found));
    int error = fuse_sim_lookup(&sim, FUSE_ROOT_ID, "missing", &found);
    CHECK(error == ENOENT || (error == 0 && !found.nodeid));
    check_attempts(2);

    CHECK(fuse_sim_releasedir(&sim, FUSE_ROOT_ID, directory.fh) == 0);
    CHECK(fuse_sim_release(&sim, entry.nodeid, file.fh) == 0);
    CHECK(fuse_sim_unlink(&sim, FUSE_ROOT_ID, "retry-file") == 0);
    fuse_sim_close(&sim);
    puts("ok: FUSE retries finish rejection, resets directory staging, and bounds retry exhaustion");
    return 0;
} /* main */
