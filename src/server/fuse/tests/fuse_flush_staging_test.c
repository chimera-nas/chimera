// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the production FLUSH/FSYNC handlers and retry adapter against a
 * controllable completion source. No backend transaction or /dev/fuse is
 * needed: this test checks exactly when owner release and the reply occur. */
#define _GNU_SOURCE 1
#include <stdio.h>
#include "../fuse_proc_io.c"

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

struct chimera_vfs_compound {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    enum chimera_vfs_error execution_status;
    enum chimera_vfs_error finish_status;
    unsigned int                    attempts;
    unsigned int                    ops;
    bool                            retry_available;
    bool                            inline_retry;
};

static struct chimera_vfs_compound    test_compound;
static struct chimera_vfs_open_handle test_handle;
static unsigned                       releases, replies;
static int                            reply_error;
static bool                           expect_release;

struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    (void) thread;
    (void) cred;
    memset(&test_compound, 0, sizeof(test_compound));
    test_compound.retry_available = true;
    return &test_compound;
} /* chimera_vfs_compound_alloc */

int
chimera_vfs_compound_add_puthandle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    flags)
{
    CHECK(handle == &test_handle);
    CHECK(flags == CHIMERA_VFS_OPEN_INFERRED);
    CHECK(compound->ops == 0);
    return compound->ops++;
} /* chimera_vfs_compound_add_puthandle */

int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_mask)
{
    CHECK(offset == 0 && count == 0 && pre_mask == 0);
    CHECK(compound->ops == 1);
    return compound->ops++;
} /* chimera_vfs_compound_add_commit */

void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    CHECK(compound->ops == 2 && releases == 0 && replies == 0);
    compound->callback     = callback;
    compound->private_data = private_data;
    compound->attempts++;
} /* chimera_vfs_compound_submit */

bool
chimera_vfs_compound_retry(struct chimera_vfs_compound *compound)
{
    CHECK(releases == 0 && replies == 0);
    if (!compound->retry_available) {
        return false;
    }
    compound->attempts++;
    if (compound->inline_retry) {
        compound->finish_status = CHIMERA_VFS_OK;
        compound->callback(compound, compound->private_data);
    }
    return true;
} /* chimera_vfs_compound_retry */

enum chimera_vfs_error
chimera_vfs_compound_finish_status(const struct chimera_vfs_compound *compound)
{
    return compound->finish_status;
} /* chimera_vfs_compound_finish_status */

enum chimera_vfs_error
chimera_vfs_compound_status(const struct chimera_vfs_compound *compound)
{
    return compound->finish_status != CHIMERA_VFS_OK ?
           compound->finish_status : compound->execution_status;
} /* chimera_vfs_compound_status */

void
chimera_fuse_locks_release_owner(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint64_t                    owner)
{
    (void) thread;
    (void) mount;
    CHECK(expect_release && releases == 0 && replies == 0);
    CHECK(fh_len == 2 && fh[0] == 123 && fh[1] == 45 && owner == 456);
    releases++;
} /* chimera_fuse_locks_release_owner */

int
chimera_fuse_reply(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len)
{
    (void) req;
    CHECK(payload == NULL && payload_len == 0 && replies == 0);
    CHECK(releases == (unsigned) expect_release);
    replies++;
    reply_error = error;
    return 0;
} /* chimera_fuse_reply */

static void
finish(
    enum chimera_vfs_error execution,
    enum chimera_vfs_error status)
{
    test_compound.execution_status = execution;
    test_compound.finish_status    = status;
    test_compound.callback(&test_compound, test_compound.private_data);
} /* finish */

static void
start(
    struct chimera_fuse_request *req,
    bool                         flush)
{
    struct chimera_fuse_open_file file     = { .handle = &test_handle };
    struct fuse_flush_in          flush_in = {
        .fh = (uint64_t) (uintptr_t) &file, .lock_owner = 456,
    };
    struct fuse_fsync_in          fsync_in = { .fh = (uint64_t) (uintptr_t) &file };

    test_handle.fh_len = 2;
    test_handle.fh[0]  = 123;
    test_handle.fh[1]  = 45;
    releases           = replies = 0;
    reply_error        = -1;
    expect_release     = flush;
    if (flush) {
        chimera_fuse_op_flush(req, NULL, &flush_in, sizeof(flush_in));
        /* Terminal cleanup must use the retained identity, not these inputs. */
        flush_in.lock_owner = 999;
    } else {
        chimera_fuse_op_fsync(req, NULL, &fsync_in, sizeof(fsync_in));
    }
    CHECK(releases == 0 && replies == 0 && test_compound.attempts == 1);
} /* start */

int
main(void)
{
    struct chimera_fuse_thread  thread  = { 0 };
    struct chimera_fuse_mount   mount   = { 0 };
    struct chimera_fuse_channel channel = { .mount = &mount };
    struct chimera_fuse_request req     = { .thread = &thread, .channel = &channel };

    start(&req, true);
    for (unsigned int i = 0; i < 2; i++) {
        finish(CHIMERA_VFS_OK, CHIMERA_VFS_EAGAIN);
        CHECK(releases == 0 && replies == 0);
    }
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_OK);
    CHECK(test_compound.attempts == 3 && releases == 1 && replies == 1 && reply_error == 0);

    start(&req, true);
    for (unsigned int i = 0; i <= CHIMERA_FRONTEND_COMPOUND_RETRIES; i++) {
        finish(CHIMERA_VFS_OK, CHIMERA_VFS_EAGAIN);
    }
    CHECK(test_compound.attempts == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1 &&
          releases == 1 && replies == 1 && reply_error == EAGAIN);

    /* An operation EAGAIN is accepted prefix semantics, never a replay. */
    start(&req, true);
    finish(CHIMERA_VFS_EAGAIN, CHIMERA_VFS_OK);
    CHECK(test_compound.attempts == 1 && releases == 1 && replies == 1 && reply_error == EAGAIN);

    start(&req, true);
    finish(CHIMERA_VFS_EIO, CHIMERA_VFS_OK);
    CHECK(test_compound.attempts == 1 && releases == 1 && replies == 1 && reply_error == EIO);

    start(&req, true);
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_EIO);
    CHECK(test_compound.attempts == 1 && releases == 1 && replies == 1 && reply_error == EIO);

    start(&req, true);
    test_compound.retry_available = false;
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_EAGAIN);
    CHECK(test_compound.attempts == 1 && releases == 1 && replies == 1 && reply_error == EAGAIN);

    start(&req, true);
    test_compound.inline_retry = true;
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_EAGAIN);
    CHECK(test_compound.attempts == 2 && releases == 1 && replies == 1 && reply_error == 0);

    start(&req, false);
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_EAGAIN);
    CHECK(releases == 0 && replies == 0);
    finish(CHIMERA_VFS_OK, CHIMERA_VFS_OK);
    CHECK(test_compound.attempts == 2 && releases == 0 && replies == 1 && reply_error == 0);

    puts("ok: FLUSH owner cleanup occurs once after retries, including close errors; FSYNC retains locks");
    return 0;
} /* main */
