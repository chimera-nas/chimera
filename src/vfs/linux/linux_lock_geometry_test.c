// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the real backend's kernel-lock calls with a preopened owner file,
 * avoiding name_to_handle_at privileges and filesystem capability skips. */
#define vfs_linux test_vfs_linux
#include "linux.c"
#undef vfs_linux

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

static unsigned completions;

static void
complete(struct chimera_vfs_request *request)
{
    completions++;
} /* complete */

static struct chimera_vfs_request *
request_init(struct chimera_linux_range_file *file)
{
    struct chimera_vfs_request *request = calloc(1, sizeof(*request));

    CHECK(request != NULL);
    request->complete = complete;
    request->fh_len   = file->fh_len;
    request->fh_hash  = file->fh_hash;
    memcpy(request->fh, file->fh, file->fh_len);
    return request;
} /* request_init */

static void
acquire(
    struct chimera_linux_thread     *thread,
    struct chimera_linux_range_file *file,
    int                              whence,
    uint64_t                         offset,
    uint64_t                         length,
    bool                             exclusive,
    bool                             success)
{
    struct chimera_vfs_request *request = request_init(file);
    unsigned                    before  = completions;

    request->claim_acquire.klass     = CHIMERA_VFS_CLAIM_KLASS_RANGE;
    request->claim_acquire.owner     = file->owner;
    request->claim_acquire.flags     = CHIMERA_VFS_CLAIM_REPLACE;
    request->claim_acquire.whence    = whence;
    request->claim_acquire.offset    = offset;
    request->claim_acquire.length    = length;
    request->claim_acquire.exclusive = exclusive;
    chimera_linux_claim_acquire(request, thread);
    CHECK(completions == before + 1);
    if (success) {
        CHECK(request->status == CHIMERA_VFS_OK && request->claim_acquire.r_granted);
    } else {
        CHECK(request->status != CHIMERA_VFS_OK || !request->claim_acquire.r_granted);
    }
    free(request);
} /* acquire */

static void
release(
    struct chimera_linux_thread     *thread,
    struct chimera_linux_range_file *file,
    uint64_t                         token,
    int                              whence,
    uint64_t                         offset,
    uint64_t                         length)
{
    struct chimera_vfs_request *request = request_init(file);
    unsigned                    before  = completions;

    request->claim_release.klass  = CHIMERA_VFS_CLAIM_KLASS_RANGE;
    request->claim_release.owner  = file->owner;
    request->claim_release.token  = token;
    request->claim_release.whence = whence;
    request->claim_release.offset = offset;
    request->claim_release.length = length;
    chimera_linux_claim_release(request, thread);
    CHECK(completions == before + 1 && request->status == CHIMERA_VFS_OK);
    free(request);
} /* release */

static short
probe(
    int   fd,
    off_t offset,
    off_t length)
{
    struct flock fl = {
        .l_type   = F_WRLCK,
        .l_whence = SEEK_SET,

        .l_start = offset,
        .l_len   = length
    };

    CHECK(fcntl(fd, CHIMERA_LINUX_LOCK_GET, &fl) == 0);
    return fl.l_type;
} /* probe */

int
main(void)
{
#ifndef F_OFD_SETLK
    return 77;
#else  /* ifndef F_OFD_SETLK */
    struct chimera_linux_shared      shared = {
        0
    };
    struct chimera_linux_thread      thread = {
        .shared = &shared
    };
    struct chimera_linux_range_file *file   = calloc(1, sizeof(*file));
    char                             path[] = "/tmp/chimera-lock-geometry-XXXXXX";
    int                              fd     = mkstemp(path);
    CHECK(fd >= 0 && file != NULL && ftruncate(fd, 4096) == 0);
    int                              observer = open(path, O_RDWR);
    CHECK(observer >= 0);
    unlink(path);
    evpl_mutex_init(&shared.range_lock, NULL);
    file->fd             = fd;
    file->refcnt         = 1; /* harness pin across whole-owner cleanup */
    file->fh[0]          = 1;
    file->fh_len         = 1;
    file->fh_hash        = 77;
    file->owner.proto    = CHIMERA_CLAIM_PROTO_POSIX;
    file->owner.owner_lo = 1;
    shared.range_files   = file;

    /* No lock anchor exists yet, but invalid EOF-relative unlock geometry
     * must still fail rather than being treated as a successful no-op. */
    struct chimera_vfs_request *invalid = request_init(file);
    invalid->claim_release.klass  = CHIMERA_VFS_CLAIM_KLASS_RANGE;
    invalid->claim_release.owner  = file->owner;
    invalid->claim_release.whence = SEEK_END;
    invalid->claim_release.offset = (uint64_t) -4097LL;
    invalid->claim_release.length = 1;
    unsigned                    before_invalid = completions;
    chimera_linux_claim_release(invalid, &thread);
    CHECK(completions == before_invalid + 1 && invalid->status == CHIMERA_VFS_EINVAL);
    CHECK(shared.ranges == NULL && file->refcnt == 1);
    free(invalid);

    acquire(&thread, file, SEEK_SET, 0, 100, true, true);
    CHECK(probe(observer, 0, 100) == F_WRLCK);
    acquire(&thread, file, SEEK_SET, 25, 50, false, true);
    CHECK(probe(observer, 0, 25) == F_WRLCK);
    CHECK(probe(observer, 25, 50) == F_RDLCK);
    CHECK(probe(observer, 75, 25) == F_WRLCK);
    CHECK(shared.ranges && shared.ranges->owner_anchor && !shared.ranges->next);
    release(&thread, file, 0, SEEK_SET, 40, 10);
    CHECK(probe(observer, 40, 10) == F_UNLCK);
    CHECK(probe(observer, 25, 15) == F_RDLCK);
    CHECK(probe(observer, 50, 25) == F_RDLCK);

    acquire(&thread, file, SEEK_END, (uint64_t) -32LL, 16, true, true);
    CHECK(probe(observer, 4064, 16) == F_WRLCK);
    release(&thread, file, 0, SEEK_END, (uint64_t) -24LL, 8);
    CHECK(probe(observer, 4064, 8) == F_WRLCK);
    CHECK(probe(observer, 4072, 8) == F_UNLCK);
    acquire(&thread, file, SEEK_END, (uint64_t) -16LL, (uint64_t) -8LL, true, true);
    CHECK(probe(observer, 4072, 8) == F_WRLCK);
    acquire(&thread, file, SEEK_END, INT64_MAX, 1, true, false);
    CHECK(probe(observer, 4072, 8) == F_WRLCK);
    release(&thread, file, 0, SEEK_SET, 0, UINT64_MAX);
    CHECK(shared.ranges == NULL && file->refcnt == 1);
    CHECK(probe(observer, 0, 0) == F_UNLCK);

    /* A legacy token survives a middle carve as two records. Releasing it
     * must remove both fragments without unlocking unrelated geometry. */
    struct flock                fl = {
        .l_type  = F_WRLCK,
        .l_whence
            =
                SEEK_SET,

        .l_start = 100,
        .l_len
            =
                100
    }
    ;
    CHECK(fcntl(fd, CHIMERA_LINUX_LOCK_SET, &fl) == 0);
    struct chimera_linux_range *legacy = calloc(1, sizeof(*legacy));
    CHECK(legacy != NULL);
    legacy->token     = 4242;
    legacy->file      = file;
    legacy->projected = 1;
    legacy->offset    = 100;
    legacy->length    = 100;
    file->refcnt++;
    shared.ranges = legacy;
    release(&thread, file, 0, SEEK_SET, 140, 20);
    CHECK(probe(observer, 100, 40) == F_WRLCK);
    CHECK(probe(observer, 140, 20) == F_UNLCK);
    CHECK(probe(observer, 160, 40) == F_WRLCK);
    CHECK(shared.ranges && shared.ranges->next && !shared.ranges->next->next);
    release(&thread, file, 4242, SEEK_SET, 0, 0);
    CHECK(shared.ranges == NULL && file->refcnt == 1);
    CHECK(probe(observer, 0, 0) == F_UNLCK);

    chimera_linux_range_file_put(&shared, file);
    close(observer);
    evpl_mutex_destroy(&shared.range_lock);
    puts("ok: typed owner anchors preserve replacement/EOF geometry; legacy unlock splits preserve token coverage");
    return 0;
#endif /* ifndef F_OFD_SETLK */
} /* main */
