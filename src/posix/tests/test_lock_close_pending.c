// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense
#include "common/thread.h"
#include <stdatomic.h>
#include "posix_test_common.h"

extern unsigned int chimera_test_lock_submission_count(
    void) __attribute__((weak));
extern void chimera_test_wait_lock_submission(
    unsigned int) __attribute__((weak));
extern void chimera_test_reject_lock_finishes(
    unsigned int,
    const void *,
    size_t) __attribute__((weak));
extern unsigned int chimera_test_lock_finish_attempts(
    void) __attribute__((weak));


#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s errno=%d\n", __FILE__, __LINE__, #c, errno); abort(); } } \
        while (0)

static const uint64_t owner_a = 0xa1, owner_b = 0xb2, owner_c = 0xc3;

struct waiting_lock { int fd, result, error; };
static void *
lock_waiter(void *arg)
{
    struct waiting_lock *waiter = arg;
    struct flock         fl     = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 16 };

    chimera_posix_set_lock_owner(&owner_b);
    waiter->result = chimera_posix_fcntl(waiter->fd, F_SETLKW, &fl);
    waiter->error  = errno;
    return NULL;
} /* lock_waiter */

struct crossed_dup { int from, to; pthread_barrier_t *barrier; };
static void *
dup_worker(void *arg)
{
    struct crossed_dup *ctx = arg;

    for (int i = 0; i < 64; i++) {
        pthread_barrier_wait(ctx->barrier);
        CHECK(chimera_posix_dup2(ctx->from, ctx->to) == ctx->to);
    }
    return NULL;
} /* dup_worker */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    pthread_t             thread;

    alarm(60);
    CHECK(chimera_test_lock_submission_count && chimera_test_wait_lock_submission);
    posix_test_init(&env, argv, argc);
    CHECK(posix_test_mount(&env) == 0);
    for (int sibling = 0; sibling < 2; sibling++) {
        chimera_posix_set_lock_owner(&owner_a);
        int                 held    = chimera_posix_open("/test/pending-close", O_RDWR | O_CREAT, 0644);
        int                 waiting = chimera_posix_open("/test/pending-close", O_RDWR, 0);
        int                 closing = sibling ? chimera_posix_dup(waiting) : waiting;
        CHECK(held >= 0 && waiting >= 0 && closing >= 0);
        struct flock        fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_len = 16 };
        CHECK(chimera_posix_fcntl(held, F_SETLK, &fl) == 0);
        struct waiting_lock waiter   = { .fd = waiting };
        unsigned int        previous = chimera_test_lock_submission_count();
        CHECK(pthread_create(&thread, NULL, lock_waiter, &waiter) == 0);
        /* No sleeps: wait until the compound executor has parked this owner. */
        chimera_test_wait_lock_submission(previous);
        chimera_posix_set_lock_owner(&owner_b);
        CHECK(chimera_posix_close(closing) == 0);
        /* Reuse the just-closed slot immediately; the cancelled request must
         * release only its old pin and must not touch the replacement. */
        int                 reused = chimera_posix_open("/test/reused-after-close", O_RDWR | O_CREAT, 0644);
        CHECK(reused == closing);
        CHECK(pthread_join(thread, NULL) == 0);
        CHECK(waiter.result == -1 && (waiter.error == EINTR || waiter.error == ECANCELED || waiter.error == EBADF));
        CHECK(chimera_posix_write(reused, "x", 1) == 1);
        chimera_posix_set_lock_owner(&owner_a);
        fl.l_type = F_UNLCK;
        CHECK(chimera_posix_fcntl(held, F_SETLK, &fl) == 0);
        chimera_posix_set_lock_owner(&owner_c);
        fl.l_type = F_WRLCK;
        CHECK(chimera_posix_fcntl(held, F_GETLK, &fl) == 0 && fl.l_type == F_UNLCK);
        CHECK(chimera_posix_close(reused) == 0);
        if (sibling) {
            chimera_posix_set_lock_owner(&owner_b);
            CHECK(chimera_posix_close(waiting) == 0);
        }
        /* The GETLK observer is a different simulated process. Retire A's
         * backend owner before test teardown unmounts this filesystem. */
        chimera_posix_set_lock_owner(&owner_a);
        CHECK(chimera_posix_close(held) == 0);
    }

    /* Crossed replacement must not retain the source fd pin the other call
     * is trying to drain. Exercise both same-file description lifetime and
     * multiple replacements of each descriptor slot. */
    int                first  = chimera_posix_open("/test/cross-dup-a", O_RDWR | O_CREAT, 0644);
    int                second = chimera_posix_open("/test/cross-dup-b", O_RDWR | O_CREAT, 0644);
    CHECK(first >= 0 && second >= 0);
    pthread_barrier_t  barrier;
    CHECK(pthread_barrier_init(&barrier, NULL, 2) == 0);
    struct crossed_dup left = { first, second, &barrier }, right = { second, first, &barrier };
    pthread_t          other;
    CHECK(pthread_create(&thread, NULL, dup_worker, &left) == 0);
    CHECK(pthread_create(&other, NULL, dup_worker, &right) == 0);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(pthread_join(other, NULL) == 0);
    pthread_barrier_destroy(&barrier);
    CHECK(chimera_posix_write(first, "a", 1) == 1);
    CHECK(chimera_posix_write(second, "b", 1) == 1);
    CHECK(chimera_posix_dup2(first, first) == first);
    CHECK(chimera_posix_close(first) == 0);
    CHECK(chimera_posix_close(second) == 0);
    /* Close cleanup is mandatory and non-retryable, even when an injected
     * finish rejects it. The error reaches close, but no lock survives it. */
    chimera_posix_set_lock_owner(&owner_a);
    int          cleanup  = chimera_posix_open("/test/cleanup-rejected-finish", O_RDWR | O_CREAT, 0644);
    int          observer = chimera_posix_open("/test/cleanup-rejected-finish", O_RDWR, 0);
    CHECK(cleanup >= 0 && observer >= 0);
    struct flock cleanup_fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_len = 16 };
    CHECK(chimera_posix_fcntl(cleanup, F_SETLK, &cleanup_fl) == 0);
    chimera_test_reject_lock_finishes(2, NULL, 0);
    errno = 0;
    CHECK(chimera_posix_close(cleanup) == -1 && errno == EAGAIN);
    CHECK(chimera_test_lock_finish_attempts() == 1);
    chimera_posix_set_lock_owner(&owner_b);
    cleanup_fl.l_type = F_WRLCK;
    CHECK(chimera_posix_fcntl(observer, F_GETLK, &cleanup_fl) == 0 && cleanup_fl.l_type == F_UNLCK);
    cleanup_fl.l_type = F_WRLCK;
    CHECK(chimera_posix_fcntl(observer, F_SETLK, &cleanup_fl) == 0);
    CHECK(chimera_posix_close(observer) == 0);

    if (getenv("CHIMERA_TEST_LOCAL_LOCK_RETRY")) {
        /* This variant runs on a backend with local-only lock arbitration.
        * Legacy projected mutation is intentionally not fault-rejected. */
        chimera_posix_set_lock_owner(&owner_a);
        int          fd = chimera_posix_open("/test/lock-finish-retry", O_RDWR | O_CREAT, 0644);
        CHECK(fd >= 0);
        struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_len = 16 };
        chimera_test_reject_lock_finishes(2, NULL, 0);
        CHECK(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0);
        CHECK(chimera_test_lock_finish_attempts() == 3);
        chimera_posix_set_lock_owner(&owner_b);
        fl.l_type = F_RDLCK;
        struct flock original = fl;
        chimera_test_reject_lock_finishes(20, &fl, sizeof(fl));
        errno = 0;
        CHECK(chimera_posix_fcntl(fd, F_GETLK, &fl) == -1 && errno == EAGAIN);
        CHECK(chimera_test_lock_finish_attempts() == 9 && !memcmp(&fl, &original, sizeof(fl)));
        chimera_test_reject_lock_finishes(2, &fl, sizeof(fl));
        CHECK(chimera_posix_fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type == F_WRLCK);
        CHECK(chimera_test_lock_finish_attempts() == 3);
        chimera_posix_set_lock_owner(&owner_a);
        fl.l_type = F_UNLCK;
        chimera_test_reject_lock_finishes(20, NULL, 0);
        errno = 0;
        CHECK(chimera_posix_fcntl(fd, F_SETLK, &fl) == -1 && errno == EAGAIN);
        CHECK(chimera_test_lock_finish_attempts() == 9);
        chimera_posix_set_lock_owner(&owner_b);
        fl.l_type = F_RDLCK;
        CHECK(chimera_posix_fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type == F_WRLCK);
        chimera_posix_set_lock_owner(&owner_a);
        fl.l_type = F_UNLCK;
        chimera_test_reject_lock_finishes(2, NULL, 0);
        CHECK(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0);
        CHECK(chimera_test_lock_finish_attempts() == 3);
        chimera_posix_set_lock_owner(&owner_b);
        fl.l_type = F_WRLCK;
        CHECK(chimera_posix_fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type == F_UNLCK);
        chimera_posix_set_lock_owner(&owner_a);
        CHECK(chimera_posix_close(fd) == 0);
    }
    alarm(0);
    posix_test_success(&env);
    return 0;
} /* main */
