// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * POSIX record-lock semantics that need more than one lock OWNER to observe.
 *
 * The POSIX client identifies a lock owner by process, so a single-process
 * test cannot normally see a conflict at all -- an owner never conflicts
 * with itself.  chimera_posix_set_lock_owner() lets this test act as several
 * logical processes on one thread, which is the same facility the quint MBT
 * harness uses to give each model process an identity of its own.
 *
 * Covers the three rules that distinguish POSIX record locks from
 * OFD/BSD-style locks:
 *
 *   - an F_UNLCK whose range is WIDER than the held lock releases it
 *     (the release must not require an exact geometry match);
 *   - a new lock by the SAME owner over bytes it already holds is granted,
 *     replacing the old coverage, across descriptions as well;
 *   - closing ANY descriptor for a file drops ALL of that process's locks on
 *     the file, including ones taken through a different description
 *     (XSH fcntl: "All locks associated with a file for a given process
 *     shall be removed when a file descriptor for that file is closed");
 *   - a re-lock REPLACES the owner's coverage of the bytes it spans, so a
 *     downgrade really downgrades.  A stale write fragment left under a
 *     downgraded range is invisible to its own owner and denies everyone
 *     else -- and blocks an F_SETLKW for good.
 */

#include "posix_test_common.h"

static struct flock
lock_desc(
    short         type,
    chimera_off_t start,
    chimera_off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type   = type;
    fl.l_whence = SEEK_SET;
    fl.l_start  = start;
    fl.l_len    = len;
    return fl;
} /* lock_desc */

static int fails;

static void
expect(
    int         ok,
    const char *what)
{
    if (!ok) {
        fprintf(stderr, "FAIL: %s\n", what);
        fails++;
    }
} /* expect */

static const char *
lock_type_str(short type)
{
    switch (type) {
        case F_RDLCK: return "F_RDLCK";
        case F_WRLCK: return "F_WRLCK";
        case F_UNLCK: return "F_UNLCK";
        default:      return "?";
    } /* switch */
} /* lock_type_str */

/*
 * F_GETLK assertions report what came back, not just that it was wrong.
 *
 * These cases are the ones that have shown up as rare CI-only failures, where
 * a single line of "FAIL: <what>" says nothing about which lock answered.  The
 * whole diagnosis turns on the returned descriptor: a stale WRITE fragment left
 * under a downgraded range reports F_WRLCK with l_start inside the downgrade,
 * whereas a range that was never carved at all reports the original geometry,
 * and a wrong-owner answer reports someone else's l_pid.  Printing rc, the
 * type, and the range costs nothing on the passing path and makes the next
 * occurrence self-explaining rather than another unreproducible line.
 */
static void
expect_getlk(
    int                 rc,
    const struct flock *got,
    short               want_type,
    const char         *what)
{
    if (rc == 0 && got->l_type == want_type) {
        return;
    }

    fprintf(stderr, "FAIL: %s\n", what);
    fprintf(stderr,
            "      F_GETLK rc=%d errno=%d; expected l_type=%s, got l_type=%s"
            " l_whence=%d l_start=%lld l_len=%lld l_pid=%lld\n",
            rc, rc == 0 ? 0 : errno,
            lock_type_str(want_type), lock_type_str(got->l_type),
            (int) got->l_whence, (long long) got->l_start,
            (long long) got->l_len, (long long) got->l_pid);
    fails++;
} /* expect_getlk */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    struct flock          fl;
    int                   fd, fd2, rc;
    uint64_t              owner_a = 0xA1ULL, owner_b = 0xB2ULL;

    posix_test_init(&env, argv, argc);

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* ---- an unlock wider than the lock still releases it --------------- */
    fd = chimera_posix_open("/test/lock_wide_unlock", O_RDWR | O_CREAT, 0644);
    expect(fd >= 0, "open lock_wide_unlock");

    chimera_posix_set_lock_owner(&owner_a);
    fl = lock_desc(F_WRLCK, 2, 1);
    expect(chimera_posix_fcntl(fd, F_SETLKW, &fl) == 0, "A locks [2,3)");

    fl = lock_desc(F_UNLCK, 2, 2);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0, "A unlocks [2,4)");

    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_RDLCK, 2, 4);
    rc = chimera_posix_fcntl(fd, F_GETLK, &fl);
    expect(rc == 0 && fl.l_type == F_UNLCK,
           "the wider unlock released the lock");

    fl = lock_desc(F_WRLCK, 2, 1);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0,
           "B can take the released range");
    /* Both simulated processes must close this file before fixture unmount.
     * Do A's cleanup after observing the unlock, so it cannot mask failure. */
    chimera_posix_set_lock_owner(&owner_a);
    fd2 = chimera_posix_dup(fd);
    expect(fd2 >= 0 && chimera_posix_close(fd2) == 0, "A closes wide-unlock file");
    chimera_posix_set_lock_owner(&owner_b);
    expect(chimera_posix_close(fd) == 0, "B closes wide-unlock file");

    /* ---- an owner never conflicts with itself -------------------------- */
    chimera_posix_set_lock_owner(&owner_a);
    fd = chimera_posix_open("/test/lock_same_owner", O_RDWR | O_CREAT, 0644);
    expect(fd >= 0, "open lock_same_owner");

    fl = lock_desc(F_WRLCK, 0, 4);
    expect(chimera_posix_fcntl(fd, F_SETLKW, &fl) == 0, "A locks [0,4)");

    fl = lock_desc(F_WRLCK, 2, 1);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0,
           "A re-locks [2,3) inside its own range");

    fd2 = chimera_posix_open("/test/lock_same_owner", O_RDWR, 0644);
    expect(fd2 >= 0, "second open of lock_same_owner");
    fl = lock_desc(F_WRLCK, 1, 2);
    expect(chimera_posix_fcntl(fd2, F_SETLK, &fl) == 0,
           "A locks [1,3) through a second description");

    /* ---- close of ANY descriptor drops the process's locks ------------- */
    /* fd2 holds no lock of its own that fd does not overlap; closing it must
     * still drop everything owner A holds on this file. */
    chimera_posix_close(fd2);

    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_WRLCK, 0, 4);
    rc = chimera_posix_fcntl(fd, F_GETLK, &fl);
    expect_getlk(rc, &fl, F_UNLCK,
                 "closing one descriptor dropped the process's locks on the file");

    fl = lock_desc(F_WRLCK, 0, 4);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0,
           "B can lock the whole range after A's close");

    chimera_posix_close(fd);

    /* ---- a re-lock replaces the owner's coverage (downgrade) ---------- */
    chimera_posix_set_lock_owner(&owner_a);
    fd = chimera_posix_open("/test/lock_downgrade", O_RDWR | O_CREAT, 0644);
    expect(fd >= 0, "open lock_downgrade");

    fl = lock_desc(F_WRLCK, 0, 4);
    expect(chimera_posix_fcntl(fd, F_SETLKW, &fl) == 0, "A write-locks [0,4)");

    fl = lock_desc(F_RDLCK, 2, 2);
    expect(chimera_posix_fcntl(fd, F_SETLKW, &fl) == 0,
           "A downgrades [2,4) to a read lock");

    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_RDLCK, 2, 1);
    rc = chimera_posix_fcntl(fd, F_GETLK, &fl);
    expect_getlk(rc, &fl, F_UNLCK,
                 "a read lock in the downgraded range is not blocked");

    /* The real symptom is a hang, so take it non-blocking: F_SETLKW here
     * would never return against a stale write fragment. */
    fl = lock_desc(F_RDLCK, 2, 1);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0,
           "B shares the downgraded range");

    /* Outside the downgraded range A's write lock must still stand. */
    fl = lock_desc(F_RDLCK, 0, 1);
    rc = chimera_posix_fcntl(fd, F_GETLK, &fl);
    expect_getlk(rc, &fl, F_WRLCK,
                 "A still holds the write lock outside the downgrade");

    chimera_posix_set_lock_owner(&owner_a);
    fd2 = chimera_posix_dup(fd);
    expect(fd2 >= 0 && chimera_posix_close(fd2) == 0, "A closes downgraded file");
    chimera_posix_set_lock_owner(&owner_b);
    expect(chimera_posix_close(fd) == 0, "B closes downgraded file");

    /* ---- closing a duplicate is also a process-wide release point ----- */
    chimera_posix_set_lock_owner(&owner_a);
    fd = chimera_posix_open("/test/lock_dup_close", O_RDWR | O_CREAT, 0644);
    expect(fd >= 0, "open duplicate release test");
    fd2 = chimera_posix_dup(fd);
    expect(fd2 >= 0, "duplicate lock descriptor");
    fl = lock_desc(F_WRLCK, 4, 8);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0, "A locks through original descriptor");
    expect(chimera_posix_close(fd2) == 0, "close duplicate");
    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_WRLCK, 4, 8);
    expect(chimera_posix_fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type == F_UNLCK,
           "closing duplicate released owner coverage");
    chimera_posix_close(fd);

    /* ---- signed normalization must never wrap a requested range -------- */
    chimera_posix_set_lock_owner(&owner_a);
    fd = chimera_posix_open("/test/lock_overflow", O_RDWR | O_CREAT, 0644);
    expect(fd >= 0, "open range overflow test");
    fl    = lock_desc(F_WRLCK, 0, (off_t) INT64_MIN);
    errno = 0;
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == -1 && errno == EINVAL,
           "INT64_MIN backward length is before zero");
    fl    = lock_desc(F_WRLCK, (off_t) INT64_MAX, 2);
    errno = 0;
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == -1 && errno == EOVERFLOW,
           "range last byte cannot overflow off_t");
    expect(chimera_posix_lseek(fd, 1, SEEK_SET) == 1, "set SEEK_CUR base");
    fl          = lock_desc(F_WRLCK, (off_t) INT64_MAX, 1);
    fl.l_whence = SEEK_CUR;
    errno       = 0;
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == -1 && errno == EOVERFLOW,
           "SEEK_CUR addition cannot overflow");
    fl          = lock_desc(F_WRLCK, -2, 1);
    fl.l_whence = SEEK_CUR;
    errno       = 0;
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == -1 && errno == EINVAL,
           "negative absolute start rejected");
    fl = lock_desc(F_WRLCK, 8, -4);
    expect(chimera_posix_fcntl(fd, F_SETLK, &fl) == 0, "valid backward range accepted");
    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_RDLCK, 4, 4);
    expect(chimera_posix_fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type == F_WRLCK &&
           fl.l_start == 4 && fl.l_len == 4, "backward range normalized to [4,8)");
    chimera_posix_set_lock_owner(&owner_a);
    chimera_posix_close(fd);

    if (fails) {
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
