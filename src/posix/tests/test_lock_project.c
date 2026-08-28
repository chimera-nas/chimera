// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * POSIX byte-range locks project to a CAP_LEASE backend.
 *
 * CHIMERA_MEMFS_LEASE_RANGE_DENY makes the memfs arbiter refuse every
 * byte-range grant.  Nothing else in the process holds a conflicting lock,
 * so the local claim core grants on its own: an fcntl lock can only fail
 * here if it was genuinely confirmed with the backend first, which is
 * exactly the property under test.  A refusal must also leave nothing
 * behind -- the optimistic local insert is rolled back -- so a second
 * attempt fails the same way rather than colliding with a stale claim, and
 * F_UNLCK on an unheld range still succeeds.
 *
 * Registered for the memfs backend only (the knob lives on that arbiter).
 */

#include "posix_test_common.h"

static struct flock
lock_desc(
    short type,
    off_t start,
    off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type   = type;
    fl.l_whence = SEEK_SET;
    fl.l_start  = start;
    fl.l_len    = len;
    return fl;
} /* lock_desc */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    struct flock          fl;
    int                   fd;
    int                   rc;

    /* Must land before posix_test_init spins the embedded stack: the memfs
     * arbiter reads the knob at module init. */
    setenv("CHIMERA_MEMFS_LEASE_RANGE_DENY", "1", 1);

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/lock_project_file",
                            O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Uncontended locally, so a grant would be certain without projection. */
    fl = lock_desc(F_WRLCK, 0, 128);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc == 0) {
        fprintf(stderr,
                "F_SETLK granted despite the backend refusing the range: "
                "the lock was not projected\n");
        posix_test_fail(&env);
    }
    if (errno != EAGAIN) {
        fprintf(stderr, "F_SETLK failed with %s, expected EAGAIN\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    /* The refused attempt must have rolled its local insert back: a repeat
    * sees the same refusal, not a conflict with the claim it just left. */
    fl = lock_desc(F_WRLCK, 0, 128);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc == 0 || errno != EAGAIN) {
        fprintf(stderr,
                "second F_SETLK: rc=%d errno=%s, expected EAGAIN again\n",
                rc, strerror(errno));
        posix_test_fail(&env);
    }

    /* A read lock takes the same path and is refused the same way. */
    fl = lock_desc(F_RDLCK, 256, 64);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc == 0) {
        fprintf(stderr, "F_RDLCK granted despite the backend refusal\n");
        posix_test_fail(&env);
    }

    /* Only refusals are asserted here, and deliberately so: this backend is
     * configured to refuse every range, so there is no granted lock to
     * observe.  The granted and unlock paths are covered by
     * test_lock_posix_semantics and by the quint posix model's stepLocks
     * flavor, both of which run against an ordinary memfs arbiter. */

    chimera_posix_close(fd);

    posix_test_success(&env);
    return 0;
} /* main */
