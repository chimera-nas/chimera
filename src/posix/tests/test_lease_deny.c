// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * CAP_LEASE backend-projection: authoritative DENY.
 *
 * memfs is configured (lease_deny="w") to act as an authoritative backend that
 * refuses to grant a WRITE lease to this node -- as if another node in a
 * distributed deployment held a conflicting write lease the backend would not
 * recall.  The VFS core projects its implicit I/O lease down into that backend
 * (vfs_state.c backend_lease_*), so a write must be DENIED end-to-end while a
 * read (which only needs an R lease, not denied) still succeeds.
 *
 * This exercises the full deny path: io_try local-grant -> backend escalate ->
 * OP_LEASE_ACQUIRE returns granted=0 -> bl_pump marks io_backend_denied ->
 * io_try fails the write with EACCES.
 */

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    ssize_t               n;
    char                  rbuf[16];
    const char           *data = "denied?";

    /* Make memfs refuse WRITE leases (simulating a conflicting remote holder).
     * Use the env knob rather than a per-module config so the built-in memfs is
     * not re-registered. */
    setenv("CHIMERA_MEMFS_LEASE_DENY", "w", 1);

    posix_test_init(&env, argv, argc);

    /* CAP_LEASE-only feature: memfs advertises it; other backends do not. */
    if (strcmp(env.backend, "memfs") != 0) {
        fprintf(stderr, "lease_deny test is memfs-only, skipping for %s\n",
                env.backend);
        posix_test_success(&env);
        return 0;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "mount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Create succeeds (open takes no implicit I/O lease). */
    fd = chimera_posix_open("/test/lease_deny_file", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "create failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* The first write needs a W lease, which the backend denies -> the write
     * must fail rather than silently succeeding. */
    n = chimera_posix_write(fd, data, strlen(data));
    if (n >= 0) {
        fprintf(stderr, "FAIL: write succeeded (%zd) but backend denied the "
                "write lease\n", n);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stderr, "PASS: write denied as expected (errno=%d %s)\n",
            errno, strerror(errno));
    chimera_posix_close(fd);

    /* A read needs only an R lease, which is NOT denied: open + read of the
     * (empty) file must succeed (returns 0 at EOF). */
    fd = chimera_posix_open("/test/lease_deny_file", O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "open for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    n = chimera_posix_read(fd, rbuf, sizeof(rbuf));
    if (n < 0) {
        fprintf(stderr, "FAIL: read denied (errno=%d %s) but only writes "
                "should be denied\n", errno, strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stderr, "PASS: read granted as expected (%zd bytes)\n", n);
    chimera_posix_close(fd);

    fprintf(stderr, "lease_deny test passed\n");
    posix_test_success(&env);
    return 0;
} /* main */
