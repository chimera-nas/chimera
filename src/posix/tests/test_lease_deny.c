// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * CAP_LEASE authoritative-deny test.
 *
 * CHIMERA_MEMFS_LEASE_DENY=w makes the memfs arbiter mask W out of every
 * aggregate grant, so the claim core's implicit-I/O gate must fail writes
 * with EACCES (the backend is the authority) while reads — whose R bit is
 * still granted — proceed normally.  Exercises the whole projection chain
 * end to end: reeval -> service-thread aggregate acquire -> partial grant
 * -> parked-I/O resume -> refused-bit EACCES.  Registered for the memfs
 * backend only (the knob only exists on the memfs arbiter).
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
    char                  buf[16];

    /* Must land before posix_test_init spins the embedded stack: the memfs
     * arbiter reads the knob at module init. */
    setenv("CHIMERA_MEMFS_LEASE_DENY", "w", 1);

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/lease_deny_file",
                            O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Writes must be refused by the backend authority. */
    n = chimera_posix_write(fd, "hello", 5);
    if (n >= 0) {
        fprintf(stderr,
                "write succeeded (%zd) despite backend lease deny\n", n);
        posix_test_fail(&env);
    }

    /* Reads still work: R was granted. */
    n = chimera_posix_read(fd, buf, sizeof(buf));
    if (n < 0) {
        fprintf(stderr, "read failed under lease deny: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    posix_test_success(&env);
    return 0;
} /* main */
