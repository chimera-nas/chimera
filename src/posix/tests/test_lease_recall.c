// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * CAP_LEASE asynchronous-recall test.
 *
 * CHIMERA_MEMFS_LEASE_RECALL=200 makes the memfs arbiter recall every
 * aggregate grant ~200ms after it lands, from its own timer sweep — the
 * async backend->core upcall.  The claim core must marshal to the service
 * thread, drain the local holders (the implicit I/O claim here; the same
 * path recalls NFSv4 delegations and SMB oplocks through their break
 * callbacks), acknowledge with a release, and transparently re-acquire on
 * the next I/O.  The test writes across several recall cycles: a hang or a
 * spurious failure anywhere in the recall/drain/re-acquire loop fails it.
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
    int                   i;

    setenv("CHIMERA_MEMFS_LEASE_RECALL", "200", 1);

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/lease_recall_file",
                            O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Ride several recall cycles: each iteration re-acquires the cover
     * after the previous recall drained and released it. */
    for (i = 0; i < 6; i++) {
        n = chimera_posix_write(fd, "chunk", 5);
        if (n != 5) {
            fprintf(stderr, "write %d failed under recall churn: %s\n",
                    i, strerror(errno));
            posix_test_fail(&env);
        }
        usleep(300000); /* outlive the 200ms recall knob each cycle */
    }

    chimera_posix_close(fd);

    posix_test_success(&env);
    return 0;
} /* main */
