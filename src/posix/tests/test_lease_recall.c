// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * CAP_LEASE backend-projection: RECALL.
 *
 * memfs is configured (CHIMERA_MEMFS_LEASE_RECALL) to act as an authoritative
 * backend that, shortly after granting a file's lease to this node, recalls it
 * (as another node in a distributed deployment would).  This drives the full
 * Phase-3 path: chimera_vfs_lease_backend_recall -> RECALLING -> break/drain the
 * implicit lease -> OP_LEASE_RELEASE -> NONE, after which the next I/O
 * re-acquires a fresh lease.
 *
 * The test writes and reads a file across that recall boundary and verifies the
 * data is intact -- a hang in the drain/release or a lost re-acquire would
 * either wedge the I/O (watchdog/timeout) or corrupt the contents.
 */

#include "posix_test_common.h"

#define CHUNK   4096
#define NCHUNKS 32

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    int                   i;
    char                  wbuf[CHUNK];
    char                  rbuf[CHUNK];

    /* memfs recalls each file's lease once, shortly after granting it. */
    setenv("CHIMERA_MEMFS_LEASE_RECALL", "1", 1);

    posix_test_init(&env, argv, argc);

    if (strcmp(env.backend, "memfs") != 0) {
        fprintf(stderr, "lease_recall test is memfs-only, skipping for %s\n",
                env.backend);
        posix_test_success(&env);
        return 0;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "mount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/lease_recall_file", O_CREAT | O_RDWR | O_TRUNC,
                            0644);
    if (fd < 0) {
        fprintf(stderr, "create failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Write NCHUNKS chunks; the lease is recalled after the first one, so the
     * remaining writes cross the recall->release->re-acquire boundary. */
    for (i = 0; i < NCHUNKS; i++) {
        ssize_t n;

        memset(wbuf, (char) ('A' + (i % 26)), sizeof(wbuf));
        n = chimera_posix_write(fd, wbuf, sizeof(wbuf));
        if (n != (ssize_t) sizeof(wbuf)) {
            fprintf(stderr, "FAIL: write chunk %d short/failed (%zd, errno=%d %s)\n",
                    i, n, errno, strerror(errno));
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
    }
    fprintf(stderr, "PASS: %d chunks written across the recall boundary\n", NCHUNKS);
    chimera_posix_close(fd);

    /* Read it all back and verify -- reads also re-acquire (an R lease) after
     * the recall. */
    fd = chimera_posix_open("/test/lease_recall_file", O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "open for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    for (i = 0; i < NCHUNKS; i++) {
        ssize_t n = chimera_posix_read(fd, rbuf, sizeof(rbuf));

        if (n != (ssize_t) sizeof(rbuf)) {
            fprintf(stderr, "FAIL: read chunk %d short/failed (%zd, errno=%d %s)\n",
                    i, n, errno, strerror(errno));
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
        memset(wbuf, (char) ('A' + (i % 26)), sizeof(wbuf));
        if (memcmp(rbuf, wbuf, sizeof(rbuf)) != 0) {
            fprintf(stderr, "FAIL: chunk %d content mismatch after recall\n", i);
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
    }
    fprintf(stderr, "PASS: all %d chunks read back intact across the recall\n", NCHUNKS);
    chimera_posix_close(fd);

    fprintf(stderr, "lease_recall test passed\n");
    posix_test_success(&env);
    return 0;
} /* main */
