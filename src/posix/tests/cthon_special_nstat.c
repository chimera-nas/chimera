// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stat a file n times
// Based on cthon/special/nstat.c from Connectathon
//
// Performance test for stat operation

#include "cthon_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    struct timeval        etim;
    float                 elapsed;
    int                   count;
    int                   i;
    struct stat           statb;
    char                  testfile[MAXPATHLEN];
    int                   fd;

    cthon_Myname = "cthon_special_nstat";

    posix_test_init(&env, argv, argc);

    optind = 1;
    count  = 1000; // default
    while ((opt = getopt(argc, argv, "hb:c:")) != -1) {
        switch (opt) {
            case 'b': break;
            case 'c':
                count = atoi(optarg);
                break;
            default: break;
        } /* switch */
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: stat performance test (%d iterations)\n", cthon_Myname, count);

    // Create a test file to stat
    snprintf(testfile, sizeof(testfile), "%s/stattest", cthon_getcwd());
    fd = chimera_posix_open(testfile, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        cthon_error("can't create test file: %s", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    cthon_starttime();
    for (i = 0; i < count; i++) {
        if (chimera_posix_stat(testfile, &statb) < 0) {
            cthon_error("pass %d: can't stat %s: %s", i, testfile, strerror(errno));
            posix_test_fail(&env);
        }
    }
    cthon_endtime(&etim);

    elapsed = (float) etim.tv_sec + (float) etim.tv_usec / 1000000.0f;
    if (elapsed == 0.0f) {
        fprintf(stdout, "\t%d calls 0.0 seconds\n", count);
    } else {
        fprintf(stdout, "\t%d calls %.2f seconds %.2f calls/sec %.2f msec/call\n",
                count, elapsed, (float) count / elapsed,
                1000.0f * elapsed / (float) count);
    }

    // Clean up
    chimera_posix_unlink(testfile);

    fprintf(stdout, "\tnstat test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
