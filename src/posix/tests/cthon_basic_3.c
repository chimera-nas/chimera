// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test lookups across mount point (stat operations)
// Based on cthon/basic/test3.c from Connectathon 2004
//
// Uses the following important system calls against the server:
//   stat()

#include "cthon_common.h"

static int Tflag = 0;   // print timing
static int Fflag = 0;   // test function only; set count to 1, negate -t
static int Nflag = 0;   // Suppress directory operations

static void
usage(void)
{
    fprintf(stdout, "usage: %s [-htfn] [count]\n", cthon_Myname);
    fprintf(stdout, "  Flags:  h    Help - print this usage info\n");
    fprintf(stdout, "          t    Print execution time statistics\n");
    fprintf(stdout, "          f    Test function only (negate -t)\n");
    fprintf(stdout, "          n    Suppress test directory create operations\n");
} /* usage */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   count = 250;
    int                   ct;
    struct stat           statb;
    struct timeval        time;
    int                   opt;
    const char           *path;

    cthon_Myname = "cthon_basic_3";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "htfnb:")) != -1) {
        switch (opt) {
            case 'h':
                usage();
                exit(1);
                break;
            case 't':
                Tflag++;
                break;
            case 'f':
                Fflag++;
                break;
            case 'n':
                Nflag++;
                break;
            case 'b':
                break;
            default:
                break;
        } /* switch */
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) {
        count = cthon_getparm(*argv, 1, "count");
        argv++;
        argc--;
    }

    if (Fflag) {
        Tflag = 0;
        count = 1;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!Nflag) {
        cthon_testdir(NULL);
    } else {
        cthon_mtestdir(NULL);
    }

    fprintf(stdout, "%s: lookups across mount point\n", cthon_Myname);

    if (Tflag) {
        cthon_starttime();
    }

    path = cthon_getcwd();

    for (ct = 0; ct < count; ct++) {
        if (chimera_posix_stat(path, &statb) < 0) {
            cthon_error("can't stat %s", path);
            posix_test_fail(&env);
        }
    }

    if (Tflag) {
        cthon_endtime(&time);
    }

    fprintf(stdout, "\t%d stats on %s", count, path);
    if (Tflag) {
        fprintf(stdout, " in %ld.%-2ld seconds",
                (long) time.tv_sec, (long) time.tv_usec / 10000);
    }
    fprintf(stdout, "\n");

    cthon_complete();

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
