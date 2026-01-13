// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test file and directory creation
// Based on cthon/basic/test1.c from Connectathon 2004
//
// Uses the following important system calls against the server:
//   mkdir()
//   creat()
//   close()

#include "cthon_common.h"

static int Tflag = 0;   // print timing
static int Fflag = 0;   // test function only; set count to 1, negate -t
static int Nflag = 0;   // Suppress directory operations

static void
usage(void)
{
    fprintf(stdout, "usage: %s [-htfn] [levels files dirs fname dname]\n", cthon_Myname);
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
    int                   levels   = CTHON_DLEVS;
    int                   files    = CTHON_DFILS;
    int                   dirs     = CTHON_DDIRS;
    char                 *fname    = CTHON_FNAME;
    char                 *dname    = CTHON_DNAME;
    int                   totfiles = 0;
    int                   totdirs  = 0;
    struct timeval        time;
    int                   opt;

    cthon_Myname = "cthon_basic_1";

    posix_test_init(&env, argv, argc);

    // Reset optind for our argument parsing
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
                // Backend option handled by posix_test_init
                break;
            default:
                break;
        } /* switch */
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) {
        levels = cthon_getparm(*argv, 1, "levels");
        argv++;
        argc--;
    }
    if (argc > 0) {
        files = cthon_getparm(*argv, 0, "files");
        argv++;
        argc--;
    }
    if (argc > 0) {
        dirs = cthon_getparm(*argv, 0, "dirs");
        argv++;
        argc--;
    }
    if (argc > 0) {
        fname = *argv;
        argv++;
        argc--;
    }
    if (argc > 0) {
        dname = *argv;
        argv++;
        argc--;
    }

    if (Fflag) {
        Tflag  = 0;
        levels = 2;
        files  = 2;
        dirs   = 2;
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

    fprintf(stdout, "%s: File and directory creation test\n", cthon_Myname);

    if (Tflag) {
        cthon_starttime();
    }

    cthon_dirtree(levels, files, dirs, fname, dname, &totfiles, &totdirs);

    if (Tflag) {
        cthon_endtime(&time);
    }

    fprintf(stdout, "\tcreated %d files %d directories %d levels deep",
            totfiles, totdirs, levels);
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
