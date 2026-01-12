// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test setattr/getattr/lookup (chmod and stat)
// Based on cthon/basic/test4.c from Connectathon 2004
//
// Uses the following important system calls against the server:
//   chmod()
//   stat()
//   creat()

#include "cthon_common.h"

static int Tflag = 0;
static int Fflag = 0;
static int Nflag = 0;

static void
usage(void)
{
    fprintf(stdout, "usage: %s [-htfn] [files count fname]\n", cthon_Myname);
    fprintf(stdout, "  Flags:  h    Help - print this usage info\n");
    fprintf(stdout, "          t    Print execution time statistics\n");
    fprintf(stdout, "          f    Test function only (negate -t)\n");
    fprintf(stdout, "          n    Suppress test directory create operations\n");
}

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   files = CTHON_DCOUNT;
    int                   fi;
    int                   count = 50;
    int                   ct;
    char                 *fname = CTHON_FNAME;
    int                   fd;
    struct stat           statb;
    struct timeval        time;
    int                   opt;
    char                  str[MAXPATHLEN];

    cthon_Myname = "cthon_basic_4";

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
        }
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) {
        files = cthon_getparm(*argv, 1, "files");
        argv++;
        argc--;
    }
    if (argc > 0) {
        count = cthon_getparm(*argv, 1, "count");
        argv++;
        argc--;
    }
    if (argc > 0) {
        fname = *argv;
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

    fprintf(stdout, "%s: setattr, getattr, and lookup\n", cthon_Myname);

    // Create test files
    for (fi = 0; fi < files; fi++) {
        snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
        fd = chimera_posix_open(str, O_CREAT | O_WRONLY | O_TRUNC, CTHON_CHMOD_RW);
        if (fd < 0) {
            cthon_error("can't create %s", str);
            posix_test_fail(&env);
        }
        if (chimera_posix_close(fd) < 0) {
            cthon_error("can't close %s", str);
            posix_test_fail(&env);
        }
    }

    if (Tflag) {
        cthon_starttime();
    }

    for (ct = 0; ct < count; ct++) {
        for (fi = 0; fi < files; fi++) {
            snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);

            // Change mode to read-only
            if (chimera_posix_chmod(str, CTHON_CHMOD_NONE) < 0) {
                cthon_error("can't chmod %s", str);
                posix_test_fail(&env);
            }

            if (chimera_posix_stat(str, &statb) < 0) {
                cthon_error("can't stat %s", str);
                posix_test_fail(&env);
            }

            if ((statb.st_mode & CTHON_CHMOD_MASK) != CTHON_CHMOD_NONE) {
                cthon_error("%s has mode %o, expected %o",
                           str, statb.st_mode & CTHON_CHMOD_MASK, CTHON_CHMOD_NONE);
                posix_test_fail(&env);
            }

            // Change mode back to read-write
            if (chimera_posix_chmod(str, CTHON_CHMOD_RW) < 0) {
                cthon_error("can't chmod %s", str);
                posix_test_fail(&env);
            }

            if (chimera_posix_stat(str, &statb) < 0) {
                cthon_error("can't stat %s", str);
                posix_test_fail(&env);
            }

            if ((statb.st_mode & CTHON_CHMOD_MASK) != CTHON_CHMOD_RW) {
                cthon_error("%s has mode %o, expected %o",
                           str, statb.st_mode & CTHON_CHMOD_MASK, CTHON_CHMOD_RW);
                posix_test_fail(&env);
            }
        }
    }

    if (Tflag) {
        cthon_endtime(&time);
    }

    // Cleanup
    for (fi = 0; fi < files; fi++) {
        snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
        chimera_posix_unlink(str);
    }

    fprintf(stdout, "\t%d chmods and stats on %d files",
            files * count * 4, files);
    if (Tflag) {
        fprintf(stdout, " in %ld.%-2ld seconds",
                (long)time.tv_sec, (long)time.tv_usec / 10000);
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
}
