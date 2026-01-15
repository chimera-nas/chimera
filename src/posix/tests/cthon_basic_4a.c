// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test getattr/lookup (stat only, no chmod)
// Based on cthon/basic/test4a.c from Connectathon 2004
//
// Uses the following important system calls against the server:
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
} /* usage */

int
main(
    int    argc,
    char **argv)
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

    cthon_Myname = "cthon_basic_4a";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "htfnb:")) != -1) {
        switch (opt) {
            case 'h':
                usage();
                exit(1);
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
        files = cthon_getparm(*argv++, 1, "files");
        argc--;
    }
    if (argc > 0) {
        count = cthon_getparm(*argv++, 1, "count");
        argc--;
    }
    if (argc > 0) {
        fname = *argv++;
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

    fprintf(stdout, "%s: getattr and lookup\n", cthon_Myname);

    // Create test files
    for (fi = 0; fi < files; fi++) {
        snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
        fd = chimera_posix_open(str, O_CREAT | O_WRONLY | O_TRUNC, CTHON_CHMOD_RW);
        if (fd < 0) {
            cthon_error("can't create %s", str);
            posix_test_fail(&env);
        }
        chimera_posix_close(fd);
    }

    if (Tflag) {
        cthon_starttime();
    }

    for (ct = 0; ct < count; ct++) {
        for (fi = 0; fi < files; fi++) {
            snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
            if (chimera_posix_stat(str, &statb) < 0) {
                cthon_error("can't stat %s", str);
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

    fprintf(stdout, "\t%d stats on %d files", files * count, files);
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
