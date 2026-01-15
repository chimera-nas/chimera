// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test statfs
// Based on cthon/basic/test9.c from Connectathon 2004
//
// Uses the following important system calls:
//   statfs() / statvfs()

#include "cthon_common.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>

static int Tflag = 0;
static int Fflag = 0;
static int Nflag = 0;

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   count = 1500;
    int                   ct;
    struct statvfs        svbuf = { 0 };
    struct timeval        time  = { 0, 0 };
    int                   opt;

    cthon_Myname = "cthon_basic_9";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "htfnb:")) != -1) {
        switch (opt) {
            case 't': Tflag++; break;
            case 'f': Fflag++; break;
            case 'n': Nflag++; break;
            case 'b': break;
            default: break;
        } /* switch */
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) {
        count = cthon_getparm(*argv++, 1, "count"); argc--;
    }

    if (Fflag) {
        Tflag = 0; count = 1;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!Nflag) {
        cthon_testdir(NULL);
    } else {
        cthon_mtestdir(NULL);
    }

    fprintf(stdout, "%s: statfs\n", cthon_Myname);

    if (Tflag) {
        cthon_starttime();
    }

    for (ct = 0; ct < count; ct++) {
        if (chimera_posix_statvfs(cthon_getcwd(), &svbuf) < 0) {
            cthon_error("can't statfs %s", cthon_getcwd());
            posix_test_fail(&env);
        }
    }

    if (Tflag) {
        cthon_endtime(&time);
    }

    fprintf(stdout, "\t%d statvfs calls", count);
    if (Tflag) {
        fprintf(stdout, " in %ld.%-2ld seconds",
                (long) time.tv_sec, (long) time.tv_usec / 10000);
    }
    fprintf(stdout, "\n");

    // Print some filesystem info
    fprintf(stdout, "\tFilesystem info:\n");
    fprintf(stdout, "\t  block size: %lu\n", (unsigned long) svbuf.f_bsize);
    fprintf(stdout, "\t  total blocks: %lu\n", (unsigned long) svbuf.f_blocks);
    fprintf(stdout, "\t  free blocks: %lu\n", (unsigned long) svbuf.f_bfree);
    fprintf(stdout, "\t  available blocks: %lu\n", (unsigned long) svbuf.f_bavail);
    fprintf(stdout, "\t  total inodes: %lu\n", (unsigned long) svbuf.f_files);
    fprintf(stdout, "\t  free inodes: %lu\n", (unsigned long) svbuf.f_ffree);

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
