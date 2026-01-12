// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test statvfs for file count
// Based on cthon/special/fstat.c from Connectathon 2004

#include "cthon_common.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    struct statvfs        fs;
    int                   rv;

    cthon_Myname = "cthon_special_fstat";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        } /* switch */
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: statvfs file count test\n", cthon_Myname);

    fs.f_files = 0;
    fs.f_ffree = 0;

    rv = chimera_posix_statvfs(cthon_getcwd(), &fs);
    if (rv < 0) {
        cthon_error("statvfs %s failed", cthon_getcwd());
        posix_test_fail(&env);
    }

    fprintf(stdout, "\ttotal inodes: %lu  free inodes: %lu\n",
            (unsigned long) fs.f_files, (unsigned long) fs.f_ffree);

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
