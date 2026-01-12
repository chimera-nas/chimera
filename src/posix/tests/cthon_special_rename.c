// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test rename a file n times
// Based on cthon/special/rename.c from Connectathon 2004

#include "cthon_common.h"

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str1[MAXPATHLEN];
    char                  str2[MAXPATHLEN];
    int                   fd;
    int                   count = 100;
    int                   i;

    cthon_Myname = "cthon_special_rename";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        }
    }

    argc -= optind;
    argv += optind;

    // Optional arg: count
    if (argc > 0) { count = atoi(argv[0]); argc--; argv++; }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: rename test (%d iterations)\n", cthon_Myname, count);

    snprintf(str1, sizeof(str1), "%s/rename1", cthon_getcwd());
    snprintf(str2, sizeof(str2), "%s/rename2", cthon_getcwd());

    // Create the initial file
    fd = chimera_posix_open(str1, O_CREAT, 0666);
    if (fd < 0) {
        cthon_error("can't create %s", str1);
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    for (i = 0; i < count; i++) {
        if (chimera_posix_rename(str1, str2) < 0) {
            cthon_error("rename %s to %s failed at iteration %d", str1, str2, i);
            posix_test_fail(&env);
        }
        if (chimera_posix_rename(str2, str1) < 0) {
            cthon_error("rename %s to %s failed at iteration %d", str2, str1, i);
            posix_test_fail(&env);
        }
    }

    // Cleanup
    chimera_posix_unlink(str1);
    chimera_posix_unlink(str2);

    fprintf(stdout, "\t%d rename pairs completed successfully\n", count);

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
