// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test for lost reply on non-idempotent requests
// Based on cthon/special/dupreq.c from Connectathon 2004
//
// Repeatedly creates, links, and unlinks files to stress test
// non-idempotent request handling

#include "cthon_common.h"

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    int                   count = 100;
    int                   i;
    int                   fd;
    int                   cfail, lfail, u1fail, u2fail;
    char                  name1[MAXPATHLEN];
    char                  name2[MAXPATHLEN];

    cthon_Myname = "cthon_special_dupreq";

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

    fprintf(stdout, "%s: duplicate request test (%d iterations)\n",
            cthon_Myname, count);

    snprintf(name1, sizeof(name1), "%s/dupreq1", cthon_getcwd());
    snprintf(name2, sizeof(name2), "%s/dupreq2", cthon_getcwd());

    cfail = lfail = u1fail = u2fail = 0;

    for (i = count; i > 0; i--) {
        // Create file
        fd = chimera_posix_open(name1, O_CREAT | O_WRONLY, 0666);
        if (fd < 0) {
            cfail++;
            fprintf(stderr, "\tcreate %s failed: %s\n", name1, strerror(errno));
            continue;
        }
        chimera_posix_close(fd);

        // Link to second name
        if (chimera_posix_link(name1, name2) < 0) {
            lfail++;
            fprintf(stderr, "\tlink %s %s failed: %s\n", name1, name2, strerror(errno));
        }

        // Unlink second name
        if (chimera_posix_unlink(name2) < 0) {
            u1fail++;
            fprintf(stderr, "\tunlink %s failed: %s\n", name2, strerror(errno));
        }

        // Unlink first name
        if (chimera_posix_unlink(name1) < 0) {
            u2fail++;
            fprintf(stderr, "\tunlink %s failed: %s\n", name1, strerror(errno));
        }
    }

    fprintf(stdout, "\t%d tries\n", count);
    if (cfail) {
        fprintf(stdout, "\t%d bad create\n", cfail);
    }
    if (lfail) {
        fprintf(stdout, "\t%d bad link\n", lfail);
    }
    if (u1fail) {
        fprintf(stdout, "\t%d bad unlink 1\n", u1fail);
    }
    if (u2fail) {
        fprintf(stdout, "\t%d bad unlink 2\n", u2fail);
    }

    if (cfail || lfail || u1fail || u2fail) {
        posix_test_fail(&env);
    }

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
