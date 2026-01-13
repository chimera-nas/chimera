// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Create files and stat them repeatedly
// Based on cthon/special/stat2.c from Connectathon
//
// Creates files in a subdirectory and stats them multiple times

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
    int                   files, count;
    int                   pass, filenum;
    char                  dirname[MAXPATHLEN];
    char                  name[MAXPATHLEN];
    struct stat           statb;
    int                   stats = 0;
    int                   fd;

    cthon_Myname = "cthon_special_stat2";

    posix_test_init(&env, argv, argc);

    optind = 1;
    files  = 10;
    count  = 100;
    while ((opt = getopt(argc, argv, "hb:f:c:")) != -1) {
        switch (opt) {
            case 'b': break;
            case 'f':
                files = atoi(optarg);
                break;
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

    fprintf(stdout, "%s: stat2 test (%d files, %d passes)\n", cthon_Myname, files, count);

    // Create a subdirectory for the test
    snprintf(dirname, sizeof(dirname), "%s/stat2dir", cthon_getcwd());
    if (chimera_posix_mkdir(dirname, 0777) < 0 && errno != EEXIST) {
        cthon_error("can't create %s: %s", dirname, strerror(errno));
        posix_test_fail(&env);
    }

    // Create the test files
    for (filenum = 0; filenum < files; filenum++) {
        snprintf(name, sizeof(name), "%s/%d", dirname, filenum);
        fd = chimera_posix_open(name, O_CREAT | O_WRONLY, 0666);
        if (fd < 0) {
            cthon_error("can't create %s: %s", name, strerror(errno));
            posix_test_fail(&env);
        }
        chimera_posix_close(fd);
    }

    fprintf(stdout, "\tcreated %d files\n", files);

    // Stat all files repeatedly
    cthon_starttime();
    for (pass = 0; pass < count; pass++) {
        for (filenum = 0; filenum < files; filenum++) {
            snprintf(name, sizeof(name), "%s/%d", dirname, filenum);
            if (chimera_posix_stat(name, &statb) < 0) {
                cthon_error("pass %d: stat of %s failed: %s", pass, name, strerror(errno));
                posix_test_fail(&env);
            }
            stats++;
        }
    }
    cthon_endtime(&etim);

    elapsed = (float) etim.tv_sec + (float) etim.tv_usec / 1000000.0f;
    fprintf(stdout, "\t%d calls in %f seconds (%f calls/sec)\n",
            stats, elapsed, (float) stats / elapsed);

    // Clean up
    for (filenum = 0; filenum < files; filenum++) {
        snprintf(name, sizeof(name), "%s/%d", dirname, filenum);
        chimera_posix_unlink(name);
    }
    chimera_posix_rmdir(dirname);

    fprintf(stdout, "\tstat2 test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
