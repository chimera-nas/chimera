// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stat all files in a directory tree
// Based on cthon/special/stat.c from Connectathon
//
// Recursively walks a directory tree, statting all entries

#include "cthon_common.h"

static int stats = 0;

static void statit(
    const char *name);

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
    char                  dirname[MAXPATHLEN];

    cthon_Myname = "cthon_special_stat";

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

    fprintf(stdout, "%s: recursive stat test\n", cthon_Myname);

    // Create a small directory tree to stat
    int totfiles = 0, totdirs = 0;
    cthon_dirtree(2, 3, 2, CTHON_FNAME, CTHON_DNAME, &totfiles, &totdirs);
    fprintf(stdout, "\tcreated directory tree: %d files, %d dirs\n", totfiles, totdirs);

    // Stat the directory tree
    snprintf(dirname, sizeof(dirname), "%s", cthon_getcwd());
    cthon_starttime();
    statit(dirname);
    cthon_endtime(&etim);

    elapsed = (float) etim.tv_sec + (float) etim.tv_usec / 1000000.0f;
    fprintf(stdout, "\t%d calls in %f seconds (%f calls/sec)\n",
            stats, elapsed, (float) stats / elapsed);

    // Clean up
    totfiles = 0;
    totdirs  = 0;
    cthon_rmdirtree(2, 3, 2, CTHON_FNAME, CTHON_DNAME, &totfiles, &totdirs, 0);

    fprintf(stdout, "\trecursive stat test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */

static void
statit(const char *name)
{
    struct stat    statb;
    struct dirent *di;
    CHIMERA_DIR   *dirp;
    char           fullpath[MAXPATHLEN];

    if (chimera_posix_lstat(name, &statb) < 0) {
        cthon_error("can't stat %s: %s", name, strerror(errno));
        return;
    }
    stats++;

    if (!S_ISDIR(statb.st_mode)) {
        return;
    }

    dirp = chimera_posix_opendir(name);
    if (dirp == NULL) {
        cthon_error("can't opendir %s: %s", name, strerror(errno));
        return;
    }

    while ((di = chimera_posix_readdir(dirp)) != NULL) {
        if (strcmp(di->d_name, ".") == 0 || strcmp(di->d_name, "..") == 0) {
            continue;
        }

        snprintf(fullpath, sizeof(fullpath), "%s/%s", name, di->d_name);

        if (chimera_posix_lstat(fullpath, &statb) < 0) {
            cthon_error("can't stat %s: %s", fullpath, strerror(errno));
            continue;
        }
        stats++;

        if (S_ISDIR(statb.st_mode)) {
            statit(fullpath);
        }
    }

    chimera_posix_closedir(dirp);
} /* statit */
