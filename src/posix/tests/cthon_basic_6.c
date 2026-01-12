// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test readdir
// Based on cthon/basic/test6.c from Connectathon 2004
//
// Uses the following important system calls:
//   opendir()
//   readdir()
//   rewinddir()
//   closedir()
//   creat()
//   unlink()

#include "cthon_common.h"

#define NFILES 200

static int Tflag = 0;
static int Fflag = 0;
static int Nflag = 0;
static int Iflag = 0;   // Ignore non-test files

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   count = CTHON_DCOUNT;
    int                   ct;
    int                   files = NFILES;
    int                   fi;
    char                 *fname = CTHON_FNAME;
    CHIMERA_DIR          *dir;
    struct dirent        *dp;
    struct timeval        time;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    int                   found;
    int                   entries;

    cthon_Myname = "cthon_basic_6";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "htfnib:")) != -1) {
        switch (opt) {
            case 't': Tflag++; break;
            case 'f': Fflag++; break;
            case 'n': Nflag++; break;
            case 'i': Iflag++; break;
            case 'b': break;
            default: break;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) { files = cthon_getparm(*argv++, 0, "files"); argc--; }
    if (argc > 0) { count = cthon_getparm(*argv++, 0, "count"); argc--; }
    if (argc > 0) { fname = *argv++; argc--; }

    if (Fflag) { Tflag = 0; count = 1; }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!Nflag) cthon_testdir(NULL);
    else cthon_mtestdir(NULL);

    fprintf(stdout, "%s: readdir\n", cthon_Myname);

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

    if (Tflag) cthon_starttime();

    for (ct = 0; ct < count; ct++) {
        dir = chimera_posix_opendir(cthon_getcwd());
        if (dir == NULL) {
            cthon_error("can't opendir %s", cthon_getcwd());
            posix_test_fail(&env);
        }

        entries = 0;
        while ((dp = chimera_posix_readdir(dir)) != NULL) {
            entries++;
            // Verify it's one of our files or . or ..
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
                continue;
            }
            // Check if it's one of our test files
            if (strncmp(dp->d_name, fname, strlen(fname)) == 0) {
                continue;
            }
            if (!Iflag) {
                cthon_error("unexpected file in directory: %s", dp->d_name);
            }
        }

        // Test rewinddir
        chimera_posix_rewinddir(dir);
        found = 0;
        while ((dp = chimera_posix_readdir(dir)) != NULL) {
            found++;
        }
        if (found != entries) {
            cthon_error("rewinddir: found %d entries first time, %d second time",
                       entries, found);
            posix_test_fail(&env);
        }

        chimera_posix_closedir(dir);

        // Unlink one file per iteration to test directory modifications
        if (ct < files) {
            snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, ct);
            chimera_posix_unlink(str);
        }
    }

    if (Tflag) cthon_endtime(&time);

    // Cleanup remaining files
    for (fi = count; fi < files; fi++) {
        snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
        chimera_posix_unlink(str);
    }

    fprintf(stdout, "\t%d readdirs on %d files", count * 2, files);
    if (Tflag) {
        fprintf(stdout, " in %ld.%-2ld seconds",
                (long)time.tv_sec, (long)time.tv_usec / 10000);
    }
    fprintf(stdout, "\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
