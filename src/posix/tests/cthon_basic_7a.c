// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test rename only
// Based on cthon/basic/test7a.c from Connectathon 2004

#include "cthon_common.h"

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
    int                   files = CTHON_DCOUNT;
    int                   fi;
    int                   count = CTHON_DCOUNT;
    int                   ct;
    char                 *fname = CTHON_FNAME;
    char                 *nname = "newfile.";
    int                   fd;
    struct stat           statb;
    struct timeval        time;
    int                   opt;
    char                  str[MAXPATHLEN];
    char                  new[MAXPATHLEN];

    cthon_Myname = "cthon_basic_7a";

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
        files = cthon_getparm(*argv++, 1, "files"); argc--;
    }
    if (argc > 0) {
        count = cthon_getparm(*argv++, 1, "count"); argc--;
    }
    if (argc > 0) {
        fname = *argv++; argc--;
    }
    if (argc > 0) {
        nname = *argv++; argc--;
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

    fprintf(stdout, "%s: rename\n", cthon_Myname);

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
            snprintf(new, sizeof(new), "%s/%s%d", cthon_getcwd(), nname, fi);

            if (chimera_posix_rename(str, new) < 0) {
                cthon_error("can't rename %s to %s", str, new);
                posix_test_fail(&env);
            }

            if (chimera_posix_stat(str, &statb) == 0) {
                cthon_error("%s still exists after rename", str);
                posix_test_fail(&env);
            }

            if (chimera_posix_stat(new, &statb) < 0) {
                cthon_error("can't stat %s after rename", new);
                posix_test_fail(&env);
            }

            // Rename back
            if (chimera_posix_rename(new, str) < 0) {
                cthon_error("can't rename %s to %s", new, str);
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

    fprintf(stdout, "\t%d renames on %d files", files * count * 2, files);
    if (Tflag) {
        fprintf(stdout, " in %ld.%-2ld seconds",
                (long) time.tv_sec, (long) time.tv_usec / 10000);
    }
    fprintf(stdout, "\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
