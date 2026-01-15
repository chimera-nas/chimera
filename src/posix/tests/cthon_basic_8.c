// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test symlink and readlink
// Based on cthon/basic/test8.c from Connectathon 2004
//
// Uses the following important system calls:
//   symlink()
//   readlink()
//   lstat()
//   unlink()

#include "cthon_common.h"

#define SNAME "/this/is/a/symlink"

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
    int                   count = 20;
    int                   ct;
    char                 *fname = CTHON_FNAME;
    char                 *sname = SNAME;
    struct stat           statb;
    struct timeval        time;
    int                   opt;
    char                  str[MAXPATHLEN];
    char                  new[MAXPATHLEN];
    char                  buf[MAXPATHLEN];
    ssize_t               ret;

    cthon_Myname = "cthon_basic_8";

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
        sname = *argv++; argc--;
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

    fprintf(stdout, "%s: symlink and readlink\n", cthon_Myname);

    if (Tflag) {
        cthon_starttime();
    }

    for (ct = 0; ct < count; ct++) {
        for (fi = 0; fi < files; fi++) {
            snprintf(str, sizeof(str), "%s/%s%d", cthon_getcwd(), fname, fi);
            snprintf(new, sizeof(new), "%s%d", sname, fi);

            if (chimera_posix_symlink(new, str) < 0) {
                int oerrno = errno;
                cthon_error("can't make symlink %s", str);
                errno = oerrno;
                if (errno == EOPNOTSUPP) {
                    cthon_complete();
                    posix_test_success(&env);
                    return 0;
                }
                posix_test_fail(&env);
            }

            if (chimera_posix_lstat(str, &statb) < 0) {
                cthon_error("can't lstat %s after symlink", str);
                posix_test_fail(&env);
            }

            if ((statb.st_mode & S_IFMT) != S_IFLNK) {
                cthon_error("mode of %s not symlink (got %o)", str, statb.st_mode);
                posix_test_fail(&env);
            }

            ret = chimera_posix_readlink(str, buf, sizeof(buf) - 1);
            if (ret != (ssize_t) strlen(new)) {
                cthon_error("readlink %s returned %zd, expected %zu",
                            str, ret, strlen(new));
                posix_test_fail(&env);
            }
            buf[ret] = '\0';

            if (strncmp(new, buf, ret) != 0) {
                cthon_error("readlink %s returned bad linkname: %s vs %s",
                            str, buf, new);
                posix_test_fail(&env);
            }

            if (chimera_posix_unlink(str) < 0) {
                cthon_error("can't unlink %s", str);
                posix_test_fail(&env);
            }
        }
    }

    if (Tflag) {
        cthon_endtime(&time);
    }

    fprintf(stdout, "\t%d symlinks and readlinks on %d files",
            files * count * 2, files);
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
