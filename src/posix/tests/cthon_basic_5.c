// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test read and write
// Based on cthon/basic/test5.c from Connectathon 2004
//
// Uses the following important system calls against the server:
//   creat()
//   open()
//   read()
//   write()
//   stat()
//   fstat()
//   unlink()

#include "cthon_common.h"

#define BUFSZ 8192
#define DSIZE 1048576

static int Tflag = 0;
static int Fflag = 0;
static int Nflag = 0;

static void
usage(void)
{
    fprintf(stdout, "usage: %s [-htfn] [size count fname]\n", cthon_Myname);
} /* usage */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   count = CTHON_DCOUNT;
    int                   ct;
    off_t                 size = DSIZE;
    off_t                 si;
    int                   fd;
    off_t                 bytes   = 0;
    char                 *bigfile = "bigfile";
    struct timeval        time;
    struct stat           statb;
    int                   opt;
    char                  buf[BUFSZ];
    char                  str[MAXPATHLEN];
    double                etime;
    ssize_t               n;

    cthon_Myname = "cthon_basic_5";

    posix_test_init(&env, argv, argc);

    // Initialize buffer with pattern
    for (int i = 0; i < BUFSZ; i++) {
        buf[i] = (char) (i % 256);
    }

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
        size = cthon_getparm(*argv++, 1, "size");
        argc--;
    }
    if (argc > 0) {
        count = cthon_getparm(*argv++, 1, "count");
        argc--;
    }
    if (argc > 0) {
        bigfile = *argv++;
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

    fprintf(stdout, "%s: read and write\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/%s", cthon_getcwd(), bigfile);

    if (Tflag) {
        cthon_starttime();
    }

    for (ct = 0; ct < count; ct++) {
        // Write phase
        fd = chimera_posix_open(str, O_CREAT | O_RDWR | O_TRUNC, CTHON_CHMOD_RW);
        if (fd < 0) {
            cthon_error("can't create %s", str);
            posix_test_fail(&env);
        }

        for (si = size; si > 0; si -= n) {
            size_t towrite = (si < BUFSZ) ? si : BUFSZ;
            n = chimera_posix_write(fd, buf, towrite);
            if (n < 0) {
                cthon_error("write failed");
                posix_test_fail(&env);
            }
            if ((size_t) n != towrite) {
                cthon_error("short write: %zd vs %zu", n, towrite);
                posix_test_fail(&env);
            }
            bytes += n;
        }

        if (chimera_posix_close(fd) < 0) {
            cthon_error("can't close %s", str);
            posix_test_fail(&env);
        }

        // Verify size
        if (chimera_posix_stat(str, &statb) < 0) {
            cthon_error("can't stat %s", str);
            posix_test_fail(&env);
        }
        if (statb.st_size != size) {
            cthon_error("%s has size %lld, expected %lld",
                        str, (long long) statb.st_size, (long long) size);
            posix_test_fail(&env);
        }

        // Read phase
        fd = chimera_posix_open(str, O_RDONLY, 0);
        if (fd < 0) {
            cthon_error("can't open %s for reading", str);
            posix_test_fail(&env);
        }

        for (si = size; si > 0; si -= n) {
            size_t toread = (si < BUFSZ) ? si : BUFSZ;
            n = chimera_posix_read(fd, buf, toread);
            if (n < 0) {
                cthon_error("read failed");
                posix_test_fail(&env);
            }
            if (n == 0) {
                cthon_error("unexpected EOF");
                posix_test_fail(&env);
            }
            bytes += n;
        }

        if (chimera_posix_close(fd) < 0) {
            cthon_error("can't close %s", str);
            posix_test_fail(&env);
        }

        // Remove for next iteration
        if (chimera_posix_unlink(str) < 0) {
            cthon_error("can't unlink %s", str);
            posix_test_fail(&env);
        }
    }

    if (Tflag) {
        cthon_endtime(&time);
    }

    fprintf(stdout, "\twrote %lld bytes, read %lld bytes",
            (long long) (size * count), (long long) (size * count));
    if (Tflag) {
        etime = (double) time.tv_sec + (double) time.tv_usec / 1000000.0;
        if (etime != 0.0) {
            fprintf(stdout, " in %ld.%-2ld seconds (%ld KB/sec)",
                    (long) time.tv_sec, (long) time.tv_usec / 10000,
                    (long) ((double) bytes / etime / 1024.0));
        } else {
            fprintf(stdout, " in %ld.%-2ld seconds",
                    (long) time.tv_sec, (long) time.tv_usec / 10000);
        }
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
