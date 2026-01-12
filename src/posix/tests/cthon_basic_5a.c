// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test write only
// Based on cthon/basic/test5a.c from Connectathon 2004

#include "cthon_common.h"

#define BUFSZ   8192
#define DSIZE   1048576

static int Tflag = 0;
static int Fflag = 0;
static int Nflag = 0;

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   count = CTHON_DCOUNT;
    int                   ct;
    off_t                 size = DSIZE;
    off_t                 si;
    int                   fd;
    off_t                 bytes = 0;
    char                 *bigfile = "bigfile";
    struct timeval        time;
    int                   opt;
    char                  buf[BUFSZ];
    char                  str[MAXPATHLEN];
    double                etime;
    ssize_t               n;

    cthon_Myname = "cthon_basic_5a";

    posix_test_init(&env, argv, argc);

    for (int i = 0; i < BUFSZ; i++) {
        buf[i] = (char)(i % 256);
    }

    optind = 1;
    while ((opt = getopt(argc, argv, "htfnb:")) != -1) {
        switch (opt) {
            case 't': Tflag++; break;
            case 'f': Fflag++; break;
            case 'n': Nflag++; break;
            case 'b': break;
            default: break;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc > 0) { size = cthon_getparm(*argv++, 1, "size"); argc--; }
    if (argc > 0) { count = cthon_getparm(*argv++, 1, "count"); argc--; }
    if (argc > 0) { bigfile = *argv++; argc--; }

    if (Fflag) { Tflag = 0; count = 1; }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!Nflag) cthon_testdir(NULL);
    else cthon_mtestdir(NULL);

    fprintf(stdout, "%s: write\n", cthon_Myname);
    snprintf(str, sizeof(str), "%s/%s", cthon_getcwd(), bigfile);

    if (Tflag) cthon_starttime();

    for (ct = 0; ct < count; ct++) {
        fd = chimera_posix_open(str, O_CREAT | O_RDWR | O_TRUNC, CTHON_CHMOD_RW);
        if (fd < 0) {
            cthon_error("can't create %s", str);
            posix_test_fail(&env);
        }

        for (si = size; si > 0; si -= n) {
            size_t towrite = (si < BUFSZ) ? si : BUFSZ;
            n = chimera_posix_write(fd, buf, towrite);
            if (n < 0 || (size_t)n != towrite) {
                cthon_error("write failed");
                posix_test_fail(&env);
            }
            bytes += n;
        }

        chimera_posix_close(fd);
        chimera_posix_unlink(str);
    }

    if (Tflag) cthon_endtime(&time);

    fprintf(stdout, "\twrote %lld bytes", (long long)(size * count));
    if (Tflag) {
        etime = (double)time.tv_sec + (double)time.tv_usec / 1000000.0;
        if (etime != 0.0) {
            fprintf(stdout, " in %ld.%-2ld seconds (%ld KB/sec)",
                    (long)time.tv_sec, (long)time.tv_usec / 10000,
                    (long)((double)bytes / etime / 1024.0));
        }
    }
    fprintf(stdout, "\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
