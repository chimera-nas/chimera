// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test read/write of holey (sparse) files
// Based on cthon/special/holey.c from Connectathon 2004

#include "cthon_common.h"

#define BUFSZ   8192
#define FILESZ  70000
#define DATASZ  4321
#define HOLESZ  9012
#define FILENM  "holeyfile"

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

static int Debug = 0;

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    int                   i, tot, ct, sz, bytes, ret;
    char                  buf[BUFSZ];
    int                   filesz = FILESZ;
    int                   datasz = DATASZ;
    int                   holesz = HOLESZ;
    int                   serrno;

    cthon_Myname = "cthon_special_holey";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hdb:")) != -1) {
        switch (opt) {
            case 'd': Debug = 1; break;
            case 'b': break;
            default: break;
        }
    }

    argc -= optind;
    argv += optind;

    // Optional args: filesz datasz holesz
    if (argc > 0) { filesz = atoi(argv[0]); argc--; argv++; }
    if (argc > 0) { datasz = atoi(argv[0]); argc--; argv++; }
    if (argc > 0) { holesz = atoi(argv[0]); argc--; argv++; }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: holey file test (filesz=%d, datasz=%d, holesz=%d)\n",
            cthon_Myname, filesz, datasz, holesz);

    if (datasz > BUFSZ) {
        cthon_error("datasize (%d) greater than maximum (%d)", datasz, BUFSZ);
        posix_test_fail(&env);
    }

    snprintf(str, sizeof(str), "%s/%s", cthon_getcwd(), FILENM);

    fd = chimera_posix_open(str, O_CREAT | O_TRUNC | O_RDWR, 0666);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        posix_test_fail(&env);
    }

    if (chimera_posix_close(fd) < 0) {
        cthon_error("can't close %s after create", str);
        posix_test_fail(&env);
    }

    fd = chimera_posix_open(str, O_RDWR, 0);
    if (fd < 0) {
        cthon_error("can't reopen %s", str);
        posix_test_fail(&env);
    }

    // Initialize buffer with pattern
    for (i = 0; i < BUFSZ / (int)sizeof(int); i++) {
        ((int *)buf)[i] = i;
    }

    // Write phase: write data, then seek over holes
    for (sz = filesz; sz > 0; ) {
        if (datasz || sz == 1) {
            bytes = MIN(sz, datasz);
            if (bytes == 0)
                bytes = 1;
            ret = chimera_posix_write(fd, buf, bytes);
            if (ret != bytes) {
                serrno = errno;
                fprintf(stderr, "write ret %d (expected %d)\n", ret, bytes);
                if (serrno) {
                    errno = serrno;
                    perror("write");
                }
                chimera_posix_close(fd);
                posix_test_fail(&env);
            }
            sz -= bytes;
        }
        if (sz && holesz) {
            bytes = MIN(sz - 1, holesz);
            if (chimera_posix_lseek(fd, bytes, SEEK_CUR) == -1L) {
                perror("lseek (write)");
                chimera_posix_close(fd);
                posix_test_fail(&env);
            }
            sz -= bytes;
        }
    }

    // Rewind for read
    if (chimera_posix_lseek(fd, 0, SEEK_SET) == -1L) {
        perror("lseek (rewind)");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Read phase: verify data and holes
    for (sz = filesz; sz > 0; ) {
        if (datasz || sz == 1) {
            bytes = MIN(sz, datasz);
            if (bytes == 0)
                bytes = 1;
            sz -= bytes;
            for (; bytes > 0; bytes -= ret) {
                if (Debug) {
                    fprintf(stderr, "--data read: offset %d, sz=%d, bytes=%d\n",
                            filesz - sz - bytes, sz, bytes);
                }
                ret = chimera_posix_read(fd, buf, bytes);
                if (ret <= 0) {
                    serrno = errno;
                    fprintf(stderr,
                            "read (data) offset %d, sz=%d, bytes=%d (ret=%d)\n",
                            filesz - sz - bytes, sz, bytes, ret);
                    if (ret < 0) {
                        errno = serrno;
                        perror("read");
                    }
                    chimera_posix_close(fd);
                    posix_test_fail(&env);
                }
                ct = bytes - (bytes % sizeof(int));
                if (Debug) {
                    fprintf(stderr, "  ret=%d, ct=%d\n", ret, ct);
                }
                for (i = 0; i < ct / (int)sizeof(int); i++) {
                    if (((int *)buf)[i] != i) {
                        fprintf(stderr, "bad data in %s\n", str);
                        if (Debug) {
                            fprintf(stderr, "  address=%d, valueis=%d, shouldbe=%d\n",
                                    i, ((int *)buf)[i], i);
                        }
                        chimera_posix_close(fd);
                        posix_test_fail(&env);
                    }
                }
            }
        }
        if (sz && holesz) {
            tot = MIN(holesz, sz - 1);
            sz -= tot;
            for (ct = 0; tot > 0; tot -= ret, ct += ret) {
                bytes = MIN(tot, BUFSZ);
                if (Debug) {
                    fprintf(stderr, "++hole read: offset %d, sz=%d, tot=%d, bytes=%d\n",
                            filesz - sz - tot, sz, tot, bytes);
                }
                ret = chimera_posix_read(fd, buf, bytes);
                if (ret <= 0) {
                    serrno = errno;
                    fprintf(stderr,
                            "read (hole) offset %d, sz=%d, bytes=%d (ret=%d)\n",
                            filesz - sz - tot, sz, bytes, ret);
                    if (ret < 0) {
                        errno = serrno;
                        perror("read");
                    }
                    chimera_posix_close(fd);
                    posix_test_fail(&env);
                }
                if (Debug) {
                    fprintf(stderr, "  ret=%d\n", ret);
                }
                for (i = 0; i < ret; i++) {
                    if (buf[i] != '\0') {
                        fprintf(stderr,
                                "non-zero data read back from hole (offset %d)\n",
                                filesz - sz + ct + i);
                        chimera_posix_close(fd);
                        posix_test_fail(&env);
                    }
                }
            }
        }
    }

    chimera_posix_close(fd);
    chimera_posix_unlink(str);

    fprintf(stdout, "\tHoley file test ok\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
