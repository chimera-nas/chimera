// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test write and reread of a large file
// Based on cthon/special/bigfile.c from Connectathon 2004
//
// This potentially covers a few problems:
// - inability of server to commit a large file range with one RPC
// - client's dirtying memory faster than it can clean it
// - server's returning bogus file attributes, confusing the client
// - client and server not propagating "filesystem full" errors

#include "cthon_common.h"

static int Tflag = 0;

static off_t file_size = 30 * 1024 * 1024;  // 30MB default
static int buffer_size = 8192;

static unsigned char
testval(off_t offset)
{
    return 'a' + (offset % 26);
}

static int
verify(char *buf, long bufsize, unsigned char val)
{
    int i;

    for (i = 0; i < bufsize; i++) {
        if ((unsigned char)(buf[i]) != val)
            return 0;
    }

    return 1;
}

static void
dump_buf(char *buf, int bufsize)
{
    int i;

    for (i = 0; i < bufsize; i++) {
        fprintf(stderr, "%02x ", (unsigned char)buf[i]);
        if ((i + 1) % 16 == 0)
            fprintf(stderr, "\n");
    }
    fprintf(stderr, "\n");
}

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    char                 *buf;
    int                   fd;
    long                  numbufs;
    int                   i;
    struct timeval        time;
    off_t                 size;

    cthon_Myname = "cthon_special_bigfile";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "htb:s:")) != -1) {
        switch (opt) {
            case 't': Tflag++; break;
            case 'b': break;
            case 's':
                size = atol(optarg) * 1024 * 1024;
                if (size > 0)
                    file_size = size;
                break;
            default: break;
        }
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: write and reread large file (%ld MB)\n",
            cthon_Myname, (long)(file_size / (1024 * 1024)));

    buf = malloc(buffer_size);
    if (buf == NULL) {
        cthon_error("can't allocate read/write buffer");
        posix_test_fail(&env);
    }

    snprintf(str, sizeof(str), "%s/bigfile", cthon_getcwd());

    fd = chimera_posix_open(str, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        free(buf);
        posix_test_fail(&env);
    }

    numbufs = file_size / buffer_size;

    if (Tflag) cthon_starttime();

    // Write phase
    for (i = 0; i < numbufs; i++) {
        unsigned char val = testval(i);
        int bytes_written;

        memset(buf, val, buffer_size);
        bytes_written = chimera_posix_write(fd, buf, buffer_size);
        if (bytes_written < 0) {
            int error = errno;
            cthon_error("write to %s failed: %s", str, strerror(error));
            if (error == EDQUOT || error == ENOSPC) {
                fprintf(stderr, "Warning: can't complete test (filesystem full)\n");
                chimera_posix_close(fd);
                chimera_posix_unlink(str);
                free(buf);
                posix_test_success(&env);  // Warn but don't fail
            }
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        } else if (bytes_written < buffer_size) {
            cthon_error("short write (%d) to %s", bytes_written, str);
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        }
    }

    if (chimera_posix_fsync(fd) < 0) {
        int error = errno;
        cthon_error("can't sync %s: %s", str, strerror(error));
        if (error == EDQUOT || error == ENOSPC) {
            chimera_posix_close(fd);
            chimera_posix_unlink(str);
            free(buf);
            posix_test_success(&env);
        }
        chimera_posix_close(fd);
        free(buf);
        posix_test_fail(&env);
    }

    // Close and reopen
    if (chimera_posix_close(fd) < 0) {
        cthon_error("can't close %s", str);
        free(buf);
        posix_test_fail(&env);
    }

    fd = chimera_posix_open(str, O_RDWR, 0666);
    if (fd < 0) {
        cthon_error("can't reopen %s", str);
        free(buf);
        posix_test_fail(&env);
    }

    // Read and verify phase
    for (i = 0; i < numbufs; i++) {
        unsigned char val = testval(i);
        int bytes_read;

        if (chimera_posix_lseek(fd, (off_t)i * buffer_size, SEEK_SET) < 0) {
            cthon_error("seek to %ld failed", (long)i * buffer_size);
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        }

        bytes_read = chimera_posix_read(fd, buf, buffer_size);
        if (bytes_read < 0) {
            cthon_error("read from %s failed", str);
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        } else if (bytes_read < buffer_size) {
            cthon_error("short read (%d) from %s", bytes_read, str);
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        }

        if (!verify(buf, buffer_size, val)) {
            fprintf(stderr, "verify failed, offset %ld; expected 0x%02x, got:\n",
                    (long)i * buffer_size, val);
            dump_buf(buf, buffer_size > 256 ? 256 : buffer_size);
            chimera_posix_close(fd);
            free(buf);
            posix_test_fail(&env);
        }
    }

    if (Tflag) cthon_endtime(&time);

    chimera_posix_close(fd);
    chimera_posix_unlink(str);
    free(buf);

    fprintf(stdout, "\tWrote and verified %ld MB", (long)(file_size / (1024 * 1024)));
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
