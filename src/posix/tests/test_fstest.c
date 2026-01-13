// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
// SPDX-FileCopyrightText: 2002 tridge@samba.org
//
// SPDX-License-Identifier: GPL-2.0

/*
 * fstest - Filesystem data integrity verification tool
 *
 * This test creates files with known data patterns, then reads them back
 * and verifies the data is correct. Designed to detect data corruption.
 *
 * Originally from xfstests (tridge@samba.org, March 2002),
 * ported to Chimera POSIX userspace API.
 *
 * Note: mmap functionality is disabled since Chimera POSIX API does not
 * support memory-mapped I/O.
 */

#include <sys/wait.h>
#include "posix_test_common.h"
#include <dirent.h>

/* Test parameters (settable on the command line) */
static int   loop_count = 10;
static int   num_files  = 2;
static int   file_size  = 256 * 1024;  /* 256KB default */
static int   block_size = 1024;
static char *base_dir   = "/test";
static int   use_sync   = 0;
static int   do_frags   = 1;

typedef unsigned char uchar;

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif /* ifndef MIN */

static void *
x_malloc(int size)
{
    void *ret = malloc(size);

    if (!ret) {
        fprintf(stderr, "Out of memory for size %d!\n", size);
        exit(1);
    }
    return ret;
} /* x_malloc */

/*
 * Generate a buffer for a particular child, fnum etc.
 * Uses a simple deterministic pattern to make debugging easy.
 */
static void
gen_buffer(
    char *buf,
    int   loop,
    int   child,
    int   fnum,
    int   ofs)
{
    uchar v = (loop + child + fnum + (ofs / block_size)) % 256;

    memset(buf, v, block_size);
} /* gen_buffer */

/*
 * Check if a buffer read from disk is correct
 */
static int
check_buffer(
    uchar *buf,
    int    loop,
    int    child,
    int    fnum,
    int    ofs)
{
    char *buf2;
    int   ret = 0;

    buf2 = x_malloc(block_size);
    gen_buffer(buf2, loop, child, fnum, ofs);

    if (memcmp(buf, buf2, block_size) != 0) {
        int i, j;

        for (i = 0; buf[i] == buf2[i] && i < block_size; i++) {
        }

        fprintf(stderr, "CORRUPTION in child %d fnum %d at offset %d\n",
                child, fnum, ofs + i);

        fprintf(stderr, "Correct:   ");
        for (j = 0; j < MIN(20, block_size - i); j++) {
            fprintf(stderr, "%02x ", (uchar) buf2[j + i]);
        }
        fprintf(stderr, "\n");

        fprintf(stderr, "Incorrect: ");
        for (j = 0; j < MIN(20, block_size - i); j++) {
            fprintf(stderr, "%02x ", buf[j + i]);
        }
        fprintf(stderr, "\n");

        for (j = i; buf[j] != buf2[j] && j < block_size; j++) {
        }
        fprintf(stderr, "Corruption length: %d bytes\n", j - i);

        ret = 1;
    }

    free(buf2);
    return ret;
} /* check_buffer */

/*
 * Create a file with a known data set for a child
 */
static int
create_file(
    const char *dir,
    int         loop,
    int         child,
    int         fnum)
{
    char *buf;
    int   size, fd;
    char  fname[1024];

    buf = x_malloc(block_size);
    snprintf(fname, sizeof(fname), "%s/file%d", dir, fnum);

    fd = chimera_posix_open(fname,
                            O_RDWR | O_CREAT | O_TRUNC | (use_sync ? O_SYNC : 0),
                            0644);
    if (fd == -1) {
        perror(fname);
        free(buf);
        return 1;
    }

    for (size = 0; size < file_size; size += block_size * do_frags) {
        ssize_t written;

        gen_buffer(buf, loop, child, fnum, size);
        written = chimera_posix_pwrite(fd, buf, block_size, size);
        if (written != block_size) {
            fprintf(stderr, "Write failed at offset %d: wrote %zd, expected %d\n",
                    size, written, block_size);
            free(buf);
            chimera_posix_close(fd);
            return 1;
        }
    }

    free(buf);
    chimera_posix_close(fd);
    return 0;
} /* create_file */

/*
 * Check that a file has the correct data
 */
static int
check_file(
    const char *dir,
    int         loop,
    int         child,
    int         fnum)
{
    uchar *buf;
    int    size, fd;
    char   fname[1024];
    int    ret = 0;

    buf = x_malloc(block_size);
    snprintf(fname, sizeof(fname), "%s/file%d", dir, fnum);

    fd = chimera_posix_open(fname, O_RDONLY, 0);
    if (fd == -1) {
        perror(fname);
        free(buf);
        return 1;
    }

    for (size = 0; size < file_size; size += block_size * do_frags) {
        ssize_t nread;

        nread = chimera_posix_pread(fd, buf, block_size, size);
        if (nread != block_size) {
            fprintf(stderr, "Read failed at offset %d: read %zd, expected %d\n",
                    size, nread, block_size);
            ret = 1;
            break;
        }
        if (check_buffer(buf, loop, child, fnum, size) != 0) {
            ret = 1;
            break;
        }
    }

    free(buf);
    chimera_posix_close(fd);
    return ret;
} /* check_file */

/*
 * Recursive directory traversal - used for cleanup
 * Calls fn() on all files/dirs in the tree
 */
static void
traverse(
    const char *dir,
    int (      *fn )(const char *))
{
    CHIMERA_DIR   *d;
    struct dirent *de;

    d = chimera_posix_opendir(dir);
    if (!d) {
        return;
    }

    while ((de = chimera_posix_readdir(d))) {
        char        fname[1024];
        struct stat st;

        if (strcmp(de->d_name, ".") == 0) {
            continue;
        }
        if (strcmp(de->d_name, "..") == 0) {
            continue;
        }

        snprintf(fname, sizeof(fname), "%s/%s", dir, de->d_name);
        if (chimera_posix_lstat(fname, &st)) {
            perror(fname);
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            traverse(fname, fn);
        }

        fn(fname);
    }

    chimera_posix_closedir(d);
} /* traverse */

static int
remove_file(const char *path)
{
    struct stat st;

    if (chimera_posix_lstat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return chimera_posix_rmdir(path);
        } else {
            return chimera_posix_unlink(path);
        }
    }
    return 0;
} /* remove_file */

/*
 * The main child function - creates/checks files for one child process
 */
static int
run_child(int child)
{
    int  i, loop;
    char dir[1024];
    int  ret = 0;

    snprintf(dir, sizeof(dir), "%s/child%d", base_dir, child);

    /* Cleanup any old files */
    traverse(dir, remove_file);
    chimera_posix_rmdir(dir);

    if (chimera_posix_mkdir(dir, 0755) != 0) {
        perror(dir);
        return 1;
    }

    for (loop = 0; loop < loop_count; loop++) {
        fprintf(stderr, "Child %d loop %d\n", child, loop);

        /* Create all files */
        for (i = 0; i < num_files; i++) {
            if (create_file(dir, loop, child, i) != 0) {
                ret = 1;
                goto cleanup;
            }
        }

        /* Verify all files */
        for (i = 0; i < num_files; i++) {
            if (check_file(dir, loop, child, i) != 0) {
                ret = 1;
                goto cleanup;
            }
        }
    }

 cleanup:
    /* Cleanup */
    fprintf(stderr, "Child %d cleaning up %s\n", child, dir);
    traverse(dir, remove_file);
    chimera_posix_rmdir(dir);

    return ret;
} /* run_child */

static void
usage(void)
{
    fprintf(stderr,
            "\nUsage: test_fstest -b <backend> [options]\n"
            "\n"
            " -b backend        VFS backend (required)\n"
            " -F                generate files with holes (fragmented)\n"
            " -n num_children   set number of child processes (default: 1)\n"
            " -f num_files      set number of files (default: %d)\n"
            " -s file_size      set file sizes (default: %d)\n"
            " -k block_size     set block (IO) size (default: %d)\n"
            " -l loops          set loop count (default: %d)\n"
            " -S                use synchronous IO\n"
            " -h                show this help message\n",
            num_files, file_size, block_size, loop_count);
} /* usage */

int
main(
    int   argc,
    char *argv[])
{
    struct posix_test_env env;
    int                   c;
    int                   num_children = 1;
    int                   i, status, ret;

    posix_test_init(&env, argv, argc);

    /* Reset optind for our own option parsing */
    optind = 1;

    while ((c = getopt(argc, argv, "b:Fn:s:f:l:k:Sh")) != -1) {
        switch (c) {
            case 'b':
                /* Already handled by posix_test_init */
                break;
            case 'F':
                do_frags = 2;
                break;
            case 'n':
                num_children = strtol(optarg, NULL, 0);
                break;
            case 'k':
                block_size = strtol(optarg, NULL, 0);
                break;
            case 'f':
                num_files = strtol(optarg, NULL, 0);
                break;
            case 's':
                file_size = strtol(optarg, NULL, 0);
                break;
            case 'S':
                use_sync = 1;
                break;
            case 'l':
                loop_count = strtol(optarg, NULL, 0);
                break;
            case 'h':
                usage();
                exit(0);
            default:
                usage();
                exit(1);
        } /* switch */
    }

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test filesystem\n");
        posix_test_fail(&env);
    }

    /* Round up the file size to block boundary */
    if (file_size % block_size != 0) {
        file_size  = (file_size + (block_size - 1)) / block_size;
        file_size *= block_size;
        fprintf(stderr, "Rounded file size to %d\n", file_size);
    }

    fprintf(stderr, "fstest: backend=%s num_children=%d file_size=%d num_files=%d "
            "loop_count=%d block_size=%d sync=%d\n",
            env.backend, num_children, file_size, num_files, loop_count, block_size, use_sync);

    fprintf(stderr, "Total data size %.1f Mbyte\n",
            num_files * num_children * 1.0e-6 * file_size);

    /* For single child, run directly */
    if (num_children == 1) {
        ret = run_child(0);
        posix_test_umount();

        if (ret != 0) {
            fprintf(stderr, "fstest failed with status %d\n", ret);
            posix_test_fail(&env);
        }

        fprintf(stderr, "fstest completed successfully\n");
        posix_test_success(&env);
        return 0;
    }

    /* Fork and run run_child() for each child */
    for (i = 0; i < num_children; i++) {
        if (fork() == 0) {
            exit(run_child(i));
        }
    }

    ret = 0;

    /* Wait for children to exit */
    while (waitpid(0, &status, 0) == 0 || errno != ECHILD) {
        if (WEXITSTATUS(status) != 0) {
            ret = WEXITSTATUS(status);
            fprintf(stderr, "Child exited with status %d\n", ret);
        }
    }

    posix_test_umount();

    if (ret != 0) {
        fprintf(stderr, "fstest failed with status %d\n", ret);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fstest completed successfully\n");
    posix_test_success(&env);
    return ret;
} /* main */
