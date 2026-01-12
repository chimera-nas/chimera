// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test operations on open file which has been unlinked
// Based on cthon/special/op_unlk.c from Connectathon 2004
//
// Steps taken:
//   1. create file
//   2. open for read/write
//   3. unlink file
//   4. write data
//   5. rewind
//   6. read data back

#include "cthon_common.h"

#define TBUFSIZ 100
static char wbuf[TBUFSIZ], rbuf[TBUFSIZ];
#define TMSG "This is a test message written to the unlinked file\n"

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    int                   ret;
    long                  lret;
    int                   errcount = 0;

    cthon_Myname = "cthon_special_op_unlk";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        }
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: operations on unlinked open file\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/op_unlk_test", cthon_getcwd());

    // Create and open the file
    fd = chimera_posix_open(str, O_CREAT | O_TRUNC | O_RDWR, CTHON_CHMOD_RW);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        posix_test_fail(&env);
    }

    // Unlink the file while it's open
    ret = chimera_posix_unlink(str);
    fprintf(stdout, "\t%s open; unlink ret = %d\n", str, ret);
    if (ret) {
        cthon_error("can't unlink %s", str);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Write data to the unlinked file
    strcpy(wbuf, TMSG);
    ret = chimera_posix_write(fd, wbuf, TBUFSIZ);
    if (ret != TBUFSIZ) {
        fprintf(stderr, "\twrite ret %d; expected %d\n", ret, TBUFSIZ);
        if (ret < 0)
            perror("\twrite");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Rewind
    lret = chimera_posix_lseek(fd, 0L, SEEK_SET);
    if (lret != 0L) {
        fprintf(stderr, "\tlseek ret %ld; expected 0\n", lret);
        if (lret < 0)
            perror("\tlseek");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Read data back
    ret = chimera_posix_read(fd, rbuf, TBUFSIZ);
    if (ret != TBUFSIZ) {
        fprintf(stderr, "\tread ret %d; expected %d\n", ret, TBUFSIZ);
        if (ret < 0)
            perror("\tread");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Compare data
    if (strcmp(wbuf, rbuf) != 0) {
        errcount++;
        fprintf(stdout, "\tread data not same as written data\n");
        fprintf(stdout, "\t written: '%s'\n\t read:    '%s'\n", wbuf, rbuf);
    } else {
        fprintf(stdout, "\tdata compare ok\n");
    }

    // Try to unlink again - should fail with ENOENT
    if (chimera_posix_unlink(str) == 0) {
        errcount++;
        fprintf(stdout, "\tError: second unlink succeeded!??\n");
    } else {
        if (errno != ENOENT) {
            errcount++;
            perror("\tunexpected error on second unlink");
        }
    }

    // Close the file
    ret = chimera_posix_close(fd);
    if (ret) {
        errcount++;
        perror("\terror on close");
    }

    // Second close should fail
    ret = chimera_posix_close(fd);
    if (ret == 0) {
        errcount++;
        fprintf(stderr, "\tsecond close didn't return error!??\n");
    }

    if (errcount == 0) {
        fprintf(stdout, "\ttest completed successfully.\n");
    } else {
        posix_test_fail(&env);
    }

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
