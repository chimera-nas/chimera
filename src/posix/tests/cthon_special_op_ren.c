// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test operations on open file which has been renamed over
// Based on cthon/special/op_ren.c from Connectathon 2004
//
// This test verifies that when a file is open, and another file
// is renamed over it, we can still read/write to the originally
// opened file.

#include "cthon_common.h"

#define TBUFSIZ 100
static char wbuf[TBUFSIZ], rbuf[TBUFSIZ];
#define TMSG    "This is a test message written to the target file\n"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  aname[MAXPATHLEN];
    char                  bname[MAXPATHLEN];
    int                   fd;
    int                   ret;
    long                  lret;
    int                   errcount = 0;

    cthon_Myname = "cthon_special_op_ren";

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

    fprintf(stdout, "%s: operations on renamed-over open file\n", cthon_Myname);

    snprintf(aname, sizeof(aname), "%s/op_ren_a", cthon_getcwd());
    snprintf(bname, sizeof(bname), "%s/op_ren_b", cthon_getcwd());

    // Create file A
    fd = chimera_posix_open(aname, O_CREAT | O_WRONLY, 0777);
    if (fd < 0) {
        cthon_error("can't create %s", aname);
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Create and open file B
    fd = chimera_posix_open(bname, O_CREAT | O_TRUNC | O_RDWR, 0777);
    if (fd < 0) {
        cthon_error("can't create %s", bname);
        chimera_posix_unlink(aname);
        posix_test_fail(&env);
    }

    // Rename A over B while B is open
    ret = chimera_posix_rename(aname, bname);
    fprintf(stdout, "\t%s open; rename ret = %d\n", bname, ret);
    if (ret) {
        cthon_error("can't rename %s to %s", aname, bname);
        chimera_posix_close(fd);
        chimera_posix_unlink(aname);
        chimera_posix_unlink(bname);
        posix_test_fail(&env);
    }

    // Write data to the original open file descriptor (which was B)
    strcpy(wbuf, TMSG);
    ret = chimera_posix_write(fd, wbuf, TBUFSIZ);
    if (ret != TBUFSIZ) {
        fprintf(stderr, "\twrite ret %d; expected %d\n", ret, TBUFSIZ);
        if (ret < 0) {
            perror("\twrite");
        }
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Rewind
    lret = chimera_posix_lseek(fd, 0L, SEEK_SET);
    if (lret != 0L) {
        fprintf(stderr, "\tlseek ret %ld; expected 0\n", lret);
        if (lret < 0) {
            perror("\tlseek");
        }
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Read data back
    ret = chimera_posix_read(fd, rbuf, TBUFSIZ);
    if (ret != TBUFSIZ) {
        fprintf(stderr, "\tread ret %d; expected %d\n", ret, TBUFSIZ);
        if (ret < 0) {
            perror("\tread");
        }
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

    // Cleanup
    chimera_posix_unlink(bname);

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
} /* main */
