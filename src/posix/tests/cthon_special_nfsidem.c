// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Idempotency test
// Based on cthon/special/nfsidem.c from Connectathon 2004
//
// Performs a sequence of operations to stress test idempotent behavior:
//   mkdir, create, chmod, rename, link, symlink, unlink, rmdir, stat

#include "cthon_common.h"

#define STRCHARS 100

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  base[MAXPATHLEN];
    char                  dir[MAXPATHLEN];
    char                  foo[MAXPATHLEN];
    char                  bar[MAXPATHLEN];
    char                  sbar[MAXPATHLEN];
    char                  tbar[MAXPATHLEN];
    char                  lbar[MAXPATHLEN];
    char                  str[STRCHARS];
    int                   count = 10;
    int                   fd, slen, lerr, slerr;
    struct stat           sb;

    cthon_Myname = "cthon_special_nfsidem";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        } /* switch */
    }

    argc -= optind;
    argv += optind;

    // Optional args: count
    if (argc > 0) {
        count = atoi(argv[0]); argc--; argv++;
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: idempotency test (%d iterations)\n", cthon_Myname, count);

    snprintf(base, sizeof(base), "%s/TEST", cthon_getcwd());
    snprintf(dir, sizeof(dir), "%s/DIR", base);
    snprintf(foo, sizeof(foo), "%s/FOO", base);
    snprintf(bar, sizeof(bar), "%s/BAR", base);
    snprintf(sbar, sizeof(sbar), "%s/SBAR", base);
    snprintf(tbar, sizeof(tbar), "%s/DIR/BAR", base);
    snprintf(lbar, sizeof(lbar), "../TEST/DIR/BAR");
    snprintf(str, sizeof(str), "Idempotency test %ld running\n", (long) getpid());
    slen = strlen(str);

    while (count--) {
        // mkdir TEST
        if (chimera_posix_mkdir(base, 0755) < 0) {
            cthon_error("mkdir %s failed", base);
            posix_test_fail(&env);
        }

        // mkdir TEST/DIR
        if (chimera_posix_mkdir(dir, 0755) < 0) {
            cthon_error("mkdir %s failed", dir);
            posix_test_fail(&env);
        }

        // create TEST/FOO
        fd = chimera_posix_open(foo, O_RDWR | O_CREAT, 0666);
        if (fd < 0) {
            cthon_error("create %s failed", foo);
            posix_test_fail(&env);
        }

        // write to FOO
        if (chimera_posix_write(fd, str, slen) != slen) {
            cthon_error("write to %s failed", foo);
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }

        if (chimera_posix_close(fd) < 0) {
            cthon_error("close %s failed", foo);
            posix_test_fail(&env);
        }

        // chmod TEST/FOO to 0611
        if (chimera_posix_chmod(foo, 0611) < 0) {
            cthon_error("chmod %s failed", foo);
            posix_test_fail(&env);
        }

        // rename TEST/FOO to TEST/DIR/BAR
        if (chimera_posix_rename(foo, tbar) < 0) {
            cthon_error("rename %s to %s failed", foo, tbar);
            posix_test_fail(&env);
        }

        // link TEST/DIR/BAR to TEST/BAR
        lerr = chimera_posix_link(tbar, bar);
        if (lerr < 0) {
            if (errno != EOPNOTSUPP) {
                cthon_error("link %s to %s failed", tbar, bar);
                posix_test_fail(&env);
            }
            lerr = 1;  // mark as failed
        } else {
            lerr = 0;
            // rename TEST/BAR to TEST/DIR/BAR (should be noop since same inode)
            if (chimera_posix_rename(bar, tbar) < 0) {
                cthon_error("rerename %s to %s failed", bar, tbar);
                posix_test_fail(&env);
            }
        }

        // symlink ../TEST/DIR/BAR to TEST/SBAR
        slerr = chimera_posix_symlink(lbar, sbar);
        if (slerr < 0) {
            if (errno != EOPNOTSUPP) {
                cthon_error("symlink %s to %s failed", lbar, sbar);
                posix_test_fail(&env);
            }
            slerr = 1;
        } else {
            slerr = 0;
        }

        // stat the file through symlink, hardlink, or direct path
        if (chimera_posix_stat(!slerr ? sbar : !lerr ? bar : tbar, &sb) < 0) {
            cthon_error("stat failed");
            posix_test_fail(&env);
        }

        // Verify file mode and size
        if ((sb.st_mode & (S_IFMT | 07777)) != (S_IFREG | 0611) ||
            sb.st_size != slen) {
            fprintf(stderr, "\tbad file type/size: mode=0%o, size=%ld (expected 0%o, %d)\n",
                    (int) sb.st_mode, (long) sb.st_size, S_IFREG | 0611, slen);
            posix_test_fail(&env);
        }

        // unlink TEST/DIR/BAR
        if (chimera_posix_unlink(tbar) < 0) {
            cthon_error("unlink %s failed", tbar);
            posix_test_fail(&env);
        }

        // unlink TEST/BAR if it exists
        if (lerr == 0) {
            if (chimera_posix_unlink(bar) < 0) {
                cthon_error("unlink %s failed", bar);
                posix_test_fail(&env);
            }
        }

        // unlink TEST/SBAR if it exists
        if (slerr == 0) {
            if (chimera_posix_unlink(sbar) < 0) {
                cthon_error("unlink %s failed", sbar);
                posix_test_fail(&env);
            }
        }

        // rmdir TEST/DIR
        if (chimera_posix_rmdir(dir) < 0) {
            cthon_error("rmdir %s failed", dir);
            posix_test_fail(&env);
        }

        // rmdir TEST
        if (chimera_posix_rmdir(base) < 0) {
            cthon_error("rmdir %s failed", base);
            posix_test_fail(&env);
        }

        // stat TEST - should fail with ENOENT
        if (chimera_posix_stat(base, &sb) == 0 || errno != ENOENT) {
            cthon_error("stat %s should have failed with ENOENT", base);
            posix_test_fail(&env);
        }

        errno = 0;  // clear error
    }

    fprintf(stdout, "\tidempotency test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
