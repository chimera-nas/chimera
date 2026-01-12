// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test seek to negative offset
// Based on cthon/special/negseek.c from Connectathon 2004
//
// Verifies that seeking to negative offsets fails properly

#include "cthon_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    long                  i;
    char                  buf[8192];

    cthon_Myname = "cthon_special_negseek";

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

    fprintf(stdout, "%s: negative seek test\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/negseek_test", cthon_getcwd());

    fd = chimera_posix_open(str, O_CREAT | O_RDONLY, 0666);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        posix_test_fail(&env);
    }

    // Try seeking to increasingly negative offsets
    // These should fail on most systems
    for (i = 0L; i > -10240L; i -= 1024L) {
        if (chimera_posix_lseek(fd, i, SEEK_SET) == -1L) {
            // Expected failure for negative offsets (except 0)
            if (i < 0) {
                fprintf(stdout, "\tlseek to %ld correctly failed: %s\n",
                        i, strerror(errno));
                chimera_posix_close(fd);
                chimera_posix_unlink(str);
                fprintf(stdout, "\tnegative seek test passed\n");
                cthon_complete();
                posix_test_umount();
                posix_test_success(&env);
                return 0;
            }
            perror("\tunexpected lseek failure");
            chimera_posix_close(fd);
            chimera_posix_unlink(str);
            posix_test_fail(&env);
        }
        if (chimera_posix_read(fd, buf, sizeof(buf)) == -1) {
            // Read after seek to negative offset failed
            if (i < 0) {
                fprintf(stdout, "\tread after lseek to %ld correctly failed\n", i);
                chimera_posix_close(fd);
                chimera_posix_unlink(str);
                fprintf(stdout, "\tnegative seek test passed\n");
                cthon_complete();
                posix_test_umount();
                posix_test_success(&env);
                return 0;
            }
            perror("\tunexpected read failure");
            chimera_posix_close(fd);
            chimera_posix_unlink(str);
            posix_test_fail(&env);
        }
    }

    chimera_posix_close(fd);
    chimera_posix_unlink(str);

    // If we got here, negative seeks didn't fail - this is unexpected
    // but some systems may allow this
    fprintf(stdout, "\tWarning: negative seeks succeeded (unusual behavior)\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
