// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

/* A cached write-only open must not satisfy a later reader (or an O_RDWR
 * open).  Exercise the reverse order too: NFS same-owner OPEN upgrades must
 * leave a writable server handle behind the coalesced stateid. */
static void
test_access_modes(struct posix_test_env *env)
{
    const int first_modes[] = { O_WRONLY, O_RDONLY };
    char      byte;
    int       first, second, both;

    for (size_t i = 0; i < 2; i++) {
        first = chimera_posix_openat(AT_FDCWD, "/test/access_modes",
                                     O_CREAT | first_modes[i], 0666);
        second = chimera_posix_openat(AT_FDCWD, "/test/access_modes",
                                      i == 0 ? O_RDONLY : O_WRONLY, 0);
        if (first < 0 || second < 0) {
            fprintf(stderr, "access-mode open failed: %s\n", strerror(errno));
            posix_test_fail(env);
        }
        if (chimera_posix_pwrite(i == 0 ? first : second, "x", 1, 0) != 1 ||
            chimera_posix_pread(i == 0 ? second : first, &byte, 1, 0) != 1 ||
            byte != 'x') {
            fprintf(stderr, "separate reader/writer I/O failed: %s\n", strerror(errno));
            posix_test_fail(env);
        }
        both = chimera_posix_openat(AT_FDCWD, "/test/access_modes", O_RDWR, 0);
        if (both < 0 || chimera_posix_pwrite(both, "y", 1, 0) != 1 ||
            chimera_posix_pread(both, &byte, 1, 0) != 1 || byte != 'y') {
            fprintf(stderr, "read-write upgrade failed: %s\n", strerror(errno));
            posix_test_fail(env);
        }
        chimera_posix_close(both);
        chimera_posix_close(second);
        chimera_posix_close(first);
        if (chimera_posix_unlink("/test/access_modes") != 0) {
            posix_test_fail(env);
        }
    }
} /* test_access_modes */

/* O_CREAT on an existing write-only file must not request read access from
 * the backend.  Use a different, unprivileged identity so root cannot hide it. */
static void
test_write_only_permissions(struct posix_test_env *env)
{
    struct chimera_vfs_cred writer;
    int                     fd;

    if (chimera_posix_mkdir("/test/write_only", 0777) != 0 ||
        chimera_posix_chmod("/test/write_only", 0777) != 0) {
        posix_test_fail(env);
    }
    fd = chimera_posix_open("/test/write_only/file", O_CREAT | O_WRONLY, 0222);
    if (fd < 0 || chimera_posix_fchmod(fd, 0222) != 0) {
        posix_test_fail(env);
    }
    chimera_posix_close(fd);
    chimera_vfs_cred_init_unix(&writer, env->cred.uid == 12345 ? 12346 : 12345,
                               12345, 0, NULL);
    chimera_posix_set_cred(&writer);
    fd = chimera_posix_open("/test/write_only/file", O_CREAT | O_WRONLY, 0);
    if (fd < 0 || chimera_posix_write(fd, "w", 1) != 1) {
        fprintf(stderr, "write-only open/I/O denied: %s\n", strerror(errno));
        posix_test_fail(env);
    }
    chimera_posix_close(fd);
    errno = 0;
    fd    = chimera_posix_open("/test/write_only/file", O_RDONLY, 0);
    if (fd != -1 || errno != EACCES) {
        fprintf(stderr, "write permission incorrectly granted read access\n");
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_unlink("/test/write_only/file") != 0 ||
        chimera_posix_rmdir("/test/write_only") != 0) {
        posix_test_fail(env);
    }
} /* test_write_only_permissions */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   fd;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing openat...\n");

    // Test with AT_FDCWD and absolute path
    fd = chimera_posix_openat(AT_FDCWD, "/test/openat_test.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "openat with AT_FDCWD failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Test with AT_FDCWD and relative path
    fd = chimera_posix_openat(AT_FDCWD, "test/openat_test2.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "openat with relative path failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Cleanup
    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/openat_test.txt", 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to unlink openat_test.txt: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/openat_test2.txt", 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to unlink openat_test2.txt: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    test_access_modes(&env);
    test_write_only_permissions(&env);

    fprintf(stderr, "openat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
