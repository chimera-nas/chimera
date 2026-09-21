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

/* NFSv4 CLAIM_FH must bind rights at OPEN, including for a non-owner. */
static void
test_open_rights_after_chmod(struct posix_test_env *env)
{
    struct chimera_vfs_cred reader;
    int                     fd, denied;
    char                    byte;

    if (strncmp(env->backend, "smb", 3) == 0) {
        return;
    }
    fd = chimera_posix_open("/test/held_rights", O_CREAT | O_RDWR, 0666);
    if (fd < 0 || chimera_posix_write(fd, "x", 1) != 1 ||
        chimera_posix_fchmod(fd, 0666) != 0) {
        posix_test_fail(env);
    }
    chimera_posix_close(fd);
    chimera_vfs_cred_init_unix(&reader, env->cred.uid == 12345 ? 12346 : 12345,
                               12345, 0, NULL);
    chimera_posix_set_cred(&reader);
    fd = chimera_posix_open("/test/held_rights", O_RDWR, 0);
    if (fd < 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_chmod("/test/held_rights", 0000) != 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&reader);
    if (env->nfs_version == 3) {
        if (chimera_posix_read(fd, &byte, 1) != -1 || errno != EACCES ||
            chimera_posix_write(fd, "y", 1) != -1 || errno != EACCES ||
            chimera_posix_ftruncate(fd, 0) != -1 || errno != EACCES) {
            fprintf(stderr, "stateless NFS I/O bypassed current DAC\n");
            posix_test_fail(env);
        }
    } else if (chimera_posix_read(fd, &byte, 1) != 1 || byte != 'x' ||
               chimera_posix_write(fd, "y", 1) != 1) {
        fprintf(stderr, "chmod revoked an existing descriptor's I/O rights\n");
        posix_test_fail(env);
    }
    denied = chimera_posix_open("/test/held_rights", O_RDWR, 0);
    if (denied != -1 || errno != EACCES) {
        fprintf(stderr, "new open reused rights granted before chmod\n");
        posix_test_fail(env);
    }
    chimera_posix_close(fd);
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_unlink("/test/held_rights") != 0) {
        posix_test_fail(env);
    }
} /* test_open_rights_after_chmod */

/* A denied unlink must not leave a phantom silly rename. Keep a second open
 * alive when the first closes so cleanup cannot remove its backing name. */
static void
test_failed_unlink_then_open_io(struct posix_test_env *env)
{
    struct chimera_vfs_cred other;
    chimera_posix_stat_t    st;
    char                    byte;
    int                     first, second;

    if (strncmp(env->backend, "smb", 3) == 0) {
        return;
    }
    if (chimera_posix_mkdir("/test/unlink_rights", 0777) != 0 ||
        chimera_posix_chmod("/test/unlink_rights", 0777) != 0) {
        posix_test_fail(env);
    }
    first = chimera_posix_open("/test/unlink_rights/file", O_CREAT | O_RDWR, 0666);
    if (first < 0 || chimera_posix_write(first, "x", 1) != 1 ||
        chimera_posix_fchmod(first, 0666) != 0) {
        posix_test_fail(env);
    }
    chimera_vfs_cred_init_unix(&other, env->cred.uid == 12345 ? 12346 : 12345,
                               12345, 0, NULL);
    chimera_posix_set_cred(&other);
    second = chimera_posix_open("/test/unlink_rights/file", O_RDONLY, 0);
    if (second < 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_chmod("/test/unlink_rights", 0755) != 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&other);
    if (chimera_posix_unlink("/test/unlink_rights/file") != -1 || errno != EACCES) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_unlink("/test/unlink_rights/file") != 0 ||
        chimera_posix_fstat(first, &st) != 0 ||
        (env->nfs_version == 3 && st.st_nlink != 1)) {
        fprintf(stderr, "failed unlink poisoned the later silly rename\n");
        posix_test_fail(env);
    }
    chimera_posix_close(first);
    chimera_posix_set_cred(&other);
    if (chimera_posix_read(second, &byte, 1) != 1 || byte != 'x') {
        fprintf(stderr, "closing another open invalidated the remaining descriptor\n");
        posix_test_fail(env);
    }
    chimera_posix_close(second);
    chimera_posix_set_cred(&env->cred);
} /* test_failed_unlink_then_open_io */

static void
test_directory_read_after_chmod(struct posix_test_env *env)
{
    struct chimera_vfs_cred other;
    int                     fd;
    char                    byte;

    if (strncmp(env->backend, "smb", 3) == 0) {
        return;
    }
    if (chimera_posix_mkdir("/test/read_directory", 0777) != 0 ||
        chimera_posix_chmod("/test/read_directory", 0777) != 0) {
        posix_test_fail(env);
    }
    chimera_vfs_cred_init_unix(&other, env->cred.uid == 12345 ? 12346 : 12345,
                               12345, 0, NULL);
    chimera_posix_set_cred(&other);
    fd = chimera_posix_open("/test/read_directory", O_RDONLY | O_DIRECTORY, 0);
    if (fd < 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_chmod("/test/read_directory", 0) != 0) {
        posix_test_fail(env);
    }
    chimera_posix_set_cred(&other);
    if (chimera_posix_read(fd, &byte, 1) != -1 || errno != EISDIR) {
        fprintf(stderr, "directory read did not report EISDIR after chmod: %s\n", strerror(errno));
        posix_test_fail(env);
    }
    chimera_posix_close(fd);
    chimera_posix_set_cred(&env->cred);
    if (chimera_posix_rmdir("/test/read_directory") != 0) {
        posix_test_fail(env);
    }
} /* test_directory_read_after_chmod */

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
    test_open_rights_after_chmod(&env);
    test_failed_unlink_then_open_io(&env);
    test_directory_read_after_chmod(&env);

    fprintf(stderr, "openat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
