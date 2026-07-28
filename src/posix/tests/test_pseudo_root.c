// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 pseudo-fs root mount ("server:/").
 *
 * Mounts the bare pseudo-fs root instead of an export and exercises the
 * pseudo-root compound handlers, most importantly nfs4_root_readdir: listing
 * the root resolves every export's backing path through an asynchronous VFS
 * lookup per entry, a path that once assumed synchronous completion and
 * crashed with a stack-use-after-return when a lookup completed off the
 * request thread.
 *
 * Verifies that READDIR of the root lists the "/share" export, that the
 * export can be entered by lookup from the root, that files created through
 * the pseudo-root path are readable back through it, and that mounting an
 * export the server does not have reports ENOENT.
 */

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    CHIMERA_DIR          *dir;
    struct dirent        *entry;
    struct stat           st;
    int                   rc;
    int                   fd;
    int                   found_share = 0;
    int                   num_entries = 0;
    const char           *test_data   = "pseudo-root";
    char                  buf[64];
    ssize_t               len;

    posix_test_init(&env, argv, argc);

    if (env.nfs_version != 4) {
        fprintf(stderr, "test_pseudo_root requires an NFS4 backend\n");
        posix_test_fail(&env);
    }

    /* Mounting an export the server does not have must report ENOENT.  The
     * mount compound's LOOKUP fails with NFS4ERR_NOENT, and the client maps
     * that per-op status rather than collapsing it into EIO. */
    rc = chimera_posix_mount_with_options("/badexport", "nfs",
                                          "127.0.0.1:/nosuchexport", "vers=4");
    if (rc == 0) {
        fprintf(stderr, "Mount of a nonexistent export unexpectedly "
                "succeeded\n");
        posix_test_fail(&env);
    }

    if (errno != ENOENT) {
        fprintf(stderr, "Mount of a nonexistent export reported %s, "
                "expected ENOENT\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/test", "nfs", "127.0.0.1:/",
                                          "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount pseudo-fs root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    /* READDIR of the pseudo-root must list each export exactly once. */
    dir = chimera_posix_opendir("/test");
    if (!dir) {
        fprintf(stderr, "Failed to open pseudo-root directory: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    while ((entry = chimera_posix_readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        fprintf(stderr, "pseudo-root entry: %s\n", entry->d_name);
        num_entries++;
        if (strcmp(entry->d_name, "share") == 0) {
            found_share++;
        }
    }

    chimera_posix_closedir(dir);

    if (found_share != 1) {
        fprintf(stderr, "Export 'share' listed %d times in pseudo-root, "
                "expected exactly once\n", found_share);
        posix_test_fail(&env);
    }

    if (num_entries != 1) {
        fprintf(stderr, "Pseudo-root listed %d entries, expected 1\n",
                num_entries);
        posix_test_fail(&env);
    }

    /* Entering the export from the pseudo-root (nfs4_root_lookup). */
    rc = chimera_posix_stat("/test/share", &st);
    if (rc != 0) {
        fprintf(stderr, "Failed to stat export via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Export root is not a directory\n");
        posix_test_fail(&env);
    }

    /* I/O through the pseudo-root path crosses into the export's backend. */
    fd = chimera_posix_open("/test/share/pseudo_root_file",
                            O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_write(fd, test_data, strlen(test_data));
    if (len != (ssize_t) strlen(test_data)) {
        fprintf(stderr, "Failed to write via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    fd = chimera_posix_open("/test/share/pseudo_root_file", O_RDONLY, 0);
    if (fd < 0) {
        fprintf(stderr, "Failed to reopen file via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_read(fd, buf, sizeof(buf));
    if (len != (ssize_t) strlen(test_data) ||
        memcmp(buf, test_data, len) != 0) {
        fprintf(stderr, "Read-back via pseudo-root mismatched\n");
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Pseudo-root mount test passed\n");

    posix_test_success(&env);

    return 0;
} /* main */
