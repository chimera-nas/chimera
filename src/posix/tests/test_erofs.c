// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Read-only VFS mount enforcement (EROFS).
 *
 * The harness exposes the SAME NFS backend through two exports:
 *   - "/share"     read-write  (mounts the backend root)
 *   - "/share_ro"  read-only   (mounts the "ro" subdirectory of the same
 *                               backend, with the VFS "ro" mount flag)
 *
 * A file created via "/share/ro/<name>" is the very same inode the read-only
 * mount exposes as "/share_ro/<name>".  This test creates its fixtures (a
 * regular file, a directory, a symlink) through the writable export, then
 * mounts the read-only export at /test_ro and asserts:
 *   - reads / stat / lookup / readdir / readlink still work, and
 *   - every mutating operation fails with EROFS.
 *
 * TAP-style output is emitted on stderr ("ok"/"not ok") so the suite can be
 * scanned for failures.
 */

#define _GNU_SOURCE 1
#include <sys/sysmacros.h>
#include "posix_test_common.h"

static int test_num   = 0;
static int test_fails = 0;

static void
tap(
    int         ok,
    const char *desc)
{
    test_num++;
    if (!ok) {
        test_fails++;
    }
    fprintf(stderr, "%s %d - %s\n", ok ? "ok" : "not ok", test_num, desc);
} /* tap */

/* Run `expr` (which sets rc<0 + errno on failure) and assert it failed with
 * EROFS. */
#define EXPECT_EROFS(desc, expr) \
        do { \
            errno = 0; \
            long _rc = (long) (expr); \
            int  _en = errno; \
            tap(_rc < 0 && _en == EROFS, desc); \
            if (!(_rc < 0 && _en == EROFS)) { \
                fprintf(stderr, "  # %s: rc=%ld errno=%d (%s), wanted EROFS\n", \
                        desc, _rc, _en, strerror(_en)); \
            } \
        } while (0)

/* Run `expr` and assert it succeeded (rc >= 0). */
#define EXPECT_OK(desc, expr) \
        do { \
            errno = 0; \
            long _rc = (long) (expr); \
            int  _en = errno; \
            tap(_rc >= 0, desc); \
            if (_rc < 0) { \
                fprintf(stderr, "  # %s: rc=%ld errno=%d (%s), wanted success\n", \
                        desc, _rc, _en, strerror(_en)); \
            } \
        } while (0)

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   fd;
    struct stat           st;
    char                  buf[64];

    /* Ask the harness to also stand up the read-only export of the backend. */
    posix_test_ro_export = 1;

    posix_test_init(&env, argv, argc);

    if (env.nfs_version <= 0) {
        fprintf(stderr, "test_erofs requires an NFS backend\n");
        posix_test_fail(&env);
    }

    const char *vers = (env.nfs_version == 4) ? "vers=4" : "vers=3";

    /* Phase 1: mount the writable export and create the fixtures under its "ro"
     * subdirectory (the same inodes the read-only export is rooted at), then
     * unmount it.  The writable and read-only exports are NOT mounted at the
     * same time: a single client mounting two exports of the same NFS server
     * concurrently is a separate concern, and unmounting first keeps this test
     * focused on the read-only enforcement. */
    rc = chimera_posix_mount_with_options("/test", "nfs", "127.0.0.1:/share",
                                          vers);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount /share (rw): %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/ro/file", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create fixture file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    (void) chimera_posix_write(fd, "hello\n", 6);
    chimera_posix_close(fd);

    if (chimera_posix_mkdir("/test/ro/dir", 0755) != 0) {
        fprintf(stderr, "Failed to create fixture dir: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_symlink("file", "/test/ro/link") != 0) {
        fprintf(stderr, "Failed to create fixture symlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_umount("/test") != 0) {
        fprintf(stderr, "Failed to unmount /share (rw): %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Phase 2: mount the read-only export and run the assertions against it. */
    rc = chimera_posix_mount_with_options("/test_ro", "nfs",
                                          "127.0.0.1:/roshare", vers);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount /roshare (ro): %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* ---- Read-side operations must still succeed on the RO mount. ---- */
    EXPECT_OK("stat regular file (ro)", chimera_posix_stat("/test_ro/file", &st));
    EXPECT_OK("lstat symlink (ro)", chimera_posix_lstat("/test_ro/link", &st));
    EXPECT_OK("stat directory (ro)", chimera_posix_stat("/test_ro/dir", &st));

    EXPECT_OK("open O_RDONLY (ro)",
              (fd = chimera_posix_open("/test_ro/file", O_RDONLY, 0)));
    if (fd >= 0) {
        EXPECT_OK("read file contents (ro)",
                  chimera_posix_read(fd, buf, sizeof(buf)));
        chimera_posix_close(fd);
    } else {
        tap(0, "read file contents (ro)");
    }

    EXPECT_OK("readlink (ro)",
              chimera_posix_readlink("/test_ro/link", buf, sizeof(buf)));

    {
        CHIMERA_DIR *d = chimera_posix_opendir("/test_ro");
        tap(d != NULL, "opendir (ro)");
        if (d) {
            int saw = 0;
            errno = 0;
            while (chimera_posix_readdir(d) != NULL) {
                saw++;
            }
            tap(saw > 0, "readdir returns entries (ro)");
            chimera_posix_closedir(d);
        } else {
            tap(0, "readdir returns entries (ro)");
        }
    }

    EXPECT_OK("faccessat R_OK (ro)",
              chimera_posix_faccessat(AT_FDCWD, "/test_ro/file", R_OK, 0));

    /* ---- Mutating operations must all fail with EROFS on the RO mount. ---- */
    EXPECT_EROFS("chmod", chimera_posix_chmod("/test_ro/file", 0600));
    EXPECT_EROFS("chown", chimera_posix_chown("/test_ro/file", 1, 1));
    EXPECT_EROFS("mkdir", chimera_posix_mkdir("/test_ro/newdir", 0755));
    EXPECT_EROFS("mkfifo",
                 chimera_posix_mknod("/test_ro/fifo", S_IFIFO | 0644, 0));
    EXPECT_EROFS("mknod (char device)",
                 chimera_posix_mknod("/test_ro/dev", S_IFCHR | 0644,
                                     makedev(1, 3)));
    EXPECT_EROFS("open O_CREAT",
                 chimera_posix_open("/test_ro/created", O_CREAT | O_RDWR, 0644));
    EXPECT_EROFS("creat",
                 chimera_posix_open("/test_ro/creatfile",
                                    O_CREAT | O_WRONLY | O_TRUNC, 0644));
    EXPECT_EROFS("rename",
                 chimera_posix_rename("/test_ro/file", "/test_ro/file2"));
    EXPECT_EROFS("rmdir", chimera_posix_rmdir("/test_ro/dir"));
    EXPECT_EROFS("symlink",
                 chimera_posix_symlink("file", "/test_ro/newlink"));
    EXPECT_EROFS("link",
                 chimera_posix_link("/test_ro/file", "/test_ro/hardlink"));
    EXPECT_EROFS("unlink", chimera_posix_unlink("/test_ro/file"));
    EXPECT_EROFS("truncate", chimera_posix_truncate("/test_ro/file", 0));

    {
        struct timespec ts[2];
        ts[0].tv_sec  = ts[1].tv_sec = 0;
        ts[0].tv_nsec = ts[1].tv_nsec = 0;
        EXPECT_EROFS("utimensat",
                     chimera_posix_utimensat(AT_FDCWD, "/test_ro/file", ts, 0));
    }

    /* Over NFS there is no server-side OPEN that mutates for an existing file
     * (NFS3 has none; the posix client resolves the handle via LOOKUP), so a
     * writable open() succeeds locally and the read-only enforcement surfaces
     * on the first actual mutating op issued against the handle.  Open the
     * fixture for writing (allowed -- no mutation yet) and confirm that both
     * write() and ftruncate() through that handle return EROFS. */
    EXPECT_OK("open O_WRONLY for mutation attempts (ro)",
              (fd = chimera_posix_open("/test_ro/file", O_WRONLY, 0)));
    if (fd >= 0) {
        EXPECT_EROFS("write", chimera_posix_write(fd, "x", 1));
        EXPECT_EROFS("ftruncate", chimera_posix_ftruncate(fd, 0));
        struct timespec ts2[2] = { { 0, 0 }, { 0, 0 } };
        EXPECT_EROFS("futimens", chimera_posix_futimens(fd, ts2));
        chimera_posix_close(fd);
    } else {
        tap(0, "write");
        tap(0, "ftruncate");
        tap(0, "futimens");
    }

    /* Confirm the fixtures are intact (no mutation slipped through): the file
     * still exists, keeps its original mode and non-zero size, and the
     * directory still exists -- all read via the read-only export. */
    EXPECT_OK("fixture file still present (ro)",
              chimera_posix_stat("/test_ro/file", &st));
    tap(S_ISREG(st.st_mode) && (st.st_mode & 0777) == 0644 && st.st_size == 6,
        "fixture file unchanged (ro)");
    EXPECT_OK("fixture dir still present (ro)",
              chimera_posix_stat("/test_ro/dir", &st));

    (void) chimera_posix_umount("/test_ro");

    fprintf(stderr, "1..%d\n", test_num);

    if (test_fails) {
        fprintf(stderr, "%d/%d EROFS subtests FAILED\n", test_fails, test_num);
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
