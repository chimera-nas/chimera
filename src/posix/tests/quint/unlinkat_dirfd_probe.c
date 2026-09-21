/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 */

#define POSIX_DRIVER_ENGINE_ONLY
#include "posix_driver.c"

static int failures;

static void
require_success(
    const char *what,
    int         rc)
{
    if (rc < 0) {
        fprintf(stderr, "FAIL: %s: %s\n", what, strerror(errno));
        exit(1);
    }
} /* require_success */

static void
expect_errno(
    const char *what,
    int         rc,
    int         expected)
{
    if (rc != -1 || errno != expected) {
        fprintf(stderr, "FAIL: %s: rc=%d errno=%d expected=%d\n",
                what, rc, errno, expected);
        failures++;
    }
} /* expect_errno */

int
main(
    int    argc,
    char **argv)
{
    const char *backend = argc > 1 ? argv[1] : "memfs";
    const int   modes[] = { O_RDONLY, O_WRONLY, O_RDWR };
    int         fd, alias, dirfd, second, victim;

    proto_out = stdout;
    setenv("CHIMERA_CLOSE_SWEEP_MIN_AGE_MS", "0", 1);
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 1);
    if (posix_env_setup(backend, NULL) != 0) {
        return 1;
    }

    dirfd = chimera_posix_open("/test", O_RDONLY | O_DIRECTORY);
    require_success("open directory", dirfd);
    for (int which = 0; which < 12; which++) {
        int access = modes[which % 3];
        int create = (which / 3) % 2 == 0;
        int flags  = access | (create ? O_CREAT : 0);

        fprintf(stderr, "dirfd case: openat=%d create=%d access=%d\n", which >= 6, create, access);
        if (!create) {
            victim = chimera_posix_open("/test/file", O_CREAT | O_RDWR, 0666);
            require_success("precreate file", victim);
            require_success("close precreated file", chimera_posix_close(victim));
        }
        fd = which < 6 ? chimera_posix_open("/test/file", flags, 0666)
                       : chimera_posix_openat(dirfd, "file", flags, 0666);
        require_success("open file", fd);
        alias = chimera_posix_dup(fd);
        require_success("duplicate file", alias);
        expect_errno("linked regular dirfd", chimera_posix_unlinkat(fd, "child", 0), ENOTDIR);
        /* The failing model trace opens the same inode again with O_CREAT,
         * unlinks it, then closes that second open while the first survives. */
        second = chimera_posix_open("/test/file", O_CREAT | O_RDWR, 0666);
        require_success("second open", second);
        require_success("unlink file", chimera_posix_unlink("/test/file"));
        require_success("close second open", chimera_posix_close(second));
        /* Let the server's inferred handles expire while the client holds
         * the descriptor. A local type error must not depend on that cache. */
        usleep(250000);
        expect_errno("unlinked regular dirfd", chimera_posix_unlinkat(fd, "child", 0), ENOTDIR);
        expect_errno("empty relative pathname", chimera_posix_unlinkat(fd, "", 0), ENOENT);
        victim = chimera_posix_open("/test/victim", O_CREAT | O_RDWR, 0666);
        require_success("create absolute-path victim", victim);
        require_success("close victim", chimera_posix_close(victim));
        require_success("absolute path ignores regular dirfd",
                        chimera_posix_unlinkat(fd, "/test/victim", 0));
        require_success("close original", chimera_posix_close(fd));
        expect_errno("duplicate unlinked dirfd", chimera_posix_unlinkat(alias, "child", AT_REMOVEDIR), ENOTDIR);
        require_success("close duplicate", chimera_posix_close(alias));
    }
    require_success("close directory", chimera_posix_close(dirfd));
    /* Recycle a file descriptor slot as a directory, without O_DIRECTORY. */
    dirfd = chimera_posix_open("/test", O_RDONLY);
    require_success("reopen directory", dirfd);
    victim = chimera_posix_openat(dirfd, "victim", O_CREAT | O_RDWR, 0666);
    require_success("create relative-path victim", victim);
    require_success("close victim", chimera_posix_close(victim));
    require_success("valid directory unlink", chimera_posix_unlinkat(dirfd, "victim", 0));
    require_success("mkdir for rmdir", chimera_posix_mkdir("/test/dir", 0777));
    require_success("valid directory rmdir", chimera_posix_unlinkat(dirfd, "dir", AT_REMOVEDIR));
    require_success("close directory", chimera_posix_close(dirfd));
    expect_errno("invalid relative dirfd", chimera_posix_unlinkat(-1, "child", 0), EBADF);
    victim = chimera_posix_open("/test/victim", O_CREAT | O_RDWR, 0666);
    require_success("create ignored-dirfd victim", victim);
    require_success("close victim", chimera_posix_close(victim));
    require_success("absolute path ignores invalid dirfd", chimera_posix_unlinkat(-1, "/test/victim", 0));
    posix_env_teardown();
    fprintf(proto_out, "unlinkat dirfd: %d failures\n", failures);
    return failures ? 1 : 0;
} /* main */
