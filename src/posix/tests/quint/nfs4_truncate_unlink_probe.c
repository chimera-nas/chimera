// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* A path truncate must not leave a stateless data handle in the open cache.
 * An ordinary open can reuse that handle, and must still hold the file alive
 * after its last name is removed.  This is the reduced sequence from
 * posixNfs4_step_256_0x100_1, whose read at step 216 returned ESTALE. */
#define POSIX_DRIVER_ENGINE_ONLY
#include "posix_driver.c"

int
main(
    int    argc,
    char **argv)
{
    char                    data[4096], buf[3072];
    int                     fd     = -1;
    int                     result = 1;
    int                     setup_result;
    int                     write_only = argc > 2 && strcmp(argv[2], "write") == 0;
    const char             *operation  = "setup";
    struct chimera_vfs_cred writer_cred;

    proto_out = stdout;
    /* Let the create's real OPEN drain before truncating, but keep the
     * truncate's cached handle long enough for the immediately following
     * open to reuse it. */
    setenv("CHIMERA_CLOSE_SWEEP_MIN_AGE_MS", "100", 1);
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 1);
    setup_result = posix_env_setup(argc > 1 ? argv[1] : "nfs4_memfs", NULL);
    if (setup_result) {
        return setup_result;
    }
    if (write_only) {
        /* A write-only data OPEN must not require read permission. */
        chimera_vfs_cred_init_unix(&writer_cred, 1000, 1000, 0, NULL);
        chimera_posix_set_cred(&writer_cred);
    }

    memset(data, 1, sizeof(data));
    for (int i = 0; i < 20; i++) {
        operation = "create";
        fd        = chimera_posix_open("/test/file", O_CREAT | O_WRONLY,
                                       write_only ? 0200 : 0444);
        if (fd < 0) {
            goto out;
        }
        operation = "write";
        if (chimera_posix_write(fd, data, sizeof(data)) != sizeof(data)) {
            goto out;
        }
        operation = "close created file";
        if (chimera_posix_close(fd)) {
            goto out;
        }
        fd = -1;
        usleep(250000);

        operation = "truncate";
        if (chimera_posix_truncate("/test/file", sizeof(data))) {
            goto out;
        }
        operation = "open existing file";
        fd        = chimera_posix_open("/test/file", (write_only ? O_WRONLY : O_RDONLY) |
                                       O_APPEND | O_NOFOLLOW);
        if (fd < 0) {
            goto out;
        }
        operation = "unlink";
        if (chimera_posix_unlink("/test/file")) {
            goto out;
        }
        usleep(250000);
        if (write_only) {
            operation = "write unlinked file";
            if (chimera_posix_write(fd, data, sizeof(data)) != sizeof(data)) {
                goto out;
            }
            goto close_file;
        }
        operation = "read unlinked file";
        if (chimera_posix_read(fd, buf, sizeof(buf)) != sizeof(buf)) {
            goto out;
        }
        operation = "verify data";
        if (memcmp(buf, data, sizeof(buf))) {
            errno = EIO;
            goto out;
        }
 close_file:
        operation = "close unlinked file";
        if (chimera_posix_close(fd)) {
            goto out;
        }
        fd = -1;
    }
    result = 0;

 out:
    if (result) {
        fprintf(stderr, "nfs4_truncate_unlink_probe: %s: %s\n",
                operation, strerror(errno));
    }
    if (fd >= 0) {
        chimera_posix_close(fd);
    }
    chimera_posix_set_cred(&g_root_cred);
    usleep(250000);
    posix_env_teardown();
    return result;
} /* main */
