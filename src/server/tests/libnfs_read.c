#include "libnfs_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;
    struct nfsfh   *fh;
    int             rc;
    char            buf[16384];
    char            buf2[16384];

    memset(buf, 'x', sizeof(buf));

    libnfs_test_init(&env, argv, argc);


    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n", nfs_get_error(env.nfs
                                                                         ));
        libnfs_test_fail(&env);
    }

    rc = nfs_create(env.nfs, "/testfile", O_CREAT | O_WRONLY, 0, &fh);

    if (rc < 0) {
        fprintf(stderr, "Failed to create file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    nfs_write(env.nfs, fh, 16384, buf);

    nfs_close(env.nfs, fh);

    rc = nfs_open(env.nfs, "/testfile", O_RDONLY, &fh);

    if (rc < 0) {
        fprintf(stderr, "Failed to open file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_read(env.nfs, fh, 16384, buf2);

    if (rc < 0) {
        fprintf(stderr, "Failed to read file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = memcmp(buf, buf2, 16384);
    if (rc != 0) {
        fprintf(stderr, "Read returned bad data: rc %d\n", rc);
        libnfs_test_fail(&env);
    }

    nfs_close(env.nfs, fh);

    nfs_umount(env.nfs);

    libnfs_test_success(&env);

} /* main */