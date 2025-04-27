#include "libnfs_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;
    int             rc;
    struct nfsfh   *fh;

    libnfs_test_init(&env, argv, argc);

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_create(env.nfs, "/testfile", O_CREAT | O_WRONLY, 0, &fh);

    if (rc < 0) {
        fprintf(stderr, "Failed to create file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    nfs_close(env.nfs, fh);

    rc = nfs_rename(env.nfs, "/testfile", "/testfile2");

    if (rc < 0) {
        fprintf(stderr, "Failed to rename file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    nfs_umount(env.nfs);

    libnfs_test_success(&env);

} /* main */