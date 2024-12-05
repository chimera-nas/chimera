#include "libnfs_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct test_env   env;
    struct nfsdir    *dir;
    struct nfsdirent *entry;

    libnfs_test_init(&env, argv, argc);

    if (nfs_mount(env.nfs, "127.0.0.1", "/") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    if (nfs_opendir(env.nfs, "/", &dir) < 0) {
        fprintf(stderr, "Failed to open root directory: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    while ((entry = nfs_readdir(env.nfs, dir)) != NULL) {
        printf(" - %s\n", entry->name);
    }

    nfs_closedir(env.nfs, dir);
    nfs_umount(env.nfs);

    libnfs_test_success(&env);

} /* main */