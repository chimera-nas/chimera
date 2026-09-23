// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Fault injection: failed backing-file truncate must preserve MDS state.
* Ordinary authorization and I/O are covered by the NFS3 model corpus. */
#include "nfs3_mbt_common.h"
#include "vfs/vfs_pnfs.h"

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition); exit(1); \
                              } } while (0)

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env     *env  = calloc(1, sizeof(*env));
    struct mbt_env_opts opts = { 0 };
    struct mbt_fh       root, file, dsroot, backing;
    struct mbt_result  *r;
    unsigned char       data[4096];
    char                name[MBT_NAME_MAX] = { 0 };

    alarm(60);
    opts.module          = argc > 1 ? argv[1] : "memfs";
    opts.pnfs_ds_version = 3;
    mbt_env_start_opts(env, &opts);
    {
        struct chimera_vfs *vfs = chimera_server_get_vfs(env->server);
        mbt_env_fs_setup(env, "localds");
        chimera_vfs_pnfs_set_enabled(vfs, 1);
        chimera_vfs_pnfs_add_device(vfs, "tcp", "127.0.0.1.8.1", NULL, "/localds", 3, 0);
        CHECK(chimera_server_pnfs_resolve(env->server) == 0);
    }
    root = mbt_mnt(env, "/fs0")->obj_fh;
    CHECK(root.len);
    file = mbt_create(env, &root, "file", 4, UNCHECKED, 0666, NULL)->obj_fh;
    CHECK(file.len);
    memset(data, 'A', sizeof(data));
    CHECK(mbt_write(env, &file, 0, data, sizeof(data), FILE_SYNC)->status == NFS3_OK);
    dsroot = mbt_mnt(env, "/localds")->obj_fh;
    r      = mbt_readdir(env, &dsroot);
    for (int i = 0; i < r->nentries; i++) {
        if (r->entries[i].name[0] != '.') {
            memcpy(name, r->entries[i].name, r->entries[i].name_len);
            break;
        }
    }
    CHECK(name[0]);
    backing = mbt_lookup(env, &dsroot, name, strlen(name))->obj_fh;
    CHECK(mbt_setattr(env, &backing, 0444, -1, NULL)->status == NFS3_OK);
    mbt_cred_set_uid(env, 1000);
    CHECK(mbt_setattr(env, &file, -1, 0, NULL)->status != NFS3_OK);
    mbt_cred_set_uid(env, 0);
    CHECK(mbt_getattr(env, &file)->obj_attrs.a.size == sizeof(data));
    r = mbt_read(env, &file, 0, sizeof(data));
    CHECK(r->status == NFS3_OK && r->data_len == sizeof(data) && memcmp(r->data, data, sizeof(data)) == 0);
    mbt_env_stop(env);
    free(env);
    puts("pNFS backing truncate failure preserves metadata and data: PASS");
    return 0;
} /* main */
