// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Regression checks for metadata authorization and backing-file consistency. */
#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "vfs/vfs_pnfs.h"

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition); exit(1); \
                              } } while (0)

static uint32_t
create_truncate(
    struct mbt_env *env,
    struct mbt_fh  *root)
{
    struct CREATE3args create = { 0 };

    mbt_call_begin(env);
    create.where.dir.data.data = root->data;
    create.where.dir.data.len  = root->len;
    create.where.name.str      = "file";
    create.where.name.len      = 4;
    create.how.mode            = UNCHECKED;
    mbt_sattr3_default(&create.how.obj_attributes);
    create.how.obj_attributes.size.set_it = 1;
    create.how.obj_attributes.size.size   = 0;
    env->nfs_v3.send_call_NFSPROC3_CREATE(&env->nfs_v3.rpc2, env->evpl, env->nfs_conn, &env->cred, &create, 0, 0, NULL,
                                          0, 0, mbt_create_cb, env);
    mbt_call_wait(env);
    return env->res.status;
} /* create_truncate */

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
    int                 remote             = argc > 2 && strcmp(argv[2], "remote") == 0;

    mbt_watchdog_arm(60);
    opts.module          = argc > 1 ? argv[1] : "memfs";
    opts.pnfs_num_ds     = remote ? 2 : 0;
    opts.pnfs_ds_version = 3;
    mbt_env_start_opts(env, &opts);
    if (!remote) {
        struct chimera_vfs *vfs = chimera_server_get_vfs(env->server);
        mbt_env_fs_setup(env, "localds");
        chimera_vfs_pnfs_set_enabled(vfs, 1);
        chimera_vfs_pnfs_add_device(vfs, "tcp", "127.0.0.1.8.1", NULL, "/localds", 3, 0);
        CHECK(chimera_server_pnfs_resolve(env->server) == 0);
    }
    root = mbt_mnt(env, "/fs0")->obj_fh;
    CHECK(root.len);
    file = mbt_create(env, &root, "file", 4, UNCHECKED, 0600, NULL)->obj_fh;
    CHECK(file.len);
    memset(data, 'A', sizeof(data));
    CHECK(mbt_write(env, &file, 0, data, sizeof(data), FILE_SYNC)->status == NFS3_OK);
    r = mbt_read(env, &file, 0, sizeof(data));
    CHECK(r->status == NFS3_OK && r->data_len == sizeof(data));
    CHECK(!r->obj_attrs.has || (r->obj_attrs.a.mode == 0600 && r->obj_attrs.a.uid == 0));
    mbt_cred_set_uid(env, 1000);
    CHECK(mbt_setattr(env, &file, -1, 0, NULL)->status == NFS3ERR_ACCES);
    CHECK(mbt_setattr(env, &file, 0666, 0, NULL)->status != NFS3_OK);
    CHECK(create_truncate(env, &root) == NFS3ERR_ACCES);
    mbt_cred_set_uid(env, 0);
    r = mbt_read(env, &file, 0, sizeof(data));
    CHECK(r->status == NFS3_OK && r->data_len == sizeof(data) && memcmp(r->data, data, sizeof(data)) == 0);
    CHECK(mbt_getattr(env, &file)->obj_attrs.a.size == sizeof(data));
    CHECK(mbt_setattr(env, &file, 0666, -1, NULL)->status == NFS3_OK);
    mbt_cred_set_uid(env, 1000);
    memset(data, 'B', sizeof(data));
    CHECK(mbt_write(env, &file, 0, data, sizeof(data), FILE_SYNC)->status == NFS3_OK);
    mbt_cred_set_uid(env, 0);
    r = mbt_read(env, &file, 0, sizeof(data));
    CHECK(r->status == NFS3_OK && r->data_len == sizeof(data) && memcmp(r->data, data, sizeof(data)) == 0);

    CHECK(create_truncate(env, &root) == NFS3_OK);
    r = mbt_read(env, &file, 0, sizeof(data));
    CHECK(r->status == NFS3_OK && r->data_len == 0);
    CHECK(mbt_getattr(env, &file)->obj_attrs.a.size == 0);

    if (!remote) {
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
    }
    mbt_env_stop(env);
    free(env);
    puts("pNFS authorization, truncate, write and READ attributes: PASS");
    return 0;
} /* main */
