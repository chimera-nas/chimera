// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the real HTTP endpoint, observe its effects over NFS, and veto
 * mutations before dispatch on rejected attempts. This tests frontend replay;
 * it does not simulate backend rollback after a mutation. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#include <jansson.h>
#undef NDEBUG
#include <assert.h>
#include "nfs3_mbt_common.h"
#include "ctl_http.h"
#include "vfs/vfs_compound.h"
#include "common/compound_retry.h"

static atomic_int armed, submissions, attempts, mutations;
static int        reject_count, operation_error;

struct rejection {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    seen;
};

static void
prepare_mutation(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct rejection *ctx = private_data;

    if (ctx->seen < reject_count || operation_error) {
        *status = CHIMERA_VFS_EAGAIN;
    } else {
        atomic_fetch_add(&mutations, 1);
    }
} /* prepare_mutation */

static void
finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rejection      *ctx    = private_data;
    enum chimera_vfs_error status = ctx->seen++ < reject_count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK;

    atomic_store(&attempts, ctx->seen);
    chimera_vfs_compound_finish_result(compound, status);
} /* finish */

static void
complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rejection               *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN ||
        ctx->seen == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1) {
        free(ctx);
    }
    callback(compound, arg);
} /* complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn         next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (!atomic_load(&armed)) {
        next(compound, callback, private_data);
        return;
    }
    assert(atomic_fetch_add(&submissions, 1) == 0);
    struct rejection *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->callback     = callback;
    ctx->private_data = private_data;
    unsigned int      found = 0;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        switch (chimera_vfs_compound_op(compound, i)->type) {
            case CHIMERA_VFS_COMPOUND_OP_REMOVE_PATH:
            case CHIMERA_VFS_COMPOUND_OP_RENAME_PATH:
            case CHIMERA_VFS_COMPOUND_OP_LINK_PATH:
            case CHIMERA_VFS_COMPOUND_OP_SETATTR:
                chimera_vfs_compound_set_op_callbacks(compound, i, prepare_mutation, NULL, ctx);
                found++;
                break;
            default:
                break;
        } /* switch */
    }
    assert(found == 1);
    chimera_vfs_compound_set_finish_handler(compound, finish, ctx);
    next(compound, complete, ctx);
} /* chimera_vfs_compound_submit */

static void
post(
    struct ctl_conn *api,
    const char      *body,
    int              rejects,
    int              op_error,
    int              want_http,
    int              want_error,
    int              want_attempts,
    int              want_mutations)
{
    struct ctl_res res;

    reject_count    = rejects;
    operation_error = op_error;
    atomic_store(&submissions, 0);
    atomic_store(&attempts, 0);
    atomic_store(&mutations, 0);
    atomic_store(&armed, 1);
    ctl_post(api, "/api/v1/debug/fsop", body, &res);
    atomic_store(&armed, 0);
    fprintf(stderr, "%s => HTTP %d, attempts %d, mutations %d: %s\n",
            body, res.status, atomic_load(&attempts), atomic_load(&mutations), res.body);
    assert(res.status == want_http);
    assert(atomic_load(&submissions) == (want_attempts != 0));
    assert(atomic_load(&attempts) == want_attempts);
    assert(atomic_load(&mutations) == want_mutations);
    json_t *root = json_loadb(res.body, res.body_len, 0, NULL);
    assert(root);
    if (want_http == 200) {
        assert(!strcmp(json_string_value(json_object_get(root, "status")), "ok"));
    } else if (want_http == 500) {
        json_t *error = json_object_get(root, "vfs_error");
        assert(json_is_integer(error) && json_integer_value(error) == want_error);
    }
    json_decref(root);
} /* post */

static struct mbt_fh
lookup(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name)
{
    struct mbt_result *r = mbt_lookup(env, dir, name, strlen(name));

    assert(!r->rpc_err && r->status == NFS3_OK);
    return r->obj_fh;
} /* lookup */

int
main(void)
{
    struct mbt_env      env;
    struct mbt_env_opts opts = { .rest_port = 8080, .rest_debug_fsops = 1 };
    struct mbt_result  *r;
    struct mbt_fh       root, file, alias;

    mbt_env_start_opts(&env, &opts);
    r = mbt_mnt(&env, "/fs0");
    assert(!r->rpc_err && r->status == MNT3_OK);
    root = r->obj_fh;
    r    = mbt_create(&env, &root, "file", 4, UNCHECKED, 0644, NULL);
    assert(!r->rpc_err && r->status == NFS3_OK);
    file = r->obj_fh;
    r    = mbt_symlink(&env, &root, "sym", 3, "file", 0777);
    assert(!r->rpc_err && r->status == NFS3_OK);
    struct evpl_http_agent *agent = evpl_http_init(env.evpl);
    struct ctl_conn        *api   = ctl_conn_open(env.evpl, agent, 8080);

    /* chmod follows the final symlink and keeps all its steps in one compound. */
    post(api, "{\"op\":\"chmod\",\"path\":\"/fs0/sym\",\"mode\":384}", 2, 0, 200, 0, 3, 1);
    r = mbt_getattr(&env, &file);
    assert(!r->rpc_err && r->status == NFS3_OK && (r->obj_attrs.a.mode & 0777) == 0600);

    post(api, "{\"op\":\"link\",\"path\":\"/fs0/file\",\"path2\":\"/fs0/hard\"}", 2, 0, 200, 0, 3, 1);
    alias = lookup(&env, &root, "hard");
    assert(alias.len == file.len && !memcmp(alias.data, file.data, file.len));
    post(api, "{\"op\":\"rename\",\"path\":\"/fs0/hard\",\"path2\":\"/fs0/renamed\"}", 2, 0, 200, 0, 3, 1);
    r = mbt_lookup(&env, &root, "hard", 4);
    assert(!r->rpc_err && r->status == NFS3ERR_NOENT);
    alias = lookup(&env, &root, "renamed");
    assert(alias.len == file.len && !memcmp(alias.data, file.data, file.len));
    post(api, "{\"op\":\"unlink\",\"path\":\"/fs0/renamed\"}", 2, 0, 200, 0, 3, 1);
    r = mbt_lookup(&env, &root, "renamed", 7);
    assert(!r->rpc_err && r->status == NFS3ERR_NOENT);

    /* Link preserves the symlink inode; unlink never removes its target. */
    post(api, "{\"op\":\"link\",\"path\":\"/fs0/sym\",\"path2\":\"/fs0/sym2\"}", 0, 0, 200, 0, 1, 1);
    alias = lookup(&env, &root, "sym2");
    r     = mbt_getattr(&env, &alias);
    assert(!r->rpc_err && r->status == NFS3_OK && r->obj_attrs.a.type == NF3LNK);
    post(api, "{\"op\":\"unlink\",\"path\":\"/fs0/sym2\"}", 0, 0, 200, 0, 1, 1);
    (void) lookup(&env, &root, "file");

    /* Exhausted finish rejection and operation EAGAIN must leave the file. */
    post(api, "{\"op\":\"unlink\",\"path\":\"/fs0/file\"}", 9, 0, 500, CHIMERA_VFS_EAGAIN, 9, 0);
    (void) lookup(&env, &root, "file");
    post(api, "{\"op\":\"unlink\",\"path\":\"/fs0/file\"}", 0, 1, 500, CHIMERA_VFS_EAGAIN, 1, 0);
    (void) lookup(&env, &root, "file");
    post(api, "{\"op\":\"chmod\",\"path\":\"/fs0/missing\",\"mode\":511}", 0, 0, 500, CHIMERA_VFS_ENOENT, 1, 0);
    post(api, "{\"op\":\"unlink\",\"path\":\"/fs0/missing\"}", 0, 0, 500, CHIMERA_VFS_ENOENT, 1, 1);

    /* Rejected JSON never dispatches a partial compound. */
    const char *invalid[] = {
        "{",                                           "{}",
        "{\"op\":\"unknown\",\"path\":\"/fs0/file\"}",
        "{\"op\":\"rename\",\"path\":\"/fs0/file\"}",
        "{\"op\":\"link\",\"path\":\"/fs0/file\"}",
        "{\"op\":\"chmod\",\"path\":\"/fs0/file\",\"mode\":\"bad\"}"
    };
    for (unsigned int i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        post(api, invalid[i], 0, 0, 400, 0, 0, 0);
    }

    ctl_conn_close(api);
    evpl_http_destroy(agent);
    mbt_env_fs_teardown(&env, "fs0");
    mbt_env_stop(&env);
    return 0;
} /* main */
