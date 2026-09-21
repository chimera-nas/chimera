// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

struct visit_ctx {
    struct chimera_server *server;
    int                    seen;
};

static int
stop_after_one(
    const char *path,
    const char *module,
    const char *module_path,
    const char *options,
    void       *data)
{
    int *calls = data;

    (void) path; (void) module; (void) module_path; (void) options;
    (*calls)++;
    return 1;
} /* stop_after_one */

static int
visit_mount(
    const char *path,
    const char *module,
    const char *module_path,
    const char *options,
    void       *data)
{
    struct visit_ctx *ctx   = data;
    int               calls = 0;

    (void) module_path;
    if (strcmp(path, "ds0") == 0 || strcmp(path, "ds1") == 0) {
        if (strcmp(module, "nfs") != 0 || !options || !strstr(options, "vers=4")) {
            fprintf(stderr, "invalid backing mount metadata for %s\n", path);
            exit(1);
        }
        /* Reentry must not hold the mount-table lock, and a nonzero callback
         * return must stop delivery even though the snapshot has more rows. */
        chimera_server_iterate_mounts(ctx->server, stop_after_one, &calls);
        if (calls != 1) {
            fprintf(stderr, "mount enumeration did not stop after one callback\n");
            exit(1);
        }
        ctx->seen++;
    }
    return 0;
} /* visit_mount */

/* Resolve two remote backing roots from an ordinary application thread,
 * before it has a VFS context or an RCU reader registration.  The full pNFS
 * corpus uses this same topology, but startup must work without a corpus. */
int
main(void)
{
    struct mbt_env      env;
    struct mbt_env_opts opts = { 0 };
    struct visit_ctx    ctx  = { 0 };

    opts.pnfs_num_ds = 2;
    mbt_watchdog_arm(60);
    mbt_env_open_opts(&env, &opts);
    if (chimera_server_mount_in_use(env.ds_server[0], "/ds_data") != 1 ||
        chimera_server_mount_in_use(env.ds_server[0], "/missing") != 0) {
        fprintf(stderr, "incorrect mount-in-use result for a data-server export\n");
        exit(1);
    }
    ctx.server = env.server;
    chimera_server_iterate_mounts(env.server, visit_mount, &ctx);
    mbt_env_stop(&env);
    mbt_watchdog_disarm();
    if (ctx.seen != 2) {
        fprintf(stderr, "expected two backing mounts, found %d\n", ctx.seen);
        return 1;
    }
    printf("pNFS startup, mount enumeration and shutdown passed\n");
    return 0;
} /* main */
