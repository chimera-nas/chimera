// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#undef NDEBUG
#include <assert.h>
#include "server/smb/smb_internal.h"
#include "vfs/vfs_compound.h"
#include "prometheus-c.h"

static struct chimera_server_smb_shared *test_shared;
static int                               reject_finishes, submissions, finishes;
static bool                              inject;

static void
finish(
    struct chimera_vfs_compound *cp,
    void                        *arg)
{
    (void) arg;
    finishes++;
    if (reject_finishes) {
        /* All rejections precede the first accepted page. No cold handles or
         * allocator advancement may escape even after page preparation ran. */
        assert(HASH_COUNT(test_shared->durable.by_pid) == 0);
        assert(atomic_load(&test_shared->next_persistent_id) == 1);
        reject_finishes--;
        chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_EAGAIN);
    } else {
        chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    }
} /* finish */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *cp,
    chimera_vfs_compound_callback_t callback,
    void                           *arg)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn real_submit = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(real_submit);
    if (inject) {
        assert(chimera_vfs_compound_num_ops(cp) == 2);
        assert(chimera_vfs_compound_op(cp, 1)->type == CHIMERA_VFS_COMPOUND_OP_SEARCH_KEYS_AT);
        submissions++;
        chimera_vfs_compound_set_finish_handler(cp, finish, NULL);
    }
    real_submit(cp, callback, arg);
} /* chimera_vfs_compound_submit */

static void
mounted(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *arg)
{
    (void) thread;
    assert(status == CHIMERA_VFS_OK);
    *(int *) arg = 1;
} /* mounted */

static void
complete(
    struct chimera_vfs_compound *cp,
    void                        *arg)
{
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    *(int *) arg = 1;
} /* complete */

static void
wait_done(
    struct evpl *evpl,
    int         *done)
{
    while (!*done) {
        evpl_continue(evpl);
    }
    *done = 0;
} /* wait_done */

int
main(void)
{
    struct chimera_vfs_module_cfg         modules[2] = { 0 };

    strcpy(modules[0].module_name, "memfs");
    strcpy(modules[1].module_name, "memkv");
    struct prometheus_metrics            *metrics = prometheus_metrics_create(NULL, NULL, 0);
    evpl_init(NULL);
    struct evpl_thread_config            *config = evpl_thread_config_init();
    evpl_thread_config_set_wait_ms(config, 1);
    struct evpl                          *evpl = evpl_create(config);
    struct chimera_vfs                   *vfs  = chimera_vfs_init(0, 0, modules, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(vfs);
    struct chimera_vfs_thread            *vthread = chimera_vfs_thread_init(evpl, vfs);
    const struct chimera_vfs_cred        *cred    = chimera_vfs_get_server_cred();
    int                                   done    = 0;
    chimera_vfs_mkfs(vthread, cred, "memfs", "fs0", NULL, mounted, &done);
    wait_done(evpl, &done);
    chimera_vfs_mount(vthread, cred, "/mem", "memfs", "fs0", NULL, mounted, &done);
    wait_done(evpl, &done);
    uint8_t                               root[CHIMERA_VFS_FH_SIZE];
    uint32_t                              root_len;
    chimera_vfs_get_root_fh(root, &root_len);
    struct chimera_vfs_compound          *cp = chimera_vfs_compound_alloc(vthread, cred);
    chimera_vfs_compound_add_putfh(cp, root, root_len);
    chimera_vfs_compound_add_lookup(cp, "mem", 3, CHIMERA_VFS_ATTR_FH, 0);
    chimera_vfs_compound_add_getfh(cp);
    chimera_vfs_compound_submit(cp, complete, &done);
    wait_done(evpl, &done);
    const struct chimera_vfs_compound_op *fh = chimera_vfs_compound_op(cp, 2);
    root_len = fh->fh_len;
    memcpy(root, fh->fh, root_len);
    chimera_vfs_compound_free(cp);

    /* More than two recovery pages; corrupt/out-of-prefix records cannot
     * accidentally become published cold entries. */
    cp = chimera_vfs_compound_alloc(vthread, cred);
    chimera_vfs_compound_add_putfh(cp, root, root_len);
    for (uint64_t i = 1; i <= 300; i++) {
        struct chimera_smb_durable_record record = { .persistent_id = i, .name_len = 1 };
        record.name[0] = 'x';
        uint8_t                           key[CHIMERA_SMB_DURABLE_KEY_LEN], value[8192];
        uint32_t                          key_len   = chimera_smb_durable_key(key, i);
        uint32_t                          value_len = chimera_smb_durable_serialize(value, sizeof(value), &record);
        assert(value_len);
        assert(chimera_vfs_compound_add_put_key_at(cp, key, key_len, value, value_len) >= 0);
    }
    chimera_vfs_compound_add_put_key_at(cp, "smbdh-bad", 9, "bad", 3);
    chimera_vfs_compound_add_put_key_at(cp, "smbdi", 5, "bad", 3);
    chimera_vfs_compound_submit(cp, complete, &done);
    wait_done(evpl, &done);
    chimera_vfs_compound_free(cp);

    struct chimera_server_smb_shared *shared = calloc(1, sizeof(*shared));
    struct chimera_server_smb_thread *thread = calloc(1, sizeof(*thread));
    assert(shared && thread);
    thread->shared     = test_shared = shared;
    thread->vfs_thread = vthread;
    thread->evpl       = evpl;
    chimera_smb_durable_table_init(&shared->durable);
    atomic_init(&shared->next_persistent_id, 1);
    inject          = true;
    reject_finishes = 20;
    chimera_smb_durable_recover_share(thread, root, root_len);
    while (thread->maintenance_compounds) {
        evpl_continue(evpl);
    }
    assert(submissions == 1 && finishes == 9);
    assert(HASH_COUNT(shared->durable.by_pid) == 0);
    reject_finishes = 2;
    submissions     = finishes = 0;
    chimera_smb_durable_recover_share(thread, root, root_len);
    while (thread->maintenance_compounds) {
        evpl_continue(evpl);
    }
    assert(submissions == 3 && finishes == 5);
    assert(HASH_COUNT(shared->durable.by_pid) == 300);
    assert(atomic_load(&shared->next_persistent_id) == 301);
    uint64_t                          id = 1;
    struct chimera_smb_durable_entry *original, *again;
    HASH_FIND(hh, shared->durable.by_pid, &id, sizeof(id), original);
    assert(original && original->cold && original->parked);
    chimera_smb_durable_recover_share(thread, root, root_len);
    while (thread->maintenance_compounds) {
        evpl_continue(evpl);
    }
    HASH_FIND(hh, shared->durable.by_pid, &id, sizeof(id), again);
    assert(original == again && HASH_COUNT(shared->durable.by_pid) == 300);
    inject = false;
    chimera_smb_durable_table_destroy(&shared->durable);
    free(thread);
    free(shared);
    chimera_vfs_umount(vthread, cred, "/mem", mounted, &done);
    wait_done(evpl, &done);
    chimera_vfs_thread_destroy(vthread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(evpl);
    prometheus_metrics_destroy(metrics);
    return 0;
} /* main */
