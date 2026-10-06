// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"
#include "vfs/vfs_claim_access.h"

/* Runs on the server loop before finish acceptance, including a retry. */
void
smb_create_cache_inspect(
    void        *private_data,
    unsigned int expected_members,
    unsigned int expected_epoch,
    unsigned int expected_state)
{
    struct smb_vfs_command        *command = private_data;
    struct chimera_smb_open_file  *open = command->open, *found;

    assert(open && command->status == SMB2_STATUS_SUCCESS);
    assert(!open->grant && !open->caching_file_state && !open->handle);
    assert(!open->access_owner && !open->share_lease_inserted);
    assert(!command->state->published && !command->request->create.r_open_file);
    struct chimera_vfs_file_state *file =
        chimera_vfs_claim_access_owner_claim(command->state->access_owner)->file;
    assert(file);
    evpl_mutex_lock(&file->lock);
    unsigned int                   matching = 0;
    for (struct chimera_vfs_claim_grant *g = file->grants; g; g = g->grant_next) {
        unsigned int members = 0;
        for (struct chimera_smb_open_file *m = g->members; m; m = m->grant_member_next) {
            assert(m != open);
            members++;
        }
        if (expected_members && !memcmp(g->claim.owner.key, open->lease_key, 16)) {
            matching++;
            assert(g->claim.construct == ((open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) ?
                                          CHIMERA_CONSTRUCT_DIR_LEASE : CHIMERA_CONSTRUCT_RQLS));
            assert(members == expected_members && g->epoch == expected_epoch);
            assert(chimera_smb_vfs_to_lease_bits(g->claim.used) == expected_state);
        }
    }
    assert(!expected_members || matching == 1);
    evpl_mutex_unlock(&file->lock);
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    HASH_FIND(hh, open->tree->open_files[bucket], &open->file_id, sizeof(open->file_id), found);
    assert(!found);
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
} /* smb_create_cache_inspect */

/* Inspect actual recovery records at the final legacy CREATE reply boundary.
 * This keeps wire assertions honest: a DH2Q response alone is not persistence. */
#include <dlfcn.h>
#include <stdatomic.h>
#include "server/smb/smb_procs.h"
#include "vfs/vfs_internal_procs.h"
static atomic_int persistent_expect_armed, persistent_expected, persistent_fail_put;
static atomic_int persistent_checks;

void
smb_persistent_create_expect(
    bool persistent,
    int  fail_put)
{
    assert(!atomic_load(&persistent_expect_armed));
    atomic_store(&persistent_expected, persistent);
    atomic_store(&persistent_fail_put, fail_put);
    atomic_store(&persistent_expect_armed, 1);
} /* smb_persistent_create_expect */

unsigned int
smb_persistent_create_checks(void)
{
    assert(!atomic_load(&persistent_expect_armed) && !atomic_load(&persistent_fail_put));
    return atomic_load(&persistent_checks);
} /* smb_persistent_create_checks */

struct ambiguous_put {
    chimera_vfs_put_key_callback_t callback;
    void                          *private_data;
};
static void
ambiguous_put_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct ambiguous_put          *ctx = private_data;

    assert(status == CHIMERA_VFS_OK); /* The record really reached storage. */
    chimera_vfs_put_key_callback_t callback = ctx->callback;
    void                          *arg      = ctx->private_data;
    free(ctx);
    callback(CHIMERA_VFS_EIO, arg);
} /* ambiguous_put_done */

__attribute__((visibility("default"))) void
chimera_vfs_put_key_at(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fh_len,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data)
{
    typedef void (*put_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *,
        const void *,
        int,
        const void *,
        uint32_t,
        const void *,
        uint32_t,
        chimera_vfs_put_key_callback_t,
        void *);
    put_fn next = (put_fn) dlsym(RTLD_NEXT, "chimera_vfs_put_key_at");
    assert(next);
    int    fail = key_len == CHIMERA_SMB_DURABLE_KEY_LEN &&
        !memcmp(key, CHIMERA_SMB_DURABLE_KEY_PREFIX, CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN) ?
        atomic_exchange(&persistent_fail_put, 0) : 0;
    if (fail == 1) {
        callback(CHIMERA_VFS_EIO, private_data); return;
    }
    if (fail == 2) {
        struct ambiguous_put *ctx = calloc(1, sizeof(*ctx));
        assert(ctx); ctx->callback = callback; ctx->private_data = private_data;
        next(thread, cred, fh, fh_len, key, key_len, value, value_len, ambiguous_put_done, ctx);
        return;
    }
    next(thread, cred, fh, fh_len, key, key_len, value, value_len, callback, private_data);
} /* chimera_vfs_put_key_at */

struct persistent_create_check {
    struct chimera_smb_request *request;
    unsigned int                status;
    bool                        persistent;
};

static void
persistent_create_record_checked(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct persistent_create_check       *ctx = private_data;

    assert(chimera_vfs_compound_status(compound) == CHIMERA_VFS_OK);
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, 1);
    assert(op->kv_num_entries == (ctx->persistent ? 1 : 0));
    if (ctx->persistent) {
        struct chimera_smb_durable_record record;
        assert(chimera_smb_durable_deserialize(op->kv_entries[0].value,
                                               op->kv_entries[0].value_len, &record) == 0);
        assert(record.persistent_id == ctx->request->create.r_open_file->file_id.pid);
    }
    chimera_vfs_compound_free(compound);
    atomic_fetch_add(&persistent_checks, 1);
    typedef void (*complete_fn)(
        struct chimera_smb_request *,
        unsigned int);
    complete_fn                 next = (complete_fn) dlsym(RTLD_NEXT, "chimera_smb_complete_request");
    assert(next);
    struct chimera_smb_request *request = ctx->request;
    unsigned int                status  = ctx->status;
    free(ctx);
    next(request, status);
} /* persistent_create_record_checked */

__attribute__((visibility("default"))) void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status)
{
    typedef void (*complete_fn)(
        struct chimera_smb_request *,
        unsigned int);
    complete_fn next = (complete_fn) dlsym(RTLD_NEXT, "chimera_smb_complete_request");
    assert(next);
    if (request->smb2_hdr.command != SMB2_CREATE || status == SMB2_STATUS_PENDING ||
        !atomic_load(&persistent_expect_armed) ||
        (status != SMB2_STATUS_SUCCESS && request->create.persist_fh_len)) {
        /* Let the real terminal-failure hook delete an unpublished record;
         * its final continuation re-enters here with the proof cleared. */
        next(request, status); return;
    }
    assert(atomic_exchange(&persistent_expect_armed, 0));
    bool                          persistent = atomic_load(&persistent_expected);
    struct chimera_smb_open_file *open       = request->create.r_open_file;
    uint64_t                      pid        = request->create.persist_pid;
    if (status == SMB2_STATUS_SUCCESS) {
        assert(open);
        pid = open->file_id.pid;
        assert(!!(open->durable_flags & CHIMERA_SMB_DURABLE_PERSISTENT) == persistent);
        assert(!!(open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) == persistent);
        assert(request->create.persist_record_written == persistent);
        struct chimera_smb_durable_entry *entry;
        evpl_mutex_lock(&request->compound->thread->shared->durable.lock);
        HASH_FIND(hh, request->compound->thread->shared->durable.by_pid,
                  &pid, sizeof(pid), entry);
        assert(!!entry == !!open->durable_flags);
        if (entry) {
            assert(entry->open_file == open && entry->persistent == persistent);
        }
        evpl_mutex_unlock(&request->compound->thread->shared->durable.lock);
    } else {
        assert(!persistent && !open && pid);
    }
    uint8_t                         key[CHIMERA_SMB_DURABLE_KEY_LEN];
    uint32_t                        key_len  = chimera_smb_durable_key(key, pid);
    struct chimera_vfs_compound    *compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread, chimera_vfs_get_server_cred());
    struct persistent_create_check *ctx = calloc(1, sizeof(*ctx));
    assert(compound && ctx);
    ctx->request = request; ctx->status = status; ctx->persistent = persistent;
    chimera_vfs_compound_add_putfh(compound, request->tree->fh, request->tree->fh_len);
    chimera_vfs_compound_add_search_keys_at(compound, key, key_len, key, key_len, 0, 1, 65536);
    chimera_vfs_compound_submit(compound, persistent_create_record_checked, ctx);
} /* chimera_smb_complete_request */

/* Constructor contention observed on the real server worker. The wire test
 * waits for this event before accepting its held first CREATE. */
static atomic_uint lease_key_pending;
unsigned int smb_lease_key_pending_count(void) { return atomic_load(&lease_key_pending); }
__attribute__((visibility("default"))) uint32_t
chimera_smb_lease_key_begin(struct chimera_smb_request *request)
{
    typedef uint32_t (*fn)(
        struct chimera_smb_request *);
    fn       next = (fn) dlsym(RTLD_NEXT, "chimera_smb_lease_key_begin");
    assert(next);
    uint32_t status = next(request);
    if (status == SMB2_STATUS_PENDING) {
        atomic_fetch_add(&lease_key_pending, 1);
    }
    return status;
} /* chimera_smb_lease_key_begin */

/* Remove an exact leaf after its successful name preflight, before SMB sees
 * that result. This models a competing frontend independently of SMB gates. */
static char       lease_unlink_name[96];
static atomic_int lease_unlink_armed, lease_unlink_hits;
void
smb_lease_unlink_after_lookup(const char *name)
{
    assert(!atomic_load(&lease_unlink_armed));
    assert(strlen(name) < sizeof(lease_unlink_name));
    strcpy(lease_unlink_name, name);
    atomic_store(&lease_unlink_hits, 0);
    atomic_store(&lease_unlink_armed, 1);
} /* smb_lease_unlink_after_lookup */
unsigned int smb_lease_unlink_count(void) { return atomic_load(&lease_unlink_hits); }
struct lease_unlink_ctx {
    struct chimera_vfs_thread       *thread;
    struct chimera_vfs_open_handle  *parent;
    chimera_vfs_lookup_at_callback_t callback;
    void                            *private_data;
    struct chimera_vfs_attrs         attr, dir_attr;
    uint32_t                         name_len;
    char                             name[96];
};
static void
lease_unlink_done(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *private_data)
{
    struct lease_unlink_ctx *ctx = private_data;

    (void) pre; (void) post;
    assert(status == CHIMERA_VFS_OK);
    atomic_fetch_add(&lease_unlink_hits, 1);
    ctx->callback(CHIMERA_VFS_OK, &ctx->attr, &ctx->dir_attr, ctx->private_data);
    free(ctx);
} /* lease_unlink_done */
static void
lease_unlink_lookup_done(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct lease_unlink_ctx *ctx = private_data;

    assert(status == CHIMERA_VFS_OK && attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH));
    ctx->attr = *attr;
    if (dir_attr) {
        ctx->dir_attr = *dir_attr;
    }
    chimera_vfs_remove_at_match_fh_flags(ctx->thread, chimera_vfs_get_server_cred(), ctx->parent,
                                         ctx->name, ctx->name_len, attr->va_fh, attr->va_fh_len,
                                         CHIMERA_VFS_REMOVE_NO_NOTIFY, 0, 0, NULL, NULL, NULL, lease_unlink_done, ctx);
} /* lease_unlink_lookup_done */
__attribute__((visibility("default"))) void
chimera_vfs_lookup_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         name_len,
    uint64_t                         mask,
    uint64_t                         dir_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data)
{
    typedef void (*fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *,
        struct chimera_vfs_open_handle *,
        const char *,
        uint32_t,
        uint64_t,
        uint64_t,
        chimera_vfs_lookup_at_callback_t,
        void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_lookup_at");
    assert(next);
    if (atomic_load(&lease_unlink_armed) && strlen(lease_unlink_name) == name_len &&
        !memcmp(name, lease_unlink_name, name_len) && atomic_exchange(&lease_unlink_armed, 0)) {
        struct lease_unlink_ctx *ctx = calloc(1, sizeof(*ctx)); assert(ctx);
        ctx->thread   = thread; ctx->parent = handle; ctx->callback = callback; ctx->private_data = private_data;
        ctx->name_len = name_len; memcpy(ctx->name, name, name_len);
        next(thread, cred, handle, name, name_len, mask, dir_mask, lease_unlink_lookup_done, ctx);
        return;
    }
    next(thread, cred, handle, name, name_len, mask, dir_mask, callback, private_data);
} /* chimera_vfs_lookup_at */
