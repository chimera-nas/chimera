// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"

/* Runs on the owning server loop, while finish acceptance is deliberately held. */
void
smb_cache_close_inspect(void *private_data, unsigned int expected_members)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_open_file *open = command->open;
    assert(open);
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    assert(!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED));
    struct chimera_smb_open_file *found;
    HASH_FIND(hh, open->tree->open_files[bucket], &open->file_id, sizeof(open->file_id), found);
    assert(found == open && open->grant && open->handle);
    assert(!chimera_vfs_claim_access_owner_is_retired(open->access_owner));
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        assert(open->base_access_owner &&
            !chimera_vfs_claim_access_owner_is_retired(open->base_access_owner));
    }
    struct chimera_vfs_claim_grant *grant = open->grant;
    pthread_mutex_lock(&grant->file->lock);
    unsigned int count = 0;
    bool found_member = false;
    for (struct chimera_smb_open_file *member = grant->members; member;
         member = member->grant_member_next) {
        count++;
        if (member == open) found_member = true;
    }
    assert(grant->claim.used && found_member && count == expected_members);
    if (count == 1) assert(grant->claim.op_handle == open->handle);
    assert(grant->claim.file == grant->file);
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        assert(grant->claim.construct == CHIMERA_CONSTRUCT_DIR_LEASE);
        assert(grant->is_v2 && open->oplock_level == SMB2_OPLOCK_LEVEL_LEASE);
        assert(!(grant->claim.used & CHIMERA_CLAIM_CW));
    }
    pthread_mutex_unlock(&grant->file->lock);
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        struct chimera_vfs_file_state *base = open->base_share_file_state;
        assert(base && base != grant->file && open->base_handle);
        pthread_mutex_lock(&base->lock);
        assert(base->stream_holders >= expected_members);
        assert(chimera_smb_base_share_claim(open)->op_handle == open->base_handle);
        assert(chimera_smb_base_share_claim(open)->file == base);
        pthread_mutex_unlock(&base->lock);
    }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
}

static _Thread_local struct chimera_server_smb_shared *parked_shared;
static _Thread_local struct chimera_smb_tree *parked_tree;
static _Thread_local uint64_t parked_pid;
static _Thread_local unsigned int pins_before_park;

/* Reproduce the bucket/registry ownership transition of session disconnect
 * while finish is held, without adding a reference which could hide a leak. */
void
smb_durable_close_inspect(void *private_data, bool park, bool persistent)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_open_file *open = command->open;
    struct chimera_server_smb_shared *shared = command->request->compound->thread->shared;
    struct chimera_smb_tree *tree = open->tree;
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    struct chimera_smb_durable_entry *entry;
    assert(open->durable_flags);
    assert(!!(open->durable_flags & CHIMERA_SMB_DURABLE_PERSISTENT) == persistent);
    assert(!!(open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) == persistent);
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    assert(!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open->handle);
    struct chimera_smb_open_file *live;
    HASH_FIND(hh, tree->open_files[bucket], &open->file_id, sizeof(open->file_id), live);
    assert(live == open && !chimera_vfs_claim_access_owner_is_retired(open->access_owner));
    pthread_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &open->file_id.pid, sizeof(open->file_id.pid), entry);
    assert(entry && entry->open_file == open && !entry->parked && !!entry->persistent == persistent);
    pthread_mutex_unlock(&shared->durable.lock);
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        struct chimera_vfs_file_state *base = open->base_share_file_state;
        assert(base && open->base_handle && open->base_access_owner);
        assert(!chimera_vfs_claim_access_owner_is_retired(open->base_access_owner));
        /* Specialized cases deliberately deny base DELETE sharing. The
         * canonical public blocker must survive a rejected/held CLOSE finish,
         * even though this compound privately sees both owners retired. */
        if (!(open->share_access & SMB2_FILE_SHARE_DELETE)) {
            struct chimera_vfs_claim probe;
            struct chimera_claim_owner owner = {
                .proto = CHIMERA_CLAIM_PROTO_SMB2,
                .client_key = UINT64_MAX, .owner_lo = UINT64_MAX, .owner_hi = UINT64_MAX,
            };
            struct chimera_vfs_claim_conflict conflict = { 0 };
            chimera_vfs_claim_init_smb_open(&probe, CHIMERA_CLAIM_D, 0, &owner);
            assert(chimera_vfs_claim_test(base, &probe, &conflict) == CHIMERA_CLAIM_DENIED);
        }
    }
    if (park) {
        assert(!parked_shared && atomic_load(&open->refcnt) >= 2);
        parked_shared = shared; parked_tree = tree; parked_pid = open->file_id.pid;
        pins_before_park = tree->compound_pins;
        unsigned int refs = atomic_load(&open->refcnt);
        HASH_DELETE(hh, tree->open_files[bucket], open);
        open->flags |= CHIMERA_SMB_OPEN_FILE_PARKED;
        open->create_conn = NULL;
        chimera_smb_durable_park(shared, open);
        assert(atomic_load(&open->refcnt) == refs && open->durable_tree_pin == tree);
        pthread_mutex_lock(&shared->durable.lock);
        HASH_FIND(hh, shared->durable.by_pid, &parked_pid, sizeof(parked_pid), entry);
        assert(entry && entry->open_file == open && entry->parked);
        pthread_mutex_unlock(&shared->durable.lock);
    }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
}

void
smb_durable_close_accepted(void)
{
    struct chimera_smb_durable_entry *entry;
    assert(parked_shared && parked_tree);
    pthread_mutex_lock(&parked_shared->durable.lock);
    HASH_FIND(hh, parked_shared->durable.by_pid, &parked_pid, sizeof(parked_pid), entry);
    assert(!entry);
    pthread_mutex_unlock(&parked_shared->durable.lock);
    /* This packet has no suffix. The batch pin may have drained too, but the
     * extra durable pin MUST have drained with the unique parked owner. */
    assert(parked_tree->compound_pins <= pins_before_park);
    parked_shared = NULL; parked_tree = NULL;
}

struct persisted_record_check {
    bool present;
    void (*done)(void *);
    void *private_data;
};

static void
persisted_record_checked(struct chimera_vfs_compound *compound, void *private_data)
{
    struct persisted_record_check *ctx = private_data;
    assert(chimera_vfs_compound_status(compound) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(compound, 1)->kv_num_entries == (ctx->present ? 1 : 0));
    void (*done)(void *) = ctx->done;
    void *arg = ctx->private_data;
    chimera_vfs_compound_free(compound);
    free(ctx);
    done(arg);
}

/* Read the actual backend recovery key while CLOSE acceptance is held. The
 * injected delete-failure case must leave a record under the current best-effort
 * policy; remove it only as fixture cleanup after the read has captured it. */
void
smb_persisted_close_record_check(void *private_data, bool present,
                                void (*done)(void *), void *arg)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_open_file *open = command->open;
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        command->request->compound->thread->vfs_thread, chimera_vfs_get_server_cred());
    uint8_t key[CHIMERA_SMB_DURABLE_KEY_LEN];
    uint32_t key_len = chimera_smb_durable_key(key, open->file_id.pid);
    chimera_vfs_compound_add_putfh(compound, open->handle->fh, open->handle->fh_len);
    chimera_vfs_compound_add_search_keys_at(compound, key, key_len, key, key_len,
        0, 1, 65536);
    if (present) { chimera_vfs_compound_add_delete_key_at(compound, key, key_len); }
    struct persisted_record_check *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->present = present; ctx->done = done; ctx->private_data = arg;
    /* This read-only verification/fixture cleanup is not a wire command group. */
    typedef void (*submit_fn)(struct chimera_vfs_compound *,
                             chimera_vfs_compound_callback_t, void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    next(compound, persisted_record_checked, ctx);
}
