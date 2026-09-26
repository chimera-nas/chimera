// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include "server/smb/smb_internal.h"

static _Thread_local struct chimera_smb_open_file *saved_open;
static _Thread_local struct chimera_server_smb_shared *saved_shared;
static _Thread_local uint8_t old_fh[CHIMERA_VFS_FH_SIZE], new_fh[CHIMERA_VFS_FH_SIZE];
static _Thread_local uint32_t old_len, new_len;

static void count_peer(void *context, const struct chimera_smb_namespace_snapshot *snapshot,
    void *private_data)
{
    unsigned *count = private_data;
    (void) snapshot;
    assert(context == saved_open);
    ++*count;
}

static unsigned peers(const uint8_t *fh, uint32_t length)
{
    typedef void (*fn)(struct chimera_smb_namespace_registry *, const uint8_t *, uint32_t,
        void (*)(void *, const struct chimera_smb_namespace_snapshot *, void *), void *);
    fn each = (fn) dlsym(RTLD_DEFAULT, "chimera_smb_namespace_foreach_locked");
    assert(each);
    unsigned count = 0;
    pthread_mutex_lock(&saved_shared->namespace_registry.lock);
    each(&saved_shared->namespace_registry, fh, length, count_peer, &count);
    pthread_mutex_unlock(&saved_shared->namespace_registry.lock);
    return count;
}

void reparse_test_before(struct chimera_vfs_compound *compound, bool ready)
{
    const struct chimera_vfs_compound_op *first = chimera_vfs_compound_op(compound, 0);
    struct chimera_smb_request *request;
    /* The SET context's first member is its request pointer. */
    memcpy(&request, first->callback_private, sizeof(request));
    saved_open = request->ioctl.rp_open_file;
    saved_shared = request->compound->thread->shared;
    assert(saved_open && saved_open->identity_rebind);
    assert(saved_open->refcnt == 2);
    assert(saved_open->handle && saved_open->access_owner && saved_open->share_file_state);
    old_len = saved_open->handle->fh_len;
    memcpy(old_fh, saved_open->handle->fh, old_len);
    assert(chimera_smb_share_claim(saved_open)->op_handle == saved_open->handle);
    assert(chimera_smb_share_claim(saved_open)->file == saved_open->share_file_state);
    assert(!chimera_vfs_claim_access_owner_is_retired(saved_open->access_owner));
    assert(peers(old_fh, old_len) == 1);
    new_len = 0;
    if (ready) {
        const struct chimera_vfs_compound_op *handle = chimera_vfs_compound_op(compound, 5);
        const struct chimera_vfs_compound_op *reserve = chimera_vfs_compound_op(compound, 7);
        assert(handle->out_handle && reserve->access_owner);
        assert(chimera_vfs_compound_op(compound, 8)->status == CHIMERA_VFS_OK);
        new_len = handle->out_handle->fh_len;
        memcpy(new_fh, handle->out_handle->fh, new_len);
        assert(new_len != old_len || memcmp(new_fh, old_fh, new_len));
        assert(peers(new_fh, new_len) == 0);
        assert(chimera_vfs_claim_access_owner_claim(reserve->access_owner)->cb_private == NULL);
    }
}

void reparse_test_after(bool migrated)
{
    assert(saved_open && !(saved_open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED));
    assert(!saved_open->identity_rebind && saved_open->refcnt >= 1);
    assert(chimera_smb_share_claim(saved_open)->op_handle == saved_open->handle);
    assert(chimera_smb_share_claim(saved_open)->file == saved_open->share_file_state);
    assert(chimera_smb_share_claim(saved_open)->cb_private == saved_open);
    assert(saved_open->handle->fh_len == saved_open->share_file_state->fh_len);
    assert(!memcmp(saved_open->handle->fh, saved_open->share_file_state->fh,
        saved_open->handle->fh_len));
    if (migrated) {
        assert(new_len && saved_open->handle->fh_len == new_len);
        assert(!memcmp(saved_open->handle->fh, new_fh, new_len));
        assert(peers(old_fh, old_len) == 0 && peers(new_fh, new_len) == 1);
    } else {
        assert(saved_open->handle->fh_len == old_len &&
            !memcmp(saved_open->handle->fh, old_fh, old_len));
        assert(peers(old_fh, old_len) == 1);
    }
    saved_open = NULL;
}
