// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#define _GNU_SOURCE
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include <stdatomic.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"

atomic_int lock_test_arm, lock_test_seen, lock_test_claims, lock_test_rehomed;
static _Thread_local struct chimera_smb_request *checked_request;
static _Thread_local bool allocation_failure;

/* Mode 1 reproduces dispatcher refusal after an initial batch-allocation
 * failure. Mode 2 proves the ordinary route is compound-native and inspects
 * the accepted RANGE owner before frontend completion releases its pin. */
__attribute__((visibility("default"))) int
chimera_smb_vfs_compound_try(struct chimera_smb_compound *wire)
{
    typedef int (*fn)(struct chimera_smb_compound *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_smb_vfs_compound_try");
    assert(next);
    struct chimera_smb_request *request = wire->requests[wire->complete_requests];
    if (request->smb2_hdr.command != SMB2_LOCK || !atomic_load(&lock_test_arm)) {
        return next(wire);
    }
    assert(!checked_request);
    allocation_failure = atomic_exchange(&lock_test_arm, 0) == 1;
    assert(allocation_failure || wire->num_requests == 1);
    checked_request = request;
    if (allocation_failure) return 0;
    int admitted = next(wire);
    assert(admitted);
    return admitted;
}

void
lock_test_inspect(struct chimera_vfs_compound *compound)
{
    if (!checked_request || allocation_failure ||
        chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_OK) return;
    struct smb_vfs_command *command = chimera_vfs_compound_group_context(compound, 0);
    if (!command || command->request != checked_request) return;
    struct chimera_smb_open_file *open = command->open;
    assert(open && open->handle && open->share_file_state && open->range_owner);
    struct chimera_claim_owner owner = chimera_smb_open_actor_owner(open);
    assert(owner.proto == CHIMERA_CLAIM_PROTO_SMB2);
    assert(owner.owner_lo == open->file_id.pid && owner.owner_hi == open->file_id.vid);
    if (atomic_load(&lock_test_rehomed)) {
        assert(owner.client_key != checked_request->session_handle->session->client_key);
    }
    unsigned int count = 0;
    pthread_mutex_lock(&open->share_file_state->lock);
    for (struct chimera_vfs_claim *claim = open->share_file_state->claims[CHIMERA_CLAIM_CLASS_RANGE];
         claim; claim = claim->next) {
        if (claim->owner.owner_lo != owner.owner_lo || claim->owner.owner_hi != owner.owner_hi) continue;
        assert(claim->owner.proto == owner.proto && claim->owner.client_key == owner.client_key);
        assert(!memcmp(claim->owner.key, owner.key, sizeof(owner.key)));
        assert(claim->op_handle == open->handle && claim->local_only && !claim->provisional);
        count++;
    }
    pthread_mutex_unlock(&open->share_file_state->lock);
    if (count != (unsigned int) atomic_load(&lock_test_claims)) {
        fprintf(stderr, "compound LOCK claims=%u expected=%d\n", count, atomic_load(&lock_test_claims));
    }
    assert(count == (unsigned int) atomic_load(&lock_test_claims));
    checked_request = NULL;
    atomic_fetch_add(&lock_test_seen, 1);
}

__attribute__((visibility("default"))) void
chimera_smb_complete_request(struct chimera_smb_request *request, unsigned int status)
{
    typedef void (*fn)(struct chimera_smb_request *, unsigned int);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_smb_complete_request");
    assert(next);
    if (request == checked_request && status != SMB2_STATUS_PENDING) {
        assert(allocation_failure && status == SMB2_STATUS_INSUFFICIENT_RESOURCES);
        checked_request = NULL;
        atomic_fetch_add(&lock_test_seen, 1);
    }
    next(request, status);
}
