// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Keep server-private types out of the wire fixture. The original CREATE
* caller owns the captured open until its EA completion; this helper takes no
* extra reference which could conceal a leaked parked-owner reference. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "server/smb/smb_internal.h"

static atomic_int capture_park;
static            _Thread_local struct chimera_server_smb_shared *captured_shared;
static            _Thread_local struct chimera_smb_open_file *captured_open;

void
smb_persistent_cleanup_test_arm_park(void)
{
    assert(!atomic_exchange(&capture_park, 1));
} /* smb_persistent_cleanup_test_arm_park */

__attribute__((visibility("default"))) void
chimera_smb_durable_register(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open,
    uint64_t                          session_id,
    uint32_t                          owner_uid,
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              persistent)
{
    typedef void (*register_fn)(
        struct chimera_server_smb_shared *,
        struct chimera_smb_open_file *,
        uint64_t,
        uint32_t,
        const uint8_t *,
        const char *,
        uint32_t,
        bool);
    register_fn next = (register_fn) dlsym(RTLD_NEXT, "chimera_smb_durable_register");
    assert(next);
    next(shared, open, session_id, owner_uid, client_guid, name, name_len, persistent);
    if (persistent && atomic_exchange(&capture_park, 0)) {
        assert(!captured_open);
        captured_shared = shared;
        captured_open   = open;
    }
} /* chimera_smb_durable_register */

/* Called on the registering worker, immediately before submitting the first
* EA LISTXATTRS compound. Reproduce the ownership transition performed by
* session_park_durables without depending on socket timing or a second worker.
* Return zero for unrelated submissions which have no armed registration. */
int
smb_persistent_cleanup_test_park(void)
{
    struct chimera_smb_open_file     *open   = captured_open;
    struct chimera_server_smb_shared *shared = captured_shared;

    if (!open) {
        return 0;
    }
    captured_open   = NULL;
    captured_shared = NULL;

    struct chimera_smb_tree          *tree   = open->tree;
    unsigned int                      bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    struct chimera_smb_open_file     *live;
    struct chimera_smb_durable_entry *entry;
    evpl_mutex_lock(&tree->open_files_lock[bucket]);
    HASH_FIND(hh, tree->open_files[bucket], &open->file_id, sizeof(open->file_id), live);
    assert(live == open);
    assert(!(open->flags & (CHIMERA_SMB_OPEN_FILE_CLOSED | CHIMERA_SMB_OPEN_FILE_PARKED)));
    assert(open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED);
    assert(open->durable_flags);
    unsigned int                      refs = atomic_load(&open->refcnt);
    assert(refs >= 2); /* tree owner plus the still-pending CREATE caller */

    evpl_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &open->file_id.pid, sizeof(open->file_id.pid), entry);
    assert(entry && entry->open_file == open && entry->persistent && !entry->parked && !entry->cold);
    evpl_mutex_unlock(&shared->durable.lock);

    HASH_DELETE(hh, tree->open_files[bucket], open);
    open->flags      |= CHIMERA_SMB_OPEN_FILE_PARKED;
    open->create_conn = NULL;
    chimera_smb_durable_park(shared, open);
    assert(atomic_load(&open->refcnt) == refs);
    assert(open->durable_tree_pin == tree);
    evpl_mutex_lock(&shared->durable.lock);
    HASH_FIND(hh, shared->durable.by_pid, &open->file_id.pid, sizeof(open->file_id.pid), entry);
    assert(entry && entry->open_file == open && entry->parked);
    evpl_mutex_unlock(&shared->durable.lock);
    evpl_mutex_unlock(&tree->open_files_lock[bucket]);
    return 1;
} /* smb_persistent_cleanup_test_park */
