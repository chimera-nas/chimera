// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"

/* Called on the server event loop from the wire fixture's submit/finish hooks. */
void
smb_resiliency_inspect(
    void    *private_data,
    int      expected,
    uint64_t timeout)
{
    struct smb_vfs_command           *command = private_data;
    struct chimera_smb_open_file     *open    = command->open;
    struct chimera_smb_durable_table *table   = &command->request->compound->thread->shared->durable;
    struct chimera_smb_durable_entry *entry;

    assert(open && open->resilient == !!expected);
    if (expected) {
        assert(open->resilient_timeout_ms == timeout);
    }
    evpl_mutex_lock(&table->lock);
    HASH_FIND(hh, table->by_pid, &open->file_id.pid, sizeof(open->file_id.pid), entry);
    assert(!!entry == !!expected);
    if (entry) {
        assert(entry->open_file == open && !entry->parked);
        unsigned int matches = 0;
        for (struct chimera_smb_durable_entry *e = table->by_pid; e; e = e->hh.next) {
            matches += e->persistent_id == open->file_id.pid;
        }
        assert(matches == 1);
    }
    evpl_mutex_unlock(&table->lock);
} /* smb_resiliency_inspect */
