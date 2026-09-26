// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "smb_internal.h"
#include "smb_lease_key.h"

/* No callback, bucket, VFS or namespace lock is taken while this lock is held.
 * A constructor owns the entry across its read-only identity checks and any
 * mutation. Published opens retain the binding across CLOSE/park/reconnect,
 * until their final reference retires. This also covers lease state NONE. */
struct chimera_smb_lease_key_entry {
    struct chimera_smb_lease_key_entry *next;
    struct chimera_smb_lease_key_table *table;
    struct chimera_smb_request *constructor;
    uint64_t client_key;
    uint8_t key[16];
    uint8_t fh[CHIMERA_VFS_FH_SIZE];
    uint32_t fh_len;
    unsigned int opens;
    unsigned int constructors;
};

void
chimera_smb_lease_key_init(struct chimera_smb_lease_key_table *table)
{
    table->entries = NULL;
    pthread_mutex_init(&table->lock, NULL);
}

void
chimera_smb_lease_key_destroy(struct chimera_smb_lease_key_table *table)
{
    chimera_smb_abort_if(table->entries, "lease key bindings survived SMB teardown");
    pthread_mutex_destroy(&table->lock);
}

static void
smb_lease_key_reap(struct chimera_smb_lease_key_entry *entry)
{
    if (entry->constructors || entry->opens) { return; }
    struct chimera_smb_lease_key_entry **link = &entry->table->entries;
    while (*link != entry) { link = &(*link)->next; }
    *link = entry->next;
    free(entry);
}

SYMBOL_EXPORT uint32_t
chimera_smb_lease_key_begin(struct chimera_smb_request *request)
{
    struct chimera_server_smb_shared *shared = request->compound->thread->shared;
    if (request->lease_key_reservation || !shared->config.leases ||
        !(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) ||
        request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {
        return SMB2_STATUS_SUCCESS;
    }
    struct chimera_smb_lease_key_table *table = &shared->lease_keys;
    uint64_t client_key = request->session_handle->session->client_key;
    pthread_mutex_lock(&table->lock);
    struct chimera_smb_lease_key_entry *entry;
    for (entry = table->entries; entry; entry = entry->next) {
        if (entry->client_key == client_key &&
            !memcmp(entry->key, request->create.rqls.key, sizeof(entry->key))) { break; }
    }
    if (entry && !entry->fh_len && entry->constructor) {
        /* A replay cannot wait behind the original CREATE's break ACK: the
         * client may wait for this replay answer before sending that ACK.
         * Classify this still-active original exactly like the existing
         * pending-create/replay path, before acquiring any backend resources. */
        struct chimera_smb_request *original = entry->constructor;
        bool replay = request->is_replay &&
            (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
            (original->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
            !memcmp(request->create.dh2q.create_guid, original->create.dh2q.create_guid, 16);
        uint32_t status = replay ?
            (shared->config.replay_pending_windows ? SMB2_STATUS_SHARING_VIOLATION :
                SMB2_STATUS_FILE_NOT_AVAILABLE) : SMB2_STATUS_PENDING;
        pthread_mutex_unlock(&table->lock);
        return status;
    }
    if (!entry) {
        entry = calloc(1, sizeof(*entry));
        if (!entry) {
            pthread_mutex_unlock(&table->lock);
            return SMB2_STATUS_INSUFFICIENT_RESOURCES;
        }
        entry->table = table;
        entry->client_key = client_key;
        memcpy(entry->key, request->create.rqls.key, sizeof(entry->key));
        entry->next = table->entries;
        table->entries = entry;
    }
    if (!entry->fh_len) { entry->constructor = request; }
    entry->constructors++;
    request->lease_key_reservation = entry;
    pthread_mutex_unlock(&table->lock);
    return SMB2_STATUS_SUCCESS;
}

bool
chimera_smb_lease_key_conflict(struct chimera_smb_request *request,
                              const uint8_t *fh, uint32_t fh_len)
{
    struct chimera_smb_lease_key_entry *entry = request->lease_key_reservation;
    if (!entry) { return false; }
    pthread_mutex_lock(&entry->table->lock);
    bool conflict = entry->fh_len &&
        (entry->fh_len != fh_len || memcmp(entry->fh, fh, fh_len));
    pthread_mutex_unlock(&entry->table->lock);
    return conflict;
}

void
chimera_smb_lease_key_attach(struct chimera_smb_request *request,
                            struct chimera_smb_open_file *open,
                            const struct chimera_vfs_open_handle *handle)
{
    struct chimera_smb_lease_key_entry *entry = request->lease_key_reservation;
    if (!entry || open->lease_key_binding) { return; }
    pthread_mutex_lock(&entry->table->lock);
    chimera_smb_abort_if(!entry->constructors || !handle ||
        (entry->fh_len && (entry->fh_len != handle->fh_len ||
            memcmp(entry->fh, handle->fh, handle->fh_len))), "unvalidated lease binding");
    entry->fh_len = handle->fh_len;
    memcpy(entry->fh, handle->fh, handle->fh_len);
    entry->opens++;
    /* Once exact identity is bound, concurrent constructors can share the
     * immutable identity reservation instead of serializing same-file joins. */
    entry->constructor = NULL;
    open->lease_key_binding = entry;
    pthread_mutex_unlock(&entry->table->lock);
}

void
chimera_smb_lease_key_end(struct chimera_smb_request *request)
{
    struct chimera_smb_lease_key_entry *entry = request->lease_key_reservation;
    if (!entry) { return; }
    struct chimera_smb_lease_key_table *table = entry->table;
    request->lease_key_reservation = NULL;
    pthread_mutex_lock(&table->lock);
    chimera_smb_abort_if(!entry->constructors, "lease reservation reference underflow");
    if (entry->constructor == request) { entry->constructor = NULL; }
    entry->constructors--;
    smb_lease_key_reap(entry);
    pthread_mutex_unlock(&table->lock);
}

void
chimera_smb_lease_key_open_release(struct chimera_smb_open_file *open)
{
    struct chimera_smb_lease_key_entry *entry = open->lease_key_binding;
    if (!entry) { return; }
    struct chimera_smb_lease_key_table *table = entry->table;
    open->lease_key_binding = NULL;
    pthread_mutex_lock(&table->lock);
    chimera_smb_abort_if(!entry->opens, "lease binding reference underflow");
    entry->opens--;
    smb_lease_key_reap(entry);
    pthread_mutex_unlock(&table->lock);
}
