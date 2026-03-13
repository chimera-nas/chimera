// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include "smb_sharemode.h"
#include "smb_session.h"
#include "smb_internal.h"
#include "common/macros.h"

static inline uint32_t
chimera_smb_sharemode_hash(
    const uint8_t *parent_fh,
    uint32_t       parent_fh_len,
    const char    *name,
    uint32_t       name_len)
{
    uint32_t hash = 5381;
    uint32_t i;

    for (i = 0; i < parent_fh_len; i++) {
        hash = ((hash << 5) + hash) + parent_fh[i];
    }

    for (i = 0; i < name_len; i++) {
        hash = ((hash << 5) + hash) + (uint8_t) name[i];
    }

    return hash;
} /* chimera_smb_sharemode_hash */

static inline int
chimera_smb_sharemode_file_match(
    const struct chimera_smb_sharemode_file *file,
    const uint8_t                           *parent_fh,
    uint32_t                                 parent_fh_len,
    const char                              *name,
    uint32_t                                 name_len)
{
    if (file->parent_fh_len != parent_fh_len ||
        file->name_len != name_len) {
        return 0;
    }

    if (parent_fh_len > 0 &&
        memcmp(file->parent_fh, parent_fh, parent_fh_len) != 0) {
        return 0;
    }

    if (name_len > 0 &&
        memcmp(file->name, name, name_len) != 0) {
        return 0;
    }

    return 1;
} /* chimera_smb_sharemode_file_match */

/* Expand generic access rights to their specific file access components.
 * Windows resolves these before share mode checks; Chimera must do the same
 * so that the conflict check sees the real access bits. */
static inline uint32_t
chimera_smb_sharemode_expand_access(uint32_t desired_access)
{
    if (desired_access & (SMB2_MAXIMUM_ALLOWED | SMB2_GENERIC_ALL)) {
        desired_access |= SMB2_FILE_READ_DATA |
            SMB2_FILE_WRITE_DATA |
            SMB2_FILE_APPEND_DATA |
            SMB2_FILE_READ_EA |
            SMB2_FILE_WRITE_EA |
            SMB2_FILE_EXECUTE |
            SMB2_FILE_DELETE_CHILD |
            SMB2_FILE_READ_ATTRIBUTES |
            SMB2_FILE_WRITE_ATTRIBUTES |
            SMB2_DELETE |
            SMB2_READ_CONTROL |
            SMB2_WRITE_DACL |
            SMB2_WRITE_OWNER |
            SMB2_SYNCHRONIZE;
    }

    if (desired_access & SMB2_GENERIC_READ) {
        desired_access |= SMB2_FILE_READ_DATA |
            SMB2_FILE_READ_ATTRIBUTES |
            SMB2_FILE_READ_EA |
            SMB2_READ_CONTROL |
            SMB2_SYNCHRONIZE;
    }

    if (desired_access & SMB2_GENERIC_WRITE) {
        desired_access |= SMB2_FILE_WRITE_DATA |
            SMB2_FILE_APPEND_DATA |
            SMB2_FILE_WRITE_ATTRIBUTES |
            SMB2_FILE_WRITE_EA |
            SMB2_READ_CONTROL |
            SMB2_SYNCHRONIZE;
    }

    if (desired_access & SMB2_GENERIC_EXECUTE) {
        desired_access |= SMB2_FILE_EXECUTE |
            SMB2_FILE_READ_ATTRIBUTES |
            SMB2_READ_CONTROL |
            SMB2_SYNCHRONIZE;
    }

    return desired_access;
} /* chimera_smb_sharemode_expand_access */

static inline int
chimera_smb_sharemode_check_conflict(
    uint32_t existing_desired_access,
    uint32_t existing_share_access,
    uint32_t new_desired_access,
    uint32_t new_share_access)
{
    /* Check if existing open's access conflicts with new open's share mode */

    if ((existing_desired_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE)) &&
        !(new_share_access & SMB2_FILE_SHARE_READ)) {
        return 1;
    }

    if ((existing_desired_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA)) &&
        !(new_share_access & SMB2_FILE_SHARE_WRITE)) {
        return 1;
    }

    if ((existing_desired_access & SMB2_DELETE) &&
        !(new_share_access & SMB2_FILE_SHARE_DELETE)) {
        return 1;
    }

    /* Check if new open's access conflicts with existing open's share mode */

    if ((new_desired_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE)) &&
        !(existing_share_access & SMB2_FILE_SHARE_READ)) {
        return 1;
    }

    if ((new_desired_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA)) &&
        !(existing_share_access & SMB2_FILE_SHARE_WRITE)) {
        return 1;
    }

    if ((new_desired_access & SMB2_DELETE) &&
        !(existing_share_access & SMB2_FILE_SHARE_DELETE)) {
        return 1;
    }

    return 0;
} /* chimera_smb_sharemode_check_conflict */

SYMBOL_EXPORT void
chimera_smb_sharemode_init(struct chimera_smb_sharemode_table *table)
{
    pthread_mutex_init(&table->lock, NULL);
    memset(table->buckets, 0, sizeof(table->buckets));
} /* chimera_smb_sharemode_init */

SYMBOL_EXPORT void
chimera_smb_sharemode_destroy(struct chimera_smb_sharemode_table *table)
{
    struct chimera_smb_sharemode_file  *file, *next_file;
    struct chimera_smb_sharemode_entry *entry, *next_entry;
    int                                 i;

    for (i = 0; i < CHIMERA_SMB_SHAREMODE_BUCKETS; i++) {
        file = table->buckets[i];

        while (file) {
            next_file = file->next;
            entry     = file->entries;

            while (entry) {
                next_entry = entry->next;
                free(entry);
                entry = next_entry;
            }

            free(file);
            file = next_file;
        }

        table->buckets[i] = NULL;
    }

    pthread_mutex_destroy(&table->lock);
} /* chimera_smb_sharemode_destroy */

SYMBOL_EXPORT int
chimera_smb_sharemode_acquire(
    struct chimera_smb_sharemode_table *table,
    const uint8_t                      *parent_fh,
    uint32_t                            parent_fh_len,
    const char                         *name,
    uint32_t                            name_len,
    uint32_t                            desired_access,
    uint32_t                            share_access,
    struct chimera_smb_open_file       *open_file)
{
    uint32_t                            bucket;
    struct chimera_smb_sharemode_file  *file;
    struct chimera_smb_sharemode_entry *entry;

    /* Expand generic access rights to specific bits before conflict
     * checks and before storing in the table entry. */
    desired_access = chimera_smb_sharemode_expand_access(desired_access);

    if ((desired_access & SMB2_SHAREMODE_ACCESS_MASK) == 0) {
        return (0);
    }

    bucket = chimera_smb_sharemode_hash(parent_fh, parent_fh_len,
                                        name, name_len) &
        CHIMERA_SMB_SHAREMODE_BUCKET_MASK;

    pthread_mutex_lock(&table->lock);

    /* Find existing file node */
    file = table->buckets[bucket];

    while (file) {
        if (chimera_smb_sharemode_file_match(file, parent_fh, parent_fh_len,
                                             name, name_len)) {
            break;
        }
        file = file->next;
    }

    if (file) {
        /* Check for conflicts with existing opens */
        entry = file->entries;

        while (entry) {
            if (chimera_smb_sharemode_check_conflict(
                    entry->desired_access, entry->share_access,
                    desired_access, share_access)) {
                pthread_mutex_unlock(&table->lock);
                return -1;
            }
            entry = entry->next;
        }
    } else {
        /* Create new file node */
        file = calloc(1, sizeof(*file));

        chimera_vfs_abort_if(file == NULL, "memory allocation failed");

        file->parent_fh_len = parent_fh_len;
        file->name_len      = name_len;

        if (parent_fh_len > 0) {
            memcpy(file->parent_fh, parent_fh, parent_fh_len);
        }

        if (name_len > 0) {
            memcpy(file->name, name, name_len);
        }

        file->next             = table->buckets[bucket];
        table->buckets[bucket] = file;
    }

    /* Add new entry */
    entry = calloc(1, sizeof(*entry));

    chimera_vfs_abort_if(entry == NULL, "memory allocation failed");

    entry->desired_access = desired_access;
    entry->share_access   = share_access;
    entry->open_file      = open_file;
    entry->next           = file->entries;
    file->entries         = entry;
    file->num_entries++;

    pthread_mutex_unlock(&table->lock);

    return 0;
} /* chimera_smb_sharemode_acquire */

SYMBOL_EXPORT void
chimera_smb_sharemode_release(
    struct chimera_smb_sharemode_table *table,
    struct chimera_smb_open_file       *open_file)
{
    uint32_t                            bucket;
    struct chimera_smb_sharemode_file  *file, **file_prev;
    struct chimera_smb_sharemode_entry *entry, **entry_prev;

    bucket = chimera_smb_sharemode_hash(
        open_file->parent_fh,
        open_file->parent_fh_len,
        open_file->name,
        open_file->name_len) &
        CHIMERA_SMB_SHAREMODE_BUCKET_MASK;

    pthread_mutex_lock(&table->lock);

    /* Find file node */
    file_prev = &table->buckets[bucket];
    file      = *file_prev;

    while (file) {
        if (chimera_smb_sharemode_file_match(file,
                                             open_file->parent_fh,
                                             open_file->parent_fh_len,
                                             open_file->name,
                                             open_file->name_len)) {
            break;
        }
        file_prev = &file->next;
        file      = file->next;
    }

    if (!file) {
        pthread_mutex_unlock(&table->lock);
        return;
    }

    /* Find and remove entry */
    entry_prev = &file->entries;
    entry      = *entry_prev;

    while (entry) {
        if (entry->open_file == open_file) {
            *entry_prev = entry->next;
            free(entry);
            file->num_entries--;
            break;
        }
        entry_prev = &entry->next;
        entry      = entry->next;
    }

    /* Remove file node if no more entries */
    if (file->num_entries == 0) {
        *file_prev = file->next;
        free(file);
    }

    pthread_mutex_unlock(&table->lock);
} /* chimera_smb_sharemode_release */
