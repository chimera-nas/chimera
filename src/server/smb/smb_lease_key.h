// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

struct chimera_smb_request;
struct chimera_smb_open_file;
struct chimera_vfs_open_handle;
struct chimera_smb_lease_key_entry;
struct chimera_smb_lease_key_table {
    pthread_mutex_t lock;
    struct chimera_smb_lease_key_entry *entries;
};

void chimera_smb_lease_key_init(struct chimera_smb_lease_key_table *table);
void chimera_smb_lease_key_destroy(struct chimera_smb_lease_key_table *table);
/* Reserve key identity before backend work; unbound keys serialize constructors.
 * PENDING owns no reservation.
 * Native callers must use explicit COORDINATE, never prepare/completion. */
uint32_t chimera_smb_lease_key_begin(struct chimera_smb_request *request);
/* Read-only identity check; NULL/zero asks whether the key is already bound. */
bool chimera_smb_lease_key_conflict(struct chimera_smb_request *request,
                                   const uint8_t *fh, uint32_t fh_len);
/* Nonallocating transfer of a prevalidated binding to an open's lifetime.
 * The request keeps its identity reservation until end. */
void chimera_smb_lease_key_attach(struct chimera_smb_request *request,
                                 struct chimera_smb_open_file *open,
                                 const struct chimera_vfs_open_handle *handle);
__attribute__((visibility("default"))) void chimera_smb_lease_key_end(struct chimera_smb_request *request);
void chimera_smb_lease_key_open_release(struct chimera_smb_open_file *open);
