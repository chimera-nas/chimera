// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

struct chimera_smb_durable_entry;
struct chimera_smb_open_file;
struct chimera_smb_request;
struct chimera_server_smb_shared;

/* Construction reserves an entire one-entry hash, including its buckets.
 * Publication transfers it or merges without allocation. Discard is valid
 * before publication, after rejection, or after an idempotent duplicate. */
struct chimera_smb_durable_entry * chimera_smb_durable_registration_prepare(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open);
void chimera_smb_durable_registration_publish(
    struct chimera_server_smb_shared  *shared,
    struct chimera_smb_durable_entry **prepared);
void chimera_smb_durable_registration_discard(
    struct chimera_smb_durable_entry **prepared);
