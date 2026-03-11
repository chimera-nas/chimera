// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include "vfs/vfs.h"
#include "smb2.h"

struct chimera_smb_open_file;

#define CHIMERA_SMB_SHAREMODE_BUCKETS     64
#define CHIMERA_SMB_SHAREMODE_BUCKET_MASK (CHIMERA_SMB_SHAREMODE_BUCKETS - 1)

/* Access rights that participate in share mode conflict checks.
 * Opens that request only attribute-level access (READ_ATTRIBUTES,
 * WRITE_ATTRIBUTES, SYNCHRONIZE, READ_CONTROL, etc.) bypass the
 * share mode check entirely, matching Windows/NTFS behavior. */
#define SMB2_SHAREMODE_ACCESS_MASK        ( \
            SMB2_MAXIMUM_ALLOWED |       \
            SMB2_GENERIC_ALL |           \
            SMB2_GENERIC_READ |          \
            SMB2_GENERIC_WRITE |         \
            SMB2_GENERIC_EXECUTE |       \
            SMB2_FILE_READ_DATA |        \
            SMB2_FILE_WRITE_DATA |       \
            SMB2_FILE_APPEND_DATA |      \
            SMB2_FILE_EXECUTE |          \
            SMB2_DELETE)

struct chimera_smb_sharemode_entry {
    uint32_t                            desired_access;
    uint32_t                            share_access;
    struct chimera_smb_open_file       *open_file;
    struct chimera_smb_sharemode_entry *next;
};

struct chimera_smb_sharemode_file {
    uint32_t                            parent_fh_len;
    uint32_t                            name_len;
    uint8_t                             parent_fh[CHIMERA_VFS_FH_SIZE];
    char                                name[SMB_FILENAME_MAX];
    int                                 num_entries;
    struct chimera_smb_sharemode_entry *entries;
    struct chimera_smb_sharemode_file  *next;
};

struct chimera_smb_sharemode_table {
    pthread_mutex_t                    lock;
    struct chimera_smb_sharemode_file *buckets[CHIMERA_SMB_SHAREMODE_BUCKETS];
};

void
chimera_smb_sharemode_init(
    struct chimera_smb_sharemode_table *table);

void
chimera_smb_sharemode_destroy(
    struct chimera_smb_sharemode_table *table);

int
chimera_smb_sharemode_acquire(
    struct chimera_smb_sharemode_table *table,
    const uint8_t                      *parent_fh,
    uint32_t                            parent_fh_len,
    const char                         *name,
    uint32_t                            name_len,
    uint32_t                            desired_access,
    uint32_t                            share_access,
    struct chimera_smb_open_file       *open_file);

void
chimera_smb_sharemode_release(
    struct chimera_smb_sharemode_table *table,
    struct chimera_smb_open_file       *open_file);
