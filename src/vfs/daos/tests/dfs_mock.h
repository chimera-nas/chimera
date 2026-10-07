// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "vfs_daos.h"

#define CHECK(condition) do { \
            if (!(condition)) { \
                fprintf(stderr, "line %d: %s\n", __LINE__, #condition); \
                exit(EXIT_FAILURE); \
            } \
} while (0)

struct dfs_obj {
    daos_obj_id_t oid;
    mode_t        mode;
    unsigned int  repairs;
};
extern unsigned int allocations, releases, lookups, stats, creates, removes, moves;
extern int          stat_error, release_error, allocation_error, repair_error;
extern unsigned int connects, disconnects, inits, finis, syncs, writes, reads, setattrs;
extern unsigned int directory_entries;
extern int          connect_error, sync_error, io_error, setattr_flags, connect_flags;
extern struct stat  setattr_value;
extern const char  *missing_name;
extern daos_size_t  read_length, file_size, submitted_length;
extern int          access_denied;
