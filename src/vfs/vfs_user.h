// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>
#include "common/chimera_rcu.h"
#include "sdk/vfs_cred.h"

// Maximum length for a Windows SID string (S-1-5-21-xxx-xxx-xxx-rid format)
#define CHIMERA_VFS_SID_MAX_LEN 80

/*
 * In-memory identity store: maps a known principal between its UNIX identity
 * (uid/gid/supplementary groups), its name, and its Windows SID.  This is the
 * single authority the protocol servers, the idmap formatter, and SMB auth all
 * consult.  Entries are either pinned (configured local users) or cached with a
 * TTL (e.g. AD users learned at auth, or resolved on demand by the identity
 * resolver).
 *
 * Concurrency: lookups are reads on the cache's RCU domain (callers take
 * chimera_rcu_read_lock(&cache->rcu) around the call and any use of what it
 * returns); all mutations (add / remove / TTL expiry) run inside a publish
 * region on that domain and are serialized by a single cache write_lock.
 * Mutations are rare (auth, config load, the 60s expiry sweep) so a single
 * writer lock keeps the three indices (name, uid, sid) trivially consistent
 * without the cross-index lock-ordering hazards a per-bucket scheme would
 * create.
 */
struct chimera_vfs_user {
    uint32_t                 uid;
    uint32_t                 gid;
    uint32_t                 ngids;
    int                      username_len;
    struct timespec          expiration;
    int                      pinned;
    chimera_rcu_head         rcu;
    struct chimera_vfs_user *next_by_name;
    struct chimera_vfs_user *next_by_uid;
    struct chimera_vfs_user *next_by_sid;
    struct chimera_vfs_user *next_builtin;
    uint32_t                 gids[CHIMERA_VFS_CRED_MAX_GIDS];
    char                     username[256];
    char                     password[256];
    char                     smbpasswd[256];
    char                     sid[CHIMERA_VFS_SID_MAX_LEN];
};

/*
 * A group record.  Deliberately a distinct type from chimera_vfs_user rather
 * than a flag on it: a group is not a user, and sharing the record would put
 * group entries in the name/uid/sid chains where lookup_by_uid, sid_to_uid and
 * the username dedup in _add would return or clobber them.  Groups live in
 * their own two chains below, sharing this cache's write_lock and expiry
 * thread.
 */
struct chimera_vfs_group {
    uint32_t                  gid;
    int                       groupname_len;
    struct timespec           expiration;
    int                       pinned;
    chimera_rcu_head          rcu;
    struct chimera_vfs_group *next_by_gid;
    struct chimera_vfs_group *next_by_sid;
    char                      groupname[256];
    char                      sid[CHIMERA_VFS_SID_MAX_LEN];
};
