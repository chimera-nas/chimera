// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs_identity.h"

/*
 * A small, process-wide, lock-protected cache of account answers (the result
 * of the SID-to-account and name-to-account identity resolver keys).  It keeps
 * each answer by SID and by name for a fixed time so that repeated LSARPC
 * lookups do not go back to the resolver.  The two indexes hold their own
 * copies and each is bounded and evicts by itself, so an account can stay
 * findable by one after the other dropped it.  Only answers are kept, never a
 * "not found".  Safe to call from any thread; none of the calls block on
 * anything but the cache's own short critical section.
 */

#define CHIMERA_SMB_ACCOUNT_CACHE_ENTRIES 4096 /* per index */
#define CHIMERA_SMB_ACCOUNT_CACHE_TTL     600 /* seconds */

struct chimera_smb_account_cache;

struct chimera_smb_account_cache *
chimera_smb_account_cache_create(
    int max_entries,
    int ttl_seconds);

void
chimera_smb_account_cache_destroy(
    struct chimera_smb_account_cache *cache);

/* Insert or refresh `acct`.  It becomes findable by its SID, by
 * "domain\name" (by "name" alone when domain is empty) and, when `as_sent` is
 * non-NULL, by that string too: the name the resolver was asked, never one
 * that was not resolved to this account.  Name keys compare without regard to
 * ASCII case. */
void
chimera_smb_account_cache_put(
    struct chimera_smb_account_cache          *cache,
    const struct chimera_vfs_identity_account *acct,
    const char                                *as_sent);

/* 1 with *out filled on a live (unexpired) hit, else 0. */
int
chimera_smb_account_cache_get_sid(
    struct chimera_smb_account_cache    *cache,
    const char                          *sid,
    struct chimera_vfs_identity_account *out);

int
chimera_smb_account_cache_get_name(
    struct chimera_smb_account_cache    *cache,
    const char                          *name,
    struct chimera_vfs_identity_account *out);
