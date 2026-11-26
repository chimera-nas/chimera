// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "vfs/vfs_identity.h"
#include "smb_account_cache.h"

/*
 * The naming rules of the LSARPC lookups, in one pure module: the well-known
 * account table, the Unix SIDs, the names winbind gives accounts, and the
 * fallback to the local user and group caches.  Nothing here adds to a cache
 * or logs (a lookup may drop an account-cache entry that has expired), so a
 * caller may run the same lookup twice -- once to find out what to ask the
 * resolver, once with its answer.  The caches it reads are live: an entry that
 * expires or is evicted between the two runs is not there for the second.
 */

#define LSA_SID_NAME_USER      1
#define LSA_SID_NAME_DOM_GRP   2
#define LSA_SID_NAME_DOMAIN    3
#define LSA_SID_NAME_ALIAS     4
#define LSA_SID_NAME_WKN_GROUP 5
#define LSA_SID_NAME_UNKNOWN   8

/* Where names come from for one lookup.  account_cache is NULL exactly when
 * winbind is not enabled. */
struct chimera_smb_lsa_naming {
    struct chimera_vfs_user_cache    *user_cache;
    struct chimera_smb_account_cache *account_cache;
    const char                       *fallback_domain;
};

/* A resolver question a lookup needs answered before it can name the input. */
struct chimera_smb_lsa_miss {
    enum chimera_vfs_identity_key key;
    uint32_t                      id;
    char                          input[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX];
};

/* The resolver's answer to that question (pass 2). */
struct chimera_smb_lsa_resolved {
    enum chimera_vfs_identity_status status;
    struct chimera_vfs_identity_account account; /* valid when status == OK */
};

enum chimera_smb_lsa_rc {
    CHIMERA_SMB_LSA_MAPPED,   /* *out filled                          */
    CHIMERA_SMB_LSA_UNMAPPED, /* nothing can name it                  */
    CHIMERA_SMB_LSA_MISS,     /* *miss filled; resolve it, then retry */
};

/* The domain a name sent without one belongs to, and the one local records
 * are reported under: `joined_domain`, the domain winbind says the host is
 * joined to, when non-NULL and non-empty; else `winbind_domain`, the
 * configured fallback for it; else "WORKGROUP". */
const char *
chimera_smb_lsa_fallback_domain(
    const char *joined_domain,
    const char *winbind_domain);

/* Split "DOMAIN\name" at the first backslash; with no backslash the domain is
 * empty and the name is the whole input.  0, or -1 when a part does not fit. */
int
chimera_smb_lsa_split_name(
    const char *in,
    char       *domain,
    size_t      domain_len,
    char       *name,
    size_t      name_len);

/* 1 with *out filled when `sid` / `name` is a well-known account, else 0. */
int
chimera_smb_lsa_wellknown_sid(
    const char                          *sid,
    struct chimera_vfs_identity_account *out);

int
chimera_smb_lsa_wellknown_name(
    const char                          *name,
    struct chimera_vfs_identity_account *out);

enum chimera_smb_lsa_rc
chimera_smb_lsa_lookup_sid(
    const struct chimera_smb_lsa_naming   *naming,
    const char                            *sid,
    int                                    may_resolve,
    const struct chimera_smb_lsa_resolved *resolved,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss);

enum chimera_smb_lsa_rc
chimera_smb_lsa_lookup_name(
    const struct chimera_smb_lsa_naming   *naming,
    const char                            *name,
    int                                    may_resolve,
    const struct chimera_smb_lsa_resolved *resolved,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss);

/* Turn a resolver result for `key` into an account.  `input` is the SID the
 * lookup asked about (used for the BY_UID / BY_GID keys). */
void
chimera_smb_lsa_account_from_result(
    enum chimera_vfs_identity_key             key,
    const char                               *input,
    const struct chimera_vfs_identity_result *result,
    struct chimera_vfs_identity_account      *out);
