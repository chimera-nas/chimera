// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <strings.h>
#endif /* ifdef _WIN32 */

#include "smb_lsarpc_names.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_idmap.h"

#define LSA_UNIX_USER_DOMAIN  "Unix User"
#define LSA_UNIX_GROUP_DOMAIN "Unix Group"
#define LSA_MODE_SID_PREFIX   "S-1-5-88-3-"

struct lsa_wellknown {
    const char *sid;
    const char *domain;
    const char *name;
    uint32_t    type;
};

/* *INDENT-OFF* */
static const struct lsa_wellknown lsa_wellknown_table[] = {
    { "S-1-1-0",      "",              "Everyone",             LSA_SID_NAME_WKN_GROUP },
    { "S-1-3-0",      "",              "CREATOR OWNER",        LSA_SID_NAME_WKN_GROUP },
    { "S-1-3-1",      "",              "CREATOR GROUP",        LSA_SID_NAME_WKN_GROUP },
    { "S-1-3-4",      "",              "OWNER RIGHTS",         LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-2",      "NT AUTHORITY",  "NETWORK",              LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-4",      "NT AUTHORITY",  "INTERACTIVE",          LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-7",      "NT AUTHORITY",  "ANONYMOUS LOGON",      LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-11",     "NT AUTHORITY",  "Authenticated Users",  LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-18",     "NT AUTHORITY",  "SYSTEM",               LSA_SID_NAME_WKN_GROUP },
    { "S-1-5-32-544", "BUILTIN",       "Administrators",       LSA_SID_NAME_ALIAS },
    { "S-1-5-32-545", "BUILTIN",       "Users",                LSA_SID_NAME_ALIAS },
    { "S-1-5-32",     "BUILTIN",       "BUILTIN",              LSA_SID_NAME_DOMAIN },
};
/* *INDENT-ON* */

#define LSA_WELLKNOWN_COUNT (sizeof(lsa_wellknown_table) / sizeof(lsa_wellknown_table[0]))

static void
lsa_account_set(
    struct chimera_vfs_identity_account *out,
    const char                          *sid,
    const char                          *domain,
    const char                          *name,
    uint32_t                             type)
{
    memset(out, 0, sizeof(*out));
    snprintf(out->sid, sizeof(out->sid), "%s", sid);
    snprintf(out->domain, sizeof(out->domain), "%s", domain);
    snprintf(out->name, sizeof(out->name), "%s", name);
    out->sid_type = type;
} /* lsa_account_set */

const char *
chimera_smb_lsa_fallback_domain(
    const char *joined_domain,
    const char *winbind_domain)
{
    if (joined_domain && joined_domain[0]) {
        return joined_domain;
    }
    return (winbind_domain && winbind_domain[0]) ? winbind_domain : "WORKGROUP";
} /* chimera_smb_lsa_fallback_domain */

int
chimera_smb_lsa_split_name(
    const char *in,
    char       *domain,
    size_t      domain_len,
    char       *name,
    size_t      name_len)
{
    const char *bs = strchr(in, '\\');
    size_t      dlen, nlen;

    if (bs) {
        dlen = (size_t) (bs - in);
        nlen = strlen(bs + 1);
    } else {
        dlen = 0;
        nlen = strlen(in);
    }

    if (dlen >= domain_len || nlen >= name_len) {
        return -1;
    }

    memcpy(domain, in, dlen);
    domain[dlen] = '\0';
    memcpy(name, bs ? bs + 1 : in, nlen);
    name[nlen] = '\0';

    return 0;
} /* chimera_smb_lsa_split_name */

int
chimera_smb_lsa_wellknown_sid(
    const char                          *sid,
    struct chimera_vfs_identity_account *out)
{
    size_t i;

    for (i = 0; i < LSA_WELLKNOWN_COUNT; i++) {
        if (strcmp(sid, lsa_wellknown_table[i].sid) == 0) {
            lsa_account_set(out, lsa_wellknown_table[i].sid,
                            lsa_wellknown_table[i].domain,
                            lsa_wellknown_table[i].name,
                            lsa_wellknown_table[i].type);
            return 1;
        }
    }

    return 0;
} /* chimera_smb_lsa_wellknown_sid */

int
chimera_smb_lsa_wellknown_name(
    const char                          *name,
    struct chimera_vfs_identity_account *out)
{
    const struct lsa_wellknown *row;
    const char                 *bs = strchr(name, '\\');
    size_t                      dlen, i;

    for (i = 0; i < LSA_WELLKNOWN_COUNT; i++) {
        row = &lsa_wellknown_table[i];

        if (!bs) {
            if (strcasecmp(name, row->name) != 0) {
                continue;
            }
        } else {
            dlen = (size_t) (bs - name);

            if (row->domain[0] == '\0' ||
                strlen(row->domain) != dlen ||
                strncasecmp(name, row->domain, dlen) != 0) {
                continue;
            }

            /* "BUILTIN\" names the BUILTIN domain itself. */
            if (strcasecmp(bs + 1, row->name) != 0 &&
                !(bs[1] == '\0' && row->type == LSA_SID_NAME_DOMAIN)) {
                continue;
            }
        }

        lsa_account_set(out, row->sid, row->domain, row->name, row->type);
        return 1;
    }

    return 0;
} /* chimera_smb_lsa_wellknown_name */

static void
lsa_miss_set(
    struct chimera_smb_lsa_miss  *miss,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *input)
{
    miss->key = key;
    miss->id  = id;
    snprintf(miss->input, sizeof(miss->input), "%s", input);
} /* lsa_miss_set */

/* Copy a user's (group's) name out of the cache.  Returns 1 when
 * found. */
static int
lsa_user_by_uid(
    struct chimera_vfs_user_cache *cache,
    uint32_t                       uid,
    char                          *name,
    size_t                         name_len)
{
    const struct chimera_vfs_user *user;
    int                            found = 0;

    chimera_rcu_read_lock(&cache->rcu);
    user = chimera_vfs_user_cache_lookup_by_uid(cache, uid);
    if (user) {
        snprintf(name, name_len, "%s", user->username);
        found = 1;
    }
    chimera_rcu_read_unlock(&cache->rcu);

    return found;
} /* lsa_user_by_uid */

static int
lsa_group_by_gid(
    struct chimera_vfs_user_cache *cache,
    uint32_t                       gid,
    char                          *name,
    size_t                         name_len)
{
    const struct chimera_vfs_group *group;
    int                             found = 0;

    chimera_rcu_read_lock(&cache->rcu);
    group = chimera_vfs_group_cache_lookup_by_gid(cache, gid);
    if (group) {
        snprintf(name, name_len, "%s", group->groupname);
        found = 1;
    }
    chimera_rcu_read_unlock(&cache->rcu);

    return found;
} /* lsa_group_by_gid */

/* Whether step 4 may name an account only from a configured (pinned) record:
 * once winbind has answered that the account does not exist, a record a logon
 * or a resolve left cached must not bring it back.  With no such answer --
 * winbind itself not reachable, winbind off, a null session, a lookup past one
 * of the resolve caps -- every record counts.  One limit is libwbclient's
 * (smb_wbclient_status.h): while winbindd runs, a domain controller it cannot
 * reach looks exactly like an account that does not exist, so for the length
 * of such an outage a cached logon record does not name its account either. */
static int
lsa_fallback_pinned_only(const struct chimera_smb_lsa_resolved *resolved)
{
    return resolved && resolved->status == CHIMERA_VFS_IDENTITY_NOT_FOUND;
} /* lsa_fallback_pinned_only */

/* Step 4 of lookup_sid: the local caches under the fallback domain. */
static int
lsa_fallback_by_sid(
    const struct chimera_smb_lsa_naming *naming,
    const char                          *sid,
    int                                  pinned_only,
    struct chimera_vfs_identity_account *out)
{
    struct chimera_vfs_user_cache  *cache = naming->user_cache;
    const struct chimera_vfs_user  *user;
    const struct chimera_vfs_group *group;
    char                            name[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX];
    uint32_t                        type = 0;

    if (!cache) {
        return 0;
    }

    chimera_rcu_read_lock(&cache->rcu);
    user = chimera_vfs_user_cache_lookup_by_sid(cache, sid);
    if (user && (!pinned_only || user->pinned)) {
        snprintf(name, sizeof(name), "%s", user->username);
        type = LSA_SID_NAME_USER;
    } else {
        group = chimera_vfs_group_cache_lookup_by_sid(cache, sid);
        if (group && (!pinned_only || group->pinned)) {
            snprintf(name, sizeof(name), "%s", group->groupname);
            type = LSA_SID_NAME_DOM_GRP;
        }
    }
    chimera_rcu_read_unlock(&cache->rcu);

    if (!type) {
        return 0;
    }

    lsa_account_set(out, sid, naming->fallback_domain, name, type);
    return 1;
} /* lsa_fallback_by_sid */

enum chimera_smb_lsa_rc
chimera_smb_lsa_lookup_sid(
    const struct chimera_smb_lsa_naming   *naming,
    const char                            *sid,
    int                                    may_resolve,
    const struct chimera_smb_lsa_resolved *resolved,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss)
{
    struct chimera_principal p;
    char name[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX];
    int is_user;

    if (strlen(sid) >= CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX ||
        strlen(sid) >= CHIMERA_VFS_SID_MAX_LEN) {
        return CHIMERA_SMB_LSA_UNMAPPED;
    }

    /* 1. Well-known accounts. */
    if (chimera_smb_lsa_wellknown_sid(sid, out)) {
        return CHIMERA_SMB_LSA_MAPPED;
    }

    /* 2. Unix SIDs. */
    if (strncmp(sid, LSA_MODE_SID_PREFIX, strlen(LSA_MODE_SID_PREFIX)) == 0) {
        return CHIMERA_SMB_LSA_UNMAPPED;
    }

    if (chimera_idmap_sid_to_principal(sid, &p) == 0 &&
        (p.type == CHIMERA_PRINCIPAL_USER || p.type == CHIMERA_PRINCIPAL_GROUP)) {
        is_user = p.type == CHIMERA_PRINCIPAL_USER;

        if (resolved && resolved->status == CHIMERA_VFS_IDENTITY_OK) {
            *out = resolved->account;
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (naming->user_cache &&
            (is_user ?
             lsa_user_by_uid(naming->user_cache, p.id, name, sizeof(name)) :
             lsa_group_by_gid(naming->user_cache, p.id, name, sizeof(name)))) {
            lsa_account_set(out, sid,
                            is_user ? LSA_UNIX_USER_DOMAIN : LSA_UNIX_GROUP_DOMAIN,
                            name,
                            is_user ? LSA_SID_NAME_USER : LSA_SID_NAME_DOM_GRP);
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (!resolved && may_resolve) {
            lsa_miss_set(miss,
                         is_user ? CHIMERA_VFS_IDENTITY_BY_UID :
                         CHIMERA_VFS_IDENTITY_BY_GID,
                         p.id, sid);
            return CHIMERA_SMB_LSA_MISS;
        }

        return CHIMERA_SMB_LSA_UNMAPPED;
    }

    /* 3. Winbind, when enabled. */
    if (naming->account_cache) {
        if (resolved && resolved->status == CHIMERA_VFS_IDENTITY_OK) {
            *out = resolved->account;
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (chimera_smb_account_cache_get_sid(naming->account_cache, sid, out)) {
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (!resolved && may_resolve) {
            lsa_miss_set(miss, CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT, 0, sid);
            return CHIMERA_SMB_LSA_MISS;
        }
    }

    /* 4. Fallback. */
    if (lsa_fallback_by_sid(naming, sid, lsa_fallback_pinned_only(resolved),
                            out)) {
        return CHIMERA_SMB_LSA_MAPPED;
    }

    return CHIMERA_SMB_LSA_UNMAPPED;
} /* chimera_smb_lsa_lookup_sid */

/* Step 4 of lookup_name. */
static int
lsa_fallback_by_name(
    const struct chimera_smb_lsa_naming *naming,
    const char                          *name,
    int                                  pinned_only,
    struct chimera_vfs_identity_account *out)
{
    struct chimera_vfs_user_cache *cache = naming->user_cache;
    const struct chimera_vfs_user *user;
    char                           username[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX];
    char                           sid[CHIMERA_VFS_SID_MAX_LEN];
    size_t                         dlen;
    int                            found   = 0;
    int                            has_sid = 0;
    uint32_t                       uid     = 0;

    if (!cache) {
        return 0;
    }

    dlen = strlen(naming->fallback_domain);
    if (strncasecmp(name, naming->fallback_domain, dlen) == 0 &&
        name[dlen] == '\\') {
        name += dlen + 1;
    }

    chimera_rcu_read_lock(&cache->rcu);
    user = chimera_vfs_user_cache_lookup_by_name(cache, name);
    if (user && (!pinned_only || user->pinned)) {
        snprintf(username, sizeof(username), "%s", user->username);
        snprintf(sid, sizeof(sid), "%s", user->sid);
        has_sid = user->sid[0] != '\0';
        uid     = user->uid;
        found   = 1;
    }
    chimera_rcu_read_unlock(&cache->rcu);

    if (!found) {
        return 0;
    }

    if (has_sid) {
        lsa_account_set(out, sid, naming->fallback_domain, username,
                        LSA_SID_NAME_USER);
    } else {
        snprintf(sid, sizeof(sid), "S-1-22-1-%u", uid);
        lsa_account_set(out, sid, LSA_UNIX_USER_DOMAIN, username,
                        LSA_SID_NAME_USER);
    }

    return 1;
} /* lsa_fallback_by_name */

enum chimera_smb_lsa_rc
chimera_smb_lsa_lookup_name(
    const struct chimera_smb_lsa_naming   *naming,
    const char                            *name,
    int                                    may_resolve,
    const struct chimera_smb_lsa_resolved *resolved,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss)
{
    size_t ulen = strlen(LSA_UNIX_USER_DOMAIN "\\");
    size_t glen = strlen(LSA_UNIX_GROUP_DOMAIN "\\");
    char qualified[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX];
    const char *ask;

    if (strlen(name) >= CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX) {
        return CHIMERA_SMB_LSA_UNMAPPED;
    }

    /* 1. Well-known accounts. */
    if (chimera_smb_lsa_wellknown_name(name, out)) {
        return CHIMERA_SMB_LSA_MAPPED;
    }

    /* 2. The Unix domains name no account by name. */
    if (strncasecmp(name, LSA_UNIX_USER_DOMAIN "\\", ulen) == 0 ||
        strncasecmp(name, LSA_UNIX_GROUP_DOMAIN "\\", glen) == 0) {
        return CHIMERA_SMB_LSA_UNMAPPED;
    }

    /* 3. Winbind, when enabled. */
    if (naming->account_cache) {
        /* winbind does not resolve an empty domain to the server's own, so a
         * bare name is asked as <fallback domain>\<name>; a name that
         * carries a domain goes as sent.  The same form keys the cache and
         * the miss, so pass 2 rebuilds the identical miss. */
        ask = name;
        if (!strchr(name, '\\')) {
            if (snprintf(qualified, sizeof(qualified), "%s\\%s",
                         naming->fallback_domain, name) >=
                (int) sizeof(qualified)) {
                return CHIMERA_SMB_LSA_UNMAPPED;
            }
            ask = qualified;
        }

        if (resolved && resolved->status == CHIMERA_VFS_IDENTITY_OK) {
            *out = resolved->account;
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (chimera_smb_account_cache_get_name(naming->account_cache, ask, out)) {
            return CHIMERA_SMB_LSA_MAPPED;
        }

        if (!resolved && may_resolve) {
            lsa_miss_set(miss, CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, 0, ask);
            return CHIMERA_SMB_LSA_MISS;
        }
    }

    /* 4. Fallback. */
    if (lsa_fallback_by_name(naming, name, lsa_fallback_pinned_only(resolved),
                             out)) {
        return CHIMERA_SMB_LSA_MAPPED;
    }

    return CHIMERA_SMB_LSA_UNMAPPED;
} /* chimera_smb_lsa_lookup_name */

void
chimera_smb_lsa_account_from_result(
    enum chimera_vfs_identity_key             key,
    const char                               *input,
    const struct chimera_vfs_identity_result *result,
    struct chimera_vfs_identity_account      *out)
{
    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            lsa_account_set(out, input, LSA_UNIX_USER_DOMAIN,
                            result->user.username, LSA_SID_NAME_USER);
            break;
        case CHIMERA_VFS_IDENTITY_BY_GID:
            lsa_account_set(out, input, LSA_UNIX_GROUP_DOMAIN,
                            result->group.groupname, LSA_SID_NAME_DOM_GRP);
            break;
        default:
            *out = result->account;
            break;
    } /* switch */
} /* chimera_smb_lsa_account_from_result */
