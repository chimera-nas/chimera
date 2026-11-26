// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit test for the LSARPC naming rules: the well-known table, Unix SIDs, the
 * winbind and fallback steps, and the resolver-result conversion.
 */

#include <stdio.h>
#include <string.h>

#include "common/chimera_rcu.h"
#include "vfs/vfs_user_cache.h"
#include "smb_lsarpc_names.h"

static int failures;

static void
report(
    const char *name,
    int         ok)
{
    if (ok) {
        printf("PASS: %s\n", name);
    } else {
        printf("FAIL: %s\n", name);
        failures++;
    }
} /* report */

static int
is_acct(
    const struct chimera_vfs_identity_account *a,
    const char                                *domain,
    const char                                *name,
    uint32_t                                   type)
{
    return strcmp(a->domain, domain) == 0 && strcmp(a->name, name) == 0 &&
           a->sid_type == type;
} /* is_acct */

static enum chimera_smb_lsa_rc
sid_lookup(
    const struct chimera_smb_lsa_naming   *n,
    const char                            *sid,
    int                                    may,
    const struct chimera_smb_lsa_resolved *res,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss)
{
    memset(out, 0, sizeof(*out));
    memset(miss, 0, sizeof(*miss));
    return chimera_smb_lsa_lookup_sid(n, sid, may, res, out, miss);
} /* sid_lookup */

static enum chimera_smb_lsa_rc
name_lookup(
    const struct chimera_smb_lsa_naming   *n,
    const char                            *name,
    int                                    may,
    const struct chimera_smb_lsa_resolved *res,
    struct chimera_vfs_identity_account   *out,
    struct chimera_smb_lsa_miss           *miss)
{
    memset(out, 0, sizeof(*out));
    memset(miss, 0, sizeof(*miss));
    return chimera_smb_lsa_lookup_name(n, name, may, res, out, miss);
} /* name_lookup */

static void
test_fallback_and_split(void)
{
    char d[64], n[64], small[4];

    report("fallback domain: the configured one, else WORKGROUP",
           strcmp(chimera_smb_lsa_fallback_domain(NULL, NULL), "WORKGROUP") == 0 &&
           strcmp(chimera_smb_lsa_fallback_domain("", ""), "WORKGROUP") == 0 &&
           strcmp(chimera_smb_lsa_fallback_domain(NULL, "EXAMPLE"), "EXAMPLE") == 0);

    report("fallback domain: the joined domain comes first",
           strcmp(chimera_smb_lsa_fallback_domain("CORP", NULL), "CORP") == 0 &&
           strcmp(chimera_smb_lsa_fallback_domain("CORP", ""), "CORP") == 0 &&
           strcmp(chimera_smb_lsa_fallback_domain("CORP", "EXAMPLE"), "CORP") == 0);

    report("split names at the first backslash",
           chimera_smb_lsa_split_name("WBSTUB\\user1002", d, sizeof(d), n, sizeof(n)) == 0 &&
           strcmp(d, "WBSTUB") == 0 && strcmp(n, "user1002") == 0 &&
           chimera_smb_lsa_split_name("user1002", d, sizeof(d), n, sizeof(n)) == 0 &&
           d[0] == '\0' && strcmp(n, "user1002") == 0 &&
           chimera_smb_lsa_split_name("A\\B\\C", d, sizeof(d), n, sizeof(n)) == 0 &&
           strcmp(d, "A") == 0 && strcmp(n, "B\\C") == 0 &&
           chimera_smb_lsa_split_name("LONGDOMAIN\\x", small, sizeof(small), n, sizeof(n)) == -1);
} /* test_fallback_and_split */

static void
test_wellknown(void)
{
    /* *INDENT-OFF* */
    static const struct {
        const char *sid, *domain, *name;
        uint32_t    type;
    } rows[] = {
        { "S-1-1-0",      "",              "Everyone",             5 },
        { "S-1-3-0",      "",              "CREATOR OWNER",        5 },
        { "S-1-3-1",      "",              "CREATOR GROUP",        5 },
        { "S-1-3-4",      "",              "OWNER RIGHTS",         5 },
        { "S-1-5-2",      "NT AUTHORITY",  "NETWORK",              5 },
        { "S-1-5-4",      "NT AUTHORITY",  "INTERACTIVE",          5 },
        { "S-1-5-7",      "NT AUTHORITY",  "ANONYMOUS LOGON",      5 },
        { "S-1-5-11",     "NT AUTHORITY",  "Authenticated Users",  5 },
        { "S-1-5-18",     "NT AUTHORITY",  "SYSTEM",               5 },
        { "S-1-5-32-544", "BUILTIN",       "Administrators",       4 },
        { "S-1-5-32-545", "BUILTIN",       "Users",                4 },
        { "S-1-5-32",     "BUILTIN",       "BUILTIN",              3 },
    };
    /* *INDENT-ON* */
    struct chimera_vfs_identity_account a;
    size_t                              i;
    int                                 ok = 1;
    char                                buf[300];

    for (i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        memset(&a, 0, sizeof(a));
        ok = ok && chimera_smb_lsa_wellknown_sid(rows[i].sid, &a) == 1 &&
            strcmp(a.sid, rows[i].sid) == 0 &&
            is_acct(&a, rows[i].domain, rows[i].name, rows[i].type);

        /* Every row also resolves by its bare name. */
        memset(&a, 0, sizeof(a));
        ok = ok && chimera_smb_lsa_wellknown_name(rows[i].name, &a) == 1 &&
            strcmp(a.sid, rows[i].sid) == 0 && a.sid_type == rows[i].type;

        if (rows[i].domain[0]) {
            snprintf(buf, sizeof(buf), "%s\\%s", rows[i].domain, rows[i].name);
            memset(&a, 0, sizeof(a));
            ok = ok && chimera_smb_lsa_wellknown_name(buf, &a) == 1 &&
                strcmp(a.sid, rows[i].sid) == 0 && a.sid_type == rows[i].type;
        }
    }
    report("well-known table, both directions",
           ok && sizeof(rows) / sizeof(rows[0]) == 12);

    report("an unrelated sid is not well-known",
           chimera_smb_lsa_wellknown_sid("S-1-5-21-7-8-9-1000", &a) == 0);

    ok = chimera_smb_lsa_wellknown_name("everyone", &a) == 1 && strcmp(a.sid, "S-1-1-0") == 0 &&
        chimera_smb_lsa_wellknown_name("SYSTEM", &a) == 1 && strcmp(a.sid, "S-1-5-18") == 0 &&
        chimera_smb_lsa_wellknown_name("nt authority\\system", &a) == 1 && strcmp(a.sid, "S-1-5-18") == 0 &&
        chimera_smb_lsa_wellknown_name("Administrators", &a) == 1 && strcmp(a.sid, "S-1-5-32-544") == 0 &&
        chimera_smb_lsa_wellknown_name("BUILTIN\\Administrators", &a) == 1 && strcmp(a.sid, "S-1-5-32-544") == 0 &&
        chimera_smb_lsa_wellknown_name("BUILTIN", &a) == 1 && strcmp(a.sid, "S-1-5-32") == 0 && a.sid_type == 3 &&
        chimera_smb_lsa_wellknown_name("BUILTIN\\", &a) == 1 && strcmp(a.sid, "S-1-5-32") == 0 && a.sid_type == 3;
    report("well-known names resolve in every accepted form", ok);

    report("a different domain prefix does not match",
           chimera_smb_lsa_wellknown_name("WORKGROUP\\SYSTEM", &a) == 0 &&
           chimera_smb_lsa_wellknown_name("NT AUTHORITY\\Everyone", &a) == 0);
} /* test_wellknown */

int
main(void)
{
    struct chimera_vfs_user_cache      *users;
    struct chimera_smb_account_cache   *acache;
    struct chimera_smb_lsa_naming       off, on;
    struct chimera_vfs_identity_account a, wa;
    struct chimera_smb_lsa_miss         miss;
    struct chimera_smb_lsa_resolved     res;
    struct chimera_vfs_identity_result  result;
    const char                         *uSid      = "S-1-5-21-7-8-9-1000";
    const char                         *goneSid   = "S-1-5-21-7-8-9-1500";
    const char                         *goneGroup = "S-1-5-21-7-8-9-2500";
    char                                longname[301];
    char                                longsid[CHIMERA_VFS_SID_MAX_LEN + 1];
    uint32_t                            gids[1] = { 2001 };

    chimera_rcu_register_thread();

    users  = chimera_vfs_user_cache_create(64, 600);
    acache = chimera_smb_account_cache_create(16, 600);

    /* Configured (pinned) accounts. */
    chimera_vfs_user_cache_add(users, "lsauser", "", "", uSid, 1001, 2001, 1, gids, 1);
    chimera_vfs_user_cache_add(users, "smbtest", "", "", NULL, 1000, 1000, 0, NULL, 1);
    chimera_vfs_group_cache_add(users, "lsagroup", "S-1-5-21-7-8-9-2001", 2001, 1);

    /* Learned ones (unpinned): what a logon or a resolve leaves cached. */
    chimera_vfs_user_cache_add(users, "gone", "", "", goneSid, 1500, 2001, 0, NULL, 0);
    chimera_vfs_group_cache_add(users, "gonegroup", goneGroup, 2500, 0);

    off = (struct chimera_smb_lsa_naming) { users, NULL, "WORKGROUP" };
    on  = (struct chimera_smb_lsa_naming) { users, acache, "EXAMPLE" };

    test_fallback_and_split();
    test_wellknown();

    /* unix sids */
    report("unix user sid names from the user cache",
           sid_lookup(&off, "S-1-22-1-1000", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "Unix User", "smbtest", 1) && strcmp(a.sid, "S-1-22-1-1000") == 0);
    report("unix group sid names from the group cache",
           sid_lookup(&off, "S-1-5-88-2-2001", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "Unix Group", "lsagroup", 2));
    report("an uncached uid asks by id",
           sid_lookup(&off, "S-1-22-1-4242", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_BY_UID && miss.id == 4242 &&
           strcmp(miss.input, "S-1-22-1-4242") == 0);
    report("an uncached gid asks by id",
           sid_lookup(&off, "S-1-22-2-4243", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_BY_GID && miss.id == 4243);
    report("an uncached uid without may_resolve is unmapped",
           sid_lookup(&off, "S-1-22-1-4242", 0, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    res.status = CHIMERA_VFS_IDENTITY_NOT_FOUND;
    report("an uncached uid with a negative answer is unmapped",
           sid_lookup(&off, "S-1-22-1-4242", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    memset(&res, 0, sizeof(res));
    res.status = CHIMERA_VFS_IDENTITY_OK;
    snprintf(res.account.sid, sizeof(res.account.sid), "S-1-22-1-4242");
    snprintf(res.account.domain, sizeof(res.account.domain), "Unix User");
    snprintf(res.account.name, sizeof(res.account.name), "resolved");
    res.account.sid_type = 1;
    report("a resolved uid answer wins",
           sid_lookup(&off, "S-1-22-1-4242", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "Unix User", "resolved", 1));
    report("a mode sid is unmapped",
           sid_lookup(&off, "S-1-5-88-3-420", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED &&
           sid_lookup(&on, "S-1-5-88-3-420", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    report("a well-known sid missing from the table continues to winbind or unmapped",
           sid_lookup(&on, "S-1-5-10", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT && strcmp(miss.input, "S-1-5-10") == 0 &&
           sid_lookup(&off, "S-1-5-10", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);

    /* without winbind */
    report("without winbind a user sid names under the fallback domain",
           sid_lookup(&off, uSid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "WORKGROUP", "lsauser", 1));
    report("without winbind a group sid names under the fallback domain",
           sid_lookup(&off, "S-1-5-21-7-8-9-2001", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "WORKGROUP", "lsagroup", 2));
    report("without winbind an unknown sid is unmapped",
           sid_lookup(&off, "S-1-5-21-7-8-9-4242", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);

    /* with winbind */
    report("with winbind an uncached sid asks winbind",
           sid_lookup(&on, uSid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT && strcmp(miss.input, uSid) == 0);
    memset(&res, 0, sizeof(res));
    res.status = CHIMERA_VFS_IDENTITY_OK;
    snprintf(res.account.sid, sizeof(res.account.sid), "%s", uSid);
    snprintf(res.account.domain, sizeof(res.account.domain), "EXAMPLE");
    snprintf(res.account.name, sizeof(res.account.name), "LsaUser");
    res.account.sid_type = 1;
    report("winbind's answer wins",
           sid_lookup(&on, uSid, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "LsaUser", 1));
    res.status = CHIMERA_VFS_IDENTITY_NOT_FOUND;
    report("a negative winbind answer still names a configured user",
           sid_lookup(&on, uSid, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "lsauser", 1));
    report("a negative winbind answer still names a configured group",
           sid_lookup(&on, "S-1-5-21-7-8-9-2001", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "lsagroup", 2));
    report("a negative winbind answer leaves a learned account unmapped",
           sid_lookup(&on, goneSid, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED &&
           sid_lookup(&on, goneGroup, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED &&
           name_lookup(&on, "gone", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED &&
           name_lookup(&on, "EXAMPLE\\gone", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    res.status = CHIMERA_VFS_IDENTITY_UNAVAILABLE;
    report("an unavailable winbind names a learned account from the caches",
           sid_lookup(&on, goneSid, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "gone", 1) &&
           sid_lookup(&on, goneGroup, 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "gonegroup", 2) &&
           name_lookup(&on, "gone", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, goneSid) == 0 && is_acct(&a, "EXAMPLE", "gone", 1));
    report("with no winbind answer a learned account names from the caches",
           sid_lookup(&on, goneSid, 0, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "gone", 1) &&
           name_lookup(&on, "gone", 0, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, goneSid) == 0 &&
           sid_lookup(&off, goneSid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "WORKGROUP", "gone", 1));
    report("an unavailable winbind and no cache entry is unmapped",
           sid_lookup(&on, "S-1-5-21-7-8-9-4242", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    memset(&wa, 0, sizeof(wa));
    snprintf(wa.sid, sizeof(wa.sid), "%s", uSid);
    snprintf(wa.domain, sizeof(wa.domain), "EXAMPLE");
    snprintf(wa.name, sizeof(wa.name), "LsaUser");
    wa.sid_type = 1;
    chimera_smb_account_cache_put(acache, &wa, NULL);
    report("an account-cache hit answers",
           sid_lookup(&on, uSid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "LsaUser", 1));

    /* names without winbind */
    report("names without winbind",
           name_lookup(&off, "lsauser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, uSid) == 0 && is_acct(&a, "WORKGROUP", "lsauser", 1) &&
           name_lookup(&off, "WORKGROUP\\lsauser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, uSid) == 0 &&
           name_lookup(&off, "workgroup\\lsauser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, uSid) == 0 && is_acct(&a, "WORKGROUP", "lsauser", 1));
    report("another domain prefix is unmapped",
           name_lookup(&off, "OTHER\\lsauser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    report("a sid-less user names as a unix user",
           name_lookup(&off, "smbtest", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, "S-1-22-1-1000") == 0 && is_acct(&a, "Unix User", "smbtest", 1));
    report("the Unix domains name no account by name",
           name_lookup(&off, "Unix User\\smbtest", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED &&
           name_lookup(&on, "unix group\\lsagroup", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    report("a well-known name resolves first",
           name_lookup(&on, "Everyone", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, "S-1-1-0") == 0);

    /* names with winbind */
    report("names with winbind ask winbind",
           name_lookup(&on, "user1002", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT && strcmp(miss.input, "EXAMPLE\\user1002") == 0 &&
           name_lookup(&on, "user1002", 0, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    report("a qualified name keeps its own domain",
           name_lookup(&on, "OTHER\\x", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT && strcmp(miss.input, "OTHER\\x") == 0);
    {
        struct chimera_vfs_identity_account cached;

        memset(&cached, 0, sizeof(cached));
        snprintf(cached.sid, sizeof(cached.sid), "S-1-5-21-1-2-3-1003");
        snprintf(cached.domain, sizeof(cached.domain), "EXAMPLE");
        snprintf(cached.name, sizeof(cached.name), "bareuser");
        cached.sid_type = 1;
        chimera_smb_account_cache_put(acache, &cached, NULL);
        report("a bare name hits the account cache through the qualified key",
               name_lookup(&on, "bareuser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
               is_acct(&a, "EXAMPLE", "bareuser", 1));
    }
    memset(&res, 0, sizeof(res));
    res.status = CHIMERA_VFS_IDENTITY_OK;
    snprintf(res.account.sid, sizeof(res.account.sid), "S-1-5-21-1-2-3-1002");
    snprintf(res.account.domain, sizeof(res.account.domain), "WBSTUB");
    snprintf(res.account.name, sizeof(res.account.name), "user1002");
    res.account.sid_type = 1;
    report("winbind's name answer wins",
           name_lookup(&on, "user1002", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, "S-1-5-21-1-2-3-1002") == 0 && is_acct(&a, "WBSTUB", "user1002", 1));
    report("a cached account answers by name",
           name_lookup(&on, "EXAMPLE\\lsauser", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           is_acct(&a, "EXAMPLE", "LsaUser", 1));
    res.status = CHIMERA_VFS_IDENTITY_NOT_FOUND;
    report("a negative name answer still names a configured user",
           name_lookup(&on, "EXAMPLE\\smbtest", 0, &res, &a, &miss) == CHIMERA_SMB_LSA_MAPPED &&
           strcmp(a.sid, "S-1-22-1-1000") == 0);

    /* over-long input: a name must fit an account name buffer and a SID
     * string a SID buffer, each with its terminator */
    snprintf(longname, sizeof(longname), "OTHER\\");
    memset(longname + 6, 'a', CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX - 1 - 6);
    longname[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX - 1] = '\0';
    report("the longest name that fits (255 bytes) is not refused for its length",
           name_lookup(&on, longname, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT &&
           strcmp(miss.input, longname) == 0);
    memset(longname, 'a', CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX - 1);
    longname[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX - 1] = '\0';
    report("a bare name whose qualified form overflows is unmapped",
           name_lookup(&on, longname, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    memset(longname, 'a', CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX);
    longname[CHIMERA_VFS_IDENTITY_ACCOUNT_NAME_MAX] = '\0';
    report("a name one byte longer (256 bytes) is unmapped",
           name_lookup(&on, longname, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    memset(longname, 'a', 300);
    longname[300] = '\0';
    report("an over-long name is unmapped",
           name_lookup(&on, longname, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    longname[0] = 'S';
    report("an over-long sid is unmapped",
           sid_lookup(&on, longname, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);
    memset(longsid, '1', CHIMERA_VFS_SID_MAX_LEN - 1);
    memcpy(longsid, "S-1-5-21-", strlen("S-1-5-21-"));
    longsid[CHIMERA_VFS_SID_MAX_LEN - 1] = '\0';
    report("the longest sid string that fits (79 bytes) is not refused for its length",
           sid_lookup(&on, longsid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           miss.key == CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT &&
           strcmp(miss.input, longsid) == 0);
    memset(longsid, '1', CHIMERA_VFS_SID_MAX_LEN);
    memcpy(longsid, "S-1-5-21-", strlen("S-1-5-21-"));
    longsid[CHIMERA_VFS_SID_MAX_LEN] = '\0';
    report("a sid string one byte longer (80 bytes) is unmapped",
           sid_lookup(&on, longsid, 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_UNMAPPED);

    /* lookups are repeatable */
    report("the same lookup twice gives the same answer",
           name_lookup(&on, "user1002", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS &&
           name_lookup(&on, "user1002", 1, NULL, &a, &miss) == CHIMERA_SMB_LSA_MISS);

    /* resolver results */
    memset(&result, 0, sizeof(result));
    snprintf(result.user.username, sizeof(result.user.username), "smbtest");
    chimera_smb_lsa_account_from_result(CHIMERA_VFS_IDENTITY_BY_UID, "S-1-22-1-1000", &result, &a);
    report("BY_UID result becomes a unix user",
           strcmp(a.sid, "S-1-22-1-1000") == 0 && is_acct(&a, "Unix User", "smbtest", 1));
    memset(&result, 0, sizeof(result));
    result.is_group = 1;
    snprintf(result.group.groupname, sizeof(result.group.groupname), "lsagroup");
    chimera_smb_lsa_account_from_result(CHIMERA_VFS_IDENTITY_BY_GID, "S-1-5-88-2-2001", &result, &a);
    report("BY_GID result becomes a unix group",
           strcmp(a.sid, "S-1-5-88-2-2001") == 0 && is_acct(&a, "Unix Group", "lsagroup", 2));
    memset(&result, 0, sizeof(result));
    result.account = wa;
    chimera_smb_lsa_account_from_result(CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT, "x", &result, &a);
    report("an account key copies the account",
           strcmp(a.sid, uSid) == 0 && is_acct(&a, "EXAMPLE", "LsaUser", 1));

    chimera_smb_account_cache_destroy(acache);
    chimera_vfs_user_cache_destroy(users);

    return failures ? 1 : 0;
} /* main */
