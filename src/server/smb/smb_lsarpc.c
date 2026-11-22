// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * LSARPC (MS-LSAD / MS-LSAT) on libdcerpc: policy handles, the server's domain
 * information, and SID <-> name translation for the ACL editor and friends.
 */

#include <stdio.h>
#include <string.h>

#include <dcerpc/lsa.h>

#include "smb_rpc_service.h"

/* Well-known names a standalone server resolves (MS-DTYP 2.4.2.4), matched
 * case-insensitively both bare and as DOMAIN\name.  `domain` is the authority
 * reported for them. */
struct lsa_wellknown {
    const char *name;
    const char *domain;
    const char *sid;
    enum lsa_SID_NAME_USE type;
};

static const struct lsa_wellknown lsa_wellknown[] = {
    { "Everyone",            "",                          "S-1-1-0",                          LSA_SidTypeWellKnownGroup
    },
    { "CREATOR OWNER",       "",                          "S-1-3-0",                          LSA_SidTypeWellKnownGroup
    },
    { "CREATOR GROUP",       "",                          "S-1-3-1",                          LSA_SidTypeWellKnownGroup
    },
    { "SYSTEM",              "NT AUTHORITY",              "S-1-5-18",                         LSA_SidTypeWellKnownGroup
    },
    { "ANONYMOUS LOGON",     "NT AUTHORITY",              "S-1-5-7",                          LSA_SidTypeWellKnownGroup
    },
    { "Authenticated Users", "NT AUTHORITY",              "S-1-5-11",                         LSA_SidTypeWellKnownGroup
    },
    { "Administrators",      "BUILTIN",                   "S-1-5-32-544",                     LSA_SidTypeAlias
    },
    { "Users",               "BUILTIN",                   "S-1-5-32-545",                     LSA_SidTypeAlias
    },
    { "Guests",              "BUILTIN",                   "S-1-5-32-546",                     LSA_SidTypeAlias
    },
    { "BUILTIN",             "BUILTIN",                   "S-1-5-32",                         LSA_SidTypeDomain
    },
};

#define LSA_NUM_WELLKNOWN (sizeof(lsa_wellknown) / sizeof(lsa_wellknown[0]))

/* Referenced-domain list a lookup builds; one entry per distinct domain SID. */
struct lsa_refdoms {
    struct lsa_LSAPR_TRUST_INFORMATION *dom;
    uint32_t                            count;
    uint32_t                            max;
};

/* What one name or SID resolved to. */
struct lsa_xlat {
    enum lsa_SID_NAME_USE type;
    RPC_SID     sid;                 /* the account's full SID */
    const char *name;                /* the account's name (without domain) */
    int32_t     domain;              /* index into the referenced domains, or -1 */
};

static int
chimera_smb_lsarpc_refdoms_init(
    struct dcerpc_pdu  *pdu,
    struct lsa_refdoms *rd,
    uint32_t            max)
{
    rd->count = 0;
    rd->max   = max ? max : 1;
    rd->dom   = dcerpc_alloc_data(pdu, rd->max * sizeof(*rd->dom));
    return rd->dom ? 0 : -1;
} /* chimera_smb_lsarpc_refdoms_init */

/* The friendly name of a domain SID. */
static const char *
chimera_smb_lsarpc_domain_name(
    const struct chimera_smb_rpc_state *state,
    const RPC_SID                      *dom)
{
    RPC_SID account;

    chimera_smb_rpc_account_sid(&account, state->host);

    if (chimera_smb_rpc_sid_equal(dom, &account)) {
        return chimera_smb_rpc_host_identity(state->host);
    }

    if (dom->IdentifierAuthority[5] == 22 && dom->SubAuthorityCount == 1) {
        if (dom->SubAuthority[0] == 1) {
            return "Unix User";
        }
        if (dom->SubAuthority[0] == 2) {
            return "Unix Group";
        }
    }

    if (dom->IdentifierAuthority[5] == 5) {
        if (dom->SubAuthorityCount == 1 && dom->SubAuthority[0] == 32) {
            return "BUILTIN";
        }
        if (dom->SubAuthorityCount == 0) {
            return "NT AUTHORITY";
        }
    }

    if (dom->IdentifierAuthority[5] <= 3 && dom->SubAuthorityCount == 0) {
        return "";    /* world / local / creator authorities */
    }

    return chimera_smb_rpc_host_workgroup(state->host);
} /* chimera_smb_lsarpc_domain_name */

/* Index of domain `dom` in the referenced list, adding it if new. */
static int32_t
chimera_smb_lsarpc_refdom(
    struct dcerpc_pdu                  *pdu,
    const struct chimera_smb_rpc_state *state,
    struct lsa_refdoms                 *rd,
    const RPC_SID                      *dom)
{
    uint32_t i;

    for (i = 0; i < rd->count; i++) {
        if (chimera_smb_rpc_sid_equal(rd->dom[i].Sid, dom)) {
            return (int32_t) i;
        }
    }

    if (rd->count == rd->max) {
        return -1;
    }

    rd->dom[i].Sid  = chimera_smb_rpc_sid_dup(pdu, dom);
    rd->dom[i].Name = chimera_smb_rpc_strdup(pdu, chimera_smb_lsarpc_domain_name(state, dom));

    if (!rd->dom[i].Sid || !rd->dom[i].Name) {
        return -1;
    }

    return (int32_t) rd->count++;
} /* chimera_smb_lsarpc_refdom */

/* The domain of a resolved account (its SID without the RID, or itself for a
 * domain) is entered in the referenced list. */
static void
chimera_smb_lsarpc_set_domain(
    struct dcerpc_pdu                  *pdu,
    const struct chimera_smb_rpc_state *state,
    struct lsa_refdoms                 *rd,
    struct lsa_xlat                    *x)
{
    RPC_SID dom = x->sid;

    if (x->type != LSA_SidTypeDomain && dom.SubAuthorityCount > 0) {
        dom.SubAuthorityCount--;
    }

    x->domain = chimera_smb_lsarpc_refdom(pdu, state, rd, &dom);
} /* chimera_smb_lsarpc_set_domain */

/* ---- name -> SID ---- */

static int
chimera_smb_lsarpc_name_to_sid(
    const struct chimera_smb_rpc_state *state,
    const char                         *name,
    struct lsa_xlat                    *x)
{
    const struct chimera_smb_rpc_host *host = state->host;
    struct chimera_smb_rpc_account     acct;
    const char                        *sep, *account;
    unsigned int                       i;

    memset(x, 0, sizeof(*x));
    x->type   = LSA_SidTypeUnknown;
    x->domain = -1;

    /* An empty name is the server's own (account) domain. */
    if (!name || !name[0]) {
        chimera_smb_rpc_account_sid(&x->sid, host);
        x->type = LSA_SidTypeDomain;
        x->name = chimera_smb_rpc_host_identity(host);
        return 0;
    }

    for (i = 0; i < LSA_NUM_WELLKNOWN; i++) {
        const struct lsa_wellknown *w  = &lsa_wellknown[i];
        size_t                      dl = strlen(w->domain);

        if (chimera_smb_rpc_strcasecmp(name, w->name) == 0 ||
            (dl && chimera_smb_rpc_strncasecmp(name, w->domain, dl) == 0 && name[dl] == '\\' &&
             (chimera_smb_rpc_strcasecmp(name + dl + 1, w->name) == 0 ||
              (w->type == LSA_SidTypeDomain && name[dl + 1] == '\0')))) {
            chimera_smb_rpc_string_to_sid(w->sid, &x->sid);
            x->type = w->type;
            x->name = w->name;
            return 0;
        }
    }

    /* DOMAIN\account: only our own domains qualify a local account. */
    sep     = strchr(name, '\\');
    account = name;

    if (sep) {
        size_t      dl = sep - name;
        const char *ad = chimera_smb_rpc_host_identity(host);
        const char *wg = chimera_smb_rpc_host_workgroup(host);

        if (!((strlen(ad) == dl && chimera_smb_rpc_strncasecmp(name, ad, dl) == 0) ||
              (strlen(wg) == dl && chimera_smb_rpc_strncasecmp(name, wg, dl) == 0) ||
              (dl == 9 && chimera_smb_rpc_strncasecmp(name, "Unix User", dl) == 0))) {
            return -1;
        }
        account = sep + 1;
    }

    if (chimera_smb_rpc_strcasecmp(account, chimera_smb_rpc_host_identity(host)) == 0) {
        chimera_smb_rpc_account_sid(&x->sid, host);
        x->type = LSA_SidTypeDomain;
        x->name = chimera_smb_rpc_host_identity(host);
        return 0;
    }

    if (chimera_smb_rpc_host_lookup_name(host, account, &acct) != 0 ||
        chimera_smb_rpc_string_to_sid(acct.sid, &x->sid) != 0 ||
        x->sid.SubAuthorityCount == 0) {
        return -1;
    }

    /* Name lookups report SIDs, not names. */
    x->type = acct.is_group ? LSA_SidTypeGroup : LSA_SidTypeUser;
    x->name = NULL;
    return 0;
} /* chimera_smb_lsarpc_name_to_sid */

/* Resolve `count` names; fills xlat[] and the referenced domains, returns the
 * number mapped. */
static uint32_t
chimera_smb_lsarpc_lookup_names(
    struct dcerpc_pdu            *pdu,
    struct chimera_smb_rpc_state *state,
    char                        **names,
    uint32_t                      count,
    struct lsa_xlat              *xlat,
    struct lsa_refdoms           *rd)
{
    uint32_t i, mapped = 0;

    for (i = 0; i < count; i++) {
        if (chimera_smb_lsarpc_name_to_sid(state, names ? names[i] : NULL, &xlat[i]) == 0) {
            chimera_smb_lsarpc_set_domain(pdu, state, rd, &xlat[i]);
            mapped++;
        } else {
            xlat[i].type   = LSA_SidTypeUnknown;
            xlat[i].domain = -1;
        }
    }

    return mapped;
} /* chimera_smb_lsarpc_lookup_names */

/* ---- SID -> name ---- */

static int
chimera_smb_lsarpc_sid_to_name(
    struct dcerpc_pdu                  *pdu,
    const struct chimera_smb_rpc_state *state,
    const RPC_SID                      *sid,
    struct lsa_xlat                    *x)
{
    struct chimera_smb_rpc_account acct;
    char                           sidstr[96];
    RPC_SID                        dom;
    unsigned int                   i;

    memset(x, 0, sizeof(*x));
    x->sid    = *sid;
    x->type   = LSA_SidTypeUnknown;
    x->domain = -1;

    chimera_smb_rpc_sid_to_string(sid, sidstr, sizeof(sidstr));

    for (i = 0; i < LSA_NUM_WELLKNOWN; i++) {
        if (strcmp(sidstr, lsa_wellknown[i].sid) == 0) {
            x->type = lsa_wellknown[i].type;
            x->name = lsa_wellknown[i].name;
            return 0;
        }
    }

    chimera_smb_rpc_account_sid(&dom, state->host);

    if (chimera_smb_rpc_sid_equal(sid, &dom)) {
        x->type = LSA_SidTypeDomain;
        x->name = chimera_smb_rpc_host_identity(state->host);
        return 0;
    }

    if (chimera_smb_rpc_host_lookup_sid(state->host, sidstr, &acct) != 0) {
        return -1;
    }

    x->type = acct.is_group ? LSA_SidTypeGroup : LSA_SidTypeUser;
    x->name = chimera_smb_rpc_strdup(pdu, acct.name);

    return x->name ? 0 : -1;
} /* chimera_smb_lsarpc_sid_to_name */

static uint32_t
chimera_smb_lsarpc_lookup_sids(
    struct dcerpc_pdu                      *pdu,
    struct chimera_smb_rpc_state           *state,
    const struct lsa_LSAPR_SID_ENUM_BUFFER *sids,
    struct lsa_xlat                        *xlat,
    struct lsa_refdoms                     *rd)
{
    uint32_t i, mapped = 0;
    char     sidstr[96];

    for (i = 0; i < sids->Entries; i++) {
        RPC_SID *sid = sids->SidInfo ? sids->SidInfo[i] : NULL;

        if (sid && chimera_smb_lsarpc_sid_to_name(pdu, state, sid, &xlat[i]) == 0) {
            chimera_smb_lsarpc_set_domain(pdu, state, rd, &xlat[i]);
            mapped++;
            continue;
        }

        /* MS-LSAT: an untranslated SID comes back as its string form. */
        xlat[i].type   = LSA_SidTypeUnknown;
        xlat[i].domain = -1;
        xlat[i].name   = "";

        if (sid) {
            chimera_smb_rpc_sid_to_string(sid, sidstr, sizeof(sidstr));
            xlat[i].name = chimera_smb_rpc_strdup(pdu, sidstr);
        }
    }

    return mapped;
} /* chimera_smb_lsarpc_lookup_sids */

static uint32_t
chimera_smb_lsarpc_map_status(
    uint32_t mapped,
    uint32_t count)
{
    if (count == 0 || mapped == count) {
        return CHIMERA_SMB_RPC_STATUS_OK;
    }

    return mapped ? CHIMERA_SMB_RPC_STATUS_SOME_NOT_MAPPED :
           CHIMERA_SMB_RPC_STATUS_NONE_MAPPED;
} /* chimera_smb_lsarpc_map_status */

static void
chimera_smb_lsarpc_set_refdoms(
    struct lsa_LSAPR_REFERENCED_DOMAIN_LIST *list,
    const struct lsa_refdoms                *rd)
{
    list->Entries    = rd->count;
    list->Domains    = rd->count ? rd->dom : NULL;
    list->MaxEntries = rd->count;
} /* chimera_smb_lsarpc_set_refdoms */

/* ---- policy handles ---- */

static int
chimera_smb_lsarpc_open_policy_handle(
    struct dcerpc_pdu            *pdu,
    struct dcerpc_context_handle *handle,
    uint32_t                     *status)
{
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);

    if (chimera_smb_rpc_handle_open(state, CHIMERA_SMB_RPC_HANDLE_LSA_POLICY, handle)) {
        memset(handle, 0, sizeof(*handle));
        *status = CHIMERA_SMB_RPC_STATUS_INSUFFICIENT_RESOURCES;
    } else {
        *status = CHIMERA_SMB_RPC_STATUS_OK;
    }

    return 0;
} /* chimera_smb_lsarpc_open_policy_handle */

static int
chimera_smb_lsarpc_policy_valid(
    struct dcerpc_pdu                  *pdu,
    const struct dcerpc_context_handle *handle)
{
    return chimera_smb_rpc_handle_valid(chimera_smb_rpc_state_of_pdu(pdu),
                                        CHIMERA_SMB_RPC_HANDLE_LSA_POLICY, handle);
} /* chimera_smb_lsarpc_policy_valid */

/* LsarClose (opnum 0). */
static int
chimera_smb_lsarpc_close(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_Close_req *req = reqv;
    struct lsa_Close_rep *rep = repv;

    (void) dce;

    if (!chimera_smb_rpc_handle_close(chimera_smb_rpc_state_of_pdu(pdu),
                                      CHIMERA_SMB_RPC_HANDLE_LSA_POLICY,
                                      &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    /* The reply handle stays zeroed: a closed handle. */
    rep->status = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_lsarpc_close */

/* LsarOpenPolicy (opnum 6). */
static int
chimera_smb_lsarpc_open_policy(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_OpenPolicy_rep *rep = repv;

    (void) dce;
    (void) reqv;

    return chimera_smb_lsarpc_open_policy_handle(pdu, &rep->PolicyHandle, &rep->status);
} /* chimera_smb_lsarpc_open_policy */

/* LsarOpenPolicy2 (opnum 44). */
static int
chimera_smb_lsarpc_open_policy2(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_OpenPolicy2_rep *rep = repv;

    (void) dce;
    (void) reqv;

    return chimera_smb_lsarpc_open_policy_handle(pdu, &rep->PolicyHandle, &rep->status);
} /* chimera_smb_lsarpc_open_policy2 */

/* LsarQueryInformationPolicy / LsarQueryInformationPolicy2 (opnums 7 / 46). */
static int
chimera_smb_lsarpc_query_info(
    struct dcerpc_pdu                   *pdu,
    const struct dcerpc_context_handle  *handle,
    enum lsa_POLICY_INFORMATION_CLASS    level,
    union lsa_LSAPR_POLICY_INFORMATION **info,
    uint32_t                            *status)
{
    struct chimera_smb_rpc_state       *state = chimera_smb_rpc_state_of_pdu(pdu);
    union lsa_LSAPR_POLICY_INFORMATION *pi;
    RPC_SID                             sid;
    char                               *name;

    if (!chimera_smb_lsarpc_policy_valid(pdu, handle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    pi = dcerpc_alloc_data(pdu, sizeof(*pi));

    if (!pi) {
        return -1;
    }

    chimera_smb_rpc_account_sid(&sid, state->host);

    switch (level) {
        case LSA_PolicyPrimaryDomainInformation:
            /* The workgroup the server is in, with the machine SID. */
            name                             = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_workgroup(state->host));
            pi->PolicyPrimaryDomainInfo.Name = name;
            pi->PolicyPrimaryDomainInfo.Sid  = chimera_smb_rpc_sid_dup(pdu, &sid);
            if (!name || !pi->PolicyPrimaryDomainInfo.Sid) {
                return -1;
            }
            break;
        case LSA_PolicyAccountDomainInformation:
        case LSA_PolicyLocalAccountDomainInformation:
            /* The account domain is the server's own: the name SAMR reports. */
            name = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_identity(state->
                                                                             host));
            pi->PolicyAccountDomainInfo.DomainName = name;
            pi->PolicyAccountDomainInfo.DomainSid  = chimera_smb_rpc_sid_dup(pdu, &sid);
            if (!name || !pi->PolicyAccountDomainInfo.DomainSid) {
                return -1;
            }
            break;
        case LSA_PolicyDnsDomainInformation:
        case LSA_PolicyDnsDomainInformationInt:
            /* Not in an AD domain: the workgroup, no DNS names, no SID. */
            name = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_workgroup(state->
                                                                              host));
            pi->PolicyDnsDomainInfo.Name          = name;
            pi->PolicyDnsDomainInfo.DnsDomainName = "";
            pi->PolicyDnsDomainInfo.DnsForestName = "";
            if (!name) {
                return -1;
            }
            break;
        case LSA_PolicyLsaServerRoleInformation:
            pi->PolicyServerRoleInfo.LsaServerRole = LSA_PolicyServerRolePrimary;
            break;
        default:
            *info   = NULL;
            *status = CHIMERA_SMB_RPC_STATUS_INVALID_PARAMETER;
            return 0;
    } /* switch */

    *info   = pi;
    *status = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_lsarpc_query_info */

static int
chimera_smb_lsarpc_query_info_policy(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_QueryInformationPolicy_req *req = reqv;
    struct lsa_QueryInformationPolicy_rep *rep = repv;

    (void) dce;

    return chimera_smb_lsarpc_query_info(pdu, &req->PolicyHandle, req->InformationClass,
                                         &rep->PolicyInformation, &rep->status);
} /* chimera_smb_lsarpc_query_info_policy */

static int
chimera_smb_lsarpc_query_info_policy2(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_QueryInformationPolicy2_req *req = reqv;
    struct lsa_QueryInformationPolicy2_rep *rep = repv;

    (void) dce;

    return chimera_smb_lsarpc_query_info(pdu, &req->PolicyHandle, req->InformationClass,
                                         &rep->PolicyInformation, &rep->status);
} /* chimera_smb_lsarpc_query_info_policy2 */

/* LsarEnumerateTrustedDomains (opnum 13): a standalone server trusts nobody. */
static int
chimera_smb_lsarpc_enum_trusted_domains(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_EnumerateTrustedDomains_req *req = reqv;
    struct lsa_EnumerateTrustedDomains_rep *rep = repv;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    rep->status = CHIMERA_SMB_RPC_STATUS_NO_MORE_ENTRIES;
    return 0;
} /* chimera_smb_lsarpc_enum_trusted_domains */

/* LsarGetUserName (opnum 45): the account the caller is authenticated as. */
static int
chimera_smb_lsarpc_get_user_name(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_GetUserName_req    *req   = reqv;
    struct lsa_GetUserName_rep    *rep   = repv;
    struct chimera_smb_rpc_state  *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct chimera_smb_rpc_account acct;
    const char                    *user;

    (void) dce;

    if (chimera_smb_rpc_host_lookup_uid(state->host, chimera_smb_rpc_host_uid(state->host),
                                        &acct) != 0) {
        snprintf(acct.name, sizeof(acct.name), "%u", chimera_smb_rpc_host_uid(state->host));
    }
    user = acct.name;

    rep->UserName.Name = dcerpc_alloc_data(pdu, sizeof(char *));

    if (!rep->UserName.Name) {
        return -1;
    }

    *rep->UserName.Name = chimera_smb_rpc_strdup(pdu, user);

    if (!*rep->UserName.Name) {
        return -1;
    }

    if (req->DomainName) {
        rep->DomainName = dcerpc_alloc_data(pdu, sizeof(*rep->DomainName));

        if (!rep->DomainName) {
            return -1;
        }

        rep->DomainName->Name = dcerpc_alloc_data(pdu, sizeof(char *));

        if (!rep->DomainName->Name) {
            return -1;
        }

        *rep->DomainName->Name = chimera_smb_rpc_strdup(
            pdu, chimera_smb_rpc_host_identity(state->host));

        if (!*rep->DomainName->Name) {
            return -1;
        }
    }

    rep->status = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_lsarpc_get_user_name */

/* LsarLookupNames (opnum 14). */
static int
chimera_smb_lsarpc_lookup_names_v1(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_LookupNames_req   *req   = reqv;
    struct lsa_LookupNames_rep   *rep   = repv;
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct lsa_xlat              *xlat;
    struct lsa_refdoms            rd;
    uint32_t                      i, n = req->Count, mapped;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    xlat                     = dcerpc_alloc_data(pdu, (n ? n : 1) * sizeof(*xlat));
    rep->TranslatedSids.Sids = dcerpc_alloc_data(pdu, (n ? n : 1) *
                                                 sizeof(*rep->TranslatedSids.Sids));

    if (!xlat || !rep->TranslatedSids.Sids || chimera_smb_lsarpc_refdoms_init(pdu, &rd, n)) {
        return -1;
    }

    mapped = chimera_smb_lsarpc_lookup_names(pdu, state, req->Names, n, xlat, &rd);

    for (i = 0; i < n; i++) {
        rep->TranslatedSids.Sids[i].Use         = xlat[i].type;
        rep->TranslatedSids.Sids[i].DomainIndex = xlat[i].domain;
        if (xlat[i].domain < 0) {
            rep->TranslatedSids.Sids[i].RelativeId = 0;
        } else if (xlat[i].type == LSA_SidTypeDomain) {
            rep->TranslatedSids.Sids[i].RelativeId = 0xffffffff;
        } else {
            rep->TranslatedSids.Sids[i].RelativeId =
                xlat[i].sid.SubAuthority[xlat[i].sid.SubAuthorityCount - 1];
        }
    }

    rep->TranslatedSids.Entries = n;
    if (!n) {
        rep->TranslatedSids.Sids = NULL;
    }
    chimera_smb_lsarpc_set_refdoms(&rep->ReferencedDomains, &rd);
    rep->MappedCount = mapped;
    rep->status      = chimera_smb_lsarpc_map_status(mapped, n);
    return 0;
} /* chimera_smb_lsarpc_lookup_names_v1 */

/* LsarLookupNames2 (opnum 58). */
static int
chimera_smb_lsarpc_lookup_names2(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_LookupNames2_req  *req   = reqv;
    struct lsa_LookupNames2_rep  *rep   = repv;
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct lsa_xlat              *xlat;
    struct lsa_refdoms            rd;
    uint32_t                      i, n = req->Count, mapped;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    xlat                     = dcerpc_alloc_data(pdu, (n ? n : 1) * sizeof(*xlat));
    rep->TranslatedSids.Sids = dcerpc_alloc_data(pdu, (n ? n : 1) *
                                                 sizeof(*rep->TranslatedSids.Sids));

    if (!xlat || !rep->TranslatedSids.Sids || chimera_smb_lsarpc_refdoms_init(pdu, &rd, n)) {
        return -1;
    }

    mapped = chimera_smb_lsarpc_lookup_names(pdu, state, req->Names, n, xlat, &rd);

    for (i = 0; i < n; i++) {
        rep->TranslatedSids.Sids[i].Use         = xlat[i].type;
        rep->TranslatedSids.Sids[i].DomainIndex = xlat[i].domain;
        if (xlat[i].domain < 0) {
            rep->TranslatedSids.Sids[i].RelativeId = 0;
        } else if (xlat[i].type == LSA_SidTypeDomain) {
            rep->TranslatedSids.Sids[i].RelativeId = 0xffffffff;
        } else {
            rep->TranslatedSids.Sids[i].RelativeId =
                xlat[i].sid.SubAuthority[xlat[i].sid.SubAuthorityCount - 1];
        }
    }

    rep->TranslatedSids.Entries = n;
    if (!n) {
        rep->TranslatedSids.Sids = NULL;
    }
    chimera_smb_lsarpc_set_refdoms(&rep->ReferencedDomains, &rd);
    rep->MappedCount = mapped;
    rep->status      = chimera_smb_lsarpc_map_status(mapped, n);
    return 0;
} /* chimera_smb_lsarpc_lookup_names2 */

/* LsarLookupNames3 (opnum 68): the full account SID per entry. */
static int
chimera_smb_lsarpc_lookup_names3(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_LookupNames3_req  *req   = reqv;
    struct lsa_LookupNames3_rep  *rep   = repv;
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct lsa_xlat              *xlat;
    struct lsa_refdoms            rd;
    uint32_t                      i, n = req->Count, mapped;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    xlat                     = dcerpc_alloc_data(pdu, (n ? n : 1) * sizeof(*xlat));
    rep->TranslatedSids.Sids = dcerpc_alloc_data(pdu, (n ? n : 1) *
                                                 sizeof(*rep->TranslatedSids.Sids));

    if (!xlat || !rep->TranslatedSids.Sids || chimera_smb_lsarpc_refdoms_init(pdu, &rd, n)) {
        return -1;
    }

    mapped = chimera_smb_lsarpc_lookup_names(pdu, state, req->Names, n, xlat, &rd);

    for (i = 0; i < n; i++) {
        rep->TranslatedSids.Sids[i].Use         = xlat[i].type;
        rep->TranslatedSids.Sids[i].DomainIndex = xlat[i].domain;
        if (xlat[i].domain >= 0) {
            rep->TranslatedSids.Sids[i].Sid = chimera_smb_rpc_sid_dup(pdu, &xlat[i].sid);
            if (!rep->TranslatedSids.Sids[i].Sid) {
                return -1;
            }
        }
    }

    rep->TranslatedSids.Entries = n;
    if (!n) {
        rep->TranslatedSids.Sids = NULL;
    }
    chimera_smb_lsarpc_set_refdoms(&rep->ReferencedDomains, &rd);
    rep->MappedCount = mapped;
    rep->status      = chimera_smb_lsarpc_map_status(mapped, n);
    return 0;
} /* chimera_smb_lsarpc_lookup_names3 */

/* LsarLookupSids (opnum 15). */
static int
chimera_smb_lsarpc_lookup_sids_v1(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_LookupSids_req    *req   = reqv;
    struct lsa_LookupSids_rep    *rep   = repv;
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct lsa_xlat              *xlat;
    struct lsa_refdoms            rd;
    uint32_t                      i, n = req->SidEnumBuffer.Entries, mapped;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    xlat                       = dcerpc_alloc_data(pdu, (n ? n : 1) * sizeof(*xlat));
    rep->TranslatedNames.Names = dcerpc_alloc_data(pdu, (n ? n : 1) *
                                                   sizeof(*rep->TranslatedNames.Names));

    if (!xlat || !rep->TranslatedNames.Names || chimera_smb_lsarpc_refdoms_init(pdu, &rd, n)) {
        return -1;
    }

    mapped = chimera_smb_lsarpc_lookup_sids(pdu, state, &req->SidEnumBuffer, xlat, &rd);

    for (i = 0; i < n; i++) {
        rep->TranslatedNames.Names[i].Use         = xlat[i].type;
        rep->TranslatedNames.Names[i].Name        = (char *) xlat[i].name;
        rep->TranslatedNames.Names[i].DomainIndex = xlat[i].domain;
    }

    rep->TranslatedNames.Entries = n;
    if (!n) {
        rep->TranslatedNames.Names = NULL;
    }
    chimera_smb_lsarpc_set_refdoms(&rep->ReferencedDomains, &rd);
    rep->MappedCount = mapped;
    rep->status      = chimera_smb_lsarpc_map_status(mapped, n);
    return 0;
} /* chimera_smb_lsarpc_lookup_sids_v1 */

/* LsarLookupSids2 (opnum 57). */
static int
chimera_smb_lsarpc_lookup_sids2(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct lsa_LookupSids2_req   *req   = reqv;
    struct lsa_LookupSids2_rep   *rep   = repv;
    struct chimera_smb_rpc_state *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct lsa_xlat              *xlat;
    struct lsa_refdoms            rd;
    uint32_t                      i, n = req->SidEnumBuffer.Entries, mapped;

    (void) dce;

    if (!chimera_smb_lsarpc_policy_valid(pdu, &req->PolicyHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    xlat                       = dcerpc_alloc_data(pdu, (n ? n : 1) * sizeof(*xlat));
    rep->TranslatedNames.Names = dcerpc_alloc_data(pdu, (n ? n : 1) *
                                                   sizeof(*rep->TranslatedNames.Names));

    if (!xlat || !rep->TranslatedNames.Names || chimera_smb_lsarpc_refdoms_init(pdu, &rd, n)) {
        return -1;
    }

    mapped = chimera_smb_lsarpc_lookup_sids(pdu, state, &req->SidEnumBuffer, xlat, &rd);

    for (i = 0; i < n; i++) {
        rep->TranslatedNames.Names[i].Use         = xlat[i].type;
        rep->TranslatedNames.Names[i].Name        = (char *) xlat[i].name;
        rep->TranslatedNames.Names[i].DomainIndex = xlat[i].domain;
    }

    rep->TranslatedNames.Entries = n;
    if (!n) {
        rep->TranslatedNames.Names = NULL;
    }
    chimera_smb_lsarpc_set_refdoms(&rep->ReferencedDomains, &rd);
    rep->MappedCount = mapped;
    rep->status      = chimera_smb_lsarpc_map_status(mapped, n);
    return 0;
} /* chimera_smb_lsarpc_lookup_sids2 */

static const struct dcerpc_stub chimera_smb_lsarpc_stubs[] = {
    { LSA_CLOSE,                   "Close",                       chimera_smb_lsarpc_close
    },
    { LSA_OPENPOLICY,              "OpenPolicy",                  chimera_smb_lsarpc_open_policy
    },
    { LSA_QUERYINFORMATIONPOLICY,  "QueryInformationPolicy",      chimera_smb_lsarpc_query_info_policy
    },
    { LSA_ENUMERATETRUSTEDDOMAINS, "EnumerateTrustedDomains",     chimera_smb_lsarpc_enum_trusted_domains
    },
    { LSA_LOOKUPNAMES,             "LookupNames",                 chimera_smb_lsarpc_lookup_names_v1
    },
    { LSA_LOOKUPSIDS,              "LookupSids",                  chimera_smb_lsarpc_lookup_sids_v1
    },
    { LSA_OPENPOLICY2,             "OpenPolicy2",                 chimera_smb_lsarpc_open_policy2
    },
    { LSA_GETUSERNAME,             "GetUserName",                 chimera_smb_lsarpc_get_user_name
    },
    { LSA_QUERYINFORMATIONPOLICY2, "QueryInformationPolicy2",     chimera_smb_lsarpc_query_info_policy2
    },
    { LSA_LOOKUPSIDS2,             "LookupSids2",                 chimera_smb_lsarpc_lookup_sids2
    },
    { LSA_LOOKUPNAMES2,            "LookupNames2",                chimera_smb_lsarpc_lookup_names2
    },
    { LSA_LOOKUPNAMES3,            "LookupNames3",                chimera_smb_lsarpc_lookup_names3
    },
    { -1,                          NULL,                          NULL
    }
};

struct dcerpc_server *
chimera_smb_lsarpc_endpoint(void)
{
    struct dcerpc_server *srv = dcerpc_server_create("lsarpc");

    if (srv && dcerpc_server_add_interface(srv, &lsa_interface, lsa_procs,
                                           chimera_smb_lsarpc_stubs,
                                           DCERPC_SERVER_ALL_NDR)) {
        dcerpc_server_destroy(srv);
        srv = NULL;
    }

    return srv;
} /* chimera_smb_lsarpc_endpoint */
