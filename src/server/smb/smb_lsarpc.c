// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_lsarpc.h"
#include "smb_dcerpc.h"
#include "lsarpc_ndr.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_idmap.h"
#include <complex.h>
#include <time.h>

static const dce_if_uuid_t LSA_INTERFACE = {
    .if_uuid       = { 0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xCD, 0xAB, 0xEF, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab },
    .if_vers_major = 0,
    .if_vers_minor = 0,
};

#define LSA_OP_CLOSE                      0
#define LSA_OP_OPENPOLICY                 6
#define LSA_OP_QUERYINFOPOLICY            7
#define LSA_OP_ENUMTRUSTDOM               13
#define LSA_OP_LOOKUPNAMES                14
#define LSA_OP_LOOKUPSIDS                 15

#define LSA_STATUS_NO_MORE_ENTRIES        0x8000001A
#define LSA_STATUS_SOME_NOT_MAPPED        0x00000107
#define LSA_STATUS_NONE_MAPPED            0xC0000073
#define LSA_STATUS_INSUFFICIENT_RESOURCES 0xC000009A
#define LSA_SID_NAME_USER                 1
#define LSA_SID_NAME_DOMAIN               3
#define LSA_SID_NAME_ALIAS                4
#define LSA_SID_NAME_WKN_GROUP            5
#define LSA_SID_NAME_UNKNOWN              8
#define LSA_OP_OPENPOLICY2                44
#define LSA_OP_GETUSERNAME                45
#define LSA_OP_QUERYINFOPOLICY2           46
#define LSA_OP_LOOKUPNAMES2               58
#define LSA_OP_LOOKUPNAMES3               68

/* Bytes of DCE/RPC framing (common + response header) the framing layer writes
* before the stub, so the generated marshaller's buffer cap stays in bounds. */
#define LSA_RPC_STUB_MAX                  (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

/*
 * Business logic for the ndrzcc-generated operations: fill the generated _out
 * struct from the _in struct.  These reproduce the responses the hand-rolled
 * handler returned (a fixed opaque policy handle, STATUS_SUCCESS); inputs are
 * not consulted, matching prior behaviour.
 */
/* Fill a SID with the server's fixed machine SID S-1-5-21-1111-2222-3333.  The
 * caller must provide zeroed storage (a dbuf allocation, or a `= { 0 }`
 * initialiser) so the unset identifier-authority and sub-authority bytes are
 * defined. */
static void
chimera_smb_lsarpc_machine_sid(struct ndr_sid *sid)
{
    sid->revision                = 1;
    sid->sub_authority_count     = 4;
    sid->identifier_authority[5] = 5; /* NT authority (big-endian) */
    sid->sub_authority[0]        = 21;
    sid->sub_authority[1]        = 1111;
    sid->sub_authority[2]        = 2222;
    sid->sub_authority[3]        = 3333;
} /* chimera_smb_lsarpc_machine_sid */

/* Binary RPC_SID <-> "S-1-..." string, bridging the NDR SID to the string form
 * the chimera idmap / user cache speak. */
static int
chimera_smb_sid_to_string(
    const struct ndr_sid *sid,
    char                 *buf,
    size_t                buflen)
{
    uint64_t auth = 0;
    int      n, i;

    for (i = 0; i < 6; i++) {
        auth = (auth << 8) | sid->identifier_authority[i];
    }
    n = snprintf(buf, buflen, "S-%u-%llu", sid->revision, (unsigned long long) auth);
    for (i = 0; i < sid->sub_authority_count && n > 0 && n < (int) buflen; i++) {
        n += snprintf(buf + n, buflen - n, "-%u", sid->sub_authority[i]);
    }
    return n;
} /* chimera_smb_sid_to_string */

/* Parse "S-1-22-1-1001" into a binary RPC_SID. */
static int
chimera_smb_string_to_sid(
    const char     *str,
    struct ndr_sid *sid)
{
    unsigned int       rev;
    unsigned long long auth;
    int                pos = 0, i;
    const char        *p;

    memset(sid, 0, sizeof(*sid));
    if (sscanf(str, "S-%u-%llu%n", &rev, &auth, &pos) < 2) {
        return -1;
    }
    sid->revision = (uint8_t) rev;
    for (i = 5; i >= 0; i--) {
        sid->identifier_authority[i] = (uint8_t) (auth & 0xff);
        auth                       >>= 8;
    }
    p = str + pos;
    i = 0;
    while (*p == '-' && i < NDR_SID_MAX_SUB_AUTH) {
        unsigned int sa;
        int          adv = 0;
        if (sscanf(p, "-%u%n", &sa, &adv) < 1) {
            break;
        }
        sid->sub_authority[i++] = sa;
        p                      += adv;
    }
    sid->sub_authority_count = (uint8_t) i;
    return 0;
} /* chimera_smb_string_to_sid */

/* SID equality over the meaningful fields (revision, authority, subauthorities). */
static int
chimera_smb_sid_equal(
    const struct ndr_sid *a,
    const struct ndr_sid *b)
{
    if (a->revision != b->revision ||
        a->sub_authority_count != b->sub_authority_count ||
        memcmp(a->identifier_authority, b->identifier_authority, 6) != 0) {
        return 0;
    }
    return memcmp(a->sub_authority, b->sub_authority,
                  a->sub_authority_count * sizeof(uint32_t)) == 0;
} /* chimera_smb_sid_equal */

/* Friendly name for a domain (account) SID: the Samba unix-id domains, else the
 * server workgroup. */
static const char *
chimera_smb_domain_name(const struct ndr_sid *domsid)
{
    if (domsid->sub_authority_count >= 1 &&
        domsid->identifier_authority[5] == 22) {
        if (domsid->sub_authority[0] == 1) {
            return "Unix User";
        }
        if (domsid->sub_authority[0] == 2) {
            return "Unix Group";
        }
    }
    /* Well-known authorities (MS-DTYP 2.4.2.4). */
    if (domsid->identifier_authority[5] == 1) {
        return "";                 /* World authority (Everyone) */
    }
    if (domsid->identifier_authority[5] == 5) {
        if (domsid->sub_authority_count >= 1 && domsid->sub_authority[0] == 32) {
            return "BUILTIN";
        }
        return "NT AUTHORITY";     /* S-1-5-* well-knowns */
    }
    return "WORKGROUP";
} /* chimera_smb_domain_name */

/* Well-known names the ACL editor / smbtorture LookupNames resolves on a
 * standalone server (MS-DTYP 2.4.2.4 / MS-LSAT).  Matched case-insensitively in
 * both the bare and DOMAIN\name forms. */
struct chimera_smb_wellknown_name {
    const char *name;
    const char *sid;
    uint32_t    type;
};

static const struct chimera_smb_wellknown_name chimera_smb_wellknown_names[] = {
    { "Everyone",                          "S-1-1-0",                             LSA_SID_NAME_WKN_GROUP
    },
    { "SYSTEM",                            "S-1-5-18",                            LSA_SID_NAME_WKN_GROUP
    },
    { "NT AUTHORITY\\SYSTEM",              "S-1-5-18",                            LSA_SID_NAME_WKN_GROUP
    },
    { "NT AUTHORITY\\ANONYMOUS LOGON",     "S-1-5-7",                             LSA_SID_NAME_WKN_GROUP
    },
    { "NT AUTHORITY\\Authenticated Users", "S-1-5-11",                            LSA_SID_NAME_WKN_GROUP
    },
    { "BUILTIN\\Administrators",           "S-1-5-32-544",                        LSA_SID_NAME_ALIAS
    },
    { "BUILTIN\\Users",                    "S-1-5-32-545",                        LSA_SID_NAME_ALIAS
    },
    { "BUILTIN",                           "S-1-5-32",                            LSA_SID_NAME_DOMAIN
    },
    { "BUILTIN\\",                         "S-1-5-32",                            LSA_SID_NAME_DOMAIN
    },
};

/* Resolve a name to a SID string + lsa_SidType; returns 1 if well-known. */
static int
chimera_smb_lookup_wellknown_name(
    const char *name,
    char       *sidbuf,
    size_t      buflen,
    uint32_t   *type)
{
    unsigned int i;

    if (!name) {
        return 0;
    }
    for (i = 0; i < sizeof(chimera_smb_wellknown_names) /
         sizeof(chimera_smb_wellknown_names[0]); i++) {
        if (strcasecmp(name, chimera_smb_wellknown_names[i].name) == 0) {
            snprintf(sidbuf, buflen, "%s", chimera_smb_wellknown_names[i].sid);
            *type = chimera_smb_wellknown_names[i].type;
            return 1;
        }
    }
    return 0;
} /* chimera_smb_lookup_wellknown_name */

/*
 * Resolve one LookupNames target shared by LookupNames / LookupNames2 /
 * LookupNames3.  Fills *full (the complete account SID), *type (lsa_SidType)
 * and *rid (the relative id, or 0xffffffff for a domain), and ensures the
 * name's domain is present in the referenced-domain list (via domains[] and
 * *ndom).  Returns the domain index, or -1 if the name does not resolve.
 */
static void chimera_smb_set_ustr(
    struct rpc_unicode_string *s,
    const char                *utf8);

static int
chimera_smb_lsarpc_resolve_name(
    const char                    *name,
    struct chimera_vfs_user_cache *cache,
    struct ndr_dbuf               *dbuf,
    struct lsa_DomainInfo         *domains,
    uint32_t                      *ndom,
    struct ndr_sid                *full,
    uint32_t                      *type,
    uint32_t                      *rid)
{
    char           sidstr[CHIMERA_IDMAP_SID_MAX];
    uint32_t       t = LSA_SID_NAME_UNKNOWN;
    int            resolved, didx = -1;
    struct ndr_sid dom;
    uint32_t       d;

    if (!name || name[0] == '\0') {
        /* Empty/NULL name -> the server's own primary (account) domain. */
        struct ndr_sid m = { 0 };
        chimera_smb_lsarpc_machine_sid(&m);
        chimera_smb_sid_to_string(&m, sidstr, sizeof(sidstr));
        t        = LSA_SID_NAME_DOMAIN;
        resolved = 1;
    } else {
        resolved = chimera_smb_lookup_wellknown_name(name, sidstr,
                                                     sizeof(sidstr), &t);
        if (!resolved) {
            const struct chimera_vfs_user *u =
                chimera_vfs_user_cache_lookup_by_name(cache, name);
            if (u) {
                if (u->sid[0]) {
                    snprintf(sidstr, sizeof(sidstr), "%s", u->sid);
                } else {
                    snprintf(sidstr, sizeof(sidstr), "S-1-22-1-%u", u->uid);
                }
                t        = LSA_SID_NAME_USER;
                resolved = 1;
            }
        }
    }

    if (!resolved ||
        chimera_smb_string_to_sid(sidstr, full) != 0 ||
        full->sub_authority_count == 0) {
        return -1;
    }

    if (t == LSA_SID_NAME_DOMAIN) {
        dom  = *full;            /* the SID is the domain; no RID */
        *rid = 0xffffffff;
    } else {
        *rid = full->sub_authority[full->sub_authority_count - 1];
        dom  = *full;            /* domain SID = SID minus the RID */
        dom.sub_authority_count--;
    }

    for (d = 0; d < *ndom; d++) {
        if (chimera_smb_sid_equal(domains[d].sid, &dom)) {
            didx = (int) d;
            break;
        }
    }
    if (didx < 0) {
        struct ndr_sid *ds = ndr_dbuf_alloc(dbuf, sizeof(*ds));
        *ds                = dom;
        domains[*ndom].sid = ds;
        chimera_smb_set_ustr(&domains[*ndom].name, chimera_smb_domain_name(&dom));
        didx = (int) (*ndom)++;
    }

    *type = t;
    return didx;
} /* chimera_smb_lsarpc_resolve_name */

/* Point an rpc_unicode_string (lsa_String) at a UTF-8 string; byte counts are
 * length==size==chars*2 (the buffer body is emitted no-NUL by ndr_push_wstring,
 * so max_count==actual_count==chars matches size/2==length/2). */
static void
chimera_smb_set_ustr(
    struct rpc_unicode_string *s,
    const char                *utf8)
{
    s->length = (uint16_t) (strlen(utf8) * 2);
    s->size   = (uint16_t) (strlen(utf8) * 2);
    s->buffer = (char *) utf8;
} /* chimera_smb_set_ustr */

/* A 16-byte handle id occupies a slot when any byte is non-zero. */
static int
chimera_smb_handle_id_set(const uint8_t uuid[16])
{
    for (int i = 0; i < 16; i++) {
        if (uuid[i]) {
            return 1;
        }
    }
    return 0;
} /* chimera_smb_handle_id_set */

/* Issue a policy handle into a free slot of this connection's open-handle set.
 * Returns 0 with h filled, or -1 if the set is full. */
static int
chimera_smb_lsarpc_handle_open(
    struct chimera_smb_conn *conn,
    struct policy_handle    *h)
{
    for (int i = 0; i < CHIMERA_SMB_RPC_MAX_HANDLES; i++) {
        uint64_t a, b;

        if (chimera_smb_handle_id_set(conn->rpc_handles[i])) {
            continue;
        }
        a = chimera_rand64();
        b = chimera_rand64();
        memcpy(conn->rpc_handles[i], &a, 8);
        memcpy(conn->rpc_handles[i] + 8, &b, 8);
        conn->rpc_handles[i][0] |= 1;   /* never all-zero, so the slot reads busy */

        h->handle_type = 0;
        memcpy(h->uuid, conn->rpc_handles[i], 16);
        return 0;
    }
    return -1;
} /* chimera_smb_lsarpc_handle_open */

/* Slot index of a handle open on this connection, or -1 (foreign, closed, or
 * zeroed -- the cases that must fault with nca_s_fault_context_mismatch). */
static int
chimera_smb_lsarpc_handle_slot(
    struct chimera_smb_conn    *conn,
    const struct policy_handle *h)
{
    if (!chimera_smb_handle_id_set(h->uuid)) {
        return -1;
    }
    for (int i = 0; i < CHIMERA_SMB_RPC_MAX_HANDLES; i++) {
        if (memcmp(conn->rpc_handles[i], h->uuid, 16) == 0) {
            return i;
        }
    }
    return -1;
} /* chimera_smb_lsarpc_handle_slot */

static int
chimera_smb_lsarpc_handle_valid(
    struct chimera_smb_conn    *conn,
    const struct policy_handle *h)
{
    return chimera_smb_lsarpc_handle_slot(conn, h) >= 0;
} /* chimera_smb_lsarpc_handle_valid */

/* Close (free) a handle's slot.  Returns 1 if it was open, 0 otherwise. */
static int
chimera_smb_lsarpc_handle_close(
    struct chimera_smb_conn    *conn,
    const struct policy_handle *h)
{
    int slot = chimera_smb_lsarpc_handle_slot(conn, h);

    if (slot < 0) {
        return 0;
    }
    memset(conn->rpc_handles[slot], 0, 16);
    return 1;
} /* chimera_smb_lsarpc_handle_close */

/*
 * Business logic for the generated operations.  Returns 0 on success (the _out
 * struct is filled for marshalling) or DCE_RC_FAULT_CONTEXT_MISMATCH when an op
 * is presented a handle this connection did not issue -- the dispatcher then
 * emits a DCE/RPC fault instead of a response.
 */
static int
chimera_smb_lsarpc_impl(
    int                      opnum,
    const void              *in,
    void                    *out,
    struct ndr_dbuf         *dbuf,
    struct chimera_vfs      *vfs,
    struct chimera_smb_conn *conn)
{
    (void) in;
    (void) vfs;

    switch (opnum) {
        case LSA_OP_OPENPOLICY2: {
            struct lsa_OpenPolicy2_out *o = out;
            if (chimera_smb_lsarpc_handle_open(conn, &o->handle) != 0) {
                memset(&o->handle, 0, sizeof(o->handle));
                o->status = LSA_STATUS_INSUFFICIENT_RESOURCES;
            } else {
                o->status = 0;
            }
            break;
        }
        case LSA_OP_OPENPOLICY: {
            struct lsa_OpenPolicy_out *o = out;
            if (chimera_smb_lsarpc_handle_open(conn, &o->handle) != 0) {
                memset(&o->handle, 0, sizeof(o->handle));
                o->status = LSA_STATUS_INSUFFICIENT_RESOURCES;
            } else {
                o->status = 0;
            }
            break;
        }
        case LSA_OP_GETUSERNAME: {
            struct lsa_GetUserName_out *o    = out;
            static const char          *user = "myuser";
            static const char          *auth = "WORKGROUP";
            struct rpc_unicode_string  *acct = ndr_dbuf_alloc(dbuf, sizeof(*acct));
            struct rpc_unicode_string  *adom = ndr_dbuf_alloc(dbuf, sizeof(*adom));
            struct lsa_StringWrap      *wrap = ndr_dbuf_alloc(dbuf, sizeof(*wrap));

            chimera_smb_set_ustr(acct, user);
            chimera_smb_set_ustr(adom, auth);
            wrap->string = adom;

            o->account_name   = acct;
            o->authority_name = wrap;
            o->status         = 0;
            break;
        }
        case LSA_OP_QUERYINFOPOLICY:
        case LSA_OP_QUERYINFOPOLICY2: {
            /* QueryInfoPolicy (7) and QueryInfoPolicy2 (46) share an identical
            * request/response shape; the generated _in/_out structs are
            * layout-identical, so the level and info/status fields line up. */
            const struct lsa_QueryInfoPolicy_in *qi = in;
            struct lsa_QueryInfoPolicy_out      *o  = out;

            if (!chimera_smb_lsarpc_handle_valid(conn, &qi->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }
            /* Levels 3 (PrimaryDomain) and 5 (AccountDomain) both return the
             * server's workgroup name and machine SID. */
            static const char                   *domain = "WORKGROUP";
            struct lsa_PolicyInformation        *pi     = ndr_dbuf_alloc(dbuf, sizeof(*pi));
            struct ndr_sid                      *sid    = ndr_dbuf_alloc(dbuf, sizeof(*sid));

            pi->info_class  = qi->level;   /* echo the requested level as the union tag */
            pi->name.length = (uint16_t) (strlen(domain) * 2);
            pi->name.size   = (uint16_t) (strlen(domain) * 2);
            pi->name.buffer = (char *) domain;
            chimera_smb_lsarpc_machine_sid(sid);
            pi->sid   = sid;
            o->info   = pi;
            o->status = 0;
            break;
        }
        case LSA_OP_ENUMTRUSTDOM: {
            const struct lsa_EnumTrustDom_in *li = in;
            struct lsa_EnumTrustDom_out      *o  = out;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }
            /* No trusted domains on a standalone server. */
            o->resume_handle   = 0;
            o->domains.count   = 0;
            o->domains.domains = NULL;
            o->status          = LSA_STATUS_NO_MORE_ENTRIES;
            break;
        }
        case LSA_OP_LOOKUPSIDS: {
            struct lsa_LookupSids_out      *o     = out;
            const struct lsa_LookupSids_in *li    = in;
            struct chimera_vfs_user_cache  *cache = vfs->vfs_user_cache;
            uint32_t                        n     = li->sids.num_sids;
            uint32_t                        ndom  = 0, mapped = 0, i;
            struct lsa_TranslatedName      *names;
            struct lsa_DomainInfo          *domains;
            struct lsa_RefDomainList       *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            names   = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*names));
            domains = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < n; i++) {
                struct ndr_sid                *sid = li->sids.sids[i].sid;
                const struct chimera_vfs_user *u   = NULL;
                char                           sidstr[CHIMERA_IDMAP_SID_MAX];

                names[i].sid_index = 0xffffffff;
                names[i].sid_type  = LSA_SID_NAME_UNKNOWN;

                if (!sid) {
                    chimera_smb_set_ustr(&names[i].name, "");
                    continue;
                }

                chimera_smb_sid_to_string(sid, sidstr, sizeof(sidstr));

                /* Default for an unmapped SID: echo it back in string form as
                 * the name (MS-LSAT returns untranslated SIDs as their string
                 * form, type Unknown).  Copy into the arena -- set_ustr stores
                 * the pointer and sidstr is loop-local. */
                {
                    size_t sl = strlen(sidstr) + 1;
                    char  *ns = ndr_dbuf_alloc(dbuf, sl);
                    memcpy(ns, sidstr, sl);
                    chimera_smb_set_ustr(&names[i].name, ns);
                }

                u = chimera_vfs_user_cache_lookup_by_sid(cache, sidstr);
                if (!u) {
                    struct chimera_principal pr;
                    if (chimera_idmap_sid_to_principal(sidstr, &pr) == 0 &&
                        pr.type == CHIMERA_PRINCIPAL_USER) {
                        u = chimera_vfs_user_cache_lookup_by_uid(cache, pr.id);
                    }
                }

                if (u) {
                    struct ndr_sid dom  = *sid;  /* domain SID = SID minus the RID */
                    int            didx = -1;
                    uint32_t       d;

                    if (dom.sub_authority_count > 0) {
                        dom.sub_authority_count--;
                    }
                    for (d = 0; d < ndom; d++) {
                        if (chimera_smb_sid_equal(domains[d].sid, &dom)) {
                            didx = (int) d;
                            break;
                        }
                    }
                    if (didx < 0) {
                        struct ndr_sid *ds = ndr_dbuf_alloc(dbuf, sizeof(*ds));
                        *ds               = dom;
                        domains[ndom].sid = ds;
                        chimera_smb_set_ustr(&domains[ndom].name,
                                             chimera_smb_domain_name(&dom));
                        didx = (int) ndom++;
                    }

                    names[i].sid_type  = LSA_SID_NAME_USER;
                    names[i].sid_index = (uint32_t) didx;
                    chimera_smb_set_ustr(&names[i].name, u->username);
                    mapped++;
                }
            }

            rdl->count     = ndom;
            rdl->domains   = ndom ? domains : NULL;
            rdl->max_size  = ndom;
            o->domains     = rdl;
            o->names.count = n;
            o->names.names = n ? names : NULL;
            o->count       = mapped;
            o->status      = (n == 0 || mapped == n) ? 0
                             : (mapped == 0) ? LSA_STATUS_NONE_MAPPED
                             : LSA_STATUS_SOME_NOT_MAPPED;
            break;
        }
        case LSA_OP_LOOKUPNAMES: {
            struct lsa_LookupNames_out      *o     = out;
            const struct lsa_LookupNames_in *li    = in;
            struct chimera_vfs_user_cache   *cache = vfs->vfs_user_cache;
            uint32_t                         nn    = li->num_names;
            uint32_t                         ndom  = 0, mapped = 0, i;
            struct lsa_TranslatedSid        *sids;
            struct lsa_DomainInfo           *domains;
            struct lsa_RefDomainList        *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            sids    = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*sids));
            domains = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < nn; i++) {
                struct ndr_sid full;
                uint32_t       type, rid;
                int            didx;

                sids[i].sid_type  = LSA_SID_NAME_UNKNOWN;
                sids[i].rid       = 0;
                sids[i].sid_index = 0xffffffff;

                didx = chimera_smb_lsarpc_resolve_name(li->names[i].buffer, cache,
                                                       dbuf, domains, &ndom,
                                                       &full, &type, &rid);
                if (didx < 0) {
                    continue;
                }
                sids[i].sid_type  = type;
                sids[i].rid       = rid;
                sids[i].sid_index = (uint32_t) didx;
                mapped++;
            }

            rdl->count    = ndom;
            rdl->domains  = ndom ? domains : NULL;
            rdl->max_size = ndom;
            o->domains    = rdl;
            o->sids.count = nn;
            o->sids.sids  = nn ? sids : NULL;
            o->count      = mapped;
            o->status     = (nn == 0 || mapped == nn) ? 0
                            : (mapped == 0) ? LSA_STATUS_NONE_MAPPED
                            : LSA_STATUS_SOME_NOT_MAPPED;
            break;
        }
        case LSA_OP_LOOKUPNAMES2: {
            const struct lsa_LookupNames2_in *li    = in;
            struct lsa_LookupNames2_out      *o     = out;
            struct chimera_vfs_user_cache    *cache = vfs->vfs_user_cache;
            uint32_t                          nn    = li->num_names;
            uint32_t                          ndom  = 0, mapped = 0, i;
            struct lsa_TranslatedSidEx       *sids;
            struct lsa_DomainInfo            *domains;
            struct lsa_RefDomainList         *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            sids    = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*sids));
            domains = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < nn; i++) {
                struct ndr_sid full;
                uint32_t       type, rid;
                int            didx;

                sids[i].sid_type  = LSA_SID_NAME_UNKNOWN;
                sids[i].rid       = 0;
                sids[i].sid_index = 0xffffffff;
                sids[i].flags     = 0;

                didx = chimera_smb_lsarpc_resolve_name(li->names[i].buffer, cache,
                                                       dbuf, domains, &ndom,
                                                       &full, &type, &rid);
                if (didx < 0) {
                    continue;
                }
                sids[i].sid_type  = type;
                sids[i].rid       = rid;
                sids[i].sid_index = (uint32_t) didx;
                mapped++;
            }

            rdl->count    = ndom;
            rdl->domains  = ndom ? domains : NULL;
            rdl->max_size = ndom;
            o->domains    = rdl;
            o->sids.count = nn;
            o->sids.sids  = nn ? sids : NULL;
            o->count      = mapped;
            o->status     = (nn == 0 || mapped == nn) ? 0
                            : (mapped == 0) ? LSA_STATUS_NONE_MAPPED
                            : LSA_STATUS_SOME_NOT_MAPPED;
            break;
        }
        case LSA_OP_LOOKUPNAMES3: {
            const struct lsa_LookupNames3_in *li    = in;
            struct lsa_LookupNames3_out      *o     = out;
            struct chimera_vfs_user_cache    *cache = vfs->vfs_user_cache;
            uint32_t                          nn    = li->num_names;
            uint32_t                          ndom  = 0, mapped = 0, i;
            struct lsa_TranslatedSidEx2      *sids;
            struct lsa_DomainInfo            *domains;
            struct lsa_RefDomainList         *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            sids    = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*sids));
            domains = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < nn; i++) {
                struct ndr_sid  full;
                struct ndr_sid *fs;
                uint32_t        type, rid;
                int             didx;

                sids[i].sid_type  = LSA_SID_NAME_UNKNOWN;
                sids[i].sid       = NULL;
                sids[i].sid_index = 0xffffffff;
                sids[i].flags     = 0;

                didx = chimera_smb_lsarpc_resolve_name(li->names[i].buffer, cache,
                                                       dbuf, domains, &ndom,
                                                       &full, &type, &rid);
                if (didx < 0) {
                    continue;
                }
                /* v3 carries the full account SID per entry rather than a RID. */
                fs                = ndr_dbuf_alloc(dbuf, sizeof(*fs));
                *fs               = full;
                sids[i].sid_type  = type;
                sids[i].sid       = fs;
                sids[i].sid_index = (uint32_t) didx;
                mapped++;
            }

            rdl->count    = ndom;
            rdl->domains  = ndom ? domains : NULL;
            rdl->max_size = ndom;
            o->domains    = rdl;
            o->sids.count = nn;
            o->sids.sids  = nn ? sids : NULL;
            o->count      = mapped;
            o->status     = (nn == 0 || mapped == nn) ? 0
                            : (mapped == 0) ? LSA_STATUS_NONE_MAPPED
                            : LSA_STATUS_SOME_NOT_MAPPED;
            break;
        }
        case LSA_OP_CLOSE: {
            const struct lsa_Close_in *li = in;
            struct lsa_Close_out      *o  = out;

            if (!chimera_smb_lsarpc_handle_close(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }
            /* Per MS-LSAD a successful Close returns a zeroed handle; closing an
             * already-closed (or foreign) handle faults above. */
            o->handle.handle_type = 0;
            memset(o->handle.uuid, 0, sizeof(o->handle.uuid));
            o->status = 0;
            break;
        }
        default:
            break;
    } /* switch */

    return 0;
} /* chimera_smb_lsarpc_impl */

/*
 * Generic NDR dispatch for a generated operation: unmarshall the request stub
 * into the _in struct, run the business logic, marshall the _out struct.  The
 * single-fragment request stub is contiguous in the cursor's current iovec.
 */
static int
chimera_smb_lsarpc_ndr_dispatch(
    const struct ndr_op_desc   *op,
    struct evpl_iovec_cursor   *cursor,
    void                       *output,
    struct chimera_smb_request *request)
{
    struct ndr_cursor        c;
    struct ndr_writer        w;
    struct ndr_dbuf          dbuf;
    struct chimera_vfs      *vfs  = request->compound->thread->shared->vfs;
    struct chimera_smb_conn *conn = request->compound->conn;
    void                    *in, *out;
    int                      n;

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    /* Starting capacity hint only; the arena grows by linking new blocks so the
     * held in/out pointers stay valid across later allocations. */
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    n = chimera_smb_lsarpc_impl(op->opnum, in, out, &dbuf, vfs, conn);

    /* A context-handle fault skips marshalling: dce_rpc emits a fault PDU from
     * the negative return. */
    if (n < 0) {
        ndr_dbuf_destroy(&dbuf);
        return n;
    }

    ndr_writer_init(&w, output, LSA_RPC_STUB_MAX);
    n = op->push_out(&w, out);

    ndr_dbuf_destroy(&dbuf);
    return n;
} /* chimera_smb_lsarpc_ndr_dispatch */

/*
 * Top-level LSA opnum dispatch: every supported operation is declared in
 * lsarpc.idl and marshalled by generated code; an unknown opnum returns -1 so
 * the framing layer replies with a DCE/RPC fault.
 */
static int
chimera_smb_lsarpc_handler(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    const struct ndr_op_desc *op;

    op = ndr_find_op(lsarpc_op_table, lsarpc_op_count, opnum);

    if (!op) {
        return -1;
    }

    return chimera_smb_lsarpc_ndr_dispatch(op, cursor, output, private_data);
} /* chimera_smb_lsarpc_handler */

int
chimera_smb_lsarpc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    int status;

    status = dce_rpc(&LSA_INTERFACE, input_iov, input_niov, output_iov, chimera_smb_lsarpc_handler, request);

    if (status != 0) {
        chimera_smb_error("LSA RPC transceive failed");
        return status;
    }

    return 0;
} /* chimera_smb_lsarpc_transceive */
