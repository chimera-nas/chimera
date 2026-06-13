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

#define LSA_OP_CLOSE               0
#define LSA_OP_OPENPOLICY          6
#define LSA_OP_QUERYINFOPOLICY     7
#define LSA_OP_ENUMTRUSTDOM        13
#define LSA_OP_LOOKUPNAMES         14
#define LSA_OP_LOOKUPSIDS          15

#define LSA_STATUS_NO_MORE_ENTRIES 0x8000001A
#define LSA_STATUS_SOME_NOT_MAPPED 0x00000107
#define LSA_STATUS_NONE_MAPPED     0xC0000073
#define LSA_SID_NAME_USER          1
#define LSA_SID_NAME_UNKNOWN       8
#define LSA_OP_OPENPOLICY2         44
#define LSA_OP_GETUSERNAME         45
#define LSA_OP_QUERYINFOPOLICY2    46

/* Bytes of DCE/RPC framing (common + response header) the framing layer writes
* before the stub, so the generated marshaller's buffer cap stays in bounds. */
#define LSA_RPC_STUB_MAX           (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

/*
 * Business logic for the ndrzcc-generated operations: fill the generated _out
 * struct from the _in struct.  These reproduce the responses the hand-rolled
 * handler returned (a fixed opaque policy handle, STATUS_SUCCESS); inputs are
 * not consulted, matching prior behaviour.
 */
/* Fill a zero-initialised SID with the server's fixed machine SID
 * S-1-5-21-1111-2222-3333.  The caller provides storage from the dbuf arena,
 * which ndr_dbuf_alloc has already zeroed. */
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
    return "WORKGROUP";
} /* chimera_smb_domain_name */

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

static void
chimera_smb_lsarpc_impl(
    int                 opnum,
    const void         *in,
    void               *out,
    struct ndr_dbuf    *dbuf,
    struct chimera_vfs *vfs)
{
    (void) in;
    (void) vfs;

    switch (opnum) {
        case LSA_OP_OPENPOLICY2: {
            struct lsa_OpenPolicy2_out *o = out;
            o->handle.handle_type = 0;
            memset(o->handle.uuid, 0xaa, sizeof(o->handle.uuid));
            o->status = 0;
            break;
        }
        case LSA_OP_OPENPOLICY: {
            struct lsa_OpenPolicy_out *o = out;
            o->handle.handle_type = 0;
            memset(o->handle.uuid, 0xaa, sizeof(o->handle.uuid));
            o->status = 0;
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
            struct lsa_EnumTrustDom_out *o = out;
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

            names   = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*names));
            domains = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < n; i++) {
                struct ndr_sid                *sid = li->sids.sids[i].sid;
                const struct chimera_vfs_user *u   = NULL;
                char                           sidstr[CHIMERA_IDMAP_SID_MAX];

                names[i].sid_index = 0xffffffff;
                names[i].sid_type  = LSA_SID_NAME_UNKNOWN;
                chimera_smb_set_ustr(&names[i].name, "");

                if (!sid) {
                    continue;
                }

                chimera_smb_sid_to_string(sid, sidstr, sizeof(sidstr));

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

            sids    = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*sids));
            domains = ndr_dbuf_alloc(dbuf, (nn ? nn : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < nn; i++) {
                const char                    *name = li->names[i].buffer;
                const struct chimera_vfs_user *u;

                sids[i].sid_type  = LSA_SID_NAME_UNKNOWN;
                sids[i].rid       = 0;
                sids[i].sid_index = 0xffffffff;

                u = name ? chimera_vfs_user_cache_lookup_by_name(cache, name) : NULL;
                if (u) {
                    char           sidstr[CHIMERA_IDMAP_SID_MAX];
                    struct ndr_sid full, dom;
                    int            didx = -1;
                    uint32_t       d, rid;

                    if (u->sid[0]) {
                        snprintf(sidstr, sizeof(sidstr), "%s", u->sid);
                    } else {
                        snprintf(sidstr, sizeof(sidstr), "S-1-22-1-%u", u->uid);
                    }
                    if (chimera_smb_string_to_sid(sidstr, &full) != 0 ||
                        full.sub_authority_count == 0) {
                        continue;
                    }
                    rid = full.sub_authority[full.sub_authority_count - 1];
                    dom = full;            /* domain SID = SID minus the RID */
                    dom.sub_authority_count--;

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

                    sids[i].sid_type  = LSA_SID_NAME_USER;
                    sids[i].rid       = rid;
                    sids[i].sid_index = (uint32_t) didx;
                    mapped++;
                }
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
            struct lsa_Close_out *o = out;
            o->handle.handle_type = 0;
            memset(o->handle.uuid, 0xaa, sizeof(o->handle.uuid));
            o->status = 0;
            break;
        }
        default:
            break;
    } /* switch */
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
    struct ndr_cursor   c;
    struct ndr_writer   w;
    struct ndr_dbuf     dbuf;
    struct chimera_vfs *vfs = request->compound->thread->shared->vfs;
    void               *in, *out;
    int                 n;

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    /* Starting capacity hint only; the arena grows by linking new blocks so the
     * held in/out pointers stay valid across later allocations. */
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    chimera_smb_lsarpc_impl(op->opnum, in, out, &dbuf, vfs);

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
