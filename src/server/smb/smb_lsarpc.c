// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_lsarpc.h"
#include "smb_dcerpc.h"
#include "smb_lsarpc_names.h"
#include "smb_async_interim.h"
#include "smb_pipe.h"
#include "lsarpc_ndr.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_idmap.h"
#include "common/ratelimit.h"
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
#define LSA_OP_OPENPOLICY2                44
#define LSA_OP_GETUSERNAME                45
#define LSA_OP_QUERYINFOPOLICY2           46
#define LSA_OP_LOOKUPNAMES2               58
#define LSA_OP_LOOKUPNAMES3               68

/* The name of the machine (account) domain: QueryInfoPolicy reports it with
 * the machine SID, and LookupNames gives it to the empty name, which names
 * that SID.  One string, so the two can never disagree. */
#define LSA_MACHINE_DOMAIN_NAME           "WORKGROUP"

/* Bytes of DCE/RPC framing (common + response header) the framing layer writes
* before the stub, so the generated marshaller's buffer cap stays in bounds. */
#define LSA_RPC_STUB_MAX                  (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

/*
 * Business logic for the ndrzcc-generated operations: fill the generated _out
 * struct from the _in struct.  These reproduce the responses the hand-rolled
 * handler returned (a fixed opaque policy handle, STATUS_SUCCESS); inputs are
 * not consulted, matching prior behaviour.
 */
/* Fill a SID with the server's account-domain (machine) SID S-1-5-21-X-Y-Z,
* whose sub-authorities are derived once at startup from a stable per-host id
* (shared->machine_domain_sub).  The caller must provide zeroed storage (a dbuf
* allocation, or a `= { 0 }` initialiser) so the unset bytes are defined. */
static void
chimera_smb_lsarpc_machine_sid(
    struct ndr_sid                         *sid,
    const struct chimera_server_smb_shared *shared)
{
    sid->revision                = 1;
    sid->sub_authority_count     = 4;
    sid->identifier_authority[5] = 5; /* NT authority (big-endian) */
    sid->sub_authority[0]        = 21;
    sid->sub_authority[1]        = shared->machine_domain_sub[0];
    sid->sub_authority[2]        = shared->machine_domain_sub[1];
    sid->sub_authority[3]        = shared->machine_domain_sub[2];
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

/*
 * Walk a UTF-8 string sequence by sequence exactly as the marshaller's
 * ndr_utf8_to_utf16le (ext/ndrzcc) does: the lead byte alone sets a
 * sequence's length, and a four-byte sequence becomes a surrogate pair only
 * when the code point its bits decode to is 0x10000 or more.  Returns the
 * bytes up to the end of the last sequence the terminator does not cut short
 * -- strlen(utf8) unless it cuts the last one -- and sets *units to the
 * UTF-16 code units the marshaller emits for those bytes.  For every string
 * the marshaller accepts, *units equals its output.  It rejects a byte that
 * cannot lead a sequence, failing the whole reply: *rejected is set for a
 * string that holds one, and chimera_smb_lsarpc_lookup() keeps a name like
 * that out of the reply, so the count given for one does not matter.  And it
 * reads past the terminator to finish a sequence that is cut short, which no
 * count can match -- chimera_smb_lsarpc_dup() drops such a tail before a
 * string reaches the reply.
 */
static size_t
chimera_smb_utf8_walk(
    const char *utf8,
    size_t     *units,
    int        *rejected)
{
    const uint8_t *s = (const uint8_t *) utf8;
    size_t         off = 0, len, i;
    uint32_t       cp;

    *units    = 0;
    *rejected = 0;
    while (s[off]) {
        if (s[off] < 0x80) {
            len = 1;
        } else if ((s[off] & 0xe0) == 0xc0) {
            len = 2;
        } else if ((s[off] & 0xf0) == 0xe0) {
            len = 3;
        } else if ((s[off] & 0xf8) == 0xf0) {
            len = 4;
        } else {
            len       = 1; /* rejected by the marshaller */
            *rejected = 1;
        }

        for (i = 1; i < len; i++) {
            if (!s[off + i]) {
                return off;    /* cut short by the terminator */
            }
        }

        if (len == 4) {
            cp = ((uint32_t) (s[off] & 0x07) << 18) |
                ((uint32_t) (s[off + 1] & 0x3f) << 12) |
                ((uint32_t) (s[off + 2] & 0x3f) << 6) |
                (uint32_t) (s[off + 3] & 0x3f);
            *units += cp < 0x10000 ? 1 : 2;
        } else {
            *units += 1;
        }
        off += len;
    }
    return off;
} /* chimera_smb_utf8_walk */

/* Point an rpc_unicode_string (lsa_String) at a UTF-8 string; byte counts are
 * length==size==units*2, units being the UTF-16 code units the body is sent
 * as (no NUL, by ndr_push_wstring_nonul), so max_count==actual_count==units
 * matches size/2==length/2.  The string must not end in a cut-short sequence
 * (see chimera_smb_utf8_walk); the literals and chimera_smb_lsarpc_dup()
 * copies this file passes never do. */
static void
chimera_smb_set_ustr(
    struct rpc_unicode_string *s,
    const char                *utf8)
{
    size_t units;
    int    rejected;

    (void) chimera_smb_utf8_walk(utf8, &units, &rejected);

    s->length = (uint16_t) (units * 2);
    s->size   = (uint16_t) (units * 2);
    s->buffer = (char *) utf8;
} /* chimera_smb_set_ustr */

struct chimera_smb_lsarpc_pending;

/* One LSARPC call's state across its passes; dce_rpc's private_data. */
struct chimera_smb_lsarpc_call {
    struct chimera_smb_request        *request;
    int                                may_resolve; /* a miss may go to the resolver   */
    int                                final;       /* the pass after a resolve round */
    struct chimera_smb_lsarpc_pending *pending;     /* that round's misses, or NULL    */
};

/* At most this many distinct misses of one request go to the resolver; the
 * rest get what the caches say (the fallback answer, or unmapped). */
#define CHIMERA_SMB_LSARPC_MAX_MISSES 64

/* At most this many lookups of one connection wait on the resolver at once
 * (conn->lsarpc_parked).  Each holds its pending (about 55 KB), a 64 KB reply
 * buffer and up to CHIMERA_SMB_LSARPC_MAX_MISSES jobs on the resolver queue
 * every connection shares; one past the bound asks the resolver nothing and
 * gets what the caches say. */
#define CHIMERA_SMB_LSARPC_MAX_PARKED 16

/* One distinct miss of a request, and the resolver's answer to it. */
struct chimera_smb_lsarpc_miss_rec {
    struct chimera_smb_lsa_miss        miss;
    struct chimera_smb_lsa_resolved    resolved;
    struct chimera_smb_lsarpc_pending *pending;   /* back pointer for the callback */
};

/*
 * A request's resolve round.  Pass 1 allocates it at the first miss.  It is
 * freed when the call is finished -- after pass 2, or once the connection
 * turns out to be gone -- except when the session closed first: the call is
 * then finished early, and the last answer frees the round (see `request`).
 * Only the request's own thread touches it: the resolver calls back there.
 */
struct chimera_smb_lsarpc_pending {
    /* NULL once the call was finished ahead of its answers because its
     * session was closing (chimera_smb_lsarpc_session_closing): the answers
     * still come, and the last one only frees this. */
    struct chimera_smb_request        *request;
    struct chimera_server_smb_shared  *shared;
    int                                outstanding; /* guard-counted join      */
    /* Set once the transceive returned ASYNC: the last answer then finishes
     * the call, or only frees this.  The async credit and the parked slot
     * taken at that point are given back by chimera_smb_lsarpc_unpark, which
     * leaves this set. */
    int                                parked;
    /* On conn->lsarpc_pending while parked on a live connection. */
    struct chimera_smb_lsarpc_pending *prev;
    struct chimera_smb_lsarpc_pending *next;
    int                                nmiss;
    int                                over_cap;    /* misses the cap left out */
    struct chimera_smb_lsarpc_miss_rec miss[CHIMERA_SMB_LSARPC_MAX_MISSES];
};

/* How often a lookup that fell back to the caches is reported: these come in
 * floods, from a client walking a large ACL or while winbind is down. */
#define CHIMERA_SMB_LSARPC_REPORT_NS (10ULL * 1000000000ULL)

/* Where this server's lookups find names. */
static void
chimera_smb_lsarpc_naming(
    const struct chimera_server_smb_shared *shared,
    struct chimera_smb_lsa_naming          *naming)
{
    naming->user_cache    = shared->vfs->vfs_user_cache;
    naming->account_cache = shared->account_cache;
    /* With winbind, the domain the host is joined to: winbind resolves a
     * name only under the domain it knows it by, and the configured
     * winbind_domain is optional and only a fallback for that one (it is
     * what server_identity holds when winbind could not say). */
    naming->fallback_domain = chimera_smb_lsa_fallback_domain(
        shared->config.auth.winbind_enabled ?
        shared->config.auth.server_identity.netbios_domain : NULL,
        shared->config.auth.winbind_domain);
} /* chimera_smb_lsarpc_naming */

/* Copy `s` into the reply arena (set_ustr keeps the pointer), without a
 * trailing UTF-8 sequence the terminator cuts short -- a name cut at a byte
 * limit can end in one, and the marshaller would read past the terminator to
 * finish it. */
static const char *
chimera_smb_lsarpc_dup(
    struct ndr_dbuf *dbuf,
    const char      *s)
{
    size_t units;
    int    rejected;
    size_t len = chimera_smb_utf8_walk(s, &units, &rejected);
    char  *d   = ndr_dbuf_alloc(dbuf, len + 1);

    memcpy(d, s, len);
    d[len] = '\0';
    return d;
} /* chimera_smb_lsarpc_dup */

/* Index of acct's domain in the referenced-domain list, adding it if new:
 * domain SID = full SID for type 3, else full SID minus its last
 * sub-authority; name = acct->domain. */
static int
chimera_smb_lsarpc_add_domain(
    const struct chimera_vfs_identity_account *acct,
    const struct ndr_sid                      *full,
    struct lsa_DomainInfo                     *domains,
    uint32_t                                  *ndom,
    struct ndr_dbuf                           *dbuf)
{
    struct ndr_sid  dom = *full;
    struct ndr_sid *ds;
    uint32_t        d;

    if (acct->sid_type != LSA_SID_NAME_DOMAIN && dom.sub_authority_count > 0) {
        dom.sub_authority_count--;
    }

    for (d = 0; d < *ndom; d++) {
        if (chimera_smb_sid_equal(domains[d].sid, &dom)) {
            return (int) d;
        }
    }

    ds                 = ndr_dbuf_alloc(dbuf, sizeof(*ds));
    *ds                = dom;
    domains[*ndom].sid = ds;
    chimera_smb_set_ustr(&domains[*ndom].name,
                         chimera_smb_lsarpc_dup(dbuf, acct->domain));
    return (int) (*ndom)++;
} /* chimera_smb_lsarpc_add_domain */

/* chimera_smb_lsa_lookup_sid or chimera_smb_lsa_lookup_name. */
typedef enum chimera_smb_lsa_rc (*chimera_smb_lsarpc_lookup_t)(
    const struct chimera_smb_lsa_naming *naming,
    const char *input,
    int may_resolve,
    const struct chimera_smb_lsa_resolved *resolved,
    struct chimera_vfs_identity_account *out,
    struct chimera_smb_lsa_miss *miss);

/* The round's record of `miss`, or NULL when it has none (past the cap). */
static struct chimera_smb_lsarpc_miss_rec *
chimera_smb_lsarpc_find_miss(
    struct chimera_smb_lsarpc_pending *pending,
    const struct chimera_smb_lsa_miss *miss)
{
    struct chimera_smb_lsarpc_miss_rec *rec;
    int                                 i;

    if (!pending) {
        return NULL;
    }

    for (i = 0; i < pending->nmiss; i++) {
        rec = &pending->miss[i];
        if (rec->miss.key == miss->key && rec->miss.id == miss->id &&
            strcmp(rec->miss.input, miss->input) == 0) {
            return rec;
        }
    }
    return NULL;
} /* chimera_smb_lsarpc_find_miss */

/* Pass 1: note a miss for the resolve round, once per distinct question and
 * for at most CHIMERA_SMB_LSARPC_MAX_MISSES of them; the ones past that are
 * counted, each time they come up, for the report.  Out of memory, there is
 * no round and the entry stays unmapped. */
static void
chimera_smb_lsarpc_record_miss(
    struct chimera_smb_lsarpc_call    *call,
    const struct chimera_smb_lsa_miss *miss)
{
    struct chimera_smb_lsarpc_pending  *pending = call->pending;
    struct chimera_smb_lsarpc_miss_rec *rec;

    if (!pending) {
        pending = calloc(1, sizeof(*pending));
        if (!pending) {
            static struct chimera_ratelimit rl;
            uint64_t                        held_back;

            if (chimera_ratelimit_allow(&rl, CHIMERA_SMB_LSARPC_REPORT_NS,
                                        &held_back)) {
                chimera_smb_error("LSA lookup: out of memory for a resolve "
                                  "round; answering from the caches (%" PRIu64
                                  " more since the last report)", held_back);
            }
            return;
        }
        pending->request = call->request;
        pending->shared  = call->request->compound->thread->shared;
        call->pending    = pending;
    }

    if (chimera_smb_lsarpc_find_miss(pending, miss)) {
        return;
    }

    if (pending->nmiss == CHIMERA_SMB_LSARPC_MAX_MISSES) {
        pending->over_cap++;
        return;
    }

    rec          = &pending->miss[pending->nmiss++];
    rec->miss    = *miss;
    rec->pending = pending;
} /* chimera_smb_lsarpc_record_miss */

/* Non-zero if the reply can carry `acct`'s names: winbind's are in its unix
 * charset and a local record's in whatever its source used, so neither has to
 * be UTF-8, and one string the marshaller rejects fails the whole reply. */
static int
chimera_smb_lsarpc_sendable(const struct chimera_vfs_identity_account *acct)
{
    size_t units;
    int    name_rejected, domain_rejected;

    (void) chimera_smb_utf8_walk(acct->name, &units, &name_rejected);
    (void) chimera_smb_utf8_walk(acct->domain, &units, &domain_rejected);
    return !name_rejected && !domain_rejected;
} /* chimera_smb_lsarpc_sendable */

static enum chimera_smb_lsa_rc
chimera_smb_lsarpc_lookup_any(
    struct chimera_smb_lsarpc_call      *call,
    chimera_smb_lsarpc_lookup_t          lookup,
    const struct chimera_smb_lsa_naming *naming,
    const char                          *input,
    struct chimera_vfs_identity_account *out)
{
    struct chimera_smb_lsarpc_miss_rec *rec;
    struct chimera_smb_lsa_miss         miss;
    enum chimera_smb_lsa_rc             rc;

    if (call->final) {
        /* may_resolve is 1 here only so that the lookup reports the miss pass
         * 1 recorded, which is how its record is found: nothing is asked. */
        rc = lookup(naming, input, 1, NULL, out, &miss);
        if (rc != CHIMERA_SMB_LSA_MISS) {
            return rc;
        }
        rec = chimera_smb_lsarpc_find_miss(call->pending, &miss);
        return lookup(naming, input, 0, rec ? &rec->resolved : NULL, out, &miss);
    }

    rc = lookup(naming, input, call->may_resolve, NULL, out, &miss);
    if (rc == CHIMERA_SMB_LSA_MISS) {
        chimera_smb_lsarpc_record_miss(call, &miss);
        rc = CHIMERA_SMB_LSA_UNMAPPED;
    }
    return rc;
} /* chimera_smb_lsarpc_lookup_any */

/* Look one SID / name up for this call, the one place lookups happen.  Pass 1
 * records a miss and answers it unmapped (a reply that is thrown away once
 * anything missed); pass 2 asks the caches first and answers a miss from its
 * record -- the fallback when the cap left it out.  An account whose names
 * the reply cannot carry is unmapped too: only its own entry, not the reply. */
static enum chimera_smb_lsa_rc
chimera_smb_lsarpc_lookup(
    struct chimera_smb_lsarpc_call      *call,
    chimera_smb_lsarpc_lookup_t          lookup,
    const struct chimera_smb_lsa_naming *naming,
    const char                          *input,
    struct chimera_vfs_identity_account *out)
{
    enum chimera_smb_lsa_rc rc;

    rc = chimera_smb_lsarpc_lookup_any(call, lookup, naming, input, out);
    if (rc == CHIMERA_SMB_LSA_MAPPED && !chimera_smb_lsarpc_sendable(out)) {
        rc = CHIMERA_SMB_LSA_UNMAPPED;
    }
    return rc;
} /* chimera_smb_lsarpc_lookup */

static enum chimera_smb_lsa_rc
chimera_smb_lsarpc_lookup_sid(
    struct chimera_smb_lsarpc_call      *call,
    const struct chimera_smb_lsa_naming *naming,
    const char                          *sid,
    struct chimera_vfs_identity_account *out)
{
    return chimera_smb_lsarpc_lookup(call, chimera_smb_lsa_lookup_sid, naming,
                                     sid, out);
} /* chimera_smb_lsarpc_lookup_sid */

static enum chimera_smb_lsa_rc
chimera_smb_lsarpc_lookup_name(
    struct chimera_smb_lsarpc_call      *call,
    const struct chimera_smb_lsa_naming *naming,
    const char                          *name,
    struct chimera_vfs_identity_account *out)
{
    return chimera_smb_lsarpc_lookup(call, chimera_smb_lsa_lookup_name, naming,
                                     name, out);
} /* chimera_smb_lsarpc_lookup_name */

/*
 * Resolve one LookupNames target shared by LookupNames / LookupNames2 /
 * LookupNames3.  Fills *full (the complete account SID), *type (lsa_SidType)
 * and *rid (the relative id, or 0xffffffff for a domain), and ensures the
 * name's domain is present in the referenced-domain list (via domains[] and
 * *ndom).  Returns the domain index, or -1 if the name does not resolve.
 */
static int
chimera_smb_lsarpc_resolve_name(
    struct chimera_smb_lsarpc_call         *call,
    const struct chimera_smb_lsa_naming    *naming,
    const char                             *name,
    struct ndr_dbuf                        *dbuf,
    const struct chimera_server_smb_shared *shared,
    struct lsa_DomainInfo                  *domains,
    uint32_t                               *ndom,
    struct ndr_sid                         *full,
    uint32_t                               *type,
    uint32_t                               *rid)
{
    struct chimera_vfs_identity_account acct;
    int                                 didx;

    if (!name || name[0] == '\0') {
        /* Empty/NULL name -> the server's own primary (account) domain, named
         * as QueryInfoPolicy names it. */
        memset(&acct, 0, sizeof(acct));
        memset(full, 0, sizeof(*full));
        chimera_smb_lsarpc_machine_sid(full, shared);
        snprintf(acct.domain, sizeof(acct.domain), "%s", LSA_MACHINE_DOMAIN_NAME);
        acct.sid_type = LSA_SID_NAME_DOMAIN;
    } else if (chimera_smb_lsarpc_lookup_name(call, naming, name, &acct) !=
               CHIMERA_SMB_LSA_MAPPED ||
               chimera_smb_string_to_sid(acct.sid, full) != 0 ||
               full->sub_authority_count == 0) {
        return -1;
    }

    didx = chimera_smb_lsarpc_add_domain(&acct, full, domains, ndom, dbuf);

    *type = acct.sid_type;
    *rid  = acct.sid_type == LSA_SID_NAME_DOMAIN ? 0xffffffff :
        full->sub_authority[full->sub_authority_count - 1];
    return didx;
} /* chimera_smb_lsarpc_resolve_name */

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
    int                                     opnum,
    const void                             *in,
    void                                   *out,
    struct ndr_dbuf                        *dbuf,
    struct chimera_vfs                     *vfs,
    struct chimera_smb_conn                *conn,
    const struct chimera_server_smb_shared *shared,
    struct chimera_smb_lsarpc_call         *call)
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
            static const char                   *domain = LSA_MACHINE_DOMAIN_NAME;
            struct lsa_PolicyInformation        *pi     = ndr_dbuf_alloc(dbuf, sizeof(*pi));
            struct ndr_sid                      *sid    = ndr_dbuf_alloc(dbuf, sizeof(*sid));

            pi->info_class  = qi->level;   /* echo the requested level as the union tag */
            pi->name.length = (uint16_t) (strlen(domain) * 2);
            pi->name.size   = (uint16_t) (strlen(domain) * 2);
            pi->name.buffer = (char *) domain;
            chimera_smb_lsarpc_machine_sid(sid, shared);
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
            struct lsa_LookupSids_out      *o  = out;
            const struct lsa_LookupSids_in *li = in;
            uint32_t                        n  = li->sids.num_sids;
            uint32_t                        ndom = 0, mapped = 0, i;
            struct chimera_smb_lsa_naming   naming;
            struct lsa_TranslatedName      *names;
            struct lsa_DomainInfo          *domains;
            struct lsa_RefDomainList       *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            chimera_smb_lsarpc_naming(shared, &naming);

            names   = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*names));
            domains = ndr_dbuf_alloc(dbuf, (n ? n : 1) * sizeof(*domains));
            rdl     = ndr_dbuf_alloc(dbuf, sizeof(*rdl));

            for (i = 0; i < n; i++) {
                struct ndr_sid                     *sid = li->sids.sids[i].sid;
                struct chimera_vfs_identity_account acct;
                char                                sidstr[CHIMERA_IDMAP_SID_MAX];
                int                                 len, didx;

                names[i].sid_index = 0xffffffff;
                names[i].sid_type  = LSA_SID_NAME_UNKNOWN;

                if (!sid) {
                    chimera_smb_set_ustr(&names[i].name, "");
                    continue;
                }

                len = chimera_smb_sid_to_string(sid, sidstr, sizeof(sidstr));

                /* Default for an unmapped SID: echo it back in string form as
                 * the name (MS-LSAT returns untranslated SIDs as their string
                 * form, type Unknown).  Copy into the arena -- set_ustr stores
                 * the pointer and sidstr is loop-local. */
                chimera_smb_set_ustr(&names[i].name,
                                     chimera_smb_lsarpc_dup(dbuf, sidstr));

                /* A SID whose string did not fit is never looked up: cut
                 * short, it could name some other account. */
                if (len < 0 || len >= (int) sizeof(sidstr) ||
                    chimera_smb_lsarpc_lookup_sid(call, &naming, sidstr, &acct) !=
                    CHIMERA_SMB_LSA_MAPPED) {
                    continue;
                }

                didx = chimera_smb_lsarpc_add_domain(&acct, sid, domains, &ndom,
                                                     dbuf);

                names[i].sid_type  = acct.sid_type;
                names[i].sid_index = (uint32_t) didx;
                chimera_smb_set_ustr(&names[i].name,
                                     chimera_smb_lsarpc_dup(dbuf, acct.name));
                mapped++;
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
            struct lsa_LookupNames_out      *o  = out;
            const struct lsa_LookupNames_in *li = in;
            uint32_t                         nn = li->num_names;
            uint32_t                         ndom  = 0, mapped = 0, i;
            struct chimera_smb_lsa_naming    naming;
            struct lsa_TranslatedSid        *sids;
            struct lsa_DomainInfo           *domains;
            struct lsa_RefDomainList        *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            chimera_smb_lsarpc_naming(shared, &naming);

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

                didx = chimera_smb_lsarpc_resolve_name(call, &naming,
                                                       li->names[i].buffer, dbuf,
                                                       shared, domains, &ndom,
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
            const struct lsa_LookupNames2_in *li = in;
            struct lsa_LookupNames2_out      *o  = out;
            uint32_t                          nn = li->num_names;
            uint32_t                          ndom  = 0, mapped = 0, i;
            struct chimera_smb_lsa_naming     naming;
            struct lsa_TranslatedSidEx       *sids;
            struct lsa_DomainInfo            *domains;
            struct lsa_RefDomainList         *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            chimera_smb_lsarpc_naming(shared, &naming);

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

                didx = chimera_smb_lsarpc_resolve_name(call, &naming,
                                                       li->names[i].buffer, dbuf,
                                                       shared, domains, &ndom,
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
            const struct lsa_LookupNames3_in *li = in;
            struct lsa_LookupNames3_out      *o  = out;
            uint32_t                          nn = li->num_names;
            uint32_t                          ndom  = 0, mapped = 0, i;
            struct chimera_smb_lsa_naming     naming;
            struct lsa_TranslatedSidEx2      *sids;
            struct lsa_DomainInfo            *domains;
            struct lsa_RefDomainList         *rdl;

            if (!chimera_smb_lsarpc_handle_valid(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            chimera_smb_lsarpc_naming(shared, &naming);

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

                didx = chimera_smb_lsarpc_resolve_name(call, &naming,
                                                       li->names[i].buffer, dbuf,
                                                       shared, domains, &ndom,
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
    const struct ndr_op_desc       *op,
    struct evpl_iovec_cursor       *cursor,
    void                           *output,
    struct chimera_smb_lsarpc_call *call)
{
    struct ndr_cursor                 c;
    struct ndr_writer                 w;
    struct ndr_dbuf                   dbuf;
    struct chimera_smb_request       *request = call->request;
    struct chimera_server_smb_shared *shared  = request->compound->thread->shared;
    struct chimera_vfs               *vfs     = shared->vfs;
    struct chimera_smb_conn          *conn    = request->compound->conn;
    void                             *in, *out;
    int                               n;

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    /* Starting capacity hint only; the arena grows by linking new blocks so the
     * held in/out pointers stay valid across later allocations. */
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    n = chimera_smb_lsarpc_impl(op->opnum, in, out, &dbuf, vfs, conn, shared,
                                call);

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
    struct chimera_smb_lsarpc_call *call = private_data;
    const struct ndr_op_desc       *op;

    op = ndr_find_op(lsarpc_op_table, lsarpc_op_count, opnum);

    if (!op) {
        return -1;
    }

    return chimera_smb_lsarpc_ndr_dispatch(op, cursor, output, call);
} /* chimera_smb_lsarpc_handler */

/* Say, before pass 2, what of this round is answered from the caches for
 * want of a resolver answer: the misses past the cap, and the questions the
 * identity service could not take.  To the client these read exactly like an
 * account that does not exist, so this is the only trace they leave. */
static void
chimera_smb_lsarpc_report_round(const struct chimera_smb_lsarpc_pending *pending)
{
    static struct chimera_ratelimit rl_cap, rl_down;
    uint64_t                        held_back;
    int                             i, down = 0;

    for (i = 0; i < pending->nmiss; i++) {
        if (pending->miss[i].resolved.status == CHIMERA_VFS_IDENTITY_UNAVAILABLE) {
            down++;
        }
    }

    if (down &&
        chimera_ratelimit_allow(&rl_down, CHIMERA_SMB_LSARPC_REPORT_NS,
                                &held_back)) {
        chimera_smb_error("LSA lookup: the identity service could not answer "
                          "%d of %d question(s); answering those from the "
                          "caches (%" PRIu64 " more request(s) since the last "
                          "report)", down, pending->nmiss, held_back);
    }

    if (pending->over_cap &&
        chimera_ratelimit_allow(&rl_cap, CHIMERA_SMB_LSARPC_REPORT_NS,
                                &held_back)) {
        chimera_smb_info("LSA lookup: one request resolves %d names; %d more "
                         "miss(es) are answered from the caches (%" PRIu64
                         " more request(s) since the last report)",
                         CHIMERA_SMB_LSARPC_MAX_MISSES, pending->over_cap,
                         held_back);
    }
} /* chimera_smb_lsarpc_report_round */

/* Pass 2: the same request PDU again, each miss answered from its record. */
static int
chimera_smb_lsarpc_final_pass(
    struct chimera_smb_lsarpc_pending *pending,
    struct evpl_iovec                 *input_iov,
    int                                input_niov,
    struct evpl_iovec                 *output_iov)
{
    struct chimera_smb_lsarpc_call call = {
        .request     = pending->request,
        .may_resolve = 0,
        .final       = 1,
        .pending     = pending,
    };
    int                            status;

    chimera_smb_lsarpc_report_round(pending);

    status = dce_rpc(&LSA_INTERFACE, input_iov, input_niov, output_iov,
                     chimera_smb_lsarpc_handler, &call);

    if (status != 0) {
        chimera_smb_error("LSA RPC transceive failed");
    }
    return status;
} /* chimera_smb_lsarpc_final_pass */

/* Take a parked call off its live connection: give back its async credit and
 * its parked slot, and unlink it. */
static void
chimera_smb_lsarpc_unpark(
    struct chimera_smb_conn           *conn,
    struct chimera_smb_lsarpc_pending *pending)
{
    if (conn->async_outstanding) {
        conn->async_outstanding--;
    }
    if (conn->lsarpc_parked) {
        conn->lsarpc_parked--;
    }
    DL_DELETE(conn->lsarpc_pending, pending);
} /* chimera_smb_lsarpc_unpark */

/*
 * A session has been taken off a live connection (LOGOFF, a SESSION_SETUP
 * that tears the session down) by chimera_smb_conn_remove_session_handle.
 * Every call of it still parked here is failed now, while the session can
 * still sign or encrypt the reply: once the session handle is gone, the
 * request has nothing left to complete under.  The resolver round cannot be
 * called off, so its record stays behind, detached from the request, until
 * the last answer comes and frees it.
 *
 * Finishing a call runs on the rest of its compound.  A request of this
 * session there is refused, its handle being off the connection, so none can
 * park behind this sweep; one of another session can park a lookup of its
 * own, so the list is searched again from its head after each one.
 */
void
chimera_smb_lsarpc_session_closing(
    struct chimera_smb_conn           *conn,
    struct chimera_smb_session_handle *session_handle)
{
    struct chimera_smb_lsarpc_pending *pending;
    struct chimera_smb_request        *request;

    for (;;) {
        DL_FOREACH(conn->lsarpc_pending, pending)
        {
            if (pending->request &&
                pending->request->session_handle == session_handle) {
                break;
            }
        }
        if (!pending) {
            return;
        }

        request = pending->request;
        chimera_smb_lsarpc_unpark(conn, pending);
        pending->request = NULL;
        chimera_smb_pipe_call_finish(request, -1);
    }
} /* chimera_smb_lsarpc_session_closing */

/*
 * The last answer of a parked round is in.  Unless the connection went away
 * while the request waited, give back its async credit and its parked slot,
 * and reply through pass 2.  A connection that is gone was drained by
 * chimera_smb_conn_free, which also zeroed its async count, and may already
 * serve another client: this code touches neither the connection nor its
 * counts, and fails the call -- the compound drops the reply.  (The
 * completion that follows still reads request->session_handle, as every async
 * completion that outlives its connection does.)  A call whose session was
 * closing was already finished by chimera_smb_lsarpc_session_closing, and
 * only its record is left to free.  Either way the pipe call is finished
 * exactly once.
 */
static void
chimera_smb_lsarpc_parked_finish(struct chimera_smb_lsarpc_pending *pending)
{
    struct chimera_smb_request  *request = pending->request;
    struct chimera_smb_compound *compound;
    struct chimera_smb_conn     *conn;
    int                          status;

    if (!request) {
        free(pending);
        return;
    }

    compound = request->compound;
    conn     = compound->conn;

    if (conn->generation != compound->conn_generation) {
        free(pending);
        chimera_smb_pipe_call_finish(request, -1);
        return;
    }

    chimera_smb_lsarpc_unpark(conn, pending);

    status = chimera_smb_lsarpc_final_pass(pending, request->pipe.input_iov,
                                           request->pipe.input_niov,
                                           request->pipe.output_iov);
    free(pending);
    chimera_smb_pipe_call_finish(request, status);
} /* chimera_smb_lsarpc_parked_finish */

/* One resolver answer: keep it for pass 2 (an account answer also goes into
 * the account cache, under the name as asked too -- the qualified form
 * lookup_name builds), then count the round down.
 * Runs on the request's thread, inline from chimera_smb_lsarpc_resolve or
 * later. */
static void
chimera_smb_lsarpc_resolve_cb(
    enum chimera_vfs_identity_status          status,
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data)
{
    struct chimera_smb_lsarpc_miss_rec *rec     = private_data;
    struct chimera_smb_lsarpc_pending  *pending = rec->pending;
    struct chimera_server_smb_shared   *shared  = pending->shared;

    rec->resolved.status = status;

    if (status == CHIMERA_VFS_IDENTITY_OK) {
        chimera_smb_lsa_account_from_result(rec->miss.key, rec->miss.input,
                                            result, &rec->resolved.account);

        if (shared->account_cache &&
            chimera_vfs_identity_key_is_account(rec->miss.key)) {
            chimera_smb_account_cache_put(
                shared->account_cache, &rec->resolved.account,
                rec->miss.key == CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT ?
                rec->miss.input : NULL);
        }
    }

    if (--pending->outstanding == 0 && pending->parked) {
        chimera_smb_lsarpc_parked_finish(pending);
    }
} /* chimera_smb_lsarpc_resolve_cb */

/*
 * Resolve pass 1's misses, then reply: at once when every answer came back
 * inline, else park the call (CHIMERA_SMB_PIPE_ASYNC) behind an interim until
 * the last one comes, with no deadline of its own -- the resolver calls back
 * once its handlers return, so the wait is as long as theirs.  A
 * call that could not park -- its connection already gone (a compound runs on
 * after it), no async credit free (the ceiling a blocking pipe READ uses), or
 * CHIMERA_SMB_LSARPC_MAX_PARKED lookups already parked on the connection --
 * asks the resolver nothing: pass 2 runs at once with no
 * record, so every miss gets what the caches say, as on a null session.  An
 * SMB2 CANCEL of a parked call is absorbed (smb_proc_cancel.c): the round
 * cannot be called off, so the lookup completes when it ends.
 */
static int
chimera_smb_lsarpc_resolve(
    struct chimera_smb_lsarpc_pending *pending,
    struct evpl_iovec                 *input_iov,
    int                                input_niov,
    struct evpl_iovec                 *output_iov)
{
    struct chimera_smb_request         *request = pending->request;
    struct chimera_server_smb_thread   *thread  = request->compound->thread;
    struct chimera_smb_conn            *conn    = request->compound->conn;
    struct chimera_smb_lsarpc_miss_rec *rec;
    int                                 i, status;

    /* Decided before any question is asked: an answer that does not come
     * back inline can only be waited for in a parked call. */
    if (conn->generation != request->compound->conn_generation ||
        conn->async_outstanding >=
        (uint32_t) thread->shared->config.smb2_max_async_credits - 1 ||
        conn->lsarpc_parked >= CHIMERA_SMB_LSARPC_MAX_PARKED) {
        static struct chimera_ratelimit rl;
        uint64_t                        held_back;

        /* A connection that is gone has no one left to tell. */
        if (conn->generation == request->compound->conn_generation &&
            chimera_ratelimit_allow(&rl, CHIMERA_SMB_LSARPC_REPORT_NS,
                                    &held_back)) {
            chimera_smb_info("LSA lookup: not waiting on the identity service "
                             "(%u lookup(s) waiting and %u async request(s) "
                             "outstanding on the connection); answering %d "
                             "miss(es) from the caches (%" PRIu64 " more "
                             "request(s) since the last report)",
                             conn->lsarpc_parked, conn->async_outstanding,
                             pending->nmiss + pending->over_cap, held_back);
        }
        pending->nmiss    = 0;
        pending->over_cap = 0;
        status            = chimera_smb_lsarpc_final_pass(pending, input_iov,
                                                          input_niov, output_iov);
        free(pending);
        return status;
    }

    /* The guard keeps an answer that comes back inline from ending the round
     * before every question is asked. */
    pending->outstanding = 1;

    for (i = 0; i < pending->nmiss; i++) {
        rec = &pending->miss[i];
        pending->outstanding++;
        chimera_vfs_identity_resolve(thread->vfs_thread, rec->miss.key,
                                     rec->miss.id, rec->miss.input,
                                     chimera_smb_lsarpc_resolve_cb, rec);
    }

    if (--pending->outstanding == 0) {
        status = chimera_smb_lsarpc_final_pass(pending, input_iov, input_niov,
                                               output_iov);
        free(pending);
        return status;
    }

    conn->async_outstanding++;
    conn->lsarpc_parked++;
    DL_APPEND(conn->lsarpc_pending, pending);
    chimera_smb_async_interim_begin(request);

    pending->parked = 1;
    return CHIMERA_SMB_PIPE_ASYNC;
} /* chimera_smb_lsarpc_resolve */

int
chimera_smb_lsarpc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    struct chimera_smb_session    *session = request->session_handle->session;
    /* A null session gets only what the caches know: it must not be able to
     * walk an account domain's RIDs through winbind. */
    struct chimera_smb_lsarpc_call call = {
        .request     = request,
        .may_resolve = !(session->flags & CHIMERA_SMB_SESSION_NULL),
        .final       = 0,
        .pending     = NULL,
    };
    int                            status;

    status = dce_rpc(&LSA_INTERFACE, input_iov, input_niov, output_iov, chimera_smb_lsarpc_handler, &call);

    if (status != 0) {
        free(call.pending);
        chimera_smb_error("LSA RPC transceive failed");
        return status;
    }

    /* Nothing missed: pass 1's reply is the answer. */
    if (!call.pending) {
        return 0;
    }

    return chimera_smb_lsarpc_resolve(call.pending, input_iov, input_niov,
                                      output_iov);
} /* chimera_smb_lsarpc_transceive */
