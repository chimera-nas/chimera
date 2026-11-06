// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <inttypes.h>

#include "vfs_acl_windows.h"
#include "vfs_internal.h"
#include "vfs_identity.h"
#include "vfs_procs.h"
#include "vfs_open_cache.h"
#include "vfs_user_cache.h"
#include "vfs_mount_table.h"
#include "sdk/vfs_sid.h"
#include "common/ratelimit.h"

/*
 * The algorithmic modefromsid SIDs are S-1-5-88-<kind>-<id>: revision 1, three
 * sub-authorities (88, kind, id), identifier authority 5.  They are encoded and
 * decoded here in binary form, which is the layout chimera_sid carries.
 */
#define ALGO_SID_SUBAUTHS 3
#define ALGO_SID_LEN      (CHIMERA_SID_MIN_LEN + 4 * ALGO_SID_SUBAUTHS)
#define ALGO_SID_PREFIX   88
#define ALGO_SID_KIND_UID 1
#define ALGO_SID_KIND_GID 2

static inline void
put_le32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) v;
    p[1] = (uint8_t) (v >> 8);
    p[2] = (uint8_t) (v >> 16);
    p[3] = (uint8_t) (v >> 24);
} /* put_le32 */

static inline uint32_t
get_le32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* get_le32 */

/* Rebuild `p` as a PRINCIPAL_SID carrying the algorithmic SID of kind/id. */
static void
algo_sid_principal(
    struct chimera_principal *p,
    uint32_t                  kind,
    uint32_t                  id)
{
    memset(p, 0, sizeof(*p));
    p->type        = CHIMERA_PRINCIPAL_SID;
    p->sid.len     = ALGO_SID_LEN;
    p->sid.data[0] = 1;
    p->sid.data[1] = ALGO_SID_SUBAUTHS;
    p->sid.data[7] = 5;
    put_le32(&p->sid.data[8], ALGO_SID_PREFIX);
    put_le32(&p->sid.data[12], kind);
    put_le32(&p->sid.data[16], id);
} /* algo_sid_principal */

/* Decode an algorithmic SID; returns 1 and sets kind/id, or 0 if it is not one. */
static int
algo_sid_decode(
    const struct chimera_sid *sid,
    uint32_t                 *kind,
    uint32_t                 *id)
{
    static const uint8_t authority[6] = { 0, 0, 0, 0, 0, 5 };

    if (sid->len != ALGO_SID_LEN ||
        sid->data[0] != 1 ||
        sid->data[1] != ALGO_SID_SUBAUTHS ||
        memcmp(&sid->data[2], authority, sizeof(authority)) != 0 ||
        get_le32(&sid->data[8]) != ALGO_SID_PREFIX) {
        return 0;
    }

    *kind = get_le32(&sid->data[12]);
    *id   = get_le32(&sid->data[16]);

    return *kind == ALGO_SID_KIND_UID || *kind == ALGO_SID_KIND_GID;
} /* algo_sid_decode */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_strip(struct chimera_acl *acl)
{
    int numeric_only = 0;

    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_principal *who = &acl->aces[i].who;

        if (who->type != CHIMERA_PRINCIPAL_USER &&
            who->type != CHIMERA_PRINCIPAL_GROUP) {
            continue;
        }

        if (chimera_sid_present(&who->sid)) {
            struct chimera_sid sid = who->sid;

            memset(who, 0, sizeof(*who));
            who->type = CHIMERA_PRINCIPAL_SID;
            who->sid  = sid;
        } else {
            algo_sid_principal(who,
                               who->type == CHIMERA_PRINCIPAL_USER ?
                               ALGO_SID_KIND_UID : ALGO_SID_KIND_GID,
                               who->id);
            numeric_only++;
        }
    }

    return numeric_only;
} /* chimera_vfs_acl_windows_strip */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_enrich_sync(struct chimera_acl *acl)
{
    int remaining = 0;

    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_ace *ace = &acl->aces[i];
        uint32_t            kind, id;

        if (ace->who.type != CHIMERA_PRINCIPAL_SID) {
            continue;
        }

        if (!algo_sid_decode(&ace->who.sid, &kind, &id)) {
            remaining++;
            continue;
        }

        memset(&ace->who, 0, sizeof(ace->who));
        ace->who.id = id;
        if (kind == ALGO_SID_KIND_GID) {
            ace->who.type = CHIMERA_PRINCIPAL_GROUP;
            ace->flags   |= CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        } else {
            ace->who.type = CHIMERA_PRINCIPAL_USER;
            ace->flags   &= ~CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        }
    }

    return remaining;
} /* chimera_vfs_acl_windows_enrich_sync */

/* ---- read-path enrichment --------------------------------------------- */

/*
 * One distinct native SID being resolved for a request's reply ACL.  Kept in
 * request memory, so it outlives the wrapper's frame while the request is
 * parked; `sid` is the canonical copy every ACE bearing it is matched
 * against and rewritten with.
 */
struct acl_windows_resolve {
    struct chimera_vfs_request *request;
    struct chimera_sid          sid;
};

SYMBOL_EXPORT struct chimera_vfs_attrs *
chimera_vfs_acl_windows_reply_attrs(struct chimera_vfs_request *request)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_GETATTR:
            return &request->getattr.r_attr;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            return &request->lookup_at.r_attr;
        case CHIMERA_VFS_OP_OPEN_AT:
            return &request->open_at.r_attr;
        case CHIMERA_VFS_OP_MKDIR_AT:
            return &request->mkdir_at.r_attr;
        case CHIMERA_VFS_OP_MKNOD_AT:
            return &request->mknod_at.r_attr;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            return &request->symlink_at.r_attr;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            return &request->create_unlinked.r_attr;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            return &request->open_stream.r_attr;
        case CHIMERA_VFS_OP_LINK_AT:
            return &request->link_at.r_attr;
        case CHIMERA_VFS_OP_READLINK:
            return &request->readlink.r_attr;
        case CHIMERA_VFS_OP_READ:
            return &request->read.r_attr;
        case CHIMERA_VFS_OP_SETATTR:
            return &request->setattr.r_post_attr;
        default:
            return NULL;
    } /* switch */
} /* chimera_vfs_acl_windows_reply_attrs */

/* The op's set_attr pointer, for handing the caller's own back. */
static struct chimera_vfs_attrs **
acl_windows_set_attr_slot(struct chimera_vfs_request *request)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_SETATTR:
            return &request->setattr.set_attr;
        case CHIMERA_VFS_OP_OPEN_AT:
            return &request->open_at.set_attr;
        case CHIMERA_VFS_OP_MKDIR_AT:
            return &request->mkdir_at.set_attr;
        case CHIMERA_VFS_OP_MKNOD_AT:
            return &request->mknod_at.set_attr;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            return &request->symlink_at.set_attr;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            return &request->create_unlinked.set_attr;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            return &request->open_stream.set_attr;
        default:
            return NULL;
    } /* switch */
} /* acl_windows_set_attr_slot */

/* Hand the caller's own set_attr back in place of the normalized copy. */
static void
acl_windows_restore_set_attr(struct chimera_vfs_request *request)
{
    struct chimera_vfs_attrs **slot = acl_windows_set_attr_slot(request);

    if (slot) {
        *slot = request->acl_windows.saved_set_attr;
    }
    request->acl_windows.saved_set_attr = NULL;
} /* acl_windows_restore_set_attr */

/* The mount-attrs lookup chimera_vfs_mount_is_readonly makes: the flags of
 * the mount request->fh is on, or 0 when it cannot be resolved.  The fh
 * every covered op carries in request->fh (the object, or the directory a
 * name is resolved or created in) is on the mount the reply comes from. */
static uint64_t
acl_windows_mount_flags(const struct chimera_vfs_request *request)
{
    struct chimera_vfs_mount_attrs attrs;

    if (request->fh_len < CHIMERA_VFS_MOUNT_ID_SIZE) {
        return 0;
    }

    if (chimera_vfs_mount_table_lookup_attrs(request->thread->vfs->mount_table,
                                             request->fh, &attrs) != 0) {
        return 0;
    }

    return attrs.flags;
} /* acl_windows_mount_flags */

static int
acl_windows_mount_flagged(const struct chimera_vfs_request *request)
{
    return !!(acl_windows_mount_flags(request) & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL);
} /* acl_windows_mount_flagged */

/*
 * An op that opened something hands the core a backend open to wrap in a
 * handle, and the core wraps it only on success.  Failing such an op here
 * would strand it, so close it first -- for exactly the opens the procs
 * would have retained (chimera_vfs_open_finish, open_stream_complete,
 * create_unlinked_complete).
 */
static void
acl_windows_abandon_open(struct chimera_vfs_request *request)
{
    struct chimera_vfs_attrs *attr;
    uint64_t                  vfs_private;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_OPEN_AT:
            if (!chimera_vfs_open_handle_retained(request->open_at.flags,
                                                  request->module->capabilities)) {
                return;
            }
            attr        = &request->open_at.r_attr;
            vfs_private = request->open_at.r_vfs_private;
            break;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            attr        = &request->open_stream.r_attr;
            vfs_private = request->open_stream.r_vfs_private;
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            attr        = &request->create_unlinked.r_attr;
            vfs_private = request->create_unlinked.r_vfs_private;
            break;
        default:
            return;
    } /* switch */

    if (vfs_private == CHIMERA_VFS_OPEN_CACHE_NO_BACKEND_OPEN ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        return;
    }

    chimera_vfs_close(request->thread, request->module, attr->va_fh,
                      attr->va_fh_len, vfs_private,
                      chimera_vfs_hash(attr->va_fh, attr->va_fh_len),
                      NULL, NULL);
} /* acl_windows_abandon_open */

/* A fail-closed outcome comes in floods while the identity service is down:
 * each site reports at error level, at most once per interval, with a count
 * of the reports it held back. */
#define ACL_WINDOWS_LOG_INTERVAL_NS (10ULL * 1000000000ULL)

#define acl_windows_error_limited(rl, fmt, ...) do { \
            uint64_t acl_windows_held_back; \
            if (chimera_ratelimit_allow((rl), ACL_WINDOWS_LOG_INTERVAL_NS, \
                                        &acl_windows_held_back)) { \
                chimera_vfs_error(fmt " (%" PRIu64 " more since the last " \
                                  "report)", __VA_ARGS__, \
                                  acl_windows_held_back); \
            } \
} while (0)

/* Fail closed: the op fails EIO and the unresolved ACL is withheld, so a
 * partially-resolved ACL never leaves the core. */
static void
acl_windows_fail(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr,
    const char                 *why)
{
    static struct chimera_ratelimit rl;

    acl_windows_error_limited(&rl, "acl_windows: op %u: the reply ACL could "
                              "not be resolved to unix identities (%s); "
                              "failing with EIO", request->opcode, why);

    acl_windows_abandon_open(request);
    attr->va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
    attr->va_acl       = NULL;
    request->status    = CHIMERA_VFS_EIO;
} /* acl_windows_fail */

/* Every resolve has answered: run the real completion (already restored). */
static void
acl_windows_finish(struct chimera_vfs_request *request)
{
    if (request->acl_windows.failed) {
        acl_windows_fail(request, chimera_vfs_acl_windows_reply_attrs(request),
                         "identity service unavailable");
    }

    request->complete(request);
} /* acl_windows_finish */

/* Rewrite every ACE still bearing `sid` as the identity it resolved to:
 * USER/GROUP by result->is_group, with the resolved id and the native SID
 * kept, and the IDENTIFIER_GROUP flag made to agree.  Principals are rebuilt
 * from zero (the by-value contract in vfs_acl.h). */
static void
acl_windows_rewrite(
    struct chimera_acl                       *acl,
    const struct chimera_sid                 *sid,
    const struct chimera_vfs_identity_result *result)
{
    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_ace *ace = &acl->aces[i];

        if (ace->who.type != CHIMERA_PRINCIPAL_SID ||
            !chimera_sid_equal(&ace->who.sid, sid)) {
            continue;
        }

        memset(&ace->who, 0, sizeof(ace->who));
        ace->who.sid = *sid;
        if (result->is_group) {
            ace->who.type = CHIMERA_PRINCIPAL_GROUP;
            ace->who.id   = result->group.gid;
            ace->flags   |= CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        } else {
            ace->who.type = CHIMERA_PRINCIPAL_USER;
            ace->who.id   = result->user.uid;
            ace->flags   &= ~CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        }
    }
} /* acl_windows_rewrite */

/*
 * A BY_SID answer, on the request's own thread: inline from
 * chimera_vfs_identity_resolve on a cache hit, else from the thread's
 * identity drain.  The last answer to arrive once the issuer has finished
 * issuing (active clear) completes the request; while it is still issuing,
 * the issuer does.
 */
static void
acl_windows_resolve_cb(
    enum chimera_vfs_identity_status          status,
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data)
{
    struct acl_windows_resolve *res     = private_data;
    struct chimera_vfs_request *request = res->request;

    if (status == CHIMERA_VFS_IDENTITY_OK && result) {
        acl_windows_rewrite(chimera_vfs_acl_windows_reply_attrs(request)->va_acl,
                            &res->sid, result);
    } else if (status != CHIMERA_VFS_IDENTITY_NOT_FOUND) {
        /* Infrastructure failure: fail closed.  (An authoritative NOT_FOUND
         * leaves the ACE opaque; it matches no caller.) */
        request->acl_windows.failed = 1;
    }

    /* `res` lives in request memory, which the completion frees. */
    if (--request->acl_windows.pending == 0 && !request->acl_windows.active) {
        acl_windows_finish(request);
    }
} /* acl_windows_resolve_cb */

/*
 * Move the reply ACL and owner/group SIDs out of module scratch -- valid only
 * until the module serves its next request, which may be before a parked
 * resolve answers -- into request memory, repointing the attrs, and allocate
 * room for `nsids` resolves alongside.  One allocation; NULL on failure.
 */
static struct acl_windows_resolve *
acl_windows_preserve(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr,
    int                         nsids)
{
    size_t              res_size = (size_t) nsids * sizeof(struct acl_windows_resolve);
    size_t              acl_size = chimera_acl_size(attr->va_acl->num_aces);
    uint8_t            *mem;
    struct chimera_sid *sids;

    mem = chimera_vfs_request_alloc_memory(request, res_size + acl_size +
                                           2 * sizeof(struct chimera_sid));
    if (!mem) {
        return NULL;
    }

    memcpy(mem + res_size, attr->va_acl, acl_size);
    attr->va_acl = (struct chimera_acl *) (mem + res_size);

    sids = (struct chimera_sid *) (mem + res_size + acl_size);
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) && attr->va_owner_sid) {
        sids[0]            = *attr->va_owner_sid;
        attr->va_owner_sid = &sids[0];
    }
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) && attr->va_group_sid) {
        sids[1]            = *attr->va_group_sid;
        attr->va_group_sid = &sids[1];
    }

    return (struct acl_windows_resolve *) mem;
} /* acl_windows_preserve */

/* The completion swapped in by chimera_vfs_acl_windows_gate_install. */
static void
acl_windows_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_attrs   *attr;
    struct chimera_acl         *acl;
    struct acl_windows_resolve *res;
    char                        str[CHIMERA_SID_STR_MAX];
    int                         remaining, nres = 0;

    /* Unwrap first: every exit below runs the real completion exactly once,
     * and a later re-dispatch of this request installs the wrapper afresh. */
    request->complete                   = request->acl_windows.saved_complete;
    request->acl_windows.saved_complete = NULL;

    if (request->acl_windows.saved_set_attr) {
        acl_windows_restore_set_attr(request);
    }

    attr = chimera_vfs_acl_windows_reply_attrs(request);

    if (request->status != CHIMERA_VFS_OK || !attr ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) || !attr->va_acl) {
        request->complete(request);
        return;
    }

    /* Algorithmic SIDs decode in place; if nothing else is SID-only, the
     * reply is complete as it stands and never leaves module scratch. */
    remaining = chimera_vfs_acl_windows_enrich_sync(attr->va_acl);
    if (remaining == 0) {
        request->complete(request);
        return;
    }

    res = acl_windows_preserve(request, attr, remaining);
    if (!res) {
        acl_windows_fail(request, attr, "out of request memory");
        request->complete(request);
        return;
    }
    acl = attr->va_acl;

    /* One resolve per distinct SID.  `active` holds off completion while
     * issuing: a cache hit (positive or negative) answers inline, before
     * chimera_vfs_identity_resolve returns, and must not finish the request
     * under this loop.  An inline OK has already rewritten every ACE bearing
     * its SID, so later ones are no longer PRINCIPAL_SID; the scan of `res`
     * catches the SIDs still waiting (or authoritatively unknown). */
    request->acl_windows.pending = 0;
    request->acl_windows.failed  = 0;
    request->acl_windows.active  = 1;

    for (unsigned i = 0; i < acl->num_aces; i++) {
        const struct chimera_principal *who = &acl->aces[i].who;
        int                             dup = 0;

        /* An inline UNAVAILABLE has already decided the outcome; asking
         * about the rest would only load a resolver that is down. */
        if (request->acl_windows.failed) {
            break;
        }

        if (who->type != CHIMERA_PRINCIPAL_SID ||
            !chimera_sid_present(&who->sid)) {
            continue;
        }

        for (int j = 0; j < nres; j++) {
            if (chimera_sid_equal(&res[j].sid, &who->sid)) {
                dup = 1;
                break;
            }
        }
        if (dup) {
            continue;
        }

        /* A malformed SID cannot be asked about; like an unknown one it
         * stays opaque and matches no caller. */
        if (chimera_sid_from_bin(&res[nres].sid, who->sid.data,
                                 who->sid.len) < 0 ||
            chimera_sid_to_str(&res[nres].sid, str, sizeof(str)) < 0) {
            continue;
        }

        res[nres].request = request;
        request->acl_windows.pending++;
        chimera_vfs_identity_resolve(request->thread,
                                     CHIMERA_VFS_IDENTITY_BY_SID, 0, str,
                                     acl_windows_resolve_cb, &res[nres]);
        nres++;
    }

    request->acl_windows.active = 0;

    /* Every answer arrived inline (or there was nothing to ask): complete
     * now.  Otherwise the request stays parked, and the last answer to come
     * through the identity drain completes it. */
    if (request->acl_windows.pending == 0) {
        acl_windows_finish(request);
    }
} /* acl_windows_complete */

SYMBOL_EXPORT void
chimera_vfs_acl_windows_gate_install(struct chimera_vfs_request *request)
{
    struct chimera_vfs_attrs *attr;

    /* Already wrapped: this is a re-dispatch of a request still in flight. */
    if (request->acl_windows.saved_complete) {
        return;
    }

    /* A normalized set_attr copy is handed back however the op completes;
     * otherwise only an op whose reply asks for the ACL on a flagged mount
     * needs the wrapper, and the request-local tests come before the
     * mount-attrs lookup. */
    if (!request->acl_windows.saved_set_attr) {
        attr = chimera_vfs_acl_windows_reply_attrs(request);
        if (!attr || !(attr->va_req_mask & CHIMERA_VFS_ATTR_ACL) ||
            !acl_windows_mount_flagged(request)) {
            return;
        }
    }

    request->acl_windows.saved_complete = request->complete;
    request->complete                   = acl_windows_complete;
} /* chimera_vfs_acl_windows_gate_install */

/* ---- write-path normalization ----------------------------------------- */

/*
 * The private set_attr an op carries while it is normalized, in request
 * memory -- followed there by one resolve context per distinct uid/gid and
 * then the ACL copy -- with room for the owner/group SID companions the
 * engine attaches.  The caller's set_attr and its ACL are never written.
 */
struct acl_windows_norm {
    struct chimera_vfs_attrs    attrs;
    struct chimera_vfs_request *request;
    struct chimera_sid          owner_sid;
    struct chimera_sid          group_sid;
};

/* One distinct uid (is_group clear) or gid being resolved for a set_attr. */
struct acl_windows_id_resolve {
    struct acl_windows_norm *norm;
    uint32_t                 id;
    uint8_t                  is_group;
};

/* The set_attr slot of an op whose module stores what its set_attr carries:
 * a setattr, a create, and an open or stream open that may create or
 * overwrite (a plain open's set_attr is never consulted). */
static struct chimera_vfs_attrs **
acl_windows_normalize_slot(struct chimera_vfs_request *request)
{
    const uint32_t store = CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_SETATTR:
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT:
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            if (!(request->open_at.flags & store)) {
                return NULL;
            }
            break;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            if (!(request->open_stream.flags & store)) {
                return NULL;
            }
            break;
        default:
            return NULL;
    } /* switch */

    return acl_windows_set_attr_slot(request);
} /* acl_windows_normalize_slot */

/* A uid (gid) change naming no owner (group) SID companion: the engine
* attaches the native SID the new id resolves to.  An explicit OWNER_SID
* (GROUP_SID), even a clearing NULL one, is the caller's and stands. */
static inline int
acl_windows_wants_owner_sid(const struct chimera_vfs_attrs *attr)
{
    return (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
           !(attr->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) &&
           attr->va_uid <= UINT32_MAX;
} /* acl_windows_wants_owner_sid */

static inline int
acl_windows_wants_group_sid(const struct chimera_vfs_attrs *attr)
{
    return (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
           !(attr->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) &&
           attr->va_gid <= UINT32_MAX;
} /* acl_windows_wants_group_sid */

static inline int
acl_windows_id_principal(const struct chimera_principal *who)
{
    return who->type == CHIMERA_PRINCIPAL_USER ||
           who->type == CHIMERA_PRINCIPAL_GROUP;
} /* acl_windows_id_principal */

/* The native SID a BY_UID (BY_GID) answer carries.  0 when it has none -- an
 * NSS-only identity, or a SID string that does not parse -- in which case the
 * id stays numeric and is stored as its algorithmic SID. */
static int
acl_windows_result_sid(
    const struct chimera_vfs_identity_result *result,
    int                                       is_group,
    struct chimera_sid                       *sid)
{
    const char *str;

    if (!result->is_group != !is_group) {
        return 0;
    }

    str = is_group ? result->group.sid : result->user.sid;

    return str[0] != '\0' && chimera_sid_from_str(sid, str) == 0;
} /* acl_windows_result_sid */

/*
 * Uid/gid `id` resolved to native SID `sid`: write it into every USER (GROUP)
 * principal of the private ACL naming that id without one -- rebuilt from
 * zero, the by-value contract in vfs_acl.h -- and attach it as the owner
 * (group) SID companion when the set_attr changes the uid (gid) to that id
 * without naming one.  This is the engine's own derivation from an identity
 * that has already passed the setattr gate, which ran on the request as the
 * client sent it; refreshing a stale stored companion this way is cache
 * coherence, not an ownership change.
 */
static void
acl_windows_attach_sid(
    struct acl_windows_norm  *norm,
    int                       is_group,
    uint32_t                  id,
    const struct chimera_sid *sid)
{
    struct chimera_vfs_attrs *attr = &norm->attrs;
    uint8_t                   type = is_group ? CHIMERA_PRINCIPAL_GROUP :
        CHIMERA_PRINCIPAL_USER;

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl) {
        for (unsigned i = 0; i < attr->va_acl->num_aces; i++) {
            struct chimera_principal *who = &attr->va_acl->aces[i].who;

            if (who->type != type || who->id != id ||
                chimera_sid_present(&who->sid)) {
                continue;
            }

            memset(who, 0, sizeof(*who));
            who->type = type;
            who->id   = id;
            who->sid  = *sid;
        }
    }

    if (!is_group && acl_windows_wants_owner_sid(attr) && attr->va_uid == id) {
        norm->owner_sid    = *sid;
        attr->va_owner_sid = &norm->owner_sid;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_OWNER_SID;
    } else if (is_group && acl_windows_wants_group_sid(attr) &&
               attr->va_gid == id) {
        norm->group_sid    = *sid;
        attr->va_group_sid = &norm->group_sid;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GROUP_SID;
    }
} /* acl_windows_attach_sid */

/*
 * Every resolve has answered.  If one found the resolver down, the op fails
 * EIO without being dispatched -- nothing reached the module, so nothing
 * needs unwinding but the caller's set_attr, handed back here because the
 * completion wrapper is not installed yet -- and this returns 1.  Otherwise
 * the private ACL is stripped to SID-only (an id that resolved carries its
 * native SID by now; one authoritatively without a domain identity gets its
 * algorithmic SID) and this returns 0: the op is ready to dispatch.
 */
static int
acl_windows_normalize_done(
    struct chimera_vfs_request *request,
    struct acl_windows_norm    *norm)
{
    if (request->acl_windows.failed) {
        static struct chimera_ratelimit rl;

        acl_windows_error_limited(&rl, "acl_windows: op %u: a uid/gid in the "
                                  "set_attr could not be resolved to a SID "
                                  "(identity service unavailable); failing "
                                  "with EIO", request->opcode);

        acl_windows_restore_set_attr(request);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return 1;
    }

    if ((norm->attrs.va_set_mask & CHIMERA_VFS_ATTR_ACL) && norm->attrs.va_acl) {
        chimera_vfs_acl_windows_strip(norm->attrs.va_acl);
    }

    return 0;
} /* acl_windows_normalize_done */

/*
 * A BY_UID/BY_GID answer, on the request's own thread: inline from
 * chimera_vfs_identity_resolve on a cache hit, else from the thread's
 * identity drain.  As on the read path, the last answer to arrive once the
 * issuer has finished issuing (active clear) finishes the request -- here by
 * dispatching it, back through chimera_vfs_dispatch, where saved_set_attr
 * marks it normalized so it goes on to the module.
 */
static void
acl_windows_id_resolve_cb(
    enum chimera_vfs_identity_status          status,
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data)
{
    struct acl_windows_id_resolve *res     = private_data;
    struct acl_windows_norm       *norm    = res->norm;
    struct chimera_vfs_request    *request = norm->request;
    struct chimera_sid             sid;

    if (status == CHIMERA_VFS_IDENTITY_OK && result) {
        if (acl_windows_result_sid(result, res->is_group, &sid)) {
            acl_windows_attach_sid(norm, res->is_group, res->id, &sid);
        }
    } else if (status != CHIMERA_VFS_IDENTITY_NOT_FOUND) {
        /* Infrastructure failure: fail closed rather than store an
         * algorithmic SID for an id that may have a domain identity.  (An
         * authoritative NOT_FOUND leaves it numeric: algorithmic.) */
        request->acl_windows.failed = 1;
    }

    /* `res` and `norm` live in request memory, which completion frees. */
    if (--request->acl_windows.pending == 0 && !request->acl_windows.active &&
        !acl_windows_normalize_done(request, norm)) {
        chimera_vfs_dispatch(request);
    }
} /* acl_windows_id_resolve_cb */

/* Resolve uid/gid `id` for this set_attr, unless it already has been asked
 * about (one resolve per distinct id, however many principals name it). */
static void
acl_windows_issue(
    struct acl_windows_norm       *norm,
    struct acl_windows_id_resolve *res,
    int                           *nres,
    int                            is_group,
    uint32_t                       id)
{
    struct chimera_vfs_request *request = norm->request;

    for (int j = 0; j < *nres; j++) {
        if (res[j].is_group == is_group && res[j].id == id) {
            return;
        }
    }

    res[*nres].norm     = norm;
    res[*nres].id       = id;
    res[*nres].is_group = (uint8_t) is_group;
    request->acl_windows.pending++;
    chimera_vfs_identity_resolve(request->thread,
                                 is_group ? CHIMERA_VFS_IDENTITY_BY_GID :
                                 CHIMERA_VFS_IDENTITY_BY_UID,
                                 id, NULL, acl_windows_id_resolve_cb,
                                 &res[*nres]);
    (*nres)++;
} /* acl_windows_issue */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_pre_dispatch(struct chimera_vfs_request *request)
{
    struct chimera_vfs_attrs     **slot, *sa;
    const struct chimera_acl      *acl      = NULL;
    size_t                         acl_size = 0;
    struct acl_windows_norm       *norm;
    struct acl_windows_id_resolve *res;
    uint8_t                       *mem;
    uint64_t                       flags;
    int                            ids = 0, strip = 0, nres = 0;

    /* The re-entry of a request this has already normalized. */
    if (request->acl_windows.saved_set_attr) {
        return 0;
    }

    /* Request-local tests first: an op without a set_attr carrying an ACL
     * or an owner/group change costs a switch and a mask test. */
    slot = acl_windows_normalize_slot(request);
    if (!slot || !*slot) {
        return 0;
    }
    sa = *slot;

    if (!(sa->va_set_mask & (CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_UID |
                             CHIMERA_VFS_ATTR_GID))) {
        return 0;
    }

    /* The flag is only ever set on an ACL-native module's mount (the mount
     * refuses it otherwise), so any other module is spared the lookup.  A
     * read-only mount refuses the op with EROFS in dispatch: nothing to ask
     * the resolver about. */
    if (!request->module ||
        !(request->module->capabilities & CHIMERA_VFS_CAP_ACL_NATIVE)) {
        return 0;
    }

    flags = acl_windows_mount_flags(request);
    if (!(flags & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL) ||
        (flags & CHIMERA_VFS_MOUNT_ATTR_READONLY)) {
        return 0;
    }

    if ((sa->va_set_mask & CHIMERA_VFS_ATTR_ACL) && sa->va_acl) {
        acl = sa->va_acl;

        /* Past the cap no protocol produces (and the pending count is
         * sized for). */
        if (acl->num_aces > CHIMERA_ACL_MAX_ACES) {
            static struct chimera_ratelimit rl;

            acl_windows_error_limited(&rl, "acl_windows: op %u: refusing an "
                                      "ACL of %u ACEs, over the cap of %u, "
                                      "with EINVAL", request->opcode,
                                      (unsigned) acl->num_aces,
                                      (unsigned) CHIMERA_ACL_MAX_ACES);
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return 1;
        }

        for (unsigned i = 0; i < acl->num_aces; i++) {
            const struct chimera_principal *who = &acl->aces[i].who;

            if (acl_windows_id_principal(who)) {
                strip = 1;
                if (!chimera_sid_present(&who->sid)) {
                    ids++;
                }
            }
        }
        acl_size = chimera_acl_size(acl->num_aces);
    }
    ids += acl_windows_wants_owner_sid(sa) + acl_windows_wants_group_sid(sa);

    /* SPECIAL and PRINCIPAL_SID principals are stored as they are: an op
     * with nothing to resolve or strip goes down untouched. */
    if (!strip && !ids) {
        return 0;
    }

    mem = chimera_vfs_request_alloc_memory(request, sizeof(*norm) +
                                           (size_t) ids * sizeof(*res) +
                                           acl_size);
    if (!mem) {
        static struct chimera_ratelimit rl;

        acl_windows_error_limited(&rl, "acl_windows: op %u: out of request "
                                  "memory normalizing the set_attr; failing "
                                  "with EIO", request->opcode);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return 1;
    }

    norm          = (struct acl_windows_norm *) mem;
    res           = (struct acl_windows_id_resolve *) (mem + sizeof(*norm));
    norm->attrs   = *sa;
    norm->request = request;
    if (acl) {
        norm->attrs.va_acl = (struct chimera_acl *)
            (mem + sizeof(*norm) + (size_t) ids * sizeof(*res));
        memcpy(norm->attrs.va_acl, acl, acl_size);
    }

    /* From here the op carries the copy; whichever way it completes, its
     * caller gets its own set_attr back. */
    request->acl_windows.saved_set_attr = sa;
    *slot                               = &norm->attrs;

    /* One resolve per distinct id.  `active` holds off the finish while
     * issuing: a cache hit (positive or negative) answers inline, before
     * chimera_vfs_identity_resolve returns, and must not dispatch the
     * request under this loop.  An inline OK has already written its SID
     * into every principal naming that id (and attached a companion it
     * matches), so later ones are skipped as already carrying a SID; the
     * scan of `res` in acl_windows_issue catches the ids still waiting or
     * authoritatively unknown. */
    request->acl_windows.pending = 0;
    request->acl_windows.failed  = 0;
    request->acl_windows.active  = 1;

    if (acl) {
        for (unsigned i = 0; i < norm->attrs.va_acl->num_aces; i++) {
            const struct chimera_principal *who = &norm->attrs.va_acl->aces[i].who;

            /* An inline UNAVAILABLE has already decided the outcome. */
            if (request->acl_windows.failed) {
                break;
            }

            if (!acl_windows_id_principal(who) ||
                chimera_sid_present(&who->sid)) {
                continue;
            }

            acl_windows_issue(norm, res, &nres,
                              who->type == CHIMERA_PRINCIPAL_GROUP, who->id);
        }
    }

    if (!request->acl_windows.failed &&
        acl_windows_wants_owner_sid(&norm->attrs)) {
        acl_windows_issue(norm, res, &nres, 0, (uint32_t) norm->attrs.va_uid);
    }

    if (!request->acl_windows.failed &&
        acl_windows_wants_group_sid(&norm->attrs)) {
        acl_windows_issue(norm, res, &nres, 1, (uint32_t) norm->attrs.va_gid);
    }

    request->acl_windows.active = 0;

    /* Every answer arrived inline (or there was nothing to ask): finish
     * now, and let dispatch carry on if the op survived.  Otherwise the
     * request stays parked, and the last answer to come through the
     * identity drain finishes it. */
    if (request->acl_windows.pending == 0) {
        return acl_windows_normalize_done(request, norm);
    }

    return 1;
} /* chimera_vfs_acl_windows_pre_dispatch */

/* ---- readdir: cache-only enrichment ----------------------------------- */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_fh_flagged(
    struct chimera_vfs_thread *thread,
    const uint8_t             *fh,
    uint32_t                   fh_len)
{
    struct chimera_vfs_mount_attrs attrs;

    if (fh_len < CHIMERA_VFS_MOUNT_ID_SIZE) {
        return 0;
    }

    if (chimera_vfs_mount_table_lookup_attrs(thread->vfs->mount_table, fh,
                                             &attrs) != 0) {
        return 0;
    }

    return !!(attrs.flags & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL);
} /* chimera_vfs_acl_windows_fh_flagged */

static uint64_t
acl_windows_sid_hash(const struct chimera_sid *sid)
{
    uint64_t h = 1469598103934665603ULL;

    for (unsigned i = 0; i < sid->len; i++) {
        h = (h ^ sid->data[i]) * 1099511628211ULL;
    }
    return h;
} /* acl_windows_sid_hash */

/* 1 if `h` was already recorded in seen[]; otherwise records it (while there
 * is room) and returns 0.  A full table reports "seen": the SID is simply
 * warmed by a later pass, never twice in this one. */
static int
acl_windows_warm_seen(
    uint64_t *seen,
    uint32_t *nseen,
    uint32_t  max,
    uint64_t  h)
{
    for (uint32_t i = 0; i < *nseen; i++) {
        if (seen[i] == h) {
            return 1;
        }
    }
    if (*nseen >= max) {
        return 1;
    }
    seen[(*nseen)++] = h;
    return 0;
} /* acl_windows_warm_seen */

SYMBOL_EXPORT void
chimera_vfs_acl_windows_enrich_dirent_pass(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_attrs  *attrs,
    uint64_t                  *seen,
    uint32_t                  *nseen,
    uint32_t                   max)
{
    struct chimera_acl *acl;
    struct chimera_sid  missing[CHIMERA_VFS_ACL_WARM_MAX];
    char                str[CHIMERA_SID_STR_MAX];
    uint64_t            local_seen[CHIMERA_VFS_ACL_WARM_MAX];
    uint32_t            local_n    = 0;
    unsigned            nmissing   = 0;
    int                 unresolved = 0;

    if (!(attrs->va_set_mask & CHIMERA_VFS_ATTR_ACL) || !attrs->va_acl) {
        return;
    }

    acl = attrs->va_acl;

    if (chimera_vfs_acl_windows_enrich_sync(acl) == 0) {
        return;
    }

    if (!seen) {
        seen  = local_seen;
        nseen = &local_n;
        max   = CHIMERA_VFS_ACL_WARM_MAX;
    }

    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_ace                *ace = &acl->aces[i];
        struct chimera_vfs_identity_result hit;
        struct chimera_sid                 sid;
        uint32_t                           id;
        int                                is_group = 0, found, neg;

        if (ace->who.type != CHIMERA_PRINCIPAL_SID) {
            continue;
        }

        sid = ace->who.sid;

        /* A SID that cannot be asked about is opaque for good, like an
         * authoritatively unknown one. */
        if (!chimera_sid_present(&sid) ||
            chimera_sid_to_str(&sid, str, sizeof(str)) < 0) {
            continue;
        }

        /* Cache only: the synchronous bridges, user then group as the
         * resolver's own BY_SID probe does. */
        found = chimera_vfs_identity_sid_to_uid(thread->vfs, str, &id) == 0;
        if (!found) {
            found    = chimera_vfs_identity_sid_to_gid(thread->vfs, str, &id) == 0;
            is_group = found;
        }

        if (found) {
            memset(&hit, 0, sizeof(hit));
            hit.is_group = is_group;
            if (is_group) {
                hit.group.gid = id;
            } else {
                hit.user.uid = id;
            }
            acl_windows_rewrite(acl, &sid, &hit);
            continue;
        }

        /* An authoritative miss the resolver already cached: the ACE stays
         * opaque and matches no caller, exactly as on the getattr path. */
        chimera_rcu_read_lock(&thread->vfs->vfs_user_cache->rcu);
        neg = chimera_vfs_user_cache_lookup_negative(thread->vfs->vfs_user_cache,
                                                     CHIMERA_VFS_IDENTITY_BY_SID,
                                                     0, str);
        chimera_rcu_read_unlock(&thread->vfs->vfs_user_cache->rcu);
        if (neg) {
            continue;
        }

        unresolved = 1;

        /* Distinct unresolved SIDs of this entry; past the cap the entry is
         * withheld all the same and the rest are warmed by a later pass. */
        {
            int dup = 0;

            for (unsigned j = 0; j < nmissing; j++) {
                if (chimera_sid_equal(&missing[j], &sid)) {
                    dup = 1;
                    break;
                }
            }
            if (!dup && nmissing < CHIMERA_VFS_ACL_WARM_MAX) {
                missing[nmissing++] = sid;
            }
        }
    }

    if (!unresolved) {
        return;
    }

    /* Withhold: a partially resolved ACL never leaves core. */
    attrs->va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
    attrs->va_set_mask |= CHIMERA_VFS_ATTR_ACL_WITHHELD;
    attrs->va_acl       = NULL;

    for (unsigned j = 0; j < nmissing; j++) {
        if (acl_windows_warm_seen(seen, nseen, max,
                                  acl_windows_sid_hash(&missing[j])) ||
            chimera_sid_to_str(&missing[j], str, sizeof(str)) < 0) {
            continue;
        }
        chimera_vfs_identity_warm(thread->vfs, CHIMERA_VFS_IDENTITY_BY_SID, 0,
                                  str);
    }
} /* chimera_vfs_acl_windows_enrich_dirent_pass */

SYMBOL_EXPORT void
chimera_vfs_acl_windows_enrich_dirent(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_attrs  *attrs)
{
    chimera_vfs_acl_windows_enrich_dirent_pass(thread, attrs, NULL, NULL, 0);
} /* chimera_vfs_acl_windows_enrich_dirent */
