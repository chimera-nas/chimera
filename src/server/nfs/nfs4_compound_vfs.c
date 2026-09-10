// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Running the tail of an NFSv4 COMPOUND as one VFS compound.
 *
 * The compound dispatcher normally walks the argarray one op at a time, each op
 * driving its own VFS calls and re-entering the dispatcher when it finishes.
 * When everything still to do is expressible in the VFS compound vocabulary
 * (vfs/vfs_compound.h), this instead builds the whole sequence, hands it to the
 * VFS, and fills every result from the single callback.  Nothing else changes:
 * a remainder that is not fully expressible is refused here and dispatched
 * exactly as before.
 *
 * WHY IT LIVES INSIDE THE DISPATCH LOOP.  The attempt is made just before each
 * op is dispatched, not once at compound entry.  A 4.1+ COMPOUND always opens
 * with SEQUENCE, which is not expressible; letting it dispatch normally and
 * re-trying at the next index is what makes 4.1/4.2 traffic reachable at all.
 *
 * WHAT IT REFUSES.  Every refusal below exists because the operation carries
 * NFSv4-specific handling the VFS knows nothing about -- the pseudo-root, the
 * synthetic named-attribute directory, junction shadowing at a "/" export's
 * root, the CB_GETATTR delegation combine, the wire handle's export id and
 * squash policy.  Refusing is always safe: the request is untouched and the
 * per-op path runs.
 *
 * INJECTED OPS.  Three NFSv4 operations do more VFS work than their
 * VFS-compound counterpart: PUTFH stats the handle it validates (the zero-link
 * staleness rule), READLINK stats the object before reading it (the symlink
 * type gate), and COMMIT stats it before flushing (the regular-file gate).
 * Each therefore encodes as two VFS ops, and the map below records which VFS
 * ops belong to which NFSv4 op so a failure lands on the right one.
 *
 * WHAT IS NOT IDENTICAL.  Execution stops at the first VFS failure, but the
 * checks above are NFSv4-side and are applied when the results are filled -- by
 * which time the ops after them have already run.  With one exception the
 * encodable set is read-only, and the reply is truncated at the failing op just
 * as it would be, so the wire result is unchanged; what differs is that some
 * reads were performed that the per-op path would have skipped.
 *
 * The exception is SETXATTR and REMOVEXATTR, which mutate.  A sequence that
 * fails partway leaves the earlier ones applied -- exactly as the per-op path
 * does, since it is the same calls in the same order -- but a mutation can now
 * also run *after* a check that will fail the reply.  A COMMIT whose type gate
 * says NFS4ERR_ISDIR, with a SETXATTR behind it, is the shape: the per-op path
 * would never reach the SETXATTR, this path already has.  So an NFSv4 op that
 * mutates is encoded only when nothing that could fail on an NFSv4-side check
 * precedes it in the same sequence.
 */

#include <stdlib.h>
#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "nfs4_named_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_op_matrix.h"
#include "server/server.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"

/* One NFSv4 op encodes to at most two VFS ops, so the NFSv4 op count is bounded
 * by the VFS compound's own limit. */
#define NFS4_VFS_COMPOUND_MAX_OPS CHIMERA_VFS_COMPOUND_MAX_OPS

/*
 * The least reply-buffer space one READDIR entry can consume: the attribute
 * value buffer alone is 256 bytes (see chimera_nfs4_readdir_entry_fill), before
 * the entry struct, its name and its attrmask array.  Used to bound how many
 * entries a given maxcount could possibly admit.
 */
#define NFS4_VFS_READDIR_MIN_ENTRY 256

/*
 * How many entries a READDIR's reply could possibly hold.
 *
 * Two things bound the page in the per-op path: maxcount, charged 16 bytes of
 * fixed READDIR4resok overhead before any entry, and the reply buffer's own
 * 8192-byte floor.  Whichever is smaller, divided by the least an entry can
 * cost, is the most entries that reply can carry -- so a VFS sequence asked for
 * that many will never be the thing that ends the page first.
 */
static uint32_t
nfs4_vfs_readdir_max_entries(
    uint32_t maxcount,
    uint64_t avail)
{
    uint64_t budget = maxcount - 16;
    uint64_t floor  = avail > 8192 ? avail - 8192 : 0;

    if (floor < budget) {
        budget = floor;
    }

    return (uint32_t) (budget / NFS4_VFS_READDIR_MIN_ENTRY);
} /* nfs4_vfs_readdir_max_entries */

struct nfs4_vfs_op {
    uint32_t res_index;   /* index into the COMPOUND's arg/res arrays        */
    int      vfs_lo;      /* first VFS op belonging to this NFSv4 op         */
    int      vfs_hi;      /* last VFS op belonging to this NFSv4 op          */
    int      vfs_aux;     /* injected helper getattr, or -1                  */
    int      vfs_res;     /* the VFS op this NFSv4 op's result comes from    */
};

struct nfs4_vfs_compound_ctx {
    struct nfs_request *req;
    uint32_t            num_ops;
    struct nfs4_vfs_op  ops[NFS4_VFS_COMPOUND_MAX_OPS];
};

static int
nfs4_vfs_op_encodable(uint32_t argop)
{
    switch (argop) {
        case OP_PUTFH:
        case OP_LOOKUP:
        case OP_LOOKUPP:
        case OP_GETATTR:
        case OP_ACCESS:
        case OP_GETFH:
        case OP_READLINK:
        case OP_SAVEFH:
        case OP_RESTOREFH:
        case OP_COMMIT:
        case OP_READDIR:
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_encodable */

/*
 * Does this op touch the reply buffer at a moment the two paths do not share?
 *
 * Results are marshalled in op order at the end, so the buffer fills the same
 * way it would have op by op -- including READDIR's, whose page is bounded by
 * the buffer's floor at exactly the point the per-op path would have met it.
 * The four xattr ops are the exception: GETXATTR, SETXATTR and REMOVEXATTR
 * stage their "user."-qualified name when the sequence is *built*, and GETXATTR
 * and LISTXATTRS decide how large an answer to ask for from the headroom at
 * that same moment -- before any result has been marshalled.  Each therefore
 * sees a different amount of free buffer than the op-at-a-time path would have
 * left it, and shifts the buffer under everything that follows.  The headroom
 * test in chimera_nfs4_compound_try_vfs is what stops either from changing an
 * answer.
 */
static int
nfs4_vfs_op_stages_early(uint32_t argop)
{
    switch (argop) {
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_stages_early */

/*
 * Is this an xattr name the sequence can carry?
 *
 * The per-op path stages the "user."-qualified name and reports NFS4ERR_INVAL
 * for an empty key and NFS4ERR_NAMETOOLONG for one that will not fit -- both
 * decided before any VFS call, so they belong to the per-op path.  Testing the
 * length here rather than staging the name and refusing afterwards matters: a
 * staged name cannot be given back to the reply buffer, and a refusal that had
 * consumed some of it would leave the per-op path with less to work with.
 */
static int
nfs4_vfs_xattr_name_ok(uint32_t wire_len)
{
    return wire_len > 0 &&
           wire_len <= CHIMERA_VFS_XATTR_NAME_MAX -
           CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
} /* nfs4_vfs_xattr_name_ok */

/*
 * An upper bound on the reply-buffer space one op can consume, staged or
 * filled.  Deliberately generous: it exists only to decide whether the buffer
 * is roomy enough that no size decision anywhere in the sequence can be
 * affected by how the two paths interleave their allocations.
 */
static uint64_t
nfs4_vfs_op_reply_bound(const struct nfs_argop4 *argop)
{
    /* Slack for the small fixed allocations around each result -- attrmask
     * arrays, opaque headers, and xdr_dbuf_alloc_space's own rounding. */
    const uint64_t slack = 512;

    switch (argop->argop) {
        case OP_GETATTR:
            /* An ACL request is refused, so the attribute buffer is the fixed
             * 4096 chimera_nfs4_getattr_fill reserves. */
            return 4096 + slack;
        case OP_READLINK:
            return 4096 + slack;
        case OP_GETFH:
            return CHIMERA_NFS_FH_MAX + slack;
        case OP_READDIR:
            /* Entries are charged against maxcount, plus one entry's worth for
             * the candidate that is allocated and rolled back when it does not
             * fit -- that allocation must succeed, or the entry would be
             * refused for want of buffer rather than for want of maxcount. */
            return (uint64_t) argop->opreaddir.maxcount + 4096 + slack;
        case OP_GETXATTR:
            return (uint64_t) CHIMERA_NFS4_GETXATTR_MAX +
                   CHIMERA_VFS_XATTR_NAME_MAX + slack;
        case OP_LISTXATTRS:
            /* The staged name buffer, plus the result array that points into
             * it: at most one entry per two bytes of names, 16 bytes each. */
            return 9 * (uint64_t) argop->oplistxattrs.lxa_maxcount + slack;
        case OP_SETXATTR:
        case OP_REMOVEXATTR:
            return CHIMERA_VFS_XATTR_NAME_MAX + slack;
        default:
            return slack;
    } /* switch */
} /* nfs4_vfs_op_reply_bound */

/*
 * The check an injected helper getattr exists for: PUTFH's zero-link staleness
 * rule and READLINK's symlink type gate.  Applied before the op's own VFS
 * status, because in the per-op path it is what decides whether the operation
 * proceeds at all.
 */
static nfsstat4
nfs4_vfs_op_precheck(
    struct nfs_request                   *req,
    uint32_t                              argop,
    const struct chimera_vfs_compound_op *aux)
{
    switch (argop) {
        case OP_PUTFH:
            return chimera_nfs4_putfh_check_stale(req, &aux->attr,
                                                  aux->fh, (int) aux->fh_len);
        case OP_READLINK:
            return chimera_nfs4_readlink_check_type(&aux->attr);
        case OP_COMMIT:
            /* RFC 7530 §16.4: COMMIT applies only to a regular file, and the
             * per-op path establishes that from a stat before it opens the
             * object for data. */
            if ((aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                !S_ISREG(aux->attr.va_mode)) {
                return chimera_nfs4_data_nonreg_status(aux->attr.va_mode);
            }
            return NFS4_OK;
        default:
            return NFS4_OK;
    } /* switch */
} /* nfs4_vfs_op_precheck */

/*
 * Marshal a READDIR page from the entries the sequence collected.
 *
 * The executor stops at its own entry bound; the reply's maxcount is applied
 * here, entry by entry, by the same code the per-op path runs from inside the
 * enumeration.  Encoding was refused unless maxcount is small enough that it
 * must bind first (NFS4_VFS_READDIR_MAX_MAXCOUNT), so a truncation here is the
 * same truncation the per-op path would have made at the same entry -- and when
 * nothing truncates here, the page ended exactly where the backend ended it and
 * carries the backend's own eof and cookie.
 */
static nfsstat4
nfs4_vfs_readdir_fill(
    struct nfs_request                   *req,
    struct READDIR4args                  *args,
    struct READDIR4res                   *res,
    const struct chimera_vfs_compound_op *vop)
{
    struct nfs_nfs4_readdir_cursor cursor;
    uint32_t                       i;
    uint32_t                       eof    = vop->eof;
    uint64_t                       cookie = vop->r_cookie;
    uint64_t                       cv;

    /* The fixed READDIR4resok overhead maxcount is charged before any entry:
     * cookieverf (8) plus the dirlist4 booleans (4 each). */
    cursor.count   = 16;
    cursor.entries = NULL;
    cursor.last    = NULL;

    for (i = 0; i < vop->num_entries; i++) {
        const struct chimera_vfs_compound_dirent *ent = &vop->entries[i];

        if (chimera_nfs4_readdir_entry_fill(req, args, &cursor,
                                            vop->fh, (int) vop->fh_len,
                                            ent->cookie,
                                            ent->name, (int) ent->name_len,
                                            &ent->attr) != 0) {
            /* This entry did not fit, so the page ends before it and there is
             * more to come from its cookie. */
            eof    = 0;
            cookie = ent->cookie;
            break;
        }
    }

    /* RFC 7530 §16.24.4: if not even one entry fit in maxcount and we are not
     * at end-of-directory, the buffer is too small.  Returning an empty,
     * non-eof page would stall a paging client. */
    if (!eof && cursor.entries == NULL) {
        return NFS4ERR_TOOSMALL;
    }

    cv = vop->r_verifier ? vop->r_verifier : cookie;
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = eof;
    res->resok4.reply.entries = cursor.entries;

    return NFS4_OK;
} /* nfs4_vfs_readdir_fill */

/*
 * Map a failed VFS op onto the status its NFSv4 operation reports.  Two
 * operations do not use the generic mapping: PUTFH turns a missing object into
 * NFS4ERR_STALE, and LISTXATTRS turns a too-small buffer into NFS4ERR_TOOSMALL
 * rather than the xattr-size error.
 */
static nfsstat4
nfs4_vfs_op_errno(
    uint32_t               argop,
    enum chimera_vfs_error err)
{
    if (argop == OP_PUTFH) {
        return chimera_nfs4_putfh_errno(err);
    }

    if (argop == OP_LISTXATTRS && err == CHIMERA_VFS_ERANGE) {
        return NFS4ERR_TOOSMALL;
    }

    return chimera_nfs4_errno_to_nfsstat4(err);
} /* nfs4_vfs_op_errno */

/* Fill one NFSv4 result from the VFS ops that produced it. */
static nfsstat4
nfs4_vfs_op_fill(
    struct nfs_request                *req,
    const struct chimera_vfs_compound *compound,
    const struct nfs4_vfs_op          *map,
    struct nfs_argop4                 *argop,
    struct nfs_resop4                 *resop)
{
    const struct chimera_vfs_compound_op *vop =
        chimera_vfs_compound_op(compound, (uint32_t) map->vfs_res);
    nfsstat4 status;
    uint32_t requested;
    void    *names;

    switch (argop->argop) {
        case OP_PUTFH:
            /* The staleness rule already ran as the precheck; nothing else in
             * a PUTFH4res but its status. */
            resop->opputfh.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUP:
            /* LOOKUP4res carries only a status; the object it resolved is the
             * sequence's current filehandle. */
            resop->oplookup.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUPP:
            /* As LOOKUP: the parent it resolved is the current filehandle. */
            resop->oplookupp.status = NFS4_OK;
            return NFS4_OK;

        case OP_SAVEFH:
            /* Save the object the sequence had current when the SAVEFH ran, so
             * a RESTOREFH -- here or, in principle, later -- finds it. */
            chimera_nfs4_savefh_apply(req, vop->fh, (int) vop->fh_len);
            resop->opsavefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_RESTOREFH:
            /* The executor has already made the saved object current; this
             * restores the request-side bookkeeping that rides with it.  A
             * RESTOREFH is only encoded when a SAVEFH earlier in this same
             * sequence filled the slot, so the export (and with it the squash
             * the sequence ran under) is the one already in force. */
            chimera_nfs4_restorefh_apply(req);
            resop->oprestorefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_COMMIT:
            /* The regular-file rule already ran as the precheck; this re-tests
             * it against the attributes the flush itself reported, as the
             * per-op completion does, and stamps the write verifier. */
            status = chimera_nfs4_commit_fill(req, &resop->opcommit,
                                              &vop->attr);
            resop->opcommit.status = status;
            return status;

        case OP_READDIR:
            status = nfs4_vfs_readdir_fill(req, &argop->opreaddir,
                                           &resop->opreaddir, vop);
            resop->opreaddir.status = status;
            return status;

        case OP_GETXATTR:
            status = chimera_nfs4_getxattr_fill(req, &resop->opgetxattr,
                                                vop->buffer, vop->buffer_len);
            resop->opgetxattr.gxr_status = status;
            return status;

        case OP_SETXATTR:
            chimera_nfs4_setxattr_fill(&resop->opsetxattr, &vop->pre_ctime,
                                       &vop->post_ctime);
            resop->opsetxattr.sxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_LISTXATTRS:
            /* The result entries point into the name buffer instead of copying
             * each name out of it, so it has to outlive the reply -- and the
             * sequence's buffer is freed the moment this callback returns.
             * Restage the names where the per-op path keeps them, in the reply
             * buffer, before pointing anything at them. */
            names = xdr_dbuf_alloc_space(vop->buffer_len ? vop->buffer_len : 1,
                                         req->encoding->dbuf);

            if (!names) {
                resop->oplistxattrs.lxr_status = NFS4ERR_RESOURCE;
                return NFS4ERR_RESOURCE;
            }

            memcpy(names, vop->buffer, vop->buffer_len);

            status = chimera_nfs4_listxattrs_fill(req, &resop->oplistxattrs,
                                                  names,
                                                  vop->buffer_count,
                                                  vop->eof, vop->r_cookie);
            resop->oplistxattrs.lxr_status = status;
            return status;

        case OP_REMOVEXATTR:
            chimera_nfs4_removexattr_fill(&resop->opremovexattr,
                                          &vop->pre_ctime, &vop->post_ctime);
            resop->opremovexattr.rxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_GETATTR:
            status = chimera_nfs4_getattr_fill(req, &argop->opgetattr,
                                               &resop->opgetattr, &vop->attr,
                                               vop->fh, (int) vop->fh_len);
            resop->opgetattr.status = status;
            return status;

        case OP_ACCESS:
            requested = chimera_nfs4_access_requested(req, &argop->opaccess,
                                                      &vop->attr, vop->fh,
                                                      (int) vop->fh_len);
            /* The executor evaluated the client's whole request against the
             * object's ACL (or mode) while it held it; the mapping back is
             * limited to the bits this server says it evaluated. */
            chimera_nfs4_access_fill(req, &resop->opaccess, requested,
                                     vop->granted);
            return NFS4_OK;

        case OP_GETFH:
            status = chimera_nfs4_getfh_fill(req, &resop->opgetfh,
                                             vop->fh, (int) vop->fh_len);
            resop->opgetfh.status = status;
            return status;

        case OP_READLINK:
            status = chimera_nfs4_readlink_fill(req, &resop->opreadlink,
                                                vop->target, vop->target_len);
            resop->opreadlink.status = status;
            return status;

        default:
            return NFS4ERR_SERVERFAULT;
    } /* switch */
} /* nfs4_vfs_op_fill */

/*
 * The sequence is over.  Fill the results of every NFSv4 op that ran, stopping
 * at the first that failed, and hand the request back to the reply path exactly
 * as a failing per-op handler would.
 */
static void
nfs4_vfs_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx    = private_data;
    struct nfs_request                   *req    = ctx->req;
    struct chimera_server_nfs_thread     *thread = req->thread;
    const struct chimera_vfs_compound_op *vop;
    uint32_t                              completed;
    uint32_t                              k;
    int                                   j;
    nfsstat4                              status    = NFS4_OK;
    uint32_t                              fail_res  = 0;
    int                                   failed    = 0;

    completed = chimera_vfs_compound_num_completed(compound);

    for (k = 0; k < ctx->num_ops && !failed; k++) {
        struct nfs4_vfs_op *map   = &ctx->ops[k];
        struct nfs_argop4  *argop = &req->args_compound->argarray[map->res_index];
        struct nfs_resop4  *resop = &req->res_compound.resarray[map->res_index];

        resop->resop = argop->argop;

        /* The same reply-buffer headroom gate the per-op dispatcher applies
         * before it runs an operation. */
        if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
            nfs4_fail_undispatched_op(thread, argop, resop, NFS4ERR_RESOURCE);
            status   = NFS4ERR_RESOURCE;
            fail_res = map->res_index;
            failed   = 1;
            break;
        }

        if (map->vfs_aux >= 0) {
            const struct chimera_vfs_compound_op *aux =
                chimera_vfs_compound_op(compound, (uint32_t) map->vfs_aux);

            if (aux->status == CHIMERA_VFS_OK) {
                status = nfs4_vfs_op_precheck(req, argop->argop, aux);

                if (status != NFS4_OK) {
                    resop->opillegal.status = status;
                    fail_res                = map->res_index;
                    failed                  = 1;
                    break;
                }
            }
        }

        for (j = map->vfs_lo; j <= map->vfs_hi; j++) {
            vop = chimera_vfs_compound_op(compound, (uint32_t) j);

            chimera_nfs_abort_if(vop == NULL || vop->status == CHIMERA_VFS_UNSET,
                                 "NFSv4 compound: VFS op %d never ran", j);

            if (vop->status != CHIMERA_VFS_OK) {
                status                  = nfs4_vfs_op_errno(argop->argop,
                                                            vop->status);
                resop->opillegal.status = status;
                fail_res                = map->res_index;
                failed                  = 1;
                break;
            }
        }

        if (failed) {
            break;
        }

        status = nfs4_vfs_op_fill(req, compound, map, argop, resop);

        if (status != NFS4_OK) {
            fail_res = map->res_index;
            failed   = 1;
        }
    }

    /* The current filehandle the COMPOUND is left with is whatever the last op
     * that ran was addressing (a LOOKUP that failed did not move it; a
     * RESTOREFH moved it back to the saved object).  Applied after the fills,
     * not before them, because a RESTOREFH's fill re-points req->fh at the
     * saved handle as its own bookkeeping -- for a RESTOREFH in the middle of a
     * sequence that is not where the COMPOUND ends up. */
    if (completed > 0) {
        vop = chimera_vfs_compound_op(compound, completed - 1);
        if (vop && vop->fh_len) {
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen = (int) vop->fh_len;
        }
    }

    /* Point req->index at the operation whose status the compound carries, so
     * chimera_nfs4_compound_complete truncates (or completes) exactly as it
     * does for a per-op handler. */
    req->index = failed ? (int) fail_res :
        (int) ctx->ops[ctx->num_ops - 1].res_index;

    chimera_vfs_compound_free(compound);
    free(ctx);

    chimera_nfs4_compound_complete(req, failed ? status : NFS4_OK);
} /* nfs4_vfs_compound_complete */

/* READDIR carries its verifier as an opaque; the VFS takes it as a value. */
static uint64_t
nfs4_vfs_readdir_verifier(const struct READDIR4args *args)
{
    uint64_t verifier;

    memcpy(&verifier, args->cookieverf, sizeof(verifier));

    return verifier;
} /* nfs4_vfs_readdir_verifier */

/*
 * Append one xattr op, staging its name the way the per-op handler does.
 * Returns the VFS op index, or -1 (which refuses the whole sequence) if the
 * name will not fit in the reply buffer -- the length itself was already
 * accepted by nfs4_vfs_xattr_name_ok during the scan.
 */
static int
nfs4_vfs_add_xattr_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop)
{
    const xdr_opaque *wire;
    char             *name;
    int               namelen;

    switch (argop->argop) {
        case OP_GETXATTR:
            wire = &argop->opgetxattr.gxa_name;
            break;
        case OP_SETXATTR:
            wire = &argop->opsetxattr.sxa_key;
            break;
        default:
            wire = &argop->opremovexattr.rxa_name;
            break;
    } /* switch */

    if (chimera_nfs4_xattr_stage_name(req, wire->data, wire->len,
                                      &name, &namelen) != NFS4_OK) {
        return -1;
    }

    switch (argop->argop) {
        case OP_GETXATTR:
            return chimera_vfs_compound_add_getxattr(
                compound, name, namelen,
                chimera_nfs4_xattr_stage_max(req, CHIMERA_NFS4_GETXATTR_MAX));
        case OP_SETXATTR:
            return chimera_vfs_compound_add_setxattr(
                compound, argop->opsetxattr.sxa_option, name, namelen,
                argop->opsetxattr.sxa_value.data,
                argop->opsetxattr.sxa_value.len);
        default:
            return chimera_vfs_compound_add_removexattr(compound, name,
                                                        namelen);
    } /* switch */
} /* nfs4_vfs_add_xattr_op */

int
chimera_nfs4_compound_try_vfs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct chimera_vfs_compound  *compound;
    struct nfs4_vfs_compound_ctx *ctx;
    struct nfs_argop4            *argop;
    uint32_t                      first, num, i, k;
    uint8_t                       cur_fh[NFS4_FHSIZE];
    int                           cur_fhlen = 0;
    int                           lead_putfh, have_lookup = 0, have_getattr = 0;
    int                           have_lookupp = 0, have_saved = 0;
    int                           cur_moved = 0, stages_early = 0;
    int                           may_fail_late = 0;
    /* The seed PUTFH the sequence always opens with. */
    uint32_t                      vfs_ops = 1;
    uint64_t                      reply_bound = 0, avail;
    int                           idx, next;

    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;

    if (first >= num || num - first > NFS4_VFS_COMPOUND_MAX_OPS) {
        return 0;
    }

    /* The dispatcher fails an op with NFS4ERR_RESOURCE rather than running it
     * when the reply buffer is nearly full; leave that to it. */
    avail = req->encoding->dbuf->size - req->encoding->dbuf->used;

    if (avail < 8192) {
        return 0;
    }

    for (i = first; i < num; i++) {
        argop = &req->args_compound->argarray[i];

        if (!nfs4_vfs_op_encodable(argop->argop)) {
            return 0;
        }

        /* The per-op gates decide statuses this path has no vocabulary for, so
         * anything they would reject goes back to the per-op path to be
         * rejected there. */
        if (nfs4_op_check_minor(argop->argop, req->minorversion, i,
                                req->seen_sequence) != NFS4_OK) {
            return 0;
        }

        if (nfs4_rofs_gate(req, argop) != NFS4_OK) {
            return 0;
        }

        reply_bound  += nfs4_vfs_op_reply_bound(argop);
        stages_early |= nfs4_vfs_op_stages_early(argop->argop);

        /* The three ops with an injected helper getattr cost two VFS ops (a
         * leading PUTFH costs one, because the seed below is its own).  Count
         * them up front: a sequence discovered to be too long only once it was
         * half built would have to be abandoned after staging xattr names into
         * the reply buffer, which cannot be taken back. */
        vfs_ops += (argop->argop == OP_READLINK ||
                    argop->argop == OP_COMMIT) ? 2 : 1;

        if (vfs_ops > NFS4_VFS_COMPOUND_MAX_OPS) {
            return 0;
        }

        /* Ops that can still fail once the whole sequence has run: the three
         * with an NFSv4-side precheck (PUTFH's staleness rule, READLINK's and
         * COMMIT's type gates) and READDIR, whose page can come back too small
         * to carry an entry.  Nothing that mutates may follow one -- see the
         * MUTATION note at the top. */
        switch (argop->argop) {
            case OP_PUTFH:
            case OP_READLINK:
            case OP_COMMIT:
            case OP_READDIR:
                may_fail_late = 1;
                break;
            default:
                break;
        } /* switch */

        switch (argop->argop) {
            case OP_PUTFH:
                /* One PUTFH, at the head.  A later one would switch export --
                 * and with it the squash policy and the credential the whole
                 * sequence runs under, which is fixed at submission. */
                if (i != first) {
                    return 0;
                }
                break;

            case OP_LOOKUP:
                if (chimera_nfs4_validate_name(&argop->oplookup.objname) !=
                    NFS4_OK) {
                    return 0;
                }
                have_lookup = 1;
                cur_moved   = 1;
                break;

            case OP_LOOKUPP:
                /* An export root and the pseudo-root have parents the VFS
                 * cannot name -- the namespace root, or the "/" export's real
                 * root -- so a LOOKUPP is encodable only from an object we can
                 * test here, which means the one the sequence starts from.
                 * Once a LOOKUP (or a RESTOREFH) has moved the current object,
                 * what a later LOOKUPP would climb from is not known until the
                 * sequence has already run past it. */
                if (cur_moved) {
                    return 0;
                }
                have_lookupp = 1;
                cur_moved    = 1;
                break;

            case OP_SAVEFH:
                have_saved = 1;
                break;

            case OP_RESTOREFH:
                /* Only a slot this sequence filled.  A handle saved before the
                 * sequence may belong to a different export, and restoring it
                 * re-derives the squash -- changing the credential the rest of
                 * the sequence would have to run under, which was fixed when it
                 * was submitted. */
                if (!have_saved) {
                    return 0;
                }
                cur_moved = 1;
                break;

            case OP_READDIR:
                /* The per-op path answers each of these itself, with statuses
                 * (TOOSMALL, BAD_COOKIE, an invalid attribute request) that
                 * belong to it rather than to any VFS result. */
                if (argop->opreaddir.maxcount < 16 ||
                    argop->opreaddir.cookie == 1 ||
                    argop->opreaddir.cookie == 2) {
                    return 0;
                }

                if (chimera_nfs4_validate_getattr_request(
                        argop->opreaddir.num_attr_request,
                        argop->opreaddir.attr_request) != NFS4_OK) {
                    return 0;
                }

                /* Per-entry ACLs are dropped for the same reason a GETATTR's
                 * is: the backend owns them only while the entry callback
                 * runs. */
                if (argop->opreaddir.num_attr_request >= 1 &&
                    (argop->opreaddir.attr_request[0] & (1U << FATTR4_ACL))) {
                    return 0;
                }

                /* A page the sequence could fill and the reply could not is
                 * fine; a page the reply could fill and the sequence could not
                 * would truncate for a reason the per-op path does not have. */
                if (nfs4_vfs_readdir_max_entries(argop->opreaddir.maxcount,
                                                 avail) >
                    CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES) {
                    return 0;
                }
                break;

            case OP_GETXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opgetxattr.gxa_name.len)) {
                    return 0;
                }
                break;

            case OP_SETXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opsetxattr.sxa_key.len)) {
                    return 0;
                }

                /* RFC 8276 §8.3: only the three defined option values are
                 * valid, and the per-op path is what says INVAL. */
                if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
                    argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
                    argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
                    return 0;
                }
                break;

            case OP_REMOVEXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opremovexattr.rxa_name.len)) {
                    return 0;
                }
                break;

            case OP_GETATTR:
                if (chimera_nfs4_validate_getattr_request(
                        argop->opgetattr.num_attr_request,
                        argop->opgetattr.attr_request) != NFS4_OK) {
                    return 0;
                }

                /* A backend owns the ACL it reports only for the duration of
                 * its own completion, so the copy the sequence keeps has a
                 * dangling va_acl by the time results are filled.  An ACL
                 * request must be answered by the per-op path, which marshals
                 * it while it is still live. */
                if (argop->opgetattr.num_attr_request >= 1 &&
                    (argop->opgetattr.attr_request[0] & (1U << FATTR4_ACL))) {
                    return 0;
                }
                have_getattr = 1;
                break;

            default:
                break;
        } /* switch */
    }

    /*
     * Reply-buffer headroom for a sequence that stages part of its answer while
     * it runs (see nfs4_vfs_op_stages_early).  Those stagings all happen before
     * any result has been marshalled, so the headroom READDIR, GETXATTR and
     * LISTXATTRS size their answers from is not the headroom the per-op path
     * would have left them.
     *
     * Rather than model the per-op path's allocation order, encode only when
     * the buffer is roomy enough that the question cannot arise: if the whole
     * sequence's worst case plus the dispatcher's 8192-byte floor fits in what
     * is free now, then each of those ops saturates its own cap in both paths
     * and neither path can reach the floor.  Same sizes, same statuses, same
     * entries -- whatever order the allocations happen in.
     */
    if (stages_early && reply_bound + 8192 > avail) {
        return 0;
    }

    /* At a "/" export's root a sibling export shadows any real entry of the
     * same name (nfs4_root_junction_check); the VFS resolves names in the
     * backend and cannot see that graft. */
    if (have_lookup && thread->shared->root_export_id != 0) {
        return 0;
    }

    /* With a "/" export configured, LOOKUPP has to recognize the namespace root
     * and hand back export roots' parents from it (nfs4_root_export_fh_get);
     * neither is visible to the VFS. */
    if (have_lookupp && thread->shared->root_export_id != 0) {
        return 0;
    }

    /* RFC 7530/8881 §10.4.3: when another client holds a write delegation the
     * GETATTR must query it via CB_GETATTR and combine the answer.  That query
     * only ever runs for a client reached through a session. */
    if (have_getattr &&
        chimera_server_config_get_nfs4_delegations(thread->shared->config) &&
        req->session && req->session->client_unified) {
        return 0;
    }

    /* Establish the object the sequence starts from. */
    lead_putfh = (req->args_compound->argarray[first].argop == OP_PUTFH);

    if (lead_putfh) {
        struct PUTFH4args *pa = &req->args_compound->argarray[first].opputfh;

        /* Mirrors chimera_nfs4_putfh.  The pseudo-root is not a wrapped VFS
         * handle at all; the decode authenticates the wire handle, recovers the
         * inner VFS handle, and re-derives the export id and squashed
         * credential the sequence will run under; a named-attribute directory
         * handle addresses a synthetic object the VFS does not have. */
        if (fh_is_nfs4_root(pa->object.data, pa->object.len) ||
            pa->object.len > NFS4_FHSIZE) {
            return 0;
        }

        if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                  cur_fh, &cur_fhlen) != CHIMERA_NFS_FH_OK) {
            return 0;
        }

        if (chimera_nfs4_fh_is_attrdir(cur_fh, cur_fhlen) ||
            !chimera_vfs_fh_is_plausible(thread->vfs_thread, cur_fh,
                                         cur_fhlen)) {
            return 0;
        }
    } else {
        if (req->fhlen == 0 ||
            fh_is_nfs4_root(req->fh, req->fhlen) ||
            chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
            return 0;
        }

        memcpy(cur_fh, req->fh, req->fhlen);
        cur_fhlen = req->fhlen;
    }

    /* An export root's parent is the NFSv4 namespace root, not the backend's
     * physical parent -- a graft the VFS knows nothing about, so its ".." is
     * the wrong answer.  Only reachable for a LOOKUPP from the object the
     * sequence starts from, which is the only one a LOOKUPP is encoded for. */
    if (have_lookupp &&
        chimera_nfs4_fh_is_vfs_mount_root(thread->vfs, cur_fh,
                                          (uint32_t) cur_fhlen)) {
        return 0;
    }

    /* ---- everything is expressible: build the sequence ---- */

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate NFSv4 compound context");
    ctx->req = req;

    /* Seed the current object.  When the remainder opens with a PUTFH this is
     * that PUTFH; otherwise it re-states the COMPOUND's current filehandle and
     * belongs to whichever op comes first. */
    idx = chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen);

    if (idx < 0) {
        goto refuse;
    }

    next = 0;

    for (i = first, k = 0; i < num; i++, k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];

        argop = &req->args_compound->argarray[i];

        map->res_index = i;
        map->vfs_lo    = next;
        map->vfs_aux   = -1;

        switch (argop->argop) {
            case OP_PUTFH:
                /* The seed PUTFH above is this op; stat the handle so the
                 * zero-link staleness rule has something to test. */
                map->vfs_res = idx;
                idx          = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_NLINK);
                map->vfs_aux = idx;
                break;

            case OP_LOOKUP:
                idx = chimera_vfs_compound_add_lookup(
                    compound,
                    (const char *) argop->oplookup.objname.data,
                    (int) argop->oplookup.objname.len,
                    0);
                map->vfs_res = idx;
                break;

            case OP_GETATTR:
                idx = chimera_vfs_compound_add_getattr(
                    compound,
                    chimera_nfs4_attr2mask(argop->opgetattr.attr_request,
                                           argop->opgetattr.num_attr_request));
                map->vfs_res = idx;
                break;

            case OP_ACCESS:
                idx = chimera_vfs_compound_add_access(
                    compound,
                    chimera_nfs4_access4_to_mask(argop->opaccess.access));
                map->vfs_res = idx;
                break;

            case OP_GETFH:
                idx          = chimera_vfs_compound_add_getfh(compound);
                map->vfs_res = idx;
                break;

            case OP_READLINK:
                /* The type gate READLINK applies before it reads. */
                idx          = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_readlink(compound);
                }
                map->vfs_res = idx;
                break;

            case OP_LOOKUPP:
                idx          = chimera_vfs_compound_add_lookupp(compound, 0);
                map->vfs_res = idx;
                break;

            case OP_SAVEFH:
                idx          = chimera_vfs_compound_add_savefh(compound);
                map->vfs_res = idx;
                break;

            case OP_RESTOREFH:
                idx          = chimera_vfs_compound_add_restorefh(compound);
                map->vfs_res = idx;
                break;

            case OP_COMMIT:
                /* The regular-file gate COMMIT applies before it flushes, from
                 * a stat of the object taken through a path open -- the same
                 * two-open shape the per-op path has. */
                idx          = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_commit(
                        compound,
                        argop->opcommit.offset,
                        argop->opcommit.count,
                        CHIMERA_VFS_ATTR_MODE);
                }
                map->vfs_res = idx;
                break;

            case OP_READDIR:
                idx = chimera_vfs_compound_add_readdir(
                    compound,
                    argop->opreaddir.cookie,
                    nfs4_vfs_readdir_verifier(&argop->opreaddir),
                    argop->opreaddir.dircount,
                    argop->opreaddir.maxcount,
                    nfs4_vfs_readdir_max_entries(argop->opreaddir.maxcount,
                                                 avail),
                    chimera_nfs4_attr2mask(argop->opreaddir.attr_request,
                                           argop->opreaddir.num_attr_request));
                map->vfs_res = idx;
                break;

            case OP_GETXATTR:
                idx = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_SETXATTR:
                idx = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_LISTXATTRS:
                idx = chimera_vfs_compound_add_listxattrs(
                    compound,
                    argop->oplistxattrs.lxa_cookie,
                    chimera_nfs4_xattr_stage_max(
                        req, argop->oplistxattrs.lxa_maxcount));
                map->vfs_res = idx;
                break;

            case OP_REMOVEXATTR:
                idx = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            default:
                idx = -1;
                break;
        } /* switch */

        if (idx < 0) {
            goto refuse;
        }

        map->vfs_hi = idx;
        next        = idx + 1;
    }

    ctx->num_ops = k;

    /* NFS4.1 current-stateid lifecycle (RFC 8881 §16.2.3.1.2): an op that
     * changes the current filehandle clears the current stateid, and
     * SAVEFH/RESTOREFH carry it alongside the saved filehandle.  The per-op
     * dispatcher applies this before dispatching each op; replaying the whole
     * sequence's worth here, in order, is the same thing -- the value never
     * leaves the request, so applying it up front rather than as each op runs
     * is not observable. */
    for (i = first; i < num; i++) {
        switch (req->args_compound->argarray[i].argop) {
            case OP_PUTFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
                chimera_nfs4_clear_current_stateid(req);
                break;
            case OP_SAVEFH:
                req->saved_current_stateid_valid = req->current_stateid_valid;
                req->saved_current_stateid       = req->current_stateid;
                break;
            case OP_RESTOREFH:
                req->current_stateid_valid = req->saved_current_stateid_valid;
                req->current_stateid       = req->saved_current_stateid;
                break;
            default:
                break;
        } /* switch */
    }

    chimera_vfs_compound_submit(compound, nfs4_vfs_compound_complete, ctx);

    return 1;

 refuse:

    chimera_vfs_compound_free(compound);
    free(ctx);

    return 0;
} /* chimera_nfs4_compound_try_vfs */
