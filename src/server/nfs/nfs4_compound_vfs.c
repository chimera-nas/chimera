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
 * WHERE THE SEQUENCE ENDS.  Usually at the end of the COMPOUND.  One op ends it
 * early: an OPEN (see nfs4_vfs_op_ends_run) is always the last op the sequence
 * carries, and whatever followed it is dispatched op by op afterwards.  Handing
 * the dispatcher back a req->index short of the end is not a special case --
 * that is what every per-op handler does when it completes.
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
    /* OPEN: an UNCHECKED4 create asked for size 0, which truncates an object
     * that already existed.  Recorded here because the create attributes it is
     * read from are blanked by the executor once the name resolves to something
     * (CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY), which is the same
     * blanking the per-op path does and for the same reason. */
    int      open_trunc_if_existed;
    /* OPEN: whether the open named a child (CLAIM_NULL) rather than re-opening
     * the current filehandle (CLAIM_FH).  The two differ in what the open
     * reports back -- an open-by-handle produces no attributes and no directory
     * change info -- and so in what may be passed on from it. */
    int      open_by_name;
};

struct nfs4_vfs_compound_ctx {
    struct nfs_request *req;
    uint32_t            num_ops;
    struct nfs4_vfs_op  ops[NFS4_VFS_COMPOUND_MAX_OPS];

    /* The OPEN this sequence carries, if any -- recorded when the sequence is
     * built, because every way out of it has to go through the OPEN's own
     * completion, including the ways where the OPEN never ran. */
    int                      open_present;
    uint32_t                 open_res_index;

    /* An OPEN that filled successfully still owes the part of itself that can
     * suspend -- the delegation grant, the deferred truncate -- and that part
     * runs after the sequence has been freed, so what it needs is copied out
     * here rather than left pointing into the compound. */
    int                      open_filled;
    int                      open_has_attr;
    struct chimera_vfs_attrs open_attr;
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
        case OP_OPEN:
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
 * Does this op end the encodable run, whatever follows it?
 *
 * OPEN does.  Everything the OPEN still owes once the object is open --
 * installing the open state, taking the share reservation, offering a
 * delegation, an UNCHECKED4 truncate -- happens when its result is filled, and
 * two of those can suspend: the delegation grant parks on an in-flight CB_NULL
 * probe, and the truncate is another VFS call.  A fill loop with ops still to
 * fill behind it cannot be suspended, so the OPEN is made the last op in the
 * sequence and completes the request itself; whatever followed it in the
 * COMPOUND is dispatched op by op from there, which is what the dispatcher
 * does anyway when it is handed back a req->index short of the end.
 */
static int
nfs4_vfs_op_ends_run(uint32_t argop)
{
    return argop == OP_OPEN;
} /* nfs4_vfs_op_ends_run */

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
 * Does the object an exclusive create collided with carry this OPEN's own
 * verifier?  If so the create is a retry of one that already succeeded and the
 * OPEN succeeds against the existing object; if not, somebody else's file is in
 * the way (RFC 7530 §16.16.4).
 */
static int
nfs4_vfs_open_verifier_matches(
    const struct OPEN4args         *args,
    const struct chimera_vfs_attrs *attr)
{
    const uint8_t *verf;
    uint32_t       verf_atime, verf_mtime;

    verf = (args->openhow.how.mode == EXCLUSIVE4) ?
           args->openhow.how.createverf :
           args->openhow.how.ch_createboth.cva_verf;

    memcpy(&verf_atime, verf, sizeof(verf_atime));
    memcpy(&verf_mtime, verf + sizeof(verf_atime), sizeof(verf_mtime));

    return (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) &&
           (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
           attr->va_atime.tv_sec == verf_atime &&
           attr->va_mtime.tv_sec == verf_mtime;
} /* nfs4_vfs_open_verifier_matches */

static int
nfs4_vfs_open_is_exclusive(const struct OPEN4args *args)
{
    return args->openhow.opentype == OPEN4_CREATE &&
           (args->openhow.how.mode == EXCLUSIVE4 ||
            args->openhow.how.mode == EXCLUSIVE4_1);
} /* nfs4_vfs_open_is_exclusive */

/*
 * Map a failed VFS op onto the status its NFSv4 operation reports.  Two
 * operations do not use the generic mapping: PUTFH turns a missing object into
 * NFS4ERR_STALE, and LISTXATTRS turns a too-small buffer into NFS4ERR_TOOSMALL
 * rather than the xattr-size error.
 */
static nfsstat4
nfs4_vfs_op_errno(
    const struct nfs_argop4              *ap,
    const struct chimera_vfs_compound_op *vop,
    struct nfs_request                   *req)
{
    enum chimera_vfs_error err   = vop->status;
    uint32_t               argop = ap->argop;

    if (argop == OP_PUTFH) {
        return chimera_nfs4_putfh_errno(err);
    }

    if (argop == OP_LISTXATTRS && err == CHIMERA_VFS_ERANGE) {
        return NFS4ERR_TOOSMALL;
    }

    /* An exclusive create whose re-open of the colliding object failed.  No
     * object that is not a regular file can be carrying the verifier an
     * exclusive create stamped, so whatever is in the way and however the
     * backend reported it, the answer the protocol wants is simply "something
     * else is already there" (RFC 7530 §16.16.4). */
    if (argop == OP_OPEN && vop->existed && nfs4_vfs_open_is_exclusive(&ap->opopen)) {
        return NFS4ERR_EXIST;
    }

    /* An OPEN refused by the type gate: the VFS reports the nearest POSIX
     * answer, but NFSv4 distinguishes a directory from a symlink from any other
     * special file, and differently in each minor version, so the mode the
     * refusal carried is what decides it.  A mode is recorded only by the
     * resolve step, which runs only when the type gate was asked for, so a
     * non-regular one here means the gate is what refused. */
    if (argop == OP_OPEN && vop->existed && vop->existing_mode &&
        !S_ISREG(vop->existing_mode)) {
        return chimera_nfs4_open_nonreg_status(req->minorversion,
                                               vop->existing_mode);
    }

    return chimera_nfs4_errno_to_nfsstat4(err);
} /* nfs4_vfs_op_errno */

/* Fill one NFSv4 result from the VFS ops that produced it. */
static nfsstat4
nfs4_vfs_op_fill(
    struct nfs_request           *req,
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx,
    const struct nfs4_vfs_op     *map,
    struct nfs_argop4            *argop,
    struct nfs_resop4            *resop)
{
    const struct chimera_vfs_compound_op *vop =
        chimera_vfs_compound_op(compound, (uint32_t) map->vfs_res);
    nfsstat4 status;
    uint32_t requested;
    void    *names;

    switch (argop->argop) {
        case OP_OPEN:
        {
            struct OPEN4args               *oargs = &argop->opopen;
            struct OPEN4res                *ores  = &resop->opopen;
            struct chimera_vfs_open_handle *handle;
            uint32_t                        install_rflags = 0;
            int                             rc;

            if (vop->existed && nfs4_vfs_open_is_exclusive(oargs)) {
                /* An exclusive create that collided.  The object was opened so
                 * that this one question could be asked of it, and the answer
                 * settles the OPEN by itself: carrying this verifier is what
                 * makes the object ours, and nothing else about it matters.
                 *
                 * In particular its TYPE does not: no object that is not a
                 * regular file can be carrying a verifier an exclusive create
                 * stamped, so a directory or a symlink here is not a type
                 * error, it is just somebody else's name (RFC 7530 §16.16.4).
                 */
                if (!nfs4_vfs_open_verifier_matches(oargs, &vop->attr)) {
                    return NFS4ERR_EXIST;
                }
            } else if ((vop->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                       !S_ISREG(vop->attr.va_mode)) {
                /* RFC 7530 §16.16.6 / RFC 8881 §18.16.4: OPEN targets a regular
                 * file.  The type gate before the open catches this for the
                 * modes that ask for it; a GUARDED4 create does not, so the
                 * object it opened is classified here, exactly as the per-op
                 * path does on its own open completion. */
                return chimera_nfs4_open_nonreg_status(req->minorversion,
                                                       vop->attr.va_mode);
            }

            handle = chimera_vfs_compound_take_handle(
                compound, (uint32_t) map->vfs_res);

            if (!handle) {
                return NFS4ERR_SERVERFAULT;
            }

            /* install_state and everything after it -- the delegation offer,
             * the completion, the 4.0 seqid advance -- read the OPEN's
             * arguments and result through req->index.  The fill loop has not
             * moved it yet, so move it here.  Safe because an OPEN is always
             * the last op of its sequence: nothing after this reads it as
             * anything else. */
            req->index = (int) map->res_index;

            /* Capture the file handle before install_state, which may release
             * the handle when it coalesces onto an existing open state. */
            memcpy(req->fh, handle->fh, handle->fh_len);
            req->fhlen = handle->fh_len;

            /* From here the two paths are the same code: the object is open
             * and its attributes are in hand, which is all install_state ever
             * needed.  It owns the handle now, including releasing it on every
             * failure.
             *
             * An open-by-handle reports no attributes, and install_state reads
             * that as "access was established when this filehandle was
             * resolved" and skips the check -- which is what it must not be
             * told by an empty attribute set that looks like a real one. */
            status = chimera_nfs4_open_install_state(req, handle,
                                                     map->open_by_name ?
                                                     &vop->attr : NULL,
                                                     vop->created,
                                                     NULL, 0,
                                                     &ores->resok4.stateid,
                                                     &install_rflags);

            if (status != NFS4_OK) {
                return status;
            }

            ores->status             = NFS4_OK;
            ores->resok4.rflags      = install_rflags |
                OPEN4_RESULT_LOCKTYPE_POSIX;
            ores->resok4.num_attrset = 0;

            /* Which of the requested attributes the create actually applied.
             * set_attr is the executor's copy, which it blanks when the name
             * resolved to something that already existed -- so an open that
             * created nothing reports nothing set, which is what the per-op
             * path reports for the same reason.
             *
             * The requested set lives in a different arm of openhow.how for
             * each create mode, and EXCLUSIVE4 has none at all: its verifier
             * occupies that slot, so reading it as an attribute request would
             * be reading the verifier's bytes as an attribute mask. */
            if (oargs->openhow.opentype == OPEN4_CREATE &&
                oargs->openhow.how.mode != EXCLUSIVE4) {
                struct chimera_vfs_attrs applied = vop->set_attr;
                uint32_t                 n_mask;
                uint32_t                *mask;

                if (oargs->openhow.how.mode == EXCLUSIVE4_1) {
                    n_mask = oargs->openhow.how.ch_createboth.cva_attrs.num_attrmask;
                    mask   = oargs->openhow.how.ch_createboth.cva_attrs.attrmask;
                } else {
                    n_mask = oargs->openhow.how.createattrs.num_attrmask;
                    mask   = oargs->openhow.how.createattrs.attrmask;
                }

                rc = xdr_dbuf_alloc_array(&ores->resok4, attrset, 4,
                                          req->encoding->dbuf);
                chimera_nfs_abort_if(rc, "Failed to allocate array");

                ores->resok4.num_attrset = chimera_nfs4_mask2attr(
                    &applied, n_mask, mask, ores->resok4.attrset);
            }

            if (map->open_by_name) {
                struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
                struct chimera_vfs_attrs post = vop->dir_post_attr;

                chimera_nfs4_set_changeinfo(&ores->resok4.cinfo, &pre, &post);
            } else {
                /* An open-by-handle changed no directory. */
                ores->resok4.cinfo.atomic = 0;
                ores->resok4.cinfo.before = 0;
                ores->resok4.cinfo.after  = 0;
            }

            /* An UNCHECKED4 size-0 create of a name that was already there.
             * Applied by chimera_nfs4_open_complete, after the share
             * reservation is held, so an OPEN that fails does not empty the
             * file on its way to failing. */
            req->open_trunc_pending = map->open_trunc_if_existed &&
                vop->existed;

            /* The rest of the OPEN -- the delegation offer and that truncate --
             * can suspend, so it runs once the sequence has been freed.  Copy
             * out what it needs; vop does not outlive the compound. */
            ctx->open_filled   = 1;
            ctx->open_has_attr = map->open_by_name;
            ctx->open_attr     = vop->attr;

            return NFS4_OK;
        }


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
                status                  = nfs4_vfs_op_errno(argop, vop, req);
                resop->opillegal.status = status;
                fail_res                = map->res_index;
                failed                  = 1;
                break;
            }
        }

        if (failed) {
            break;
        }

        status = nfs4_vfs_op_fill(req, compound, ctx, map, argop, resop);

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
     * does for a per-op handler.  When nothing failed that is the last op the
     * sequence carried, which for a sequence ending in an OPEN is short of the
     * COMPOUND's end -- the dispatcher picks the remainder up from there. */
    req->index = failed ? (int) fail_res :
        (int) ctx->ops[ctx->num_ops - 1].res_index;

    /*
     * Every way out of an OPEN goes through its own completion, not the generic
     * one.  chimera_nfs4_open_finish is what advances a 4.0 open_owner's seqid
     * and drops the reference the encoder pinned on it -- and it has to run for
     * the outcomes that FAIL too, because most OPEN errors are in the advance
     * set (RFC 7530 §9.1.7).  Skipping it on the failure path leaves the owner
     * one seqid behind, and the client's next OPEN is answered NFS4ERR_BAD_SEQID
     * for a request that was perfectly good.
     */
    if (failed && ctx->open_present && fail_res == ctx->open_res_index) {
        req->index = (int) fail_res;

        chimera_vfs_compound_free(compound);
        free(ctx);

        chimera_nfs4_open_complete(req, status);
        return;
    }

    if (ctx->open_present && !ctx->open_filled && req->open_4_0_owner) {
        /* The sequence stopped before the OPEN ran at all, so there is no
         * seqid to advance -- but the encoder's pin is still outstanding. */
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    if (!failed && ctx->open_filled) {
        /* The OPEN's own tail.  It can park on a CB_NULL probe and it can issue
         * a truncate, so it owns the completion from here; nothing of the
         * sequence may still be needed, which is why the attributes it takes
         * were copied out of the compound before this. */
        struct chimera_vfs_attrs fattr        = ctx->open_attr;
        int                      ctx_has_attr = ctx->open_has_attr;
        struct OPEN4res         *ores         =
            &req->res_compound.resarray[ctx->open_res_index].opopen;

        chimera_vfs_compound_free(compound);
        free(ctx);

        if (chimera_nfs4_open_grant_delegation(req, ores,
                                               ctx_has_attr ? &fattr : NULL)) {
            return; /* parked; resumes through nfs4_cb_null_complete */
        }

        chimera_nfs4_open_complete(req, NFS4_OK);
        return;
    }

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

/*
 * The create attributes an exclusive OPEN carries.
 *
 * EXCLUSIVE4 carries none at all -- the verifier occupies the attribute slot --
 * so the object's mode is undefined until the client's follow-up SETATTR (RFC
 * 7530 §16.16.5) and it is created owner-only, the same safe default the per-op
 * path, Linux nfsd and NFS-Ganesha all use.  EXCLUSIVE4_1 does carry
 * attributes, and takes the same default when they leave the mode out.
 *
 * Either way the verifier is stamped into atime and mtime, overwriting anything
 * the client asked for there -- which is why an EXCLUSIVE4_1 that sets
 * time_access_set or time_modify_set is refused rather than silently clobbered.
 */
static void
nfs4_vfs_open_exclusive_attrs(
    const struct OPEN4args   *args,
    struct chimera_vfs_attrs *attr)
{
    const uint8_t *verf;
    uint32_t       part;

    if (args->openhow.how.mode == EXCLUSIVE4_1) {
        chimera_nfs4_unmarshall_attrs(
            attr,
            args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
            args->openhow.how.ch_createboth.cva_attrs.attrmask,
            args->openhow.how.ch_createboth.cva_attrs.attr_vals.data,
            args->openhow.how.ch_createboth.cva_attrs.attr_vals.len,
            NULL, 0);
        verf = args->openhow.how.ch_createboth.cva_verf;
    } else {
        verf = args->openhow.how.createverf;
    }

    attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;

    memcpy(&part, verf, 4);
    attr->va_atime.tv_sec  = part;
    attr->va_atime.tv_nsec = 0;
    memcpy(&part, verf + 4, 4);
    attr->va_mtime.tv_sec  = part;
    attr->va_mtime.tv_nsec = 0;

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        attr->va_mode      = 0600;
    }
} /* nfs4_vfs_open_exclusive_attrs */

/*
 * Append an OPEN, marshalling its arguments the way the per-op path does before
 * it makes any VFS call.
 *
 * All of this is a pure function of the OPEN's own arguments -- the create mode
 * selects flags and unmarshals the create attributes, share_access selects the
 * data-access intent -- which is why it can happen here, when the sequence is
 * built, rather than from inside it.  The two things that are NOT arguments,
 * because they depend on what the name resolves to, are the options: refusing a
 * non-regular object by type, and applying the create attributes only to an
 * object this open actually creates.
 */
static int
nfs4_vfs_add_open_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop,
    struct nfs4_vfs_op          *map)
{
    const struct OPEN4args  *args = &argop->opopen;
    struct chimera_vfs_attrs attr;
    unsigned int             flags = 0;
    uint32_t                 opts  = 0;
    const char              *name    = NULL;
    int                      namelen = 0;
    uint64_t                 attr_mask;

    memset(&attr, 0, sizeof(attr));

    if (args->claim.claim == CLAIM_NULL) {
        name              = (const char *) args->claim.file.data;
        namelen           = (int) args->claim.file.len;
        map->open_by_name = 1;
    }

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        if (args->openhow.how.mode != UNCHECKED4) {
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
        }

        if (args->openhow.how.mode == UNCHECKED4 ||
            args->openhow.how.mode == GUARDED4) {
            /* Resolve an existing name's type rather than opening it, so a
             * socket or a directory is answered for by type.  An exclusive
             * create does NOT ask for this: it wants the plain collision, so
             * that whatever is in the way it can go and look at it.  (The
             * per-op path draws the line in the same place.) */
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;

            chimera_nfs4_unmarshall_attrs(&attr,
                                          args->openhow.how.createattrs.num_attrmask,
                                          args->openhow.how.createattrs.attrmask,
                                          args->openhow.how.createattrs.attr_vals.data,
                                          args->openhow.how.createattrs.attr_vals.len,
                                          NULL, 0);
        } else {
            /* EXCLUSIVE4 and EXCLUSIVE4_1 stamp the client's verifier into the
             * object's atime and mtime, which is how a repeat of the same
             * create recognises its own earlier one.  (Linux nfsd does the
             * same; a server-private xattr would be better and is a TODO on the
             * per-op path.)  A collision therefore has to be looked at rather
             * than refused, which is what EXCLUSIVE_RETRY is for. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;

            nfs4_vfs_open_exclusive_attrs(args, &attr);
        }

        if (args->openhow.how.mode == UNCHECKED4) {
            /* An UNCHECKED4 create of a name that is already there opens it
             * without restyling it, except that size 0 truncates -- and the
             * truncate is deliberately not part of the open, so an OPEN that
             * fails afterwards leaves the file's contents alone. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY |
                CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY;

            map->open_trunc_if_existed =
                (attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) && attr.va_size == 0;
        }
    } else if (namelen) {
        /* A plain open must classify a non-regular object before a backend
         * tries to open it. */
        opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY;
    }

    /* The share access the client asked for is the data-access intent the
     * engine's open gate authorizes and stamps on the handle for every later
     * stateful READ/WRITE through this open. */
    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        flags |= CHIMERA_VFS_OPEN_WRITE_ONLY;
    }

    (void) req;

    /* The same attributes the per-op path's open asks for -- no more, so an
     * object is not stat'd more thoroughly on one path than the other.  An
     * exclusive create adds the two the verifier lives in. */
    attr_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME;

    if (opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) {
        attr_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
    }

    return chimera_vfs_compound_add_open(compound, name, namelen, flags, opts,
                                         &attr, attr_mask);
} /* nfs4_vfs_add_open_op */

int
chimera_nfs4_compound_try_vfs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct chimera_vfs_compound  *compound;
    struct nfs4_vfs_compound_ctx *ctx;
    struct nfs_argop4            *argop;
    uint32_t                      first, num, nenc, i, k;
    uint8_t                       cur_fh[NFS4_FHSIZE];
    int                           cur_fhlen = 0;
    int                           lead_putfh, have_lookup = 0, have_getattr = 0;
    int                           have_lookupp = 0, have_saved = 0;
    int                           cur_moved = 0, stages_early = 0;
    int                           may_fail_late = 0;
    /* Index of the OPEN this sequence carries, or -1.  At most one: an OPEN is
     * always the last op of its run. */
    int                           open_at = -1;
    /* Set when the scan meets an op the sequence cannot carry: the run ends in
     * front of it, and the dispatcher picks up from there. */
    int                           stop = 0;
    /* The seed PUTFH the sequence always opens with. */
    uint32_t                      vfs_ops = 1;
    uint64_t                      reply_bound = 0, avail;
    int                           idx, next;
    int                           open_4_0_pinned = 0;

    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;
    /* One past the last op the sequence will carry.  Normally the whole
     * remainder; an op that ends the run (see nfs4_vfs_op_ends_run) pulls it
     * in, and what is left is dispatched op by op afterwards. */
    nenc  = num;

    if (first >= num) {
        return 0;
    }

    /* A remainder longer than the sequence can hold is not refused: the scan's
     * own vfs_ops budget ends the run when it fills up, and the rest is
     * dispatched op by op. */

    /* The dispatcher fails an op with NFS4ERR_RESOURCE rather than running it
     * when the reply buffer is nearly full; leave that to it. */
    avail = req->encoding->dbuf->size - req->encoding->dbuf->used;

    if (avail < 8192) {
        return 0;
    }

    for (i = first; i < num; i++) {
        argop = &req->args_compound->argarray[i];

        if (!nfs4_vfs_op_encodable(argop->argop)) {
            nenc = i;
            break;
        }

        /* The per-op gates decide statuses this path has no vocabulary for, so
         * anything they would reject goes back to the per-op path to be
         * rejected there. */
        if (nfs4_op_check_minor(argop->argop, req->minorversion, i,
                                req->seen_sequence) != NFS4_OK) {
            nenc = i;
            break;
        }

        if (nfs4_rofs_gate(req, argop) != NFS4_OK) {
            nenc = i;
            break;
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
            nenc = i;
            break;
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
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                break;

            case OP_LOOKUP:
                if (chimera_nfs4_validate_name(&argop->oplookup.objname) !=
                    NFS4_OK) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
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
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
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
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
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
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                if (chimera_nfs4_validate_getattr_request(
                        argop->opreaddir.num_attr_request,
                        argop->opreaddir.attr_request) != NFS4_OK) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* Per-entry ACLs are dropped for the same reason a GETATTR's
                 * is: the backend owns them only while the entry callback
                 * runs. */
                if (argop->opreaddir.num_attr_request >= 1 &&
                    (argop->opreaddir.attr_request[0] & (1U << FATTR4_ACL))) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* A page the sequence could fill and the reply could not is
                 * fine; a page the reply could fill and the sequence could not
                 * would truncate for a reason the per-op path does not have. */
                if (nfs4_vfs_readdir_max_entries(argop->opreaddir.maxcount,
                                                 avail) >
                    CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                break;

            case OP_GETXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opgetxattr.gxa_name.len)) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                break;

            case OP_SETXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opsetxattr.sxa_key.len)) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* RFC 8276 §8.3: only the three defined option values are
                 * valid, and the per-op path is what says INVAL. */
                if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
                    argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
                    argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                break;

            case OP_REMOVEXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opremovexattr.rxa_name.len)) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                break;

            case OP_GETATTR:
                if (chimera_nfs4_validate_getattr_request(
                        argop->opgetattr.num_attr_request,
                        argop->opgetattr.attr_request) != NFS4_OK) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* A backend owns the ACL it reports only for the duration of
                 * its own completion, so the copy the sequence keeps has a
                 * dangling va_acl by the time results are filled.  An ACL
                 * request must be answered by the per-op path, which marshals
                 * it while it is still live. */
                if (argop->opgetattr.num_attr_request >= 1 &&
                    (argop->opgetattr.attr_request[0] & (1U << FATTR4_ACL))) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }
                have_getattr = 1;
                break;

            case OP_OPEN:
            {
                struct OPEN4args *oa = &argop->opopen;

                /* OPEN creates, so the mutation rule applies to it exactly as
                 * it does to SETXATTR: nothing whose NFSv4-side check fails
                 * after the sequence has run may precede it. */
                if (may_fail_late) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* 4.0 classifies the open_owner's seqid before any VFS work,
                 * and that classification can answer the OPEN outright -- a
                 * replay is served from the owner's cached reply.  Answering
                 * outright means running none of the sequence, so nothing may
                 * precede the OPEN in it; the seqid advance on the way out
                 * needs no such rule, because both paths leave through the same
                 * chimera_nfs4_open_finish. */
                if (req->minorversion == 0 && i != first) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* Only the two claims that are an ordinary open of a name or of
                 * the current filehandle.  CLAIM_PREVIOUS is a reclaim,
                 * CLAIM_DELEGATE_CUR validates a delegation the client cites,
                 * and CLAIM_DELEGATE_PREV is refused outright. */
                if (oa->claim.claim != CLAIM_NULL &&
                    oa->claim.claim != CLAIM_FH) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* RFC 8881 §18.16.3: an EXCLUSIVE4_1 attribute outside
                 * suppattr_exclcreat is NFS4ERR_INVAL, and the per-op path is
                 * what says so.  Without this the verifier would silently
                 * clobber a time_access_set or time_modify_set the client
                 * asked for. */
                if (oa->openhow.opentype == OPEN4_CREATE &&
                    oa->openhow.how.mode == EXCLUSIVE4_1 &&
                    (chimera_nfs4_validate_createattrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK ||
                     chimera_nfs4_validate_exclcreat_attrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK ||
                     (oa->openhow.how.ch_createboth.cva_attrs.num_attrmask >= 1 &&
                      (oa->openhow.how.ch_createboth.cva_attrs.attrmask[0] &
                       (1U << FATTR4_ACL))))) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Statuses the per-op path decides before it opens anything. */
                if ((oa->share_access & (OPEN4_SHARE_ACCESS_READ |
                                         OPEN4_SHARE_ACCESS_WRITE)) == 0) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                if (oa->claim.claim == CLAIM_NULL &&
                    chimera_nfs4_validate_name(&oa->claim.file) != NFS4_OK) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                if (oa->openhow.opentype == OPEN4_CREATE &&
                    (oa->openhow.how.mode == UNCHECKED4 ||
                     oa->openhow.how.mode == GUARDED4)) {
                    if (chimera_nfs4_validate_createattrs(
                            oa->openhow.how.createattrs.num_attrmask,
                            oa->openhow.how.createattrs.attrmask) != NFS4_OK) {
                        {
                    nenc = i;
                    stop = 1;
                    break;
                }
                    }

                    /* An ACL in the create attributes would have to survive
                     * from the moment the sequence is built to the moment it
                     * runs, and the sequence deliberately carries no ACL. */
                    if (oa->openhow.how.createattrs.num_attrmask >= 1 &&
                        (oa->openhow.how.createattrs.attrmask[0] &
                         (1U << FATTR4_ACL))) {
                        {
                    nenc = i;
                    stop = 1;
                    break;
                }
                    }
                }

                /* The grace-window and per-client reclaim gates, which the
                 * per-op path applies at OPEN entry.  When either would refuse,
                 * let it be the one to say so. */
                if (nfs_recovery_open_check(&thread->shared->nfs4_recovery,
                                            req->session ?
                                            req->session->client_unified : NULL,
                                            false) != NFS4_OK) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                if (req->minorversion > 0 && req->session &&
                    !nfs4_client_reclaim_complete(
                        &thread->shared->nfs4_shared_clients,
                        req->session->nfs4_session_clientid)) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* A delegation grant can park on an in-flight CB_NULL probe,
                 * which suspends the OPEN.  The fill can absorb that only
                 * because the OPEN is last; but the probe is also *kicked* by
                 * the grant attempt, and which OPEN kicks it is observable
                 * (the kicking OPEN gets no delegation, the next one does).
                 * Rather than move that, leave any OPEN that could earn a
                 * delegation to the per-op path. */
                if (chimera_server_config_get_nfs4_delegations(
                        thread->shared->config)) {
                    {
                    nenc = i;
                    stop = 1;
                    break;
                }
                }

                /* Everything after an OPEN is dispatched op by op. */
                open_at = (int) i;
                nenc    = i + 1;
                break;
            }

            default:
                break;
        } /* switch */

        if (stop || nfs4_vfs_op_ends_run(argop->argop)) {
            break;
        }
    }

    /* Nothing at all was expressible, so there is no sequence to build. */
    if (nenc <= first) {
        return 0;
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

    /* ---- ops [first, nenc) are expressible: build the sequence ---- */

    /* The 4.0 OPEN's entry-time seqid classification.  Deliberately the last
     * thing before the sequence is built: it pins the open_owner on the request
     * for chimera_nfs4_open_finish to advance, so it must not run ahead of a
     * decision that could still send this COMPOUND back to the per-op path --
     * which would resolve the same owner a second time and pin it twice. */
    if (open_at >= 0 && req->minorversion == 0) {
        nfsstat4 entry_status;

        if (chimera_nfs4_open_4_0_entry(thread, req, (uint32_t) open_at,
                                        &entry_status)) {
            /* Answered without any VFS work.  The OPEN is the first op of the
             * run, so there is nothing before it that still had to happen. */
            req->index = open_at;
            chimera_nfs4_compound_complete(req, entry_status);
            return 1;
        }

        open_4_0_pinned = 1;
    }

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

    for (i = first, k = 0; i < nenc; i++, k++) {
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

            case OP_OPEN:
                ctx->open_present   = 1;
                ctx->open_res_index = i;
                idx                 = nfs4_vfs_add_open_op(req, compound,
                                                           argop, map);
                map->vfs_res        = idx;
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
     * is not observable.
     *
     * Only the ops the sequence CARRIES.  Anything past nenc is dispatched op
     * by op afterwards and the dispatcher applies this to it then; doing it
     * here as well would apply it twice, and out of order with the ops in
     * between. */
    for (i = first; i < nenc; i++) {
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

    /* The build gave up after the 4.0 entry pinned the owner.  The per-op path
     * is about to resolve it again, so drop this reference rather than leave
     * two outstanding against one chimera_nfs4_open_finish. */
    if (open_4_0_pinned && req->open_4_0_owner) {
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    chimera_vfs_compound_free(compound);
    free(ctx);

    return 0;
} /* chimera_nfs4_compound_try_vfs */
