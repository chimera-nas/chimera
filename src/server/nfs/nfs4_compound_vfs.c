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
 * INJECTED OPS.  Two NFSv4 operations do more VFS work than their VFS-compound
 * counterpart: PUTFH stats the handle it validates (the zero-link staleness
 * rule) and READLINK stats the object before reading it (the symlink type
 * gate).  Each therefore encodes as two VFS ops, and the map below records
 * which VFS ops belong to which NFSv4 op so a failure lands on the right one.
 *
 * WHAT IS NOT IDENTICAL.  Execution stops at the first VFS failure, but the two
 * checks above are NFSv4-side and are applied when the results are filled --
 * by which time the ops after them have already run.  Every operation in the
 * encodable set is read-only, and the reply is truncated at the failing op just
 * as it would be, so the wire result is unchanged; what differs is that some
 * reads were performed that the per-op path would have skipped.
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
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"

/* One NFSv4 op encodes to at most two VFS ops, so the NFSv4 op count is bounded
 * by the VFS compound's own limit. */
#define NFS4_VFS_COMPOUND_MAX_OPS CHIMERA_VFS_COMPOUND_MAX_OPS

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
        case OP_GETATTR:
        case OP_ACCESS:
        case OP_GETFH:
        case OP_READLINK:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_encodable */

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
        default:
            return NFS4_OK;
    } /* switch */
} /* nfs4_vfs_op_precheck */

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

    /* The current filehandle the COMPOUND is left with is whatever the last op
     * that ran was addressing (a LOOKUP that failed did not move it). */
    if (completed > 0) {
        vop = chimera_vfs_compound_op(compound, completed - 1);
        if (vop && vop->fh_len) {
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen = (int) vop->fh_len;
        }
    }

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
                status = (argop->argop == OP_PUTFH) ?
                    chimera_nfs4_putfh_errno(vop->status) :
                    chimera_nfs4_errno_to_nfsstat4(vop->status);
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

    /* Point req->index at the operation whose status the compound carries, so
     * chimera_nfs4_compound_complete truncates (or completes) exactly as it
     * does for a per-op handler. */
    req->index = failed ? (int) fail_res :
        (int) ctx->ops[ctx->num_ops - 1].res_index;

    chimera_vfs_compound_free(compound);
    free(ctx);

    chimera_nfs4_compound_complete(req, failed ? status : NFS4_OK);
} /* nfs4_vfs_compound_complete */

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
    int                           idx, next;

    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;

    if (first >= num || num - first > NFS4_VFS_COMPOUND_MAX_OPS) {
        return 0;
    }

    /* The dispatcher fails an op with NFS4ERR_RESOURCE rather than running it
     * when the reply buffer is nearly full; leave that to it. */
    if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
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

    /* At a "/" export's root a sibling export shadows any real entry of the
     * same name (nfs4_root_junction_check); the VFS resolves names in the
     * backend and cannot see that graft. */
    if (have_lookup && thread->shared->root_export_id != 0) {
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
     * changes the current filehandle clears the current stateid. */
    if (lead_putfh || have_lookup) {
        chimera_nfs4_clear_current_stateid(req);
    }

    chimera_vfs_compound_submit(compound, nfs4_vfs_compound_complete, ctx);

    return 1;

 refuse:

    chimera_vfs_compound_free(compound);
    free(ctx);

    return 0;
} /* chimera_nfs4_compound_try_vfs */
