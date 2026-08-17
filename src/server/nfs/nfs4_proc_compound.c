// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_recovery.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"
#include "evpl/evpl_bind.h"
#include "nfs4_dump.h"
#include "nfs4_trace.h"
#include "nfs4_op_matrix.h"
#include "nfs4_rofs.h"
#include "nfs_drc_reply.h"

static int
nfs4_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    return nfs_drc_send_cached_reply(thread,
                                     req->encoding,
                                     req->replay_slot->cached_buf,
                                     req->replay_slot->cached_len);
} /* nfs4_send_cached_reply */

static void
nfs4_release_write_args(
    struct chimera_server_nfs_thread *thread,
    struct COMPOUND4args             *args)
{
    for (uint32_t i = 0; i < args->num_argarray; i++) {
        struct nfs_argop4 *ap = &args->argarray[i];

        if (ap->argop == OP_WRITE && ap->opwrite.data.niov) {
            evpl_iovecs_release(thread->evpl,
                                ap->opwrite.data.iov,
                                ap->opwrite.data.niov);
            ap->opwrite.data.niov = 0;
        }
    }
} /* nfs4_release_write_args */

/*
 * Enforce per-export read-only policy at dispatch time.  Extracts the inputs
 * nfs4_rofs_check needs from the request and the op's arguments; that header
 * carries the policy itself and the reasoning behind each branch.
 *
 * This gate covers only mutations reached through the current filehandle.  Ops
 * that mutate a handle carried by a stateid cannot use it to escape: ALLOCATE,
 * DEALLOCATE, and COPY call nfs_state_check_write_for_fh, and SETATTR performs
 * the same stateid-vs-current-filehandle check inline, so the stateid must
 * name the gated filehandle.  WRITE instead relies on its NFS4ERR_OPENMODE
 * check: a write-mode open can only have been granted through a writable
 * export.  (After a runtime flip to read-only, existing write-mode opens keep
 * writing -- the same window the COMMIT rationale in nfs4_rofs.h accepts.)
 *
 * Only call this after nfs4_op_check_minor has accepted the op (argop is then a
 * valid matrix index).
 */
static nfsstat4
nfs4_rofs_gate(
    struct nfs_request      *req,
    const struct nfs_argop4 *argop)
{
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    struct nfs4_rofs_input            in     = {
        .op            = argop->argop,
        .export_ro     = chimera_nfs_export_id_is_ro(shared, req->export_id),
        .have_saved_fh = req->saved_fhlen != 0,
    };

    if (in.have_saved_fh) {
        in.saved_export_ro = chimera_nfs_export_id_is_ro(shared,
                                                         req->saved_export_id);
    }

    if (argop->argop == OP_OPEN) {
        in.open_writes =
            (argop->opopen.share_access & OPEN4_SHARE_ACCESS_WRITE) != 0 ||
            argop->opopen.openhow.opentype == OPEN4_CREATE;
    } else if (argop->argop == OP_OPENATTR) {
        in.openattr_creates = argop->opopenattr.createdir != 0;
    }

    return nfs4_rofs_check(&in);
} /* nfs4_rofs_gate */

/*
 * Prepare the result slot of an op that failed before any handler ran.
 *
 * The per-op result union is uninitialized dbuf memory (xdr_dbuf_alloc_space
 * does not zero), and some result types marshal fields even on the error
 * branch -- SETATTR4res carries num_attrsset and attrsset outside any union --
 * so an un-zeroed slot encodes heap contents onto the wire or crashes the
 * encoder.  Zero it and set the discriminant.
 *
 * A WRITE that never dispatches also still holds the +1 the XDR unmarshal
 * cloned onto its data iovecs: chimera_nfs4_compound_complete sweeps only the
 * WRITEs past the failed op, and the release that normally happens in the
 * WRITE completion will never run.  Releasing here is idempotent with that
 * sweep and with the replay path, both of which skip a niov already zeroed.
 *
 * WRITE is the only op needing the release: xdr_iovecr appears only on
 * WRITE4args.data, while SETXATTR, SETATTR and CREATE payloads are xdr_opaque
 * in dbuf memory freed with the encoding.
 */
static void
nfs4_fail_undispatched_op(
    struct chimera_server_nfs_thread *thread,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop,
    nfsstat4                          status)
{
    memset(resop, 0, sizeof(*resop));

    /* An out-of-range argop is not a valid result discriminant. */
    resop->resop = (status == NFS4ERR_OP_ILLEGAL) ? OP_ILLEGAL : argop->argop;

    if (argop->argop == OP_WRITE && argop->opwrite.data.niov) {
        evpl_iovecs_release(thread->evpl,
                            argop->opwrite.data.iov,
                            argop->opwrite.data.niov);
        argop->opwrite.data.niov = 0;
    }
} /* nfs4_fail_undispatched_op */

void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_argop4                *argop;
    struct nfs_resop4                *resop;
    int                               rc;

 again:

    /* Close out the previous operation's span (if any).  Every continuation --
     * the synchronous goto-again below and the async re-entry from
     * compound_complete -- lands here with req->index already advanced, so this
     * ends each op's span exactly once. */
    nfs4_trace_op_end(req);

    /* SEQUENCE replay short-circuit: the SEQUENCE op detected a
     * retransmit on a CACHED slot.  Resend the cached COMPOUND body through
     * the current RPC request so the reply carries the retransmit's XID, then
     * skip compound execution entirely.  The cached buf is alive because the
     * slot stays CACHED until the next seqid+1 advances it. */
    if (req->replay_action == NFS4_REPLAY_ACTION_FROM_CACHE &&
        req->replay_slot && req->replay_slot->cached_buf) {
        /* XDR clones a +1 ref on every WRITE4args.data; the retransmit
         * we are about to discard had its args unmarshalled but will
         * never dispatch, so release any cloned iovecs here. */
        nfs4_release_write_args(thread, req->args_compound);
        rc = nfs4_send_cached_reply(thread, req);
        chimera_nfs_abort_if(rc, "Failed to send cached RPC2 reply");
        /* Release the slot the retransmit path claimed (IN_PROGRESS) back to
        * CACHED now that the pinned buffer has been sent; this re-arms the
        * slot for further retransmits and lets a waiting advance proceed. */
        nfs4_replay_slot_replay_done(req);
        nfs_request_free(thread, req);
        return;
    }

    if (status != NFS4_OK) {
        req->res_compound.status = status;
        req->index               = req->res_compound.num_resarray;
    }

    if (req->index >= req->res_compound.num_resarray) {

        //dump_COMPOUND4res("res", &req->res_compound);

        if (req->session &&
            req->res_compound.status == NFS4_OK &&
            req->res_compound.num_resarray > 0 &&
            req->session->nfs4_session_fore_attrs.ca_maxresponsesize &&
            marshall_length_COMPOUND4res(&req->res_compound) >
            (int) req->session->nfs4_session_fore_attrs.ca_maxresponsesize) {
            req->index = req->res_compound.num_resarray - 1;
            chimera_nfs4_compound_complete(req, NFS4ERR_REP_TOO_BIG);
            return;
        }

        rc = shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            NULL,
            &req->res_compound,
            req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        /* Advance the SEQUENCE replay slot to CACHED/COMPLETED.  Must
         * run *after* send_reply because the reply-capture callback
         * (armed in nfs4_replay_slot_acquire when sa_cachethis was set)
         * fires from inside send_reply and writes the captured bytes
         * onto slot->cached_buf, which finalize then promotes to
         * CACHED state. */
        nfs4_replay_slot_finalize(req);

        nfs_request_free(thread, req);
        return;
    }

    argop = &req->args_compound->argarray[req->index];
    resop = &req->res_compound.resarray[req->index];

    resop->resop = argop->argop;

    /* Open a span for this operation (child of the compound span) and re-point
     * the VFS parent at it so the op's VFS work nests under the op, not the
     * whole compound. */
    nfs4_trace_op_begin(req, argop);

    thread->active = 1;

    {
        nfsstat4 gate;

        /* If the response buffer is running low, fail early with RESOURCE
         * rather than letting individual procs abort on allocation failure */
        if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
            gate = NFS4ERR_RESOURCE;
        } else {
            gate = nfs4_op_check_minor(argop->argop,
                                       req->minorversion,
                                       (uint32_t) req->index,
                                       req->seen_sequence);

            if (gate == NFS4_OK) {
                gate = nfs4_rofs_gate(req, argop);
            }
        }

        if (gate != NFS4_OK) {
            nfs4_fail_undispatched_op(thread, argop, resop, gate);
            chimera_nfs4_compound_complete(req, gate);
        } else {
            /* NFS4.1 current-stateid lifecycle (RFC 8881 §16.2.3.1.2):
             * ops that change the current filehandle clear the current
             * stateid, while SAVEFH/RESTOREFH carry it alongside the
             * saved filehandle.  Stateid-returning ops set it in their
             * own handlers. */
            switch (argop->argop) {
                case OP_PUTFH:
                case OP_PUTROOTFH:
                case OP_PUTPUBFH:
                case OP_LOOKUP:
                case OP_LOOKUPP:
                case OP_CREATE:
                case OP_OPENATTR:
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

            switch (argop->argop) {
                case OP_ACCESS:
                    chimera_nfs4_access(thread, req, argop, resop);
                    break;
                case OP_GETFH:
                    chimera_nfs4_getfh(thread, req, argop, resop);
                    break;
                case OP_PUTROOTFH:
                    chimera_nfs4_putrootfh(thread, req, argop, resop);
                    break;
                case OP_PUTPUBFH:
                    chimera_nfs4_putpubfh(thread, req, argop, resop);
                    break;
                case OP_GETATTR:
                    chimera_nfs4_getattr(thread, req, argop, resop);
                    break;
                case OP_SETATTR:
                    chimera_nfs4_setattr(thread, req, argop, resop);
                    break;
                case OP_CREATE:
                    chimera_nfs4_create(thread, req, argop, resop);
                    break;
                case OP_LOOKUP:
                    chimera_nfs4_lookup(thread, req, argop, resop);
                    break;
                case OP_LOOKUPP:
                    chimera_nfs4_lookupp(thread, req, argop, resop);
                    break;
                case OP_PUTFH:
                    chimera_nfs4_putfh(thread, req, argop, resop);
                    break;
                case OP_SAVEFH:
                    chimera_nfs4_savefh(thread, req, argop, resop);
                    break;
                case OP_RESTOREFH:
                    chimera_nfs4_restorefh(thread, req, argop, resop);
                    break;
                case OP_LINK:
                    chimera_nfs4_link(thread, req, argop, resop);
                    break;
                case OP_RENAME:
                    chimera_nfs4_rename(thread, req, argop, resop);
                    break;
                case OP_OPEN:
                    chimera_nfs4_open(thread, req, argop, resop);
                    break;
                case OP_READDIR:
                    chimera_nfs4_readdir(thread, req, argop, resop);
                    break;
                case OP_READ:
                    chimera_nfs4_read(thread, req, argop, resop);
                    break;
                case OP_WRITE:
                    chimera_nfs4_write(thread, req, argop, resop);
                    break;
                case OP_COPY:
                    chimera_nfs4_copy(thread, req, argop, resop);
                    break;
                case OP_COMMIT:
                    chimera_nfs4_commit(thread, req, argop, resop);
                    break;
                case OP_CLOSE:
                    chimera_nfs4_close(thread, req, argop, resop);
                    break;
                case OP_REMOVE:
                    chimera_nfs4_remove(thread, req, argop, resop);
                    break;
                case OP_READLINK:
                    chimera_nfs4_readlink(thread, req, argop, resop);
                    break;
                case OP_SETCLIENTID:
                    chimera_nfs4_setclientid(thread, req, argop, resop);
                    break;
                case OP_SETCLIENTID_CONFIRM:
                    chimera_nfs4_setclientid_confirm(thread, req, argop, resop);
                    break;
                case OP_RENEW:
                    chimera_nfs4_renew(thread, req, argop, resop);
                    break;
                case OP_OPEN_CONFIRM:
                    chimera_nfs4_open_confirm(thread, req, argop, resop);
                    break;
                case OP_OPEN_DOWNGRADE:
                    chimera_nfs4_open_downgrade(thread, req, argop, resop);
                    break;
                case OP_RELEASE_LOCKOWNER:
                    chimera_nfs4_release_lockowner(thread, req, argop, resop);
                    break;
                case OP_EXCHANGE_ID:
                    chimera_nfs4_exchange_id(thread, req, argop, resop);
                    break;
                case OP_CREATE_SESSION:
                    chimera_nfs4_create_session(thread, req, argop, resop);
                    break;
                case OP_DESTROY_SESSION:
                    chimera_nfs4_destroy_session(thread, req, argop, resop);
                    break;
                case OP_DESTROY_CLIENTID:
                    chimera_nfs4_destroy_clientid(thread, req, argop, resop);
                    break;
                case OP_SEQUENCE:
                    req->seen_sequence = true;
                    chimera_nfs4_sequence(thread, req, argop, resop);
                    break;
                case OP_RECLAIM_COMPLETE:
                    chimera_nfs4_reclaim_complete(thread, req, argop, resop);
                    break;
                case OP_SECINFO:
                    chimera_nfs4_secinfo(thread, req, argop, resop);
                    break;
                case OP_SECINFO_NO_NAME:
                    chimera_nfs4_secinfo_no_name(thread, req, argop, resop);
                    break;
                case OP_FREE_STATEID:
                    chimera_nfs4_free_stateid(thread, req, argop, resop);
                    break;
                case OP_BACKCHANNEL_CTL:
                    chimera_nfs4_backchannel_ctl(thread, req, argop, resop);
                    break;
                case OP_BIND_CONN_TO_SESSION:
                    chimera_nfs4_bind_conn_to_session(thread, req, argop, resop);
                    break;
                case OP_TEST_STATEID:
                    chimera_nfs4_test_stateid(thread, req, argop, resop);
                    break;
                case OP_SET_SSV:
                    chimera_nfs4_set_ssv(thread, req, argop, resop);
                    break;
                case OP_ALLOCATE:
                    chimera_nfs4_allocate(thread, req, argop, resop);
                    break;
                case OP_DEALLOCATE:
                    chimera_nfs4_deallocate(thread, req, argop, resop);
                    break;
                case OP_SEEK:
                    chimera_nfs4_seek(thread, req, argop, resop);
                    break;
                case OP_GETXATTR:
                    chimera_nfs4_getxattr(thread, req, argop, resop);
                    break;
                case OP_SETXATTR:
                    chimera_nfs4_setxattr(thread, req, argop, resop);
                    break;
                case OP_LISTXATTRS:
                    chimera_nfs4_listxattrs(thread, req, argop, resop);
                    break;
                case OP_REMOVEXATTR:
                    chimera_nfs4_removexattr(thread, req, argop, resop);
                    break;
                case OP_GETDEVICEINFO:
                    chimera_nfs4_getdeviceinfo(thread, req, argop, resop);
                    break;
                case OP_LAYOUTGET:
                    chimera_nfs4_layoutget(thread, req, argop, resop);
                    break;
                case OP_LAYOUTRETURN:
                    chimera_nfs4_layoutreturn(thread, req, argop, resop);
                    break;
                case OP_LAYOUTCOMMIT:
                    chimera_nfs4_layoutcommit(thread, req, argop, resop);
                    break;
                case OP_LAYOUTSTATS:
                    chimera_nfs4_layoutstats(thread, req, argop, resop);
                    break;
                case OP_LAYOUTERROR:
                    chimera_nfs4_layouterror(thread, req, argop, resop);
                    break;
                case OP_GETDEVICELIST:
                    chimera_nfs4_getdevicelist(thread, req, argop, resop);
                    break;
                case OP_LOCK:
                    chimera_nfs4_lock(thread, req, argop, resop);
                    break;
                case OP_LOCKT:
                    chimera_nfs4_lockt(thread, req, argop, resop);
                    break;
                case OP_LOCKU:
                    chimera_nfs4_locku(thread, req, argop, resop);
                    break;
                case OP_VERIFY:
                    chimera_nfs4_verify(thread, req, argop, resop);
                    break;
                case OP_NVERIFY:
                    chimera_nfs4_nverify(thread, req, argop, resop);
                    break;
                case OP_DELEGRETURN:
                    chimera_nfs4_delegreturn(thread, req, argop, resop);
                    break;
                case OP_DELEGPURGE:
                    chimera_nfs4_delegpurge(thread, req, argop, resop);
                    break;
                default:
                    chimera_nfs_error("Unsupported operation: %d", argop->argop);
                    if (argop->argop >= OP_ACCESS && argop->argop <= OP_REMOVEXATTR) {
                        chimera_nfs4_compound_complete(req, NFS4ERR_NOTSUPP);
                    } else {
                        resop->resop = OP_ILLEGAL;
                        chimera_nfs4_compound_complete(req, NFS4ERR_OP_ILLEGAL);
                    }
                    break;
            } /* switch */

        }
    }
    thread->active = 0;

    if (thread->again) {
        thread->again = 0;
        req->index++;
        goto again;
    }
} /* chimera_nfs4_compound_process */

void
chimera_nfs4_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMPOUND4args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    int                               rc;

    /* First NFSv4 compound on a worker thread (which owns a live vfs_thread)
     * drives the deferred cold-start recovery load.  Run-once and cheap after
     * the first call (atomic fast-path inside). */
    nfs_recovery_kickoff(thread);

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred_req(req, cred);
    req->export_id = 0;

    /* Capture the RPC principal for EXCHANGE_ID record-matching. */
    if (cred && cred->flavor == EVPL_RPC2_AUTH_SYS) {
        req->principal_flavor          = cred->flavor;
        req->principal_uid             = cred->authsys.uid;
        req->principal_gid             = cred->authsys.gid;
        req->principal_machinename     = cred->authsys.machinename;
        req->principal_machinename_len = cred->authsys.machinename_len > 0 ?
            (uint32_t) cred->authsys.machinename_len : 0;
    } else if (cred && cred->flavor == EVPL_RPC2_AUTH_RPCSEC_GSS &&
               cred->gss.principal) {
        /* For RPCSEC_GSS the "machine credential" identity is the GSS principal
         * name; carry it as the machinename so EXCHANGE_ID can bind it for
         * SP4_MACH_CRED and per-op enforcement can compare against it. */
        req->principal_flavor          = cred->flavor;
        req->principal_uid             = 0;
        req->principal_gid             = 0;
        req->principal_machinename     = cred->gss.principal;
        req->principal_machinename_len = (uint32_t) strlen(cred->gss.principal);
    } else {
        req->principal_flavor          = cred ? cred->flavor : EVPL_RPC2_AUTH_NONE;
        req->principal_uid             = 0;
        req->principal_gid             = 0;
        req->principal_machinename     = NULL;
        req->principal_machinename_len = 0;
    }

    nfs4_dump_compound(req, args);
    nfs4_trace_compound(req, args);

    /* The conn caches its bound nfs4_session in private_data.  The conn
     * holds a refcount on it (set up by nfs4_session_bind_conn) so the
     * memory is valid for the duration of this request.  However the
     * session may already have been destroyed by a prior DESTROY_SESSION
     * compound -- in that case treat the cache as empty so that the
     * lookup-by-id paths produce the appropriate BADSESSION/BAD_STATEID
     * errors. */
    {
        struct nfs4_session *cached = evpl_rpc2_conn_get_private_data(conn);

        req->session = nfs4_session_is_live(cached) ? cached : NULL;
    }
    req->args_compound       = args;
    req->res_compound.status = NFS4_OK;
    /* RFC 7530 §16.2.4: the server MUST echo the request tag back to the
     * client unchanged. xdr_opaque .data is owned by the request msg buffer
     * which lives until the response is sent. */
    req->res_compound.tag = args->tag;
    req->fhlen            = 0;
    req->saved_fhlen      = 0;
    /* Requests are recycled from a per-thread free list without being zeroed,
     * and only SAVEFH ever writes saved_export_id -- reset it so a policy
     * check on the saved side cannot read a previous compound's export. */
    req->saved_export_id             = 0;
    req->minorversion                = (uint8_t) args->minorversion;
    req->seen_sequence               = false;
    req->current_stateid_valid       = false;
    req->saved_current_stateid_valid = false;
    req->open_4_0_owner              = NULL;
    req->lock_4_0_open_owner         = NULL;
    req->lock_4_0_lock_owner         = NULL;

    /* NFSv4.0 duplicate-request caching is handled before the compound is
     * decoded, by the dispatcher wrapper in nfs4_v40_drc.c. */

    /* Chimera implements NFS v4.0, v4.1 and v4.2 (minorversions 0–2).
     * Reject unknown minor versions with NFS4ERR_MINOR_VERS_MISMATCH per
     * RFC 7530 §16.2.4 / RFC 5661 §15.1.5.4. */
    if (args->minorversion > 2) {
        req->res_compound.status       = NFS4ERR_MINOR_VERS_MISMATCH;
        req->res_compound.num_resarray = 0;
        req->res_compound.resarray     = NULL;

        rc = thread->shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            NULL,
            &req->res_compound,
            req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        nfs_request_free(thread, req);
        return;
    }

    /* RFC 7530 §16.2.3: the COMPOUND tag is a utf8str_cs; a malformed tag
     * is rejected with NFS4ERR_INVAL before any operation runs. */
    if (args->tag.len &&
        !chimera_nfs4_utf8_valid(args->tag.data, args->tag.len)) {
        req->res_compound.status       = NFS4ERR_INVAL;
        req->res_compound.num_resarray = 0;
        req->res_compound.resarray     = NULL;

        rc = thread->shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            NULL,
            &req->res_compound,
            req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        nfs_request_free(thread, req);
        return;
    }

    rc = xdr_dbuf_alloc_array(&req->res_compound, resarray, args->num_argarray, req->encoding->dbuf);

    if (rc) {
        req->res_compound.status       = NFS4ERR_RESOURCE;
        req->res_compound.num_resarray = 0;
        req->res_compound.resarray     = NULL;

        rc = thread->shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(
            thread->evpl,
            NULL,
            &req->res_compound,
            req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        nfs_request_free(thread, req);
        return;
    }

    req->index = 0;

    chimera_nfs4_compound_process(req, NFS4_OK);

} /* chimera_nfs4_compound */
