// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"
#include "evpl/evpl_bind.h"
#include "nfs4_dump.h"
#include "nfs4_op_matrix.h"

static uint32_t
nfs4_replay_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) |
           ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) |
           (uint32_t) p[3];
} /* nfs4_replay_be32 */

static bool
nfs4_replay_cached_body_offset(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *offset)
{
    uint32_t v, verf_len, pad;
    uint32_t pos = 4; /* TCP record marker */

    if (len < 28) {
        return false;
    }

    v = nfs4_replay_be32(buf);
    if ((v & 0x80000000u) == 0 || (v & 0x7fffffffu) + 4 != len) {
        return false;
    }

    pos += 4; /* xid */

    v = nfs4_replay_be32(buf + pos); /* mtype */
    if (v != 1) { /* REPLY */
        return false;
    }
    pos += 4;

    v = nfs4_replay_be32(buf + pos); /* reply stat */
    if (v != 0) { /* MSG_ACCEPTED */
        return false;
    }
    pos += 4;

    pos     += 4; /* verifier flavor */
    verf_len = nfs4_replay_be32(buf + pos);
    pos     += 4;

    pad = (4 - (verf_len & 3)) & 3;
    if (pos + verf_len + pad + 4 > len) {
        return false;
    }
    pos += verf_len + pad;

    v = nfs4_replay_be32(buf + pos); /* accept stat */
    if (v != 0) { /* SUCCESS */
        return false;
    }
    pos += 4;

    *offset = pos;
    return true;
} /* nfs4_replay_cached_body_offset */

static int
nfs4_send_cached_reply_buf(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const uint8_t                    *cached,
    uint32_t                          cached_len)
{
    uint32_t           body_offset, body_len, reserve;
    struct evpl_iovec *msg_iov;
    int                niov;

    if (!nfs4_replay_cached_body_offset(cached, cached_len, &body_offset)) {
        return -1;
    }

    body_len = cached_len - body_offset;
    reserve  = req->encoding->program->reserve;

    msg_iov = xdr_dbuf_alloc_space(sizeof(*msg_iov), req->encoding->dbuf);
    if (!msg_iov) {
        return -1;
    }

    niov = evpl_iovec_alloc(thread->evpl, body_len + reserve, 8, 1, 0, msg_iov);
    if (niov != 1) {
        return -1;
    }

    memcpy((uint8_t *) msg_iov->data + reserve, cached + body_offset, body_len);

    return evpl_rpc2_send_reply_dispatch(thread->evpl,
                                         req->encoding,
                                         NULL,
                                         msg_iov,
                                         1,
                                         body_len + reserve);
} /* nfs4_send_cached_reply_buf */

static int
nfs4_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    return nfs4_send_cached_reply_buf(thread,
                                      req,
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

static bool
nfs4_v40_drc_lookup(
    struct nfs4_v40_drc   *drc,
    struct evpl_rpc2_conn *conn,
    uint32_t               xid,
    uint8_t              **out_buf,
    uint32_t              *out_len)
{
    uint8_t *buf = NULL;
    uint32_t len = 0;

    pthread_mutex_lock(&drc->lock);
    for (uint32_t i = 0; i < NFS4_V40_DRC_SLOTS; i++) {
        struct nfs4_v40_drc_entry *entry = &drc->entries[i];

        if (entry->valid && entry->conn == conn && entry->xid == xid) {
            len = entry->len;
            buf = malloc(len);
            if (buf) {
                memcpy(buf, entry->buf, len);
            }
            break;
        }
    }
    pthread_mutex_unlock(&drc->lock);

    if (!buf) {
        return false;
    }

    *out_buf = buf;
    *out_len = len;
    return true;
} /* nfs4_v40_drc_lookup */

static void
nfs4_v40_drc_insert(
    struct nfs4_v40_drc     *drc,
    struct evpl_rpc2_conn   *conn,
    uint32_t                 xid,
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length)
{
    struct nfs4_v40_drc_entry *entry;
    uint8_t                   *buf;
    size_t                     offset = 0;

    if (total_length <= 0 ||
        (uint32_t) total_length > NFS4_MAX_CACHED_RESPONSE_SIZE) {
        return;
    }

    buf = malloc((size_t) total_length);
    if (!buf) {
        return;
    }

    for (int i = 0; i < niov; i++) {
        memcpy(buf + offset, iov[i].data, iov[i].length);
        offset += iov[i].length;
    }

    pthread_mutex_lock(&drc->lock);

    for (uint32_t i = 0; i < NFS4_V40_DRC_SLOTS; i++) {
        entry = &drc->entries[i];
        if (entry->valid && entry->conn == conn && entry->xid == xid) {
            drc->bytes -= entry->len;
            free(entry->buf);
            entry->buf  = buf;
            entry->len  = (uint32_t) total_length;
            drc->bytes += entry->len;
            pthread_mutex_unlock(&drc->lock);
            return;
        }
    }

    while (drc->bytes + (uint32_t) total_length > NFS4_MAX_REPLY_CACHE_BYTES) {
        entry     = &drc->entries[drc->next];
        drc->next = (drc->next + 1) % NFS4_V40_DRC_SLOTS;
        if (!entry->valid) {
            break;
        }
        drc->bytes -= entry->len;
        free(entry->buf);
        entry->buf   = NULL;
        entry->len   = 0;
        entry->valid = 0;
    }

    entry     = &drc->entries[drc->next];
    drc->next = (drc->next + 1) % NFS4_V40_DRC_SLOTS;
    if (entry->valid) {
        drc->bytes -= entry->len;
        free(entry->buf);
    }

    entry->conn  = conn;
    entry->xid   = xid;
    entry->len   = (uint32_t) total_length;
    entry->buf   = buf;
    entry->valid = 1;
    drc->bytes  += entry->len;

    pthread_mutex_unlock(&drc->lock);
} /* nfs4_v40_drc_insert */

static void
nfs4_v40_drc_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    void                    *private_data)
{
    struct nfs_request *req = private_data;

    if (!req || req->minorversion != 0) {
        return;
    }

    nfs4_v40_drc_insert(&req->thread->shared->v40_drc,
                        req->conn,
                        req->encoding->xid,
                        iov,
                        niov,
                        total_length);
} /* nfs4_v40_drc_capture_reply */

static void
nfs4_v40_drc_arm_capture(struct nfs_request *req)
{
    if (req->encoding) {
        req->encoding->reply_capture_cb      = nfs4_v40_drc_capture_reply;
        req->encoding->reply_capture_private = req;
    }
} /* nfs4_v40_drc_arm_capture */

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
        req->replay_slot = NULL;
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

    thread->active = 1;

    /* If the response buffer is running low, fail early with RESOURCE
     * rather than letting individual procs abort on allocation failure */
    if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
        chimera_nfs4_compound_complete(req, NFS4ERR_RESOURCE);
    } else {
        nfsstat4 gate = nfs4_op_check_minor(argop->argop,
                                            req->minorversion,
                                            (uint32_t) req->index,
                                            req->seen_sequence);

        if (gate != NFS4_OK) {
            if (gate == NFS4ERR_OP_ILLEGAL) {
                resop->resop = OP_ILLEGAL;
            }
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
                case OP_TEST_STATEID:
                    chimera_nfs4_test_stateid(thread, req, argop, resop);
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

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred(&req->cred, cred);

    /* Capture the RPC principal for EXCHANGE_ID record-matching. */
    if (cred && cred->flavor == EVPL_RPC2_AUTH_SYS) {
        req->principal_flavor          = cred->flavor;
        req->principal_uid             = cred->authsys.uid;
        req->principal_gid             = cred->authsys.gid;
        req->principal_machinename     = cred->authsys.machinename;
        req->principal_machinename_len = cred->authsys.machinename_len > 0 ?
            (uint32_t) cred->authsys.machinename_len : 0;
    } else {
        req->principal_flavor          = cred ? cred->flavor : EVPL_RPC2_AUTH_NONE;
        req->principal_uid             = 0;
        req->principal_gid             = 0;
        req->principal_machinename     = NULL;
        req->principal_machinename_len = 0;
    }

    nfs4_dump_compound(req, args);

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
    req->res_compound.tag            = args->tag;
    req->fhlen                       = 0;
    req->saved_fhlen                 = 0;
    req->minorversion                = (uint8_t) args->minorversion;
    req->seen_sequence               = false;
    req->current_stateid_valid       = false;
    req->saved_current_stateid_valid = false;
    req->open_4_0_owner              = NULL;
    req->lock_4_0_open_owner         = NULL;
    req->lock_4_0_lock_owner         = NULL;

    if (args->minorversion == 0) {
        uint8_t *cached_buf;
        uint32_t cached_len;

        if (nfs4_v40_drc_lookup(&thread->shared->v40_drc,
                                conn,
                                encoding->xid,
                                &cached_buf,
                                &cached_len)) {
            nfs4_release_write_args(thread, args);
            rc = nfs4_send_cached_reply_buf(thread, req, cached_buf, cached_len);
            free(cached_buf);
            chimera_nfs_abort_if(rc, "Failed to send cached RPC2 reply");
            nfs_request_free(thread, req);
            return;
        }

        nfs4_v40_drc_arm_capture(req);
    }

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
