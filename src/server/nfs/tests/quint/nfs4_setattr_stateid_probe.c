/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 size-changing SETATTR stateid probe.
 *
 * RFC 7530 section 16.32.4 / RFC 8881 section 18.30.4: the stateid a SETATTR
 * carries provides the byte-range locking context for a size change, which
 * "has the same locking requirements as a corresponding WRITE".  An open
 * stateid with write access therefore authorizes a truncation of its file, and
 * so does a lock stateid anchored to such an open.
 *
 * A Linux client sends the lock stateid whenever the process holds a POSIX
 * lock on the file (nfs4_select_rw_stateid prefers it over the open stateid),
 * and it sends it with seqid 0, the NFSv4.1 "current seqid" wildcard.  A
 * server that refuses that stateid with NFS4ERR_BAD_STATEID while TEST_STATEID
 * still reports it valid leaves the client nothing to recover: it re-tests,
 * finds the stateid good, and resends the SETATTR forever.  lockf() followed
 * by ftruncate() hangs, and so does LTP's growfiles -l.
 *
 * Each cell creates a file, grows it to FILE_SIZE through its creating open,
 * then opens it again under a fresh open owner with the cell's share_access,
 * optionally takes a byte-range lock through that open, and truncates the file
 * to TRUNC_SIZE through the resulting stateid.  The SETATTR's own status is the
 * measurement; where it succeeds, a GETATTR in the same compound must report
 * the new size.  The refusal cells keep the check honest: a lock stateid does
 * not widen the open it is anchored to, and does not authorize a file other
 * than the one it was taken on.
 *
 * With --deleg the server serves delegations and a second matrix runs
 * instead (deleg_cells): the stateids a delegation holder sends.  A Linux
 * client holding a delegation and no POSIX lock sends the delegation stateid
 * itself (nfs4_select_rw_stateid falls back to it before the open stateid;
 * truncate(2) with no open context tries nfs4_copy_delegation_stateid before
 * the anonymous stateid), and with a POSIX lock held it sends the lock
 * stateid.  Either way the holder is coherent with its own size change, so
 * the server must not recall the holder's own delegation; the probe's back
 * channel counts CB_RECALLs to catch exactly that.
 *
 * The delegation matrix covers every operation the holder sends through its
 * delegation stateid: READ, WRITE and SETATTR(size), plus an attribute-only
 * SETATTR, READ_PLUS, WRITE_SAME, ALLOCATE, DEALLOCATE and COPY (see the
 * deleg_cells comment).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define FILE_SIZE  65536
#define TRUNC_SIZE 16384

static int failures = 0;

static void
expect(
    const char *label,
    int         ok,
    const char *detail)
{
    if (!ok) {
        printf("  FAIL %s: %s\n", label, detail);
        failures++;
    } else {
        printf("  ok  %s: %s\n", label, detail);
    }
} /* expect */

/* ---- one probed 4.1 client ------------------------------------------------ */

#define P_MAX_RECALL 8

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
    int                    nrecalls;              /* CB_RECALLs received */
    uint8_t                recalls[P_MAX_RECALL][12];
};

/* Minimal back-channel responder: answer CB_SEQUENCE, record and acknowledge
 * CB_RECALL (the delegation cells measure that none arrives for the holder's
 * own truncate), and refuse the rest to keep the transport clean. */
static void
ss_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct pc             *pc = private_data;
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4   resarray[4];
    uint32_t               i;
    int                    rc;

    (void) conn;
    (void) cred;

    memset(&res, 0, sizeof(res));
    memset(resarray, 0, sizeof(resarray));

    for (i = 0; i < args->num_argarray && i < 4; i++) {
        const struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4       *resop = &resarray[i];

        memset(resop, 0, sizeof(*resop));
        resop->resop = argop->argop;
        switch (argop->argop) {
            case OP_CB_SEQUENCE:
                memcpy(resop->opcbsequence.csr_resok4.csr_sessionid,
                       argop->opcbsequence.csa_sessionid, 16);
                resop->opcbsequence.csr_resok4.csr_sequenceid =
                    argop->opcbsequence.csa_sequenceid;
                resop->opcbsequence.csr_resok4.csr_slotid =
                    argop->opcbsequence.csa_slotid;
                resop->opcbsequence.csr_resok4.csr_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_resok4.csr_target_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_status = NFS4_OK;
                break;
            case OP_CB_RECALL:
                if (pc->nrecalls < P_MAX_RECALL) {
                    memcpy(pc->recalls[pc->nrecalls++],
                           argop->opcbrecall.stateid.other, 12);
                }
                resop->opcbrecall.status = NFS4_OK;
                break;
            default:
                resop->opcbrecall.status = NFS4ERR_NOTSUPP;
                break;
        } /* switch */
    }
    res.status       = NFS4_OK;
    res.num_resarray = i;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                                 encoding);
    (void) rc;
} /* ss_cb_compound */

/* ---- one compound's parsed reply ------------------------------------------ */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];           /* per-op status */
    struct mbt_fh   fh;               /* last GETFH */
    struct stateid4 open_sid;         /* last OPEN stateid */
    int             open_idx;         /* index of the last OPEN in st[] */
    uint32_t        deleg_type;       /* last OPEN's delegation type */
    struct stateid4 deleg_sid;        /* last OPEN's delegation stateid */
    struct stateid4 lock_sid;         /* last LOCK stateid */
    int             lock_idx;         /* index of the last LOCK in st[] */
    int             meas_idx;         /* index of the measured op (SETATTR,
                                       * WRITE, READ, READ_PLUS, WRITE_SAME,
                                       * ALLOCATE, DEALLOCATE or COPY) in
                                       * st[] */
    int             eof;              /* last READ / READ_PLUS's eof */
    int             has_size;         /* GETATTR returned FATTR4_SIZE */
    uint64_t        size;
    int             has_mode;         /* GETATTR returned FATTR4_MODE */
    uint32_t        mode;
    uint64_t        clientid;         /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];    /* CREATE_SESSION */
};

static uint64_t
unpack_be64(const uint8_t *p)
{
    uint64_t v = 0;
    int      i;

    for (i = 0; i < 8; i++) {
        v = (v << 8) | p[i];
    }
    return v;
} /* unpack_be64 */

static uint32_t
unpack_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) | (uint32_t) p[3];
} /* unpack_be32 */

static void
prep_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct prep *p = private_data;
    uint32_t     i;

    (void) evpl;
    (void) verf;
    p->rpc_err  = status;
    p->open_idx = -1;
    p->lock_idx = -1;
    p->meas_idx = -1;
    if (status == 0) {
        p->status = reply->status;
        p->nres   = (int) reply->num_resarray;
        for (i = 0; i < reply->num_resarray && i < 10; i++) {
            const struct nfs_resop4 *r = &reply->resarray[i];

            switch (r->resop) {
                case OP_SEQUENCE:
                    p->st[i] = r->opsequence.sr_status;
                    break;
                case OP_PUTFH:
                    p->st[i] = r->opputfh.status;
                    break;
                case OP_PUTROOTFH:
                    p->st[i] = r->opputrootfh.status;
                    break;
                case OP_LOOKUP:
                    p->st[i] = r->oplookup.status;
                    break;
                case OP_GETFH:
                    p->st[i] = r->opgetfh.status;
                    if (p->st[i] == NFS4_OK) {
                        mbt_copy_fh(&p->fh, &r->opgetfh.resok4.object);
                    }
                    break;
                case OP_OPEN:
                    p->st[i]    = r->opopen.status;
                    p->open_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid   = r->opopen.resok4.stateid;
                        p->deleg_type =
                            r->opopen.resok4.delegation.delegation_type;
                        if (p->deleg_type == OPEN_DELEGATE_READ) {
                            p->deleg_sid =
                                r->opopen.resok4.delegation.read.stateid;
                        } else if (p->deleg_type == OPEN_DELEGATE_WRITE) {
                            p->deleg_sid =
                                r->opopen.resok4.delegation.write.stateid;
                        }
                    }
                    break;
                case OP_DELEGRETURN:
                    p->st[i] = r->opdelegreturn.status;
                    break;
                case OP_LOCK:
                    p->st[i]    = r->oplock.status;
                    p->lock_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->lock_sid = r->oplock.resok4.lock_stateid;
                    }
                    break;
                case OP_LOCKU:
                    p->st[i] = r->oplocku.status;
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_SETATTR:
                    p->st[i]    = r->opsetattr.status;
                    p->meas_idx = (int) i;
                    break;
                case OP_WRITE:
                    p->st[i]    = r->opwrite.status;
                    p->meas_idx = (int) i;
                    break;
                case OP_READ:
                    p->st[i]    = r->opread.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        int k;

                        p->eof = r->opread.resok4.eof;
                        /* The reply's data iovecs carry references the
                         * caller now owns. */
                        for (k = 0; k < r->opread.resok4.data.niov; k++) {
                            evpl_iovec_release(evpl, &r->opread.resok4.data.iov[k]);
                        }
                    }
                    break;
                case OP_READ_PLUS:
                    p->st[i]    = r->opread_plus.rp_status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        /* The segments' data is a copying opaque: nothing
                         * to release. */
                        p->eof = r->opread_plus.rp_resok4.rpr_eof;
                    }
                    break;
                case OP_WRITE_SAME:
                    p->st[i]    = r->opwrite_same.wsr_status;
                    p->meas_idx = (int) i;
                    break;
                case OP_ALLOCATE:
                    p->st[i]    = r->opallocate.ar_status;
                    p->meas_idx = (int) i;
                    break;
                case OP_DEALLOCATE:
                    p->st[i]    = r->opdeallocate.dr_status;
                    p->meas_idx = (int) i;
                    break;
                case OP_COPY:
                    p->st[i]    = r->opcopy.cr_status;
                    p->meas_idx = (int) i;
                    break;
                case OP_SAVEFH:
                    p->st[i] = r->opsavefh.status;
                    break;
                case OP_GETATTR:
                    p->st[i] = r->opgetattr.status;
                    /* Either FATTR4_SIZE or FATTR4_MODE alone is requested,
                     * so the value is the sole one in the blob. */
                    if (p->st[i] == NFS4_OK &&
                        r->opgetattr.resok4.obj_attributes.num_attrmask >= 1 &&
                        (r->opgetattr.resok4.obj_attributes.attrmask[0] &
                         (1U << FATTR4_SIZE)) &&
                        r->opgetattr.resok4.obj_attributes.attr_vals.len >= 8) {
                        p->has_size = 1;
                        p->size     = unpack_be64(
                            r->opgetattr.resok4.obj_attributes.attr_vals.data);
                    } else if (p->st[i] == NFS4_OK &&
                               r->opgetattr.resok4.obj_attributes.num_attrmask >= 2 &&
                               (r->opgetattr.resok4.obj_attributes.attrmask[1] &
                                (1U << (FATTR4_MODE - 32))) &&
                               r->opgetattr.resok4.obj_attributes.attr_vals.len >= 4) {
                        p->has_mode = 1;
                        p->mode     = unpack_be32(
                            r->opgetattr.resok4.obj_attributes.attr_vals.data);
                    }
                    break;
                case OP_EXCHANGE_ID:
                    p->st[i] = r->opexchange_id.eir_status;
                    if (p->st[i] == NFS4_OK) {
                        p->clientid = r->opexchange_id.eir_resok4.eir_clientid;
                        p->eir_seq  = r->opexchange_id.eir_resok4.eir_sequenceid;
                    }
                    break;
                case OP_CREATE_SESSION:
                    p->st[i] = r->opcreate_session.csr_status;
                    if (p->st[i] == NFS4_OK) {
                        memcpy(p->sessionid,
                               r->opcreate_session.csr_resok4.csr_sessionid, 16);
                    }
                    break;
                default:
                    p->st[i] = 0;
                    break;
            } /* switch */
        }
    }
    p->done = 1;
} /* prep_cb */

/* The minor version every compound is sent with: 1 for the open/lock
 * matrix, 2 for the delegation matrix, which measures the RFC 7862 ops
 * (READ_PLUS, WRITE_SAME, ALLOCATE, DEALLOCATE, COPY) a 4.2 client sends
 * through its delegation stateid. */
static uint32_t probe_minor = 1;

static struct prep
pc_compound(
    struct pc         *pc,
    struct nfs_argop4 *ops,
    int                nops,
    int                with_seq)
{
    struct nfs_argop4    argarray[10];
    struct COMPOUND4args args;
    struct prep          p;
    int                  base = 0;
    int                  i;

    memset(&p, 0, sizeof(p));
    memset(&args, 0, sizeof(args));

    if (with_seq) {
        memset(&argarray[0], 0, sizeof(argarray[0]));
        argarray[0].argop = OP_SEQUENCE;
        memcpy(argarray[0].opsequence.sa_sessionid, pc->sessionid, 16);
        argarray[0].opsequence.sa_sequenceid     = ++pc->slot_seq;
        argarray[0].opsequence.sa_slotid         = 0;
        argarray[0].opsequence.sa_highest_slotid = 0;
        argarray[0].opsequence.sa_cachethis      = 0;
        base                                     = 1;
    }
    for (i = 0; i < nops; i++) {
        argarray[base + i] = ops[i];
    }
    args.minorversion = probe_minor;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) (base + nops);

    pc->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&pc->env->nfs_v4.rpc2,
                                                pc->env->evpl, pc->conn,
                                                &pc->env->cred, &args,
                                                0, 0, NULL, 0, 0,
                                                prep_cb, &p);
    while (!p.done) {
        evpl_continue(pc->env->evpl);
    }
    if (p.rpc_err) {
        fprintf(stderr, "transport error %d\n", p.rpc_err);
        exit(2);
    }
    return p;
} /* pc_compound */

static void
pc_setup(
    struct pc      *pc,
    struct mbt_env *env,
    int             ownersym)
{
    struct evpl_rpc2_program         *cb_programs[1];
    struct evpl_endpoint             *ep;
    struct nfs_argop4                 op;
    struct prep                       p;
    static char                       owners[4][32];
    char                             *owner = owners[ownersym & 3];
    struct channel_attrs4             chan  = {
        .ca_headerpadsize          = 0,
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
        .num_ca_rdma_ird           = 0,
    };
    static struct callback_sec_parms4 sec = { .cb_secflavor = 0 };

    memset(pc, 0, sizeof(*pc));
    pc->env = env;

    cb_programs[0] = &env->nfs_v4_cb.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    pc->conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                        EVPL_STREAM_INPROC, ep,
                                        cb_programs, 1, pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    snprintf(owner, 32, "ss-probe-%d", ownersym);
    memset(op.opexchange_id.eia_clientowner.co_verifier, ownersym + 1, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;
    p                                                = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "EXCHANGE_ID failed: %u\n", p.status);
        exit(2);
    }
    pc->clientid = p.clientid;

    memset(&op, 0, sizeof(op));
    op.argop                                = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid        = pc->clientid;
    op.opcreate_session.csa_sequence        = p.eir_seq;
    op.opcreate_session.csa_flags           = CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    op.opcreate_session.csa_fore_chan_attrs = chan;
    op.opcreate_session.csa_back_chan_attrs = chan;
    op.opcreate_session.csa_cb_program      = 0x40000000;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = &sec;
    p                                       = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", p.status);
        exit(2);
    }
    memcpy(pc->sessionid, p.sessionid, 16);

    memset(&op, 0, sizeof(op));
    op.argop                         = OP_RECLAIM_COMPLETE;
    op.opreclaim_complete.rca_one_fs = 0;
    p                                = pc_compound(pc, &op, 1, 1);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "RECLAIM_COMPLETE failed: %u\n", p.status);
        exit(2);
    }
} /* pc_setup */

/* ---- op builders ---------------------------------------------------------- */

static struct nfs_argop4
op_putfh(const struct mbt_fh *fh)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop               = OP_PUTFH;
    a.opputfh.object.data = (void *) fh->data;
    a.opputfh.object.len  = fh->len;
    return a;
} /* op_putfh */

static struct nfs_argop4
op_getfh(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_GETFH;
    return a;
} /* op_getfh */

static void
pack_be32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) ((v >> 24) & 0xff);
    p[1] = (uint8_t) ((v >> 16) & 0xff);
    p[2] = (uint8_t) ((v >> 8) & 0xff);
    p[3] = (uint8_t) (v & 0xff);
} /* pack_be32 */

/* OPEN(CLAIM_NULL).  `create` makes it a creating open with mode 0644 in
 * createattrs; otherwise it is OPEN4_NOCREATE.
 *
 * The bitmap and value blob are static because the argument structure only
 * borrows them, and the compound is sent before the next call overwrites them. */
static struct nfs_argop4
op_open(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    uint32_t    access,
    int         create)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[4];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = access;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);
    if (create) {
        bitmap[0] = 0;
        bitmap[1] = 1U << (FATTR4_MODE - 32);
        pack_be32(blob, 0644);

        a.opopen.openhow.opentype                       = OPEN4_CREATE;
        a.opopen.openhow.how.mode                       = UNCHECKED4;
        a.opopen.openhow.how.createattrs.num_attrmask   = 2;
        a.opopen.openhow.how.createattrs.attrmask       = bitmap;
        a.opopen.openhow.how.createattrs.attr_vals.data = blob;
        a.opopen.openhow.how.createattrs.attr_vals.len  = 4;
    } else {
        a.opopen.openhow.opentype = OPEN4_NOCREATE;
    }
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open */

/* LOCK of the whole file, establishing a new lock owner through `open_sid`.
 * The seqids are zero: NFSv4.1 ignores them (RFC 8881 section 18.10.3). */
static struct nfs_argop4
op_lock(
    uint64_t               clientid,
    const char            *lock_owner,
    const struct stateid4 *open_sid,
    nfs_lock_type4         locktype)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                                          = OP_LOCK;
    a.oplock.locktype                                = locktype;
    a.oplock.reclaim                                 = 0;
    a.oplock.offset                                  = 0;
    a.oplock.length                                  = UINT64_MAX;
    a.oplock.locker.new_lock_owner                   = 1;
    a.oplock.locker.open_owner.open_seqid            = 0;
    a.oplock.locker.open_owner.open_stateid          = *open_sid;
    a.oplock.locker.open_owner.lock_seqid            = 0;
    a.oplock.locker.open_owner.lock_owner.clientid   = clientid;
    a.oplock.locker.open_owner.lock_owner.owner.data = (void *) lock_owner;
    a.oplock.locker.open_owner.lock_owner.owner.len  = (uint32_t) strlen(lock_owner);
    return a;
} /* op_lock */

static struct nfs_argop4
op_locku(
    const struct stateid4 *lock_sid,
    nfs_lock_type4         locktype)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_LOCKU;
    a.oplocku.locktype     = locktype;
    a.oplocku.seqid        = 0;
    a.oplocku.lock_stateid = *lock_sid;
    a.oplocku.offset       = 0;
    a.oplocku.length       = UINT64_MAX;
    return a;
} /* op_locku */

/* SETATTR(size) through `sid`.  Static bitmap and blob, as in op_open:
 * each compound is sent before the next call reuses them. */
static struct nfs_argop4
op_setattr_size(
    const struct stateid4 *sid,
    uint64_t               size)
{
    static uint32_t   bitmap[1];
    static uint8_t    blob[8];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SETATTR;

    bitmap[0] = 1U << FATTR4_SIZE;
    pack_be32(blob, (uint32_t) (size >> 32));
    pack_be32(blob + 4, (uint32_t) size);

    a.opsetattr.stateid                       = *sid;
    a.opsetattr.obj_attributes.num_attrmask   = 1;
    a.opsetattr.obj_attributes.attrmask       = bitmap;
    a.opsetattr.obj_attributes.attr_vals.data = blob;
    a.opsetattr.obj_attributes.attr_vals.len  = 8;
    return a;
} /* op_setattr_size */

static struct nfs_argop4
op_getattr_size(void)
{
    static uint32_t   bitmap[1];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_GETATTR;
    bitmap[0]                    = 1U << FATTR4_SIZE;
    a.opgetattr.num_attr_request = 1;
    a.opgetattr.attr_request     = bitmap;
    return a;
} /* op_getattr_size */

/* WRITE of `len` bytes of 'A' at offset 0 through `sid`, FILE_SYNC.  The
 * iovec struct is static for the same reason the bitmaps are (the argument
 * only borrows it), but its buffer is allocated afresh on every call: the
 * send releases the reference it takes. */
static struct nfs_argop4
op_write_bytes(
    struct evpl           *evpl,
    const struct stateid4 *sid,
    uint32_t               len)
{
    static struct evpl_iovec iov;
    struct nfs_argop4        a;

    if (evpl_iovec_alloc(evpl, len, 4096, 1, 0, &iov) < 0) {
        fprintf(stderr, "iovec alloc failed\n");
        exit(2);
    }
    memset(iov.data, 0x41, len);
    iov.length = len;

    memset(&a, 0, sizeof(a));
    a.argop               = OP_WRITE;
    a.opwrite.stateid     = *sid;
    a.opwrite.offset      = 0;
    a.opwrite.stable      = FILE_SYNC4;
    a.opwrite.data.iov    = &iov;
    a.opwrite.data.niov   = 1;
    a.opwrite.data.length = len;
    return a;
} /* op_write_bytes */

/* READ of `len` bytes at offset 0 through `sid`. */
static struct nfs_argop4
op_read_bytes(
    const struct stateid4 *sid,
    uint32_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop          = OP_READ;
    a.opread.stateid = *sid;
    a.opread.offset  = 0;
    a.opread.count   = len;
    return a;
} /* op_read_bytes */

static struct nfs_argop4
op_close(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_CLOSE;
    a.opclose.seqid        = 0;
    a.opclose.open_stateid = *sid;
    return a;
} /* op_close */

static struct nfs_argop4
op_delegreturn(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                       = OP_DELEGRETURN;
    a.opdelegreturn.deleg_stateid = *sid;
    return a;
} /* op_delegreturn */

/* Attribute-only SETATTR: FATTR4_MODE through `sid`.  The stateid is not
 * consulted for a mode change, but a Linux client sends its delegation
 * stateid here all the same (nfs4_do_setattr copies it for every SETATTR
 * under a write delegation). */
static struct nfs_argop4
op_setattr_mode(
    const struct stateid4 *sid,
    uint32_t               mode)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[4];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SETATTR;

    bitmap[0] = 0;
    bitmap[1] = 1U << (FATTR4_MODE - 32);
    pack_be32(blob, mode);

    a.opsetattr.stateid                       = *sid;
    a.opsetattr.obj_attributes.num_attrmask   = 2;
    a.opsetattr.obj_attributes.attrmask       = bitmap;
    a.opsetattr.obj_attributes.attr_vals.data = blob;
    a.opsetattr.obj_attributes.attr_vals.len  = 4;
    return a;
} /* op_setattr_mode */

static struct nfs_argop4
op_getattr_mode(void)
{
    static uint32_t   bitmap[2];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_GETATTR;
    bitmap[0]                    = 0;
    bitmap[1]                    = 1U << (FATTR4_MODE - 32);
    a.opgetattr.num_attr_request = 2;
    a.opgetattr.attr_request     = bitmap;
    return a;
} /* op_getattr_mode */

/* READ_PLUS of `len` bytes at offset 0 through `sid`. */
static struct nfs_argop4
op_read_plus(
    const struct stateid4 *sid,
    uint32_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_READ_PLUS;
    a.opread_plus.rpa_stateid = *sid;
    a.opread_plus.rpa_offset  = 0;
    a.opread_plus.rpa_count   = len;
    return a;
} /* op_read_plus */

/* WRITE_SAME of TRUNC_SIZE bytes at offset 0 through `sid`: 4 KiB blocks, a
 * 16-byte pattern at the start of each, no per-block-number stamping. */
#define WS_BLOCK_SIZE 4096
static struct nfs_argop4
op_write_same(const struct stateid4 *sid)
{
    static uint8_t    pattern[16];
    struct nfs_argop4 a;

    memset(pattern, 0x42, sizeof(pattern));
    memset(&a, 0, sizeof(a));
    a.argop                                    = OP_WRITE_SAME;
    a.opwrite_same.wsa_stateid                 = *sid;
    a.opwrite_same.wsa_stable                  = FILE_SYNC4;
    a.opwrite_same.wsa_adb.adb_offset          = 0;
    a.opwrite_same.wsa_adb.adb_block_size      = WS_BLOCK_SIZE;
    a.opwrite_same.wsa_adb.adb_block_count     = TRUNC_SIZE / WS_BLOCK_SIZE;
    a.opwrite_same.wsa_adb.adb_reloff_blocknum = NFS4_UINT64_MAX;
    a.opwrite_same.wsa_adb.adb_block_num       = 0;
    a.opwrite_same.wsa_adb.adb_reloff_pattern  = 0;
    a.opwrite_same.wsa_adb.adb_pattern.data    = pattern;
    a.opwrite_same.wsa_adb.adb_pattern.len     = sizeof(pattern);
    return a;
} /* op_write_same */

/* ALLOCATE of [0, len) through `sid`. */
static struct nfs_argop4
op_allocate(
    const struct stateid4 *sid,
    uint64_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_ALLOCATE;
    a.opallocate.aa_stateid = *sid;
    a.opallocate.aa_offset  = 0;
    a.opallocate.aa_length  = len;
    return a;
} /* op_allocate */

/* DEALLOCATE of [0, len) through `sid`. */
static struct nfs_argop4
op_deallocate(
    const struct stateid4 *sid,
    uint64_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_DEALLOCATE;
    a.opdeallocate.da_stateid = *sid;
    a.opdeallocate.da_offset  = 0;
    a.opdeallocate.da_length  = len;
    return a;
} /* op_deallocate */

static struct nfs_argop4
op_savefh(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SAVEFH;
    return a;
} /* op_savefh */

/* Synchronous, consecutive COPY of `len` bytes from offset 0 of the saved
 * filehandle (through `src_sid`) to offset 0 of the current one (through
 * `dst_sid`). */
static struct nfs_argop4
op_copy(
    const struct stateid4 *src_sid,
    const struct stateid4 *dst_sid,
    uint64_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                       = OP_COPY;
    a.opcopy.ca_src_stateid       = *src_sid;
    a.opcopy.ca_dst_stateid       = *dst_sid;
    a.opcopy.ca_src_offset        = 0;
    a.opcopy.ca_dst_offset        = 0;
    a.opcopy.ca_count             = len;
    a.opcopy.ca_consecutive       = 1;
    a.opcopy.ca_synchronous       = 1;
    a.opcopy.num_ca_source_server = 0;
    return a;
} /* op_copy */

/* ---- the matrix ----------------------------------------------------------- */

/* locktype 0 truncates through the open stateid itself -- the path that has
 * always worked, kept as the baseline the lock cells are measured against.
 * zero_seqid sends the stateid with seqid 0, as the Linux client does.
 * other_file takes the open and lock on a second file and presents that lock
 * stateid against the first.
 * *INDENT-OFF* */
static const struct {
    const char    *label;
    uint32_t       access;       /* share_access of the open the stateid names */
    nfs_lock_type4 locktype;     /* 0: no lock, use the open stateid */
    int            zero_seqid;
    int            other_file;
    uint32_t       expect;
} cells[] = {
    { "open-stateid",               OPEN4_SHARE_ACCESS_BOTH, 0,        0, 0, NFS4_OK             },
    { "write-lock-stateid",         OPEN4_SHARE_ACCESS_BOTH, WRITE_LT, 0, 0, NFS4_OK             },
    { "write-lock-stateid-seqid0",  OPEN4_SHARE_ACCESS_BOTH, WRITE_LT, 1, 0, NFS4_OK             },
    { "read-lock-on-write-open",    OPEN4_SHARE_ACCESS_BOTH, READ_LT,  0, 0, NFS4_OK             },
    { "read-lock-on-read-open",     OPEN4_SHARE_ACCESS_READ, READ_LT,  0, 0, NFS4ERR_OPENMODE    },
    { "lock-stateid-of-other-file", OPEN4_SHARE_ACCESS_BOTH, WRITE_LT, 0, 1, NFS4ERR_BAD_STATEID },
};
/* *INDENT-ON* */

/* Create `name`, grow it to FILE_SIZE through its creating open, and close
 * that open, so the cell's own open is the only state on the file.  With
 * delegations served, the creating open is granted a write delegation that
 * outlives the close; `deleg` (optional) receives its stateid and
 * `deleg_type` its type.  grow = 0 leaves the file empty. */
static int
create_file(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *name,
    const char          *owner,
    struct mbt_fh       *out_fh,
    struct stateid4     *deleg,
    uint32_t            *deleg_type,
    int                  grow,
    char                *buf,
    size_t               buflen)
{
    struct nfs_argop4 ops[4];
    struct prep       p;
    struct stateid4   sid;
    int               n = 0;

    ops[0] = op_putfh(root);
    ops[1] = op_open(pc->clientid, owner, name, OPEN4_SHARE_ACCESS_BOTH, 1);
    ops[2] = op_getfh();
    p      = pc_compound(pc, ops, 3, 1);
    if (p.status != NFS4_OK || !p.fh.has) {
        snprintf(buf, buflen, "create %s failed: %u", name, p.status);
        return 0;
    }
    *out_fh = p.fh;
    sid     = p.open_sid;
    if (deleg) {
        *deleg      = p.deleg_sid;
        *deleg_type = p.deleg_type;
    }

    ops[n++] = op_putfh(out_fh);
    if (grow) {
        ops[n++] = op_setattr_size(&sid, FILE_SIZE);
    }
    ops[n++] = op_close(&sid);
    p        = pc_compound(pc, ops, n, 1);
    if (p.status != NFS4_OK) {
        snprintf(buf, buflen, "grow+close of %s failed: %u", name, p.status);
        return 0;
    }
    return 1;
} /* create_file */

static void
run_cell(
    struct pc           *pc,
    const struct mbt_fh *root,
    unsigned int         i)
{
    const char       *label = cells[i].label;
    char              fname[32], oname[40];
    char              create_owner[40], open_owner[40], lock_owner[40];
    char              buf[200];
    struct nfs_argop4 ops[4];
    struct prep       p;
    struct mbt_fh     target_fh, source_fh;
    const char       *source_name;
    struct stateid4   open_sid, lock_sid, sid;
    uint32_t          got;

    /* Fresh names and owners per cell: no state an earlier cell left can
     * coalesce with or answer for this one. */
    snprintf(fname, sizeof(fname), "s%u", i);
    snprintf(oname, sizeof(oname), "s%u-other", i);
    snprintf(create_owner, sizeof(create_owner), "ss-create-%u", i);
    snprintf(open_owner, sizeof(open_owner), "ss-open-%u", i);
    snprintf(lock_owner, sizeof(lock_owner), "ss-lock-%u", i);

    if (!create_file(pc, root, fname, create_owner, &target_fh, NULL, NULL, 1,
                     buf, sizeof(buf))) {
        expect(label, 0, buf);
        return;
    }
    source_fh   = target_fh;
    source_name = fname;
    if (cells[i].other_file) {
        if (!create_file(pc, root, oname, create_owner, &source_fh, NULL, NULL, 1,
                         buf, sizeof(buf))) {
            expect(label, 0, buf);
            return;
        }
        source_name = oname;
    }

    /* The open (and lock) whose stateid the SETATTR will present. */
    ops[0] = op_putfh(root);
    ops[1] = op_open(pc->clientid, open_owner, source_name, cells[i].access, 0);
    p      = pc_compound(pc, ops, 2, 1);
    if (p.status != NFS4_OK) {
        snprintf(buf, sizeof(buf), "open of %s failed: %u", source_name,
                 p.status);
        expect(label, 0, buf);
        return;
    }
    open_sid = p.open_sid;
    sid      = open_sid;

    if (cells[i].locktype) {
        ops[0] = op_putfh(&source_fh);
        ops[1] = op_lock(pc->clientid, lock_owner, &open_sid,
                         cells[i].locktype);
        p = pc_compound(pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "lock of %s failed: %u", source_name,
                     p.status);
            expect(label, 0, buf);
            return;
        }
        lock_sid = p.lock_sid;
        sid      = lock_sid;
    }
    if (cells[i].zero_seqid) {
        sid.seqid = 0;
    }

    /* The measurement: truncate the target through that stateid. */
    ops[0] = op_putfh(&target_fh);
    ops[1] = op_setattr_size(&sid, TRUNC_SIZE);
    ops[2] = op_getattr_size();
    p      = pc_compound(pc, ops, 3, 1);

    /* The SETATTR's own status, or nothing: a compound that short-circuited
     * before SETATTR must fail the cell rather than substitute an earlier
     * operation's error, which could coincide with the expected one. */
    if (p.meas_idx < 0) {
        snprintf(buf, sizeof(buf),
                 "compound returned %u without reaching SETATTR", p.status);
        expect(label, 0, buf);
    } else {
        got = p.st[p.meas_idx];
        snprintf(buf, sizeof(buf),
                 "SETATTR(size=%u) through %s stateid (seqid %u): "
                 "expected %u, got %u",
                 TRUNC_SIZE, cells[i].locktype ? "lock" : "open",
                 sid.seqid, cells[i].expect, got);
        expect(label, got == cells[i].expect, buf);

        if (got == NFS4_OK && cells[i].expect == NFS4_OK) {
            snprintf(buf, sizeof(buf), "size after truncate: expected %u, "
                     "got %llu%s", TRUNC_SIZE, (unsigned long long) p.size,
                     p.has_size ? "" : " (no size returned)");
            expect(label, p.has_size && p.size == TRUNC_SIZE, buf);
        }
    }

    /* Release the lock and the open so no state outlives the cell. */
    if (cells[i].locktype) {
        ops[0] = op_putfh(&source_fh);
        ops[1] = op_locku(&lock_sid, cells[i].locktype);
        pc_compound(pc, ops, 2, 1);
    }
    ops[0] = op_putfh(&source_fh);
    ops[1] = op_close(&open_sid);
    pc_compound(pc, ops, 2, 1);
} /* run_cell */

/* ---- the delegation matrix (--deleg) -------------------------------------- */

/* A Linux client holding a delegation and no POSIX lock sends the delegation
 * stateid in SETATTR(size) (nfs4_select_rw_stateid falls back to it before
 * the open stateid; truncate(2) with no open context tries
 * nfs4_copy_delegation_stateid before the anonymous stateid).  A write
 * delegation authorizes the truncate, and the holder is coherent with its own
 * size change, so no CB_RECALL may be sent to it.  A read delegation grants
 * no write (NFS4ERR_OPENMODE, as for a read-only open), and a delegation on
 * another file designates no state for this one (NFS4ERR_BAD_STATEID).
 *
 * The holder is just as coherent with a size change it sends through its
 * open or lock stateid while it holds the delegation (the process holds a
 * POSIX lock, or the client prefers the open stateid): those must not recall
 * the holder's own delegation either.
 *
 * The same client sends the same stateid for everything else it does to the
 * file while it holds the delegation: every SETATTR, not only a size change
 * (nfs4_do_setattr copies the delegation stateid for a chmod or a utimes
 * under a write delegation), READ_PLUS (its ordinary read at 4.2), and
 * ALLOCATE, DEALLOCATE and COPY (fallocate, punch-hole and
 * copy_file_range).  Each of those must answer what the delegation grants
 * -- a read delegation refuses the writes with NFS4ERR_OPENMODE -- and none
 * may recall the holder's own delegation; a server that answers
 * NFS4ERR_BAD_STATEID while TEST_STATEID reports the stateid valid leaves
 * the client resending forever (chimera-nas/chimera#1728).
 *
 * The cells create the file empty and extend it, so the only size change
 * the file ever sees is the measured one; the reads and the hole punch
 * fill it through the creating open first.
 *
 * via: which stateid the measured op carries.  access is the share_access
 * of the open the delegation is granted on (BOTH -> write delegation, READ
 * -> read delegation).  A COPY cell copies TRUNC_SIZE bytes from a second
 * file, read through that file's own write delegation, into the target;
 * `via` and `access` describe the destination stateid, and the
 * other_file variant presents the source file's delegation as the
 * destination stateid.
 * *INDENT-OFF* */
enum via { VIA_DELEG, VIA_OPEN, VIA_LOCK, VIA_ANON };
enum meas {
    M_SETATTR_SIZE, M_SETATTR_MODE, M_WRITE, M_READ, M_READ_PLUS,
    M_WRITE_SAME, M_ALLOCATE, M_DEALLOCATE, M_COPY
};
static const struct {
    const char *label;
    enum via    via;
    enum meas   op;
    uint32_t    access;
    int         zero_seqid;
    int         other_file;  /* present another file's write delegation */
    int         recalled;    /* another client's conflicting OPEN first:
                              * the delegation is under recall when the
                              * holder sends the measured op, and stays
                              * the holder's until it returns it */
    uint32_t    expect;
} deleg_cells[] = {
    { "open-stateid-under-own-deleg",        VIA_OPEN,  M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "lock-stateid-under-own-deleg",        VIA_LOCK,  M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "write-open-stateid-under-own-deleg",  VIA_OPEN,  M_WRITE,        OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "write-deleg-stateid",                 VIA_DELEG, M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "write-deleg-stateid-seqid0",          VIA_DELEG, M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 1, 0, 0, NFS4_OK             },
    { "read-deleg-stateid",                  VIA_DELEG, M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "deleg-stateid-of-other-file",         VIA_DELEG, M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "write-deleg-stateid-under-recall",    VIA_DELEG, M_SETATTR_SIZE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 1, NFS4_OK             },
    { "write-via-write-deleg-stateid",       VIA_DELEG, M_WRITE,        OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "write-via-write-deleg-stateid-under-recall", VIA_DELEG, M_WRITE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 1, NFS4_OK             },
    { "write-via-read-deleg-stateid",        VIA_DELEG, M_WRITE,        OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "write-via-deleg-stateid-of-other-file", VIA_DELEG, M_WRITE,      OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "read-via-write-deleg-stateid",        VIA_DELEG, M_READ,         OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "read-via-read-deleg-stateid",         VIA_DELEG, M_READ,         OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4_OK             },
    { "read-via-deleg-stateid-of-other-file", VIA_DELEG, M_READ,        OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "setattr-mode-via-write-deleg-stateid", VIA_DELEG, M_SETATTR_MODE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK            },
    { "setattr-mode-via-read-deleg-stateid", VIA_DELEG, M_SETATTR_MODE, OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4_OK             },
    { "setattr-mode-via-open-stateid-under-own-deleg", VIA_OPEN, M_SETATTR_MODE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK    },
    { "setattr-mode-via-anon-stateid-under-own-deleg", VIA_ANON, M_SETATTR_MODE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK    },
    { "setattr-mode-via-write-deleg-stateid-under-recall", VIA_DELEG, M_SETATTR_MODE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 1, NFS4_OK },
    { "read-plus-via-write-deleg-stateid",   VIA_DELEG, M_READ_PLUS,    OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "read-plus-via-read-deleg-stateid",    VIA_DELEG, M_READ_PLUS,    OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4_OK             },
    { "read-plus-via-deleg-stateid-of-other-file", VIA_DELEG, M_READ_PLUS, OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "write-same-via-write-deleg-stateid",  VIA_DELEG, M_WRITE_SAME,   OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "write-same-via-read-deleg-stateid",   VIA_DELEG, M_WRITE_SAME,   OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "write-same-via-deleg-stateid-of-other-file", VIA_DELEG, M_WRITE_SAME, OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "allocate-via-write-deleg-stateid",    VIA_DELEG, M_ALLOCATE,     OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "allocate-via-read-deleg-stateid",     VIA_DELEG, M_ALLOCATE,     OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "allocate-via-deleg-stateid-of-other-file", VIA_DELEG, M_ALLOCATE, OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
    { "deallocate-via-write-deleg-stateid",  VIA_DELEG, M_DEALLOCATE,   OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "deallocate-via-write-deleg-stateid-under-recall", VIA_DELEG, M_DEALLOCATE, OPEN4_SHARE_ACCESS_BOTH, 0, 0, 1, NFS4_OK   },
    { "deallocate-via-read-deleg-stateid",   VIA_DELEG, M_DEALLOCATE,   OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "copy-dst-via-write-deleg-stateid",    VIA_DELEG, M_COPY,         OPEN4_SHARE_ACCESS_BOTH, 0, 0, 0, NFS4_OK             },
    { "copy-dst-via-read-deleg-stateid",     VIA_DELEG, M_COPY,         OPEN4_SHARE_ACCESS_READ, 0, 0, 0, NFS4ERR_OPENMODE    },
    { "copy-dst-via-deleg-stateid-of-other-file", VIA_DELEG, M_COPY,    OPEN4_SHARE_ACCESS_BOTH, 0, 1, 0, NFS4ERR_BAD_STATEID },
};
/* *INDENT-ON* */

static const char *
meas_name(enum meas op)
{
    switch (op) {
        case M_SETATTR_SIZE: return "SETATTR(size)";
        case M_SETATTR_MODE: return "SETATTR(mode)";
        case M_WRITE:        return "WRITE";
        case M_READ:         return "READ";
        case M_READ_PLUS:    return "READ_PLUS";
        case M_WRITE_SAME:   return "WRITE_SAME";
        case M_ALLOCATE:     return "ALLOCATE";
        case M_DEALLOCATE:   return "DEALLOCATE";
        case M_COPY:         return "COPY";
        default:             return "?";
    } /* switch */
} /* meas_name */

/* The ops that read the file, or punch a hole in it, need data to be
 * there: the cell fills the target through its creating open first. */
static int
meas_needs_fill(enum meas op)
{
    return op == M_READ || op == M_READ_PLUS || op == M_DEALLOCATE;
} /* meas_needs_fill */

/* Only memfs implements READ_PLUS among the probe's backends; elsewhere the
 * backend answers NFS4ERR_NOTSUPP once the stateid has been judged.  That is
 * the gate passing, so an OK-expected cell takes it, skips the data check,
 * and still requires that nothing was recalled.  WRITE_SAME needs no such
 * allowance: the VFS expands the pattern into ordinary writes on a backend
 * without it, and those writes must not recall the holder's delegation
 * either. */
static const char *probe_module = "memfs";

static int
meas_gate_passed(
    enum meas op,
    uint32_t  expect,
    uint32_t  got)
{
    if (got == expect) {
        return 1;
    }
    return expect == NFS4_OK && got == NFS4ERR_NOTSUPP &&
           op == M_READ_PLUS && strcmp(probe_module, "memfs") != 0;
} /* meas_gate_passed */

static int
recall_seen(
    const struct pc       *pc,
    const struct stateid4 *sid)
{
    int i;

    for (i = 0; i < pc->nrecalls; i++) {
        if (memcmp(pc->recalls[i], sid->other, 12) == 0) {
            return 1;
        }
    }
    return 0;
} /* recall_seen */

/* `other` is a second client, used by the recalled cells to break the
 * holder's delegation with a conflicting OPEN. */
static void
run_deleg_cell(
    struct pc           *pc,
    struct pc           *other,
    const struct mbt_fh *root,
    unsigned int         i)
{
    const char       *label = deleg_cells[i].label;
    enum meas         op    = deleg_cells[i].op;
    char              fname[32], oname[40];
    char              create_owner[40], open_owner[40], lock_owner[40];
    char              other_owner[40];
    char              buf[200];
    struct nfs_argop4 ops[6];
    struct prep       p;
    struct mbt_fh     target_fh, other_fh;
    struct stateid4   target_open, target_deleg, other_deleg, lock_sid, sid;
    uint32_t          target_deleg_type, want_type, got;
    const char       *opname = meas_name(op);
    int               has_open = 1, has_other = 0, has_lock = 0;
    int               n;

    snprintf(fname, sizeof(fname), "d%u", i);
    snprintf(oname, sizeof(oname), "d%u-other", i);
    snprintf(create_owner, sizeof(create_owner), "sd-create-%u", i);
    snprintf(open_owner, sizeof(open_owner), "sd-open-%u", i);
    snprintf(lock_owner, sizeof(lock_owner), "sd-lock-%u", i);
    snprintf(other_owner, sizeof(other_owner), "sd-other-%u", i);

    /* Create the target empty; the creating open is granted a write
     * delegation (4.1 with a back channel grants from the first open). */
    ops[0] = op_putfh(root);
    ops[1] = op_open(pc->clientid, create_owner, fname, OPEN4_SHARE_ACCESS_BOTH, 1);
    ops[2] = op_getfh();
    p      = pc_compound(pc, ops, 3, 1);
    if (p.status != NFS4_OK || !p.fh.has) {
        snprintf(buf, sizeof(buf), "create %s failed: %u", fname, p.status);
        expect(label, 0, buf);
        return;
    }
    target_fh         = p.fh;
    target_open       = p.open_sid;
    target_deleg      = p.deleg_sid;
    target_deleg_type = p.deleg_type;
    if (target_deleg_type != OPEN_DELEGATE_WRITE) {
        snprintf(buf, sizeof(buf), "creating open of %s granted deleg type %u, "
                 "expected write (%u)", fname, target_deleg_type,
                 OPEN_DELEGATE_WRITE);
        expect(label, 0, buf);
        return;
    }
    sid       = target_deleg;
    want_type = OPEN_DELEGATE_WRITE;

    /* A read (or a hole punch) needs data: fill the file through the
     * creating open, which the write-open-stateid-under-own-deleg cell shows
     * is recall-free, and which is still the write-capable creating open at
     * this point. */
    if (meas_needs_fill(op)) {
        ops[0] = op_putfh(&target_fh);
        ops[1] = op_write_bytes(pc->env->evpl, &target_open, TRUNC_SIZE);
        p      = pc_compound(pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "fill of %s failed: %u", fname,
                     p.status);
            expect(label, 0, buf);
            return;
        }
    }

    /* The second file: the stateid presented is its write delegation
     * (other_file), or it is the COPY source, grown so there is something
     * to copy, read through that same delegation. */
    if (deleg_cells[i].other_file || op == M_COPY) {
        if (!create_file(pc, root, oname, create_owner, &other_fh,
                         &other_deleg, &want_type, op == M_COPY, buf,
                         sizeof(buf))) {
            expect(label, 0, buf);
            return;
        }
        has_other = 1;
        if (want_type != OPEN_DELEGATE_WRITE) {
            snprintf(buf, sizeof(buf), "creating open of %s granted deleg type "
                     "%u, expected write", oname, want_type);
            expect(label, 0, buf);
            return;
        }
    }
    if (deleg_cells[i].other_file) {
        sid = other_deleg;
    } else if (deleg_cells[i].via == VIA_DELEG &&
               deleg_cells[i].access == OPEN4_SHARE_ACCESS_READ) {
        /* Close the creating open, hand its write delegation back and
         * re-open read-only to be granted a read delegation. */
        ops[0] = op_putfh(&target_fh);
        ops[1] = op_close(&target_open);
        ops[2] = op_delegreturn(&target_deleg);
        p      = pc_compound(pc, ops, 3, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "close+delegreturn of %s failed: %u",
                     fname, p.status);
            expect(label, 0, buf);
            return;
        }
        ops[0] = op_putfh(root);
        ops[1] = op_open(pc->clientid, open_owner, fname,
                         OPEN4_SHARE_ACCESS_READ, 0);
        p = pc_compound(pc, ops, 2, 1);
        if (p.status != NFS4_OK || p.deleg_type != OPEN_DELEGATE_READ) {
            snprintf(buf, sizeof(buf), "read open of %s: status %u, deleg "
                     "type %u (expected read)", fname, p.status, p.deleg_type);
            expect(label, 0, buf);
            return;
        }
        target_open  = p.open_sid;
        target_deleg = p.deleg_sid;
        sid          = target_deleg;
        want_type    = OPEN_DELEGATE_READ;
    } else if (deleg_cells[i].via == VIA_OPEN) {
        sid = target_open;
    } else if (deleg_cells[i].via == VIA_ANON) {
        memset(&sid, 0, sizeof(sid));
    } else if (deleg_cells[i].via == VIA_LOCK) {
        ops[0] = op_putfh(&target_fh);
        ops[1] = op_lock(pc->clientid, lock_owner, &target_open, WRITE_LT);
        p      = pc_compound(pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "lock of %s failed: %u", fname,
                     p.status);
            expect(label, 0, buf);
            return;
        }
        has_lock = 1;
        lock_sid = p.lock_sid;
        sid      = lock_sid;
    }
    if (deleg_cells[i].zero_seqid) {
        sid.seqid = 0;
    }

    /* Put the delegation under recall: the other client's conflicting OPEN
     * is answered NFS4ERR_DELAY and a CB_RECALL reaches the holder.  The
     * holder's stateid stays valid until it returns the delegation, so its
     * own I/O still goes through -- without a second recall. */
    if (deleg_cells[i].recalled) {
        int k;

        ops[0] = op_putfh(root);
        ops[1] = op_open(other->clientid, other_owner, fname,
                         OPEN4_SHARE_ACCESS_READ, 0);
        p = pc_compound(other, ops, 2, 1);
        for (k = 0; k < 100 && !recall_seen(pc, &target_deleg); k++) {
            evpl_continue(pc->env->evpl);
        }
        snprintf(buf, sizeof(buf), "conflicting OPEN by another client: "
                 "status %u (expected %u), recall of the delegation %s",
                 p.status, NFS4ERR_DELAY,
                 recall_seen(pc, &target_deleg) ? "seen" : "NOT seen");
        if (p.status != NFS4ERR_DELAY || !recall_seen(pc, &target_deleg)) {
            expect(label, 0, buf);
            return;
        }
    }

    /* The measurement, through that stateid: SETATTR extends the empty
     * target to TRUNC_SIZE or sets its mode, WRITE / WRITE_SAME / ALLOCATE
     * / COPY fill TRUNC_SIZE bytes into it, DEALLOCATE punches the filled
     * bytes out again (the size stays), READ / READ_PLUS read them back. */
    pc->nrecalls = 0;
    n            = 0;
    if (op == M_COPY) {
        ops[n++] = op_putfh(&other_fh);
        ops[n++] = op_savefh();
    }
    ops[n++] = op_putfh(&target_fh);
    switch (op) {
        case M_SETATTR_MODE:
            ops[n++] = op_setattr_mode(&sid, 0600);
            break;
        case M_WRITE:
            ops[n++] = op_write_bytes(pc->env->evpl, &sid, TRUNC_SIZE);
            break;
        case M_READ:
            ops[n++] = op_read_bytes(&sid, TRUNC_SIZE);
            break;
        case M_READ_PLUS:
            ops[n++] = op_read_plus(&sid, TRUNC_SIZE);
            break;
        case M_WRITE_SAME:
            ops[n++] = op_write_same(&sid);
            break;
        case M_ALLOCATE:
            ops[n++] = op_allocate(&sid, TRUNC_SIZE);
            break;
        case M_DEALLOCATE:
            ops[n++] = op_deallocate(&sid, TRUNC_SIZE);
            break;
        case M_COPY:
            ops[n++] = op_copy(&other_deleg, &sid, TRUNC_SIZE);
            break;
        default:
            ops[n++] = op_setattr_size(&sid, TRUNC_SIZE);
            break;
    } /* switch */
    ops[n++] = (op == M_SETATTR_MODE) ? op_getattr_mode() : op_getattr_size();
    p        = pc_compound(pc, ops, n, 1);

    if (p.meas_idx < 0) {
        snprintf(buf, sizeof(buf),
                 "compound returned %u without reaching %s", p.status, opname);
        expect(label, 0, buf);
    } else {
        got = p.st[p.meas_idx];
        snprintf(buf, sizeof(buf),
                 "%s through %s stateid (seqid %u) while holding a %s "
                 "delegation: expected %u, got %u",
                 opname,
                 deleg_cells[i].via == VIA_OPEN ? "open" :
                 deleg_cells[i].via == VIA_LOCK ? "lock" :
                 deleg_cells[i].via == VIA_ANON ? "the anonymous" :
                 "delegation",
                 sid.seqid,
                 want_type == OPEN_DELEGATE_WRITE ? "write" : "read",
                 deleg_cells[i].expect, got);
        expect(label, meas_gate_passed(op, deleg_cells[i].expect, got), buf);

        if (got == NFS4_OK && deleg_cells[i].expect == NFS4_OK) {
            if (op == M_READ || op == M_READ_PLUS) {
                snprintf(buf, sizeof(buf), "%s of %u bytes: eof=%d", opname,
                         TRUNC_SIZE, p.eof);
                expect(label, p.eof, buf);
            } else if (op == M_SETATTR_MODE) {
                snprintf(buf, sizeof(buf), "mode after %s: expected 0%o, "
                         "got 0%o%s", opname, 0600, p.mode,
                         p.has_mode ? "" : " (no mode returned)");
                expect(label, p.has_mode && p.mode == 0600, buf);
            } else {
                snprintf(buf, sizeof(buf), "size after %s: expected %u, "
                         "got %llu%s", opname, TRUNC_SIZE,
                         (unsigned long long) p.size,
                         p.has_size ? "" : " (no size returned)");
                expect(label, p.has_size && p.size == TRUNC_SIZE, buf);
            }
        }
        if (meas_gate_passed(op, deleg_cells[i].expect, got) &&
            deleg_cells[i].expect == NFS4_OK) {
            /* The holder is coherent with its own I/O: no recall. */
            snprintf(buf, sizeof(buf), "CB_RECALLs during the %s: %d "
                     "(own delegation recalled: %s)", opname, pc->nrecalls,
                     recall_seen(pc, &target_deleg) ? "yes" : "no");
            expect(label, pc->nrecalls == 0, buf);
        }
    }

    /* Release every lock, open and delegation so no state outlives the
     * cell.  A recalled delegation is still returnable. */
    if (has_lock) {
        ops[0] = op_putfh(&target_fh);
        ops[1] = op_locku(&lock_sid, WRITE_LT);
        pc_compound(pc, ops, 2, 1);
    }
    if (has_open) {
        ops[0] = op_putfh(&target_fh);
        ops[1] = op_close(&target_open);
        pc_compound(pc, ops, 2, 1);
    }
    /* The delegations must still be the holder's to return.  A server that
     * parked the measured op behind the holder's own delegation and revoked
     * it at the recall deadline answers NFS4ERR_DELEG_REVOKED here, even
     * though the op itself then went through without a new recall. */
    ops[0] = op_putfh(&target_fh);
    ops[1] = op_delegreturn(&target_deleg);
    p      = pc_compound(pc, ops, 2, 1);
    snprintf(buf, sizeof(buf), "DELEGRETURN of %s's delegation: expected %u, "
             "got %u", fname, NFS4_OK, p.status);
    expect(label, p.status == NFS4_OK, buf);
    if (has_other) {
        ops[0] = op_putfh(&other_fh);
        ops[1] = op_delegreturn(&other_deleg);
        p      = pc_compound(pc, ops, 2, 1);
        snprintf(buf, sizeof(buf), "DELEGRETURN of %s's delegation: "
                 "expected %u, got %u", oname, NFS4_OK, p.status);
        expect(label, p.status == NFS4_OK, buf);
    }
} /* run_deleg_cell */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts opts = {
        /* Exact attributes: the size after the truncate must be what the
         * backend holds now, not a cached answer from before it. */
        .disable_caches = 1,
    };
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           pc, other;
    struct mbt_fh       root;
    unsigned int        i;
    int                 deleg = 0;

    setvbuf(stdout, NULL, _IONBF, 0);
    /* Passthrough backends create the export root on the host; keep its mode
     * the one the model expects, as the replayers do. */
    umask(0);
    mbt_watchdog_arm(120);

    /* Usage: nfs4_setattr_stateid_probe [<module>] [--deleg].  --deleg serves
     * delegations and runs the delegation cells instead of the open/lock
     * ones: the two matrices need different server policies. */
    opts.module = "memfs";
    for (i = 1; i < (unsigned int) argc; i++) {
        if (strcmp(argv[i], "--deleg") == 0) {
            deleg                 = 1;
            opts.nfs4_delegations = 1;
        } else {
            opts.module = argv[i];
        }
    }
    probe_module = opts.module;
    probe_minor  = deleg ? 2 : 1;
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = ss_cb_compound;

    /* Resolve the export root (minor 0, no session). */
    {
        struct nfs_argop4    ops[3];
        struct COMPOUND4args args;
        struct prep          rp;

        memset(ops, 0, sizeof(ops));
        ops[0].argop                 = OP_PUTROOTFH;
        ops[1].argop                 = OP_LOOKUP;
        ops[1].oplookup.objname.data = "fs0";
        ops[1].oplookup.objname.len  = 3;
        ops[2].argop                 = OP_GETFH;

        memset(&args, 0, sizeof(args));
        args.minorversion = 0;
        args.argarray     = ops;
        args.num_argarray = 3;
        memset(&rp, 0, sizeof(rp));
        env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl,
                                                env->nfs_conn, &env->cred, &args,
                                                0, 0, NULL, 0, 0, prep_cb, &rp);
        while (!rp.done) {
            evpl_continue(env->evpl);
        }
        if (rp.status != NFS4_OK || !rp.fh.has) {
            fprintf(stderr, "resolve export root failed: %u\n", rp.status);
            return 2;
        }
        root = rp.fh;
    }

    pc_setup(&pc, env, 1);
    if (deleg) {
        pc_setup(&other, env, 2);
    }

    printf("# --- NFSv4 %s stateid authorization (%s) ---\n",
           deleg ? "delegation holder" : "SETATTR(size)", opts.module);

    if (deleg) {
        for (i = 0; i < sizeof(deleg_cells) / sizeof(deleg_cells[0]); i++) {
            run_deleg_cell(&pc, &other, &root, i);
        }
    } else {
        for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
            run_cell(&pc, &root, i);
        }
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 SETATTR stateid check(s) FAILED\n",
                failures);
        return 1;
    }
    printf("all NFSv4 SETATTR stateid checks passed\n");
    return 0;
} /* main */
