/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 byte-range re-lock probe.
 *
 * RFC 7530 section 9.3: a lock-owner holding a WRITE lock may LOCK the same
 * bytes again as READ to downgrade it atomically, and one holding a READ lock
 * may LOCK them as WRITE to upgrade it.  A server that cannot do either
 * answers NFS4ERR_LOCK_NOTSUPP; one that answers NFS4_OK has changed the
 * lock.  The server also returns OPEN4_RESULT_LOCKTYPE_POSIX (RFC 7530
 * section 16.16.5), which promises fcntl semantics: a new lock by an owner
 * replaces that owner's coverage of the bytes it spans, and the parts of an
 * old lock outside them keep standing in their old mode.
 *
 * pynfs LOCKCHGD and LOCKCHGU check only that the re-lock answers NFS4_OK.
 * This probe measures what OTHER lock-owners see afterwards, which is where a
 * re-lock that changed nothing shows: after a downgrade a second owner's READ
 * lock must be granted and a WRITE test must name a READ holder.
 *
 * Each cell is a script of LOCK / LOCKT / LOCKU / CLOSE steps by up to three
 * lock-owners (A, B, C) of one NFSv4.1 client against a fresh file, opened
 * once and shared by all three the way a Linux client shares one open owner
 * between processes.  Every step states the status it expects and, for
 * NFS4ERR_DENIED, the conflicting lock the reply must describe.  Where
 * several locks conflict, the cells are built so that all of them have the
 * same type and range, because which one a server names is its choice.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define TO_EOF    UINT64_MAX
#define MAX_STEPS 8

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

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
};

/* Minimal back-channel responder: this probe never takes a delegation, but the
 * session negotiates a back channel, so answer CB_SEQUENCE (and refuse the
 * rest) to keep the transport clean. */
static void
rl_cb_compound(
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
} /* rl_cb_compound */

/* ---- one compound's parsed reply ------------------------------------------ */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];           /* per-op status */
    struct mbt_fh   fh;               /* last GETFH */
    struct stateid4 open_sid;         /* last OPEN stateid */
    struct stateid4 lock_sid;         /* stateid of the last LOCK or LOCKU */
    int             meas_idx;         /* index of the last LOCK/LOCKT/LOCKU/CLOSE */
    nfs_lock_type4  d_type;           /* NFS4ERR_DENIED: the conflicting lock */
    uint64_t        d_offset;
    uint64_t        d_length;
    uint64_t        clientid;         /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];    /* CREATE_SESSION */
};

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
                    p->st[i] = r->opopen.status;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen.resok4.stateid;
                    }
                    break;
                case OP_LOCK:
                    p->st[i]    = r->oplock.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->lock_sid = r->oplock.resok4.lock_stateid;
                    } else if (p->st[i] == NFS4ERR_DENIED) {
                        p->d_type   = r->oplock.denied.locktype;
                        p->d_offset = r->oplock.denied.offset;
                        p->d_length = r->oplock.denied.length;
                    }
                    break;
                case OP_LOCKT:
                    p->st[i]    = r->oplockt.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4ERR_DENIED) {
                        p->d_type   = r->oplockt.denied.locktype;
                        p->d_offset = r->oplockt.denied.offset;
                        p->d_length = r->oplockt.denied.length;
                    }
                    break;
                case OP_LOCKU:
                    p->st[i]    = r->oplocku.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->lock_sid = r->oplocku.lock_stateid;
                    }
                    break;
                case OP_CLOSE:
                    p->st[i]    = r->opclose.status;
                    p->meas_idx = (int) i;
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
    args.minorversion = 1;
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
    struct mbt_env *env)
{
    struct evpl_rpc2_program         *cb_programs[1];
    struct evpl_endpoint             *ep;
    struct nfs_argop4                 op;
    struct prep                       p;
    static char                       owner[32];
    struct channel_attrs4             chan = {
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
    snprintf(owner, sizeof(owner), "rl-probe");
    memset(op.opexchange_id.eia_clientowner.co_verifier, 1, 8);
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

/* A creating OPEN(CLAIM_NULL) for read and write, mode 0644.  The bitmap and
 * value blob are static because the argument structure only borrows them, and
 * the compound is sent before the next call overwrites them. */
static struct nfs_argop4
op_open_create(
    uint64_t    clientid,
    const char *owner,
    const char *name)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[4];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = OPEN4_SHARE_ACCESS_BOTH;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);

    bitmap[0] = 0;
    bitmap[1] = 1U << (FATTR4_MODE - 32);
    pack_be32(blob, 0644);

    a.opopen.openhow.opentype                       = OPEN4_CREATE;
    a.opopen.openhow.how.mode                       = UNCHECKED4;
    a.opopen.openhow.how.createattrs.num_attrmask   = 2;
    a.opopen.openhow.how.createattrs.attrmask       = bitmap;
    a.opopen.openhow.how.createattrs.attr_vals.data = blob;
    a.opopen.openhow.how.createattrs.attr_vals.len  = 4;
    a.opopen.claim.claim                            = CLAIM_NULL;
    a.opopen.claim.file.data                        = (void *) name;
    a.opopen.claim.file.len                         = (uint32_t) strlen(name);
    return a;
} /* op_open_create */

/* LOCK establishing a new lock owner through `open_sid`.  The seqids are
 * zero: NFSv4.1 ignores them (RFC 8881 section 18.10.3). */
static struct nfs_argop4
op_lock_new(
    uint64_t               clientid,
    const char            *lock_owner,
    const struct stateid4 *open_sid,
    nfs_lock_type4         type,
    uint64_t               offset,
    uint64_t               length)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                                          = OP_LOCK;
    a.oplock.locktype                                = type;
    a.oplock.reclaim                                 = 0;
    a.oplock.offset                                  = offset;
    a.oplock.length                                  = length;
    a.oplock.locker.new_lock_owner                   = 1;
    a.oplock.locker.open_owner.open_seqid            = 0;
    a.oplock.locker.open_owner.open_stateid          = *open_sid;
    a.oplock.locker.open_owner.lock_seqid            = 0;
    a.oplock.locker.open_owner.lock_owner.clientid   = clientid;
    a.oplock.locker.open_owner.lock_owner.owner.data = (void *) lock_owner;
    a.oplock.locker.open_owner.lock_owner.owner.len  = (uint32_t) strlen(lock_owner);
    return a;
} /* op_lock_new */

/* LOCK by a lock owner that already has a lock stateid on the file: the path
 * a client takes for every re-lock, the downgrade included. */
static struct nfs_argop4
op_lock_existing(
    const struct stateid4 *lock_sid,
    nfs_lock_type4         type,
    uint64_t               offset,
    uint64_t               length)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                                 = OP_LOCK;
    a.oplock.locktype                       = type;
    a.oplock.reclaim                        = 0;
    a.oplock.offset                         = offset;
    a.oplock.length                         = length;
    a.oplock.locker.new_lock_owner          = 0;
    a.oplock.locker.lock_owner.lock_stateid = *lock_sid;
    a.oplock.locker.lock_owner.lock_seqid   = 0;
    return a;
} /* op_lock_existing */

static struct nfs_argop4
op_lockt(
    uint64_t       clientid,
    const char    *lock_owner,
    nfs_lock_type4 type,
    uint64_t       offset,
    uint64_t       length)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                    = OP_LOCKT;
    a.oplockt.locktype         = type;
    a.oplockt.offset           = offset;
    a.oplockt.length           = length;
    a.oplockt.owner.clientid   = clientid;
    a.oplockt.owner.owner.data = (void *) lock_owner;
    a.oplockt.owner.owner.len  = (uint32_t) strlen(lock_owner);
    return a;
} /* op_lockt */

static struct nfs_argop4
op_locku(
    const struct stateid4 *lock_sid,
    nfs_lock_type4         type,
    uint64_t               offset,
    uint64_t               length)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_LOCKU;
    a.oplocku.locktype     = type;
    a.oplocku.seqid        = 0;
    a.oplocku.lock_stateid = *lock_sid;
    a.oplocku.offset       = offset;
    a.oplocku.length       = length;
    return a;
} /* op_locku */

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

/* ---- the scripts ---------------------------------------------------------- */

enum step_op {
    S_END = 0,    /* terminates a cell's script */
    S_LOCK,
    S_LOCKT,
    S_LOCKU,
    S_CLOSE,      /* closes the shared open; `who` is ignored */
};

enum { OA, OB, OC, N_OWNERS };

struct step {
    enum step_op op;
    int            who;
    nfs_lock_type4 type;
    uint64_t       offset;
    uint64_t       length;
    uint32_t       expect;
    /* For an expected NFS4ERR_DENIED: the conflicting lock the reply names. */
    nfs_lock_type4 d_type;
    uint64_t       d_offset;
    uint64_t       d_length;
};

struct cell {
    const char *label;
    struct step steps[MAX_STEPS];
};

/* *INDENT-OFF* */
static const struct cell cells[] = {
    /* The reported case: A downgrades its whole WRITE lock, so B may share
     * the range and a WRITE test names a READ holder. */
    { "downgrade-whole-range", {
        { S_LOCK,  OA, WRITE_LT,    0,    100, NFS4_OK },
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OB, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKT, OC, WRITE_LT,    0,    100, NFS4ERR_DENIED, READ_LT,     0,    100 },
    } },
    /* Downgrading the middle of a WRITE lock leaves WRITE on both sides. */
    { "downgrade-middle", {
        { S_LOCK,  OA, WRITE_LT,    0,    300, NFS4_OK },
        { S_LOCK,  OA, READ_LT,   100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,   100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    100, NFS4ERR_DENIED, WRITE_LT,    0,    100 },
        { S_LOCKT, OB, READ_LT,   200,    100, NFS4ERR_DENIED, WRITE_LT,  200,    100 },
        { S_LOCKT, OB, WRITE_LT,  100,    100, NFS4ERR_DENIED, READ_LT,   100,    100 },
    } },
    /* The tail left after downgrading inside a to-EOF lock is still to-EOF. */
    { "downgrade-inside-to-eof", {
        { S_LOCK,  OA, WRITE_LT,    0, TO_EOF, NFS4_OK },
        { S_LOCK,  OA, READ_LT,   100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,   100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    100, NFS4ERR_DENIED, WRITE_LT,    0,    100 },
        { S_LOCKT, OB, READ_LT,  1000,    100, NFS4ERR_DENIED, WRITE_LT,  200, TO_EOF },
    } },
    /* An upgrade must not leave the READ lock beneath the WRITE, or the next
     * downgrade merges with it and the WRITE survives. */
    { "upgrade-then-downgrade", {
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,    0,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    100, NFS4ERR_DENIED, WRITE_LT,    0,    100 },
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,    100, NFS4ERR_DENIED, READ_LT,     0,    100 },
    } },
    /* A refused upgrade changes nothing: A's READ lock still stands once B
     * lets go. */
    { "refused-upgrade-keeps-read", {
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OB, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,    0,    100, NFS4ERR_DENIED, READ_LT,     0,    100 },
        { S_LOCKU, OB, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKT, OC, WRITE_LT,    0,    100, NFS4ERR_DENIED, READ_LT,     0,    100 },
        { S_LOCKT, OC, READ_LT,     0,    100, NFS4_OK },
    } },
    /* A downgrade next to a READ lock coalesces with it into one READ lock. */
    { "downgrade-merges-read-neighbour", {
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,  100,    100, NFS4_OK },
        { S_LOCK,  OA, READ_LT,   100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    200, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,    200, NFS4ERR_DENIED, READ_LT,     0,    200 },
    } },
    /* A WRITE spanning a READ lock and a WRITE lock trims the READ to what
     * lies outside it and joins the WRITE. */
    { "relock-spans-two-locks", {
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,  200,    100, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,   50,    200, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,     50, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,     50, NFS4ERR_DENIED, READ_LT,     0,     50 },
        { S_LOCKT, OB, READ_LT,    50,      1, NFS4ERR_DENIED, WRITE_LT,   50,    250 },
    } },
    /* A same-mode re-lock inside a held lock leaves one lock over the whole
     * range, and LOCKU of that range frees all of it. */
    { "same-mode-relock-inside", {
        { S_LOCK,  OA, WRITE_LT,    0,    300, NFS4_OK },
        { S_LOCK,  OA, WRITE_LT,  100,    100, NFS4_OK },
        { S_LOCKT, OB, READ_LT,     0,    300, NFS4ERR_DENIED, WRITE_LT,    0,    300 },
        { S_LOCKU, OA, WRITE_LT,    0,    300, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,    300, NFS4_OK },
    } },
    /* LOCKU after a downgrade frees the range entirely. */
    { "unlock-after-downgrade", {
        { S_LOCK,  OA, WRITE_LT,    0,    100, NFS4_OK },
        { S_LOCK,  OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKU, OA, READ_LT,     0,    100, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,    100, NFS4_OK },
    } },
    /* CLOSE after a partial downgrade releases the remainders too. */
    { "close-after-partial-downgrade", {
        { S_LOCK,  OA, WRITE_LT,    0,    300, NFS4_OK },
        { S_LOCK,  OA, READ_LT,   100,    100, NFS4_OK },
        { S_CLOSE, OA, 0,           0,      0, NFS4_OK },
        { S_LOCKT, OB, WRITE_LT,    0,    300, NFS4_OK },
    } },
};
/* *INDENT-ON* */

static const char *
step_op_name(enum step_op op)
{
    switch (op) {
        case S_LOCK:
            return "LOCK";
        case S_LOCKT:
            return "LOCKT";
        case S_LOCKU:
            return "LOCKU";
        case S_CLOSE:
            return "CLOSE";
        default:
            return "?";
    } /* switch */
} /* step_op_name */

static const char *
lock_type_name(nfs_lock_type4 type)
{
    return (type == READ_LT || type == READW_LT) ? "READ" : "WRITE";
} /* lock_type_name */

/* "[offset,end)", or "[offset,EOF)" for a to-EOF length. */
static void
range_str(
    char    *buf,
    size_t   buflen,
    uint64_t offset,
    uint64_t length)
{
    if (length == TO_EOF) {
        snprintf(buf, buflen, "[%llu,EOF)", (unsigned long long) offset);
    } else {
        snprintf(buf, buflen, "[%llu,%llu)", (unsigned long long) offset,
                 (unsigned long long) (offset + length));
    }
} /* range_str */

/* "OK", "DENIED by WRITE [0,100)", or the bare status number. */
static void
describe(
    char          *buf,
    size_t         buflen,
    uint32_t       status,
    nfs_lock_type4 d_type,
    uint64_t       d_offset,
    uint64_t       d_length)
{
    char rng[48];

    if (status == NFS4_OK) {
        snprintf(buf, buflen, "OK");
    } else if (status == NFS4ERR_DENIED) {
        range_str(rng, sizeof(rng), d_offset, d_length);
        snprintf(buf, buflen, "DENIED by %s %s", lock_type_name(d_type), rng);
    } else {
        snprintf(buf, buflen, "status %u", status);
    }
} /* describe */

static void
run_cell(
    struct pc           *pc,
    const struct mbt_fh *root,
    unsigned int         ci)
{
    const struct cell *c = &cells[ci];
    char               fname[32], open_owner[40];
    char               lock_owner[N_OWNERS][40];
    char               label[96], want[64], got_desc[64], rng[48];
    char               buf[512];
    struct nfs_argop4  ops[3];
    struct prep        p;
    struct mbt_fh      fh;
    struct stateid4    open_sid;
    struct stateid4    sid[N_OWNERS];
    int                has_sid[N_OWNERS];
    int                closed = 0;
    int                i, w, ok;
    uint32_t           got;

    /* Fresh file and owners per cell: nothing an earlier cell left can
     * coalesce with or conflict with this one. */
    snprintf(fname, sizeof(fname), "r%u", ci);
    snprintf(open_owner, sizeof(open_owner), "rl-open-%u", ci);
    for (w = 0; w < N_OWNERS; w++) {
        snprintf(lock_owner[w], sizeof(lock_owner[w]), "rl-%u-%c", ci,
                 'A' + w);
        has_sid[w] = 0;
    }

    ops[0] = op_putfh(root);
    ops[1] = op_open_create(pc->clientid, open_owner, fname);
    ops[2] = op_getfh();
    p      = pc_compound(pc, ops, 3, 1);
    if (p.status != NFS4_OK || !p.fh.has) {
        snprintf(buf, sizeof(buf), "create %s failed: %u", fname, p.status);
        expect(c->label, 0, buf);
        return;
    }
    fh       = p.fh;
    open_sid = p.open_sid;

    for (i = 0; i < MAX_STEPS && c->steps[i].op != S_END; i++) {
        const struct step *s = &c->steps[i];

        w = s->who;
        snprintf(label, sizeof(label), "%s/%d", c->label, i + 1);
        range_str(rng, sizeof(rng), s->offset, s->length);

        ops[0] = op_putfh(&fh);
        switch (s->op) {
            case S_LOCK:
                ops[1] = has_sid[w]
                    ? op_lock_existing(&sid[w], s->type, s->offset, s->length)
                    : op_lock_new(pc->clientid, lock_owner[w], &open_sid,
                                  s->type, s->offset, s->length);
                break;
            case S_LOCKT:
                ops[1] = op_lockt(pc->clientid, lock_owner[w], s->type,
                                  s->offset, s->length);
                break;
            case S_LOCKU:
                if (!has_sid[w]) {
                    expect(label, 0, "LOCKU by an owner with no lock stateid");
                    continue;
                }
                ops[1] = op_locku(&sid[w], s->type, s->offset, s->length);
                break;
            default:
                ops[1] = op_close(&open_sid);
                break;
        } /* switch */
        p = pc_compound(pc, ops, 2, 1);

        /* The step's own status, or nothing: a compound that stopped before
         * the step must fail it rather than lend it an earlier status. */
        if (p.meas_idx < 0) {
            snprintf(buf, sizeof(buf),
                     "%s: compound returned %u without reaching it",
                     step_op_name(s->op), p.status);
            expect(label, 0, buf);
            continue;
        }
        got = p.st[p.meas_idx];
        if (got == NFS4_OK && (s->op == S_LOCK || s->op == S_LOCKU)) {
            sid[w]     = p.lock_sid;
            has_sid[w] = 1;
        }
        if (got == NFS4_OK && s->op == S_CLOSE) {
            closed = 1;
        }

        ok = (got == s->expect);
        if (ok && got == NFS4ERR_DENIED) {
            ok = p.d_type == s->d_type &&
                p.d_offset == s->d_offset &&
                p.d_length == s->d_length;
        }

        describe(want, sizeof(want), s->expect, s->d_type, s->d_offset,
                 s->d_length);
        describe(got_desc, sizeof(got_desc), got, p.d_type, p.d_offset,
                 p.d_length);
        if (s->op == S_CLOSE) {
            snprintf(buf, sizeof(buf), "CLOSE: expected %s, got %s",
                     want, got_desc);
        } else {
            snprintf(buf, sizeof(buf), "%c %s %s %s: expected %s, got %s",
                     'A' + w, step_op_name(s->op), lock_type_name(s->type),
                     rng, want, got_desc);
        }
        expect(label, ok, buf);
    }

    /* Release whatever the script left held, so no state outlives the
     * cell. */
    if (!closed) {
        for (w = 0; w < N_OWNERS; w++) {
            if (has_sid[w]) {
                ops[0] = op_putfh(&fh);
                ops[1] = op_locku(&sid[w], WRITE_LT, 0, TO_EOF);
                pc_compound(pc, ops, 2, 1);
            }
        }
        ops[0] = op_putfh(&fh);
        ops[1] = op_close(&open_sid);
        pc_compound(pc, ops, 2, 1);
    }
} /* run_cell */

int
main(void)
{
    struct mbt_env_opts opts;
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           pc;
    struct mbt_fh       root;
    unsigned int        i;

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);

    memset(&opts, 0, sizeof(opts));
    opts.module = "memfs";
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = rl_cb_compound;

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

    pc_setup(&pc, env);

    printf("# --- NFSv4 byte-range re-lock (memfs) ---\n");

    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
        run_cell(&pc, &root, i);
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 re-lock check(s) FAILED\n", failures);
        return 1;
    }
    printf("all NFSv4 re-lock checks passed\n");
    return 0;
} /* main */
