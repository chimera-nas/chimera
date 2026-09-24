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
    struct stateid4 lock_sid;         /* last LOCK stateid */
    int             lock_idx;         /* index of the last LOCK in st[] */
    int             setattr_idx;      /* index of the last SETATTR in st[] */
    int             has_size;         /* GETATTR returned FATTR4_SIZE */
    uint64_t        size;
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
    p->rpc_err     = status;
    p->open_idx    = -1;
    p->lock_idx    = -1;
    p->setattr_idx = -1;
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
                        p->open_sid = r->opopen.resok4.stateid;
                    }
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
                    p->st[i]       = r->opsetattr.status;
                    p->setattr_idx = (int) i;
                    break;
                case OP_GETATTR:
                    p->st[i] = r->opgetattr.status;
                    /* Only FATTR4_SIZE is requested, so it is the sole value
                     * in the blob. */
                    if (p->st[i] == NFS4_OK &&
                        r->opgetattr.resok4.obj_attributes.num_attrmask >= 1 &&
                        (r->opgetattr.resok4.obj_attributes.attrmask[0] &
                         (1U << FATTR4_SIZE)) &&
                        r->opgetattr.resok4.obj_attributes.attr_vals.len >= 8) {
                        p->has_size = 1;
                        p->size     = unpack_be64(
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
    snprintf(owner, sizeof(owner), "ss-probe");
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
 * that open, so the cell's own open is the only state on the file. */
static int
create_file(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *name,
    const char          *owner,
    struct mbt_fh       *out_fh,
    char                *buf,
    size_t               buflen)
{
    struct nfs_argop4 ops[4];
    struct prep       p;
    struct stateid4   sid;

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

    ops[0] = op_putfh(out_fh);
    ops[1] = op_setattr_size(&sid, FILE_SIZE);
    ops[2] = op_close(&sid);
    p      = pc_compound(pc, ops, 3, 1);
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

    if (!create_file(pc, root, fname, create_owner, &target_fh,
                     buf, sizeof(buf))) {
        expect(label, 0, buf);
        return;
    }
    source_fh   = target_fh;
    source_name = fname;
    if (cells[i].other_file) {
        if (!create_file(pc, root, oname, create_owner, &source_fh,
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
    if (p.setattr_idx < 0) {
        snprintf(buf, sizeof(buf),
                 "compound returned %u without reaching SETATTR", p.status);
        expect(label, 0, buf);
    } else {
        got = p.st[p.setattr_idx];
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
    struct pc           pc;
    struct mbt_fh       root;
    unsigned int        i;

    setvbuf(stdout, NULL, _IONBF, 0);
    /* Passthrough backends create the export root on the host; keep its mode
     * the one the model expects, as the replayers do. */
    umask(0);
    mbt_watchdog_arm(60);

    opts.module = argc > 1 ? argv[1] : "memfs";
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

    pc_setup(&pc, env);

    printf("# --- NFSv4 SETATTR(size) stateid authorization (%s) ---\n",
           opts.module);

    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
        run_cell(&pc, &root, i);
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
