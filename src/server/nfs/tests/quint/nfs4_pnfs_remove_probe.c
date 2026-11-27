// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * pNFS REMOVE, and RENAME over a last link, against layout holders.
 *
 * Each run stands up one metadata server whose data server is a local memfs
 * mount (/localds, also exported so this probe can look at it over NFSv3),
 * drives NFSv4.1 clients with real backchannels over the inproc transport,
 * and runs the one subtest named on the command line.  The checks use only
 * what a client or the data-server export can see: replies, the
 * CB_LAYOUTRECALLs a client receives, whether the backing file exists, and the
 * server's Prometheus counters.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_unlinked.h"
#include "vfs/vfs_ref.h"
#include "server/nfs/nfs4_pnfs_test_hooks.h"

#define P_LAYOUT_FLEX 0x4   /* LAYOUT4_FLEX_FILES (RFC 8435) */
#define P_MAX_OPS     10
#define P_WATCHDOG_S  60    /* per-subtest watchdog, seconds */
#define P_FANOUT      40    /* more holders than one recall batch (32) */

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
                exit(1); \
            } \
        } while (0)

/* ---- one NFSv4.1 client ---------------------------------------------------- */

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
    uint32_t               cs_seq;         /* last CREATE_SESSION csa_sequence */
    struct channel_attrs4  chan;           /* its channel attributes */
    char                   owner[32];
    uint32_t               recall_answer;  /* what a CB_LAYOUTRECALL gets */
    int                    recall_delays;  /* ... after this many DELAYs */
    int                    nlayoutrecalls; /* CB_LAYOUTRECALLs received */
    struct mbt_fh          fh;             /* the file this client opened */
    struct stateid4        open_sid;
    struct stateid4        layout_sid;
};

static void
pc_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct pc             *pc = private_data;
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4  *resarray;
    uint32_t               status = NFS4_OK;
    uint32_t               i;
    int                    rc;

    (void) conn;
    (void) cred;

    memset(&res, 0, sizeof(res));
    resarray = xdr_dbuf_alloc_space(
        sizeof(*resarray) * (args->num_argarray ? args->num_argarray : 1),
        encoding->dbuf);
    CHECK(resarray != NULL);

    for (i = 0; i < args->num_argarray; i++) {
        struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4 *resop = &resarray[i];

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
            case OP_CB_LAYOUTRECALL:
                pc->nlayoutrecalls++;
                if (pc->recall_delays > 0) {
                    pc->recall_delays--;
                    resop->opcblayoutrecall.clorr_status = NFS4ERR_DELAY;
                    status                               = NFS4ERR_DELAY;
                    break;
                }
                resop->opcblayoutrecall.clorr_status = pc->recall_answer;
                /* The server judges the recall by the compound status. */
                status = pc->recall_answer;
                break;
            case OP_CB_RECALL:
                resop->opcbrecall.status = NFS4_OK;
                break;
            default:
                resop->opcbrecall.status = NFS4ERR_NOTSUPP;
                status                   = NFS4ERR_NOTSUPP;
                break;
        } /* switch */
    }

    res.status       = status;
    res.num_resarray = args->num_argarray;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res, encoding);
    (void) rc;
} /* pc_cb_compound */

/* ---- compound plumbing ----------------------------------------------------- */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    uint32_t        st[P_MAX_OPS];
    struct mbt_fh   fh;
    struct stateid4 open_sid;
    struct stateid4 layout_sid;
    uint64_t        clientid;
    uint32_t        eir_seq;
    uint8_t         sessionid[16];
    uint32_t        read_len;         /* last READ's byte count */
    uint8_t         read_byte0;       /* and its first byte */
};

struct p_async {
    struct nfs_argop4 argarray[P_MAX_OPS];
    struct prep       p;
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

    p->rpc_err = status;
    if (status == 0) {
        p->status = reply->status;
        for (i = 0; i < reply->num_resarray && i < P_MAX_OPS; i++) {
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
                case OP_SAVEFH:
                    p->st[i] = r->opsavefh.status;
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
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_REMOVE:
                    p->st[i] = r->opremove.status;
                    break;
                case OP_RENAME:
                    p->st[i] = r->oprename.status;
                    break;
                case OP_LINK:
                    p->st[i] = r->oplink.status;
                    break;
                case OP_SETATTR:
                    p->st[i] = r->opsetattr.status;
                    break;
                case OP_LAYOUTGET:
                    p->st[i] = r->oplayoutget.logr_status;
                    if (p->st[i] == NFS4_OK) {
                        p->layout_sid = r->oplayoutget.logr_resok4.logr_stateid;
                    }
                    break;
                case OP_LAYOUTRETURN:
                    p->st[i] = r->oplayoutreturn.lorr_status;
                    break;
                case OP_WRITE:
                    p->st[i] = r->opwrite.status;
                    break;
                case OP_READ:
                    p->st[i] = r->opread.status;
                    if (p->st[i] == NFS4_OK) {
                        int k;

                        p->read_len = r->opread.resok4.data.length;
                        if (r->opread.resok4.data.niov > 0 && p->read_len > 0) {
                            p->read_byte0 =
                                ((const uint8_t *) r->opread.resok4.data.iov[0].data)[0];
                        }
                        /* The reply's data iovecs carry references the
                         * caller now owns. */
                        for (k = 0; k < r->opread.resok4.data.niov; k++) {
                            evpl_iovec_release(evpl, &r->opread.resok4.data.iov[k]);
                        }
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
                case OP_RECLAIM_COMPLETE:
                    p->st[i] = r->opreclaim_complete.rcr_status;
                    break;
                default:
                    p->st[i] = 0;
                    break;
            } /* switch */
        }
    }
    p->done = 1;
} /* prep_cb */

static void
p_send(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    uint32_t               minor,
    struct nfs_argop4     *ops,
    int                    nops,
    struct prep           *p)
{
    struct COMPOUND4args args;

    memset(&args, 0, sizeof(args));
    memset(p, 0, sizeof(*p));
    args.minorversion = minor;
    args.argarray     = ops;
    args.num_argarray = (uint32_t) nops;

    env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl, conn,
                                            &env->cred, &args, 0, 0, NULL, 0, 0,
                                            prep_cb, p);
} /* p_send */

static void
p_wait(
    struct mbt_env *env,
    struct prep    *p)
{
    while (!p->done) {
        evpl_continue(env->evpl);
    }
    CHECK(p->rpc_err == 0);
} /* p_wait */

static void
pc_fill(
    struct pc               *pc,
    struct nfs_argop4       *argarray,
    const struct nfs_argop4 *ops,
    int                      nops)
{
    int i;

    CHECK(nops + 1 <= P_MAX_OPS);
    memset(&argarray[0], 0, sizeof(argarray[0]));
    argarray[0].argop = OP_SEQUENCE;
    memcpy(argarray[0].opsequence.sa_sessionid, pc->sessionid, 16);
    argarray[0].opsequence.sa_sequenceid     = ++pc->slot_seq;
    argarray[0].opsequence.sa_slotid         = 0;
    argarray[0].opsequence.sa_highest_slotid = 0;
    argarray[0].opsequence.sa_cachethis      = 0;
    for (i = 0; i < nops; i++) {
        argarray[i + 1] = ops[i];
    }
} /* pc_fill */

/* SEQUENCE + ops, waited for.  st[0] is SEQUENCE; st[k] is ops[k - 1]. */
static struct prep
pc_compound(
    struct pc               *pc,
    const struct nfs_argop4 *ops,
    int                      nops)
{
    struct nfs_argop4 argarray[P_MAX_OPS];
    struct prep       p;

    pc_fill(pc, argarray, ops, nops);
    p_send(pc->env, pc->conn, 1, argarray, nops + 1, &p);
    p_wait(pc->env, &p);
    return p;
} /* pc_compound */

/* SEQUENCE + ops, sent but not waited for; the caller p_waits and frees. */
static struct p_async *
pc_compound_async(
    struct pc               *pc,
    const struct nfs_argop4 *ops,
    int                      nops)
{
    struct p_async *a = calloc(1, sizeof(*a));

    CHECK(a != NULL);
    pc_fill(pc, a->argarray, ops, nops);
    p_send(pc->env, pc->conn, 1, a->argarray, nops + 1, &a->p);
    return a;
} /* pc_compound_async */

/* ---- op builders ------------------------------------------------------------ */

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
op_savefh(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SAVEFH;
    return a;
} /* op_savefh */

static struct nfs_argop4
op_getfh(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_GETFH;
    return a;
} /* op_getfh */

static struct nfs_argop4
op_open(
    const struct pc *pc,
    const char      *name,
    int              create)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = OPEN4_SHARE_ACCESS_BOTH;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = pc->clientid;
    a.opopen.owner.owner.data = (void *) pc->owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(pc->owner);
    if (create) {
        a.opopen.openhow.opentype                      = OPEN4_CREATE;
        a.opopen.openhow.how.mode                      = UNCHECKED4;
        a.opopen.openhow.how.createattrs.num_attrmask  = 0;
        a.opopen.openhow.how.createattrs.attr_vals.len = 0;
    } else {
        a.opopen.openhow.opentype = OPEN4_NOCREATE;
    }
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open */

/* OPEN of the current filehandle (CLAIM_FH): the file is named by the handle
 * alone, as a client opening a file it already has a handle for does. */
static struct nfs_argop4
op_open_fh(const struct pc *pc)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.share_access     = OPEN4_SHARE_ACCESS_BOTH;
    a.opopen.owner.clientid   = pc->clientid;
    a.opopen.owner.owner.data = (void *) pc->owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(pc->owner);
    a.opopen.openhow.opentype = OPEN4_NOCREATE;
    a.opopen.claim.claim      = CLAIM_FH;
    return a;
} /* op_open_fh */

static struct nfs_argop4
op_layoutget(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                                = OP_LAYOUTGET;
    a.oplayoutget.loga_signal_layout_avail = 0;
    a.oplayoutget.loga_layout_type         = P_LAYOUT_FLEX;
    a.oplayoutget.loga_iomode              = LAYOUTIOMODE4_RW;
    a.oplayoutget.loga_offset              = 0;
    a.oplayoutget.loga_length              = UINT64_MAX;
    a.oplayoutget.loga_minlength           = 0;
    a.oplayoutget.loga_stateid             = *sid;
    a.oplayoutget.loga_maxcount            = 65536;
    return a;
} /* op_layoutget */

static struct nfs_argop4
op_layoutreturn(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                                                   = OP_LAYOUTRETURN;
    a.oplayoutreturn.lora_reclaim                             = 0;
    a.oplayoutreturn.lora_layout_type                         = P_LAYOUT_FLEX;
    a.oplayoutreturn.lora_iomode                              = LAYOUTIOMODE4_ANY;
    a.oplayoutreturn.lora_layoutreturn.lr_returntype          = LAYOUTRETURN4_FILE;
    a.oplayoutreturn.lora_layoutreturn.lr_layout.lrf_offset   = 0;
    a.oplayoutreturn.lora_layoutreturn.lr_layout.lrf_length   = UINT64_MAX;
    a.oplayoutreturn.lora_layoutreturn.lr_layout.lrf_stateid  = *sid;
    a.oplayoutreturn.lora_layoutreturn.lr_layout.lrf_body.len = 0;
    return a;
} /* op_layoutreturn */

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
op_remove(const char *name)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_REMOVE;
    a.opremove.target.data = (void *) name;
    a.opremove.target.len  = (uint32_t) strlen(name);
    return a;
} /* op_remove */

static struct nfs_argop4
op_rename(
    const char *oldname,
    const char *newname)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_RENAME;
    a.oprename.oldname.data = (void *) oldname;
    a.oprename.oldname.len  = (uint32_t) strlen(oldname);
    a.oprename.newname.data = (void *) newname;
    a.oprename.newname.len  = (uint32_t) strlen(newname);
    return a;
} /* op_rename */

static struct nfs_argop4
op_link(const char *newname)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop               = OP_LINK;
    a.oplink.newname.data = (void *) newname;
    a.oplink.newname.len  = (uint32_t) strlen(newname);
    return a;
} /* op_link */

/* WRITE of `len` bytes of 'A' at offset 0 through `sid`, FILE_SYNC.  The
 * iovec is static (the op only borrows it); its buffer is allocated afresh on
 * every call and the send releases the reference it takes. */
static struct nfs_argop4
op_write_bytes(
    struct evpl           *evpl,
    const struct stateid4 *sid,
    uint32_t               len)
{
    static struct evpl_iovec iov;
    struct nfs_argop4        a;

    CHECK(evpl_iovec_alloc(evpl, len, 4096, 1, 0, &iov) >= 0);
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

/* SETATTR(size) through `sid`.  Static bitmap and blob: each compound is
 * encoded before the next call reuses them. */
static struct nfs_argop4
op_setattr_size(
    const struct stateid4 *sid,
    uint64_t               size)
{
    static uint32_t   bitmap[1];
    static uint8_t    blob[8];
    struct nfs_argop4 a;
    int               i;

    memset(&a, 0, sizeof(a));
    a.argop   = OP_SETATTR;
    bitmap[0] = 1U << FATTR4_SIZE;
    for (i = 0; i < 8; i++) {
        blob[i] = (uint8_t) (size >> (56 - 8 * i));
    }
    a.opsetattr.stateid                       = *sid;
    a.opsetattr.obj_attributes.num_attrmask   = 1;
    a.opsetattr.obj_attributes.attrmask       = bitmap;
    a.opsetattr.obj_attributes.attr_vals.data = blob;
    a.opsetattr.obj_attributes.attr_vals.len  = 8;
    return a;
} /* op_setattr_size */

/* ---- client setup and common sequences ------------------------------------- */

static struct callback_sec_parms4 p_cb_sec = { .cb_secflavor = 0 };

static void
pc_setup(
    struct pc      *pc,
    struct mbt_env *env,
    int             sym,
    int             backchannel)
{
    struct evpl_rpc2_program   *cb_programs[1];
    struct evpl_endpoint       *ep;
    struct nfs_argop4           op;
    struct prep                 p;
    struct channel_attrs4       chan = {
        .ca_headerpadsize          = 0,
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
        .num_ca_rdma_ird           = 0,
    };
    struct callback_sec_parms4 *secp = &p_cb_sec;

    memset(pc, 0, sizeof(*pc));
    pc->env           = env;
    pc->recall_answer = NFS4_OK;
    snprintf(pc->owner, sizeof(pc->owner), "pnfs-remove-%d", sym);

    cb_programs[0] = &env->nfs_v4_cb.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    pc->conn = evpl_rpc2_client_connect(env->rpc2_thread, EVPL_STREAM_INPROC, ep,
                                        cb_programs, 1, pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    memset(op.opexchange_id.eia_clientowner.co_verifier, sym + 1, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = pc->owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(pc->owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;
    p_send(env, pc->conn, 1, &op, 1, &p);
    p_wait(env, &p);
    CHECK(p.status == NFS4_OK);
    pc->clientid = p.clientid;
    pc->cs_seq   = p.eir_seq;

    memset(&op, 0, sizeof(op));
    op.argop                                = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid        = pc->clientid;
    op.opcreate_session.csa_sequence        = p.eir_seq;
    op.opcreate_session.csa_flags           = backchannel ? CREATE_SESSION4_FLAG_CONN_BACK_CHAN : 0;
    op.opcreate_session.csa_fore_chan_attrs = chan;
    op.opcreate_session.csa_back_chan_attrs = chan;
    op.opcreate_session.csa_cb_program      = 0x40000000;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = secp;
    p_send(env, pc->conn, 1, &op, 1, &p);
    p_wait(env, &p);
    CHECK(p.status == NFS4_OK);
    memcpy(pc->sessionid, p.sessionid, 16);
    pc->chan = chan;

    /* A fresh 4.1 client is held in grace until it declares reclaim done. */
    memset(&op, 0, sizeof(op));
    op.argop                         = OP_RECLAIM_COMPLETE;
    op.opreclaim_complete.rca_one_fs = 0;
    p                                = pc_compound(pc, &op, 1);
    CHECK(p.status == NFS4_OK);
} /* pc_setup */

/* A SEQUENCE-only compound: renews the client's lease.  Returns the SEQUENCE
 * status. */
static uint32_t
pc_renew(struct pc *pc)
{
    struct prep p = pc_compound(pc, NULL, 0);

    return p.st[0];
} /* pc_renew */

static void
pc_open(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *name,
    int                  create)
{
    struct nfs_argop4 ops[3];
    struct prep       p;

    ops[0] = op_putfh(root);
    ops[1] = op_open(pc, name, create);
    ops[2] = op_getfh();
    p      = pc_compound(pc, ops, 3);
    CHECK(p.status == NFS4_OK);
    pc->fh       = p.fh;
    pc->open_sid = p.open_sid;
} /* pc_open */

static void
pc_layoutget(struct pc *pc)
{
    struct nfs_argop4 ops[2];
    struct prep       p;

    ops[0] = op_putfh(&pc->fh);
    ops[1] = op_layoutget(&pc->open_sid);
    p      = pc_compound(pc, ops, 2);
    CHECK(p.status == NFS4_OK);
    pc->layout_sid = p.layout_sid;
} /* pc_layoutget */

static void
pc_open_layout(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *name,
    int                  create)
{
    pc_open(pc, root, name, create);
    pc_layoutget(pc);
} /* pc_open_layout */

static struct prep
pc_layoutreturn(struct pc *pc)
{
    struct nfs_argop4 ops[2];

    ops[0] = op_putfh(&pc->fh);
    ops[1] = op_layoutreturn(&pc->layout_sid);
    return pc_compound(pc, ops, 2);
} /* pc_layoutreturn */

static void
pc_close(struct pc *pc)
{
    struct nfs_argop4 ops[2];
    struct prep       p;

    ops[0] = op_putfh(&pc->fh);
    ops[1] = op_close(&pc->open_sid);
    p      = pc_compound(pc, ops, 2);
    CHECK(p.status == NFS4_OK);
} /* pc_close */

static uint32_t
pc_remove(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *name)
{
    struct nfs_argop4 ops[2];
    struct prep       p;

    ops[0] = op_putfh(root);
    ops[1] = op_remove(name);
    p      = pc_compound(pc, ops, 2);
    return p.status;
} /* pc_remove */

/* ---- environment ---------------------------------------------------------- */

/* An MDS on memfs whose data server's backing root is the mount `ds_path`:
 * the local memfs mount /localds, or /fs0 itself for the combined topology
 * where the metadata server is its own data server.  A NULL `ds_path`
 * registers no data server at all. */
static struct mbt_env *
p_env_start_ds(const char *ds_path)
{
    struct mbt_env     *env  = calloc(1, sizeof(*env));
    struct mbt_env_opts opts = { 0 };
    struct chimera_vfs *vfs;

    CHECK(env != NULL);
    opts.disable_caches = 1;
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = pc_cb_compound;

    vfs = chimera_server_get_vfs(env->server);
    mbt_env_fs_setup(env, "localds");
    chimera_vfs_pnfs_set_enabled(vfs, 1);
    if (ds_path) {
        CHECK(chimera_vfs_pnfs_add_device(vfs, "tcp", "127.0.0.1.8.1", NULL,
                                          ds_path, 3, 0) >= 0);
        CHECK(chimera_server_pnfs_resolve(env->server) == 0);
    }
    return env;
} /* p_env_start_ds */

static struct mbt_env *
p_env_start(void)
{
    return p_env_start_ds("/localds");
} /* p_env_start */

static void
p_vfs_cleanup(
    void);

static void
p_env_stop(struct mbt_env *env)
{
    p_vfs_cleanup();
    mbt_env_stop(env);
    free(env);
} /* p_env_stop */

static struct mbt_fh
p_root(struct mbt_env *env)
{
    struct nfs_argop4 ops[3];
    struct prep       p;

    memset(ops, 0, sizeof(ops));
    ops[0].argop                 = OP_PUTROOTFH;
    ops[1].argop                 = OP_LOOKUP;
    ops[1].oplookup.objname.data = "fs0";
    ops[1].oplookup.objname.len  = 3;
    ops[2].argop                 = OP_GETFH;
    p_send(env, env->nfs_conn, 0, ops, 3, &p);
    p_wait(env, &p);
    CHECK(p.status == NFS4_OK && p.fh.has);
    return p.fh;
} /* p_root */

static struct mbt_fh
p_dsroot(struct mbt_env *env)
{
    struct mbt_fh fh = mbt_mnt(env, "/localds")->obj_fh;

    CHECK(fh.has);
    return fh;
} /* p_dsroot */

/* ---- what the data server holds ------------------------------------------- */

/* Number of backing files in the data server's root; the first one's name is
 * copied to `name` (MBT_NAME_MAX bytes). */
static int
ds_backing(
    struct mbt_env      *env,
    const struct mbt_fh *dsroot,
    char                *name)
{
    struct mbt_result *r = mbt_readdir(env, dsroot);
    int                i, n = 0;

    CHECK(r->status == NFS3_OK);
    name[0] = '\0';
    for (i = 0; i < r->nentries; i++) {
        if (r->entries[i].name[0] == '.') {
            continue;
        }
        if (n == 0) {
            memcpy(name, r->entries[i].name, r->entries[i].name_len);
            name[r->entries[i].name_len] = '\0';
        }
        n++;
    }
    return n;
} /* ds_backing */

static int
ds_has(
    struct mbt_env      *env,
    const struct mbt_fh *dsroot,
    const char          *name)
{
    return mbt_lookup(env, dsroot, name, (uint32_t) strlen(name))->status == NFS3_OK;
} /* ds_has */

static void
p_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
} /* p_tick */

/* Run the event loop for `rounds` rounds of about 10 ms.  evpl_continue
 * blocks while nothing is pending, so each round arms a one-shot timer to be
 * sure it returns. */
static void
p_pump(
    struct mbt_env *env,
    int             rounds)
{
    struct evpl_timer t;
    int               i;

    for (i = 0; i < rounds; i++) {
        evpl_add_oneshot_timer(env->evpl, &t, p_tick, 10000);
        evpl_continue(env->evpl);
        evpl_remove_timer(env->evpl, &t);
        usleep(10000);
    }
} /* p_pump */

/* Wait (10 ms a try) until the backing file's presence equals `present`. */
static int
p_wait_backing(
    struct mbt_env      *env,
    const struct mbt_fh *dsroot,
    const char          *name,
    int                  present,
    int                  tries)
{
    int i;

    for (i = 0; i < tries; i++) {
        if (ds_has(env, dsroot, name) == present) {
            return 1;
        }
        p_pump(env, 1);
    }
    return ds_has(env, dsroot, name) == present;
} /* p_wait_backing */

static int
p_recalls(
    const struct pc *c,
    int              n)
{
    int i, s = 0;

    for (i = 0; i < n; i++) {
        s += c[i].nlayoutrecalls;
    }
    return s;
} /* p_recalls */

/* Pump until the n clients have received `want` CB_LAYOUTRECALLs, or 5 s. */
static int
p_pump_until_recalls(
    struct mbt_env  *env,
    const struct pc *c,
    int              n,
    int              want)
{
    int i;

    for (i = 0; i < 500 && p_recalls(c, n) < want; i++) {
        p_pump(env, 1);
    }
    return p_recalls(c, n);
} /* p_pump_until_recalls */

/* ---- the server's own account --------------------------------------------- */

static long long
p_metric(
    struct mbt_env *env,
    const char     *name,
    const char     *label,
    const char     *value)
{
    static char buf[1 << 20];
    char        want[160];
    const char *p;
    int         n;

    n = prometheus_metrics_scrape(env->metrics, buf, sizeof(buf) - 1);
    if (n <= 0) {
        return -1;
    }
    buf[n] = '\0';
    snprintf(want, sizeof(want), "\n%s{%s=\"%s\"} ", name, label, value);
    p = strstr(buf, want);
    return p ? atoll(p + strlen(want)) : -1;
} /* p_metric */

/* The VFS's own account of unlinked files, which counts every protocol. */
static long long
p_vfs_ev(
    struct mbt_env *env,
    const char     *event)
{
    return p_metric(env, "chimera_vfs_unlinked", "event", event);
} /* p_vfs_ev */

static int
p_wait_vfs_ev(
    struct mbt_env *env,
    const char     *event,
    long long       want)
{
    int i;

    for (i = 0; i < 500 && p_vfs_ev(env, event) != want; i++) {
        p_pump(env, 1);
    }
    return p_vfs_ev(env, event) == want;
} /* p_wait_vfs_ev */

/* ---- a VFS handle held by the probe itself, standing in for an open from a
* protocol the probe cannot speak in-process (SMB) ----------------------- */

static struct chimera_vfs_thread      *p_vfs_thr;
static uint8_t                         p_fh[CHIMERA_VFS_FH_SIZE];
static uint16_t                        p_fh_len;
static _Atomic int                     p_hook_hit;
static struct chimera_vfs_open_handle *p_held;

struct p_vopen {
    int                             done;
    enum chimera_vfs_error status;
    struct chimera_vfs_open_handle *handle;
};

static void
p_vopen_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct p_vopen *o = private_data;

    o->status = error_code;
    o->handle = handle;
    o->done   = 1;
} /* p_vopen_cb */

/* The metadata server's own handle for the file, remembered by the first
 * LAYOUTGET that reaches registration. */
static void
p_capture_hook(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    void                             *arg)
{
    (void) thread;
    (void) arg;
    memcpy(p_fh, fh, fh_len);
    p_fh_len = fh_len;
} /* p_capture_hook */

static void
p_capture_on(void)
{
    p_fh_len                                              = 0;
    chimera_nfs_pnfs_test_hooks.layoutget_before_register = p_capture_hook;
} /* p_capture_on */

static void
p_capture_off(void)
{
    chimera_nfs_pnfs_test_hooks.layoutget_before_register = NULL;
} /* p_capture_off */

struct p_vlookup {
    int      done;
    int      status;
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
    uint16_t fh_len;
};

static void
p_vlookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct p_vlookup *l = private_data;

    l->status = error_code;
    if (error_code == CHIMERA_VFS_OK && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(l->fh, attr->va_fh, attr->va_fh_len);
        l->fh_len = (uint16_t) attr->va_fh_len;
    }
    l->done = 1;
} /* p_vlookup_cb */

/* Remember the VFS handle of /fs0/<name>, which is not the handle an NFS client
 * is given. */
static void
p_vfs_find(
    struct mbt_env *env,
    const char     *name)
{
    struct chimera_vfs_cred cred;
    struct p_vlookup        l = { 0 };
    uint8_t                 root[CHIMERA_VFS_FH_SIZE];
    uint32_t                root_len = 0;

    if (!p_vfs_thr) {
        p_vfs_thr = chimera_vfs_thread_init(env->evpl, chimera_server_get_vfs(env->server));
        CHECK(p_vfs_thr != NULL);
    }
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);
    chimera_vfs_get_root_fh(chimera_server_get_vfs(env->server), root, &root_len);

    chimera_vfs_lookup(p_vfs_thr, &cred, root, (int) root_len, "fs0", 3,
                       CHIMERA_VFS_ATTR_FH, 0, p_vlookup_cb, &l);
    while (!l.done) {
        evpl_continue(env->evpl);
    }
    CHECK(l.status == CHIMERA_VFS_OK && l.fh_len != 0);

    memcpy(root, l.fh, l.fh_len);
    root_len = l.fh_len;
    memset(&l, 0, sizeof(l));
    chimera_vfs_lookup(p_vfs_thr, &cred, root, (int) root_len, name, (int) strlen(name),
                       CHIMERA_VFS_ATTR_FH, 0, p_vlookup_cb, &l);
    while (!l.done) {
        evpl_continue(env->evpl);
    }
    CHECK(l.status == CHIMERA_VFS_OK && l.fh_len != 0);
    memcpy(p_fh, l.fh, l.fh_len);
    p_fh_len = l.fh_len;
} /* p_vfs_find */

static void
p_hold(struct mbt_env *env)
{
    struct chimera_vfs_cred cred;
    struct p_vopen          o = { 0 };

    CHECK(p_fh_len != 0);
    if (!p_vfs_thr) {
        p_vfs_thr = chimera_vfs_thread_init(env->evpl, chimera_server_get_vfs(env->server));
        CHECK(p_vfs_thr != NULL);
    }
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);
    chimera_vfs_open_fh(p_vfs_thr, &cred, p_fh, p_fh_len, CHIMERA_VFS_OPEN_INFERRED,
                        p_vopen_cb, &o);
    while (!o.done) {
        evpl_continue(env->evpl);
    }
    CHECK(o.status == CHIMERA_VFS_OK);
    p_held = o.handle;
} /* p_hold */

static void
p_unhold(void)
{
    if (p_held) {
        chimera_vfs_release(p_vfs_thr, p_held);
        p_held = NULL;
    }
} /* p_unhold */

static void
p_vfs_cleanup(void)
{
    p_unhold();
    if (p_vfs_thr) {
        chimera_vfs_thread_destroy(p_vfs_thr);
        p_vfs_thr = NULL;
    }
} /* p_vfs_cleanup */

/* Runs on the server thread inside LAYOUTGET: the foreign reference goes just
 * as LAYOUTGET is about to turn its own open into a layout. */
static void
p_drop_hook(
    struct chimera_vfs_thread *vfs_thread,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    void                      *arg)
{
    (void) fh;
    (void) fh_len;
    (void) arg;
    atomic_fetch_add(&p_hook_hit, 1);
    if (p_held) {
        chimera_vfs_release(vfs_thread, p_held);
        p_held = NULL;
    }
} /* p_drop_hook */

/* ---- subtests -------------------------------------------------------------- */

/* REMOVE deletes the backing file LAYOUTGET created, named
 * <MDS mount id, hex> '_' <file id, 16 hex digits>.  The layout is returned
 * first, so nothing waits. */
static void
t_name(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_open_layout(&a, &root, "f", 1);

    CHECK(ds_backing(env, &dsroot, backing) == 1);
    CHECK(strlen(backing) == CHIMERA_VFS_PNFS_BACKING_NAME_MAX - 1);
    CHECK(backing[CHIMERA_VFS_MOUNTID_SIZE * 2] == '_');

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    pc_close(&a);
    CHECK(pc_remove(&a, &root, "f") == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));

    p_env_stop(env);
} /* t_name */

/* 40 clients hold a layout on one file; a SETATTR(size) by one of them must
 * recall every holder, not only the first batch of 32, before it truncates.
 * Holders decline, so each is revoked as soon as its recall is answered. */
static void
t_fanout_setattr(void)
{
    struct mbt_env   *env  = p_env_start();
    struct mbt_fh     root = p_root(env);
    struct pc        *c    = calloc(P_FANOUT, sizeof(*c));
    struct p_async   *sa;
    struct nfs_argop4 ops[2];
    int               i;

    CHECK(c != NULL);
    for (i = 0; i < P_FANOUT; i++) {
        pc_setup(&c[i], env, i, 1);
        c[i].recall_answer = NFS4ERR_NOMATCHING_LAYOUT;
        pc_open_layout(&c[i], &root, "big", i == 0);
    }

    ops[0] = op_putfh(&c[0].fh);
    ops[1] = op_setattr_size(&c[0].open_sid, 0);
    sa     = pc_compound_async(&c[0], ops, 2);

    CHECK(p_pump_until_recalls(env, c, P_FANOUT, P_FANOUT) == P_FANOUT);
    for (i = 0; i < P_FANOUT; i++) {
        CHECK(c[i].nlayoutrecalls == 1);
    }
    p_wait(env, &sa->p);
    CHECK(sa->p.status == NFS4_OK);

    free(sa);
    free(c);
    p_env_stop(env);
} /* t_fanout_setattr */

/* A accepts the recall but never returns its layout, and keeps renewing its
 * lease (3 s here).  Two lease periods after the recall that layout alone is
 * revoked, and B's SETATTR(size), parked behind it, completes.  A's lease is
 * untouched. */
static void
t_deadline_setattr(void)
{
    struct mbt_env   *env  = p_env_start();
    struct mbt_fh     root = p_root(env);
    struct pc         a, b;
    struct p_async   *sa;
    struct nfs_argop4 ops[2];
    int               i;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    pc_open(&b, &root, "f", 0);

    ops[0] = op_putfh(&b.fh);
    ops[1] = op_setattr_size(&b.open_sid, 0);
    sa     = pc_compound_async(&b, ops, 2);

    for (i = 0; i < 60 && !sa->p.done; i++) {
        CHECK(pc_renew(&a) == NFS4_OK);
        p_pump(env, 25);
    }
    CHECK(sa->p.done);
    CHECK(sa->p.rpc_err == 0);
    CHECK(sa->p.status == NFS4_OK);
    CHECK(a.nlayoutrecalls == 1);
    CHECK(pc_renew(&a) == NFS4_OK);

    free(sa);
    pc_close(&b);
    pc_close(&a);
    p_env_stop(env);
} /* t_deadline_setattr */

/* A holds a layout and has closed the file; B removes it.  No real open is
 * left, so B's REMOVE is answered at once and A is recalled at once.  The
 * backing file stays and LAYOUTGET is refused until A returns its layout, and
 * only then is the file deleted. */
static void
t_deferred(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[2];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);

    p_pump(env, 50);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "tracked") == 1);
    CHECK(p_vfs_ev(env, "purged") == 0);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_layoutget(&a.layout_sid);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4ERR_RECALLCONFLICT);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(p_vfs_ev(env, "purge_failed") == 0);

    p_env_stop(env);
} /* t_deferred */

/* A holder with no callback channel cannot be asked to return its layout, but
 * a lost backchannel is often temporary, so it is not revoked at once.  It
 * stays a recalled holder and the delete waits for it; it keeps renewing its
 * 3 s lease and loses the layout alone two lease periods after the recall.
 * No CB_LAYOUTRECALL ever reaches it. */
static void
t_revoke_nocb(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];
    int             i;

    pc_setup(&a, env, 1, 0);
    pc_setup(&b, env, 2, 1);
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    pc_close(&a);
    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    p_pump(env, 50);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "tracked") == 1);
    CHECK(p_vfs_ev(env, "purged") == 0);

    for (i = 0; i < 60 && ds_has(env, &dsroot, backing); i++) {
        CHECK(pc_renew(&a) == NFS4_OK);
        p_pump(env, 25);
    }
    CHECK(!ds_has(env, &dsroot, backing));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(a.nlayoutrecalls == 0);
    CHECK(pc_renew(&a) == NFS4_OK);

    p_env_stop(env);
} /* t_revoke_nocb */

/* A holder that declines the recall is revoked when it answers. */
static void
t_revoke_decline(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4ERR_NOMATCHING_LAYOUT;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    pc_close(&a);
    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_revoke_decline */

/* 40 holders, one remover: every holder is recalled exactly once. */
static void
t_fanout_remove(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc      *c      = calloc(P_FANOUT, sizeof(*c));
    struct pc       r;
    char            backing[MBT_NAME_MAX];
    int             i;

    CHECK(c != NULL);
    for (i = 0; i < P_FANOUT; i++) {
        pc_setup(&c[i], env, i, 1);
        c[i].recall_answer = NFS4ERR_NOMATCHING_LAYOUT;
        pc_open_layout(&c[i], &root, "big", i == 0);
    }
    pc_setup(&r, env, P_FANOUT, 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    for (i = 0; i < P_FANOUT; i++) {
        pc_close(&c[i]);
    }

    CHECK(pc_remove(&r, &root, "big") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, c, P_FANOUT, P_FANOUT) == P_FANOUT);
    for (i = 0; i < P_FANOUT; i++) {
        CHECK(c[i].nlayoutrecalls == 1);
    }
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    free(c);
    p_env_stop(env);
} /* t_fanout_remove */

/* The server shuts down while a holder that accepted the recall of an unlinked
 * file never returns its layout.  Shutdown must not hang or touch freed state
 * (ASan judges the latter), and the VFS counts the file as left behind. */
static void
t_shutdown_pending(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];
    uint64_t        leaked = chimera_vfs_unlinked_test_total_event(CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN);

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_vfs_ev(env, "purged") == 0);

    p_env_stop(env);
    CHECK(chimera_vfs_unlinked_test_total_event(CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN) == leaked + 1);
} /* t_shutdown_pending */

/* A closes its file but keeps the layout, accepts the recall and never
 * returns it, renewing its 3 s lease all the while.  Two lease periods after
 * the recall the layout alone is revoked and the backing file is deleted;
 * A's lease is untouched. */
static void
t_deadline_remove(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];
    int             i;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    for (i = 0; i < 60 && ds_has(env, &dsroot, backing); i++) {
        CHECK(pc_renew(&a) == NFS4_OK);
        p_pump(env, 25);
    }
    CHECK(!ds_has(env, &dsroot, backing));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(pc_renew(&a) == NFS4_OK);

    p_env_stop(env);
} /* t_deadline_remove */

/* A has the file open and a layout; B removes it.  Nothing is recalled while A
 * has it open: A keeps its layout, gets a new one, and does I/O.  A's CLOSE, the
 * last one, recalls the layout; the backing file stays until A returns it. */
static void
t_last_close(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[2];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "recalled") == 0);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_write_bytes(env->evpl, &a.open_sid, 4096);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[2] == NFS4_OK);
    ops[1] = op_read_bytes(&a.open_sid, 4096);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[2] == NFS4_OK);
    CHECK(p.read_len == 4096);
    CHECK(p.read_byte0 == 0x41);

    pc_close(&a);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 1);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "purged") == 0);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(p_vfs_ev(env, "purge_failed") == 0);

    p_env_stop(env);
} /* t_last_close */

/* A closed its file but kept the layout.  After B's REMOVE, A can still
 * PUTFH the unlinked file and return its layout, and the delete follows. */
static void
t_putfh_after_close(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    struct prep     p;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(ds_has(env, &dsroot, backing));

    p = pc_layoutreturn(&a);
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_putfh_after_close */

/* Removing one of two links recalls nothing and deletes nothing. */
static void
t_other_link(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[4];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_savefh();
    ops[2] = op_putfh(&root);
    ops[3] = op_link("g");
    p      = pc_compound(&a, ops, 4);
    CHECK(p.status == NFS4_OK);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "recalled") == 0);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    pc_close(&a);
    p_env_stop(env);
} /* t_other_link */

/* A accepts the recall, never returns the layout and stops renewing its
 * lease.  The deferred delete completes once A's state is reaped after the
 * lease (3 s) and courtesy period (1 s) set by this test's environment. */
static void
t_lease(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(ds_has(env, &dsroot, backing));

    CHECK(p_wait_backing(env, &dsroot, backing, 0, 3000));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_lease */

/* A holds a layout and keeps it; B removes the last link; A, which still has
 * the file open, links it under a new name.  When A has returned the layout
 * and closed, the deferred delete finds the file linked again and keeps the
 * backing file: the data is still readable through the new name. */
static void
t_link_back(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[4];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_write_bytes(env->evpl, &a.open_sid, 4096);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[2] == NFS4_OK);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_savefh();
    ops[2] = op_putfh(&root);
    ops[3] = op_link("g");
    p      = pc_compound(&a, ops, 4);
    CHECK(p.st[4] == NFS4_OK);

    /* The last close recalls the layout (the file is linked again but its
     * record stays); the purge then finds the link and keeps the data. */
    pc_close(&a);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(pc_layoutreturn(&a).st[2] == NFS4_OK);
    CHECK(p_wait_vfs_ev(env, "relinked", 1));
    CHECK(p_vfs_ev(env, "purged") == 0);
    CHECK(ds_has(env, &dsroot, backing));

    pc_open(&b, &root, "g", 0);
    ops[0] = op_putfh(&b.fh);
    ops[1] = op_read_bytes(&b.open_sid, 4096);
    p      = pc_compound(&b, ops, 2);
    CHECK(p.st[2] == NFS4_OK);
    CHECK(p.read_len == 4096);
    CHECK(p.read_byte0 == 0x41);
    pc_close(&b);

    p_env_stop(env);
} /* t_link_back */

/* B renames its own file over A's pNFS file, the last link to it.  A is
 * recalled, the replaced file's backing file stays until A returns its
 * layout, and is deleted after. */
static void
t_rename(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[4];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "victim", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    pc_open(&b, &root, "src", 1);
    pc_close(&b);

    ops[0] = op_putfh(&root);
    ops[1] = op_savefh();
    ops[2] = op_putfh(&root);
    ops[3] = op_rename("src", "victim");
    p      = pc_compound(&b, ops, 4);
    CHECK(p.status == NFS4_OK);

    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    p_pump(env, 20);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "tracked") == 1);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_rename */

/* Renaming a name onto itself replaces nothing: no recall, no deferred
 * delete, the backing file stays. */
static void
t_rename_self(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 ops[4];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "victim", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    ops[0] = op_putfh(&root);
    ops[1] = op_savefh();
    ops[2] = op_putfh(&root);
    ops[3] = op_rename("victim", "victim");
    p      = pc_compound(&b, ops, 4);
    CHECK(p.status == NFS4_OK);

    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "recalled") == 0);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    pc_close(&a);
    p_env_stop(env);
} /* t_rename_self */


/* ---- a recall that starts while a LAYOUTGET is between its VFS work and its
 * registration ------------------------------------------------------------- */

/* The hook runs on the server thread that is finishing a LAYOUTGET, once, and
 * starts a recall of the file exactly as a conflicting operation on another
 * thread would.  race_resumed counts the recall's completions: the recall
 * completes only when every holder it snapshotted has gone, and a holder it did
 * not snapshot keeps the file held. */
static _Atomic int race_armed;
static _Atomic int race_resumed;

static void
race_resume(void *arg)
{
    (void) arg;
    atomic_fetch_add(&race_resumed, 1);
} /* race_resume */

static void
race_hook(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    void                             *arg)
{
    (void) arg;
    if (atomic_exchange(&race_armed, 0)) {
        chimera_nfs_pnfs_test_recall(thread, fh, fh_len, race_resume, NULL);
    }
} /* race_hook */

static void
race_arm(void)
{
    atomic_store(&race_resumed, 0);
    atomic_store(&race_armed, 1);
    chimera_nfs_pnfs_test_hooks.layoutget_before_register = race_hook;
} /* race_arm */

static void
race_disarm(void)
{
    chimera_nfs_pnfs_test_hooks.layoutget_before_register = NULL;
} /* race_disarm */

/* Pump until the started recall has completed, or 5 s. */
static int
race_wait_resumed(struct mbt_env *env)
{
    int i;

    for (i = 0; i < 500 && atomic_load(&race_resumed) < 1; i++) {
        p_pump(env, 1);
    }
    return atomic_load(&race_resumed);
} /* race_wait_resumed */

/* A holds a layout.  B's LAYOUTGET has done its VFS work when a recall of the
 * file starts and snapshots A alone.  B must be refused, not registered: a
 * holder the recall never snapshotted is never recalled, never revoked, and
 * would keep the file held until its lease lapsed.  Once A returns, the recall
 * is complete. */
static void
t_register_race(void)
{
    struct mbt_env   *env  = p_env_start();
    struct mbt_fh     root = p_root(env);
    struct pc         a, b;
    struct nfs_argop4 ops[2];
    struct prep       p;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_open_layout(&a, &root, "f", 1);
    pc_open(&b, &root, "f", 0);

    race_arm();
    ops[0] = op_putfh(&b.fh);
    ops[1] = op_layoutget(&b.open_sid);
    p      = pc_compound(&b, ops, 2);
    race_disarm();
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4ERR_RECALLCONFLICT);

    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(race_wait_resumed(env) == 1);
    CHECK(b.nlayoutrecalls == 0);

    /* The refusal left nothing behind: B can take the layout afterwards. */
    pc_layoutget(&b);
    CHECK(pc_layoutreturn(&b).status == NFS4_OK);

    pc_close(&b);
    pc_close(&a);
    p_env_stop(env);
} /* t_register_race */

/* The same window for a client that already holds the layout: B's second
 * LAYOUTGET must not advance a layout stateid whose recall has just gone out.
 * Both holders are recalled and, once both have returned, the recall is
 * complete. */
static void
t_bump_race(void)
{
    struct mbt_env   *env  = p_env_start();
    struct mbt_fh     root = p_root(env);
    struct pc         a, b;
    struct nfs_argop4 ops[2];
    struct prep       p;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_open_layout(&a, &root, "f", 1);
    pc_open_layout(&b, &root, "f", 0);

    race_arm();
    ops[0] = op_putfh(&b.fh);
    ops[1] = op_layoutget(&b.layout_sid);
    p      = pc_compound(&b, ops, 2);
    race_disarm();
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4ERR_RECALLCONFLICT);

    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_pump_until_recalls(env, &b, 1, 1) == 1);
    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(pc_layoutreturn(&b).status == NFS4_OK);
    CHECK(race_wait_resumed(env) == 1);

    pc_close(&b);
    pc_close(&a);
    p_env_stop(env);
} /* t_bump_race */

/* A answers the first two CB_LAYOUTRECALLs with NFS4ERR_DELAY and the third
 * with OK, and returns its layout only then.  DELAY means ask again, not that
 * the client has nothing to return: the server asks exactly three times, A's
 * layout is not revoked while it is asked, and the delete waits for the
 * return. */
static void
t_recall_delay(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    a.recall_delays = 2;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 3) == 3);

    p_pump(env, 100);
    CHECK(a.nlayoutrecalls == 3);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(p_vfs_ev(env, "purged") == 0);

    CHECK(pc_layoutreturn(&a).st[2] == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(a.nlayoutrecalls == 3);

    p_env_stop(env);
} /* t_recall_delay */

/* A answers every CB_LAYOUTRECALL with NFS4ERR_DELAY and keeps renewing its
 * 3 s lease.  It is asked again and again, never revoked early, and loses the
 * layout alone at the two-lease deadline. */
static void
t_delay_deadline(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];
    int             i;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_delays = 1000000;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 3) == 3);
    CHECK(ds_has(env, &dsroot, backing));
    for (i = 0; i < 60 && ds_has(env, &dsroot, backing); i++) {
        CHECK(pc_renew(&a) == NFS4_OK);
        p_pump(env, 25);
    }
    CHECK(!ds_has(env, &dsroot, backing));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(a.nlayoutrecalls >= 3);
    CHECK(pc_renew(&a) == NFS4_OK);

    p_env_stop(env);
} /* t_delay_deadline */

/* A client holds a layout and has made a second session on the same
 * connection, so the connection is bound to the second session while the first
 * is the one carrying the recall.  It answers the recall with NFS4ERR_DELAY and
 * then drops the connection while the server's retry is pending.  The server
 * must forget the connection of every session that rode it, not only of the
 * one the connection was last bound to: libevpl frees the connection on the
 * drop, and the retry would send on it.  The holder is still revoked at the
 * deadline once the lease runs out. */
static void
t_delay_conn_drop(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 op;
    struct prep       p;
    char              backing[MBT_NAME_MAX];
    int               i;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_delays = 1000000;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    /* A second session for the same client on the same connection, not a
     * backchannel: the connection is now bound to it, not to the first. */
    memset(&op, 0, sizeof(op));
    op.argop                                = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid        = a.clientid;
    op.opcreate_session.csa_sequence        = ++a.cs_seq;
    op.opcreate_session.csa_flags           = 0;
    op.opcreate_session.csa_fore_chan_attrs = a.chan;
    op.opcreate_session.csa_back_chan_attrs = a.chan;
    op.opcreate_session.csa_cb_program      = 0x40000000;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = &p_cb_sec;
    p_send(env, a.conn, 1, &op, 1, &p);
    p_wait(env, &p);
    CHECK(p.status == NFS4_OK);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);

    /* The client goes away with the retry (250 ms) still pending. */
    evpl_rpc2_client_disconnect(env->rpc2_thread, a.conn);
    p_pump(env, 10);

    /* Past the retry: it must not send on the freed connection. */
    p_pump(env, 100);
    CHECK(a.nlayoutrecalls == 1);
    CHECK(ds_has(env, &dsroot, backing));

    /* B keeps its lease; A's lapses and the holder goes with it. */
    for (i = 0; i < 1000 && ds_has(env, &dsroot, backing); i++) {
        CHECK(pc_renew(&b) == NFS4_OK);
        p_pump(env, 5);
    }
    CHECK(!ds_has(env, &dsroot, backing));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_delay_conn_drop */

/* ---- the revised lifecycle: a layout is a reference, the data goes at the
 * last one ---------------------------------------------------------------- */

static struct prep
pc_layoutget_raw(
    struct pc             *pc,
    const struct stateid4 *sid)
{
    struct nfs_argop4 ops[2];

    ops[0] = op_putfh(&pc->fh);
    ops[1] = op_layoutget(sid);
    return pc_compound(pc, ops, 2);
} /* pc_layoutget_raw */

/* A has the file open with a layout; B removes it.  The file is still open, so
 * nothing is recalled, A and C (who opened the same file) can still get
 * layouts and do I/O.  Only when both have closed are they recalled. */
static void
t_layoutget_orphan(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b, c;
    struct nfs_argop4 ops[2];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_setup(&c, env, 3, 1);
    a.recall_answer = NFS4_OK;
    c.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    pc_open(&c, &root, "f", 0);
    CHECK(ds_backing(env, &dsroot, backing) == 1);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));

    p = pc_layoutget_raw(&a, &a.layout_sid);
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4_OK);
    a.layout_sid = p.layout_sid;
    pc_layoutget(&c);

    ops[0] = op_putfh(&a.fh);
    ops[1] = op_write_bytes(env->evpl, &a.open_sid, 4096);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[2] == NFS4_OK);
    ops[1] = op_read_bytes(&a.open_sid, 4096);
    p      = pc_compound(&a, ops, 2);
    CHECK(p.st[2] == NFS4_OK);
    CHECK(p.read_len == 4096);
    CHECK(a.nlayoutrecalls == 0 && c.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));

    pc_close(&a);
    p_pump(env, 30);
    CHECK(a.nlayoutrecalls == 0 && c.nlayoutrecalls == 0);
    pc_close(&c);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_pump_until_recalls(env, &c, 1, 1) == 1);
    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(pc_layoutreturn(&c).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));

    p_env_stop(env);
} /* t_layoutget_orphan */

/* Three clients hold layouts and none has the file open.  One removal recalls
 * each exactly once, and the data stays until the third has returned. */
static void
t_several_layouts(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       c[3], r;
    char            backing[MBT_NAME_MAX];
    int             i;

    for (i = 0; i < 3; i++) {
        pc_setup(&c[i], env, i, 1);
        c[i].recall_answer = NFS4_OK;
        pc_open_layout(&c[i], &root, "f", i == 0);
    }
    pc_setup(&r, env, 3, 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    for (i = 0; i < 3; i++) {
        pc_close(&c[i]);
    }

    CHECK(pc_remove(&r, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, c, 3, 3) == 3);
    p_pump(env, 30);
    for (i = 0; i < 3; i++) {
        CHECK(c[i].nlayoutrecalls == 1);
    }

    CHECK(pc_layoutreturn(&c[0]).status == NFS4_OK);
    CHECK(pc_layoutreturn(&c[1]).status == NFS4_OK);
    p_pump(env, 30);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(pc_layoutreturn(&c[2]).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(p_vfs_ev(env, "recalled") >= 1);

    p_env_stop(env);
} /* t_several_layouts */

/* The unlink arrives over NFSv3, which has no recall of its own: the layout
 * holder is recalled all the same and the backing file goes after the
 * return. */
static void
t_nfs3_remove(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct mbt_fh   v3root;
    struct pc       a;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    v3root = mbt_mnt(env, "/fs0")->obj_fh;
    CHECK(v3root.has);
    CHECK(mbt_remove(env, &v3root, "f", 1)->status == NFS3_OK);

    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    p_pump(env, 30);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_nfs3_remove */

/* A has only a layout; B removes the file and A is recalled.  Before A
 * returns, C opens the file by its handle: that succeeds.  C's LAYOUTGET is
 * refused until A has returned, then granted, since C keeps the file open. */
static void
t_open_during_recall(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b, c;
    struct nfs_argop4 ops[3];
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_setup(&c, env, 3, 1);
    a.recall_answer = NFS4_OK;
    c.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);

    c.fh   = a.fh;
    ops[0] = op_putfh(&c.fh);
    ops[1] = op_open_fh(&c);
    p      = pc_compound(&c, ops, 2);
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4_OK);
    c.open_sid = p.open_sid;

    p = pc_layoutget_raw(&c, &c.open_sid);
    CHECK(p.st[2] == NFS4ERR_RECALLCONFLICT);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(ds_has(env, &dsroot, backing));
    pc_layoutget(&c);
    CHECK(ds_has(env, &dsroot, backing));

    pc_close(&c);
    CHECK(p_pump_until_recalls(env, &c, 1, 1) == 1);
    CHECK(pc_layoutreturn(&c).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));

    p_env_stop(env);
} /* t_open_during_recall */

/* A has a layout and no open; a VFS handle held by the probe stands in for an
 * open from another protocol.  B removes the file: no recall while that handle
 * is held, the recall when it goes. */
static void
t_last_close_foreign(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    p_capture_on();
    pc_open_layout(&a, &root, "f", 1);
    p_capture_off();
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    p_hold(env);
    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    p_pump(env, 50);
    CHECK(a.nlayoutrecalls == 0);
    CHECK(ds_has(env, &dsroot, backing));

    p_unhold();
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    p_pump(env, 30);
    CHECK(ds_has(env, &dsroot, backing));
    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));

    p_env_stop(env);
} /* t_last_close_foreign */

/* D's layout is returned and D has closed.  A foreign handle keeps the file
 * unlinked but referenced.  A's LAYOUTGET (no open of its own) runs its VFS
 * work, and the foreign handle goes just before the layout would be taken:
 * LAYOUTGET's own open is then the last real reference, so the layout is
 * refused, not granted over a file about to be purged. */
static void
t_layoutget_last_strong(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b, d;
    struct stateid4 anon;
    struct prep     p;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_setup(&d, env, 3, 1);
    p_capture_on();
    pc_open_layout(&d, &root, "f", 1);
    p_capture_off();
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    CHECK(pc_layoutreturn(&d).status == NFS4_OK);
    pc_close(&d);

    p_hold(env);
    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);

    a.fh = d.fh;
    memset(&anon, 0, sizeof(anon));
    atomic_store(&p_hook_hit, 0);
    chimera_nfs_pnfs_test_hooks.layoutget_before_recallable = p_drop_hook;
    p                                                       = pc_layoutget_raw(&a, &anon);
    chimera_nfs_pnfs_test_hooks.layoutget_before_recallable = NULL;

    CHECK(atomic_load(&p_hook_hit) == 1);
    CHECK(p.st[1] == NFS4_OK);
    CHECK(p.st[2] == NFS4ERR_LAYOUTUNAVAILABLE);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(a.nlayoutrecalls == 0);

    p_env_stop(env);
} /* t_layoutget_last_strong */

/* After the last return, the old handle is gone: PUTFH answers NFS4ERR_STALE. */
static void
t_stale_after_purge(void)
{
    struct mbt_env   *env    = p_env_start();
    struct mbt_fh     root   = p_root(env);
    struct mbt_fh     dsroot = p_dsroot(env);
    struct pc         a, b;
    struct nfs_argop4 op;
    struct prep       p;
    char              backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    op = op_putfh(&a.fh);
    p  = pc_compound(&a, &op, 1);
    CHECK(p.st[1] == NFS4_OK);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_backing(env, &dsroot, backing, 0, 500));
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    op = op_putfh(&a.fh);
    p  = pc_compound(&a, &op, 1);
    CHECK(p.st[1] == NFS4ERR_STALE);

    p_env_stop(env);
} /* t_stale_after_purge */

static struct {
    int done;
    enum chimera_vfs_error status;
} p_umount;

static void
p_umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    p_umount.status = status;
    p_umount.done   = 1;
} /* p_umount_cb */

/* A holds a layout on a linked file that nothing has open, and the file's mount
 * is unmounted.  The layout is a reference, so the umount would wait for it:
 * the holder is recalled for the umount instead.  Once the mount is going its
 * handles no longer resolve, so A cannot return the layout by LAYOUTRETURN; it
 * answers the recall with NFS4ERR_NOMATCHING_LAYOUT, which drops the layout, and
 * the umount then completes. */
static void
t_umount_layout(void)
{
    struct mbt_env         *env  = p_env_start();
    struct mbt_fh           root = p_root(env);
    struct pc               a;
    struct chimera_vfs_cred cred;
    int                     i;

    pc_setup(&a, env, 1, 1);
    a.recall_answer = NFS4ERR_NOMATCHING_LAYOUT;
    pc_open_layout(&a, &root, "f", 1);
    pc_close(&a);

    p_vfs_thr = chimera_vfs_thread_init(env->evpl, chimera_server_get_vfs(env->server));
    CHECK(p_vfs_thr != NULL);
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);
    memset(&p_umount, 0, sizeof(p_umount));
    chimera_vfs_umount(p_vfs_thr, &cred, "/fs0", p_umount_cb, NULL);

    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_metric(env, "chimera_nfs4_layout_recall", "cause", "umount") == 1);

    for (i = 0; i < 500 && !p_umount.done; i++) {
        p_pump(env, 1);
    }
    CHECK(p_umount.done);
    CHECK(p_umount.status == CHIMERA_VFS_OK);

    p_env_stop(env);
} /* t_umount_layout */

/* ---- dispatch -------------------------------------------------------------- */

/* The backing file is already gone when the delete runs: that is not a leak.
 * The delete counts as completed, not failed. */
static void
t_backing_gone(void)
{
    struct mbt_env *env    = p_env_start();
    struct mbt_fh   root   = p_root(env);
    struct mbt_fh   dsroot = p_dsroot(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    CHECK(ds_backing(env, &dsroot, backing) == 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(ds_has(env, &dsroot, backing));

    mbt_remove(env, &dsroot, backing, strlen(backing));
    CHECK(!ds_has(env, &dsroot, backing));

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(p_vfs_ev(env, "purge_failed") == 0);

    p_env_stop(env);
} /* t_backing_gone */

/* The combined topology: the metadata server is its own data server, so the
 * backing root is the very mount the files live in.  A holds a layout on a file
 * nobody has open and B removes it: A must be recalled, and the backing file,
 * which sits beside the file in the same directory, goes only once A returns. */
static void
t_combined(void)
{
    struct mbt_env *env  = p_env_start_ds("/fs0");
    struct mbt_fh   root = p_root(env);
    struct pc       a, b;
    char            backing[MBT_NAME_MAX];

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    a.recall_answer = NFS4_OK;
    pc_open_layout(&a, &root, "f", 1);
    pc_close(&a);

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    CHECK(p_pump_until_recalls(env, &a, 1, 1) == 1);
    CHECK(p_vfs_ev(env, "tracked") == 1);

    CHECK(pc_layoutreturn(&a).status == NFS4_OK);
    CHECK(p_wait_vfs_ev(env, "purged", 1));
    CHECK(p_vfs_ev(env, "purge_failed") == 0);
    CHECK(ds_backing(env, &root, backing) == 0);

    p_env_stop(env);
} /* t_combined */

static _Atomic int p_recallable_hits;

static void
p_recallable_fn(
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    enum chimera_vfs_recall_reason reason,
    void                          *arg)
{
    (void) fh;
    (void) fh_len;
    (void) reason;
    (void) arg;
    atomic_fetch_add(&p_recallable_hits, 1);
} /* p_recallable_fn */

/* A holder whose layout is not a flex-files one, a block layout from a backend
 * that sources its own layouts, say, has no data server: pNFS is enabled with an
 * empty device table.  The file it holds a recallable reference on is removed by
 * another client, and the holder must still be asked to give it back. */
static void
t_recallable_no_ds(void)
{
    struct mbt_env               *env  = p_env_start_ds(NULL);
    struct mbt_fh                 root = p_root(env);
    struct pc                     a, b;
    struct chimera_vfs_recallable r;
    int                           i;

    pc_setup(&a, env, 1, 1);
    pc_setup(&b, env, 2, 1);
    pc_open(&a, &root, "f", 1);
    pc_close(&a);

    p_vfs_find(env, "f");
    p_hold(env);

    memset(&r, 0, sizeof(r));
    r.fn  = p_recallable_fn;
    r.arg = &r;
    CHECK(chimera_vfs_ref_make_recallable(p_vfs_thr, p_held, &r) == CHIMERA_VFS_OK);
    p_held = NULL;

    CHECK(pc_remove(&b, &root, "f") == NFS4_OK);
    for (i = 0; i < 500 && atomic_load(&p_recallable_hits) == 0; i++) {
        p_pump(env, 1);
    }
    CHECK(atomic_load(&p_recallable_hits) == 1);

    chimera_vfs_ref_release_recallable(p_vfs_thr, &r);
    CHECK(p_wait_vfs_ev(env, "purged", 1));

    p_env_stop(env);
} /* t_recallable_no_ds */

int
main(
    int    argc,
    char **argv)
{
    void (*run)(
        void) = NULL;

    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc != 2) {
        fprintf(stderr, "usage: %s <subtest>\n", argv[0]);
        return 2;
    }
    if (strcmp(argv[1], "name") == 0) {
        run = t_name;
    } else if (strcmp(argv[1], "fanout_setattr") == 0) {
        run = t_fanout_setattr;
    } else if (strcmp(argv[1], "deadline_setattr") == 0) {
        run = t_deadline_setattr;
    } else if (strcmp(argv[1], "deferred") == 0) {
        run = t_deferred;
    } else if (strcmp(argv[1], "revoke_nocb") == 0) {
        run = t_revoke_nocb;
    } else if (strcmp(argv[1], "revoke_decline") == 0) {
        run = t_revoke_decline;
    } else if (strcmp(argv[1], "fanout_remove") == 0) {
        run = t_fanout_remove;
    } else if (strcmp(argv[1], "shutdown_pending") == 0) {
        run = t_shutdown_pending;
    } else if (strcmp(argv[1], "deadline_remove") == 0) {
        run = t_deadline_remove;
    } else if (strcmp(argv[1], "last_close") == 0) {
        run = t_last_close;
    } else if (strcmp(argv[1], "putfh_after_close") == 0) {
        run = t_putfh_after_close;
    } else if (strcmp(argv[1], "other_link") == 0) {
        run = t_other_link;
    } else if (strcmp(argv[1], "lease") == 0) {
        run = t_lease;
    } else if (strcmp(argv[1], "link_back") == 0) {
        run = t_link_back;
    } else if (strcmp(argv[1], "rename") == 0) {
        run = t_rename;
    } else if (strcmp(argv[1], "rename_self") == 0) {
        run = t_rename_self;
    } else if (strcmp(argv[1], "backing_gone") == 0) {
        run = t_backing_gone;
    } else if (strcmp(argv[1], "register_race") == 0) {
        run = t_register_race;
    } else if (strcmp(argv[1], "bump_race") == 0) {
        run = t_bump_race;
    } else if (strcmp(argv[1], "recall_delay") == 0) {
        run = t_recall_delay;
    } else if (strcmp(argv[1], "delay_conn_drop") == 0) {
        run = t_delay_conn_drop;
    } else if (strcmp(argv[1], "delay_deadline") == 0) {
        run = t_delay_deadline;
    } else if (strcmp(argv[1], "layoutget_orphan") == 0) {
        run = t_layoutget_orphan;
    } else if (strcmp(argv[1], "several_layouts") == 0) {
        run = t_several_layouts;
    } else if (strcmp(argv[1], "nfs3_remove") == 0) {
        run = t_nfs3_remove;
    } else if (strcmp(argv[1], "open_during_recall") == 0) {
        run = t_open_during_recall;
    } else if (strcmp(argv[1], "last_close_foreign") == 0) {
        run = t_last_close_foreign;
    } else if (strcmp(argv[1], "layoutget_last_strong") == 0) {
        run = t_layoutget_last_strong;
    } else if (strcmp(argv[1], "stale_after_purge") == 0) {
        run = t_stale_after_purge;
    } else if (strcmp(argv[1], "umount_layout") == 0) {
        run = t_umount_layout;
    } else if (strcmp(argv[1], "combined") == 0) {
        run = t_combined;
    } else if (strcmp(argv[1], "recallable_no_ds") == 0) {
        run = t_recallable_no_ds;
    }
    if (run) {
        mbt_watchdog_arm(P_WATCHDOG_S);
        run();
        mbt_watchdog_disarm();
        printf("pNFS remove probe %s: PASS\n", argv[1]);
        return 0;
    }
    fprintf(stderr, "unknown subtest %s\n", argv[1]);
    return 2;
} /* main */
