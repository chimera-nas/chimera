/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 exclusive create: the attributes OPEN's attrset must name.
 *
 * An exclusive create has nowhere to keep its 8-byte verifier but the new
 * object itself, and this server keeps it in time_access and time_modify (the
 * Linux server's choice).  RFC 7530 16.16.5 and RFC 8881 18.16.3/18.16.4 make
 * the server say so in the OPEN reply's attrset, and the client then MUST send
 * a SETATTR that sets those attributes to real values.  A Linux client sends
 * that SETATTR only for the time attributes attrset names, so a server that
 * leaves them out keeps the verifier as the file's timestamps for good: every
 * O_CREAT|O_EXCL file dated 1970.
 *
 * Nothing else in this tree sends an exclusive OPEN.  chimera's own NFSv4
 * client creates with GUARDED4 only, so the pjd ports over NFS never reach this
 * path, and nothing asserted attrset at all.
 *
 * Each cell sends one creating OPEN and measures its status and attrset:
 *   - EXCLUSIVE4 and EXCLUSIVE4_1 creates name time_access and time_modify
 *     (or their _set forms, which a client treats the same way);
 *   - so does a retry with the same verifier -- the request a client resends
 *     when the first reply was lost, which must read like that reply;
 *   - EXCLUSIVE4_1 also names the cva_attrs it applied (mode here): a client
 *     takes attrset minus cva_attrs as the verifier's attributes;
 *   - a retry with another verifier is NFS4ERR_EXIST;
 *   - an UNCHECKED4 create names no time attribute, or the client would set
 *     times it never asked to set;
 *   - at NFSv4.0, an exclusive OPEN resent with the same seqid on a new
 *     connection -- a lost reply after a reconnect -- is answered from the
 *     open-owner's replay record, and that answer names them too.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define W1(attr) (1U << ((attr) - 32))

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
    uint32_t               minor;
};

/* ---- one compound's parsed reply ------------------------------------------ */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];          /* per-op status */
    struct mbt_fh   fh;              /* last GETFH */
    struct stateid4 open_sid;        /* last OPEN stateid */
    int             open_idx;        /* index of the OPEN in st[] */
    uint32_t        attrset[2];      /* OPEN attrset words 0-1 */
    uint32_t        num_attrset;
    uint64_t        clientid;        /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];   /* CREATE_SESSION */
    uint8_t         confirm[8];      /* SETCLIENTID */
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
    uint32_t     i, w;

    (void) evpl;
    (void) verf;
    p->rpc_err  = status;
    p->open_idx = -1;
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
                        const struct OPEN4resok *ok = &r->opopen.resok4;

                        p->open_sid    = ok->stateid;
                        p->num_attrset = ok->num_attrset;
                        for (w = 0; w < 2; w++) {
                            p->attrset[w] = w < ok->num_attrset ?
                                ok->attrset[w] : 0;
                        }
                    }
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_OPEN_CONFIRM:
                    p->st[i] = r->opopen_confirm.status;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen_confirm.resok4.open_stateid;
                    }
                    break;
                case OP_SETCLIENTID:
                    p->st[i] = r->opsetclientid.status;
                    if (p->st[i] == NFS4_OK) {
                        p->clientid = r->opsetclientid.resok4.clientid;
                        memcpy(p->confirm,
                               r->opsetclientid.resok4.setclientid_confirm, 8);
                    }
                    break;
                case OP_SETCLIENTID_CONFIRM:
                    p->st[i] = r->opsetclientid_confirm.status;
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
    args.minorversion = pc->minor;
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

/* A fresh connection to the server, as a client opens after a reconnect. */
static struct evpl_rpc2_conn *
pc_connect(struct pc *pc)
{
    struct evpl_rpc2_program *cb_programs[1];
    struct evpl_endpoint     *ep;

    cb_programs[0] = &pc->env->nfs_v4_cb.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    return evpl_rpc2_client_connect(pc->env->rpc2_thread, EVPL_STREAM_INPROC,
                                    ep, cb_programs, 1, pc);
} /* pc_connect */

static void
pc_setup(
    struct pc      *pc,
    struct mbt_env *env)
{
    struct nfs_argop4                 op;
    struct prep                       p;
    static char                       owner[] = "excl-create-probe";
    struct channel_attrs4             chan    = {
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
    pc->env   = env;
    pc->minor = 1;
    pc->conn  = pc_connect(pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
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
    op.opcreate_session.csa_flags           = 0;
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

/* An NFSv4.0 client: SETCLIENTID and SETCLIENTID_CONFIRM, no session. */
static void
pc40_setup(
    struct pc      *pc,
    struct mbt_env *env)
{
    struct nfs_argop4 op;
    struct prep       p;
    static char       owner[] = "excl-create-probe-40";

    memset(pc, 0, sizeof(*pc));
    pc->env   = env;
    pc->minor = 0;
    pc->conn  = pc_connect(pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_SETCLIENTID;
    memset(op.opsetclientid.client.verifier, 2, 8);
    op.opsetclientid.client.id.data                      = owner;
    op.opsetclientid.client.id.len                       = (uint32_t) strlen(owner);
    op.opsetclientid.callback.cb_program                 = 0x40000000;
    op.opsetclientid.callback.cb_location.na_r_netid.str = "tcp";
    op.opsetclientid.callback.cb_location.na_r_netid.len = 3;
    op.opsetclientid.callback.cb_location.na_r_addr.str  = "0.0.0.0.0.0";
    op.opsetclientid.callback.cb_location.na_r_addr.len  = 11;
    op.opsetclientid.callback_ident                      = 1;
    p                                                    = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "SETCLIENTID failed: %u\n", p.status);
        exit(2);
    }
    pc->clientid = p.clientid;

    memset(&op, 0, sizeof(op));
    op.argop                          = OP_SETCLIENTID_CONFIRM;
    op.opsetclientid_confirm.clientid = pc->clientid;
    memcpy(op.opsetclientid_confirm.setclientid_confirm, p.confirm, 8);
    p = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "SETCLIENTID_CONFIRM failed: %u\n", p.status);
        exit(2);
    }
} /* pc40_setup */

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

/* MODE 0644 create attributes.  The bitmap and value blob are static because
 * the argument structure only borrows them, and the compound is sent before
 * the next call overwrites them. */
static void
mode_attrs(struct fattr4 *f)
{
    static uint32_t bitmap[2];
    static uint8_t  blob[4];

    bitmap[0] = 0;
    bitmap[1] = W1(FATTR4_MODE);
    pack_be32(blob, 0644);

    f->num_attrmask   = 2;
    f->attrmask       = bitmap;
    f->attr_vals.data = blob;
    f->attr_vals.len  = 4;
} /* mode_attrs */

enum how {
    H_EXCL4,
    H_EXCL41,
    H_UNCHECKED,
};

/* Creating OPEN(CLAIM_NULL) for read and write.  EXCLUSIVE4 carries only the
 * verifier; EXCLUSIVE4_1 the verifier and a MODE 0644 cva_attrs; UNCHECKED4 a
 * MODE 0644 createattrs. */
static struct nfs_argop4
op_open_create(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    enum how    how,
    uint8_t     verf_byte)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = OPEN4_SHARE_ACCESS_BOTH;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);
    a.opopen.openhow.opentype = OPEN4_CREATE;
    switch (how) {
        case H_EXCL4:
            a.opopen.openhow.how.mode = EXCLUSIVE4;
            memset(a.opopen.openhow.how.createverf, verf_byte,
                   NFS4_VERIFIER_SIZE);
            break;
        case H_EXCL41:
            a.opopen.openhow.how.mode = EXCLUSIVE4_1;
            memset(a.opopen.openhow.how.ch_createboth.cva_verf, verf_byte,
                   NFS4_VERIFIER_SIZE);
            mode_attrs(&a.opopen.openhow.how.ch_createboth.cva_attrs);
            break;
        default:
            a.opopen.openhow.how.mode = UNCHECKED4;
            mode_attrs(&a.opopen.openhow.how.createattrs);
            break;
    } /* switch */
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open_create */

/* ---- the cells ------------------------------------------------------------ */

enum tri {
    T_ANY,
    T_NO,
    T_YES,
};

/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
static const struct {
    const char *label;
    enum how    how;
    const char *name;
    uint8_t     verf;
    uint32_t    expect;
    enum tri    times;   /* time_access and time_modify (or the _set forms) */
    enum tri    mode;    /* FATTR4_MODE */
} cells[] = {
    { "excl4-create",                H_EXCL4,     "excl4.bin",  0x11, NFS4_OK,       T_YES, T_NO  },
    { "excl4-retry-same-verifier",   H_EXCL4,     "excl4.bin",  0x11, NFS4_OK,       T_YES, T_NO  },
    { "excl4-retry-other-verifier",  H_EXCL4,     "excl4.bin",  0x22, NFS4ERR_EXIST, T_ANY, T_ANY },
    { "excl41-create",               H_EXCL41,    "excl41.bin", 0x33, NFS4_OK,       T_YES, T_YES },
    { "excl41-retry-same-verifier",  H_EXCL41,    "excl41.bin", 0x33, NFS4_OK,       T_YES, T_YES },
    { "excl41-retry-other-verifier", H_EXCL41,    "excl41.bin", 0x44, NFS4ERR_EXIST, T_ANY, T_ANY },
    { "unchecked-create-no-times",   H_UNCHECKED, "plain.bin",  0,    NFS4_OK,       T_NO,  T_ANY },
};
/* *INDENT-ON* */

static const char *
how_name(enum how how)
{
    switch (how) {
        case H_EXCL4:
            return "EXCLUSIVE4";
        case H_EXCL41:
            return "EXCLUSIVE4_1";
        default:
            return "UNCHECKED4";
    } /* switch */
} /* how_name */

static void
run_cell(
    struct pc           *pc,
    const struct mbt_fh *root,
    unsigned int         i)
{
    const char       *label = cells[i].label;
    char              buf[400];
    char              owner[40];
    struct nfs_argop4 ops[3];
    struct prep       p;
    uint32_t          w1;
    int               atime, mtime, any_time, mode;

    snprintf(owner, sizeof(owner), "excl-%u", i);

    ops[0] = op_putfh(root);
    ops[1] = op_open_create(pc->clientid, owner, cells[i].name, cells[i].how,
                            cells[i].verf);
    ops[2] = op_getfh();
    p      = pc_compound(pc, ops, 3, 1);

    if (p.open_idx < 0) {
        snprintf(buf, sizeof(buf), "compound returned %u without reaching OPEN",
                 p.status);
        expect(label, 0, buf);
        return;
    }
    snprintf(buf, sizeof(buf), "OPEN(%s) %s verifier 0x%02x: expected %u, got %u",
             how_name(cells[i].how), cells[i].name, cells[i].verf,
             cells[i].expect, p.st[p.open_idx]);
    expect(label, p.st[p.open_idx] == cells[i].expect, buf);
    if (p.st[p.open_idx] != NFS4_OK) {
        return;
    }

    w1       = p.attrset[1];
    atime    = (w1 & (W1(FATTR4_TIME_ACCESS) | W1(FATTR4_TIME_ACCESS_SET))) != 0;
    mtime    = (w1 & (W1(FATTR4_TIME_MODIFY) | W1(FATTR4_TIME_MODIFY_SET))) != 0;
    any_time = (w1 & (W1(FATTR4_TIME_ACCESS) | W1(FATTR4_TIME_ACCESS_SET) |
                      W1(FATTR4_TIME_MODIFY) | W1(FATTR4_TIME_MODIFY_SET))) != 0;
    mode = (w1 & W1(FATTR4_MODE)) != 0;

    if (cells[i].times != T_ANY) {
        snprintf(buf, sizeof(buf),
                 "attrset times: expected %s, got time_access %d time_modify %d "
                 "(%u words, word0 0x%08x word1 0x%08x)",
                 cells[i].times == T_YES ? "both" : "neither", atime, mtime,
                 p.num_attrset, p.attrset[0], p.attrset[1]);
        expect(label, cells[i].times == T_YES ? (atime && mtime) : !any_time,
               buf);
    }
    if (cells[i].mode != T_ANY) {
        snprintf(buf, sizeof(buf), "attrset mode: expected %d, got %d",
                 cells[i].mode == T_YES, mode);
        expect(label, mode == (cells[i].mode == T_YES), buf);
    }

    if (p.fh.has) {
        ops[0] = op_putfh(&p.fh);
        ops[1] = op_close(&p.open_sid);
        p      = pc_compound(pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "CLOSE: expected %u, got %u", NFS4_OK,
                     p.status);
            expect(label, 0, buf);
        }
    }
} /* run_cell */

static int
attrset_names_both_times(const struct prep *p)
{
    uint32_t w1 = p->attrset[1];

    return (w1 & (W1(FATTR4_TIME_ACCESS) | W1(FATTR4_TIME_ACCESS_SET))) &&
           (w1 & (W1(FATTR4_TIME_MODIFY) | W1(FATTR4_TIME_MODIFY_SET)));
} /* attrset_names_both_times */

/* NFSv4.0 lost reply after a reconnect.  A 4.0 client whose OPEN reply never
 * arrived resends the same OPEN with the same seqid.  On the same connection
 * the v4.0 reply cache answers it byte for byte; on a new connection -- the
 * usual way a 4.0 reply gets lost -- the server answers it from the
 * open-owner's replay record, and that answer must name the verifier
 * attributes as the lost one did, or the client never resets them. */
static void
run_v40_resend_cell(
    struct mbt_env      *env,
    const struct mbt_fh *root)
{
    static const char *label   = "excl4-v40-resend-new-connection";
    static char        owner[] = "excl40-owner";
    struct pc          pc, again;
    struct nfs_argop4  ops[3];
    struct prep        p;
    struct mbt_fh      fh;
    struct stateid4    sid;
    char               buf[400];

    pc40_setup(&pc, env);

    ops[0] = op_putfh(root);
    ops[1] = op_open_create(pc.clientid, owner, "excl40.bin",
                            H_EXCL4, 0x55);
    ops[1].opopen.seqid = 1;
    ops[2]              = op_getfh();
    p                   = pc_compound(&pc, ops, 3, 0);
    if (p.open_idx < 0 || p.st[p.open_idx] != NFS4_OK || !p.fh.has) {
        snprintf(buf, sizeof(buf), "OPEN(EXCLUSIVE4) excl40.bin: expected %u, "
                 "got %u", NFS4_OK, p.open_idx < 0 ? p.status :
                 p.st[p.open_idx]);
        expect(label, 0, buf);
        return;
    }
    fh  = p.fh;
    sid = p.open_sid;
    snprintf(buf, sizeof(buf), "first reply attrset word1 0x%08x names both "
             "times", p.attrset[1]);
    expect(label, attrset_names_both_times(&p), buf);

    /* The same OPEN, same seqid, from a new connection. */
    again      = pc;
    again.conn = pc_connect(&again);
    p          = pc_compound(&again, ops, 2, 0);
    snprintf(buf, sizeof(buf), "resent OPEN: expected %u, got %u", NFS4_OK,
             p.open_idx < 0 ? p.status : p.st[p.open_idx]);
    expect(label, p.open_idx >= 0 && p.st[p.open_idx] == NFS4_OK, buf);
    if (p.open_idx >= 0 && p.st[p.open_idx] == NFS4_OK) {
        snprintf(buf, sizeof(buf), "resent reply attrset word1 0x%08x names "
                 "both times", p.attrset[1]);
        expect(label, attrset_names_both_times(&p), buf);
    }

    /* Confirm and close the open, from the first connection. */
    ops[0] = op_putfh(&fh);
    memset(&ops[1], 0, sizeof(ops[1]));
    ops[1].argop                       = OP_OPEN_CONFIRM;
    ops[1].opopen_confirm.open_stateid = sid;
    ops[1].opopen_confirm.seqid        = 2;
    p                                  = pc_compound(&pc, ops, 2, 0);
    if (p.status == NFS4_OK) {
        ops[1]               = op_close(&p.open_sid);
        ops[1].opclose.seqid = 3;
        pc_compound(&pc, ops, 2, 0);
    }
} /* run_v40_resend_cell */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts opts = {
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

    /* Usage: nfs4_excl_create_probe [<module>] */
    opts.module = argc > 1 ? argv[1] : "memfs";
    mbt_env_start_opts(env, &opts);

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

    printf("# --- NFSv4 exclusive create attrset (%s) ---\n", opts.module);

    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
        run_cell(&pc, &root, i);
    }
    run_v40_resend_cell(env, &root);

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 exclusive create check(s) FAILED\n", failures);
        return 1;
    }
    printf("all NFSv4 exclusive create checks passed\n");
    return 0;
} /* main */
