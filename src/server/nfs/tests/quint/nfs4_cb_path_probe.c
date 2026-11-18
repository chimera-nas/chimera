// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground-truth probe for the health of an NFSv4.1 client's callback path,
 * against the in-process server, where the harness answers callbacks inline on
 * whichever connection carries them.  A delegation is only safe to grant while
 * the server can recall it, so each check is about the two things that follow
 * from the path's state: whether a delegation is granted, and whether SEQUENCE
 * tells the client its path is down (SEQ4_STATUS_CB_PATH_DOWN, RFC 8881
 * Section 18.46.3) so that it re-establishes one.
 *
 *   L1  the connection carrying a session's only backchannel closes: the
 *       client's next SEQUENCE reports CB_PATH_DOWN and no delegation is
 *       granted until it binds a backchannel again.
 *   R1  BIND_CONN_TO_SESSION(BACK_OR_BOTH) on a fresh connection, the way a
 *       client answers L1: the path comes back up on that connection, the
 *       recall a conflict raised while it was down is sent, SEQUENCE stops
 *       reporting it down, delegations are granted again, and a recall
 *       reaches the client over the new connection.
 *   N1  a session created without CONN_BACK_CHAN, whose client then binds a
 *       backchannel with BIND_CONN_TO_SESSION: a 4.1 path, so delegations are
 *       granted, and recalls are delivered with the AUTH_SYS credentials the
 *       session's CREATE_SESSION asked for.
 *   B1  BACKCHANNEL_CTL on the backchannel connection, with a new program and
 *       credentials: the next recall goes to the new program with the new
 *       credentials, and so does one after the client binds its backchannel
 *       to a fresh connection.
 *
 * Each grant check opens twice; at 4.1 the first OPEN is granted already
 * (nfs4_deleg_probe.c, K1), and the second opens the file the check is about.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define P_DELAY      10008
#define P_MAX_RECALL 16
#define P_PROG_A     0x40000000u
#define P_PROG_B     0x40000001u

static int failures = 0;

static void
expect(
    const char *label,
    int         ok,
    const char *fmt,
    ...)
{
    va_list ap;

    printf("  %s %s: ", ok ? "ok  " : "FAIL", label);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    if (!ok) {
        failures++;
    }
} /* expect */

/* ---- one 4.1 client ------------------------------------------------------- */

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
    uint8_t                recalls[P_MAX_RECALL][12];
    uint32_t               recall_prog[P_MAX_RECALL];
    uint32_t               recall_flavor[P_MAX_RECALL];
    uint32_t               recall_uid[P_MAX_RECALL];
    int                    nrecalls;
};

/* A second callback program, P_PROG_B, served on every client connection
 * beside the harness's own (P_PROG_A), so a check can tell which program a
 * recall was sent to. */
static struct NFS_V4_CB cb_prog_b;

/* The client's callback service: answer CB_SEQUENCE and record CB_RECALL,
 * with the program it came to and the credentials it carried. */
static void
pc_cb_serve(
    struct evpl               *evpl,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    struct pc                 *pc,
    uint32_t                   prog)
{
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4  *resarray;
    uint32_t               i;
    int                    rc;

    memset(&res, 0, sizeof(res));
    resarray = xdr_dbuf_alloc_space(
        sizeof(*resarray) * (args->num_argarray ? args->num_argarray : 1),
        encoding->dbuf);
    if (!resarray) {
        res.status = NFS4ERR_RESOURCE;
        rc         = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                               encoding);
        (void) rc;
        return;
    }

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
            case OP_CB_RECALL:
                if (pc->nrecalls < P_MAX_RECALL) {
                    memcpy(pc->recalls[pc->nrecalls],
                           argop->opcbrecall.stateid.other, 12);
                    pc->recall_prog[pc->nrecalls]   = prog;
                    pc->recall_flavor[pc->nrecalls] = cred->flavor;
                    pc->recall_uid[pc->nrecalls]    =
                        cred->flavor == AUTH_SYS ? cred->authsys.uid : 0;
                    pc->nrecalls++;
                }
                resop->opcbrecall.status = NFS4_OK;
                break;
            default:
                resop->opcbrecall.status = NFS4ERR_NOTSUPP;
                break;
        } /* switch */
    }
    res.status       = NFS4_OK;
    res.num_resarray = args->num_argarray;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                                 encoding);
    (void) rc;
} /* pc_cb_serve */

static void
pc_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    (void) conn;
    pc_cb_serve(evpl, cred, args, encoding, private_data, P_PROG_A);
} /* pc_cb_compound */

static void
pc_cb_compound_b(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    (void) conn;
    pc_cb_serve(evpl, cred, args, encoding, private_data, P_PROG_B);
} /* pc_cb_compound_b */

/* Everything a check reads out of one COMPOUND's reply. */
struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[8];
    uint32_t        seq_flags;        /* SEQUENCE sr_status_flags */
    struct mbt_fh   fh;
    struct stateid4 open_sid;
    uint32_t        deleg_type;
    struct stateid4 deleg_sid;
    uint64_t        clientid;
    uint32_t        eir_seq;
    uint8_t         sessionid[16];
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
        p->nres   = (int) reply->num_resarray;
        for (i = 0; i < reply->num_resarray && i < 8; i++) {
            const struct nfs_resop4 *r = &reply->resarray[i];

            switch (r->resop) {
                case OP_SEQUENCE:
                    p->st[i] = r->opsequence.sr_status;
                    if (p->st[i] == NFS4_OK) {
                        p->seq_flags = r->opsequence.sr_resok4.sr_status_flags;
                    }
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
                case OP_BIND_CONN_TO_SESSION:
                    p->st[i] = r->opbind_conn_to_session.bctsr_status;
                    break;
                case OP_BACKCHANNEL_CTL:
                    p->st[i] = r->opbackchannel_ctl.bcr_status;
                    break;
                case OP_EXCHANGE_ID:
                    p->st[i] = r->opexchange_id.eir_status;
                    if (p->st[i] == NFS4_OK) {
                        p->clientid = r->opexchange_id.eir_resok4.eir_clientid;
                        p->eir_seq  =
                            r->opexchange_id.eir_resok4.eir_sequenceid;
                    }
                    break;
                case OP_CREATE_SESSION:
                    p->st[i] = r->opcreate_session.csr_status;
                    if (p->st[i] == NFS4_OK) {
                        memcpy(p->sessionid,
                               r->opcreate_session.csr_resok4.csr_sessionid,
                               16);
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

/* One COMPOUND on the client's current connection, preceded by SEQUENCE on
 * its session when with_seq is set. */
static struct prep
pc_compound(
    struct pc         *pc,
    struct nfs_argop4 *ops,
    int                nops,
    int                with_seq)
{
    struct nfs_argop4    argarray[9];
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
        argarray[0].opsequence.sa_sequenceid = ++pc->slot_seq;
        base                                 = 1;
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

/* A connection whose client side serves this client's callback program, so
 * the server may run the backchannel over it. */
static struct evpl_rpc2_conn *
pc_connect(struct pc *pc)
{
    struct evpl_rpc2_program *cb_programs[2];
    struct evpl_endpoint     *ep;
    struct evpl_rpc2_conn    *conn;

    cb_programs[0] = &pc->env->nfs_v4_cb.rpc2;
    cb_programs[1] = &cb_prog_b.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    conn = evpl_rpc2_client_connect(pc->env->rpc2_thread, EVPL_STREAM_INPROC,
                                    ep, cb_programs, 2, pc);
    if (!conn) {
        fprintf(stderr, "cannot open a connection\n");
        exit(2);
    }
    return conn;
} /* pc_connect */

/* EXCHANGE_ID, CREATE_SESSION (with or without the creating connection as its
 * backchannel; the callback program P_PROG_A and the callback security are
 * offered either way, AUTH_NONE unless sec is given) and RECLAIM_COMPLETE. */
static void
pc_setup(
    struct pc                        *pc,
    struct mbt_env                   *env,
    const char                       *owner,
    int                               back_chan,
    const struct callback_sec_parms4 *sec)
{
    struct nfs_argop4                 op;
    struct prep                       p;
    struct channel_attrs4             chan = {
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
    };
    static struct callback_sec_parms4 sec_none = { .cb_secflavor = AUTH_NONE };

    if (!sec) {
        sec = &sec_none;
    }
    memset(pc, 0, sizeof(*pc));
    pc->env  = env;
    pc->conn = pc_connect(pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    memset(op.opexchange_id.eia_clientowner.co_verifier, 0x3c, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;
    p                                                = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "EXCHANGE_ID failed: %u\n", p.status);
        exit(2);
    }
    pc->clientid = p.clientid;

    memset(&op, 0, sizeof(op));
    op.argop                         = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid = pc->clientid;
    op.opcreate_session.csa_sequence = p.eir_seq;
    op.opcreate_session.csa_flags    =
        back_chan ? CREATE_SESSION4_FLAG_CONN_BACK_CHAN : 0;
    op.opcreate_session.csa_fore_chan_attrs = chan;
    op.opcreate_session.csa_back_chan_attrs = chan;
    op.opcreate_session.csa_cb_program      = P_PROG_A;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = (struct callback_sec_parms4 *) sec;
    p                                       = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", p.status);
        exit(2);
    }
    memcpy(pc->sessionid, p.sessionid, 16);

    memset(&op, 0, sizeof(op));
    op.argop = OP_RECLAIM_COMPLETE;
    p        = pc_compound(pc, &op, 1, 1);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "RECLAIM_COMPLETE failed: %u\n", p.status);
        exit(2);
    }
} /* pc_setup */

static uint32_t
pc_bind(
    struct pc               *pc,
    channel_dir_from_client4 dir)
{
    struct nfs_argop4 op;
    struct prep       p;

    memset(&op, 0, sizeof(op));
    op.argop = OP_BIND_CONN_TO_SESSION;
    memcpy(op.opbind_conn_to_session.bctsa_sessid, pc->sessionid, 16);
    op.opbind_conn_to_session.bctsa_dir = dir;
    p                                   = pc_compound(pc, &op, 1, 0);
    return p.nres >= 1 ? p.st[0] : p.status;
} /* pc_bind */

/* The flags of a bare SEQUENCE, which must succeed. */
static uint32_t
pc_seq_flags(struct pc *pc)
{
    struct prep p = pc_compound(pc, NULL, 0, 1);

    if (p.nres < 1 || p.st[0] != NFS4_OK) {
        fprintf(stderr, "SEQUENCE failed: %u\n", p.status);
        exit(2);
    }
    return p.seq_flags;
} /* pc_seq_flags */

/* BACKCHANNEL_CTL(program, sec), after SEQUENCE, on the current connection. */
static uint32_t
pc_backchannel_ctl(
    struct pc                        *pc,
    uint32_t                          program,
    const struct callback_sec_parms4 *sec)
{
    struct nfs_argop4 op;
    struct prep       p;

    memset(&op, 0, sizeof(op));
    op.argop                               = OP_BACKCHANNEL_CTL;
    op.opbackchannel_ctl.bca_cb_program    = program;
    op.opbackchannel_ctl.num_bca_sec_parms = 1;
    op.opbackchannel_ctl.bca_sec_parms     = (struct callback_sec_parms4 *) sec;
    p                                      = pc_compound(pc, &op, 1, 1);
    return p.status;
} /* pc_backchannel_ctl */

/* ---- ops ------------------------------------------------------------------ */

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

/* OPEN(name) for read and write, creating it, under PUTFH(root). */
static struct prep
do_open(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *owner,
    const char          *name)
{
    struct nfs_argop4 ops[2];

    ops[0] = op_putfh(root);
    memset(&ops[1], 0, sizeof(ops[1]));
    ops[1].argop                   = OP_OPEN;
    ops[1].opopen.share_access     = 3;
    ops[1].opopen.owner.clientid   = pc->clientid;
    ops[1].opopen.owner.owner.data = (void *) owner;
    ops[1].opopen.owner.owner.len  = (uint32_t) strlen(owner);
    ops[1].opopen.openhow.opentype = OPEN4_CREATE;
    ops[1].opopen.openhow.how.mode = UNCHECKED4;
    ops[1].opopen.claim.claim      = CLAIM_NULL;
    ops[1].opopen.claim.file.data  = (void *) name;
    ops[1].opopen.claim.file.len   = (uint32_t) strlen(name);
    return pc_compound(pc, ops, 2, 1);
} /* do_open */

/* Whether this client is granted a delegation now: two eligible OPENs of a
 * fresh file (the first may only kick the path probe).  Returns the second
 * OPEN's delegation type, and its stateid in *sid. */
static uint32_t
deleg_granted(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *owner,
    const char          *name,
    struct stateid4     *sid)
{
    char        kick[64];
    struct prep p;

    snprintf(kick, sizeof(kick), "%s-kick", name);
    (void) do_open(pc, root, owner, kick);
    p = do_open(pc, root, owner, name);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "OPEN %s failed: %u\n", name, p.status);
        exit(2);
    }
    if (sid) {
        *sid = p.deleg_sid;
    }
    return p.deleg_type;
} /* deleg_granted */

/* The index of the first recall of sid this client has seen, or -1. */
static int
recall_index(
    struct pc             *pc,
    const struct stateid4 *sid)
{
    int i;

    for (i = 0; i < pc->nrecalls; i++) {
        if (memcmp(pc->recalls[i], sid->other, 12) == 0) {
            return i;
        }
    }
    return -1;
} /* recall_index */

static int
recall_seen(
    struct pc             *pc,
    const struct stateid4 *sid)
{
    return recall_index(pc, sid) >= 0;
} /* recall_seen */

/* Another client opens `name` for write: the conflict that recalls a
 * delegation.  Pumps until `holder` has seen the recall (or gives up), then
 * returns whether it did. */
static int
conflict_recalls(
    struct pc             *other,
    struct pc             *holder,
    const struct mbt_fh   *root,
    const char            *name,
    const struct stateid4 *sid)
{
    struct prep p;
    int         i;

    p = do_open(other, root, "conflict-oo", name);
    for (i = 0; i < 200 && !recall_seen(holder, sid); i++) {
        evpl_continue(other->env->evpl);
    }
    (void) p;
    return recall_seen(holder, sid);
} /* conflict_recalls */

/* Callback security asking for AUTH_SYS with the given uid and gid. */
static struct callback_sec_parms4
sec_authsys(uint32_t id)
{
    struct callback_sec_parms4 sec;

    memset(&sec, 0, sizeof(sec));
    sec.cb_secflavor      = AUTH_SYS;
    sec.cbsp_sys_cred.uid = id;
    sec.cbsp_sys_cred.gid = id;
    return sec;
} /* sec_authsys */

static const char *
dt_name(uint32_t t)
{
    return t == 0 ? "none" : t == 1 ? "read" : t == 2 ? "write" : "?";
} /* dt_name */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts        opts = {
        .nfs4_delegations = 1,
        .disable_caches   = 1,
    };
    struct mbt_env            *env = malloc(sizeof(*env));
    struct pc                  a, c, d, other;
    struct mbt_fh              root;
    struct stateid4            sid, l0_sid;
    uint32_t                   dt, flags, st;
    int                        ri;
    struct callback_sec_parms4 sec_c = sec_authsys(4242);
    struct callback_sec_parms4 sec_d = sec_authsys(4243);

    opts.sec = mbt_sec_scan_argv(argc, argv);
    if (opts.sec != MBT_SEC_SYS) {
        printf("AUTH_SYS only; skipping\n");
        free(env);
        return 77;
    }

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = pc_cb_compound;
    cb_prog_b                            = env->nfs_v4_cb;
    cb_prog_b.rpc2.program               = P_PROG_B;
    cb_prog_b.rpc2.program_data          = &cb_prog_b;
    cb_prog_b.recv_call_CB_COMPOUND      = pc_cb_compound_b;

    /* The export root, from a 4.0 COMPOUND on the harness's own connection. */
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
        args.argarray     = ops;
        args.num_argarray = 3;
        memset(&rp, 0, sizeof(rp));
        env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl,
                                                env->nfs_conn, &env->cred,
                                                &args, 0, 0, NULL, 0, 0,
                                                prep_cb, &rp);
        while (!rp.done) {
            evpl_continue(env->evpl);
        }
        if (rp.status != NFS4_OK || !rp.fh.has) {
            fprintf(stderr, "cannot resolve export root: %u\n", rp.status);
            return 2;
        }
        root = rp.fh;
    }

    pc_setup(&other, env, "cbp-other", 1, NULL);

    /* ---- L1 + R1 ---------------------------------------------------------- */
    printf("the connection carrying the backchannel closes, then a rebind\n");
    pc_setup(&a, env, "cbp-a", 1, NULL);
    dt = deleg_granted(&a, &root, "a-oo", "l0", &l0_sid);
    expect("L0", dt == OPEN_DELEGATE_WRITE,
           "a delegation while the backchannel is up: %s", dt_name(dt));

    /* The client loses the connection that carried both channels and carries
     * on over a new one, bound for the fore channel only. */
    {
        struct evpl_rpc2_conn *old = a.conn;

        a.conn = pc_connect(&a);
        st     = pc_bind(&a, CDFC4_FORE);
        expect("L1", st == NFS4_OK, "BIND_CONN_TO_SESSION(FORE): %u", st);
        evpl_rpc2_client_disconnect(env->rpc2_thread, old);
    }

    /* The server sees the disconnect on its own thread, in its own time: poll
     * for the report, for up to about two seconds. */
    for (int i = 0; i < 200; i++) {
        flags = pc_seq_flags(&a);
        if (flags & SEQ4_STATUS_CB_PATH_DOWN) {
            break;
        }
        usleep(10000);
    }
    expect("L1", (flags & SEQ4_STATUS_CB_PATH_DOWN) != 0,
           "SEQUENCE reports CB_PATH_DOWN once the backchannel is gone: "
           "flags 0x%x", flags);
    dt = deleg_granted(&a, &root, "a-oo", "l1", NULL);
    expect("L1", dt == OPEN_DELEGATE_NONE,
           "no delegation with no backchannel: %s", dt_name(dt));

    /* A conflict on l0 while the path is down: the recall cannot be sent, so
     * the opener waits and the delegation stays BREAKING for the rebind to
     * recall. */
    {
        struct prep p = do_open(&other, &root, "conflict-oo", "l0");

        expect("L1", p.status == P_DELAY,
               "a conflicting OPEN with no backchannel is delayed: %u",
               p.status);
        expect("L1", !recall_seen(&a, &l0_sid),
               "no recall reaches the client with no backchannel");
    }

    /* The client answers CB_PATH_DOWN as Linux does: a new connection, bound
     * for both channels. */
    a.conn = pc_connect(&a);
    st     = pc_bind(&a, CDFC4_BACK_OR_BOTH);
    expect("R1", st == NFS4_OK, "BIND_CONN_TO_SESSION(BACK_OR_BOTH) on a fresh "
           "connection: %u", st);
    for (int i = 0; i < 200 && !recall_seen(&a, &l0_sid); i++) {
        (void) pc_seq_flags(&a);
        usleep(10000);
    }
    expect("R1", recall_seen(&a, &l0_sid),
           "the rebind sends the recall held back while the path was down");
    flags = pc_seq_flags(&a);
    expect("R1", (flags & SEQ4_STATUS_CB_PATH_DOWN) == 0,
           "SEQUENCE no longer reports CB_PATH_DOWN: flags 0x%x", flags);
    dt = deleg_granted(&a, &root, "a-oo", "r1", &sid);
    expect("R1", dt == OPEN_DELEGATE_WRITE,
           "a delegation once the backchannel is bound again: %s",
           dt_name(dt));
    if (dt == OPEN_DELEGATE_WRITE) {
        expect("R1", conflict_recalls(&other, &a, &root, "r1", &sid),
               "a conflicting OPEN recalls it over the new connection");
    }

    /* ---- N1 --------------------------------------------------------------- */
    printf("a session created without a backchannel, bound to one later\n");
    pc_setup(&c, env, "cbp-c", 0, &sec_c);
    st = pc_bind(&c, CDFC4_BACK_OR_BOTH);
    expect("N1", st == NFS4_OK, "BIND_CONN_TO_SESSION(BACK_OR_BOTH): %u", st);
    flags = pc_seq_flags(&c);
    expect("N1", (flags & SEQ4_STATUS_CB_PATH_DOWN) == 0,
           "SEQUENCE does not report CB_PATH_DOWN: flags 0x%x", flags);
    dt = deleg_granted(&c, &root, "c-oo", "n1", &sid);
    expect("N1", dt == OPEN_DELEGATE_WRITE,
           "a delegation over the late backchannel: %s", dt_name(dt));
    if (dt == OPEN_DELEGATE_WRITE) {
        expect("N1", conflict_recalls(&other, &c, &root, "n1", &sid),
               "a conflicting OPEN recalls it");
        ri = recall_index(&c, &sid);
        expect("N1", ri >= 0 && c.recall_flavor[ri] == AUTH_SYS &&
               c.recall_uid[ri] == 4242,
               "the recall carries the AUTH_SYS uid CREATE_SESSION asked for: "
               "flavor %u uid %u", ri >= 0 ? c.recall_flavor[ri] : 0,
               ri >= 0 ? c.recall_uid[ri] : 0);
    }

    /* ---- B1 --------------------------------------------------------------- */
    printf("BACKCHANNEL_CTL changes the program and the credentials\n");
    pc_setup(&d, env, "cbp-d", 1, NULL);
    dt = deleg_granted(&d, &root, "d-oo", "b0", &sid);
    expect("B1", dt == OPEN_DELEGATE_WRITE, "a delegation: %s", dt_name(dt));
    if (dt == OPEN_DELEGATE_WRITE) {
        expect("B1", conflict_recalls(&other, &d, &root, "b0", &sid),
               "a conflicting OPEN recalls it");
        ri = recall_index(&d, &sid);
        expect("B1", ri >= 0 && d.recall_prog[ri] == P_PROG_A &&
               d.recall_flavor[ri] == AUTH_NONE,
               "before BACKCHANNEL_CTL the recall goes to the CREATE_SESSION "
               "program with AUTH_NONE: program 0x%x flavor %u",
               ri >= 0 ? d.recall_prog[ri] : 0,
               ri >= 0 ? d.recall_flavor[ri] : 0);
    }

    st = pc_backchannel_ctl(&d, P_PROG_B, &sec_d);
    expect("B1", st == NFS4_OK, "BACKCHANNEL_CTL on the backchannel "
           "connection: %u", st);
    dt = deleg_granted(&d, &root, "d-oo", "b1", &sid);
    expect("B1", dt == OPEN_DELEGATE_WRITE, "a delegation after "
           "BACKCHANNEL_CTL: %s", dt_name(dt));
    if (dt == OPEN_DELEGATE_WRITE) {
        expect("B1", conflict_recalls(&other, &d, &root, "b1", &sid),
               "a conflicting OPEN recalls it");
        ri = recall_index(&d, &sid);
        expect("B1", ri >= 0 && d.recall_prog[ri] == P_PROG_B &&
               d.recall_flavor[ri] == AUTH_SYS && d.recall_uid[ri] == 4243,
               "the recall goes to the new program with the new credentials: "
               "program 0x%x flavor %u uid %u",
               ri >= 0 ? d.recall_prog[ri] : 0,
               ri >= 0 ? d.recall_flavor[ri] : 0,
               ri >= 0 ? d.recall_uid[ri] : 0);
    }

    /* The backchannel moves to a fresh connection: it keeps what
     * BACKCHANNEL_CTL set, not what CREATE_SESSION gave. */
    d.conn = pc_connect(&d);
    st     = pc_bind(&d, CDFC4_BACK_OR_BOTH);
    expect("B1", st == NFS4_OK, "BIND_CONN_TO_SESSION(BACK_OR_BOTH) on a fresh "
           "connection: %u", st);
    dt = deleg_granted(&d, &root, "d-oo", "b2", &sid);
    expect("B1", dt == OPEN_DELEGATE_WRITE, "a delegation after the rebind: %s",
           dt_name(dt));
    if (dt == OPEN_DELEGATE_WRITE) {
        expect("B1", conflict_recalls(&other, &d, &root, "b2", &sid),
               "a conflicting OPEN recalls it");
        ri = recall_index(&d, &sid);
        expect("B1", ri >= 0 && d.recall_prog[ri] == P_PROG_B &&
               d.recall_flavor[ri] == AUTH_SYS && d.recall_uid[ri] == 4243,
               "after a rebind the recall still goes to the BACKCHANNEL_CTL "
               "program and credentials: program 0x%x flavor %u uid %u",
               ri >= 0 ? d.recall_prog[ri] : 0,
               ri >= 0 ? d.recall_flavor[ri] : 0,
               ri >= 0 ? d.recall_uid[ri] : 0);
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("\n%d check(s) failed\n", failures);
        return 1;
    }
    printf("\nall checks passed\n");
    return 0;
} /* main */
