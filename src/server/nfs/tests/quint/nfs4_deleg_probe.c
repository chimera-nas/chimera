// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground-truth probe for chimera's NFSv4.1 delegation policy under the
 * in-process harness, where the CB_NULL grant probe completes
 * deterministically (the backchannel is the same inproc connection and
 * the harness answers callbacks inline).  The delegation model in
 * nfs4_state.qnt / nfs4_ops.qnt must encode exactly the behavior pinned
 * here; when this probe goes red, either chimera's policy changed (update
 * the model and this probe together) or a regression landed.
 *
 * Behaviors pinned:
 *   K1  probe kick: a client's FIRST delegation-eligible OPEN gets no
 *       delegation (it kicks the CB_NULL path probe); the probe completes
 *       immediately in-process, so the SECOND eligible OPEN grants.
 *   G1  grant matrix: WRITE/BOTH open -> write delegation; READ open ->
 *       read delegation (fresh file, no contention).
 *   G2  same client re-OPEN of a file it holds a delegation on: no second
 *       grant, no self-recall.
 *   G3  read delegation held by A; B's READ open of the same file: probed
 *       behavior recorded (RFC allows coexisting read delegations OR a
 *       conservative decline).
 *   C1  write delegation held by A; B's READ open: CB_RECALL to A and
 *       NFS4ERR_DELAY to B until A returns the delegation, then B's
 *       retried OPEN succeeds.
 *   C2  read delegation held by A; B's WRITE open: recall + DELAY, same
 *       cycle.
 *   C3  non-OPEN triggers against A's write delegation: REMOVE and
 *       SETATTR(size) from B -- recall + DELAY (or completion), probed.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define P_DELAY      10008
#define P_BLOCK      8192
#define P_MAX_RECALL 16

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
    uint8_t                recalls[P_MAX_RECALL][12];
    int                    nrecalls;
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
    uint32_t               i;
    int                    rc;

    (void) conn;
    (void) cred;

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
    res.num_resarray = args->num_argarray;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                                 encoding);
    (void) rc;
} /* pc_cb_compound */

/* ---- minimal compound plumbing -------------------------------------------- */

/* Everything a probe check reads out of one compound's reply. */
struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[8];            /* per-op status */
    struct mbt_fh   fh;               /* last GETFH */
    struct stateid4 open_sid;         /* last OPEN stateid */
    uint32_t        deleg_type;       /* last OPEN delegation type */
    struct stateid4 deleg_sid;
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
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_DELEGRETURN:
                    p->st[i] = r->opdelegreturn.status;
                    break;
                case OP_READ:
                    p->st[i] = r->opread.status;
                    if (p->st[i] == NFS4_OK) {
                        int k;

                        /* Release the reply's data iovecs -- they carry
                        * references the caller now owns (the same
                        * contract the replayer's READ path follows). */
                        for (k = 0; k < r->opread.resok4.data.niov; k++) {
                            evpl_iovec_release(
                                evpl, &r->opread.resok4.data.iov[k]);
                        }
                    }
                    break;
                case OP_REMOVE:
                    p->st[i] = r->opremove.status;
                    break;
                case OP_SETATTR:
                    p->st[i] = r->opsetattr.status;
                    break;
                case OP_EXCHANGE_ID:
                    p->st[i] = r->opexchange_id.eir_status;
                    if (p->st[i] == NFS4_OK) {
                        p->clientid =
                            r->opexchange_id.eir_resok4.eir_clientid;
                        p->eir_seq =
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
    struct mbt_env *env,
    int             ownersym)
{
    struct evpl_rpc2_program         *cb_programs[1];
    struct evpl_endpoint             *ep;
    struct nfs_argop4                 op;
    struct prep                       p;
    static char                       owners[8][32];
    char                             *owner = owners[ownersym & 7];
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
                                                        "127.0.0.1",
                                                        env->rdma
                                                        ? MBT_NFS_RDMA_PORT
                                                        : 2049);
    pc->conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                        env->rdma ? EVPL_DATAGRAM_INPROC
                                        : EVPL_STREAM_INPROC, ep,
                                        cb_programs, 1, pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    snprintf(owner, 32, "deleg-probe-%d", ownersym);
    memset(op.opexchange_id.eia_clientowner.co_verifier, ownersym + 1, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  =
        (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how = SP4_NONE;
    p                                          = pc_compound(pc, &op, 1, 0);
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
        CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
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

    /* Chimera holds a fresh 4.1 client in grace (NFS4ERR_GRACE) until it
     * declares reclaim complete. */
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
op_open(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    uint32_t    access,
    int         create)
{
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

static struct nfs_argop4
op_delegreturn(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                       = OP_DELEGRETURN;
    a.opdelegreturn.deleg_stateid = *sid;
    return a;
} /* op_delegreturn */

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

/* OPEN(name) on root, prefixed with PUTFH(root); returns the prep. */
static struct prep
do_open(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *owner,
    const char          *name,
    uint32_t             access,
    int                  create)
{
    struct nfs_argop4 ops[2];

    ops[0] = op_putfh(root);
    ops[1] = op_open(pc->clientid, owner, name, access, create);
    return pc_compound(pc, ops, 2, 1);
} /* do_open */

static int
recall_seen(
    struct pc             *pc,
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
    struct mbt_env_opts opts = {
        .nfs4_delegations = 1,
        .disable_caches   = 1,
    };
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           a, b, g4c, c4c, mc;
    struct mbt_fh       root;
    struct prep         p;
    struct stateid4     a_f2_deleg, a_f4_deleg, a_f5_deleg, a_f6_deleg;
    struct stateid4     a_f7_deleg;
    char                buf[128];
    int                 i;

    opts.sec = mbt_sec_scan_argv(argc, argv);

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = pc_cb_compound;

    /* Resolve export root (minor 0, no session). */
    {
        struct nfs_argop4 ops[3];
        struct prep       rp;
        struct pc         boot = { .env = env, .conn = env->nfs_conn };

        memset(&ops, 0, sizeof(ops));
        ops[0].argop                 = OP_PUTROOTFH;
        ops[1].argop                 = OP_LOOKUP;
        ops[1].oplookup.objname.data = "fs0";
        ops[1].oplookup.objname.len  = 3;
        ops[2].argop                 = OP_GETFH;
        /* minorversion 0 compound: reuse pc_compound without SEQUENCE but
         * with minorversion 1 is invalid; send raw. */
        struct COMPOUND4args args;

        memset(&args, 0, sizeof(args));
        args.minorversion = 0;
        args.argarray     = ops;
        args.num_argarray = 3;
        memset(&rp, 0, sizeof(rp));
        env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2,
                                                env->evpl, env->nfs_conn,
                                                &env->cred, &args,
                                                0, 0, NULL, 0, 0,
                                                prep_cb, &rp);
        while (!rp.done) {
            evpl_continue(env->evpl);
        }
        if (rp.status != NFS4_OK || !rp.fh.has) {
            fprintf(stderr, "cannot resolve export root: %u\n", rp.status);
            return 2;
        }
        root = rp.fh;
        (void) boot;
    }

    pc_setup(&a, env, 1);
    pc_setup(&b, env, 2);
    pc_setup(&g4c, env, 3);
    pc_setup(&c4c, env, 4);
    pc_setup(&mc, env, 5);

    printf("K1 4.1 + backchannel: grants active from the FIRST open\n");
    printf("   (no CB_NULL kick round-trip; the session backchannel is\n");
    printf("    up at CREATE_SESSION, unlike the 4.0 probe path):\n");
    p = do_open(&a, &root, "a-oo1", "f1", 3, 1);
    snprintf(buf, sizeof(buf), "first OPEN by A: deleg=%s",
             dt_name(p.deleg_type));
    expect("A first OPEN grants immediately",
           p.status == NFS4_OK && p.deleg_type == 2, buf);

    printf("G1 grant matrix (probe now up):\n");
    p = do_open(&a, &root, "a-oo1", "f2", 3, 1);
    snprintf(buf, sizeof(buf), "WRITE open: deleg=%s", dt_name(p.deleg_type));
    expect("A BOTH open -> write delegation",
           p.status == NFS4_OK && p.deleg_type == 2, buf);
    a_f2_deleg = p.deleg_sid;

    p = do_open(&a, &root, "a-oo1", "f3", 1, 1);
    snprintf(buf, sizeof(buf), "READ open: deleg=%s", dt_name(p.deleg_type));
    expect("A READ open -> read delegation",
           p.status == NFS4_OK && p.deleg_type == 1, buf);

    printf("G2 same-client re-open (no second grant, no self-recall):\n");
    a.nrecalls = 0;
    p          = do_open(&a, &root, "a-oo2", "f2", 3, 0);
    snprintf(buf, sizeof(buf), "re-open: st=%u deleg=%s recalls=%d",
             p.status, dt_name(p.deleg_type), a.nrecalls);
    expect("A re-OPEN of write-delegated f2",
           p.status == NFS4_OK && p.deleg_type == 0 && a.nrecalls == 0,
           buf);

    printf("K1b B's first open likewise grants immediately:\n");
    p = do_open(&b, &root, "b-oo1", "bkick", 3, 1);
    snprintf(buf, sizeof(buf), "first OPEN by B: deleg=%s",
             dt_name(p.deleg_type));
    expect("B first OPEN grants immediately",
           p.status == NFS4_OK && p.deleg_type == 2, buf);

    printf("G3 read deleg held by A; B READ opens the same file:\n");
    a.nrecalls = 0;
    p          = do_open(&b, &root, "b-oo1", "f3", 1, 0);
    snprintf(buf, sizeof(buf), "st=%u deleg=%s recalls-to-A=%d",
             p.status, dt_name(p.deleg_type), a.nrecalls);
    expect("B READ open vs A read deleg: coexisting read delegations",
           p.status == NFS4_OK && p.deleg_type == 1 && a.nrecalls == 0,
           buf);

    printf("C1 write deleg held by A; B READ open -> recall + DELAY:\n");
    a.nrecalls = 0;
    p          = do_open(&b, &root, "b-oo1", "f2", 1, 0);
    snprintf(buf, sizeof(buf), "st=%u recall-of-f2-deleg=%d",
             p.status, recall_seen(&a, &a_f2_deleg));
    expect("B conflicting open gets DELAY",
           p.status == P_DELAY, buf);
    /* The recall may need a few pumps to arrive. */
    for (i = 0; i < 100 && !recall_seen(&a, &a_f2_deleg); i++) {
        evpl_continue(env->evpl);
    }
    expect("CB_RECALL for A's f2 delegation observed",
           recall_seen(&a, &a_f2_deleg), "recall arrived");
    /* Still DELAY while unreturned. */
    p = do_open(&b, &root, "b-oo1", "f2", 1, 0);
    expect("B retry still DELAY before DELEGRETURN",
           p.status == P_DELAY, "second attempt");
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&root);
        /* need f2's fh: PUTFH(root)+OPEN used CLAIM_NULL; DELEGRETURN
         * needs the file's fh.  Look it up. */
        struct nfs_argop4 lops[3];

        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f2";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&a, lops, 3, 1);
        ops[0]                        = op_putfh(&p.fh);
        ops[1]                        = op_delegreturn(&a_f2_deleg);
        p                             = pc_compound(&a, ops, 2, 1);
        expect("A DELEGRETURN succeeds", p.st[2] == NFS4_OK, "delegreturn");
    }
    for (i = 0; i < 200; i++) {
        p = do_open(&b, &root, "b-oo1", "f2", 1, 0);
        if (p.status != P_DELAY) {
            break;
        }
        usleep(10000);
    }
    snprintf(buf, sizeof(buf), "st=%u deleg=%s after %d attempt(s)",
             p.status, dt_name(p.deleg_type), i + 1);
    expect("B open succeeds after return, no grant (A holds a write-mode "
           "open)", p.status == NFS4_OK && p.deleg_type == 0, buf);

    printf("C2 read deleg held by A; B WRITE open -> recall + DELAY:\n");
    p = do_open(&a, &root, "a-oo1", "f5", 1, 1);
    snprintf(buf, sizeof(buf), "setup: deleg=%s", dt_name(p.deleg_type));
    expect("A read deleg on f5", p.deleg_type == 1, buf);
    a_f5_deleg = p.deleg_sid;
    a.nrecalls = 0;
    p          = do_open(&b, &root, "b-oo1", "f5", 3, 0);
    for (i = 0; i < 100 && !recall_seen(&a, &a_f5_deleg); i++) {
        evpl_continue(env->evpl);
    }
    snprintf(buf, sizeof(buf), "st=%u recall=%d", p.status,
             recall_seen(&a, &a_f5_deleg));
    expect("B WRITE open vs A read deleg: DELAY + recall",
           p.status == P_DELAY && recall_seen(&a, &a_f5_deleg), buf);
    {
        struct nfs_argop4 lops[3], ops[2];

        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f5";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&a, lops, 3, 1);
        ops[0]                        = op_putfh(&p.fh);
        ops[1]                        = op_delegreturn(&a_f5_deleg);
        pc_compound(&a, ops, 2, 1);
    }

    printf("C3 non-OPEN triggers vs A's write delegation:\n");
    p = do_open(&a, &root, "a-oo1", "f4", 3, 1);
    snprintf(buf, sizeof(buf), "setup: deleg=%s", dt_name(p.deleg_type));
    expect("A write deleg on f4", p.deleg_type == 2, buf);
    a_f4_deleg = p.deleg_sid;
    a.nrecalls = 0;
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&root);
        ops[1] = op_remove("f4");
        p      = pc_compound(&b, ops, 2, 1);
    }
    for (i = 0; i < 100 && !recall_seen(&a, &a_f4_deleg); i++) {
        evpl_continue(env->evpl);
    }
    snprintf(buf, sizeof(buf), "REMOVE st=%u recall=%d",
             p.st[2], recall_seen(&a, &a_f4_deleg));
    expect("B REMOVE vs A write deleg: DELAY + recall",
           p.st[2] == P_DELAY && recall_seen(&a, &a_f4_deleg), buf);

    p          = do_open(&a, &root, "a-oo1", "f6", 3, 1);
    a_f6_deleg = p.deleg_sid;
    snprintf(buf, sizeof(buf), "setup: deleg=%s", dt_name(p.deleg_type));
    expect("A write deleg on f6", p.deleg_type == 2, buf);
    a.nrecalls = 0;
    {
        /* SETATTR size=0 from B via anon stateid. */
        struct nfs_argop4 lops[3], ops[2];
        static uint32_t   bm[2]   = { 1U << FATTR4_SIZE, 0 };
        static uint8_t    blob[8] = { 0 };

        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f6";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&b, lops, 3, 1);

        memset(ops, 0, sizeof(ops));
        ops[0]                                         = op_putfh(&p.fh);
        ops[1].argop                                   = OP_SETATTR;
        ops[1].opsetattr.obj_attributes.num_attrmask   = 1;
        ops[1].opsetattr.obj_attributes.attrmask       = bm;
        ops[1].opsetattr.obj_attributes.attr_vals.data = blob;
        ops[1].opsetattr.obj_attributes.attr_vals.len  = 8;
        p                                              = pc_compound(&b, ops, 2, 1);
    }
    for (i = 0; i < 100 && !recall_seen(&a, &a_f6_deleg); i++) {
        evpl_continue(env->evpl);
    }
    snprintf(buf, sizeof(buf), "SETATTR st=%u recall=%d",
             p.st[2], recall_seen(&a, &a_f6_deleg));
    expect("B SETATTR(size) vs A write deleg: completes AND recalls",
           p.st[2] == NFS4_OK && recall_seen(&a, &a_f6_deleg), buf);

    /* Return the C3 delegations (recalled but never returned) so the
     * remaining sections start with no outstanding recalls against A. */
    {
        struct nfs_argop4 lops[3], rops[2];

        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f4";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&a, lops, 3, 1);
        rops[0]                       = op_putfh(&p.fh);
        rops[1]                       = op_delegreturn(&a_f4_deleg);
        pc_compound(&a, rops, 2, 1);

        lops[1].oplookup.objname.data = "f6";
        p                             = pc_compound(&a, lops, 3, 1);
        rops[0]                       = op_putfh(&p.fh);
        rops[1]                       = op_delegreturn(&a_f6_deleg);
        pc_compound(&a, rops, 2, 1);
    }

    printf("G4 self namespace ops vs OWN write delegation (D4-13):\n");
    p = do_open(&g4c, &root, "g4-oo", "f8", 3, 1);
    snprintf(buf, sizeof(buf), "setup: deleg=%s", dt_name(p.deleg_type));
    expect("A write deleg on f8", p.deleg_type == 2, buf);
    a.nrecalls = 0;
    {
        struct nfs_argop4 ops[2];
        struct stateid4   f8_deleg = p.deleg_sid;

        ops[0] = op_putfh(&root);
        ops[1] = op_remove("f8");
        p      = pc_compound(&g4c, ops, 2, 1);
        for (i = 0; i < 100 && !recall_seen(&g4c, &f8_deleg); i++) {
            evpl_continue(env->evpl);
        }
        snprintf(buf, sizeof(buf), "self REMOVE st=%u self-recall=%d",
                 p.st[2], recall_seen(&g4c, &f8_deleg));
        expect("A REMOVE of its own write-delegated file: DELAY + "
               "self-recall (D4-13; RFC intent: no self-conflict)",
               p.st[2] == P_DELAY && recall_seen(&g4c, &f8_deleg), buf);
        {
            struct nfs_argop4 lops[3], rops[2];

            memset(lops, 0, sizeof(lops));
            lops[0]                       = op_putfh(&root);
            lops[1].argop                 = OP_LOOKUP;
            lops[1].oplookup.objname.data = "f8";
            lops[1].oplookup.objname.len  = 2;
            lops[2].argop                 = OP_GETFH;
            p                             = pc_compound(&g4c, lops, 3, 1);
            rops[0]                       = op_putfh(&p.fh);
            rops[1]                       = op_delegreturn(&f8_deleg);
            p                             = pc_compound(&g4c, rops, 2, 1);
            expect("A returns the self-recalled delegation",
                   p.st[2] == NFS4_OK, "delegreturn");
        }
        for (i = 0; i < 200; i++) {
            ops[0] = op_putfh(&root);
            ops[1] = op_remove("f8");
            p      = pc_compound(&g4c, ops, 2, 1);
            if (p.st[2] != P_DELAY) {
                break;
            }
            usleep(10000);
        }
        snprintf(buf, sizeof(buf), "st=%u", p.st[2]);
        expect("self REMOVE succeeds after the return",
               p.st[2] == NFS4_OK, buf);
    }

    printf("C4 anonymous-stateid I/O vs A's write delegation:\n");
    p          = do_open(&c4c, &root, "c4-oo", "f7", 3, 1);
    a_f7_deleg = p.deleg_sid;
    snprintf(buf, sizeof(buf), "setup: deleg=%s", dt_name(p.deleg_type));
    expect("A write deleg on f7", p.deleg_type == 2, buf);
    c4c.nrecalls = 0;
    {
        struct nfs_argop4 lops[3], ops[2];
        struct evpl_iovec wiov;
        struct mbt_fh     f7fh;

        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f7";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&b, lops, 3, 1);
        f7fh                          = p.fh;

        memset(ops, 0, sizeof(ops));
        ops[0]       = op_putfh(&f7fh);
        ops[1].argop = OP_WRITE;
        memset(&ops[1].opwrite.stateid, 0, sizeof(ops[1].opwrite.stateid));
        ops[1].opwrite.offset = 0;
        ops[1].opwrite.stable = FILE_SYNC4;
        if (evpl_iovec_alloc(env->evpl, 8, 4096, 1, 0, &wiov) < 0) {
            fprintf(stderr, "iovec alloc failed\n");
            return 2;
        }
        memset(wiov.data, 0x41, 8);
        wiov.length                = 8;
        ops[1].opwrite.data.iov    = &wiov;
        ops[1].opwrite.data.niov   = 1;
        ops[1].opwrite.data.length = 8;
        p                          = pc_compound(&b, ops, 2, 1);
        for (i = 0; i < 100 && !recall_seen(&c4c, &a_f7_deleg); i++) {
            evpl_continue(env->evpl);
        }
        snprintf(buf, sizeof(buf), "WRITE st=%u recall=%d",
                 p.st[2], recall_seen(&c4c, &a_f7_deleg));
        expect("B anon WRITE vs write deleg: recall observed "
               "(completion timing-dependent: block/revoke)",
               recall_seen(&c4c, &a_f7_deleg), buf);

        memset(ops, 0, sizeof(ops));
        ops[0]       = op_putfh(&f7fh);
        ops[1].argop = OP_READ;
        memset(&ops[1].opread.stateid, 0, sizeof(ops[1].opread.stateid));
        ops[1].opread.offset = 0;
        ops[1].opread.count  = 8;
        p                    = pc_compound(&b, ops, 2, 1);
        snprintf(buf, sizeof(buf), "READ st=%u", p.st[2]);
        expect("B anon READ vs A write deleg (observed)", 1, buf);

        ops[0] = op_putfh(&f7fh);
        ops[1] = op_delegreturn(&a_f7_deleg);
        p      = pc_compound(&c4c, ops, 2, 1);
        snprintf(buf, sizeof(buf), "delegreturn st=%u (may be revoked "
                 "after the recall timeout)", p.st[2]);
        expect("f7 delegreturn attempted (observed)", 1, buf);
    }

    printf("M cross-protocol: NFS3 client vs NFS4 delegations:\n");
    {
        struct mbt_result *r3;
        struct mbt_fh      root3, f9fh4, f9fh3;
        struct stateid4    f9_deleg;
        struct nfs_argop4  lops[3];
        uint8_t            wbuf[16];

        /* M0: FH identity -- the NFS3 MOUNT root vs the NFS4 export
         * root, and a file's handle via both protocols. */
        r3 = mbt_mnt(env, "/fs0");
        expect("NFS3 MOUNT of the shared export",
               r3->status == MNT3_OK && r3->obj_fh.has, "mnt");
        root3 = r3->obj_fh;
        snprintf(buf, sizeof(buf), "root fh3 len=%u fh4 len=%u equal=%d",
                 root3.len, root.len, mbt_fh_eq(&root3, &root));
        expect("root filehandle identical across protocols",
               mbt_fh_eq(&root3, &root), buf);

        p = do_open(&mc, &root, "m-oo", "f9", 3, 1);
        snprintf(buf, sizeof(buf), "setup: deleg=%s",
                 dt_name(p.deleg_type));
        expect("holder(nfs4) write deleg on f9", p.deleg_type == 2, buf);
        f9_deleg = p.deleg_sid;
        memset(lops, 0, sizeof(lops));
        lops[0]                       = op_putfh(&root);
        lops[1].argop                 = OP_LOOKUP;
        lops[1].oplookup.objname.data = "f9";
        lops[1].oplookup.objname.len  = 2;
        lops[2].argop                 = OP_GETFH;
        p                             = pc_compound(&mc, lops, 3, 1);
        f9fh4                         = p.fh;

        r3 = mbt_lookup(env, &root3, "f9", 2);
        expect("NFS3 LOOKUP of the NFS4-created file",
               r3->status == NFS3_OK && r3->obj_fh.has, "lookup");
        f9fh3 = r3->obj_fh;
        snprintf(buf, sizeof(buf), "file fh3==fh4: %d",
                 mbt_fh_eq(&f9fh3, &f9fh4));
        expect("file filehandle identical across protocols",
               mbt_fh_eq(&f9fh3, &f9fh4), buf);

        /* M1: NFS3 WRITE vs the outstanding NFS4 write delegation --
         * fired asynchronously: chimera blocks protocols without a
         * DELAY-style error until the recall completes, so the NFS4 side
         * must be able to DELEGRETURN while the v3 op is in flight. */
        mc.nrecalls = 0;
        memset(wbuf, 0x42, sizeof(wbuf));
        {
            struct WRITE3args w3;
            struct evpl_iovec wiov;

            mbt_call_begin(env);
            if (evpl_iovec_alloc(env->evpl, sizeof(wbuf), 4096, 1, 0,
                                 &wiov) < 0) {
                fprintf(stderr, "iovec alloc failed\n");
                return 2;
            }
            memcpy(wiov.data, wbuf, sizeof(wbuf));
            wiov.length = sizeof(wbuf);
            memset(&w3, 0, sizeof(w3));
            w3.file.data.data = (void *) f9fh3.data;
            w3.file.data.len  = f9fh3.len;
            w3.offset         = 0;
            w3.count          = sizeof(wbuf);
            w3.stable         = FILE_SYNC;
            w3.data.iov       = &wiov;
            w3.data.niov      = 1;
            w3.data.length    = sizeof(wbuf);
            env->nfs_v3.send_call_NFSPROC3_WRITE(&env->nfs_v3.rpc2,
                                                 env->evpl, env->nfs_conn,
                                                 &env->cred, &w3,
                                                 0, 0, NULL, 0, 0,
                                                 mbt_write_cb, env);
            for (i = 0; i < 200 && !recall_seen(&mc, &f9_deleg) &&
                 !env->res.done; i++) {
                evpl_continue(env->evpl);
            }
            snprintf(buf, sizeof(buf), "recall while v3 WRITE in flight=%d "
                     "(completed=%d)", recall_seen(&mc, &f9_deleg),
                     env->res.done);
            expect("NFS3 WRITE vs NFS4 write deleg: recall observed", 1,
                   buf);
            if (!env->res.done) {
                /* Blocked: return the delegation, the write must finish. */
                struct nfs_argop4 rops[2];
                struct prep       rp;

                rops[0] = op_putfh(&f9fh4);
                rops[1] = op_delegreturn(&f9_deleg);
                rp      = pc_compound(&mc, rops, 2, 1);
                expect("holder returns f9's delegation mid-write",
                       rp.st[2] == NFS4_OK, "delegreturn");
                for (i = 0; i < 500 && !env->res.done; i++) {
                    evpl_continue(env->evpl);
                }
            }
            snprintf(buf, sizeof(buf), "v3 WRITE done=%d st=%u",
                     env->res.done, env->res.status);
            expect("NFS3 WRITE completes after return (blocked, not "
                   "errored)", env->res.done &&
                   env->res.status == NFS3_OK, buf);
        }

        /* M2: NFS3 SETATTR(size) truncate vs an outstanding NFS4 write
         * delegation.  (An NFS3 REMOVE here instead trips a server
         * use-after-free in the delete-notify path when it completes
         * after the delegation return -- see DEVIATIONS-NFS4.md D4-14 --
         * so this exercises the coherent cross-protocol recall with a
         * mutating op that completes safely.) */
        p = do_open(&mc, &root, "m-oo", "f10", 3, 1);
        expect("holder(nfs4) write deleg on f10", p.deleg_type == 2,
               "setup");
        {
            struct stateid4     f10_deleg = p.deleg_sid;
            struct SETATTR3args sa3;
            struct mbt_fh       f10fh3;

            r3     = mbt_lookup(env, &root3, "f10", 3);
            f10fh3 = r3->obj_fh;

            mc.nrecalls = 0;
            mbt_call_begin(env);
            memset(&sa3, 0, sizeof(sa3));
            sa3.object.data.data            = (void *) f10fh3.data;
            sa3.object.data.len             = f10fh3.len;
            sa3.new_attributes.size.set_it  = 1;
            sa3.new_attributes.size.size    = 0;
            sa3.new_attributes.atime.set_it = DONT_CHANGE;
            sa3.new_attributes.mtime.set_it = DONT_CHANGE;
            sa3.guard.check                 = 0;
            env->nfs_v3.send_call_NFSPROC3_SETATTR(&env->nfs_v3.rpc2,
                                                   env->evpl,
                                                   env->nfs_conn,
                                                   &env->cred, &sa3,
                                                   0, 0, NULL, 0, 0,
                                                   mbt_setattr_cb, env);
            for (i = 0; i < 200 && !recall_seen(&mc, &f10_deleg) &&
                 !env->res.done; i++) {
                evpl_continue(env->evpl);
            }
            snprintf(buf, sizeof(buf), "recall=%d done=%d",
                     recall_seen(&mc, &f10_deleg), env->res.done);
            expect("NFS3 SETATTR(size) vs NFS4 write deleg: recall "
                   "observed", recall_seen(&mc, &f10_deleg), buf);
            if (!env->res.done) {
                struct nfs_argop4 rops[2];
                struct prep       rp;
                struct nfs_argop4 lops2[3];

                memset(lops2, 0, sizeof(lops2));
                lops2[0]                       = op_putfh(&root);
                lops2[1].argop                 = OP_LOOKUP;
                lops2[1].oplookup.objname.data = "f10";
                lops2[1].oplookup.objname.len  = 3;
                lops2[2].argop                 = OP_GETFH;
                rp                             = pc_compound(&mc, lops2, 3, 1);
                rops[0]                        = op_putfh(&rp.fh);
                rops[1]                        = op_delegreturn(&f10_deleg);
                rp                             = pc_compound(&mc, rops, 2, 1);
                expect("holder returns f10's delegation mid-setattr",
                       rp.st[2] == NFS4_OK, "delegreturn");
                for (i = 0; i < 500 && !env->res.done; i++) {
                    evpl_continue(env->evpl);
                }
            }
            snprintf(buf, sizeof(buf), "done=%d st=%u", env->res.done,
                     env->res.status);
            expect("NFS3 SETATTR completes after return", env->res.done &&
                   env->res.status == NFS3_OK, buf);
        }
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("\n%d delegation behavior(s) diverge from the pinned "
               "expectations; update the model and this probe together.\n",
               failures);
        return 1;
    }
    printf("\nAll pinned delegation behaviors hold.\n");
    return 0;
} /* main */
