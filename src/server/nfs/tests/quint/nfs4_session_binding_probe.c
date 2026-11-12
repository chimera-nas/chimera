// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground-truth probe for how NFSv4.1 connections are associated with sessions,
 * against the in-process server.  Each check is a COMPOUND whose answer the
 * RFC fixes.
 *
 *   B1  SP4_MACH_CRED, SEQUENCE on a connection never associated with the
 *       session: NFS4ERR_CONN_NOT_BOUND_TO_SESSION (RFC 8881 Section 18.46.3;
 *       chimera always puts BIND_CONN_TO_SESSION in spo_must_enforce, which is
 *       what turns connection association enforcement on, Section 18.35.3).
 *       The SEQUENCE must not associate the connection either: a second one
 *       on it is refused the same way.
 *   B5  BIND_CONN_TO_SESSION under a principal other than the client's
 *       machine credential: NFS4ERR_WRONG_CRED (Section 2.10.8.3).
 *   B2  after BIND_CONN_TO_SESSION on that connection, SEQUENCE succeeds.
 *   B6  DESTROY_SESSION under another principal, on an associated
 *       connection: NFS4ERR_WRONG_CRED (Section 18.37.3), and the session
 *       survives.
 *   B3  SP4_NONE, SEQUENCE on a fresh connection: accepted, as chimera has
 *       always done; nothing in Section 18.46.3 forbids it without
 *       enforcement.
 *   B4  SP4_MACH_CRED, two sessions created over one connection: the
 *       connection is associated with both (CREATE_SESSION associates the
 *       connection it arrives on), so SEQUENCE on either succeeds.
 *   D1  SEQUENCE(S1) + DESTROY_SESSION(S2) + PUTROOTFH: DESTROY_SESSION of a
 *       session other than the SEQUENCE's may sit anywhere after the first
 *       operation (Section 18.37.3), so the COMPOUND succeeds and S2 is gone.
 *   D2  SEQUENCE(S1) + DESTROY_SESSION(S1) + PUTROOTFH: the same session, so
 *       DESTROY_SESSION must be last: NFS4ERR_NOT_ONLY_OP, and S1 survives.
 *
 * AUTH_SYS only: the machine credential here is the harness's AUTH_SYS one.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

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

/* ---- one COMPOUND --------------------------------------------------------- */

struct sb_reply {
    int      done;
    int      rpc_err;
    uint32_t status;
    int      nres;
    uint32_t st[8];
    uint64_t clientid;
    uint32_t eir_seq;
    uint8_t  sessionid[NFS4_SESSIONID_SIZE];
};

static void
sb_reply_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct sb_reply *r = private_data;
    uint32_t         i;

    (void) evpl;
    (void) verf;

    r->rpc_err = status;
    if (status == 0) {
        r->status = reply->status;
        r->nres   = (int) reply->num_resarray;
        for (i = 0; i < reply->num_resarray && i < 8; i++) {
            const struct nfs_resop4 *op = &reply->resarray[i];

            switch (op->resop) {
                case OP_SEQUENCE:
                    r->st[i] = op->opsequence.sr_status;
                    break;
                case OP_EXCHANGE_ID:
                    r->st[i] = op->opexchange_id.eir_status;
                    if (r->st[i] == NFS4_OK) {
                        r->clientid = op->opexchange_id.eir_resok4.eir_clientid;
                        r->eir_seq  =
                            op->opexchange_id.eir_resok4.eir_sequenceid;
                    }
                    break;
                case OP_CREATE_SESSION:
                    r->st[i] = op->opcreate_session.csr_status;
                    if (r->st[i] == NFS4_OK) {
                        memcpy(r->sessionid,
                               op->opcreate_session.csr_resok4.csr_sessionid,
                               NFS4_SESSIONID_SIZE);
                    }
                    break;
                case OP_BIND_CONN_TO_SESSION:
                    r->st[i] = op->opbind_conn_to_session.bctsr_status;
                    break;
                case OP_DESTROY_SESSION:
                    r->st[i] = op->opdestroy_session.dsr_status;
                    break;
                case OP_PUTROOTFH:
                    r->st[i] = op->opputrootfh.status;
                    break;
                case OP_RECLAIM_COMPLETE:
                    r->st[i] = op->opreclaim_complete.rcr_status;
                    break;
                default:
                    r->st[i] = 0;
                    break;
            } /* switch */
        }
    }
    r->done = 1;
} /* sb_reply_cb */

/* The credential the next COMPOUND goes out under, when not the harness's. */
static const struct evpl_rpc2_cred *sb_as;

/* A session as the probe tracks it: its id and slot 0's sequence id. */
struct sb_session {
    uint8_t  id[NFS4_SESSIONID_SIZE];
    uint32_t seq;
};

/* Send ops on conn, preceded by SEQUENCE on `sess` unless it is NULL.  A
* SEQUENCE that is refused leaves the slot's sequence id where it was. */
static struct sb_reply
sb_compound(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sb_session     *sess,
    struct nfs_argop4     *ops,
    int                    nops)
{
    struct nfs_argop4    argarray[8];
    struct COMPOUND4args args;
    struct sb_reply      r;
    int                  base = 0;
    int                  i;

    memset(&r, 0, sizeof(r));
    memset(&args, 0, sizeof(args));
    memset(argarray, 0, sizeof(argarray));

    if (sess) {
        argarray[0].argop = OP_SEQUENCE;
        memcpy(argarray[0].opsequence.sa_sessionid, sess->id,
               NFS4_SESSIONID_SIZE);
        argarray[0].opsequence.sa_sequenceid = sess->seq + 1;
        base                                 = 1;
    }
    for (i = 0; i < nops; i++) {
        argarray[base + i] = ops[i];
    }
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) (base + nops);

    env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl, conn,
                                            sb_as ? sb_as : &env->cred, &args,
                                            0, 0, NULL, 0, 0,
                                            sb_reply_cb, &r);
    while (!r.done) {
        evpl_continue(env->evpl);
    }
    if (r.rpc_err) {
        fprintf(stderr, "transport error %d\n", r.rpc_err);
        exit(2);
    }
    if (sess && r.nres > 0 && r.st[0] == NFS4_OK) {
        sess->seq++;
    }
    return r;
} /* sb_compound */

static struct evpl_rpc2_conn *
sb_connect(struct mbt_env *env)
{
    struct evpl_endpoint  *ep;
    struct evpl_rpc2_conn *conn;

    ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                            "127.0.0.1", 2049);
    conn = evpl_rpc2_client_connect(env->rpc2_thread, EVPL_STREAM_INPROC, ep,
                                    NULL, 0, NULL);
    if (!conn) {
        fprintf(stderr, "cannot open a connection\n");
        exit(2);
    }
    return conn;
} /* sb_connect */

/* EXCHANGE_ID under the given state protection.  Returns the client id and
 * the sequence id the first CREATE_SESSION must carry. */
static uint64_t
sb_exchange_id(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    const char            *owner,
    state_protect_how4     how,
    uint32_t              *csa_seq)
{
    /* The set RFC 8881 Section 18.35.3 names as the one a client requests:
     * DELEGPURGE(7); BIND_CONN_TO_SESSION(41), EXCHANGE_ID(42),
     * CREATE_SESSION(43), DESTROY_SESSION(44), DESTROY_CLIENTID(57). */
    static uint32_t   enforce[2] = {
        1u << 7,
            (1u << (41 - 32)) | (1u << (42 - 32)) | (1u << (43 - 32)) |
            (1u << (44 - 32)) | (1u << (57 - 32)),
    };
    struct nfs_argop4 op;
    struct sb_reply   r;

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    memset(op.opexchange_id.eia_clientowner.co_verifier, 0x5b, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = how;
    if (how == SP4_MACH_CRED) {
        op.opexchange_id.eia_state_protect.spa_mach_ops.num_spo_must_enforce = 2;
        op.opexchange_id.eia_state_protect.spa_mach_ops.spo_must_enforce     =
            enforce;
    }

    r = sb_compound(env, conn, NULL, &op, 1);
    if (r.status != NFS4_OK) {
        fprintf(stderr, "EXCHANGE_ID (%s) failed: %u\n", owner, r.status);
        exit(2);
    }
    *csa_seq = r.eir_seq;
    return r.clientid;
} /* sb_exchange_id */

/* CREATE_SESSION over conn, which it associates with the new session. */
static void
sb_create_session(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    uint64_t               clientid,
    uint32_t              *csa_seq,
    struct sb_session     *sess)
{
    struct channel_attrs4             chan = {
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 8,
    };
    static struct callback_sec_parms4 sec = { .cb_secflavor = 0 };
    struct nfs_argop4                 op;
    struct sb_reply                   r;

    memset(&op, 0, sizeof(op));
    op.argop                                = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid        = clientid;
    op.opcreate_session.csa_sequence        = (*csa_seq)++;
    op.opcreate_session.csa_fore_chan_attrs = chan;
    op.opcreate_session.csa_back_chan_attrs = chan;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = &sec;

    r = sb_compound(env, conn, NULL, &op, 1);
    if (r.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", r.status);
        exit(2);
    }
    memcpy(sess->id, r.sessionid, NFS4_SESSIONID_SIZE);
    sess->seq = 0;
} /* sb_create_session */

static uint32_t
sb_sequence(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sb_session     *sess)
{
    struct sb_reply r = sb_compound(env, conn, sess, NULL, 0);

    return r.st[0];
} /* sb_sequence */

static uint32_t
sb_bind(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sb_session     *sess)
{
    struct nfs_argop4 op;
    struct sb_reply   r;

    memset(&op, 0, sizeof(op));
    op.argop = OP_BIND_CONN_TO_SESSION;
    memcpy(op.opbind_conn_to_session.bctsa_sessid, sess->id,
           NFS4_SESSIONID_SIZE);
    op.opbind_conn_to_session.bctsa_dir = CDFC4_FORE_OR_BOTH;

    r = sb_compound(env, conn, NULL, &op, 1);
    return r.st[0];
} /* sb_bind */

/* ---- the checks ----------------------------------------------------------- */

static void
check_mach_cred_binding(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sb_connect(env);
    struct evpl_rpc2_conn *b = sb_connect(env);
    struct sb_session      s;
    struct evpl_rpc2_cred  other = env->cred;
    struct nfs_argop4      op;
    struct sb_reply        r;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    /* Another principal on the same machine. */
    other.authsys.uid++;
    other.authsys.gid++;

    printf("SP4_MACH_CRED: a connection is associated before it is used\n");
    clientid = sb_exchange_id(env, a, "sb-mach", SP4_MACH_CRED, &csa_seq);
    sb_create_session(env, a, clientid, &csa_seq, &s);

    st = sb_sequence(env, a, &s);
    expect("B0", st == NFS4_OK,
           "SEQUENCE on the connection CREATE_SESSION came in on: %u", st);

    st = sb_sequence(env, b, &s);
    expect("B1", st == NFS4ERR_CONN_NOT_BOUND_TO_SESSION,
           "SEQUENCE on a connection never associated with the session: %u "
           "(want %u)", st, NFS4ERR_CONN_NOT_BOUND_TO_SESSION);

    st = sb_sequence(env, b, &s);
    expect("B1", st == NFS4ERR_CONN_NOT_BOUND_TO_SESSION,
           "and the refused SEQUENCE did not associate it: %u", st);

    sb_as = &other;
    st    = sb_bind(env, b, &s);
    sb_as = NULL;
    expect("B5", st == NFS4ERR_WRONG_CRED,
           "BIND_CONN_TO_SESSION under another principal: %u (want %u)", st,
           NFS4ERR_WRONG_CRED);

    st = sb_bind(env, b, &s);
    expect("B2", st == NFS4_OK, "BIND_CONN_TO_SESSION on it: %u", st);

    st = sb_sequence(env, b, &s);
    expect("B2", st == NFS4_OK, "SEQUENCE on it after the bind: %u", st);

    memset(&op, 0, sizeof(op));
    op.argop = OP_DESTROY_SESSION;
    memcpy(op.opdestroy_session.dsa_sessionid, s.id, NFS4_SESSIONID_SIZE);
    sb_as = &other;
    r     = sb_compound(env, a, NULL, &op, 1);
    sb_as = NULL;
    expect("B6", r.st[0] == NFS4ERR_WRONG_CRED,
           "DESTROY_SESSION under another principal: %u (want %u)", r.st[0],
           NFS4ERR_WRONG_CRED);
    st = sb_sequence(env, a, &s);
    expect("B6", st == NFS4_OK, "the session survives: SEQUENCE %u", st);

    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
    evpl_rpc2_client_disconnect(env->rpc2_thread, b);
} /* check_mach_cred_binding */

static void
check_none_binding(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sb_connect(env);
    struct evpl_rpc2_conn *b = sb_connect(env);
    struct sb_session      s;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("SP4_NONE: no enforcement\n");
    clientid = sb_exchange_id(env, a, "sb-none", SP4_NONE, &csa_seq);
    sb_create_session(env, a, clientid, &csa_seq, &s);

    st = sb_sequence(env, b, &s);
    expect("B3", st == NFS4_OK,
           "SEQUENCE on a fresh connection, no enforcement: %u", st);

    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
    evpl_rpc2_client_disconnect(env->rpc2_thread, b);
} /* check_none_binding */

static void
check_two_sessions_one_conn(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sb_connect(env);
    struct sb_session      s1, s2;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("SP4_MACH_CRED: one connection associated with two sessions\n");
    clientid = sb_exchange_id(env, a, "sb-two", SP4_MACH_CRED, &csa_seq);
    sb_create_session(env, a, clientid, &csa_seq, &s1);
    sb_create_session(env, a, clientid, &csa_seq, &s2);

    st = sb_sequence(env, a, &s2);
    expect("B4", st == NFS4_OK, "SEQUENCE(S2) on it: %u", st);
    st = sb_sequence(env, a, &s1);
    expect("B4", st == NFS4_OK, "SEQUENCE(S1) on it: %u", st);
    st = sb_sequence(env, a, &s2);
    expect("B4", st == NFS4_OK, "SEQUENCE(S2) on it again: %u", st);

    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
} /* check_two_sessions_one_conn */

static void
check_destroy_session_position(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sb_connect(env);
    struct sb_session      s1, s2;
    struct nfs_argop4      ops[2];
    struct sb_reply        r;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("DESTROY_SESSION position (RFC 8881 Section 18.37.3)\n");
    clientid = sb_exchange_id(env, a, "sb-destroy", SP4_NONE, &csa_seq);
    sb_create_session(env, a, clientid, &csa_seq, &s1);
    sb_create_session(env, a, clientid, &csa_seq, &s2);

    memset(ops, 0, sizeof(ops));
    ops[0].argop = OP_DESTROY_SESSION;
    memcpy(ops[0].opdestroy_session.dsa_sessionid, s2.id, NFS4_SESSIONID_SIZE);
    ops[1].argop = OP_PUTROOTFH;
    r            = sb_compound(env, a, &s1, ops, 2);
    expect("D1", r.status == NFS4_OK && r.nres == 3 && r.st[1] == NFS4_OK,
           "SEQUENCE(S1) + DESTROY_SESSION(S2) + PUTROOTFH: compound %u, "
           "%d results, DESTROY_SESSION %u", r.status, r.nres, r.st[1]);
    st = sb_sequence(env, a, &s2);
    expect("D1", st == NFS4ERR_BADSESSION, "S2 is gone: SEQUENCE(S2) %u", st);

    memset(ops, 0, sizeof(ops));
    ops[0].argop = OP_DESTROY_SESSION;
    memcpy(ops[0].opdestroy_session.dsa_sessionid, s1.id, NFS4_SESSIONID_SIZE);
    ops[1].argop = OP_PUTROOTFH;
    r            = sb_compound(env, a, &s1, ops, 2);
    expect("D2", r.status == NFS4ERR_NOT_ONLY_OP,
           "SEQUENCE(S1) + DESTROY_SESSION(S1) + PUTROOTFH: %u (want %u)",
           r.status, NFS4ERR_NOT_ONLY_OP);
    st = sb_sequence(env, a, &s1);
    expect("D2", st == NFS4_OK, "S1 survives: SEQUENCE(S1) %u", st);

    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
} /* check_destroy_session_position */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts opts = { .disable_caches = 1 };
    struct mbt_env     *env  = malloc(sizeof(*env));

    opts.sec = mbt_sec_scan_argv(argc, argv);
    if (opts.sec != MBT_SEC_SYS) {
        printf("AUTH_SYS only; skipping\n");
        free(env);
        return 77;
    }

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);

    check_mach_cred_binding(env);
    check_none_binding(env);
    check_two_sessions_one_conn(env);
    check_destroy_session_position(env);

    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("\n%d check(s) failed\n", failures);
        return 1;
    }
    printf("\nall checks passed\n");
    return 0;
} /* main */
