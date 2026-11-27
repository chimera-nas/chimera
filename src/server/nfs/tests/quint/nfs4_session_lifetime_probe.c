// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * What happens to an NFSv4.1 session under a COMPOUND that is running on it,
 * against the in-process server.  The server's test gate
 * (nfs4_compound_test_gate.h) parks a COMPOUND after its SEQUENCE; the check
 * then changes the connection or the session underneath it and releases it.
 * A COMPOUND that uses the session after it was freed is reported by ASan, so
 * run it from a sanitizer build.
 *
 *   close     the parked COMPOUND's connection is closed and the session is
 *             then destroyed from another connection; the COMPOUND completes.
 *   rebind    the session is destroyed from another connection and a second
 *             COMPOUND on the same connection binds a different session; the
 *             first COMPOUND completes.
 *   destroy   the session is destroyed from another connection and the parked
 *             COMPOUND's connection is then closed; the COMPOUND completes.
 *   replay    SEQUENCE(S1) + CREATE_SESSION in one COMPOUND, retransmitted:
 *             the retransmit is answered from S1's slot.
 *
 * Sessions are created under SP4_NONE, so any connection may use them.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "server/nfs/nfs4_compound_test_gate.h"

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

struct sl_reply {
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
sl_reply_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct sl_reply *r = private_data;
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
                case OP_DESTROY_SESSION:
                    r->st[i] = op->opdestroy_session.dsr_status;
                    break;
                case OP_PUTROOTFH:
                    r->st[i] = op->opputrootfh.status;
                    break;
                default:
                    r->st[i] = 0;
                    break;
            } /* switch */
        }
    }
    r->done = 1;
} /* sl_reply_cb */

/* A session as the probe tracks it: its id and slot 0's sequence id. */
struct sl_session {
    uint8_t  id[NFS4_SESSIONID_SIZE];
    uint32_t seq;
};

/* Send ops on conn, preceded by SEQUENCE on `sess` unless it is NULL, and
 * return without waiting.  `r` must outlive the call: a COMPOUND whose
 * connection is closed may be answered, with an error, long after. */
static void
sl_send(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sl_session     *sess,
    int                    cachethis,
    struct nfs_argop4     *ops,
    int                    nops,
    struct sl_reply       *r)
{
    struct nfs_argop4    argarray[8];
    struct COMPOUND4args args;
    int                  base = 0;
    int                  i;

    memset(r, 0, sizeof(*r));
    memset(&args, 0, sizeof(args));
    memset(argarray, 0, sizeof(argarray));

    if (sess) {
        argarray[0].argop = OP_SEQUENCE;
        memcpy(argarray[0].opsequence.sa_sessionid, sess->id,
               NFS4_SESSIONID_SIZE);
        argarray[0].opsequence.sa_sequenceid = sess->seq + 1;
        argarray[0].opsequence.sa_cachethis  = cachethis;
        base                                 = 1;
    }
    for (i = 0; i < nops; i++) {
        argarray[base + i] = ops[i];
    }
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) (base + nops);

    env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl, conn,
                                            &env->cred, &args,
                                            0, 0, NULL, 0, 0,
                                            sl_reply_cb, r);
} /* sl_send */

static void
sl_wait(
    struct mbt_env  *env,
    struct sl_reply *r)
{
    while (!r->done) {
        evpl_continue(env->evpl);
    }
    if (r->rpc_err) {
        fprintf(stderr, "transport error %d\n", r->rpc_err);
        exit(2);
    }
} /* sl_wait */

static struct sl_reply
sl_compound(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sl_session     *sess,
    struct nfs_argop4     *ops,
    int                    nops)
{
    struct sl_reply r;

    sl_send(env, conn, sess, 0, ops, nops, &r);
    sl_wait(env, &r);
    if (sess && r.nres > 0 && r.st[0] == NFS4_OK) {
        sess->seq++;
    }
    return r;
} /* sl_compound */

static struct evpl_rpc2_conn *
sl_connect(struct mbt_env *env)
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
} /* sl_connect */

/* ---- waiting -------------------------------------------------------------- */

struct sl_tick {
    struct evpl_timer timer;
    volatile int      ticks;
};

static void
sl_tick_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    ((struct sl_tick *) timer)->ticks++;
} /* sl_tick_cb */

/* Pump the client's event loop for `ms` milliseconds, or until the gate holds
 * `parked` COMPOUNDs when `parked` is not negative. */
static int
sl_pump(
    struct mbt_env *env,
    int             ms,
    int             parked)
{
    struct sl_tick t;
    int            met;

    t.ticks = 0;
    evpl_add_timer(env->evpl, &t.timer, sl_tick_cb, 1000);
    while (!(met = parked >= 0 &&
                 atomic_load(&chimera_nfs4_compound_test_gate.parked) == parked) &&
           t.ticks < ms) {
        evpl_continue(env->evpl);
    }
    evpl_remove_timer(env->evpl, &t.timer);
    return met;
} /* sl_pump */

/* ---- session setup -------------------------------------------------------- */

static uint64_t
sl_exchange_id(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    const char            *owner,
    uint32_t              *csa_seq)
{
    struct nfs_argop4 op;
    struct sl_reply   r;

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    memset(op.opexchange_id.eia_clientowner.co_verifier, 0x5b, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;

    r = sl_compound(env, conn, NULL, &op, 1);
    if (r.status != NFS4_OK) {
        fprintf(stderr, "EXCHANGE_ID (%s) failed: %u\n", owner, r.status);
        exit(2);
    }
    *csa_seq = r.eir_seq;
    return r.clientid;
} /* sl_exchange_id */

static void
sl_create_session_op(
    struct nfs_argop4 *op,
    uint64_t           clientid,
    uint32_t           csa_seq)
{
    static struct channel_attrs4      chan = {
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 8,
    };
    static struct callback_sec_parms4 sec = { .cb_secflavor = 0 };

    memset(op, 0, sizeof(*op));
    op->argop                                = OP_CREATE_SESSION;
    op->opcreate_session.csa_clientid        = clientid;
    op->opcreate_session.csa_sequence        = csa_seq;
    op->opcreate_session.csa_fore_chan_attrs = chan;
    op->opcreate_session.csa_back_chan_attrs = chan;
    op->opcreate_session.num_csa_sec_parms   = 1;
    op->opcreate_session.csa_sec_parms       = &sec;
} /* sl_create_session_op */

/* CREATE_SESSION over conn, which binds the new session. */
static void
sl_create_session(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    uint64_t               clientid,
    uint32_t              *csa_seq,
    struct sl_session     *sess)
{
    struct nfs_argop4 op;
    struct sl_reply   r;

    sl_create_session_op(&op, clientid, (*csa_seq)++);
    r = sl_compound(env, conn, NULL, &op, 1);
    if (r.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", r.status);
        exit(2);
    }
    memcpy(sess->id, r.sessionid, NFS4_SESSIONID_SIZE);
    sess->seq = 0;
} /* sl_create_session */

static uint32_t
sl_destroy_session(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sl_session     *sess)
{
    struct nfs_argop4 op;
    struct sl_reply   r;

    memset(&op, 0, sizeof(op));
    op.argop = OP_DESTROY_SESSION;
    memcpy(op.opdestroy_session.dsa_sessionid, sess->id, NFS4_SESSIONID_SIZE);
    r = sl_compound(env, conn, NULL, &op, 1);
    return r.st[0];
} /* sl_destroy_session */

static uint32_t
sl_sequence(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sl_session     *sess)
{
    struct sl_reply r = sl_compound(env, conn, sess, NULL, 0);

    return r.st[0];
} /* sl_sequence */

/* Park SEQUENCE(sess) + PUTROOTFH sent on conn.  `r` outlives the call. */
static int
sl_park(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn,
    struct sl_session     *sess,
    struct sl_reply       *r)
{
    struct nfs_argop4 op;

    memset(&op, 0, sizeof(op));
    op.argop = OP_PUTROOTFH;
    atomic_store(&chimera_nfs4_compound_test_gate.armed, 1);
    sl_send(env, conn, sess, 0, &op, 1, r);
    sess->seq++;
    if (!sl_pump(env, 10000, 1)) {
        atomic_store(&chimera_nfs4_compound_test_gate.armed, 0);
        fprintf(stderr, "the gate did not take the COMPOUND\n");
        exit(2);
    }
    return 1;
} /* sl_park */

/* Let the parked COMPOUND run, and wait until it has. */
static void
sl_release(struct mbt_env *env)
{
    atomic_fetch_add(&chimera_nfs4_compound_test_gate.released, 1);
    if (!sl_pump(env, 10000, 0)) {
        fprintf(stderr, "the parked COMPOUND was not released\n");
        exit(2);
    }
    /* Whatever it does after the gate lets go of it happens on the server's
     * thread; give it time, and let the server answer something after. */
    sl_pump(env, 200, -1);
} /* sl_release */

/* ---- the checks ----------------------------------------------------------- */

/* Parked replies are written to long after the check that sent them returns
 * when their connection is closed, so they are not on any stack. */
static struct sl_reply parked_reply;
static struct sl_reply second_reply;

static void
check_close(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sl_connect(env);
    struct evpl_rpc2_conn *b = sl_connect(env);
    struct sl_session      s1, s3;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("close: connection closed, then the session destroyed, under a "
           "parked COMPOUND\n");
    clientid = sl_exchange_id(env, a, "sl-close", &csa_seq);
    sl_create_session(env, a, clientid, &csa_seq, &s1);
    sl_create_session(env, b, clientid, &csa_seq, &s3);

    sl_park(env, a, &s1, &parked_reply);
    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
    sl_pump(env, 300, -1);
    st = sl_destroy_session(env, b, &s1);
    expect("close", st == NFS4_OK, "DESTROY_SESSION(S1) from another "
           "connection: %u", st);
    sl_release(env);

    st = sl_sequence(env, b, &s3);
    expect("close", st == NFS4_OK, "the server still answers: %u", st);
    evpl_rpc2_client_disconnect(env->rpc2_thread, b);
} /* check_close */

static void
check_rebind(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sl_connect(env);
    struct evpl_rpc2_conn *b = sl_connect(env);
    struct sl_session      s1, s2;
    struct nfs_argop4      op;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("rebind: the session destroyed, then the connection bound to "
           "another, under a parked COMPOUND\n");
    clientid = sl_exchange_id(env, a, "sl-rebind", &csa_seq);
    sl_create_session(env, a, clientid, &csa_seq, &s1);
    sl_create_session(env, b, clientid, &csa_seq, &s2);

    sl_park(env, a, &s1, &parked_reply);
    st = sl_destroy_session(env, b, &s1);
    expect("rebind", st == NFS4_OK, "DESTROY_SESSION(S1) from another "
           "connection: %u", st);

    memset(&op, 0, sizeof(op));
    op.argop = OP_PUTROOTFH;
    sl_send(env, a, &s2, 0, &op, 1, &second_reply);
    sl_wait(env, &second_reply);
    s2.seq++;
    expect("rebind", second_reply.status == NFS4_OK,
           "SEQUENCE(S2) on the same connection: %u", second_reply.status);
    sl_release(env);

    st = sl_sequence(env, b, &s2);
    expect("rebind", st == NFS4_OK, "the server still answers: %u", st);
    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
    evpl_rpc2_client_disconnect(env->rpc2_thread, b);
} /* check_rebind */

static void
check_destroy(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sl_connect(env);
    struct evpl_rpc2_conn *b = sl_connect(env);
    struct sl_session      s1, s3;
    uint64_t               clientid;
    uint32_t               csa_seq, st;

    printf("destroy: the session destroyed, then the connection closed, "
           "under a parked COMPOUND\n");
    clientid = sl_exchange_id(env, a, "sl-destroy", &csa_seq);
    sl_create_session(env, a, clientid, &csa_seq, &s1);
    sl_create_session(env, b, clientid, &csa_seq, &s3);

    sl_park(env, a, &s1, &parked_reply);
    st = sl_destroy_session(env, b, &s1);
    expect("destroy", st == NFS4_OK, "DESTROY_SESSION(S1) from another "
           "connection: %u", st);
    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
    sl_pump(env, 300, -1);
    sl_release(env);

    st = sl_sequence(env, b, &s3);
    expect("destroy", st == NFS4_OK, "the server still answers: %u", st);
    evpl_rpc2_client_disconnect(env->rpc2_thread, b);
} /* check_destroy */

static void
check_replay(struct mbt_env *env)
{
    struct evpl_rpc2_conn *a = sl_connect(env);
    struct sl_session      s1;
    struct nfs_argop4      ops[1];
    struct sl_reply        first, again;
    uint64_t               clientid;
    uint32_t               csa_seq, cs_seq, seq;

    printf("replay: SEQUENCE(S1) + CREATE_SESSION, retransmitted\n");
    clientid = sl_exchange_id(env, a, "sl-replay", &csa_seq);
    sl_create_session(env, a, clientid, &csa_seq, &s1);

    cs_seq = csa_seq;
    seq    = s1.seq;
    sl_create_session_op(&ops[0], clientid, cs_seq);
    sl_send(env, a, &s1, 1, ops, 1, &first);
    sl_wait(env, &first);
    expect("replay", first.status == NFS4_OK && first.nres == 2 &&
           first.st[0] == NFS4_OK && first.st[1] == NFS4_OK,
           "the COMPOUND: %u, SEQUENCE %u, CREATE_SESSION %u", first.status,
           first.st[0], first.st[1]);

    /* The same slot, sequence id and operations again. */
    s1.seq = seq;
    sl_create_session_op(&ops[0], clientid, cs_seq);
    sl_send(env, a, &s1, 1, ops, 1, &again);
    sl_wait(env, &again);
    expect("replay", again.status == NFS4_OK && again.nres == 2 &&
           again.st[0] == NFS4_OK && again.st[1] == NFS4_OK &&
           memcmp(again.sessionid, first.sessionid, NFS4_SESSIONID_SIZE) == 0,
           "the retransmit: %u, SEQUENCE %u, CREATE_SESSION %u, same new "
           "session %s", again.status, again.st[0], again.st[1],
           memcmp(again.sessionid, first.sessionid,
                  NFS4_SESSIONID_SIZE) == 0 ? "yes" : "no");

    evpl_rpc2_client_disconnect(env->rpc2_thread, a);
} /* check_replay */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts opts  = { .disable_caches = 1 };
    struct mbt_env     *env   = malloc(sizeof(*env));
    const char         *check = NULL;
    int                 i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            opts.module = argv[++i];
        } else if (strcmp(argv[i], "--check") == 0 && i + 1 < argc) {
            check = argv[++i];
        }
    }
    opts.sec = mbt_sec_scan_argv(argc, argv);
    if (opts.sec != MBT_SEC_SYS) {
        printf("AUTH_SYS only; skipping\n");
        free(env);
        return 77;
    }
    if (!check) {
        fprintf(stderr, "usage: %s --check close|rebind|destroy|replay\n",
                argv[0]);
        free(env);
        return 2;
    }

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);

    if (strcmp(check, "close") == 0) {
        check_close(env);
    } else if (strcmp(check, "rebind") == 0) {
        check_rebind(env);
    } else if (strcmp(check, "destroy") == 0) {
        check_destroy(env);
    } else if (strcmp(check, "replay") == 0) {
        check_replay(env);
    } else {
        fprintf(stderr, "unknown check %s\n", check);
        mbt_env_stop(env);
        free(env);
        return 2;
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
