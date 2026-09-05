// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground truth for the duplicate-request-cache model.
 *
 * The nfsdrc model says what a correct cache does.  Several of its
 * configuration constants are not statements about correctness at all but
 * about the server and the transport this harness runs against -- whether two
 * in-process connections look like one client, whether a re-created name gets
 * a new filehandle, whether the NFSv4 and NFSv3 handle bytes agree.  A wrong
 * guess at any of those makes every trace fail for a reason that has nothing
 * to do with the cache.
 *
 * This probe measures them, one narrow question at a time, so the model's
 * constants are recorded rather than assumed.  It doubles as the regression
 * test for the cache behaviour the traces then exercise in bulk: each check
 * below is a property, stated in the terms the model uses, that a correct
 * server has to satisfy.
 *
 * Run with --dump to print what was observed even for the checks that passed.
 */

#include "nfs_drc_mbt_common.h"

static int dump      = 0;
static int no_caches = 0;
static int failures  = 0;

static void
report(
    const char *name,
    int         ok,
    const char *fmt,
    ...)
{
    va_list ap;

    if (!ok) {
        failures++;
    }
    if (ok && !dump) {
        printf("ok   %s\n", name);
        return;
    }
    printf("%-4s %s: ", ok ? "ok" : "FAIL", name);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
} /* report */

/* One observed status against the one the model predicts. */
static void
check_status(
    const char *name,
    uint32_t    got,
    uint32_t    want)
{
    report(name, got == want, "status=%u, model says %u", got, want);
} /* check_status */

/* ---- NFSv3 helpers, parameterised by connection / credential / xid ------- */

static uint32_t
v3_mkdir(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name,
    struct mbt_fh               *out_fh)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_mkdir(c->env, &c->dir_fh, name, (uint32_t) strlen(name), DRC_MODE);
    drc_v3_end(c);
    if (out_fh && r->obj_fh.has) {
        *out_fh = r->obj_fh;
    }
    return r->status;
} /* v3_mkdir */

static uint32_t
v3_remove(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_remove(c->env, &c->dir_fh, name, (uint32_t) strlen(name));
    drc_v3_end(c);
    return r->status;
} /* v3_remove */

static uint32_t
v3_rmdir(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_rmdir(c->env, &c->dir_fh, name, (uint32_t) strlen(name));
    drc_v3_end(c);
    return r->status;
} /* v3_rmdir */

static uint32_t
v3_lookup(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_lookup(c->env, &c->dir_fh, name, (uint32_t) strlen(name));
    drc_v3_end(c);
    return r->status;
} /* v3_lookup */

static uint32_t
v3_getattr_st(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const struct mbt_fh         *fh)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_getattr(c->env, fh);
    drc_v3_end(c);
    return r->status;
} /* v3_getattr_st */

static uint32_t
v3_setsize(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const struct mbt_fh         *fh,
    int64_t                      size)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_setattr(c->env, fh, -1, size, NULL);
    drc_v3_end(c);
    return r->status;
} /* v3_setsize */

static uint32_t
v3_create(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name,
    struct mbt_fh               *out_fh)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_create(c->env, &c->dir_fh, name, (uint32_t) strlen(name), GUARDED,
                   DRC_MODE, NULL);
    drc_v3_end(c);
    if (out_fh && r->obj_fh.has) {
        *out_fh = r->obj_fh;
    }
    return r->status;
} /* v3_create */

static uint32_t
v3_symlink(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_symlink(c->env, &c->dir_fh, name, (uint32_t) strlen(name), "t",
                    DRC_MODE);
    drc_v3_end(c);
    return r->status;
} /* v3_symlink */

static uint32_t
v3_rename(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *from,
    const char                  *to)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_rename(c->env, &c->dir_fh, from, (uint32_t) strlen(from),
                   &c->dir_fh, to, (uint32_t) strlen(to));
    drc_v3_end(c);
    return r->status;
} /* v3_rename */

static uint32_t
v3_link(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const struct mbt_fh         *src,
    const char                  *to)
{
    struct mbt_result *r;

    drc_v3_begin(c, conn, cred, xid);
    r = mbt_link(c->env, src, &c->dir_fh, to, (uint32_t) strlen(to));
    drc_v3_end(c);
    return r->status;
} /* v3_link */

/* ---- NFSv4.0 helpers ------------------------------------------------------ */

static uint32_t
v4_create_dir(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct nfs_argop4 a[2];
    struct drc_v4_res res;
    uint32_t          bitmap[2] = { 0, 1U << (FATTR4_MODE - 32) };
    uint8_t           blob[4]   = { 0, 0, 0x01, 0xff };  /* mode 0777 */

    memset(a, 0, sizeof(a));
    drc_v4_putfh(&a[0], &c->dir_fh);
    a[1].argop                               = OP_CREATE;
    a[1].opcreate.objtype.type               = NF4DIR;
    a[1].opcreate.objname.data               = (void *) name;
    a[1].opcreate.objname.len                = (uint32_t) strlen(name);
    a[1].opcreate.createattrs.num_attrmask   = 2;
    a[1].opcreate.createattrs.attrmask       = bitmap;
    a[1].opcreate.createattrs.attr_vals.data = blob;
    a[1].opcreate.createattrs.attr_vals.len  = 4;

    drc_v4_call(c, conn, cred, xid, a, 2, 0, &res);
    return res.status;
} /* v4_create_dir */

static uint32_t
v4_remove(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct nfs_argop4 a[2];
    struct drc_v4_res res;

    memset(a, 0, sizeof(a));
    drc_v4_putfh(&a[0], &c->dir_fh);
    a[1].argop                = OP_REMOVE;
    a[1].opremove.target.data = (void *) name;
    a[1].opremove.target.len  = (uint32_t) strlen(name);

    drc_v4_call(c, conn, cred, xid, a, 2, 0, &res);
    return res.status;
} /* v4_remove */

static uint32_t
v4_lookup(
    struct drc_ctx              *c,
    int                          conn,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name)
{
    struct nfs_argop4 a[2];
    struct drc_v4_res res;

    memset(a, 0, sizeof(a));
    drc_v4_putfh(&a[0], &c->dir_fh);
    a[1].argop                 = OP_LOOKUP;
    a[1].oplookup.objname.data = (void *) name;
    a[1].oplookup.objname.len  = (uint32_t) strlen(name);

    drc_v4_call(c, conn, cred, xid, a, 2, 0, &res);
    return res.status;
} /* v4_lookup */

/* Resolve the working directory in the NFSv4 namespace: the pseudo-root, then
 * the export, then the directory the trace works in.  Whether the bytes agree
 * with the NFSv3 handle for the same object is one of the things this probe
 * exists to answer, so they are obtained independently. */
static void
v4_resolve_dir(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *cred,
    struct mbt_fh               *out)
{
    struct nfs_argop4 a[4];
    struct drc_v4_res res;

    memset(a, 0, sizeof(a));
    a[0].argop                 = OP_PUTROOTFH;
    a[1].argop                 = OP_LOOKUP;
    a[1].oplookup.objname.data = (void *) (c->mntpath + 1);
    a[1].oplookup.objname.len  = (uint32_t) strlen(c->mntpath + 1);
    a[2].argop                 = OP_LOOKUP;
    a[2].oplookup.objname.data = (void *) DRC_DIR_NAME;
    a[2].oplookup.objname.len  = (uint32_t) strlen(DRC_DIR_NAME);
    a[3].argop                 = OP_GETFH;

    drc_v4_call(c, 0, cred, 900001, a, 4, 0, &res);
    if (res.status != NFS4_OK || !res.fh.has) {
        fprintf(stderr, "drc probe: cannot resolve the working directory in "
                "the NFSv4 namespace (status %u)\n", res.status);
        exit(2);
    }
    *out = res.fh;
} /* v4_resolve_dir */


/* ---- NFSv4.1: the session reply cache ------------------------------------ */

/*
 * The 4.1 reply cache is a different mechanism from the two this suite models
 * -- a slot table keyed by sequence id rather than a key over transport
 * identity -- and it belongs with the sessions in the nfs4 model.  What is
 * pinned here are the three answers a retry can get, because each is a
 * normative requirement that is easy to get wrong and hard to reach from a
 * random walk:
 *
 *   the reply was cached     replay it
 *   it was not               SEQUENCE answers NFS4_OK and the operation AFTER
 *                            it carries NFS4ERR_RETRY_UNCACHED_REP.  RFC 8881
 *                            Section 2.10.6.1.3 forbids answering the Sequence
 *                            operation itself with that error when Sequence is
 *                            first, which it always is.
 *   a different principal    NFS4ERR_SEQ_FALSE_RETRY, which Section
 *                            2.10.6.1.3.1 makes a MUST
 */

struct drc_session {
    uint64_t clientid;
    uint32_t seqid;              /* csr_sequence from CREATE_SESSION */
    uint8_t  id[NFS4_SESSIONID_SIZE];
};

static void
v41_establish(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *cred,
    struct drc_session          *out)
{
    struct nfs_argop4                 a[1];
    struct drc_v4_res                 res;
    struct channel_attrs4             chan = {
        .ca_headerpadsize          = 0,
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
        .num_ca_rdma_ird           = 0,
    };
    static struct callback_sec_parms4 sec     = { .cb_secflavor = 0 };
    static uint8_t                    verf[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    static const char                *owner   = "quintdrc-client";

    memset(a, 0, sizeof(a));
    a[0].argop = OP_EXCHANGE_ID;
    memcpy(a[0].opexchange_id.eia_clientowner.co_verifier, verf, 8);
    a[0].opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    a[0].opexchange_id.eia_clientowner.co_ownerid.len  =
        (uint32_t) strlen(owner);
    a[0].opexchange_id.eia_flags                 = 0;
    a[0].opexchange_id.eia_state_protect.spa_how = SP4_NONE;
    a[0].opexchange_id.num_eia_client_impl_id    = 0;

    drc_v4_call(c, 0, cred, 800001, a, 1, 1, &res);
    if (res.status != NFS4_OK) {
        fprintf(stderr, "drc probe: EXCHANGE_ID failed: %u\n", res.status);
        exit(2);
    }
    out->clientid = res.clientid;
    out->seqid    = res.seqid;

    memset(a, 0, sizeof(a));
    a[0].argop                                = OP_CREATE_SESSION;
    a[0].opcreate_session.csa_clientid        = out->clientid;
    a[0].opcreate_session.csa_sequence        = out->seqid;
    a[0].opcreate_session.csa_flags           = 0;
    a[0].opcreate_session.csa_fore_chan_attrs = chan;
    a[0].opcreate_session.csa_back_chan_attrs = chan;
    a[0].opcreate_session.csa_cb_program      = 0x40000000;
    a[0].opcreate_session.num_csa_sec_parms   = 1;
    a[0].opcreate_session.csa_sec_parms       = &sec;

    drc_v4_call(c, 0, cred, 800002, a, 1, 1, &res);
    if (res.status != NFS4_OK) {
        fprintf(stderr, "drc probe: CREATE_SESSION failed: %u\n", res.status);
        exit(2);
    }
    memcpy(out->id, res.sessionid, NFS4_SESSIONID_SIZE);
} /* v41_establish */

/* SEQUENCE(slot, seq, cachethis) followed by one GETATTR of the working
 * directory, so the reply has a second operation for the uncached-retry error
 * to land on.  Returns the compound status; the per-operation statuses are in
 * *out. */
static uint32_t
v41_seq_getattr(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *cred,
    struct drc_session          *sess,
    uint32_t                     slot,
    uint32_t                     seq,
    int                          cachethis,
    struct drc_v4_res           *out)
{
    struct nfs_argop4 a[3];
    uint32_t          bitmap[1] = { 1U << FATTR4_TYPE };

    memset(a, 0, sizeof(a));
    a[0].argop = OP_SEQUENCE;
    memcpy(a[0].opsequence.sa_sessionid, sess->id, NFS4_SESSIONID_SIZE);
    a[0].opsequence.sa_sequenceid     = seq;
    a[0].opsequence.sa_slotid         = slot;
    a[0].opsequence.sa_highest_slotid = 31;
    a[0].opsequence.sa_cachethis      = cachethis;

    drc_v4_putfh(&a[1], &c->dir_fh);

    a[2].argop                      = OP_GETATTR;
    a[2].opgetattr.attr_request     = bitmap;
    a[2].opgetattr.num_attr_request = 1;

    /* A fixed XID: the NFSv4.0 cache does not look at 4.1 COMPOUNDs, so
     * nothing here depends on it. */
    drc_v4_call(c, 0, cred, 810000 + seq, a, 3, 1, out);
    return out->status;
} /* v41_seq_getattr */

/* ------------------------------------------------------------------------- */

int
main(
    int   argc,
    char *argv[])
{
    struct mbt_env        env;
    struct mbt_env_opts   opts;
    struct drc_ctx        c;
    struct evpl_rpc2_cred u1, u2;
    struct mbt_fh         fh_a1 = { 0 }, fh_a2 = { 0 };
    struct mbt_fh         v3_dir = { 0 }, v4_dir = { 0 };
    struct drc_dirsnap    snap;
    uint32_t              st1, st2;
    int                   i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--dump") == 0) {
            dump = 1;
        } else if (strcmp(argv[i], "--no-caches") == 0) {
            /* What the replay harness runs with: exact comparison of the
             * directory after every step cannot tolerate a stale cache. */
            no_caches = 1;
        }
    }

    umask(0);

    memset(&opts, 0, sizeof(opts));
    opts.sec            = mbt_sec_scan_argv(argc, argv);
    opts.nfs3_drc       = 1;
    opts.disable_caches = no_caches;

    mbt_env_open_opts(&env, &opts);
    mbt_env_fs_setup(&env, "fs0");

    memset(&c, 0, sizeof(c));
    c.env = &env;
    snprintf(c.mntpath, sizeof(c.mntpath), "/fs0");

    drc_cred_init(&u1, 1000);
    drc_cred_init(&u2, 1001);

    /* The shared wrappers used by trace setup run as root, which is what
     * creates the 0777 working directory the two users then share. */
    drc_trace_setup(&c);
    v3_dir = c.dir_fh;

    /* ------------------------------------------------------------------ *
    *  1. Does a retransmit replay?  The whole premise.                   *
    * ------------------------------------------------------------------ */
    st1 = v3_mkdir(&c, 0, &u1, 100, "a", &fh_a1);
    st2 = v3_mkdir(&c, 0, &u1, 100, "a", NULL);
    report("v3/replay-same-conn", st1 == NFS3_OK && st2 == NFS3_OK,
           "first=%u second=%u (a re-executed MKDIR would answer %u)",
           st1, st2, NFS3ERR_EXIST);

    /* ------------------------------------------------------------------ *
    *  2. A different XID is a different request.                         *
    * ------------------------------------------------------------------ */
    st2 = v3_mkdir(&c, 0, &u1, 101, "a", NULL);
    report("v3/other-xid-misses", st2 == NFS3ERR_EXIST, "status=%u", st2);

    /* ------------------------------------------------------------------ *
    *  3. The procedure is part of the key.  REMOVE and RMDIR carry       *
    *     byte-identical arguments, so only the procedure number tells    *
    *     them apart.                                                     *
    * ------------------------------------------------------------------ */
    st1 = v3_remove(&c, 0, &u1, 110, "a");           /* "a" is a directory */
    st2 = v3_rmdir(&c, 0, &u1, 110, "a");
    report("v3/proc-in-key", st2 == NFS3_OK,
           "REMOVE(dir)=%u then RMDIR at the same xid=%u "
           "(a key without the procedure would replay %u)", st1, st2, st1);

    /* ------------------------------------------------------------------ *
    *  4. Is a non-cacheable procedure ever answered from the cache?      *
    *     A LOOKUP's arguments are byte-identical to the REMOVE above.    *
    * ------------------------------------------------------------------ */
    (void) v3_mkdir(&c, 0, &u1, 120, "b", NULL);
    st2 = v3_remove(&c, 0, &u1, 121, "b");   /* a directory: ISDIR, no effect */
    st1 = v3_lookup(&c, 0, &u1, 121, "b");
    report("v3/idempotent-not-cached", st1 == NFS3_OK,
           "REMOVE(dir)=%u then LOOKUP at the same xid=%u "
           "(%u would mean the LOOKUP was answered from the REMOVE's entry)",
           st2, st1, st2);

    /* ------------------------------------------------------------------ *
    *  5. Two connections, one address.  Under an in-process transport    *
    *     every peer reports the same address, so an address-scoped entry *
    *     is shared -- which is what the model's ADDR_OF records.         *
    * ------------------------------------------------------------------ */
    st1 = v3_mkdir(&c, 0, &u1, 130, "c", NULL);
    st2 = v3_mkdir(&c, 1, &u1, 130, "c", NULL);
    report("v3/scope-is-address", st2 == NFS3_OK,
           "conn0=%u conn1=%u -- %s", st1, st2,
           st2 == NFS3_OK ? "the two connections share one address"
                          : "the two connections are separate clients");

    /* ------------------------------------------------------------------ *
    *  6. A reconnect must not lose an address-scoped entry: a reconnect  *
    *     is the ordinary reason a retransmit exists.                     *
    * ------------------------------------------------------------------ */
    drc_reconnect(&c, 0);
    st2 = v3_mkdir(&c, 0, &u1, 130, "c", NULL);
    report("v3/survives-reconnect", st2 == NFS3_OK, "status=%u", st2);

    /* ------------------------------------------------------------------ *
    *  7. A different principal is a different requester.                 *
    * ------------------------------------------------------------------ */
    st2 = v3_mkdir(&c, 0, &u2, 130, "c", NULL);
    report("v3/cred-in-key", st2 == NFS3ERR_EXIST,
           "the same MKDIR under a second uid = %u "
           "(%u would mean one user was answered from the other's entry)",
           st2, NFS3_OK);

    /* ------------------------------------------------------------------ *
    *  8. A handle survives its object.  A retransmit carries the handle  *
    *     it was built with, so the model must distinguish incarnations.  *
    * ------------------------------------------------------------------ */
    (void) v3_setsize(&c, 0, &u1, 140, &fh_a1, 4096);
    (void) v3_rmdir(&c, 0, &u1, 141, "c");
    (void) v3_mkdir(&c, 0, &u1, 142, "c", &fh_a2);
    report("v3/fh-per-incarnation",
           fh_a1.has && fh_a2.has && !mbt_fh_eq(&fh_a1, &fh_a2),
           "a re-created name %s a new filehandle",
           mbt_fh_eq(&fh_a1, &fh_a2) ? "keeps" : "gets");

    /* ------------------------------------------------------------------ *
    *  9. NFSv4.0: the same questions, connection-scoped.                 *
    * ------------------------------------------------------------------ */
    v4_resolve_dir(&c, &u1, &v4_dir);
    report("v4/handle-matches-v3", mbt_fh_eq(&v3_dir, &v4_dir),
           "the NFSv4 handle for the working directory %s the NFSv3 one",
           mbt_fh_eq(&v3_dir, &v4_dir) ? "equals" : "differs from");

    st1 = v4_create_dir(&c, 0, &u1, 200, "e");
    st2 = v4_create_dir(&c, 0, &u1, 200, "e");
    report("v4/replay-same-conn", st1 == NFS4_OK && st2 == NFS4_OK,
           "first=%u second=%u (a re-executed CREATE would answer %u)",
           st1, st2, NFS4ERR_EXIST);

    st2 = v4_create_dir(&c, 1, &u1, 200, "e");
    report("v4/scope-is-connection", st2 == NFS4ERR_EXIST,
           "the same COMPOUND on a second connection = %u "
           "(%u would mean the entry was not connection-scoped)",
           st2, NFS4_OK);

    /* ------------------------------------------------------------------ *
    * 10. A reconnect drops a connection-scoped entry -- including when   *
    *     the replacement is handed the memory the old one just freed.    *
    * ------------------------------------------------------------------ */
    drc_reconnect(&c, 0);
    st2 = v4_create_dir(&c, 0, &u1, 200, "e");
    report("v4/dropped-on-reconnect", st2 == NFS4ERR_EXIST,
           "the same COMPOUND after a reconnect = %u "
           "(%u would mean a recycled connection inherited a cache)",
           st2, NFS4_OK);

    /* ------------------------------------------------------------------ *
    * 11. A read-only COMPOUND is never cached.  The v4 cache looks every *
    *     COMPOUND up, so this is a real question rather than a           *
    *     structural impossibility as it is for v3.                       *
    * ------------------------------------------------------------------ */
    (void) v4_remove(&c, 0, &u1, 210, "e");
    st2 = v4_lookup(&c, 0, &u1, 210, "e");
    report("v4/idempotent-not-cached", st2 == NFS4ERR_NOENT,
           "LOOKUP after a REMOVE at the same xid = %u", st2);

    (void) v4_lookup(&c, 0, &u1, 211, "e");
    st2 = v4_create_dir(&c, 0, &u1, 211, "e");
    report("v4/lookup-leaves-no-entry", st2 == NFS4_OK,
           "CREATE at a LOOKUP's xid = %u", st2);

    /* ------------------------------------------------------------------ *
    * 12. The per-connection cache is bounded, and evicts oldest-first.   *
    *     Sixteen entries plus one pushes the first out, so its           *
    *     retransmit re-executes.                                         *
    * ------------------------------------------------------------------ */
    (void) v4_remove(&c, 0, &u1, 212, "e");
    for (i = 0; i < 17; i++) {
        char name[DRC_NAME_MAX];

        snprintf(name, sizeof(name), "v%d", i);
        st1 = v4_create_dir(&c, 0, &u1, (uint32_t) (300 + i), name);
        if (st1 != NFS4_OK) {
            fprintf(stderr, "drc probe: CREATE %s failed: %u\n", name, st1);
            exit(2);
        }
    }
    st2 = v4_create_dir(&c, 0, &u1, 300, "v0");
    report("v4/oldest-evicted", st2 == NFS4ERR_EXIST,
           "the oldest of 17 entries replayed as %u "
           "(%u would mean it was still cached)", st2, NFS4_OK);

    st2 = v4_create_dir(&c, 0, &u1, 316, "v16");
    report("v4/newest-retained", st2 == NFS4_OK,
           "the newest of 17 entries replayed as %u", st2);

    /* ------------------------------------------------------------------ *
    * 13. A different principal on an NFSv4.0 COMPOUND.                   *
    * ------------------------------------------------------------------ */
    st2 = v4_create_dir(&c, 0, &u2, 316, "v16");
    report("v4/cred-in-key", st2 == NFS4ERR_EXIST,
           "the same COMPOUND under a second uid = %u "
           "(%u would mean one user was answered from the other's entry)",
           st2, NFS4_OK);

    /* ------------------------------------------------------------------ *
    * 14. The operation semantics the model's filesystem slice encodes.   *
    *     None of these are about the cache; they are what the cache is    *
    *     replaying, and a wrong guess at any of them would fail every     *
    *     trace for a reason that has nothing to do with a cache.  Each is *
    *     run at a fresh XID so nothing here is answered from a cache.     *
    * ------------------------------------------------------------------ */
    {
        struct mbt_fh f_fh, d_fh, gone_fh;
        uint32_t      x = 400;

        check_status("op/create-guarded-new",
                     v3_create(&c, 0, &u1, x++, "f", &f_fh), NFS3_OK);
        check_status("op/create-guarded-exists",
                     v3_create(&c, 0, &u1, x++, "f", NULL), NFS3ERR_EXIST);
        check_status("op/mkdir-exists",
                     v3_mkdir(&c, 0, &u1, x++, "f", NULL), NFS3ERR_EXIST);
        check_status("op/symlink-exists",
                     v3_symlink(&c, 0, &u1, x++, "f"), NFS3ERR_EXIST);

        check_status("op/remove-absent",
                     v3_remove(&c, 0, &u1, x++, "zz"), NFS3ERR_NOENT);
        check_status("op/rmdir-absent",
                     v3_rmdir(&c, 0, &u1, x++, "zz"), NFS3ERR_NOENT);
        check_status("op/rmdir-not-a-directory",
                     v3_rmdir(&c, 0, &u1, x++, "f"), NFS3ERR_NOTDIR);

        check_status("op/mkdir-new",
                     v3_mkdir(&c, 0, &u1, x++, "g", &d_fh), NFS3_OK);
        check_status("op/remove-a-directory",
                     v3_remove(&c, 0, &u1, x++, "g"), NFS3ERR_ISDIR);

        check_status("op/rename-absent-source",
                     v3_rename(&c, 0, &u1, x++, "zz", "yy"), NFS3ERR_NOENT);
        check_status("op/rename-free-target",
                     v3_rename(&c, 0, &u1, x++, "f", "h"), NFS3_OK);

        check_status("op/link-free-target",
                     v3_link(&c, 0, &u1, x++, &f_fh, "i"), NFS3_OK);
        check_status("op/link-taken-target",
                     v3_link(&c, 0, &u1, x++, &f_fh, "i"), NFS3ERR_EXIST);

        check_status("op/setsize-file",
                     v3_setsize(&c, 0, &u1, x++, &f_fh, 4096), NFS3_OK);
        check_status("op/setsize-directory",
                     v3_setsize(&c, 0, &u1, x++, &d_fh, 0), NFS3ERR_ISDIR);

        /*
         * A handle whose object is gone is NFS3ERR_STALE (RFC 1813 section 3.3:
         * "the file referred to by that file handle no longer exists").  It is
         * also the state a retransmit can legitimately be in, which is what the
         * replay check further down turns on.
         */
        (void) v3_create(&c, 0, &u1, x++, "j", &gone_fh);
        (void) v3_remove(&c, 0, &u1, x++, "j");
        check_status("op/setsize-stale-handle",
                     v3_setsize(&c, 0, &u1, x++, &gone_fh, 0), NFS3ERR_STALE);
        check_status("op/link-stale-handle",
                     v3_link(&c, 0, &u1, x++, &gone_fh, "k"), NFS3ERR_STALE);

        /* A removed directory's handle, the same way. */
        {
            struct mbt_fh dead_dir;

            (void) v3_mkdir(&c, 0, &u1, x++, "n", &dead_dir);
            (void) v3_rmdir(&c, 0, &u1, x++, "n");
            check_status("op/getattr-removed-directory",
                         v3_getattr_st(&c, 0, &u1, x++, &dead_dir),
                         NFS3ERR_STALE);
        }

        /* The same directory removed through NFSv4 REMOVE rather than NFSv3
         * RMDIR.  Same object, same removal, so the handle dies the same way --
         * which protocol asked is not something a filehandle knows. */
        {
            struct mbt_fh v4_dead_dir;

            (void) v3_mkdir(&c, 0, &u1, x++, "o", &v4_dead_dir);
            check_status("op/v4-remove-directory",
                         v4_remove(&c, 0, &u1, x++, "o"), NFS4_OK);
            check_status("op/getattr-v4-removed-directory",
                         v3_getattr_st(&c, 0, &u1, x++, &v4_dead_dir),
                         NFS3ERR_STALE);
        }

        /*
         * The same removal, but with the handle USED before it.
         *
         * Resolving a handle puts it in the VFS handle cache, which pins the
         * object until the close sweep releases it -- so it still resolves,
         * and GETATTR still succeeds, for a window afterwards.  That is not a
         * defect and it is why the model never issues a FRESH request on a
         * dead handle: Linux behaves the same way, because __ext4_iget's
         * free-inode check sits after the "already cached" early return and an
         * inode pinned by an open file or a dentry is returned without it.
         * NFSv4 read-after-unlink depends on exactly this.
         *
         * It is asserted rather than merely tolerated because the alternative
         * -- a handle going stale the instant its last name does, whatever
         * still holds the object -- would break that.
         */
        {
            struct mbt_fh touched;

            (void) v3_mkdir(&c, 0, &u1, x++, "t1", &touched);
            check_status("op/getattr-live-directory",
                         v3_getattr_st(&c, 0, &u1, x++, &touched), NFS3_OK);
            (void) v3_rmdir(&c, 0, &u1, x++, "t1");
            check_status("op/getattr-pinned-removed-directory",
                         v3_getattr_st(&c, 0, &u1, x++, &touched), NFS3_OK);
        }

        /* The same, for a regular file. */
        {
            struct mbt_fh tf;

            (void) v3_create(&c, 0, &u1, x++, "t2", &tf);
            check_status("op/getattr-live-file",
                         v3_getattr_st(&c, 0, &u1, x++, &tf), NFS3_OK);
            (void) v3_remove(&c, 0, &u1, x++, "t2");
            check_status("op/getattr-pinned-removed-file",
                         v3_getattr_st(&c, 0, &u1, x++, &tf), NFS3_OK);
        }

        /* And a directory both created and removed through NFSv4. */
        {
            struct mbt_fh v4_dir;

            check_status("op/v4-create-directory",
                         v4_create_dir(&c, 0, &u1, x++, "p4"), NFS4_OK);
            drc_v3_begin(&c, 0, &u1, x++);
            {
                struct mbt_result *lr = mbt_lookup(&env, &c.dir_fh, "p4", 2);

                v4_dir = lr->obj_fh;
            }
            drc_v3_end(&c);
            check_status("op/v4-remove-v4-directory",
                         v4_remove(&c, 0, &u1, x++, "p4"), NFS4_OK);
            check_status("op/getattr-v4-created-removed-directory",
                         v3_getattr_st(&c, 0, &u1, x++, &v4_dir),
                         NFS3ERR_STALE);
        }

        /* RENAME over an occupied target: replaced when the types agree, and
         * refused by type when they do not (RFC 1813 3.3.14 / POSIX
         * rename(2)).  Renaming a name onto another name for the same object
         * succeeds having done nothing. */
        {
            struct mbt_fh shared;

            (void) v3_create(&c, 0, &u1, x++, "p", &shared);
            (void) v3_create(&c, 0, &u1, x++, "q", NULL);
            check_status("op/rename-replaces-same-type",
                         v3_rename(&c, 0, &u1, x++, "p", "q"), NFS3_OK);
            (void) v3_mkdir(&c, 0, &u1, x++, "r", NULL);
            check_status("op/rename-file-onto-directory",
                         v3_rename(&c, 0, &u1, x++, "q", "r"), NFS3ERR_ISDIR);
            (void) v3_create(&c, 0, &u1, x++, "s", NULL);
            check_status("op/rename-directory-onto-file",
                         v3_rename(&c, 0, &u1, x++, "r", "s"), NFS3ERR_NOTDIR);
            /* Two names for one object. */
            (void) v3_link(&c, 0, &u1, x++, &shared, "u");
            check_status("op/rename-onto-same-object",
                         v3_rename(&c, 0, &u1, x++, "q", "u"), NFS3_OK);
        }

        /* A size on a symlink is not a size at all. */
        {
            struct mbt_fh lnk;

            (void) v3_symlink(&c, 0, &u1, x++, "w");
            drc_v3_begin(&c, 0, &u1, x++);
            {
                struct mbt_result *lr = mbt_lookup(&env, &c.dir_fh, "w", 1);

                lnk = lr->obj_fh;
            }
            drc_v3_end(&c);
            check_status("op/setsize-symlink",
                         v3_setsize(&c, 0, &u1, x++, &lnk, 0), NFS3ERR_INVAL);
        }

        /* NFSv4 REMOVE covers both unlink and rmdir, so it has no type
         * assertion of its own to make. */
        check_status("op/v4-create-a-directory",
                     v4_create_dir(&c, 0, &u1, x++, "m"), NFS4_OK);
        check_status("op/v4-remove-a-directory",
                     v4_remove(&c, 0, &u1, x++, "m"), NFS4_OK);
        check_status("op/v4-lookup-absent",
                     v4_lookup(&c, 0, &u1, x++, "zz"), NFS4ERR_NOENT);
    }

    /* ------------------------------------------------------------------ *
    * 15. A retransmit carrying a handle whose object is gone.            *
    *                                                                     *
    *     The most demanding thing a cache does.  The client sent a        *
    *     SETATTR, lost the reply, and removed the file before retrying;   *
    *     the retry still carries the handle it was built with.  Answering *
    *     it means not resolving that handle at all -- a server that       *
    *     re-executed would answer for an object that no longer exists.    *
    *     The random corpus does not generate this (what a FRESH request on *
    *     a dead handle answers depends on chimera's own handle cache, and *
    *     a trace that walked into it would be flaky); here it is exact.   *
    * ------------------------------------------------------------------ */
    {
        struct mbt_fh doomed;

        uint32_t      retry;

        (void) v3_create(&c, 0, &u1, 500, "z", &doomed);
        st1   = v3_setsize(&c, 0, &u1, 501, &doomed, 4096);
        st2   = v3_remove(&c, 0, &u1, 502, "z");
        retry = v3_setsize(&c, 0, &u1, 501, &doomed, 4096);
        report("drc/replay-outlives-its-object",
               st1 == NFS3_OK && st2 == NFS3_OK && retry == NFS3_OK,
               "SETATTR=%u REMOVE=%u, and the retransmitted SETATTR answered "
               "%u after its file was removed", st1, st2, retry);
    }

    /* ------------------------------------------------------------------ *
    * 16. The NFSv4.1 session reply cache: the three answers a retry gets. *
    * ------------------------------------------------------------------ */
    {
        struct drc_session sess;
        struct drc_v4_res  r;

        v41_establish(&c, &u1, &sess);

        /* A cached request, then its retry: replayed. */
        st1 = v41_seq_getattr(&c, &u1, &sess, 0, 1, 1, &r);
        report("v41/first-sequence", st1 == NFS4_OK, "status=%u", st1);

        st1 = v41_seq_getattr(&c, &u1, &sess, 0, 1, 1, &r);
        report("v41/cached-retry-replays", st1 == NFS4_OK, "status=%u", st1);

        /*
         * A request whose reply was NOT cached, then its retry.
         *
         * RFC 8881 Section 2.10.6.1.1 makes the SEQUENCE reply itself always
         * cached, and Section 2.10.6.1.3 says a replier "MUST NOT return
         * NFS4ERR_RETRY_UNCACHED_REP in reply to a Sequence operation if the
         * Sequence operation is the first operation" -- it goes on the
         * operation after it instead.  Answering the SEQUENCE with the error
         * tells the client its session is unusable rather than that this one
         * reply is gone.
         */
        st1 = v41_seq_getattr(&c, &u1, &sess, 1, 1, 0, &r);
        report("v41/first-uncached-sequence", st1 == NFS4_OK, "status=%u", st1);

        st1 = v41_seq_getattr(&c, &u1, &sess, 1, 1, 0, &r);
        report("v41/uncached-retry-spares-sequence",
               r.nres >= 2 && r.resop[0] == OP_SEQUENCE &&
               r.opstatus[0] == NFS4_OK &&
               r.opstatus[1] == NFS4ERR_RETRY_UNCACHED_REP &&
               st1 == NFS4ERR_RETRY_UNCACHED_REP,
               "compound=%u, %d result(s), SEQUENCE=%u next=%u", st1, r.nres,
               r.nres > 0 ? r.opstatus[0] : 0,
               r.nres > 1 ? r.opstatus[1] : 0);

        /*
         * The same slot and sequence id under a different principal.
         *
         * RFC 8881 Section 2.10.6.1.3.1: "If the replier determines the users
         * are different between the original request and a retry, then the
         * replier MUST return NFS4ERR_SEQ_FALSE_RETRY."  Replaying instead
         * hands the second user the first one's reply.
         */
        st1 = v41_seq_getattr(&c, &u1, &sess, 2, 1, 1, &r);
        report("v41/first-sequence-slot2", st1 == NFS4_OK, "status=%u", st1);

        st1 = v41_seq_getattr(&c, &u2, &sess, 2, 1, 1, &r);
        report("v41/other-principal-is-a-false-retry",
               st1 == NFS4ERR_SEQ_FALSE_RETRY,
               "the same slot and sequence id under a second uid answered %u "
               "(%u would mean one user was handed the other's reply)",
               st1, NFS4_OK);

        /* An error from SEQUENCE leaves the slot alone (RFC 8881
         * Section 2.10.6.1.2), so the rightful owner still replays. */
        st1 = v41_seq_getattr(&c, &u1, &sess, 2, 1, 1, &r);
        report("v41/false-retry-leaves-the-slot", st1 == NFS4_OK,
               "status=%u", st1);
    }

    /* ------------------------------------------------------------------ *
    * 17. The directory the model predicts is the directory that is       *
    *     there -- the check every trace step makes.                      *
    * ------------------------------------------------------------------ */
    drc_dir_snapshot(&c, &snap);
    if (dump) {
        printf("     working directory holds %d entr%s:", snap.n,
               snap.n == 1 ? "y" : "ies");
        for (i = 0; i < snap.n; i++) {
            printf(" %s(type=%u,size=%llu)", snap.ents[i].name,
                   snap.ents[i].ftype,
                   (unsigned long long) snap.ents[i].size);
        }
        printf("\n");
    }
    report("dir/snapshot-readable", snap.n > 0, "%d entries", snap.n);

    drc_conns_close(&c);
    mbt_env_fs_teardown(&env, "fs0");
    mbt_env_stop(&env);

    if (failures) {
        printf("\n%d check(s) failed\n", failures);
        return 1;
    }
    printf("\nall checks passed\n");
    return 0;
} /* main */
