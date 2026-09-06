// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Harness support for the duplicate-request-cache model tests.
 *
 * The NFS3/NFS4 suites ask what a request does.  This one asks what a
 * *repeated* request does, which needs three things the other harnesses have
 * no reason to offer:
 *
 *   - control of the transaction id.  A retransmit is the same call under the
 *     same XID; an rpc2 client normally picks its own, monotonically, so
 *     reproducing a call means saying which XID it carries
 *     (evpl_rpc2_conn_set_next_xid).
 *   - more than one connection, and the ability to drop and reopen one.  The
 *     NFSv3 cache is scoped to a source address and must survive a reconnect;
 *     the NFSv4.0 cache is scoped to a connection and must not.  Only a trace
 *     that reconnects can tell those two apart.
 *   - more than one credential.  Two users behind one address can send
 *     byte-identical requests, and whether the server tells them apart is the
 *     difference between a cache and a way to lose one user's mutations.
 *
 * Everything else -- the server, the client event loop, the NFSv3 call
 * wrappers -- is nfs3_mbt_common.h's.  The NFSv3 wrappers there read
 * env->nfs_conn and env->cred, so a call on a particular connection under a
 * particular credential is made by swapping both for the duration of the call
 * (drc_v3_begin / drc_v3_end).  That is deliberately cheaper than duplicating
 * a thousand lines of wrappers, and the whole client is single-threaded and
 * issues one RPC at a time, so there is nothing for the swap to race with.
 */

#include "nfs3_mbt_common.h"

/* Model connections.  Two is what the suite needs; the extra room costs an
 * unused pointer each. */
#define DRC_MAX_CONNS   4

/* Filehandles remembered per (name, incarnation).  A trace of a few hundred
 * steps over three names creates each of them a few dozen times; 512 is well
 * clear of that and an overflow is a hard error rather than a silent reuse. */
#define DRC_MAX_HANDLES 512

#define DRC_NAME_MAX    32

/*
 * The test directory these traces work in, created fresh per trace inside the
 * export root.  A directory of its own rather than the export root itself so
 * its mode can be set without touching the root, and so a listing of it is
 * exactly the model's `dir` with nothing else in it.
 */
#define DRC_DIR_NAME    "d"

/* Both credentials are unprivileged, so that neither is the identity an export
 * might squash the other onto and the two are genuinely different principals.
 * The directory and everything created in it is 0777, so the two users are
 * interchangeable as far as permissions go -- which is the point: any
 * difference in what they observe is the cache's doing, not the DAC's. */
#define DRC_MODE        0777

struct drc_conn {
    struct evpl_rpc2_conn *conn;
    /*
     * How many times this connection has been reopened.  The model's cache
     * keys carry the same counter, because a reconnect must not inherit the
     * previous connection's entries -- and a server that keyed on a handle it
     * then recycles would do exactly that.
     */
    int                    gen;
};

/* One remembered filehandle, keyed by the identity the model gives the object
 * it names.  Objects rather than names, because that is what a handle names: a
 * hard link shares one, a rename carries one, and a name created again after a
 * removal gets a different one.
 *
 * A handle is kept after its object is gone.  That is the point -- a retransmit
 * carries the bytes it was built with, and answering it from the cache rather
 * than re-resolving them is exactly what a duplicate-request cache is for. */
struct drc_handle {
    int           id;
    struct mbt_fh fh;
};

struct drc_ctx {
    struct mbt_env        *env;

    struct drc_conn        conns[DRC_MAX_CONNS];

    /* Where the per-trace filesystem is mounted (fs_setup exports each
     * trace's filesystem under its own name), the export root, and the
     * per-trace working directory under it. */
    char                   mntpath[80];
    struct mbt_fh          root_fh;
    struct mbt_fh          dir_fh;

    struct drc_handle      handles[DRC_MAX_HANDLES];
    int                    nhandles;

    /* Saved across a drc_v3_begin/end pair. */
    struct evpl_rpc2_conn *saved_conn;
    struct evpl_rpc2_cred  saved_cred;
};

/* ---- credentials --------------------------------------------------------- */

static inline void
drc_cred_init(
    struct mbt_env        *env,
    struct evpl_rpc2_cred *cred,
    uint32_t               uid)
{
    /*
     * Whose call this is, said the way the flavor in force says it.
     *
     * The distinction this suite turns on -- that a retransmit from a
     * different requester is not a replay of somebody else's answer -- is
     * asserted differently by each flavor: AUTH_SYS names a uid and a machine,
     * RPCSEC_GSS names an authenticated principal and nothing else.  Chimera
     * folds whichever of those the call carried into the identity it caches
     * against, so a suite that only ever sent AUTH_SYS would leave the GSS arm
     * of that untested no matter which security flavor the cells claimed to
     * run under.
     */
    if (env->sec != MBT_SEC_SYS) {
        /* One context per user, established on the env's own connection and
         * used on whichever the suite is testing.  A context handle is
         * server-global, and a real client keeps one per (user, server) rather
         * than per connection, so this is both simpler and closer to life. */
        *cred = *mbt_cred_for_uid(env, env->nfs_conn, &env->nfs_v3.rpc2, uid);
        return;
    }

    memset(cred, 0, sizeof(*cred));
    cred->flavor                  = EVPL_RPC2_AUTH_SYS;
    cred->authsys.uid             = uid;
    cred->authsys.gid             = uid;
    cred->authsys.num_gids        = 0;
    cred->authsys.gids            = NULL;
    cred->authsys.machinename     = "quintdrc";
    cred->authsys.machinename_len = 8;
} /* drc_cred_init */

/* ---- connections --------------------------------------------------------- */

static inline struct evpl_rpc2_conn *
drc_conn_open(struct drc_ctx *c)
{
    struct evpl_endpoint  *ep;
    struct evpl_rpc2_conn *conn;

    /* Follow the env's transport.  These are the connections the suite
     * actually tests on -- the model's connection ids map to them -- so
     * leaving them on the stream endpoint would make an --rdma run a
     * verbatim repeat of the stream one. */
    ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                            "127.0.0.1",
                                            c->env->rdma ? MBT_NFS_RDMA_PORT
                                            : 2049);
    conn = evpl_rpc2_client_connect(c->env->rpc2_thread,
                                    c->env->rdma ? EVPL_DATAGRAM_INPROC
                                    : EVPL_STREAM_INPROC,
                                    ep, NULL, 0, NULL);
    if (!conn) {
        fprintf(stderr, "drc: failed to open an NFS connection\n");
        exit(2);
    }
    return conn;
} /* drc_conn_open */

/* The connection for one model connection id, opened on first use. */
static inline struct evpl_rpc2_conn *
drc_conn(
    struct drc_ctx *c,
    int             id)
{
    if (id < 0 || id >= DRC_MAX_CONNS) {
        fprintf(stderr, "drc: connection id %d out of range\n", id);
        exit(2);
    }
    if (!c->conns[id].conn) {
        c->conns[id].conn = drc_conn_open(c);
        c->conns[id].gen  = 1;
    }
    return c->conns[id].conn;
} /* drc_conn */

/* One tick of the drain timer below. */
struct drc_drain_ctx {
    struct evpl_timer timer;
    volatile int      ticks;
};

#define DRC_DRAIN_TICK_US 1000
#define DRC_DRAIN_TICKS   20

static void
drc_drain_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    ((struct drc_drain_ctx *) timer)->ticks++;
} /* drc_drain_tick */

/*
 * Run the client's event loop for a bounded window with nothing outstanding.
 *
 * evpl_continue() BLOCKS until something happens, so a bare pump loop with no
 * traffic in flight waits forever.  A periodic timer gives it something that
 * always happens, which is what turns "pump a few times" into a bounded wait
 * rather than a hang.
 */
static inline void
drc_drain(struct drc_ctx *c)
{
    struct drc_drain_ctx ctx;

    ctx.ticks = 0;
    evpl_add_timer(c->env->evpl, &ctx.timer, drc_drain_tick,
                   DRC_DRAIN_TICK_US);
    while (ctx.ticks < DRC_DRAIN_TICKS) {
        evpl_continue(c->env->evpl);
    }
    evpl_remove_timer(c->env->evpl, &ctx.timer);
} /* drc_drain */

/*
 * Drop a connection and open another in its place.
 *
 * The close has to reach the server before the replacement is used, because
 * what is under test is that the server dropped the old connection's cache --
 * and the fresh connection may well be handed the memory the old one just
 * freed, which is precisely the case a server gets wrong.  Nothing in the
 * client's own event loop reports that the peer has processed a close, so the
 * loop is drained for a bounded window first; the server is another thread in
 * this process and an inproc close reaches it immediately, so the window is
 * generous rather than tight.
 */
static inline void
drc_reconnect(
    struct drc_ctx *c,
    int             id)
{
    struct evpl_rpc2_conn *old = drc_conn(c, id);

    evpl_rpc2_client_disconnect(c->env->rpc2_thread, old);
    drc_drain(c);

    c->conns[id].conn = drc_conn_open(c);
    c->conns[id].gen++;
} /* drc_reconnect */

static inline void
drc_conns_close(struct drc_ctx *c)
{
    int i;

    for (i = 0; i < DRC_MAX_CONNS; i++) {
        if (c->conns[i].conn) {
            evpl_rpc2_client_disconnect(c->env->rpc2_thread, c->conns[i].conn);
            c->conns[i].conn = NULL;
        }
    }
} /* drc_conns_close */

/* ---- NFSv3 calls on a chosen connection, credential and XID -------------- */

/*
 * Point the shared NFSv3 wrappers at one connection and credential, and fix
 * the XID the next call will carry.
 *
 * The XID is always set rather than saved and restored: every call this
 * harness makes takes its XID from the trace, so the connection's own counter
 * is never read and there is nothing to preserve.  That also keeps the harness
 * honest -- an XID that appeared by accident rather than by the model's choice
 * could make a hit or a miss that the model did not predict.
 */
static inline void
drc_v3_begin(
    struct drc_ctx              *c,
    int                          conn_id,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid)
{
    struct evpl_rpc2_conn *conn = drc_conn(c, conn_id);

    c->saved_conn    = c->env->nfs_conn;
    c->saved_cred    = c->env->cred;
    c->env->nfs_conn = conn;
    c->env->cred     = *cred;

    evpl_rpc2_conn_set_next_xid(conn, xid);
} /* drc_v3_begin */

static inline void
drc_v3_end(struct drc_ctx *c)
{
    c->env->nfs_conn = c->saved_conn;
    c->env->cred     = c->saved_cred;
} /* drc_v3_end */

/* ---- remembered filehandles ---------------------------------------------- */

static inline struct drc_handle *
drc_handle_find(
    struct drc_ctx *c,
    int             id)
{
    int i;

    for (i = 0; i < c->nhandles; i++) {
        if (c->handles[i].id == id) {
            return &c->handles[i];
        }
    }
    return NULL;
} /* drc_handle_find */

static inline void
drc_handle_record(
    struct drc_ctx      *c,
    int                  id,
    const struct mbt_fh *fh)
{
    struct drc_handle *h = drc_handle_find(c, id);

    if (!h) {
        if (c->nhandles >= DRC_MAX_HANDLES) {
            fprintf(stderr, "drc: filehandle table full (%d)\n",
                    DRC_MAX_HANDLES);
            exit(2);
        }
        h     = &c->handles[c->nhandles++];
        h->id = id;
    }
    h->fh = *fh;
} /* drc_handle_record */

static inline void
drc_handles_reset(struct drc_ctx *c)
{
    c->nhandles = 0;
} /* drc_handles_reset */

/* ---- NFSv4.0 COMPOUNDs --------------------------------------------------- */

/*
 * The NFSv4 half of this suite stays on the directory handle: every COMPOUND
 * it sends is PUTFH of the working directory followed by one namespace
 * operation.  That is enough for every question the v4.0 cache raises -- which
 * operations are cached, which connection an entry belongs to, when one is
 * evicted, what a reconnect does -- and none of those turn on which object the
 * operation names.  Operations that would need a second handle (LINK, SETATTR)
 * are left to the NFSv3 traces, which already carry them.
 */
#define DRC_V4_MAX_OPS 6

struct drc_v4_res {
    int           done;
    int           rpc_err;
    uint32_t      status;                    /* the COMPOUND's status */
    int           nres;
    uint32_t      opstatus[DRC_V4_MAX_OPS];  /* per-operation status */
    uint8_t       resop[DRC_V4_MAX_OPS];     /* which operation each result is */
    struct mbt_fh fh;                        /* the last GETFH's result */

    /* Session establishment, for the NFSv4.1 checks. */
    uint64_t      clientid;                  /* EXCHANGE_ID */
    uint32_t      seqid;                     /* EXCHANGE_ID sequence */
    uint8_t       sessionid[NFS4_SESSIONID_SIZE];  /* CREATE_SESSION */
};

struct drc_v4_ctx {
    struct drc_v4_res *rep;
};

static void
drc_v4_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct drc_v4_ctx *ctx = private_data;
    uint32_t           i;

    (void) evpl;
    (void) verf;

    ctx->rep->rpc_err = status;
    if (status == 0) {
        ctx->rep->status = reply->status;
        ctx->rep->nres   = (int) reply->num_resarray;
        if (ctx->rep->nres > DRC_V4_MAX_OPS) {
            ctx->rep->nres = DRC_V4_MAX_OPS;
        }
        for (i = 0; i < (uint32_t) ctx->rep->nres; i++) {
            struct nfs_resop4 *r = &reply->resarray[i];

            /* Every result's first field is its status, whatever the arm. */
            ctx->rep->opstatus[i] = r->opillegal.status;
            ctx->rep->resop[i]    = (uint8_t) r->resop;

            if (r->resop == OP_GETFH && r->opgetfh.status == NFS4_OK) {
                mbt_copy_fh(&ctx->rep->fh, &r->opgetfh.resok4.object);
            }
            if (r->resop == OP_EXCHANGE_ID &&
                r->opexchange_id.eir_status == NFS4_OK) {
                ctx->rep->clientid = r->opexchange_id.eir_resok4.eir_clientid;
                ctx->rep->seqid    =
                    r->opexchange_id.eir_resok4.eir_sequenceid;
            }
            if (r->resop == OP_CREATE_SESSION &&
                r->opcreate_session.csr_status == NFS4_OK) {
                memcpy(ctx->rep->sessionid,
                       r->opcreate_session.csr_resok4.csr_sessionid,
                       NFS4_SESSIONID_SIZE);
            }
        }
    }
    ctx->rep->done = 1;
} /* drc_v4_cb */

/*
 * Send one COMPOUND and wait for it.
 *
 * The tag is left empty on purpose.  It is part of the request body and so
 * part of what a cache checksums, and a tag that varied per call -- a step
 * number, say -- would make every request unique and no retransmit would ever
 * match.  The model's notion of two requests being identical is exactly
 * "same operations, same arguments", so the wire form must carry nothing else.
 */
static inline void
drc_v4_call(
    struct drc_ctx              *c,
    int                          conn_id,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    struct nfs_argop4           *argarray,
    int                          nops,
    uint32_t                     minorversion,
    struct drc_v4_res           *out)
{
    struct evpl_rpc2_conn *conn = drc_conn(c, conn_id);
    struct COMPOUND4args   args;
    struct drc_v4_ctx      ctx;

    memset(out, 0, sizeof(*out));
    ctx.rep = out;

    memset(&args, 0, sizeof(args));
    args.tag.data     = NULL;
    args.tag.len      = 0;
    args.minorversion = minorversion;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) nops;

    evpl_rpc2_conn_set_next_xid(conn, xid);

    c->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&c->env->nfs_v4.rpc2,
                                               c->env->evpl, conn, cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               drc_v4_cb, &ctx);

    while (!out->done) {
        evpl_continue(c->env->evpl);
    }

    if (out->rpc_err != 0) {
        fprintf(stderr, "drc: rpc2 transport error %d on a COMPOUND\n",
                out->rpc_err);
        exit(3);
    }
} /* drc_v4_call */

static inline void
drc_v4_putfh(
    struct nfs_argop4   *a,
    const struct mbt_fh *fh)
{
    a->argop               = OP_PUTFH;
    a->opputfh.object.data = (void *) fh->data;
    a->opputfh.object.len  = fh->len;
} /* drc_v4_putfh */

/* ---- per-trace working directory ----------------------------------------- */

/*
 * Mount the export, create the working directory inside it, and forget every
 * filehandle the previous trace remembered.
 *
 * The directory is 0777 so both of the model's users can create and remove in
 * it, and every object created inside it is 0777 for the same reason: any
 * difference between what the two users observe has to be the cache's doing.
 */
static inline void
drc_trace_setup(struct drc_ctx *c)
{
    struct mbt_result *r;

    drc_handles_reset(c);

    r = mbt_mnt(c->env, c->mntpath);
    if (r->status != MNT3_OK || !r->obj_fh.has) {
        fprintf(stderr, "drc: MNT %s failed: %u\n", c->mntpath, r->status);
        exit(2);
    }
    c->root_fh = r->obj_fh;

    r = mbt_mkdir(c->env, &c->root_fh, DRC_DIR_NAME,
                  (uint32_t) strlen(DRC_DIR_NAME), DRC_MODE);
    if (r->status != NFS3_OK || !r->obj_fh.has) {
        fprintf(stderr, "drc: MKDIR %s failed: %u\n", DRC_DIR_NAME, r->status);
        exit(2);
    }
    c->dir_fh = r->obj_fh;
} /* drc_trace_setup */

/* ---- directory snapshot --------------------------------------------------- */

/*
 * What the working directory holds, as the model describes it: a name, a type
 * and a size for each entry.
 *
 * Read after every step, and the reason the suite can see a cache at all.  A
 * reply says what the server decided to tell the client; the directory says
 * what it actually did.  A cache that answers the right status from the wrong
 * entry, or one that re-executes a request it should have replayed, is
 * invisible in the first and unmistakable in the second.
 */
struct drc_dirent {
    char     name[DRC_NAME_MAX];
    uint32_t ftype;
    uint64_t size;
};

struct drc_dirsnap {
    struct drc_dirent ents[64];
    int               n;
};

static inline int
drc_dirent_cmp(
    const void *a,
    const void *b)
{
    return strcmp(((const struct drc_dirent *) a)->name,
                  ((const struct drc_dirent *) b)->name);
} /* drc_dirent_cmp */

static inline void
drc_dir_snapshot(
    struct drc_ctx     *c,
    struct drc_dirsnap *out)
{
    struct mbt_result *r;
    int                i;

    memset(out, 0, sizeof(*out));

    r = mbt_readdirplus(c->env, &c->dir_fh);
    if (r->status != NFS3_OK) {
        fprintf(stderr, "drc: READDIRPLUS of the working directory failed: "
                "%u\n", r->status);
        exit(2);
    }
    if (r->entries_overflow) {
        fprintf(stderr, "drc: working directory has more entries than the "
                "snapshot holds\n");
        exit(2);
    }

    for (i = 0; i < r->nentries; i++) {
        struct mbt_entry *e = &r->entries[i];

        if (strcmp(e->name, ".") == 0 || strcmp(e->name, "..") == 0) {
            continue;
        }
        if (out->n >= (int) (sizeof(out->ents) / sizeof(out->ents[0]))) {
            fprintf(stderr, "drc: too many directory entries\n");
            exit(2);
        }
        /* Names here are the model's own (a few characters); the explicit
         * precision bounds the copy for gcc's format-truncation checker. */
        snprintf(out->ents[out->n].name, DRC_NAME_MAX, "%.*s",
                 DRC_NAME_MAX - 1, e->name);
        if (e->attrs.has) {
            out->ents[out->n].ftype = e->attrs.a.type;
            out->ents[out->n].size  = e->attrs.a.size;
        }
        out->n++;
    }

    qsort(out->ents, (size_t) out->n, sizeof(out->ents[0]), drc_dirent_cmp);
} /* drc_dir_snapshot */
