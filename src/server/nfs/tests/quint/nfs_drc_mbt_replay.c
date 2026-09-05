// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replay a Quint-generated ITF trace of the DUPLICATE-REQUEST CACHES -- the
 * NFSv3 connectionless one and the NFSv4.0 per-connection one -- against an
 * in-process chimera server over the libevpl inproc transport.
 *
 * Each state of the trace carries a `lastOp` naming one RPC: which connection
 * carried it, under which transaction id and which credential, what it asked
 * for, and what the server must answer (see the nfsdrc model in
 * ext/specs/quint/nfsdrc).  Replaying it needs one thing the other suites do
 * not: the XID is dictated by the trace rather than chosen by the client, so
 * that a retransmit is a retransmit and not merely a similar request.
 *
 * What is checked, per step:
 *
 *   status    what the server answered.  For most of the operations here a
 *             replay and a re-execution differ in exactly this -- a replayed
 *             MKDIR is NFS3_OK where a re-executed one is NFS3ERR_EXIST.
 *   identity  when the model says the reply came from the cache, it names the
 *             execution that produced it, and the reply must be THAT one --
 *             the same filehandle, not merely the same status.  A cache that
 *             answered from the wrong entry passes a status comparison and
 *             hands the client another request's object.
 *   the tree  under --paranoid, the working directory after every step.  This
 *             is what makes a cache visible at all for the operations whose
 *             status does not distinguish a replay: a SETATTR replayed leaves
 *             the size alone, and a SETATTR re-executed puts it back.
 *
 * ISOLATION.  The NFSv3 cache is keyed by client address and nothing evicts it
 * on a timescale a test can wait for, so its entries outlive the filesystem a
 * trace runs on and would be visible to the next trace.  Two measures make a
 * trace independent of its predecessors; trace_setup() carries the details.
 */

#include <getopt.h>
#include <jansson.h>

#include "nfs_drc_mbt_common.h"
#include "common/mbt_trace_dir.h"

#define DRC_MAX_MISM         16
#define DRC_MISM_LEN         512
#define DRC_HISTORY          10

/* One entry per execution the model counts.  A trace is a few hundred steps
* and not every step executes, so this is comfortably clear of the bound. */
#define DRC_MAX_TAGS         2048

/* XIDs the harness issues on its own account, for the LOOKUPs that learn a
 * filehandle.  Far above anything the model's XID set contains, so a
 * housekeeping call can never collide with a modeled one -- and LOOKUP is
 * idempotent, so the NFSv3 cache returns before it would even look. */
#define DRC_HOUSEKEEPING_XID 0x7f000000u

struct mism {
    int  n;
    char msg[DRC_MAX_MISM][DRC_MISM_LEN];
};

static void
mism_add(
    struct mism *m,
    const char  *fmt,
    ...)
{
    va_list ap;

    if (m->n >= DRC_MAX_MISM) {
        return;
    }
    va_start(ap, fmt);
    vsnprintf(m->msg[m->n], DRC_MISM_LEN, fmt, ap);
    va_end(ap);
    m->n++;
} /* mism_add */

/* What one execution returned, so a replay of it can be checked to be that
 * reply rather than one that merely agrees on the status. */
struct tag_reply {
    int           used;
    uint32_t      st;
    struct mbt_fh fh;    /* the created object, where the reply carries one */
};

struct hist_ent {
    int   idx;
    char  tag[32];
    char *dump;
};

struct oracle {
    struct mbt_env  *env;
    struct drc_ctx   c;
    int              trace_seq;
    int              verbose;

    struct tag_reply tags[DRC_MAX_TAGS];

    struct hist_ent  history[DRC_HISTORY];
    int              nhist;
};

/*
 * There is deliberately no known-deviation registry here.
 *
 * The NFSv3 suite keeps one because its model asserts RFC-correct replies over
 * ground chimera diverges on.  This model has no such ground: the one place it
 * could have had is a FRESH request on a filehandle whose object is gone, and
 * the traces never ask.  Not because the answer is wrong -- it is
 * NFS3ERR_STALE, as RFC 1813 Section 3.3 requires -- but because a handle the
 * server has recently resolved is pinned by its own handle cache and keeps
 * resolving for a window afterwards, exactly as an unlinked-but-open inode does
 * on Linux.  Which of the two a live server gives depends on its cache state,
 * so a corpus that walked into it would be flaky for a reason unrelated to a
 * reply cache.  The step relation keeps out of it (see actFhLive in the model)
 * and nfs_drc_probe.c pins both answers directly, where they are
 * deterministic.
 */

/* ---- ITF decoding helpers (jansson) -------------------------------------- */

static int64_t
itf_i64(json_t *v)
{
    json_t *big;

    if (json_is_integer(v)) {
        return json_integer_value(v);
    }
    if (json_is_object(v)) {
        big = json_object_get(v, "#bigint");
        if (big && json_is_string(big)) {
            return strtoll(json_string_value(big), NULL, 10);
        }
    }
    return 0;
} /* itf_i64 */

static int64_t
op_i64(
    json_t     *op,
    const char *key)
{
    return itf_i64(json_object_get(op, key));
} /* op_i64 */

static const char *
op_str(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    return (v && json_is_string(v)) ? json_string_value(v) : "";
} /* op_str */

static int
op_bool(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    return v && json_is_true(v);
} /* op_bool */

static const char *
jf_tag(json_t *v)
{
    json_t *t = v ? json_object_get(v, "tag") : NULL;

    return (t && json_is_string(t)) ? json_string_value(t) : "";
} /* jf_tag */

static json_t *
jf_val(json_t *v)
{
    return v ? json_object_get(v, "value") : NULL;
} /* jf_val */

/* A Quint map, which ITF spells as {"#map": [[k, v], ...]}. */
static json_t *
itf_map(json_t *v)
{
    json_t *inner = v ? json_object_get(v, "#map") : NULL;

    return (inner && json_is_array(inner)) ? inner : NULL;
} /* itf_map */

static json_t *
state_var(
    json_t     *state,
    const char *name)
{
    const char *key;
    json_t     *val;
    size_t      n = strlen(name);

    json_object_foreach(state, key, val)
    {
        size_t klen = strlen(key);

        if (klen >= n && strcmp(key + klen - n, name) == 0 &&
            (klen == n || key[klen - n - 1] == ':')) {
            return val;
        }
    }
    return NULL;
} /* state_var */

/* ---- issuing one modeled request ----------------------------------------- */

/* The handle the model's object id names, or NULL if the harness never learned
 * one (which would be a harness bug, not a divergence). */
static const struct mbt_fh *
handle_for(
    struct oracle *o,
    int            id,
    struct mism   *m)
{
    struct drc_handle *h;

    if (id == 0) {
        return &o->c.dir_fh;   /* the working directory itself */
    }
    h = drc_handle_find(&o->c, id);
    if (!h) {
        mism_add(m, "no filehandle learned for object %d", id);
        return NULL;
    }
    return &h->fh;
} /* handle_for */

/* One NFSv3 request, as the model describes it.  Returns the reply; the
 * filehandle it carries (when it carries one) is left in *out_fh. */
static uint32_t
issue_v3(
    struct oracle *o,
    int            conn,
    uint32_t       xid,
    uint32_t       uid,
    json_t        *act,
    struct mbt_fh *out_fh,
    struct mism   *m)
{
    struct evpl_rpc2_cred cred;
    struct mbt_result    *r;
    const char           *tag = jf_tag(act);
    json_t               *v   = jf_val(act);
    const struct mbt_fh  *fh;
    uint32_t              st;

    drc_cred_init(&cred, uid);
    drc_v3_begin(&o->c, conn, &cred, xid);

    if (strcmp(tag, "ACreate") == 0) {
        const char *name  = op_str(v, "name");
        int64_t     ftype = op_i64(v, "ftype");

        if (ftype == 2) {
            r = mbt_mkdir(o->env, &o->c.dir_fh, name,
                          (uint32_t) strlen(name), DRC_MODE);
        } else if (ftype == 5) {
            r = mbt_symlink(o->env, &o->c.dir_fh, name,
                            (uint32_t) strlen(name), "t", DRC_MODE);
        } else {
            r = mbt_create(o->env, &o->c.dir_fh, name,
                           (uint32_t) strlen(name), GUARDED, DRC_MODE, NULL);
        }
    } else if (strcmp(tag, "ARemove") == 0) {
        const char *name = op_str(v, "name");

        if (op_bool(v, "asDir")) {
            r = mbt_rmdir(o->env, &o->c.dir_fh, name, (uint32_t) strlen(name));
        } else {
            r = mbt_remove(o->env, &o->c.dir_fh, name, (uint32_t) strlen(name));
        }
    } else if (strcmp(tag, "ARename") == 0) {
        const char *from = op_str(v, "from");
        const char *to   = op_str(v, "to");

        r = mbt_rename(o->env, &o->c.dir_fh, from, (uint32_t) strlen(from),
                       &o->c.dir_fh, to, (uint32_t) strlen(to));
    } else if (strcmp(tag, "ALink") == 0) {
        const char *to = op_str(v, "to");

        fh = handle_for(o, (int) op_i64(v, "src"), m);
        if (!fh) {
            drc_v3_end(&o->c);
            return 0;
        }
        r = mbt_link(o->env, fh, &o->c.dir_fh, to, (uint32_t) strlen(to));
    } else if (strcmp(tag, "ASetsize") == 0) {
        fh = handle_for(o, (int) op_i64(v, "fh"), m);
        if (!fh) {
            drc_v3_end(&o->c);
            return 0;
        }
        r = mbt_setattr(o->env, fh, -1, op_i64(v, "size"), NULL);
    } else if (strcmp(tag, "ALookup") == 0) {
        const char *name = op_str(v, "name");

        r = mbt_lookup(o->env, &o->c.dir_fh, name, (uint32_t) strlen(name));
    } else if (strcmp(tag, "AGetattr") == 0) {
        fh = handle_for(o, (int) op_i64(v, "fh"), m);
        if (!fh) {
            drc_v3_end(&o->c);
            return 0;
        }
        r = mbt_getattr(o->env, fh);
    } else {
        drc_v3_end(&o->c);
        mism_add(m, "no NFSv3 encoding for action %s", tag);
        return 0;
    }

    st = r->status;
    if (out_fh) {
        *out_fh = r->obj_fh;
    }
    drc_v3_end(&o->c);
    return st;
} /* issue_v3 */

/* One NFSv4.0 COMPOUND.  Every one is PUTFH of the working directory followed
 * by a single namespace operation; see nfs_drc_mbt_common.h for why. */
static uint32_t
issue_v4(
    struct oracle *o,
    int            conn,
    uint32_t       xid,
    uint32_t       uid,
    json_t        *act,
    struct mism   *m)
{
    struct evpl_rpc2_cred cred;
    struct nfs_argop4     a[4];
    struct drc_v4_res     res;
    const char           *tag       = jf_tag(act);
    json_t               *v         = jf_val(act);
    uint32_t              bitmap[2] = { 0, 1U << (FATTR4_MODE - 32) };
    uint8_t               blob[4];
    int                   nops;

    /* mode 0777, big-endian, as an NFSv4 attribute value. */
    blob[0] = 0;
    blob[1] = 0;
    blob[2] = (uint8_t) ((DRC_MODE >> 8) & 0xff);
    blob[3] = (uint8_t) (DRC_MODE & 0xff);

    memset(a, 0, sizeof(a));
    drc_v4_putfh(&a[0], &o->c.dir_fh);

    if (strcmp(tag, "ACreate") == 0) {
        const char *name  = op_str(v, "name");
        int64_t     ftype = op_i64(v, "ftype");

        a[1].argop                 = OP_CREATE;
        a[1].opcreate.objname.data = (void *) name;
        a[1].opcreate.objname.len  = (uint32_t) strlen(name);
        if (ftype == 5) {
            a[1].opcreate.objtype.type          = NF4LNK;
            a[1].opcreate.objtype.linkdata.data = (void *) "t";
            a[1].opcreate.objtype.linkdata.len  = 1;
        } else if (ftype == 2) {
            a[1].opcreate.objtype.type = NF4DIR;
        } else {
            /* The model's NFSv4 traces never ask for one: CREATE makes
             * non-regular objects only (RFC 7530 Section 16.4). */
            mism_add(m, "NFSv4 CREATE cannot make ftype %d", (int) ftype);
            return 0;
        }
        a[1].opcreate.createattrs.num_attrmask   = 2;
        a[1].opcreate.createattrs.attrmask       = bitmap;
        a[1].opcreate.createattrs.attr_vals.data = blob;
        a[1].opcreate.createattrs.attr_vals.len  = 4;
        nops                                     = 2;
    } else if (strcmp(tag, "ARemove") == 0) {
        const char *name = op_str(v, "name");

        a[1].argop                = OP_REMOVE;
        a[1].opremove.target.data = (void *) name;
        a[1].opremove.target.len  = (uint32_t) strlen(name);
        nops                      = 2;
    } else if (strcmp(tag, "ARename") == 0) {
        const char *from = op_str(v, "from");
        const char *to   = op_str(v, "to");

        a[1].argop = OP_SAVEFH;
        drc_v4_putfh(&a[2], &o->c.dir_fh);
        a[3].argop                 = OP_RENAME;
        a[3].oprename.oldname.data = (void *) from;
        a[3].oprename.oldname.len  = (uint32_t) strlen(from);
        a[3].oprename.newname.data = (void *) to;
        a[3].oprename.newname.len  = (uint32_t) strlen(to);
        nops                       = 4;
    } else if (strcmp(tag, "ALookup") == 0) {
        const char *name = op_str(v, "name");

        a[1].argop                 = OP_LOOKUP;
        a[1].oplookup.objname.data = (void *) name;
        a[1].oplookup.objname.len  = (uint32_t) strlen(name);
        nops                       = 2;
    } else {
        mism_add(m, "no NFSv4 encoding for action %s", tag);
        return 0;
    }

    drc_cred_init(&cred, uid);
    drc_v4_call(&o->c, conn, &cred, xid, a, nops, 0, &res);
    return res.status;
} /* issue_v4 */

/* ---- learning filehandles ------------------------------------------------ */

/*
 * Make sure the harness holds a handle for every object the model's directory
 * currently names.
 *
 * A handle is learned once, when its object first appears, and kept
 * afterwards -- including after the object is gone, which is the state a
 * retransmit carrying it will be in.  An NFSv3 CREATE hands the handle back in
 * its reply, so the common case costs nothing; anything else (an object a
 * COMPOUND created) is looked up by name the moment it appears.
 *
 * The LOOKUP rides an XID far outside the model's, and LOOKUP is idempotent so
 * the NFSv3 cache returns before it would consult an entry.  It therefore
 * cannot make or match a cache entry, and the trace is unaffected by it.
 */
static void
learn_handles(
    struct oracle *o,
    json_t        *state,
    struct mism   *m)
{
    struct evpl_rpc2_cred cred;
    json_t               *entries = itf_map(state_var(state, "dir"));
    size_t                i;

    if (!entries) {
        return;
    }

    drc_cred_init(&cred, 1000);

    for (i = 0; i < json_array_size(entries); i++) {
        json_t     *pair = json_array_get(entries, i);
        const char *name;
        json_t     *obj;
        int         id;

        if (!json_is_array(pair) || json_array_size(pair) != 2) {
            continue;
        }
        name = json_string_value(json_array_get(pair, 0));
        obj  = json_array_get(pair, 1);
        if (!name || !obj) {
            continue;
        }
        id = (int) op_i64(obj, "id");
        if (drc_handle_find(&o->c, id)) {
            continue;
        }

        drc_v3_begin(&o->c, 1, &cred, DRC_HOUSEKEEPING_XID);
        {
            struct mbt_result *r = mbt_lookup(o->env, &o->c.dir_fh, name,
                                              (uint32_t) strlen(name));

            if (r->status == NFS3_OK && r->obj_fh.has) {
                drc_handle_record(&o->c, id, &r->obj_fh);
            } else {
                mism_add(m, "cannot learn a handle for %s (object %d): "
                         "LOOKUP answered %u", name, id, r->status);
            }
        }
        drc_v3_end(&o->c);
    }
} /* learn_handles */

/* ---- checking ------------------------------------------------------------ */

/*
 * The working directory, against what the model says it holds.
 *
 * The reply comparison alone cannot see a cache doing the wrong thing for
 * every operation.  A replayed SETATTR and a re-executed one both answer
 * NFS3_OK; the difference is a size, and it is only visible here.  A cache that
 * answered a MKDIR from another MKDIR's entry likewise agrees on the status and
 * differs in what is on disk.
 */
static void
check_tree(
    struct oracle *o,
    json_t        *state,
    struct mism   *m)
{
    struct drc_dirsnap snap;
    json_t            *entries = itf_map(state_var(state, "dir"));
    size_t             want    = entries ? json_array_size(entries) : 0;
    size_t             i;

    drc_dir_snapshot(&o->c, &snap);

    if ((size_t) snap.n != want) {
        mism_add(m, "directory holds %d entries, model says %zu",
                 snap.n, want);
        return;
    }

    for (i = 0; i < want; i++) {
        json_t     *pair = json_array_get(entries, i);
        const char *name = json_string_value(json_array_get(pair, 0));
        json_t     *obj  = json_array_get(pair, 1);
        uint32_t    ftype;
        uint64_t    size;
        int         j, found = 0;

        if (!name || !obj) {
            continue;
        }
        ftype = (uint32_t) op_i64(obj, "ftype");
        size  = (uint64_t) op_i64(obj, "size");

        for (j = 0; j < snap.n; j++) {
            if (strcmp(snap.ents[j].name, name) != 0) {
                continue;
            }
            found = 1;
            if (snap.ents[j].ftype != ftype) {
                mism_add(m, "%s is type %u, model says %u", name,
                         snap.ents[j].ftype, ftype);
            }
            /* Only regular files carry a size the model tracks; a directory's
             * is the backend's business and a symlink's is its target's
             * length. */
            if (ftype == 1 && snap.ents[j].size != size) {
                mism_add(m, "%s is %llu bytes, model says %llu", name,
                         (unsigned long long) snap.ents[j].size,
                         (unsigned long long) size);
            }
            break;
        }
        if (!found) {
            mism_add(m, "%s is missing; the model has it", name);
        }
    }
} /* check_tree */

/*
 * One step's reply.
 *
 * A replay is held to more than its status: the model names the execution whose
 * reply it must be, and where that reply carried a filehandle the replayed one
 * has to carry the same bytes.  That is what separates "a cache answered" from
 * "a cache answered from the right entry" -- the two agree on every status a
 * status comparison can see.
 */
static void
check_reply(
    struct oracle *o,
    json_t        *op,
    uint32_t       got,
    struct mbt_fh *got_fh,
    struct mism   *m)
{
    uint32_t want     = (uint32_t) op_i64(op, "st");
    int64_t  tag      = op_i64(op, "tag");
    int      replayed = strcmp(jf_tag(json_object_get(op, "served")),
                               "SReplayed") == 0;

    if (tag < 0 || tag >= DRC_MAX_TAGS) {
        mism_add(m, "reply tag %lld out of range", (long long) tag);
        return;
    }

    if (got != want) {
        mism_add(m, "status %u, model says %u", got, want);
        return;
    }

    if (replayed) {
        struct tag_reply *t = &o->tags[tag];

        if (!t->used) {
            mism_add(m, "model replays execution %lld, which never ran",
                     (long long) tag);
            return;
        }
        if (t->fh.has && got_fh && got_fh->has &&
            !mbt_fh_eq(&t->fh, got_fh)) {
            mism_add(m, "replay carries a different filehandle from the "
                     "execution (%lld) the model says it came from",
                     (long long) tag);
        }
        if (t->fh.has && got_fh && !got_fh->has) {
            mism_add(m, "replay carries no filehandle; execution %lld did",
                     (long long) tag);
        }
    } else {
        struct tag_reply *t = &o->tags[tag];

        t->used = 1;
        t->st   = got;
        if (got_fh) {
            t->fh = *got_fh;
        }
    }
} /* check_reply */

/* ---- per-trace setup and teardown ---------------------------------------- */

/*
 * Make a trace independent of the ones before it.
 *
 * The NFSv3 cache is the reason this needs saying.  It is keyed by client
 * address, its entries have no lifetime a test can wait out, and it is shared
 * by every connection in the process -- so an entry one trace left behind is
 * visible to the next, and a step that should miss would hit.  Two things
 * separate them:
 *
 *   - every trace gets its own XID band.  The model's XIDs are small integers,
 *     so trace N offsets them by N << 16: no two traces can present the same
 *     {address, procedure, XID} triple, whatever their arguments.
 *   - every trace gets fresh connections.  The NFSv4.0 cache is keyed by
 *     connection, so closing them is all its isolation needs, and starting each
 *     trace with the connection generations at a known point is what makes the
 *     model's reconnect counting line up.
 *
 * The filesystem is fresh per trace already (mbt_env_fs_setup), which is what
 * makes the directory comparison meaningful.
 */
static uint32_t
trace_xid(
    struct oracle *o,
    int64_t        model_xid)
{
    return (uint32_t) ((o->trace_seq + 1) << 16) + (uint32_t) model_xid;
} /* trace_xid */

static int
trace_setup(struct oracle *o)
{
    drc_conns_close(&o->c);
    memset(o->c.conns, 0, sizeof(o->c.conns));
    memset(o->tags, 0, sizeof(o->tags));

    drc_trace_setup(&o->c);
    return 0;
} /* trace_setup */

static void
trace_teardown(struct oracle *o)
{
    drc_conns_close(&o->c);
} /* trace_teardown */

/* ---- divergence reporting ------------------------------------------------ */

static void
history_push(
    struct oracle *o,
    int            idx,
    const char    *tag,
    json_t        *op)
{
    struct hist_ent *h;

    if (o->nhist == DRC_HISTORY) {
        free(o->history[0].dump);
        memmove(&o->history[0], &o->history[1],
                sizeof(o->history[0]) * (DRC_HISTORY - 1));
        o->nhist--;
    }
    h      = &o->history[o->nhist++];
    h->idx = idx;
    snprintf(h->tag, sizeof(h->tag), "%s", tag);
    h->dump = op ? json_dumps(op, JSON_COMPACT | JSON_SORT_KEYS) : NULL;
} /* history_push */

static void
report_divergence(
    const char    *path,
    struct oracle *o,
    int            idx,
    const char    *tag,
    json_t        *op,
    struct mism   *m)
{
    char *dump = op ? json_dumps(op, JSON_INDENT(2) | JSON_SORT_KEYS) : NULL;
    int   i;

    fprintf(stderr, "\nDIVERGENCE at %s step %d (%s)\n", path, idx, tag);
    for (i = 0; i < m->n; i++) {
        fprintf(stderr, "  - %s\n", m->msg[i]);
    }
    if (dump) {
        fprintf(stderr, "\nstep:\n%s\n", dump);
        free(dump);
    }
    fprintf(stderr, "\nlast steps before the failure:\n");
    for (i = 0; i < o->nhist; i++) {
        fprintf(stderr, "  [%4d] %-12s %s\n", o->history[i].idx,
                o->history[i].tag,
                o->history[i].dump ? o->history[i].dump : "");
    }
} /* report_divergence */

/* ---- the trace loop ------------------------------------------------------- */

static int paranoid;

static int
run_trace(
    struct mbt_env *env,
    const char     *trace_path,
    const char     *fsname,
    int             trace_seq,
    int             verbose,
    int             dry_run)
{
    json_error_t   jerr;
    json_t        *root, *states, *state, *last_op, *op;
    const char    *tag;
    struct oracle *o;
    size_t         idx, nstates;
    int            failed = 0;
    struct mism    m;

    root = json_load_file(trace_path, 0, &jerr);
    if (!root) {
        fprintf(stderr, "%s: JSON parse error: %s (line %d)\n", trace_path,
                jerr.text, jerr.line);
        return 1;
    }
    states = json_object_get(root, "states");
    if (!states || !json_is_array(states) || !json_object_get(root, "vars")) {
        fprintf(stderr, "%s: not an ITF trace\n", trace_path);
        json_decref(root);
        return 1;
    }
    nstates = json_array_size(states);

    if (dry_run) {
        printf("%s: %zu steps, format OK\n", trace_path, nstates - 1);
        json_decref(root);
        return 0;
    }

    /* Backstop for a server deadlock: with everything in one process a hung
     * reply spins in the pump forever; SIGALRM's default disposition kills the
     * test with a nonzero status. */
    alarm(180);

    o = calloc(1, sizeof(*o));
    mbt_env_fs_setup(env, fsname);

    o->env   = env;
    o->c.env = env;
    snprintf(o->c.mntpath, sizeof(o->c.mntpath), "/%s", fsname);
    o->verbose   = verbose;
    o->trace_seq = trace_seq;

    if (trace_setup(o)) {
        failed = 1;
        goto out;
    }

    for (idx = 1; idx < nstates; idx++) {
        struct mbt_fh got_fh;
        uint32_t      got;

        state   = json_array_get(states, idx);
        last_op = state_var(state, "lastOp");
        if (!last_op) {
            fprintf(stderr, "%s: state %zu has no lastOp\n", trace_path, idx);
            failed = 1;
            goto out;
        }
        tag = jf_tag(last_op);
        op  = jf_val(last_op);

        memset(&m, 0, sizeof(m));
        memset(&got_fh, 0, sizeof(got_fh));

        if (strcmp(tag, "OV3") == 0) {
            got = issue_v3(o, (int) op_i64(op, "conn"),
                           trace_xid(o, op_i64(op, "xid")),
                           (uint32_t) op_i64(op, "cred"),
                           json_object_get(op, "act"), &got_fh, &m);
            if (!m.n) {
                check_reply(o, op, got, &got_fh, &m);
            }
        } else if (strcmp(tag, "OV4") == 0) {
            got = issue_v4(o, (int) op_i64(op, "conn"),
                           trace_xid(o, op_i64(op, "xid")),
                           (uint32_t) op_i64(op, "cred"),
                           json_object_get(op, "act"), &m);
            if (!m.n) {
                check_reply(o, op, got, NULL, &m);
            }
        } else if (strcmp(tag, "OReconnect") == 0) {
            drc_reconnect(&o->c, (int) op_i64(op, "conn"));
        } else if (strcmp(tag, "ONull") == 0) {
            struct evpl_rpc2_cred cred;

            drc_cred_init(&cred, 1000);
            drc_v3_begin(&o->c, (int) op_i64(op, "conn"), &cred,
                         DRC_HOUSEKEEPING_XID);
            mbt_null(o->env);
            drc_v3_end(&o->c);
        } else {
            fprintf(stderr, "%s: step %zu: no handler for %s\n", trace_path,
                    idx, tag);
            failed = 1;
            goto out;
        }

        if (!m.n) {
            learn_handles(o, state, &m);
        }
        if (!m.n && paranoid) {
            check_tree(o, state, &m);
        }

        history_push(o, (int) idx, tag, op);
        if (verbose) {
            printf("  [%4zu] %s\n", idx, tag);
        }
        if (m.n) {
            report_divergence(trace_path, o, (int) idx, tag, op, &m);
            failed = 1;
            goto out;
        }
    }

    printf("%s: %zu steps replayed\n", trace_path, nstates - 1);

 out:
    trace_teardown(o);
    mbt_env_fs_teardown(env, fsname);
    while (o->nhist) {
        free(o->history[--o->nhist].dump);
    }
    free(o);
    json_decref(root);
    alarm(0);
    return failed;
} /* run_trace */

int
main(
    int    argc,
    char **argv)
{
    /* *INDENT-OFF* */
    /* uncrustify 0.78.1 does not converge on this aligned initializer: each
     * pass pushes the last column further right than the one before.  Pin a
     * stable manual alignment. */
    static struct option long_options[] = {
        { "trace",          required_argument, 0, 't' },
        { "trace-dir",      required_argument, 0, 'D' },
        { "exclude-prefix", required_argument, 0, 'X' },
        { "dry-run",        no_argument,       0, 'n' },
        { "verbose",        no_argument,       0, 'v' },
        { "paranoid",       no_argument,       0, 'p' },
        { "no-nfs3-drc",    no_argument,       0, 'N' },
        { "backend",        required_argument, 0, 'B' },
        { "rdma",           no_argument,       0, 'R' },
        { 0,                0,                 0, 0   },
    };
    /* *INDENT-ON* */
    char              **traces;
    int                 ntraces  = 0;
    int                 dry_run  = 0;
    int                 verbose  = 0;
    int                 nfs3_drc = 1;
    const char         *backend  = "memfs";
    int                 rdma     = 0;
    int                 failures = 0;
    int                 c, i;
    struct mbt_env      env;
    struct mbt_env_opts opts;

    /* Line-buffer stdout so a crash cannot swallow the progress output.
     * These drivers print one line per trace, and both that and chimera's log
     * (which defaults to stdout) are block-buffered when stdout is a pipe --
     * which it always is under ctest.  glibc's abort() does not flush stdio,
     * so on Linux an aborting run loses everything since the last 4 KB
     * boundary, including the line naming the trace that was executing and
     * the fatal log message itself.  That is exactly what made a CI abort
     * here undiagnosable from its artifacts. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    umask(0);

    traces = mbt_collect_traces(argc, argv, &ntraces);

    while ((c = getopt_long(argc, argv, "t:D:X:nvpNB:R", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;   /* handled by mbt_collect_traces */
            case 'n':
                dry_run = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'p':
                paranoid = 1;
                break;
            case 'N':
                nfs3_drc = 0;
                break;
            case 'B':
                backend = optarg;
                break;
            case 'R':
                rdma = 1;
                break;
            default:
                fprintf(stderr,
                        "usage: %s [--trace FILE ...] [--trace-dir DIR] "
                        "[--backend memfs|diskfs|cairn|linux|io_uring] "
                        "[--rdma] [--no-nfs3-drc] "
                        "[--dry-run] [--verbose] [--paranoid]\n", argv[0]);
                mbt_free_traces(traces, ntraces);
                return 2;
        } /* switch */
    }

    if (ntraces == 0) {
        fprintf(stderr, "%s: at least one --trace or --trace-dir is required\n",
                argv[0]);
        mbt_free_traces(traces, ntraces);
        return 2;
    }

    if (!dry_run) {
        memset(&opts, 0, sizeof(opts));
        opts.module = backend;
        opts.rdma   = rdma;
        /* The corpus generated from the nfsdrcNoV3 profile is replayed with the
         * NFSv3 cache off, which is the server's own default; everything else
         * needs it on.  The NFSv4.0 cache has no switch. */
        opts.nfs3_drc = nfs3_drc;
        /* Exact comparison of the directory after every step cannot tolerate a
         * stale attribute cache. */
        opts.disable_caches = 1;
        mbt_env_open_opts(&env, &opts);
    }

    for (i = 0; i < ntraces; i++) {
        char fsname[32];

        snprintf(fsname, sizeof(fsname), "fs_%d", i);
        failures += run_trace(dry_run ? NULL : &env, traces[i], fsname, i,
                              verbose, dry_run);
    }

    if (!dry_run) {
        mbt_env_stop(&env);
    }

    mbt_free_traces(traces, ntraces);
    return failures ? 1 : 0;
} /* main */
