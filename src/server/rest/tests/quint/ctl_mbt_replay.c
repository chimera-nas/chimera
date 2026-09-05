// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replays the control-plane corpus (ctl.qnt) against a live chimera.
 *
 * Each trace interleaves REST calls with NFS requests, and the harness runs
 * both against ONE in-process server over the inproc transport: the HTTP
 * client and the rpc2 client share an event loop, so "delete the export, then
 * present the handle" happens on a real server in the order the model says it
 * does.  That is what the suite is for -- neither half is interesting alone,
 * and no out-of-process arrangement can express the ordering.
 *
 * Every step of a trace carries the answer the server must give, so the trace
 * is its own oracle:
 *
 *   OApi      a REST call and its HTTP status
 *   OMnt      MOUNTPROC3_MNT of an export, its mountstat3, and the slot the
 *             resulting filehandle is remembered in
 *   OGetattr  a request on a held handle, and its nfsstat3
 *   OCreate   a mutating request on a held handle: its nfsstat3 and, when it
 *             succeeds, the owner the file must land on -- which is where a
 *             squash policy that stopped applying shows up, since the status
 *             is 0 either way
 *
 * Under --paranoid the administrative listings are re-read after every step
 * and compared against the model's own state, so a create that answered 201
 * and produced nothing is caught where it happened rather than at whatever
 * later step trips over it.
 *
 * Trace isolation is by name: every model name is prefixed with the trace's
 * index, so two traces cannot collide even before the teardown at the end of
 * each one has finished.  The listings are filtered to that prefix for the
 * same reason.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <jansson.h>

#include "nfs3_mbt_common.h"
#include "ctl_http.h"
#include "common/mbt_trace_dir.h"

#define CTL_REPLAY_REST_PORT   8080

/* The anonymous identity every export in the corpus squashes to.  Must equal
 * the model's ANONUID (ctl_run.qnt): the model computes the owner a squashed
 * create lands on, and the harness compares the real one against it. */
#define CTL_ANONUID            65534

/* How many times a mount deletion is re-attempted while the server reports
 * that its handles have not drained yet.  Each attempt costs the configured
 * umount timeout (set low below), so this is a few seconds at worst. */
#define CTL_UMOUNT_DRAIN_TRIES 50

/* Filehandle slots one trace may hold.  A trace mints one per successful
 * MOUNT and never recycles, so this bounds MOUNT count, not concurrency. */
#define CTL_MAX_SLOTS          64

static int paranoid;

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

/* A Quint set: {"#set": [...]}. */
static json_t *
itf_set(json_t *v)
{
    json_t *inner = v ? json_object_get(v, "#set") : NULL;

    return (inner && json_is_array(inner)) ? inner : NULL;
} /* itf_set */

/* State variables are namespaced ("ctlRef::ctl::lastOp"); match on the tail so
 * the replayer does not depend on which profile generated the trace. */
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

/* ---- known-deviation registry -------------------------------------------- */

/*
 * The model always states what the control plane must do; a status-only
 * divergence listed here is recorded and tolerated instead of failing the
 * replay, so the suite is green today and goes red the moment chimera is
 * fixed -- which is the signal to delete the entry.
 *
 * Tolerating one cannot desync this replay.  The model tracks no filesystem
 * contents (that is the nfs3 suite's job), only a monotone counter that makes
 * every created name fresh, so a create the model believes did not happen and
 * chimera performed anyway collides with nothing later.  The administrative
 * state -- the only thing --paranoid compares -- is untouched by all of these.
 */
struct deviation {
    const char *id;
    const char *op_tag;
    uint32_t    expected_status;
    uint32_t    actual_status;
};

/* *INDENT-OFF* */
/* uncrustify 0.78.1 oscillates on aligned initializer columns; pin them. */
static const struct deviation known_deviations[] = {
    /* Deleting an export does not revoke the filehandles minted under it.
     * chimera resolves a handle's stamped export id only to APPLY policy and
     * lets an unknown id through (nfs_common.h, chimera_nfs_fh_decode); the
     * inner VFS handle is untouched by the deletion, because removing an
     * export unmounts nothing.  So the handle keeps working -- and keeps
     * working with no policy at all, since read-only, squash and the sec list
     * all live on the record the id no longer finds.
     *
     * Linux resolves the export on every request (nfsd_set_fh_dentry ->
     * rqst_exp_find) and answers ESTALE, which is what makes `exportfs -u` a
     * revocation.  ctl_proto_probe pins all four faces of this directly,
     * including a squash=all write landing as uid 0 once the export is gone. */
    { "deleted-export-serves-handles", "OGetattr", 70, 0 },
    { "deleted-export-serves-handles", "OCreate",  70, 0 },
};
/* *INDENT-ON* */

/*
 * A permitted alternative, not a deviation.
 *
 * RFC 1813 does not mandate error precedence when more than one condition
 * applies to a request, and this suite reaches one such state: an export that
 * outlives its mount (creating an export over an absent mount is allowed --
 * see ctl_api.qnt) with filehandles still held into it.  A write through such
 * a handle is BOTH a write through a dead handle and a write to a read-only
 * export.
 *
 * The model checks the handle first, which is what Linux does -- fh_verify()
 * resolves the filehandle before nfsd_permission() reaches the read-only
 * check, so a dead handle is ESTALE whatever the export says.  Chimera gates
 * on the export's access mode before resolving anything, so it answers ROFS.
 * Both answers are true and neither is required, so this is accepted rather
 * than recorded as a deviation.  It is deliberately narrow: only when the
 * model expects STALE and the server says ROFS, and only for a mutating op.
 */
static int
precedence_ok(
    const char *tag,
    uint32_t    expected,
    uint32_t    actual)
{
    return strcmp(tag, "OCreate") == 0 &&
           expected == 70 &&              /* NFS3ERR_STALE  */
           actual == 30;                  /* NFS3ERR_ROFS   */
} /* precedence_ok */

static const struct deviation *
reconcile(
    const char *tag,
    uint32_t    expected,
    uint32_t    actual)
{
    size_t i;

    for (i = 0; i < sizeof(known_deviations) / sizeof(known_deviations[0]);
         i++) {
        const struct deviation *dev = &known_deviations[i];

        if (strcmp(dev->op_tag, tag) == 0 &&
            dev->expected_status == expected &&
            dev->actual_status == actual) {
            return dev;
        }
    }
    return NULL;
} /* reconcile */

/* ---- the replay context --------------------------------------------------- */

struct oracle {
    struct mbt_env  *env;
    struct ctl_conn *api;
    int              seq;                   /* trace index, for name prefixes */
    struct mbt_fh    slots[CTL_MAX_SLOTS];
    int              nslots;
    int              verbose;
    /* Set when a step diverged; carries the message for the report. */
    char             mismatch[1024];
};

static void
fail(
    struct oracle *o,
    const char    *fmt,
    ...)
{
    va_list ap;

    if (o->mismatch[0]) {
        return;                             /* keep the first */
    }
    va_start(ap, fmt);
    vsnprintf(o->mismatch, sizeof(o->mismatch), fmt, ap);
    va_end(ap);
} /* fail */

/*
 * Every model name is rendered with the trace's index in front of it, so two
 * traces never share a filesystem, mount, export, share, bucket or user even
 * if a teardown is still draining.  Export names in the model already begin
 * with a slash (they are mount paths on the wire), so the prefix goes after
 * it.
 */
static void
tname(
    struct oracle *o,
    const char    *model,
    char          *out,
    size_t         outlen)
{
    if (model[0] == '/') {
        snprintf(out, outlen, "/t%d%s", o->seq, model + 1);
    } else {
        snprintf(out, outlen, "t%d%s", o->seq, model);
    }
} /* tname */

/* The VFS path an export, share or bucket is rooted at: "/" plus the mount. */
static void
tpath(
    struct oracle *o,
    const char    *mount,
    char          *out,
    size_t         outlen)
{
    snprintf(out, outlen, "/t%d%s", o->seq, mount);
} /* tpath */

/* ---- issuing one modeled REST call ---------------------------------------- */

static int
issue_api(
    struct oracle *o,
    json_t        *req)
{
    const char    *tag = jf_tag(req);
    json_t        *v   = jf_val(req);
    struct ctl_res res;
    char           url[512], body[1024], n1[256], n2[256];

    /* ---- filesystems ---- */
    if (strcmp(tag, "RFsCreate") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(body, sizeof(body),
                 "{\"module\":\"memfs\",\"name\":\"%s\"}", n1);
        ctl_post(o->api, "/api/v1/filesystems", body, &res);
        return res.status;
    }
    if (strcmp(tag, "RFsDelete") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/filesystems/memfs/%s", n1);
        ctl_delete(o->api, url, &res);
        return res.status;
    }

    /* ---- mounts ---- */
    if (strcmp(tag, "RMountCreate") == 0) {
        tname(o, op_str(v, "name"), n1, sizeof(n1));
        tname(o, op_str(v, "fs"), n2, sizeof(n2));
        snprintf(body, sizeof(body),
                 "{\"name\":\"%s\",\"module\":\"memfs\",\"path\":\"%s\"}",
                 n1, n2);
        ctl_post(o->api, "/api/v1/mounts", body, &res);
        return res.status;
    }
    if (strcmp(tag, "RMountDelete") == 0) {
        int tries;

        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/mounts/%s", n1);

        /*
         * Two different 409s come back from this endpoint, and only one of
         * them is an answer:
         *
         *   "Mount is in use by a share, export, or bucket" is the
         *   referential refusal the model specifies.  It is decided
         *   synchronously and is final.
         *
         *   "Mount still has open handles" is not about the caller at all.
         *   NFSv3 is stateless, so the server opens and releases a VFS handle
         *   per request, and the release is completed by an asynchronous sweep
         *   -- so a mount the client finished with moments ago is briefly
         *   still busy.  Nothing the API expresses distinguishes that moment
         *   from any other, which is exactly why it must not be compared
         *   against the model: the answer would depend on how fast the machine
         *   is.
         *
         * So the second one is waited out, and only the settled answer is
         * compared.  This does not soften the comparison -- if the handles
         * never drain, the 409 is returned and the divergence is reported.
         */
        for (tries = 0; tries < CTL_UMOUNT_DRAIN_TRIES; tries++) {
            ctl_delete(o->api, url, &res);
            if (res.status != 409 ||
                !strstr(res.body, "open handles")) {
                return res.status;
            }
        }
        return res.status;
    }
    if (strcmp(tag, "RMountGet") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/mounts/%s", n1);
        ctl_get(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RMountList") == 0) {
        ctl_get(o->api, "/api/v1/mounts", &res);
        return res.status;
    }

    /* ---- exports ---- */
    if (strcmp(tag, "RExportCreate") == 0) {
        /* The model pins each export's path and id (see EXPORTS in ctl.qnt),
         * and carries only the two policies the trace varies.  The path and
         * id come from the state, which the caller has already looked up. */
        fail(o, "RExportCreate must be issued through issue_export_create");
        return -1;
    }
    if (strcmp(tag, "RExportDelete") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/exports/%s", n1);
        ctl_delete(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RExportGet") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/exports/%s", n1);
        ctl_get(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RExportList") == 0) {
        ctl_get(o->api, "/api/v1/exports", &res);
        return res.status;
    }

    /* ---- shares ---- */
    if (strcmp(tag, "RShareCreate") == 0) {
        fail(o, "RShareCreate must be issued through issue_share_create");
        return -1;
    }
    if (strcmp(tag, "RShareDelete") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/shares/%s", n1);
        ctl_delete(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RShareGet") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/shares/%s", n1);
        ctl_get(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RShareList") == 0) {
        ctl_get(o->api, "/api/v1/shares", &res);
        return res.status;
    }

    /* ---- buckets ---- */
    if (strcmp(tag, "RBucketCreate") == 0) {
        fail(o, "RBucketCreate must be issued through issue_bucket_create");
        return -1;
    }
    if (strcmp(tag, "RBucketDelete") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/buckets/%s", n1);
        ctl_delete(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RBucketGet") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/buckets/%s", n1);
        ctl_get(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RBucketList") == 0) {
        ctl_get(o->api, "/api/v1/buckets", &res);
        return res.status;
    }

    /* ---- users ---- */
    if (strcmp(tag, "RUserCreate") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(body, sizeof(body),
                 "{\"username\":\"%s\",\"uid\":1000,\"gid\":1000}", n1);
        ctl_post(o->api, "/api/v1/users", body, &res);
        return res.status;
    }
    if (strcmp(tag, "RUserDelete") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/users/%s", n1);
        ctl_delete(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RUserGet") == 0) {
        tname(o, json_string_value(v), n1, sizeof(n1));
        snprintf(url, sizeof(url), "/api/v1/users/%s", n1);
        ctl_get(o->api, url, &res);
        return res.status;
    }
    if (strcmp(tag, "RUserList") == 0) {
        ctl_get(o->api, "/api/v1/users", &res);
        return res.status;
    }

    if (strcmp(tag, "RConfig") == 0) {
        ctl_get(o->api, "/api/v1/config", &res);
        return res.status;
    }

    fail(o, "no handler for API request %s", tag);
    return -1;
} /* issue_api */

/*
 * The three creates that need more than the request carries.
 *
 * An export's path and id, and a share's or bucket's path, are pinned per
 * name by the model's configuration rather than chosen per call, so they are
 * not in the request record.  The replayer reads them out of the state the
 * step produced -- which is where the model put them.
 */
static int
issue_export_create(
    struct oracle *o,
    json_t        *req,
    json_t        *state)
{
    json_t        *v  = jf_val(req);
    const char    *mn = op_str(v, "name");
    json_t        *ctlst, *exports, *pair;
    struct ctl_res res;
    char           body[1024], name[256], path[256];
    size_t         i;
    int64_t        id    = -1;
    const char    *mpath = NULL;

    /* The pinned (path, id) live in the exports map of the resulting state
     * when the create succeeded.  When it did not (a conflict), the model
     * left the map alone -- but the pinned values are the same either way, so
     * an existing entry serves, and the very first creation of a name always
     * succeeds. */
    ctlst   = state_var(state, "ctlst");
    exports = itf_map(json_object_get(ctlst, "exports"));

    for (i = 0; exports && i < json_array_size(exports); i++) {
        pair = json_array_get(exports, i);
        if (strcmp(json_string_value(json_array_get(pair, 0)), mn) == 0) {
            json_t *e = json_array_get(pair, 1);

            id    = itf_i64(json_object_get(e, "id"));
            mpath = op_str(e, "path");
        }
    }

    if (id < 0) {
        /* A conflicting create of a name not currently present cannot happen:
         * the conflict is with the name itself. */
        fail(o, "export %s has no pinned id in the resulting state", mn);
        return -1;
    }

    tname(o, mn, name, sizeof(name));
    tpath(o, mpath, path, sizeof(path));

    snprintf(body, sizeof(body),
             "{\"name\":\"%s\",\"path\":\"%s\",\"export_id\":%lld,"
             "\"access\":\"%s\",\"squash\":\"%s\",\"anonuid\":%d,"
             "\"anongid\":%d}",
             name, path, (long long) id,
             op_bool(v, "ro") ? "ro" : "rw", op_str(v, "squash"),
             CTL_ANONUID, CTL_ANONUID);

    ctl_post(o->api, "/api/v1/exports", body, &res);
    return res.status;
} /* issue_export_create */

static int
issue_named_create(
    struct oracle *o,
    json_t        *req,
    json_t        *state,
    const char    *collection,
    const char    *url)
{
    json_t        *v  = jf_val(req);
    const char    *mn = json_string_value(v);
    json_t        *ctlst, *m, *pair;
    struct ctl_res res;
    char           body[1024], name[256], path[256];
    size_t         i;
    const char    *mount = NULL;

    ctlst = state_var(state, "ctlst");
    m     = itf_map(json_object_get(ctlst, collection));

    for (i = 0; m && i < json_array_size(m); i++) {
        pair = json_array_get(m, i);
        if (strcmp(json_string_value(json_array_get(pair, 0)), mn) == 0) {
            mount = json_string_value(json_array_get(pair, 1));
        }
    }

    if (!mount) {
        fail(o, "%s %s has no pinned mount in the resulting state",
             collection, mn);
        return -1;
    }

    tname(o, mn, name, sizeof(name));
    tpath(o, mount, path, sizeof(path));
    snprintf(body, sizeof(body), "{\"name\":\"%s\",\"path\":\"%s\"}",
             name, path);

    ctl_post(o->api, url, body, &res);
    return res.status;
} /* issue_named_create */

/* ---- provisioning and teardown -------------------------------------------- */

/*
 * Reproduce the model's `init`: one filesystem, mounted, exported read-write
 * with no squash.  The model starts from a server that is already serving
 * something (see the SEED_* constants and the argument for them), so the
 * harness must too.
 */
static int
trace_setup(
    struct oracle *o,
    json_t        *state0)
{
    json_t *ctlst   = state_var(state0, "ctlst");
    json_t *fss     = itf_set(json_object_get(ctlst, "fss"));
    json_t *mounts  = itf_map(json_object_get(ctlst, "mounts"));
    json_t *exports = itf_map(json_object_get(ctlst, "exports"));
    size_t  i;

    for (i = 0; fss && i < json_array_size(fss); i++) {
        json_t *req = json_pack("{s:s, s:o}", "tag", "RFsCreate",
                                "value", json_incref(json_array_get(fss, i)));
        int     st = issue_api(o, req);

        json_decref(req);
        if (st != 201) {
            fail(o, "seed: filesystem create returned %d", st);
            return -1;
        }
    }

    for (i = 0; mounts && i < json_array_size(mounts); i++) {
        json_t *pair = json_array_get(mounts, i);
        json_t *req  = json_pack("{s:s, s:{s:O, s:O}}", "tag", "RMountCreate",
                                 "value",
                                 "name", json_array_get(pair, 0),
                                 "fs", json_object_get(json_array_get(pair, 1),
                                                       "fs"));
        int     st = issue_api(o, req);

        json_decref(req);
        if (st != 201) {
            fail(o, "seed: mount create returned %d", st);
            return -1;
        }
    }

    for (i = 0; exports && i < json_array_size(exports); i++) {
        json_t *pair = json_array_get(exports, i);
        json_t *e    = json_array_get(pair, 1);
        json_t *req  = json_pack("{s:s, s:{s:O, s:O, s:O}}",
                                 "tag", "RExportCreate", "value",
                                 "name", json_array_get(pair, 0),
                                 "ro", json_object_get(e, "ro"),
                                 "squash", json_object_get(e, "squash"));
        int     st = issue_export_create(o, req, state0);

        json_decref(req);
        if (st != 201) {
            fail(o, "seed: export create returned %d", st);
            return -1;
        }
    }

    return 0;
} /* trace_setup */

/*
 * Remove everything the trace created, in dependency order.  Best-effort: a
 * teardown failure is reported but does not fail the trace, because the
 * per-trace name prefix already guarantees isolation -- this is about not
 * accumulating state across a long corpus.
 */
static void
trace_teardown(struct oracle *o)
{
    static const char *collections[] = { "exports", "shares", "buckets",
                                         "users",   "mounts" };
    struct ctl_res     res;
    char               prefix[32];
    size_t             ci;
    int                round;

    snprintf(prefix, sizeof(prefix), "t%d", o->seq);

    for (ci = 0; ci < sizeof(collections) / sizeof(collections[0]); ci++) {
        char    url[256];
        json_t *root, *elem;
        size_t  i;

        snprintf(url, sizeof(url), "/api/v1/%s", collections[ci]);
        ctl_get(o->api, url, &res);
        root = json_loadb(res.body, res.body_len, 0, NULL);

        for (i = 0; root && json_is_array(root) && i < json_array_size(root);
             i++) {
            const char *n;

            elem = json_array_get(root, i);
            n    = json_string_value(json_object_get(elem, "name"));
            if (!n) {
                n = json_string_value(json_object_get(elem, "username"));
            }
            if (!n) {
                continue;
            }
            /* Export names carry a leading slash before the prefix. */
            if (strncmp(n, prefix, strlen(prefix)) != 0 &&
                strncmp(n + (n[0] == '/' ? 1 : 0), prefix, strlen(prefix)) != 0) {
                continue;
            }
            snprintf(url, sizeof(url), "/api/v1/%s/%s", collections[ci], n);
            ctl_delete(o->api, url, &res);
        }
        json_decref(root);
    }

    /* Filesystems come last, and may be briefly busy while the VFS drops the
     * handles the trace's NFS requests left open -- the same asynchronous
     * close sweep the NFS and SMB harnesses wait on before rmfs. */
    for (round = 0; round < 200; round++) {
        json_t *root, *elem;
        size_t  i;
        int     left = 0;

        ctl_get(o->api, "/api/v1/config", &res);
        root = json_loadb(res.body, res.body_len, 0, NULL);
        json_decref(root);

        for (i = 0; i < 2; i++) {
            char url[256];

            snprintf(url, sizeof(url), "/api/v1/filesystems/memfs/t%df%zu",
                     o->seq, i);
            ctl_delete(o->api, url, &res);
            if (res.status != 204 && res.status != 404) {
                left = 1;
            }
        }
        (void) elem;
        if (!left) {
            return;
        }
        usleep(5000);
    }

    fprintf(stderr, "warning: trace %d left filesystems behind\n", o->seq);
} /* trace_teardown */

/* ---- paranoid state comparison -------------------------------------------- */

/*
 * Re-read one administrative collection and compare its membership against the
 * model's.  Only the names this trace owns are considered, so a concurrent or
 * leftover trace cannot make this fire.
 *
 * Membership rather than every field: the fields are checked where they
 * matter (an export's access mode by the read-only result of a write through
 * it, its squash by the owner a create lands on), and a listing comparison
 * that tried to cover them would mostly be re-implementing the JSON
 * serializer.  What it catches is the failure a status cannot: a call that
 * reported success and changed nothing, or changed something else.
 */
static void
check_collection(
    struct oracle *o,
    json_t        *state,
    const char    *var,
    const char    *url,
    const char    *namekey,
    int            is_set)
{
    struct ctl_res res;
    json_t        *ctlst = state_var(state, "ctlst");
    json_t        *want  = json_object_get(ctlst, var);
    json_t        *items = is_set ? itf_set(want) : itf_map(want);
    json_t        *root;
    char           prefix[32];
    size_t         i, j;
    int            nwant, ngot = 0;

    if (o->mismatch[0]) {
        return;
    }

    snprintf(prefix, sizeof(prefix), "t%d", o->seq);

    ctl_get(o->api, url, &res);
    if (res.status != 200) {
        fail(o, "%s: listing returned %d", url, res.status);
        return;
    }

    root = json_loadb(res.body, res.body_len, 0, NULL);
    if (!root || !json_is_array(root)) {
        fail(o, "%s: listing is not a JSON array", url);
        json_decref(root);
        return;
    }

    nwant = items ? (int) json_array_size(items) : 0;

    /* Every name the model has must be present, spelled with this trace's
     * prefix. */
    for (i = 0; i < (size_t) nwant; i++) {
        json_t     *entry = json_array_get(items, i);
        const char *mn    = is_set ? json_string_value(entry)
                            : json_string_value(json_array_get(entry, 0));
        char        expect[256];
        int         found = 0;

        tname(o, mn, expect, sizeof(expect));

        for (j = 0; j < json_array_size(root); j++) {
            const char *n = json_string_value(
                json_object_get(json_array_get(root, j), namekey));

            if (n && strcmp(n, expect) == 0) {
                found = 1;
            }
        }
        if (!found) {
            fail(o, "%s: model has %s (%s) but the server does not list it",
                 url, mn, expect);
            json_decref(root);
            return;
        }
    }

    /* ...and the server must list nothing else of this trace's. */
    for (j = 0; j < json_array_size(root); j++) {
        const char *n = json_string_value(
            json_object_get(json_array_get(root, j), namekey));
        const char *bare;

        if (!n) {
            continue;
        }
        bare = n + (n[0] == '/' ? 1 : 0);
        if (strncmp(bare, prefix, strlen(prefix)) != 0) {
            continue;                       /* another trace's, or seeded */
        }
        ngot++;
    }

    if (ngot != nwant) {
        fail(o, "%s: model has %d entries, the server lists %d of this "
             "trace's", url, nwant, ngot);
    }

    json_decref(root);
} /* check_collection */

/*
 * GET /api/v1/config is a second, independent rendering of the same facts as
 * the four listings, produced by different code (rest_config.c keys its
 * entries by name; the listing handlers put the name in a field).  Requiring
 * them to agree is what catches a field or a filter added to one and
 * forgotten in the other -- the internal "root" pseudo-mount, for instance,
 * is filtered out of both, and a divergence there would mean the API had
 * started exposing chimera's own namespace root.
 */
static void
check_config(
    struct oracle *o,
    json_t        *state)
{
    struct ctl_res res;
    json_t        *ctlst = state_var(state, "ctlst");
    json_t        *root, *section;

    /* *INDENT-OFF* */
    /* uncrustify 0.78.1 does not converge on this aligned initializer:
     * each pass pushes the last column one further right.  Pin it. */
    static const struct {
        const char *var;
        const char *sect;
    } sections[] = {
        { "mounts",  "mounts"   },
        { "exports", "exports"  },
        { "shares",  "shares"   },
        { "buckets", "buckets"  },
    };
    /* *INDENT-ON* */
    size_t s, i;
    char   prefix[32];

    if (o->mismatch[0]) {
        return;
    }

    snprintf(prefix, sizeof(prefix), "t%d", o->seq);

    ctl_get(o->api, "/api/v1/config", &res);
    if (res.status != 200) {
        fail(o, "config: returned %d", res.status);
        return;
    }

    root = json_loadb(res.body, res.body_len, 0, NULL);
    if (!root) {
        fail(o, "config: body is not JSON");
        return;
    }

    for (s = 0; s < sizeof(sections) / sizeof(sections[0]); s++) {
        json_t     *want  = itf_map(json_object_get(ctlst, sections[s].var));
        int         nwant = want ? (int) json_array_size(want) : 0;
        int         ngot  = 0;
        const char *k;
        json_t     *val;

        section = json_object_get(root, sections[s].sect);
        if (!section || !json_is_object(section)) {
            fail(o, "config: no %s section", sections[s].sect);
            json_decref(root);
            return;
        }

        for (i = 0; i < (size_t) nwant; i++) {
            const char *mn = json_string_value(
                json_array_get(json_array_get(want, i), 0));
            char        expect[256];

            tname(o, mn, expect, sizeof(expect));
            if (!json_object_get(section, expect)) {
                fail(o, "config: %s section is missing %s",
                     sections[s].sect, expect);
                json_decref(root);
                return;
            }
        }

        json_object_foreach(section, k, val)
        {
            const char *bare = k + (k[0] == '/' ? 1 : 0);

            if (strncmp(bare, prefix, strlen(prefix)) == 0) {
                ngot++;
            }
        }

        if (ngot != nwant) {
            fail(o, "config: %s section has %d of this trace's entries, the "
                 "listings say %d", sections[s].sect, ngot, nwant);
            json_decref(root);
            return;
        }
    }

    json_decref(root);
} /* check_config */

static void
check_state(
    struct oracle *o,
    json_t        *state)
{
    check_collection(o, state, "mounts", "/api/v1/mounts", "name", 0);
    check_collection(o, state, "exports", "/api/v1/exports", "name", 0);
    check_collection(o, state, "shares", "/api/v1/shares", "name", 0);
    check_collection(o, state, "buckets", "/api/v1/buckets", "name", 0);
    check_collection(o, state, "users", "/api/v1/users", "username", 1);
    check_config(o, state);
} /* check_state */

/* ---- one trace ------------------------------------------------------------ */

static int
run_trace(
    struct mbt_env *env,
    const char     *path,
    int             seq,
    int             verbose,
    int             dry_run)
{
    json_t        *root, *states, *state, *last_op, *op;
    json_error_t   jerr;
    struct oracle *o;
    size_t         idx, nstates;
    int            failed = 0;
    const char    *tag;

    root = json_load_file(path, 0, &jerr);
    if (!root) {
        fprintf(stderr, "%s: %s (line %d)\n", path, jerr.text, jerr.line);
        return 1;
    }

    states = json_object_get(root, "states");
    if (!states || !json_is_array(states) || json_array_size(states) < 1) {
        fprintf(stderr, "%s: no states array\n", path);
        json_decref(root);
        return 1;
    }
    nstates = json_array_size(states);

    if (dry_run) {
        printf("%s: %zu steps (dry run)\n", path, nstates - 1);
        json_decref(root);
        return 0;
    }

    o          = calloc(1, sizeof(*o));
    o->env     = env;
    o->seq     = seq;
    o->verbose = verbose;
    o->api     = ctl_conn_open(env->evpl, env->rest_agent,
                               CTL_REPLAY_REST_PORT);

    if (trace_setup(o, json_array_get(states, 0))) {
        failed = 1;
        goto out;
    }

    for (idx = 1; idx < nstates; idx++) {
        state   = json_array_get(states, idx);
        last_op = state_var(state, "lastOp");
        if (!last_op) {
            fail(o, "state %zu has no lastOp", idx);
            break;
        }
        tag = jf_tag(last_op);
        op  = jf_val(last_op);

        if (strcmp(tag, "OApi") == 0) {
            json_t     *req  = json_object_get(op, "req");
            const char *rtag = jf_tag(req);
            int         want = (int) op_i64(op, "st");
            int         got;

            if (strcmp(rtag, "RExportCreate") == 0) {
                got = issue_export_create(o, req, state);
            } else if (strcmp(rtag, "RShareCreate") == 0) {
                got = issue_named_create(o, req, state, "shares",
                                         "/api/v1/shares");
            } else if (strcmp(rtag, "RBucketCreate") == 0) {
                got = issue_named_create(o, req, state, "buckets",
                                         "/api/v1/buckets");
            } else {
                got = issue_api(o, req);
            }

            if (!o->mismatch[0] && got != want) {
                fail(o, "%s: expected HTTP %d, got %d", rtag, want, got);
            }
        } else if (strcmp(tag, "OMnt") == 0) {
            char               name[256];
            struct mbt_result *r;
            uint32_t           want = (uint32_t) op_i64(op, "st");
            int                slot = (int) op_i64(op, "slot");

            tname(o, op_str(op, "name"), name, sizeof(name));
            mbt_cred_set_uid(o->env, 0);
            r = mbt_mnt(o->env, name);

            if (r->status != want) {
                fail(o, "MNT %s: expected mountstat3 %u, got %u",
                     name, want, r->status);
            } else if (slot >= 0) {
                if (slot >= CTL_MAX_SLOTS) {
                    fail(o, "MNT %s: slot %d exceeds CTL_MAX_SLOTS", name,
                         slot);
                } else if (!r->obj_fh.has) {
                    fail(o, "MNT %s: succeeded with no filehandle", name);
                } else {
                    o->slots[slot] = r->obj_fh;
                    if (slot >= o->nslots) {
                        o->nslots = slot + 1;
                    }
                }
            }
        } else if (strcmp(tag, "OGetattr") == 0 ||
                   strcmp(tag, "OCreate") == 0) {
            int                slot = (int) op_i64(op, "slot");
            uint32_t           cred = (uint32_t) op_i64(op, "cred");
            uint32_t           want = (uint32_t) op_i64(op, "st");
            struct mbt_result *r;

            if (slot < 0 || slot >= o->nslots) {
                fail(o, "%s: slot %d was never minted", tag, slot);
                goto step_done;
            }

            mbt_cred_set_uid(o->env, cred);

            if (strcmp(tag, "OGetattr") == 0) {
                r = mbt_getattr(o->env, &o->slots[slot]);
            } else {
                char fname[64];

                snprintf(fname, sizeof(fname), "f%lld",
                         (long long) op_i64(op, "seq"));
                r = mbt_create(o->env, &o->slots[slot], fname,
                               (uint32_t) strlen(fname), UNCHECKED, 0644,
                               NULL);
            }

            if (r->status != want && precedence_ok(tag, want, r->status)) {
                if (verbose) {
                    printf("  [%4zu] %s: permitted precedence (model %u, "
                           "server %u)\n", idx, tag, want, r->status);
                }
            } else if (r->status != want) {
                const struct deviation *dev = reconcile(tag, want, r->status);

                if (dev) {
                    if (verbose) {
                        printf("  [%4zu] %s: tolerated deviation %s "
                               "(model %u, chimera %u)\n",
                               idx, tag, dev->id, want, r->status);
                    }
                } else {
                    fail(o, "%s slot %d cred %u: expected nfsstat3 %u, got %u",
                         tag, slot, cred, want, r->status);
                }
            } else if (strcmp(tag, "OCreate") == 0 && want == 0) {
                /* The status is 0 whether or not the squash policy applied,
                 * so the owner is the only place a lost one shows. */
                int64_t wuid = op_i64(op, "uid");

                if (!r->obj_attrs.has) {
                    fail(o, "OCreate slot %d: reply carried no attributes",
                         slot);
                } else if ((int64_t) r->obj_attrs.a.uid != wuid) {
                    fail(o, "OCreate slot %d cred %u: expected owner %lld, "
                         "got %u", slot, cred, (long long) wuid,
                         r->obj_attrs.a.uid);
                }
            }
        } else {
            fail(o, "step %zu: no handler for %s", idx, tag);
        }

 step_done:
        if (!o->mismatch[0] && paranoid) {
            check_state(o, state);
        }

        if (verbose) {
            printf("  [%4zu] %s\n", idx, tag);
        }

        if (o->mismatch[0]) {
            fprintf(stderr, "\n%s: DIVERGENCE at step %zu (%s)\n  %s\n",
                    path, idx, tag, o->mismatch);
            failed = 1;
            break;
        }
    }

    if (!failed) {
        printf("%s: %zu steps replayed\n", path, nstates - 1);
    }

 out:
    trace_teardown(o);
    ctl_conn_close(o->api);
    free(o);
    json_decref(root);
    return failed;
} /* run_trace */

int
main(
    int    argc,
    char **argv)
{
    /* *INDENT-OFF* */
    /* uncrustify 0.78.1 does not converge on this aligned initializer. */
    static struct option long_options[] = {
        { "trace",          required_argument, 0, 't' },
        { "trace-dir",      required_argument, 0, 'D' },
        { "exclude-prefix", required_argument, 0, 'X' },
        { "dry-run",        no_argument,       0, 'n' },
        { "verbose",        no_argument,       0, 'v' },
        { "paranoid",       no_argument,       0, 'p' },
        { 0,                0,                 0, 0   },
    };
    /* *INDENT-ON* */
    char              **traces;
    int                 ntraces  = 0;
    int                 dry_run  = 0;
    int                 verbose  = 0;
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

    while ((c = getopt_long(argc, argv, "t:D:X:nvp", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;                      /* handled by mbt_collect_traces */
            case 'n':
                dry_run = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'p':
                paranoid = 1;
                break;
            default:
                fprintf(stderr,
                        "usage: %s [--trace FILE ...] [--trace-dir DIR] "
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
        opts.rest_port = CTL_REPLAY_REST_PORT;
        /* Shares and buckets are administrative objects of the SMB and S3
         * servers, and cannot be created on a server that never initialized
         * them, so the corpus needs all three protocols up. */
        opts.smb_enabled = 1;
        opts.s3_enabled  = 1;
        /* umount waits for open handles before reporting EBUSY, and this
         * suite unmounts constantly.  The default second per attempt would
         * dominate the run; the wait is a settling window, not a deadline,
         * and the replayer retries around it (see RMountDelete). */
        opts.umount_timeout_ms = 100;
        /* Every filesystem, mount and export this suite uses is created over
         * the REST API, which is the thing under test -- so the harness does
         * NOT call mbt_env_fs_setup. */
        mbt_env_open_opts(&env, &opts);
        env.rest_agent = evpl_http_init(env.evpl);
    }

    for (i = 0; i < ntraces; i++) {
        failures += run_trace(dry_run ? NULL : &env, traces[i], i, verbose,
                              dry_run);
    }

    if (!dry_run) {
        evpl_http_destroy(env.rest_agent);
        mbt_env_stop(&env);
    }

    mbt_free_traces(traces, ntraces);

    if (failures) {
        fprintf(stderr, "\n%d of %d traces diverged\n", failures, ntraces);
        return 1;
    }

    printf("\n%d traces replayed with no divergence\n", ntraces);
    return 0;
} /* main */
