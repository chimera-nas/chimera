// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Replay Quint-generated S3 traces (ITF JSON) against the in-process chimera
 * S3 server over HTTP, comparing every response against the model's
 * expectation.
 *
 * Model-to-wire mapping:
 *  - model bucket names are wire bucket names (path-style addressing);
 *  - a model key (a list of path components) joins with '/' into the wire
 *    key;
 *  - content block symbol s at index i is block_size bytes of 0x40+s at
 *    offset i * block_size; range requests scale block units by block_size;
 *  - the model's `status` is the HTTP status line, its `err` the <Code> of
 *    the XML error body;
 *  - ETag values are not predicted; the harness learns an object's ETag at
 *    its last write (Put/Copy) and requires every later read (Get, Head,
 *    ListObjects <Contents>) to report the same value until the object is
 *    rewritten or deleted.
 *
 * Divergence policy (mirrors nfs3_mbt_replay.c): the model always encodes
 * the official AWS behavior.  A known, documented divergence of chimera is
 * listed in the deviation registry below and tolerated -- tolerating skips
 * the response-shape checks that assume the official status, but still
 * performs the state bookkeeping (ETag learn/forget) that keeps the replay
 * in sync.  Anything else is a divergence: the trace fails with a report of
 * the step, the mismatches, and recent history. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <jansson.h>

#include "s3_mbt_common.h"
#include "common/mbt_trace_dir.h"

#define MBT_MAX_MISM   16
#define MBT_MISM_LEN   512
#define MBT_MAX_KEYLEN 160
#define MBT_MAX_ETAGS  256
#define MBT_HIST       10

/* ---- mismatch accumulator ------------------------------------------------ */

struct mism {
    int  count;
    char msg[MBT_MAX_MISM][MBT_MISM_LEN];
};

__attribute__((format(printf, 2, 3)))
static void
mism_add(
    struct mism *m,
    const char  *fmt,
    ...)
{
    va_list ap;

    if (m->count >= MBT_MAX_MISM) {
        return;
    }
    va_start(ap, fmt);
    vsnprintf(m->msg[m->count], MBT_MISM_LEN, fmt, ap);
    va_end(ap);
    m->count++;
} /* mism_add */

/* ---- ITF decoding -------------------------------------------------------- */

/* Quint ints arrive as {"#bigint":"N"} (or a bare integer for small models). */
static int64_t
itf_i64(json_t *v)
{
    json_t *big;

    if (json_is_integer(v)) {
        return json_integer_value(v);
    }
    big = json_object_get(v, "#bigint");
    if (big && json_is_string(big)) {
        return strtoll(json_string_value(big), NULL, 10);
    }
    fprintf(stderr, "malformed ITF integer\n");
    exit(2);
} /* itf_i64 */

static json_t *
op_field(
    json_t     *op,
    const char *name)
{
    json_t *v = json_object_get(op, name);

    if (!v) {
        fprintf(stderr, "op lacks field '%s'\n", name);
        exit(2);
    }
    return v;
} /* op_field */

static int64_t
op_i64(
    json_t     *op,
    const char *name)
{
    return itf_i64(op_field(op, name));
} /* op_i64 */

static const char *
op_str(
    json_t     *op,
    const char *name)
{
    json_t *v = op_field(op, name);

    if (!json_is_string(v)) {
        fprintf(stderr, "op field '%s' is not a string\n", name);
        exit(2);
    }
    return json_string_value(v);
} /* op_str */

static int
op_bool(
    json_t     *op,
    const char *name)
{
    json_t *v = op_field(op, name);

    if (!json_is_boolean(v)) {
        fprintf(stderr, "op field '%s' is not a bool\n", name);
        exit(2);
    }
    return json_is_true(v);
} /* op_bool */

/* The tag of a sum-type field, e.g. op["range"] = {"tag":"RFrom",...}. */
static const char *
op_tag(
    json_t     *op,
    const char *name)
{
    json_t *v   = op_field(op, name);
    json_t *tag = json_object_get(v, "tag");

    if (!tag || !json_is_string(tag)) {
        fprintf(stderr, "op field '%s' is not a sum value\n", name);
        exit(2);
    }
    return json_string_value(tag);
} /* op_tag */

/* A model key (JSON array of component strings) joined with '/'.  The empty
 * key (the start-after sentinel) renders as "". */
static void
key_str(
    json_t *karr,
    char   *out,
    size_t  outlen)
{
    size_t i, n = json_array_size(karr);
    size_t off = 0;

    out[0] = '\0';
    for (i = 0; i < n; i++) {
        off += snprintf(out + off, outlen - off, "%s%s", i ? "/" : "",
                        json_string_value(json_array_get(karr, i)));
    }
} /* key_str */

/* A model prefix record ({comps, slash}) rendered as its wire string. */
static void
prefix_str(
    json_t *pfx,
    char   *out,
    size_t  outlen)
{
    json_t *comps = op_field(pfx, "comps");
    size_t  off   = 0;
    size_t  i, n = json_array_size(comps);

    out[0] = '\0';
    for (i = 0; i < n; i++) {
        off += snprintf(out + off, outlen - off, "%s/",
                        json_string_value(json_array_get(comps, i)));
    }
    /* comps render "d/e/"; a bare prefix drops the trailing slash */
    if (!op_bool(pfx, "slash") && off > 0) {
        out[off - 1] = '\0';
    }
} /* prefix_str */

/* Content blocks (JSON array of block symbols) into an int array. */
static int
blocks_of(
    json_t *arr,
    int    *syms,
    int     max)
{
    int i, n = (int) json_array_size(arr);

    if (n > max) {
        fprintf(stderr, "trace has %d blocks, harness limit %d\n", n, max);
        exit(2);
    }
    for (i = 0; i < n; i++) {
        syms[i] = (int) itf_i64(json_array_get(arr, i));
    }
    return n;
} /* blocks_of */

/* Model post-state navigation.  bkts = {#map:[[name, Bkt]]} where Bkt =
 * { objs: {#map:[[comps, Obj]]}, btags: {#set}, mpu: {#map:[[id, Mpu]]} },
 * Obj = { data: [...], mtag, tags: {#set} }, Mpu = { key, parts: {#map} }. */

static json_t *
model_bucket(
    json_t     *post_bkts,
    const char *bucket)
{
    json_t *outer = json_object_get(post_bkts, "#map");
    size_t  i;

    for (i = 0; i < json_array_size(outer); i++) {
        json_t *pair = json_array_get(outer, i);

        if (strcmp(json_string_value(json_array_get(pair, 0)), bucket) == 0) {
            return json_array_get(pair, 1);
        }
    }
    return NULL;
} /* model_bucket */

/* The post-state Obj record of (bucket, key), or NULL. */
static json_t *
model_obj(
    json_t     *post_bkts,
    const char *bucket,
    const char *key)
{
    json_t *bk = model_bucket(post_bkts, bucket);
    json_t *inner;
    size_t  j;

    if (!bk) {
        return NULL;
    }
    inner = json_object_get(json_object_get(bk, "objs"), "#map");
    for (j = 0; j < json_array_size(inner); j++) {
        json_t *kv = json_array_get(inner, j);
        char    kstr[MBT_MAX_KEYLEN];

        key_str(json_array_get(kv, 0), kstr, sizeof(kstr));
        if (strcmp(kstr, key) == 0) {
            return json_array_get(kv, 1);
        }
    }
    return NULL;
} /* model_obj */

static int
model_size_blocks(
    json_t     *post_bkts,
    const char *bucket,
    const char *key)
{
    json_t *o = model_obj(post_bkts, bucket, key);

    if (!o) {
        return -1;
    }
    return (int) json_array_size(json_object_get(o, "data"));
} /* model_size_blocks */

/* The post-state Mpu record of (bucket, uplid), or NULL. */
static json_t *
model_mpu(
    json_t     *post_bkts,
    const char *bucket,
    int64_t     uplid)
{
    json_t *bk = model_bucket(post_bkts, bucket);
    json_t *inner;
    size_t  j;

    if (!bk) {
        return NULL;
    }
    inner = json_object_get(json_object_get(bk, "mpu"), "#map");
    for (j = 0; j < json_array_size(inner); j++) {
        json_t *kv = json_array_get(inner, j);

        if (itf_i64(json_array_get(kv, 0)) == uplid) {
            return json_array_get(kv, 1);
        }
    }
    return NULL;
} /* model_mpu */

/* The size in blocks of one uploaded part in the post-state, or -1. */
static int
model_part_blocks(
    json_t     *post_bkts,
    const char *bucket,
    int64_t     uplid,
    int64_t     pn)
{
    json_t *m = model_mpu(post_bkts, bucket, uplid);
    json_t *parts;
    size_t  j;

    if (!m) {
        return -1;
    }
    parts = json_object_get(json_object_get(m, "parts"), "#map");
    for (j = 0; j < json_array_size(parts); j++) {
        json_t *kv = json_array_get(parts, j);

        if (itf_i64(json_array_get(kv, 0)) == pn) {
            return (int) json_array_size(json_array_get(kv, 1));
        }
    }
    return -1;
} /* model_part_blocks */

/* The model mtag -> (Content-Type, x-amz-meta-m) mapping the harness sends
 * on PUT and expects echoed on GET/HEAD; mtag 0 sends neither and the
 * response Content-Type falls back to application/octet-stream. */
static const char *
mtag_content_type(int mtag)
{
    return mtag == 1 ? "text/plain"
         : mtag == 2 ? "application/json" : NULL;
} /* mtag_content_type */

static const char *
mtag_meta(int mtag)
{
    return mtag == 1 ? "v1" : mtag == 2 ? "v2" : NULL;
} /* mtag_meta */

/* A tag id set as its sorted id array; the harness maps id i to the
 * ("tk<i>", "tv<i>") pair. */
static int
tagset_of(
    json_t *tags,
    int    *ids,
    int     max)
{
    json_t *arr = json_object_get(tags, "#set");
    int     n   = 0;
    int     want;
    size_t  i;

    for (want = 1; want <= 9 && n < max; want++) {
        for (i = 0; i < json_array_size(arr); i++) {
            if (itf_i64(json_array_get(arr, i)) == want) {
                ids[n++] = want;
            }
        }
    }
    return n;
} /* tagset_of */

/* ---- deviation registry -------------------------------------------------- */

/* Documented divergences of chimera from official AWS S3 behavior, verified
 * by s3_mbt_probe.c (which goes red when one stops reproducing -- the signal
 * to retire its entry here).  A status divergence listed here is tolerated;
 * everything else fails the trace.  `pred`, when set, must also hold for the
 * entry to apply. */
struct deviation {
    const char *id;
    const char *op_tag;
    unsigned    expected;
    unsigned    actual;
    int         (*pred)(
        json_t *op);
};

/* range-full-200 applies only when the requested range resolves to the whole
 * object (the model's returned data spans block 0 through the full size). */
static int
dev_range_is_full(json_t *op)
{
    return op_i64(op, "first") == 0 &&
           (int64_t) json_array_size(op_field(op, "data")) ==
           op_i64(op, "total");
} /* dev_range_is_full */

/* copy-self-200 applies only to a copy of an object onto itself. */
static int
dev_copy_is_self(json_t *op)
{
    char sk[MBT_MAX_KEYLEN], dk[MBT_MAX_KEYLEN];

    key_str(op_field(op, "srcKey"), sk, sizeof(sk));
    key_str(op_field(op, "dstKey"), dk, sizeof(dk));
    return strcmp(op_str(op, "srcBucket"), op_str(op, "dstBucket")) == 0 &&
           strcmp(sk, dk) == 0;
} /* dev_copy_is_self */

static const struct deviation known_deviations[] = {
    /* AWS DeleteObject returns 204 No Content; chimera returns 200 for an
     * existing key... */
    { "delete-object-200",          "ODeleteObject",            204,               200,               NULL
    },
    /* ...and 404 NoSuchKey for a missing one (AWS is idempotent). */
    { "delete-object-missing-404",  "ODeleteObject",            204,               404,               NULL
    },
    /* AWS answers DELETE on a non-empty bucket with 409 BucketNotEmpty;
     * chimera maps BUCKET_NOT_EMPTY through its default 500 InternalError. */
    { "delete-bucket-nonempty-500", "ODeleteBucket",            409,               500,               NULL
    },
    /* AWS rejects a copy of an object onto itself (no metadata directive)
     * with 400 InvalidRequest; chimera performs it and returns 200. */
    { "copy-self-200",              "OCopyObject",              400,               200,               dev_copy_is_self
    },
    /* AWS returns 206 for every satisfiable Range, including one resolving
    * to the whole object; chimera collapses whole-object ranges to 200. */
    { "range-full-200",             "OGetObject",               206,               200,               dev_range_is_full
    },
};

static const struct deviation *
reconcile(
    const char *tag,
    json_t     *op,
    unsigned    expected,
    unsigned    actual)
{
    size_t i;

    for (i = 0; i < sizeof(known_deviations) / sizeof(known_deviations[0]);
         i++) {
        const struct deviation *d = &known_deviations[i];

        if (strcmp(d->op_tag, tag) == 0 && d->expected == expected &&
            d->actual == actual && (!d->pred || d->pred(op))) {
            return d;
        }
    }
    return NULL;
} /* reconcile */

/* ---- oracle -------------------------------------------------------------- */

struct etag_ent {
    int  used;
    char bucket[80];
    char key[MBT_MAX_KEYLEN];
    char etag[160];
};

struct hist_ent {
    int  idx;
    char tag[32];
    int  status;
    char op_dump[512];
};

/* One live multipart upload: the model's abstract id, the wire's 32-hex
 * UploadId learned from the Initiate response, and enough context to abort
 * it at trace teardown (an in-flight upload holds VFS handles that would
 * otherwise pin the per-trace filesystem forever). */
struct upl_ent {
    int  used;
    long uplid;
    char bucket[80];
    char key[MBT_MAX_KEYLEN];
    char wire[40];
};

#define MBT_MAX_UPLOADS 64

/* Per-part ETag learned from UploadPart/UploadPartCopy, keyed by the model
 * upload id + part number; replayed into the Complete manifest and checked
 * against ListParts. */
struct part_etag_ent {
    int  used;
    long uplid;
    long pn;
    char etag[160];
};

#define MBT_MAX_PART_ETAGS  256

/* A syntactically valid (32 hex chars) upload id no Initiate ever minted,
 * for the model's UNKNOWN_UPLOAD requests. */
#define MBT_UNKNOWN_WIRE_ID "ffffffffffffffffffffffffffffffff"

struct oracle {
    struct s3_mbt_env   *env;
    int                  block_size;
    int                  verbose;
    struct etag_ent      etags[MBT_MAX_ETAGS];
    struct upl_ent       upls[MBT_MAX_UPLOADS];
    struct part_etag_ent petags[MBT_MAX_PART_ETAGS];
    struct hist_ent      history[MBT_HIST];
    int                  hist_len;
    uint8_t             *expect_buf; /* expected-body scratch */
    char                *xml_buf;    /* NUL-terminated response body copy */
};

static struct upl_ent *
upl_find(
    struct oracle *o,
    long           uplid)
{
    int i;

    for (i = 0; i < MBT_MAX_UPLOADS; i++) {
        if (o->upls[i].used && o->upls[i].uplid == uplid) {
            return &o->upls[i];
        }
    }
    return NULL;
} /* upl_find */

/* The wire UploadId a model id maps to; the unknown sentinel otherwise. */
static const char *
upl_wire(
    struct oracle *o,
    long           uplid)
{
    struct upl_ent *e = upl_find(o, uplid);

    return e ? e->wire : MBT_UNKNOWN_WIRE_ID;
} /* upl_wire */

static void
upl_learn(
    struct oracle *o,
    long           uplid,
    const char    *bucket,
    const char    *key,
    const char    *wire)
{
    int i;

    for (i = 0; i < MBT_MAX_UPLOADS; i++) {
        if (!o->upls[i].used) {
            o->upls[i].used  = 1;
            o->upls[i].uplid = uplid;
            snprintf(o->upls[i].bucket, sizeof(o->upls[i].bucket), "%s", bucket);
            snprintf(o->upls[i].key, sizeof(o->upls[i].key), "%s", key);
            snprintf(o->upls[i].wire, sizeof(o->upls[i].wire), "%s", wire);
            return;
        }
    }
    fprintf(stderr, "harness limit: more than %d uploads; raise "
            "MBT_MAX_UPLOADS\n", MBT_MAX_UPLOADS);
    exit(3);
} /* upl_learn */

static void
petag_set(
    struct oracle *o,
    long           uplid,
    long           pn,
    const char    *etag)
{
    int i, free_i = -1;

    for (i = 0; i < MBT_MAX_PART_ETAGS; i++) {
        if (o->petags[i].used && o->petags[i].uplid == uplid &&
            o->petags[i].pn == pn) {
            snprintf(o->petags[i].etag, sizeof(o->petags[i].etag), "%s", etag);
            return;
        }
        if (!o->petags[i].used && free_i < 0) {
            free_i = i;
        }
    }
    if (free_i < 0) {
        fprintf(stderr, "harness limit: more than %d part etags; raise "
                "MBT_MAX_PART_ETAGS\n", MBT_MAX_PART_ETAGS);
        exit(3);
    }
    o->petags[free_i].used  = 1;
    o->petags[free_i].uplid = uplid;
    o->petags[free_i].pn    = pn;
    snprintf(o->petags[free_i].etag, sizeof(o->petags[free_i].etag), "%s", etag);
} /* petag_set */

static const char *
petag_get(
    struct oracle *o,
    long           uplid,
    long           pn)
{
    int i;

    for (i = 0; i < MBT_MAX_PART_ETAGS; i++) {
        if (o->petags[i].used && o->petags[i].uplid == uplid &&
            o->petags[i].pn == pn) {
            return o->petags[i].etag;
        }
    }
    return NULL;
} /* petag_get */

/* Drop an upload's map entry and its part ETags (Complete/Abort consumed it). */
static void
upl_forget(
    struct oracle *o,
    long           uplid)
{
    struct upl_ent *e = upl_find(o, uplid);
    int             i;

    if (e) {
        e->used = 0;
    }
    for (i = 0; i < MBT_MAX_PART_ETAGS; i++) {
        if (o->petags[i].used && o->petags[i].uplid == uplid) {
            o->petags[i].used = 0;
        }
    }
} /* upl_forget */

static struct etag_ent *
etag_find(
    struct oracle *o,
    const char    *bucket,
    const char    *key)
{
    int i;

    for (i = 0; i < MBT_MAX_ETAGS; i++) {
        if (o->etags[i].used && strcmp(o->etags[i].bucket, bucket) == 0 &&
            strcmp(o->etags[i].key, key) == 0) {
            return &o->etags[i];
        }
    }
    return NULL;
} /* etag_find */

static void
etag_learn(
    struct oracle *o,
    const char    *bucket,
    const char    *key,
    const char    *etag)
{
    struct etag_ent *e = etag_find(o, bucket, key);
    int              i;

    if (!e) {
        for (i = 0; i < MBT_MAX_ETAGS; i++) {
            if (!o->etags[i].used) {
                e = &o->etags[i];
                break;
            }
        }
        if (!e) {
            fprintf(stderr, "harness limit: more than %d live ETags; raise "
                    "MBT_MAX_ETAGS\n", MBT_MAX_ETAGS);
            exit(3);
        }
        e->used = 1;
        snprintf(e->bucket, sizeof(e->bucket), "%s", bucket);
        snprintf(e->key, sizeof(e->key), "%s", key);
    }
    snprintf(e->etag, sizeof(e->etag), "%s", etag);
} /* etag_learn */

static void
etag_forget(
    struct oracle *o,
    const char    *bucket,
    const char    *key)
{
    struct etag_ent *e = etag_find(o, bucket, key);

    if (e) {
        e->used = 0;
    }
} /* etag_forget */

/* An ETag reported for (bucket, key) must be well-formed (quoted) and match
 * the one learned at the object's last write. */
static void
etag_check(
    struct oracle *o,
    const char    *bucket,
    const char    *key,
    const char    *etag,
    const char    *what,
    struct mism   *m)
{
    struct etag_ent *e;
    size_t           n = strlen(etag);

    if (n < 2 || etag[0] != '"' || etag[n - 1] != '"') {
        mism_add(m, "%s: ETag '%s' for %s/%s is not a quoted string",
                 what, etag, bucket, key);
        return;
    }
    e = etag_find(o, bucket, key);
    if (!e) {
        /* first sighting (e.g. the write's own response): learn */
        etag_learn(o, bucket, key, etag);
        return;
    }
    if (strcmp(e->etag, etag) != 0) {
        mism_add(m, "%s: ETag for %s/%s changed without a write: learned %s, "
                 "got %s", what, bucket, key, e->etag, etag);
    }
} /* etag_check */

/* ---- status comparison --------------------------------------------------- */

enum st_result {
    ST_MATCH,
    ST_TOLERATED,
    ST_MISMATCH,
};

static enum st_result
check_status(
    struct oracle *o,
    const char    *tag,
    json_t        *op,
    unsigned       expected,
    unsigned       actual,
    struct mism   *m)
{
    const struct deviation *d;

    if (expected == actual) {
        return ST_MATCH;
    }
    d = reconcile(tag, op, expected, actual);
    if (d) {
        if (o->verbose) {
            fprintf(stderr, "  (deviation %s: %s expected %u, got %u)\n",
                    d->id, tag, expected, actual);
        }
        return ST_TOLERATED;
    }
    mism_add(m, "status: expected %u, got %u", expected, actual);
    return ST_MISMATCH;
} /* check_status */

/* ---- XML response parsing ------------------------------------------------ */

/* NUL-terminate the response body into o->xml_buf and return it. */
static const char *
resp_xml(
    struct oracle            *o,
    const struct s3_mbt_resp *res)
{
    memcpy(o->xml_buf, res->body, res->body_len);
    o->xml_buf[res->body_len] = '\0';
    if (strlen(o->xml_buf) != res->body_len) {
        /* an embedded NUL would silently truncate every check */
        fprintf(stderr, "response body contains NUL\n");
        exit(3);
    }
    return o->xml_buf;
} /* resp_xml */

/* Extract the text of the first <elem> after `from` (a scan cursor into an
 * XML document); returns the position after the element, or NULL if absent.
 * Real XML parsing is not needed: the harness's keys and bucket names are
 * [a-z/] only and the server pretty-prints one element per line. */
static const char *
xml_text_after(
    const char *from,
    const char *elem,
    char       *out,
    size_t      outlen)
{
    char        open_tag[64], close_tag[64];
    const char *s, *e;
    size_t      n;

    snprintf(open_tag, sizeof(open_tag), "<%s>", elem);
    snprintf(close_tag, sizeof(close_tag), "</%s>", elem);

    s = strstr(from, open_tag);
    if (!s) {
        return NULL;
    }
    s += strlen(open_tag);
    e  = strstr(s, close_tag);
    if (!e) {
        return NULL;
    }
    n = (size_t) (e - s);
    if (n >= outlen) {
        n = outlen - 1;
    }
    memcpy(out, s, n);
    out[n] = '\0';
    return e + strlen(close_tag);
} /* xml_text_after */

/* The <Code> of an XML error body ("" if none). */
static void
resp_error_code(
    struct oracle            *o,
    const struct s3_mbt_resp *res,
    char                     *out,
    size_t                    outlen)
{
    out[0] = '\0';
    xml_text_after(resp_xml(o, res), "Code", out, outlen);
} /* resp_error_code */

/* Compare the XML error body's <Code> against the model's err field.  Only
 * meaningful when expected == actual (a tolerated deviation's body carries
 * the server's own code, not the model's). */
static void
check_error_code(
    struct oracle            *o,
    const struct s3_mbt_resp *res,
    json_t                   *op,
    struct mism              *m)
{
    const char *want = op_str(op, "err");
    char        got[64];

    resp_error_code(o, res, got, sizeof(got));
    if (strcmp(want, got) != 0) {
        mism_add(m, "error <Code>: expected '%s', got '%s'", want, got);
    }
} /* check_error_code */

/* ---- request/response helpers ------------------------------------------- */

static void
build_path(
    json_t     *op,
    const char *bucket_field,
    const char *key_field,
    char       *out,
    size_t      outlen)
{
    char key[MBT_MAX_KEYLEN];

    if (key_field) {
        key_str(op_field(op, key_field), key, sizeof(key));
        snprintf(out, outlen, "/%s/%s", op_str(op, bucket_field), key);
    } else {
        snprintf(out, outlen, "/%s", op_str(op, bucket_field));
    }
} /* build_path */

/* Expected body for a block list; returns the byte length. */
static size_t
expect_body(
    struct oracle            *o,
    json_t                   *data,
    struct mism              *m,
    const struct s3_mbt_resp *res,
    const char               *what)
{
    int    syms[64];
    int    n   = blocks_of(data, syms, 64);
    size_t len = (size_t) n * o->block_size;

    s3_mbt_expand_blocks(syms, n, o->block_size, o->expect_buf);

    if (res->body_len != len) {
        mism_add(m, "%s: body length %zu, expected %zu", what, res->body_len,
                 len);
    } else if (len && memcmp(res->body, o->expect_buf, len) != 0) {
        size_t i;

        for (i = 0; i < len && res->body[i] == o->expect_buf[i]; i++) {
        }
        mism_add(m, "%s: body differs at offset %zu (got 0x%02x, expected "
                 "0x%02x)", what, i, res->body[i], o->expect_buf[i]);
    }
    return len;
} /* expect_body */

/* ---- per-op handlers ----------------------------------------------------- */

typedef void (*op_handler_t)(
    struct oracle *,
    json_t *op,
    json_t *post_bkts,
    struct mism *);

static void
op_create_bucket(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    char                want_loc[144];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT };
    struct s3_mbt_resp *res;

    (void) post_bkts;
    build_path(op, "bucket", NULL, path, sizeof(path));
    req.path = path;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OCreateBucket", op, (unsigned) op_i64(op, "status"),
                     (unsigned) res->status, m) != ST_MATCH) {
        return;
    }
    snprintf(want_loc, sizeof(want_loc), "%s", path);
    if (strcmp(res->location, want_loc) != 0) {
        mism_add(m, "CreateBucket Location: expected '%s', got '%s'",
                 want_loc, res->location);
    }
} /* op_create_bucket */

static void
op_head_bucket(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_HEAD };
    struct s3_mbt_resp *res;

    (void) post_bkts;
    build_path(op, "bucket", NULL, path, sizeof(path));
    req.path = path;

    res = s3_mbt_call(o->env, &req);

    check_status(o, "OHeadBucket", op, (unsigned) op_i64(op, "status"),
                 (unsigned) res->status, m);
    if (res->body_len != 0) {
        mism_add(m, "HeadBucket returned a body (%zu bytes)", res->body_len);
    }
} /* op_head_bucket */

static void
op_delete_bucket(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_DELETE };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");

    (void) post_bkts;
    build_path(op, "bucket", NULL, path, sizeof(path));
    req.path = path;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "ODeleteBucket", op, expected,
                     (unsigned) res->status, m) != ST_MATCH) {
        return;
    }
    if (expected != 204) {
        check_error_code(o, res, op, m);
    }
} /* op_delete_bucket */

static void
op_list_buckets(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                .path   = "/" };
    struct s3_mbt_resp *res;
    json_t             *want = json_object_get(op_field(op, "buckets"), "#set");
    const char         *cur;
    char                name[80];
    char                got[16][80];
    int                 ngot = 0;
    size_t              i;
    int                 j;

    (void) post_bkts;
    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OListBuckets", op, (unsigned) op_i64(op, "status"),
                     (unsigned) res->status, m) != ST_MATCH) {
        return;
    }

    /* Collect every <Bucket><Name>; compared as a set -- AWS documents no
     * ordering contract for ListBuckets (and the server iterates a hash). */
    cur = resp_xml(o, res);
    while ((cur = strstr(cur, "<Bucket>")) != NULL) {
        cur = xml_text_after(cur, "Name", name, sizeof(name));
        if (!cur) {
            break;
        }
        if (ngot < 16) {
            snprintf(got[ngot], sizeof(got[ngot]), "%s", name);
        }
        ngot++;
    }

    if ((size_t) ngot != json_array_size(want)) {
        mism_add(m, "ListBuckets: %d buckets, expected %zu", ngot,
                 json_array_size(want));
        return;
    }
    for (i = 0; i < json_array_size(want); i++) {
        const char *w     = json_string_value(json_array_get(want, i));
        int         found = 0;

        for (j = 0; j < ngot; j++) {
            if (strcmp(got[j], w) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism_add(m, "ListBuckets: bucket '%s' missing from response", w);
        }
    }
} /* op_list_buckets */

static void
op_put_object(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    int                 syms[64];
    int                 n;
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    enum st_result      st;

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));

    n = blocks_of(op_field(op, "data"), syms, 64);
    s3_mbt_expand_blocks(syms, n, o->block_size, o->expect_buf);

    req.path         = path;
    req.body         = o->expect_buf;
    req.body_len     = (size_t) n * o->block_size;
    req.content_type = mtag_content_type((int) op_i64(op, "mtag"));
    req.meta         = mtag_meta((int) op_i64(op, "mtag"));

    res = s3_mbt_call(o->env, &req);

    st = check_status(o, "OPutObject", op, expected, (unsigned) res->status, m);

    /* A successful write (whatever its exact 2xx shape) re-keys the ETag. */
    if (res->status >= 200 && res->status < 300 && expected == 200) {
        etag_forget(o, bucket, key);
        etag_check(o, bucket, key, res->etag, "PutObject", m);
    }
    if (st == ST_MATCH && expected == 404) {
        check_error_code(o, res, op, m);
    }
} /* op_put_object */

/* GET/HEAD metadata echo: the stored Content-Type (or the
 * application/octet-stream fallback) and the x-amz-meta-m value, both from
 * the post-state object's mtag. */
static void
check_meta_echo(
    struct oracle            *o,
    const struct s3_mbt_resp *res,
    json_t                   *post_bkts,
    const char               *bucket,
    const char               *key,
    const char               *what,
    struct mism              *m)
{
    json_t     *obj = model_obj(post_bkts, bucket, key);
    int         mtag;
    const char *want_ct;
    const char *want_meta;

    (void) o;
    if (!obj) {
        mism_add(m, "%s: %s/%s missing from model post-state", what, bucket,
                 key);
        return;
    }
    mtag      = (int) itf_i64(json_object_get(obj, "mtag"));
    want_ct   = mtag_content_type(mtag);
    want_meta = mtag_meta(mtag);

    if (strcmp(res->content_type,
               want_ct ? want_ct : "application/octet-stream") != 0) {
        mism_add(m, "%s: Content-Type '%s', expected '%s'", what,
                 res->content_type,
                 want_ct ? want_ct : "application/octet-stream");
    }
    if (strcmp(res->meta, want_meta ? want_meta : "") != 0) {
        mism_add(m, "%s: x-amz-meta-m '%s', expected '%s'", what, res->meta,
                 want_meta ? want_meta : "");
    }
} /* check_meta_echo */

static void
op_get_object(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    char                range_hdr[80];
    char                want_cr[96];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    const char         *rtag     = op_tag(op, "range");
    json_t             *rval     = json_object_get(op_field(op, "range"), "value");
    int64_t             bs       = o->block_size;
    enum st_result      st;
    size_t              got_len;

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));
    req.path = path;

    if (strcmp(rtag, "RClosed") == 0) {
        snprintf(range_hdr, sizeof(range_hdr),
                 "bytes=%" PRId64 "-%" PRId64,
                 itf_i64(json_object_get(rval, "first")) * bs,
                 (itf_i64(json_object_get(rval, "last")) + 1) * bs - 1);
        req.range = range_hdr;
    } else if (strcmp(rtag, "RFrom") == 0) {
        snprintf(range_hdr, sizeof(range_hdr), "bytes=%" PRId64 "-",
                 itf_i64(rval) * bs);
        req.range = range_hdr;
    } else if (strcmp(rtag, "RSuffix") == 0) {
        snprintf(range_hdr, sizeof(range_hdr), "bytes=-%" PRId64,
                 itf_i64(rval) * bs);
        req.range = range_hdr;
    }

    res = s3_mbt_call(o->env, &req);

    st = check_status(o, "OGetObject", op, expected, (unsigned) res->status, m);
    if (st == ST_MISMATCH) {
        return;
    }

    if (expected == 200 || expected == 206) {
        got_len = expect_body(o, op_field(op, "data"), m, res, "GetObject");
        if (res->has_content_length &&
            res->content_length != (int64_t) got_len) {
            mism_add(m, "GetObject Content-Length %" PRId64 ", body %zu",
                     res->content_length, got_len);
        }
        etag_check(o, bucket, key, res->etag, "GetObject", m);
        check_meta_echo(o, res, post_bkts, bucket, key, "GetObject", m);
        /* On a tolerated whole-object 200 (range-full-200) there is no
        * Content-Range; only an exact 206 must carry the right one. */
        if (st == ST_MATCH && expected == 206) {
            snprintf(want_cr, sizeof(want_cr),
                     "bytes %" PRId64 "-%" PRId64 "/%" PRId64,
                     op_i64(op, "first") * bs,
                     op_i64(op, "first") * bs + (int64_t) got_len - 1,
                     op_i64(op, "total") * bs);
            if (strcmp(res->content_range, want_cr) != 0) {
                mism_add(m, "GetObject Content-Range: expected '%s', got '%s'",
                         want_cr, res->content_range);
            }
        }
    } else if (expected == 416 && st == ST_MATCH) {
        snprintf(want_cr, sizeof(want_cr), "bytes */%" PRId64,
                 op_i64(op, "total") * bs);
        if (strcmp(res->content_range, want_cr) != 0) {
            mism_add(m, "416 Content-Range: expected '%s', got '%s'",
                     want_cr, res->content_range);
        }
        check_error_code(o, res, op, m);
    } else if (expected == 404 && st == ST_MATCH) {
        check_error_code(o, res, op, m);
    }
} /* op_get_object */

static void
op_head_object(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_HEAD };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));
    req.path = path;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OHeadObject", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (res->body_len != 0) {
        mism_add(m, "HeadObject returned a body (%zu bytes)", res->body_len);
    }
    if (expected == 200) {
        int64_t want = op_i64(op, "sizeBlocks") * o->block_size;

        if (!res->has_content_length || res->content_length != want) {
            mism_add(m, "HeadObject Content-Length: expected %" PRId64
                     ", got %" PRId64, want,
                     res->has_content_length ? res->content_length : -1);
        }
        etag_check(o, bucket, key, res->etag, "HeadObject", m);
        check_meta_echo(o, res, post_bkts, bucket, key, "HeadObject", m);

        /* x-amz-tagging-count must report the object's tag count; with no
         * tags AWS omits the header while the server under test sends "0",
         * so the zero case accepts either. */
        {
            json_t *obj = model_obj(post_bkts, bucket, key);

            if (obj) {
                int ntags = (int) json_array_size(
                    json_object_get(json_object_get(obj, "tags"), "#set"));

                if (ntags > 0) {
                    if (atoi(res->tag_count) != ntags) {
                        mism_add(m, "HeadObject x-amz-tagging-count '%s', "
                                 "expected %d", res->tag_count, ntags);
                    }
                } else if (res->tag_count[0] &&
                           strcmp(res->tag_count, "0") != 0) {
                    mism_add(m, "HeadObject x-amz-tagging-count '%s', "
                             "expected 0/absent", res->tag_count);
                }
            }
        }
    }
} /* op_head_object */

static void
op_delete_object(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_DELETE };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    enum st_result      st;

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));
    req.path = path;

    res = s3_mbt_call(o->env, &req);

    st = check_status(o, "ODeleteObject", op, expected, (unsigned) res->status,
                      m);

    /* The object is gone (or never was) whenever the bucket existed --
     * whichever of 204/200/404 came back; drop the learned ETag. */
    if (expected == 204) {
        etag_forget(o, bucket, key);
    }
    if (st == ST_MATCH && expected == 404) {
        check_error_code(o, res, op, m);
    }
} /* op_delete_object */

static void
op_copy_object(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                src[MBT_MAX_KEYLEN + 96];
    char                sb[80], db[80], sk[MBT_MAX_KEYLEN], dk[MBT_MAX_KEYLEN];
    char                etag[160];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    enum st_result      st;

    (void) post_bkts;
    snprintf(sb, sizeof(sb), "%s", op_str(op, "srcBucket"));
    snprintf(db, sizeof(db), "%s", op_str(op, "dstBucket"));
    key_str(op_field(op, "srcKey"), sk, sizeof(sk));
    key_str(op_field(op, "dstKey"), dk, sizeof(dk));

    build_path(op, "dstBucket", "dstKey", path, sizeof(path));
    snprintf(src, sizeof(src), "/%s/%s", sb, sk);

    req.path        = path;
    req.copy_source = src;

    res = s3_mbt_call(o->env, &req);

    st = check_status(o, "OCopyObject", op, expected, (unsigned) res->status,
                      m);

    if (res->status == 200 && (expected == 200 || st == ST_TOLERATED)) {
        /* the destination was (re)written: its ETag is whatever
         * <CopyObjectResult><ETag> reports */
        etag_forget(o, db, dk);
        if (xml_text_after(resp_xml(o, res), "ETag", etag, sizeof(etag))) {
            etag_check(o, db, dk, etag, "CopyObject", m);
        } else if (st == ST_MATCH) {
            mism_add(m, "CopyObject: response lacks <ETag>");
        }
        if (st == ST_MATCH &&
            !strstr(o->xml_buf, "<CopyObjectResult")) {
            mism_add(m, "CopyObject: response lacks <CopyObjectResult>");
        }
    }
    if (st == ST_MATCH && (expected == 404 || expected == 400)) {
        check_error_code(o, res, op, m);
    }
} /* op_copy_object */

/* Percent-encode a query value the way the AWS SDKs do.  Harness values
 * are [a-z/] only, so '/' -> %2F is the whole alphabet. */
static void
qenc(
    const char *src,
    char       *dst,
    size_t      dstlen)
{
    size_t off = 0;

    for (; *src && off + 4 < dstlen; src++) {
        if (*src == '/') {
            off += snprintf(dst + off, dstlen - off, "%%2F");
        } else {
            dst[off++] = *src;
        }
    }
    dst[off] = '\0';
} /* qenc */

static void
op_list_objects(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    char                query[512];
    char                pfx[MBT_MAX_KEYLEN], sa[MBT_MAX_KEYLEN];
    char                bucket[80];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET };
    struct s3_mbt_resp *res;
    unsigned            expected  = (unsigned) op_i64(op, "status");
    int                 delim     = op_bool(op, "delim");
    int                 mode      = (int) op_i64(op, "mode");
    const char         *ctag      = mode == 3 ? "<Version>" : "<Contents>";
    const char         *ctag_end  = mode == 3 ? "</Version>" : "</Contents>";
    json_t             *want_keys = op_field(op, "keys");
    json_t             *want_pfxs = op_field(op, "prefixes");
    size_t              off       = 0;
    const char         *cur;
    int                 nkeys = 0, npfx = 0;
    char                text[MBT_MAX_KEYLEN];
    char                itrunc[16], kcount[16];

    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    build_path(op, "bucket", NULL, path, sizeof(path));
    prefix_str(op_field(op, "prefix"), pfx, sizeof(pfx));
    key_str(op_field(op, "startAfter"), sa, sizeof(sa));

    /* params in byte order, values percent-encoded the way the SDKs send
     * them, matching the harness's sign-what-you-send contract.  The three
     * modes dress the same walk differently: V1 uses marker, V2 list-type=2
     * + start-after, Versions the bare versions subresource + key-marker. */
    if (delim) {
        off += snprintf(query + off, sizeof(query) - off, "delimiter=%%2F&");
    }
    if (sa[0] && mode != 2) {
        char enc[2 * MBT_MAX_KEYLEN];

        qenc(sa, enc, sizeof(enc));
        off += snprintf(query + off, sizeof(query) - off, "%s=%s&",
                        mode == 1 ? "marker" : "key-marker", enc);
    }
    if (mode == 2) {
        off += snprintf(query + off, sizeof(query) - off, "list-type=2&");
    }
    off += snprintf(query + off, sizeof(query) - off, "max-keys=%" PRId64,
                    op_i64(op, "maxKeys"));
    if (pfx[0]) {
        char enc[2 * MBT_MAX_KEYLEN];

        qenc(pfx, enc, sizeof(enc));
        off += snprintf(query + off, sizeof(query) - off, "&prefix=%s", enc);
    }
    if (sa[0] && mode == 2) {
        char enc[2 * MBT_MAX_KEYLEN];

        qenc(sa, enc, sizeof(enc));
        off += snprintf(query + off, sizeof(query) - off, "&start-after=%s",
                        enc);
    }
    if (mode == 3) {
        snprintf(query + off, sizeof(query) - off, "&versions=");
    }

    req.path  = path;
    req.query = query;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OListObjects", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected == 404) {
        check_error_code(o, res, op, m);
        return;
    }

    /* Walk the <Contents> (or <Version>) blocks in order: keys, sizes,
     * ETags. */
    cur = resp_xml(o, res);
    while ((cur = strstr(cur, ctag)) != NULL) {
        const char *blk_end = strstr(cur, ctag_end);
        const char *p;
        char        size_txt[32];
        char        etag_txt[160];

        p = xml_text_after(cur, "Key", text, sizeof(text));
        if (!p || (blk_end && p > blk_end + strlen(ctag_end))) {
            mism_add(m, "ListObjects: malformed <Contents> block");
            break;
        }
        if (nkeys < (int) json_array_size(want_keys)) {
            char want[MBT_MAX_KEYLEN];

            key_str(json_array_get(want_keys, nkeys), want, sizeof(want));
            if (strcmp(want, text) != 0) {
                mism_add(m, "ListObjects: key[%d] = '%s', expected '%s'",
                         nkeys, text, want);
            }
        }
        /* every listed object's Size and ETag must agree with the model
         * post-state / the ETag learned at its last write */
        if (xml_text_after(cur, "Size", size_txt, sizeof(size_txt))) {
            int mb = model_size_blocks(post_bkts, bucket, text);

            if (mb >= 0 &&
                strtoll(size_txt, NULL, 10) !=
                (int64_t) mb * o->block_size) {
                mism_add(m, "ListObjects: %s Size %s, model %d blocks", text,
                         size_txt, mb);
            }
        }
        if (xml_text_after(cur, "ETag", etag_txt, sizeof(etag_txt))) {
            etag_check(o, bucket, text, etag_txt, "ListObjects", m);
        }
        nkeys++;
        cur += strlen(ctag);
    }

    /* Then the <CommonPrefixes> blocks, in order. */
    cur = resp_xml(o, res);
    while ((cur = strstr(cur, "<CommonPrefixes>")) != NULL) {
        if (!xml_text_after(cur, "Prefix", text, sizeof(text))) {
            mism_add(m, "ListObjects: malformed <CommonPrefixes> block");
            break;
        }
        if (npfx < (int) json_array_size(want_pfxs)) {
            char want[MBT_MAX_KEYLEN];

            key_str(json_array_get(want_pfxs, npfx), want, sizeof(want));
            /* a common prefix always renders with a trailing slash */
            strncat(want, "/", sizeof(want) - strlen(want) - 1);
            if (strcmp(want, text) != 0) {
                mism_add(m, "ListObjects: prefix[%d] = '%s', expected '%s'",
                         npfx, text, want);
            }
        }
        npfx++;
        cur += strlen("<CommonPrefixes>");
    }

    if (nkeys != (int) json_array_size(want_keys)) {
        /* name the tail keys the model did not predict -- the first
         * unexpected one is usually the story (a phantom entry, a stale
         * cache hit, an internal temp name) */
        mism_add(m, "ListObjects: %d keys, expected %zu (last parsed: '%s')",
                 nkeys, json_array_size(want_keys), text);
    }
    if (npfx != (int) json_array_size(want_pfxs)) {
        mism_add(m, "ListObjects: %d common prefixes, expected %zu", npfx,
                 json_array_size(want_pfxs));
    }

    /* IsTruncated and KeyCount (v2 counts both sections). */
    itrunc[0] = '\0';
    xml_text_after(o->xml_buf, "IsTruncated", itrunc, sizeof(itrunc));
    if (strcmp(itrunc, op_bool(op, "truncated") ? "true" : "false") != 0) {
        mism_add(m, "ListObjects: IsTruncated '%s', expected '%s'", itrunc,
                 op_bool(op, "truncated") ? "true" : "false");
    }
    /* KeyCount is a V2-only element. */
    if (mode == 2) {
        kcount[0] = '\0';
        if (xml_text_after(o->xml_buf, "KeyCount", kcount, sizeof(kcount))) {
            int want = (int) (json_array_size(want_keys) +
                              json_array_size(want_pfxs));

            if (atoi(kcount) != want) {
                mism_add(m, "ListObjects: KeyCount %s, expected %d", kcount,
                         want);
            }
        } else {
            mism_add(m, "ListObjects: response lacks <KeyCount>");
        }
    }
} /* op_list_objects */

/* ---- DeleteObjects batch ------------------------------------------------- */

static void
op_delete_objects(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    char                bucket[80];
    char                body[4096];
    size_t              blen = 0;
    struct s3_mbt_req   req  = { .method = EVPL_HTTP_REQUEST_TYPE_POST };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    int                 quiet    = op_bool(op, "quiet");
    json_t             *keys     = op_field(op, "keys");
    size_t              i;
    const char         *cur;
    char                text[MBT_MAX_KEYLEN];
    int                 ndel = 0;

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    build_path(op, "bucket", NULL, path, sizeof(path));

    blen += snprintf(body + blen, sizeof(body) - blen, "<Delete>");
    if (quiet) {
        blen += snprintf(body + blen, sizeof(body) - blen,
                         "<Quiet>true</Quiet>");
    }
    for (i = 0; i < json_array_size(keys); i++) {
        char kstr[MBT_MAX_KEYLEN];

        key_str(json_array_get(keys, i), kstr, sizeof(kstr));
        blen += snprintf(body + blen, sizeof(body) - blen,
                         "<Object><Key>%s</Key></Object>", kstr);
    }
    blen += snprintf(body + blen, sizeof(body) - blen, "</Delete>");

    req.path     = path;
    req.query    = "delete=";
    req.body     = (const uint8_t *) body;
    req.body_len = blen;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "ODeleteObjects", op, expected,
                     (unsigned) res->status, m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }

    /* every named key is gone; drop the learned ETags */
    for (i = 0; i < json_array_size(keys); i++) {
        char kstr[MBT_MAX_KEYLEN];

        key_str(json_array_get(keys, i), kstr, sizeof(kstr));
        etag_forget(o, bucket, kstr);
    }

    /* non-quiet: one <Deleted><Key> per named key, in request order;
     * quiet: none.  Never an <Error> (every key is deletable as root). */
    cur = resp_xml(o, res);
    while ((cur = strstr(cur, "<Deleted>")) != NULL) {
        if (!xml_text_after(cur, "Key", text, sizeof(text))) {
            mism_add(m, "DeleteObjects: malformed <Deleted> block");
            break;
        }
        if (!quiet && ndel < (int) json_array_size(keys)) {
            char want[MBT_MAX_KEYLEN];

            key_str(json_array_get(keys, ndel), want, sizeof(want));
            if (strcmp(want, text) != 0) {
                mism_add(m, "DeleteObjects: Deleted[%d] = '%s', expected '%s'",
                         ndel, text, want);
            }
        }
        ndel++;
        cur += strlen("<Deleted>");
    }
    if (quiet && ndel != 0) {
        mism_add(m, "DeleteObjects: %d <Deleted> entries in Quiet mode", ndel);
    }
    if (!quiet && ndel != (int) json_array_size(keys)) {
        mism_add(m, "DeleteObjects: %d <Deleted> entries, expected %zu", ndel,
                 json_array_size(keys));
    }
    if (strstr(o->xml_buf, "<Error>")) {
        mism_add(m, "DeleteObjects: unexpected <Error> entry");
    }
} /* op_delete_objects */

/* ---- GetObjectAttributes ------------------------------------------------- */

static void
op_get_attrs(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    char                text[160];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));

    /* The x-amz-object-attributes header is deliberately not sent: the
     * server ignores it and always reports the filesystem-suppliable fixed
     * set (ETag, StorageClass, ObjectSize) this handler checks.  (AWS would
     * 400 without the header; that strictness is not modeled.) */
    req.path  = path;
    req.query = "attributes=";

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OGetAttrs", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }

    if (xml_text_after(resp_xml(o, res), "ObjectSize", text, sizeof(text))) {
        int64_t want = op_i64(op, "sizeBlocks") * o->block_size;

        if (strtoll(text, NULL, 10) != want) {
            mism_add(m, "GetAttrs: ObjectSize %s, expected %" PRId64, text,
                     want);
        }
    } else {
        mism_add(m, "GetAttrs: response lacks <ObjectSize>");
    }
    if (!xml_text_after(o->xml_buf, "StorageClass", text, sizeof(text)) ||
        strcmp(text, "STANDARD") != 0) {
        mism_add(m, "GetAttrs: StorageClass missing or not STANDARD");
    }
    /* the attributes ETag is the same value GET/HEAD report, unquoted */
    if (xml_text_after(o->xml_buf, "ETag", text, sizeof(text))) {
        char quoted[164];

        snprintf(quoted, sizeof(quoted), "\"%s\"", text);
        etag_check(o, bucket, key, quoted, "GetAttrs", m);
    } else {
        mism_add(m, "GetAttrs: response lacks <ETag>");
    }
} /* op_get_attrs */

/* ---- tagging ------------------------------------------------------------- */

/* <Tagging><TagSet><Tag><Key>tk<i></Key><Value>tv<i></Value></Tag>... */
static size_t
tagging_body(
    const int *ids,
    int        n,
    char      *out,
    size_t     outlen)
{
    size_t off = 0;
    int    i;

    off += snprintf(out + off, outlen - off, "<Tagging><TagSet>");
    for (i = 0; i < n; i++) {
        off += snprintf(out + off, outlen - off,
                        "<Tag><Key>tk%d</Key><Value>tv%d</Value></Tag>",
                        ids[i], ids[i]);
    }
    off += snprintf(out + off, outlen - off, "</TagSet></Tagging>");
    return off;
} /* tagging_body */

/* Collect the tag ids of a <Tagging> response (tags named tk<i> carrying
 * value tv<i>). */
static int
parse_tagset(
    struct oracle            *o,
    const struct s3_mbt_resp *res,
    int                      *ids,
    int                       max,
    struct mism              *m)
{
    const char *cur = resp_xml(o, res);
    char        ktext[64], vtext[64];
    int         n = 0;

    while ((cur = strstr(cur, "<Tag>")) != NULL) {
        if (!xml_text_after(cur, "Key", ktext, sizeof(ktext)) ||
            !xml_text_after(cur, "Value", vtext, sizeof(vtext))) {
            mism_add(m, "tagging: malformed <Tag> block");
            break;
        }
        if (n < max) {
            /* an unexpected tag key still fills its slot (with a sentinel
            * no model tag id uses), so every returned index is defined */
            ids[n] = -1;
        }
        if (strncmp(ktext, "tk", 2) != 0) {
            mism_add(m, "tagging: unexpected tag key '%s'", ktext);
        } else if (n < max) {
            ids[n] = atoi(ktext + 2);
            if (strncmp(vtext, "tv", 2) != 0 || atoi(vtext + 2) != ids[n]) {
                mism_add(m, "tagging: tag %s has value '%s'", ktext, vtext);
            }
        }
        n++;
        cur += strlen("<Tag>");
    }
    return n;
} /* parse_tagset */

/* Compare a parsed tag-id list against the label's tag set. */
static void
check_tagset(
    json_t      *want_tags,
    const int   *ids,
    int          n,
    const char  *what,
    struct mism *m)
{
    int want_ids[16];
    int wn = tagset_of(want_tags, want_ids, 16);
    int i, j, found;

    if (n != wn) {
        mism_add(m, "%s: %d tags, expected %d", what, n, wn);
        return;
    }
    for (i = 0; i < wn; i++) {
        found = 0;
        for (j = 0; j < n; j++) {
            if (ids[j] == want_ids[i]) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism_add(m, "%s: tag id %d missing", what, want_ids[i]);
        }
    }
} /* check_tagset */

/* One handler serves the object- and bucket-level variants of each tagging
 * verb; the object one has a key. */
static void
tagging_common(
    struct oracle              *o,
    json_t                     *op,
    struct mism                *m,
    const char                 *tag,
    enum evpl_http_request_type method,
    int                         has_key,
    int                         send_tags)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                body[2048];
    struct s3_mbt_req   req = { .method = method };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    int                 ids[16];
    int                 n;

    build_path(op, "bucket", has_key ? "key" : NULL, path, sizeof(path));
    req.path  = path;
    req.query = "tagging=";

    if (send_tags) {
        n            = tagset_of(op_field(op, "tags"), ids, 16);
        req.body     = (const uint8_t *) body;
        req.body_len = tagging_body(ids, n, body, sizeof(body));
    }

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, tag, op, expected, (unsigned) res->status, m)
        != ST_MATCH) {
        return;
    }
    if (expected != 200 && expected != 204) {
        check_error_code(o, res, op, m);
        return;
    }
    if (method == EVPL_HTTP_REQUEST_TYPE_GET && expected == 200) {
        n = parse_tagset(o, res, ids, 16, m);
        check_tagset(op_field(op, "tags"), ids, n, tag, m);
    }
} /* tagging_common */

static void
op_put_obj_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "OPutObjTagging", EVPL_HTTP_REQUEST_TYPE_PUT,
                   1, 1);
} /* op_put_obj_tagging */

static void
op_get_obj_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "OGetObjTagging", EVPL_HTTP_REQUEST_TYPE_GET,
                   1, 0);
} /* op_get_obj_tagging */

static void
op_del_obj_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "ODelObjTagging", EVPL_HTTP_REQUEST_TYPE_DELETE,
                   1, 0);
} /* op_del_obj_tagging */

static void
op_put_bkt_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "OPutBktTagging", EVPL_HTTP_REQUEST_TYPE_PUT,
                   0, 1);
} /* op_put_bkt_tagging */

static void
op_get_bkt_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "OGetBktTagging", EVPL_HTTP_REQUEST_TYPE_GET,
                   0, 0);
} /* op_get_bkt_tagging */

static void
op_del_bkt_tagging(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    (void) post_bkts;
    tagging_common(o, op, m, "ODelBktTagging", EVPL_HTTP_REQUEST_TYPE_DELETE,
                   0, 0);
} /* op_del_bkt_tagging */

/* ---- multipart ----------------------------------------------------------- */

static void
op_create_mpu(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    char                text[64];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_POST };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));
    req.path  = path;
    req.query = "uploads=";

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OCreateMpu", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }
    if (!xml_text_after(resp_xml(o, res), "UploadId", text, sizeof(text)) ||
        strlen(text) != 32) {
        mism_add(m, "CreateMpu: missing/malformed <UploadId> ('%s')", text);
        return;
    }
    upl_learn(o, (long) op_i64(op, "uplid"), bucket, key, text);

    if (!xml_text_after(o->xml_buf, "Key", text, sizeof(text)) ||
        strcmp(text, key) != 0) {
        mism_add(m, "CreateMpu: <Key> mismatch");
    }
} /* op_create_mpu */

static void
op_upload_part(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                query[96];
    int                 syms[64];
    int                 n;
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    long                uplid    = (long) op_i64(op, "uplid");
    long                pn       = (long) op_i64(op, "partNum");

    (void) post_bkts;
    build_path(op, "bucket", "key", path, sizeof(path));
    snprintf(query, sizeof(query), "partNumber=%ld&uploadId=%s", pn,
             upl_wire(o, uplid));

    n = blocks_of(op_field(op, "data"), syms, 64);
    s3_mbt_expand_blocks(syms, n, o->block_size, o->expect_buf);

    req.path     = path;
    req.query    = query;
    req.body     = o->expect_buf;
    req.body_len = (size_t) n * o->block_size;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OUploadPart", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }
    if (res->etag[0] != '"') {
        mism_add(m, "UploadPart: ETag missing/unquoted ('%s')", res->etag);
        return;
    }
    petag_set(o, uplid, pn, res->etag);
} /* op_upload_part */

static void
op_upload_part_copy(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                query[96];
    char                src[MBT_MAX_KEYLEN + 96];
    char                range_hdr[80];
    char                text[160];
    char                sk[MBT_MAX_KEYLEN];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    long                uplid    = (long) op_i64(op, "uplid");
    long                pn       = (long) op_i64(op, "partNum");
    const char         *rtag     = op_tag(op, "range");
    json_t             *rval     = json_object_get(op_field(op, "range"), "value");
    int64_t             bs       = o->block_size;

    (void) post_bkts;
    build_path(op, "bucket", "key", path, sizeof(path));
    key_str(op_field(op, "srcKey"), sk, sizeof(sk));
    snprintf(src, sizeof(src), "/%s/%s", op_str(op, "srcBucket"), sk);
    snprintf(query, sizeof(query), "partNumber=%ld&uploadId=%s", pn,
             upl_wire(o, uplid));

    req.path        = path;
    req.query       = query;
    req.copy_source = src;

    if (strcmp(rtag, "RClosed") == 0) {
        snprintf(range_hdr, sizeof(range_hdr),
                 "bytes=%" PRId64 "-%" PRId64,
                 itf_i64(json_object_get(rval, "first")) * bs,
                 (itf_i64(json_object_get(rval, "last")) + 1) * bs - 1);
        req.copy_range = range_hdr;
    }

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OUploadPartCopy", op, expected,
                     (unsigned) res->status, m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }
    /* the part's ETag arrives in the <CopyPartResult> body, not a header */
    if (!strstr(resp_xml(o, res), "<CopyPartResult")) {
        mism_add(m, "UploadPartCopy: response lacks <CopyPartResult>");
        return;
    }
    if (!xml_text_after(o->xml_buf, "ETag", text, sizeof(text))) {
        mism_add(m, "UploadPartCopy: response lacks <ETag>");
        return;
    }
    petag_set(o, uplid, pn, text);
} /* op_upload_part_copy */

static void
op_complete_mpu(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                query[96];
    char                bucket[80], key[MBT_MAX_KEYLEN];
    char                body[4096];
    char                text[160];
    size_t              blen = 0;
    struct s3_mbt_req   req  = { .method = EVPL_HTTP_REQUEST_TYPE_POST };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    long                uplid    = (long) op_i64(op, "uplid");
    json_t             *manifest = op_field(op, "manifest");
    size_t              i;

    (void) post_bkts;
    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    key_str(op_field(op, "key"), key, sizeof(key));
    build_path(op, "bucket", "key", path, sizeof(path));
    snprintf(query, sizeof(query), "uploadId=%s", upl_wire(o, uplid));

    blen += snprintf(body + blen, sizeof(body) - blen,
                     "<CompleteMultipartUpload>");
    for (i = 0; i < json_array_size(manifest); i++) {
        long        pn = (long) itf_i64(json_array_get(manifest, i));
        const char *pe = petag_get(o, uplid, pn);

        /* a never-uploaded part still needs a syntactically valid ETag --
         * the parser requires one per <Part> */
        blen += snprintf(body + blen, sizeof(body) - blen,
                         "<Part><PartNumber>%ld</PartNumber><ETag>%s</ETag>"
                         "</Part>", pn,
                         pe ? pe : "\"00000000000000000000000000000000\"");
    }
    blen += snprintf(body + blen, sizeof(body) - blen,
                     "</CompleteMultipartUpload>");

    req.path     = path;
    req.query    = query;
    req.body     = (const uint8_t *) body;
    req.body_len = blen;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OCompleteMpu", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }

    /* the upload is consumed and the key now holds the assembled object;
     * its ETag will be learned from the next read -- the Complete
     * response's own "<hex>-<N>" ETag is a digest over the part ETags and
     * is NOT the value later GET/HEAD report (deviation
     * mpu-complete-etag-detached; AWS reports one consistent value) */
    upl_forget(o, uplid);
    etag_forget(o, bucket, key);

    if (!strstr(resp_xml(o, res), "<CompleteMultipartUploadResult")) {
        mism_add(m, "CompleteMpu: response lacks result element");
        return;
    }
    if (xml_text_after(o->xml_buf, "ETag", text, sizeof(text))) {
        char want_suffix[16];

        snprintf(want_suffix, sizeof(want_suffix), "-%zu\"",
                 json_array_size(manifest));
        if (strlen(text) < strlen(want_suffix) ||
            strcmp(text + strlen(text) - strlen(want_suffix),
                   want_suffix) != 0) {
            mism_add(m, "CompleteMpu: ETag '%s' lacks part-count suffix %s",
                     text, want_suffix);
        }
    } else {
        mism_add(m, "CompleteMpu: response lacks <ETag>");
    }
} /* op_complete_mpu */

static void
op_abort_mpu(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                query[96];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_DELETE };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    long                uplid    = (long) op_i64(op, "uplid");

    (void) post_bkts;
    build_path(op, "bucket", "key", path, sizeof(path));
    snprintf(query, sizeof(query), "uploadId=%s", upl_wire(o, uplid));
    req.path  = path;
    req.query = query;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OAbortMpu", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected == 204) {
        upl_forget(o, uplid);
    } else {
        check_error_code(o, res, op, m);
    }
} /* op_abort_mpu */

static void
op_list_parts(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[MBT_MAX_KEYLEN + 96];
    char                query[96];
    char                bucket[80];
    char                text[160];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    long                uplid    = (long) op_i64(op, "uplid");
    json_t             *want     = op_field(op, "partNums");
    const char         *cur;
    int                 np = 0;

    snprintf(bucket, sizeof(bucket), "%s", op_str(op, "bucket"));
    build_path(op, "bucket", "key", path, sizeof(path));
    snprintf(query, sizeof(query), "uploadId=%s", upl_wire(o, uplid));
    req.path  = path;
    req.query = query;

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OListParts", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }

    cur = resp_xml(o, res);
    while ((cur = strstr(cur, "<Part>")) != NULL) {
        long pn;

        if (!xml_text_after(cur, "PartNumber", text, sizeof(text))) {
            mism_add(m, "ListParts: malformed <Part> block");
            break;
        }
        pn = atol(text);
        if (np < (int) json_array_size(want) &&
            pn != itf_i64(json_array_get(want, np))) {
            mism_add(m, "ListParts: part[%d] = %ld, expected %" PRId64, np,
                     pn, itf_i64(json_array_get(want, np)));
        }
        if (xml_text_after(cur, "Size", text, sizeof(text))) {
            int mb = model_part_blocks(post_bkts, bucket, uplid, pn);

            if (mb >= 0 &&
                strtoll(text, NULL, 10) != (int64_t) mb * o->block_size) {
                mism_add(m, "ListParts: part %ld Size %s, model %d blocks",
                         pn, text, mb);
            }
        }
        if (xml_text_after(cur, "ETag", text, sizeof(text))) {
            const char *pe = petag_get(o, uplid, pn);

            if (pe && strcmp(pe, text) != 0) {
                mism_add(m, "ListParts: part %ld ETag '%s', learned '%s'",
                         pn, text, pe);
            }
        }
        np++;
        cur += strlen("<Part>");
    }
    if (np != (int) json_array_size(want)) {
        mism_add(m, "ListParts: %d parts, expected %zu", np,
                 json_array_size(want));
    }
} /* op_list_parts */

static void
op_list_mpu(
    struct oracle *o,
    json_t        *op,
    json_t        *post_bkts,
    struct mism   *m)
{
    char                path[128];
    char                ktext[MBT_MAX_KEYLEN], utext[64];
    struct s3_mbt_req   req = { .method = EVPL_HTTP_REQUEST_TYPE_GET };
    struct s3_mbt_resp *res;
    unsigned            expected = (unsigned) op_i64(op, "status");
    json_t             *want     = json_object_get(op_field(op, "uploads"), "#set");
    const char         *cur;
    int                 nup = 0;

    (void) post_bkts;
    build_path(op, "bucket", NULL, path, sizeof(path));
    req.path  = path;
    req.query = "uploads=";

    res = s3_mbt_call(o->env, &req);

    if (check_status(o, "OListMpu", op, expected, (unsigned) res->status,
                     m) != ST_MATCH) {
        return;
    }
    if (expected != 200) {
        check_error_code(o, res, op, m);
        return;
    }

    /* Compared as a set: each response row's UploadId must map (via the
     * learned wire ids) to a model upload whose {key, uplid} the label
     * carries; the counts must agree. */
    cur = resp_xml(o, res);
    while ((cur = strstr(cur, "<Upload>")) != NULL) {
        struct upl_ent *e;
        int             found = 0;
        size_t          i;

        if (!xml_text_after(cur, "Key", ktext, sizeof(ktext)) ||
            !xml_text_after(cur, "UploadId", utext, sizeof(utext))) {
            mism_add(m, "ListMpu: malformed <Upload> block");
            break;
        }
        e = NULL;
        for (i = 0; i < MBT_MAX_UPLOADS; i++) {
            if (o->upls[i].used && strcmp(o->upls[i].wire, utext) == 0) {
                e = &o->upls[i];
                break;
            }
        }
        if (!e) {
            mism_add(m, "ListMpu: unknown UploadId '%s' (key '%s')", utext,
                     ktext);
        } else {
            for (i = 0; i < json_array_size(want); i++) {
                json_t *row = json_array_get(want, i);
                char    wk[MBT_MAX_KEYLEN];

                key_str(json_object_get(row, "key"), wk, sizeof(wk));
                if (itf_i64(json_object_get(row, "uplid")) == e->uplid &&
                    strcmp(wk, ktext) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism_add(m, "ListMpu: upload %ld ('%s') not in the model set",
                         e->uplid, ktext);
            }
        }
        nup++;
        cur += strlen("<Upload>");
    }
    if (nup != (int) json_array_size(want)) {
        mism_add(m, "ListMpu: %d uploads, expected %zu", nup,
                 json_array_size(want));
    }
} /* op_list_mpu */

/* ---- dispatch ------------------------------------------------------------ */

static const struct {
    const char  *tag;
    op_handler_t fn;
} handlers[] = {
/* *INDENT-OFF* */
    { "OCreateBucket",   op_create_bucket    },
    { "OHeadBucket",     op_head_bucket      },
    { "ODeleteBucket",   op_delete_bucket    },
    { "OListBuckets",    op_list_buckets     },
    { "OPutObject",      op_put_object       },
    { "OGetObject",      op_get_object       },
    { "OHeadObject",     op_head_object      },
    { "ODeleteObject",   op_delete_object    },
    { "ODeleteObjects",  op_delete_objects   },
    { "OCopyObject",     op_copy_object      },
    { "OGetAttrs",       op_get_attrs        },
    { "OListObjects",    op_list_objects     },
    { "OPutObjTagging",  op_put_obj_tagging  },
    { "OGetObjTagging",  op_get_obj_tagging  },
    { "ODelObjTagging",  op_del_obj_tagging  },
    { "OPutBktTagging",  op_put_bkt_tagging  },
    { "OGetBktTagging",  op_get_bkt_tagging  },
    { "ODelBktTagging",  op_del_bkt_tagging  },
    { "OCreateMpu",      op_create_mpu       },
    { "OUploadPart",     op_upload_part      },
    { "OUploadPartCopy", op_upload_part_copy },
    { "OCompleteMpu",    op_complete_mpu     },
    { "OAbortMpu",       op_abort_mpu        },
    { "OListParts",      op_list_parts       },
    { "OListMpu",        op_list_mpu         },
/* *INDENT-ON* */
};

static op_handler_t
find_handler(const char *tag)
{
    size_t i;

    for (i = 0; i < sizeof(handlers) / sizeof(handlers[0]); i++) {
        if (strcmp(handlers[i].tag, tag) == 0) {
            return handlers[i].fn;
        }
    }
    return NULL;
} /* find_handler */

/* ---- history + divergence report ---------------------------------------- */

static void
history_push(
    struct oracle *o,
    int            idx,
    const char    *tag,
    int            status,
    json_t        *op)
{
    struct hist_ent *h;
    char            *dump;

    if (o->hist_len == MBT_HIST) {
        memmove(&o->history[0], &o->history[1],
                sizeof(o->history[0]) * (MBT_HIST - 1));
        o->hist_len--;
    }
    h         = &o->history[o->hist_len++];
    h->idx    = idx;
    h->status = status;
    snprintf(h->tag, sizeof(h->tag), "%s", tag);
    dump = json_dumps(op, JSON_COMPACT | JSON_SORT_KEYS);
    snprintf(h->op_dump, sizeof(h->op_dump), "%s", dump ? dump : "?");
    free(dump);
} /* history_push */

static void
report_divergence(
    struct oracle *o,
    const char    *trace_path,
    int            idx,
    const char    *tag,
    json_t        *op,
    struct mism   *m)
{
    char *dump = json_dumps(op, JSON_COMPACT | JSON_SORT_KEYS);
    int   i;

    fprintf(stderr, "\n=== DIVERGENCE in %s ===\n", trace_path);
    fprintf(stderr, "step %d: %s %s\n", idx, tag, dump ? dump : "?");
    free(dump);

    for (i = 0; i < m->count; i++) {
        fprintf(stderr, "  MISMATCH: %s\n", m->msg[i]);
    }

    fprintf(stderr, "last %d ops:\n", o->hist_len);
    for (i = 0; i < o->hist_len; i++) {
        fprintf(stderr, "  [%d] %s -> %d  %s\n", o->history[i].idx,
                o->history[i].tag, o->history[i].status,
                o->history[i].op_dump);
    }
} /* report_divergence */

/* ---- trace driver -------------------------------------------------------- */

static int
run_trace(
    struct s3_mbt_env *env,
    const char        *trace_path,
    const char        *fsname,
    int                block_size,
    int                verbose,
    int                dry_run)
{
    json_error_t   jerr;
    json_t        *trace, *states;
    struct oracle *o;
    int            rc = 0;
    size_t         idx, nstates;

    trace = json_load_file(trace_path, 0, &jerr);
    if (!trace) {
        fprintf(stderr, "%s: JSON parse error: %s (line %d)\n", trace_path,
                jerr.text, jerr.line);
        return 1;
    }

    states = json_object_get(trace, "states");
    if (!states || !json_is_array(states) ||
        !json_object_get(trace, "vars")) {
        fprintf(stderr, "%s: not an ITF trace\n", trace_path);
        json_decref(trace);
        return 1;
    }
    nstates = json_array_size(states);

    if (dry_run) {
        printf("%s: %zu steps\n", trace_path, nstates - 1);
        json_decref(trace);
        return 0;
    }

    /* Everything runs in one process, so a request the server never answers
     * would spin in s3_mbt_call forever; SIGALRM's default disposition kills
     * the test with the trace name already printed. */
    alarm(120);

    if (verbose) {
        printf("%s: %zu steps\n", trace_path, nstates - 1);
    }

    s3_mbt_env_fs_setup(env, fsname);

    o             = calloc(1, sizeof(*o));
    o->env        = env;
    o->block_size = block_size;
    o->verbose    = verbose;
    o->expect_buf = malloc((size_t) 64 * block_size);
    o->xml_buf    = malloc(S3_MBT_BODY_MAX + 1);

    for (idx = 1; idx < nstates; idx++) {
        json_t      *state     = json_array_get(states, idx);
        json_t      *last_op   = json_object_get(state, "lastOp");
        json_t      *post_bkts = json_object_get(state, "bkts");
        const char  *tag;
        json_t      *op;
        op_handler_t fn;
        struct mism  m;

        if (!last_op || !post_bkts) {
            fprintf(stderr, "%s: state %zu lacks lastOp/bkts\n", trace_path,
                    idx);
            rc = 1;
            break;
        }
        tag = json_string_value(json_object_get(last_op, "tag"));
        op  = json_object_get(last_op, "value");

        fn = find_handler(tag);
        if (!fn) {
            fprintf(stderr, "%s: state %zu has unknown op '%s'\n", trace_path,
                    idx, tag);
            rc = 1;
            break;
        }

        memset(&m, 0, sizeof(m));
        fn(o, op, post_bkts, &m);
        history_push(o, (int) idx, tag, env->res.status, op);

        if (m.count) {
            report_divergence(o, trace_path, (int) idx, tag, op, &m);
            rc = 1;
            break;
        }
    }

    /* Abort any upload the trace left in flight: an in-flight upload holds
     * open VFS handles (pinning the per-trace filesystem so rmfs never
     * drains), and its record lives in a process-global table keyed only by
     * id and bucket NAME -- unaborted, it would resurface in the next
     * trace's same-named bucket's ListMultipartUploads. */
    {
        int u;

        for (u = 0; u < MBT_MAX_UPLOADS; u++) {
            struct s3_mbt_req areq = { .method = EVPL_HTTP_REQUEST_TYPE_DELETE };
            char              apath[MBT_MAX_KEYLEN + 96];
            char              aquery[96];

            if (!o->upls[u].used) {
                continue;
            }
            snprintf(apath, sizeof(apath), "/%s/%s", o->upls[u].bucket,
                     o->upls[u].key);
            snprintf(aquery, sizeof(aquery), "uploadId=%s", o->upls[u].wire);
            areq.path  = apath;
            areq.query = aquery;
            s3_mbt_call(env, &areq);
        }
    }

    s3_mbt_env_fs_teardown(env, fsname);

    free(o->expect_buf);
    free(o->xml_buf);
    free(o);
    json_decref(trace);
    alarm(0);

    return rc;
} /* run_trace */

int
main(
    int    argc,
    char **argv)
{
    static struct option long_options[] = {
/* *INDENT-OFF* */
        { "trace",          required_argument, 0, 't' },
        { "trace-dir",      required_argument, 0, 'D' },
        { "exclude-prefix", required_argument, 0, 'X' },
        { "block-size",     required_argument, 0, 'b' },
        { "verbose",        no_argument,       0, 'v' },
        { "dry-run",        no_argument,       0, 'n' },
        { "sigv2",          no_argument,       0, '2' },
        { "backend",        required_argument, 0, 'B' },
        { 0, 0, 0, 0 }
/* *INDENT-ON* */
    };
    struct s3_mbt_env    env;
    char               **traces;
    int                  ntraces;
    int                  block_size = 8192;
    int                  verbose = 0, dry_run = 0, sigv2 = 0;
    const char          *backend  = "memfs";
    int                  failures = 0;
    int                  i, c;

    /* mbt_collect_traces scans raw argv for the trace options; getopt then
     * only needs to recognize the rest. */
    traces = mbt_collect_traces(argc, argv, &ntraces);

    while ((c = getopt_long(argc, argv, "t:D:X:b:vn2B:", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;
            case 'b':
                block_size = atoi(optarg);
                break;
            case 'v':
                verbose = 1;
                break;
            case 'n':
                dry_run = 1;
                break;
            case '2':
                sigv2 = 1;
                break;
            case 'B':
                backend = optarg;
                break;
            default:
                fprintf(stderr,
                        "usage: %s [--trace f.itf.json | --trace-dir dir]\n"
                        "          [--exclude-prefix p] [--block-size n]\n"
                        "          [--sigv2] [--backend m] [--verbose] [--dry-run]\n", argv[0]);
                mbt_free_traces(traces, ntraces);
                return 2;
        } /* switch */
    }

    if (ntraces == 0) {
        fprintf(stderr, "no traces given (--trace / --trace-dir)\n");
        mbt_free_traces(traces, ntraces);
        return 2;
    }

    s3_mbt_env_open_module(&env, backend);
    env.sigv2 = sigv2;

    for (i = 0; i < ntraces; i++) {
        char fsname[32];

        snprintf(fsname, sizeof(fsname), "fs_%d", i);
        failures += run_trace(&env, traces[i], fsname, block_size, verbose,
                              dry_run);
    }

    s3_mbt_env_stop(&env);
    mbt_free_traces(traces, ntraces);

    if (failures) {
        fprintf(stderr, "%d trace(s) diverged\n", failures);
        return 1;
    }

    printf("%d trace(s) replayed with no divergence\n", ntraces);
    return 0;
} /* main */
