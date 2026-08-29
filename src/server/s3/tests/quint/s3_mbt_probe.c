// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ground-truth probe for the S3 MBT harness: brings up the in-process S3
 * server + HTTP client (s3_mbt_common.h), walks the modeled surface once
 * end-to-end, and asserts each documented chimera deviation from official
 * AWS S3 behavior still reproduces.  Goes red when chimera is fixed -- the
 * signal to retire the corresponding entry in s3_mbt_replay.c's deviation
 * registry (and flip the model expectation if one was encoded there) -- or
 * if a deviation's shape drifts.  Needs no trace corpus. */

#include "s3_mbt_common.h"
#include "common/mbt_watchdog.h"

static int failures;

#define CHECK(cond, ...)                                \
        do {                                            \
            if (!(cond)) {                              \
                fprintf(stderr, "FAIL(line %d): ", __LINE__); \
                fprintf(stderr, __VA_ARGS__);           \
                fprintf(stderr, "\n");                  \
                failures++;                             \
            }                                           \
        } while (0)

#define BS 8192

static struct s3_mbt_resp *
simple(
    struct s3_mbt_env          *env,
    enum evpl_http_request_type method,
    const char                 *path)
{
    struct s3_mbt_req req = { .method = method, .path = path };

    return s3_mbt_call(env, &req);
} /* simple */

static struct s3_mbt_resp *
put_blocks(
    struct s3_mbt_env *env,
    const char        *path,
    const int         *syms,
    int                nsyms)
{
    static uint8_t    body[4 * BS];
    struct s3_mbt_req req = { .method   = EVPL_HTTP_REQUEST_TYPE_PUT,
                              .path     = path,                      .body = body,
                              .body_len = (size_t) nsyms * BS };

    s3_mbt_expand_blocks(syms, nsyms, BS, body);
    return s3_mbt_call(env, &req);
} /* put_blocks */

static int
body_is_blocks(
    const struct s3_mbt_resp *res,
    const int                *syms,
    int                       nsyms)
{
    static uint8_t want[4 * BS];

    if (res->body_len != (size_t) nsyms * BS) {
        return 0;
    }
    s3_mbt_expand_blocks(syms, nsyms, BS, want);
    return memcmp(res->body, want, res->body_len) == 0;
} /* body_is_blocks */

static int
body_has(
    const struct s3_mbt_resp *res,
    const char               *needle)
{
    /* Response bodies here are small XML documents; NUL-terminate a copy. */
    static char tmp[65536];
    size_t      n = res->body_len < sizeof(tmp) - 1 ? res->body_len
                                                    : sizeof(tmp) - 1;

    memcpy(tmp, res->body, n);
    tmp[n] = '\0';
    return strstr(tmp, needle) != NULL;
} /* body_has */

int
main(
    int    argc,
    char **argv)
{
    struct s3_mbt_env   env;
    struct s3_mbt_resp *r;
    char                etag_a[160];
    int                 s1[]   = { 1 };
    int                 s12[]  = { 1, 2 };
    int                 s123[] = { 1, 2, 3 };
    int                 s23[]  = { 2, 3 };
    int                 s3[]   = { 3 };

    (void) argc;
    (void) argv;

    mbt_watchdog_arm(120);

    s3_mbt_env_open(&env);
    s3_mbt_env_fs_setup(&env, "fs0");

    /* ---- bucket lifecycle ---------------------------------------------- */

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_HEAD, "/bk0");
    CHECK(r->status == 404, "HEAD missing bucket: got %d want 404", r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_PUT, "/bk0");
    CHECK(r->status == 200, "CreateBucket: got %d want 200", r->status);
    CHECK(strcmp(r->location, "/bk0") == 0,
          "CreateBucket Location: got '%s' want '/bk0'", r->location);

    /* idempotent re-create (us-east-1 semantics) */
    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_PUT, "/bk0");
    CHECK(r->status == 200, "re-CreateBucket: got %d want 200", r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_HEAD, "/bk0");
    CHECK(r->status == 200, "HeadBucket: got %d want 200", r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/");
    CHECK(r->status == 200, "ListBuckets: got %d want 200", r->status);
    CHECK(body_has(r, "<Name>bk0</Name>"), "ListBuckets lacks bk0");

    /* ---- object core ---------------------------------------------------- */

    r = put_blocks(&env, "/bk0/a", s12, 2);
    CHECK(r->status == 200, "PutObject: got %d want 200", r->status);
    CHECK(r->etag[0] == '"', "PutObject ETag missing/unquoted: '%s'", r->etag);
    snprintf(etag_a, sizeof(etag_a), "%s", r->etag);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/a");
    CHECK(r->status == 200, "GetObject: got %d want 200", r->status);
    CHECK(body_is_blocks(r, s12, 2), "GetObject body mismatch");
    CHECK(strcmp(r->etag, etag_a) == 0,
          "GetObject ETag: got '%s' want '%s'", r->etag, etag_a);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_HEAD, "/bk0/a");
    CHECK(r->status == 200, "HeadObject: got %d want 200", r->status);
    CHECK(r->has_content_length && r->content_length == 2 * BS,
          "HeadObject Content-Length: got %" PRId64 " want %d",
          r->content_length, 2 * BS);
    CHECK(r->body_len == 0, "HeadObject returned a body");
    CHECK(strcmp(r->etag, etag_a) == 0, "HeadObject ETag mismatch");

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/b");
    CHECK(r->status == 404, "GetObject missing: got %d want 404", r->status);
    CHECK(body_has(r, "<Code>NoSuchKey</Code>"),
          "GetObject missing: body lacks NoSuchKey");

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/nosuch/a");
    CHECK(r->status == 404, "GetObject missing bucket: got %d want 404",
          r->status);
    CHECK(body_has(r, "<Code>NoSuchBucket</Code>"),
          "GetObject missing bucket: body lacks NoSuchBucket");

    /* overwrite changes content (and, with it, the reported ETag stays
     * self-consistent on every later read) */
    r = put_blocks(&env, "/bk0/a", s123, 3);
    CHECK(r->status == 200, "PutObject overwrite: got %d want 200", r->status);
    snprintf(etag_a, sizeof(etag_a), "%s", r->etag);

    /* ---- ranges ---------------------------------------------------------- */

    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path   = "/bk0/a",                  .range = "bytes=0-8191" };
        char              want_cr[128];

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 206, "ranged GET: got %d want 206", r->status);
        CHECK(body_is_blocks(r, s1, 1), "ranged GET body mismatch");
        snprintf(want_cr, sizeof(want_cr), "bytes 0-%d/%d", BS - 1, 3 * BS);
        CHECK(strcmp(r->content_range, want_cr) == 0,
              "ranged GET Content-Range: got '%s' want '%s'",
              r->content_range, want_cr);

        req.range = "bytes=8192-";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 206, "open-ended GET: got %d want 206", r->status);
        CHECK(body_is_blocks(r, s23, 2), "open-ended GET body mismatch");

        req.range = "bytes=-8192";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 206, "suffix GET: got %d want 206", r->status);
        CHECK(body_is_blocks(r, s3, 1), "suffix GET body mismatch");

        /* a closed range overrunning EOF is clamped */
        req.range = "bytes=16384-999999";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 206, "clamped GET: got %d want 206", r->status);
        CHECK(body_is_blocks(r, s3, 1), "clamped GET body mismatch");

        /* start at/past EOF: unsatisfiable */
        req.range = "bytes=24576-24577";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 416, "past-EOF GET: got %d want 416", r->status);
        snprintf(want_cr, sizeof(want_cr), "bytes */%d", 3 * BS);
        CHECK(strcmp(r->content_range, want_cr) == 0,
              "416 Content-Range: got '%s' want '%s'",
              r->content_range, want_cr);

        /* DEVIATION range-full-200: a Range resolving to the entire object
         * (bytes=0- here) is 206 Partial Content on AWS; chimera collapses
         * it to a plain 200. */
        req.range = "bytes=0-";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 200,
              "deviation range-full-200 no longer reproduces: got %d "
              "(fixed? retire it in s3_mbt_replay.c)", r->status);
        CHECK(body_is_blocks(r, s123, 3), "full-range GET body mismatch");
    }

    /* ---- zero-length object ---------------------------------------------- */

    /* the GET of an empty object must still run the send path to completion
     * and release its handle -- a leak here pins the filesystem (see the
     * RECEIVE_COMPLETE handler in s3.c) */
    r = put_blocks(&env, "/bk0/empty", s1, 0);
    CHECK(r->status == 200, "put empty: got %d want 200", r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/empty");
    CHECK(r->status == 200, "get empty: got %d want 200", r->status);
    CHECK(r->body_len == 0, "get empty returned %zu bytes", r->body_len);
    CHECK(r->has_content_length && r->content_length == 0,
          "get empty Content-Length: got %" PRId64, r->content_length);

    simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/empty");

    /* ---- listing --------------------------------------------------------- */

    r = put_blocks(&env, "/bk0/b", s1, 1);
    CHECK(r->status == 200, "put b: got %d", r->status);
    r = put_blocks(&env, "/bk0/d/a", s1, 1);
    CHECK(r->status == 200, "put d/a: got %d", r->status);

    {
        /* deliberately UNENCODED delimiter: a legal '/' in a query value
         * must not corrupt the bucket/key split (s3.c guards the split
         * against scanning past the '?') */
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path   = "/bk0",
                                  .query  = "delimiter=/&list-type=2" };

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "ListObjectsV2: got %d want 200", r->status);
        CHECK(body_has(r, "<Key>a</Key>"), "list lacks key a");
        CHECK(body_has(r, "<Key>b</Key>"), "list lacks key b");
        CHECK(!body_has(r, "<Key>d/a</Key>"), "delimited list leaked d/a");
        CHECK(body_has(r, "<Prefix>d/</Prefix>"), "list lacks CommonPrefix d/");
        CHECK(body_has(r, "<KeyCount>3</KeyCount>"),
              "list KeyCount != 3");
        CHECK(body_has(r, "<IsTruncated>false</IsTruncated>"),
              "list should not be truncated");

        /* percent-encoded, the way the AWS SDKs send it */
        req.query = "list-type=2&prefix=d%2F";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "prefixed list: got %d want 200", r->status);
        CHECK(body_has(r, "<Key>d/a</Key>"), "prefixed list lacks d/a");
        CHECK(!body_has(r, "<Key>a</Key>") || body_has(r, "<Key>d/a</Key>"),
              "prefixed list shape");

        /* start-after inside a rolled-up group: the group's CommonPrefix
         * string ("d/") sorts at or before start-after ("d/a"), but member
         * keys past start-after keep it in the listing (AWS semantics; the
         * naive skip-entries-at-or-below-start drops it). */
        r = put_blocks(&env, "/bk0/d/b", s1, 1);
        CHECK(r->status == 200, "put d/b: got %d", r->status);

        req.query = "delimiter=%2F&list-type=2&start-after=d%2Fa";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "start-after list: got %d want 200",
              r->status);
        CHECK(body_has(r, "<Prefix>d/</Prefix>"),
              "start-after-in-group list dropped CommonPrefix d/");
        CHECK(body_has(r, "<KeyCount>1</KeyCount>"),
              "start-after-in-group list KeyCount != 1");

        /* ...and once no member remains past start-after, the group is gone
         * -- even though the backing directory still exists. */
        req.query = "delimiter=%2F&list-type=2&start-after=d%2Fb";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "start-after-past list: got %d want 200",
              r->status);
        CHECK(!body_has(r, "<Prefix>d/</Prefix>"),
              "consumed group d/ still listed after start-after=d/b");
        CHECK(body_has(r, "<KeyCount>0</KeyCount>"),
              "start-after-past list KeyCount != 0");
    }

    /* ---- copy ------------------------------------------------------------ */

    {
        struct s3_mbt_req req = { .method      = EVPL_HTTP_REQUEST_TYPE_PUT,
                                  .path        = "/bk0/c",
                                  .copy_source = "/bk0/a" };

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "CopyObject: got %d want 200", r->status);
        CHECK(body_has(r, "<CopyObjectResult"), "copy body lacks result");
        CHECK(body_has(r, "<ETag>"), "copy body lacks ETag");

        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/c");
        CHECK(r->status == 200 && body_is_blocks(r, s123, 3),
              "copied object mismatch");

        /* missing source key */
        req.path        = "/bk0/x";
        req.copy_source = "/bk0/nosuchkey";
        r               = s3_mbt_call(&env, &req);
        CHECK(r->status == 404, "copy missing src: got %d want 404", r->status);
        CHECK(body_has(r, "<Code>NoSuchKey</Code>"),
              "copy missing src: body lacks NoSuchKey");

        /* DEVIATION copy-self-200: copying an object onto itself with no
         * metadata directive is 400 InvalidRequest on AWS; chimera performs
         * the copy and returns 200. */
        req.path        = "/bk0/a";
        req.copy_source = "/bk0/a";
        r               = s3_mbt_call(&env, &req);
        CHECK(r->status == 200,
              "deviation copy-self-200 no longer reproduces: got %d "
              "(fixed? retire it in s3_mbt_replay.c)", r->status);
    }

    /* ---- delete ---------------------------------------------------------- */

    /* DEVIATION delete-object-200: AWS DeleteObject returns 204 No Content;
     * chimera returns 200 with an empty body. */
    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/c");
    CHECK(r->status == 200,
          "deviation delete-object-200 no longer reproduces: got %d "
          "(fixed? retire it in s3_mbt_replay.c)", r->status);
    CHECK(r->body_len == 0, "DeleteObject returned a body");

    /* DEVIATION delete-object-missing-404: AWS DeleteObject is idempotent
     * (204 for a missing key); chimera returns 404 NoSuchKey. */
    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/c");
    CHECK(r->status == 404,
          "deviation delete-object-missing-404 no longer reproduces: got %d "
          "(fixed? retire it in s3_mbt_replay.c)", r->status);

    /* DEVIATION delete-bucket-nonempty-500: AWS answers DELETE on a
     * non-empty bucket with 409 BucketNotEmpty; chimera maps its internal
     * BUCKET_NOT_EMPTY through the default 500 InternalError. */
    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0");
    CHECK(r->status == 500,
          "deviation delete-bucket-nonempty-500 no longer reproduces: got %d "
          "(fixed? retire it in s3_mbt_replay.c)", r->status);
    CHECK(body_has(r, "<Code>InternalError</Code>"),
          "non-empty DeleteBucket: body lacks InternalError");

    /* drain the bucket, then it deletes cleanly */
    simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/a");
    simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/b");
    simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/d/a");
    simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/d/b");

    /* the key-path directory "d" still exists on the filesystem, but with no
     * objects under it, it must NOT surface as a phantom CommonPrefix -- AWS
     * defines CommonPrefixes over keys, not directories */
    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path   = "/bk0",
                                  .query  = "delimiter=%2F&list-type=2" };

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "post-drain list: got %d want 200", r->status);
        CHECK(!body_has(r, "<Prefix>d/</Prefix>"),
              "empty key-path dir d/ listed as phantom CommonPrefix");
        CHECK(body_has(r, "<KeyCount>0</KeyCount>"),
              "post-drain list KeyCount != 0");
    }

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0");
    CHECK(r->status == 204, "DeleteBucket: got %d want 204", r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_HEAD, "/bk0");
    CHECK(r->status == 404, "HeadBucket after delete: got %d want 404",
          r->status);

    r = simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0");
    CHECK(r->status == 404, "DeleteBucket missing: got %d want 404", r->status);
    CHECK(body_has(r, "<Code>NoSuchBucket</Code>"),
          "DeleteBucket missing: body lacks NoSuchBucket");

    /* ---- authentication -------------------------------------------------- */

    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path   = "/bk0/a" };

        /* recreate state: bucket + object for the auth round-trips */
        simple(&env, EVPL_HTTP_REQUEST_TYPE_PUT, "/bk0");
        put_blocks(&env, "/bk0/a", s12, 2);

        req.auth = S3_MBT_AUTH_NONE;
        r        = s3_mbt_call(&env, &req);
        CHECK(r->status == 400, "no-auth GET: got %d want 400", r->status);
        CHECK(body_has(r, "<Code>MissingSecurityHeader</Code>"),
              "no-auth GET: body lacks MissingSecurityHeader");

        req.auth = S3_MBT_AUTH_V4_BADSIG;
        r        = s3_mbt_call(&env, &req);
        CHECK(r->status == 403, "bad-sig GET: got %d want 403", r->status);
        CHECK(body_has(r, "<Code>SignatureDoesNotMatch</Code>"),
              "bad-sig GET: body lacks SignatureDoesNotMatch");

        req.auth = S3_MBT_AUTH_V4_BADKEY;
        r        = s3_mbt_call(&env, &req);
        CHECK(r->status == 403, "bad-key GET: got %d want 403", r->status);
        CHECK(body_has(r, "<Code>InvalidAccessKeyId</Code>"),
              "bad-key GET: body lacks InvalidAccessKeyId");

        /* SigV2 round-trip: GET, ranged GET, PUT, and a bad V2 signature */
        req.auth = S3_MBT_AUTH_V2;
        r        = s3_mbt_call(&env, &req);
        CHECK(r->status == 200 && body_is_blocks(r, s12, 2),
              "V2 GET failed (%d)", r->status);

        req.range = "bytes=8192-";
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 206, "V2 ranged GET: got %d want 206", r->status);
        req.range = NULL;

        req.auth = S3_MBT_AUTH_V2_BADSIG;
        r        = s3_mbt_call(&env, &req);
        CHECK(r->status == 403, "V2 bad-sig GET: got %d want 403", r->status);
    }

    {
        struct s3_mbt_req req = { .method       = EVPL_HTTP_REQUEST_TYPE_PUT,
                                  .path         = "/bk0/v2put",
                                  .auth         = S3_MBT_AUTH_V2,
                                  .content_type = "text/plain" };
        static uint8_t    pb[BS];

        s3_mbt_expand_blocks(s1, 1, BS, pb);
        req.body     = pb;
        req.body_len = BS;
        r            = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "V2 PUT: got %d want 200", r->status);

        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/v2put");
        CHECK(r->status == 200 && body_is_blocks(r, s1, 1),
              "V2-put object GET failed");
        CHECK(strcmp(r->content_type, "text/plain") == 0,
              "stored Content-Type not echoed: '%s'", r->content_type);
        simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/v2put");
    }

    /* ---- virtual-host addressing ----------------------------------------- */

    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path   = "/a",
                                  .host   = "bk0.chimera" };

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200 && body_is_blocks(r, s12, 2),
              "virtual-host GET failed (%d)", r->status);
    }

    /* ---- aws-chunked upload ---------------------------------------------- */

    {
        static uint8_t    raw[2 * BS];
        static uint8_t    framed[2 * BS + 512];
        struct s3_mbt_req req = { .method      = EVPL_HTTP_REQUEST_TYPE_PUT,
                                  .path        = "/bk0/chunked",
                                  .content_sha =
                                      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" };

        s3_mbt_expand_blocks(s23, 2, BS, raw);
        req.body     = framed;
        req.body_len = s3_mbt_aws_chunkify(raw, sizeof(raw), 5000, framed);

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "aws-chunked PUT: got %d want 200", r->status);

        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/chunked");
        CHECK(r->status == 200 && body_is_blocks(r, s23, 2),
              "aws-chunked object GET failed (%d, %zu bytes)", r->status,
              r->body_len);
        simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/chunked");
    }

    /* ---- tagging ---------------------------------------------------------- */

    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_PUT,
                                  .path   = "/bk0/a",                  .query = "tagging=" };
        static const char tags[] =
            "<Tagging><TagSet><Tag><Key>tk1</Key><Value>tv1</Value></Tag>"
            "</TagSet></Tagging>";

        req.body     = (const uint8_t *) tags;
        req.body_len = sizeof(tags) - 1;
        r            = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "PutObjectTagging: got %d want 200",
              r->status);

        req.method   = EVPL_HTTP_REQUEST_TYPE_GET;
        req.body     = NULL;
        req.body_len = 0;
        r            = s3_mbt_call(&env, &req);
        CHECK(r->status == 200 && body_has(r, "<Key>tk1</Key>"),
              "GetObjectTagging lacks tk1 (%d)", r->status);

        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_HEAD, "/bk0/a");
        CHECK(strcmp(r->tag_count, "1") == 0,
              "x-amz-tagging-count '%s', want 1", r->tag_count);

        req.method = EVPL_HTTP_REQUEST_TYPE_DELETE;
        r          = s3_mbt_call(&env, &req);
        CHECK(r->status == 204, "DeleteObjectTagging: got %d want 204",
              r->status);

        /* bucket-level: no tags stored -> 404 NoSuchTagSet */
        req.method = EVPL_HTTP_REQUEST_TYPE_GET;
        req.path   = "/bk0";
        r          = s3_mbt_call(&env, &req);
        CHECK(r->status == 404 && body_has(r, "<Code>NoSuchTagSet</Code>"),
              "empty GetBucketTagging: got %d, want 404 NoSuchTagSet",
              r->status);
    }

    /* ---- multipart smoke -------------------------------------------------- */

    {
        struct s3_mbt_req req = { .method = EVPL_HTTP_REQUEST_TYPE_POST,
                                  .path   = "/bk0/mp",                  .query = "uploads=" };
        char              uploadid[64];
        char              etag1[160], etag2[160];
        char              q[128];
        char              cbody[1024];
        static uint8_t    pb[BS];

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "CreateMultipartUpload: got %d", r->status);
        CHECK(body_has(r, "<UploadId>"), "initiate lacks UploadId");
        {
            const char *b = strstr((const char *) r->body, "<UploadId>");

            memcpy(uploadid, b + 10, 32);
            uploadid[32] = '\0';
        }

        s3_mbt_expand_blocks(s3, 1, BS, pb);
        req.method   = EVPL_HTTP_REQUEST_TYPE_PUT;
        req.body     = pb;
        req.body_len = BS;

        snprintf(q, sizeof(q), "partNumber=1&uploadId=%s", uploadid);
        req.query = q;
        r         = s3_mbt_call(&env, &req);
        CHECK(r->status == 200 && r->etag[0] == '"',
              "UploadPart 1: got %d etag '%s'", r->status, r->etag);
        snprintf(etag1, sizeof(etag1), "%s", r->etag);

        snprintf(q, sizeof(q), "partNumber=2&uploadId=%s", uploadid);
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "UploadPart 2: got %d", r->status);
        snprintf(etag2, sizeof(etag2), "%s", r->etag);

        /* a NON-final part under the 5 MiB minimum fails the Complete */
        snprintf(cbody, sizeof(cbody),
                 "<CompleteMultipartUpload>"
                 "<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>"
                 "<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>"
                 "</CompleteMultipartUpload>", etag1, etag2);
        snprintf(q, sizeof(q), "uploadId=%s", uploadid);
        req.method   = EVPL_HTTP_REQUEST_TYPE_POST;
        req.query    = q;
        req.body     = (const uint8_t *) cbody;
        req.body_len = strlen(cbody);
        r            = s3_mbt_call(&env, &req);
        CHECK(r->status == 400 && body_has(r, "<Code>EntityTooSmall</Code>"),
              "undersized non-final part: got %d, want 400 EntityTooSmall",
              r->status);

        /* an ascending SUBSET manifest is legal: complete with part 1 only
         * (any size is fine for the final part), orphaning part 2 */
        snprintf(cbody, sizeof(cbody),
                 "<CompleteMultipartUpload>"
                 "<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>"
                 "</CompleteMultipartUpload>", etag1);
        req.body_len = strlen(cbody);
        r            = s3_mbt_call(&env, &req);
        CHECK(r->status == 200 && body_has(r, "-1\"</ETag>"),
              "CompleteMultipartUpload: got %d (want 200, ETag ...-1)",
              r->status);

        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/mp");
        CHECK(r->status == 200 && body_is_blocks(r, s3, 1),
              "assembled object GET failed (%d)", r->status);
        simple(&env, EVPL_HTTP_REQUEST_TYPE_DELETE, "/bk0/mp");
    }

    /* ---- synthetic clock: credential TTL --------------------------------- */

    /* Credential expiry is driven by ticking the cred cache's synthetic
     * clock (chimera_server_advance_s3_cred_clock advances it and sweeps
     * synchronously) -- never by waiting out the sweeper thread's wall
     * cadence.  The server's default TTL is 3600s. */
    {
        struct s3_mbt_req req = { .method     = EVPL_HTTP_REQUEST_TYPE_GET,
                                  .path       = "/bk0/a",
                                  .access_key = "tempaccess",
                                  .secret_key = "tempsecret" };

        CHECK(chimera_server_add_s3_cred(env.server, "tempaccess",
                                         "tempsecret", 0) == 0,
              "add unpinned cred failed");

        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "unpinned-cred GET: got %d want 200",
              r->status);

        /* re-adding the same key replaces the entry (fresh TTL stamp) */
        CHECK(chimera_server_add_s3_cred(env.server, "tempaccess",
                                         "tempsecret", 0) == 0,
              "re-add unpinned cred failed");
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "replaced-cred GET: got %d want 200",
              r->status);

        /* an advance short of the TTL leaves the cred alive... */
        chimera_server_advance_s3_cred_clock(env.server, 3599);
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "pre-TTL GET: got %d want 200", r->status);

        /* ...and ticking past it sweeps the cred: same request now fails
         * with an unknown access key */
        chimera_server_advance_s3_cred_clock(env.server, 2);
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 403 &&
              body_has(r, "<Code>InvalidAccessKeyId</Code>"),
              "post-TTL GET: got %d, want 403 InvalidAccessKeyId", r->status);

        /* the pinned harness credential is immune to the clock */
        r = simple(&env, EVPL_HTTP_REQUEST_TYPE_GET, "/bk0/a");
        CHECK(r->status == 200, "pinned cred after advance: got %d want 200",
              r->status);

        /* a pinned extra credential survives any advance, and explicit
         * removal (not expiry) is what retires it */
        CHECK(chimera_server_add_s3_cred(env.server, "tempaccess",
                                         "tempsecret", 1) == 0,
              "add pinned cred failed");
        chimera_server_advance_s3_cred_clock(env.server, 100000);
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 200, "pinned temp cred GET: got %d want 200",
              r->status);

        CHECK(chimera_server_remove_s3_cred(env.server, "tempaccess") == 0,
              "remove cred failed");
        r = s3_mbt_call(&env, &req);
        CHECK(r->status == 403, "removed-cred GET: got %d want 403",
              r->status);
        CHECK(chimera_server_remove_s3_cred(env.server, "tempaccess") == -1,
              "double remove should report absence");
    }

    /* ---- teardown -------------------------------------------------------- */

    s3_mbt_env_fs_teardown(&env, "fs0");
    s3_mbt_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d probe failure(s)\n", failures);
        return 1;
    }

    printf("s3_mbt_probe: all checks passed\n");
    return 0;
} /* main */
