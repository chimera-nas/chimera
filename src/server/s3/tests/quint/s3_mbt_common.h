// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Shared environment for the S3 model-based tests: a chimera server (S3
 * enabled, memfs backend) and a libevpl HTTP/1.1 client in ONE process,
 * connected over libevpl's inproc transport.  Nothing binds a TCP port, needs
 * a network namespace, or spawns a daemon -- every trace runs fully parallel
 * on any host.
 *
 * The client half issues real S3 requests: each call builds the HTTP request
 * (method, path, query, headers, body), signs it with AWS Signature V4 the
 * way any AWS SDK would, dispatches it on the client's own evpl loop and
 * spins evpl_continue() until the response completes.  Responses are captured
 * whole (status, the headers S3 semantics live in, and the body) into one
 * result struct reused per call, so the replayer's oracle has a uniform view.
 *
 * SigV4 notes: the server verifies the signature over the request exactly as
 * sent, copies the client's x-amz-content-sha256 into the canonical request
 * without hashing the payload, and never checks the date for freshness -- so
 * the harness signs with UNSIGNED-PAYLOAD and a fixed x-amz-date, keeping
 * every request byte-deterministic.  The Host header is set explicitly (to a
 * dot-free name, selecting path-style bucket addressing) because SigV4 signs
 * whatever Host goes on the wire. */

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "server/s3/s3.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"

#define S3_MBT_PORT        5000
#define S3_MBT_BODY_MAX    (1024 * 1024)

#define S3_MBT_ACCESS_KEY  "quintaccess"
#define S3_MBT_SECRET_KEY  "quintsecret"

/* Fixed signing date: the server never checks it for freshness, and a fixed
 * date keeps requests byte-identical run to run. */
#define S3_MBT_AMZ_DATE    "20260101T000000Z"
#define S3_MBT_DATESTAMP   "20260101"
#define S3_MBT_SCOPE       S3_MBT_DATESTAMP "/us-east-1/s3/aws4_request"
#define S3_MBT_SIGNED_HDRS "host;x-amz-content-sha256;x-amz-date"
#define S3_MBT_PAYLOAD     "UNSIGNED-PAYLOAD"
#define S3_MBT_HOST        "localhost"

/* One response, captured whole.  Headers are copied out in the notify
 * callback -- the request object is freed the moment the terminal callback
 * returns, so nothing may be read from it afterwards. */
struct s3_mbt_resp {
    int      done;
    int      status;
    int      has_content_length;
    int64_t  content_length;
    char     etag[160];
    char     content_range[160];
    char     location[256];
    char     content_type[128];
    size_t   body_len;
    uint8_t *body;              /* borrows env->body_buf */
};

struct s3_mbt_env {
    struct chimera_server     *server;
    struct prometheus_metrics *metrics;
    struct evpl               *evpl;
    struct evpl_http_agent    *agent;
    struct evpl_http_conn     *conn;
    struct evpl_endpoint      *ep;
    char                       session_dir[256];
    uint8_t                   *body_buf;
    struct s3_mbt_resp         res;
};

/* One request, declaratively.  query/range/copy_source may be NULL. */
struct s3_mbt_req {
    enum evpl_http_request_type method;
    const char    *path;                     /* "/bucket/key", starts with '/' */
    const char    *query;                    /* pre-sorted, no leading '?' */
    const char    *range;                    /* "bytes=0-8191" */
    const char    *copy_source;              /* "/srcbucket/srckey" */
    const uint8_t *body;
    size_t         body_len;
};

/* ---- crypto helpers (OpenSSL, same library the server verifies with) ---- */

static inline void
s3_mbt_sha256_hex(
    const void *data,
    size_t      len,
    char       *hex65)
{
    unsigned char hash[32];
    unsigned int  hl  = sizeof(hash);
    EVP_MD_CTX   *ctx = EVP_MD_CTX_new();

    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, hash, &hl);
    EVP_MD_CTX_free(ctx);

    for (int i = 0; i < 32; i++) {
        sprintf(hex65 + i * 2, "%02x", hash[i]);
    }
} /* s3_mbt_sha256_hex */

static inline void
s3_mbt_hmac256(
    const void   *key,
    size_t        keylen,
    const void   *data,
    size_t        datalen,
    unsigned char out[32])
{
    unsigned int hl = 32;

    HMAC(EVP_sha256(), key, (int) keylen, data, datalen, out, &hl);
} /* s3_mbt_hmac256 */

/* AWS SigV4 Authorization header for one request as the harness sends it:
 * fixed signed-header set (host, x-amz-content-sha256, x-amz-date), unsigned
 * payload.  Any extra headers (Range, x-amz-copy-source) are deliberately
 * left out of SignedHeaders, which SigV4 permits. */
static inline void
s3_mbt_sign(
    const struct s3_mbt_req *req,
    char                    *authorization,
    size_t                   authorization_len)
{
    char          canonical[4096];
    char          to_sign[512];
    char          cr_hash[65];
    char          sig_hex[65];
    unsigned char k[32], sig[32];
    const char   *method;

    switch (req->method) {
        case EVPL_HTTP_REQUEST_TYPE_GET:    method = "GET";    break;
        case EVPL_HTTP_REQUEST_TYPE_HEAD:   method = "HEAD";   break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:    method = "PUT";    break;
        case EVPL_HTTP_REQUEST_TYPE_POST:   method = "POST";   break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE: method = "DELETE"; break;
        default:
            fprintf(stderr, "s3_mbt_sign: unsupported method %d\n", req->method);
            exit(3);
    } /* switch */

    snprintf(canonical, sizeof(canonical),
             "%s\n"                          /* method */
             "%s\n"                          /* canonical URI: the raw path */
             "%s\n"                          /* canonical query, as sent */
             "host:%s\n"
             "x-amz-content-sha256:%s\n"
             "x-amz-date:%s\n"
             "\n"
             "%s\n"
             "%s",
             method, req->path, req->query ? req->query : "",
             S3_MBT_HOST, S3_MBT_PAYLOAD, S3_MBT_AMZ_DATE,
             S3_MBT_SIGNED_HDRS, S3_MBT_PAYLOAD);

    s3_mbt_sha256_hex(canonical, strlen(canonical), cr_hash);

    snprintf(to_sign, sizeof(to_sign),
             "AWS4-HMAC-SHA256\n%s\n%s\n%s",
             S3_MBT_AMZ_DATE, S3_MBT_SCOPE, cr_hash);

    s3_mbt_hmac256("AWS4" S3_MBT_SECRET_KEY, strlen("AWS4" S3_MBT_SECRET_KEY),
                   S3_MBT_DATESTAMP, strlen(S3_MBT_DATESTAMP), k);
    s3_mbt_hmac256(k, 32, "us-east-1", strlen("us-east-1"), k);
    s3_mbt_hmac256(k, 32, "s3", strlen("s3"), k);
    s3_mbt_hmac256(k, 32, "aws4_request", strlen("aws4_request"), k);
    s3_mbt_hmac256(k, 32, to_sign, strlen(to_sign), sig);

    for (int i = 0; i < 32; i++) {
        sprintf(sig_hex + i * 2, "%02x", sig[i]);
    }

    snprintf(authorization, authorization_len,
             "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
             S3_MBT_ACCESS_KEY, S3_MBT_SCOPE, S3_MBT_SIGNED_HDRS, sig_hex);
} /* s3_mbt_sign */

/* ---- response capture --------------------------------------------------- */

static inline void
s3_mbt_copy_header(
    struct evpl_http_request *request,
    const char               *name,
    char                     *dst,
    size_t                    dstlen)
{
    const char *v = evpl_http_response_header(request, name);

    if (v) {
        snprintf(dst, dstlen, "%s", v);
    } else {
        dst[0] = '\0';
    }
} /* s3_mbt_copy_header */

static inline void
s3_mbt_drain(
    struct evpl              *evpl,
    struct evpl_http_request *request,
    struct s3_mbt_resp       *res)
{
    struct evpl_iovec iov[64];
    uint64_t          avail;
    int               niov, i;

    avail = evpl_http_request_get_data_avail(request);

    while (avail > 0) {
        niov = evpl_http_request_get_datav(evpl, request, iov, (int) avail);

        for (i = 0; i < niov; i++) {
            if (res->body_len + iov[i].length > S3_MBT_BODY_MAX) {
                fprintf(stderr, "s3_mbt: response body exceeds %d bytes\n",
                        S3_MBT_BODY_MAX);
                exit(3);
            }
            memcpy(res->body + res->body_len, iov[i].data, iov[i].length);
            res->body_len += iov[i].length;
            evpl_iovec_release(evpl, &iov[i]);
        }

        avail = evpl_http_request_get_data_avail(request);
    }
} /* s3_mbt_drain */

static inline void
s3_mbt_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  notify_type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *notify_data,
    void                       *private_data)
{
    struct s3_mbt_resp *res = notify_data;
    const char         *cl;

    (void) agent;
    (void) request_type;
    (void) uri;
    (void) private_data;

    switch (notify_type) {
        case EVPL_HTTP_NOTIFY_RESPONSE_HEADERS:
            res->status = evpl_http_request_status(request);
            s3_mbt_copy_header(request, "ETag", res->etag, sizeof(res->etag));
            s3_mbt_copy_header(request, "Content-Range", res->content_range,
                               sizeof(res->content_range));
            s3_mbt_copy_header(request, "Location", res->location,
                               sizeof(res->location));
            s3_mbt_copy_header(request, "Content-Type", res->content_type,
                               sizeof(res->content_type));
            cl = evpl_http_response_header(request, "Content-Length");
            if (cl) {
                res->has_content_length = 1;
                res->content_length     = strtoll(cl, NULL, 10);
            }
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_DATA:
            s3_mbt_drain(evpl, request, res);
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE:
            s3_mbt_drain(evpl, request, res);
            res->done = 1;
            break;
        case EVPL_HTTP_NOTIFY_WANT_DATA:
        case EVPL_HTTP_NOTIFY_RESPONSE_COMPLETE:
            break;
        case EVPL_HTTP_NOTIFY_FAILED:
            /* A transport failure is never a modeled outcome: the trace is
             * unusable past this point, so bail loudly. */
            fprintf(stderr,
                    "s3_mbt: HTTP request failed at transport level (%d)\n",
                    evpl_http_request_status(request));
            exit(3);
    } /* switch */
} /* s3_mbt_notify */

/* Issue one signed request and wait for the full response. */
static inline struct s3_mbt_resp *
s3_mbt_call(
    struct s3_mbt_env       *env,
    const struct s3_mbt_req *req)
{
    struct evpl_http_request *request;
    struct evpl_iovec         iov;
    char                      url[4096];
    char                      authorization[512];

    memset(&env->res, 0, sizeof(env->res));
    env->res.body           = env->body_buf;
    env->res.content_length = -1;

    if (req->query && req->query[0]) {
        snprintf(url, sizeof(url), "%s?%s", req->path, req->query);
    } else {
        snprintf(url, sizeof(url), "%s", req->path);
    }

    request = evpl_http_request_create(env->conn, req->method, url);

    if (!request) {
        fprintf(stderr, "s3_mbt: evpl_http_request_create(%s) failed\n", url);
        exit(3);
    }

    s3_mbt_sign(req, authorization, sizeof(authorization));

    evpl_http_request_add_header(request, "Host", S3_MBT_HOST);
    evpl_http_request_add_header(request, "x-amz-date", S3_MBT_AMZ_DATE);
    evpl_http_request_add_header(request, "x-amz-content-sha256", S3_MBT_PAYLOAD);
    evpl_http_request_add_header(request, "Authorization", authorization);

    if (req->range) {
        evpl_http_request_add_header(request, "Range", req->range);
    }
    if (req->copy_source) {
        evpl_http_request_add_header(request, "x-amz-copy-source",
                                     req->copy_source);
    }

    /* Stage the whole request body up front; no WANT_DATA handling needed. */
    if (req->method == EVPL_HTTP_REQUEST_TYPE_PUT ||
        req->method == EVPL_HTTP_REQUEST_TYPE_POST) {
        evpl_http_client_set_request_length(request, req->body_len);
        if (req->body_len) {
            evpl_iovec_alloc(env->evpl, req->body_len, 0, 1, 0, &iov);
            memcpy(evpl_iovec_data(&iov), req->body, req->body_len);
            evpl_iovec_set_length(&iov, req->body_len);
            evpl_http_request_add_datav(request, &iov, 1);
        }
    }

    evpl_http_request_dispatch(request, s3_mbt_notify, &env->res);

    while (!env->res.done) {
        evpl_continue(env->evpl);
    }

    return &env->res;
} /* s3_mbt_call */

/* ---- server + client lifecycle ------------------------------------------ */

static inline void
s3_mbt_env_open(struct s3_mbt_env *env)
{
    struct chimera_server_config *config;
    struct evpl_thread_config    *tcfg;

    memset(env, 0, sizeof(*env));

    snprintf(env->session_dir, sizeof(env->session_dir), "/tmp/s3_mbt_XXXXXX");
    if (!mkdtemp(env->session_dir)) {
        fprintf(stderr, "mkdtemp(%s) failed\n", env->session_dir);
        exit(1);
    }

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);

    /* The VFS releases closed handles on an async sweep thread, so a
     * filesystem stays busy for a window after the last close.  A fast sweep
     * keeps the per-trace rmfs recycle from stalling; set before the server's
     * threads start below. */
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 0);

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, env->session_dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_s3_enabled(config, 1);
    chimera_server_config_set_s3_port(config, S3_MBT_PORT);

    env->server = chimera_server_init(config, env->metrics);

    chimera_server_start(env->server);

    /* Runtime CreateBucket materializes bucket dirs under this VFS path (the
     * per-trace mount recreates /share, so the root stays valid across the
     * whole batch), and every request must be signed with a known cred. */
    chimera_server_set_s3_bucket_root(env->server, "/share");
    chimera_server_add_s3_cred(env->server, S3_MBT_ACCESS_KEY,
                               S3_MBT_SECRET_KEY, 1);

    /* Client half: its own evpl loop; the response callbacks run inside
     * evpl_continue() on this (the only) test thread.  The 1 ms wait bound
     * matters: the default (-1) parks evpl_continue in the poller, and 0
     * (pure spin) starves the server's threads -- measured 3x slower on the
     * SMB harness this pattern comes from. */
    tcfg = evpl_thread_config_init();
    evpl_thread_config_set_wait_ms(tcfg, 1);
    env->evpl  = evpl_create(tcfg);
    env->agent = evpl_http_init(env->evpl);

    env->ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                 "127.0.0.1", S3_MBT_PORT);
    env->conn = evpl_http_client_connect(env->agent, EVPL_STREAM_INPROC,
                                         env->ep, EVPL_HTTP_VERSION_HTTP1,
                                         NULL);

    env->body_buf = malloc(S3_MBT_BODY_MAX);
} /* s3_mbt_env_open */

/* Per-trace filesystem: a fresh named memfs mounted at /share (unique fsname
 * => distinct fsid, so no stale cache entry can be hit across traces).
 * Buckets are NOT pre-created -- CreateBucket is part of the modeled surface
 * and traces create what they use. */
static inline void
s3_mbt_env_fs_setup(
    struct s3_mbt_env *env,
    const char        *fsname)
{
    if (chimera_server_mkfs(env->server, "memfs", fsname, NULL) != 0) {
        fprintf(stderr, "failed to create memfs filesystem %s\n", fsname);
        exit(1);
    }
    chimera_server_mount(env->server, "share", "memfs", fsname, NULL);
} /* s3_mbt_env_fs_setup */

static int
s3_mbt_collect_bucket(
    const struct s3_bucket *bucket,
    void                   *data)
{
    char (*names)[64] = data;
    int i;

    for (i = 0; i < 16; i++) {
        if (names[i][0] == '\0') {
            snprintf(names[i], sizeof(names[i]), "%s",
                     chimera_s3_bucket_get_name(bucket));
            break;
        }
    }
    return 0;
} /* s3_mbt_collect_bucket */

/* Tear the per-trace filesystem back down: drop the bucket-map entries the
 * trace created (they point into /share), unmount, then rmfs -- retried,
 * because the VFS closes handles on an async sweep. */
static inline void
s3_mbt_env_fs_teardown(
    struct s3_mbt_env *env,
    const char        *fsname)
{
    char names[16][64];
    int  tries = 0;
    int  i;

    memset(names, 0, sizeof(names));
    chimera_server_iterate_buckets(env->server, s3_mbt_collect_bucket, names);
    for (i = 0; i < 16 && names[i][0]; i++) {
        chimera_server_remove_bucket(env->server, names[i]);
    }

    chimera_server_unmount(env->server, "share");

    while (chimera_server_rmfs(env->server, "memfs", fsname) != 0) {
        if (++tries >= 5000) {
            fprintf(stderr, "warning: rmfs %s still failed after %d retries\n",
                    fsname, tries);
            break;
        }
        usleep(1000);
    }
} /* s3_mbt_env_fs_teardown */

static inline void
s3_mbt_env_stop(struct s3_mbt_env *env)
{
    char cmd[300];

    evpl_http_client_close(env->agent, env->conn);
    evpl_http_destroy(env->agent);
    evpl_destroy(env->evpl);

    chimera_server_destroy(env->server);
    prometheus_metrics_destroy(env->metrics);

    free(env->body_buf);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", env->session_dir);
    }
} /* s3_mbt_env_stop */

/* ---- content expansion --------------------------------------------------- */

/* Block symbol s expands to block_size repetitions of byte 0x40+s, matching
 * the model's contract (symbols are 1..3, so bytes 'A'..'C'). */
static inline void
s3_mbt_expand_blocks(
    const int *syms,
    int        nsyms,
    int        block_size,
    uint8_t   *out)
{
    int i;

    for (i = 0; i < nsyms; i++) {
        memset(out + (size_t) i * block_size, 0x40 + syms[i], block_size);
    }
} /* s3_mbt_expand_blocks */
