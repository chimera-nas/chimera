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
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "server/s3/s3.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"
#include "common/mbt_artifacts.h"

#define S3_MBT_PORT        5000
/* Multipart traces replay with 5 MiB blocks; whole-object GETs of an
 * assembled upload run tens of MiB. */
#define S3_MBT_BODY_MAX    (64 * 1024 * 1024)

#define S3_MBT_ACCESS_KEY  "quintaccess"
#define S3_MBT_USER        "quintuser"
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
    char     meta[64];          /* x-amz-meta-m */
    char     tag_count[16];     /* x-amz-tagging-count */
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
    const char                *module;      /* VFS backend under test */
    char                       pt_root[300]; /* passthrough backing root */
    int                        sigv2; /* sign with SigV2 by default */
    uint8_t                   *body_buf;
    struct s3_mbt_resp         res;
};

/* How to authenticate one request.  DEFAULT follows the environment (V4, or
 * V2 when env->sigv2 is set); the explicit modes let the probe exercise the
 * V2 verifier and each authentication failure path. */
enum s3_mbt_auth {
    S3_MBT_AUTH_DEFAULT = 0,
    S3_MBT_AUTH_V4,
    S3_MBT_AUTH_V2,
    S3_MBT_AUTH_NONE,      /* no Authorization header -> 400 MissingSecurityHeader */
    S3_MBT_AUTH_V4_BADSIG, /* corrupted V4 signature  -> 403 SignatureDoesNotMatch */
    S3_MBT_AUTH_V4_BADKEY, /* unknown access key      -> 403 InvalidAccessKeyId */
    S3_MBT_AUTH_V2_BADSIG, /* corrupted V2 signature  -> 403 SignatureDoesNotMatch */
};

/* One request, declaratively.  Pointer fields may be NULL. */
struct s3_mbt_req {
    enum evpl_http_request_type method;
    const char    *path;                     /* "/bucket/key", starts with '/' */
    const char    *query;                    /* pre-sorted, no leading '?' */
    const char    *range;                    /* "bytes=0-8191" */
    const char    *copy_source;              /* "/srcbucket/srckey" */
    const char    *copy_range;               /* x-amz-copy-source-range */
    const char    *host;                     /* Host override (virtual-host) */
    const char    *content_type;             /* Content-Type header */
    const char    *meta;                     /* value for the x-amz-meta-m header */
    const char    *content_sha;              /* x-amz-content-sha256 override
                                              * (aws-chunked: STREAMING-...) */
    const char    *access_key;               /* credential override */
    const char    *secret_key;
    enum s3_mbt_auth auth;
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
 * payload (or the aws-chunked STREAMING- marker).  Any extra headers (Range,
 * x-amz-copy-source, Content-Type, x-amz-meta-*) are deliberately left out
 * of SignedHeaders, which SigV4 permits. */
static inline void
s3_mbt_sign(
    const struct s3_mbt_req *req,
    const char              *access_key,
    const char              *secret_key,
    char                    *authorization,
    size_t                   authorization_len)
{
    char          k0[264];
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
             req->host ? req->host : S3_MBT_HOST,
             req->content_sha ? req->content_sha : S3_MBT_PAYLOAD,
             S3_MBT_AMZ_DATE, S3_MBT_SIGNED_HDRS,
             req->content_sha ? req->content_sha : S3_MBT_PAYLOAD);

    s3_mbt_sha256_hex(canonical, strlen(canonical), cr_hash);

    snprintf(to_sign, sizeof(to_sign),
             "AWS4-HMAC-SHA256\n%s\n%s\n%s",
             S3_MBT_AMZ_DATE, S3_MBT_SCOPE, cr_hash);

    snprintf(k0, sizeof(k0), "AWS4%s", secret_key);
    s3_mbt_hmac256(k0, strlen(k0),
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
             access_key, S3_MBT_SCOPE, S3_MBT_SIGNED_HDRS, sig_hex);
} /* s3_mbt_sign */

/* AWS SigV2 Authorization header ("AWS <key>:<base64 hmac-sha1>").  The
 * string-to-sign mirrors the server's verifier: Content-MD5 is never sent,
 * the Date line is empty because x-amz-date is always sent, the
 * CanonicalizedAmzHeaders are exactly the x-amz-* headers this harness emits
 * (sorted), and the CanonicalizedResource is the URI path only -- with the
 * bucket-level trailing-slash quirk the server expects. */
static inline void
s3_mbt_sign_v2(
    const struct s3_mbt_req *req,
    const char              *access_key,
    const char              *secret_key,
    char                    *authorization,
    size_t                   authorization_len)
{
    char          sts[4096];
    unsigned char sig[20];
    unsigned char sig_b64[64];
    unsigned int  siglen = sizeof(sig);
    size_t        off    = 0;
    size_t        plen;
    const char   *method;

    switch (req->method) {
        case EVPL_HTTP_REQUEST_TYPE_GET:    method = "GET";    break;
        case EVPL_HTTP_REQUEST_TYPE_HEAD:   method = "HEAD";   break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:    method = "PUT";    break;
        case EVPL_HTTP_REQUEST_TYPE_POST:   method = "POST";   break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE: method = "DELETE"; break;
        default:
            fprintf(stderr, "s3_mbt_sign_v2: unsupported method %d\n",
                    req->method);
            exit(3);
    } /* switch */

    /* METHOD \n Content-MD5 \n Content-Type \n Date(empty; x-amz-date) \n */
    off += snprintf(sts + off, sizeof(sts) - off, "%s\n\n%s\n\n",
                    method, req->content_type ? req->content_type : "");

    /* CanonicalizedAmzHeaders: the x-amz-* headers s3_mbt_call sends, in
     * lexicographic name order (copy-source < date < meta-m). */
    if (req->copy_source) {
        off += snprintf(sts + off, sizeof(sts) - off,
                        "x-amz-copy-source:%s\n", req->copy_source);
    }
    if (req->copy_range) {
        off += snprintf(sts + off, sizeof(sts) - off,
                        "x-amz-copy-source-range:%s\n", req->copy_range);
    }
    off += snprintf(sts + off, sizeof(sts) - off, "x-amz-date:%s\n",
                    S3_MBT_AMZ_DATE);
    if (req->meta) {
        off += snprintf(sts + off, sizeof(sts) - off, "x-amz-meta-m:%s\n",
                        req->meta);
    }

    /* CanonicalizedResource: the path, sans query; a bucket-level path
     * (/bucket, no key) takes a trailing slash. */
    off += snprintf(sts + off, sizeof(sts) - off, "%s", req->path);
    plen = strlen(req->path);
    if (plen > 1 && req->path[plen - 1] != '/' &&
        strchr(req->path + 1, '/') == NULL) {
        off += snprintf(sts + off, sizeof(sts) - off, "/");
    }

    HMAC(EVP_sha1(), secret_key, (int) strlen(secret_key),
         (const unsigned char *) sts, off, sig, &siglen);
    EVP_EncodeBlock(sig_b64, sig, (int) siglen);

    snprintf(authorization, authorization_len, "AWS %s:%s", access_key,
             sig_b64);
} /* s3_mbt_sign_v2 */

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
    struct evpl_iovec iov[256];
    uint64_t          avail;
    uint64_t          take;
    int               niov, i;

    avail = evpl_http_request_get_data_avail(request);

    while (avail > 0) {
        /* get_datav's last argument is a byte count and it emits as many
         * iovecs as the ring needs for it -- bound each bite so a
         * multipart-scale body cannot overrun the scatter array. */
        take = avail < 256 * 1024 ? avail : 256 * 1024;
        niov = evpl_http_request_get_datav(evpl, request, iov, (int) take);

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
            s3_mbt_copy_header(request, "x-amz-meta-m", res->meta,
                               sizeof(res->meta));
            s3_mbt_copy_header(request, "x-amz-tagging-count", res->tag_count,
                               sizeof(res->tag_count));
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

    enum s3_mbt_auth auth = req->auth;
    const char      *ak   = req->access_key ? req->access_key : S3_MBT_ACCESS_KEY;
    const char      *sk   = req->secret_key ? req->secret_key : S3_MBT_SECRET_KEY;

    if (auth == S3_MBT_AUTH_DEFAULT) {
        auth = env->sigv2 ? S3_MBT_AUTH_V2 : S3_MBT_AUTH_V4;
    }

    evpl_http_request_add_header(request, "Host",
                                 req->host ? req->host : S3_MBT_HOST);
    evpl_http_request_add_header(request, "x-amz-date", S3_MBT_AMZ_DATE);

    if (req->content_type) {
        evpl_http_request_add_header(request, "Content-Type",
                                     req->content_type);
    }
    if (req->meta) {
        evpl_http_request_add_header(request, "x-amz-meta-m", req->meta);
    }
    if (req->range) {
        evpl_http_request_add_header(request, "Range", req->range);
    }
    if (req->copy_source) {
        evpl_http_request_add_header(request, "x-amz-copy-source",
                                     req->copy_source);
    }
    if (req->copy_range) {
        evpl_http_request_add_header(request, "x-amz-copy-source-range",
                                     req->copy_range);
    }

    switch (auth) {
        case S3_MBT_AUTH_V4:
        case S3_MBT_AUTH_V4_BADSIG:
        case S3_MBT_AUTH_V4_BADKEY:
            /* V2 requests deliberately omit x-amz-content-sha256: the server
             * canonicalizes every x-amz-* header it receives, so any header
             * sent must also be in the harness's V2 string-to-sign. */
            evpl_http_request_add_header(request, "x-amz-content-sha256",
                                         req->content_sha ? req->content_sha
                                                          : S3_MBT_PAYLOAD);
            s3_mbt_sign(req,
                        auth == S3_MBT_AUTH_V4_BADKEY ? "nosuchaccesskey" : ak,
                        sk, authorization, sizeof(authorization));
            if (auth == S3_MBT_AUTH_V4_BADSIG) {
                /* corrupt the last hex digit of the signature */
                size_t n = strlen(authorization);
                authorization[n - 1] = authorization[n - 1] == '0' ? '1' : '0';
            }
            evpl_http_request_add_header(request, "Authorization",
                                         authorization);
            break;
        case S3_MBT_AUTH_V2:
        case S3_MBT_AUTH_V2_BADSIG:
            s3_mbt_sign_v2(req, ak, sk, authorization, sizeof(authorization));
            if (auth == S3_MBT_AUTH_V2_BADSIG) {
                /* corrupt the first character of the base64 signature */
                char *colon = strrchr(authorization, ':');
                colon[1] = colon[1] == 'A' ? 'B' : 'A';
            }
            evpl_http_request_add_header(request, "Authorization",
                                         authorization);
            break;
        case S3_MBT_AUTH_NONE:
            break;
        default:
            fprintf(stderr, "s3_mbt_call: bad auth mode %d\n", auth);
            exit(3);
    } /* switch */

    /* Stage the whole request body up front; no WANT_DATA handling needed.
     * A multipart-scale body (5+ MiB blocks) spans several evpl buffer
     * slabs, so allocate a scatter list and honor the returned count. */
    if (req->method == EVPL_HTTP_REQUEST_TYPE_PUT ||
        req->method == EVPL_HTTP_REQUEST_TYPE_POST) {
        evpl_http_client_set_request_length(request, req->body_len);
        if (req->body_len) {
            struct evpl_iovec iov[64];
            int               niov;
            size_t            off = 0;
            int               i;

            niov = evpl_iovec_alloc(env->evpl, req->body_len, 0, 64, 0, iov);
            if (niov <= 0) {
                fprintf(stderr, "s3_mbt: evpl_iovec_alloc(%zu) failed (%d)\n",
                        req->body_len, niov);
                exit(3);
            }
            for (i = 0; i < niov; i++) {
                size_t n = iov[i].length;

                if (n > req->body_len - off) {
                    n = req->body_len - off;
                    evpl_iovec_set_length(&iov[i], n);
                }
                memcpy(evpl_iovec_data(&iov[i]), req->body + off, n);
                off += n;
            }
            evpl_http_request_add_datav(request, iov, niov);
        }
    }

    evpl_http_request_dispatch(request, s3_mbt_notify, &env->res);

    while (!env->res.done) {
        evpl_continue(env->evpl);
    }

    return &env->res;
} /* s3_mbt_call */

/* ---- server + client lifecycle ------------------------------------------ */

/* memfs/diskfs/cairn create named filesystems (mkfs); linux and io_uring are
 * passthrough backends that mount a host directory and have no mkfs. */
static inline int
s3_mbt_module_is_passthrough(const char *module)
{
    return strcmp(module, "linux") == 0 || strcmp(module, "io_uring") == 0;
} /* s3_mbt_module_is_passthrough */

static inline void
s3_mbt_env_open_module(
    struct s3_mbt_env *env,
    const char        *module)
{
    struct chimera_server_config *config;
    struct evpl_thread_config    *tcfg;

    mbt_debug_log_start();

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

    /* Debug aid, mirroring the NFS3 harness's disable_caches option: run
     * with the VFS attr/name caches off to separate cache-coherence
     * suspects from backend behavior. */
    if (getenv("S3_MBT_DISABLE_CACHES")) {
        chimera_server_config_set_attr_cache_enabled(config, 0);
        chimera_server_config_set_name_cache_enabled(config, 0);
    }

    env->module = module ? module : "memfs";

    /* Backend module registration, mirroring nfs3_mbt_common.h: memfs is a
    * default module; diskfs and cairn are self-provisioned under the
    * per-process session_dir (1 GiB sparse libaio image / rocksdb dir) so
    * every replay process is isolated and cleaned up with its temp dir. */
    if (strcmp(env->module, "diskfs") == 0) {
        char img[300], cfg[512];
        int  fd;

        snprintf(img, sizeof(img), "%s/device-0.img", env->session_dir);
        fd = open(img, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0 || ftruncate(fd, 1024LL * 1024 * 1024) != 0) {
            fprintf(stderr, "diskfs device image %s: %s\n", img,
                    strerror(errno));
            exit(1);
        }
        close(fd);
        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"%s\",\"size\":1,\"path\":\"%s\"}]}",
                 CHIMERA_DISKFS_DEVICE_TYPE, img);
        chimera_server_config_add_module(config, "diskfs", NULL, cfg);
    } else if (strcmp(env->module, "cairn") == 0) {
        char dir[300], cfg[512];

        snprintf(dir, sizeof(dir), "%s/cairn", env->session_dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
            fprintf(stderr, "cairn dir %s: %s\n", dir, strerror(errno));
            exit(1);
        }
        snprintf(cfg, sizeof(cfg), "{\"initialize\":true,\"path\":\"%s\"}",
                 dir);
        chimera_server_config_add_module(config, "cairn", NULL, cfg);
    }

    /* Passthrough backends store their trees on a real host filesystem that
     * must support name_to_handle_at (tmpfs and overlayfs do not).  Root the
     * backing store at $CHIMERA_MBT_SCRATCH (default: the current directory);
     * an unsupported filesystem surfaces as an ENOTSUP mount that fs_setup
     * turns into a clean 77 skip. */
    if (s3_mbt_module_is_passthrough(env->module)) {
        const char *scratch = getenv("CHIMERA_MBT_SCRATCH");
        char       *abs_scratch;

        if (!scratch || !scratch[0]) {
            scratch = ".";
        }
        abs_scratch = realpath(scratch, NULL);
        if (!abs_scratch) {
            fprintf(stderr, "realpath(%s) failed: %s\n", scratch,
                    strerror(errno));
            exit(1);
        }
        snprintf(env->pt_root, sizeof(env->pt_root),
                 "%s/s3_mbt_pt_XXXXXX", abs_scratch);
        free(abs_scratch);
        if (!mkdtemp(env->pt_root)) {
            fprintf(stderr, "mkdtemp(%s) failed: %s\n", env->pt_root,
                    strerror(errno));
            exit(1);
        }
    }

    env->server = chimera_server_init(config, env->metrics);

    chimera_server_start(env->server);

    /* Runtime CreateBucket materializes bucket dirs under this VFS path (the
     * per-trace mount recreates /share, so the root stays valid across the
     * whole batch), and every request must be signed with a known cred. */
    chimera_server_set_s3_bucket_root(env->server, "/share");

    /* Bind the harness key to a uid-0 user so the model keeps exercising the
     * same reachable state it did when every key ran as root.  The identity
     * mapping itself is covered by the s3_identity unit test; extending the
     * model to a second, unprivileged tenant (and asserting cross-tenant
     * denial) is follow-on work. */
    chimera_server_add_user(env->server, S3_MBT_USER, "", "", NULL,
                            0, 0, 0, NULL, 1);
    chimera_server_add_s3_cred(env->server, S3_MBT_ACCESS_KEY,
                               S3_MBT_SECRET_KEY, S3_MBT_USER,
                               NULL, NULL, 1);

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
} /* s3_mbt_env_open_module */

static inline void
s3_mbt_env_open(struct s3_mbt_env *env)
{
    s3_mbt_env_open_module(env, "memfs");
} /* s3_mbt_env_open */

/* Per-trace filesystem mounted at /share.  mkfs backends get a fresh named
 * filesystem (unique fsname => distinct fsid, so no stale cache entry can be
 * hit across traces); passthrough backends get a fresh host subdirectory (a
 * brand-new inode is the passthrough analogue of a fresh fsid).  Buckets are
 * NOT pre-created -- CreateBucket is part of the modeled surface and traces
 * create what they use. */
static inline void
s3_mbt_env_fs_setup(
    struct s3_mbt_env *env,
    const char        *fsname)
{
    if (s3_mbt_module_is_passthrough(env->module)) {
        char dir[340];
        int  mrc;

        snprintf(dir, sizeof(dir), "%s/%s", env->pt_root, fsname);
        if (mkdir(dir, 0777) != 0 && errno != EEXIST) {
            fprintf(stderr, "failed to create %s backing dir %s: %s\n",
                    env->module, dir, strerror(errno));
            exit(1);
        }
        mrc = chimera_server_mount(env->server, "share", env->module, dir,
                                   NULL);
        if (mrc == CHIMERA_VFS_ENOTSUP) {
            /* The backing filesystem cannot produce file handles
             * (name_to_handle_at): point CHIMERA_MBT_SCRATCH at an
             * ext4/xfs/btrfs path to exercise this backend.  _exit() to
             * bypass evpl's atexit leak-check. */
            fprintf(stderr, "SKIP: %s backend needs a name_to_handle_at-"
                    "capable scratch fs; %s is not one (set "
                    "CHIMERA_MBT_SCRATCH)\n", env->module, dir);
            _exit(77);
        }
        if (mrc != 0) {
            fprintf(stderr, "mount %s at %s failed: status=%d\n",
                    env->module, dir, mrc);
            _exit(1);
        }
        return;
    }

    if (chimera_server_mkfs(env->server, env->module, fsname, NULL) != 0) {
        fprintf(stderr, "failed to create %s filesystem %s\n", env->module,
                fsname);
        exit(1);
    }
    chimera_server_mount(env->server, "share", env->module, fsname, NULL);
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

    /* Passthrough backends have no filesystem to remove; the unmounted host
     * dir is intentionally left in place (its inode must not be reused
     * mid-run) and is reaped with the scratch dir. */
    if (s3_mbt_module_is_passthrough(env->module)) {
        return;
    }

    while (chimera_server_rmfs(env->server, env->module, fsname) != 0) {
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
    char cmd[340];

    evpl_http_client_close(env->agent, env->conn);
    evpl_http_destroy(env->agent);
    evpl_destroy(env->evpl);

    chimera_server_destroy(env->server);
    mbt_metrics_dump(env->metrics);
    prometheus_metrics_destroy(env->metrics);

    free(env->body_buf);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", env->session_dir);
    }
    if (env->pt_root[0]) {
        /* Debug aid: keep the passthrough backing tree for post-mortem
         * inspection of what the server actually left on disk. */
        if (getenv("S3_MBT_KEEP_SCRATCH")) {
            fprintf(stderr, "keeping passthrough scratch %s\n", env->pt_root);
        } else {
            snprintf(cmd, sizeof(cmd), "rm -rf %s", env->pt_root);
            if (system(cmd) != 0) {
                fprintf(stderr, "warning: failed to remove %s\n",
                        env->pt_root);
            }
        }
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

/* Frame a payload as aws-chunked (SigV4 streaming upload) content: chunks of
 * `chunk_size` as "<hex-size>;chunk-signature=<64 zeros>\r\n<data>\r\n",
 * terminated by the zero-length chunk.  The server decodes the framing and
 * never verifies the per-chunk signatures, so a fixed dummy signature keeps
 * the request deterministic.  Returns the framed length. */
static inline size_t
s3_mbt_aws_chunkify(
    const uint8_t *in,
    size_t         in_len,
    size_t         chunk_size,
    uint8_t       *out)
{
    static const char dummy_sig[] =
        "0000000000000000000000000000000000000000000000000000000000000000";
    size_t            off = 0, pos = 0;

    while (pos < in_len) {
        size_t n = in_len - pos < chunk_size ? in_len - pos : chunk_size;

        off += sprintf((char *) out + off, "%zx;chunk-signature=%s\r\n", n,
                       dummy_sig);
        memcpy(out + off, in + pos, n);
        off       += n;
        out[off++] = '\r';
        out[off++] = '\n';
        pos       += n;
    }
    off += sprintf((char *) out + off, "0;chunk-signature=%s\r\n\r\n",
                   dummy_sig);
    return off;
} /* s3_mbt_aws_chunkify */
