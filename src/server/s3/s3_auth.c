// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <urcu/urcu-qsbr.h>

#include "evpl/evpl_http.h"
#include "s3_auth.h"
#include "s3_cred_cache.h"
#include "s3_internal.h"

#define AWS4_HMAC_SHA256     "AWS4-HMAC-SHA256"
#define AWS4_REQUEST         "aws4_request"
#define SHA256_DIGEST_LENGTH 32
#define SHA1_DIGEST_LENGTH   20

/*
 * Compute SHA256 hash of data and output as hex string
 */
static void
sha256_hex(
    const unsigned char *data,
    size_t               len,
    char                *out)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    EVP_MD_CTX   *ctx = EVP_MD_CTX_new();

    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, hash, NULL);
    EVP_MD_CTX_free(ctx);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(out + (i * 2), "%02x", hash[i]);
    }
    out[64] = '\0';
} /* sha256_hex */

/*
 * Compute HMAC-SHA256
 */
static void
hmac_sha256(
    const unsigned char *key,
    size_t               key_len,
    const unsigned char *data,
    size_t               data_len,
    unsigned char       *out)
{
    unsigned int out_len = SHA256_DIGEST_LENGTH;

    HMAC(EVP_sha256(), key, key_len, data, data_len, out, &out_len);
} /* hmac_sha256 */

/*
 * Compute HMAC-SHA1
 */
static void
hmac_sha1(
    const unsigned char *key,
    size_t               key_len,
    const unsigned char *data,
    size_t               data_len,
    unsigned char       *out)
{
    unsigned int out_len = SHA1_DIGEST_LENGTH;

    HMAC(EVP_sha1(), key, key_len, data, data_len, out, &out_len);
} /* hmac_sha1 */

/*
 * Base64 encode data
 */
static int
base64_encode(
    const unsigned char *data,
    size_t               len,
    char                *out,
    size_t               out_max)
{
    BIO     *b64, *bio;
    BUF_MEM *bufferPtr;
    int      result_len;

    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    BIO_write(bio, data, len);
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);

    result_len = bufferPtr->length;
    if (result_len >= (int) out_max) {
        result_len = out_max - 1;
    }
    memcpy(out, bufferPtr->data, result_len);
    out[result_len] = '\0';

    BIO_free_all(bio);

    return result_len;
} /* base64_encode */

/*
 * Collected x-amz-* header for V2 canonical headers section.
 * Name is stored lowercased; value points into the request's header storage.
 */
struct amz_header_entry {
    char        name[64];
    const char *value;
};

#define V2_MAX_AMZ_HEADERS 64

struct amz_header_collector {
    struct amz_header_entry entries[V2_MAX_AMZ_HEADERS];
    int                     count;
};

static void
collect_amz_header(
    const char *name,
    const char *value,
    void       *private_data)
{
    struct amz_header_collector *c = private_data;
    struct amz_header_entry     *e;
    int                          i;

    if (strncasecmp(name, "x-amz-", 6) != 0) {
        return;
    }
    if (c->count >= V2_MAX_AMZ_HEADERS) {
        return;
    }

    e = &c->entries[c->count++];

    for (i = 0; name[i] && i < (int) sizeof(e->name) - 1; i++) {
        e->name[i] = tolower((unsigned char) name[i]);
    }
    e->name[i] = '\0';
    e->value   = value;
} /* collect_amz_header */

static int
amz_header_compare(
    const void *a,
    const void *b)
{
    const struct amz_header_entry *ha = a;
    const struct amz_header_entry *hb = b;

    return strcmp(ha->name, hb->name);
} /* amz_header_compare */

/*
 * Parse AWS Signature V2 Authorization header
 * Format: AWS AWSAccessKeyId:Signature
 */
static int
parse_auth_header_v2(
    const char *auth_header,
    char       *access_key,
    int         access_key_max,
    char       *signature,
    int         signature_max)
{
    const char *p;
    const char *end;

    /* Check for "AWS " prefix */
    if (strncmp(auth_header, "AWS ", 4) != 0) {
        return -1;
    }

    p = auth_header + 4;

    /* Extract access key (up to ':') */
    end = strchr(p, ':');
    if (!end || (end - p) >= access_key_max) {
        return -1;
    }
    strncpy(access_key, p, end - p);
    access_key[end - p] = '\0';
    p                   = end + 1;

    /* Extract signature (rest of string) */
    strncpy(signature, p, signature_max - 1);
    signature[signature_max - 1] = '\0';

    /* Trim trailing whitespace from signature */
    int sig_len = strlen(signature);
    while (sig_len > 0 && isspace(signature[sig_len - 1])) {
        signature[--sig_len] = '\0';
    }

    return 0;
} /* parse_auth_header_v2 */

/*
 * Build string to sign for AWS Signature V2
 *
 * StringToSign = HTTP-Verb + "\n" +
 *                Content-MD5 + "\n" +
 *                Content-Type + "\n" +
 *                Date + "\n" +
 *                CanonicalizedAmzHeaders +
 *                CanonicalizedResource
 *
 * Note: If x-amz-date is present, Date should be empty and x-amz-date
 * goes in CanonicalizedAmzHeaders
 */
/*
 * Bounded append into a fixed-size signing buffer.
 *
 * This replaces the unchecked `offset += snprintf(buf + offset, max - offset,
 * ...)` idiom.  That idiom overflows the buffer as soon as `offset` reaches
 * `max`: snprintf returns the length it *would* have written, so one oversized
 * field (a long attacker-controlled header value, URI, or query string) pushes
 * `offset` past `max`; the next call then passes a negative `int` `max - offset`
 * that converts to a huge `size_t`, with `buf + offset` already past the end of
 * the buffer -- an unbounded out-of-bounds write.
 *
 * Returns 0 on success and advances *offset; returns -1 if the formatted text
 * does not fit.  Truncation is treated as failure by the callers, which is
 * correct: a request whose canonical form overflows the buffer cannot match a
 * legitimate signature anyway, so it is rejected rather than mis-signed.
 */
static int
sts_appendf(
    char       *buf,
    int         max_len,
    int        *offset,
    const char *fmt,
    ...)
{
    va_list ap;
    int     avail, n;

    if (*offset < 0 || *offset >= max_len) {
        return -1;
    }

    avail = max_len - *offset;

    va_start(ap, fmt);
    n = vsnprintf(buf + *offset, avail, fmt, ap);
    va_end(ap);

    if (n < 0 || n >= avail) {
        return -1;
    }

    *offset += n;
    return 0;
} /* sts_appendf */

static int
build_string_to_sign_v2(
    struct evpl_http_request *request,
    char                     *string_to_sign,
    int                       max_len)
{
    const char *method;
    const char *content_md5;
    const char *content_type;
    const char *date;
    const char *amz_date;
    const char *host;
    const char *uri;
    int         uri_len;
    int         offset = 0;

    /* HTTP Method */
    switch (evpl_http_request_type(request)) {
        case EVPL_HTTP_REQUEST_TYPE_GET:
            method = "GET";
            break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:
            method = "PUT";
            break;
        case EVPL_HTTP_REQUEST_TYPE_POST:
            method = "POST";
            break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE:
            method = "DELETE";
            break;
        case EVPL_HTTP_REQUEST_TYPE_HEAD:
            method = "HEAD";
            break;
        default:
            return -1;
    } /* switch */

    if (sts_appendf(string_to_sign, max_len, &offset, "%s\n", method)) {
        return -1;
    }

    /* Content-MD5 (empty if not present) */
    content_md5 = evpl_http_request_header(request, "Content-MD5");
    if (sts_appendf(string_to_sign, max_len, &offset, "%s\n",
                    content_md5 ? content_md5 : "")) {
        return -1;
    }

    /* Content-Type (empty if not present) */
    content_type = evpl_http_request_header(request, "Content-Type");
    if (sts_appendf(string_to_sign, max_len, &offset, "%s\n",
                    content_type ? content_type : "")) {
        return -1;
    }

    /* Date - if x-amz-date is present, Date should be empty */
    amz_date = evpl_http_request_header(request, "x-amz-date");
    if (amz_date) {
        /* x-amz-date is present, Date line should be empty */
        if (sts_appendf(string_to_sign, max_len, &offset, "\n")) {
            return -1;
        }
    } else {
        date = evpl_http_request_header(request, "Date");
        if (sts_appendf(string_to_sign, max_len, &offset, "%s\n",
                        date ? date : "")) {
            return -1;
        }
    }

    /*
     * CanonicalizedAmzHeaders: every x-amz-* request header, lowercased,
     * sorted lexicographically by name, emitted as "name:value\n".
     *
     * Recent boto3 (>= 1.36) adds x-amz-checksum-* and
     * x-amz-sdk-checksum-algorithm headers on PUT/POST by default and
     * includes them in the signing string, so we must canonicalize all
     * x-amz-* headers rather than only the ones we recognize.
     */
    {
        struct amz_header_collector amz = { .count = 0 };

        evpl_http_request_header_iterate(request, collect_amz_header, &amz);

        if (amz.count > 1) {
            qsort(amz.entries, amz.count, sizeof(amz.entries[0]),
                  amz_header_compare);
        }

        for (int i = 0; i < amz.count; i++) {
            if (sts_appendf(string_to_sign, max_len, &offset, "%s:%s\n",
                            amz.entries[i].name, amz.entries[i].value)) {
                return -1;
            }
        }
    }

    /* Canonicalized Resource */
    /* For virtual-hosted style: /<bucket>/<key> */
    /* For path style: /<bucket>/<key> as-is from URI */
    /*
     * IMPORTANT: AWS V2 signing only includes specific subresources in the
     * canonical resource, NOT all query parameters. The subresources are:
     * acl, delete, lifecycle, location, logging, notification, partNumber,
     * policy, requestPayment, torrent, uploadId, uploads, versionId,
     * versioning, versions, website
     *
     * Regular query parameters like list-type, delimiter, encoding-type
     * are NOT included in the signature.
     *
     * IMPORTANT: For bucket-level operations (path is /bucket without key),
     * boto3 uses auth_path = /bucket/ with a trailing slash.
     */

    host = evpl_http_request_header(request, "Host");
    uri  = evpl_http_request_url(request, &uri_len);

    /* Find where the query string starts (if any) */
    const char *query_start = memchr(uri, '?', uri_len);
    int         path_len    = query_start ? (query_start - uri) : uri_len;

    chimera_s3_debug("V2 build_sts: host=%s uri=%.*s path_len=%d",
                     host ? host : "(null)", uri_len, uri, path_len);

    /* Check if virtual-hosted style (bucket is in host) */
    int         bucket_extracted = 0;

    if (host) {
        /* Look for bucket name before the port or first '.' */
        const char *port_start = strchr(host, ':');
        const char *dot        = strchr(host, '.');

        /* If there's a port (e.g., mybucket.localhost:5000) */
        if (port_start) {
            /* Check if there's text before the host portion */
            /* For "mybucket.localhost:5000", bucket is "mybucket" */
            const char *host_end = port_start;
            while (host_end > host && *(host_end - 1) != '.') {
                host_end--;
            }

            if (host_end > host) {
                /* Virtual-hosted style: prepend bucket name */
                int bucket_len = host_end - host - 1;                 /* -1 for the trailing dot */
                if (sts_appendf(string_to_sign, max_len, &offset, "/")) {
                    return -1;
                }
                if (bucket_len > 0) {
                    int copy_len = bucket_len;
                    if (offset + copy_len < max_len) {
                        memcpy(string_to_sign + offset, host, copy_len);
                        offset          += copy_len;
                        bucket_extracted = 1;
                    }
                }
            }
        } else if (dot) {
            /* No port, check for bucket.s3.amazonaws.com style */
            int bucket_len = dot - host;
            if (sts_appendf(string_to_sign, max_len, &offset, "/")) {
                return -1;
            }
            if (bucket_len > 0 && offset + bucket_len < max_len) {
                memcpy(string_to_sign + offset, host, bucket_len);
                offset          += bucket_len;
                bucket_extracted = 1;
            }
        }
    }

    /* Append only the URI path (NOT query parameters) for V2 */
    {
        const char *path_start_ptr = uri;
        int         copy_len       = path_len;
        int         needs_trailing_slash;

        /* Skip leading slashes from URI if bucket was already prepended */
        if (bucket_extracted) {
            while (*path_start_ptr == '/' && copy_len > 0) {
                path_start_ptr++;
                copy_len--;
            }
            /* Add a single slash separator (leave room for the NUL) */
            if (offset < max_len - 1) {
                string_to_sign[offset++] = '/';
            }
        }

        if (offset + copy_len < max_len) {
            memcpy(string_to_sign + offset, path_start_ptr, copy_len);
            offset += copy_len;
        }

        /*
         * boto3 uses auth_path with a trailing slash for bucket-level operations.
         * A bucket-level path is /bucket (no key), while an object-level path is
         * /bucket/key. We detect this by checking if there's no '/' after the
         * bucket name in the path.
         *
         * Path format: /bucket or /bucket/key
         * - /mybucket -> bucket-level, needs trailing slash -> /mybucket/
         * - /mybucket/key -> object-level, no change
         */
        needs_trailing_slash = 0;
        if (copy_len > 0 && path_start_ptr[copy_len - 1] != '/') {
            /* Path doesn't end with slash, check if it's bucket-level */
            /* Find the first '/' in the path (after leading /) */
            const char *slash = memchr(path_start_ptr + 1, '/', copy_len - 1);
            if (!slash) {
                /* No slash found after bucket name, this is a bucket-level path */
                needs_trailing_slash = 1;
            }
        }

        if (needs_trailing_slash && offset < max_len - 1) {
            string_to_sign[offset++] = '/';
        }
    }

    string_to_sign[offset] = '\0';

    chimera_s3_debug("V2 String to sign:\n%s", string_to_sign);

    return offset;
} /* build_string_to_sign_v2 */

/*
 * Verify AWS Signature V2
 */
static enum chimera_s3_auth_result
verify_signature_v2(
    struct chimera_s3_cred_cache *cred_cache,
    struct evpl_http_request     *request,
    const char                   *auth_header)
{
    char access_key[128];
    char signature[128];
    char string_to_sign[4096];
    char expected_signature[128];
    unsigned char sig_bytes[SHA1_DIGEST_LENGTH];
    const struct chimera_s3_cred *cred;
    int sts_len;
    const char *hdr;

    /* Parse the V2 Authorization header */
    if (parse_auth_header_v2(auth_header,
                             access_key, sizeof(access_key),
                             signature, sizeof(signature)) != 0) {
        chimera_s3_debug("Failed to parse V2 auth header");
        return CHIMERA_S3_AUTH_INVALID_AUTH_HEADER;
    }

    chimera_s3_debug("V2 Auth: access_key=%s, signature=%s", access_key, signature);

    /* Debug: print all relevant headers */
    hdr = evpl_http_request_header(request, "Date");
    chimera_s3_debug("V2 Header Date: %s", hdr ? hdr : "(null)");
    hdr = evpl_http_request_header(request, "x-amz-date");
    chimera_s3_debug("V2 Header x-amz-date: %s", hdr ? hdr : "(null)");
    hdr = evpl_http_request_header(request, "Content-Type");
    chimera_s3_debug("V2 Header Content-Type: %s", hdr ? hdr : "(null)");
    hdr = evpl_http_request_header(request, "Content-MD5");
    chimera_s3_debug("V2 Header Content-MD5: %s", hdr ? hdr : "(null)");
    hdr = evpl_http_request_header(request, "Host");
    chimera_s3_debug("V2 Header Host: %s", hdr ? hdr : "(null)");

    /* Look up credentials */
    rcu_read_lock();
    cred = chimera_s3_cred_cache_lookup(cred_cache, access_key, strlen(access_key));
    if (!cred) {
        rcu_read_unlock();
        chimera_s3_debug("Unknown access key: %s", access_key);
        return CHIMERA_S3_AUTH_UNKNOWN_ACCESS_KEY;
    }

    /* Build string to sign */
    sts_len = build_string_to_sign_v2(request, string_to_sign, sizeof(string_to_sign));
    if (sts_len < 0) {
        rcu_read_unlock();
        chimera_s3_debug("Failed to build string to sign");
        return CHIMERA_S3_AUTH_INVALID_AUTH_HEADER;
    }

    chimera_s3_debug("V2 String to sign:\n%s", string_to_sign);

    /* Compute HMAC-SHA1 */
    hmac_sha1((unsigned char *) cred->secret_key, strlen(cred->secret_key),
              (unsigned char *) string_to_sign, sts_len, sig_bytes);

    rcu_read_unlock();

    /* Base64 encode the result */
    base64_encode(sig_bytes, SHA1_DIGEST_LENGTH,
                  expected_signature, sizeof(expected_signature));

    chimera_s3_debug("V2 Expected signature: %s", expected_signature);
    chimera_s3_debug("V2 Received signature: %s", signature);

    /* Compare signatures */
    if (strcmp(expected_signature, signature) != 0) {
        chimera_s3_debug("V2 Signature mismatch");
        return CHIMERA_S3_AUTH_SIGNATURE_MISMATCH;
    }

    chimera_s3_debug("V2 Authentication successful");
    return CHIMERA_S3_AUTH_OK;
} /* verify_signature_v2 */

/*
 * Parse AWS Signature V4 Authorization header
 * Format: AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/<service>/aws4_request, SignedHeaders=<headers>, Signature=<sig>
 */
static int
parse_auth_header_v4(
    const char *auth_header,
    char       *access_key,
    int         access_key_max,
    char       *date_stamp,       /* YYYYMMDD */
    int         date_stamp_max,
    char       *region,
    int         region_max,
    char       *service,
    int         service_max,
    char       *signed_headers,
    int         signed_headers_max,
    char       *signature,
    int         signature_max)
{
    const char *p;
    const char *end;

    /* Check algorithm */
    if (strncmp(auth_header, AWS4_HMAC_SHA256 " ", strlen(AWS4_HMAC_SHA256) + 1) != 0) {
        return -1;
    }

    p = auth_header + strlen(AWS4_HMAC_SHA256) + 1;

    /* Parse Credential */
    if (strncmp(p, "Credential=", 11) != 0) {
        return -1;
    }
    p += 11;

    /* Extract access key (up to '/') */
    end = strchr(p, '/');
    if (!end || (end - p) >= access_key_max) {
        return -1;
    }
    strncpy(access_key, p, end - p);
    access_key[end - p] = '\0';
    p                   = end + 1;

    /* Extract date stamp (YYYYMMDD, up to '/') */
    end = strchr(p, '/');
    if (!end || (end - p) >= date_stamp_max) {
        return -1;
    }
    strncpy(date_stamp, p, end - p);
    date_stamp[end - p] = '\0';
    p                   = end + 1;

    /* Extract region (up to '/') */
    end = strchr(p, '/');
    if (!end || (end - p) >= region_max) {
        return -1;
    }
    strncpy(region, p, end - p);
    region[end - p] = '\0';
    p               = end + 1;

    /* Extract service (up to '/') */
    end = strchr(p, '/');
    if (!end || (end - p) >= service_max) {
        return -1;
    }
    strncpy(service, p, end - p);
    service[end - p] = '\0';
    p                = end + 1;

    /* Skip aws4_request */
    if (strncmp(p, AWS4_REQUEST, strlen(AWS4_REQUEST)) != 0) {
        return -1;
    }
    p += strlen(AWS4_REQUEST);

    /* Skip ", SignedHeaders=" */
    if (strncmp(p, ", SignedHeaders=", 16) != 0) {
        /* Try alternate format with space */
        if (strncmp(p, ",SignedHeaders=", 15) == 0) {
            p += 15;
        } else {
            return -1;
        }
    } else {
        p += 16;
    }

    /* Extract signed headers (up to ',') */
    end = strchr(p, ',');
    if (!end || (end - p) >= signed_headers_max) {
        return -1;
    }
    strncpy(signed_headers, p, end - p);
    signed_headers[end - p] = '\0';
    p                       = end + 1;

    /* Skip " Signature=" or "Signature=" */
    while (*p == ' ') {
        p++;
    }
    if (strncmp(p, "Signature=", 10) != 0) {
        return -1;
    }
    p += 10;

    /* Extract signature (rest of string) */
    strncpy(signature, p, signature_max - 1);
    signature[signature_max - 1] = '\0';

    /* Trim trailing whitespace from signature */
    int sig_len = strlen(signature);
    while (sig_len > 0 && isspace(signature[sig_len - 1])) {
        signature[--sig_len] = '\0';
    }

    return 0;
} /* parse_auth_header_v4 */

/*
 * Compare function for qsort to sort query parameters
 */
static int
query_param_compare(
    const void *a,
    const void *b)
{
    return strcmp(*(const char **) a, *(const char **) b);
} /* query_param_compare */

/*
 * Canonicalize query string by sorting parameters alphabetically
 * Input: query string without leading '?'
 * Output: sorted query string
 */
static int
canonicalize_query_string(
    const char *query,
    int         query_len,
    char       *out,
    int         out_max)
{
    char  query_copy[4096];
    char *params[256];
    int   param_count = 0;
    char *p, *saveptr;
    int   offset = 0;

    if (!query || query_len == 0) {
        out[0] = '\0';
        return 0;
    }

    /* Copy query string for tokenization */
    if (query_len >= (int) sizeof(query_copy)) {
        query_len = sizeof(query_copy) - 1;
    }
    strncpy(query_copy, query, query_len);
    query_copy[query_len] = '\0';

    /* Parse into individual parameters */
    p = strtok_r(query_copy, "&", &saveptr);
    while (p && param_count < 256) {
        params[param_count++] = p;
        p                     = strtok_r(NULL, "&", &saveptr);
    }

    /* Sort parameters alphabetically */
    qsort(params, param_count, sizeof(char *), query_param_compare);

    /* Reconstruct sorted query string. Per AWS V4 spec: bare keys (no value)
     * must be canonicalized as "key=". */
    for (int i = 0; i < param_count && offset < out_max - 1; i++) {
        if (i > 0) {
            out[offset++] = '&';
        }
        size_t len = strlen(params[i]);
        if (offset + len >= out_max) {
            len = out_max - offset - 1;
        }
        memcpy(out + offset, params[i], len);
        offset += len;
        if (memchr(params[i], '=', len) == NULL && offset < out_max - 1) {
            out[offset++] = '=';
        }
    }
    out[offset] = '\0';

    return offset;
} /* canonicalize_query_string */

/*
 * Build canonical request string for V4
 */
static int
build_canonical_request_v4(
    struct evpl_http_request *request,
    const char               *signed_headers,
    char                     *canonical_request,
    int                       max_len)
{
    const char *method;
    const char *uri;
    int         uri_len;
    char        payload_hash[65];
    int         offset = 0;

    /* HTTP Method */
    switch (evpl_http_request_type(request)) {
        case EVPL_HTTP_REQUEST_TYPE_GET:
            method = "GET";
            break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:
            method = "PUT";
            break;
        case EVPL_HTTP_REQUEST_TYPE_POST:
            method = "POST";
            break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE:
            method = "DELETE";
            break;
        case EVPL_HTTP_REQUEST_TYPE_HEAD:
            method = "HEAD";
            break;
        default:
            return -1;
    } /* switch */

    if (sts_appendf(canonical_request, max_len, &offset, "%s\n", method)) {
        return -1;
    }

    /*
     * Canonical URI.
     *
     * Per AWS SigV4, CanonicalURI is the URI-encoded request path: each path
     * segment is RFC 3986 encoded (unreserved A-Z a-z 0-9 - _ . ~ kept
     * literally, every other byte as %XX with UPPERCASE hex) while the '/'
     * segment separators are left literal. For S3 the path is encoded exactly
     * once (S3 does not double-encode the way most other services do).
     *
     * The client signs using that encoded path, and evpl_http_request_url()
     * returns the request-target verbatim from the wire -- i.e. already in the
     * exact percent-encoded form the client signed. We must therefore use it
     * as-is. Running url_encode() over it would re-encode the '%' of each
     * existing escape (e.g. "%20" -> "%2520"), producing a CanonicalURI that
     * never matches the client's and rejecting every key with a space, '+',
     * '%', or non-ASCII byte. So copy the path through unchanged.
     */
    uri = evpl_http_request_url(request, &uri_len);

    /* Find query string separator */
    const char *query = strchr(uri, '?');
    int         path_len;
    if (query) {
        path_len = query - uri;
    } else {
        path_len = uri_len;
    }

    if (sts_appendf(canonical_request, max_len, &offset, "%.*s\n",
                    path_len, uri)) {
        return -1;
    }

    /* Canonical Query String - must be sorted alphabetically */
    if (query) {
        char sorted_query[4096];
        int  query_str_len = uri_len - (query + 1 - uri);
        canonicalize_query_string(query + 1, query_str_len, sorted_query, sizeof(sorted_query));
        if (sts_appendf(canonical_request, max_len, &offset, "%s\n", sorted_query)) {
            return -1;
        }
    } else {
        if (sts_appendf(canonical_request, max_len, &offset, "\n")) {
            return -1;
        }
    }

    /* Canonical Headers */
    char        signed_headers_copy[1024];
    const char *header_name;
    char       *saveptr;
    char        header_lower[256];

    snprintf(signed_headers_copy, sizeof(signed_headers_copy), "%s", signed_headers);

    header_name = strtok_r(signed_headers_copy, ";", &saveptr);
    while (header_name) {
        int         i;
        for (i = 0; header_name[i] && i < (int) sizeof(header_lower) - 1; i++) {
            header_lower[i] = tolower(header_name[i]);
        }
        header_lower[i] = '\0';

        const char *header_value = evpl_http_request_header(request, header_name);
        if (!header_value) {
            header_value = evpl_http_request_header(request, header_lower);
        }

        if (header_value) {
            while (*header_value == ' ') {
                header_value++;
            }
            if (sts_appendf(canonical_request, max_len, &offset,
                            "%s:%s\n", header_lower, header_value)) {
                return -1;
            }
        } else {
            if (sts_appendf(canonical_request, max_len, &offset,
                            "%s:\n", header_lower)) {
                return -1;
            }
        }

        header_name = strtok_r(NULL, ";", &saveptr);
    }

    /* Empty line after headers */
    if (sts_appendf(canonical_request, max_len, &offset, "\n")) {
        return -1;
    }

    /* Signed Headers */
    if (sts_appendf(canonical_request, max_len, &offset, "%s\n", signed_headers)) {
        return -1;
    }

    /* Hashed Payload.
     *
     * For SigV4 streaming uploads the client sends a sentinel here
     * (STREAMING-AWS4-HMAC-SHA256-PAYLOAD or STREAMING-UNSIGNED-PAYLOAD-TRAILER)
     * rather than a payload hash, and that literal value is exactly what the
     * seed signature is computed over -- so copying it verbatim is correct.
     * The streaming sentinel additionally signals that the body is wrapped in
     * aws-chunked framing; the PUT / UploadPart handlers detect it (see
     * s3_server_dispatch) and de-chunk the body before storing it. */
    const char *content_sha256 = evpl_http_request_header(request, "x-amz-content-sha256");
    if (content_sha256) {
        strncpy(payload_hash, content_sha256, sizeof(payload_hash) - 1);
        payload_hash[sizeof(payload_hash) - 1] = '\0';
    } else {
        sha256_hex((const unsigned char *) "", 0, payload_hash);
    }

    if (sts_appendf(canonical_request, max_len, &offset, "%s", payload_hash)) {
        return -1;
    }

    return offset;
} /* build_canonical_request_v4 */

/*
 * Derive the signing key for V4
 */
static void
derive_signing_key_v4(
    const char    *secret_key,
    const char    *date_stamp,
    const char    *region,
    const char    *service,
    unsigned char *signing_key)
{
    unsigned char k_date[SHA256_DIGEST_LENGTH];
    unsigned char k_region[SHA256_DIGEST_LENGTH];
    unsigned char k_service[SHA256_DIGEST_LENGTH];
    char          aws4_key[260];

    snprintf(aws4_key, sizeof(aws4_key), "AWS4%s", secret_key);
    hmac_sha256((unsigned char *) aws4_key, strlen(aws4_key),
                (unsigned char *) date_stamp, strlen(date_stamp), k_date);

    hmac_sha256(k_date, SHA256_DIGEST_LENGTH,
                (unsigned char *) region, strlen(region), k_region);

    hmac_sha256(k_region, SHA256_DIGEST_LENGTH,
                (unsigned char *) service, strlen(service), k_service);

    hmac_sha256(k_service, SHA256_DIGEST_LENGTH,
                (unsigned char *) AWS4_REQUEST, strlen(AWS4_REQUEST), signing_key);
} /* derive_signing_key_v4 */

/*
 * Verify AWS Signature V4
 */
static enum chimera_s3_auth_result
verify_signature_v4(
    struct chimera_s3_cred_cache *cred_cache,
    struct evpl_http_request     *request,
    const char                   *auth_header)
{
    const char *amz_date;
    const struct chimera_s3_cred *cred;
    char access_key[128];
    char date_stamp[16];
    char region[64];
    char service[32];
    char signed_headers[1024];
    char signature[128];
    char canonical_request[8192];
    char canonical_hash[65];
    char string_to_sign[1024];
    char expected_signature[65];
    unsigned char signing_key[SHA256_DIGEST_LENGTH];
    unsigned char sig_bytes[SHA256_DIGEST_LENGTH];
    int cr_len;

    /* Parse the V4 Authorization header */
    if (parse_auth_header_v4(auth_header,
                             access_key, sizeof(access_key),
                             date_stamp, sizeof(date_stamp),
                             region, sizeof(region),
                             service, sizeof(service),
                             signed_headers, sizeof(signed_headers),
                             signature, sizeof(signature)) != 0) {
        chimera_s3_debug("Failed to parse V4 auth header");
        return CHIMERA_S3_AUTH_INVALID_AUTH_HEADER;
    }

    chimera_s3_debug("V4 Parsed: access_key=%s date=%s region=%s service=%s signed_headers=%s",
                     access_key, date_stamp, region, service, signed_headers);

    /* Get x-amz-date header */
    amz_date = evpl_http_request_header(request, "x-amz-date");
    if (!amz_date) {
        amz_date = evpl_http_request_header(request, "Date");
        if (!amz_date) {
            chimera_s3_debug("No date header found");
            return CHIMERA_S3_AUTH_DATE_MISSING;
        }
    }

    /* Look up credentials */
    rcu_read_lock();
    cred = chimera_s3_cred_cache_lookup(cred_cache, access_key, strlen(access_key));
    if (!cred) {
        rcu_read_unlock();
        chimera_s3_debug("Unknown access key: %s", access_key);
        return CHIMERA_S3_AUTH_UNKNOWN_ACCESS_KEY;
    }

    /* Build canonical request */
    cr_len = build_canonical_request_v4(request, signed_headers,
                                        canonical_request, sizeof(canonical_request));
    if (cr_len < 0) {
        rcu_read_unlock();
        return CHIMERA_S3_AUTH_INVALID_AUTH_HEADER;
    }

    chimera_s3_debug("V4 Canonical request:\n%s", canonical_request);

    /* Hash the canonical request */
    sha256_hex((unsigned char *) canonical_request, cr_len, canonical_hash);

    /* Build string to sign */
    snprintf(string_to_sign, sizeof(string_to_sign),
             "%s\n%s\n%s/%s/%s/%s\n%s",
             AWS4_HMAC_SHA256,
             amz_date,
             date_stamp, region, service, AWS4_REQUEST,
             canonical_hash);

    chimera_s3_debug("V4 String to sign:\n%s", string_to_sign);

    /* Derive signing key */
    derive_signing_key_v4(cred->secret_key, date_stamp, region, service, signing_key);

    rcu_read_unlock();

    /* Calculate expected signature */
    hmac_sha256(signing_key, SHA256_DIGEST_LENGTH,
                (unsigned char *) string_to_sign, strlen(string_to_sign), sig_bytes);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(expected_signature + (i * 2), "%02x", sig_bytes[i]);
    }
    expected_signature[64] = '\0';

    /* Compare signatures */
    if (strcasecmp(expected_signature, signature) != 0) {
        chimera_s3_debug("V4 Signature mismatch: expected=%s, got=%s",
                         expected_signature, signature);
        return CHIMERA_S3_AUTH_SIGNATURE_MISMATCH;
    }

    chimera_s3_debug("V4 Authentication successful");
    return CHIMERA_S3_AUTH_OK;
} /* verify_signature_v4 */

enum chimera_s3_auth_result
chimera_s3_auth_verify(
    struct chimera_s3_cred_cache *cred_cache,
    struct evpl_http_request     *request)
{
    const char *auth_header;

    /* Get Authorization header */
    auth_header = evpl_http_request_header(request, "Authorization");
    if (!auth_header) {
        return CHIMERA_S3_AUTH_NO_AUTH_HEADER;
    }

    chimera_s3_debug("Auth header: %s", auth_header);

    /* Detect signature version and verify */
    if (strncmp(auth_header, "AWS4-HMAC-SHA256 ", 17) == 0) {
        /* AWS Signature Version 4 */
        return verify_signature_v4(cred_cache, request, auth_header);
    } else if (strncmp(auth_header, "AWS ", 4) == 0) {
        /* AWS Signature Version 2 */
        return verify_signature_v2(cred_cache, request, auth_header);
    } else {
        chimera_s3_debug("Unsupported auth type");
        return CHIMERA_S3_AUTH_INVALID_AUTH_HEADER;
    }
} /* chimera_s3_auth_verify */

const char *
chimera_s3_auth_error_message(enum chimera_s3_auth_result result)
{
    switch (result) {
        case CHIMERA_S3_AUTH_OK:
            return "OK";
        case CHIMERA_S3_AUTH_NO_AUTH_HEADER:
            return "Missing Authorization header";
        case CHIMERA_S3_AUTH_INVALID_AUTH_HEADER:
            return "Invalid Authorization header format";
        case CHIMERA_S3_AUTH_UNKNOWN_ACCESS_KEY:
            return "Unknown access key";
        case CHIMERA_S3_AUTH_SIGNATURE_MISMATCH:
            return "Signature does not match";
        case CHIMERA_S3_AUTH_DATE_MISSING:
            return "Missing date header";
        case CHIMERA_S3_AUTH_DATE_EXPIRED:
            return "Request date is expired";
        default:
            return "Unknown error";
    } /* switch */
} /* chimera_s3_auth_error_message */
