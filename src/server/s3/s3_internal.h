// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <inttypes.h>
#include <time.h>
#include <utlist.h>

#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "s3_status.h"
#include "vfs/vfs.h"
#include "common/tcp_flavor.h"
#include "vfs/sdk/vfs_cred.h"
#include "s3_cred_cache.h"
#include "s3_chunk.h"

enum chimera_s3_vfs_state {
    CHIMERA_S3_VFS_STATE_INIT,
    CHIMERA_S3_VFS_STATE_RECV,
    CHIMERA_S3_VFS_STATE_RECVED,
    CHIMERA_S3_VFS_STATE_SEND,
    CHIMERA_S3_VFS_STATE_SENT,
    CHIMERA_S3_VFS_STATE_COMPLETE,
};

enum chimera_s3_http_state {
    CHIMERA_S3_HTTP_STATE_INIT,
    CHIMERA_S3_HTTP_STATE_RECVED,
    CHIMERA_S3_HTTP_STATE_SEND,
    CHIMERA_S3_HTTP_STATE_COMPLETE,
};

#define CHIMERA_S3_IOV_MAX       256

#define CHIMERA_S3_UPLOAD_ID_LEN 32

/* S3 keys are limited to 1024 bytes; delimiters are normally a single
 * character but we allow a small amount of slack. */
#define CHIMERA_S3_KEY_MAX       1024
#define CHIMERA_S3_DELIM_MAX     16

/* A single collected listing element: either an object (is_prefix == 0) or a
 * rolled-up CommonPrefix (is_prefix == 1). Objects carry the attributes needed
 * to render <Contents>; the key string is heap-allocated and NUL-terminated. */
struct chimera_s3_list_entry {
    int             is_prefix;
    uint64_t        size;
    uint64_t        etag[2];
    struct timespec mtime;
    char           *key;
};

struct chimera_server_s3_thread;
struct chimera_s3_multipart_table;
struct chimera_s3_multipart_upload;
struct chimera_s3_tagging_ctx;

/* One <Object> entry from a DeleteObjects (POST /bucket?delete) request.
 * key points into the accumulated request body (unescaped in place). */
struct chimera_s3_delete_entry {
    char       *key;
    int         key_len;
    int         deleted;     /* 1 once the key is resolved as removed/absent */
    const char *err_code;    /* NULL on success, else S3 error code string   */
    const char *err_msg;
};

struct chimera_s3_config {
    int      port;
    uint64_t io_size;
    /* Identity used for an access key configured without one.  Defaults to
     * nobody (65534) rather than root, so a key that was never bound to a user
     * gets no privilege by omission. */
    uint32_t anon_uid;
    uint32_t anon_gid;
};

struct chimera_s3_io {
    struct chimera_s3_request *request;
    int                        niov;
    struct chimera_s3_io      *next;
    struct evpl_iovec          iov[CHIMERA_S3_IOV_MAX];
};

struct chimera_s3_request {
    enum chimera_s3_status           status;
    enum chimera_s3_vfs_state        vfs_state;
    enum chimera_s3_http_state       http_state;
    /*
     * The POSIX identity the authenticated access key acts as.  Every VFS
     * operation this request issues runs under it, so the store's ownership
     * and mode bits are what keep one key out of another key's objects -- the
     * S3 layer itself performs no authorization.  Stamped from the matched
     * credential once authentication succeeds, and never from a server-wide
     * identity: sharing one privileged credential across requests is what made
     * every valid key root over every bucket.
     */
    struct chimera_vfs_cred          cred;
    /* S3 identity of the authenticated requester, reported as the <Owner> in
    * an ACL reply.  Copied out of the credential cache at authentication time
    * because the cached entry is only valid inside that RCU read section. */
    char                             owner_id[CHIMERA_S3_CANON_ID_MAX];
    char                             owner_display[CHIMERA_S3_DISPLAY_MAX];
    const char                      *bucket_name;
    int                              bucket_namelen;
    int                              bucket_fhlen;
    int                              io_pending;
    /*
     * How many things still hold this request.  One reference belongs to the
     * HTTP layer, from the moment the request is dispatched until libevpl
     * reports it over -- with a response (RESPONSE_COMPLETE) or without one
     * (FAILED).  Every asynchronous handoff to the VFS takes another, because
     * its completion callback dereferences the request and there is nothing
     * else keeping it alive: a connection dropped mid-operation ends the HTTP
     * reference while the VFS work is still in flight.
     *
     * The request is torn down when the count reaches zero, which is the only
     * point at which nothing can reach it.
     */
    int                              refcount;
    /*
     * The peer is gone: http_request has been freed by libevpl and is NULL, so
     * nothing may be written in reply.  The VFS work already in flight still
     * runs to completion -- there is no way to recall it -- and unwinds
     * through the same paths, which check this before touching the response.
     */
    int                              abandoned;
    int                              name_len;
    int                              path_len;
    int                              is_list;
    int                              has_uploads;
    int                              has_upload_id;
    int                              has_delete;
    int                              has_versions;
    int                              has_part_number;
    /* ?tagging subresource (object/bucket tag get/put/delete). */
    int                              has_tagging;
    /* ?acl subresource (Get/PutObjectAcl, Get/PutBucketAcl). */
    int                              has_acl;
    /* The x-amz-acl canned ACL parsed into an id; CHIMERA_S3_CANNED_NONE when
     * the header is absent, in which case the backend default mode stands. */
    int                              canned_acl;
    /* Set when the request targets a bucket itself (Create/Delete/Head/List
     * bucket, ListBuckets) rather than an object; tells the body notifier to
     * drain any request body instead of treating it as object data. */
    int                              op_bucket;
    int                              has_attributes;
    int                              chunked;
    /* Set once the response status/headers have been dispatched, so the
     * several paths that can observe a request ready to send only emit it
     * once (see s3_server_respond). */
    int                              responded;
    int                              have_content_type;
    int                              query_upload_idlen;
    int                              query_part_number;
    char                             query_upload_id[CHIMERA_S3_UPLOAD_ID_LEN + 1];
    int64_t                          file_offset;
    int64_t                          file_cur_offset;
    int64_t                          file_length;
    int64_t                          file_real_length;
    int64_t                          file_left;
    uint64_t                         elapsed;
    uint64_t                         etag[2];
    const char                      *path;
    const char                      *name;
    struct evpl_http_request        *http_request;
    struct chimera_server_s3_thread *thread;
    struct chimera_vfs_open_handle  *dir_handle;
    struct chimera_vfs_open_handle  *file_handle;
    struct timespec                  start_time;
    struct timespec                  end_time;
    struct otel_span                 otel;
    struct chimera_s3_request       *prev;
    struct chimera_s3_request       *next;
    struct chimera_vfs_attrs         set_attr;
    struct s3_chunk_decoder          chunk;
    uint8_t                          bucket_fh[CHIMERA_VFS_FH_SIZE];

    union {
        struct {
            int                      tmp_name_len;
            /* The publish (rename/link of the staged file into place) must
             * fire exactly once, when the body is fully written AND the
             * metadata xattr chain is done.  Body drain and the xattr chain
             * run concurrently on async backends, so both the last write
             * completion and the metadata-done callback can observe the
             * finished state: meta_pending gates the publish until the
             * xattrs land and published latches the first fire. */
            int                      meta_pending;
            int                      published;
            struct chimera_vfs_attrs set_attr;
            char                     tmp_name[64];
        } put;

        struct {
            int                           list_type;     /* 1 (V1) or 2 (V2) */
            int                           max_keys;
            int                           encoding_url;  /* encoding-type=url */
            int                           versions;      /* emit ListVersionsResult */
            int                           prefix_len;
            int                           delimiter_len;
            int                           has_start;     /* skip past 'start' (exclusive) */
            int                           start_len;
            int                           marker_len;     /* V1 marker echo */
            int                           ctoken_len;     /* V2 continuation-token echo */
            int                           startafter_len; /* V2 start-after echo */
            int                           fetch_owner;    /* V2 fetch-owner=true */
            int                           n_entries;
            int                           cap_entries;
            struct chimera_s3_list_entry *entries;
            char                          delimiter[CHIMERA_S3_DELIM_MAX];
            char                          prefix[CHIMERA_S3_KEY_MAX];
            char                          start[CHIMERA_S3_KEY_MAX];
            char                          marker[CHIMERA_S3_KEY_MAX];
            char                          ctoken[CHIMERA_S3_KEY_MAX];
            char                          startafter[CHIMERA_S3_KEY_MAX];
        } list;

        struct {
            int                                 tmp_name_len;
            int                                 part_number;
            int                                 upload_idlen;
            struct chimera_s3_multipart_upload *upload;
            char                               *rp;
            struct evpl_iovec                   response;
            /* CompleteMultipartUpload request body accumulator. */
            char                               *body_buf;
            int                                 body_len;
            int                                 body_cap;
            /* ListParts / ListMultipartUploads pagination parameters,
             * captured from the query string. */
            int                                 max_parts;        /* ListParts max-parts (default 1000) */
            int                                 part_number_marker; /* ListParts part-number-marker */
            int                                 max_uploads;      /* ListMultipartUploads max-uploads (default 1000) */
            int                                 key_marker_len;
            int                                 upload_id_marker_len;
            char                                key_marker[CHIMERA_S3_KEY_MAX];
            char                                upload_id_marker[CHIMERA_S3_UPLOAD_ID_LEN + 1];
            /* UploadPartCopy: source byte range from x-amz-copy-source-range. */
            int                                 is_copy;        /* 1 => emit CopyPartResult */
            int                                 has_copy_range;
            int64_t                             copy_range_first;
            int64_t                             copy_range_last;
            char                                upload_id[CHIMERA_S3_UPLOAD_ID_LEN + 1];
            char                                tmp_name[64];
            char                                path_buf[1024];
        } multipart;

        struct {
            /* Request body (the <Delete> document) accumulator. */
            char                           *body_buf;
            int                             body_len;
            int                             body_cap;
            int                             quiet;
            /* Parsed object keys and the sequential delete cursor. */
            struct chimera_s3_delete_entry *entries;
            int                             n_keys;
            int                             cur;
            /* Trampoline flags so inline VFS completions don't recurse. */
            int                             pending;
            int                             synchronous;
            /* Working state for the key currently being removed. */
            const char                     *cur_name;
            int                             cur_name_len;
            /* Response (<DeleteResult>) builder. */
            char                           *resp_buf;
            int                             resp_len;
            int                             resp_cap;
        } del;
    };

    /* Tagging working state. Heap-allocated only for ?tagging requests or PUTs
     * carrying an x-amz-tagging header; kept outside the union above because a
     * multipart-completion PUT needs both the multipart and tagging state. */
    struct chimera_s3_tagging_ctx *tagging;
};

struct chimera_server_s3_thread {
    struct evpl                     *evpl;
    struct evpl_http_agent          *agent;
    struct evpl_http_server         *server;
    struct chimera_server_s3_shared *shared;
    struct chimera_vfs_thread       *vfs;
    struct chimera_s3_request       *free_requests;
    struct chimera_s3_io            *free_ios;
};

struct chimera_server_s3_shared {
    struct chimera_s3_config          *config;
    struct s3_bucket_map              *bucket_map;
    struct chimera_s3_cred_cache      *cred_cache;
    struct chimera_s3_multipart_table *multipart_table;
    struct evpl_endpoint              *endpoint;
    struct evpl_listener              *listener;
    enum evpl_protocol_id              tcp_protocol;

    /* The flavor tcp_protocol was resolved from; the endpoint has to be
     * built from it too, since the in-process transport is named rather
     * than bound. */
    enum chimera_tcp_flavor            tcp_flavor;
    uint32_t                           root_fh_len;
    uint8_t                            root_fh[CHIMERA_VFS_FH_SIZE];
    /* VFS path (relative to root_fh) under which dynamically created buckets
     * are materialized as directories. Empty if runtime bucket creation is
     * disabled (no bucket root configured). */
    int                                bucket_root_pathlen;
    char                               bucket_root_path[256];
};

/*
 * Take a reference for an asynchronous handoff.
 *
 * Every VFS call whose completion callback dereferences the request needs one,
 * and so does every context object that outlives the call which created it.
 * Without it, a connection dropped while the operation is in flight ends the
 * HTTP layer's reference and the callback lands on a recycled request.
 */
static inline void
chimera_s3_request_get(struct chimera_s3_request *request)
{
    request->refcount++;
} /* chimera_s3_request_get */

/*
 * Drop a reference, tearing the request down at zero: releasing any VFS handle
 * it still holds, ending its span, and returning it to the thread's free list.
 * Nothing may touch the request afterwards.
 */
void
chimera_s3_request_put(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

/* Drop a reference without the caller needing the thread to hand. */
static inline void
chimera_s3_request_drop(struct chimera_s3_request *request)
{
    chimera_s3_request_put(request->thread, request);
} /* chimera_s3_request_drop */

static inline void
chimera_s3_request_unref(struct chimera_s3_request **requestp)
{
    struct chimera_s3_request *request = *requestp;

    chimera_s3_request_put(request->thread, request);
} /* chimera_s3_request_unref */

/*
 * Hold the reference an asynchronous handoff took, and drop it when the
 * callback returns -- by whichever of its exits it takes.
 *
 * Declared first in the body so that its cleanup runs last, after everything
 * the callback does with the request.  The alternative is a put before every
 * return in every completion callback, of which there are dozens: the one that
 * gets missed is a leak, and the one that gets duplicated is the
 * use-after-free this exists to prevent.  cleanup is a GNU extension, which
 * both compilers chimera builds with have long supported, and it makes neither
 * mistake expressible.
 *
 * The variable exists only for its cleanup, so it is never read; `unused`
 * keeps that from tripping -Wunused-variable, which Apple clang raises for
 * cleanup variables even though gcc does not.
 */
#define CHIMERA_S3_HOLD_REQUEST(pd) \
        struct chimera_s3_request *_s3_held \
        __attribute__((cleanup(chimera_s3_request_unref), unused)) = (pd)

static inline struct chimera_s3_io *
chimera_s3_io_alloc(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_s3_io *io = thread->free_ios;

    if (io) {
        LL_DELETE(thread->free_ios, io);
    } else {
        io = calloc(1, sizeof(*io));
    }

    io->request = request;

    /* The io outlives the call that created it: its completion callback
     * dereferences the request, so it holds a reference of its own. */
    chimera_s3_request_get(request);

    return io;
} /* chimera_s3_io_alloc */

static inline void
chimera_s3_io_free(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_io            *io)
{
    struct chimera_s3_request *request = io->request;

    LL_PREPEND(thread->free_ios, io);

    chimera_s3_request_put(thread, request);
} /* chimera_s3_io_free */

static inline int
chimera_s3_format_date(
    char                  *buf,
    size_t                 len,
    const struct timespec *ts)
{
    struct tm tm;

    gmtime_r(&ts->tv_sec, &tm);

    int       ret = strftime(buf, len, "%Y-%m-%dT%H:%M:%S", &tm);
    if (ret > 0) {
        ret += snprintf(buf + ret, len - ret, ".%03ldZ", ts->tv_nsec / 1000000);
    }
    return ret;

} /* chimera_s3_format_date */

/*
 * Attach an HTTP Last-Modified header from an object's mtime. The AWS CLI
 * (unlike boto3) requires this header on GET/HEAD responses; without it
 * `aws s3 cp s3://...` aborts with a KeyError on 'LastModified'.
 */
static inline void
chimera_s3_attach_last_modified(
    struct evpl_http_request       *request,
    const struct chimera_vfs_attrs *attr)
{
    char      buf[64];
    struct tm tm;

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {
        return;
    }

    if (!request) {
        return;
    }

    gmtime_r(&attr->va_mtime.tv_sec, &tm);
    strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    evpl_http_request_add_header(request, "Last-Modified", buf);
} /* chimera_s3_attach_last_modified */

void
s3_server_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

/*
 * Add to the response, unless there is nobody to send it to.
 *
 * A connection dropped mid-operation ends the request's HTTP side while its
 * VFS work is still in flight, and that work unwinds through the same response
 * builders it always does.  They go through these so that "the peer is gone"
 * is handled once, here, rather than at every place that writes a header or a
 * payload -- and so that content already staged for a response nobody will
 * read is released rather than leaked.
 */
static inline void
chimera_s3_response_add_header(
    struct chimera_s3_request *request,
    const char                *name,
    const char                *value)
{
    if (request->abandoned) {
        return;
    }

    evpl_http_request_add_header(request->http_request, name, value);
} /* chimera_s3_response_add_header */

static inline void
chimera_s3_response_add_datav(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    struct evpl_iovec         *iov,
    int                        niov)
{
    if (request->abandoned) {
        evpl_iovecs_release(evpl, iov, niov);
        return;
    }

    evpl_http_request_add_datav(request->http_request, iov, niov);
} /* chimera_s3_response_add_datav */


#define chimera_s3_debug(...) chimera_debug("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_info(...)  chimera_info("s3", \
                                           __FILE__, \
                                           __LINE__, \
                                           __VA_ARGS__)
#define chimera_s3_error(...) chimera_error("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_fatal(...) chimera_fatal("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_abort(...) chimera_abort("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)

#define chimera_s3_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "s3", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

#define chimera_s3_abort_if(cond, ...) \
        chimera_abort_if(cond, "s3", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)
