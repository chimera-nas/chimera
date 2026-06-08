// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <time.h>
#include <utlist.h>

#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "s3_status.h"
#include "vfs/vfs.h"
#include "vfs/vfs_cred.h"
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

struct chimera_server_s3_thread;
struct chimera_s3_multipart_table;
struct chimera_s3_multipart_upload;

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
    const char                      *bucket_name;
    int                              bucket_namelen;
    int                              bucket_fhlen;
    int                              io_pending;
    int                              name_len;
    int                              path_len;
    int                              is_list;
    int                              has_uploads;
    int                              has_upload_id;
    int                              has_delete;
    int                              has_versions;
    int                              has_part_number;
    /* Set when the request targets a bucket itself (Create/Delete/Head/List
     * bucket, ListBuckets) rather than an object; tells the body notifier to
     * drain any request body instead of treating it as object data. */
    int                              op_bucket;
    int                              has_attributes;
    int                              chunked;
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
    struct chimera_s3_request       *prev;
    struct chimera_s3_request       *next;
    struct chimera_vfs_attrs         set_attr;
    struct s3_chunk_decoder          chunk;
    uint8_t                          bucket_fh[CHIMERA_VFS_FH_SIZE];

    union {
        struct {
            int                      tmp_name_len;
            struct chimera_vfs_attrs set_attr;
            char                     tmp_name[64];
        } put;

        struct {
            int               prefix_len;
            int               base_path_len;
            int               filter_len;
            int               max_keys;
            int               versions;   /* emit ListVersionsResult, not ListBucketResult */
            char             *rp;
            struct evpl_iovec response;
            uint8_t           root_fh[CHIMERA_VFS_FH_SIZE];
            char              prefix[256];
            char              base_path[256];
            char              filter[256];
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
    struct chimera_vfs_cred            cred;
    uint32_t                           root_fh_len;
    uint8_t                            root_fh[CHIMERA_VFS_FH_SIZE];
    /* VFS path (relative to root_fh) under which dynamically created buckets
     * are materialized as directories. Empty if runtime bucket creation is
     * disabled (no bucket root configured). */
    int                                bucket_root_pathlen;
    char                               bucket_root_path[256];
};

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

    return io;
} /* chimera_s3_io_alloc */

static inline void
chimera_s3_io_free(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_io            *io)
{
    LL_PREPEND(thread->free_ios, io);
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

    gmtime_r(&attr->va_mtime.tv_sec, &tm);
    strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    evpl_http_request_add_header(request, "Last-Modified", buf);
} /* chimera_s3_attach_last_modified */

void
s3_server_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

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
