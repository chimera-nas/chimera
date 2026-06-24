// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <utlist.h>
#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/protocol.h"
#include "server/server.h"
#include "common/macros.h"
#include "common/misc.h"
#include "s3_internal.h"
#include "s3_status.h"
#include "s3_procs.h"
#include "s3_dump.h"
#include "s3_bucket_map.h"
#include "s3_auth.h"
#include "s3_multipart.h"
#include "s3_tagging.h"
#include "s3.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static inline int
chimera_s3_hexval(int c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    } else if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    } else if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
} /* chimera_s3_hexval */

/* Percent-decode an S3 query-string value (%XX escapes only; '+' is left
 * intact because S3, unlike form encoding, does not treat it as a space).
 * Writes a NUL-terminated result and returns its length. */
static inline int
chimera_s3_pct_decode(
    char       *dst,
    int         dstcap,
    const char *src,
    int         srclen)
{
    int o = 0, i = 0;

    while (i < srclen && o < dstcap - 1) {
        int c = (unsigned char) src[i];

        if (c == '%' && i + 2 < srclen) {
            int hi = chimera_s3_hexval((unsigned char) src[i + 1]);
            int lo = chimera_s3_hexval((unsigned char) src[i + 2]);
            if (hi >= 0 && lo >= 0) {
                dst[o++] = (char) ((hi << 4) | lo);
                i       += 3;
                continue;
            }
        }
        dst[o++] = (char) c;
        i++;
    }

    dst[o] = '\0';
    return o;
} /* chimera_s3_pct_decode */

/* expect range_str in the form: bytes=1000-1599 */
static inline int
chimera_s3_parse_range(
    const char *range_str,
    int64_t    *offset,
    int64_t    *length)
{
    const char *dash;
    char       *end;

    if (strncmp(range_str, "bytes=", 6) != 0) {
        return -1;
    }

    range_str += 6;

    dash = strchr(range_str, '-');

    if (!dash) {
        return -1;
    }

    if (range_str == dash) {
        *offset = -1;
        *length = strtoul(dash + 1, &end, 10);
    } else if (*(dash + 1) == '\0') {
        *offset = strtoul(range_str, &end, 10);
        *length = -1;
    } else {
        *offset = strtoul(range_str, &end, 10);
        *length = strtoul(dash + 1, &end, 10) - *offset + 1;
    }

    return 0;
} /* chimera_s3_parse_range */

static inline struct chimera_s3_request *
chimera_s3_request_alloc(struct chimera_server_s3_thread *thread)
{
    struct chimera_s3_request *request;

    request = thread->free_requests;

    if (request) {
        DL_DELETE(thread->free_requests, request);
    } else {
        request         = calloc(1, sizeof(*request));
        request->thread = thread;
    }

    return request;
} /* chimera_s3_request_alloc */

static inline void
chimera_s3_request_free(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    DL_PREPEND(thread->free_requests, request);
} /* chimera_s3_request_free */

void
s3_server_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct evpl_iovec iov;
    char             *error_response;
    int               error_response_len;
    int               http_code;
    char              range_header[128];
    char              date_ts[64];
    time_t            now = time(NULL);

    /* The response headers/status are emitted exactly once. Several paths can
     * race to call this for the same request: for synchronous VFS backends
     * (e.g. memfs) the VFS completion callback runs inline, so both it and the
     * HTTP RECEIVE_COMPLETE notification observe the request ready to send.
     * Responding twice duplicates the response headers and desyncs the
     * connection, so dispatch the status line + headers only on the first
     * call. (GET streams its body via later WANT_DATA notifications, which is
     * why we cannot key this off vfs_state.) */
    if (request->responded) {
        return;
    }
    request->responded = 1;

    strftime(date_ts, sizeof(date_ts), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&now));

    evpl_http_request_add_header(request->http_request, "Date", date_ts);

    evpl_http_request_add_header(request->http_request, "Server", "chimera-s3");

    if (request->is_list) {
        evpl_http_request_add_header(request->http_request, "Content-Type", "application/xml");
    } else if (!request->have_content_type) {
        /* GET/HEAD attaches the object's stored Content-Type (if any) before
         * dispatch; only fall back to the generic type when none was stored. */
        evpl_http_request_add_header(request->http_request, "Content-Type", "application/octet-stream");
    }
    evpl_http_request_add_header(request->http_request, "Accept-Ranges", "bytes");

    request->http_state = CHIMERA_S3_HTTP_STATE_SEND;

    if (request->status == CHIMERA_S3_STATUS_OK) {

        if (request->file_offset != 0 || request->file_length != request->file_real_length) {

            snprintf(range_header, sizeof(range_header), "bytes %ld-%ld/%ld",
                     request->file_offset, request->file_offset + request->file_length - 1,
                     request->file_real_length);

            evpl_http_request_add_header(request->http_request, "Content-Range", range_header);

            evpl_http_server_set_response_length(request->http_request, request->file_length);
            evpl_http_server_dispatch_default(request->http_request, 206);
        } else {
            evpl_http_server_set_response_length(request->http_request, request->file_length);
            evpl_http_server_dispatch_default(request->http_request, 200);
        }
    } else if (request->status == CHIMERA_S3_STATUS_NO_CONTENT) {
        evpl_http_server_set_response_length(request->http_request, 0);
        evpl_http_server_dispatch_default(request->http_request, 204);
    } else {
        if (request->status == CHIMERA_S3_STATUS_INVALID_RANGE) {
            /* S3 echoes the object size on a 416 so the client can re-request a
             * satisfiable range. */
            snprintf(range_header, sizeof(range_header), "bytes */%ld",
                     request->file_real_length);
            evpl_http_request_add_header(request->http_request, "Content-Range", range_header);
        }

        evpl_iovec_alloc(evpl, 1024, 0, 1, 0, &iov);

        error_response = (char *) evpl_iovec_data(&iov);
        http_code      = chimera_s3_prepare_error_response(request, error_response, &error_response_len);

        evpl_iovec_set_length(&iov, error_response_len);

        evpl_http_request_add_datav(request->http_request, &iov, 1);

        evpl_http_server_set_response_length(request->http_request, error_response_len);
        evpl_http_server_dispatch_default(request->http_request, http_code);
    }
} /* s3_server_respond */

static void
s3_server_drain_body(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct evpl_iovec iov[CHIMERA_S3_IOV_MAX];
    uint64_t          avail;
    int               niov;

    while ((avail = evpl_http_request_get_data_avail(request->http_request)) > 0) {
        niov = evpl_http_request_get_datav(evpl, request->http_request,
                                           iov, avail);
        evpl_iovecs_release(evpl, iov, niov);
    }
} /* s3_server_drain_body */

static void
s3_server_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  notify_type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *notify_data,
    void                       *private_data)
{
    struct chimera_s3_request       *s3_request = notify_data;
    struct chimera_server_s3_thread *thread     = private_data;
    int                              is_upload_part;
    int                              is_complete_mpu;
    int                              is_delete_objects;

    is_upload_part  = s3_request->has_upload_id && s3_request->has_part_number;
    is_complete_mpu = (request_type == EVPL_HTTP_REQUEST_TYPE_POST &&
                       s3_request->has_upload_id);
    is_delete_objects = (request_type == EVPL_HTTP_REQUEST_TYPE_POST &&
                         s3_request->has_delete);

    switch (notify_type) {
        case EVPL_HTTP_NOTIFY_RECEIVE_DATA:
            if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                s3_request->has_tagging) {
                /* PutObjectTagging: accumulate the <Tagging> body. */
                chimera_s3_put_tagging_recv(evpl, s3_request);
            } else if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                       s3_request->vfs_state == CHIMERA_S3_VFS_STATE_RECV) {
                if (is_upload_part) {
                    chimera_s3_upload_part_recv(evpl, s3_request);
                } else {
                    chimera_s3_put_recv(evpl, s3_request);
                }
            } else if (is_complete_mpu) {
                /* Accumulate the client's part manifest. */
                chimera_s3_complete_multipart_upload_recv(evpl, s3_request);
            } else if (is_delete_objects) {
                /* Accumulate the <Delete> document. */
                chimera_s3_delete_objects_recv(evpl, s3_request);
            } else if (request_type == EVPL_HTTP_REQUEST_TYPE_POST) {
                /* CreateMultipartUpload: body is empty in practice. */
                s3_server_drain_body(evpl, s3_request);
            } else if (s3_request->op_bucket) {
                /* Bucket-level request (e.g. CreateBucket's optional
                 * CreateBucketConfiguration body): discard the body. */
                s3_server_drain_body(evpl, s3_request);
            }
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE:

            s3_request->http_state = CHIMERA_S3_HTTP_STATE_RECVED;

            if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                s3_request->has_tagging) {
                /* PutObjectTagging: drain any trailing body, then parse + store
                 * once the bucket FH is resolved. If the bucket lookup is still
                 * in flight, its callback drives body_done instead. */
                chimera_s3_put_tagging_recv(evpl, s3_request);
                if (s3_request->bucket_fhlen != 0 &&
                    s3_request->vfs_state != CHIMERA_S3_VFS_STATE_COMPLETE) {
                    chimera_s3_put_tagging_body_done(evpl, s3_request);
                }
            } else if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                       s3_request->vfs_state == CHIMERA_S3_VFS_STATE_RECV) {
                if (is_upload_part) {
                    chimera_s3_upload_part_recv(evpl, s3_request);
                } else {
                    chimera_s3_put_recv(evpl, s3_request);
                }
            } else if (is_complete_mpu) {
                chimera_s3_complete_multipart_upload_recv(evpl, s3_request);
                /* Body fully in hand: parse + validate + assemble. */
                chimera_s3_complete_multipart_upload_body_done(evpl, s3_request);
            } else if (is_delete_objects) {
                chimera_s3_delete_objects_recv(evpl, s3_request);
                /* Body fully in hand: parse keys + remove them. */
                chimera_s3_delete_objects_body_done(evpl, s3_request);
            } else if (request_type == EVPL_HTTP_REQUEST_TYPE_POST) {
                s3_server_drain_body(evpl, s3_request);
            }

            if (s3_request->vfs_state == CHIMERA_S3_VFS_STATE_SEND ||
                s3_request->vfs_state == CHIMERA_S3_VFS_STATE_COMPLETE) {
                s3_server_respond(evpl, s3_request);
            }
            break;
        case EVPL_HTTP_NOTIFY_WANT_DATA:


            s3_request->http_state = CHIMERA_S3_HTTP_STATE_SEND;

            if (request_type == EVPL_HTTP_REQUEST_TYPE_GET &&
                s3_request->vfs_state == CHIMERA_S3_VFS_STATE_SEND) {
                chimera_s3_get_send(evpl, s3_request);
            }
            break;
        case EVPL_HTTP_NOTIFY_RESPONSE_COMPLETE:
            clock_gettime(CLOCK_MONOTONIC, &s3_request->end_time);

            s3_request->elapsed = chimera_get_elapsed_ns(&s3_request->end_time, &s3_request->start_time);

            chimera_s3_dump_response(s3_request);

            chimera_s3_tagging_request_cleanup(s3_request);

            chimera_s3_request_free(thread, s3_request);
            break;
        default:
            /* no action required */

            break;
    } /* switch */
} /* chimera_metrics_notify */

static void
chimera_s3_dispatch_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *s3_request = private_data;
    struct chimera_server_s3_thread *thread     = s3_request->thread;
    struct evpl                     *evpl       = thread->evpl;

    if (error_code) {
        s3_request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    memcpy(s3_request->bucket_fh, attr->va_fh, attr->va_fh_len);
    s3_request->bucket_fhlen = attr->va_fh_len;

    /* ?tagging subresource (object or bucket): route to the tagging handlers
     * before the normal object/bucket method routing. */
    if (s3_request->has_tagging) {
        switch (evpl_http_request_type(s3_request->http_request)) {
            case EVPL_HTTP_REQUEST_TYPE_GET:
                chimera_s3_get_tagging(evpl, thread, s3_request);
                break;
            case EVPL_HTTP_REQUEST_TYPE_DELETE:
                chimera_s3_delete_tagging(evpl, thread, s3_request);
                break;
            case EVPL_HTTP_REQUEST_TYPE_PUT:
                /* PutObjectTagging: the <Tagging> body is parsed once fully
                 * received (chimera_s3_put_tagging_body_done), which then
                 * invokes chimera_s3_put_tagging. If the body already arrived
                 * before this lookup completed, drive it now. */
                if (s3_request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
                    chimera_s3_put_tagging_body_done(evpl, s3_request);
                }
                break;
            default:
                s3_request->status    = CHIMERA_S3_STATUS_METHOD_NOT_ALLOWED;
                s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
                break;
        } /* switch */
        return;
    }

    switch (evpl_http_request_type(s3_request->http_request)) {
        case EVPL_HTTP_REQUEST_TYPE_HEAD:
            chimera_s3_get(evpl, thread, s3_request);
            break;
        case EVPL_HTTP_REQUEST_TYPE_GET:
            if (s3_request->has_uploads && s3_request->path_len == 0) {
                chimera_s3_list_multipart_uploads(evpl, thread, s3_request);
            } else if (s3_request->has_upload_id) {
                chimera_s3_list_parts(evpl, thread, s3_request);
            } else if (s3_request->has_attributes) {
                chimera_s3_get_object_attributes(evpl, thread, s3_request);
            } else if (s3_request->is_list) {
                chimera_s3_list(evpl, thread, s3_request);
            } else {
                chimera_s3_get(evpl, thread, s3_request);
            }
            break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:
        {
            const char *copy_source = evpl_http_request_header(
                s3_request->http_request, "x-amz-copy-source");

            if (s3_request->has_upload_id && s3_request->has_part_number) {
                if (copy_source) {
                    /* UploadPartCopy: copy a byte range of an existing object
                     * into this in-progress multipart upload as part N. */
                    chimera_s3_upload_part_copy(evpl, thread, s3_request);
                } else {
                    chimera_s3_upload_part(evpl, thread, s3_request);
                }
            } else if (copy_source) {
                chimera_s3_copy(evpl, thread, s3_request);
            } else {
                chimera_s3_put(evpl, thread, s3_request);
            }
            break;
        }
        case EVPL_HTTP_REQUEST_TYPE_POST:
            if (s3_request->has_uploads) {
                chimera_s3_create_multipart_upload(evpl, thread, s3_request);
            } else if (s3_request->has_upload_id) {
                chimera_s3_complete_multipart_upload(evpl, thread, s3_request);
            } else if (s3_request->has_delete) {
                chimera_s3_delete_objects(evpl, thread, s3_request);
            } else {
                s3_request->status    = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
                s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            }
            break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE:
            if (s3_request->has_upload_id) {
                chimera_s3_abort_multipart_upload(evpl, thread, s3_request);
            } else {
                chimera_s3_delete(evpl, thread, s3_request);
            }
            break;
        default:
            s3_request->status    = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            break;
    } /* switch */

} /* chimera_s3_dispatch_callback */

static void
s3_server_dispatch(
    struct evpl                 *evpl,
    struct evpl_http_agent      *agent,
    struct evpl_http_request    *request,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data,
    void                        *private_data)
{
    struct chimera_server_s3_thread *thread = private_data;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_request       *s3_request;
    struct s3_bucket                *bucket;
    const char                      *urlp, *slash, *dot, *host_header;
    int                              host_pathing = 0;
    const char                      *range_str;
    enum chimera_s3_auth_result      auth_result;

    s3_request = chimera_s3_request_alloc(thread);

    clock_gettime(CLOCK_MONOTONIC, &s3_request->start_time);

    s3_request->status             = CHIMERA_S3_STATUS_OK;
    s3_request->vfs_state          = CHIMERA_S3_VFS_STATE_INIT;
    s3_request->http_state         = CHIMERA_S3_HTTP_STATE_INIT;
    s3_request->io_pending         = 0;
    s3_request->is_list            = 0;
    s3_request->has_uploads        = 0;
    s3_request->has_upload_id      = 0;
    s3_request->has_delete         = 0;
    s3_request->has_versions       = 0;
    s3_request->has_part_number    = 0;
    s3_request->has_tagging        = 0;
    s3_request->tagging            = NULL;
    s3_request->bucket_fhlen       = 0;
    s3_request->op_bucket          = 0;
    s3_request->has_attributes     = 0;
    s3_request->chunked            = 0;
    s3_request->responded          = 0;
    s3_request->have_content_type  = 0;
    s3_request->query_upload_idlen = 0;
    s3_request->query_part_number  = 0;
    s3_request->http_request       = request;

    /* Requests are recycled through a per-thread free list (see
     * chimera_s3_request_alloc) and are NOT zeroed on reuse. The VFS open
     * handles are released by each handler's completion path, but several of
     * those paths drop the reference without clearing the pointer, so a
     * recycled request can carry a dangling file_handle / dir_handle from a
     * prior op. If a later handler's error path then releases that stale
     * pointer (e.g. CompleteMultipartUpload's create-dir error branch in
     * chimera_s3_complete_finish_common), and the VFS close thread has since
     * swept the now-idle cached handle, the release is a use-after-free.
     * Clear them here so a fresh request always starts with no held handles. */
    s3_request->file_handle = NULL;
    s3_request->dir_handle  = NULL;

    *notify_callback = s3_server_notify;
    *notify_data     = s3_request;

    /* Verify AWS authentication */
    auth_result = chimera_s3_auth_verify(shared->cred_cache, request);

    switch (auth_result) {
        case CHIMERA_S3_AUTH_OK:
            /* Authentication successful */
            break;
        case CHIMERA_S3_AUTH_NO_AUTH_HEADER:
            /* No auth header - authentication required */
            s3_request->status    = CHIMERA_S3_STATUS_MISSING_AUTH_HEADER;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
        case CHIMERA_S3_AUTH_UNKNOWN_ACCESS_KEY:
            s3_request->status    = CHIMERA_S3_STATUS_INVALID_ACCESS_KEY_ID;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
        case CHIMERA_S3_AUTH_SIGNATURE_MISMATCH:
            s3_request->status    = CHIMERA_S3_STATUS_SIGNATURE_MISMATCH;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
        default:
            s3_request->status    = CHIMERA_S3_STATUS_ACCESS_DENIED;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
    } /* switch */

    /* SigV4 streaming uploads (aws-chunked) carry the object body wrapped in
     * chunk framing rather than as raw bytes. The sentinel lives in
     * x-amz-content-sha256; flag the request so the PUT / UploadPart body
     * handlers de-chunk before writing to the VFS. */
    {
        const char *content_sha256 = evpl_http_request_header(request, "x-amz-content-sha256");

        if (content_sha256 && strncmp(content_sha256, "STREAMING-", 10) == 0) {
            s3_request->chunked = 1;
            s3_chunk_decoder_init(&s3_request->chunk);
        }
    }

    host_header = evpl_http_request_header(request, "Host");

    if (host_header) {
        const char *p       = host_header;
        int         has_dot = 0;
        int         is_ip   = 1;

        dot = NULL;

        while (*p) {
            if (*p == '.' && !has_dot) {
                has_dot = 1;
                dot     = p;
            } else if (!isdigit(*p) && *p != '.' && *p != ':') {
                is_ip = 0;
            }
            p++;
        }

        host_pathing = (has_dot && !is_ip);
    }

    if (host_pathing) {

        s3_request->bucket_name    = host_header;
        s3_request->bucket_namelen = dot - host_header;
        s3_request->path           = evpl_http_request_url(request, &s3_request->path_len);

        while (*s3_request->path == '/') {
            s3_request->path++;
        }

        s3_request->path_len = strlen(s3_request->path);

    } else {

        urlp = evpl_http_request_url(request, NULL);

        while (*urlp == '/') {
            urlp++;
        }

        slash = strchr(urlp, '/');

        if (slash) {
            s3_request->bucket_name    = urlp;
            s3_request->bucket_namelen = slash - urlp;

            while (*slash == '/') {
                slash++;
            }

            s3_request->path     = slash;
            s3_request->path_len = strlen(s3_request->path);
        } else {
            /* URL is /bucket or /bucket?query - no path after bucket */
            const char *query = strchr(urlp, '?');
            if (query) {
                s3_request->bucket_name    = urlp;
                s3_request->bucket_namelen = query - urlp;
                s3_request->path           = query;
                s3_request->path_len       = strlen(query);
            } else {
                s3_request->bucket_name    = urlp;
                s3_request->bucket_namelen = strlen(urlp);
                s3_request->path           = "";
                s3_request->path_len       = 0;
            }
        }
    }

    {
        const char *qmark = NULL;
        if (s3_request->path_len > 0) {
            qmark = memchr(s3_request->path, '?', s3_request->path_len);
        }
        if (qmark != NULL) {
            const char *key_start, *value_start;
            const char *p;
            int         key_len, value_len;
            /* Staging buffers; we don't know yet whether this is a LIST or a
             * multipart request, and they share a union. LIST values are
             * percent-decoded since clients (e.g. the AWS CLI) encode prefix,
             * delimiter and continuation tokens in the query string. */
            char        prefix_buf[CHIMERA_S3_KEY_MAX];
            char        delim_buf[CHIMERA_S3_DELIM_MAX];
            char        marker_buf[CHIMERA_S3_KEY_MAX];
            char        ctoken_buf[CHIMERA_S3_KEY_MAX];
            char        startafter_buf[CHIMERA_S3_KEY_MAX];
            int         prefix_len     = 0;
            int         delim_len      = 0;
            int         marker_len     = 0;
            int         ctoken_len     = 0;
            int         startafter_len = 0;
            int         max_keys       = 1000;
            int         list_type      = 1;
            int         encoding_url   = 0;
            int         fetch_owner    = 0;
            int         bad_max_keys   = 0;
            /* Multipart pagination parameters (ListParts /
             * ListMultipartUploads), captured here since the parse loop runs
             * before the request type is classified. */
            int         max_parts          = 1000;
            int         part_number_marker = 0;
            int         max_uploads        = 1000;
            char        upload_id_marker_buf[CHIMERA_S3_UPLOAD_ID_LEN + 1];
            int         upload_id_marker_len = 0;
            /* Set when the query carries a key we don't recognize as a list
             * parameter or handled subresource: an unimplemented bucket/object
             * subresource (acl/policy/tagging/cors/lifecycle/...). */
            int         saw_unknown = 0;

            upload_id_marker_buf[0] = '\0';

            p = qmark + 1;

            while (*p) {
                key_start = p;
                while (*p && *p != '=' && *p != '&') {
                    p++;
                }
                key_len = p - key_start;

                if (*p == '=') {
                    value_start = ++p;
                    while (*p && *p != '&') {
                        p++;
                    }
                    value_len = p - value_start;
                } else {
                    /* Bare key (no value): e.g. ?uploads */
                    value_start = p;
                    value_len   = 0;
                }

                if (key_len == 7 && memcmp(key_start, "uploads", 7) == 0) {
                    s3_request->has_uploads = 1;
                } else if (key_len == 8 && memcmp(key_start, "versions", 8) == 0) {
                    s3_request->has_versions = 1;
                } else if (key_len == 6 && memcmp(key_start, "delete", 6) == 0) {
                    s3_request->has_delete = 1;
                } else if (key_len == 8 && memcmp(key_start, "uploadId", 8) == 0) {
                    int copy = value_len;
                    if (copy > CHIMERA_S3_UPLOAD_ID_LEN) {
                        copy = CHIMERA_S3_UPLOAD_ID_LEN;
                    }
                    memcpy(s3_request->query_upload_id, value_start, copy);
                    s3_request->query_upload_id[copy] = '\0';
                    s3_request->query_upload_idlen    = copy;
                    s3_request->has_upload_id         = 1;
                } else if (key_len == 10 && memcmp(key_start, "partNumber", 10) == 0) {
                    s3_request->query_part_number = atoi(value_start);
                    s3_request->has_part_number   = 1;
                } else if (key_len == 9 && memcmp(key_start, "max-parts", 9) == 0) {
                    max_parts = atoi(value_start);
                } else if (key_len == 18 && memcmp(key_start, "part-number-marker", 18) == 0) {
                    part_number_marker = atoi(value_start);
                } else if (key_len == 11 && memcmp(key_start, "max-uploads", 11) == 0) {
                    max_uploads = atoi(value_start);
                } else if (key_len == 16 && memcmp(key_start, "upload-id-marker", 16) == 0) {
                    int copy = value_len;
                    if (copy > CHIMERA_S3_UPLOAD_ID_LEN) {
                        copy = CHIMERA_S3_UPLOAD_ID_LEN;
                    }
                    memcpy(upload_id_marker_buf, value_start, copy);
                    upload_id_marker_buf[copy] = '\0';
                    upload_id_marker_len       = copy;
                } else if (key_len == 7 && memcmp(key_start, "tagging", 7) == 0) {
                    s3_request->has_tagging = 1;
                } else if (key_len == 6 && memcmp(key_start, "prefix", 6) == 0) {
                    prefix_len = chimera_s3_pct_decode(prefix_buf, sizeof(prefix_buf),
                                                       value_start, value_len);
                } else if (key_len == 8 && memcmp(key_start, "max-keys", 8) == 0) {
                    /* max-keys must be a non-negative integer; a non-numeric or
                     * empty value is an InvalidArgument (400), not a silent 0. */
                    int mk_valid = (value_len > 0);
                    for (int mi = 0; mi < value_len; mi++) {
                        if (value_start[mi] < '0' || value_start[mi] > '9') {
                            mk_valid = 0;
                            break;
                        }
                    }
                    if (mk_valid) {
                        max_keys = atoi(value_start);
                    } else {
                        bad_max_keys = 1;
                    }
                } else if (key_len == 10 && memcmp(key_start, "attributes", 10) == 0) {
                    s3_request->has_attributes = 1;
                } else if (key_len == 9 && memcmp(key_start, "list-type", 9) == 0) {
                    list_type = atoi(value_start);
                } else if (key_len == 9 && memcmp(key_start, "delimiter", 9) == 0) {
                    delim_len = chimera_s3_pct_decode(delim_buf, sizeof(delim_buf),
                                                      value_start, value_len);
                } else if (key_len == 6 && memcmp(key_start, "marker", 6) == 0) {
                    marker_len = chimera_s3_pct_decode(marker_buf, sizeof(marker_buf),
                                                       value_start, value_len);
                } else if (key_len == 10 && memcmp(key_start, "key-marker", 10) == 0) {
                    /* ListObjectVersions pagination cursor; reuse the marker. */
                    marker_len = chimera_s3_pct_decode(marker_buf, sizeof(marker_buf),
                                                       value_start, value_len);
                } else if (key_len == 18 && memcmp(key_start, "continuation-token", 18) == 0) {
                    ctoken_len = chimera_s3_pct_decode(ctoken_buf, sizeof(ctoken_buf),
                                                       value_start, value_len);
                } else if (key_len == 11 && memcmp(key_start, "start-after", 11) == 0) {
                    startafter_len = chimera_s3_pct_decode(startafter_buf, sizeof(startafter_buf),
                                                           value_start, value_len);
                } else if (key_len == 13 && memcmp(key_start, "encoding-type", 13) == 0) {
                    if (value_len == 3 && strncasecmp(value_start, "url", 3) == 0) {
                        encoding_url = 1;
                    }
                } else if (key_len == 11 && memcmp(key_start, "fetch-owner", 11) == 0) {
                    /* V2: emit <Owner> in each <Contents> when true. */
                    fetch_owner = (value_len == 4 &&
                                   strncasecmp(value_start, "true", 4) == 0);
                } else if (key_len == 17 && memcmp(key_start, "version-id-marker", 17) == 0) {
                    /* Recognized list parameter we accept but don't act on. */
                } else {
                    /* Unrecognized query key: an unimplemented subresource. */
                    saw_unknown = 1;
                }

                if (*p == '&') {
                    p++;
                }
            }

            if (s3_request->has_uploads || s3_request->has_upload_id) {
                /* Multipart request: strip query from path so handlers see the
                 * raw key. Copy into request-owned storage so consumers can
                 * treat it as a null-terminated string. */
                int new_len = qmark - s3_request->path;
                if (new_len >= (int) sizeof(s3_request->multipart.path_buf)) {
                    new_len = sizeof(s3_request->multipart.path_buf) - 1;
                }
                memcpy(s3_request->multipart.path_buf, s3_request->path, new_len);
                s3_request->multipart.path_buf[new_len] = '\0';
                s3_request->path                        = s3_request->multipart.path_buf;
                s3_request->path_len                    = new_len;
                if (s3_request->has_upload_id) {
                    memcpy(s3_request->multipart.upload_id,
                           s3_request->query_upload_id,
                           s3_request->query_upload_idlen + 1);
                    s3_request->multipart.upload_idlen =
                        s3_request->query_upload_idlen;
                }
                if (s3_request->has_part_number) {
                    s3_request->multipart.part_number =
                        s3_request->query_part_number;
                }
                /* Pagination parameters for ListParts / ListMultipartUploads.
                 * Copy from the parse-loop staging vars into the multipart
                 * union now that we know this is a multipart request. */
                s3_request->multipart.max_parts =
                    (max_parts > 0) ? max_parts : 1000;
                s3_request->multipart.part_number_marker = part_number_marker;
                s3_request->multipart.max_uploads        =
                    (max_uploads > 0) ? max_uploads : 1000;

                if (marker_len > (int) sizeof(s3_request->multipart.key_marker) - 1) {
                    marker_len = sizeof(s3_request->multipart.key_marker) - 1;
                }
                memcpy(s3_request->multipart.key_marker, marker_buf, marker_len);
                s3_request->multipart.key_marker[marker_len] = '\0';
                s3_request->multipart.key_marker_len         = marker_len;

                memcpy(s3_request->multipart.upload_id_marker,
                       upload_id_marker_buf, upload_id_marker_len + 1);
                s3_request->multipart.upload_id_marker_len = upload_id_marker_len;
            } else if (s3_request->has_delete) {
                /* POST /bucket?delete: the keys to remove arrive in the
                 * request body, so there is no object key in the path.
                 * Initialize the body/response accumulators here, before any
                 * RECEIVE_DATA notification can fire. */
                s3_request->path         = "";
                s3_request->path_len     = 0;
                s3_request->del.body_buf = NULL;
                s3_request->del.body_len = 0;
                s3_request->del.body_cap = 0;
                s3_request->del.resp_buf = NULL;
                s3_request->del.resp_len = 0;
                s3_request->del.resp_cap = 0;
                s3_request->del.entries  = NULL;
                s3_request->del.n_keys   = 0;
                s3_request->del.cur      = 0;
                s3_request->del.quiet    = 0;
            } else if (qmark == s3_request->path) {
                if (s3_request->has_tagging) {
                    /* Bucket-level ?tagging: no object key, route to the
                     * tagging handlers (which operate on the bucket dir). */
                    s3_request->path     = "";
                    s3_request->path_len = 0;
                } else if (saw_unknown && !s3_request->has_versions) {
                    /* Path is "?<subresource>" we don't implement (acl, policy,
                     * tagging, cors, lifecycle, versioning, website, ...). These
                     * configure features that have no meaning for a filesystem-
                     * backed bucket shared with NFS/SMB, so we accept and ignore
                     * them with an empty 200 rather than (a) mis-listing or (b)
                     * routing a PUT to an object write that would create a junk
                     * "?subresource" key visible to every protocol. vfs_state
                     * COMPLETE short-circuits the routing below.
                     *
                     * NOTE: this means security-relevant configs (bucket policy,
                     * encryption, ACL) are silently NOT enforced. */
                    s3_request->status           = CHIMERA_S3_STATUS_OK;
                    s3_request->file_length      = 0;
                    s3_request->file_real_length = 0;
                    s3_request->file_offset      = 0;
                    s3_request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
                    s3_request->op_bucket        = 1;
                } else if (bad_max_keys) {
                    /* A non-numeric/empty max-keys is an InvalidArgument (400). */
                    s3_request->status           = CHIMERA_S3_STATUS_BAD_REQUEST;
                    s3_request->file_length      = 0;
                    s3_request->file_real_length = 0;
                    s3_request->file_offset      = 0;
                    s3_request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
                    s3_request->op_bucket        = 1;
                } else {
                    /* Bucket-level LIST query (ListObjects V1/V2 or
                     * ListObjectVersions). */
                    chimera_s3_list_setup(s3_request, list_type, max_keys, encoding_url,
                                          prefix_buf, prefix_len,
                                          delim_buf, delim_len,
                                          marker_buf, marker_len,
                                          ctoken_buf, ctoken_len,
                                          startafter_buf, startafter_len);
                    s3_request->list.versions    = s3_request->has_versions;
                    s3_request->list.fetch_owner = fetch_owner;
                }
            } else {
                /* Path has '?' mid-string but no recognized subresource: strip
                 * the query suffix and treat as a regular object request. */
                s3_request->path_len = qmark - s3_request->path;
            }
        }
    }

    range_str = evpl_http_request_header(request, "Range");

    if (range_str) {
        chimera_s3_parse_range(range_str, &s3_request->file_offset, &s3_request->file_length);
    } else {
        s3_request->file_offset = 0;
        s3_request->file_length = 0;
    }

    s3_request->file_left       = s3_request->file_length;
    s3_request->file_cur_offset = s3_request->file_offset;
    chimera_s3_dump_request(s3_request);

    {
        enum evpl_http_request_type method = evpl_http_request_type(request);

        /* The query parser already resolved this request (e.g. an unimplemented
         * subresource); don't run the bucket/object routing over it. */
        if (s3_request->vfs_state == CHIMERA_S3_VFS_STATE_COMPLETE) {
            return;
        }

        /* Service-level request (no bucket in the path): GET / == ListBuckets. */
        if (s3_request->bucket_namelen == 0) {
            s3_request->op_bucket = 1;
            if (method == EVPL_HTTP_REQUEST_TYPE_GET) {
                chimera_s3_list_buckets(evpl, thread, s3_request);
            } else {
                s3_request->status    = CHIMERA_S3_STATUS_METHOD_NOT_ALLOWED;
                s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            }
            return;
        }

        /* CreateBucket: PUT /bucket with no object key. The target bucket does
         * not exist yet, so this is handled before the bucket-map lookup. */
        if (method == EVPL_HTTP_REQUEST_TYPE_PUT && s3_request->path_len == 0 &&
            !s3_request->has_upload_id && !s3_request->has_tagging) {
            s3_request->op_bucket = 1;
            chimera_s3_create_bucket(evpl, thread, s3_request);
            return;
        }

        bucket = s3_bucket_map_get(shared->bucket_map, s3_request->bucket_name, s3_request->bucket_namelen);

        if (bucket == NULL) {
            s3_request->status    = CHIMERA_S3_STATUS_NO_SUCH_BUCKET;
            s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            s3_bucket_map_release(shared->bucket_map);
            return;
        }

        /* Bucket-level operations on an existing bucket: empty object key with
         * no object-style subresource query. */
        if (s3_request->path_len == 0 && !s3_request->has_uploads &&
            !s3_request->has_delete && !s3_request->has_upload_id &&
            !s3_request->is_list && !s3_request->has_tagging) {

            if (method == EVPL_HTTP_REQUEST_TYPE_DELETE) {
                s3_request->op_bucket = 1;
                /* Release the read lock before dispatching: DeleteBucket's VFS
                 * remove can complete inline and then takes the map write lock
                 * (s3_bucket_map_remove), which would self-deadlock if we still
                 * held the read lock on this thread. */
                s3_bucket_map_release(shared->bucket_map);
                chimera_s3_delete_bucket(evpl, thread, s3_request);
                return;
            } else if (method == EVPL_HTTP_REQUEST_TYPE_HEAD) {
                s3_request->op_bucket = 1;
                s3_bucket_map_release(shared->bucket_map);
                chimera_s3_head_bucket(evpl, thread, s3_request);
                return;
            } else if (method == EVPL_HTTP_REQUEST_TYPE_GET) {
                /* GET /bucket with no object key == ListObjects (v1). */
                s3_request->op_bucket = 1;
                chimera_s3_list_setup(s3_request, 1, 1000, 0,
                                      "", 0, "", 0, "", 0, "", 0, "", 0);
            }
        }

        chimera_vfs_lookup(thread->vfs,
                           &thread->shared->cred,
                           shared->root_fh,
                           shared->root_fh_len,
                           bucket->path,
                           strlen(bucket->path),
                           CHIMERA_VFS_ATTR_FH,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_dispatch_callback,
                           s3_request);

        s3_bucket_map_release(shared->bucket_map);
    }

} /* s3_server_dispatch */

SYMBOL_EXPORT void
chimera_s3_add_bucket(
    void       *s3_shared,
    const char *name,
    const char *path)
{
    struct chimera_server_s3_shared *shared = s3_shared;

    s3_bucket_map_put(shared->bucket_map, name, strlen(name), path);

} /* chimera_s3_add_bucket */


SYMBOL_EXPORT void
chimera_s3_set_bucket_root(
    void       *s3_shared,
    const char *path)
{
    struct chimera_server_s3_shared *shared = s3_shared;
    int                              len    = strlen(path);

    if (len >= (int) sizeof(shared->bucket_root_path)) {
        len = sizeof(shared->bucket_root_path) - 1;
    }

    memcpy(shared->bucket_root_path, path, len);
    shared->bucket_root_path[len] = '\0';
    shared->bucket_root_pathlen   = len;
} /* chimera_s3_set_bucket_root */

SYMBOL_EXPORT int
chimera_s3_remove_bucket(
    void       *s3_shared,
    const char *name)
{
    struct chimera_server_s3_shared *shared = s3_shared;

    return s3_bucket_map_remove(shared->bucket_map, name, strlen(name));
} /* chimera_s3_remove_bucket */

SYMBOL_EXPORT const struct s3_bucket *
chimera_s3_get_bucket(
    void       *s3_shared,
    const char *name)
{
    struct chimera_server_s3_shared *shared = s3_shared;

    return s3_bucket_map_get(shared->bucket_map, name, strlen(name));
} /* chimera_s3_get_bucket */

SYMBOL_EXPORT void
chimera_s3_release_bucket(void *s3_shared)
{
    struct chimera_server_s3_shared *shared = s3_shared;

    s3_bucket_map_release(shared->bucket_map);
} /* chimera_s3_release_bucket */

static int
s3_bucket_iterate_wrapper(
    const struct s3_bucket *bucket,
    void                   *data)
{
    void                       **ctx      = data;
    chimera_s3_bucket_iterate_cb callback = ctx[0];
    void                        *userdata = ctx[1];

    return callback(bucket, userdata);
} /* s3_bucket_iterate_wrapper */

SYMBOL_EXPORT void
chimera_s3_iterate_buckets(
    void                        *s3_shared,
    chimera_s3_bucket_iterate_cb callback,
    void                        *data)
{
    struct chimera_server_s3_shared *shared = s3_shared;
    void                            *ctx[2];

    ctx[0] = (void *) callback;
    ctx[1] = data;

    s3_bucket_map_iterate(shared->bucket_map, s3_bucket_iterate_wrapper, ctx);
} /* chimera_s3_iterate_buckets */

SYMBOL_EXPORT const char *
chimera_s3_bucket_get_name(const struct s3_bucket *bucket)
{
    return bucket->name;
} /* chimera_s3_bucket_get_name */

SYMBOL_EXPORT const char *
chimera_s3_bucket_get_path(const struct s3_bucket *bucket)
{
    return bucket->path;
} /* chimera_s3_bucket_get_path */

SYMBOL_EXPORT int
chimera_s3_add_cred(
    void       *s3_shared,
    const char *access_key,
    const char *secret_key,
    int         pinned)
{
    struct chimera_server_s3_shared *shared = s3_shared;

    return chimera_s3_cred_cache_add(shared->cred_cache, access_key, secret_key, pinned);
} /* chimera_s3_add_cred */

static void *
s3_server_init(
    const struct chimera_server_config *config,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_server_s3_shared *shared;

    shared = calloc(1, sizeof(*shared));

    shared->config = calloc(1, sizeof(*shared->config));

    shared->tcp_protocol = chimera_server_config_get_tcp_stream_protocol(config);

    shared->config->port    = chimera_server_config_get_s3_port(config);
    shared->config->io_size = 128 * 1024;

    shared->endpoint = evpl_endpoint_create("0.0.0.0", shared->config->port);

    shared->listener = evpl_listener_create();

    shared->bucket_map = s3_bucket_map_create();

    /* Create S3 credential cache with 64 buckets and 1 hour TTL */
    shared->cred_cache = chimera_s3_cred_cache_create(64, 3600);

    shared->multipart_table = chimera_s3_multipart_table_create(256);

    /* Initialize root credentials for now - TODO: proper credential mapping */
    chimera_vfs_cred_init_unix(&shared->cred, 0, 0, 0, NULL);

    /* Initialize the root file handle for VFS lookups */
    chimera_vfs_get_root_fh(shared->root_fh, &shared->root_fh_len);

    return shared;
} /* s3_server_init */ /* s3_server_init */

static void
s3_server_stop(void *data)
{
    struct chimera_server_s3_shared *shared = data;

    evpl_listener_destroy(shared->listener);
} /* s3_server_stop */

static void
s3_server_destroy(void *data)
{
    struct chimera_server_s3_shared *shared = data;

    s3_bucket_map_destroy(shared->bucket_map);

    chimera_s3_cred_cache_destroy(shared->cred_cache);

    chimera_s3_multipart_table_destroy(shared->multipart_table);

    free(shared->config);

    free(shared);
} /* s3_server_destroy */ /* s3_server_destroy */

static void
s3_server_start(void *data)
{
    struct chimera_server_s3_shared *shared = data;

    evpl_listen(shared->listener, shared->tcp_protocol, shared->endpoint);
} /* s3_server_start */

static void *
s3_server_thread_init(
    struct evpl               *evpl,
    struct chimera_vfs_thread *vfs_thread,
    void                      *data)
{
    struct chimera_server_s3_shared *shared = data;
    struct chimera_server_s3_thread *thread;

    thread = calloc(1, sizeof(*thread));

    thread->evpl   = evpl;
    thread->shared = shared;
    thread->vfs    = vfs_thread;

    thread->agent = evpl_http_init(evpl);

    thread->server = evpl_http_attach(thread->agent, shared->listener, s3_server_dispatch, thread);


    return thread;
} /* s3_server_thread_init */

static void
s3_server_thread_destroy(void *data)
{
    struct chimera_server_s3_thread *thread = data;
    struct chimera_s3_request       *request;
    struct chimera_s3_io            *io;

    evpl_http_server_destroy(thread->agent, thread->server);
    evpl_http_destroy(thread->agent);

    while (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
        free(request);
    }

    while (thread->free_ios) {
        io = thread->free_ios;
        LL_DELETE(thread->free_ios, io);
        free(io);
    }

    free(thread);
} /* s3_server_thread_destroy */

SYMBOL_EXPORT struct chimera_server_protocol s3_protocol = {
    .init           = s3_server_init,
    .destroy        = s3_server_destroy,
    .start          = s3_server_start,
    .stop           = s3_server_stop,
    .thread_init    = s3_server_thread_init,
    .thread_destroy = s3_server_thread_destroy,
};