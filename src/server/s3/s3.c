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
#include "common/macros.h"
#include "common/misc.h"
#include "s3_internal.h"
#include "s3_status.h"
#include "s3_procs.h"
#include "s3_dump.h"
#include "s3_bucket_map.h"
#include "s3_auth.h"
#include "s3.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static inline void
chimera_s3_sterilize_path(
    struct chimera_s3_request *request,
    const char                *prefix,
    int                        prefix_len)
{
    request->path     = request->list.prefix;
    request->path_len = request->list.prefix_len;
} /* chimera_s3_sterilize_path */

/* expect range_str in the form: bytes=1000-1599 */
static inline int
chimera_s3_parse_range(
    const char *range_str,
    int64_t    *offset,
    int64_t    *length)
{
    char *dash;
    char *end;

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

    strftime(date_ts, sizeof(date_ts), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&now));

    evpl_http_request_add_header(request->http_request, "Date", date_ts);

    evpl_http_request_add_header(request->http_request, "Server", "chimera-s3");

    if (request->is_list) {
        evpl_http_request_add_header(request->http_request, "Content-Type", "application/xml");
    } else {
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
    } else {
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

    switch (notify_type) {
        case EVPL_HTTP_NOTIFY_RECEIVE_DATA:
            if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                s3_request->vfs_state == CHIMERA_S3_VFS_STATE_RECV) {
                chimera_s3_put_recv(evpl, s3_request);
            }
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE:

            s3_request->http_state = CHIMERA_S3_HTTP_STATE_RECVED;

            if (request_type == EVPL_HTTP_REQUEST_TYPE_PUT &&
                s3_request->vfs_state == CHIMERA_S3_VFS_STATE_RECV) {
                chimera_s3_put_recv(evpl, s3_request);
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

    switch (evpl_http_request_type(s3_request->http_request)) {
        case EVPL_HTTP_REQUEST_TYPE_HEAD:
            chimera_s3_get(evpl, thread, s3_request);
            break;
        case EVPL_HTTP_REQUEST_TYPE_GET:
            if (s3_request->is_list) {
                chimera_s3_list(evpl, thread, s3_request);
            } else {
                chimera_s3_get(evpl, thread, s3_request);
            }
            break;
        case EVPL_HTTP_REQUEST_TYPE_PUT:
            chimera_s3_put(evpl, thread, s3_request);
            break;
        case EVPL_HTTP_REQUEST_TYPE_DELETE:
            chimera_s3_delete(evpl, thread, s3_request);
            break;
        default:
            s3_request->status = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
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

    s3_request->status       = CHIMERA_S3_STATUS_OK;
    s3_request->vfs_state    = CHIMERA_S3_VFS_STATE_INIT;
    s3_request->http_state   = CHIMERA_S3_HTTP_STATE_INIT;
    s3_request->io_pending   = 0;
    s3_request->is_list      = 0;
    s3_request->http_request = request;

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

    if (*s3_request->path == '?') {
        const char *key_start, *value_start;
        const char *p;
        int         key_len, value_len;

        s3_request->is_list         = 1;
        s3_request->list.prefix_len = 0;
        s3_request->list.max_keys   = 1000;

        p = s3_request->path + 1;

        while (*p) {
            key_start = p;
            while (*p && *p != '=') {
                p++;
            }
            if (!*p) {
                break;
            }

            value_start = ++p;
            while (*p && *p != '&') {
                p++;
            }

            key_len   = value_start - key_start - 1;
            value_len = p - value_start;

            if (key_len == 6 && memcmp(key_start, "prefix", 6) == 0) {
                s3_request->list.prefix_len = value_len;
                memcpy(s3_request->list.prefix, value_start, value_len);
                s3_request->list.prefix[value_len] = '\0';
            }

            if (key_len == 8 && memcmp(key_start, "max-keys", 8) == 0) {
                s3_request->list.max_keys = atoi(value_start);
            }

            if (*p) {
                p++;
            }
        }

        chimera_s3_sterilize_path(s3_request, s3_request->list.prefix, s3_request->list.prefix_len);
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

    bucket = s3_bucket_map_get(shared->bucket_map, s3_request->bucket_name, s3_request->bucket_namelen);

    if (bucket == NULL) {
        s3_request->status    = CHIMERA_S3_STATUS_NO_SUCH_BUCKET;
        s3_request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        s3_bucket_map_release(shared->bucket_map);
        return;
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

    shared->config->port    = 5000;
    shared->config->io_size = 128 * 1024;

    shared->endpoint = evpl_endpoint_create("0.0.0.0", shared->config->port);

    shared->listener = evpl_listener_create();

    shared->bucket_map = s3_bucket_map_create();

    /* Create S3 credential cache with 64 buckets and 1 hour TTL */
    shared->cred_cache = chimera_s3_cred_cache_create(64, 3600);

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

    free(shared->config);

    free(shared);
} /* s3_server_destroy */ /* s3_server_destroy */

static void
s3_server_start(void *data)
{
    struct chimera_server_s3_shared *shared = data;

    evpl_listen(shared->listener, EVPL_STREAM_SOCKET_TCP, shared->endpoint);
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