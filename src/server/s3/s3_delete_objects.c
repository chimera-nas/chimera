// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * DeleteObjects (POST /bucket?delete): the S3 batch-delete operation used by
 * `aws s3 rm --recursive` and the delete phase of `aws s3 sync`. The request
 * body is a <Delete> document listing object keys; the response is a
 * <DeleteResult> reporting each key as <Deleted> or <Error>. Deletes are
 * idempotent: removing a key that does not exist is reported as success.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_compound.h"
#include "s3_procs.h"

#define CHIMERA_S3_DEL_BODY_HARD_CAP (4 * 1024 * 1024)
#define CHIMERA_S3_DEL_BODY_OVERFLOW (-1)
#define CHIMERA_S3_DEL_MAX_KEYS      1000

/* ----- request body accumulation ----- */

void
chimera_s3_delete_objects_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct evpl_iovec iov[CHIMERA_S3_IOV_MAX];
    uint64_t          avail, total;
    int               niov, i;

    while ((avail = evpl_http_request_get_data_avail(request->http_request)) > 0) {
        niov  = evpl_http_request_get_datav(evpl, request->http_request, iov, avail);
        total = 0;
        for (i = 0; i < niov; i++) {
            total += iov[i].length;
        }

        if (request->del.body_len == CHIMERA_S3_DEL_BODY_OVERFLOW) {
            /* Already over the hard cap; keep draining to release iovs. */
        } else if (request->del.body_len + total > CHIMERA_S3_DEL_BODY_HARD_CAP) {
            free(request->del.body_buf);
            request->del.body_buf = NULL;
            request->del.body_cap = 0;
            request->del.body_len = CHIMERA_S3_DEL_BODY_OVERFLOW;
        } else {
            if (request->del.body_len + total > request->del.body_cap) {
                int new_cap = request->del.body_cap ? request->del.body_cap * 2 : 4096;
                while ((uint64_t) new_cap < request->del.body_len + total) {
                    new_cap *= 2;
                }
                request->del.body_buf = realloc(request->del.body_buf, new_cap);
                request->del.body_cap = new_cap;
            }
            for (i = 0; i < niov; i++) {
                memcpy(request->del.body_buf + request->del.body_len,
                       iov[i].data, iov[i].length);
                request->del.body_len += iov[i].length;
            }
        }

        evpl_iovecs_release(evpl, iov, niov);
    }
} /* chimera_s3_delete_objects_recv */

/* ----- XML helpers ----- */

/* Locate `tag` between [start, end) and return pointer past its end. */
static const char *
chimera_s3_del_xml_find(
    const char *start,
    const char *end,
    const char *tag)
{
    size_t tag_len = strlen(tag);

    if ((size_t) (end - start) < tag_len) {
        return NULL;
    }
    for (const char *p = start; p <= end - tag_len; p++) {
        if (memcmp(p, tag, tag_len) == 0) {
            return p + tag_len;
        }
    }
    return NULL;
} /* chimera_s3_del_xml_find */

/* Decode the five predefined XML entities in place. Always shrinks, so the
 * result fits within the original [s, s+len) range. Returns the new length. */
static int
chimera_s3_del_xml_unescape(
    char *s,
    int   len)
{
    int r = 0, w = 0;

    while (r < len) {
        if (s[r] == '&') {
            if (r + 5 <= len && memcmp(s + r, "&amp;", 5) == 0) {
                s[w++] = '&';
                r     += 5;
                continue;
            }
            if (r + 4 <= len && memcmp(s + r, "&lt;", 4) == 0) {
                s[w++] = '<';
                r     += 4;
                continue;
            }
            if (r + 4 <= len && memcmp(s + r, "&gt;", 4) == 0) {
                s[w++] = '>';
                r     += 4;
                continue;
            }
            if (r + 6 <= len && memcmp(s + r, "&quot;", 6) == 0) {
                s[w++] = '"';
                r     += 6;
                continue;
            }
            if (r + 6 <= len && memcmp(s + r, "&apos;", 6) == 0) {
                s[w++] = '\'';
                r     += 6;
                continue;
            }
        }
        s[w++] = s[r++];
    }
    return w;
} /* chimera_s3_del_xml_unescape */

/* ----- response buffer (growable) ----- */

static void
chimera_s3_del_resp_append(
    struct chimera_s3_request *request,
    const char                *s,
    int                        len)
{
    if (request->del.resp_len + len > request->del.resp_cap) {
        int new_cap = request->del.resp_cap ? request->del.resp_cap * 2 : 8192;
        while (new_cap < request->del.resp_len + len) {
            new_cap *= 2;
        }
        request->del.resp_buf = realloc(request->del.resp_buf, new_cap);
        request->del.resp_cap = new_cap;
    }
    memcpy(request->del.resp_buf + request->del.resp_len, s, len);
    request->del.resp_len += len;
} /* chimera_s3_del_resp_append */

static void
chimera_s3_del_resp_str(
    struct chimera_s3_request *request,
    const char                *s)
{
    chimera_s3_del_resp_append(request, s, strlen(s));
} /* chimera_s3_del_resp_str */

/* Append a key, escaping the predefined XML entities. */
static void
chimera_s3_del_resp_key(
    struct chimera_s3_request *request,
    const char                *s,
    int                        len)
{
    int i;

    for (i = 0; i < len; i++) {
        switch (s[i]) {
            case '&':
                chimera_s3_del_resp_append(request, "&amp;", 5);
                break;
            case '<':
                chimera_s3_del_resp_append(request, "&lt;", 4);
                break;
            case '>':
                chimera_s3_del_resp_append(request, "&gt;", 4);
                break;
            case '"':
                chimera_s3_del_resp_append(request, "&quot;", 6);
                break;
            case '\'':
                chimera_s3_del_resp_append(request, "&apos;", 6);
                break;
            default:
                chimera_s3_del_resp_append(request, &s[i], 1);
                break;
        } /* switch */
    }
} /* chimera_s3_del_resp_key */

/* ----- body parsing ----- */

/*
 * Parse the <Delete> document into request->del.entries. Keys are unescaped
 * in place inside the body buffer (which outlives parsing). Returns
 * CHIMERA_S3_STATUS_OK or MALFORMED_XML.
 */
static enum chimera_s3_status
chimera_s3_del_parse_body(struct chimera_s3_request *request)
{
    char *body         = request->del.body_buf;
    const char *end    = body + request->del.body_len;
    const char *cursor = body;
    struct chimera_s3_delete_entry *entries = NULL;
    int n   = 0;
    int cap = 0;
    const char *quiet_open, *quiet_close;

    /* Optional <Quiet> flag: suppress per-key <Deleted> entries. */
    quiet_open = chimera_s3_del_xml_find(body, end, "<Quiet>");
    if (quiet_open) {
        quiet_close = chimera_s3_del_xml_find(quiet_open, end, "</Quiet>");
        if (quiet_close) {
            const char *v = quiet_open;
            int vlen      = (quiet_close - strlen("</Quiet>")) - v;
            if (vlen >= 4 && strncasecmp(v, "true", 4) == 0) {
                request->del.quiet = 1;
            }
        }
    }

    while (cursor < end) {
        const char *obj_open = chimera_s3_del_xml_find(cursor, end, "<Object>");
        const char *obj_close, *key_open, *key_close;
        char *key;
        int key_len;

        if (!obj_open) {
            break;
        }
        obj_close = chimera_s3_del_xml_find(obj_open, end, "</Object>");
        if (!obj_close) {
            free(entries);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }

        key_open  = chimera_s3_del_xml_find(obj_open, obj_close, "<Key>");
        key_close = key_open ? chimera_s3_del_xml_find(key_open, obj_close, "</Key>") : NULL;
        if (!key_open || !key_close) {
            free(entries);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }

        key     = (char *) key_open;
        key_len = (key_close - strlen("</Key>")) - key_open;
        key_len = chimera_s3_del_xml_unescape(key, key_len);

        if (n >= CHIMERA_S3_DEL_MAX_KEYS) {
            free(entries);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }
        if (n == cap) {
            cap = cap ? cap * 2 : 16;
            if (cap > CHIMERA_S3_DEL_MAX_KEYS) {
                cap = CHIMERA_S3_DEL_MAX_KEYS;
            }
            entries = realloc(entries, cap * sizeof(*entries));
        }

        entries[n].key      = key;
        entries[n].key_len  = key_len;
        entries[n].deleted  = 0;
        entries[n].err_code = NULL;
        entries[n].err_msg  = NULL;
        n++;

        cursor = obj_close;
    }

    request->del.entries = entries;
    request->del.n_keys  = n;
    return CHIMERA_S3_STATUS_OK;
} /* chimera_s3_del_parse_body */

/* ----- sequential deletion driver ----- */

static void chimera_s3_del_drive(
    struct chimera_s3_request *request);

static void
chimera_s3_del_finalize(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct evpl_iovec iov;
    int               i;

    chimera_s3_del_resp_str(request, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_del_resp_str(request,
                            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for (i = 0; i < request->del.n_keys; i++) {
        struct chimera_s3_delete_entry *e = &request->del.entries[i];

        if (e->deleted) {
            if (!request->del.quiet) {
                chimera_s3_del_resp_str(request, " <Deleted><Key>");
                chimera_s3_del_resp_key(request, e->key, e->key_len);
                chimera_s3_del_resp_str(request, "</Key></Deleted>\n");
            }
        } else {
            char tail[256];

            chimera_s3_del_resp_str(request, " <Error><Key>");
            chimera_s3_del_resp_key(request, e->key, e->key_len);
            chimera_s3_del_resp_append(request, tail,
                                       snprintf(tail, sizeof(tail),
                                                "</Key><Code>%s</Code><Message>%s</Message></Error>\n",
                                                e->err_code, e->err_msg));
        }
    }

    chimera_s3_del_resp_str(request, "</DeleteResult>\n");

    /* Copy the assembled document into an evpl iovec for the HTTP response.
     * Keys in resp_buf are independent copies, so the body buffer and entry
     * array can now be released. */
    evpl_iovec_alloc(evpl, request->del.resp_len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), request->del.resp_buf, request->del.resp_len);
    evpl_iovec_set_length(&iov, request->del.resp_len);
    chimera_s3_response_add_datav(evpl, request, &iov, 1);

    request->file_length      = request->del.resp_len;
    request->file_real_length = request->del.resp_len;
    request->file_offset      = 0;
    request->is_list          = 1; /* render Content-Type: application/xml */
    request->status           = CHIMERA_S3_STATUS_OK;

    free(request->del.resp_buf);
    request->del.resp_buf = NULL;
    free(request->del.entries);
    request->del.entries = NULL;
    free(request->del.body_buf);
    request->del.body_buf = NULL;

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_del_finalize */

/*
 * Record the outcome for the current key. If we are unwinding from an
 * inline (synchronous) VFS completion, just clear the pending flag and let
 * chimera_s3_del_drive()'s loop advance; otherwise advance and resume the
 * loop here. This trampoline keeps a long batch from recursing per key.
 */
static void
chimera_s3_del_record(
    struct chimera_s3_request *request,
    int                        deleted,
    const char                *code,
    const char                *msg)
{
    struct chimera_s3_delete_entry *e = &request->del.entries[request->del.cur];

    e->deleted  = deleted;
    e->err_code = code;
    e->err_msg  = msg;

    request->del.pending = 0;

    if (!request->del.synchronous) {
        request->del.cur++;
        chimera_s3_del_drive(request);
    }
} /* chimera_s3_del_record */

/*
 * Terminal of the current key's VFS chain: stash the wire outcome and hand
 * the key's compound to the driver.  A CHIMERA_VFS_ECOMPOUND_CONFLICT
 * replays the key from chimera_s3_del_key_start (the outcome fields are
 * simply overwritten by the replay's terminal); anything else settles in
 * chimera_s3_del_key_reply.
 */
static void
chimera_s3_del_key_outcome(
    struct chimera_s3_request *request,
    int                        deleted,
    const char                *code,
    const char                *msg,
    enum chimera_vfs_error     vfs_status)
{
    request->del.cur_deleted = deleted;
    request->del.cur_code    = code;
    request->del.cur_msg     = msg;

    chimera_s3_compound_finish(request, vfs_status);
} /* chimera_s3_del_key_outcome */

static void
chimera_s3_del_remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    chimera_vfs_release(thread->vfs, request->dir_handle);
    request->dir_handle = NULL;

    if (error_code == CHIMERA_VFS_OK || error_code == CHIMERA_VFS_ENOENT) {
        /* Removed, or already absent: both count as success (committed). */
        chimera_s3_del_key_outcome(request, 1, NULL, NULL, CHIMERA_VFS_OK);
    } else if (error_code == CHIMERA_VFS_EACCES) {
        chimera_s3_del_key_outcome(request, 0, "AccessDenied", "Access Denied",
                                   error_code);
    } else {
        chimera_s3_del_key_outcome(request, 0, "InternalError",
                                   "We encountered an internal error. Please try again.",
                                   error_code);
    }
} /* chimera_s3_del_remove_cb */

static void
chimera_s3_del_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        /* The parent existed a moment ago (its lookup succeeded); any
        * failure to open it is a real error, not an absent object. */
        chimera_s3_del_key_outcome(request, 0, "InternalError",
                                   "We encountered an internal error. "
                                   "Please try again.",
                                   error_code);
        return;
    }

    request->dir_handle = oh;

    chimera_s3_request_get(request);

    chimera_vfs_remove_at(thread->vfs, &thread->shared->cred, request->compound,
                          oh,
                          request->del.cur_name,
                          request->del.cur_name_len,
                          NULL, 0, 0, 0, 0,
                          NULL,
                          chimera_s3_del_remove_cb,
                          request);
} /* chimera_s3_del_open_cb */

static void
chimera_s3_del_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code == CHIMERA_VFS_ENOENT || error_code == CHIMERA_VFS_ENOTDIR) {
        /* Parent path does not exist: object is effectively gone. */
        chimera_s3_del_key_outcome(request, 1, NULL, NULL, CHIMERA_VFS_OK);
        return;
    } else if (error_code) {
        /* Any other lookup failure (ESTALE, EIO, ...) must not masquerade
         * as a successful delete: the object may well still exist. */
        chimera_s3_del_key_outcome(request, 0, "InternalError",
                                   "We encountered an internal error. "
                                   "Please try again.",
                                   error_code);
        return;
    }

    chimera_s3_request_get(request);

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, request->compound,
                        attr->va_fh,
                        attr->va_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_s3_del_open_cb,
                        request);
} /* chimera_s3_del_lookup_cb */

/* Compound start for the CURRENT key: (re)issue its lookup -> open ->
 * remove chain.  The dirpath/name split points into the request body
 * buffer, which outlives every replay. */
static void
chimera_s3_del_key_start(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    chimera_s3_request_get(request);

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, request->compound,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       request->del.cur_dirpath,
                       request->del.cur_dirpathlen,
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_del_lookup_cb,
                       request);
} /* chimera_s3_del_key_start */

/* The key's compound has settled (commit, abort, or replay exhaustion):
 * record the wire outcome and let the trampoline advance the batch. */
static void
chimera_s3_del_key_reply(struct chimera_s3_request *request)
{
    if (request->compound_op_status == CHIMERA_VFS_ECOMPOUND_EXHAUSTED) {
        /* The key's compound lost a conflict it may not replay: report the
         * standard retriable per-key error. */
        request->del.cur_deleted = 0;
        request->del.cur_code    = "SlowDown";
        request->del.cur_msg     = "Please reduce your request rate.";
    }

    chimera_s3_del_record(request, request->del.cur_deleted,
                          request->del.cur_code, request->del.cur_msg);
} /* chimera_s3_del_key_reply */

static void
chimera_s3_del_drive(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;

    while (request->del.cur < request->del.n_keys) {
        struct chimera_s3_delete_entry *e       = &request->del.entries[request->del.cur];
        const char                     *key     = e->key;
        int                             key_len = e->key_len;
        const char                     *dirpath, *name;
        int                             dirpathlen, name_len, i;
        const char                     *slash = NULL;

        for (i = key_len - 1; i >= 0; i--) {
            if (key[i] == '/') {
                slash = key + i;
                break;
            }
        }

        if (slash) {
            dirpath    = key;
            dirpathlen = slash - key;
            name       = slash + 1;
            while (name < key + key_len && *name == '/') {
                name++;
            }
            name_len = (key + key_len) - name;
        } else {
            dirpath    = "/";
            dirpathlen = 1;
            name       = key;
            name_len   = key_len;
        }

        /* Empty key, or a key naming only directories: nothing to remove. */
        if (name_len == 0) {
            e->deleted = 1;
            request->del.cur++;
            continue;
        }

        request->del.cur_name       = name;
        request->del.cur_name_len   = name_len;
        request->del.cur_dirpath    = dirpath;
        request->del.cur_dirpathlen = dirpathlen;
        request->del.cur_deleted    = 0;
        request->del.cur_code       = NULL;
        request->del.cur_msg        = NULL;

        request->del.synchronous = 1;
        request->del.pending     = 1;

        /* One compound PER KEY, matching the per-key wire results: each
         * key's lookup -> open -> remove chain commits (durably) before
         * its <Deleted>/<Error> entry is decided, and a conflict replays
         * only that key. */
        chimera_s3_compound_run(request,
                                request->bucket_fh, request->bucket_fhlen,
                                CHIMERA_VFS_COMPOUND_WRITE,
                                CHIMERA_VFS_COMPOUND_RETRYABLE,
                                chimera_s3_del_key_start,
                                chimera_s3_del_key_reply);

        if (request->del.pending) {
            /* Completion is asynchronous; the callback chain will resume the
             * loop via chimera_s3_del_record(). */
            request->del.synchronous = 0;
            return;
        }

        /* Completed inline; result already recorded. Advance and continue. */
        request->del.cur++;
    }

    chimera_s3_del_finalize(evpl, request);
} /* chimera_s3_del_drive */

/* ----- entry points ----- */

void
chimera_s3_delete_objects(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    (void) thread;

    /* Body/response accumulators were initialized during request parsing.
     * The real work begins once the <Delete> body is fully received. */
    request->vfs_state = CHIMERA_S3_VFS_STATE_INIT;

    /* Reached here from the bucket-resolution callback with bucket_fh now in
     * hand.  On an async backend (cairn) that resolution can land AFTER the
     * body's RECEIVE_COMPLETE, whose delete-objects arm deferred to us because
     * bucket_fhlen was still 0; drive the per-key removes now.  If the body is
     * not yet received, the RECEIVE_COMPLETE arm drives it (bucket_fh is set
     * by then). */
    if (request->bucket_fhlen != 0 &&
        request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        chimera_s3_delete_objects_body_done(evpl, request);
    }
} /* chimera_s3_delete_objects */

void
chimera_s3_delete_objects_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    enum chimera_s3_status err;

    if (request->del.body_len == CHIMERA_S3_DEL_BODY_OVERFLOW) {
        request->status    = CHIMERA_S3_STATUS_MALFORMED_XML;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    err = chimera_s3_del_parse_body(request);

    /* A well-formed request lists at least one object. */
    if (err == CHIMERA_S3_STATUS_OK && request->del.n_keys == 0) {
        err = CHIMERA_S3_STATUS_MALFORMED_XML;
    }

    if (err != CHIMERA_S3_STATUS_OK) {
        free(request->del.entries);
        request->del.entries = NULL;
        free(request->del.body_buf);
        request->del.body_buf = NULL;
        request->status       = err;
        request->vfs_state    = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    request->del.cur = 0;
    chimera_s3_del_drive(request);
} /* chimera_s3_delete_objects_body_done */
