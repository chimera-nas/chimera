// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct evpl;
struct chimera_s3_request;
struct chimera_server_s3_thread;

/* S3 object/bucket tagging stored as filesystem extended attributes under the
 * "user.s3.tag." namespace so the tags are visible to NFS/SMB as well. */

#define CHIMERA_S3_TAG_PREFIX      "user.s3.tag."
#define CHIMERA_S3_TAG_PREFIX_LEN  12

#define CHIMERA_S3_TAG_MAX_TAGS    10
#define CHIMERA_S3_TAG_MAX_KEY_LEN 128
#define CHIMERA_S3_TAG_MAX_VAL_LEN 256

/* One parsed tag. key/value are NUL-terminated. */
struct chimera_s3_tag {
    char key[CHIMERA_S3_TAG_MAX_KEY_LEN + 1];
    char val[CHIMERA_S3_TAG_MAX_VAL_LEN + 1];
};

/* Per-request tagging working state (heap-allocated; freed on response). */
struct chimera_s3_tagging_ctx {
    /* PUT request body (the <Tagging> document) accumulator. */
    char                           *body_buf;
    int                             body_len;
    int                             body_cap;
    /* Parsed tag set. */
    struct chimera_s3_tag           tags[CHIMERA_S3_TAG_MAX_TAGS];
    int                             n_tags;
    /* Sequential cursor over xattr ops (set/remove/get). */
    int                             cur;
    int                             prev_cur;
    int                             total;
    /* Which subresource operation is in flight (enum chimera_s3_tagging_op). */
    int                             op;
    /* Names returned by list_xattrs (for GET/DELETE), staged here. */
    char                           *names;
    int                             names_len;
    /* Scratch buffer for a single xattr value read (GET). */
    char                           *valbuf;
    /* Response (<Tagging>) builder. */
    char                           *resp_buf;
    int                             resp_len;
    int                             resp_cap;
    /* Object/bucket handle the tag xattrs live on. */
    struct chimera_vfs_open_handle *handle;
    /* Continuation invoked once existing tag xattrs have been cleared. */
    void                            (*after)(
        struct evpl               *evpl,
        struct chimera_s3_request *request);
    /* Continuation for store-by-path (PutObject x-amz-tagging / multipart). */
    void                            (*store_done)(
        struct evpl               *evpl,
        struct chimera_s3_request *request);
};

/* Parse an x-amz-tagging header value ("k1=v1&k2=v2", URL-encoded) into the
 * ctx tag set. Returns 0 on success, -1 on a limit/format violation. */
int
chimera_s3_tagging_parse_header(
    struct chimera_s3_tagging_ctx *ctx,
    const char                    *value);

/* Defensive teardown: free any tagging ctx still attached to the request
 * (e.g. an aborted request). Safe to call when request->tagging is NULL. */
void
chimera_s3_tagging_request_cleanup(
    struct chimera_s3_request *request);

/* PutObjectTagging body accumulation (RECEIVE_DATA / RECEIVE_COMPLETE). */
void
chimera_s3_put_tagging_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_put_tagging_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

/* HEAD object: count the object's tag xattrs, add the x-amz-tagging-count
 * response header, then invoke done_cb to finish the request. The object FH is
 * supplied directly (already resolved by the HEAD lookup). */
void
chimera_s3_tagging_count_for_head(
    struct evpl *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request *request,
    const void *fh,
    int fh_len,
    void ( *done_cb )(struct evpl *evpl, struct chimera_s3_request *request));

/* Subresource entry points (?tagging on an object). */
void
chimera_s3_get_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_put_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_delete_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

/* Store a parsed tag set (request->tagging->tags) as xattrs on the object at
 * request->path under request->bucket_fh, replacing any existing tag xattrs.
 * Used by PutObject (x-amz-tagging) and CompleteMultipartUpload after the
 * object has been materialized. On completion (or error) calls done_cb, which
 * is expected to drive the request to its terminal response. The tag set must
 * already be parsed/validated into request->tagging. */
void
chimera_s3_tagging_store_by_path(
    struct evpl *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request *request,
    void ( *done_cb )(struct evpl *evpl, struct chimera_s3_request *request));
