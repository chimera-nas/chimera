// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct evpl;
struct chimera_vfs_compound;
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
    char                 *body_buf;
    int                   body_len;
    int                   body_cap;
    /* Parsed tag set. */
    struct chimera_s3_tag tags[CHIMERA_S3_TAG_MAX_TAGS];
    int                   n_tags;
    /* Number of matching tag names in this execution attempt. */
    int                   total;
    /* Which subresource operation is in flight (enum chimera_s3_tagging_op). */
    int                   op;
    /* Names returned by list_xattrs (for GET/DELETE), staged here. */
    char                 *names;
    int                   names_len;
    /* Response (<Tagging>) builder. */
    char                 *resp_buf;
    int                   resp_len;
    int                   resp_cap;

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

/* Append parsed tags to a newly created object's current open handle. Values
 * remain owned by request->tagging through compound acceptance. */
int chimera_s3_tagging_compound_store(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_request   *request);
