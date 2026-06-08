// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Bucket-level S3 operations: ListBuckets (GET /), CreateBucket (PUT /bucket),
 * DeleteBucket (DELETE /bucket), and HeadBucket (HEAD /bucket).
 *
 * Buckets are directories. A statically configured bucket maps a name to an
 * arbitrary VFS path; a dynamically created one is materialized as a directory
 * under the configured bucket root (shared->bucket_root_path) and registered in
 * the bucket map at runtime. The map is the source of truth for which buckets
 * exist, so List/Head consult it directly.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "s3_internal.h"
#include "s3.h"
#include "s3_procs.h"

/* ---------------------------------------------------------------- ListBuckets */

struct chimera_s3_list_buckets_ctx {
    char *bp;
};

static int
chimera_s3_list_buckets_cb(
    const struct s3_bucket *bucket,
    void                   *data)
{
    struct chimera_s3_list_buckets_ctx *ctx = data;

    ctx->bp += sprintf(ctx->bp, "  <Bucket>\n");
    ctx->bp += sprintf(ctx->bp, "   <Name>%s</Name>\n",
                       chimera_s3_bucket_get_name(bucket));
    ctx->bp += sprintf(ctx->bp,
                       "   <CreationDate>2025-01-01T00:00:00.000Z</CreationDate>\n");
    ctx->bp += sprintf(ctx->bp, "  </Bucket>\n");

    return 0;
} /* chimera_s3_list_buckets_cb */

void
chimera_s3_list_buckets(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_s3_list_buckets_ctx ctx;
    struct evpl_iovec                  iov;
    char                              *start;

    evpl_iovec_alloc(evpl, 1024 * 1024, 0, 1, 0, &iov);
    start  = evpl_iovec_data(&iov);
    ctx.bp = start;

    ctx.bp += sprintf(ctx.bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    ctx.bp += sprintf(ctx.bp,
                      "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    ctx.bp += sprintf(ctx.bp, " <Owner>\n");
    ctx.bp += sprintf(ctx.bp, "  <ID>chimera</ID>\n");
    ctx.bp += sprintf(ctx.bp, "  <DisplayName>chimera</DisplayName>\n");
    ctx.bp += sprintf(ctx.bp, " </Owner>\n");
    ctx.bp += sprintf(ctx.bp, " <Buckets>\n");

    chimera_s3_iterate_buckets(thread->shared,
                               chimera_s3_list_buckets_cb, &ctx);

    ctx.bp += sprintf(ctx.bp, " </Buckets>\n");
    ctx.bp += sprintf(ctx.bp, "</ListAllMyBucketsResult>\n");

    evpl_iovec_set_length(&iov, ctx.bp - start);
    evpl_http_request_add_datav(request->http_request, &iov, 1);

    request->is_list          = 1;
    request->file_length      = ctx.bp - start;
    request->file_real_length = request->file_length;
    request->file_offset      = 0;
    request->status           = CHIMERA_S3_STATUS_OK;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_list_buckets */

/* --------------------------------------------------------------- CreateBucket */

static void
chimera_s3_create_bucket_mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    struct chimera_server_s3_shared *shared  = thread->shared;
    char                             name[256];
    char                             path[512];
    char                             location[300];

    /* EEXIST is success in us-east-1: recreating your own bucket is a no-op. */
    if (error_code && error_code != CHIMERA_VFS_EEXIST) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    snprintf(name, sizeof(name), "%.*s",
             request->bucket_namelen, request->bucket_name);
    snprintf(path, sizeof(path), "%.*s/%s",
             shared->bucket_root_pathlen, shared->bucket_root_path, name);

    chimera_s3_add_bucket(shared, name, path);

    snprintf(location, sizeof(location), "/%s", name);
    evpl_http_request_add_header(request->http_request, "Location", location);

    request->status           = CHIMERA_S3_STATUS_OK;
    request->file_length      = 0;
    request->file_real_length = 0;
    request->file_offset      = 0;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_create_bucket_mkdir_cb */

static void
chimera_s3_create_bucket_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code || !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        /* Bucket root path is missing/unresolvable. */
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Stash the bucket-root directory fh and create the new bucket dir. */
    memcpy(request->bucket_fh, attr->va_fh, attr->va_fh_len);
    request->bucket_fhlen = attr->va_fh_len;

    memset(&request->set_attr, 0, sizeof(request->set_attr));
    request->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    request->set_attr.va_mode = S_IFDIR | 0755;
    request->set_attr.va_uid  = 0;
    request->set_attr.va_gid  = 0;

    chimera_vfs_mkdir(thread->vfs, &thread->shared->cred,
                      request->bucket_fh, request->bucket_fhlen,
                      request->bucket_name, request->bucket_namelen,
                      &request->set_attr, CHIMERA_VFS_ATTR_FH,
                      chimera_s3_create_bucket_mkdir_cb, request);
} /* chimera_s3_create_bucket_lookup_cb */

void
chimera_s3_create_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;

    if (shared->bucket_root_pathlen == 0) {
        /* Runtime bucket creation is not configured. */
        request->status    = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    chimera_vfs_lookup(thread->vfs, &shared->cred, NULL,
                       shared->root_fh, shared->root_fh_len,
                       shared->bucket_root_path, shared->bucket_root_pathlen,
                       CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_create_bucket_lookup_cb, request);
} /* chimera_s3_create_bucket */

/* --------------------------------------------------------------- DeleteBucket
 *
 * S3 buckets are flat, but chimera stores hierarchical object keys (foo/bar) as
 * a real VFS directory tree, so an "empty" bucket may still hold empty key-path
 * directories. DeleteBucket therefore: (1) walks the bucket subtree, (2) fails
 * with BucketNotEmpty if any regular file (object) remains, otherwise (3)
 * removes the leftover empty directories deepest-first and finally the bucket
 * directory itself. */

struct s3_delbucket_ctx {
    struct chimera_s3_request *request;
    char                       bucket_path[512];
    int                        bucket_path_len;
    uint8_t                    bucket_fh[CHIMERA_VFS_FH_SIZE];
    int                        bucket_fhlen;
    int                        has_file;
    int                        ndirs;
    int                        capdirs;
    char                     **dirs;   /* relative dir paths, no leading slash */
    int                        cur;
};

static void chimera_s3_delbucket_finish(
    struct s3_delbucket_ctx *ctx,
    enum chimera_s3_status   status);

static int
chimera_s3_delbucket_filter(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    /* Visit every entry. */
    return 0;
} /* chimera_s3_delbucket_filter */

static int
chimera_s3_delbucket_collect(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct s3_delbucket_ctx *ctx = private_data;

    while (pathlen > 0 && path[0] == '/') {
        path++;
        pathlen--;
    }
    if (pathlen == 0) {
        return 0;
    }

    if ((attr->va_mode & S_IFMT) == S_IFDIR) {
        if (ctx->ndirs == ctx->capdirs) {
            ctx->capdirs = ctx->capdirs ? ctx->capdirs * 2 : 16;
            ctx->dirs    = realloc(ctx->dirs, ctx->capdirs * sizeof(char *));
        }
        ctx->dirs[ctx->ndirs] = strndup(path, pathlen);
        ctx->ndirs++;
    } else {
        /* A regular file means the bucket still has objects. */
        ctx->has_file = 1;
    }
    return 0;
} /* chimera_s3_delbucket_collect */

/* Order directories deepest-first so children are removed before parents. */
static int
chimera_s3_delbucket_depth_cmp(
    const void *a,
    const void *b)
{
    const char *pa = *(const char *const *) a;
    const char *pb = *(const char *const *) b;
    int         da = 0, db = 0;
    const char *p;

    for (p = pa; *p; p++) {
        if (*p == '/') {
            da++;
        }
    }
    for (p = pb; *p; p++) {
        if (*p == '/') {
            db++;
        }
    }
    return db - da;
} /* chimera_s3_delbucket_depth_cmp */

/* Final step: remove the now-empty bucket directory itself. */
static void
chimera_s3_delbucket_root_removed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct s3_delbucket_ctx *ctx = private_data;

    /* ENOENT (already gone) is fine; anything else maps to internal error. */
    if (error_code && error_code != CHIMERA_VFS_ENOENT) {
        chimera_s3_delbucket_finish(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }
    chimera_s3_delbucket_finish(ctx, CHIMERA_S3_STATUS_NO_CONTENT);
} /* chimera_s3_delbucket_root_removed */

static void chimera_s3_delbucket_remove_next(
    struct s3_delbucket_ctx *ctx);

static void
chimera_s3_delbucket_dir_removed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct s3_delbucket_ctx *ctx = private_data;

    /* Best-effort: ignore per-dir errors (e.g. a racing create) and keep going;
     * the final bucket rmdir will surface a real "not empty" if it matters. */
    ctx->cur++;
    chimera_s3_delbucket_remove_next(ctx);
} /* chimera_s3_delbucket_dir_removed */

static void
chimera_s3_delbucket_remove_next(struct s3_delbucket_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_server_s3_shared *shared  = thread->shared;

    if (ctx->cur < ctx->ndirs) {
        chimera_vfs_remove(thread->vfs, &shared->cred,
                           ctx->bucket_fh, ctx->bucket_fhlen,
                           ctx->dirs[ctx->cur], strlen(ctx->dirs[ctx->cur]),
                           chimera_s3_delbucket_dir_removed, ctx);
        return;
    }

    /* All scaffolding gone; remove the bucket directory from the bucket root. */
    chimera_vfs_remove(thread->vfs, &shared->cred,
                       shared->root_fh, shared->root_fh_len,
                       ctx->bucket_path, ctx->bucket_path_len,
                       chimera_s3_delbucket_root_removed, ctx);
} /* chimera_s3_delbucket_remove_next */

static void
chimera_s3_delbucket_find_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct s3_delbucket_ctx *ctx = private_data;

    if (ctx->has_file) {
        chimera_s3_delbucket_finish(ctx, CHIMERA_S3_STATUS_BUCKET_NOT_EMPTY);
        return;
    }

    if (ctx->ndirs > 1) {
        qsort(ctx->dirs, ctx->ndirs, sizeof(char *),
              chimera_s3_delbucket_depth_cmp);
    }

    ctx->cur = 0;
    chimera_s3_delbucket_remove_next(ctx);
} /* chimera_s3_delbucket_find_complete */

static void
chimera_s3_delbucket_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct s3_delbucket_ctx         *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code || !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        /* The bucket directory is gone; drop the map entry and report success. */
        chimera_s3_delbucket_finish(ctx, CHIMERA_S3_STATUS_NO_CONTENT);
        return;
    }

    memcpy(ctx->bucket_fh, attr->va_fh, attr->va_fh_len);
    ctx->bucket_fhlen = attr->va_fh_len;

    chimera_vfs_find(thread->vfs, &thread->shared->cred,
                     ctx->bucket_fh, ctx->bucket_fhlen,
                     CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                     chimera_s3_delbucket_filter,
                     chimera_s3_delbucket_collect,
                     chimera_s3_delbucket_find_complete,
                     ctx);
} /* chimera_s3_delbucket_lookup_cb */

static void
chimera_s3_delbucket_finish(
    struct s3_delbucket_ctx *ctx,
    enum chimera_s3_status   status)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    int                              i;

    /* On success the bucket no longer exists; drop it from the map. */
    if (status == CHIMERA_S3_STATUS_NO_CONTENT) {
        char name[256];

        snprintf(name, sizeof(name), "%.*s",
                 request->bucket_namelen, request->bucket_name);
        chimera_s3_remove_bucket(thread->shared, name);
    }

    for (i = 0; i < ctx->ndirs; i++) {
        free(ctx->dirs[i]);
    }
    free(ctx->dirs);

    request->status    = status;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

    free(ctx);
} /* chimera_s3_delbucket_finish */

void
chimera_s3_delete_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;
    struct s3_delbucket_ctx         *ctx;

    if (shared->bucket_root_pathlen == 0) {
        request->status    = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;

    ctx->bucket_path_len = snprintf(ctx->bucket_path, sizeof(ctx->bucket_path),
                                    "%.*s/%.*s",
                                    shared->bucket_root_pathlen, shared->bucket_root_path,
                                    request->bucket_namelen, request->bucket_name);

    /* Resolve the bucket directory, then walk + purge it. */
    chimera_vfs_lookup(thread->vfs, &shared->cred, NULL,
                       shared->root_fh, shared->root_fh_len,
                       ctx->bucket_path, ctx->bucket_path_len,
                       CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_delbucket_lookup_cb, ctx);
} /* chimera_s3_delete_bucket */

/* ----------------------------------------------------------------- HeadBucket */

void
chimera_s3_head_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    /* Existence is determined by bucket-map membership, which the dispatcher
     * has already confirmed before routing here. Reply 200 with no body. */
    request->status           = CHIMERA_S3_STATUS_OK;
    request->file_length      = 0;
    request->file_real_length = 0;
    request->file_offset      = 0;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_head_bucket */
