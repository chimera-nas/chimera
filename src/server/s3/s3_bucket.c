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
 * exist. HeadBucket also verifies that the backing path is accessible.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "vfs/vfs.h"
#include "vfs/vfs_internal_procs.h"
#include "s3_internal.h"
#include "s3_compound.h"
#include "s3_acl.h"
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
    chimera_s3_response_add_datav(evpl, request, &iov, 1);

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
        request->status    = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        goto request_drop;
    }

    snprintf(name, sizeof(name), "%.*s",
             request->bucket_namelen, request->bucket_name);
    snprintf(path, sizeof(path), "%.*s/%s",
             shared->bucket_root_pathlen, shared->bucket_root_path, name);

    chimera_s3_add_bucket(shared, name, path);

    snprintf(location, sizeof(location), "/%s", name);
    chimera_s3_response_add_header(request, "Location", location);

    request->status           = CHIMERA_S3_STATUS_OK;
    request->file_length      = 0;
    request->file_real_length = 0;
    request->file_offset      = 0;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
 request_drop:
    chimera_s3_request_drop(private_data);
} /* chimera_s3_create_bucket_mkdir_cb */

static void
chimera_s3_create_bucket_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    chimera_s3_create_bucket_mkdir_cb(status, NULL, private_data);
} /* chimera_s3_create_bucket_complete */

void
chimera_s3_create_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_vfs_compound     *compound;
    struct chimera_vfs_attrs         attributes = { 0 };

    if (!shared->bucket_root_pathlen) {
        request->status    = CHIMERA_S3_STATUS_NOT_IMPLEMENTED;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }
    attributes.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    attributes.va_mode     = S_IFDIR | chimera_s3_canned_acl_to_mode(
        request->canned_acl == CHIMERA_S3_CANNED_NONE ? CHIMERA_S3_CANNED_PUBLIC_READ :
        request->canned_acl, 1);
    attributes.va_uid = request->cred.uid;
    attributes.va_gid = request->cred.gid;
    compound          = chimera_vfs_compound_alloc(thread->vfs, &request->cred);
    chimera_vfs_compound_add_putfh(compound, shared->root_fh, shared->root_fh_len);
    chimera_vfs_compound_add_lookup_path(compound, shared->bucket_root_path,
                                         shared->bucket_root_pathlen, CHIMERA_VFS_ATTR_FH,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_create_path(compound, CHIMERA_VFS_COMPOUND_CREATE_DIR, request->bucket_name, request->
                                         bucket_namelen, NULL, 0, &attributes, CHIMERA_VFS_ATTR_FH, 0);
    chimera_s3_request_get(request);
    chimera_frontend_compound_submit(compound, chimera_s3_create_bucket_complete, request);
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

static void
chimera_s3_delbucket_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct s3_delbucket_ctx *ctx = private_data;

    for (int i = 0; i < ctx->ndirs; i++) {
        free(ctx->dirs[i]);
    }
    ctx->ndirs    = 0;
    ctx->has_file = 0;
} /* chimera_s3_delbucket_reset */

static void
chimera_s3_delbucket_root_status(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (*status == CHIMERA_VFS_ENOENT) {
        *status = CHIMERA_VFS_OK;
    }
} /* chimera_s3_delbucket_root_status */

static void
chimera_s3_delbucket_find_done(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct s3_delbucket_ctx         *ctx    = private_data;
    struct chimera_server_s3_shared *shared = ctx->request->thread->shared;
    int                              removal;

    if (*status) {
        return;
    }
    if (ctx->has_file) {
        *status = CHIMERA_VFS_ENOTEMPTY;
        return;
    }
    qsort(ctx->dirs, ctx->ndirs, sizeof(char *), chimera_s3_delbucket_depth_cmp);
    chimera_vfs_compound_add_remove_paths(compound, (const char *const *) ctx->dirs,
                                          ctx->ndirs, CHIMERA_VFS_REMOVE_ISDIR, 1);
    chimera_vfs_compound_add_putfh(compound, shared->root_fh, shared->root_fh_len);
    removal = chimera_vfs_compound_add_remove_path(compound, ctx->bucket_path,
                                                   ctx->bucket_path_len, CHIMERA_VFS_REMOVE_ISDIR);
    chimera_vfs_compound_set_op_callbacks(compound, removal, NULL, chimera_s3_delbucket_root_status, ctx);
} /* chimera_s3_delbucket_find_done */

static void
chimera_s3_delbucket_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_delbucket_ctx *ctx    = private_data;
    enum chimera_vfs_error   status = chimera_vfs_compound_status(compound);
    enum chimera_s3_status   result;

    chimera_s3_debug("DeleteBucket compound status %u, files %d, directories %d",
                     status, ctx->has_file, ctx->ndirs);
    if (status == CHIMERA_VFS_OK || status == CHIMERA_VFS_ENOENT) {
        result = CHIMERA_S3_STATUS_NO_CONTENT;
    } else if (status == CHIMERA_VFS_ENOTEMPTY) {
        result = CHIMERA_S3_STATUS_BUCKET_NOT_EMPTY;
    } else {
        result = chimera_s3_status_from_vfs(status, CHIMERA_S3_STATUS_INTERNAL_ERROR);
    }
    chimera_vfs_compound_free(compound);
    chimera_s3_delbucket_finish(ctx, result);
} /* chimera_s3_delbucket_compound_complete */

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
    chimera_s3_request_drop(ctx->request);
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
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(thread->evpl, request);
        }
        return;
    }

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;
    chimera_s3_request_get(request);

    ctx->bucket_path_len = snprintf(ctx->bucket_path, sizeof(ctx->bucket_path),
                                    "%.*s/%.*s",
                                    shared->bucket_root_pathlen, shared->bucket_root_path,
                                    request->bucket_namelen, request->bucket_name);

    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);
    chimera_vfs_compound_add_putfh(compound, shared->root_fh, shared->root_fh_len);
    chimera_vfs_compound_add_lookup_path(compound, ctx->bucket_path, ctx->bucket_path_len,
                                         CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
    int                          find = chimera_vfs_compound_add_find(compound,
                                                                      CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                                                                      chimera_s3_delbucket_reset,
                                                                      chimera_s3_delbucket_filter,
                                                                      chimera_s3_delbucket_collect, ctx);
    chimera_vfs_compound_set_op_callbacks(compound, find, NULL, chimera_s3_delbucket_find_done, ctx);
    chimera_frontend_compound_submit(compound, chimera_s3_delbucket_compound_complete, ctx);
} /* chimera_s3_delete_bucket */

/* ----------------------------------------------------------------- HeadBucket */

static void
chimera_s3_head_bucket_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{

    struct chimera_s3_request *request = private_data;
    enum chimera_vfs_error     status  = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->status = status == CHIMERA_VFS_OK ? CHIMERA_S3_STATUS_OK :
        status == CHIMERA_VFS_ENOENT ? CHIMERA_S3_STATUS_NO_SUCH_BUCKET :
        chimera_s3_status_from_vfs(status, CHIMERA_S3_STATUS_INTERNAL_ERROR);
    request->file_length      = 0;
    request->file_real_length = 0;
    request->file_offset      = 0;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(request->thread->evpl, request);
    }
    chimera_s3_request_drop(private_data);
} /* chimera_s3_head_bucket_complete */

void
chimera_s3_head_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound = chimera_s3_compound_alloc(request);

    chimera_s3_request_get(request);
    chimera_frontend_compound_submit(compound, chimera_s3_head_bucket_complete, request);
} /* chimera_s3_head_bucket */
