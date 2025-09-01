// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#define BUF_SIZE 16384     /* 16 KiB */

typedef struct {
    struct test_env *env;
    FILE            *fp;
    size_t           size_left;
    S3Status         status;
    int              finished;
    pthread_mutex_t  mutex;
    pthread_cond_t   cond;
} ObjContext;

/* -------- Data callback: feeds S3_put_object() with file bytes -------- */
static int
put_object_data_cb(
    int   buffer_size,
    char *buffer,
    void *callback_data)
{
    ObjContext *ctx = (ObjContext *) callback_data;

    size_t      want = (ctx->size_left < (size_t) buffer_size)
                    ? ctx->size_left : (size_t) buffer_size;

    ctx->size_left -= want;

    return want;
} /* put_object_data_cb */

static void
complete_cb(
    S3Status              status,
    const S3ErrorDetails *error,
    void                 *callback_data)
{
    ObjContext *ctx = (ObjContext *) callback_data;

    if (error->message == NULL) {
        fprintf(stderr, "Success\n");
    } else {
        fprintf(stderr, "Failed: %s (%d)\n", S3_get_status_name(status), status);
        if (error && error->message) {
            fprintf(stderr, "  S3 Error: %s\n", error->message);
        }
    }

    pthread_mutex_lock(&ctx->mutex);
    ctx->status   = error->message ? status : S3StatusOK;
    ctx->finished = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
} /* complete_cb */

static S3Status
get_object_data_cb(
    int         buffer_size,
    const char *buffer,
    void       *callback_data)
{
    ObjContext *ctx = (ObjContext *) callback_data;

    size_t      want = (ctx->size_left < (size_t) buffer_size)
                    ? ctx->size_left : (size_t) buffer_size;

    ctx->size_left -= want;

    return want;
} /* get_object_data_cb */

static inline void
wait_for_completion(ObjContext *ctx)
{
    pthread_mutex_lock(&ctx->mutex);
    while (!ctx->finished) {
        pthread_cond_wait(&ctx->cond, &ctx->mutex);
    }
    pthread_mutex_unlock(&ctx->mutex);

    if (ctx->status != S3StatusOK) {
        libs3_test_fail(ctx->env);
        exit(1);
    }
} /* wait_for_completion */

void
put_object(
    struct test_env *env,
    const char      *path,
    size_t           size)
{
    S3PutObjectHandler put_handler = {
        .responseHandler.completeCallback = &complete_cb,
        .putObjectDataCallback            = &put_object_data_cb
    };

    S3BucketContext    bucket_ctx = {
        .hostName        = "localhost:5000",
        .bucketName      = "mybucket",
        .protocol        = S3ProtocolHTTP,
        .uriStyle        = S3UriStyleVirtualHost, // S3UriStylePath,
        .accessKeyId     = "myaccessid",
        .secretAccessKey = "mysecretkey",
    };

    ObjContext         ctx = {
        .env       = env,
        .size_left = size,
        .finished  = 0,
        .mutex     = PTHREAD_MUTEX_INITIALIZER,
        .cond      = PTHREAD_COND_INITIALIZER,
    };

    S3_put_object(&bucket_ctx,
                  path,
                  size,
                  NULL,                   /* optional MD5 checksum  */
                  NULL,                   /* optional meta headers  */
                  &put_handler,
                  &ctx);

    wait_for_completion(&ctx);
} /* put_object */

void
get_object(
    struct test_env *env,
    const char      *path,
    size_t           offset,
    size_t           size)
{
    S3GetObjectHandler get_handler = {
        .responseHandler.completeCallback = &complete_cb,
        .getObjectDataCallback            = &get_object_data_cb
    };

    S3BucketContext    bucket_ctx = {
        .hostName        = "localhost:5000",
        .bucketName      = "mybucket",
        .protocol        = S3ProtocolHTTP,
        .uriStyle        = S3UriStyleVirtualHost, // S3UriStylePath,
        .accessKeyId     = "myaccessid",
        .secretAccessKey = "mysecretkey",
    };

    ObjContext         ctx = {
        .env       = env,
        .size_left = size,
        .finished  = 0,
        .mutex     = PTHREAD_MUTEX_INITIALIZER,
        .cond      = PTHREAD_COND_INITIALIZER,
    };

    S3_get_object(&bucket_ctx,
                  path,
                  NULL,
                  offset,
                  size,
                  NULL,
                  &get_handler,
                  &ctx);

    wait_for_completion(&ctx);
} /* get_object */

void
head_object(
    struct test_env *env,
    const char      *path)
{
    S3ResponseHandler head_handler = {
        .completeCallback = &complete_cb,
    };

    S3BucketContext   bucket_ctx = {
        .hostName        = "localhost:5000",
        .bucketName      = "mybucket",
        .protocol        = S3ProtocolHTTP,
        .uriStyle        = S3UriStyleVirtualHost, // S3UriStylePath,
        .accessKeyId     = "myaccessid",
        .secretAccessKey = "mysecretkey",
    };

    ObjContext        ctx = {
        .env       = env,
        .size_left = 0,
        .finished  = 0,
        .mutex     = PTHREAD_MUTEX_INITIALIZER,
        .cond      = PTHREAD_COND_INITIALIZER,
    };

    S3_head_object(&bucket_ctx,
                   path,
                   NULL,
                   &head_handler,
                   &ctx);

    wait_for_completion(&ctx);
} /* head_object */

void
delete_object(
    struct test_env *env,
    const char      *path)
{
    S3ResponseHandler head_handler = {
        .completeCallback = &complete_cb,
    };

    S3BucketContext   bucket_ctx = {
        .hostName        = "localhost:5000",
        .bucketName      = "mybucket",
        .protocol        = S3ProtocolHTTP,
        .uriStyle        = S3UriStyleVirtualHost, // S3UriStylePath,
        .accessKeyId     = "myaccessid",
        .secretAccessKey = "mysecretkey",
    };

    ObjContext        ctx = {
        .env       = env,
        .size_left = 0,
        .finished  = 0,
        .mutex     = PTHREAD_MUTEX_INITIALIZER,
        .cond      = PTHREAD_COND_INITIALIZER,
    };

    S3_delete_object(&bucket_ctx,
                     path,
                     NULL,
                     &head_handler,
                     &ctx);

    wait_for_completion(&ctx);
} /* delete_object */

static S3Status
list_bucket_cb(
    int                        isTruncated,
    const char                *nextMarker,
    int                        contentsCount,
    const S3ListBucketContent *contents,
    int                        commonPrefixesCount,
    const char               **commonPrefixes,
    void                      *callbackData)
{
    ObjContext *ctx = (ObjContext *) callbackData;

    if (!ctx) {
        fprintf(stderr, "Error: callbackData is NULL\n");
        return S3StatusInternalError;
    }

    if (contentsCount > 0 && !contents) {
        fprintf(stderr, "Error: contents is NULL but contentsCount > 0\n");
        return S3StatusInternalError;
    }

    if (commonPrefixesCount > 0 && !commonPrefixes) {
        fprintf(stderr, "Error: commonPrefixes is NULL but commonPrefixesCount > 0\n");
        return S3StatusInternalError;
    }

    fprintf(stderr, "List bucket callback: isTruncated=%d, contentsCount=%d, commonPrefixesCount=%d\n",
            isTruncated, contentsCount, commonPrefixesCount);

    if (contents) {
        for (int i = 0; i < contentsCount && i < 1000; i++) {
            if (!contents[i].key) {
                fprintf(stderr, "Warning: contents[%d].key is NULL\n", i);
                continue;
            }
            fprintf(stderr, "Key: %s, Size: %llu\n",
                    contents[i].key,
                    (unsigned long long) contents[i].size);
        }
    }

    if (commonPrefixes) {
        for (int i = 0; i < commonPrefixesCount && i < 1000; i++) {
            if (!commonPrefixes[i]) {
                fprintf(stderr, "Warning: commonPrefixes[%d] is NULL\n", i);
                continue;
            }
            fprintf(stderr, "Common Prefix: %s\n", commonPrefixes[i]);
        }
    }

    return S3StatusOK;
} /* list_bucket_cb */

static S3Status
properties_cb(
    const S3ResponseProperties *properties,
    void                       *callbackData)
{
    return S3StatusOK;
} /* properties_cb */

void
list_object(
    struct test_env *env,
    const char      *path)
{
    S3ListBucketHandler list_handler = {
        .responseHandler        = {
            .propertiesCallback = &properties_cb,
            .completeCallback   = &complete_cb
        },
        .listBucketCallback     = &list_bucket_cb,
    };

    S3BucketContext     bucket_ctx = {
        .hostName        = "localhost:5000",
        .bucketName      = "mybucket",
        .protocol        = S3ProtocolHTTP,
        .uriStyle        = env->path_style ? S3UriStylePath : S3UriStyleVirtualHost,
        .accessKeyId     = "myaccessid",
        .secretAccessKey = "mysecretkey",
    };

    ObjContext          ctx = {
        .env       = env,
        .size_left = 0,
        .finished  = 0,
        .mutex     = PTHREAD_MUTEX_INITIALIZER,
        .cond      = PTHREAD_COND_INITIALIZER,
    };

    S3_list_bucket(&bucket_ctx,
                   path,
                   NULL,
                   "/",
                   100,
                   NULL,
                   &list_handler,
                   &ctx);

    wait_for_completion(&ctx);
} /* delete_object */