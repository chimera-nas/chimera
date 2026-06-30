// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <ctype.h>
#include <time.h>
#include <pthread.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/format.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_multipart.h"
#include "s3_etag.h"
#include "s3_procs.h"
#include "s3.h"
#include "s3_tagging.h"

static _Atomic uint64_t chimera_s3_multipart_id_counter;

/* ----- table primitives ----- */

static inline uint32_t
chimera_s3_multipart_hash(const char *upload_id)
{
    uint32_t h = 0;

    for (int i = 0; i < CHIMERA_S3_UPLOAD_ID_LEN; i++) {
        h = h * 31u + (unsigned char) upload_id[i];
    }
    return h;
} /* chimera_s3_multipart_hash */

struct chimera_s3_multipart_table *
chimera_s3_multipart_table_create(int nbuckets)
{
    struct chimera_s3_multipart_table *table;

    table           = calloc(1, sizeof(*table));
    table->nbuckets = nbuckets;
    table->buckets  = calloc(nbuckets, sizeof(*table->buckets));
    pthread_rwlock_init(&table->lock, NULL);

    return table;
} /* chimera_s3_multipart_table_create */

/* Forward declaration: cleanup-and-free a single part, async if temp file. */
static void chimera_s3_multipart_part_destroy_async(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_part          *part);

static void
chimera_s3_multipart_upload_free_now(struct chimera_s3_multipart_upload *upload)
{
    free(upload->bucket_name);
    free(upload->object_key);
    free(upload->tagging);
    pthread_mutex_destroy(&upload->lock);
    free(upload);
} /* chimera_s3_multipart_upload_free_now */

void
chimera_s3_multipart_table_destroy(struct chimera_s3_multipart_table *table)
{
    struct chimera_s3_multipart_upload *upload, *next;
    struct chimera_s3_part             *part, *part_next;

    /* Called from server destroy AFTER all worker threads are torn down, so
     * there is no VFS thread to release file_handles against. Just free
     * heap-allocated structs; the OS reclaims open file descriptors and any
     * tmp-files on disk are left as collateral. Phase 2 will pair this with
     * a graceful pre-stop drain. */
    for (int i = 0; i < table->nbuckets; i++) {
        upload = table->buckets[i];
        while (upload) {
            next = upload->next;
            part = upload->parts;
            while (part) {
                part_next = part->next;
                free(part);
                part = part_next;
            }
            chimera_s3_multipart_upload_free_now(upload);
            upload = next;
        }
    }

    pthread_rwlock_destroy(&table->lock);
    free(table->buckets);
    free(table);
} /* chimera_s3_multipart_table_destroy */

void
chimera_s3_multipart_generate_id(
    struct chimera_server_s3_thread *thread,
    char                            *out)
{
    struct {
        uint64_t        counter;
        uint64_t        thread_ptr;
        struct timespec ts;
    } seed;
    struct timespec ts;
    uint64_t        hash[2];
    XXH128_hash_t   h;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    memset(&seed, 0, sizeof(seed));
    seed.counter    = atomic_fetch_add(&chimera_s3_multipart_id_counter, 1);
    seed.thread_ptr = (uint64_t) (uintptr_t) thread;
    seed.ts         = ts;

    h       = XXH3_128bits((const void *) &seed, sizeof(seed));
    hash[0] = h.low64;
    hash[1] = h.high64;

    format_hex(out, CHIMERA_S3_UPLOAD_ID_LEN + 1, hash, sizeof(hash));
    out[CHIMERA_S3_UPLOAD_ID_LEN] = '\0';
} /* chimera_s3_multipart_generate_id */

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_insert(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    const char                        *bucket_name,
    int                                bucket_namelen,
    const uint8_t                     *bucket_fh,
    int                                bucket_fhlen,
    const char                        *object_key,
    int                                object_keylen)
{
    struct chimera_s3_multipart_upload *upload;
    uint32_t                            bucket_idx;

    upload = calloc(1, sizeof(*upload));
    memcpy(upload->upload_id, upload_id, CHIMERA_S3_UPLOAD_ID_LEN);
    upload->upload_id[CHIMERA_S3_UPLOAD_ID_LEN] = '\0';

    upload->bucket_name = malloc(bucket_namelen + 1);
    memcpy(upload->bucket_name, bucket_name, bucket_namelen);
    upload->bucket_name[bucket_namelen] = '\0';
    upload->bucket_namelen              = bucket_namelen;

    upload->object_key = malloc(object_keylen + 1);
    memcpy(upload->object_key, object_key, object_keylen);
    upload->object_key[object_keylen] = '\0';
    upload->object_keylen             = object_keylen;

    memcpy(upload->bucket_fh, bucket_fh, bucket_fhlen);
    upload->bucket_fhlen = bucket_fhlen;

    clock_gettime(CLOCK_REALTIME, &upload->created);
    pthread_mutex_init(&upload->lock, NULL);
    upload->refcount = 1; /* the table itself */
    upload->removed  = 0;
    upload->parts    = NULL;

    bucket_idx = chimera_s3_multipart_hash(upload->upload_id) % table->nbuckets;

    pthread_rwlock_wrlock(&table->lock);
    upload->prev = NULL;
    upload->next = table->buckets[bucket_idx];
    if (table->buckets[bucket_idx]) {
        table->buckets[bucket_idx]->prev = upload;
    }
    table->buckets[bucket_idx] = upload;
    pthread_rwlock_unlock(&table->lock);

    return upload;
} /* chimera_s3_multipart_table_insert */

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_lookup(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    int                                upload_idlen)
{
    struct chimera_s3_multipart_upload *upload;
    uint32_t                            bucket_idx;

    if (upload_idlen != CHIMERA_S3_UPLOAD_ID_LEN) {
        return NULL;
    }

    bucket_idx = chimera_s3_multipart_hash(upload_id) % table->nbuckets;

    pthread_rwlock_rdlock(&table->lock);
    for (upload = table->buckets[bucket_idx]; upload; upload = upload->next) {
        if (memcmp(upload->upload_id, upload_id, CHIMERA_S3_UPLOAD_ID_LEN) == 0 &&
            !upload->removed) {
            pthread_mutex_lock(&upload->lock);
            upload->refcount++;
            pthread_mutex_unlock(&upload->lock);
            break;
        }
    }
    pthread_rwlock_unlock(&table->lock);

    return upload;
} /* chimera_s3_multipart_table_lookup */

void
chimera_s3_multipart_upload_release(
    struct chimera_server_s3_thread    *thread,
    struct chimera_s3_multipart_upload *upload)
{
    int free_now = 0;

    pthread_mutex_lock(&upload->lock);
    upload->refcount--;
    if (upload->refcount == 0 && upload->removed) {
        free_now = 1;
    }
    pthread_mutex_unlock(&upload->lock);

    if (free_now) {
        struct chimera_s3_part *part, *next;
        part = upload->parts;
        while (part) {
            next = part->next;
            chimera_s3_multipart_part_destroy_async(thread, part);
            part = next;
        }
        chimera_s3_multipart_upload_free_now(upload);
    }
} /* chimera_s3_multipart_upload_release */

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_detach(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    int                                upload_idlen)
{
    struct chimera_s3_multipart_upload *upload;
    uint32_t                            bucket_idx;

    if (upload_idlen != CHIMERA_S3_UPLOAD_ID_LEN) {
        return NULL;
    }

    bucket_idx = chimera_s3_multipart_hash(upload_id) % table->nbuckets;

    pthread_rwlock_wrlock(&table->lock);
    for (upload = table->buckets[bucket_idx]; upload; upload = upload->next) {
        if (memcmp(upload->upload_id, upload_id, CHIMERA_S3_UPLOAD_ID_LEN) == 0 &&
            !upload->removed) {
            if (upload->prev) {
                upload->prev->next = upload->next;
            } else {
                table->buckets[bucket_idx] = upload->next;
            }
            if (upload->next) {
                upload->next->prev = upload->prev;
            }
            upload->prev    = NULL;
            upload->next    = NULL;
            upload->removed = 1;
            break;
        }
    }
    pthread_rwlock_unlock(&table->lock);

    return upload;
} /* chimera_s3_multipart_table_detach */

/* ----- async part destruction (release handle + unlink tmp file) ----- */

struct chimera_s3_part_destroy_ctx {
    struct chimera_server_s3_thread *thread;
    struct chimera_s3_part          *part;
    struct chimera_vfs_open_handle  *dir_handle;
};

static void
chimera_s3_part_destroy_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_part_destroy_ctx *ctx = private_data;

    chimera_vfs_release(ctx->thread->vfs, ctx->dir_handle);
    free(ctx->part);
    free(ctx);
} /* chimera_s3_part_destroy_remove_callback */

static void
chimera_s3_part_destroy_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_part_destroy_ctx *ctx = private_data;

    if (error_code) {
        /* Cannot open dir; leak the tmp file rather than crashing. */
        free(ctx->part);
        free(ctx);
        return;
    }

    ctx->dir_handle = oh;

    chimera_vfs_remove_at(
        ctx->thread->vfs,
        &ctx->thread->shared->cred, NULL,
        oh,
        ctx->part->tmp_name,
        ctx->part->tmp_name_len,
        NULL,
        0,
        0,
        0,
        NULL,
        chimera_s3_part_destroy_remove_callback,
        ctx);
} /* chimera_s3_part_destroy_open_dir_callback */

static void
chimera_s3_multipart_part_destroy_async(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_part          *part)
{
    struct chimera_s3_part_destroy_ctx *ctx;

    if (part->file_handle) {
        chimera_vfs_release(thread->vfs, part->file_handle);
        part->file_handle = NULL;
    }

    if (part->tmp_name_len == 0) {
        /* Unlinked file — auto-deleted on handle release. */
        free(part);
        return;
    }

    ctx         = calloc(1, sizeof(*ctx));
    ctx->thread = thread;
    ctx->part   = part;

    chimera_vfs_open_fh(
        thread->vfs,
        &thread->shared->cred, NULL,
        part->dir_fh,
        part->dir_fhlen,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_s3_part_destroy_open_dir_callback,
        ctx);
} /* chimera_s3_multipart_part_destroy_async */

/* ----- response helpers ----- */

static inline void
chimera_s3_mp_append(
    char      **p,
    const char *fmt,
    ...) __attribute__((format(printf, 2, 3)));

static inline void
chimera_s3_mp_append(
    char      **p,
    const char *fmt,
    ...)
{
    va_list ap;

    va_start(ap, fmt);
    *p += vsprintf(*p, fmt, ap);
    va_end(ap);
} /* chimera_s3_mp_append */

static void
chimera_s3_mp_send_response(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    char                      *body_start,
    char                      *body_end)
{
    evpl_iovec_set_length(&request->multipart.response,
                          body_end - body_start);

    evpl_http_request_add_datav(request->http_request,
                                &request->multipart.response, 1);

    request->file_length      = body_end - body_start;
    request->file_real_length = request->file_length;
    request->file_offset      = 0;
    request->is_list          = 1; /* triggers application/xml Content-Type */
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_mp_send_response */

static void
chimera_s3_mp_format_etag(
    char          *out,
    int            out_len,
    const uint64_t etag[2])
{
    char *p = out;

    *p++ = '\"';
    p   += format_hex(p, out_len - (p - out), etag, sizeof(uint64_t) * 2);
    *p++ = '\"';
    *p   = '\0';
} /* chimera_s3_mp_format_etag */

static void
chimera_s3_mp_format_date(
    char                  *buf,
    size_t                 len,
    const struct timespec *ts)
{
    struct tm tm;
    int       n;

    gmtime_r(&ts->tv_sec, &tm);
    n = strftime(buf, len, "%Y-%m-%dT%H:%M:%S", &tm);
    if (n > 0) {
        snprintf(buf + n, len - n, ".%03ldZ", ts->tv_nsec / 1000000);
    }
} /* chimera_s3_mp_format_date */

/* ----- CreateMultipartUpload ----- */

void
chimera_s3_create_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;
    char                                upload_id[CHIMERA_S3_UPLOAD_ID_LEN + 1];
    char                               *bp, *body_start;

    chimera_s3_multipart_generate_id(thread, upload_id);

    upload = chimera_s3_multipart_table_insert(
        shared->multipart_table,
        upload_id,
        request->bucket_name,
        request->bucket_namelen,
        request->bucket_fh,
        request->bucket_fhlen,
        request->path,
        request->path_len);

    /* Remember an x-amz-tagging header so the tags are applied to the assembled
     * object on CompleteMultipartUpload. */
    {
        const char *tagging_hdr = evpl_http_request_header(
            request->http_request, "x-amz-tagging");

        if (tagging_hdr && tagging_hdr[0]) {
            upload->tagging = strdup(tagging_hdr);
        }
    }

    evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    chimera_s3_mp_append(&bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_mp_append(&bp, "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    chimera_s3_mp_append(&bp, "  <Bucket>%.*s</Bucket>\n",
                         request->bucket_namelen, request->bucket_name);
    chimera_s3_mp_append(&bp, "  <Key>%.*s</Key>\n",
                         request->path_len, request->path);
    chimera_s3_mp_append(&bp, "  <UploadId>%.*s</UploadId>\n",
                         CHIMERA_S3_UPLOAD_ID_LEN, upload_id);
    chimera_s3_mp_append(&bp, "</InitiateMultipartUploadResult>\n");

    chimera_s3_mp_send_response(evpl, request, body_start, bp);
} /* chimera_s3_create_multipart_upload */

/* ----- UploadPart ----- */

static void
chimera_s3_upload_part_finish(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread    *thread = request->thread;
    struct evpl                        *evpl   = thread->evpl;
    struct chimera_s3_multipart_upload *upload = request->multipart.upload;
    struct chimera_s3_part             *part, *prev, *cur;
    struct chimera_s3_part             *replaced = NULL;
    char                                etag_hex[80];

    part               = calloc(1, sizeof(*part));
    part->part_number  = request->multipart.part_number;
    part->file_handle  = request->file_handle;
    part->size         = request->file_cur_offset;
    part->tmp_name_len = request->multipart.tmp_name_len;
    if (part->tmp_name_len > 0) {
        memcpy(part->tmp_name, request->multipart.tmp_name,
               part->tmp_name_len);
        memcpy(part->dir_fh, request->dir_handle->fh,
               request->dir_handle->fh_len);
        part->dir_fhlen = request->dir_handle->fh_len;
    }
    clock_gettime(CLOCK_REALTIME, &part->uploaded);

    /* Fake per-part ETag: XXH3_128 of fixed metadata (size + part_number +
     * tmp_name + dir_fh).  Cannot use chimera_s3_compute_etag because we
     * don't have the file's mtime attribute yet. */
    {
        struct {
            int64_t size;
            int32_t part_number;
            int32_t tmp_name_len;
            uint8_t dir_fh[CHIMERA_VFS_FH_SIZE];
            char    tmp_name[64];
        } __attribute__((packed)) seed;
        XXH128_hash_t h;

        memset(&seed, 0, sizeof(seed));
        seed.size         = part->size;
        seed.part_number  = part->part_number;
        seed.tmp_name_len = part->tmp_name_len;
        if (part->tmp_name_len > 0) {
            memcpy(seed.tmp_name, part->tmp_name, part->tmp_name_len);
            memcpy(seed.dir_fh, part->dir_fh, part->dir_fhlen);
        }
        h             = XXH3_128bits((const void *) &seed, sizeof(seed));
        part->etag[0] = h.low64;
        part->etag[1] = h.high64;
    }

    /* Insert sorted; replace if part_number already present. */
    pthread_mutex_lock(&upload->lock);
    prev = NULL;
    cur  = upload->parts;
    while (cur && cur->part_number < part->part_number) {
        prev = cur;
        cur  = cur->next;
    }
    if (cur && cur->part_number == part->part_number) {
        /* Replace existing part. */
        replaced   = cur;
        part->next = cur->next;
        if (prev) {
            prev->next = part;
        } else {
            upload->parts = part;
        }
    } else {
        part->next = cur;
        if (prev) {
            prev->next = part;
        } else {
            upload->parts = part;
        }
    }
    pthread_mutex_unlock(&upload->lock);

    if (replaced) {
        chimera_s3_multipart_part_destroy_async(thread, replaced);
    }

    chimera_s3_mp_format_etag(etag_hex, sizeof(etag_hex), part->etag);

    /* Release the directory handle (no longer needed). */
    chimera_vfs_release(thread->vfs, request->dir_handle);
    request->dir_handle = NULL;

    /* Release our upload reference (only takes effect if upload was detached). */
    chimera_s3_multipart_upload_release(thread, upload);
    request->multipart.upload = NULL;

    if (request->multipart.is_copy) {
        /* UploadPartCopy: return a <CopyPartResult> document carrying the
         * part's ETag and LastModified rather than a bare ETag header. */
        char *bp, *body_start;
        char  date_buf[64];

        chimera_s3_mp_format_date(date_buf, sizeof(date_buf), &part->uploaded);

        evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);
        bp = body_start = evpl_iovec_data(&request->multipart.response);

        chimera_s3_mp_append(&bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        chimera_s3_mp_append(&bp, "<CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
        chimera_s3_mp_append(&bp, "  <LastModified>%s</LastModified>\n", date_buf);
        chimera_s3_mp_append(&bp, "  <ETag>%s</ETag>\n", etag_hex);
        chimera_s3_mp_append(&bp, "</CopyPartResult>\n");

        chimera_s3_mp_send_response(evpl, request, body_start, bp);
        return;
    }

    /* Plain UploadPart: attach ETag header, empty body. */
    evpl_http_request_add_header(request->http_request, "ETag", etag_hex);

    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_upload_part_finish */

static void
chimera_s3_upload_part_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_io            *io      = private_data;
    struct chimera_s3_request       *request = io->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    evpl_iovecs_release(evpl, io->iov, io->niov);
    chimera_s3_io_free(thread, io);

    request->io_pending--;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    if (request->io_pending == 0 &&
        request->vfs_state == CHIMERA_S3_VFS_STATE_RECVED) {
        chimera_s3_upload_part_finish(request);
    }
} /* chimera_s3_upload_part_write_callback */

void
chimera_s3_upload_part_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_config        *config = shared->config;
    struct chimera_s3_io            *io;
    uint64_t                         avail;
    int                              final;

    final = (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED);

 again:

    avail = evpl_http_request_get_data_avail(request->http_request);

    if (avail < config->io_size && !final) {
        return;
    }

    if (avail > config->io_size) {
        avail = config->io_size;
    }

    if (avail == 0 && final) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_RECVED;

        if (request->io_pending == 0) {
            chimera_s3_upload_part_finish(request);
        }
        return;
    }

    io = chimera_s3_io_alloc(thread, request);

    if (request->chunked) {
        /* De-chunk the aws-chunked framing into a fresh buffer. Decoding only
         * strips framing, so the decoded length never exceeds the raw input
         * and a single output iovec of `avail` bytes always suffices. The raw
         * input is pulled into a separate scratch array and released once
         * copied; the decoded output is allocated directly into io->iov[0] so
         * libevpl's iovec ownership tracking stays intact. */
        struct evpl_iovec in_iov[CHIMERA_S3_IOV_MAX];
        int               in_niov;
        int               out_len = 0;
        int               i;

        in_niov = evpl_http_request_get_datav(evpl, request->http_request,
                                              in_iov, avail);

        evpl_iovec_alloc(evpl, avail, 0, 1, 0, &io->iov[0]);

        for (i = 0; i < in_niov; i++) {
            s3_chunk_decode(&request->chunk,
                            evpl_iovec_data(&in_iov[i]),
                            evpl_iovec_length(&in_iov[i]),
                            evpl_iovec_data(&io->iov[0]),
                            &out_len);
        }

        evpl_iovecs_release(evpl, in_iov, in_niov);

        if (request->chunk.error) {
            evpl_iovec_release(evpl, &io->iov[0]);
            chimera_s3_io_free(thread, io);
            request->status    = CHIMERA_S3_STATUS_BAD_REQUEST;
            request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
        }

        if (out_len == 0) {
            /* This read carried only framing (e.g. a chunk header or the
             * trailer); nothing to write yet. */
            evpl_iovec_release(evpl, &io->iov[0]);
            chimera_s3_io_free(thread, io);
            goto again;
        }

        evpl_iovec_set_length(&io->iov[0], out_len);
        io->niov = 1;
        avail    = out_len;
    } else {
        io->niov = evpl_http_request_get_datav(evpl, request->http_request,
                                               io->iov, avail);
    }

    request->io_pending++;

    chimera_vfs_write(thread->vfs, &thread->shared->cred, NULL,
                      request->file_handle,
                      request->file_cur_offset,
                      avail,
                      1,
                      0,
                      0,
                      io->iov,
                      io->niov,
                      chimera_s3_upload_part_write_callback,
                      io);

    request->file_cur_offset += avail;

    goto again;
} /* chimera_s3_upload_part_recv */

static void
chimera_s3_upload_part_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
        chimera_s3_multipart_upload_release(thread, request->multipart.upload);
        request->multipart.upload = NULL;
        request->status           = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    request->file_handle = oh;
    request->vfs_state   = CHIMERA_S3_VFS_STATE_RECV;

    chimera_s3_upload_part_recv(evpl, request);
} /* chimera_s3_upload_part_create_unlinked_callback */

static void
chimera_s3_upload_part_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
        chimera_s3_multipart_upload_release(thread, request->multipart.upload);
        request->multipart.upload = NULL;
        request->status           = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    request->file_handle = oh;
    request->vfs_state   = CHIMERA_S3_VFS_STATE_RECV;

    chimera_s3_upload_part_recv(evpl, request);
} /* chimera_s3_upload_part_create_callback */

static void
chimera_s3_upload_part_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *module;

    if (error_code) {
        chimera_s3_multipart_upload_release(thread, request->multipart.upload);
        request->multipart.upload = NULL;
        request->status           = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    request->dir_handle = oh;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    module = chimera_vfs_get_module(thread->vfs, oh->fh, oh->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {
        request->multipart.tmp_name_len = 0;

        chimera_vfs_create_unlinked(
            thread->vfs, &thread->shared->cred, NULL,
            oh->fh,
            oh->fh_len,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            chimera_s3_upload_part_create_unlinked_callback,
            request);
    } else {
        request->multipart.tmp_name_len = snprintf(
            request->multipart.tmp_name,
            sizeof(request->multipart.tmp_name),
            "._chimera_mpu_%.16s_%d",
            request->multipart.upload_id,
            request->multipart.part_number);

        chimera_vfs_open_at(
            thread->vfs, &thread->shared->cred, NULL,
            oh,
            request->multipart.tmp_name,
            request->multipart.tmp_name_len,
            CHIMERA_VFS_OPEN_CREATE,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            0,
            0,
            chimera_s3_upload_part_create_callback,
            request);
    }
} /* chimera_s3_upload_part_open_dir_callback */

static void
chimera_s3_upload_part_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        chimera_s3_multipart_upload_release(thread, request->multipart.upload);
        request->multipart.upload = NULL;
        request->status           = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    chimera_vfs_open_fh(
        thread->vfs, &thread->shared->cred, NULL,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_s3_upload_part_open_dir_callback,
        request);
} /* chimera_s3_upload_part_lookup_callback */

void
chimera_s3_upload_part(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;
    const char                         *slash;
    const char                         *dirpath = request->path;
    int                                 dirpathlen;

    /* AWS spec: part numbers must be in [1, 10000]. */
    if (request->multipart.part_number < 1 ||
        request->multipart.part_number > 10000) {
        request->status    = CHIMERA_S3_STATUS_INVALID_PART_NUMBER;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    upload = chimera_s3_multipart_table_lookup(
        shared->multipart_table,
        request->multipart.upload_id,
        request->multipart.upload_idlen);

    if (!upload) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_UPLOAD;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    request->multipart.upload  = upload;
    request->multipart.is_copy = 0;

    slash = rindex(request->path, '/');

    if (slash) {
        dirpathlen    = slash - request->path;
        request->name = slash + 1;
        while (*request->name == '/') {
            request->name++;
        }
    } else {
        dirpath       = "/";
        dirpathlen    = 1;
        request->name = request->path;
    }
    request->name_len = strlen(request->name);

    request->io_pending           = 0;
    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    /* Use chimera_vfs_create so the parent directory chain is materialized
     * lazily (mirrors PUT behavior). */
    chimera_vfs_create(
        thread->vfs, &thread->shared->cred, NULL,
        request->bucket_fh,
        request->bucket_fhlen,
        dirpath,
        dirpathlen,
        &request->set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_s3_upload_part_lookup_callback,
        request);
} /* chimera_s3_upload_part */

/* ----- UploadPartCopy ----- */

/*
 * Copy a byte range of an existing object into an in-progress multipart
 * upload as part N. The source object is resolved (like CopyObject), the
 * requested range is read and written into a freshly created part file, and
 * the part is registered with the upload via the shared finish path so it
 * participates in a later CompleteMultipartUpload.
 */

struct chimera_s3_upload_copy_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *src_handle;
    enum chimera_s3_upc_mode {
        CHIMERA_S3_UPC_COPY,
        CHIMERA_S3_UPC_RW,
    } mode;
    int64_t                         src_first; /* first byte offset in source */
    int64_t                         length; /* total bytes to copy         */
    int64_t                         copied; /* bytes copied so far         */
    int                             src_bucket_namelen;
    int                             src_key_len;
    int                             rw_niov;
    char                            src_bucket_name[256];
    char                            src_key[1024];
    struct evpl_iovec               rw_iov[CHIMERA_S3_IOV_MAX];
};

/*
 * Parse an x-amz-copy-source value of the form "[/]bucket/key[?versionId=..]"
 * into the ctx's source bucket name and key. Returns 0 on success, -1 if
 * malformed.
 */
static int
chimera_s3_upc_parse_source(
    struct chimera_s3_upload_copy_ctx *ctx,
    const char                        *copy_source)
{
    const char *p = copy_source;
    const char *slash, *qmark;
    int         bucket_len, key_len, i, o;

    while (*p == '/') {
        p++;
    }

    slash = strchr(p, '/');
    if (!slash || slash[1] == '\0') {
        return -1;
    }

    bucket_len = slash - p;
    if (bucket_len <= 0 || bucket_len >= (int) sizeof(ctx->src_bucket_name)) {
        return -1;
    }

    key_len = strlen(slash + 1);
    qmark   = memchr(slash + 1, '?', key_len);
    if (qmark) {
        key_len = qmark - (slash + 1);
    }
    if (key_len <= 0 || key_len >= (int) sizeof(ctx->src_key)) {
        return -1;
    }

    /* Percent-decode both components in place. */
    o = 0;
    for (i = 0; i < bucket_len; ) {
        if (p[i] == '%' && i + 2 < bucket_len &&
            isxdigit((unsigned char) p[i + 1]) &&
            isxdigit((unsigned char) p[i + 2])) {
            char hex[3] = { p[i + 1], p[i + 2], '\0' };
            ctx->src_bucket_name[o++] = (char) strtol(hex, NULL, 16);
            i                        += 3;
        } else {
            ctx->src_bucket_name[o++] = p[i++];
        }
    }
    ctx->src_bucket_name[o] = '\0';
    ctx->src_bucket_namelen = o;

    o = 0;
    for (i = 0; i < key_len; ) {
        if (slash[1 + i] == '%' && i + 2 < key_len &&
            isxdigit((unsigned char) slash[1 + i + 1]) &&
            isxdigit((unsigned char) slash[1 + i + 2])) {
            char hex[3] = { slash[1 + i + 1], slash[1 + i + 2], '\0' };
            ctx->src_key[o++] = (char) strtol(hex, NULL, 16);
            i                += 3;
        } else {
            ctx->src_key[o++] = slash[1 + i++];
        }
    }
    ctx->src_key[o]  = '\0';
    ctx->src_key_len = o;

    return 0;
} /* chimera_s3_upc_parse_source */

/*
 * Parse "bytes=first-last" strictly. Returns 0 on success and fills *first
 * and *last; returns -1 for any improperly formatted range (missing
 * "bytes=", missing bound, non-numeric, or multiple ranges). AWS maps these
 * to InvalidArgument (400), distinct from a well-formed but unsatisfiable
 * range (InvalidRange/416) which the caller checks against the object size.
 */
static int
chimera_s3_upc_parse_range(
    const char *range,
    int64_t    *first,
    int64_t    *last)
{
    const char *p = range;
    int64_t     a = 0, b = 0;
    int         saw_a = 0, saw_b = 0;

    if (strncmp(p, "bytes=", 6) != 0) {
        return -1;
    }
    p += 6;

    if (*p < '0' || *p > '9') {
        return -1;
    }
    while (*p >= '0' && *p <= '9') {
        a = a * 10 + (*p - '0');
        p++;
        saw_a = 1;
    }
    if (*p != '-') {
        return -1;
    }
    p++;
    if (*p < '0' || *p > '9') {
        return -1;
    }
    while (*p >= '0' && *p <= '9') {
        b = b * 10 + (*p - '0');
        p++;
        saw_b = 1;
    }
    /* Reject trailing junk (e.g. a second comma-separated range). */
    if (*p != '\0' || !saw_a || !saw_b) {
        return -1;
    }
    if (b < a) {
        return -1;
    }

    *first = a;
    *last  = b;
    return 0;
} /* chimera_s3_upc_parse_range */

static void chimera_s3_upc_step(
    struct chimera_s3_upload_copy_ctx *ctx);

/* Terminal error path for the copy phase. */
static void
chimera_s3_upc_fail(
    struct chimera_s3_upload_copy_ctx *ctx,
    enum chimera_s3_status             status)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (ctx->src_handle) {
        chimera_vfs_release(thread->vfs, ctx->src_handle);
    }
    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }
    if (request->dir_handle) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
    }
    if (request->multipart.upload) {
        chimera_s3_multipart_upload_release(thread, request->multipart.upload);
        request->multipart.upload = NULL;
    }

    free(ctx);

    request->status    = status;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_upc_fail */

/* Copy phase complete: hand off to the shared part-registration + response
 * path. The request fields (file_handle, dir_handle, tmp_name, part_number,
 * upload, file_cur_offset) are already populated, matching what
 * chimera_s3_upload_part_finish expects. */
static void
chimera_s3_upc_done(struct chimera_s3_upload_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (ctx->src_handle) {
        chimera_vfs_release(thread->vfs, ctx->src_handle);
        ctx->src_handle = NULL;
    }

    request->file_cur_offset = ctx->length;

    free(ctx);

    chimera_s3_upload_part_finish(request);
} /* chimera_s3_upc_done */

static void
chimera_s3_upc_copy_callback(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    ctx->copied += length;
    chimera_s3_upc_step(ctx);
} /* chimera_s3_upc_copy_callback */

static void
chimera_s3_upc_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread   *thread = ctx->request->thread;
    struct evpl                       *evpl   = thread->evpl;

    evpl_iovecs_release(evpl, ctx->rw_iov, ctx->rw_niov);
    ctx->rw_niov = 0;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    ctx->copied += length;
    chimera_s3_upc_step(ctx);
} /* chimera_s3_upc_write_callback */

static void
chimera_s3_upc_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread   *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    ctx->rw_niov = niov;

    chimera_vfs_write(
        thread->vfs, &thread->shared->cred, NULL,
        ctx->request->file_handle,
        ctx->copied,           /* destination part offset */
        count,
        1,
        0,
        0,
        ctx->rw_iov,
        ctx->rw_niov,
        chimera_s3_upc_write_callback,
        ctx);
} /* chimera_s3_upc_read_callback */

static void
chimera_s3_upc_step(struct chimera_s3_upload_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    uint64_t                         remaining, chunk;

    remaining = ctx->length - ctx->copied;
    if (remaining == 0) {
        chimera_s3_upc_done(ctx);
        return;
    }

    chunk = thread->shared->config->io_size;
    if (chunk > remaining) {
        chunk = remaining;
    }

    if (ctx->mode == CHIMERA_S3_UPC_COPY) {
        chimera_vfs_copy_range(
            thread->vfs, &thread->shared->cred, NULL,
            ctx->src_handle,
            ctx->src_first + ctx->copied,
            request->file_handle,
            ctx->copied,
            chunk,
            0, 0,
            chimera_s3_upc_copy_callback,
            ctx);
    } else {
        ctx->rw_niov = CHIMERA_S3_IOV_MAX;
        chimera_vfs_read(
            thread->vfs, &thread->shared->cred, NULL,
            ctx->src_handle,
            ctx->src_first + ctx->copied,
            chunk,
            ctx->rw_iov,
            ctx->rw_niov,
            0,
            chimera_s3_upc_read_callback,
            ctx);
    }
} /* chimera_s3_upc_step */

/* Both source and destination part are open; choose the transfer primitive
 * (copy_range when both live on the same module, else buffered read+write). */
static void
chimera_s3_upc_start_transfer(struct chimera_s3_upload_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *src_module, *dst_module;

    if (ctx->length == 0) {
        chimera_s3_upc_done(ctx);
        return;
    }

    src_module = chimera_vfs_get_module(thread->vfs,
                                        ctx->src_handle->fh,
                                        ctx->src_handle->fh_len);
    dst_module = chimera_vfs_get_module(thread->vfs,
                                        request->file_handle->fh,
                                        request->file_handle->fh_len);

    if (dst_module && src_module == dst_module &&
        (dst_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        ctx->mode = CHIMERA_S3_UPC_COPY;
    } else {
        ctx->mode = CHIMERA_S3_UPC_RW;
    }

    ctx->copied = 0;
    chimera_s3_upc_step(ctx);
} /* chimera_s3_upc_start_transfer */

/* ----- destination part file creation (mirrors UploadPart) ----- */

static void
chimera_s3_upc_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    ctx->request->file_handle = oh;
    chimera_s3_upc_start_transfer(ctx);
} /* chimera_s3_upc_create_unlinked_callback */

static void
chimera_s3_upc_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    ctx->request->file_handle = oh;
    chimera_s3_upc_start_transfer(ctx);
} /* chimera_s3_upc_create_callback */

static void
chimera_s3_upc_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx     = private_data;
    struct chimera_s3_request         *request = ctx->request;
    struct chimera_server_s3_thread   *thread  = request->thread;
    struct chimera_vfs_module         *module;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    request->dir_handle = oh;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    module = chimera_vfs_get_module(thread->vfs, oh->fh, oh->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {
        request->multipart.tmp_name_len = 0;
        chimera_vfs_create_unlinked(
            thread->vfs, &thread->shared->cred, NULL,
            oh->fh,
            oh->fh_len,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            chimera_s3_upc_create_unlinked_callback,
            ctx);
    } else {
        request->multipart.tmp_name_len = snprintf(
            request->multipart.tmp_name,
            sizeof(request->multipart.tmp_name),
            "._chimera_mpu_%.16s_%d",
            request->multipart.upload_id,
            request->multipart.part_number);

        chimera_vfs_open_at(
            thread->vfs, &thread->shared->cred, NULL,
            oh,
            request->multipart.tmp_name,
            request->multipart.tmp_name_len,
            CHIMERA_VFS_OPEN_CREATE,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            0,
            0,
            chimera_s3_upc_create_callback,
            ctx);
    }
} /* chimera_s3_upc_open_dir_callback */

static void
chimera_s3_upc_create_dir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread   *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_vfs_open_fh(
        thread->vfs, &thread->shared->cred, NULL,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_s3_upc_open_dir_callback,
        ctx);
} /* chimera_s3_upc_create_dir_callback */

/* Source object is open and its size validated against the requested range;
 * create the destination part file under the object key's parent directory. */
static void
chimera_s3_upc_open_src_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx     = private_data;
    struct chimera_s3_request         *request = ctx->request;
    struct chimera_server_s3_thread   *thread  = request->thread;
    const char                        *slash;
    const char                        *dirpath = request->path;
    int                                dirpathlen;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    ctx->src_handle = oh;

    slash = rindex(request->path, '/');
    if (slash) {
        dirpathlen    = slash - request->path;
        request->name = slash + 1;
        while (*request->name == '/') {
            request->name++;
        }
    } else {
        dirpath       = "/";
        dirpathlen    = 1;
        request->name = request->path;
    }
    request->name_len = strlen(request->name);

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    chimera_vfs_create(
        thread->vfs, &thread->shared->cred, NULL,
        request->bucket_fh,
        request->bucket_fhlen,
        dirpath,
        dirpathlen,
        &request->set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_s3_upc_create_dir_callback,
        ctx);
} /* chimera_s3_upc_open_src_callback */

static void
chimera_s3_upc_lookup_src_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread   *thread = ctx->request->thread;
    int64_t                            src_size;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    src_size = attr->va_size;

    /* No explicit range copies the whole object. */
    if (!ctx->request->multipart.has_copy_range) {
        ctx->src_first = 0;
        ctx->length    = src_size;
    } else {
        /* Range bounds were validated as well-formed at entry; now check
         * satisfiability against the object size -> InvalidRange (416). */
        if (ctx->src_first >= src_size || ctx->length > src_size ||
            ctx->src_first + ctx->length > src_size) {
            chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INVALID_RANGE);
            return;
        }
    }

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, NULL,
                        attr->va_fh,
                        attr->va_fh_len,
                        0,
                        chimera_s3_upc_open_src_callback,
                        ctx);
} /* chimera_s3_upc_lookup_src_callback */

static void
chimera_s3_upc_lookup_src_bucket_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_upload_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread   *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_NO_SUCH_BUCKET);
        return;
    }

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, NULL,
                       attr->va_fh,
                       attr->va_fh_len,
                       ctx->src_key,
                       ctx->src_key_len,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_upc_lookup_src_callback,
                       ctx);
} /* chimera_s3_upc_lookup_src_bucket_callback */

void
chimera_s3_upload_part_copy(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;
    struct chimera_s3_upload_copy_ctx  *ctx;
    const char                         *copy_source, *copy_range;
    const struct s3_bucket             *src_bucket;
    const char                         *src_path;

    /* Validate part number first (cheap). */
    if (request->multipart.part_number < 1 ||
        request->multipart.part_number > 10000) {
        request->status    = CHIMERA_S3_STATUS_INVALID_PART_NUMBER;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    copy_source = evpl_http_request_header(request->http_request,
                                           "x-amz-copy-source");
    copy_range = evpl_http_request_header(request->http_request,
                                          "x-amz-copy-source-range");

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;

    request->dir_handle        = NULL;
    request->file_handle       = NULL;
    request->multipart.is_copy = 1;
    request->multipart.upload  = NULL;
    request->io_pending        = 0;

    if (!copy_source || chimera_s3_upc_parse_source(ctx, copy_source) != 0) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INVALID_ARGUMENT);
        return;
    }

    if (copy_range) {
        if (chimera_s3_upc_parse_range(copy_range,
                                       &request->multipart.copy_range_first,
                                       &request->multipart.copy_range_last) != 0) {
            chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_INVALID_ARGUMENT);
            return;
        }
        request->multipart.has_copy_range = 1;
        ctx->src_first                    = request->multipart.copy_range_first;
        ctx->length                       = request->multipart.copy_range_last -
            request->multipart.copy_range_first + 1;
    } else {
        request->multipart.has_copy_range = 0;
    }

    /* Resolve and pin the in-progress upload. */
    upload = chimera_s3_multipart_table_lookup(
        shared->multipart_table,
        request->multipart.upload_id,
        request->multipart.upload_idlen);

    if (!upload) {
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_NO_SUCH_UPLOAD);
        return;
    }
    request->multipart.upload = upload;

    /* Resolve the source bucket -> VFS path. */
    src_bucket = chimera_s3_get_bucket(shared, ctx->src_bucket_name);
    if (src_bucket == NULL) {
        chimera_s3_release_bucket(shared);
        chimera_s3_upc_fail(ctx, CHIMERA_S3_STATUS_NO_SUCH_BUCKET);
        return;
    }

    src_path = chimera_s3_bucket_get_path(src_bucket);

    chimera_vfs_lookup(thread->vfs,
                       &thread->shared->cred, NULL,
                       shared->root_fh,
                       shared->root_fh_len,
                       src_path,
                       strlen(src_path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_upc_lookup_src_bucket_callback,
                       ctx);

    chimera_s3_release_bucket(shared);
} /* chimera_s3_upload_part_copy */

/* ----- CompleteMultipartUpload body accumulation + parser ----- */

#define CHIMERA_S3_MP_BODY_HARD_CAP (16 * 1024 * 1024)
#define CHIMERA_S3_MP_MIN_PART_SIZE (5 * 1024 * 1024)
#define CHIMERA_S3_MP_BODY_OVERFLOW (-1)

struct chimera_s3_client_part {
    int  part_number;
    char etag[80]; /* with surrounding quotes */
};

void
chimera_s3_complete_multipart_upload_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct evpl_iovec iov[CHIMERA_S3_IOV_MAX];
    uint64_t          avail, total;
    int               niov, i;

    while ((avail = evpl_http_request_get_data_avail(request->http_request)) > 0) {
        niov = evpl_http_request_get_datav(evpl, request->http_request,
                                           iov, avail);
        total = 0;
        for (i = 0; i < niov; i++) {
            total += iov[i].length;
        }

        if (request->multipart.body_len == CHIMERA_S3_MP_BODY_OVERFLOW) {
            /* Already over the hard cap; keep draining to release iovs. */
        } else if (request->multipart.body_len + total > CHIMERA_S3_MP_BODY_HARD_CAP) {
            /* Body too large; mark and drain remainder. */
            free(request->multipart.body_buf);
            request->multipart.body_buf = NULL;
            request->multipart.body_cap = 0;
            request->multipart.body_len = CHIMERA_S3_MP_BODY_OVERFLOW;
        } else {
            if (request->multipart.body_len + total > request->multipart.body_cap) {
                int new_cap = request->multipart.body_cap ?
                    request->multipart.body_cap * 2 : 4096;
                while ((uint64_t) new_cap < request->multipart.body_len + total) {
                    new_cap *= 2;
                }
                request->multipart.body_buf = realloc(request->multipart.body_buf,
                                                      new_cap);
                request->multipart.body_cap = new_cap;
            }
            for (i = 0; i < niov; i++) {
                memcpy(request->multipart.body_buf + request->multipart.body_len,
                       iov[i].data, iov[i].length);
                request->multipart.body_len += iov[i].length;
            }
        }

        evpl_iovecs_release(evpl, iov, niov);
    }
} /* chimera_s3_complete_multipart_upload_recv */

/* Locate `tag` between [start, end) and return pointer past its end. */
static const char *
chimera_s3_xml_find(
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
} /* chimera_s3_xml_find */

/*
 * Parse a CompleteMultipartUpload body into an array of client_parts.
 * Caller frees *r_parts. Returns CHIMERA_S3_STATUS_OK or MALFORMED_XML.
 */
static enum chimera_s3_status
chimera_s3_parse_complete_body(
    const char                     *body,
    int                             body_len,
    struct chimera_s3_client_part **r_parts,
    int                            *r_n_parts)
{
    const char *end = body + body_len;
    const char *cursor;
    struct chimera_s3_client_part *parts = NULL;
    int n_parts = 0;
    int cap     = 0;

    *r_parts   = NULL;
    *r_n_parts = 0;

    cursor = body;

    while (cursor < end) {
        const char *part_open = chimera_s3_xml_find(cursor, end, "<Part>");
        const char *part_close;
        const char *pn_open, *pn_close, *etag_open, *etag_close;

        if (!part_open) {
            break;
        }
        part_close = chimera_s3_xml_find(part_open, end, "</Part>");
        if (!part_close) {
            free(parts);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }

        pn_open    = chimera_s3_xml_find(part_open, part_close, "<PartNumber>");
        pn_close   = pn_open ? chimera_s3_xml_find(pn_open, part_close, "</PartNumber>") : NULL;
        etag_open  = chimera_s3_xml_find(part_open, part_close, "<ETag>");
        etag_close = etag_open ? chimera_s3_xml_find(etag_open, part_close, "</ETag>") : NULL;

        if (!pn_open || !pn_close || !etag_open || !etag_close) {
            free(parts);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }

        if (n_parts == cap) {
            cap = cap ? cap * 2 : 16;
            if (cap > 10000) {
                cap = 10000;
            }
            parts = realloc(parts, cap * sizeof(*parts));
        }
        if (n_parts >= 10000) {
            free(parts);
            return CHIMERA_S3_STATUS_MALFORMED_XML;
        }

        /* PartNumber: digits between pn_open and pn_close - strlen("</PartNumber>") */
        {
            const char *pn_value_end = pn_close - strlen("</PartNumber>");
            int pn                   = 0;
            const char *p            = pn_open;

            while (p < pn_value_end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) {
                p++;
            }
            while (p < pn_value_end && *p >= '0' && *p <= '9') {
                pn = pn * 10 + (*p - '0');
                p++;
            }
            parts[n_parts].part_number = pn;
        }

        /* ETag: copy verbatim (keep quotes) */
        {
            const char *etag_value_end = etag_close - strlen("</ETag>");
            int len;
            const char *p = etag_open;

            while (p < etag_value_end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) {
                p++;
            }
            len = etag_value_end - p;
            while (len > 0 && (p[len - 1] == ' ' || p[len - 1] == '\t' ||
                               p[len - 1] == '\n' || p[len - 1] == '\r')) {
                len--;
            }
            if (len >= (int) sizeof(parts[n_parts].etag)) {
                free(parts);
                return CHIMERA_S3_STATUS_MALFORMED_XML;
            }
            memcpy(parts[n_parts].etag, p, len);
            parts[n_parts].etag[len] = '\0';
        }

        n_parts++;
        cursor = part_close;
    }

    *r_parts   = parts;
    *r_n_parts = n_parts;
    return CHIMERA_S3_STATUS_OK;
} /* chimera_s3_parse_complete_body */

/*
 * Compare two ETag strings ignoring optional surrounding double-quotes
 * and ASCII case. The HTTP ETag header carries the value enclosed in
 * quotes per RFC; some clients echo it back verbatim in the
 * CompleteMultipartUpload body, others (e.g., some botocore releases)
 * strip the quotes. Both forms must validate.
 */
static int
chimera_s3_etag_equal(
    const char *a,
    const char *b)
{
    int alen = strlen(a);
    int blen = strlen(b);

    if (alen >= 2 && a[0] == '"' && a[alen - 1] == '"') {
        a++;
        alen -= 2;
    }
    if (blen >= 2 && b[0] == '"' && b[blen - 1] == '"') {
        b++;
        blen -= 2;
    }
    if (alen != blen) {
        return 0;
    }
    for (int i = 0; i < alen; i++) {
        char ca = a[i];
        char cb = b[i];
        if (ca >= 'A' && ca <= 'Z') {
            ca = (char) (ca + ('a' - 'A'));
        }
        if (cb >= 'A' && cb <= 'Z') {
            cb = (char) (cb + ('a' - 'A'));
        }
        if (ca != cb) {
            return 0;
        }
    }
    return 1;
} /* chimera_s3_etag_equal */

/*
 * Validate the client manifest against the upload's part list.
 *
 * Populates *r_server_parts with pointers (in client order) to the matched
 * server-side parts on success. Caller frees that array. upload->lock must
 * be held by caller.
 */
static enum chimera_s3_status
chimera_s3_validate_complete_manifest(
    const struct chimera_s3_client_part *client_parts,
    int                                  n_client_parts,
    struct chimera_s3_multipart_upload  *upload,
    struct chimera_s3_part            ***r_server_parts)
{
    struct chimera_s3_part **out;
    int prev_pn = 0;

    *r_server_parts = NULL;

    if (n_client_parts == 0) {
        return CHIMERA_S3_STATUS_MALFORMED_XML;
    }

    out = calloc(n_client_parts, sizeof(*out));

    for (int i = 0; i < n_client_parts; i++) {
        struct chimera_s3_part *sp;
        char server_etag[80];

        /* Ascending part-number order. */
        if (client_parts[i].part_number <= prev_pn) {
            free(out);
            return CHIMERA_S3_STATUS_INVALID_PART_ORDER;
        }
        prev_pn = client_parts[i].part_number;

        /* Locate matching server part. */
        for (sp = upload->parts; sp; sp = sp->next) {
            if (sp->part_number == client_parts[i].part_number) {
                break;
            }
        }
        if (!sp) {
            free(out);
            return CHIMERA_S3_STATUS_INVALID_PART;
        }

        /* ETag must match what the server returned for this part. Tolerate
         * the optional surrounding quotes and ASCII case (some clients
         * strip the quotes from the header before echoing it back). */
        chimera_s3_mp_format_etag(server_etag, sizeof(server_etag), sp->etag);
        if (!chimera_s3_etag_equal(server_etag, client_parts[i].etag)) {
            free(out);
            return CHIMERA_S3_STATUS_INVALID_PART;
        }

        /* EntityTooSmall: every non-final part must be >= 5 MiB. */
        if (i < n_client_parts - 1 && sp->size < CHIMERA_S3_MP_MIN_PART_SIZE) {
            free(out);
            return CHIMERA_S3_STATUS_ENTITY_TOO_SMALL;
        }

        out[i] = sp;
    }

    *r_server_parts = out;
    return CHIMERA_S3_STATUS_OK;
} /* chimera_s3_validate_complete_manifest */

/* ----- CompleteMultipartUpload assembly ----- */

static void
chimera_s3_complete_send_response(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    int                        part_count,
    const uint64_t             combined_etag[2])
{
    char *bp, *body_start;
    char  etag_buf[80];

    chimera_s3_mp_format_etag(etag_buf, sizeof(etag_buf), combined_etag);

    evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    chimera_s3_mp_append(&bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_mp_append(&bp, "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    chimera_s3_mp_append(&bp, "  <Location>http://%.*s/%.*s</Location>\n",
                         request->bucket_namelen, request->bucket_name,
                         request->path_len, request->path);
    chimera_s3_mp_append(&bp, "  <Bucket>%.*s</Bucket>\n",
                         request->bucket_namelen, request->bucket_name);
    chimera_s3_mp_append(&bp, "  <Key>%.*s</Key>\n",
                         request->path_len, request->path);
    /* AWS multipart ETag shape: "<hex>-<partcount>" */
    {
        char *q = etag_buf + strlen(etag_buf) - 1; /* trailing quote */
        char  tail[16];
        snprintf(tail, sizeof(tail), "-%d\"", part_count);
        memcpy(q, tail, strlen(tail) + 1);
    }
    chimera_s3_mp_append(&bp, "  <ETag>%s</ETag>\n", etag_buf);
    chimera_s3_mp_append(&bp, "</CompleteMultipartUploadResult>\n");

    chimera_s3_mp_send_response(evpl, request, body_start, bp);
} /* chimera_s3_complete_send_response */

/* Deferred response after object tags were applied (store-by-path callback).
* Part count + combined etag were stashed on the request by finish_common. */
static void
chimera_s3_complete_send_response_deferred(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    uint64_t etag[2] = { request->etag[0], request->etag[1] };

    chimera_s3_complete_send_response(evpl, request, request->multipart.part_number, etag);
} /* chimera_s3_complete_send_response_deferred */

enum chimera_s3_assemble_mode {
    CHIMERA_S3_ASSEMBLE_MOVE,
    CHIMERA_S3_ASSEMBLE_COPY,
    CHIMERA_S3_ASSEMBLE_RW,
};

struct chimera_s3_complete_ctx {
    struct chimera_s3_request          *request;
    struct chimera_s3_multipart_upload *upload;
    int                                 part_count;
    uint64_t                            combined_etag[2];
    struct chimera_s3_part            **client_parts; /* pointers into upload->parts */
    int                                 client_idx;
    int64_t                             write_offset;
    int64_t                             part_offset;
    enum chimera_s3_assemble_mode       assemble_mode;
    int                                 rw_niov;
    struct evpl_iovec                   rw_iov[CHIMERA_S3_IOV_MAX];
    /* Bounded retry of the destination parent-dir create. The linux backend
     * can transiently return ESTALE from open_by_handle_at when a cached
     * directory handle is being recycled under concurrent load; re-walking the
     * create from the (re-resolved) bucket handle clears it. */
    int                                 create_retries;
    int                                 dirpathlen;
    char                                dirpath[CHIMERA_S3_KEY_MAX];
    /* One-shot timer used to back off between destination parent-dir create
     * retries so a concurrent open-handle-cache eviction (the source of the
     * transient ESTALE/ENOENT, see chimera_s3_complete_create_root_callback)
     * has time to drain before we re-walk. */
    struct evpl_timer                   create_retry_timer;
};

#define CHIMERA_S3_COMPLETE_CREATE_MAX_RETRIES      32
/* Back-off between create retries. The transient ESTALE/ENOENT is caused by a
 * directory handle being recycled by the VFS close-sweep thread under load; the
 * earlier immediate (same-event-loop) re-walks could all race the same eviction
 * window and exhaust the bound while the sweep was still in flight. Yielding for
 * a short interval between attempts lets the sweep drain so the re-walk resolves
 * a fresh handle. 1ms * 32 retries = up to ~32ms, comfortably longer than the
 * eviction window while still well inside the client's request timeout. */
#define CHIMERA_S3_COMPLETE_CREATE_RETRY_BACKOFF_US 1000

static void chimera_s3_complete_assemble_next(
    struct chimera_s3_complete_ctx *ctx);

static void chimera_s3_complete_finalize(
    struct chimera_s3_complete_ctx *ctx);

static void chimera_s3_complete_finish_common(
    enum chimera_vfs_error error_code,
    void                  *private_data);

static void chimera_s3_complete_create_dir(
    struct chimera_s3_complete_ctx *ctx);

static int chimera_s3_complete_create_retry(
    struct chimera_s3_complete_ctx *ctx,
    enum chimera_vfs_error          error_code);

/* ----- Assembly: walk parts list, copy/move/rw each part into dest ----- */

static void
chimera_s3_complete_assemble_done_part(struct chimera_s3_complete_ctx *ctx)
{
    ctx->client_idx++;
    ctx->part_offset = 0;
    chimera_s3_complete_assemble_next(ctx);
} /* chimera_s3_complete_assemble_done_part */

static void
chimera_s3_complete_move_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *src_post_attr,
    struct chimera_vfs_attrs *dst_pre_attr,
    struct chimera_vfs_attrs *dst_post_attr,
    void                     *private_data)
{
    struct chimera_s3_complete_ctx *ctx = private_data;

    if (error_code) {
        /* memfs move_range is a zero-copy block-pointer swap and rejects
         * sub-block-aligned moves with EINVAL. Real S3 part sizes (e.g. the
         * AWS CLI's trailing part) are rarely block-aligned, so fall back to
         * read+write for the rest of the assembly. Once the running write
         * offset is unaligned no further move_range can succeed anyway, so
         * switching the whole remainder to RW (rather than per-part probing)
         * avoids repeated failed moves. Nothing was moved on EINVAL, so
         * part_offset/write_offset are unchanged and the retry is safe. */
        if (error_code == CHIMERA_VFS_EINVAL &&
            ctx->assemble_mode == CHIMERA_S3_ASSEMBLE_MOVE) {
            ctx->assemble_mode = CHIMERA_S3_ASSEMBLE_RW;
            chimera_s3_complete_assemble_next(ctx);
            return;
        }
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    ctx->write_offset += ctx->client_parts[ctx->client_idx]->size - ctx->part_offset;
    chimera_s3_complete_assemble_done_part(ctx);
} /* chimera_s3_complete_move_callback */

static void
chimera_s3_complete_copy_callback(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_complete_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    ctx->write_offset += length;
    ctx->part_offset  += length;

    if (ctx->part_offset >= ctx->client_parts[ctx->client_idx]->size) {
        chimera_s3_complete_assemble_done_part(ctx);
    } else {
        /* Short copy: continue with the same part. */
        chimera_s3_complete_assemble_next(ctx);
    }
} /* chimera_s3_complete_copy_callback */

static void
chimera_s3_complete_rw_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_complete_ctx  *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct evpl                     *evpl   = thread->evpl;

    evpl_iovecs_release(evpl, ctx->rw_iov, ctx->rw_niov);
    ctx->rw_niov = 0;

    if (error_code) {
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    ctx->write_offset += length;
    ctx->part_offset  += length;

    if (ctx->part_offset >= ctx->client_parts[ctx->client_idx]->size) {
        chimera_s3_complete_assemble_done_part(ctx);
    } else {
        chimera_s3_complete_assemble_next(ctx);
    }
} /* chimera_s3_complete_rw_write_callback */

static void
chimera_s3_complete_rw_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_complete_ctx  *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    /* Pass the iov filled by read straight through to write. The iov
     * descriptors live in ctx->rw_iov (the caller-provided array the
     * backend populated), so the write callback can release them. */
    ctx->rw_niov = niov;

    chimera_vfs_write(
        thread->vfs, &thread->shared->cred, NULL,
        request->file_handle,
        ctx->write_offset,
        count,
        1,
        0,
        0,
        ctx->rw_iov,
        ctx->rw_niov,
        chimera_s3_complete_rw_write_callback,
        ctx);
} /* chimera_s3_complete_rw_read_callback */

static void
chimera_s3_complete_assemble_rw(struct chimera_s3_complete_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_part          *part    = ctx->client_parts[ctx->client_idx];
    uint64_t                         chunk   = thread->shared->config->io_size;
    uint64_t                         remaining;

    remaining = part->size - ctx->part_offset;
    if (chunk > remaining) {
        chunk = remaining;
    }

    /* Tell read it may fill up to CHIMERA_S3_IOV_MAX slots in ctx->rw_iov.
     * For backends that allocate their own buffers (memfs zero-copy), the
     * callback will hand back a different iov pointer; the read callback
     * copies entries back into ctx->rw_iov. */
    ctx->rw_niov = CHIMERA_S3_IOV_MAX;

    chimera_vfs_read(
        thread->vfs, &thread->shared->cred, NULL,
        part->file_handle,
        ctx->part_offset,
        chunk,
        ctx->rw_iov,
        ctx->rw_niov,
        0,
        chimera_s3_complete_rw_read_callback,
        ctx);
} /* chimera_s3_complete_assemble_rw */

static void
chimera_s3_complete_assemble_next(struct chimera_s3_complete_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_part          *part;
    uint64_t                         remaining;

    /* Skip any zero-byte parts. */
    while (ctx->client_idx < ctx->part_count &&
           ctx->client_parts[ctx->client_idx]->size == 0) {
        ctx->client_idx++;
        ctx->part_offset = 0;
    }

    if (ctx->client_idx >= ctx->part_count) {
        /* All parts processed; finalize (link/rename into place). */
        chimera_s3_complete_finalize(ctx);
        return;
    }

    part      = ctx->client_parts[ctx->client_idx];
    remaining = part->size - ctx->part_offset;

    switch (ctx->assemble_mode) {
        case CHIMERA_S3_ASSEMBLE_MOVE:
            chimera_vfs_move_range(
                thread->vfs, &thread->shared->cred,
                part->file_handle,
                ctx->part_offset,
                request->file_handle,
                ctx->write_offset,
                remaining,
                0, 0, 0,
                chimera_s3_complete_move_callback,
                ctx);
            break;
        case CHIMERA_S3_ASSEMBLE_COPY:
            chimera_vfs_copy_range(
                thread->vfs, &thread->shared->cred, NULL,
                part->file_handle,
                ctx->part_offset,
                request->file_handle,
                ctx->write_offset,
                remaining,
                0, 0,
                chimera_s3_complete_copy_callback,
                ctx);
            break;
        case CHIMERA_S3_ASSEMBLE_RW:
            chimera_s3_complete_assemble_rw(ctx);
            break;
    } /* switch */
} /* chimera_s3_complete_assemble_next */

static void
chimera_s3_complete_finish_common(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_complete_ctx  *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    int                              count   = ctx->part_count;
    uint64_t                         etag[2];
    char                            *tagging = NULL;

    etag[0] = ctx->combined_etag[0];
    etag[1] = ctx->combined_etag[1];

    if (request->dir_handle) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
    }
    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }

    /* Capture any x-amz-tagging set at CreateMultipartUpload before dropping
     * the upload (which owns the string). */
    if (!error_code && ctx->upload && ctx->upload->tagging) {
        tagging = strdup(ctx->upload->tagging);
    }

    /* Stash the part count + etag for the deferred response. */
    request->multipart.part_number = count;
    request->etag[0]               = etag[0];
    request->etag[1]               = etag[1];

    /* Dispose of the upload. The upload was intentionally left in the table
     * during assembly (marked `completing`) so a retried CompleteMultipartUpload
     * remained resolvable and idempotent. ctx->upload holds the lookup ref.
     *
     *  - On success: detach (unlinks from the table + marks removed), then drop
     *    BOTH the now-orphaned table ref and our ctx ref, freeing the upload and
     *    triggering async part cleanup.
     *  - On failure: clear `completing` and drop only our ctx ref, leaving the
     *    upload in the table so the client can retry Complete (or Abort).
     */
    if (ctx->upload) {
        struct chimera_server_s3_shared *shared = thread->shared;

        if (!error_code) {
            chimera_s3_multipart_table_detach(shared->multipart_table,
                                              request->multipart.upload_id,
                                              request->multipart.upload_idlen);
            /* Drop the table's implicit (insert) ref now that it is unlinked. */
            chimera_s3_multipart_upload_release(thread, ctx->upload);
        } else {
            pthread_mutex_lock(&ctx->upload->lock);
            ctx->upload->completing = 0;
            pthread_mutex_unlock(&ctx->upload->lock);
        }
        /* Drop our (ctx) ref. On success this is the last ref -> free + async
         * part cleanup. On failure the table's ref remains, keeping it alive. */
        chimera_s3_multipart_upload_release(thread, ctx->upload);
        ctx->upload = NULL;
    }

    free(ctx->client_parts);
    free(ctx);

    free(request->multipart.body_buf);
    request->multipart.body_buf = NULL;
    request->multipart.body_len = 0;
    request->multipart.body_cap = 0;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Apply the object tags (if any) before emitting the success response. The
    * multipart.response iovec used by the response builder must not be
    * allocated until tagging is done, so we defer it to the store callback. */
    if (tagging) {
        struct chimera_s3_tagging_ctx *tctx = calloc(1, sizeof(*tctx));

        request->tagging = tctx;

        if (chimera_s3_tagging_parse_header(tctx, tagging) == 0 &&
            tctx->n_tags > 0) {
            free(tagging);
            chimera_s3_tagging_store_by_path(evpl, thread, request,
                                             chimera_s3_complete_send_response_deferred);
            return;
        }

        /* Empty/invalid tagging: ignore and fall through to the response. */
        free(request->tagging);
        request->tagging = NULL;
    }

    free(tagging);

    chimera_s3_complete_send_response(evpl, request, count, etag);
} /* chimera_s3_complete_finish_common */

static void
chimera_s3_complete_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    chimera_s3_complete_finish_common(error_code, private_data);
} /* chimera_s3_complete_rename_callback */

static void
chimera_s3_complete_link_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    chimera_s3_complete_finish_common(error_code, private_data);
} /* chimera_s3_complete_link_callback */

static void
chimera_s3_complete_finalize(struct chimera_s3_complete_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (request->multipart.tmp_name_len) {
        chimera_vfs_rename_at(
            thread->vfs,
            &thread->shared->cred, NULL,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->multipart.tmp_name,
            request->multipart.tmp_name_len,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->name,
            request->name_len,
            NULL,
            0,
            0,
            0,
            NULL,
            NULL,
            chimera_s3_complete_rename_callback,
            ctx);
    } else {
        chimera_vfs_link_at(
            thread->vfs,
            &thread->shared->cred, NULL,
            request->file_handle->fh,
            request->file_handle->fh_len,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->name,
            request->name_len,
            1,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            NULL,
            NULL,
            chimera_s3_complete_link_callback,
            ctx);
    }
} /* chimera_s3_complete_finalize */

static void
chimera_s3_complete_start_assembly(struct chimera_s3_complete_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *module;

    /* The destination file's module determines which assembly primitive
     * we use. Sources and destination must be on the same module since
     * range ops are intra-module. All parts in this upload were created
     * in the same dir, so this is automatically true. */
    module = chimera_vfs_get_module(thread->vfs,
                                    request->file_handle->fh,
                                    request->file_handle->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_MOVE_RANGE) {
        ctx->assemble_mode = CHIMERA_S3_ASSEMBLE_MOVE;
    } else if (module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE) {
        ctx->assemble_mode = CHIMERA_S3_ASSEMBLE_COPY;
    } else {
        ctx->assemble_mode = CHIMERA_S3_ASSEMBLE_RW;
    }

    ctx->client_idx   = 0;
    ctx->write_offset = 0;
    ctx->part_offset  = 0;

    chimera_s3_complete_assemble_next(ctx);
} /* chimera_s3_complete_start_assembly */

static void
chimera_s3_complete_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_s3_complete_ctx *ctx     = private_data;
    struct chimera_s3_request      *request = ctx->request;

    if (error_code) {
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    request->file_handle = oh;
    chimera_s3_complete_start_assembly(ctx);
} /* chimera_s3_complete_create_unlinked_callback */

static void
chimera_s3_complete_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_s3_complete_ctx *ctx     = private_data;
    struct chimera_s3_request      *request = ctx->request;

    if (error_code) {
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    request->file_handle = oh;
    chimera_s3_complete_start_assembly(ctx);
} /* chimera_s3_complete_create_callback */

static void
chimera_s3_complete_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_complete_ctx  *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *module;

    if (error_code) {
        /* Same transient ESTALE/ENOENT window as create_root_callback: the just
         * -created dir's handle can be recycled before this open resolves it.
         * Re-walk the create (after a back-off) rather than failing the whole
         * Complete. */
        if (chimera_s3_complete_create_retry(ctx, error_code)) {
            return;
        }
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    request->dir_handle = oh;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    module = chimera_vfs_get_module(thread->vfs, oh->fh, oh->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {
        request->multipart.tmp_name_len = 0;

        chimera_vfs_create_unlinked(
            thread->vfs, &thread->shared->cred, NULL,
            oh->fh,
            oh->fh_len,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            chimera_s3_complete_create_unlinked_callback,
            ctx);
    } else {
        request->multipart.tmp_name_len = snprintf(
            request->multipart.tmp_name,
            sizeof(request->multipart.tmp_name),
            "._chimera_mpufinal_%lx%lx",
            (uint64_t) request,
            (uint64_t) request->start_time.tv_nsec);

        chimera_vfs_open_at(
            thread->vfs, &thread->shared->cred, NULL,
            oh,
            request->multipart.tmp_name,
            request->multipart.tmp_name_len,
            CHIMERA_VFS_OPEN_CREATE,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            0,
            0,
            chimera_s3_complete_create_callback,
            ctx);
    }
} /* chimera_s3_complete_open_dir_callback */

static void
chimera_s3_complete_create_root_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_complete_ctx  *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        /* The linux backend can transiently return ESTALE (and, while a parent
         * directory is being recycled, ENOENT) from open_by_handle_at when a
         * cached directory handle is being evicted concurrently. Re-walking the
         * create from the bucket handle (after a back-off so the concurrent
         * eviction drains) resolves a fresh handle and succeeds. Bound the
         * retries so a genuinely missing parent still errors out. */
        if (chimera_s3_complete_create_retry(ctx, error_code)) {
            return;
        }
        chimera_s3_complete_finish_common(error_code, ctx);
        return;
    }

    chimera_vfs_open_fh(
        thread->vfs, &thread->shared->cred, NULL,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_s3_complete_open_dir_callback,
        ctx);
} /* chimera_s3_complete_create_root_callback */

/* Kick off (or retry) the destination parent-directory create. */
static void
chimera_s3_complete_create_dir(struct chimera_s3_complete_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    chimera_vfs_create(
        thread->vfs, &thread->shared->cred, NULL,
        request->bucket_fh,
        request->bucket_fhlen,
        ctx->dirpath,
        ctx->dirpathlen,
        &request->set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_s3_complete_create_root_callback,
        ctx);
} /* chimera_s3_complete_create_dir */

/* One-shot timer callback: re-drive the parent-dir create after a back-off. */
static void
chimera_s3_complete_create_retry_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_s3_complete_ctx *ctx = (struct chimera_s3_complete_ctx *)
        ((char *) timer - offsetof(struct chimera_s3_complete_ctx,
                                   create_retry_timer));

    (void) evpl;
    chimera_s3_complete_create_dir(ctx);
} /* chimera_s3_complete_create_retry_timer_cb */

/*
 * Schedule a back-off'd retry of the destination parent-dir create. Returns 1 if
 * a retry was armed (caller must return), 0 if the bound is exhausted or the
 * error is not a transient handle-recycle error (caller should fail the op).
 *
 * The transient ESTALE/ENOENT is produced by the linux backend's
 * open_by_handle_at when a cached directory handle is being evicted by the VFS
 * close-sweep thread under concurrent load. An immediate re-walk in the same
 * event-loop turn races the same eviction window; backing off for a short
 * interval lets the sweep drain so the re-walk resolves a fresh handle.
 */
static int
chimera_s3_complete_create_retry(
    struct chimera_s3_complete_ctx *ctx,
    enum chimera_vfs_error          error_code)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if ((error_code != CHIMERA_VFS_ESTALE &&
         error_code != CHIMERA_VFS_ENOENT) ||
        ctx->create_retries >= CHIMERA_S3_COMPLETE_CREATE_MAX_RETRIES) {
        return 0;
    }

    ctx->create_retries++;

    /* Drop any partially-resolved destination handles before re-walking so the
     * retry starts clean (and so a stale handle is not released twice). */
    if (ctx->request->dir_handle) {
        chimera_vfs_release(thread->vfs, ctx->request->dir_handle);
        ctx->request->dir_handle = NULL;
    }
    if (ctx->request->file_handle) {
        chimera_vfs_release(thread->vfs, ctx->request->file_handle);
        ctx->request->file_handle = NULL;
    }

    evpl_add_oneshot_timer(thread->evpl, &ctx->create_retry_timer,
                           chimera_s3_complete_create_retry_timer_cb,
                           CHIMERA_S3_COMPLETE_CREATE_RETRY_BACKOFF_US);
    return 1;
} /* chimera_s3_complete_create_retry */

/*
 * Phase 1: dispatcher entry. Just initialize the body buffer so the
 * notifier can accumulate the manifest. All real work waits for the
 * full body to arrive.
 */
void
chimera_s3_complete_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    (void) evpl;
    (void) thread;

    request->multipart.body_buf = NULL;
    request->multipart.body_len = 0;
    request->multipart.body_cap = 0;
    request->vfs_state          = CHIMERA_S3_VFS_STATE_INIT;
} /* chimera_s3_complete_multipart_upload */

/* Parse a quoted 32-hex ETag ("\"<32hex>\"") as written by
 * chimera_s3_mp_format_etag into a 128-bit value (low64, high64). Returns 0 on
 * success, -1 on malformed input. The client echoes the exact per-part ETags
 * the server returned, so this round-trips losslessly. */
static int
chimera_s3_mp_parse_etag(
    const char *quoted,
    uint64_t    out[2])
{
    const char *p = quoted;
    uint8_t     bytes[16];

    if (*p == '"') {
        p++;
    }

    for (int i = 0; i < 16; i++) {
        int hi, lo;

        if (!isxdigit((unsigned char) p[0]) || !isxdigit((unsigned char) p[1])) {
            return -1;
        }
        hi       = (p[0] <= '9') ? p[0] - '0' : (tolower(p[0]) - 'a' + 10);
        lo       = (p[1] <= '9') ? p[1] - '0' : (tolower(p[1]) - 'a' + 10);
        bytes[i] = (hi << 4) | lo;
        p       += 2;
    }

    /* Mirror format_hex's byte order (it serializes the raw uint64[2]). */
    memcpy(out, bytes, 16);
    return 0;
} /* chimera_s3_mp_parse_etag */

/* Recompute the combined CompleteMultipartUpload ETag from the client's
 * manifest alone (no server-side part list). Used to answer an idempotent
 * retry whose upload was already detached by the winning Complete. */
static int
chimera_s3_mp_combined_etag_from_manifest(
    const struct chimera_s3_client_part *client_parts,
    int                                  n_client,
    uint64_t                             out[2])
{
    struct {
        uint64_t etag[2];
    } *etag_buf;
    XXH128_hash_t h;

    out[0] = 0;
    out[1] = 0;

    if (n_client <= 0) {
        return 0;
    }

    etag_buf = malloc(n_client * sizeof(*etag_buf));

    for (int i = 0; i < n_client; i++) {
        if (chimera_s3_mp_parse_etag(client_parts[i].etag, etag_buf[i].etag) != 0) {
            free(etag_buf);
            return -1;
        }
    }

    h      = XXH3_128bits((const void *) etag_buf, n_client * sizeof(*etag_buf));
    out[0] = h.low64;
    out[1] = h.high64;
    free(etag_buf);
    return 0;
} /* chimera_s3_mp_combined_etag_from_manifest */

/* Context for the idempotent-retry existence check (lookup-miss path). */
struct chimera_s3_complete_retry_ctx {
    struct chimera_s3_request *request;
    int                        part_count;
    uint64_t                   combined_etag[2];
};

/* Result of the target-object existence check on a Complete whose upload was
 * already removed from the table. CompleteMultipartUpload must be idempotent:
 * S3 SDKs (boto3) retry it, and the winning attempt detaches the upload, so a
 * legitimate retry would otherwise see the upload gone and return NoSuchUpload.
 * If the object the prior Complete assembled is present, replay the success
 * response; otherwise the upload truly never existed -> NoSuchUpload. */
static void
chimera_s3_complete_retry_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_complete_retry_ctx *rctx    = private_data;
    struct chimera_s3_request            *request = rctx->request;
    struct chimera_server_s3_thread      *thread  = request->thread;
    struct evpl                          *evpl    = thread->evpl;

    free(request->multipart.body_buf);
    request->multipart.body_buf = NULL;
    request->multipart.body_len = 0;
    request->multipart.body_cap = 0;

    if (error_code == CHIMERA_VFS_OK) {
        /* Prior Complete already assembled the object: idempotent success. */
        chimera_s3_complete_send_response(evpl, request, rctx->part_count,
                                          rctx->combined_etag);
    } else {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_UPLOAD;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
    }

    free(rctx);
} /* chimera_s3_complete_retry_lookup_callback */

/*
 * Phase 2: full body in hand. Parse the manifest, validate against the
 * server's recorded parts, detach the upload, and kick off assembly.
 */
void
chimera_s3_complete_multipart_upload_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread    *thread = request->thread;
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;
    struct chimera_s3_complete_ctx     *ctx;
    struct chimera_s3_part             *part;
    struct chimera_s3_part            **server_parts = NULL;
    struct chimera_s3_client_part      *client_parts = NULL;
    int                                 n_client     = 0;
    enum chimera_s3_status              err;

    struct {
        uint64_t etag[2];
    } *etag_buf = NULL;
    XXH128_hash_t                       h;
    const char                         *slash;
    const char                         *dirpath = request->path;
    int                                 dirpathlen;
    int                                 i;

    if (request->multipart.body_len == CHIMERA_S3_MP_BODY_OVERFLOW) {
        request->status    = CHIMERA_S3_STATUS_MALFORMED_XML;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Parse the client's manifest first; cheaper to fail before lookup. */
    err = chimera_s3_parse_complete_body(request->multipart.body_buf,
                                         request->multipart.body_len,
                                         &client_parts, &n_client);
    if (err != CHIMERA_S3_STATUS_OK) {
        request->status    = err;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Lookup (not detach yet) so we can validate the manifest. If
     * validation fails, the upload stays in the table for retry. */
    upload = chimera_s3_multipart_table_lookup(shared->multipart_table,
                                               request->multipart.upload_id,
                                               request->multipart.upload_idlen);
    if (!upload) {
        /* The upload is not in the table. CompleteMultipartUpload must be
         * idempotent: S3 clients (boto3's default retry mode) retry it, and
         * the winning attempt detaches the upload before responding, so a
         * legitimate retry lands here even though the operation succeeded.
         * Distinguish a real "never existed" from "already completed" by
         * checking whether the target object is present; if so, replay the
         * success response (recomputing the combined ETag from the client's
         * manifest, which echoes the same per-part ETags the server issued). */
        struct chimera_s3_complete_retry_ctx *rctx;
        uint64_t                              combined[2];

        if (n_client > 0 &&
            chimera_s3_mp_combined_etag_from_manifest(client_parts, n_client,
                                                      combined) == 0) {
            rctx                   = calloc(1, sizeof(*rctx));
            rctx->request          = request;
            rctx->part_count       = n_client;
            rctx->combined_etag[0] = combined[0];
            rctx->combined_etag[1] = combined[1];
            free(client_parts);

            chimera_vfs_lookup(thread->vfs, &thread->shared->cred, NULL,
                               request->bucket_fh, request->bucket_fhlen,
                               request->path, request->path_len,
                               CHIMERA_VFS_ATTR_FH,
                               0,
                               chimera_s3_complete_retry_lookup_callback,
                               rctx);
            return;
        }

        free(client_parts);
        free(request->multipart.body_buf);
        request->multipart.body_buf = NULL;
        request->multipart.body_len = 0;
        request->multipart.body_cap = 0;
        request->status             = CHIMERA_S3_STATUS_NO_SUCH_UPLOAD;
        request->vfs_state          = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    pthread_mutex_lock(&upload->lock);
    err = chimera_s3_validate_complete_manifest(client_parts, n_client,
                                                upload, &server_parts);

    if (err == CHIMERA_S3_STATUS_OK && upload->completing) {
        /* Another CompleteMultipartUpload (or an earlier retry) already won the
         * race to assemble this upload and is in flight. Treat this as an
         * idempotent duplicate: replay the success response from the manifest
         * rather than assembling again. */
        uint64_t combined[2];

        pthread_mutex_unlock(&upload->lock);
        free(server_parts);
        chimera_s3_multipart_upload_release(thread, upload);

        if (chimera_s3_mp_combined_etag_from_manifest(client_parts, n_client,
                                                      combined) != 0) {
            combined[0] = 0;
            combined[1] = 0;
        }
        free(client_parts);
        free(request->multipart.body_buf);
        request->multipart.body_buf = NULL;
        request->multipart.body_len = 0;
        request->multipart.body_cap = 0;
        chimera_s3_complete_send_response(evpl, request, n_client, combined);
        return;
    }

    if (err == CHIMERA_S3_STATUS_OK) {
        /* We win the race: claim assembly. The upload stays in the table (so a
         * retried Complete still resolves it) until assembly succeeds. */
        upload->completing = 1;
    }
    pthread_mutex_unlock(&upload->lock);
    free(client_parts);

    if (err != CHIMERA_S3_STATUS_OK) {
        /* Drop our lookup ref; upload remains in the table. */
        free(server_parts);
        chimera_s3_multipart_upload_release(thread, upload);
        request->status    = err;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Keep the lookup ref: ctx->upload owns it through assembly. The upload is
     * deliberately NOT detached yet so a retried Complete still resolves it and
     * is handled idempotently (via the `completing` flag above). finish_common
     * detaches + frees it on success, or clears `completing` + drops the ref on
     * failure so a genuine retry can re-drive assembly. */

    /* Build ctx. Combined ETag = XXH3_128 over the etags of the parts the
     * client selected (in client order), formatted "<hex>-<count>". */
    ctx                   = calloc(1, sizeof(*ctx));
    ctx->request          = request;
    ctx->upload           = upload;
    ctx->part_count       = n_client;
    ctx->client_parts     = server_parts;
    ctx->combined_etag[0] = 0;
    ctx->combined_etag[1] = 0;

    if (n_client > 0) {
        etag_buf = malloc(n_client * sizeof(*etag_buf));
        for (i = 0; i < n_client; i++) {
            etag_buf[i].etag[0] = server_parts[i]->etag[0];
            etag_buf[i].etag[1] = server_parts[i]->etag[1];
        }
        h = XXH3_128bits((const void *) etag_buf,
                         n_client * sizeof(*etag_buf));
        ctx->combined_etag[0] = h.low64;
        ctx->combined_etag[1] = h.high64;
        free(etag_buf);
    }

    /* Suppress unused warning when zero parts had no contribution. */
    (void) part;

    /* Create the final object using the same dir-open + create pattern as
     * PUT. Compute parent dir from object key. */
    slash = rindex(request->path, '/');
    if (slash) {
        dirpathlen    = slash - request->path;
        request->name = slash + 1;
        while (*request->name == '/') {
            request->name++;
        }
    } else {
        dirpath       = "/";
        dirpathlen    = 1;
        request->name = request->path;
    }
    request->name_len = strlen(request->name);

    /* Stash the parent dirpath so the create can be retried on a transient
     * ESTALE (see chimera_s3_complete_create_root_callback). */
    if (dirpathlen > (int) sizeof(ctx->dirpath) - 1) {
        dirpathlen = sizeof(ctx->dirpath) - 1;
    }
    memcpy(ctx->dirpath, dirpath, dirpathlen);
    ctx->dirpath[dirpathlen] = '\0';
    ctx->dirpathlen          = dirpathlen;

    chimera_s3_complete_create_dir(ctx);
} /* chimera_s3_complete_multipart_upload_body_done */

/* ----- AbortMultipartUpload ----- */

void
chimera_s3_abort_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;

    upload = chimera_s3_multipart_table_detach(
        shared->multipart_table,
        request->multipart.upload_id,
        request->multipart.upload_idlen);

    if (!upload) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_UPLOAD;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Drop our table reference; async cleanup of parts fires on last ref. */
    chimera_s3_multipart_upload_release(thread, upload);

    request->status    = CHIMERA_S3_STATUS_NO_CONTENT;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_abort_multipart_upload */

/* ----- ListParts ----- */

void
chimera_s3_list_parts(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_upload *upload;
    struct chimera_s3_part             *part;
    char                               *bp, *body_start;
    char                                etag_hex[80];
    char                                date_buf[64];
    int                                 marker, max_parts;
    int                                 emitted, is_truncated, next_marker;

    upload = chimera_s3_multipart_table_lookup(
        shared->multipart_table,
        request->multipart.upload_id,
        request->multipart.upload_idlen);

    if (!upload) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_UPLOAD;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    marker    = request->multipart.part_number_marker; /* exclusive lower bound */
    max_parts = request->multipart.max_parts;
    if (max_parts <= 0) {
        max_parts = 1000;
    }

    /* First pass under the lock: count how many parts fall in the page
     * (part-number > marker, capped at max_parts) and whether more remain.
     * Decoupling truncation from rendering keeps the header/body in order. */
    emitted      = 0;
    is_truncated = 0;
    next_marker  = 0;

    pthread_mutex_lock(&upload->lock);
    for (part = upload->parts; part; part = part->next) {
        if (part->part_number <= marker) {
            continue;
        }
        if (emitted >= max_parts) {
            is_truncated = 1;
            break;
        }
        next_marker = part->part_number;
        emitted++;
    }

    evpl_iovec_alloc(evpl, 1024 * 1024, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    chimera_s3_mp_append(&bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_mp_append(&bp, "<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    chimera_s3_mp_append(&bp, "  <Bucket>%.*s</Bucket>\n",
                         request->bucket_namelen, request->bucket_name);
    chimera_s3_mp_append(&bp, "  <Key>%s</Key>\n", upload->object_key);
    chimera_s3_mp_append(&bp, "  <UploadId>%.*s</UploadId>\n",
                         CHIMERA_S3_UPLOAD_ID_LEN, upload->upload_id);
    chimera_s3_mp_append(&bp, "  <PartNumberMarker>%d</PartNumberMarker>\n", marker);
    chimera_s3_mp_append(&bp, "  <MaxParts>%d</MaxParts>\n", max_parts);
    chimera_s3_mp_append(&bp, "  <IsTruncated>%s</IsTruncated>\n",
                         is_truncated ? "true" : "false");
    if (is_truncated) {
        chimera_s3_mp_append(&bp, "  <NextPartNumberMarker>%d</NextPartNumberMarker>\n",
                             next_marker);
    }
    chimera_s3_mp_append(&bp, "  <StorageClass>STANDARD</StorageClass>\n");

    /* Second pass: render the selected page. */
    emitted = 0;
    for (part = upload->parts; part; part = part->next) {
        if (part->part_number <= marker) {
            continue;
        }
        if (emitted >= max_parts) {
            break;
        }
        chimera_s3_mp_format_etag(etag_hex, sizeof(etag_hex), part->etag);
        chimera_s3_mp_format_date(date_buf, sizeof(date_buf), &part->uploaded);
        chimera_s3_mp_append(&bp, "  <Part>\n");
        chimera_s3_mp_append(&bp, "    <PartNumber>%d</PartNumber>\n", part->part_number);
        chimera_s3_mp_append(&bp, "    <LastModified>%s</LastModified>\n", date_buf);
        chimera_s3_mp_append(&bp, "    <ETag>%s</ETag>\n", etag_hex);
        chimera_s3_mp_append(&bp, "    <Size>%ld</Size>\n", (long) part->size);
        chimera_s3_mp_append(&bp, "  </Part>\n");
        emitted++;
    }
    pthread_mutex_unlock(&upload->lock);

    /* Owner / Initiator. No per-user identity model exists yet, so emit a
     * stable placeholder identity. */
    chimera_s3_mp_append(&bp, "  <Initiator>\n");
    chimera_s3_mp_append(&bp, "    <ID>chimera</ID>\n");
    chimera_s3_mp_append(&bp, "    <DisplayName>chimera</DisplayName>\n");
    chimera_s3_mp_append(&bp, "  </Initiator>\n");
    chimera_s3_mp_append(&bp, "  <Owner>\n");
    chimera_s3_mp_append(&bp, "    <ID>chimera</ID>\n");
    chimera_s3_mp_append(&bp, "    <DisplayName>chimera</DisplayName>\n");
    chimera_s3_mp_append(&bp, "  </Owner>\n");

    chimera_s3_mp_append(&bp, "</ListPartsResult>\n");

    chimera_s3_multipart_upload_release(thread, upload);

    chimera_s3_mp_send_response(evpl, request, body_start, bp);
} /* chimera_s3_list_parts */

/* ----- ListMultipartUploads ----- */

/* A snapshot row for sorting/pagination, copied out from under the table
 * lock so we never render while holding it. */
struct chimera_s3_mpu_row {
    char            key[CHIMERA_S3_KEY_MAX];
    char            upload_id[CHIMERA_S3_UPLOAD_ID_LEN + 1];
    struct timespec created;
};

static int
chimera_s3_mpu_row_cmp(
    const void *a,
    const void *b)
{
    const struct chimera_s3_mpu_row *ra = a;
    const struct chimera_s3_mpu_row *rb = b;
    int                              c  = strcmp(ra->key, rb->key);

    if (c != 0) {
        return c;
    }
    return strcmp(ra->upload_id, rb->upload_id);
} /* chimera_s3_mpu_row_cmp */

void
chimera_s3_list_multipart_uploads(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared    *shared = thread->shared;
    struct chimera_s3_multipart_table  *table  = shared->multipart_table;
    struct chimera_s3_multipart_upload *upload;
    struct chimera_s3_mpu_row          *rows = NULL;
    char                               *bp, *body_start;
    char                                date_buf[64];
    int                                 i, n = 0, cap = 0;
    int                                 max_uploads, emitted, is_truncated;
    int                                 start;
    const char                         *key_marker;
    const char                         *uid_marker;

    max_uploads = request->multipart.max_uploads;
    if (max_uploads <= 0) {
        max_uploads = 1000;
    }
    key_marker = request->multipart.key_marker_len ?
        request->multipart.key_marker : NULL;
    uid_marker = request->multipart.upload_id_marker_len ?
        request->multipart.upload_id_marker : NULL;

    /* Snapshot matching uploads out from under the table lock. */
    pthread_rwlock_rdlock(&table->lock);
    for (i = 0; i < table->nbuckets; i++) {
        for (upload = table->buckets[i]; upload; upload = upload->next) {
            if (upload->removed) {
                continue;
            }
            if (upload->bucket_namelen != request->bucket_namelen ||
                memcmp(upload->bucket_name, request->bucket_name,
                       request->bucket_namelen) != 0) {
                continue;
            }
            if (n == cap) {
                cap  = cap ? cap * 2 : 16;
                rows = realloc(rows, cap * sizeof(*rows));
            }
            snprintf(rows[n].key, sizeof(rows[n].key), "%s", upload->object_key);
            memcpy(rows[n].upload_id, upload->upload_id,
                   CHIMERA_S3_UPLOAD_ID_LEN + 1);
            rows[n].created = upload->created;
            n++;
        }
    }
    pthread_rwlock_unlock(&table->lock);

    /* Sort by (key, upload-id) so pagination markers are well-defined. */
    if (n > 1) {
        qsort(rows, n, sizeof(*rows), chimera_s3_mpu_row_cmp);
    }

    /* Apply the (key-marker, upload-id-marker) exclusive cursor. */
    start = 0;
    if (key_marker) {
        while (start < n) {
            int kc = strcmp(rows[start].key, key_marker);
            if (kc > 0) {
                break;
            }
            if (kc == 0) {
                if (!uid_marker) {
                    start++;
                    continue;
                }
                if (strcmp(rows[start].upload_id, uid_marker) > 0) {
                    break;
                }
                start++;
                continue;
            }
            start++;
        }
    }

    is_truncated = (n - start) > max_uploads;

    evpl_iovec_alloc(evpl, 1024 * 1024, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    chimera_s3_mp_append(&bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_mp_append(&bp, "<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    chimera_s3_mp_append(&bp, "  <Bucket>%.*s</Bucket>\n",
                         request->bucket_namelen, request->bucket_name);
    chimera_s3_mp_append(&bp, "  <KeyMarker>%s</KeyMarker>\n",
                         key_marker ? key_marker : "");
    chimera_s3_mp_append(&bp, "  <UploadIdMarker>%s</UploadIdMarker>\n",
                         uid_marker ? uid_marker : "");
    chimera_s3_mp_append(&bp, "  <MaxUploads>%d</MaxUploads>\n", max_uploads);
    chimera_s3_mp_append(&bp, "  <IsTruncated>%s</IsTruncated>\n",
                         is_truncated ? "true" : "false");

    emitted = 0;
    for (i = start; i < n && emitted < max_uploads; i++, emitted++) {
        chimera_s3_mp_format_date(date_buf, sizeof(date_buf), &rows[i].created);
        chimera_s3_mp_append(&bp, "  <Upload>\n");
        chimera_s3_mp_append(&bp, "    <Key>%s</Key>\n", rows[i].key);
        chimera_s3_mp_append(&bp, "    <UploadId>%.*s</UploadId>\n",
                             CHIMERA_S3_UPLOAD_ID_LEN, rows[i].upload_id);
        chimera_s3_mp_append(&bp, "    <Initiator>\n");
        chimera_s3_mp_append(&bp, "      <ID>chimera</ID>\n");
        chimera_s3_mp_append(&bp, "      <DisplayName>chimera</DisplayName>\n");
        chimera_s3_mp_append(&bp, "    </Initiator>\n");
        chimera_s3_mp_append(&bp, "    <Owner>\n");
        chimera_s3_mp_append(&bp, "      <ID>chimera</ID>\n");
        chimera_s3_mp_append(&bp, "      <DisplayName>chimera</DisplayName>\n");
        chimera_s3_mp_append(&bp, "    </Owner>\n");
        chimera_s3_mp_append(&bp, "    <StorageClass>STANDARD</StorageClass>\n");
        chimera_s3_mp_append(&bp, "    <Initiated>%s</Initiated>\n", date_buf);
        chimera_s3_mp_append(&bp, "  </Upload>\n");
    }

    if (is_truncated && emitted > 0) {
        chimera_s3_mp_append(&bp, "  <NextKeyMarker>%s</NextKeyMarker>\n",
                             rows[start + emitted - 1].key);
        chimera_s3_mp_append(&bp, "  <NextUploadIdMarker>%.*s</NextUploadIdMarker>\n",
                             CHIMERA_S3_UPLOAD_ID_LEN,
                             rows[start + emitted - 1].upload_id);
    }

    chimera_s3_mp_append(&bp, "</ListMultipartUploadsResult>\n");

    free(rows);

    chimera_s3_mp_send_response(evpl, request, body_start, bp);
} /* chimera_s3_list_multipart_uploads */
