// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <time.h>
#include <stdint.h>

#include "s3_internal.h"

struct evpl;

struct chimera_s3_part {
    int                             part_number;
    int                             tmp_name_len;
    int                             dir_fhlen;
    int64_t                         size;
    struct chimera_vfs_open_handle *file_handle;
    struct timespec                 uploaded;
    uint64_t                        etag[2];
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
    char                            tmp_name[64];
    struct chimera_s3_part         *next;
};

struct chimera_s3_multipart_upload {
    char                                upload_id[CHIMERA_S3_UPLOAD_ID_LEN + 1];
    int                                 bucket_namelen;
    int                                 bucket_fhlen;
    int                                 object_keylen;
    char                               *bucket_name;
    char                               *object_key;
    struct timespec                     created;
    pthread_mutex_t                     lock;
    int                                 refcount;
    int                                 removed;
    struct chimera_s3_part             *parts;
    struct chimera_s3_multipart_upload *prev, *next;
    uint8_t                             bucket_fh[CHIMERA_VFS_FH_SIZE];
};

struct chimera_s3_multipart_table {
    pthread_rwlock_t                     lock;
    int                                  nbuckets;
    struct chimera_s3_multipart_upload **buckets;
};

struct chimera_s3_multipart_table *
chimera_s3_multipart_table_create(
    int nbuckets);

void
chimera_s3_multipart_table_destroy(
    struct chimera_s3_multipart_table *table);

void
chimera_s3_multipart_generate_id(
    struct chimera_server_s3_thread *thread,
    char                            *out);

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_insert(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    const char                        *bucket_name,
    int                                bucket_namelen,
    const uint8_t                     *bucket_fh,
    int                                bucket_fhlen,
    const char                        *object_key,
    int                                object_keylen);

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_lookup(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    int                                upload_idlen);

void
chimera_s3_multipart_upload_release(
    struct chimera_server_s3_thread    *thread,
    struct chimera_s3_multipart_upload *upload);

struct chimera_s3_multipart_upload *
chimera_s3_multipart_table_detach(
    struct chimera_s3_multipart_table *table,
    const char                        *upload_id,
    int                                upload_idlen);

void
chimera_s3_create_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_upload_part(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_upload_part_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_complete_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

/* Accumulate CompleteMultipartUpload body bytes from the wire into the
 * request's body buffer. Called repeatedly from the HTTP notifier as data
 * arrives. */
void
chimera_s3_complete_multipart_upload_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

/* Called once the full body is in hand. Parses the XML manifest,
 * validates it against the server's part list, and kicks off assembly. */
void
chimera_s3_complete_multipart_upload_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_abort_multipart_upload(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_list_parts(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_list_multipart_uploads(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);
