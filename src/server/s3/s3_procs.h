// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once


void
chimera_s3_put_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_put(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request
    );

void
chimera_s3_copy(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request
    );

void
chimera_s3_get_send(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_get(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_head(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_delete(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_list(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

/* Populate request->list from parsed query parameters (decoded, NUL-free).
 * Derives the effective pagination start key and the directory boundary used
 * to prune the VFS walk for '/'-delimited (folder-style) listings. */
void
chimera_s3_list_setup(
    struct chimera_s3_request *request,
    int                        list_type,
    int                        max_keys,
    int                        encoding_url,
    const char                *prefix,
    int                        prefix_len,
    const char                *delimiter,
    int                        delimiter_len,
    const char                *marker,
    int                        marker_len,
    const char                *ctoken,
    int                        ctoken_len,
    const char                *startafter,
    int                        startafter_len);

void
chimera_s3_delete_objects(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_delete_objects_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_delete_objects_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

void
chimera_s3_list_buckets(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_create_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_delete_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

void
chimera_s3_head_bucket(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);