// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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