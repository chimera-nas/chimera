// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* The server's management plane (the REST API), reached through these hooks
 * rather than by name.  REST manages the server -- its shares, exports,
 * buckets and users -- so it depends on the server; the server must not
 * depend back on it.  chimera_server_init (server_rest.c) supplies REST's
 * hooks; a server built with chimera_server_init_with_mgmt(..., NULL) runs
 * without a management plane. */

#include "common/macros.h"

struct evpl;
struct chimera_server;
struct chimera_server_config;
struct chimera_vfs;
struct chimera_vfs_thread;
struct prometheus_metrics;

struct chimera_server_mgmt {
    void *(*init)(
        const struct chimera_server_config *config,
        struct chimera_server              *server,
        struct chimera_vfs                 *vfs,
        struct prometheus_metrics          *metrics);
    void  (*start)(
        void *data);
    void  (*stop)(
        void *data);
    void  (*destroy)(
        void *data);
    void *(*thread_init)(
        struct evpl               *evpl,
        void                      *data,
        struct chimera_vfs_thread *vfs_thread);
    void  (*thread_destroy)(
        void *thread_data);
};

SYMBOL_EXPORT struct chimera_server *
chimera_server_init_with_mgmt(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics,
    const struct chimera_server_mgmt   *mgmt);
