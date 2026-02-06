// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct chimera_server_config;
struct chimera_server;
struct chimera_vfs;
struct prometheus_metrics;
struct evpl;
struct chimera_rest_server;

/**
 * Initialize the REST API server
 *
 * @param config Server configuration
 * @param server Main Chimera server instance
 * @param vfs VFS instance (unused for now, but may be needed for future APIs)
 * @param metrics Prometheus metrics (unused for now)
 * @return REST server instance or NULL if disabled (port is 0)
 */
struct chimera_rest_server *
chimera_rest_init(
    const struct chimera_server_config *config,
    struct chimera_server              *server,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics);

/**
 * Start the REST API server (begin accepting connections)
 *
 * @param rest REST server instance
 */
void
chimera_rest_start(
    struct chimera_rest_server *rest);

/**
 * Stop the REST API server
 *
 * @param rest REST server instance
 */
void
chimera_rest_stop(
    struct chimera_rest_server *rest);

/**
 * Destroy the REST API server and free resources
 *
 * @param rest REST server instance
 */
void
chimera_rest_destroy(
    struct chimera_rest_server *rest);

/**
 * Initialize per-thread REST API state
 *
 * @param evpl Event loop for this thread
 * @param rest REST server shared state
 * @return Thread-local REST state
 */
void *
chimera_rest_thread_init(
    struct evpl                *evpl,
    struct chimera_rest_server *rest);

/**
 * Destroy per-thread REST API state
 *
 * @param thread_data Thread-local REST state
 */
void
chimera_rest_thread_destroy(
    void *thread_data);
