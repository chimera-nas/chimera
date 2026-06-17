// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * chimera_tracing -- process-wide setup for OpenTelemetry span tracing.
 *
 * Wraps oteltracing-c (the embeddable span library) and its two exporters: the
 * optional SQLite local store (oteltracing-sqlite) and the OTLP/gRPC exporter
 * over libevpl's HTTP/2 client (evpl_otel).  All knobs come from the shared
 * "common.tracing" config section.  Tracing is off by default; when disabled (or
 * when the library was not built) every span call elsewhere in the tree compiles
 * or short-circuits to nothing.
 *
 * Lifecycle (daemon):
 *   chimera_tracing_init(config)         once, after evpl_init(), before threads
 *   chimera_tracing_thread_register()    on each span-producing (core) thread
 *   chimera_tracing_thread_unregister()  on each such thread at shutdown
 *   chimera_tracing_destroy()            once at process exit
 */

#pragma once

#include <jansson.h>

/*
 * Parse common.tracing and bring tracing up if enabled.  Returns 1 if tracing is
 * active afterwards, 0 if disabled or unavailable.  `config` is the parsed
 * top-level JSON (may be NULL).
 */
int chimera_tracing_init(
    json_t *config);

/* Tear down tracing (flush + close exporters).  Safe if init returned 0. */
void chimera_tracing_destroy(
    void);

/* Register/unregister the calling thread as a span producer.  No-ops unless
 * tracing came up. */
void chimera_tracing_thread_register(
    void);
void chimera_tracing_thread_unregister(
    void);
