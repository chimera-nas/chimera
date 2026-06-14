// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/chimera_tracing.h"
#include "common/common_config.h"
#include "common/macros.h"

/*
 * oteltracing.h is always on the include path (header-only when the library is
 * not built: with OTEL_TRACING=0 it provides no-op inlines).  CHIMERA_HAVE_OTEL
 * is defined by CMake when liboteltracing-c is actually built and linked.
 */
#include "oteltracing.h"

#if CHIMERA_HAVE_OTEL_SQLITE
#include "otel_sqlite.h"
#endif /* CHIMERA_HAVE_OTEL_SQLITE */

#if CHIMERA_HAVE_OTEL_GRPC
#include "evpl/evpl.h"
#include "evpl/evpl_otel.h"
#endif /* CHIMERA_HAVE_OTEL_GRPC */

#define chimera_tracing_log(fmt, ...) \
        fprintf(stderr, "chimera tracing: " fmt "\n", ## __VA_ARGS__)

#if CHIMERA_HAVE_OTEL

/* Active once init succeeded; gates thread register/unregister + teardown so we
 * never touch the tracer when it was never initialized. */
static int g_tracing_on;

#if CHIMERA_HAVE_OTEL_SQLITE
static int g_sqlite_open;
#endif /* CHIMERA_HAVE_OTEL_SQLITE */

#if CHIMERA_HAVE_OTEL_GRPC

/* The OTLP/gRPC exporter is single-threaded: it lives on its own dedicated evpl
 * thread, which is the sole drain consumer and periodically flushes finished
 * spans to the collector.  The span producers (core threads) only ever enqueue
 * into their lock-free rings, so this export I/O never touches a hot path. */
static struct evpl_thread        *g_grpc_thread;
static struct evpl_otel_exporter *g_grpc_exporter;
static struct evpl_timer          g_grpc_timer;
static char                       g_grpc_host[256];
static int                        g_grpc_port;

#define OTEL_GRPC_FLUSH_INTERVAL_US 250000   /* 250 ms */

static void
chimera_tracing_grpc_flush(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (g_grpc_exporter) {
        evpl_otel_exporter_flush(g_grpc_exporter);
    }
} /* chimera_tracing_grpc_flush */

static void *
chimera_tracing_grpc_init(
    struct evpl *evpl,
    void        *private_data)
{
    g_grpc_exporter = evpl_otel_exporter_create(evpl, g_grpc_host, g_grpc_port);
    if (!g_grpc_exporter) {
        chimera_tracing_log("failed to create OTLP/gRPC exporter for %s:%d",
                            g_grpc_host, g_grpc_port);
        return NULL;
    }
    evpl_add_timer(evpl, &g_grpc_timer, chimera_tracing_grpc_flush,
                   OTEL_GRPC_FLUSH_INTERVAL_US);
    return NULL;
} /* chimera_tracing_grpc_init */

static void
chimera_tracing_grpc_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    if (g_grpc_exporter) {
        evpl_otel_exporter_flush(g_grpc_exporter);   /* final drain */
        evpl_remove_timer(evpl, &g_grpc_timer);
        evpl_otel_exporter_destroy(g_grpc_exporter);
        g_grpc_exporter = NULL;
    }
} /* chimera_tracing_grpc_shutdown */

/* Split "host:port" into g_grpc_host / g_grpc_port.  Returns 0 on success. */
static int
chimera_tracing_parse_endpoint(const char *endpoint)
{
    const char *colon = strrchr(endpoint, ':');
    size_t      hlen;

    if (!colon || colon == endpoint) {
        return -1;
    }
    hlen = (size_t) (colon - endpoint);
    if (hlen >= sizeof(g_grpc_host)) {
        return -1;
    }
    memcpy(g_grpc_host, endpoint, hlen);
    g_grpc_host[hlen] = '\0';
    g_grpc_port       = atoi(colon + 1);
    return g_grpc_port > 0 ? 0 : -1;
} /* chimera_tracing_parse_endpoint */

#endif /* CHIMERA_HAVE_OTEL_GRPC */

#endif /* CHIMERA_HAVE_OTEL */

SYMBOL_EXPORT int
chimera_tracing_init(json_t *config)
{
    struct chimera_common_tracing tc;

    chimera_common_tracing_config(config, &tc);

    if (!tc.enabled) {
        return 0;
    }

#if !CHIMERA_HAVE_OTEL
    chimera_tracing_log("enabled in config but oteltracing-c was not built; "
                        "tracing disabled");
    return 0;
#else  /* CHIMERA_HAVE_OTEL */

    otel_init("chimera");
    otel_set_sampler(tc.sample_rate);

    if (tc.grpc_endpoint) {
#if CHIMERA_HAVE_OTEL_GRPC
        if (chimera_tracing_parse_endpoint(tc.grpc_endpoint) != 0) {
            chimera_tracing_log("invalid grpc_endpoint '%s' (want host:port)",
                                tc.grpc_endpoint);
            otel_shutdown();
            return 0;
        }
        g_grpc_thread = evpl_thread_create(NULL, chimera_tracing_grpc_init,
                                           chimera_tracing_grpc_shutdown, NULL);
        if (!g_grpc_thread) {
            chimera_tracing_log("failed to start OTLP/gRPC exporter thread");
            otel_shutdown();
            return 0;
        }
        chimera_tracing_log("OTLP/gRPC export to %s, sample_rate %.4f",
                            tc.grpc_endpoint, tc.sample_rate);
#else  /* !CHIMERA_HAVE_OTEL_GRPC */
        chimera_tracing_log("grpc_endpoint set but evpl_otel was not built; "
                            "tracing disabled");
        otel_shutdown();
        return 0;
#endif /* CHIMERA_HAVE_OTEL_GRPC */
    } else if (tc.sqlite_path) {
#if CHIMERA_HAVE_OTEL_SQLITE
        if (otel_sqlite_open(tc.sqlite_path, 0) != 0) {
            chimera_tracing_log("failed to open span store '%s'", tc.sqlite_path);
            otel_shutdown();
            return 0;
        }
        g_sqlite_open = 1;
        chimera_tracing_log("SQLite span store '%s', sample_rate %.4f",
                            tc.sqlite_path, tc.sample_rate);
#else  /* !CHIMERA_HAVE_OTEL_SQLITE */
        chimera_tracing_log("sqlite_path set but the SQLite sink was not built; "
                            "tracing disabled");
        otel_shutdown();
        return 0;
#endif /* CHIMERA_HAVE_OTEL_SQLITE */
    } else {
        chimera_tracing_log("enabled but neither sqlite_path nor grpc_endpoint "
                            "set; tracing disabled");
        otel_shutdown();
        return 0;
    }

    g_tracing_on = 1;
    return 1;
#endif /* CHIMERA_HAVE_OTEL */
} /* chimera_tracing_init */

SYMBOL_EXPORT void
chimera_tracing_destroy(void)
{
#if CHIMERA_HAVE_OTEL
    if (!g_tracing_on) {
        return;
    }

#if CHIMERA_HAVE_OTEL_GRPC
    if (g_grpc_thread) {
        evpl_thread_destroy(g_grpc_thread);   /* runs grpc_shutdown: final flush */
        g_grpc_thread = NULL;
    }
#endif /* CHIMERA_HAVE_OTEL_GRPC */

#if CHIMERA_HAVE_OTEL_SQLITE
    if (g_sqlite_open) {
        otel_sqlite_close();                  /* stops flusher, final commit */
        g_sqlite_open = 0;
    }
#endif /* CHIMERA_HAVE_OTEL_SQLITE */

    otel_shutdown();
    g_tracing_on = 0;
#endif /* CHIMERA_HAVE_OTEL */
} /* chimera_tracing_destroy */

SYMBOL_EXPORT void
chimera_tracing_thread_register(void)
{
#if CHIMERA_HAVE_OTEL
    if (g_tracing_on) {
        otel_thread_register();
    }
#endif /* CHIMERA_HAVE_OTEL */
} /* chimera_tracing_thread_register */

SYMBOL_EXPORT void
chimera_tracing_thread_unregister(void)
{
#if CHIMERA_HAVE_OTEL
    if (g_tracing_on) {
        otel_thread_unregister();
    }
#endif /* CHIMERA_HAVE_OTEL */
} /* chimera_tracing_thread_unregister */
