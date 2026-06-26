// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "common/macros.h"
#include "prometheus-c.h"

#define chimera_metrics_debug(...) chimera_debug("metrics", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_metrics_info(...)  chimera_info("metrics", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_metrics_error(...) chimera_error("metrics", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_metrics_fatal(...) chimera_fatal("metrics", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_metrics_abort(...) chimera_abort("metrics", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_metrics_abort_if(cond, ...) \
        chimera_abort_if(cond, "metrics", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_metrics_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "metrics", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_metrics {
    int                        port;
    struct prometheus_metrics *metrics;
    struct evpl               *evpl;
    struct evpl_thread        *thread;
    struct evpl_endpoint      *endpoint;
    struct evpl_listener      *listener;
    struct evpl_http_agent    *agent;
    struct evpl_http_server   *server;
};

static void
chimera_metrics_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  notify_type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *notify_data,
    void                       *private_data)
{
    struct chimera_metrics *metrics = private_data;
    struct evpl_iovec       iov;
    char                   *buf;
    int                     cap;
    ssize_t                 len;
    int                     evpl_len;
    int                     n;

    switch (notify_type) {
        case EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE:

            if (request_type != EVPL_HTTP_REQUEST_TYPE_GET || strcmp(uri, "/metrics")) {
                evpl_http_server_set_response_length(request, 0);
                evpl_http_server_dispatch_default(request, 400);
                break;
            }

            n = evpl_iovec_alloc(evpl, 2 * 1024 * 1024, 0, 1, 0, &iov);

            if (n < 1) {
                evpl_http_server_set_response_length(request, 0);
                evpl_http_server_dispatch_default(request, 500);
                break;
            }

            buf = (char *) evpl_iovec_data(&iov);
            cap = evpl_iovec_length(&iov);

            /* Chimera's own metrics first, then libevpl's appended into
             * the remaining space.  Both emit Prometheus text format and
             * use disjoint name prefixes (chimera_* vs evpl_*), so the
             * concatenation is a valid single exposition page.
             */
            len = prometheus_metrics_scrape(metrics->metrics, buf, cap);

            if (len < 0) {
                evpl_iovec_release(evpl, &iov);
                evpl_http_server_set_response_length(request, 0);
                evpl_http_server_dispatch_default(request, 500);
                break;
            }

            evpl_len = evpl_metrics_scrape(buf + len, cap - len);

            if (evpl_len < 0) {
                evpl_iovec_release(evpl, &iov);
                evpl_http_server_set_response_length(request, 0);
                evpl_http_server_dispatch_default(request, 500);
                break;
            }

            len += evpl_len;

            evpl_iovec_set_length(&iov, len);

            evpl_http_server_set_response_length(request, len);
            evpl_http_request_add_header(request, "Content-Type", "text/plain; version=0.0.4");
            evpl_http_request_add_datav(request, &iov, 1);
            evpl_http_server_dispatch_default(request, 200);

            break;
        default:
            /* no action required */
            break;
    } /* switch */
} /* chimera_metrics_notify */

static void
chimera_metrics_dispatch(
    struct evpl                 *evpl,
    struct evpl_http_agent      *agent,
    struct evpl_http_request    *request,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data,
    void                        *private_data)
{
    *notify_callback = chimera_metrics_notify;
    *notify_data     = NULL;
} /* chimera_metrics_dispatch */

static void *
chimera_metrics_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_metrics *metrics = private_data;

    metrics->metrics = prometheus_metrics_create(NULL, NULL, 0);

    metrics->endpoint = evpl_endpoint_create("0.0.0.0", metrics->port);

    metrics->agent = evpl_http_init(evpl);

    metrics->endpoint = evpl_endpoint_create("0.0.0.0", metrics->port);
    metrics->listener = evpl_listener_create();

    metrics->server = evpl_http_attach(metrics->agent, metrics->listener, chimera_metrics_dispatch, metrics);

    evpl_listen(metrics->listener, EVPL_STREAM_SOCKET_TCP, metrics->endpoint);

    chimera_metrics_info("Serving prometheus metrics on http://0.0.0.0:%d/metrics", metrics->port);

    return metrics;
} /* chimera_metrics_thread_init */

static void
chimera_metrics_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_metrics *metrics = private_data;

    prometheus_metrics_destroy(metrics->metrics);

    evpl_http_server_destroy(metrics->agent, metrics->server);
    evpl_listener_destroy(metrics->listener);
    evpl_http_destroy(metrics->agent);
    evpl_endpoint_close(metrics->endpoint);
} /* chimera_metrics_thread_shutdown */

SYMBOL_EXPORT struct chimera_metrics *
chimera_metrics_init(int port)
{
    struct chimera_metrics *metrics = calloc(1, sizeof(*metrics));

    metrics->port = port;

    metrics->thread = evpl_thread_create(NULL,
                                         chimera_metrics_thread_init,
                                         chimera_metrics_thread_shutdown,
                                         metrics);

    return metrics;
} /* chimera_metrics_init */

SYMBOL_EXPORT void
chimera_metrics_destroy(struct chimera_metrics *metrics)
{
    evpl_thread_destroy(metrics->thread);

    free(metrics);
} /* chimera_metrics_destroy */

SYMBOL_EXPORT struct prometheus_metrics *
chimera_metrics_get(struct chimera_metrics *metrics)
{
    return metrics->metrics;
} /* chimera_metrics_get */

SYMBOL_EXPORT int
chimera_metrics_dump_file(
    struct prometheus_metrics *metrics,
    const char                *path)
{
    FILE  *fp;
    char  *buf;
    int    cap = 4 * 1024 * 1024;
    int    len, evpl_len;
    size_t written;

    if (!path) {
        return 0;
    }

    buf = malloc(cap);

    if (!buf) {
        chimera_metrics_error("Failed to allocate buffer to dump metrics to %s", path);
        return -1;
    }

    /* Same concatenation the live /metrics endpoint produces: chimera's own
     * registry first, then libevpl's appended into the remaining space.  Both
     * emit Prometheus text format with disjoint name prefixes (chimera_* vs
     * evpl_*), so the result is a single valid exposition page.
     */
    len = prometheus_metrics_scrape(metrics, buf, cap);

    if (len < 0) {
        chimera_metrics_error("Failed to scrape chimera metrics for %s", path);
        free(buf);
        return -1;
    }

    evpl_len = evpl_metrics_scrape(buf + len, cap - len);

    if (evpl_len > 0) {
        len += evpl_len;
    }

    fp = fopen(path, "w");

    if (!fp) {
        chimera_metrics_error("Failed to open metrics file %s for writing", path);
        free(buf);
        return -1;
    }

    /*
     * The daemon runs with umask(0), so fopen("w") would leave this file
     * world-writable (mode 0666).  Restrict it to 0644 (owner-write,
     * world-readable is fine for non-sensitive metrics) so other local users
     * cannot tamper with the dump -- CWE-276 (Incorrect Default Permissions).
     */
    if (fchmod(fileno(fp), 0644) < 0) {
        chimera_metrics_error("Failed to set metrics file permissions on %s", path);
    }

    written = fwrite(buf, 1, len, fp);

    fclose(fp);
    free(buf);

    if (written != (size_t) len) {
        chimera_metrics_error("Short write dumping metrics to %s", path);
        return -1;
    }

    chimera_metrics_info("Wrote %d bytes of metrics to %s", len, path);

    return 0;
} /* chimera_metrics_dump_file */
