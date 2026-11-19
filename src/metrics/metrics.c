// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/host_file.h"
#include "common/thread_name.h"
#include <stdio.h>
#include <stdarg.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "common/macros.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"
#include "server/server_inspect.h"

#define CHIMERA_METRICS_MAX_WORKERS 4096

struct worker_export_sample {
    uint64_t cpu_ns, loop_iters, loop_iters_with_work;
};

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
    int                          port;
    enum chimera_tcp_flavor      flavor;
    struct prometheus_metrics   *metrics;
    struct evpl                 *evpl;
    struct evpl_thread          *thread;
    struct evpl_endpoint        *endpoint;
    struct evpl_listener        *listener;
    struct evpl_http_agent      *agent;
    struct evpl_http_server     *server;
    evpl_mutex_t                 inspect_lock;
    _Atomic(struct chimera_server_inspect *) inspect;
    struct worker_export_sample *previous;
    uint64_t                     previous_sample_ns;
    unsigned int                 previous_count;
};

static int
append_worker_metrics(
    struct chimera_metrics *metrics,
    char                   *buffer,
    size_t                  capacity,
    size_t                 *offset)
{
    evpl_mutex_lock(&metrics->inspect_lock);
    struct chimera_server_inspect *inspect = atomic_load_explicit(&metrics->inspect, memory_order_relaxed);

    if (!inspect) {
        evpl_mutex_unlock(&metrics->inspect_lock);
        return 0;
    }
    struct chimera_worker_sample  *samples = calloc(CHIMERA_METRICS_MAX_WORKERS, sizeof(*samples));
    if (!samples) {
        evpl_mutex_unlock(&metrics->inspect_lock);
        return -1;
    }
    unsigned int                   count = chimera_server_inspect_read_workers(inspect, samples,
                                                                               CHIMERA_METRICS_MAX_WORKERS);
    struct timespec                now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    uint64_t                       sample_ns  = (uint64_t) now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
    uint64_t                       elapsed_ns = metrics->previous_sample_ns && sample_ns > metrics->previous_sample_ns ?
        sample_ns - metrics->previous_sample_ns : 0;
#define APPEND(...) do { \
            int written = snprintf(buffer + *offset, capacity - *offset, __VA_ARGS__); \
            if (written < 0 || (size_t) written >= capacity - *offset) { \
                free(samples); evpl_mutex_unlock(&metrics->inspect_lock); return -1; \
            } \
            *offset += (size_t) written; \
} while (0)
    APPEND("# HELP chimera_worker_cpu_ns_total Cumulative worker CPU time in nanoseconds.\n"
           "# TYPE chimera_worker_cpu_ns_total counter\n"
           "# HELP chimera_worker_loop_iterations_total Cumulative worker event-loop iterations.\n"
           "# TYPE chimera_worker_loop_iterations_total counter\n"
           "# HELP chimera_worker_loop_work_iterations_total Cumulative worker iterations that handled work.\n"
           "# TYPE chimera_worker_loop_work_iterations_total counter\n"
           "# HELP chimera_worker_cpu_percent CPU use between the last two scrapes.\n"
           "# TYPE chimera_worker_cpu_percent gauge\n"
           "# HELP chimera_worker_busy_percent Event-loop iterations with work between the last two scrapes.\n"
           "# TYPE chimera_worker_busy_percent gauge\n"
           "# HELP chimera_worker_rpc_accepts_total RPC connections accepted by the worker.\n"
           "# TYPE chimera_worker_rpc_accepts_total counter\n"
           "# HELP chimera_worker_rpc_live_connections Current accepted RPC connections on the worker.\n"
           "# TYPE chimera_worker_rpc_live_connections gauge\n"
           "# HELP chimera_worker_rpc_rx_bytes_total RPC bytes received by the worker.\n"
           "# TYPE chimera_worker_rpc_rx_bytes_total counter\n"
           "# HELP chimera_worker_rpc_tx_bytes_total RPC bytes transmitted by the worker.\n"
           "# TYPE chimera_worker_rpc_tx_bytes_total counter\n"
           "# HELP chimera_worker_listener_assigned_total Connections assigned to this worker by a listener.\n"
           "# TYPE chimera_worker_listener_assigned_total counter\n"
           "# HELP chimera_worker_rdma_remote_ip_accepts_total RDMA RPC accepts by remote IP and worker.\n"
           "# TYPE chimera_worker_rdma_remote_ip_accepts_total counter\n"
           "# HELP chimera_worker_rdma_remote_ip_overflow_accepts_total RDMA accepts beyond the per-worker IP table.\n"
           "# TYPE chimera_worker_rdma_remote_ip_overflow_accepts_total counter\n"
           "# HELP chimera_worker_vfs_operations_total VFS operation completions by worker and operation.\n"
           "# TYPE chimera_worker_vfs_operations_total counter\n");
    for (unsigned int i = 0; i < count; i++) {
        APPEND("chimera_worker_cpu_ns_total{worker_id=\"%u\"} %" PRIu64 "\n"
               "chimera_worker_loop_iterations_total{worker_id=\"%u\"} %" PRIu64 "\n"
               "chimera_worker_loop_work_iterations_total{worker_id=\"%u\"} %" PRIu64 "\n",
               i, samples[i].cpu_ns, i, samples[i].loop.loop_iters,
               i, samples[i].loop.loop_iters_with_work);
        if (i < metrics->previous_count && elapsed_ns && samples[i].cpu_ns >= metrics->previous[i].cpu_ns &&
            samples[i].loop.loop_iters >= metrics->previous[i].loop_iters &&
            samples[i].loop.loop_iters_with_work >= metrics->previous[i].loop_iters_with_work) {
            uint64_t cpu_delta  = samples[i].cpu_ns - metrics->previous[i].cpu_ns;
            uint64_t iter_delta = samples[i].loop.loop_iters - metrics->previous[i].loop_iters;
            uint64_t work_delta = samples[i].loop.loop_iters_with_work - metrics->previous[i].loop_iters_with_work;
            APPEND("chimera_worker_cpu_percent{worker_id=\"%u\"} %.6f\n",
                   i, 100.0 * (double) cpu_delta / (double) elapsed_ns);
            if (iter_delta) {
                APPEND("chimera_worker_busy_percent{worker_id=\"%u\"} %.6f\n",
                       i, 100.0 * (double) work_delta / (double) iter_delta);
            }
        }
        metrics->previous[i].cpu_ns               = samples[i].cpu_ns;
        metrics->previous[i].loop_iters           = samples[i].loop.loop_iters;
        metrics->previous[i].loop_iters_with_work = samples[i].loop.loop_iters_with_work;
        struct chimera_inspect_sample rpc = { 0 };
        chimera_server_inspect_read_live_rpc(inspect, i, &rpc);
        if (rpc.has_rpc) {
            APPEND("chimera_worker_rpc_accepts_total{worker_id=\"%u\"} %" PRIu64 "\n"
                   "chimera_worker_rpc_live_connections{worker_id=\"%u\"} %" PRIu64 "\n"
                   "chimera_worker_rpc_rx_bytes_total{worker_id=\"%u\"} %" PRIu64 "\n"
                   "chimera_worker_rpc_tx_bytes_total{worker_id=\"%u\"} %" PRIu64 "\n",
                   i, rpc.rpc.accepts, i, rpc.rpc.active_connections,
                   i, rpc.rpc.rx_bytes, i, rpc.rpc.tx_bytes);
            for (unsigned int j = 0; j < rpc.rpc.nlisteners; j++) {
                const char *policy = "unknown";
                if (rpc.rpc.listeners[j].placement_policy == 1) {
                    policy = "round_robin";
                } else if (rpc.rpc.listeners[j].placement_policy == 2) {
                    policy = "distributed";
                }
                APPEND(
                    "chimera_worker_listener_assigned_total{worker_id=\"%u\",program=\"%u\",version=\"%u\",policy=\"%s\"} %"
                    PRIu64 "\n",
                    i, rpc.rpc.listeners[j].program, rpc.rpc.listeners[j].version,
                    policy, rpc.rpc.listeners[j].assigned_total);
            }
            for (unsigned int j = 0; j < rpc.rpc.nrdma_remote_ips; j++) {
                APPEND("chimera_worker_rdma_remote_ip_accepts_total{worker_id=\"%u\",remote_ip=\"%s\"} %" PRIu64 "\n",
                       i, rpc.rpc.rdma_remote_ips[j].address, rpc.rpc.rdma_remote_ips[j].accepts);
            }
            APPEND("chimera_worker_rdma_remote_ip_overflow_accepts_total{worker_id=\"%u\"} %" PRIu64 "\n",
                   i, rpc.rpc.rdma_remote_ip_overflow_accepts);
        }
        struct chimera_vfs_op_count vfs[CHIMERA_INSPECT_VFS_OPS];
        unsigned int                nvfs = chimera_server_inspect_read_vfs(inspect, i, CHIMERA_INSPECT_VFS_OPS, vfs);
        for (unsigned int j = 0; j < nvfs; j++) {
            APPEND("chimera_worker_vfs_operations_total{worker_id=\"%u\",operation=\"%s\"} %" PRIu64 "\n",
                   i, vfs[j].name, vfs[j].count);
        }
    }
    metrics->previous_count     = count;
    metrics->previous_sample_ns = sample_ns;
    free(samples);
    evpl_mutex_unlock(&metrics->inspect_lock);
#undef APPEND
    return 0;
} /* append_worker_metrics */

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
    size_t                  worker_metrics_len;

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

            worker_metrics_len = 0;
            if (append_worker_metrics(metrics, buf + len, (size_t) (cap - len), &worker_metrics_len) != 0) {
                evpl_iovec_release(evpl, &iov);
                evpl_http_server_set_response_length(request, 0);
                evpl_http_server_dispatch_default(request, 500);
                break;
            }

            len += (ssize_t) worker_metrics_len;

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
    int                     rc;

    metrics->metrics  = prometheus_metrics_create(NULL, NULL, 0);
    metrics->previous = calloc(CHIMERA_METRICS_MAX_WORKERS, sizeof(*metrics->previous));
    chimera_metrics_abort_if(!metrics->previous, "failed to allocate worker scrape history");

    metrics->agent = evpl_http_init(evpl);

    metrics->endpoint = chimera_tcp_flavor_endpoint_create(metrics->flavor,
                                                           "0.0.0.0",
                                                           metrics->port);
    metrics->listener = evpl_listener_create_named("chm_prom_listen");

    metrics->server = evpl_http_attach(metrics->agent, metrics->listener, chimera_metrics_dispatch, metrics);

    rc = evpl_listen(metrics->listener,
                     chimera_tcp_flavor_to_protocol(metrics->flavor),
                     metrics->endpoint);
    chimera_metrics_abort_if(rc, "failed to listen for prometheus metrics on port %d",
                             metrics->port);

    chimera_metrics_info("Serving prometheus metrics on http://0.0.0.0:%d/metrics", metrics->port);

    return metrics;
} /* chimera_metrics_thread_init */

static void
chimera_metrics_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_metrics *metrics = private_data;

    free(metrics->previous);
    evpl_mutex_destroy(&metrics->inspect_lock);
    prometheus_metrics_destroy(metrics->metrics);

    evpl_http_server_destroy(metrics->agent, metrics->server);
    evpl_listener_destroy(metrics->listener);
    evpl_http_destroy(metrics->agent);
    evpl_endpoint_close(metrics->endpoint);
} /* chimera_metrics_thread_shutdown */

SYMBOL_EXPORT struct chimera_metrics *
chimera_metrics_init(
    int                     port,
    enum chimera_tcp_flavor flavor)
{
    struct chimera_metrics *metrics = calloc(1, sizeof(*metrics));

    atomic_init(&metrics->inspect, NULL);
    evpl_mutex_init(&metrics->inspect_lock, NULL);
    metrics->port   = port;
    metrics->flavor = flavor;

    metrics->thread = evpl_thread_create(chimera_thread_config("chm_metrics"),
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

SYMBOL_EXPORT void
chimera_metrics_set_inspect(
    struct chimera_metrics        *metrics,
    struct chimera_server_inspect *inspect)
{
    evpl_mutex_lock(&metrics->inspect_lock);
    atomic_store_explicit(&metrics->inspect, inspect, memory_order_relaxed);
    evpl_mutex_unlock(&metrics->inspect_lock);
} /* chimera_metrics_set_inspect */

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
    if (chimera_host_fchmod(fileno(fp), 0644) < 0) {
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
