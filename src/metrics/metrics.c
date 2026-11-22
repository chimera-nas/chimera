// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/host_file.h"
#include "common/thread_name.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef __linux__
#include <dirent.h>
#include <unistd.h>
#endif /* ifdef __linux__ */
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "common/macros.h"
#include "common/tcp_flavor.h"
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

/* Worker threads whose CPU time is exported.  A default server has 16 (eight
 * chm_core and eight chm_sync); threads past the limit are not exported. */
#define CHIMERA_METRICS_MAX_CPU_THREADS 128

struct chimera_metrics_thread_cpu {
    char                                name[16];
    uint64_t                            ticks;
    int                                 seen;
    struct prometheus_counter_series   *series;
    struct prometheus_counter_instance *instance;
};

struct chimera_metrics {
    int                               port;
    char                             *hostname;
    enum chimera_tcp_flavor           flavor;
    struct prometheus_metrics        *metrics;
    struct evpl                      *evpl;
    struct evpl_thread               *thread;
    struct evpl_endpoint             *endpoint;
    struct evpl_listener             *listener;
    struct evpl_http_agent           *agent;
    struct evpl_http_server          *server;
#ifdef __linux__
    struct prometheus_counter        *thread_cpu;
    uint64_t                          ns_per_tick;
    int                               num_cpu_threads;
    struct chimera_metrics_thread_cpu cpu_threads[CHIMERA_METRICS_MAX_CPU_THREADS];
#endif /* ifdef __linux__ */
};

#ifdef __linux__

/* The pools that do request work.  Helper threads (close, RCU reclaim,
 * logging) are left out so the series count follows the configured worker
 * counts and nothing else. */
static int
chimera_metrics_is_worker(const char *name)
{
    return strncmp(name, "chm_core_", 9) == 0 ||
           strncmp(name, "chm_sync_", 9) == 0 ||
           strncmp(name, "chm_async_", 10) == 0;
} /* chimera_metrics_is_worker */

/*
 * Set chimera_thread_cpu_nanoseconds_total from the CPU time the kernel
 * keeps for each thread.  Runs on the metrics thread just before a scrape:
 * the workers are not involved and nothing is added to a request path.
 * rate() of the counter is the fraction of a core a thread uses, which for
 * an event loop is also how busy the loop is.
 *
 * /proc/self/task/<tid>/stat is "tid (name) state ..." with utime and stime
 * as fields 14 and 15, in clock ticks.  Threads that share a name share a
 * series and are summed.  A thread that has exited keeps its last value.
 */
static void
chimera_metrics_refresh_thread_cpu(struct chimera_metrics *metrics)
{
    struct chimera_metrics_thread_cpu *thread;
    const char                        *label_name = "thread";
    const char                        *label_value;
    struct dirent                     *entry;
    DIR                               *dir;
    FILE                              *fp;
    char                               path[320], line[512];
    char                              *open_paren, *close_paren;
    unsigned long                      utime, stime;
    int                                i;

    dir = opendir("/proc/self/task");

    if (!dir) {
        return;
    }

    for (i = 0; i < metrics->num_cpu_threads; i++) {
        metrics->cpu_threads[i].ticks = 0;
        metrics->cpu_threads[i].seen  = 0;
    }

    while ((entry = readdir(dir))) {

        if (entry->d_name[0] < '0' || entry->d_name[0] > '9') {
            continue;
        }

        snprintf(path, sizeof(path), "/proc/self/task/%s/stat", entry->d_name);

        fp = fopen(path, "r");

        if (!fp) {
            continue; /* the thread exited since readdir */
        }

        if (!fgets(line, sizeof(line), fp)) {
            fclose(fp);
            continue;
        }

        fclose(fp);

        /* The name may itself contain parentheses, so take the last ')'. */
        open_paren  = strchr(line, '(');
        close_paren = strrchr(line, ')');

        if (!open_paren || !close_paren || close_paren < open_paren) {
            continue;
        }

        *close_paren = '\0';

        if (!chimera_metrics_is_worker(open_paren + 1)) {
            continue;
        }

        if (sscanf(close_paren + 1,
                   " %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu",
                   &utime, &stime) != 2) {
            continue;
        }

        thread = NULL;

        for (i = 0; i < metrics->num_cpu_threads; i++) {
            if (strcmp(metrics->cpu_threads[i].name, open_paren + 1) == 0) {
                thread = &metrics->cpu_threads[i];
                break;
            }
        }

        if (!thread) {
            if (metrics->num_cpu_threads == CHIMERA_METRICS_MAX_CPU_THREADS) {
                continue;
            }

            thread = &metrics->cpu_threads[metrics->num_cpu_threads++];

            snprintf(thread->name, sizeof(thread->name), "%s", open_paren + 1);

            label_value    = thread->name;
            thread->ticks  = 0;
            thread->series = prometheus_counter_create_series(metrics->thread_cpu,
                                                              &label_name, &label_value, 1);
            thread->instance = prometheus_counter_series_create_instance(thread->series);
        }

        thread->ticks += utime + stime;
        thread->seen   = 1;
    }

    closedir(dir);

    for (i = 0; i < metrics->num_cpu_threads; i++) {
        if (metrics->cpu_threads[i].seen) {
            metrics->cpu_threads[i].instance->value =
                metrics->cpu_threads[i].ticks * metrics->ns_per_tick;
        }
    }
} /* chimera_metrics_refresh_thread_cpu */

#endif /* ifdef __linux__ */

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
#ifdef __linux__
            chimera_metrics_refresh_thread_cpu(metrics);
#endif /* ifdef __linux__ */

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
    int                     rc;

    metrics->metrics = prometheus_metrics_create(NULL, NULL, 0);

#ifdef __linux__
    {
        long clk_tck = sysconf(_SC_CLK_TCK);

        metrics->ns_per_tick = 1000000000ULL / (clk_tck > 0 ? clk_tck : 100);
    }
    metrics->thread_cpu = prometheus_metrics_create_counter(
        metrics->metrics, "chimera_thread_cpu_nanoseconds_total",
        "CPU time used by each worker thread");
#endif /* ifdef __linux__ */

    metrics->agent = evpl_http_init(evpl);

    metrics->endpoint = chimera_tcp_flavor_endpoint_create(metrics->flavor,
                                                           metrics->hostname,
                                                           metrics->port);
    metrics->listener = evpl_listener_create_named("chm_prom_listen");

    metrics->server = evpl_http_attach(metrics->agent, metrics->listener, chimera_metrics_dispatch, metrics);

    rc = evpl_listen(metrics->listener,
                     chimera_tcp_flavor_to_protocol(metrics->flavor),
                     metrics->endpoint);
    chimera_metrics_abort_if(rc, "failed to listen for prometheus metrics on %s port %d",
                             metrics->hostname, metrics->port);

    chimera_metrics_info("Serving prometheus metrics on %s port %d (/metrics)",
                         metrics->hostname, metrics->port);

    return metrics;
} /* chimera_metrics_thread_init */

static void
chimera_metrics_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_metrics *metrics = private_data;

#ifdef __linux__
    for (int i = 0; i < metrics->num_cpu_threads; i++) {
        prometheus_counter_series_destroy_instance(metrics->cpu_threads[i].series,
                                                   metrics->cpu_threads[i].instance);
        prometheus_counter_destroy_series(metrics->thread_cpu,
                                          metrics->cpu_threads[i].series);
    }
    prometheus_counter_destroy(metrics->metrics, metrics->thread_cpu);
#endif /* ifdef __linux__ */

    prometheus_metrics_destroy(metrics->metrics);

    evpl_http_server_destroy(metrics->agent, metrics->server);
    evpl_listener_destroy(metrics->listener);
    evpl_http_destroy(metrics->agent);
    evpl_endpoint_close(metrics->endpoint);
} /* chimera_metrics_thread_shutdown */

SYMBOL_EXPORT struct chimera_metrics *
chimera_metrics_init(
    const char             *hostname,
    int                     port,
    enum chimera_tcp_flavor flavor)
{
    struct chimera_metrics *metrics = calloc(1, sizeof(*metrics));

    /* Own a copy: the listener is created later, on the metrics thread, and
     * the caller's string (e.g. from the parsed config) may be gone by then. */
    metrics->hostname = strdup(hostname ? hostname : "0.0.0.0");
    metrics->port     = port;
    metrics->flavor   = flavor;

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

    free(metrics->hostname);
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
