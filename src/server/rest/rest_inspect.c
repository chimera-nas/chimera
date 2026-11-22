// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <inttypes.h>
#include <limits.h>
#include "common/platform.h"
#ifndef _WIN32
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */
#include "evpl/evpl_http.h"
#include "server/server_inspect.h"
#include "rest_internal.h"

/* GET /api/v1/connections: the connections the server holds, collected from
 * the worker threads that own them (see server_inspect.h). */

struct rest_inspect_context {
    struct chimera_rest_thread       *thread;
    struct evpl_http_request         *request;
    struct chimera_inspect_collector *collector;
};

/* Counters can pass 2^53, which a JSON number does not carry exactly. */
static json_t *
decimal(uint64_t value)
{
    char text[32];

    snprintf(text, sizeof(text), "%" PRIu64, value);
    return json_string(text);
} /* decimal */

static json_t *
endpoint_json(const struct evpl_endpoint_info *endpoint)
{
    json_t *obj = json_pack("{s:s,s:s}", "family", endpoint->family, "address", endpoint->address);

    json_object_set_new(obj, "port", endpoint->port < 0 ? json_null() : json_integer(endpoint->port));

    if (endpoint->scope_id) {
        json_object_set_new(obj, "scope_id", json_integer(endpoint->scope_id));
    }

    return obj;
} /* endpoint_json */

static json_t *
connection_json(
    const struct chimera_inspect_sample     *sample,
    const struct chimera_inspect_connection *row,
    uint64_t                                 now_ns)
{
    char        id[48];
    const char *transport;
    json_t     *obj, *programs, *minors, *stats;
    uint64_t    age = now_ns >= row->bind.created_monotonic_ns ?
        now_ns - row->bind.created_monotonic_ns : 0;

    chimera_inspect_transport(row->bind.protocol, &transport);

    snprintf(id, sizeof(id), "%d:%" PRIu64, sample->worker_id, row->bind.serial);

    obj = json_pack("{s:s,s:i,s:s,s:s,s:s,s:s}",
                    "connection_id", id,
                    "worker_id", sample->worker_id,
                    "worker_name", sample->identity.name,
                    "state", row->bind.closing ? "closing" : "open",
                    "service", chimera_inspect_service_name(row->service),
                    "transport", transport);

    json_object_set_new(obj, "age_ms", decimal(age / 1000000));
    json_object_set_new(obj, "local", endpoint_json(&row->bind.local));
    json_object_set_new(obj, "remote", endpoint_json(&row->bind.remote));

    stats = json_object();
    json_object_set_new(stats, "rx_messages", decimal(row->rx_msgs));
    json_object_set_new(stats, "tx_messages", decimal(row->tx_msgs));
    json_object_set_new(stats, "rx_bytes", decimal(row->rx_bytes));
    json_object_set_new(stats, "tx_bytes", decimal(row->tx_bytes));
    json_object_set_new(stats, "requests_completed", decimal(row->requests_done));
    json_object_set_new(stats, "requests_in_flight", decimal(row->inflight));
    json_object_set_new(obj, "stats", stats);

    programs = json_array();
    minors   = json_array();

    for (int i = 0; i < row->nprograms; i++) {
        json_array_append_new(programs,
                              json_pack("{s:i,s:i}", "program", row->programs[i],
                                        "version", row->versions[i]));
    }

    for (int i = 0; i < 3; i++) {
        if (row->minor_versions & (1ULL << i)) {
            json_array_append_new(minors, json_integer(i));
        }
    }

    json_object_set_new(obj, "observed_rpc_programs", programs);
    json_object_set_new(obj, "nfs_minor_versions_seen", minors);
    json_object_set_new(obj, "rpc_framing",
                        json_string(row->rdma_framing ? "rpc_rdma" : "record_marking"));

    return obj;
} /* connection_json */

static void
collection_complete(
    const struct chimera_inspect_result *result,
    void                                *data)
{
    struct rest_inspect_context *ctx = data;
    struct timespec              now;
    uint64_t                     now_ns;
    json_t                      *obj, *missing, *rows;
    int                          truncated = 0;

    if (!ctx->request) {
        return;
    }

    if (!result->responded) {
        chimera_rest_send_error(ctx->thread->evpl, ctx->request, 503, "Service Unavailable",
                                "No worker responded");
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &now);
    now_ns = (uint64_t) now.tv_sec * 1000000000ULL + now.tv_nsec;

    missing = json_array();
    rows    = json_array();

    for (unsigned int i = 0; i < result->nworkers; i++) {
        const struct chimera_inspect_sample *sample = &result->samples[i];

        if (!result->present[i]) {
            json_array_append_new(missing, json_integer(sample->worker_id));
            continue;
        }

        truncated |= sample->truncated;

        for (unsigned int j = 0; j < sample->nrows; j++) {
            json_array_append_new(rows, connection_json(sample, &sample->rows[j], now_ns));
        }
    }

    obj = json_object();
    json_object_set_new(obj, "server_instance_id", json_string(result->server_instance_id));
    json_object_set_new(obj, "complete",
                        json_boolean(!truncated && result->responded == result->nworkers));
    json_object_set_new(obj, "truncated", json_boolean(truncated));
    json_object_set_new(obj, "missing_workers", missing);
    json_object_set_new(obj, "connections", rows);

    chimera_rest_send_json(ctx->thread->evpl, ctx->request, 200, obj);
} /* collection_complete */

static void
inspect_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *data,
    void                       *private_data)
{
    struct rest_inspect_context *ctx = data;

    if (type == EVPL_HTTP_NOTIFY_FAILED || type == EVPL_HTTP_NOTIFY_RESPONSE_COMPLETE) {
        /* The request is gone: stop the collection and drop our reference.
         * Workers still holding a task keep the collector alive until they
         * finish. */
        ctx->request = NULL;

        if (ctx->collector) {
            chimera_server_inspect_cancel(ctx->collector);
            chimera_server_inspect_release(ctx->collector);
        }

        free(ctx);
    } else if (type == EVPL_HTTP_NOTIFY_RECEIVE_DATA) {
        struct evpl_iovec iov[64];
        uint64_t          available;

        while ((available = evpl_http_request_get_data_avail(request)) != 0) {
            int n = evpl_http_request_get_datav(evpl, request, iov, available > 64 ? 64 : available);

            if (n <= 0) {
                break;
            }

            evpl_iovecs_release(evpl, iov, n);
        }
    }
} /* inspect_notify */

/* Percent-decode one query component into printable ASCII. */
static int
decode(
    const char *input,
    size_t      len,
    char       *output,
    size_t      size)
{
    size_t used = 0;

    for (size_t i = 0; i < len; i++) {
        unsigned char ch = input[i];

        if (ch == '%') {
            int value = 0;

            if (i + 2 >= len) {
                return -1;
            }

            for (int j = 0; j < 2; j++) {
                unsigned char h     = input[++i];
                int           digit = h >= '0' && h <= '9' ? h - '0' :
                    h >= 'a' && h <= 'f' ? h - 'a' + 10 :
                    h >= 'A' && h <= 'F' ? h - 'A' + 10 : -1;

                if (digit < 0) {
                    return -1;
                }

                value = value * 16 + digit;
            }

            ch = value;
        }

        if (ch < 33 || ch > 126 || used + 1 >= size) {
            return -1;
        }

        output[used++] = ch;
    }

    output[used] = 0;

    return used ? 0 : -1;
} /* decode */

static int
number(
    const char *text,
    int         max,
    int        *out)
{
    uint64_t value = 0;

    if (!*text) {
        return -1;
    }

    for (; *text; text++) {
        if (*text < '0' || *text > '9') {
            return -1;
        }

        value = value * 10 + *text - '0';

        if (value > (unsigned int) max) {
            return -1;
        }
    }

    *out = value;
    return 0;
} /* number */

/* worker_id, service, remote_ip, remote_port, local_ip, local_port and
 * transport narrow the result and are ANDed.  An unknown, repeated, empty or
 * malformed parameter is an error. */
static int
parse_filter(
    const char                    *query,
    int                            len,
    struct chimera_inspect_filter *filter)
{
    static const char *keys[] = { "worker_id", "service",     "remote_ip",     "remote_port",
                                  "local_ip",  "local_port",  "transport" };
    unsigned int       seen = 0;

    if (!len) {
        return 0;
    }

    if (len > 2048) {
        return -1;
    }

    while (len) {
        const char *end   = memchr(query, '&', len);
        int         part  = end ? end - query : len;
        const char *equal = memchr(query, '=', part);
        char        key[32], value[256];
        int         index;

        if (!equal ||
            decode(query, equal - query, key, sizeof(key)) ||
            decode(equal + 1, part - (equal - query) - 1, value, sizeof(value))) {
            return -1;
        }

        for (index = 0; index < 7 && strcmp(keys[index], key); index++) {
        }

        if (index == 7 || (seen & (1U << index))) {
            return -1;
        }

        seen |= 1U << index;

        if (index == 0) {
            if (number(value, INT_MAX, &filter->worker_id)) {
                return -1;
            }
        } else if (index == 1) {
            int service;

            for (service = 0; service < CHIMERA_INSPECT_SERVICES &&
                 strcmp(value, chimera_inspect_service_name(service)); service++) {
            }

            if (service == CHIMERA_INSPECT_SERVICES) {
                return -1;
            }

            filter->services = 1U << service;
        } else if (index == 2 || index == 4) {
            unsigned char address[16];
            int           family = strchr(value, ':') ? AF_INET6 : AF_INET;

            if (inet_pton(family, value, address) != 1) {
                return -1;
            }

            inet_ntop(family, address, index == 2 ? filter->remote_ip : filter->local_ip, 64);
        } else if (index == 3 || index == 5) {
            if (number(value, 65535, index == 3 ? &filter->remote_port : &filter->local_port)) {
                return -1;
            }
        } else {
            if (strcmp(value, "tcp") && strcmp(value, "udp") && strcmp(value, "rdma") &&
                strcmp(value, "unix") && strcmp(value, "inproc") && strcmp(value, "fabric")) {
                return -1;
            }

            snprintf(filter->transport, sizeof(filter->transport), "%.31s", value);
        }

        if (!end) {
            break;
        }

        len  -= part + 1;
        query = end + 1;

        if (!len) {
            return -1;
        }
    }

    return 0;
} /* parse_filter */

void
chimera_rest_inspect_connections(
    struct chimera_rest_thread  *thread,
    struct evpl_http_request    *request,
    const char                  *query,
    int                          query_len,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data)
{
    /* Without a service filter only the NFS service is listed; the mount,
     * lock, status and portmap listeners are selected by name. */
    struct chimera_inspect_filter filter = {
        .worker_id       = -1,
        .services        = 1U << CHIMERA_INSPECT_NFS,
            .remote_port = -1,
            .local_port  = -1,
    };
    struct rest_inspect_context  *ctx;
    int                           status;

    if (parse_filter(query, query_len, &filter)) {
        chimera_rest_send_error(thread->evpl, request, 400, "Bad Request",
                                "Invalid inspection filters");
        return;
    }

    ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable",
                                "Inspection allocation failed");
        return;
    }

    ctx->thread  = thread;
    ctx->request = request;

    status = chimera_server_inspect_collect(chimera_server_get_inspect(thread->shared->server),
                                            thread->evpl, &filter, collection_complete, ctx,
                                            &ctx->collector);

    if (status) {
        free(ctx);

        if (status == 429) {
            evpl_http_request_add_header(request, "Retry-After", "1");
        }

        chimera_rest_send_error(thread->evpl, request, status,
                                status == 429 ? "Too Many Requests" :
                                status == 400 ? "Bad Request" : "Service Unavailable",
                                status == 429 ? "A collection is still outstanding" :
                                status == 400 ? "No such worker" : "Inspection unavailable");
        return;
    }

    *notify_callback = inspect_notify;
    *notify_data     = ctx;
} /* chimera_rest_inspect_connections */
