// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <time.h>
#include "common/platform.h"
#ifndef _WIN32
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */
#include "evpl/evpl_http.h"
#include "server/server_inspect.h"
#include "rest_internal.h"

static json_t * decimal(
    uint64_t value);

void
chimera_rest_inspect_nfs_sessions(
    struct chimera_rest_thread *thread,
    struct evpl_http_request   *request,
    const char                 *query,
    int                         query_len)
{
    struct chimera_server_inspect *inspect = chimera_server_get_inspect(thread->shared->server);
    unsigned int                   limit = 100, offset = 0, total = 0;
    uint64_t                       client_filter     = 0;
    int                            has_client_filter = 0;
    int                            seen_limit = 0, seen_offset = 0, seen_client = 0;
    char                          *query_copy = NULL;

    if (query_len < 0 || query_len > 1024) {
        chimera_rest_send_error(thread->evpl, request, 400, "Bad Request", "Invalid query string");
        return;
    }
    if (query_len) {
        query_copy = strndup(query, (size_t) query_len);
        if (!query_copy) {
            chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable", "Inspection allocation failed");
            return;
        }
        char *save = NULL;
        for (char *part = strtok_r(query_copy, "&", &save); part; part = strtok_r(NULL, "&", &save)) {
            char              *equal = strchr(part, '=');
            if (!equal || equal == part || !equal[1]) {
                free(query_copy);
                chimera_rest_send_error(thread->evpl, request, 400, "Bad Request", "Invalid query parameter");
                return;
            }
            *equal++ = '\0';
            char              *end = NULL;
            errno = 0;
            unsigned long long value = strtoull(equal, &end, 10);
            if (errno || !end || *end) {
                free(query_copy);
                chimera_rest_send_error(thread->evpl, request, 400, "Bad Request", "Invalid numeric query parameter");
                return;
            }
            if (!strcmp(part, "limit") && !seen_limit && value > 0 && value <= CHIMERA_INSPECT_ROWS) {
                limit      = (unsigned int) value;
                seen_limit = 1;
            } else if (!strcmp(part, "offset") && !seen_offset && value <= UINT_MAX) {
                offset      = (unsigned int) value;
                seen_offset = 1;
            } else if (!strcmp(part, "client_id") && !seen_client) {
                client_filter     = (uint64_t) value;
                has_client_filter = 1;
                seen_client       = 1;
            } else {
                free(query_copy);
                chimera_rest_send_error(thread->evpl, request, 400, "Bad Request",
                                        "Unknown or out-of-range query parameter");
                return;
            }
        }
        free(query_copy);
    }

    struct chimera_inspect_session *rows = calloc(CHIMERA_INSPECT_ROWS, sizeof(*rows));
    if (!rows) {
        chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable", "Inspection allocation failed");
        return;
    }
    unsigned int                    count = chimera_server_inspect_read_sessions(inspect, rows, CHIMERA_INSPECT_ROWS,
                                                                                 &total);
    json_t                         *result   = json_object();
    json_t                         *sessions = json_array();
    unsigned int                    matching = 0, emitted = 0;
    for (unsigned int i = 0; i < count; i++) {
        const struct chimera_inspect_session *session = &rows[i];
        if (has_client_filter && session->client_id != client_filter) {
            continue;
        }
        if (matching++ < offset || emitted >= limit) {
            continue;
        }
        json_t                               *item = json_pack("{s:s,s:b}", "session_id", session->session_id,
                                                               "has_slots_in_use", session->has_slots_in_use);
        json_object_set_new(item, "client_id", decimal(session->client_id));
        json_object_set_new(item, "created_at_monotonic_ns", decimal(session->created_monotonic_ns));
        json_object_set_new(item, "created_at_wall_ns", decimal(session->created_wall_ns));
        json_object_set_new(item, "requested_slots", json_integer(session->requested_slots));
        json_object_set_new(item, "negotiated_slots", json_integer(session->negotiated_slots));
        json_object_set_new(item, "server_max_slots", json_integer(session->server_max_slots));
        json_object_set_new(item, "slots_in_progress", json_integer(session->slots_in_progress));
        json_object_set_new(item, "slots_cached", json_integer(session->slots_cached));
        json_object_set_new(item, "slots_completed", json_integer(session->slots_completed));
        json_object_set_new(item, "slots_unused", json_integer(session->slots_unused));
        json_object_set_new(item, "replay_hits", decimal(session->replay_hits));
        json_object_set_new(item, "seq_misordered", decimal(session->seq_misordered));
        json_object_set_new(item, "bad_slot", decimal(session->bad_slot));
        json_object_set_new(item, "delay_errors", decimal(session->delay_errors));
        json_object_set_new(item, "slot_in_progress_retries",
                            decimal(session->slot_in_progress_retries));
        json_t      *connections = json_array();
        json_t      *workers     = json_array();
        int          worker_seen[64];
        unsigned int nworkers = 0;
        for (unsigned int j = 0; j < session->connection_count; j++) {
            char    connection_id[48];
            snprintf(connection_id, sizeof(connection_id), "%d:%" PRIu64,
                     session->connections[j].worker_id, session->connections[j].serial);
            json_t *connection = json_pack("{s:s,s:i}", "connection_id", connection_id,
                                           "worker_id", session->connections[j].worker_id);
            json_array_append_new(connections, connection);
            int     found = 0;
            for (unsigned int k = 0; k < nworkers; k++) {
                found |= worker_seen[k] == session->connections[j].worker_id;
            }
            if (!found && nworkers < 64) {
                worker_seen[nworkers++] = session->connections[j].worker_id;
                json_array_append_new(workers, json_integer(session->connections[j].worker_id));
            }
        }
        json_object_set_new(item, "connections", connections);
        json_object_set_new(item, "workers", workers);
        json_object_set_new(item, "connections_truncated", json_boolean(session->connections_truncated));
        json_object_set_new(item, "max_request_size", json_integer(session->max_request_size));
        json_object_set_new(item, "max_response_size", json_integer(session->max_response_size));
        json_object_set_new(item, "max_response_size_cached", json_integer(session->max_response_size_cached));
        json_object_set_new(item, "highest_slot_in_use", session->has_slots_in_use ?
                            json_integer(session->highest_slot_in_use) : json_null());
        json_array_append_new(sessions, item);
        emitted++;
    }
    int truncated = total > count ||
        (has_client_filter ? matching : total) > offset + emitted;
    json_object_set_new(result, "sessions", sessions);
    json_object_set_new(result, "total", json_integer(has_client_filter ? matching : total));
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "truncated", json_boolean(truncated));
    json_object_set_new(result, "complete", json_boolean(!truncated));
    json_object_set_new(result, "server_instance_id",
                        json_string(chimera_server_inspect_instance_id(inspect)));
    free(rows);
    chimera_rest_send_json(thread->evpl, request, 200, result);
} /* chimera_rest_inspect_nfs_sessions */

struct rest_inspect_context {
    struct chimera_rest_thread       *thread;
    struct evpl_http_request         *request;
    struct chimera_inspect_collector *collector;
    int                               connections;
};

static json_t *
decimal(uint64_t value)
{
    char text[32];

    snprintf(text, sizeof(text), "%" PRIu64, value);
    return json_string(text);
} /* decimal */
static json_t *
signed_decimal(int64_t value)
{
    char text[32];

    snprintf(text, sizeof(text), "%" PRId64, value);
    return json_string(text);
} /* signed_decimal */

void
chimera_rest_inspect_nfs_slots(
    struct chimera_rest_thread *thread,
    struct evpl_http_request   *request,
    const char                 *session_id)
{
    struct chimera_server_inspect *inspect = chimera_server_get_inspect(thread->shared->server);
    struct chimera_inspect_slot   *slots   = calloc(CHIMERA_INSPECT_ROWS, sizeof(*slots));
    unsigned int                   total   = 0;

    if (!slots) {
        chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable", "Inspection allocation failed");
        return;
    }
    int                            count = chimera_server_inspect_read_session_slots(inspect, session_id, slots,
                                                                                     CHIMERA_INSPECT_ROWS, &total);
    if (count < 0) {
        free(slots);
        chimera_rest_send_error(thread->evpl, request, 404, "Not Found", "Unknown NFS session");
        return;
    }
    json_t                        *result = json_object();
    json_t                        *rows   = json_array();
    for (int i = 0; i < count; i++) {
        json_t *row = json_pack("{s:i,s:s,s:i}", "slot", slots[i].slot,
                                "state", slots[i].state, "seqid", slots[i].seqid);
        if (slots[i].has_age) {
            json_object_set_new(row, "age_ns", decimal(slots[i].age_ns));
            json_object_set_new(row, "operation", json_null());
        }
        json_array_append_new(rows, row);
    }
    json_object_set_new(result, "slots", rows);
    json_object_set_new(result, "total", json_integer(total));
    json_object_set_new(result, "truncated", json_boolean(total > (unsigned int) count));
    json_object_set_new(result, "complete", json_boolean(total <= (unsigned int) count));
    json_object_set_new(result, "server_instance_id",
                        json_string(chimera_server_inspect_instance_id(inspect)));
    free(slots);
    chimera_rest_send_json(thread->evpl, request, 200, result);
} /* chimera_rest_inspect_nfs_slots */

static uint64_t
approx_percentile_ticks(
    uint64_t        total,
    const uint64_t *buckets,
    unsigned int    bucket_count,
    unsigned int    percentile)
{
    uint64_t target     = total / 100 * percentile + ((total % 100) * percentile + 99) / 100;
    uint64_t cumulative = 0;

    if (!total) {
        return 0;
    }
    for (unsigned int i = 0; i < bucket_count; i++) {
        cumulative += buckets[i];
        if (cumulative >= target) {
            return i == 63 ? UINT64_MAX : UINT64_C(1) << (i + 1);
        }
    }
    return UINT64_MAX;
} /* approx_percentile_ticks */

static void
latency_json_fields(
    json_t         *item,
    uint64_t        count,
    uint64_t        sum_ticks,
    const uint64_t *buckets,
    unsigned int    bucket_count)
{
    if (!count) {
        return;
    }
    uint64_t mean_ns = stopwatch_ticks_to_ns(&prometheus_stopwatch_ctx, sum_ticks / count);
    uint64_t p50_ns  = stopwatch_ticks_to_ns(&prometheus_stopwatch_ctx,
                                             approx_percentile_ticks(count, buckets, bucket_count, 50));
    uint64_t p99_ns = stopwatch_ticks_to_ns(&prometheus_stopwatch_ctx,
                                            approx_percentile_ticks(count, buckets, bucket_count, 99));
    json_object_set_new(item, "latency_avg_ns", decimal(mean_ns));
    json_object_set_new(item, "latency_p50_ns", decimal(p50_ns));
    json_object_set_new(item, "latency_p99_ns", decimal(p99_ns));
} /* latency_json_fields */

static json_t *
procedure_json(const struct evpl_rpc2_procedure_stats *stats)
{
    json_t *item = json_pack("{s:i,s:i,s:i,s:s}", "program", stats->program,
                             "version", stats->version, "procedure", stats->procedure,
                             "name", stats->name);

    json_object_set_new(item, "latency_samples_total", decimal(stats->latency_samples_total));
    latency_json_fields(item, stats->latency_samples_total, stats->latency_sum_ticks,
                        stats->latency_buckets, stats->latency_bucket_count);
    return item;
} /* procedure_json */

void
chimera_rest_inspect_live(
    struct chimera_rest_thread *thread,
    struct evpl_http_request   *request,
    int                         include_sessions)
{
    struct chimera_server_inspect *inspect  = chimera_server_get_inspect(thread->shared->server);
    unsigned int                   capacity = 4096;
    struct chimera_worker_sample  *samples  = calloc(capacity, sizeof(*samples));

    if (!samples) {
        chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable", "Inspection allocation failed");
        return;
    }
    unsigned int                   count = chimera_server_inspect_read_workers(inspect, samples, capacity);
    json_t                        *obj = json_object(), *workers = json_array();
    struct timespec                now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    uint64_t                       sampled_monotonic_ns = (uint64_t) now.tv_sec * 1000000000 + now.tv_nsec;
    for (unsigned int i = 0; i < count; i++) {
        const struct chimera_worker_sample *sample = &samples[i];
        json_t                             *worker = json_pack("{s:i,s:s,s:s}",
                                                               "worker_id", (int) sample->worker_id,
                                                               "worker_name", sample->identity.name,
                                                               "executor", sample->identity.executor);
        json_object_set_new(worker, "os_tid", decimal(sample->os_tid));
        json_object_set_new(worker, "cpu_ns", decimal(sample->cpu_ns));
        json_object_set_new(worker, "runq_wait_ns", decimal(sample->runq_wait_ns));
        json_object_set_new(worker, "loop_iters", decimal(sample->loop.loop_iters));
        json_object_set_new(worker, "loop_iters_with_work", decimal(sample->loop.loop_iters_with_work));
        struct chimera_vfs_op_count   vfs[CHIMERA_INSPECT_VFS_OPS];
        unsigned int                  nvfs = chimera_server_inspect_read_vfs(inspect, sample->worker_id,
                                                                             CHIMERA_INSPECT_VFS_OPS, vfs);
        json_t                       *operations = json_array();
        for (unsigned int j = 0; j < nvfs; j++) {
            json_t *operation = json_pack("{s:s}", "name", vfs[j].name);
            json_object_set_new(operation, "latency_samples_total", decimal(vfs[j].count));
            latency_json_fields(operation, vfs[j].count, vfs[j].sum_ticks,
                                vfs[j].buckets, vfs[j].bucket_count);
            json_array_append_new(operations, operation);
        }
        json_t                       *vfs_object = json_object();
        json_object_set_new(vfs_object, "operations", operations);
        json_object_set_new(worker, "vfs", vfs_object);
        struct chimera_inspect_sample rpc_sample = { 0 };
        chimera_server_inspect_read_live_rpc(inspect, sample->worker_id, &rpc_sample);
        if (rpc_sample.has_rpc) {
            json_t *rpc = json_object(), *procedures = json_array(), *listeners = json_array(), *remote_ips = json_array
                    ();
            json_object_set_new(worker, "rpc_thread_id", json_integer(rpc_sample.rpc.thread_id));
            json_object_set_new(rpc, "server_inflight", signed_decimal(rpc_sample.rpc.server_inflight));
            json_object_set_new(rpc, "client_inflight", signed_decimal(rpc_sample.rpc.client_inflight));
            json_object_set_new(rpc, "rx_msgs", decimal(rpc_sample.rpc.rx_msgs));
            json_object_set_new(rpc, "tx_msgs", decimal(rpc_sample.rpc.tx_msgs));
            json_object_set_new(rpc, "rx_bytes", decimal(rpc_sample.rpc.rx_bytes));
            json_object_set_new(rpc, "tx_bytes", decimal(rpc_sample.rpc.tx_bytes));
            json_object_set_new(rpc, "rpc_errors", decimal(rpc_sample.rpc.rpc_errors));
            json_object_set_new(rpc, "accepts", decimal(rpc_sample.rpc.accepts));
            json_object_set_new(rpc, "closes", decimal(rpc_sample.rpc.closes));
            json_object_set_new(rpc, "live_connections", decimal(rpc_sample.rpc.active_connections));
            for (unsigned int j = 0; j < rpc_sample.rpc.nlisteners; j++) {
                const char *policy = "unknown";
                if (rpc_sample.rpc.listeners[j].placement_policy == 1) {
                    policy = "round_robin";
                } else if (rpc_sample.rpc.listeners[j].placement_policy == 2) {
                    policy = "distributed";
                }
                json_t     *listener = json_pack("{s:i,s:i,s:s}",
                                                 "program", rpc_sample.rpc.listeners[j].program,
                                                 "version", rpc_sample.rpc.listeners[j].version,
                                                 "placement_policy", policy);
                json_object_set_new(listener, "assigned_total", decimal(rpc_sample.rpc.listeners[j].assigned_total));
                json_object_set_new(listener, "live_connections", decimal(rpc_sample.rpc.listeners[j].live_connections))
                ;
                json_array_append_new(listeners, listener);
            }
            json_object_set_new(rpc, "listeners", listeners);
            for (unsigned int j = 0; j < rpc_sample.rpc.nrdma_remote_ips; j++) {
                json_t *remote = json_pack("{s:s}", "address", rpc_sample.rpc.rdma_remote_ips[j].address);
                json_object_set_new(remote, "accepts", decimal(rpc_sample.rpc.rdma_remote_ips[j].accepts));
                json_array_append_new(remote_ips, remote);
            }
            json_object_set_new(rpc, "rdma_remote_ips", remote_ips);
            json_object_set_new(rpc, "rdma_remote_ip_overflow_accepts",
                                decimal(rpc_sample.rpc.rdma_remote_ip_overflow_accepts));
            for (unsigned int j = 0; j < rpc_sample.nprocedures; j++) {
                json_array_append_new(procedures, procedure_json(&rpc_sample.procedures[j]));
            }
            json_object_set_new(rpc, "procedures", procedures);
            json_object_set_new(worker, "rpc", rpc);
        }
        json_array_append_new(workers, worker);
    }
    json_object_set_new(obj, "threads", workers);
    json_object_set_new(obj, "live", json_true());
    json_object_set_new(obj, "complete", json_true());
    json_object_set_new(obj, "server_instance_id", json_string(chimera_server_inspect_instance_id(inspect)));
    json_object_set_new(obj, "sampled_monotonic_ns", decimal(sampled_monotonic_ns));
    if (include_sessions) {
        const unsigned int              session_capacity = 4096;
        struct chimera_inspect_session *sessions         = calloc(session_capacity, sizeof(*sessions));
        unsigned int                    session_total    = 0;
        if (!sessions) {
            free(samples);
            json_decref(obj);
            chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable",
                                    "Session snapshot allocation failed");
            return;
        }
        unsigned int                    session_count = chimera_server_inspect_read_sessions(
            inspect, sessions, session_capacity, &session_total);
        json_t                         *session_array = json_array();
        for (unsigned int i = 0; i < session_count; i++) {
            const struct chimera_inspect_session *session = &sessions[i];
            json_t                               *item    = json_pack("{s:s,s:i,s:b}",
                                                                      "session_id", session->session_id,
                                                                      "negotiated_slots", (int) session->
                                                                      negotiated_slots,
                                                                      "has_slots_in_use", session->has_slots_in_use);
            json_object_set_new(item, "client_id", decimal(session->client_id));
            if (session->has_slots_in_use) {
                json_object_set_new(item, "highest_slot_in_use",
                                    json_integer(session->highest_slot_in_use));
            } else {
                json_object_set_new(item, "highest_slot_in_use", json_null());
            }
            json_array_append_new(session_array, item);
        }
        json_object_set_new(obj, "nfs4_sessions", session_array);
        json_object_set_new(obj, "nfs4_sessions_total", json_integer(session_total));
        json_object_set_new(obj, "nfs4_sessions_truncated",
                            json_boolean(session_total > session_count));
        free(sessions);
    }
    free(samples);
    chimera_rest_send_json(thread->evpl, request, 200, obj);
} /* chimera_rest_inspect_live */
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
    const struct chimera_inspect_connection *row)
{
    char        id[48];
    const char *transport, *backend, *raw;

    chimera_inspect_transport(row->bind.protocol, &transport, &backend, &raw);
    snprintf(id, sizeof(id), "%d:%" PRIu64, sample->worker_id, row->bind.serial);
    json_t     *obj = json_pack("{s:s,s:i,s:s,s:s,s:s,s:s,s:s,s:s,s:s}",
                                "connection_id", id, "worker_id", sample->worker_id, "worker_name", sample->identity.
                                name,
                                "origin", "accepted", "state", row->bind.closing ? "closing" : "open",
                                "service", chimera_inspect_service_name(row->service), "transport", transport,
                                "backend", backend, "raw_transport", raw);
    json_object_set_new(obj, "os_tid", decimal(sample->os_tid));
    uint64_t    age = sample->sampled_monotonic_ns >= row->bind.created_monotonic_ns ?
        sample->sampled_monotonic_ns - row->bind.created_monotonic_ns : 0;
    json_object_set_new(obj, "age_ms", decimal(age / 1000000));
    json_object_set_new(obj, "local", endpoint_json(&row->bind.local));
    json_object_set_new(obj, "remote", endpoint_json(&row->bind.remote));
    json_object_set_new(obj, "per_connection_stats", json_null());
    if (row->service <= CHIMERA_INSPECT_RPCBIND) {
        json_t *programs = json_array(), *minors = json_array();
        for (int i = 0; i < row->nprograms; i++) {
            json_array_append_new(programs, json_pack("{s:i,s:i}", "program", row->programs[i], "version", row->versions
                                                      [i]));
        }
        for (int i = 0; i < 3; i++) {
            if (row->minor_versions & (1ULL << i)) {
                json_array_append_new(minors, json_integer(i));
            }
        }
        json_object_set_new(obj, "observed_rpc_programs", programs);
        json_object_set_new(obj, "nfs_minor_versions_seen", minors);
        json_object_set_new(obj, "rpc_framing", json_string(row->rdma_framing ? "rpc_rdma" : "record_marking"));
        json_object_set_new(obj, "per_connection_stats", json_pack("{s:o,s:o,s:o,s:o,s:o,s:o}",
                                                                   "rx_msgs", decimal(row->rx_msgs),
                                                                   "tx_msgs", decimal(row->tx_msgs),
                                                                   "rx_bytes", decimal(row->rx_bytes),
                                                                   "tx_bytes", decimal(row->tx_bytes),
                                                                   "requests_done", decimal(row->requests_done),
                                                                   "inflight", decimal(row->inflight)));
    } else if (row->service == CHIMERA_INSPECT_SMB) {
        json_object_set_new(obj, "negotiated_dialect", row->smb_dialect ? json_integer(row->smb_dialect) : json_null());
        json_object_set_new(obj, "per_connection_stats", json_pack("{s:I,s:i}",
                                                                   "messages_received", (json_int_t) row->
                                                                   messages_received, "counter_bits", 32));
    } else if (row->service == CHIMERA_INSPECT_S3) {
        json_object_set_new(obj, "http_framing", json_string(row->http_framing));
    }
    return obj;
} /* connection_json */
static json_t *
sample_json(
    const struct chimera_inspect_sample *sample,
    int                                  connections,
    const char                          *sampled_at)
{
    json_t *obj = json_pack("{s:i,s:s}", "worker_id", sample->worker_id, "sampled_at", sampled_at);

    json_object_set_new(obj, "sampled_monotonic_ns", decimal(sample->sampled_monotonic_ns));
    json_object_set_new(obj, "truncated", json_boolean(sample->truncated));
    if (connections) {
        json_object_set_new(obj, "scanned", json_integer(sample->scanned));
        json_object_set_new(obj, "returned", json_integer(sample->nrows));
        return obj;
    }
    json_object_set_new(obj, "worker_name", json_string(sample->identity.name));
    json_object_set_new(obj, "executor", json_string(sample->identity.executor));
    json_object_set_new(obj, "os_tid", decimal(sample->os_tid));
    json_t *counts = json_object();
    for (int i = 0; i < CHIMERA_INSPECT_SERVICES; i++) {
        json_object_set_new(counts, chimera_inspect_service_name(i), decimal(sample->connection_counts[i]));
    }
    json_object_set_new(obj, "connection_counts", counts);
    if (sample->has_rpc) {
        json_object_set_new(obj, "rpc_thread_id", json_integer(sample->rpc.thread_id));
        json_t *rpc = json_object(), *procedures = json_array();
        json_object_set_new(rpc, "server_inflight", signed_decimal(sample->rpc.server_inflight));
        json_object_set_new(rpc, "client_inflight", signed_decimal(sample->rpc.client_inflight));
        json_object_set_new(rpc, "rx_msgs", decimal(sample->rpc.rx_msgs));
        json_object_set_new(rpc, "tx_msgs", decimal(sample->rpc.tx_msgs));
        json_object_set_new(rpc, "rx_bytes", decimal(sample->rpc.rx_bytes));
        json_object_set_new(rpc, "tx_bytes", decimal(sample->rpc.tx_bytes));
        json_object_set_new(rpc, "rpc_errors", decimal(sample->rpc.rpc_errors));
        json_object_set_new(rpc, "accepts", decimal(sample->rpc.accepts));
        json_object_set_new(rpc, "closes", decimal(sample->rpc.closes));
        for (unsigned int i = 0; i < sample->nprocedures; i++) {
            json_array_append_new(procedures, procedure_json(&sample->procedures[i]));
        }
        json_object_set_new(rpc, "procedures", procedures);
        json_object_set_new(obj, "rpc", rpc);
    }
    if (sample->nvfs) {
        json_t *vfs = json_object(), *operations = json_array();
        for (unsigned int i = 0; i < sample->nvfs; i++) {
            json_t *op = json_pack("{s:s}", "name", sample->vfs[i].name);
            json_object_set_new(op, "latency_samples_total", decimal(sample->vfs[i].count));
            json_array_append_new(operations, op);
        }
        json_object_set_new(vfs, "operations", operations);
        json_object_set_new(obj, "vfs", vfs);
    }
    return obj;
} /* sample_json */

/* JSON_COMPACT leaves UTF-8 intact and escapes ASCII controls, quotes and
* backslashes. Count their worst-case expansion before allocating JSON. */
static size_t
string_budget(const char *text)
{
    size_t bytes = 0;

    for (const unsigned char *p = (const unsigned char *) text; *p; p++) {
        bytes += *p < 32 || *p == '"' || *p == '\\' ? 6 : 1;
    }
    return bytes;
} /* string_budget */

static void
snapshot_complete(
    const struct chimera_inspect_result *result,
    void                                *data)
{
    struct rest_inspect_context *ctx = data;

    if (!ctx->request) {
        return;
    }
    if (!result->responded) {
        chimera_rest_send_error(ctx->thread->evpl, ctx->request, 503, "Service Unavailable",
                                "No inspection worker responded");
        return;
    }
    struct timespec now;
    struct tm       utc;
    char            sampled_at[40];
    clock_gettime(CLOCK_REALTIME, &now);
    gmtime_r(&now.tv_sec, &utc);
    strftime(sampled_at, sizeof(sampled_at), "%Y-%m-%dT%H:%M:%SZ", &utc);
    /* Conservative escaped-JSON upper bound, checked before building objects.
    * The SDK caps reads at 8 MiB; leave room for the envelope below 6 MiB. */
    size_t          budget = 0;
    for (unsigned int i = 0; i < result->nworkers; i++) {
        if (!result->present[i]) {
            continue;
        }
        const struct chimera_inspect_sample *sample = &result->samples[i];
        budget += 1024 + string_budget(sample->identity.name) + string_budget(sample->identity.executor);
        if (ctx->connections) {
            for (unsigned int j = 0; j < sample->nrows; j++) {
                const struct chimera_inspect_connection *row = &sample->rows[j];
                budget += 1024 + string_budget(sample->identity.name) +
                    string_budget(row->bind.local.address) + string_budget(row->bind.remote.address);
            }
        } else {
            for (unsigned int j = 0; j < sample->nprocedures; j++) {
                budget += 256 + string_budget(sample->procedures[j].name);
            }
            for (unsigned int j = 0; j < sample->nvfs; j++) {
                budget += 128 + string_budget(sample->vfs[j].name);
            }
        }
    }
    if (budget > 6 * 1024 * 1024) {
        chimera_rest_send_error(ctx->thread->evpl, ctx->request, 503, "Service Unavailable",
                                "Inspection response exceeds the output budget; select a worker");
        return;
    }
    json_t *obj = json_object(), *missing = json_array(), *samples = json_array(), *rows = json_array();
    int     truncated = 0;
    for (unsigned int i = 0; i < result->nworkers; i++) {
        const struct chimera_inspect_sample *sample = &result->samples[i];
        if (!result->present[i]) {
            json_array_append_new(missing, json_integer(sample->worker_id)); continue;
        }
        truncated |= sample->truncated;
        json_array_append_new(samples, sample_json(sample, ctx->connections, sampled_at));
        if (ctx->connections) {
            for (unsigned int j = 0; j < sample->nrows; j++) {
                json_array_append_new(rows, connection_json(sample, &sample->rows[j]));
            }
        }
    }
    json_object_set_new(obj, "server_instance_id", json_string(result->server_instance_id));
    json_object_set_new(obj, "complete", json_boolean(!truncated && result->responded == result->nworkers));
    json_object_set_new(obj, "truncated", json_boolean(truncated));
    json_object_set_new(obj, "missing_workers", missing);
    json_object_set_new(obj, ctx->connections ? "worker_samples" : "threads", samples);
    if (ctx->connections) {
        json_object_set_new(obj, "connections", rows);
    } else {
        json_decref(rows);
    }
    chimera_rest_send_json(ctx->thread->evpl, ctx->request, 200, obj);
} /* snapshot_complete */

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
            if (i + 2 >= len) {
                return -1;
            }
            int value = 0;
            for (int j = 0; j < 2; j++) {
                unsigned char h     = input[++i];
                int           digit = h >= '0' && h <= '9' ? h - '0' : h >= 'a' && h <= 'f' ? h - 'a' + 10 : h >= 'A' &&
                    h <= 'F' ? h - 'A' + 10 : -1;
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
static int
parse_filter(
    const char                    *query,
    int                            len,
    struct chimera_inspect_filter *filter)
{
    unsigned int seen = 0;

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
        if (!equal || decode(query, equal - query, key, sizeof(key)) ||
            decode(equal + 1, part - (equal - query) - 1, value, sizeof(value))) {
            return -1;
        }
        const char *keys[] = { "worker_id", "service", "remote_ip", "remote_port", "local_ip", "local_port",
                               "transport" };
        int         index;
        for (index = 0; index < 7 && strcmp(keys[index], key); index++) {
        }
        if (index == 7 || (seen & (1U << index)) || (!filter->connections && index != 0)) {
            return -1;
        }
        seen |= 1U << index;
        if (index == 0) {
            if (number(value, INT_MAX, &filter->worker_id)) {
                return -1;
            }
        } else if (index == 1) {
            int service;
            for (service = 0; service < CHIMERA_INSPECT_SERVICES && strcmp(value, chimera_inspect_service_name(service))
                 ; service++) {
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
chimera_rest_inspect(
    struct chimera_rest_thread  *thread,
    struct evpl_http_request    *request,
    int                          connections,
    const char                  *query,
    int                          query_len,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data)
{
    struct chimera_inspect_filter filter = { .connections = connections,
                                             .worker_id   = -1,
                                             .services    = (1U << CHIMERA_INSPECT_NFS) | (1U << CHIMERA_INSPECT_SMB) |
                                                 (1U << CHIMERA_INSPECT_S3),
                                             .remote_port = -1,
                                             .local_port  = -1 };

    if (parse_filter(query, query_len, &filter)) {
        chimera_rest_send_error(thread->evpl, request, 400, "Bad Request", "Invalid inspection filters");
        return;
    }
    struct rest_inspect_context  *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        chimera_rest_send_error(thread->evpl, request, 503, "Service Unavailable", "Inspection allocation failed");
        return;
    }
    ctx->thread      = thread;
    ctx->request     = request;
    ctx->connections = connections;
    int                           status = chimera_server_inspect_collect(chimera_server_get_inspect(thread->shared->
                                                                                                     server),
                                                                          thread->evpl, &filter, snapshot_complete, ctx,
                                                                          &ctx->collector);
    if (status) {
        free(ctx);
        if (status == 429) {
            evpl_http_request_add_header(request, "Retry-After", "1");
        }
        chimera_rest_send_error(thread->evpl, request, status,
                                status == 429 ? "Too Many Requests" : status == 400 ? "Bad Request" :
                                "Service Unavailable",
                                status == 429 ? "An inspection collector is still outstanding" :
                                "Inspection worker unavailable");
        return;
    }
    *notify_callback = inspect_notify;
    *notify_data     = ctx;
} /* chimera_rest_inspect */
