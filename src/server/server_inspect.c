// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <errno.h>
#include <inttypes.h>
#include <unistd.h>
#include "server_inspect.h"
#include "inspect_mpsc.h"
#include "protocol.h"
#include "vfs/vfs.h"
#include "common/platform.h"
#include "common/uuid.h"

struct chimera_inspect_task {
    struct chimera_inspect_mpsc_node  node;
    struct chimera_inspect_collector *collector;
    struct chimera_inspect_endpoint  *target;
    unsigned int                      index;
    int                               result;
};
struct chimera_inspect_endpoint {
    atomic_uint                       refs;
    struct chimera_server_inspect    *inspect;
    struct evpl                      *evpl;
    struct chimera_worker_stats      *stats;
    struct evpl_doorbell              doorbell;
    struct evpl_doorbell_sender      *sender;
    int                               worker_id;
    struct chimera_inspect_mpsc_queue queue;
    struct chimera_inspect_collector *active;
    struct chimera_vfs_thread        *vfs;
    int                               nprotocols;
    struct chimera_server_protocol  **protocols;
    void                            **contexts;
};

struct chimera_server_inspect {
    evpl_mutex_t                      lock;
    int                               closing, busy;
    unsigned int                      nworkers;
    char                              instance[37];
    struct chimera_inspect_endpoint **workers;
    struct chimera_worker_stats      *worker_stats;
};
struct chimera_inspect_collector {
    atomic_uint                        refs;
    struct chimera_server_inspect     *inspect;
    struct chimera_inspect_endpoint   *origin;
    struct chimera_inspect_filter      filter;
    struct evpl_timer                  deadline;
    chimera_inspect_callback           callback;
    void                              *data;
    int                                finished;
    unsigned int                       pending;
    struct chimera_inspect_result      result;
    struct chimera_inspect_sample     *samples;
    unsigned char                     *present;
    struct chimera_inspect_task       *tasks;
    struct chimera_inspect_connection *rows;
};

static void
read_schedstat(
    uint64_t  tid,
    uint64_t *cpu_ns,
    uint64_t *runq_wait_ns)
{
    char  path[64];
    char  buffer[128];

    snprintf(path, sizeof(path), "/proc/self/task/%" PRIu64 "/schedstat", tid);
    FILE *file = fopen(path, "r");
    if (!file) {
        return;
    }
    if (fgets(buffer, sizeof(buffer), file)) {
        (void) sscanf(buffer, "%" SCNu64 " %" SCNu64, cpu_ns, runq_wait_ns);
    }
    fclose(file);
} /* read_schedstat */

SYMBOL_EXPORT unsigned int
chimera_server_inspect_read_workers(
    struct chimera_server_inspect *inspect,
    struct chimera_worker_sample  *out,
    unsigned int                   capacity)
{
    unsigned int count = 0;

    for (unsigned int i = 0; i < inspect->nworkers && count < capacity; i++) {
        struct chimera_worker_stats  *stats  = &inspect->worker_stats[i];
        struct chimera_worker_sample *sample = &out[count++];
        memset(sample, 0, sizeof(*sample));
        sample->worker_id = i;
        sample->os_tid    = stats->os_tid;
        sample->identity  = stats->identity;
        read_schedstat(sample->os_tid, &sample->cpu_ns, &sample->runq_wait_ns);
        evpl_get_loop_stats(stats->evpl, &sample->loop);
    }
    return count;
} /* chimera_server_inspect_read_workers */

SYMBOL_EXPORT unsigned int
chimera_server_inspect_read_vfs(
    struct chimera_server_inspect *inspect,
    unsigned int                   worker_id,
    unsigned int                   capacity,
    struct chimera_vfs_op_count   *out)
{
    if (worker_id >= inspect->nworkers) {
        return 0;
    }
    return chimera_vfs_inspect_counts(inspect->worker_stats[worker_id].vfs, capacity, out);
} /* chimera_server_inspect_read_vfs */

SYMBOL_EXPORT void
chimera_server_inspect_read_live_rpc(
    struct chimera_server_inspect *inspect,
    unsigned int                   worker_id,
    struct chimera_inspect_sample *sample)
{
    if (worker_id >= inspect->nworkers) {
        return;
    }
    struct chimera_worker_stats *stats = &inspect->worker_stats[worker_id];
    for (int i = 0; i < stats->nprotocols; i++) {
        if (stats->contexts[i] && stats->protocols[i]->inspect_live) {
            stats->protocols[i]->inspect_live(stats->contexts[i], sample);
        }
    }
} /* chimera_server_inspect_read_live_rpc */

SYMBOL_EXPORT const char *
chimera_server_inspect_instance_id(struct chimera_server_inspect *inspect)
{
    return inspect->instance;
} /* chimera_server_inspect_instance_id */

SYMBOL_EXPORT const char *
chimera_inspect_service_name(enum chimera_inspect_service service)
{
    static const char *names[] = { "nfs", "mount", "nlm", "nsm", "rpcbind", "smb", "s3" };

    return service < CHIMERA_INSPECT_SERVICES ? names[service] : "unknown";
} /* chimera_inspect_service_name */

SYMBOL_EXPORT void
chimera_inspect_transport(
    enum evpl_protocol_id protocol,
    const char          **transport,
    const char          **backend,
    const char          **raw)
{
    static const struct {
        const char *transport;
        const char *backend;
        const char *raw;
    } names[] = {
        {
            .transport = "udp",
            .backend   = "socket_udp",
            .raw       = "EVPL_DATAGRAM_SOCKET_UDP"
        },
        {
            .transport = "rdma",
            .backend   = "rdmacm_rc",
            .raw       = "EVPL_DATAGRAM_RDMACM_RC"
        },
        {
            .transport = "rdma",
            .backend   = "rdmacm_ud",
            .raw       = "EVPL_DATAGRAM_RDMACM_UD"
        },
        {
            .transport = "tcp",
            .backend   = "socket_tcp",
            .raw       = "EVPL_STREAM_SOCKET_TCP"
        },
        {
            .transport = "tcp",
            .backend   = "xlio_tcp",
            .raw       = "EVPL_STREAM_XLIO_TCP"
        },
        {
            .transport = "tcp",
            .backend   = "io_uring_tcp",
            .raw       = "EVPL_STREAM_IO_URING_TCP"
        },
        {
            .transport = "rdma",
            .backend   = "rdmacm_rc",
            .raw       = "EVPL_STREAM_RDMACM_RC"
        },
        {
            .transport = "tcp",
            .backend   = "socket_tls",
            .raw       = "EVPL_STREAM_SOCKET_TLS"
        },
        {
            .transport = "tcp",
            .backend   = "tcp_rdma",
            .raw       = "EVPL_DATAGRAM_TCP_RDMA"
        },
        {
            .transport = "unix",
            .backend   = "socket_unix",
            .raw       = "EVPL_STREAM_SOCKET_UNIX"
        },
        {
            .transport = "inproc",
            .backend   = "inproc",
            .raw       = "EVPL_STREAM_INPROC"
        },
        {
            .transport = "inproc",
            .backend   = "inproc",
            .raw       = "EVPL_DATAGRAM_INPROC"
        },
        {
            .transport = "fabric",
            .backend   = "libfabric_msg",
            .raw       = "EVPL_STREAM_LIBFABRIC_MSG"
        },
        {
            .transport = "fabric",
            .backend   = "libfabric_msg",
            .raw       = "EVPL_DATAGRAM_LIBFABRIC_MSG"
        },
        {
            .transport = "fabric",
            .backend   = "libfabric_rdm",
            .raw       = "EVPL_DATAGRAM_LIBFABRIC_RDM"
        },
        {
            .transport = "tcp",
            .backend   = "spdk_tcp",
            .raw       = "EVPL_STREAM_SPDK_TCP"
        },
        {
            .transport = "tcp",
            .backend   = "spdk_tcp_uring",
            .raw       = "EVPL_STREAM_SPDK_TCP_URING"
        },
    };
    *transport = *backend = *raw = "unknown";
    if ((unsigned int) protocol < sizeof(names) / sizeof(names[0])) {
        *transport = names[protocol].transport;
        *backend   = names[protocol].backend;
        *raw       = names[protocol].raw;
    }
} /* chimera_inspect_transport */

static void
endpoint_release(struct chimera_inspect_endpoint *endpoint)
{
    if (atomic_fetch_sub(&endpoint->refs, 1) == 1) {
        evpl_doorbell_sender_release(endpoint->sender);
        free(endpoint);
    }
} /* endpoint_release */

SYMBOL_EXPORT void
chimera_server_inspect_release(struct chimera_inspect_collector *collector)
{
    if (atomic_fetch_sub(&collector->refs, 1) != 1) {
        return;
    }
    struct chimera_server_inspect *inspect = collector->inspect;
    endpoint_release(collector->origin);
    free(collector->samples);
    free(collector->present);
    free(collector->tasks);
    free(collector->rows);
    free(collector);
    /* Held through the final task, including tasks stranded after cancellation. */
    evpl_mutex_lock(&inspect->lock);
    inspect->busy = 0;
    evpl_mutex_unlock(&inspect->lock);
} /* chimera_server_inspect_release */

static void
task_release(struct chimera_inspect_task *task)
{
    endpoint_release(task->target);
    chimera_server_inspect_release(task->collector);
} /* task_release */

/* A successful enqueue transfers ownership even if the wake is revoked before
 * signal. The receiver's shutdown drain owns every task already in its queue. */
static void
enqueue(
    struct chimera_inspect_endpoint *endpoint,
    struct chimera_inspect_task     *task)
{
    atomic_fetch_add_explicit(&endpoint->refs, 1, memory_order_relaxed);
    if (!chimera_inspect_mpsc_push(&endpoint->queue, &task->node)) {
        task_release(task);
        endpoint_release(endpoint);
        return;
    }
    evpl_doorbell_signal(endpoint->sender);
    endpoint_release(endpoint);
} /* enqueue */

static void
finish(struct chimera_inspect_collector *collector)
{
    if (collector->finished) {
        return;
    }
    collector->finished = 1;
    evpl_remove_timer(collector->origin->evpl, &collector->deadline);
    collector->origin->active = NULL;
    chimera_inspect_callback callback = collector->callback;
    collector->callback = NULL;
    if (callback) {
        callback(&collector->result, collector->data);
    }
} /* finish */

SYMBOL_EXPORT void
chimera_server_inspect_cancel(struct chimera_inspect_collector *collector)
{
    collector->callback = NULL;
    finish(collector);
} /* chimera_server_inspect_cancel */

static void
deadline(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_inspect_collector *collector =
        container_of(timer, struct chimera_inspect_collector, deadline);

    finish(collector);
} /* deadline */

static int
emit(
    struct chimera_inspect_writer           *writer,
    const struct chimera_inspect_connection *row)
{
    const struct chimera_inspect_filter *filter = writer->filter;
    struct chimera_inspect_sample       *sample = writer->sample;
    const char                          *transport, *backend, *raw;

    chimera_inspect_transport(row->bind.protocol, &transport, &backend, &raw);
    if (!(filter->services & (1U << row->service)) ||
        (filter->remote_port >= 0 && filter->remote_port != row->bind.remote.port) ||
        (filter->local_port >= 0 && filter->local_port != row->bind.local.port) ||
        (filter->remote_ip[0] && strcmp(filter->remote_ip, row->bind.remote.address)) ||
        (filter->local_ip[0] && strcmp(filter->local_ip, row->bind.local.address)) ||
        (filter->transport[0] && strcmp(filter->transport, transport))) {
        return 1;
    }
    if (sample->nrows == sample->quota) {
        sample->truncated = 1; return 0;
    }
    sample->rows[sample->nrows++] = *row;
    return 1;
} /* emit */

static void
collect_worker(
    struct chimera_inspect_endpoint *endpoint,
    struct chimera_inspect_task     *task)
{
    struct chimera_inspect_collector *collector = task->collector;
    struct chimera_inspect_sample    *sample    = &collector->samples[task->index];
    struct chimera_inspect_writer     writer    = { &collector->filter, sample, emit };
    struct timespec                   now;

    evpl_thread_get_identity(endpoint->evpl, &sample->identity);
    sample->os_tid = chimera_gettid();
    clock_gettime(CLOCK_MONOTONIC, &now);
    sample->sampled_monotonic_ns = (uint64_t) now.tv_sec * 1000000000 + now.tv_nsec;
    for (int i = 0; i < endpoint->nprotocols; i++) {
        if (endpoint->contexts[i] && endpoint->protocols[i]->inspect) {
            endpoint->protocols[i]->inspect(endpoint->contexts[i], &writer);
        }
    }
    if (!collector->filter.connections) {
        sample->nvfs = chimera_vfs_inspect_counts(endpoint->vfs, CHIMERA_INSPECT_VFS_OPS,
                                                  sample->vfs);
    }
    task->result = 1;
    enqueue(collector->origin, task);
} /* collect_worker */

static int
compare_connection(
    const void *left,
    const void *right)
{
    const struct chimera_inspect_connection *a = left, *b = right;

    return (a->bind.serial > b->bind.serial) - (a->bind.serial < b->bind.serial);
} /* compare_connection */

static void
inbox(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_inspect_endpoint  *endpoint = container_of(doorbell, struct chimera_inspect_endpoint, doorbell);
    struct chimera_inspect_mpsc_node *nodes    = chimera_inspect_mpsc_drain(&endpoint->queue);

    while (nodes) {
        struct chimera_inspect_task *task = (struct chimera_inspect_task *) nodes;
        nodes = nodes->next;
        if (!task->result) {
            collect_worker(endpoint, task);
        } else {
            struct chimera_inspect_collector *collector = task->collector;
            if (!collector->finished) {
                struct chimera_inspect_sample *sample = &collector->samples[task->index];
                qsort(sample->rows, sample->nrows, sizeof(*sample->rows), compare_connection);
                collector->present[task->index] = 1;
                collector->result.responded++;
                if (--collector->pending == 0) {
                    finish(collector);
                }
            }
            task_release(task);
        }
    }
} /* inbox */

struct chimera_server_inspect *
chimera_server_inspect_create(unsigned int workers)
{
    struct chimera_server_inspect *inspect = calloc(1, sizeof(*inspect));
    uint8_t                        uuid[16];

    if (!inspect || chimera_uuid_generate(uuid)) {
        abort();
    }
    chimera_uuid_unparse(uuid, inspect->instance);
    evpl_mutex_init(&inspect->lock, NULL);
    inspect->nworkers = workers;
    inspect->workers  = calloc(workers, sizeof(*inspect->workers));
    if (posix_memalign((void **) &inspect->worker_stats, 64,
                       workers * sizeof(*inspect->worker_stats)) || !inspect->workers) {
        abort();
    }
    memset(inspect->worker_stats, 0, workers * sizeof(*inspect->worker_stats));
    for (unsigned int i = 0; i < workers; i++) {
        struct chimera_worker_stats *stats = &inspect->worker_stats[i];
        atomic_init(&stats->counters.rx_msgs, 0);
        atomic_init(&stats->counters.tx_msgs, 0);
        atomic_init(&stats->counters.rx_bytes, 0);
        atomic_init(&stats->counters.tx_bytes, 0);
        atomic_init(&stats->counters.rpc_errors, 0);
        atomic_init(&stats->counters.accepts, 0);
        atomic_init(&stats->counters.closes, 0);
        atomic_init(&stats->counters.active_connections, 0);
    }
    return inspect;
} /* chimera_server_inspect_create */

struct chimera_inspect_endpoint *
chimera_server_inspect_register(
    struct chimera_server_inspect   *inspect,
    struct evpl                     *evpl,
    struct chimera_vfs_thread       *vfs,
    int                              nprotocols,
    struct chimera_server_protocol **protocols,
    void                           **contexts)
{
    struct chimera_inspect_endpoint *endpoint = calloc(1, sizeof(*endpoint));
    struct evpl_thread_identity      identity;

    if (!endpoint) {
        abort();
    }
    evpl_thread_get_identity(evpl, &identity);
    endpoint->worker_id  = identity.pool_index;
    endpoint->inspect    = inspect;
    endpoint->evpl       = evpl;
    endpoint->vfs        = vfs;
    endpoint->nprotocols = nprotocols;
    endpoint->protocols  = protocols;
    endpoint->contexts   = contexts;
    struct chimera_worker_stats *stats = &inspect->worker_stats[endpoint->worker_id];
    endpoint->stats   = stats;
    stats->os_tid     = chimera_gettid();
    stats->pthread    = pthread_self();
    stats->evpl       = evpl;
    stats->vfs        = vfs;
    stats->nprotocols = nprotocols;
    stats->protocols  = protocols;
    stats->contexts   = contexts;
    stats->identity   = identity;
    for (int i = 0; i < nprotocols; i++) {
        if (contexts[i] && protocols[i]->inspect_register) {
            protocols[i]->inspect_register(contexts[i], stats);
        }
    }
    atomic_init(&endpoint->refs, 1);
    chimera_inspect_mpsc_init(&endpoint->queue);
    evpl_add_doorbell(evpl, &endpoint->doorbell, inbox);
    endpoint->sender = evpl_doorbell_sender(&endpoint->doorbell);
    evpl_mutex_lock(&inspect->lock);
    inspect->workers[endpoint->worker_id] = endpoint;
    evpl_mutex_unlock(&inspect->lock);
    return endpoint;
} /* chimera_server_inspect_register */

void
chimera_server_inspect_stop(struct chimera_server_inspect *inspect)
{
    evpl_mutex_lock(&inspect->lock);
    inspect->closing = 1;
    evpl_mutex_unlock(&inspect->lock);
} /* chimera_server_inspect_stop */

void
chimera_server_inspect_unregister(struct chimera_inspect_endpoint *endpoint)
{
    struct chimera_server_inspect    *inspect = endpoint->inspect;

    evpl_mutex_lock(&inspect->lock);
    inspect->workers[endpoint->worker_id] = NULL;
    evpl_mutex_unlock(&inspect->lock);
    if (endpoint->active) {
        chimera_server_inspect_cancel(endpoint->active);
    }
    struct chimera_inspect_mpsc_node *nodes = chimera_inspect_mpsc_close(&endpoint->queue);
    evpl_remove_doorbell(endpoint->evpl, &endpoint->doorbell);
    while (nodes) {
        struct chimera_inspect_task *task = (struct chimera_inspect_task *) nodes;
        nodes = nodes->next;
        task_release(task);
    }
    endpoint_release(endpoint);
} /* chimera_server_inspect_unregister */

void
chimera_server_inspect_destroy(struct chimera_server_inspect *inspect)
{
    evpl_mutex_destroy(&inspect->lock);
    free(inspect->worker_stats);
    free(inspect->workers);
    free(inspect);
} /* chimera_server_inspect_destroy */

SYMBOL_EXPORT int
chimera_server_inspect_collect(
    struct chimera_server_inspect       *inspect,
    struct evpl                         *evpl,
    const struct chimera_inspect_filter *filter,
    chimera_inspect_callback             callback,
    void                                *data,
    struct chimera_inspect_collector   **out)
{
    struct chimera_inspect_collector *collector;
    struct chimera_inspect_endpoint  *origin   = NULL;
    unsigned int                      selected = 0;

    *out = NULL;
    evpl_mutex_lock(&inspect->lock);
    if (inspect->closing) {
        evpl_mutex_unlock(&inspect->lock); return 503;
    }
    if (filter->worker_id >= (int) inspect->nworkers) {
        evpl_mutex_unlock(&inspect->lock); return 400;
    }
    if (inspect->busy) {
        evpl_mutex_unlock(&inspect->lock); return 429;
    }
    for (unsigned int i = 0; i < inspect->nworkers; i++) {
        if (inspect->workers[i] && inspect->workers[i]->evpl == evpl) {
            origin = inspect->workers[i];
        }
        if (filter->worker_id < 0 || filter->worker_id == (int) i) {
            selected++;
        }
    }
    if (!origin || !selected) {
        evpl_mutex_unlock(&inspect->lock); return 503;
    }
    collector = calloc(1, sizeof(*collector));
    if (!collector) {
        evpl_mutex_unlock(&inspect->lock); return 503;
    }
    collector->samples = calloc(selected, sizeof(*collector->samples));
    collector->tasks   = calloc(selected, sizeof(*collector->tasks));
    collector->present = calloc(selected, 1);
    collector->rows    = calloc(CHIMERA_INSPECT_ROWS, sizeof(*collector->rows));
    if (!collector->samples || !collector->tasks || !collector->present || !collector->rows) {
        free(collector->samples); free(collector->tasks); free(collector->present); free(collector->rows); free(
            collector);
        evpl_mutex_unlock(&inspect->lock); return 503;
    }
    inspect->busy      = 1;
    collector->inspect = inspect;
    collector->origin  = origin;
    atomic_fetch_add(&origin->refs, 1);
    /* Initial HTTP ref plus one reference per possible task. Reserve them all
     * before publication so an immediate worker response cannot free storage. */
    atomic_init(&collector->refs, 1);
    collector->filter   = *filter;
    collector->callback = callback;
    collector->data     = data;
    snprintf(collector->result.server_instance_id, 37, "%s", inspect->instance);
    collector->result.nworkers = selected;
    collector->result.samples  = collector->samples;
    collector->result.present  = collector->present;
    unsigned int index = 0, offset = 0;
    for (unsigned int i = 0; i < inspect->nworkers; i++) {
        if (filter->worker_id >= 0 && filter->worker_id != (int) i) {
            continue;
        }
        struct chimera_inspect_sample *sample = &collector->samples[index];
        struct chimera_inspect_task   *task   = &collector->tasks[index];
        sample->worker_id = i;
        sample->quota     = CHIMERA_INSPECT_ROWS / selected + (index < CHIMERA_INSPECT_ROWS % selected);
        sample->rows      = collector->rows + offset;
        offset           += sample->quota;
        task->collector   = collector;
        task->target      = inspect->workers[i];
        task->index       = index++;
        if (task->target) {
            atomic_fetch_add(&task->target->refs, 1);
            atomic_fetch_add(&collector->refs, 1);
            collector->pending++;
        }
    }
    evpl_mutex_unlock(&inspect->lock);
    origin->active = collector;
    *out           = collector;
    evpl_add_oneshot_timer(evpl, &collector->deadline, deadline, 1000000);
    for (unsigned int i = 0; i < selected; i++) {
        if (collector->tasks[i].target) {
            enqueue(collector->tasks[i].target, &collector->tasks[i]);
        }
    }
    return 0;
} /* chimera_server_inspect_collect */
