// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>
#include <evpl/evpl.h>
#include <evpl/evpl_rpc2.h>
#include "common/macros.h"
#include "protocol.h"
#include "vfs/vfs.h"

#define CHIMERA_INSPECT_ROWS       1024
#define CHIMERA_INSPECT_SCAN       4096
#define CHIMERA_INSPECT_PROCEDURES 128
#define CHIMERA_INSPECT_VFS_OPS    64

enum chimera_inspect_service {
    CHIMERA_INSPECT_NFS, CHIMERA_INSPECT_MOUNT, CHIMERA_INSPECT_NLM,
    CHIMERA_INSPECT_NSM, CHIMERA_INSPECT_RPCBIND, CHIMERA_INSPECT_SMB,
    CHIMERA_INSPECT_S3, CHIMERA_INSPECT_SERVICES
};

/* All published records contain values only, never protocol/transport pointers. */
struct chimera_inspect_connection {
    struct evpl_bind_info        bind;
    enum chimera_inspect_service service;
    uint64_t                     minor_versions;
    uint32_t                     programs[4], versions[4];
    int                          nprograms;
    uint16_t                     smb_dialect;
    uint32_t                     messages_received;
    uint64_t                     rx_msgs, tx_msgs, rx_bytes, tx_bytes;
    uint64_t                     requests_done, inflight;
    int                          rdma_framing;
    char                         http_framing[16];
};
struct chimera_inspect_filter {
    int          connections;
    int          worker_id; /* -1 means all */
    unsigned int services;
    int          remote_port, local_port; /* -1 means all */
    char         remote_ip[64], local_ip[64], transport[32];
};
struct chimera_inspect_sample {
    int                                worker_id;
    struct evpl_thread_identity        identity;
    uint64_t                           os_tid, sampled_monotonic_ns;
    char                               sampled_at[40];
    uint64_t                           connection_counts[CHIMERA_INSPECT_SERVICES];
    int                                has_rpc;
    struct evpl_rpc2_thread_stats      rpc;
    struct evpl_rpc2_procedure_stats   procedures[CHIMERA_INSPECT_PROCEDURES];
    unsigned int                       nprocedures;
    struct chimera_vfs_op_count        vfs[CHIMERA_INSPECT_VFS_OPS];
    unsigned int                       nvfs;
    struct chimera_inspect_connection *rows;
    unsigned int                       quota, nrows, scanned;
    int                                truncated;
};
struct chimera_inspect_writer {
    const struct chimera_inspect_filter *filter;
    struct chimera_inspect_sample       *sample;
    int                                  (*emit)(
        struct chimera_inspect_writer *,
        const struct chimera_inspect_connection *);
};
struct chimera_server;
struct chimera_vfs_thread;
struct chimera_server_protocol;
struct chimera_server_inspect;
struct chimera_inspect_endpoint;
struct chimera_inspect_collector;
struct chimera_inspect_result {
    char                                 server_instance_id[37];
    unsigned int                         nworkers, responded;
    const struct chimera_inspect_sample *samples;
    const unsigned char                 *present;
};
struct CHIMERA_ALIGNED(64) chimera_worker_stats {
    atomic_bool                      registered;
    uint64_t                         os_tid;
    pthread_t                        pthread;
    struct evpl                     *evpl;
    struct chimera_vfs_thread       *vfs;
    int                              nprotocols;
    struct chimera_server_protocol **protocols;
    void                           **contexts;
    struct evpl_thread_identity      identity;
    struct evpl_rpc2_worker_counters counters;
};
struct chimera_worker_sample {
    unsigned int                worker_id;
    uint64_t                    os_tid, cpu_ns, runq_wait_ns;
    struct evpl_thread_identity identity;
    struct evpl_loop_stats      loop;
};

#define CHIMERA_STAT_ADD(stats, field, n) do { \
            uint64_t chimera_stat_value = atomic_load_explicit(&(stats)->counters.field, memory_order_relaxed); \
            atomic_store_explicit(&(stats)->counters.field, chimera_stat_value + (uint64_t) (n), memory_order_relaxed); \
} while (0)
#define CHIMERA_STAT_READ(stats, field)   atomic_load_explicit(&(stats)->counters.field, memory_order_relaxed)
typedef void (*chimera_inspect_callback)(
    const struct chimera_inspect_result *,
    void *);

struct chimera_server_inspect * chimera_server_inspect_create(
    unsigned int workers);
void chimera_server_inspect_stop(
    struct chimera_server_inspect *inspect);
void chimera_server_inspect_destroy(
    struct chimera_server_inspect *inspect);
struct chimera_inspect_endpoint * chimera_server_inspect_register(
    struct chimera_server_inspect   *inspect,
    struct evpl                     *evpl,
    struct chimera_vfs_thread       *vfs,
    int                              nprotocols,
    struct chimera_server_protocol **protocols,
    void                           **contexts);
void chimera_server_inspect_unregister(
    struct chimera_inspect_endpoint *endpoint);
SYMBOL_EXPORT struct chimera_server_inspect * chimera_server_get_inspect(
    struct chimera_server *server);
SYMBOL_EXPORT unsigned int chimera_server_inspect_read_workers(
    struct chimera_server_inspect *inspect,
    struct chimera_worker_sample  *out,
    unsigned int                   capacity);
SYMBOL_EXPORT unsigned int chimera_server_inspect_read_vfs(
    struct chimera_server_inspect *inspect,
    unsigned int                   worker_id,
    unsigned int                   capacity,
    struct chimera_vfs_op_count   *out);
SYMBOL_EXPORT void chimera_server_inspect_read_live_rpc(
    struct chimera_server_inspect *inspect,
    unsigned int                   worker_id,
    struct chimera_inspect_sample *sample);
SYMBOL_EXPORT unsigned int chimera_server_inspect_read_sessions(
    struct chimera_server_inspect  *inspect,
    struct chimera_inspect_session *out,
    unsigned int                    capacity,
    unsigned int                   *total);
SYMBOL_EXPORT int chimera_server_inspect_read_session_slots(
    struct chimera_server_inspect *inspect,
    const char                    *session_id,
    struct chimera_inspect_slot   *out,
    unsigned int                   capacity,
    unsigned int                  *total);
SYMBOL_EXPORT const char * chimera_server_inspect_instance_id(
    struct chimera_server_inspect *inspect);
/* Start/cancel/release are called only on the requesting worker. Callback is
 * asynchronous, including when collecting from that same worker. */
SYMBOL_EXPORT int chimera_server_inspect_collect(
    struct chimera_server_inspect       *inspect,
    struct evpl                         *evpl,
    const struct chimera_inspect_filter *filter,
    chimera_inspect_callback             callback,
    void                                *data,
    struct chimera_inspect_collector   **collector);
SYMBOL_EXPORT void chimera_server_inspect_cancel(
    struct chimera_inspect_collector *collector);
SYMBOL_EXPORT void chimera_server_inspect_release(
    struct chimera_inspect_collector *collector);
SYMBOL_EXPORT const char * chimera_inspect_service_name(
    enum chimera_inspect_service service);
SYMBOL_EXPORT void chimera_inspect_transport(
    enum evpl_protocol_id protocol,
    const char          **transport,
    const char          **backend,
    const char          **raw);
