// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdint.h>
#include <evpl/evpl.h>
#include "common/macros.h"

/*
 * Read-only inspection of the connections a running server holds.
 *
 * A connection belongs to one worker thread and its state is plain data that
 * only that thread may read, so a collection asks every worker to copy its
 * own connections into a bounded buffer and gathers the copies.  Nothing is
 * added to a request path: the work happens only while someone asks.
 */

#define CHIMERA_INSPECT_ROWS 1024 /* connection rows in one collection */
#define CHIMERA_INSPECT_SCAN 4096 /* connections one worker examines   */

enum chimera_inspect_service {
    CHIMERA_INSPECT_NFS,
    CHIMERA_INSPECT_MOUNT,
    CHIMERA_INSPECT_NLM,
    CHIMERA_INSPECT_NSM,
    CHIMERA_INSPECT_RPCBIND,
    CHIMERA_INSPECT_SERVICES
};

/* Values only, never protocol or transport pointers. */
struct chimera_inspect_connection {
    struct evpl_bind_info        bind;
    enum chimera_inspect_service service;
    uint64_t                     rx_msgs, tx_msgs, rx_bytes, tx_bytes;
    uint64_t                     requests_done, inflight;
    uint64_t                     minor_versions; /* bit n: NFSv4.n seen */
    uint32_t                     programs[4], versions[4];
    int                          nprograms;
    int                          rdma_framing;
};

struct chimera_inspect_filter {
    int          worker_id; /* -1 means all */
    unsigned int services;  /* bit per enum chimera_inspect_service */
    int          remote_port, local_port; /* -1 means all */
    char         remote_ip[64], local_ip[64], transport[32];
};

struct chimera_inspect_sample {
    int                                worker_id;
    struct evpl_thread_identity        identity;
    struct chimera_inspect_connection *rows;
    unsigned int                       quota, nrows, scanned;
    int                                truncated;
};

/* What a protocol's inspect hook writes into. */
struct chimera_inspect_writer {
    const struct chimera_inspect_filter *filter;
    struct chimera_inspect_sample       *sample;

    /* Adds a connection to the sample if it passes the filter.  Returns 0 when
     * the sample is full and the walk should stop.  A pointer, not an exported
     * function, because the protocol libraries sit below the server core and
     * cannot call up into it. */
    int                                  (*emit)(
        struct chimera_inspect_writer           *writer,
        const struct chimera_inspect_connection *row);
};

struct chimera_server;
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

typedef void (*chimera_inspect_callback)(
    const struct chimera_inspect_result *,
    void *);

struct chimera_server_inspect * chimera_server_inspect_create(
    unsigned int workers);
void chimera_server_inspect_stop(
    struct chimera_server_inspect *inspect);
void chimera_server_inspect_destroy(
    struct chimera_server_inspect *inspect);

/* Called on a worker thread once its protocols are up, and again to remove it. */
struct chimera_inspect_endpoint * chimera_server_inspect_register(
    struct chimera_server_inspect   *inspect,
    struct evpl                     *evpl,
    int                              nprotocols,
    struct chimera_server_protocol **protocols,
    void                           **contexts);
void chimera_server_inspect_unregister(
    struct chimera_inspect_endpoint *endpoint);

SYMBOL_EXPORT struct chimera_server_inspect * chimera_server_get_inspect(
    struct chimera_server *server);

/*
 * Start, cancel and release are called only on the requesting worker.  The
 * callback is asynchronous, including when the requesting worker is the only
 * one collected.  Returns 0 and a collector, or an HTTP status: 400 for a
 * worker id that does not exist, 429 while another collection is outstanding,
 * 503 when the server is shutting down or no worker can be reached.
 */
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
    const char          **transport);
