// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "rest_internal.h"

/*
 * Cluster control plane (WS-D eviction signal).
 *
 *   GET    /api/v1/cluster              -> { local_node_id, evicted: [..] }
 *   POST   /api/v1/cluster/evict/{node} -> mark node evicted (shared-KV write)
 *   DELETE /api/v1/cluster/evict/{node} -> clear an eviction (node rejoined)
 *
 * This is the minimal admin/orchestrator hook; chimera does not detect node
 * death itself.  Marking a node evicted authorizes survivors to reclaim its
 * durable handles and (later) release its leases and open the grace window.
 */

void
chimera_rest_handle_cluster_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct chimera_server *server = thread->shared->server;
    json_t                *root;
    json_t                *evicted;
    uint8_t                list[256];
    int                    n, i;

    root = json_object();
    json_object_set_new(root, "local_node_id",
                        json_integer(chimera_server_cluster_local_node_id(server)));

    evicted = json_array();
    n       = chimera_server_cluster_evicted_list(server, list, sizeof(list));
    for (i = 0; i < n; i++) {
        json_array_append_new(evicted, json_integer(list[i]));
    }
    json_object_set_new(root, "evicted", evicted);

    chimera_rest_send_json(evpl, request, 200, root);
} /* chimera_rest_handle_cluster_list */

void
chimera_rest_handle_cluster_evict(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *node_str)
{
    struct chimera_server *server  = thread->shared->server;
    int                    node_id = atoi(node_str);
    json_t                *root;

    if (chimera_server_cluster_evict(server, thread->vfs_thread, node_id) != 0) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "node_id must be 1..255 and not the local node");
        return;
    }

    root = json_object();
    json_object_set_new(root, "node_id", json_integer(node_id));
    json_object_set_new(root, "evicted", json_true());
    chimera_rest_send_json(evpl, request, 200, root);
} /* chimera_rest_handle_cluster_evict */

void
chimera_rest_handle_cluster_rejoin(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *node_str)
{
    struct chimera_server *server  = thread->shared->server;
    int                    node_id = atoi(node_str);
    json_t                *root;

    if (chimera_server_cluster_rejoin(server, thread->vfs_thread, node_id) != 0) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "node_id must be 1..255");
        return;
    }

    root = json_object();
    json_object_set_new(root, "node_id", json_integer(node_id));
    json_object_set_new(root, "evicted", json_false());
    chimera_rest_send_json(evpl, request, 200, root);
} /* chimera_rest_handle_cluster_rejoin */
