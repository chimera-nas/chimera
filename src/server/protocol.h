// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include "evpl/evpl.h"

struct chimera_inspect_writer;
struct chimera_inspect_sample;
struct chimera_worker_stats;
struct chimera_inspect_session {
    char         session_id[33];
    uint64_t     client_id;
    uint64_t     created_monotonic_ns;
    uint64_t     created_wall_ns;
    uint32_t     requested_slots;
    uint32_t     negotiated_slots;
    uint32_t     server_max_slots;
    uint32_t     max_request_size;
    uint32_t     max_response_size;
    uint32_t     max_response_size_cached;
    uint32_t     highest_slot_in_use;
    uint32_t     slots_in_progress;
    uint32_t     slots_cached;
    uint32_t     slots_completed;
    uint32_t     slots_unused;
    unsigned int connection_count;
    int          connections_truncated;
    struct {
        int      worker_id;
        uint64_t serial;
    } connections[64];
    int          has_slots_in_use;
};
struct chimera_inspect_slot {
    uint32_t slot;
    uint32_t seqid;
    char     state[16];
    uint64_t age_ns;
    int      has_age;
};
struct chimera_server_config;
struct chimera_vfs;
struct chimera_vfs_thread;
struct prometheus_metrics;
struct chimera_server_protocol {
    void         (*inspect)(
        void                          *data,
        struct chimera_inspect_writer *writer);

    void        *(*init)(
        const struct chimera_server_config *config,
        struct chimera_vfs                 *vfs,
        struct prometheus_metrics          *metrics);

    void         (*destroy)(
        void *data);

    void         (*start)(
        void *data);

    void         (*stop)(
        void *data);

    void        *(*thread_init)(
        struct evpl *,
        struct chimera_vfs_thread *vfs_thread,
        void                      *data);
    void         (*thread_destroy)(
        void *data);

    void         (*inspect_live)(
        void                          *data,
        struct chimera_inspect_sample *sample);

    unsigned int (*inspect_sessions)(
        void                           *data,
        struct chimera_inspect_session *out,
        unsigned int                    capacity,
        unsigned int                   *total);

    int          (*inspect_session_slots)(
        void                        *data,
        const char                  *session_id,
        struct chimera_inspect_slot *out,
        unsigned int                 capacity,
        unsigned int                *total);

    void         (*inspect_register)(
        void                        *data,
        struct chimera_worker_stats *stats);
};
