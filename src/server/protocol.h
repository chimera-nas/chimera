// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "evpl/evpl.h"

struct chimera_server_config;
struct chimera_vfs;
struct chimera_vfs_thread;
struct prometheus_metrics;
struct chimera_server_protocol {

    void *(*init)(
        const struct chimera_server_config *config,
        struct chimera_vfs                 *vfs,
        struct prometheus_metrics          *metrics);

    void  (*destroy)(
        void *data);

    void  (*start)(
        void *data);

    void *(*thread_init)(
        struct evpl *,
        struct chimera_vfs_thread *vfs_thread,
        void                      *data);
    void  (*thread_destroy)(
        void *data);
};