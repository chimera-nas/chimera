#pragma once

#include "core/evpl.h"

struct chimera_server_config;
struct chimera_vfs;

struct chimera_server_protocol {
    void *(*init)(
        const struct chimera_server_config *config,
        struct chimera_vfs                 *vfs);
    void  (*destroy)(
        void *data);
    void *(*thread_init)(
        struct evpl *,
        void *data);
    void  (*thread_destroy)(
        void *data);
};