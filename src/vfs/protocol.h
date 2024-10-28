#pragma once

#include "core/evpl.h"

struct chimera_server_protocol
{
    void *(*init)(void);
    void (*destroy)(void *data);
    void *(*thread_init)(struct evpl *, void *data);
    void (*thread_destroy)(struct evpl *, void *data);
};