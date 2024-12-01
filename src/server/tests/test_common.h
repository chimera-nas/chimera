#pragma once

#include "common/logging.h"

static inline struct chimera_server *
test_server_init(
    char **argv,
    int    argc)
{
    struct chimera_server *server;

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    server = chimera_server_init(NULL);

    return server;
} /* test_server_init */