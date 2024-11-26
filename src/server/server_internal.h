#pragma once

#include "common/logging.h"

#define chimera_server_debug(...) chimera_debug("server", __VA_ARGS__)
#define chimera_server_info(...)  chimera_info("server", __VA_ARGS__)
#define chimera_server_error(...) chimera_error("server", __VA_ARGS__)
#define chimera_server_fatal(...) chimera_fatal("server", __VA_ARGS__)
#define chimera_server_abort(...) chimera_abort("core", __VA_ARGS__)

#define chimera_server_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "server", __VA_ARGS__)