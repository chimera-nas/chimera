#pragma once

#include "common/logging.h"

#define chimera_client_debug(...) chimera_debug("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_info(...)  chimera_info("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_error(...) chimera_error("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_fatal(...) chimera_fatal("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_abort(...) chimera_abort("client", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_client_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "client", __FILE__, __LINE__, __VA_ARGS__)
