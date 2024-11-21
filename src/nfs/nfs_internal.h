#pragma once

#include "common/logging.h"

#define chimera_nfs_debug(...) chimera_debug("nfs", __VA_ARGS__)
#define chimera_nfs_info(...)  chimera_info("nfs", __VA_ARGS__)
#define chimera_nfs_error(...) chimera_error("nfs", __VA_ARGS__)
#define chimera_nfs_fatal(...) chimera_fatal("nfs", __VA_ARGS__)
#define chimera_nfs_abort(...) chimera_abort("core", __VA_ARGS__)

#define chimera_nfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "nfs", __VA_ARGS__)
