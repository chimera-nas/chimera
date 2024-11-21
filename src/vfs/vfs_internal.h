#pragma once

#include "common/logging.h"

#define chimera_vfs_debug(...) chimera_debug("vfs", __VA_ARGS__)
#define chimera_vfs_info(...)  chimera_info("vfs", __VA_ARGS__)
#define chimera_vfs_error(...) chimera_error("vfs", __VA_ARGS__)
#define chimera_vfs_fatal(...) chimera_fatal("vfs", __VA_ARGS__)
#define chimera_vfs_abort(...) chimera_abort("core", __VA_ARGS__)

#define chimera_vfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "vfs", __VA_ARGS__)
