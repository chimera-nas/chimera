// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Logging entry points for VFS modules.
 *
 * Deliberately self-contained: the prototypes below mirror the exported
 * logging functions from common/logging.h so that a module builds against
 * the SDK headers alone.  The chimera_vfs_* macros are the module-facing
 * vocabulary; the level gate is the only logic they carry.
 */

extern int ChimeraLogLevel;

#define CHIMERA_LOG_FATAL 1
#define CHIMERA_LOG_ERROR 2
#define CHIMERA_LOG_INFO  3
#define CHIMERA_LOG_DEBUG 4

void __chimera_debug(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

void __chimera_info(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

void __chimera_error(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

__attribute__((noreturn)) void __chimera_fatal(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

__attribute__((noreturn)) void __chimera_abort(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

#define chimera_vfs_debug(...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { \
                __chimera_debug("vfs", __FILE__, __LINE__, __VA_ARGS__); \
            } \
}

#define chimera_vfs_info(...)  { \
            if (ChimeraLogLevel >= CHIMERA_LOG_INFO) { \
                __chimera_info("vfs", __FILE__, __LINE__, __VA_ARGS__); \
            } \
}

#define chimera_vfs_error(...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_ERROR) { \
                __chimera_error("vfs", __FILE__, __LINE__, __VA_ARGS__); \
            } \
}

#define chimera_vfs_fatal(...) { \
            __chimera_fatal("vfs", __FILE__, __LINE__, __VA_ARGS__); \
}

#define chimera_vfs_abort(...) { \
            __chimera_abort("vfs", __FILE__, __LINE__, __VA_ARGS__); \
}

#define chimera_vfs_fatal_if(cond, ...) \
        if (cond)                       \
        {                               \
            chimera_vfs_fatal(__VA_ARGS__); \
        }

#define chimera_vfs_abort_if(cond, ...) \
        if (cond)                       \
        {                               \
            chimera_vfs_abort(__VA_ARGS__); \
        }
