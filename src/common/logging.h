#pragma once

#include <stdarg.h>

extern int ChimeraLogLevel;

#define CHIMERA_LOG_FATAL 1
#define CHIMERA_LOG_ERROR 2
#define CHIMERA_LOG_INFO  3
#define CHIMERA_LOG_DEBUG 4

void chimera_log_init(
    void);

void chimera_enable_crash_handler(
    void);

void
chimera_vlog(
    const char *level,
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    va_list     argp);

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
void __chimera_fatal(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);
void __chimera_abort(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

#define chimera_debug(mod, file, line, ...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { \
                __chimera_debug(mod, file, line, __VA_ARGS__); \
            } \
}

#define chimera_info(mod, file, line, ...)  { \
            if (ChimeraLogLevel >= CHIMERA_LOG_INFO) { \
                __chimera_info(mod, file, line, __VA_ARGS__); \
            } \
}

#define chimera_error(mod, file, line, ...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_ERROR) { \
                __chimera_error(mod, file, line, __VA_ARGS__); \
            } \
}

#define chimera_fatal(mod, file, line, ...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_FATAL) { \
                __chimera_fatal(mod, file, line, __VA_ARGS__); \
            } \
}

#define chimera_abort(mod, file, line, ...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_FATAL) { \
                __chimera_abort(mod, file, line, __VA_ARGS__); \
            } \
}

#define chimera_fatal_if(cond, mod, file, line, ...) \
        if (cond)                                    \
        {                                           \
            chimera_fatal(mod, file, line, __VA_ARGS__); \
        }

#define chimera_abort_if(cond, mod, file, line, ...) \
        if (cond)                                  \
        {                                         \
            chimera_abort(mod, file, line, __VA_ARGS__); \
        }