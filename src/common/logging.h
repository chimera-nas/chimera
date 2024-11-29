#pragma once

#define CHIMERA_LOG_NONE  0
#define CHIMERA_LOG_DEBUG 1
#define CHIMERA_LOG_INFO  2
#define CHIMERA_LOG_ERROR 3
#define CHIMERA_LOG_FATAL 4

void chimera_debug(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);
void chimera_info(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);
void chimera_error(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);
void chimera_fatal(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);
void chimera_abort(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...);

#define chimera_fatal_if(cond, ...) \
        if (cond)                    \
        {                            \
            chimera_fatal(__VA_ARGS__); \
        }

#define chimera_abort_if(cond, ...) \
        if (cond)                    \
        {                            \
            chimera_abort(__VA_ARGS__); \
        }