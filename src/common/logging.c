#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include "logging.h"

static const char *level_string[] = {
    "none",
    "debug",
    "info",
    "error",
    "fatal"
};


void
chimera_vlog(
    int         level,
    const char *mod,
    const char *fmt,
    va_list     argp)
{
    struct timespec ts;
    struct tm       tm_info;
    char            buf[256], *bp = buf;
    uint64_t        pid, tid;

    clock_gettime(CLOCK_REALTIME, &ts);

    gmtime_r(&ts.tv_sec, &tm_info);

    pid = getpid();

    tid = gettid();

    bp += snprintf(bp, sizeof(buf),
                   "time=%04d-%02d-%02dT%02d:%02d:%02d.%09ldZ message=\"",
                   tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday,
                   tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, ts.tv_nsec);

    bp += vsnprintf(bp, (buf + sizeof(buf)) - bp, fmt, argp);
    bp += snprintf(bp, (buf + sizeof(buf)) - bp,
                   "\" process=%lu thread=%lu level=%s module=%s\n",
                   pid, tid, level_string[level], mod);
    fprintf(stderr, "%s", buf);
} /* chimera_vlog */


void
chimera_debug(
    const char *mod,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_DEBUG, mod, fmt, argp);
    va_end(argp);
} /* chimera_debug */

void
chimera_info(
    const char *mod,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_INFO, mod, fmt, argp);
    va_end(argp);
} /* chimera_info */

void
chimera_error(
    const char *mod,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_ERROR, mod, fmt, argp);
    va_end(argp);
} /* chimera_error */

void
chimera_fatal(
    const char *mod,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_FATAL, mod, fmt, argp);
    va_end(argp);

    exit(1);
} /* chimera_fatal */

void
chimera_abort(
    const char *mod,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_FATAL, mod, fmt, argp);
    va_end(argp);

    abort();
} /* chimera_abort */
