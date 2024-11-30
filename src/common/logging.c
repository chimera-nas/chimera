#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include "logging.h"

static const char *level_string[] = {
    "none",
    "debug",
    "info",
    "error",
    "fatal"
};

#define CHIMERA_LOG_BUF_SIZE 1024 * 1024
char               ChimeraLogBuf[CHIMERA_LOG_BUF_SIZE];
char              *ChimeraLogBufPtr  = ChimeraLogBuf;
int                ChimeraLogRun     = 1;
pthread_mutex_t    ChimeraLogBufLock = PTHREAD_MUTEX_INITIALIZER;
pthread_t          ChimeraLogThread;
pthread_once_t     ChimeraLogOnce = PTHREAD_ONCE_INIT;

static void *
chimera_log_thread(void *arg)
{
    char tmp[CHIMERA_LOG_BUF_SIZE];

    while (ChimeraLogRun || ChimeraLogBufPtr > ChimeraLogBuf) {

        if (ChimeraLogBufPtr > ChimeraLogBuf) {

            pthread_mutex_lock(&ChimeraLogBufLock);
            memcpy(tmp, ChimeraLogBuf, ChimeraLogBufPtr - ChimeraLogBuf);
            ChimeraLogBufPtr = ChimeraLogBuf;
            pthread_mutex_unlock(&ChimeraLogBufLock);

            fprintf(stderr, "%s", tmp);
        }
        usleep(1000);
    }

    return NULL;
} /* chimera_log_thread */

static void
chimera_log_thread_exit(void)
{
    ChimeraLogRun = 0;
    pthread_join(ChimeraLogThread, NULL);
} /* chimera_log_thread_exit */

static void
chimera_log_thread_init(void)
{
    pthread_create(&ChimeraLogThread, NULL, chimera_log_thread, NULL);
    atexit(chimera_log_thread_exit);
} /* chimera_log_thread_init */

void
chimera_vlog(
    int         level,
    const char *mod,
    const char *file,
    int         line,
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
                   "\" process=%lu thread=%lu level=%s module=%s source=\"%s:%d\"\n",
                   pid, tid, level_string[level], mod, file, line);

    pthread_once(&ChimeraLogOnce, chimera_log_thread_init);
    pthread_mutex_lock(&ChimeraLogBufLock);
    memcpy(ChimeraLogBufPtr, buf, bp - buf);
    ChimeraLogBufPtr += (bp - buf);
    pthread_mutex_unlock(&ChimeraLogBufLock);
} /* chimera_vlog */




void
chimera_debug(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_DEBUG, mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_debug */

void
chimera_info(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_INFO, mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_info */

void
chimera_error(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_ERROR, mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_error */

void
chimera_fatal(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_FATAL, mod, file, line, fmt, argp);
    va_end(argp);

    exit(1);
} /* chimera_fatal */

void
chimera_abort(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(CHIMERA_LOG_FATAL, mod, file, line, fmt, argp);
    va_end(argp);

    abort();
} /* chimera_abort */
