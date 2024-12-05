#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
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

char              *ChimeraLogBuffers[2];
int                ChimeraLogIndex   = 0;
char              *ChimeraLogBuf     = NULL;
char              *ChimeraLogBufPtr  = NULL;
int                ChimeraLogRun     = 1;
int                ChimeraLogLevel   = CHIMERA_LOG_INFO;
pthread_mutex_t    ChimeraLogBufLock = PTHREAD_MUTEX_INITIALIZER;
pthread_t          ChimeraLogThread;
pthread_once_t     ChimeraLogOnce = PTHREAD_ONCE_INIT;

static void *
chimera_log_thread(void *arg)
{
    int   i;
    char *tmp;

    while (ChimeraLogRun || ChimeraLogBufPtr > ChimeraLogBuf) {

        if (ChimeraLogBufPtr > ChimeraLogBuf) {

            pthread_mutex_lock(&ChimeraLogBufLock);
            tmp              = ChimeraLogBuf;
            ChimeraLogIndex  = !ChimeraLogIndex;
            ChimeraLogBuf    = ChimeraLogBuffers[ChimeraLogIndex];
            ChimeraLogBufPtr = ChimeraLogBuf;
            pthread_mutex_unlock(&ChimeraLogBufLock);

            fprintf(stdout, "%s", tmp);
            fflush(stdout);
        }
        usleep(1000);
    }

    for (i = 0; i < 2; ++i) {
        free(ChimeraLogBuffers[i]);
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
chimera_log_flush(int signum)
{
    ChimeraLogRun = 0;
    pthread_join(ChimeraLogThread, NULL);
} /* chimera_log_flush */

static void
chimera_log_thread_init(void)
{
    int i;

    for (i = 0; i < 2; ++i) {
        ChimeraLogBuffers[i] = calloc(CHIMERA_LOG_BUF_SIZE, sizeof(char));
    }

    ChimeraLogBuf    = ChimeraLogBuffers[ChimeraLogIndex];
    ChimeraLogBufPtr = ChimeraLogBuf;

    struct sigaction sa;
    sa.sa_handler = chimera_log_flush;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGABRT, &sa, NULL);

    pthread_create(&ChimeraLogThread, NULL, chimera_log_thread, NULL);
    atexit(chimera_log_thread_exit);
} /* chimera_log_thread_init */

void
chimera_log_init(void)
{
    pthread_once(&ChimeraLogOnce, chimera_log_thread_init);
} /* chimera_log_init */

static inline void
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
    uint64_t        pid, tid;

    clock_gettime(CLOCK_REALTIME, &ts);

    gmtime_r(&ts.tv_sec, &tm_info);

    pid = getpid();
    tid = gettid();

    pthread_mutex_lock(&ChimeraLogBufLock);

    while ((ChimeraLogBufPtr + 4096) > (ChimeraLogBuf + CHIMERA_LOG_BUF_SIZE)) {
        pthread_mutex_unlock(&ChimeraLogBufLock);
        usleep(1);
        pthread_mutex_lock(&ChimeraLogBufLock);
    }

    ChimeraLogBufPtr += snprintf(ChimeraLogBufPtr,
                                 1024,
                                 "time=%04d-%02d-%02dT%02d:%02d:%02d.%09ldZ message=\"",
                                 tm_info.tm_year + 1900, tm_info.tm_mon + 1,
                                 tm_info.tm_mday, tm_info.tm_hour,
                                 tm_info.tm_min, tm_info.tm_sec, ts.tv_nsec);

    ChimeraLogBufPtr += vsnprintf(ChimeraLogBufPtr,
                                  1024,
                                  fmt, argp);

    ChimeraLogBufPtr += snprintf(ChimeraLogBufPtr,
                                 1024,
                                 "\" process=%lu thread=%lu level=%s module=%s source=\"%s:%d\"\n",
                                 pid, tid, level_string[level], mod, file,
                                 line);

    pthread_mutex_unlock(&ChimeraLogBufLock);
} /* chimera_vlog */




void
__chimera_debug(
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
__chimera_info(
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
__chimera_error(
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
__chimera_fatal(
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
__chimera_abort(
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
