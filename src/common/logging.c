// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <execinfo.h>

#include "evpl/evpl.h"

 #define UNW_LOCAL_ONLY
#include <libunwind.h>

#include "common/macros.h"
#include "common/logging.h"
#include "common/pthread_util.h"
#include "common/snprintf.h"

#define SECS_PER_HOUR (60 * 60)
#define SECS_PER_DAY  (SECS_PER_HOUR * 24)

static const unsigned short int __mon_yday[2][13] =
{
    /* Normal years.  */
    { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 },
    /* Leap years.  */
    { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 }
};

#define DIV(a, b)            ((a) / (b) - ((a) % (b) < 0))
#define LEAPS_THRU_END_OF(y) (DIV(y, 4) - DIV(y, 100) + DIV(y, 400))

static void
chimera_crash_handler(
    int signum);

static void
chimera_timet2tmZ(
    time_t     t,
    struct tm *tp)
{
    int64_t                   days, rem, y;
    const unsigned short int *ip;

    // Calculate the number of days and remaining seconds from the given time
    days = t / SECS_PER_DAY;
    rem  = t % SECS_PER_DAY;

    // Adjust for negative remainder to ensure rem is within a day
    while (rem < 0) {
        rem += SECS_PER_DAY;
        --days;
    }
    while (rem >= SECS_PER_DAY) {
        rem -= SECS_PER_DAY;
        ++days;
    }

    // Calculate hours, minutes, and seconds from the remainder
    tp->tm_hour = rem / SECS_PER_HOUR;
    rem        %= SECS_PER_HOUR;
    tp->tm_min  = rem / 60;
    tp->tm_sec  = rem % 60;

    // Calculate the day of the week, with January 1, 1970 as a Thursday
    tp->tm_wday = (4 + days) % 7;
    if (tp->tm_wday < 0) {
        tp->tm_wday += 7;
    }

    y = 1970;

    // Adjust the year and days to match the correct year
    while (days < 0 || days >= (__isleap(y) ? 366 : 365)) {
        int64_t yg = y + days / 365 - (days % 365 < 0);

        days -= ((yg - y) * 365
                 + LEAPS_THRU_END_OF(yg - 1)
                 - LEAPS_THRU_END_OF(y - 1));
        y = yg;
    }

    // Set the year in the tm structure, ensuring it is within valid range
    if ((y - 1900) < INT_MIN) {
        tp->tm_year = INT_MIN;
    } else if ((y - 1900) > INT_MAX) {
        tp->tm_year = INT_MAX;
    } else {
        tp->tm_year = y - 1900;
    }

    // Calculate the day of the year
    tp->tm_yday = days;

    // Determine the month and day of the month
    ip = __mon_yday[__isleap(y)];
    for (y = 11; days < (long int) ip[y]; --y) {
        continue;
    }
    days       -= ip[y];
    tp->tm_mon  = y;
    tp->tm_mday = days + 1;
} /* ot_timet2tmZ */


static const char *level_string[] = {
    "none",
    "fatal",
    "error",
    "info",
    "debug"
};

#define CHIMERA_LOG_BUF_SIZE 1024 * 1024

char              *ChimeraLogBuffers[2];
int                ChimeraLogIndex    = 0;
char              *ChimeraLogBuf      = NULL;
char              *ChimeraLogBufPtr   = NULL;
volatile int       ChimeraLogRun      = 1;
SYMBOL_EXPORT int  ChimeraLogLevel    = CHIMERA_LOG_INFO;
FILE              *ChimeraLogFile     = NULL; /* NULL => write to stdout */
int                ChimeraLogDisabled = 0;
pthread_mutex_t    ChimeraLogBufLock  = PTHREAD_MUTEX_INITIALIZER;
/* Held by the flusher across its stdio write and by the atfork prepare
 * handler: without it, fork() can land while the flusher is inside
 * fprintf/fflush holding the C library's stream lock, and the child inherits
 * that lock permanently taken -- its first write to the same stream (e.g.
 * chimera_vlog's inline drain once the buffer fills with no flusher alive)
 * deadlocks.  ChimeraLogBufLock alone cannot prevent this because the flusher
 * deliberately prints outside it. */
pthread_mutex_t    ChimeraLogFlushLock = PTHREAD_MUTEX_INITIALIZER;
pthread_t          ChimeraLogThread;
pthread_once_t     ChimeraLogOnce = PTHREAD_ONCE_INIT;

static void *
chimera_log_thread(void *arg)
{
    int   i;
    char *tmp;
    FILE *out = ChimeraLogFile ? ChimeraLogFile : stdout;

    while (ChimeraLogRun || ChimeraLogBufPtr > ChimeraLogBuf) {

        if (ChimeraLogBufPtr > ChimeraLogBuf) {

            /* FlushLock before BufLock; the atfork prepare handler takes them
             * in the same order. */
            pthread_mutex_lock(&ChimeraLogFlushLock);
            pthread_mutex_lock(&ChimeraLogBufLock);
            tmp              = ChimeraLogBuf;
            ChimeraLogIndex  = !ChimeraLogIndex;
            ChimeraLogBuf    = ChimeraLogBuffers[ChimeraLogIndex];
            ChimeraLogBufPtr = ChimeraLogBuf;
            pthread_mutex_unlock(&ChimeraLogBufLock);

            fprintf(out, "%s", tmp);
            fflush(out);
            pthread_mutex_unlock(&ChimeraLogFlushLock);
        }
        usleep(1000);
    }

    /* Clear the pointers under the lock so a straggling chimera_vlog()
     * after the flush sees NULL and bails instead of writing into freed
     * memory. */
    pthread_mutex_lock(&ChimeraLogBufLock);
    for (i = 0; i < 2; ++i) {
        free(ChimeraLogBuffers[i]);
        ChimeraLogBuffers[i] = NULL;
    }
    ChimeraLogBuf    = NULL;
    ChimeraLogBufPtr = NULL;
    pthread_mutex_unlock(&ChimeraLogBufLock);

    if (ChimeraLogFile) {
        fclose(ChimeraLogFile);
        ChimeraLogFile = NULL;
    }

    return NULL;
} /* chimera_log_thread */

static void
chimera_log_thread_exit(void)
{
    if (ChimeraLogRun) {
        ChimeraLogRun = 0;
        pthread_join(ChimeraLogThread, NULL);
    }
} /* chimera_log_thread_exit */

SYMBOL_EXPORT void
chimera_log_flush(void)
{
    if (ChimeraLogRun) {
        ChimeraLogRun = 0;
        pthread_join(ChimeraLogThread, NULL);
    }
} /* chimera_log_flush */

static void
chimera_log_flush_signal(int signum)
{
    chimera_log_flush();
} /* chimera_log_flush */

static void
chimera_log_atfork_prepare(void)
{
    /* FlushLock first (same order as the flusher): holding it across fork()
     * guarantees the flusher is not mid-fprintf/fflush, so the child cannot
     * inherit the C library's stream lock in a taken state. */
    pthread_mutex_lock(&ChimeraLogFlushLock);
    pthread_mutex_lock(&ChimeraLogBufLock);
} /* chimera_log_atfork_prepare */

static void
chimera_log_atfork_parent(void)
{
    pthread_mutex_unlock(&ChimeraLogBufLock);
    pthread_mutex_unlock(&ChimeraLogFlushLock);
} /* chimera_log_atfork_parent */

static void
chimera_log_atfork_child(void)
{
    /*
     * Reinitialize the mutex regardless of its state at fork time: if the
     * parent held it when fork() was called, the child inherits it locked
     * and no thread in the child will ever release it.  Re-initializing is
     * safe in both cases.  Also clear ChimeraLogRun so the inherited atexit
     * handler does not attempt to pthread_join() the parent's (now-invalid)
     * thread handle.  Any log data buffered by the parent at fork time is
     * discarded in the child (the parent's own copy still gets flushed by
     * the parent's log thread).
     */
    pthread_mutex_init(&ChimeraLogBufLock, NULL);
    pthread_mutex_init(&ChimeraLogFlushLock, NULL);
    ChimeraLogRun = 0;

    if (ChimeraLogBuf) {
        ChimeraLogBufPtr = ChimeraLogBuf;
        ChimeraLogBuf[0] = '\0';
    }
} /* chimera_log_atfork_child */

static void
chimera_log_thread_init(void)
{
    int i;

    if (ChimeraLogDisabled) {
        /*
         * Logging is disabled: don't allocate buffers or start the logging
         * thread.  Clear ChimeraLogRun so chimera_log_flush()/the atexit
         * handler don't try to join a thread that was never created.
         */
        ChimeraLogRun = 0;
        return;
    }

    for (i = 0; i < 2; ++i) {
        ChimeraLogBuffers[i] = calloc(CHIMERA_LOG_BUF_SIZE, sizeof(char));
    }

    ChimeraLogBuf    = ChimeraLogBuffers[ChimeraLogIndex];
    ChimeraLogBufPtr = ChimeraLogBuf;

    struct sigaction sa;
    sa.sa_handler = chimera_log_flush_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGABRT, &sa, NULL);

    pthread_atfork(chimera_log_atfork_prepare, chimera_log_atfork_parent,
                   chimera_log_atfork_child);

    int rc = chimera_pthread_create(&ChimeraLogThread, NULL,
                                    chimera_log_thread, NULL);

    if (rc != 0) {
        /* Without a flusher thread nothing ever drains the log buffer, so
         * later log calls would stall once it fills.  Report directly to
         * stderr (the logging system is the thing that failed) and abort. */
        fprintf(stderr, "chimera_log_init: pthread_create failed: %s\n",
                strerror(rc));
        abort();
    }

    atexit(chimera_log_thread_exit);

} /* chimera_log_thread_init */

SYMBOL_EXPORT void
chimera_log_init(void)
{
    pthread_once(&ChimeraLogOnce, chimera_log_thread_init);
} /* chimera_log_init */

SYMBOL_EXPORT void
chimera_log_set_file(FILE *fp)
{
    ChimeraLogFile     = fp;
    ChimeraLogDisabled = 0;
} /* chimera_log_set_file */

SYMBOL_EXPORT void
chimera_log_disable(void)
{
    ChimeraLogDisabled = 1;
} /* chimera_log_disable */

SYMBOL_EXPORT void
chimera_vlog(
    const char *level,
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    va_list     argp)
{
    struct timespec ts;
    struct tm       tm_info;
    uint64_t        pid, tid;

    if (ChimeraLogDisabled) {
        return;
    }

    clock_gettime(CLOCK_REALTIME, &ts);

    chimera_timet2tmZ(ts.tv_sec, &tm_info);

    pid = getpid();
    tid = gettid();

    pthread_mutex_lock(&ChimeraLogBufLock);

    if (!ChimeraLogBuf) {
        /* Buffers already torn down by chimera_log_flush(). */
        pthread_mutex_unlock(&ChimeraLogBufLock);
        return;
    }

    while ((ChimeraLogBufPtr + 4096) > (ChimeraLogBuf + CHIMERA_LOG_BUF_SIZE)) {
        if (!ChimeraLogRun) {
            /* No flusher thread is alive to drain the buffer (e.g. in a
             * fork()ed child, where the parent's log thread does not
             * survive), so waiting would spin forever: drain it inline. */
            FILE *out = ChimeraLogFile ? ChimeraLogFile : stdout;

            fprintf(out, "%s", ChimeraLogBuf);
            fflush(out);
            ChimeraLogBufPtr = ChimeraLogBuf;
            ChimeraLogBuf[0] = '\0';
            break;
        }
        pthread_mutex_unlock(&ChimeraLogBufLock);
        usleep(1);
        pthread_mutex_lock(&ChimeraLogBufLock);
    }

    ChimeraLogBufPtr += chimera_snprintf(ChimeraLogBufPtr,
                                         1024,
                                         "time=%04d-%02d-%02dT%02d:%02d:%02d.%09ldZ message=\"",
                                         tm_info.tm_year + 1900, tm_info.tm_mon + 1,
                                         tm_info.tm_mday, tm_info.tm_hour,
                                         tm_info.tm_min, tm_info.tm_sec, ts.tv_nsec);

    ChimeraLogBufPtr += chimera_vsnprintf(ChimeraLogBufPtr,
                                          1024,
                                          fmt, argp);

    ChimeraLogBufPtr += chimera_snprintf(ChimeraLogBufPtr,
                                         1024,
                                         "\" process=%lu thread=%lu level=%s module=%s source=\"%s:%d\"\n",
                                         pid, tid, level, mod, file, line);

    pthread_mutex_unlock(&ChimeraLogBufLock);
} /* chimera_vlog */

SYMBOL_EXPORT void
__chimera_debug(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(level_string[CHIMERA_LOG_DEBUG], mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_debug */

SYMBOL_EXPORT void
__chimera_info(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(level_string[CHIMERA_LOG_INFO], mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_info */

SYMBOL_EXPORT void
__chimera_error(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(level_string[CHIMERA_LOG_ERROR], mod, file, line, fmt, argp);
    va_end(argp);
} /* chimera_error */

SYMBOL_EXPORT void
__chimera_fatal(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(level_string[CHIMERA_LOG_FATAL], mod, file, line, fmt, argp);
    va_end(argp);

    chimera_log_flush();

    exit(1);
} /* chimera_fatal */

SYMBOL_EXPORT void
__chimera_abort(
    const char *mod,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list argp;

    va_start(argp, fmt);
    chimera_vlog(level_string[CHIMERA_LOG_FATAL], mod, file, line, fmt, argp);
    va_end(argp);

#ifndef CHIMERA_SANITIZE
    chimera_crash_handler(SIGABRT);
#else  /* ifndef CHIMERA_SANITIZE */
    chimera_log_flush();
 #endif /* ifndef CHIMERA_SANITIZE */

    abort();
} /* chimera_abort */

#define BACKTRACE_SIZE 64

static void
chimera_crash_handler(int signum)
{
    unw_cursor_t  cursor;
    unw_context_t context;
    unw_word_t    ip, sp, off;
    char          symbol[256];

    chimera_error("core", __FILE__, __LINE__, "Received signal %d.", signum);

    unw_getcontext(&context);
    unw_init_local(&cursor, &context);

    while (unw_step(&cursor) > 0) {
        unw_get_reg(&cursor, UNW_REG_IP, &ip);
        unw_get_reg(&cursor, UNW_REG_SP, &sp);

        if (unw_get_proc_name(&cursor, symbol, sizeof(symbol), &off) == 0) {
            chimera_error("core", __FILE__, __LINE__, "ip = %lx, sp = %lx (%s+0x%lx)",
                          (long) ip, (long) sp, symbol, (long) off);
        } else {
            chimera_error("core", __FILE__, __LINE__, "ip = %lx, sp = %lx (unknown)",
                          (long) ip, (long) sp);
        }
    }

    chimera_log_flush_signal(signum);

    signal(signum, SIG_DFL);
    raise(signum);
} /* chimera_crash_handler */

SYMBOL_EXPORT void
chimera_enable_crash_handler(void)
{
    struct sigaction sa;

    sa.sa_handler = chimera_crash_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESETHAND; // Reset to default handler after first signal

    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGFPE, &sa, NULL);
    sigaction(SIGILL, &sa, NULL);
    sigaction(SIGBUS, &sa, NULL);
} /* chimera_enable_crash_handler */
