/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Deadlock backstop for the quint MBT harnesses, with the context intact.
 *
 * Every harness runs the whole stack -- server, client and the model oracle
 * -- in one process, so a server deadlock leaves the driver spinning in its
 * reply wait forever rather than failing.  Each one therefore arms an alarm
 * as a backstop.  Left at SIGALRM's default disposition that turns a hang
 * into a bare "SIGALRM" and nothing else: the harness dies with its stdout
 * still sitting in a block buffer (stdout is a pipe under ctest, not a tty),
 * so the per-trace progress it printed on the way in is discarded along with
 * it.  Three separate CI hangs of nfsaux/batch_memfs reported exactly that
 * and no more, which is not enough to act on.
 *
 * Arming through this header keeps the backstop and the context: stdout is
 * switched to line buffering so whatever the harness already printed has
 * escaped, and the handler names the position the alarm caught -- trace and
 * step where the caller tracks one -- before letting the default disposition
 * kill the process, so ctest still reports the failure as SIGALRM.
 *
 * The handler touches nothing but write(2) and its own sig_atomic_t state.
 */

#ifndef CHIMERA_MBT_WATCHDOG_H
#define CHIMERA_MBT_WATCHDOG_H

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Position the alarm would report, published by the driver as it advances.
 * The trace and tag are string literals or long-lived buffers owned by the
 * caller; the handler only ever reads them. */
static const char *volatile  mbt_watchdog_trace = NULL;
static const char *volatile  mbt_watchdog_tag   = NULL;
static volatile sig_atomic_t mbt_watchdog_step  = -1;

static inline void
mbt_watchdog_write(const char *s)
{
    if (s) {
        ssize_t rc = write(STDERR_FILENO, s, strlen(s));

        (void) rc;
    }
} /* mbt_watchdog_write */

/* Decimal conversion done by hand: snprintf is not async-signal-safe. */
static inline void
mbt_watchdog_write_int(long v)
{
    char buf[24];
    int  i = (int) sizeof(buf);

    if (v < 0) {
        mbt_watchdog_write("?");
        return;
    }
    buf[--i] = '\0';
    do {
        buf[--i] = (char) ('0' + (v % 10));
        v       /= 10;
    } while (v && i > 0);
    mbt_watchdog_write(&buf[i]);
} /* mbt_watchdog_write_int */

static inline void
mbt_watchdog_on_alarm(int sig)
{
    mbt_watchdog_write("\n*** MBT WATCHDOG: no progress before the alarm; the "
                       "stack is wedged ***\n  trace: ");
    mbt_watchdog_write(mbt_watchdog_trace ? mbt_watchdog_trace : "(none)");
    mbt_watchdog_write("\n  step:  ");
    mbt_watchdog_write_int((long) mbt_watchdog_step);
    mbt_watchdog_write(" (");
    mbt_watchdog_write(mbt_watchdog_tag ? mbt_watchdog_tag : "(none)");
    mbt_watchdog_write(")\n");

    /* Die by SIGALRM after all, so ctest still reports it as such. */
    signal(sig, SIG_DFL);
    raise(sig);
} /* mbt_watchdog_on_alarm */

/*
 * Arm the backstop for `seconds`.  Idempotent with respect to the handler and
 * the buffering mode, so the per-trace callers can re-arm freely.
 */
static inline void
mbt_watchdog_arm(unsigned int seconds)
{
    static int installed = 0;

    if (!installed) {
        /* Line buffering so the progress printed before a hang survives it. */
        setvbuf(stdout, NULL, _IOLBF, 0);
        signal(SIGALRM, mbt_watchdog_on_alarm);
        installed = 1;
    }
    alarm(seconds);
} /* mbt_watchdog_arm */

/* Publish the position the watchdog should name if it fires. */
static inline void
mbt_watchdog_at(
    const char *trace,
    int         step,
    const char *tag)
{
    mbt_watchdog_trace = trace;
    mbt_watchdog_step  = step;
    mbt_watchdog_tag   = tag;
} /* mbt_watchdog_at */

static inline void
mbt_watchdog_disarm(void)
{
    alarm(0);
    mbt_watchdog_at(NULL, -1, NULL);
} /* mbt_watchdog_disarm */

#endif /* CHIMERA_MBT_WATCHDOG_H */
