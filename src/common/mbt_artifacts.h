/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Debug-log and metrics artifacts for the quint MBT harnesses.
 *
 * Every quint harness runs the chimera stack at CHIMERA_LOG_DEBUG with the
 * log routed to a file: the per-request dump paths -- nfs3_dump_* and
 * nfs4_dump_*, smb_dump_*, the vfs_dump macros -- are compiled in but dead
 * at the default INFO level, so replaying the corpus in debug mode is what
 * makes the model traffic execute them at all.  Routing to a file rather than
 * stdout keeps the harness's own reporting -- and the line-protocol
 * drivers' stdout protocol -- clean.
 *
 * At teardown each harness scrapes its Prometheus registry once through
 * chimera_metrics_dump_file(), the same rendering the live /metrics
 * endpoint serves, again purely so every corpus run exercises the metrics
 * export path end to end.
 *
 * Both artifacts land in the process's working directory (for a ctest, the
 * registering directory's build tree) as <name>.debug.log and
 * <name>.metrics.prom.  <name> is $CHIMERA_MBT_ARTIFACTS when set -- each
 * quint ctest sets it to its own test name so tests sharing a harness
 * binary do not clobber each other -- and falls back to the program's
 * short name for manual runs.
 */

#ifndef CHIMERA_MBT_ARTIFACTS_H
#define CHIMERA_MBT_ARTIFACTS_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/logging.h"
#include "metrics/metrics.h"

static inline const char *
mbt_artifacts_name(void)
{
    const char *name = getenv("CHIMERA_MBT_ARTIFACTS");

    if (name && *name) {
        return name;
    }
#ifdef __APPLE__
    return getprogname();
#else  /* ifdef __APPLE__ */
    {
        /* Declared in <errno.h> only under _GNU_SOURCE, which the including
         * harness may or may not define; the symbol itself is always there. */
        extern char *program_invocation_short_name;

        return program_invocation_short_name;
    }
#endif /* ifdef __APPLE__ */
} /* mbt_artifacts_name */

/* Call before the first chimera_*_init: routes the log to <name>.debug.log
 * and raises the level to CHIMERA_LOG_DEBUG.  Idempotent, so environments
 * that cycle within one process (the probes) set up logging exactly once. */
static inline void
mbt_debug_log_start(void)
{
    static int started = 0;
    char       path[512];
    FILE      *fp;

    if (started) {
        return;
    }
    started = 1;

    snprintf(path, sizeof(path), "%s.debug.log", mbt_artifacts_name());

    fp = fopen(path, "w");
    if (!fp) {
        fprintf(stderr, "mbt_artifacts: cannot open %s: %s\n",
                path, strerror(errno));
        exit(1);
    }

    chimera_log_set_file(fp);
    chimera_log_init();
    ChimeraLogLevel = CHIMERA_LOG_DEBUG;
} /* mbt_debug_log_start */

/* Call at teardown, while the registry is still alive.  Environments that
 * cycle within one process rewrite the file each time; the final cycle's
 * scrape is the artifact.  A failed dump fails the test -- the scrape is
 * part of what the harness exists to exercise. */
static inline void
mbt_metrics_dump(struct prometheus_metrics *metrics)
{
    char path[512];

    snprintf(path, sizeof(path), "%s.metrics.prom", mbt_artifacts_name());

    if (chimera_metrics_dump_file(metrics, path) != 0) {
        fprintf(stderr, "mbt_artifacts: metrics dump to %s failed\n", path);
        exit(1);
    }
} /* mbt_metrics_dump */

#endif /* CHIMERA_MBT_ARTIFACTS_H */
