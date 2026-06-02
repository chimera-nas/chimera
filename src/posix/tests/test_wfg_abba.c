// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * test_wfg_abba - force ABBA lock cycles in the diskfs backend and assert the
 * wait-for-graph deadlock detector resolves them (no hang, correct results).
 *
 * The cycle: a directory readdir holds the directory lock and then acquires each
 * child inode lock (dir -> child order, which is NOT canonical fh order), while a
 * same-directory rename acquires the child and the directory in canonical fh
 * order (child -> dir when the child's fh sorts first).  Run many readdir and
 * rename threads concurrently against ONE shared client/server so the server
 * processes them in parallel: readdir holds dir wants child, rename holds child
 * wants dir => ABBA.  Under wait-die this starved/hung; under WFG exactly one
 * (the youngest explicit txn) aborts and retries, both complete.
 *
 * Threads share a single client (no fork) so they drive genuine concurrent RPCs.
 */

#include <pthread.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include "posix_test_common.h"

#define WFG_DIR         "/test/wfg"
#define WFG_NFILES      16
#define WFG_MAX_THREADS 16

static int          WFG_RENAMERS = 4;
static int          WFG_READERS  = 4;
static int          WFG_SECONDS  = 10;

static volatile int g_stop;
static _Atomic long g_renames;
static _Atomic long g_readdirs;
static _Atomic int  g_failed;

static void *
wfg_rename_thread(void *arg)
{
    long id = (long) arg;

    while (!g_stop) {
        for (int i = id; i < WFG_NFILES; i += WFG_RENAMERS) {
            char a[128], b[128];

            snprintf(a, sizeof(a), WFG_DIR "/f%d", i);
            snprintf(b, sizeof(b), WFG_DIR "/g%d", i);

            /* Rename back and forth in the same directory: locks the child
             * inode and the parent directory (canonical fh order). */
            if (chimera_posix_rename(a, b) == 0) {
                atomic_fetch_add(&g_renames, 1);
                if (chimera_posix_rename(b, a) == 0) {
                    atomic_fetch_add(&g_renames, 1);
                } else {
                    /* leave it as b; the readers tolerate either name */
                }
            }
        }
    }
    return NULL;
} /* wfg_rename_thread */

static void *
wfg_readdir_thread(void *arg)
{
    (void) arg;

    while (!g_stop) {
        CHIMERA_DIR   *d = chimera_posix_opendir(WFG_DIR);
        struct dirent *de;
        int            n = 0;

        if (!d) {
            continue;
        }
        /* Bounded: readdir under continuous concurrent rename can keep returning
         * entries (the hash-ordered cursor never converges to EOF as names move),
         * which is a POSIX-undefined readdir-under-modification livelock, not a
         * lock issue.  Cap the walk so the thread always makes progress; we are
         * here to exercise the dir+child lock acquisition, not to drain to EOF. */
        while ((de = chimera_posix_readdir(d)) != NULL) {
            if (++n > WFG_NFILES * 8) {
                break;
            }
        }
        chimera_posix_closedir(d);
        atomic_fetch_add(&g_readdirs, 1);
        (void) n;
    }
    return NULL;
} /* wfg_readdir_thread */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    pthread_t             renamers[WFG_MAX_THREADS];
    pthread_t             readers[WFG_MAX_THREADS];
    int                   i;
    const char           *e;

    if ((e = getenv("WFG_RENAMERS"))) {
        WFG_RENAMERS = atoi(e);
    }
    if ((e = getenv("WFG_READERS"))) {
        WFG_READERS = atoi(e);
    }
    if ((e = getenv("WFG_SECONDS"))) {
        WFG_SECONDS = atoi(e);
    }

    posix_test_init(&env, argv, argc);

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "wfg_abba: mount failed\n");
        posix_test_fail(&env);
        return 1;
    }

    chimera_posix_mkdir(WFG_DIR, 0755);
    for (i = 0; i < WFG_NFILES; i++) {
        char p[128];
        int  fd;
        snprintf(p, sizeof(p), WFG_DIR "/f%d", i);
        fd = chimera_posix_open(p, O_CREAT | O_RDWR, 0644);
        if (fd >= 0) {
            chimera_posix_close(fd);
        }
    }

    g_stop = 0;
    for (i = 0; i < WFG_RENAMERS; i++) {
        pthread_create(&renamers[i], NULL, wfg_rename_thread, (void *) (long) i);
    }
    for (i = 0; i < WFG_READERS; i++) {
        pthread_create(&readers[i], NULL, wfg_readdir_thread, NULL);
    }

    sleep(WFG_SECONDS);
    g_stop = 1;

    for (i = 0; i < WFG_RENAMERS; i++) {
        pthread_join(renamers[i], NULL);
    }
    for (i = 0; i < WFG_READERS; i++) {
        pthread_join(readers[i], NULL);
    }

    fprintf(stderr, "wfg_abba: %ld renames, %ld readdirs completed\n",
            (long) g_renames, (long) g_readdirs);

    /* Every file must still exist as exactly one of f<i> or g<i> (no lost or
     * duplicated entries from a mishandled conflict/retry). */
    for (i = 0; i < WFG_NFILES; i++) {
        char        a[128], b[128];
        struct stat st;
        int         have_a, have_b;

        snprintf(a, sizeof(a), WFG_DIR "/f%d", i);
        snprintf(b, sizeof(b), WFG_DIR "/g%d", i);
        have_a = (chimera_posix_stat(a, &st) == 0);
        have_b = (chimera_posix_stat(b, &st) == 0);
        if (have_a + have_b != 1) {
            fprintf(stderr, "wfg_abba: file %d corrupt (f=%d g=%d)\n", i, have_a, have_b);
            atomic_store(&g_failed, 1);
        }
    }

    if ((WFG_RENAMERS && g_renames == 0) || (WFG_READERS && g_readdirs == 0)) {
        fprintf(stderr, "wfg_abba: workload did not run (renames=%ld readdirs=%ld)\n",
                (long) g_renames, (long) g_readdirs);
        atomic_store(&g_failed, 1);
    }

    if (atomic_load(&g_failed)) {
        posix_test_fail(&env);
        return 1;
    }

    fprintf(stderr, "wfg_abba completed successfully\n");
    posix_test_success(&env);
    return 0;
} /* main */
