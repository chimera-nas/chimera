// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* A fatal error raised while an evpl thread is still running, as when a VFS
 * module's init fails after the metrics server has started, must exit with
 * status 1 and flush its message, not abort in libevpl's atexit cleanup. */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "evpl/evpl_thread.h"
#include "common/logging.h"

static void *
idle_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    return private_data;
} /* idle_thread_init */

int
main(
    int    argc,
    char **argv)
{
    FILE  *output = tmpfile();
    char   text[4096];
    size_t length;
    int    status;
    pid_t  pid;

    (void) argc;
    (void) argv;

    assert(output);

    pid = fork();
    assert(pid >= 0);

    if (pid == 0) {
        assert(dup2(fileno(output), STDERR_FILENO) == STDERR_FILENO);
        assert(dup2(fileno(output), STDOUT_FILENO) == STDOUT_FILENO);
        chimera_log_init();
        /* Leave a live evpl context behind, the way the metrics server does. */
        assert(evpl_thread_create(NULL, idle_thread_init, NULL, NULL));
        chimera_fatal("test", __FILE__, __LINE__, "fatal with a live evpl thread");
    }

    assert(waitpid(pid, &status, 0) == pid);
    assert(fseek(output, 0, SEEK_SET) == 0);
    length = fread(text, 1, sizeof(text) - 1, output);
    assert(!ferror(output));
    text[length] = '\0';
    fclose(output);
    /* Keep the child's output in CTest's log if an assertion fails. */
    fprintf(stderr, "%s", text);

    assert(WIFEXITED(status) && WEXITSTATUS(status) == 1);
    assert(strstr(text, "fatal with a live evpl thread"));

    return 0;
} /* main */
