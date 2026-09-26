// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Inject ENOMEM without exhausting the host, and check that both shared and
 * worker initialization leave resource diagnostics before disabling/aborting. */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <liburing.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "common/logging.h"
#include "common/macros.h"
#include "vfs/io_uring/io_uring.h"

static int fail_worker;

SYMBOL_EXPORT int
io_uring_queue_init_params(
    unsigned int            entries,
    struct io_uring        *ring,
    struct io_uring_params *params)
{
    int (*real_init)(
        unsigned int,
        struct io_uring *,
        struct io_uring_params *);

    if (!fail_worker || (params->flags & IORING_SETUP_ATTACH_WQ)) {
        return -ENOMEM;
    }
    real_init = dlsym(RTLD_NEXT, "io_uring_queue_init_params");
    assert(real_init != NULL);
    return real_init(entries, ring, params);
} /* io_uring_queue_init_params */

static void
check_failure(int worker)
{
    FILE  *output = tmpfile();
    char   text[65536];
    size_t length;
    int    status;
    pid_t  pid;

    assert(output != NULL);
    fail_worker = worker;
    pid         = fork();
    assert(pid >= 0);
    if (pid == 0) {
        void *shared;

        assert(dup2(fileno(output), STDERR_FILENO) == STDERR_FILENO);
        assert(dup2(fileno(output), STDOUT_FILENO) == STDOUT_FILENO);
        assert(setenv("CHIMERA_IO_URING_MAX_INFLIGHT", "256", 1) == 0);
        chimera_log_init();
        shared = vfs_io_uring.init(NULL, NULL);
        if (worker) {
            struct evpl *evpl;

            assert(shared != NULL);
            evpl = evpl_create(NULL);
            vfs_io_uring.thread_init(evpl, shared);
            _exit(1); /* A worker allocation failure must remain fatal. */
        }
        assert(shared == NULL);
        chimera_log_flush();
        _exit(0);
    }

    assert(waitpid(pid, &status, 0) == pid);
    assert(fseek(output, 0, SEEK_SET) == 0);
    length = fread(text, 1, sizeof(text) - 1, output);
    assert(!ferror(output));
    assert(feof(output));
    text[length] = '\0';
    fclose(output);
    /* Keep the child's diagnostics in CTest's log if an assertion fails. */
    fprintf(stderr, "%s", text);
    if (worker) {
        assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
        assert(strstr(text, "worker queue initialization failed: entries=1024 max_inflight=256 flags="));
        assert(strstr(text, "Failed to create io_uring queue"));
    } else {
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        assert(strstr(text, "shared queue initialization failed: entries=256 max_inflight=256 flags="));
        assert(strstr(text, "io_uring disabled"));
    }
    assert(strstr(text, "rc=-12"));
    assert(strstr(text, "/proc/meminfo: MemTotal:"));
    assert(strstr(text, "/proc/self/status: VmRSS:"));
    assert(strstr(text, "Max locked memory"));
    assert(strstr(text, "/proc/self/cgroup:"));
    /* The cgroup mount can be absent; even then its diagnostic must say so. */
    assert(strstr(text, "/sys/fs/cgroup/memory.current:"));
    assert(strstr(text, "/sys/fs/cgroup/memory.events:"));
} /* check_failure */

int
main(void)
{
    struct rlimit core_limit = { 0, 0 };

    assert(setrlimit(RLIMIT_CORE, &core_limit) == 0);
    check_failure(0);
    check_failure(1);
    return 0;
} /* main */
