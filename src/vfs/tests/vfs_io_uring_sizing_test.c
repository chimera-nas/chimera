// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the module's real initialization and kernel allocations. Interpose
* only to observe the requested and allocated worker-ring sizes: changing the
* libevpl setting alone must not make this test pass. Each case runs in a fresh
* process because invalid configuration deliberately aborts initialization. */
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dlfcn.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>
#include <liburing.h>

#include "evpl/evpl.h"
#include "common/logging.h"
#include "common/macros.h"
#include "vfs/io_uring/io_uring.h"

static unsigned int requested_entries;
static unsigned int allocated_entries;
static unsigned int completed;

static void
complete_request(struct chimera_vfs_request *request)
{
    completed++;
} /* complete_request */

static void
check_synchronous_requests(void *thread)
{
    const unsigned int             ops[] = {
        CHIMERA_VFS_OP_MOUNT,    CHIMERA_VFS_OP_UMOUNT,
        CHIMERA_VFS_OP_ALLOCATE, CHIMERA_VFS_OP_SEEK
    };
    struct chimera_vfs_request     request;
    struct chimera_vfs_open_handle handle = { 0 };
    char                           scratch[CHIMERA_VFS_PLUGIN_DATA_SIZE];

    handle.vfs_private = -1;
    for (unsigned int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        for (unsigned int j = 0; j < 2; j++) {
            memset(&request, 0, sizeof(request));
            request.opcode      = ops[i];
            request.complete    = complete_request;
            request.plugin_data = scratch;
            if (ops[i] == CHIMERA_VFS_OP_MOUNT) {
                request.mount.path = ""; /* ENOENT, without a host export. */
            } else if (ops[i] == CHIMERA_VFS_OP_ALLOCATE) {
                request.allocate.handle = &handle;
                request.allocate.length = 4096;
            } else if (ops[i] == CHIMERA_VFS_OP_SEEK) {
                request.seek.handle = &handle;
            }
            completed = 0;
            vfs_io_uring.dispatch(&request, thread);
            /* These inline completions must release their in-flight slot,
             * including the error paths; the next op must never queue. */
            assert(completed == 1);
            assert(request.status == (ops[i] == CHIMERA_VFS_OP_UMOUNT ?
                                      CHIMERA_VFS_OK : ops[i] == CHIMERA_VFS_OP_MOUNT ?
                                      CHIMERA_VFS_ENOENT : CHIMERA_VFS_EBADF));
        }
    }
} /* check_synchronous_requests */

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
    int rc;

    real_init = dlsym(RTLD_NEXT, "io_uring_queue_init_params");
    assert(real_init != NULL);
    rc = real_init(entries, ring, params);
    if (params->flags & IORING_SETUP_ATTACH_WQ) {
        requested_entries = entries;
        allocated_entries = params->sq_entries;
    }
    return rc;
} /* io_uring_queue_init_params */

struct sizing_case {
    const char  *env;
    const char  *config;
    unsigned int requested;
    unsigned int allocated;
};

static const struct sizing_case cases[] = {
    { NULL,                               "",                                                         4096,
      4096                                                                },
    { "256",                              "",                                                         1024,
      1024                                                                                                                             },
    { "1",                                "",                                                         4,
      4                                                                                                                                                                                            },
    { "3",                                "",                                                         12,
      16                                                                                                                                                                                                                                                       },
    { "8192",                             "",                                                         32768,
      32768                                                                                                                                                                                                                                                                                                                },
    { NULL,                               "{\"max_inflight\":128}",                                   512,
      512                                                                                                                                                                                                                                                                                                                                                                              },
    { "256",                              "{\"max_inflight\":64}",                                    256,
      256                                                                                                                                                                                                                                                                                                                                                                                                                                          },
    { "",                                 "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        },
    { "0",                                "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    },
    { "-1",                               "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                },
    { "8193",                             "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            },
    { "1x",                               "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        },
    { "99999999999999999999999999999999", "",                                                         0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    },
    { NULL,                               "{\"max_inflight\":0}",                                     0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                },
    { NULL,                               "{\"max_inflight\":-1}",                                    0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            },
    { NULL,                               "{\"max_inflight\":8193}",                                  0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        },
    { NULL,                               "{\"max_inflight\":1.5}",                                   0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    },
    { NULL,                               "{\"max_inflight\":\"256\"}",                               0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                },
    { NULL,                               "{\"max_inflight\":true}",                                  0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            },
    { NULL,                               "{\"max_inflight\":null}",                                  0,
      0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            }
};

static void
run_case(const struct sizing_case *test)
{
    struct evpl *evpl;
    void        *shared, *thread;

    if (test->env) {
        assert(setenv("CHIMERA_IO_URING_MAX_INFLIGHT", test->env, 1) == 0);
    } else {
        assert(unsetenv("CHIMERA_IO_URING_MAX_INFLIGHT") == 0);
    }
    chimera_log_init();
    shared = vfs_io_uring.init(test->config, NULL);
    /* Invalid input must abort in init itself, not in a test assertion or a
     * later worker allocation. Returning normally makes the parent fail. */
    if (!test->requested) {
        if (shared) {
            vfs_io_uring.destroy(shared);
        }
        return;
    }
    assert(shared != NULL);
    evpl   = evpl_create(NULL);
    thread = vfs_io_uring.thread_init(evpl, shared);
    assert(requested_entries == test->requested);
    assert(allocated_entries == test->allocated);
    if (test->requested == 4) {
        check_synchronous_requests(thread);
    }
    vfs_io_uring.thread_destroy(thread);
    vfs_io_uring.destroy(shared);
    evpl_destroy(evpl);
    evpl_cleanup();
} /* run_case */

int
main(void)
{
    struct rlimit core_limit = { 0, 0 };
    int           status;
    pid_t         pid;

    assert(setrlimit(RLIMIT_CORE, &core_limit) == 0);
    for (unsigned int i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        pid = fork();
        assert(pid >= 0);
        if (pid == 0) {
            run_case(&cases[i]);
            _exit(0);
        }
        assert(waitpid(pid, &status, 0) == pid);
        if (cases[i].requested) {
            assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        } else {
            assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
        }
        fprintf(stderr, "PASS: io_uring sizing case %u\n", i);
    }
    return 0;
} /* main */
