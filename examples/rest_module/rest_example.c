// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: MIT

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <chimera_rest_sdk.h>

#ifndef EXAMPLE_API_VERSION
#define EXAMPLE_API_VERSION 1
#endif /* ifndef EXAMPLE_API_VERSION */
#ifndef EXAMPLE_ABI_VERSION
#define EXAMPLE_ABI_VERSION CHIMERA_REST_ABI_VERSION
#endif /* ifndef EXAMPLE_ABI_VERSION */

struct example_state {
    const struct chimera_rest_host *host;
    atomic_int                      waiting;
};

struct example_thread {
    struct example_state *state;
    pthread_t             workers[32];
    unsigned int          count;
};

struct example_work {
    struct example_state        *state;
    struct chimera_rest_request *request;
};

static int32_t
example_init(
    const struct chimera_rest_host *host,
    const char                     *config,
    void                          **data)
{
    struct example_state *state;

    if (host->abi_version != CHIMERA_REST_ABI_VERSION ||
        host->struct_size < sizeof(*host)) {
        return -1;
    }
    state = calloc(1, sizeof(*state));
    if (!state) {
        return -1;
    }
    state->host = host;
    atomic_init(&state->waiting, 0);
    *data = state;
    return 0;
} /* example_init */

static int32_t
example_thread_init(
    void                        *data,
    struct chimera_rest_context *context,
    void                       **result)
{
    struct example_thread *thread = calloc(1, sizeof(*thread));

    if (!thread) {
        return -1;
    }
    thread->state = data;
    *result       = thread;
    return 0;
} /* example_thread_init */

static void
hello(
    struct chimera_rest_request *request,
    void                        *data)
{
    struct example_thread          *thread = data;
    const struct chimera_rest_host *host   = thread->state->host;
    const char                     *name   = host->param(request, "name");

    host->reply(request, 200, "text/plain", name, strlen(name));
} /* hello */

static void
echo(
    struct chimera_rest_request *request,
    void                        *data)
{
    struct example_thread          *thread = data;
    const struct chimera_rest_host *host   = thread->state->host;
    size_t                          length;
    const void                     *body = host->body(request, &length);

    host->reply(request, 200, "application/octet-stream", body, length);
} /* echo */

static void *
worker(void *data)
{
    struct example_work            *work  = data;
    const struct chimera_rest_host *host  = work->state->host;
    struct timespec                 delay = { .tv_nsec = 1000000 };

    atomic_fetch_add(&work->state->waiting, 1);
    while (!host->cancelled(work->request)) {
        nanosleep(&delay, NULL);
    }
    atomic_fetch_sub(&work->state->waiting, 1);
    /* A reply after disconnect still releases the module's ownership. */
    host->reply(work->request, 204, NULL, NULL, 0);
    free(work);
    return NULL;
} /* worker */

static void
wait_for_disconnect(
    struct chimera_rest_request *request,
    void                        *data)
{
    struct example_thread *thread = data;
    struct example_work   *work   = malloc(sizeof(*work));

    if (!work || thread->count == 32) {
        free(work);
        thread->state->host->reply(request, 503, NULL, NULL, 0);
        return;
    }
    work->state   = thread->state;
    work->request = request;
    if (pthread_create(&thread->workers[thread->count], NULL, worker, work)) {
        free(work);
        thread->state->host->reply(request, 500, NULL, NULL, 0);
        return;
    }
    thread->count++;
} /* wait_for_disconnect */

static void
waiting(
    struct chimera_rest_request *request,
    void                        *data)
{
    struct example_thread *thread = data;
    const char            *body   = atomic_load(&thread->state->waiting) ? "waiting" : "idle";

    thread->state->host->reply(request, 200, "text/plain", body, strlen(body));
} /* waiting */

static void
example_thread_destroy(void *data)
{
    struct example_thread *thread = data;
    unsigned int           i;

    for (i = 0; i < thread->count; i++) {
        pthread_join(thread->workers[i], NULL);
    }
    free(thread);
} /* example_thread_destroy */

static const struct chimera_rest_route        hello_route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/hello/{name}",
    .handle      = hello,
};
static const struct chimera_rest_route        echo_route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/echo",
    .max_body    = 64,
    .handle      = echo,
};
static const struct chimera_rest_route        wait_route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/wait",
    .handle      = wait_for_disconnect,
};
static const struct chimera_rest_route        waiting_route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/waiting",
    .handle      = waiting,
};
static const struct chimera_rest_route *const routes[] = {
    &hello_route, &echo_route, &wait_route, &waiting_route,
};
static const struct chimera_rest_module       module = {
    .abi_version    = EXAMPLE_ABI_VERSION,
    .struct_size    = sizeof(struct chimera_rest_module),
    .name           = "example",
    .api_version    = EXAMPLE_API_VERSION,
    .routes         = routes,
    .num_routes     = sizeof(routes) / sizeof(routes[0]),
    .init           = example_init,
    .thread_init    = example_thread_init,
    .thread_destroy = example_thread_destroy,
    .destroy        = free,
};

CHIMERA_REST_EXPORT const struct chimera_rest_module *
chimera_rest_module_get_v1(void)
{
    return &module;
} /* chimera_rest_module_get_v1 */
