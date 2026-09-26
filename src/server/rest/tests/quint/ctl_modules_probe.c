// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/resource.h>
#include <jansson.h>
#include <stdatomic.h>
#include <pthread.h>
#include "ctl_mbt_common.h"

static const char *mode;

static void
check(
    int         success,
    const char *what)
{
    if (!success) {
        fprintf(stderr, "REST modules: %s\n", what);
        exit(1);
    }
} /* check */

static void
configure(struct chimera_server_config *config)
{
    if (!strcmp(mode, "empty")) {
        return;
    }
    if (!strcmp(mode, "docs")) {
        check(!chimera_server_config_add_rest_module(config, "docs", NULL, NULL, 1), "configure docs");
        return;
    }
    if (!strcmp(mode, "core")) {
        check(!chimera_server_config_add_rest_module(config, "core", NULL, NULL, 1), "configure core");
        return;
    }
    if (!strcmp(mode, "badpublic")) {
        chimera_server_config_add_rest_module(config, "core", NULL, NULL, 0);
        return;
    }
    if (!strcmp(mode, "missing")) {
        chimera_server_config_add_rest_module(config, "example", "/missing/chimera-rest.so", NULL, 0);
        return;
    }
    if (!strcmp(mode, "badabi")) {
        chimera_server_config_add_rest_module(config, "example", EXAMPLE_BAD_ABI, NULL, 0);
        return;
    }
    check(!chimera_server_config_add_rest_module(config, "example", EXAMPLE_V1, NULL, 0), "configure example v1");
    check(!chimera_server_config_add_rest_module(config, "example",
                                                 !strcmp(mode, "duplicate") ? EXAMPLE_V1 : EXAMPLE_V2, NULL, 0),
          "configure example v2");
    check(!chimera_server_config_add_rest_module(config, "docs", NULL, NULL, 1), "configure docs");
} /* configure */

struct shutdown_state {
    struct chimera_server *server;
    atomic_int             done;
};

static void *
shutdown_server(void *data)
{
    struct shutdown_state *state = data;

    chimera_server_destroy(state->server);
    atomic_store(&state->done, 1);
    return NULL;
} /* shutdown_server */

int
main(
    int    argc,
    char **argv)
{
    struct ctl_env            env;
    struct ctl_env_opts       opts = { .no_rest_modules = 1, .configure = configure };
    struct ctl_conn          *api, *pending;
    struct ctl_res            res, pending_result = { 0 };
    struct evpl_http_request *request;
    struct rlimit             no_core = { 0, 0 };
    uint64_t                  deadline;
    char                      body[66];
    json_t                   *doc, *paths;

    setrlimit(RLIMIT_CORE, &no_core);
    mode              = argc > 1 ? argv[1] : "modules";
    opts.auth_enabled = !strcmp(mode, "auth");
    ctl_env_open(&env, &opts);
    api = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);

    ctl_get(api, "/api/core/v1/version", &res);
    check(res.status == (!strcmp(mode, "core") ? 200 : 404), "core must be loaded explicitly");
    ctl_get(api, "/api/debug/v1/fsop", &res);
    check(res.status == 404, "debug must not load implicitly");
    ctl_get(api, "/api/docs/v1/openapi.json", &res);
    check(res.status == ((!strcmp(mode, "empty") || !strcmp(mode, "core")) ? 404 : 200), "docs selection");
    if (!strcmp(mode, "empty") || !strcmp(mode, "core") || !strcmp(mode, "docs")) {
        goto done;
    }
    doc   = json_loads(res.body, 0, NULL);
    paths = json_object_get(doc, "paths");
    check(json_object_get(paths, "/api/example/v1/hello/{name}") != NULL, "v1 documentation");
    check(json_object_get(paths, "/api/example/v2/hello/{name}") != NULL, "v2 documentation");
    check(json_object_get(paths, "/api/core/v1/users") == NULL, "unloaded core omitted from docs");
    json_decref(doc);

    ctl_get(api, "/api/example/v1/hello/Ada", &res);
    check(res.status == (opts.auth_enabled ? 401 : 200), "external routes inherit authentication");
    if (opts.auth_enabled) {
        goto done;
    }
    check(!strcmp(res.body, "Ada"), "named parameter");
    ctl_get(api, "/api/example/v2/hello/Grace", &res);
    check(res.status == 200 && !strcmp(res.body, "Grace"), "independent version, same entry symbol");
    ctl_get(api, "/api/example/v1/hello/Ada%20Lovelace?ignored=yes", &res);
    check(res.status == 200 && !strcmp(res.body, "Ada Lovelace"), "query separated and parameter decoded");
    ctl_get(api, "/api/example/v1/hello/a%2fb", &res);
    check(res.status == 400, "encoded slash rejected");
    ctl_get(api, "/api/example/v1/hello/%00", &res);
    check(res.status == 400, "encoded NUL rejected");
    ctl_get(api, "/api/example/v3/hello/Ada", &res);
    check(res.status == 404, "unknown version");
    ctl_get(api, "/api/example/v10/hello/Ada", &res);
    check(res.status == 404, "version prefix boundary");
    ctl_get(api, "/version", &res);
    check(res.status == 404, "old version URL removed");
    ctl_post(api, "/api/example/v1/hello/Ada", "body", &res);
    check(res.status == 405, "wrong method with body finishes");
    ctl_post(api, "/api/example/v1/missing", "body", &res);
    check(res.status == 404, "unknown path with body finishes");
    ctl_post(api, "/api/example/v1/echo", "hello", &res);
    check(res.status == 200 && !strcmp(res.body, "hello"), "body copied before completion");
    memset(body, 'a', sizeof(body));
    body[64] = '\0';
    ctl_post(api, "/api/example/v1/echo", body, &res);
    check(res.status == 200 && res.body_len == 64, "exact body limit");
    body[64] = 'a'; body[65] = '\0';
    ctl_post(api, "/api/example/v1/echo", body, &res);
    check(res.status == 413, "oversized body is rejected, not truncated");

    pending             = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);
    pending_result.body = pending->buf;
    request             = evpl_http_request_create(pending->conn, EVPL_HTTP_REQUEST_TYPE_GET,
                                                   "/api/example/v1/wait");
    evpl_http_client_set_request_length(request, 0);
    evpl_http_request_dispatch(request, ctl_client_notify, &pending_result);
    deadline = ctl_now_ms() + CTL_HANG_MS;
    do {
        ctl_get(api, "/api/example/v1/waiting", &res);
        check(ctl_now_ms() < deadline, "worker never started");
    } while (strcmp(res.body, "waiting"));
    /* Server shutdown disconnects the pending transport before waiting for
     * its external worker. The worker replies through the SDK after failure. */
    struct shutdown_state shutdown = { .server = env.server };
    pthread_t             shutdown_thread;
    atomic_init(&shutdown.done, 0);
    check(!pthread_create(&shutdown_thread, NULL, shutdown_server, &shutdown), "shutdown thread");
    while (!pending_result.done || !atomic_load(&shutdown.done)) {
        evpl_continue(env.evpl);
        check(ctl_now_ms() < deadline, "disconnect completion missing");
    }
    pthread_join(shutdown_thread, NULL);
    ctl_conn_close(pending);
    ctl_conn_close(api);
    evpl_http_destroy(env.agent);
    evpl_destroy(env.evpl);
    prometheus_metrics_destroy(env.registry);
    char cleanup[512];
    snprintf(cleanup, sizeof(cleanup), "rm -rf %s", env.session_dir);
    check(!system(cleanup), "cleanup");
    return 0;

 done:
    ctl_conn_close(api);
    ctl_env_close(&env);
    return 0;
} /* main */
