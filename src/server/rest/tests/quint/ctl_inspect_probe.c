// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "ctl_mbt_common.h"
#include <jansson.h>

static void
check(
    int         condition,
    const char *name)
{
    if (!condition) {
        fprintf(stderr, "inspection: %s\n", name); abort();
    }
} /* check */
static json_t *
get(
    struct ctl_conn *api,
    const char      *path,
    int              expected)
{
    struct ctl_res res;
    /* A previous response may reach the client before the server's terminal
     * callback. Admission intentionally covers that interval. */
    uint64_t       until = ctl_now_ms() + 3000;

    do {
        ctl_get(api, path, &res);
    } while (res.status == 429 && ctl_now_ms() < until);
    if (res.status != expected) {
        fprintf(stderr, "%s: %d %s\n", path, res.status, res.body); abort();
    }
    json_t        *obj = json_loadb(res.body, res.body_len, 0, NULL);
    check(obj != NULL, "valid JSON");
    return obj;
} /* get */
int
main(void)
{
    struct ctl_env      env;
    struct ctl_env_opts opts = { .nfs_enabled = 1, .smb_enabled = 1, .s3_enabled = 1 };

    ctl_env_open(&env, &opts);
    struct ctl_conn    *api      = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);
    json_t             *snapshot = get(api, "/api/v1/threads", 200);
    check(json_is_true(json_object_get(snapshot, "complete")), "complete idle snapshot");
    json_t             *threads = json_object_get(snapshot, "threads");
    check(json_array_size(threads) == 8, "all eight idle workers");
    for (unsigned int i = 0; i < 8; i++) {
        json_t *thread = json_array_get(threads, i);
        check(json_integer_value(json_object_get(thread, "worker_id")) == i, "pool index preserved");
        check(json_is_string(json_object_get(thread, "os_tid")), "native TID string");
        check(json_is_string(json_object_get(thread, "sampled_monotonic_ns")), "monotonic timestamp string");
        check(json_is_object(json_object_get(thread, "rpc")), "RPC metrics copied");
    }
    char           instance[64];
    snprintf(instance, sizeof(instance), "%s", json_string_value(json_object_get(snapshot, "server_instance_id")));
    json_decref(snapshot);
    const char    *bad[] = {
        "/api/v1/threads?service=nfs",             "/api/v1/threads?worker_id=-1",
        "/api/v1/threads?worker_id=8",
        "/api/v1/connections?remote_ip=localhost", "/api/v1/connections?remote_port=65536",
        "/api/v1/connections?service=invalid",     "/api/v1/connections?worker_id=0&worker_id=1",
        "/api/v1/connections?foo=x",               "/api/v1/connections?local_ip=%00",
        "/api/v1/connections?transport=tcp&",
        "/api/v1/connections?service=",            "/api/v1/connections?remote_port=9999999999999999999"
    };
    for (unsigned int i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        json_decref(get(api, bad[i], 400));
    }
    json_decref(get(api, "/api/v1/connections?local_ip=2001%3Adb8%3A%3A1&transport=tcp", 200));
    struct ctl_res res;
    ctl_delete(api, "/api/v1/threads", &res);
    check(res.status == 405, "GET only");
    snapshot = get(api, "/api/v1/connections", 200);
    check(json_array_size(json_object_get(snapshot, "connections")) == 0, "management excluded");
    json_decref(snapshot);

    /* Accepted S3 HTTP connections are visible, while REST connections on the
     * other agent never leak into that list. Every endpoint keeps its family. */
    struct ctl_conn *s3[8];
    for (int i = 0; i < 8; i++) {
        s3[i] = ctl_conn_open(env.evpl, env.agent, 5000);
        ctl_get(s3[i], "/", &res);
    }
    snapshot = get(api, "/api/v1/connections?service=s3", 200);
    json_t          *rows = json_object_get(snapshot, "connections");
    check(json_array_size(rows) == 8, "eight accepted S3 connections");
    unsigned int     owners = 0;
    char             previous_id[64];
    for (unsigned int i = 0; i < 8; i++) {
        json_t *row    = json_array_get(rows, i);
        int     worker = json_integer_value(json_object_get(row, "worker_id"));
        owners |= 1U << worker;
        check(!strcmp(json_string_value(json_object_get(json_object_get(row, "local"), "family")), "inproc"),
              "inproc family");
        check(json_is_null(json_object_get(json_object_get(row, "local"), "port")), "inproc has no IP port");
        check(!strcmp(json_string_value(json_object_get(row, "http_framing")), "h1"), "HTTP framing");
    }
    check(owners == 255, "listener rotor gives eight owners");
    snprintf(previous_id, sizeof(previous_id), "%s", json_string_value(json_object_get(json_array_get(rows, 0),
                                                                                       "connection_id")));
    json_decref(snapshot);
    snapshot = get(api, "/api/v1/threads", 200);
    threads  = json_object_get(snapshot, "threads");
    for (int i = 0; i < 8; i++) {
        json_t *counts = json_object_get(json_array_get(threads, i), "connection_counts");
        check(!strcmp(json_string_value(json_object_get(counts, "s3")), "1"), "exact lifecycle count");
    }
    json_decref(snapshot);
    for (int i = 0; i < 8; i++) {
        ctl_conn_close(s3[i]);
    }
    uint64_t until = ctl_now_ms() + 3000;
    do {
        snapshot = get(api, "/api/v1/connections?service=s3", 200);
        unsigned int count = json_array_size(json_object_get(snapshot, "connections"));
        json_decref(snapshot);
        if (!count) {
            break;
        }
        check(ctl_now_ms() < until, "disconnects retire rows");
    } while (1);
    for (int i = 0; i < 8; i++) {
        s3[i] = ctl_conn_open(env.evpl, env.agent, 5000); ctl_get(s3[i], "/", &res);
    }
    snapshot = get(api, "/api/v1/connections?service=s3", 200);
    rows     = json_object_get(snapshot, "connections");
    for (int i = 0; i < 8; i++) {
        check(strcmp(previous_id, json_string_value(json_object_get(json_array_get(rows, i), "connection_id"))) != 0,
              "fresh bind identity on reconnect");
    }
    json_decref(snapshot);
    for (int i = 0; i < 8; i++) {
        ctl_conn_close(s3[i]);
    }
    snapshot = get(api, "/api/v1/threads?worker_id=3", 200);
    check(json_array_size(json_object_get(snapshot, "threads")) == 1, "worker filter");
    check(!strcmp(instance, json_string_value(json_object_get(snapshot, "server_instance_id"))),
          "stable instance identity");
    json_decref(snapshot);
    for (int i = 0; i < 32; i++) {
        struct ctl_conn          *abandoned = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);
        struct evpl_http_request *request   = evpl_http_request_create(abandoned->conn,
                                                                       EVPL_HTTP_REQUEST_TYPE_GET,
                                                                       "/api/v1/connections");
        evpl_http_client_set_request_length(request, 0);
        evpl_http_request_dispatch(request, NULL, NULL);
        evpl_continue(env.evpl);
        ctl_conn_close(abandoned);
    }
    json_decref(get(api, "/api/v1/threads", 200));
    ctl_conn_close(api);
    ctl_env_close(&env);
    opts.auth_enabled = 1;
    ctl_env_open(&env, &opts);
    api = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);
    json_decref(get(api, "/api/v1/threads", 401));
    json_decref(get(api, "/api/v1/connections?service=nfs", 401));
    ctl_conn_close(api);
    ctl_env_close(&env);

    return 0;
} /* main */
