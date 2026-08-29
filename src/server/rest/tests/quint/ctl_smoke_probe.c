// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Control-plane smoke probe: proves the whole REST API and the Prometheus
 * scrape endpoint are reachable in-process over the inproc transport, with
 * no port bound and no curl.
 *
 * What it pins, in order:
 *
 *   1. The unauthenticated surface answers: GET /version, GET /api/openapi.json.
 *   2. A method the route does not implement is 405, not 404, and an unknown
 *      path is 404.
 *   3. The full admin lifecycle over HTTP -- filesystem, mount, export, share,
 *      bucket, user -- each created, listed, fetched, and deleted, with the
 *      documented conflict/not-found statuses on the edges.
 *   4. GET /api/v1/config reconstructs exactly what those endpoints report.
 *   5. GET /metrics returns a Prometheus exposition carrying both registries
 *      (chimera_* and evpl_*), and a non-GET or wrong URI on that endpoint is
 *      rejected.
 *
 * This is the harness's ground truth: the model-based suite built on top
 * assumes every one of these holds, so a break here is a break in the
 * plumbing rather than in a generated trace.
 */

#include "ctl_mbt_common.h"

#include <jansson.h>

static int passed, failed;

static void
ck(
    int         ok,
    const char *name)
{
    if (ok) {
        fprintf(stderr, "  PASS: %s\n", name);
        passed++;
    } else {
        fprintf(stderr, "  FAIL: %s\n", name);
        failed++;
    }
} /* ck */

/* Status assertion that reports what it actually got. */
static void
ck_status(
    struct ctl_res *res,
    int             want,
    const char     *name)
{
    if (res->status == want) {
        fprintf(stderr, "  PASS: %s (%d)\n", name, res->status);
        passed++;
    } else {
        fprintf(stderr, "  FAIL: %s: expected %d, got %d; body: %.*s\n",
                name, want, res->status,
                res->body_len > 300 ? 300 : res->body_len, res->body);
        failed++;
    }
} /* ck_status */

/* Does the JSON array in res->body contain an object with "name" == name? */
static int
array_has_name(
    struct ctl_res *res,
    const char     *name)
{
    json_t      *root, *elem;
    json_error_t err;
    size_t       i;
    int          found = 0;

    root = json_loadb(res->body, res->body_len, 0, &err);
    if (!root || !json_is_array(root)) {
        json_decref(root);
        return 0;
    }

    json_array_foreach(root, i, elem)
    {
        const char *n = json_string_value(json_object_get(elem, "name"));

        if (n && strcmp(n, name) == 0) {
            found = 1;
        }
    }

    json_decref(root);
    return found;
} /* array_has_name */

static int
json_field_is(
    struct ctl_res *res,
    const char     *field,
    const char     *want)
{
    json_t      *root;
    json_error_t err;
    const char  *got;
    int          ok;

    root = json_loadb(res->body, res->body_len, 0, &err);
    if (!root) {
        return 0;
    }

    got = json_string_value(json_object_get(root, field));
    ok  = got && strcmp(got, want) == 0;

    json_decref(root);
    return ok;
} /* json_field_is */

int
main(
    int   argc,
    char *argv[])
{
    struct ctl_env      env;
    struct ctl_env_opts opts = { 0 };
    struct ctl_conn    *api, *scrape;
    struct ctl_res      res;

    opts.nfs_enabled     = 1;
    opts.smb_enabled     = 1;
    opts.s3_enabled      = 1;
    opts.metrics_enabled = 1;
    /* Auth off: this probe is about the resource endpoints.  The auth
     * surface has its own probe. */
    opts.auth_enabled = 0;

    ctl_env_open(&env, &opts);

    api    = ctl_conn_open(env.evpl, env.agent, CTL_REST_PORT);
    scrape = ctl_conn_open(env.evpl, env.agent, CTL_METRICS_PORT);

    /* ---- 1. unauthenticated surface ----------------------------------- */

    ctl_get(api, "/version", &res);
    ck_status(&res, 200, "version/status");
    ck(res.body_len > 0 && strchr(res.body, '{') != NULL,
       "version/json-body");

    ctl_get(api, "/api/openapi.json", &res);
    ck_status(&res, 200, "openapi/status");

    /* ---- 2. routing edges ---------------------------------------------- */

    ctl_delete(api, "/version", &res);
    ck_status(&res, 405, "version/delete-is-405");

    ctl_get(api, "/api/v1/nonesuch", &res);
    ck_status(&res, 404, "unknown-path-is-404");

    /* The debug fsop endpoint is invisible unless configured on. */
    ctl_post(api, "/api/v1/debug/fsop", "{}", &res);
    ck_status(&res, 404, "debug-fsop-hidden-by-default");

    /* ---- 3. admin lifecycle -------------------------------------------- */

    ctl_post(api, "/api/v1/filesystems",
             "{\"module\":\"memfs\",\"name\":\"fs0\"}", &res);
    ck_status(&res, 201, "fs/create");

    ctl_post(api, "/api/v1/filesystems",
             "{\"module\":\"memfs\",\"name\":\"fs0\"}", &res);
    ck_status(&res, 409, "fs/create-duplicate-is-409");

    ctl_post(api, "/api/v1/filesystems",
             "{\"module\":\"nosuchmodule\",\"name\":\"fs1\"}", &res);
    ck_status(&res, 404, "fs/create-unknown-module-is-404");

    ctl_post(api, "/api/v1/filesystems", "{\"name\":\"fs1\"}", &res);
    ck_status(&res, 400, "fs/create-missing-module-is-400");

    ctl_post(api, "/api/v1/mounts",
             "{\"name\":\"m0\",\"module\":\"memfs\",\"path\":\"fs0\"}", &res);
    ck_status(&res, 201, "mount/create");

    ctl_post(api, "/api/v1/mounts",
             "{\"name\":\"m0\",\"module\":\"memfs\",\"path\":\"fs0\"}", &res);
    ck_status(&res, 409, "mount/create-duplicate-is-409");

    ctl_get(api, "/api/v1/mounts", &res);
    ck_status(&res, 200, "mount/list");
    ck(array_has_name(&res, "m0"), "mount/list-contains-m0");

    ctl_get(api, "/api/v1/mounts/m0", &res);
    ck_status(&res, 200, "mount/get");
    ck(json_field_is(&res, "module", "memfs"), "mount/get-module");

    /* The internal "root" pseudo-mount must not be visible. */
    ctl_get(api, "/api/v1/mounts/root", &res);
    ck_status(&res, 404, "mount/root-pseudo-mount-hidden");

    /* A filesystem with a live mount cannot be removed. */
    ctl_delete(api, "/api/v1/filesystems/memfs/fs0", &res);
    ck_status(&res, 409, "fs/delete-while-mounted-is-409");

    ctl_post(api, "/api/v1/exports",
             "{\"name\":\"e0\",\"path\":\"/m0\",\"access\":\"rw\"}", &res);
    ck_status(&res, 201, "export/create");

    ctl_get(api, "/api/v1/exports/e0", &res);
    ck_status(&res, 200, "export/get");
    ck(json_field_is(&res, "access", "rw"), "export/get-access");

    ctl_post(api, "/api/v1/shares",
             "{\"name\":\"s0\",\"path\":\"/m0\"}", &res);
    ck_status(&res, 201, "share/create");

    ctl_get(api, "/api/v1/shares", &res);
    ck(array_has_name(&res, "s0"), "share/list-contains-s0");

    ctl_post(api, "/api/v1/buckets",
             "{\"name\":\"b0\",\"path\":\"/m0\"}", &res);
    ck_status(&res, 201, "bucket/create");

    ctl_get(api, "/api/v1/buckets/b0", &res);
    ck_status(&res, 200, "bucket/get");

    ctl_post(api, "/api/v1/users",
             "{\"username\":\"alice\",\"uid\":1000,\"gid\":1000,"
             "\"gids\":[1000,2000]}", &res);
    ck_status(&res, 201, "user/create");

    ctl_get(api, "/api/v1/users/alice", &res);
    ck_status(&res, 200, "user/get");

    ctl_get(api, "/api/v1/users/nobodyhere", &res);
    ck_status(&res, 404, "user/get-missing-is-404");

    /* A mount in use by a share, export or bucket is pinned. */
    ctl_delete(api, "/api/v1/mounts/m0", &res);
    ck_status(&res, 409, "mount/delete-while-in-use-is-409");

    /* ---- 4. config reconstruction --------------------------------------- */

    ctl_get(api, "/api/v1/config", &res);
    ck_status(&res, 200, "config/status");
    {
        json_t      *root;
        json_error_t err;
        int          ok;

        root = json_loadb(res.body, res.body_len, 0, &err);
        ok   = root &&
            json_object_get(json_object_get(root, "mounts"), "m0") &&
            json_object_get(json_object_get(root, "exports"), "e0") &&
            json_object_get(json_object_get(root, "shares"), "s0") &&
            json_object_get(json_object_get(root, "buckets"), "b0") &&
            !json_object_get(json_object_get(root, "mounts"), "root");
        ck(ok, "config/reflects-live-state");
        json_decref(root);
    }

    /* ---- teardown, in dependency order ---------------------------------- */

    ctl_delete(api, "/api/v1/buckets/b0", &res);
    ck_status(&res, 204, "bucket/delete");
    ctl_delete(api, "/api/v1/buckets/b0", &res);
    ck_status(&res, 404, "bucket/delete-twice-is-404");

    ctl_delete(api, "/api/v1/shares/s0", &res);
    ck_status(&res, 204, "share/delete");

    ctl_delete(api, "/api/v1/exports/e0", &res);
    ck_status(&res, 204, "export/delete");

    ctl_delete(api, "/api/v1/users/alice", &res);
    ck_status(&res, 204, "user/delete");

    ctl_delete(api, "/api/v1/mounts/m0", &res);
    ck_status(&res, 204, "mount/delete");

    ctl_delete(api, "/api/v1/filesystems/memfs/fs0", &res);
    ck_status(&res, 204, "fs/delete");

    ctl_get(api, "/api/v1/mounts/m0", &res);
    ck_status(&res, 404, "mount/gone-after-delete");

    /* ---- 5. the scrape endpoint ----------------------------------------- */

    ctl_get(scrape, "/metrics", &res);
    ck_status(&res, 200, "metrics/status");
    ck(res.body_len > 0 && strstr(res.body, "chimera_") != NULL,
       "metrics/carries-chimera-registry");
    ck(strstr(res.body, "evpl_") != NULL,
       "metrics/carries-evpl-registry");

    ctl_get(scrape, "/nonesuch", &res);
    ck_status(&res, 400, "metrics/wrong-uri-is-400");

    ctl_post(scrape, "/metrics", NULL, &res);
    ck_status(&res, 400, "metrics/non-get-is-400");

    ctl_conn_close(scrape);
    ctl_conn_close(api);
    ctl_env_close(&env);

    fprintf(stderr, "\n%d passed, %d failed\n", passed, failed);

    return failed ? 1 : 0;
} /* main */
