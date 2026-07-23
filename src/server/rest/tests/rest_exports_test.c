// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * REST API NFS Exports Test
 *
 * Exercises the /api/v1/exports endpoints in rest_shares.c with REST
 * authentication disabled, focusing on the export_id support:
 *   1. Create an export with an explicit export_id returns 201
 *   2. GET the export echoes the export_id back
 *   3. List exports includes the export_id on the matching entry
 *   4. GET /api/v1/config round-trips the export_id
 *   5. A duplicate export_id returns 409 and the export is not created
 *   6. Out-of-range or non-integer export_id returns 400
 *   7. Auto-assignment skips slots pinned by explicit ids
 *   8. Delete frees the id so it can be pinned again
 *   9. Missing required fields return 400; a duplicate name returns 409
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define REST_PORT 18082

static int tests_passed = 0;
static int tests_failed = 0;

static void
test_pass(const char *name)
{
    fprintf(stderr, "  PASS: %s\n", name);
    tests_passed++;
} /* test_pass */

static void
test_fail(const char *name)
{
    fprintf(stderr, "  FAIL: %s\n", name);
    tests_failed++;
} /* test_fail */

/* Issue a request and return only the HTTP status code. */
static int
curl_get_code(
    const char *method,
    const char *path,
    const char *body,
    long       *http_code)
{
    char  cmd[8192];
    char  output[4096];
    FILE *fp;
    int   rc;

    if (body) {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -o /dev/null -w '%%{http_code}' "
                 "-X %s -H 'Content-Type: application/json' "
                 "-d '%s' http://localhost:%d%s 2>&1",
                 method, body, REST_PORT, path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -o /dev/null -w '%%{http_code}' "
                 "-X %s http://localhost:%d%s 2>&1",
                 method, REST_PORT, path);
    }

    fp = popen(cmd, "r");
    if (!fp) {
        return -1;
    }

    output[0] = '\0';
    if (fgets(output, sizeof(output), fp) == NULL) {
        output[0] = '\0';
    }

    rc = pclose(fp);

    if (WIFEXITED(rc) && WEXITSTATUS(rc) == 0) {
        *http_code = strtol(output, NULL, 10);
        return 0;
    }

    return -1;
} /* curl_get_code */

/* Issue a request and capture both the response body and the status code. */
static int
curl_get_body(
    const char *method,
    const char *path,
    const char *body,
    char       *response,
    int         response_size,
    long       *http_code)
{
    char  cmd[8192];
    char  output[8192];
    FILE *fp;
    int   rc;

    if (body) {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -w '\\n%%{http_code}' "
                 "-X %s -H 'Content-Type: application/json' "
                 "-d '%s' http://localhost:%d%s 2>&1",
                 method, body, REST_PORT, path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -w '\\n%%{http_code}' "
                 "-X %s http://localhost:%d%s 2>&1",
                 method, REST_PORT, path);
    }

    fp = popen(cmd, "r");
    if (!fp) {
        return -1;
    }

    output[0] = '\0';
    {
        int total = 0;
        while (fgets(output + total, sizeof(output) - total, fp) != NULL) {
            total += strlen(output + total);
        }
    }

    rc = pclose(fp);

    if (!WIFEXITED(rc) || WEXITSTATUS(rc) != 0) {
        return -1;
    }

    /* Last line is the HTTP code */
    {
        char *last_newline = strrchr(output, '\n');
        if (last_newline && last_newline > output) {
            *http_code    = strtol(last_newline + 1, NULL, 10);
            *last_newline = '\0';
        } else {
            *http_code = 0;
        }
    }

    snprintf(response, response_size, "%s", output);

    return 0;
} /* curl_get_body */

/* Assert that a request returns the expected status code. */
static void
check_code(
    const char *name,
    const char *method,
    const char *path,
    const char *body,
    long        expect,
    int        *failures)
{
    long http_code = 0;
    int  rc        = curl_get_code(method, path, body, &http_code);

    if (rc == 0 && http_code == expect) {
        test_pass(name);
    } else {
        test_fail(name);
        fprintf(stderr, "    Expected %ld, got %ld\n", expect, http_code);
        (*failures)++;
    }
} /* check_code */

/* Assert that a request returns the expected status code and that its body
 * does or does not contain a given substring. */
static void
check_body_contains(
    const char *name,
    const char *method,
    const char *path,
    const char *body,
    long        expect_code,
    const char *needle,
    int         want_present,
    int        *failures)
{
    char response[8192];
    long http_code = 0;
    int  rc        = curl_get_body(method, path, body, response,
                                   sizeof(response), &http_code);
    int  present;

    if (rc != 0 || http_code != expect_code) {
        test_fail(name);
        fprintf(stderr, "    Expected %ld, got %ld (rc %d)\n",
                expect_code, http_code, rc);
        (*failures)++;
        return;
    }

    present = (needle && strstr(response, needle) != NULL);
    if (present == want_present) {
        test_pass(name);
    } else {
        test_fail(name);
        fprintf(stderr, "    %s substring \"%s\" in: %s\n",
                want_present ? "missing" : "unexpected", needle, response);
        (*failures)++;
    }
} /* check_body_contains */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_server        *server;
    struct chimera_server_config *config;
    struct prometheus_metrics    *metrics;
    int                           failures = 0;

    (void) argc;
    (void) argv;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "REST API NFS Exports Test\n");
    fprintf(stderr, "========================================\n");

    if (system("which curl >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nERROR: curl not found in PATH\n");
        return EXIT_FAILURE;
    }

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!metrics) {
        fprintf(stderr, "Failed to create metrics\n");
        return EXIT_FAILURE;
    }

    config = chimera_server_config_init();
    chimera_server_config_set_rest_http_port(config, REST_PORT);
    /* Disable auth so the export endpoints can be exercised directly,
     * mirroring the admin pytest setup. */
    chimera_server_config_set_rest_auth_enabled(config, 0);

    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }

    chimera_server_mount(server, "share", "memfs", "/", NULL);

    chimera_server_start(server);
    fprintf(stderr, "Server started (REST on port %d)\n", REST_PORT);
    usleep(200000);

    /* ===== Test 1: Create an export with an explicit export_id ===== */
    fprintf(stderr, "\n  Test: Create export with explicit export_id...\n");
    check_code("POST /api/v1/exports with export_id returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"exp1\",\"path\":\"/share\",\"export_id\":42}",
               201, &failures);

    /* ===== Test 2: GET echoes the export_id back ===== */
    fprintf(stderr, "\n  Test: export_id is echoed on read...\n");
    check_body_contains("GET /api/v1/exports/exp1 echoes export_id",
                        "GET", "/api/v1/exports/exp1", NULL,
                        200, "\"export_id\":42", 1, &failures);

    /* ===== Test 3: List includes the export_id ===== */
    check_body_contains("GET /api/v1/exports lists export_id",
                        "GET", "/api/v1/exports", NULL,
                        200, "\"export_id\":42", 1, &failures);

    /* ===== Test 4: Config round-trips the export_id ===== */
    check_body_contains("GET /api/v1/config round-trips export_id",
                        "GET", "/api/v1/config", NULL,
                        200, "\"export_id\":42", 1, &failures);

    /* ===== Test 5: Duplicate export_id is rejected with 409 ===== */
    fprintf(stderr, "\n  Test: Duplicate export_id rejected with 409...\n");
    check_body_contains("Duplicate export_id returns 409",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"exp2\",\"path\":\"/share\",\"export_id\":42}",
                        409, "export_id already in use", 1, &failures);

    check_code("Rejected export exp2 does not exist (404)",
               "GET", "/api/v1/exports/exp2", NULL, 404, &failures);

    /* ===== Test 6: Out-of-range / non-integer export_id ===== */
    fprintf(stderr, "\n  Test: Invalid export_id rejected with 400...\n");
    check_code("export_id 0 returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"bad1\",\"path\":\"/share\",\"export_id\":0}",
               400, &failures);

    check_code("export_id 4096 returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"bad2\",\"path\":\"/share\",\"export_id\":4096}",
               400, &failures);

    check_code("Negative export_id returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"bad3\",\"path\":\"/share\",\"export_id\":-1}",
               400, &failures);

    check_code("Non-integer export_id returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"bad4\",\"path\":\"/share\",\"export_id\":\"abc\"}",
               400, &failures);

    check_code("Rejected export bad1 does not exist (404)",
               "GET", "/api/v1/exports/bad1", NULL, 404, &failures);

    check_code("Rejected export bad2 does not exist (404)",
               "GET", "/api/v1/exports/bad2", NULL, 404, &failures);

    check_code("Rejected export bad3 does not exist (404)",
               "GET", "/api/v1/exports/bad3", NULL, 404, &failures);

    check_code("Rejected export bad4 does not exist (404)",
               "GET", "/api/v1/exports/bad4", NULL, 404, &failures);

    /* ===== Test 7: Auto-assignment skips explicit ids =====
     * On this fresh server the auto counter is at 1.  Pin id 1, then an auto
     * export must skip to 2; pin 3, and the next auto export must skip over
     * it to 4. */
    fprintf(stderr, "\n  Test: Auto-assignment skips explicit ids...\n");
    check_code("POST export pinned to id 1 returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expa\",\"path\":\"/share\",\"export_id\":1}",
               201, &failures);

    check_code("POST auto-assigned export returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expb\",\"path\":\"/share\"}",
               201, &failures);

    check_body_contains("Auto export skipped pinned id 1, got 2",
                        "GET", "/api/v1/exports/expb", NULL,
                        200, "\"export_id\":2", 1, &failures);

    check_code("POST export pinned to id 3 returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expc\",\"path\":\"/share\",\"export_id\":3}",
               201, &failures);

    check_code("POST second auto-assigned export returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expd\",\"path\":\"/share\"}",
               201, &failures);

    check_body_contains("Auto export skipped pinned id 3, got 4",
                        "GET", "/api/v1/exports/expd", NULL,
                        200, "\"export_id\":4", 1, &failures);

    /* ===== Test 8: Delete frees the id for reuse ===== */
    fprintf(stderr, "\n  Test: Delete frees the export_id...\n");
    check_code("DELETE /api/v1/exports/exp1 returns 204",
               "DELETE", "/api/v1/exports/exp1", NULL, 204, &failures);

    check_code("GET deleted export returns 404",
               "GET", "/api/v1/exports/exp1", NULL, 404, &failures);

    check_code("Freed export_id can be pinned again",
               "POST", "/api/v1/exports",
               "{\"name\":\"exp1b\",\"path\":\"/share\",\"export_id\":42}",
               201, &failures);

    /* ===== Test 9: Missing fields / duplicate name regressions ===== */
    fprintf(stderr, "\n  Test: Bad requests and conflicts...\n");
    check_code("Missing path returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"nopath\"}",
               400, &failures);

    check_code("Duplicate export name returns 409",
               "POST", "/api/v1/exports",
               "{\"name\":\"exp1b\",\"path\":\"/share\"}",
               409, &failures);

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Test Summary\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "Passed: %d\n", tests_passed);
    fprintf(stderr, "Failed: %d\n", tests_failed);

    chimera_server_destroy(server);
    prometheus_metrics_destroy(metrics);

    if (failures > 0) {
        fprintf(stderr, "\nSome tests FAILED\n\n");
        return EXIT_FAILURE;
    }

    fprintf(stderr, "\nAll tests PASSED\n\n");
    return EXIT_SUCCESS;
} /* main */
