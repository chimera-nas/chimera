// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * REST API NFS Exports Test
 *
 * Exercises the /api/v1/exports endpoints in rest_exports.c with REST
 * authentication disabled, focusing on the export_id support:
 *   1. Create an export with an explicit export_id returns 201
 *   2. GET the export echoes the export_id back
 *   3. List exports includes the export_id on the matching entry
 *   4. GET /api/v1/config round-trips the export_id
 *   5. A duplicate export_id returns 409 and the export is not created
 *   6. Out-of-range or non-integer export_id returns 400; the id space
 *      boundary (65535) is accepted
 *   7. Auto-assignment skips slots pinned by explicit ids
 *   8. Delete frees the id so it can be pinned again
 *   9. Missing required fields return 400; a duplicate name returns 409
 *  10. The access mode round-trips; the legacy "options" key and invalid
 *      access/squash/anonuid/anongid values are rejected with 400 rather
 *      than silently ignored; squash aliases are accepted
 *  11. A sec restriction round-trips; no/empty restriction omits the field;
 *      malformed sec shapes are rejected with 400
 *  12. Creating past the configured nfs_max_exports cap returns 409 and a
 *      delete frees a slot under the cap again
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define REST_PORT        18082

/* Concurrent-export cap configured for this test (nfs_max_exports). */
#define TEST_MAX_EXPORTS 8

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

/* Return the number of live exports per GET /api/v1/exports, or -1 on any
 * request/parse failure.  Used so the count-cap test fills from the actual
 * live count instead of hard-coding how many exports earlier tests left. */
static long
count_exports(void)
{
    char    response[8192];
    long    http_code = 0;
    json_t *array;
    long    count;

    if (curl_get_body("GET", "/api/v1/exports", NULL, response,
                      sizeof(response), &http_code) != 0 ||
        http_code != 200) {
        return -1;
    }

    array = json_loads(response, 0, NULL);
    if (!array || !json_is_array(array)) {
        json_decref(array);
        return -1;
    }

    count = (long) json_array_size(array);
    json_decref(array);

    return count;
} /* count_exports */

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
    /* Small concurrent-export cap so the nfs_max_exports enforcement can be
     * exercised without thousands of creates; test 11 queries the live
     * export count and fills the remaining slots up to this cap. */
    chimera_server_config_set_nfs_max_exports(config, TEST_MAX_EXPORTS);

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
                        200, "\"export_id\":42,", 1, &failures);

    /* ===== Test 3: List includes the export_id ===== */
    check_body_contains("GET /api/v1/exports lists export_id",
                        "GET", "/api/v1/exports", NULL,
                        200, "\"export_id\":42,", 1, &failures);

    /* ===== Test 4: Config round-trips the export_id ===== */
    check_body_contains("GET /api/v1/config round-trips export_id",
                        "GET", "/api/v1/config", NULL,
                        200, "\"export_id\":42,", 1, &failures);

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

    check_code("export_id 65536 returns 400",
               "POST", "/api/v1/exports",
               "{\"name\":\"bad2\",\"path\":\"/share\",\"export_id\":65536}",
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

    /* The top of the id space is valid: 65535 is the largest id that fits the
     * 16-bit wire file-handle field. */
    check_code("export_id 65535 (id space boundary) returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expmax\",\"path\":\"/share\",\"export_id\":65535}",
               201, &failures);

    check_body_contains("GET /api/v1/exports/expmax echoes export_id",
                        "GET", "/api/v1/exports/expmax", NULL,
                        200, "\"export_id\":65535,", 1, &failures);

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
                        200, "\"export_id\":2,", 1, &failures);

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
                        200, "\"export_id\":4,", 1, &failures);

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

    /* The name-conflict message distinguishes this 409 from a duplicate
     * export_id (the uniqueness check runs inside create, under the exports
     * lock, so it cannot be raced by a concurrent create of the same name). */
    check_body_contains("Duplicate export name returns 409",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"exp1b\",\"path\":\"/share\"}",
                        409, "already exists", 1, &failures);

    /* ===== Test 10: access mode round-trip; legacy "options" rejected ===== */
    fprintf(stderr, "\n  Test: access mode round-trip and legacy key...\n");
    check_code("POST export with access=ro returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expro\",\"path\":\"/share\",\"access\":\"ro\"}",
               201, &failures);

    check_body_contains("GET echoes access=ro",
                        "GET", "/api/v1/exports/expro", NULL,
                        200, "\"access\":\"ro\"", 1, &failures);

    check_body_contains("Legacy \"options\" key returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expleg\",\"path\":\"/share\",\"options\":\"ro\"}",
                        400, "has been renamed", 1, &failures);

    check_code("Rejected legacy export does not exist (404)",
               "GET", "/api/v1/exports/expleg", NULL, 404, &failures);

    check_code("DELETE access round-trip export returns 204",
               "DELETE", "/api/v1/exports/expro", NULL, 204, &failures);

    /* Unrecognized or mistyped export option values must be rejected, not
     * silently replaced with the (more permissive) defaults. */
    check_body_contains("Invalid access value returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"access\":\"readonly\"}",
                        400, "access must be", 1, &failures);

    check_body_contains("Non-string access value returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"access\":1}",
                        400, "access must be", 1, &failures);

    check_body_contains("Invalid squash value returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"squash\":\"rootsquash\"}",
                        400, "squash must be", 1, &failures);

    check_body_contains("String anonuid returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"anonuid\":\"1000\"}",
                        400, "anonuid must be", 1, &failures);

    check_body_contains("Negative anonuid returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"anonuid\":-1}",
                        400, "anonuid must be", 1, &failures);

    check_body_contains("Out-of-range anongid returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"anongid\":4294967296}",
                        400, "anongid must be", 1, &failures);

    check_code("Rejected export expbad does not exist (404)",
               "GET", "/api/v1/exports/expbad", NULL, 404, &failures);

    /* Squash aliases parse to the canonical value. */
    check_code("POST export with squash=root_squash returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expsq\",\"path\":\"/share\",\"squash\":\"root_squash\",\"anonuid\":1000}",
               201, &failures);

    check_body_contains("GET echoes canonical squash=root",
                        "GET", "/api/v1/exports/expsq", NULL,
                        200, "\"squash\":\"root\"", 1, &failures);

    check_body_contains("GET echoes anonuid=1000",
                        "GET", "/api/v1/exports/expsq", NULL,
                        200, "\"anonuid\":1000,", 1, &failures);

    check_code("DELETE squash alias export returns 204",
               "DELETE", "/api/v1/exports/expsq", NULL, 204, &failures);

    /* ===== Test 11: sec restriction round-trip and validation ===== */
    fprintf(stderr, "\n  Test: sec restriction round-trip...\n");
    check_code("POST export with sec returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expsec\",\"path\":\"/share\",\"sec\":[\"krb5\",\"krb5i\"]}",
               201, &failures);

    check_body_contains("GET echoes the sec array",
                        "GET", "/api/v1/exports/expsec", NULL,
                        200, "\"sec\":[\"krb5\",\"krb5i\"]", 1, &failures);

    /* No restriction (or an empty one) means any flavor: the field must be
    * absent so a captured config round-trips the config-file semantics. */
    check_body_contains("Export without a restriction omits sec",
                        "GET", "/api/v1/exports/expa", NULL,
                        200, "\"sec\"", 0, &failures);

    check_code("POST export with empty sec returns 201",
               "POST", "/api/v1/exports",
               "{\"name\":\"expsece\",\"path\":\"/share\",\"sec\":[]}",
               201, &failures);

    check_body_contains("Empty sec restriction omits the field",
                        "GET", "/api/v1/exports/expsece", NULL,
                        200, "\"sec\"", 0, &failures);

    /* Malformed sec shapes are rejected, not silently widened to "any". */
    check_body_contains("Unknown sec flavor returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"sec\":[\"krb5x\"]}",
                        400, "sec flavors must be", 1, &failures);

    check_body_contains("Non-array sec returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"sec\":\"krb5\"}",
                        400, "sec must be an array", 1, &failures);

    check_body_contains("Non-string sec entry returns 400",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"expbad\",\"path\":\"/share\",\"sec\":[5]}",
                        400, "sec flavors must be", 1, &failures);

    check_code("Rejected sec export expbad does not exist (404)",
               "GET", "/api/v1/exports/expbad", NULL, 404, &failures);

    check_code("DELETE sec export returns 204",
               "DELETE", "/api/v1/exports/expsec", NULL, 204, &failures);

    check_code("DELETE empty-sec export returns 204",
               "DELETE", "/api/v1/exports/expsece", NULL, 204, &failures);

    /* ===== Test 12: nfs_max_exports count cap =====
     * Fill from the live export count (queried, so inserting a create in an
     * earlier test cannot break the arithmetic here) up to the configured
     * cap of 8; the next create is rejected, and deleting one frees a slot
     * under the cap again. */
    fprintf(stderr, "\n  Test: nfs_max_exports cap enforcement...\n");
    {
        long live = count_exports();

        if (live < 1 || live >= TEST_MAX_EXPORTS) {
            test_fail("Live export count within cap before fill");
            fprintf(stderr, "    Expected 1..%d live exports, found %ld\n",
                    TEST_MAX_EXPORTS - 1, live);
            failures++;
        } else {
            test_pass("Live export count within cap before fill");
        }

        for (long i = live; i < TEST_MAX_EXPORTS; i++) {
            char label[64];
            char fill_body[128];

            snprintf(label, sizeof(label),
                     "Create below the cap returns 201 (%ld/%d)",
                     i + 1, TEST_MAX_EXPORTS);
            snprintf(fill_body, sizeof(fill_body),
                     "{\"name\":\"fill%ld\",\"path\":\"/share\"}", i);
            check_code(label, "POST", "/api/v1/exports", fill_body,
                       201, &failures);
        }
    }

    check_body_contains("Create past the cap returns 409",
                        "POST", "/api/v1/exports",
                        "{\"name\":\"fillover\",\"path\":\"/share\"}",
                        409, "Export limit reached", 1, &failures);

    check_code("Rejected export fillover does not exist (404)",
               "GET", "/api/v1/exports/fillover", NULL, 404, &failures);

    check_code("DELETE frees a slot under the cap",
               "DELETE", "/api/v1/exports/exp1b", NULL, 204, &failures);

    check_code("Create succeeds again after delete",
               "POST", "/api/v1/exports",
               "{\"name\":\"fillafter\",\"path\":\"/share\"}",
               201, &failures);

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
