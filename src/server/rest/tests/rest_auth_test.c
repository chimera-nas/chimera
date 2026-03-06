// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * REST API Authentication Test
 *
 * Tests JWT authentication for the REST API:
 *   1. Protected endpoints return 401 without token
 *   2. Login with bad credentials returns 401
 *   3. Login with valid credentials returns 200 + token
 *   4. Protected endpoints succeed with valid Bearer token
 *   5. Public endpoints work without token
 *   6. Invalid/garbage token returns 401
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define REST_PORT  18081
#define ADMIN_USER "admin"
#define ADMIN_PASS "adminpass"
#define ADMIN_HASH \
        "$6$testsalt$eBXKG..hXMuMyU2qJeRwFHrphEZTnovHazyD.YLjz/QKAbAvZj7z8MGdfCgwsM3n3k6pWpuGnuW/58UHKaWzL0"

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

static int
curl_get_code(
    const char *method,
    const char *path,
    const char *body,
    const char *bearer_token,
    long       *http_code)
{
    char  cmd[8192];
    char  output[4096];
    FILE *fp;
    int   rc;
    char  auth_header[4200];

    if (bearer_token && bearer_token[0]) {
        snprintf(auth_header, sizeof(auth_header),
                 "-H 'Authorization: Bearer %s' ", bearer_token);
    } else {
        auth_header[0] = '\0';
    }

    if (body) {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -o /dev/null -w '%%{http_code}' "
                 "-X %s -H 'Content-Type: application/json' "
                 "%s"
                 "-d '%s' http://localhost:%d%s 2>&1",
                 method, auth_header, body, REST_PORT, path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -o /dev/null -w '%%{http_code}' "
                 "-X %s %s"
                 "http://localhost:%d%s 2>&1",
                 method, auth_header, REST_PORT, path);
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

static int
curl_get_body(
    const char *method,
    const char *path,
    const char *body,
    const char *bearer_token,
    char       *response,
    int         response_size,
    long       *http_code)
{
    char  cmd[8192];
    char  output[8192];
    FILE *fp;
    int   rc;
    char  auth_header[4200];

    if (bearer_token && bearer_token[0]) {
        snprintf(auth_header, sizeof(auth_header),
                 "-H 'Authorization: Bearer %s' ", bearer_token);
    } else {
        auth_header[0] = '\0';
    }

    if (body) {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -w '\\n%%{http_code}' "
                 "-X %s -H 'Content-Type: application/json' "
                 "%s"
                 "-d '%s' http://localhost:%d%s 2>&1",
                 method, auth_header, body, REST_PORT, path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -s -w '\\n%%{http_code}' "
                 "-X %s %s"
                 "http://localhost:%d%s 2>&1",
                 method, auth_header, REST_PORT, path);
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

static int
extract_token(
    const char *response,
    char       *token,
    int         token_size)
{
    const char *start, *end;

    start = strstr(response, "\"token\":\"");
    if (!start) {
        return -1;
    }
    start += 9;

    end = strchr(start, '"');
    if (!end || end - start >= token_size) {
        return -1;
    }

    memcpy(token, start, end - start);
    token[end - start] = '\0';
    return 0;
} /* extract_token */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_server        *server;
    struct chimera_server_config *config;
    struct prometheus_metrics    *metrics;
    int                           failures  = 0;
    long                          http_code = 0;
    int                           rc;
    char                          response[8192];
    char                          token[4096];

    (void) argc;
    (void) argv;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "REST API Authentication Test\n");
    fprintf(stderr, "========================================\n");

    /* Check prerequisites */
    if (system("which curl >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nERROR: curl not found in PATH\n");
        return EXIT_FAILURE;
    }

    /* Initialize logging */
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!metrics) {
        fprintf(stderr, "Failed to create metrics\n");
        return EXIT_FAILURE;
    }

    config = chimera_server_config_init();
    chimera_server_config_set_rest_http_port(config, REST_PORT);

    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }

    chimera_server_mount(server, "share", "memfs", "/");

    /* Add admin user with known password hash */
    chimera_server_add_user(server, ADMIN_USER, ADMIN_HASH, NULL,
                            NULL, 0, 0, 0, NULL, 1);

    chimera_server_start(server);
    fprintf(stderr, "Server started (REST on port %d)\n", REST_PORT);
    usleep(200000);

    /* ===== Test 1: Protected endpoints return 401 without token ===== */
    fprintf(stderr, "\n  Test: Protected endpoints require auth...\n");
    rc = curl_get_code("GET", "/api/v1/users", NULL, NULL, &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("GET /api/v1/users without token returns 401");
    } else {
        test_fail("GET /api/v1/users without token should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/v1/shares", NULL, NULL, &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("GET /api/v1/shares without token returns 401");
    } else {
        test_fail("GET /api/v1/shares without token should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    /* ===== Test 2: Login with bad credentials returns 401 ===== */
    fprintf(stderr, "\n  Test: Bad credentials rejected...\n");
    rc = curl_get_code("POST", "/api/v1/auth/login",
                       "{\"username\":\"admin\",\"password\":\"wrong\"}",
                       NULL, &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("Login with bad password returns 401");
    } else {
        test_fail("Login with bad password should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("POST", "/api/v1/auth/login",
                       "{\"username\":\"nouser\",\"password\":\"nopass\"}",
                       NULL, &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("Login with unknown user returns 401");
    } else {
        test_fail("Login with unknown user should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    /* ===== Test 3: Login with valid credentials returns 200 + token ===== */
    fprintf(stderr, "\n  Test: Valid login returns token...\n");
    {
        char login_body[256];
        snprintf(login_body, sizeof(login_body),
                 "{\"username\":\"%s\",\"password\":\"%s\"}",
                 ADMIN_USER, ADMIN_PASS);

        rc = curl_get_body("POST", "/api/v1/auth/login",
                           login_body, NULL,
                           response, sizeof(response), &http_code);
        if (rc == 0 && http_code == 200) {
            test_pass("Login returns 200");
        } else {
            test_fail("Login should return 200");
            fprintf(stderr, "    Got: %ld\n", http_code);
            failures++;
            goto done;
        }

        if (extract_token(response, token, sizeof(token)) == 0) {
            test_pass("Response contains token");
        } else {
            test_fail("Response should contain token");
            fprintf(stderr, "    Response: %s\n", response);
            failures++;
            goto done;
        }
    }

    /* ===== Test 4: Protected endpoints succeed with valid Bearer token === */
    fprintf(stderr, "\n  Test: Authenticated requests succeed...\n");
    rc = curl_get_code("GET", "/api/v1/users", NULL, token, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("GET /api/v1/users with token returns 200");
    } else {
        test_fail("GET /api/v1/users with token should return 200");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/v1/shares", NULL, token, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("GET /api/v1/shares with token returns 200");
    } else {
        test_fail("GET /api/v1/shares with token should return 200");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/v1/exports", NULL, token, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("GET /api/v1/exports with token returns 200");
    } else {
        test_fail("GET /api/v1/exports with token should return 200");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/v1/buckets", NULL, token, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("GET /api/v1/buckets with token returns 200");
    } else {
        test_fail("GET /api/v1/buckets with token should return 200");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    /* ===== Test 5: Public endpoints work without token ===== */
    fprintf(stderr, "\n  Test: Public endpoints don't require auth...\n");
    rc = curl_get_code("GET", "/version", NULL, NULL, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("/version accessible without token");
    } else {
        test_fail("/version should be accessible without token");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/openapi.json", NULL, NULL, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("/api/openapi.json accessible without token");
    } else {
        test_fail("/api/openapi.json should be accessible without token");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/docs", NULL, NULL, &http_code);
    if (rc == 0 && http_code == 200) {
        test_pass("/api/docs accessible without token");
    } else {
        test_fail("/api/docs should be accessible without token");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    /* ===== Test 6: Invalid/garbage token returns 401 ===== */
    fprintf(stderr, "\n  Test: Invalid tokens rejected...\n");
    rc = curl_get_code("GET", "/api/v1/users", NULL,
                       "garbage.token.here", &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("Garbage token returns 401");
    } else {
        test_fail("Garbage token should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

    rc = curl_get_code("GET", "/api/v1/users", NULL,
                       "not-a-jwt", &http_code);
    if (rc == 0 && http_code == 401) {
        test_pass("Non-JWT string returns 401");
    } else {
        test_fail("Non-JWT string should return 401");
        fprintf(stderr, "    Got: %ld\n", http_code);
        failures++;
    }

 done:
    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Test Summary\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "Passed: %d\n", tests_passed);
    fprintf(stderr, "Failed: %d\n", tests_failed);

    if (failures > 0) {
        fprintf(stderr, "\nSome tests FAILED\n\n");
        chimera_server_destroy(server);
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "\nAll tests PASSED\n\n");
    chimera_server_destroy(server);
    prometheus_metrics_destroy(metrics);
    return EXIT_SUCCESS;
} /* main */
