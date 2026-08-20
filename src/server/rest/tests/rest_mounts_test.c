// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * REST API VFS Mounts Test
 *
 * Exercises the /api/v1/mounts endpoints in rest_mounts.c with REST
 * authentication disabled, focusing on the mount options support:
 *   1. Create a mount with options returns 201
 *   2. GET the mount echoes the options back
 *   3. List mounts includes the options on the matching entry
 *   4. GET /api/v1/config round-trips the options
 *   5. A malformed options string returns 400 with a descriptive message
 *      (empty key / too many options)
 *   6. A mount created without options omits the "options" key on read
 *   7. Missing required fields return 400; a duplicate name returns 409
 *   8. Delete removes the mount (204) and it is afterwards 404
 *   9. /api/v1/filesystems lifecycle: create a second filesystem (201),
 *      duplicate create (409), mount it, delete while mounted (409),
 *      unmount, delete (204)
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define REST_PORT 18081

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

/* Assert that a POST returns the expected status code. */
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

/* Assert that a GET returns the expected status code and that its body does or
 * does not contain a given substring. */
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
    fprintf(stderr, "REST API VFS Mounts Test\n");
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
    /* Disable auth so the mount endpoints can be exercised directly, mirroring
     * the admin pytest setup. */
    chimera_server_config_set_rest_auth_enabled(config, 0);

    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }

    if (chimera_server_mkfs(server, "memfs", "fs0", NULL) != 0) {
        fprintf(stderr, "Failed to create fs0 filesystem in memfs\n");
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }
    chimera_server_mount(server, "share", "memfs", "fs0", NULL);

    chimera_server_start(server);
    fprintf(stderr, "Server started (REST on port %d)\n", REST_PORT);
    usleep(200000);

    /* ===== Test 1: Create a mount with options ===== */
    fprintf(stderr, "\n  Test: Create mount with options...\n");
    check_code("POST /api/v1/mounts with options returns 201",
               "POST", "/api/v1/mounts",
               "{\"name\":\"optmount\",\"module\":\"memfs\",\"path\":\"fs0\","
               "\"options\":\"ro,foo=bar\"}",
               201, &failures);

    /* ===== Test 2: GET echoes the options back ===== */
    fprintf(stderr, "\n  Test: Options are echoed on read...\n");
    check_body_contains("GET /api/v1/mounts/optmount echoes options",
                        "GET", "/api/v1/mounts/optmount", NULL,
                        200, "\"options\":\"ro,foo=bar\"", 1, &failures);

    /* ===== Test 3: List includes the options ===== */
    check_body_contains("GET /api/v1/mounts lists options",
                        "GET", "/api/v1/mounts", NULL,
                        200, "\"options\":\"ro,foo=bar\"", 1, &failures);

    /* ===== Test 4: Config round-trips the options ===== */
    check_body_contains("GET /api/v1/config round-trips options",
                        "GET", "/api/v1/config", NULL,
                        200, "\"options\":\"ro,foo=bar\"", 1, &failures);

    /* ===== Test 5: Malformed options are rejected with 400 ===== */
    fprintf(stderr, "\n  Test: Malformed options rejected with 400...\n");
    check_body_contains("Empty option key returns 400 with reason",
                        "POST", "/api/v1/mounts",
                        "{\"name\":\"bad1\",\"module\":\"memfs\",\"path\":\"fs0\","
                        "\"options\":\"=noKey\"}",
                        400, "empty option key", 1, &failures);

    check_body_contains("Too many options returns 400 with reason",
                        "POST", "/api/v1/mounts",
                        "{\"name\":\"bad2\",\"module\":\"memfs\",\"path\":\"fs0\","
                        "\"options\":\"a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q\"}",
                        400, "too many options", 1, &failures);

    /* A present-but-non-string options value is rejected too. */
    check_code("Non-string options returns 400",
               "POST", "/api/v1/mounts",
               "{\"name\":\"bad3\",\"module\":\"memfs\",\"path\":\"fs0\","
               "\"options\":123}",
               400, &failures);

    /* The rejected mounts must not have been created. */
    check_code("Rejected mount bad1 does not exist (404)",
               "GET", "/api/v1/mounts/bad1", NULL, 404, &failures);

    check_code("Rejected mount bad2 does not exist (404)",
               "GET", "/api/v1/mounts/bad2", NULL, 404, &failures);

    check_code("Rejected mount bad3 does not exist (404)",
               "GET", "/api/v1/mounts/bad3", NULL, 404, &failures);

    /* ===== Test 6: A mount without options omits the key ===== */
    fprintf(stderr, "\n  Test: Mount without options omits the key...\n");
    check_code("POST /api/v1/mounts without options returns 201",
               "POST", "/api/v1/mounts",
               "{\"name\":\"plainmount\",\"module\":\"memfs\",\"path\":\"fs0\"}",
               201, &failures);

    check_body_contains("GET /api/v1/mounts/plainmount omits options",
                        "GET", "/api/v1/mounts/plainmount", NULL,
                        200, "\"options\"", 0, &failures);

    /* ===== Test 7: Missing required fields / duplicate name ===== */
    fprintf(stderr, "\n  Test: Bad requests and conflicts...\n");
    check_code("Missing module returns 400",
               "POST", "/api/v1/mounts",
               "{\"name\":\"nomodule\",\"path\":\"fs0\"}",
               400, &failures);

    check_code("Duplicate mount name returns 409",
               "POST", "/api/v1/mounts",
               "{\"name\":\"optmount\",\"module\":\"memfs\",\"path\":\"fs0\"}",
               409, &failures);

    /* ===== Test 8: Delete removes the mount ===== */
    fprintf(stderr, "\n  Test: Delete removes the mount...\n");
    check_code("DELETE /api/v1/mounts/optmount returns 204",
               "DELETE", "/api/v1/mounts/optmount", NULL, 204, &failures);

    check_code("GET deleted mount returns 404",
               "GET", "/api/v1/mounts/optmount", NULL, 404, &failures);

    /* ===== Test 9: Named filesystems lifecycle ===== */
    fprintf(stderr, "\n  Test: Filesystems endpoint lifecycle...\n");
    check_code("POST /api/v1/filesystems creates fs1 (201)",
               "POST", "/api/v1/filesystems",
               "{\"module\":\"memfs\",\"name\":\"fs1\"}",
               201, &failures);

    check_code("Duplicate filesystem create returns 409",
               "POST", "/api/v1/filesystems",
               "{\"module\":\"memfs\",\"name\":\"fs1\"}",
               409, &failures);

    check_code("Mount of fs1 via REST returns 201",
               "POST", "/api/v1/mounts",
               "{\"name\":\"fs1mount\",\"module\":\"memfs\",\"path\":\"fs1\"}",
               201, &failures);

    check_code("DELETE mounted filesystem returns 409",
               "DELETE", "/api/v1/filesystems/memfs/fs1", NULL, 409, &failures);

    check_code("DELETE /api/v1/mounts/fs1mount returns 204",
               "DELETE", "/api/v1/mounts/fs1mount", NULL, 204, &failures);

    check_code("DELETE unmounted filesystem returns 204",
               "DELETE", "/api/v1/filesystems/memfs/fs1", NULL, 204, &failures);

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
