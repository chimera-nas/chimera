// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * REST API User and Share Manipulation Test
 *
 * This test verifies that the REST API for managing users and shares
 * works correctly in conjunction with SMB authentication.
 *
 * Test scenarios:
 *   1. User lifecycle: create user via REST, verify SMB access, delete user
 *   2. Share lifecycle: create share via REST, verify SMB access, delete share
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define REST_PORT 18080
#define REST_USER "restuser"
#define REST_PASS "restpassword"
#define REST_UID  2000
#define REST_GID  2000

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
run_smbclient(
    const char *share_name,
    const char *auth_args,
    const char *commands)
{
    char cmd[4096];
    int  rc;

    snprintf(cmd, sizeof(cmd),
             "smbclient //localhost/%s %s -c '%s' 2>&1",
             share_name, auth_args, commands);

    fprintf(stderr, "    Running: smbclient //localhost/%s %s -c '%s'\n",
            share_name, auth_args, commands);

    rc = system(cmd);

    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }

    return -1;
} /* run_smbclient */

static int
run_curl(
    const char *method,
    const char *path,
    const char *body,
    long       *http_code)
{
    char  cmd[4096];
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

    fprintf(stderr, "    Running: curl -X %s http://localhost:%d%s\n",
            method, REST_PORT, path);

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
} /* run_curl */

/* ============================================================================
 * User Lifecycle Tests
 * ============================================================================ */

static int
run_user_tests(void)
{
    char auth_args[256];
    long http_code = 0;
    int  rc;
    int  failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "REST User Manipulation Tests\n");
    fprintf(stderr, "========================================\n");

    snprintf(auth_args, sizeof(auth_args), "-U %s%%%s", REST_USER, REST_PASS);

    /* Step 1: Verify user does not exist - smbclient should fail */
    fprintf(stderr, "\n  Testing SMB access with non-existent user...\n");
    rc = run_smbclient("share", auth_args, "ls");
    if (rc != 0) {
        test_pass("SMB rejected non-existent user");
    } else {
        test_fail("SMB should reject non-existent user");
        failures++;
    }

    /* Step 2: Create user via REST API */
    fprintf(stderr, "\n  Creating user via REST API...\n");
    {
        char body[512];
        snprintf(body, sizeof(body),
                 "{\"username\":\"%s\",\"smbpasswd\":\"%s\","
                 "\"uid\":%d,\"gid\":%d}",
                 REST_USER, REST_PASS, REST_UID, REST_GID);

        rc = run_curl("POST", "/api/v1/users", body, &http_code);
        if (rc == 0 && http_code == 201) {
            test_pass("REST create user");
        } else {
            test_fail("REST create user");
            fprintf(stderr, "    HTTP code: %ld\n", http_code);
            failures++;
            return failures;
        }
    }

    /* Step 3: Verify user can now authenticate via SMB */
    fprintf(stderr, "\n  Testing SMB access with REST-created user...\n");
    rc = run_smbclient("share", auth_args, "ls");
    if (rc == 0) {
        test_pass("SMB accepted REST-created user");
    } else {
        test_fail("SMB should accept REST-created user");
        failures++;
    }

    /* Step 4: Delete user via REST API */
    fprintf(stderr, "\n  Deleting user via REST API...\n");
    {
        char path[256];
        snprintf(path, sizeof(path), "/api/v1/users/%s", REST_USER);

        rc = run_curl("DELETE", path, NULL, &http_code);
        if (rc == 0 && http_code == 204) {
            test_pass("REST delete user");
        } else {
            test_fail("REST delete user");
            fprintf(stderr, "    HTTP code: %ld\n", http_code);
            failures++;
            return failures;
        }
    }

    /* Step 5: Verify user can no longer authenticate via SMB */
    fprintf(stderr, "\n  Testing SMB access after user deletion...\n");
    rc = run_smbclient("share", auth_args, "ls");
    if (rc != 0) {
        test_pass("SMB rejected deleted user");
    } else {
        test_fail("SMB should reject deleted user");
        failures++;
    }

    return failures;
} /* run_user_tests */

/* ============================================================================
 * Share Lifecycle Tests
 * ============================================================================ */

static int
run_share_tests(struct chimera_server *server)
{
    char auth_args[256];
    long http_code = 0;
    int  rc;
    int  failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "REST Share Manipulation Tests\n");
    fprintf(stderr, "========================================\n");

    /* Add a user for authentication (via direct API, not REST) */
    chimera_server_add_user(server, REST_USER, NULL, REST_PASS,
                            NULL, REST_UID, REST_GID, 0, NULL, 1);

    snprintf(auth_args, sizeof(auth_args), "-U %s%%%s", REST_USER, REST_PASS);

    /* Step 1: Verify share does not exist - smbclient should fail */
    fprintf(stderr, "\n  Testing SMB access to non-existent share...\n");
    rc = run_smbclient("restshare", auth_args, "ls");
    if (rc != 0) {
        test_pass("SMB rejected non-existent share");
    } else {
        test_fail("SMB should reject non-existent share");
        failures++;
    }

    /* Step 2: Create share via REST API */
    fprintf(stderr, "\n  Creating share via REST API...\n");
    {
        const char *body =
            "{\"name\":\"restshare\",\"path\":\"testvfs\"}";

        rc = run_curl("POST", "/api/v1/shares", body, &http_code);
        if (rc == 0 && http_code == 201) {
            test_pass("REST create share");
        } else {
            test_fail("REST create share");
            fprintf(stderr, "    HTTP code: %ld\n", http_code);
            failures++;
            return failures;
        }
    }

    /* Step 3: Verify share is now accessible via SMB */
    fprintf(stderr, "\n  Testing SMB access to REST-created share...\n");
    rc = run_smbclient("restshare", auth_args, "ls");
    if (rc == 0) {
        test_pass("SMB accepted REST-created share");
    } else {
        test_fail("SMB should accept REST-created share");
        failures++;
    }

    /* Step 4: Delete share via REST API */
    fprintf(stderr, "\n  Deleting share via REST API...\n");
    rc = run_curl("DELETE", "/api/v1/shares/restshare", NULL, &http_code);
    if (rc == 0 && http_code == 204) {
        test_pass("REST delete share");
    } else {
        test_fail("REST delete share");
        fprintf(stderr, "    HTTP code: %ld\n", http_code);
        failures++;
        return failures;
    }

    /* Step 5: Verify share is no longer accessible via SMB */
    fprintf(stderr, "\n  Testing SMB access after share deletion...\n");
    rc = run_smbclient("restshare", auth_args, "ls");
    if (rc != 0) {
        test_pass("SMB rejected deleted share");
    } else {
        test_fail("SMB should reject deleted share");
        failures++;
    }

    /* Clean up the test user */
    chimera_server_remove_user(server, REST_USER);

    return failures;
} /* run_share_tests */

/* ============================================================================
 * Main
 * ============================================================================ */

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
    fprintf(stderr, "REST User/Share Manipulation Test\n");
    fprintf(stderr, "========================================\n");

    /* Check prerequisites */
    if (system("which smbclient >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nERROR: smbclient not found in PATH\n");
        return EXIT_FAILURE;
    }
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

    /* Initialize server configuration */
    config = chimera_server_config_init();
    chimera_server_config_set_rest_http_port(config, REST_PORT);

    /* Initialize server */
    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        prometheus_metrics_destroy(metrics);
        return EXIT_FAILURE;
    }

    /* Mount VFS backends:
     * - "share" for user tests (pre-existing share)
     * - "testvfs" for share tests (shares created dynamically via REST) */
    chimera_server_mount(server, "share", "memfs", "/");
    chimera_server_mount(server, "testvfs", "memfs", "/");

    /* Create the "share" SMB share for user tests */
    chimera_server_create_share(server, "share", "share");

    /* Start server (SMB + REST) - no users added initially */
    chimera_server_start(server);

    fprintf(stderr, "Server started (REST on port %d)\n", REST_PORT);

    /* Give server a moment to be ready */
    usleep(200000);

    /* Run tests */
    failures += run_user_tests();
    failures += run_share_tests(server);

    /* Summary */
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
