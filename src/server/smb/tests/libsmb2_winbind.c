// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Winbind/NTLM Authentication Integration Test
 *
 * This test verifies NTLM authentication via winbind works end-to-end.
 * It requires:
 *   - WINBINDD_SOCKET_DIR pointing to a running winbind socket
 *   - AD_REALM, AD_DOMAIN environment variables set
 *   - Test users created in AD (testuser1/Password1!)
 *
 * Run via: scripts/ad_test_wrapper.sh
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "common/test_users.h"
#include "smb2/smb2.h"
#include "smb2/libsmb2.h"
#include <errno.h>
#include <fcntl.h>
#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define TEST_DIR  "wbtest"
#define TEST_FILE "wbtest/test.txt"

struct test_env {
    struct smb2_context       *ctx;
    struct chimera_server     *server;
    char                       session_dir[256];
    struct prometheus_metrics *metrics;
};

static void
test_cleanup(
    struct test_env *env,
    int              remove_session)
{
    if (env->ctx) {
        smb2_disconnect_share(env->ctx);
        smb2_destroy_context(env->ctx);
    }

    if (env->server) {
        chimera_server_destroy(env->server);
    }

    if (env->metrics) {
        prometheus_metrics_destroy(env->metrics);
    }

    if (remove_session && env->session_dir[0] != '\0') {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        if (system(cmd) != 0) {
            fprintf(stderr, "Warning: failed to clean up session dir\n");
        }
    }
} /* test_cleanup */

static void
test_fail(
    struct test_env *env,
    const char      *msg)
{
    fprintf(stderr, "FAIL: %s\n", msg);
    test_cleanup(env, 0);
    exit(EXIT_FAILURE);
} /* test_fail */

static void
test_pass(const char *msg)
{
    fprintf(stderr, "PASS: %s\n", msg);
} /* test_pass */

static int
verify_winbind_environment(void)
{
    const char *socket_dir = getenv("WINBINDD_SOCKET_DIR");
    const char *realm      = getenv("AD_REALM");
    const char *domain     = getenv("AD_DOMAIN");
    char        socket_path[512];

    fprintf(stderr, "\n=== Winbind Environment ===\n");
    fprintf(stderr, "WINBINDD_SOCKET_DIR: %s\n", socket_dir ? socket_dir : "(not set)");
    fprintf(stderr, "AD_REALM:            %s\n", realm ? realm : "(not set)");
    fprintf(stderr, "AD_DOMAIN:           %s\n", domain ? domain : "(not set)");

    if (!socket_dir) {
        fprintf(stderr, "\nERROR: Winbind environment not configured.\n");
        fprintf(stderr, "Run this test via ad_test_wrapper.sh\n");
        return -1;
    }

    /* Check if winbind socket exists */
    snprintf(socket_path, sizeof(socket_path), "%s/pipe", socket_dir);
    if (access(socket_path, F_OK) != 0) {
        fprintf(stderr, "ERROR: Winbind socket not found: %s\n", socket_path);
        return -1;
    }

    fprintf(stderr, "Winbind socket found: %s\n", socket_path);
    return 0;
} /* verify_winbind_environment */

int
main(
    int    argc,
    char **argv)
{
    struct test_env               env = { 0 };
    struct chimera_server_config *config;
    struct smb2fh                *fd;
    struct timespec               tv;
    int                           rc;
    const char                   *test_content = "Winbind test content";
    const char                   *domain;
    uint8_t                       buffer[128];

    (void) argc;
    (void) argv;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "SMB Winbind/NTLM Authentication Test\n");
    fprintf(stderr, "========================================\n");

    /* Verify winbind environment is set up */
    if (verify_winbind_environment() < 0) {
        return EXIT_FAILURE;
    }

    domain = getenv("AD_DOMAIN");
    if (!domain) {
        domain = "TEST";
    }

    /* Initialize logging and metrics */
    ChimeraLogLevel = CHIMERA_LOG_DEBUG;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    env.metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!env.metrics) {
        test_fail(&env, "Failed to create metrics");
    }

    /* Create session directory */
    clock_gettime(CLOCK_MONOTONIC, &tv);
    snprintf(env.session_dir, sizeof(env.session_dir),
             "/tmp/smb_wb_test_%d_%lu", getpid(), tv.tv_sec);

    if (mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        test_fail(&env, "Failed to create session directory");
    }

    fprintf(stderr, "\nSession directory: %s\n", env.session_dir);

    /* Initialize server */
    config     = chimera_server_config_init();
    env.server = chimera_server_init(config, env.metrics);

    if (!env.server) {
        test_fail(&env, "Failed to initialize server");
    }

    /* Mount a memfs share for testing */
    chimera_server_mount(env.server, "share", "memfs", "/");
    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share");

    test_pass("Server started with winbind support");

    /* Initialize SMB2 client with NTLM authentication */
    env.ctx = smb2_init_context();
    if (!env.ctx) {
        test_fail(&env, "Failed to init SMB2 context");
    }

    smb2_set_security_mode(env.ctx, SMB2_NEGOTIATE_SIGNING_ENABLED);
    smb2_set_authentication(env.ctx, SMB2_SEC_NTLMSSP);

    /* Set AD credentials - testuser1 was created by ad_test_wrapper.sh */
    smb2_set_user(env.ctx, "testuser1");
    smb2_set_password(env.ctx, "Password1!");
    smb2_set_domain(env.ctx, domain);

    fprintf(stderr, "\nConnecting as %s\\testuser1 via NTLM/winbind...\n", domain);

    rc = smb2_connect_share(env.ctx, "localhost", "share", "testuser1");
    if (rc != 0) {
        fprintf(stderr, "smb2_connect_share failed: %s\n", smb2_get_error(env.ctx));
        test_fail(&env, "NTLM/winbind authentication failed");
    }

    test_pass("Connected with NTLM authentication via winbind");

    /* Verify we can perform file operations */
    fprintf(stderr, "\nTesting file operations...\n");

    rc = smb2_mkdir(env.ctx, TEST_DIR);
    if (rc < 0) {
        fprintf(stderr, "mkdir failed: %s\n", smb2_get_error(env.ctx));
        test_fail(&env, "mkdir failed");
    }
    test_pass("Created directory");

    fd = smb2_open(env.ctx, TEST_FILE, O_WRONLY | O_CREAT);
    if (!fd) {
        fprintf(stderr, "open failed: %s\n", smb2_get_error(env.ctx));
        test_fail(&env, "open for write failed");
    }

    rc = smb2_write(env.ctx, fd, (uint8_t *) test_content, strlen(test_content));
    if (rc < 0) {
        fprintf(stderr, "write failed: %s\n", smb2_get_error(env.ctx));
        smb2_close(env.ctx, fd);
        test_fail(&env, "write failed");
    }

    smb2_close(env.ctx, fd);
    test_pass("Wrote test file");

    /* Read back and verify */
    fd = smb2_open(env.ctx, TEST_FILE, O_RDONLY);
    if (!fd) {
        test_fail(&env, "open for read failed");
    }

    memset(buffer, 0, sizeof(buffer));
    rc = smb2_read(env.ctx, fd, buffer, sizeof(buffer) - 1);
    if (rc < 0) {
        smb2_close(env.ctx, fd);
        test_fail(&env, "read failed");
    }

    smb2_close(env.ctx, fd);

    if (strcmp((char *) buffer, test_content) != 0) {
        fprintf(stderr, "Content mismatch: got '%s', expected '%s'\n",
                buffer, test_content);
        test_fail(&env, "Content verification failed");
    }
    test_pass("Read and verified test file");

    /* Cleanup test files */
    smb2_unlink(env.ctx, TEST_FILE);
    smb2_rmdir(env.ctx, TEST_DIR);

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "All Winbind/NTLM tests PASSED\n");
    fprintf(stderr, "========================================\n\n");

    test_cleanup(&env, 1);
    return EXIT_SUCCESS;
} /* main */
