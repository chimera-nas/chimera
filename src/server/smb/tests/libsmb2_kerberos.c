// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Kerberos Authentication Integration Test
 *
 * This test verifies Kerberos (GSSAPI/SPNEGO) authentication works end-to-end.
 * It requires:
 *   - KRB5_CONFIG pointing to a valid krb5.conf
 *   - KRB5_KTNAME pointing to a server keytab with cifs/localhost principal
 *   - A valid TGT obtained via kinit for the test user
 *
 * Run via: scripts/kerberos_test_wrapper.sh or scripts/ad_test_wrapper.sh
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

#define TEST_DIR  "kerbtest"
#define TEST_FILE "kerbtest/test.txt"

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
verify_kerberos_environment(void)
{
    const char *krb5_config = getenv("KRB5_CONFIG");
    const char *keytab      = getenv("KRB5_KTNAME");
    const char *ccache      = getenv("KRB5CCNAME");

    fprintf(stderr, "\n=== Kerberos Environment ===\n");
    fprintf(stderr, "KRB5_CONFIG: %s\n", krb5_config ? krb5_config : "(not set)");
    fprintf(stderr, "KRB5_KTNAME: %s\n", keytab ? keytab : "(not set)");
    fprintf(stderr, "KRB5CCNAME:  %s\n", ccache ? ccache : "(default)");

    if (!krb5_config || !keytab) {
        fprintf(stderr, "\nERROR: Kerberos environment not configured.\n");
        fprintf(stderr, "Run this test via kerberos_test_wrapper.sh or ad_test_wrapper.sh\n");
        return -1;
    }

    if (access(krb5_config, R_OK) != 0) {
        fprintf(stderr, "ERROR: Cannot read KRB5_CONFIG: %s\n", krb5_config);
        return -1;
    }

    if (access(keytab, R_OK) != 0) {
        fprintf(stderr, "ERROR: Cannot read keytab: %s\n", keytab);
        return -1;
    }

    return 0;
} /* verify_kerberos_environment */

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
    const char                   *test_content = "Kerberos test content";
    uint8_t                       buffer[128];

    (void) argc;
    (void) argv;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "SMB Kerberos Authentication Test\n");
    fprintf(stderr, "========================================\n");

    /* Verify Kerberos environment is set up */
    if (verify_kerberos_environment() < 0) {
        return EXIT_FAILURE;
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
             "/tmp/smb_krb_test_%d_%lu", getpid(), tv.tv_sec);

    if (mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        test_fail(&env, "Failed to create session directory");
    }

    fprintf(stderr, "\nSession directory: %s\n", env.session_dir);

    /* Initialize server with Kerberos enabled */
    config = chimera_server_config_init();

    /* Configure Kerberos authentication */
    chimera_server_config_set_smb_kerberos_enabled(config, 1);

    const char *keytab = getenv("KRB5_KTNAME");
    if (keytab) {
        chimera_server_config_set_smb_kerberos_keytab(config, keytab);
    }

    const char *realm = getenv("KRB_REALM");
    if (!realm) {
        realm = "TEST.LOCAL";
    }
    chimera_server_config_set_smb_kerberos_realm(config, realm);

    fprintf(stderr, "Kerberos config: realm=%s, keytab=%s\n",
            realm, keytab ? keytab : "(default)");

    env.server = chimera_server_init(config, env.metrics);

    if (!env.server) {
        test_fail(&env, "Failed to initialize server");
    }

    /* Mount a memfs share for testing */
    chimera_server_mount(env.server, "share", "memfs", "/");
    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share");

    test_pass("Server started with Kerberos support");

    /* Initialize SMB2 client with Kerberos authentication */
    env.ctx = smb2_init_context();
    if (!env.ctx) {
        test_fail(&env, "Failed to init SMB2 context");
    }

    smb2_set_security_mode(env.ctx, SMB2_NEGOTIATE_SIGNING_ENABLED);
    smb2_set_authentication(env.ctx, SMB2_SEC_KRB5);

    /* Set user to match the principal in the ccache
     * libsmb2 uses this to form the GSSAPI initiator credentials */
    smb2_set_user(env.ctx, "testuser1");

    fprintf(stderr, "\nConnecting with Kerberos authentication as testuser1...\n");

    /* Connect using Kerberos - password not needed, uses ccache TGT */
    rc = smb2_connect_share(env.ctx, "localhost", "share", "testuser1");
    if (rc != 0) {
        fprintf(stderr, "smb2_connect_share failed: %s\n", smb2_get_error(env.ctx));
        test_fail(&env, "Kerberos authentication failed");
    }

    test_pass("Connected with Kerberos authentication");

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
    fprintf(stderr, "All Kerberos tests PASSED\n");
    fprintf(stderr, "========================================\n\n");

    test_cleanup(&env, 1);
    return EXIT_SUCCESS;
} /* main */
