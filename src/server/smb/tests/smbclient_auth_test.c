// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Authentication Test using smbclient
 *
 * This test verifies SMB authentication works with the standard Samba smbclient,
 * providing interoperability testing beyond libsmb2.
 *
 * Supports three authentication modes:
 *   --mode=ntlm      - Built-in NTLM authentication (default)
 *   --mode=kerberos  - Kerberos/GSSAPI authentication (requires KDC setup)
 *   --mode=winbind   - NTLM via winbind (requires AD environment)
 *   --mode=all       - Run all available auth tests
 *
 * For Kerberos: Run via scripts/kerberos_test_wrapper.sh
 * For Winbind:  Run via scripts/ad_test_wrapper.sh
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "common/test_users.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define TEST_DIR     "smbclient_test"
#define TEST_FILE    "smbclient_test/test.txt"
#define TEST_CONTENT "smbclient authentication test content"

struct test_env {
    struct chimera_server     *server;
    char                       session_dir[256];
    char                       smb_conf_path[512];
    struct prometheus_metrics *metrics;
    int                        kerberos_enabled;
    int                        winbind_enabled;
};

static int tests_passed = 0;
static int tests_failed = 0;

static void
test_cleanup(
    struct test_env *env,
    int              remove_session)
{
    if (env->server) {
        chimera_server_destroy(env->server);
        env->server = NULL;
    }

    if (env->metrics) {
        prometheus_metrics_destroy(env->metrics);
        env->metrics = NULL;
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

static const char *smbclient_config_file = NULL;
static const char *smbclient_host        = "localhost";

static int
run_smbclient(
    const char *auth_args,
    const char *commands)
{
    char cmd[4096];
    int  rc;

    /* Build smbclient command
     * -N = no password prompt (we pass credentials in args)
     * -c = commands to execute */
    if (smbclient_config_file) {
        snprintf(cmd, sizeof(cmd),
                 "smbclient //%s/share %s --configfile=%s -c '%s' 2>&1",
                 smbclient_host, auth_args, smbclient_config_file, commands);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "smbclient //%s/share %s -c '%s' 2>&1",
                 smbclient_host, auth_args, commands);
    }

    fprintf(stderr, "    Running: smbclient //%s/share %s -c '%s'\n",
            smbclient_host, auth_args, commands);

    rc = system(cmd);

    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }

    return -1;
} /* run_smbclient */

static int
run_smbclient_with_output(
    const char *auth_args,
    const char *commands,
    char       *output,
    size_t      output_size)
{
    char  cmd[4096];
    FILE *fp;
    int   rc;

    if (smbclient_config_file) {
        snprintf(cmd, sizeof(cmd),
                 "smbclient //%s/share %s --configfile=%s -c '%s' 2>&1",
                 smbclient_host, auth_args, smbclient_config_file, commands);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "smbclient //%s/share %s -c '%s' 2>&1",
                 smbclient_host, auth_args, commands);
    }

    fprintf(stderr, "    Running: smbclient //%s/share %s -c '%s'\n",
            smbclient_host, auth_args, commands);

    fp = popen(cmd, "r");
    if (!fp) {
        return -1;
    }

    output[0] = '\0';
    while (fgets(output + strlen(output), output_size - strlen(output), fp)) {
        /* Keep reading */
    }

    rc = pclose(fp);

    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }

    return -1;
} /* run_smbclient_with_output */

/* ============================================================================
 * Built-in NTLM Tests
 * ============================================================================ */

static int
test_ntlm_valid_credentials(void)
{
    int rc;

    fprintf(stderr, "\n  Testing NTLM with valid credentials...\n");

    /* Test with valid myuser credentials */
    rc = run_smbclient("-U myuser%mypassword", "ls");

    if (rc == 0) {
        test_pass("NTLM valid credentials");
        return 0;
    } else {
        test_fail("NTLM valid credentials");
        return -1;
    }
} /* test_ntlm_valid_credentials */

static int
test_ntlm_invalid_password(void)
{
    int rc;

    fprintf(stderr, "\n  Testing NTLM with invalid password...\n");

    /* Test with wrong password - should fail */
    rc = run_smbclient("-U myuser%wrongpassword", "ls");

    if (rc != 0) {
        test_pass("NTLM invalid password rejected");
        return 0;
    } else {
        test_fail("NTLM invalid password should be rejected");
        return -1;
    }
} /* test_ntlm_invalid_password */

static int
test_ntlm_invalid_user(void)
{
    int rc;

    fprintf(stderr, "\n  Testing NTLM with invalid user...\n");

    /* Test with non-existent user - should fail */
    rc = run_smbclient("-U nonexistent%password", "ls");

    if (rc != 0) {
        test_pass("NTLM invalid user rejected");
        return 0;
    } else {
        test_fail("NTLM invalid user should be rejected");
        return -1;
    }
} /* test_ntlm_invalid_user */

static int
test_ntlm_file_operations(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing NTLM file operations...\n");

    /* Create directory */
    rc = run_smbclient("-U myuser%mypassword", "mkdir " TEST_DIR);
    if (rc != 0) {
        test_fail("NTLM mkdir");
        return -1;
    }

    /* Create and write file using put with a local temp file */
    {
        char  tmp_file[256];
        FILE *f;
        char  put_cmd[512];

        snprintf(tmp_file, sizeof(tmp_file), "/tmp/smbclient_test_%d.txt", getpid());
        f = fopen(tmp_file, "w");
        if (f) {
            fprintf(f, "%s", TEST_CONTENT);
            fclose(f);
        }

        snprintf(put_cmd, sizeof(put_cmd), "put %s %s", tmp_file, TEST_FILE);
        rc = run_smbclient("-U myuser%mypassword", put_cmd);
        unlink(tmp_file);

        if (rc != 0) {
            test_fail("NTLM put file");
            return -1;
        }
    }

    /* List directory to verify file exists */
    rc = run_smbclient_with_output("-U myuser%mypassword",
                                   "ls " TEST_DIR "/*", output, sizeof(output));
    if (rc != 0 || strstr(output, "test.txt") == NULL) {
        test_fail("NTLM ls file");
        return -1;
    }

    /* Download file and verify content */
    {
        char  tmp_file[256];
        char  get_cmd[512];
        FILE *f;
        char  content[256];

        snprintf(tmp_file, sizeof(tmp_file), "/tmp/smbclient_get_%d.txt", getpid());
        snprintf(get_cmd, sizeof(get_cmd), "get %s %s", TEST_FILE, tmp_file);

        rc = run_smbclient("-U myuser%mypassword", get_cmd);
        if (rc != 0) {
            test_fail("NTLM get file");
            return -1;
        }

        f = fopen(tmp_file, "r");
        if (!f) {
            test_fail("NTLM read downloaded file");
            unlink(tmp_file);
            return -1;
        }

        content[0] = '\0';
        if (fgets(content, sizeof(content), f) == NULL) {
            content[0] = '\0';
        }
        fclose(f);
        unlink(tmp_file);

        if (strcmp(content, TEST_CONTENT) != 0) {
            fprintf(stderr, "    Content mismatch: got '%s', expected '%s'\n",
                    content, TEST_CONTENT);
            test_fail("NTLM file content verification");
            return -1;
        }
    }

    /* Clean up */
    run_smbclient("-U myuser%mypassword", "rm " TEST_FILE);
    run_smbclient("-U myuser%mypassword", "rmdir " TEST_DIR);

    test_pass("NTLM file operations");
    return 0;
} /* test_ntlm_file_operations */

static int
run_ntlm_tests(void)
{
    int failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Built-in NTLM Authentication Tests\n");
    fprintf(stderr, "========================================\n");

    if (test_ntlm_valid_credentials() < 0) {
        failures++;
    }
    if (test_ntlm_invalid_password() < 0) {
        failures++;
    }
    if (test_ntlm_invalid_user() < 0) {
        failures++;
    }
    if (test_ntlm_file_operations() < 0) {
        failures++;
    }

    return failures;
} /* run_ntlm_tests */

/* ============================================================================
 * Kerberos Tests
 * ============================================================================ */

static char kerberos_auth_args[512];

static int
verify_kerberos_environment(void)
{
    const char *krb5_config = getenv("KRB5_CONFIG");
    const char *ccache      = getenv("KRB5CCNAME");
    const char *krb_user    = getenv("KRB_USER");
    const char *krb_realm   = getenv("KRB_REALM");

    if (!krb5_config) {
        fprintf(stderr, "  Skipping Kerberos tests - KRB5_CONFIG not set\n");
        return -1;
    }

    if (!ccache) {
        fprintf(stderr, "  Skipping Kerberos tests - KRB5CCNAME not set\n");
        return -1;
    }

    fprintf(stderr, "  KRB5_CONFIG: %s\n", krb5_config);
    fprintf(stderr, "  KRB5CCNAME:  %s\n", ccache);

    /* smbclient uses Samba's bundled Heimdal which needs --use-krb5-ccache
     * to find the credential cache (it doesn't honor KRB5CCNAME directly).
     * We also need -U user@REALM so smbclient matches the ccache principal
     * instead of defaulting to the Unix username (root). */
    if (krb_user && krb_realm) {
        snprintf(kerberos_auth_args, sizeof(kerberos_auth_args),
                 "--use-kerberos=required --use-krb5-ccache=%s -U %s@%s -N",
                 ccache, krb_user, krb_realm);
    } else {
        snprintf(kerberos_auth_args, sizeof(kerberos_auth_args),
                 "--use-kerberos=required --use-krb5-ccache=%s -N", ccache);
    }

    return 0;
} /* verify_kerberos_environment */

static int
test_kerberos_valid_ticket(void)
{
    int rc;

    fprintf(stderr, "\n  Testing Kerberos with valid TGT...\n");

    rc = run_smbclient(kerberos_auth_args, "ls");

    if (rc == 0) {
        test_pass("Kerberos valid ticket");
        return 0;
    } else {
        test_fail("Kerberos valid ticket");
        return -1;
    }
} /* test_kerberos_valid_ticket */

static int
test_kerberos_file_operations(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing Kerberos file operations...\n");

    /* Create directory */
    rc = run_smbclient(kerberos_auth_args, "mkdir " TEST_DIR);
    if (rc != 0) {
        test_fail("Kerberos mkdir");
        return -1;
    }

    /* Create and write file */
    {
        char  tmp_file[256];
        FILE *f;
        char  put_cmd[512];

        snprintf(tmp_file, sizeof(tmp_file), "/tmp/smbclient_krb_%d.txt", getpid());
        f = fopen(tmp_file, "w");
        if (f) {
            fprintf(f, "%s", TEST_CONTENT);
            fclose(f);
        }

        snprintf(put_cmd, sizeof(put_cmd), "put %s %s", tmp_file, TEST_FILE);
        rc = run_smbclient(kerberos_auth_args, put_cmd);
        unlink(tmp_file);

        if (rc != 0) {
            test_fail("Kerberos put file");
            return -1;
        }
    }

    /* List to verify */
    rc = run_smbclient_with_output(kerberos_auth_args, "ls " TEST_DIR "/*", output, sizeof(output));
    if (rc != 0 || strstr(output, "test.txt") == NULL) {
        test_fail("Kerberos ls file");
        return -1;
    }

    /* Clean up */
    run_smbclient(kerberos_auth_args, "rm " TEST_FILE);
    run_smbclient(kerberos_auth_args, "rmdir " TEST_DIR);

    test_pass("Kerberos file operations");
    return 0;
} /* test_kerberos_file_operations */

static int
run_kerberos_tests(struct test_env *env)
{
    int failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Kerberos Authentication Tests\n");
    fprintf(stderr, "========================================\n");

    if (!env->kerberos_enabled) {
        fprintf(stderr, "  Skipping - Kerberos not enabled on server\n");
        return 0;
    }

    if (verify_kerberos_environment() < 0) {
        return 0; /* Skip, not fail */
    }

    if (test_kerberos_valid_ticket() < 0) {
        failures++;
    }
    if (test_kerberos_file_operations() < 0) {
        failures++;
    }

    return failures;
} /* run_kerberos_tests */

/* ============================================================================
 * Winbind NTLM Tests
 * ============================================================================ */

static int
verify_winbind_environment(void)
{
    const char *socket_dir = getenv("WINBINDD_SOCKET_DIR");
    const char *realm      = getenv("AD_REALM");
    const char *domain     = getenv("AD_DOMAIN");

    if (!socket_dir || !realm || !domain) {
        fprintf(stderr, "  Skipping winbind tests - AD environment not configured\n");
        fprintf(stderr, "  WINBINDD_SOCKET_DIR: %s\n", socket_dir ? socket_dir : "(not set)");
        fprintf(stderr, "  AD_REALM: %s\n", realm ? realm : "(not set)");
        fprintf(stderr, "  AD_DOMAIN: %s\n", domain ? domain : "(not set)");
        return -1;
    }

    fprintf(stderr, "  WINBINDD_SOCKET_DIR: %s\n", socket_dir);
    fprintf(stderr, "  AD_REALM: %s\n", realm);
    fprintf(stderr, "  AD_DOMAIN: %s\n", domain);

    return 0;
} /* verify_winbind_environment */

static int
test_winbind_valid_credentials(void)
{
    const char *domain = getenv("AD_DOMAIN");
    char        auth_args[256];
    int         rc;

    fprintf(stderr, "\n  Testing winbind NTLM with valid AD credentials...\n");

    snprintf(auth_args, sizeof(auth_args), "-U %s\\\\testuser1%%Password1!", domain);
    rc = run_smbclient(auth_args, "ls");

    if (rc == 0) {
        test_pass("Winbind NTLM valid credentials");
        return 0;
    } else {
        test_fail("Winbind NTLM valid credentials");
        return -1;
    }
} /* test_winbind_valid_credentials */

static int
test_winbind_invalid_password(void)
{
    const char *domain = getenv("AD_DOMAIN");
    char        auth_args[256];
    int         rc;

    fprintf(stderr, "\n  Testing winbind NTLM with invalid password...\n");

    snprintf(auth_args, sizeof(auth_args), "-U %s\\\\testuser1%%WrongPassword", domain);
    rc = run_smbclient(auth_args, "ls");

    if (rc != 0) {
        test_pass("Winbind NTLM invalid password rejected");
        return 0;
    } else {
        test_fail("Winbind NTLM invalid password should be rejected");
        return -1;
    }
} /* test_winbind_invalid_password */

static int
test_winbind_file_operations(void)
{
    const char *domain = getenv("AD_DOMAIN");
    char        auth_args[256];
    char        output[4096];
    int         rc;

    fprintf(stderr, "\n  Testing winbind NTLM file operations...\n");

    snprintf(auth_args, sizeof(auth_args), "-U %s\\\\testuser1%%Password1!", domain);

    /* Create directory */
    rc = run_smbclient(auth_args, "mkdir " TEST_DIR);
    if (rc != 0) {
        test_fail("Winbind mkdir");
        return -1;
    }

    /* Create file */
    {
        char  tmp_file[256];
        FILE *f;
        char  put_cmd[512];

        snprintf(tmp_file, sizeof(tmp_file), "/tmp/smbclient_wb_%d.txt", getpid());
        f = fopen(tmp_file, "w");
        if (f) {
            fprintf(f, "%s", TEST_CONTENT);
            fclose(f);
        }

        snprintf(put_cmd, sizeof(put_cmd), "put %s %s", tmp_file, TEST_FILE);
        rc = run_smbclient(auth_args, put_cmd);
        unlink(tmp_file);

        if (rc != 0) {
            test_fail("Winbind put file");
            return -1;
        }
    }

    /* List to verify */
    rc = run_smbclient_with_output(auth_args, "ls " TEST_DIR "/*",
                                   output, sizeof(output));
    if (rc != 0 || strstr(output, "test.txt") == NULL) {
        test_fail("Winbind ls file");
        return -1;
    }

    /* Clean up */
    run_smbclient(auth_args, "rm " TEST_FILE);
    run_smbclient(auth_args, "rmdir " TEST_DIR);

    test_pass("Winbind NTLM file operations");
    return 0;
} /* test_winbind_file_operations */

static int
run_winbind_tests(struct test_env *env)
{
    int failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Winbind NTLM Authentication Tests\n");
    fprintf(stderr, "========================================\n");

    if (!env->winbind_enabled) {
        fprintf(stderr, "  Skipping - Winbind not enabled on server\n");
        return 0;
    }

    if (verify_winbind_environment() < 0) {
        return 0; /* Skip, not fail */
    }

    if (test_winbind_valid_credentials() < 0) {
        failures++;
    }
    if (test_winbind_invalid_password() < 0) {
        failures++;
    }
    if (test_winbind_file_operations() < 0) {
        failures++;
    }

    return failures;
} /* run_winbind_tests */

/* ============================================================================
 * Main
 * ============================================================================ */

static void
print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [options]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  --mode=ntlm      Test built-in NTLM only (default)\n");
    fprintf(stderr, "  --mode=kerberos  Test Kerberos (requires KDC setup)\n");
    fprintf(stderr, "  --mode=winbind   Test winbind NTLM (requires AD)\n");
    fprintf(stderr, "  --mode=all       Run all available tests\n");
    fprintf(stderr, "  -b <backend>     VFS backend (memfs, linux, demofs)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "For Kerberos tests, run via: kerberos_test_wrapper.sh\n");
    fprintf(stderr, "For Winbind tests, run via:  ad_test_wrapper.sh\n");
} /* print_usage */

int
main(
    int    argc,
    char **argv)
{
    struct test_env               env = { 0 };
    struct chimera_server_config *config;
    struct timespec               tv;
    const char                   *mode     = "ntlm";
    const char                   *backend  = "memfs";
    int                           failures = 0;
    int                           i;

    /* Parse arguments */
    for (i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--mode=", 7) == 0) {
            mode = argv[i] + 7;
        } else if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            backend = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "SMB smbclient Authentication Test\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "Mode: %s\n", mode);
    fprintf(stderr, "Backend: %s\n", backend);

    /* Check if smbclient is available */
    if (system("which smbclient >/dev/null 2>&1") != 0) {
        fprintf(stderr, "\nERROR: smbclient not found in PATH\n");
        fprintf(stderr, "Install with: apt-get install smbclient\n");
        return EXIT_FAILURE;
    }

    /* Initialize logging */
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    env.metrics = prometheus_metrics_create(NULL, NULL, 0);
    if (!env.metrics) {
        fprintf(stderr, "Failed to create metrics\n");
        return EXIT_FAILURE;
    }

    /* Create session directory */
    clock_gettime(CLOCK_MONOTONIC, &tv);
    snprintf(env.session_dir, sizeof(env.session_dir),
             "/tmp/smbclient_test_%d_%lu", getpid(), tv.tv_sec);

    if (mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create session directory: %s\n", strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(stderr, "Session directory: %s\n", env.session_dir);

    /* Initialize server configuration */
    config = chimera_server_config_init();

    /* Configure authentication based on environment */
    const char *keytab = getenv("KRB5_KTNAME");
    if (keytab && (strcmp(mode, "kerberos") == 0 || strcmp(mode, "all") == 0)) {
        chimera_server_config_set_smb_kerberos_enabled(config, 1);
        chimera_server_config_set_smb_kerberos_keytab(config, keytab);

        const char *realm = getenv("KRB_REALM");
        if (!realm) {
            realm = "TEST.LOCAL";
        }
        chimera_server_config_set_smb_kerberos_realm(config, realm);
        env.kerberos_enabled = 1;

        /* smbclient refuses Kerberos auth to 'localhost' (hardcoded check),
         * so use a real hostname from the test environment */
        const char *smb_host = getenv("KRB_SMB_HOST");
        if (smb_host) {
            smbclient_host = smb_host;
        }

        /* Create a custom smb.conf so smbclient's bundled Heimdal
         * can find the realm and KDC configuration */
        snprintf(env.smb_conf_path, sizeof(env.smb_conf_path),
                 "%s/smb.conf", env.session_dir);
        {
            FILE *smb_conf = fopen(env.smb_conf_path, "w");
            if (smb_conf) {
                fprintf(smb_conf, "[global]\n");
                fprintf(smb_conf, "    workgroup = %.*s\n",
                        (int) (strchr(realm, '.') ? strchr(realm, '.') - realm : strlen(realm)), realm);
                fprintf(smb_conf, "    realm = %s\n", realm);
                fprintf(smb_conf, "    kerberos method = system keytab\n");
                fprintf(smb_conf, "    client signing = if_required\n");
                fclose(smb_conf);
                smbclient_config_file = env.smb_conf_path;
                fprintf(stderr, "Created smbclient config: %s\n", env.smb_conf_path);
            }
        }

        fprintf(stderr, "Kerberos enabled: realm=%s, keytab=%s\n", realm, keytab);
    }

    const char *socket_dir = getenv("WINBINDD_SOCKET_DIR");
    if (socket_dir && (strcmp(mode, "winbind") == 0 || strcmp(mode, "all") == 0)) {
        chimera_server_config_set_smb_winbind_enabled(config, 1);

        const char *domain = getenv("AD_DOMAIN");
        if (domain) {
            chimera_server_config_set_smb_winbind_domain(config, domain);
        }
        env.winbind_enabled = 1;

        fprintf(stderr, "Winbind enabled: domain=%s\n", domain ? domain : "(default)");
    }

    /* Initialize server */
    env.server = chimera_server_init(config, env.metrics);
    if (!env.server) {
        fprintf(stderr, "Failed to initialize server\n");
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    /* Mount filesystem */
    if (strcmp(backend, "memfs") == 0) {
        chimera_server_mount(env.server, "share", "memfs", "/");
    } else if (strcmp(backend, "linux") == 0) {
        chimera_server_mount(env.server, "share", "linux", env.session_dir);
    } else {
        fprintf(stderr, "Unknown backend: %s\n", backend);
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share");

    fprintf(stderr, "Server started\n");

    /* Give server a moment to be ready */
    usleep(100000);

    /* Run tests based on mode */
    if (strcmp(mode, "ntlm") == 0 || strcmp(mode, "all") == 0) {
        failures += run_ntlm_tests();
    }

    if (strcmp(mode, "kerberos") == 0 || strcmp(mode, "all") == 0) {
        failures += run_kerberos_tests(&env);
    }

    if (strcmp(mode, "winbind") == 0 || strcmp(mode, "all") == 0) {
        failures += run_winbind_tests(&env);
    }

    /* Summary */
    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Test Summary\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "Passed: %d\n", tests_passed);
    fprintf(stderr, "Failed: %d\n", tests_failed);

    if (failures > 0) {
        fprintf(stderr, "\nSome tests FAILED\n\n");
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "\nAll tests PASSED\n\n");
    test_cleanup(&env, 1);
    return EXIT_SUCCESS;
} /* main */
