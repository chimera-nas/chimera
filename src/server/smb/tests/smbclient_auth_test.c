// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Authentication Test using smbclient
 *
 * This test verifies SMB authentication works with the standard Samba smbclient,
 * providing interoperability testing beyond libsmb2.
 *
 * Supports these authentication modes:
 *   --mode=ntlm                   - Built-in NTLM authentication (default)
 *   --mode=kerberos               - Kerberos/GSSAPI authentication (requires KDC setup)
 *   --mode=kerberos-winbind-down  - Kerberos with winbind_enabled and no winbindd:
 *                                   the logon must be REFUSED (requires KDC setup)
 *   --mode=kerberos-no-fallback   - Kerberos with winbind off and no fallback knob:
 *                                   the logon must be REFUSED (requires KDC setup)
 *   --mode=winbind                - NTLM via winbind (requires AD environment)
 *   --mode=all                    - Run all available auth tests
 *
 * For Kerberos: Run via scripts/kerberos_test_wrapper.sh
 * For Winbind:  Run via scripts/ad_test_wrapper.sh
 */

#include "common/test_host.h"
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
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include <sys/wait.h>
#include <time.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* ifdef _WIN32 */

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

/* ---------------------------------------------------------------------------
* Raw-wire SPNEGO probe.  Samba's smbclient falls back to NTLM by itself when
* a Kerberos leg fails, so it cannot tell the server steering it to NTLMSSP
* (RFC 4178 negTokenResp accept-incomplete) from the server refusing it.  A
* domain-joined Windows client can: it takes LOGON_FAILURE as final.  This
* minimal SMB2 client sends the first SESSION_SETUP leg with a hand-built
* negTokenInit and reads the status and security buffer the server answers.
* ------------------------------------------------------------------------- */

/* negTokenInit, mechTypes [MS KRB5, KRB5, NTLMSSP], opaque mechToken: the
 * first leg of a domain-joined Windows client. */
/* *INDENT-OFF* */
static const uint8_t wire_kerberos_first[] = {
    0x60, 0x3a,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x30,
    0x30, 0x2e,
    0xa0, 0x24,
    0x30, 0x22,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    0xa2, 0x06,
    0x04, 0x04, 0xde, 0xad, 0xbe, 0xef,
};

/* negTokenInit, mechTypes [KRB5] only: a client that cannot do NTLM. */
static const uint8_t wire_kerberos_only[] = {
    0x60, 0x23,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x19,
    0x30, 0x17,
    0xa0, 0x0d,
    0x30, 0x0b,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0xa2, 0x06,
    0x04, 0x04, 0xde, 0xad, 0xbe, 0xef,
};

/* The NEGOTIATE security buffer a server without Kerberos must offer:
 * negTokenInit with mechTypes { NTLMSSP } alone. */
static const uint8_t wire_negotiate_ntlmssp_only[] = {
    0x60, 0x1c,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x12,
    0x30, 0x10,
    0xa0, 0x0e,
    0x30, 0x0c,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
};

/* The steering reply: negTokenResp { accept-incomplete, supportedMech NTLMSSP }. */
static const uint8_t wire_ntlmssp_hint[] = {
    0xa1, 0x15,
    0x30, 0x13,
    0xa0, 0x03, 0x0a, 0x01, 0x01,
    0xa1, 0x0c,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
};
/* *INDENT-ON* */

#define WIRE_STATUS_MORE_PROCESSING_REQUIRED 0xC0000016u
#define WIRE_STATUS_LOGON_FAILURE            0xC000006Du

static void
wire_put16(
    uint8_t *p,
    uint16_t v)
{
    p[0] = (uint8_t) v;
    p[1] = (uint8_t) (v >> 8);
} /* wire_put16 */

static void
wire_put32(
    uint8_t *p,
    uint32_t v)
{
    wire_put16(p, (uint16_t) v);
    wire_put16(p + 2, (uint16_t) (v >> 16));
} /* wire_put32 */

static uint16_t
wire_get16(const uint8_t *p)
{
    return (uint16_t) (p[0] | (p[1] << 8));
} /* wire_get16 */

static uint32_t
wire_get32(const uint8_t *p)
{
    return (uint32_t) wire_get16(p) | ((uint32_t) wire_get16(p + 2) << 16);
} /* wire_get32 */

/* Send one SMB2 message (NBSS framed) and read the reply into buf.  Returns
 * the reply length from the SMB2 header on, or -1. */
static int
wire_exchange(
    int            fd,
    uint8_t        command,
    uint64_t       message_id,
    const uint8_t *body,
    size_t         body_len,
    uint8_t       *buf,
    size_t         buf_len)
{
    uint8_t  msg[4 + 64 + 512];
    size_t   len = 64 + body_len;
    uint8_t  nb[4];
    size_t   got;
    ssize_t  n;
    uint32_t rlen;

    if (body_len > 512) {
        return -1;
    }
    memset(msg, 0, sizeof(msg));
    msg[0] = 0;
    msg[1] = (uint8_t) (len >> 16);
    msg[2] = (uint8_t) (len >> 8);
    msg[3] = (uint8_t) len;
    memcpy(msg + 4, "\xfeSMB", 4);
    wire_put16(msg + 4 + 4, 64);              /* StructureSize */
    wire_put16(msg + 4 + 12, command);        /* Command */
    wire_put16(msg + 4 + 14, 1);              /* CreditRequest */
    wire_put32(msg + 4 + 24, (uint32_t) message_id);
    wire_put32(msg + 4 + 28, (uint32_t) (message_id >> 32));
    memcpy(msg + 4 + 64, body, body_len);

    if (write(fd, msg, 4 + len) != (ssize_t) (4 + len)) {
        return -1;
    }

    got = 0;
    while (got < 4) {
        n = read(fd, nb + got, 4 - got);
        if (n <= 0) {
            return -1;
        }
        got += (size_t) n;
    }
    rlen = ((uint32_t) nb[1] << 16) | ((uint32_t) nb[2] << 8) | nb[3];
    if (rlen < 64 || rlen > buf_len) {
        return -1;
    }
    got = 0;
    while (got < rlen) {
        n = read(fd, buf + got, rlen - got);
        if (n <= 0) {
            return -1;
        }
        got += (size_t) n;
    }
    return (int) rlen;
} /* wire_exchange */

/* NEGOTIATE at SMB 2.0.2/2.1 (no negotiate contexts, no preauth integrity),
 * then one SESSION_SETUP leg carrying blob.  On success stores the reply
 * status and copies the reply security buffer (truncated to sec_len) and
 * returns 0; -1 on any transport or framing problem. */
static int
wire_first_session_setup_leg(
    const uint8_t *blob,
    size_t         blob_len,
    uint32_t      *status,
    uint8_t       *sec,
    size_t        *sec_len)
{
    struct sockaddr_in sa;
    uint8_t            body[128 + 128];
    uint8_t            reply[4096];
    int                fd, n;
    uint16_t           off, len;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(445);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *) &sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }

    /* SMB2 NEGOTIATE: StructureSize 36, DialectCount 2, SecurityMode
     * SIGNING_ENABLED, Capabilities 0, ClientGuid, ClientStartTime 0,
     * Dialects 0x0202 0x0210. */
    memset(body, 0, sizeof(body));
    wire_put16(body, 36);
    wire_put16(body + 2, 2);
    wire_put16(body + 4, 1);
    memset(body + 12, 0x5a, 16);
    wire_put16(body + 36, 0x0202);
    wire_put16(body + 38, 0x0210);
    n = wire_exchange(fd, 0, 0, body, 40, reply, sizeof(reply));
    if (n < 0 || wire_get32(reply + 8) != 0) {
        close(fd);
        return -1;
    }

    /* SMB2 SESSION_SETUP: StructureSize 25, Flags 0, SecurityMode
    * SIGNING_ENABLED, Capabilities 0, Channel 0, SecurityBufferOffset 88,
    * SecurityBufferLength, PreviousSessionId 0, then the blob. */
    memset(body, 0, sizeof(body));
    wire_put16(body, 25);
    body[3] = 1;
    wire_put16(body + 12, 88);
    wire_put16(body + 14, (uint16_t) blob_len);
    memcpy(body + 24, blob, blob_len);
    n = wire_exchange(fd, 1, 1, body, 24 + blob_len, reply, sizeof(reply));
    close(fd);
    if (n < 0) {
        return -1;
    }

    *status = wire_get32(reply + 8);
    off     = wire_get16(reply + 64 + 4);
    len     = wire_get16(reply + 64 + 6);
    if (wire_get16(reply + 64) != 9 || (size_t) off + len > (size_t) n) {
        *sec_len = 0;
        return 0;
    }
    if (len > *sec_len) {
        len = (uint16_t) *sec_len;
    }
    memcpy(sec, reply + off, len);
    *sec_len = len;
    return 0;
} /* wire_first_session_setup_leg */

/* NEGOTIATE at SMB 2.x and return the server's security buffer. */
static int
wire_negotiate_security_buffer(
    uint8_t *sec,
    size_t  *sec_len)
{
    struct sockaddr_in sa;
    uint8_t            body[64];
    uint8_t            reply[4096];
    int                fd, n;
    uint16_t           off, len;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(445);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *) &sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }

    memset(body, 0, sizeof(body));
    wire_put16(body, 36);
    wire_put16(body + 2, 2);
    wire_put16(body + 4, 1);
    memset(body + 12, 0x5a, 16);
    wire_put16(body + 36, 0x0202);
    wire_put16(body + 38, 0x0210);
    n = wire_exchange(fd, 0, 0, body, 40, reply, sizeof(reply));
    close(fd);
    if (n < 0 || wire_get32(reply + 8) != 0 || wire_get16(reply + 64) != 65) {
        return -1;
    }

    /* NEGOTIATE response: SecurityBufferOffset at 56, Length at 58. */
    off = wire_get16(reply + 64 + 56);
    len = wire_get16(reply + 64 + 58);
    if ((size_t) off + len > (size_t) n) {
        return -1;
    }
    if (len > *sec_len) {
        len = (uint16_t) *sec_len;
    }
    memcpy(sec, reply + off, len);
    *sec_len = len;
    return 0;
} /* wire_negotiate_security_buffer */

static int
test_negotiate_offers_ntlmssp_only(void)
{
    uint8_t sec[128];
    size_t  sec_len = sizeof(sec);

    fprintf(stderr, "\n  Testing NEGOTIATE offers NTLMSSP alone without Kerberos...\n");

    if (wire_negotiate_security_buffer(sec, &sec_len) < 0) {
        fprintf(stderr, "    raw SMB2 NEGOTIATE failed\n");
        test_fail("NEGOTIATE security buffer read");
        return -1;
    }
    if (sec_len != sizeof(wire_negotiate_ntlmssp_only) ||
        memcmp(sec, wire_negotiate_ntlmssp_only, sec_len) != 0) {
        fprintf(stderr, "    security buffer is %zu bytes, expected the %zu-byte NTLMSSP-only token\n",
                sec_len, sizeof(wire_negotiate_ntlmssp_only));
        test_fail("NEGOTIATE offers NTLMSSP alone");
        return -1;
    }
    test_pass("NEGOTIATE offers NTLMSSP alone");
    return 0;
} /* test_negotiate_offers_ntlmssp_only */

static int
test_spnego_kerberos_first_is_steered(void)
{
    uint32_t status = 0;
    uint8_t  sec[64];
    size_t   sec_len = sizeof(sec);

    fprintf(stderr, "\n  Testing a Kerberos-first SESSION_SETUP is steered to NTLMSSP...\n");

    if (wire_first_session_setup_leg(wire_kerberos_first, sizeof(wire_kerberos_first),
                                     &status, sec, &sec_len) < 0) {
        fprintf(stderr, "    raw SMB2 exchange failed\n");
        test_fail("Kerberos-first leg answered on the wire");
        return -1;
    }
    if (status != WIRE_STATUS_MORE_PROCESSING_REQUIRED) {
        fprintf(stderr, "    status 0x%08x, expected STATUS_MORE_PROCESSING_REQUIRED\n", status);
        test_fail("Kerberos-first leg gets STATUS_MORE_PROCESSING_REQUIRED");
        return -1;
    }
    test_pass("Kerberos-first leg gets STATUS_MORE_PROCESSING_REQUIRED");

    if (sec_len != sizeof(wire_ntlmssp_hint) ||
        memcmp(sec, wire_ntlmssp_hint, sec_len) != 0) {
        fprintf(stderr, "    security buffer (%zu bytes) is not the NTLMSSP steering hint\n", sec_len);
        test_fail("Kerberos-first leg carries the NTLMSSP steering hint");
        return -1;
    }
    test_pass("Kerberos-first leg carries the NTLMSSP steering hint");
    return 0;
} /* test_spnego_kerberos_first_is_steered */

static int
test_spnego_kerberos_only_is_refused(void)
{
    uint32_t status = 0;
    uint8_t  sec[64];
    size_t   sec_len = sizeof(sec);

    fprintf(stderr, "\n  Testing a Kerberos-only SESSION_SETUP is refused...\n");

    if (wire_first_session_setup_leg(wire_kerberos_only, sizeof(wire_kerberos_only),
                                     &status, sec, &sec_len) < 0) {
        fprintf(stderr, "    raw SMB2 exchange failed\n");
        test_fail("Kerberos-only leg answered on the wire");
        return -1;
    }
    if (status != WIRE_STATUS_LOGON_FAILURE) {
        fprintf(stderr, "    status 0x%08x, expected STATUS_LOGON_FAILURE\n", status);
        test_fail("Kerberos-only leg gets STATUS_LOGON_FAILURE");
        return -1;
    }
    test_pass("Kerberos-only leg gets STATUS_LOGON_FAILURE");
    return 0;
} /* test_spnego_kerberos_only_is_refused */

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
    if (test_spnego_kerberos_first_is_steered() < 0) {
        failures++;
    }
    if (test_spnego_kerberos_only_is_refused() < 0) {
        failures++;
    }
    if (test_negotiate_offers_ntlmssp_only() < 0) {
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

    /* Samba 4.23+ ships a private Heimdal that cannot read credential
    * caches created by the system MIT kinit.  When a password is
    * available (KRB_PASSWORD), pass it directly so smbclient's
    * bundled Heimdal acquires its own ticket from the KDC.
    * Fall back to ccache-only for environments without a password. */
    {
        const char *krb_pass = getenv("KRB_PASSWORD");

        if (krb_user && krb_realm && krb_pass) {
            snprintf(kerberos_auth_args, sizeof(kerberos_auth_args),
                     "--use-kerberos=required -U %s@%s%%%s",
                     krb_user, krb_realm, krb_pass);
        } else if (krb_user && krb_realm) {
            snprintf(kerberos_auth_args, sizeof(kerberos_auth_args),
                     "--use-kerberos=required"
                     " --use-krb5-ccache=%s -U %s@%s -N",
                     ccache, krb_user, krb_realm);
        } else {
            snprintf(kerberos_auth_args, sizeof(kerberos_auth_args),
                     "--use-kerberos=required"
                     " --use-krb5-ccache=%s -N",
                     ccache);
        }
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

/* A Kerberos logon the server must REFUSE.  The GSSAPI accept itself succeeds
 * (the keytab is valid and the client holds a service ticket), but the server
 * has no identity for the principal -- in the winbind-down mode because
 * winbind_enabled is set and no winbindd answers -- so the SESSION_SETUP must
 * complete with STATUS_LOGON_FAILURE.  Before the fix it was established as
 * uid/gid 65534 and `ls` succeeded. */
static int
test_kerberos_logon_refused(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing that the Kerberos logon is refused...\n");

    rc = run_smbclient_with_output(kerberos_auth_args, "ls", output, sizeof(output));

    if (rc == 0) {
        fprintf(stderr, "    smbclient succeeded: the server granted a session it had no identity for\n");
        test_fail("Kerberos logon refused");
        return -1;
    }

    if (strstr(output, "NT_STATUS_LOGON_FAILURE") == NULL) {
        fprintf(stderr, "    smbclient failed for another reason:\n%s\n", output);
        test_fail("Kerberos logon refused with NT_STATUS_LOGON_FAILURE");
        return -1;
    }

    test_pass("Kerberos logon refused with NT_STATUS_LOGON_FAILURE");
    return 0;
} /* test_kerberos_logon_refused */

/* The server has no Kerberos configured but the client holds a ticket for
 * it, so smbclient goes Kerberos first (--use-kerberos=desired), and the
 * local user's password must then log on over NTLM.  smbclient falls back to
 * NTLM by itself when the Kerberos leg fails, so this passes whether the
 * server steers or refuses: it guards the end-to-end logon, not the steering,
 * which the raw wire probe in the NTLM mode pins. */
static int
test_kerberos_first_falls_back_to_ntlm(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing Kerberos-first logon falls back to NTLM...\n");

    rc = run_smbclient_with_output("--use-kerberos=desired -U myuser%mypassword",
                                   "ls", output, sizeof(output));
    if (rc != 0) {
        fprintf(stderr, "    smbclient failed:\n%s\n", output);
        test_fail("Kerberos-first client logs on over NTLM");
        return -1;
    }
    test_pass("Kerberos-first client logs on over NTLM");
    return 0;
} /* test_kerberos_first_falls_back_to_ntlm */

/* A client that insists on Kerberos must still be refused: the steering
 * hint is not an open door. */
static int
test_kerberos_required_still_refused(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing Kerberos-required logon is refused...\n");

    rc = run_smbclient_with_output("--use-kerberos=required -U myuser%mypassword",
                                   "ls", output, sizeof(output));
    if (rc == 0) {
        fprintf(stderr, "    smbclient succeeded with Kerberos against a server without it\n");
        test_fail("Kerberos-required logon refused");
        return -1;
    }
    test_pass("Kerberos-required logon refused");
    return 0;
} /* test_kerberos_required_still_refused */

/* A client that keeps sending Kerberos must fail that leg, and the server
 * must stay healthy: a plain NTLM logon on a fresh connection still works. */
static int
test_kerberos_hint_then_kerberos_again(void)
{
    char output[4096];
    int  rc;

    fprintf(stderr, "\n  Testing the server survives a client that ignores the hint...\n");

    (void) run_smbclient_with_output("--use-kerberos=required -U myuser%mypassword",
                                     "ls", output, sizeof(output));
    rc = run_smbclient_with_output("--use-kerberos=off -U myuser%mypassword",
                                   "ls", output, sizeof(output));
    if (rc != 0) {
        fprintf(stderr, "    plain NTLM logon failed after a refused Kerberos one:\n%s\n", output);
        test_fail("NTLM logon after refused Kerberos leg");
        return -1;
    }
    test_pass("NTLM logon after refused Kerberos leg");
    return 0;
} /* test_kerberos_hint_then_kerberos_again */

static int
run_kerberos_fallback_tests(void)
{
    int failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Kerberos-first NTLM Fallback Tests\n");
    fprintf(stderr, "========================================\n");

    if (verify_kerberos_environment() < 0) {
        return 0; /* Skip, not fail */
    }
    if (test_kerberos_first_falls_back_to_ntlm() < 0) {
        failures++;
    }
    if (test_kerberos_required_still_refused() < 0) {
        failures++;
    }
    if (test_kerberos_hint_then_kerberos_again() < 0) {
        failures++;
    }
    return failures;
} /* run_kerberos_fallback_tests */

static int
run_kerberos_refusal_tests(
    struct test_env *env,
    const char      *mode)
{
    int failures = 0;

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Kerberos Refusal Tests (%s)\n", mode);
    fprintf(stderr, "========================================\n");

    if (!env->kerberos_enabled) {
        fprintf(stderr, "  Skipping - Kerberos not enabled on server\n");
        return 0;
    }

    if (verify_kerberos_environment() < 0) {
        return 0; /* Skip, not fail */
    }

    if (test_kerberos_logon_refused() < 0) {
        failures++;
    }

    return failures;
} /* run_kerberos_refusal_tests */

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

/* Modes that stand up a Kerberos-accepting server.  Each needs the KDC, keytab
 * and ticket scripts/kerberos_test_wrapper.sh provides. */
static int
mode_uses_kerberos(const char *mode)
{
    return strcmp(mode, "kerberos") == 0 ||
           strcmp(mode, "kerberos-no-fallback") == 0 ||
           strcmp(mode, "kerberos-winbind-down") == 0 ||
           strcmp(mode, "all") == 0;
} /* mode_uses_kerberos */

/* Modes whose CLIENT holds a Kerberos ticket and talks to the realm host
 * name: every server-side Kerberos mode plus the fallback mode, whose
 * server deliberately has no Kerberos at all. */
static int
mode_client_has_ticket(const char *mode)
{
    return mode_uses_kerberos(mode) ||
           strcmp(mode, "kerberos-ntlm-fallback") == 0;
} /* mode_client_has_ticket */

/* Modes whose one assertion is that the Kerberos logon is REFUSED. */
static int
mode_expects_refusal(const char *mode)
{
    return strcmp(mode, "kerberos-no-fallback") == 0 ||
           strcmp(mode, "kerberos-winbind-down") == 0;
} /* mode_expects_refusal */

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
    fprintf(stderr,
            "  --mode=kerberos-winbind-down  Kerberos with winbind_enabled and no winbindd; logon must be refused\n");
    fprintf(stderr,
            "  --mode=kerberos-no-fallback   Kerberos with winbind off and no fallback knob; logon must be refused\n");
    fprintf(stderr,
            "  --mode=kerberos-ntlm-fallback Client holds a ticket, server has no Kerberos; logon must fall back to NTLM\n");
    fprintf(stderr, "  --mode=winbind   Test winbind NTLM (requires AD)\n");
    fprintf(stderr, "  --mode=all       Run all available tests\n");
    fprintf(stderr, "  -b <backend>     VFS backend (memfs, linux, diskfs)\n");
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
        fprintf(stderr, "\nSKIP: smbclient not found in PATH\n");
        fprintf(stderr, "Install with: apt-get install smbclient\n");
        return 77;   /* ctest SKIP_RETURN_CODE */
    }

    /* The refusal modes assert a security property: run with no KDC they would
     * pass vacuously, so they skip outright unless the Kerberos wrapper set the
     * environment up, and they need winbind to be genuinely absent. */
    if (mode_expects_refusal(mode) || strcmp(mode, "kerberos-ntlm-fallback") == 0) {
        if (!getenv("KRB5_KTNAME")) {
            fprintf(stderr, "\nSKIP: KRB5_KTNAME not set; run via scripts/kerberos_test_wrapper.sh\n");
            return 77;
        }
        if (!getenv("KRB5_CONFIG")) {
            fprintf(stderr, "\nSKIP: KRB5_CONFIG not set; run via scripts/kerberos_test_wrapper.sh\n");
            return 77;
        }
        if (!getenv("KRB5CCNAME")) {
            fprintf(stderr, "\nSKIP: KRB5CCNAME not set; run via scripts/kerberos_test_wrapper.sh\n");
            return 77;
        }
        if (getenv("WINBINDD_SOCKET_DIR")) {
            fprintf(stderr, "\nSKIP: WINBINDD_SOCKET_DIR is set; this mode needs winbind to be absent\n");
            return 77;
        }
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

    if (chimera_test_mkdir(env.session_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create session directory: %s\n", strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(stderr, "Session directory: %s\n", env.session_dir);

    /* Initialize server configuration */
    config = chimera_server_config_init();
    chimera_server_config_set_smb_enabled(config, 1);

    /* Configure authentication based on environment */
    const char *keytab = getenv("KRB5_KTNAME");
    if (keytab && mode_uses_kerberos(mode)) {
        chimera_server_config_set_smb_kerberos_enabled(config, 1);
        chimera_server_config_set_smb_kerberos_keytab(config, keytab);

        /* The positive modes run against an MIT realm with no winbind, so the
         * only identity a principal can get is the opt-in nobody mapping; what
         * they exercise is the GSSAPI exchange.  The refusal modes leave the
         * knob off. */
        if (!mode_expects_refusal(mode)) {
            chimera_server_config_set_smb_kerberos_anonymous_fallback(config, 1);
        }

        const char *realm = getenv("KRB_REALM");
        if (!realm) {
            realm = "TEST.LOCAL";
        }
        chimera_server_config_set_smb_kerberos_realm(config, realm);
        env.kerberos_enabled = 1;

        fprintf(stderr, "Kerberos enabled: realm=%s, keytab=%s\n", realm, keytab);
    }

    /* Client side of the realm: the host name smbclient may do Kerberos
     * against and an smb.conf naming the realm.  Needed by every mode whose
     * client holds a ticket, including the fallback mode whose server has
     * no Kerberos configured at all. */
    if (keytab && mode_client_has_ticket(mode)) {
        const char *realm = getenv("KRB_REALM");
        if (!realm) {
            realm = "TEST.LOCAL";
        }

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
    }

    if (strcmp(mode, "kerberos-winbind-down") == 0) {
        /* winbind_enabled with nothing to answer: the Kerberos wrapper stands
         * up an MIT KDC and no winbindd, so the principal cannot be mapped and
         * the server must refuse the logon rather than serve it as uid 65534. */
        chimera_server_config_set_smb_winbind_enabled(config, 1);
        fprintf(stderr, "Winbind enabled with no winbindd running: Kerberos logons must be refused\n");
    }

    if (strcmp(mode, "kerberos-no-fallback") == 0) {
        /* Neither winbind nor the fallback knob: the server has no identity
         * source for the principal and must refuse the logon. */
        fprintf(stderr, "No identity source configured: Kerberos logons must be refused\n");
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
        if (chimera_server_mkfs(env.server, "memfs", "fs0", NULL) != 0) {
            fprintf(stderr, "Failed to create fs0 filesystem in memfs\n");
            test_cleanup(&env, 0);
            return EXIT_FAILURE;
        }
        chimera_server_mount(env.server, "share", "memfs", "fs0", NULL);
    } else if (strcmp(backend, "linux") == 0) {
        chimera_server_mount(env.server, "share", "linux", env.session_dir, NULL);
    } else {
        fprintf(stderr, "Unknown backend: %s\n", backend);
        test_cleanup(&env, 0);
        return EXIT_FAILURE;
    }

    chimera_server_start(env.server);
    chimera_test_add_server_users(env.server);
    chimera_server_create_share(env.server, "share", "share", 0);

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

    if (mode_expects_refusal(mode)) {
        failures += run_kerberos_refusal_tests(&env, mode);
    }

    if (strcmp(mode, "kerberos-ntlm-fallback") == 0) {
        failures += run_kerberos_fallback_tests();
    }

    if (strcmp(mode, "winbind") == 0 || strcmp(mode, "all") == 0) {
        failures += run_winbind_tests(&env);
    }

    /* A refusal mode asserts a security property (the logon is refused); it
     * must never report success having asserted nothing.  The early skip above
     * covers a missing wrapper variable, but this is the backstop: if the one
     * assertion in run_kerberos_refusal_tests() was itself skipped (e.g.
     * verify_kerberos_environment() still failed), tests_passed stays 0 with
     * no failures, and that is a vacuous pass, not a PASS. */
    if ((mode_expects_refusal(mode) || strcmp(mode, "kerberos-ntlm-fallback") == 0) &&
        tests_passed == 0 && failures == 0) {
        fprintf(stderr, "\nSKIP: refusal mode ran no assertion\n");
        test_cleanup(&env, 1);
        return 77;
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
