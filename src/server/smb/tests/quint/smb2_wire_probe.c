/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2/3 wire-protection ground-truth probe.
 *
 * Every other probe and the whole trace corpus run over ONE connection
 * profile: SMB 2.1/3.0, an anonymous logon, no signing, no encryption.  That
 * leaves the server's entire protection layer untested -- dialect 3.1.1 and
 * its negotiate contexts, algorithm selection, NTLMv2 credential validation,
 * SMB3 key derivation, message signing, and the TRANSFORM-header encryption
 * path -- even though it sits under every request the corpus already sends.
 *
 * This probe walks a matrix of profiles and, for each, asserts that a full
 * session works end to end: handshake, TREE_CONNECT, CREATE, WRITE, READ back
 * the same bytes, CLOSE.  The assertions are deliberately ordinary file I/O,
 * because that is what makes them ground truth for the protection layer --
 * a wrong signing key, a wrong KDF label, a mis-built transform header or a
 * botched negotiate context does not produce subtly wrong data, it produces a
 * dropped connection or an undecryptable reply.  If the bytes come back, both
 * sides agreed on every one of those.
 *
 * What each profile pins:
 *   W1  3.0  + NTLMv2, unsigned      -- real credential validation
 *                                       (smb_ntlm.c validate_local_user)
 *   W2  2.1  + signing               -- HMAC-SHA256, session key used verbatim
 *   W3  3.0  + signing               -- AES-CMAC, SP800-108 signing-key KDF
 *   W4  3.1.1 + signing, AES-GMAC    -- preauth-bound keys, GMAC nonce rules,
 *                                       negotiate contexts + algorithm select
 *   W5  3.1.1 + signing, AES-CMAC    -- the server honours a client's
 *                                       non-default signing preference
 *   W6  3.0  + encryption            -- pre-3.1.1 cipher KDF ("SMB2AESCCM",
 *                                       "ServerIn "/"ServerOut")
 *   W7  3.1.1 + encryption, GCM      -- transform header + AEAD, 3.1.1 KDF
 *   W8  3.1.1 + encryption, CCM      -- the CCM arm of the same path
 *   W12 3.0  + signing, no KEY_EXCH  -- the session key is the key-exchange
 *                                       key itself, as for every client that
 *                                       follows the server's flags
 *
 * A bad-credential case (W9) pins the negative: authentication must FAIL, so
 * that a profile passing tells us the exchange was actually verified rather
 * than waved through.  W10, W11, W13 and W14 pin the key exchange's own
 * negative: an AUTHENTICATE that sets KEY_EXCH must carry a 16-byte
 * EncryptedRandomSessionKey inside the message, and one that carries none, 8
 * bytes, 24 bytes, or 16 bytes running past the end of the message is refused
 * as a bad token (MS-NLMP 3.2.5.1.2) rather than keyed with the wrong key.
 * W12 is the allowed side of that refusal: no KEY_EXCH, no key, accepted.
 *
 * With --winbind (run under scripts/ad_test_wrapper.sh) W2-W4 and W7 (as
 * WB2-WB4 and WB7), W10, W11 and W12 run again as a domain user that the
 * server validates through winbind.  winbind returns only the session base
 * key, so this is the case where the server must unwrap the client's key
 * itself.  A client that key-exchanges (as this one does except in W12, and as
 * older Linux kernel cifs clients such as 4.18 do on every new session, whether
 * or not the server offers it) signs with the unwrapped key, so without that
 * step its first signed request fails verification.
 */

#include "smb2_mbt_common.h"

static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

/* *INDENT-OFF* */
/* uncrustify 0.78.1 does not converge on aligned designated initializers: each
 * pass widens the '=' column, so `make syntax` and syntax-check disagree
 * forever.  Same guard as the other aligned tables in the tree. */
static const struct smb2_wire_profile profiles[] = {
    { .name = "W1 3.0 ntlmv2 plain",
      .max_dialect = 0x0300, .ntlmv2 = 1 },
    { .name = "W2 2.1 signed hmac-sha256",
      .max_dialect = 0x0210, .ntlmv2 = 1, .sign = 1 },
    { .name = "W3 3.0 signed aes-cmac",
      .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1 },
    { .name = "W4 3.1.1 signed aes-gmac",
      .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
      .signing_alg = SMB2W_SIGN_AES_GMAC },
    { .name = "W5 3.1.1 signed aes-cmac",
      .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
      .signing_alg = SMB2W_SIGN_AES_CMAC },
    { .name = "W6 3.0 encrypted aes-128-ccm",
      .max_dialect = 0x0300, .ntlmv2 = 1, .encrypt = 1,
      .cipher = SMB2W_CIPHER_AES128_CCM },
    { .name = "W7 3.1.1 encrypted aes-128-gcm",
      .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
      .cipher = SMB2W_CIPHER_AES128_GCM,
      .signing_alg = SMB2W_SIGN_AES_GMAC },
    { .name = "W8 3.1.1 encrypted aes-128-ccm",
      .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
      .cipher = SMB2W_CIPHER_AES128_CCM,
      .signing_alg = SMB2W_SIGN_AES_GMAC },
    { .name = "W12 3.0 signed without key exchange",
      .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1, .no_key_exch = 1 },
};
/* *INDENT-ON* */

/* Drive one profile through a complete session.  Returns the dialect actually
 * negotiated so the caller can assert the profile got the wire it asked for. */
static uint16_t
run_profile(
    const struct smb2_wire_profile *w,
    const char                     *user,
    const char                     *password)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn      *c;
    struct smb2_create_out co;
    const char             payload[] = "WIREPROBE";   /* 9 bytes, no NUL */
    uint8_t                rdbuf[64];
    uint32_t               st, count = 0, rlen = 0;
    uint16_t               dialect;

    printf("# --- %s ---\n", w->name);

    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w->name, st);

    st = smb2_session_setup_as(c, user, password);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);

    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);

    printf("#   dialect=0x%04x signing_alg=0x%04x cipher=0x%04x "
           "signing_on=%d encrypt_on=%d\n",
           c->dialect, c->signing_alg, c->cipher, c->signing_on, c->encrypt_on);

    st = smb2_create(c, "wire.txt", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: CREATE -> 0x%08x", w->name, st);

    st = smb2_write(c, co.file_id, 0, payload, 9, &count);
    CHECK(st == ST_SUCCESS && count == 9,
          "%s: WRITE 9 bytes -> 0x%08x (count=%u)", w->name, st, count);

    st = smb2_read(c, co.file_id, 0, 9, rdbuf, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 9 && memcmp(rdbuf, payload, 9) == 0,
          "%s: READ returns the bytes written (len=%u)", w->name, rlen);

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "%s: CLOSE -> 0x%08x", w->name, st);

    dialect = c->dialect;

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return dialect;
} /* run_profile */

/* A profile whose password is wrong must be REFUSED.  Without this, every
 * positive result above is compatible with a server that accepts anything. */
static void
run_bad_credentials(void)
{
    struct smb2_env                       env;
    struct smb2_env_opts                  opts = { 0 };
    struct smb2_conn                     *c;
    uint32_t                              st;
    static const struct smb2_wire_profile w = {
        .name = "W9 bad password", .max_dialect = 0x0300, .ntlmv2 = 1
    };

    printf("# --- %s ---\n", w.name);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    /* Re-register the account with a DIFFERENT password than the client will
     * present, so the NTLMv2 proof cannot verify. */
    chimera_server_remove_user(env.server, SMB2W_USER);
    {
        const uint32_t gids[1] = { SMB2W_GID };

        chimera_server_add_user(env.server, SMB2W_USER, "not-the-password",
                                "not-the-password", NULL,
                                SMB2W_UID, SMB2W_GID, 1, gids, 1);
    }

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w.name, st);

    st = smb2_session_setup(c);
    CHECK(st != ST_SUCCESS,
          "%s: SESSION_SETUP is REFUSED (0x%08x)", w.name, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_bad_credentials */

/* SESSION_SETUP whose AUTHENTICATE sets KEY_EXCH but carries an
 * EncryptedRandomSessionKey of kx_len bytes instead of 16, or, with
 * past_end, 16 bytes whose offset runs them 8 bytes past the end of the
 * message.  The key is the last field of the payload smb2w_ntlm_auth_ntlmv2
 * builds, so resizing it is a matter of trimming or padding the message and
 * fixing its length fields. */
static uint32_t
session_setup_bad_kx(
    struct smb2_conn *c,
    const char       *user,
    const char       *password,
    uint16_t          kx_len,
    int               past_end)
{
    const struct smb2_wire_profile *w = c->wire;
    uint8_t                         blob[2048], session_key[16];
    int                             n;
    uint32_t                        st;

    n  = smb2w_ntlm_negotiate(blob, 0, 1);
    st = smb2c_session_setup_leg(c, blob, n);
    if (st != ST_MORE_PROCESSING_REQUIRED) {
        return st;
    }
    c->session_id = g64(c->rbuf + 4, 40);

    {
        const uint8_t *rbody = c->rbuf + 4 + SMB2_HDR_SIZE;
        uint16_t       soff  = g16(rbody, 4);
        uint16_t       slen  = g16(rbody, 6);

        n = smb2w_ntlm_auth_ntlmv2(c->rbuf + 4 + soff, slen, user, password,
                                   w->winbind_domain ? w->winbind_domain : SMB2W_DOMAIN,
                                   1, blob, session_key);
    }

    if (past_end) {
        p32(blob, 56, n - 8);
    } else {
        if (kx_len > 16) {
            memset(blob + n, 0xa5, kx_len - 16);
        }
        n += kx_len - 16;
        p16(blob, 52, kx_len);
        p16(blob, 54, kx_len);
    }

    return smb2c_session_setup_leg(c, blob, n);
} /* session_setup_bad_kx */

/* A KEY_EXCH AUTHENTICATE without a usable EncryptedRandomSessionKey must be
 * refused as a malformed token.  Accepting it would key the session with the
 * key-exchange key, which a key-exchanging client never signs with. */
static void
run_bad_key_exchange(
    const struct smb2_wire_profile *w,
    const char                     *user,
    const char                     *password,
    uint16_t                        kx_len,
    int                             past_end)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;
    uint32_t             st;

    printf("# --- %s ---\n", w->name);

    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w->name, st);

    st = session_setup_bad_kx(c, user, password, kx_len, past_end);
    CHECK(st == ST_INVALID_PARAMETER,
          "%s: SESSION_SETUP is refused as a bad token (0x%08x)", w->name, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_bad_key_exchange */

static void
run_profiles(
    const struct smb2_wire_profile *list,
    unsigned int                    count,
    const char                     *user,
    const char                     *password)
{
    unsigned int i;

    for (i = 0; i < count; i++) {
        uint16_t dialect = run_profile(&list[i], user, password);

        /* The profile asked for a ceiling; the server picks within it.  A
         * 3.1.1 profile that quietly settled for 3.0 would still pass every
         * I/O check above while testing none of the 3.1.1 paths, so pin the
         * dialect itself. */
        if (list[i].max_dialect == 0x0311) {
            CHECK(dialect == 0x0311,
                  "%s: negotiated 3.1.1 (0x%04x)", list[i].name, dialect);
        }
    }
} /* run_profiles */

/* The domain user scripts/ad_test_wrapper.sh creates. */
#define WINBIND_USER     "testuser1"
#define WINBIND_PASSWORD "Password1!"

static int
run_winbind(void)
{
    const char *domain     = getenv("AD_DOMAIN");
    const char *socket_dir = getenv("WINBINDD_SOCKET_DIR");

    if (!domain || !socket_dir) {
        printf("# SKIP: --winbind needs AD_DOMAIN and WINBINDD_SOCKET_DIR "
               "(run under scripts/ad_test_wrapper.sh)\n");
        return 77;
    }

    /* *INDENT-OFF* */
    const struct smb2_wire_profile wb_profiles[] = {
        { .name = "WB2 2.1 signed hmac-sha256",
          .max_dialect = 0x0210, .ntlmv2 = 1, .sign = 1,
          .winbind_domain = domain },
        { .name = "WB3 3.0 signed aes-cmac",
          .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1,
          .winbind_domain = domain },
        { .name = "WB4 3.1.1 signed aes-gmac",
          .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
          .signing_alg = SMB2W_SIGN_AES_GMAC, .winbind_domain = domain },
        { .name = "WB7 3.1.1 encrypted aes-128-gcm",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .cipher = SMB2W_CIPHER_AES128_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC, .winbind_domain = domain },
        { .name = "WB12 3.0 signed without key exchange",
          .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1, .no_key_exch = 1,
          .winbind_domain = domain },
    };
    const struct smb2_wire_profile no_key = {
        .name = "WB10 key exchange without a key", .max_dialect = 0x0300,
        .ntlmv2 = 1, .winbind_domain = domain
    };
    const struct smb2_wire_profile short_key = {
        .name = "WB11 key exchange with an 8-byte key", .max_dialect = 0x0300,
        .ntlmv2 = 1, .winbind_domain = domain
    };
    /* *INDENT-ON* */

    run_profiles(wb_profiles,
                 sizeof(wb_profiles) / sizeof(wb_profiles[0]),
                 WINBIND_USER, WINBIND_PASSWORD);
    run_bad_key_exchange(&no_key, WINBIND_USER, WINBIND_PASSWORD, 0, 0);
    run_bad_key_exchange(&short_key, WINBIND_USER, WINBIND_PASSWORD, 8, 0);
    return 0;
} /* run_winbind */

int
main(
    int   argc,
    char *argv[])
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile no_key = {
        .name = "W10 key exchange without a key", .max_dialect = 0x0300,
        .ntlmv2 = 1
    };
    static const struct smb2_wire_profile short_key = {
        .name = "W11 key exchange with an 8-byte key", .max_dialect = 0x0300,
        .ntlmv2 = 1
    };
    static const struct smb2_wire_profile past_end_key = {
        .name = "W13 key exchange with a key past the end of the message",
        .max_dialect = 0x0300, .ntlmv2 = 1
    };
    static const struct smb2_wire_profile long_key = {
        .name = "W14 key exchange with a 24-byte key", .max_dialect = 0x0300,
        .ntlmv2 = 1
    };
    /* *INDENT-ON* */

    /* The server logs to stderr; keeping stdout unbuffered means a drop or an
     * abort lands in the transcript after the check that provoked it rather
     * than after everything. */
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc > 1 && strcmp(argv[1], "--winbind") == 0) {
        if (run_winbind() == 77) {
            return 77;
        }
    } else {
        run_profiles(profiles, sizeof(profiles) / sizeof(profiles[0]),
                     SMB2W_USER, SMB2W_PASSWORD);
        run_bad_credentials();
        run_bad_key_exchange(&no_key, SMB2W_USER, SMB2W_PASSWORD, 0, 0);
        run_bad_key_exchange(&short_key, SMB2W_USER, SMB2W_PASSWORD, 8, 0);
        run_bad_key_exchange(&past_end_key, SMB2W_USER, SMB2W_PASSWORD, 16, 1);
        run_bad_key_exchange(&long_key, SMB2W_USER, SMB2W_PASSWORD, 24, 0);
    }

    if (failures) {
        fprintf(stderr, "%d wire-protection check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 wire-protection checks passed\n");
    return 0;
} /* main */
