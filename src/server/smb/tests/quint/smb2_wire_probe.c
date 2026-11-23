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
 * The E profiles pin each smb_encryption mode, as the server presents it and
 * as it answers:
 *   E1, E2    off: no ENCRYPTION_CAPABILITIES context (3.1.1), no
 *             SMB2_GLOBAL_CAP_ENCRYPTION (3.0)
 *   E8, E12   off with a share marked encrypt_data: the cipher (3.1.1) or the
 *             capability (3.0) is offered, and the share's I/O is encrypted
 *   E3-E5     available: the session is not ENCRYPT_DATA; encrypted requests
 *             are answered encrypted, cleartext ones in cleartext
 *   E11       available with a 3.0 client that only signs: let in, cleartext
 *   E6, E7    enabled and required: the session is ENCRYPT_DATA, every reply
 *             encrypted
 *   E9, E10   a compressed READ reply to an encrypted request, under available
 *             and under an encrypt_data share, is compressed and encrypted
 *
 * L1 refuses a reconnect whose PreviousSessionId names a session of another
 * dialect (MS-SMB2 3.3.5.5.1), a SESSION_SETUP refused after authentication.
 *
 * The C profiles pin cipher selection against smb_encryption_ciphers:
 *   C1-C3     3.1.1 takes the first cipher in the client's list that the
 *             server allows
 *   C4, C5    3.1.1 with nothing in common: the context carries cipher 0;
 *             available lets the session in, in cleartext, required refuses it
 *   C6, C7    3.0 and 3.0.2 without AES-128-CCM: no capability; available lets
 *             the session in, required refuses it
 *
 * With --winbind (run under scripts/ad_test_wrapper.sh) W2-W4 and W7 (as
 * WB2-WB4 and WB7), W10, W11 and W12 run again as a domain user that the
 * server validates through winbind.  winbind returns only the session base
 * key, so this is the case where the server must unwrap the client's key
 * itself.  A client that key-exchanges (as this one does except in W12, and as
 * older Linux kernel cifs clients such as 4.18 do on every new session, whether
 * or not the server offers it) signs with the unwrapped key, so without that
 * step its first signed request fails verification.
 *
 * WB9, the counterpart of W9, logs on as that domain user with the wrong
 * password and must be refused.
 *
 * Built as smb2_wire_probe_wbstub (SMB2W_WBSTUB), --winbind runs the same
 * profiles against wbclient_stub.c instead of a domain controller.  That needs
 * no root, netns or DC, so the winbind branch of the server's NTLM validation
 * runs in the quick tier too.  Against the stub, whose SIDs are known, WB15
 * also checks the identity the logon produced: a file the domain user creates
 * is owned by the user's SID, with its primary group's SID as the group.
 */

#include "smb2_mbt_common.h"
#ifdef SMB2W_WBSTUB
#include "wbclient_stub.h"
#endif /* ifdef SMB2W_WBSTUB */

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

/* SMB2_SESSION_FLAG_ENCRYPT_DATA (MS-SMB2 2.2.6): the server requires the
 * session's traffic to be encrypted. */
#define WIRE_SESSION_FLAG_ENCRYPT_DATA 0x0004

/* One CREATE / WRITE / READ / CLOSE round on an established tree, so a check
 * can say "I/O works" in one line. */
static void
wire_io(
    struct smb2_conn *c,
    const char       *name,
    const char       *step)
{
    struct smb2_create_out co;
    const char             payload[] = "WIREPROBE";   /* 9 bytes, no NUL */
    uint8_t                rdbuf[64];
    uint32_t               st, count = 0, rlen = 0;

    st = smb2_create(c, "wire.txt", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: %s CREATE -> 0x%08x", name, step, st);
    st = smb2_write(c, co.file_id, 0, payload, 9, &count);
    CHECK(st == ST_SUCCESS && count == 9,
          "%s: %s WRITE -> 0x%08x (count=%u)", name, step, st, count);
    st = smb2_read(c, co.file_id, 0, 9, rdbuf, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 9 && memcmp(rdbuf, payload, 9) == 0,
          "%s: %s READ returns the bytes written (len=%u)", name, step, rlen);
    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "%s: %s CLOSE -> 0x%08x", name, step, st);
} /* wire_io */

/* With smb_encryption off the server must not claim to encrypt: a 3.1.1
 * NEGOTIATE that carries an ENCRYPTION_CAPABILITIES context gets none back
 * (MS-SMB2 3.3.5.4: "If IsEncryptionSupported is FALSE, the server MUST
 * ignore the context"), and a 3.0 client that advertises
 * SMB2_GLOBAL_CAP_ENCRYPTION does not get it echoed.  A client that sees
 * neither knows at NEGOTIATE that the server does not encrypt, instead of
 * encrypting anyway and being disconnected. */
static void
run_encryption_off(void)
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile off311 = {
        .name = "E1 3.1.1 server encryption off",
        .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
        .signing_alg = SMB2W_SIGN_AES_GMAC,
        .server_encryption = SMB2W_SERVER_ENC_OFF
    };
    static const struct smb2_wire_profile off30 = {
        .name = "E2 3.0 server encryption off",
        .max_dialect = 0x0300, .ntlmv2 = 1, .encrypt = 1,
        .server_encryption = SMB2W_SERVER_ENC_OFF
    };
    /* *INDENT-ON* */
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st;

    printf("# --- %s ---\n", off311.name);
    smb2_env_open_wire(&env, &opts, &off311);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == 0x0311,
          "%s: NEGOTIATE 3.1.1 -> 0x%08x (0x%04x)", off311.name, st, c->dialect);
    CHECK(!c->encryption_ctx,
          "%s: no ENCRYPTION_CAPABILITIES context in the response", off311.name);
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", off311.name, st);
    CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
          "%s: session not marked ENCRYPT_DATA (0x%04x)", off311.name,
          c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", off311.name, st);
    wire_io(c, off311.name, "signed");
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);

    /* NEGOTIATE only: this client advertises encryption and would encrypt
     * after SESSION_SETUP, which is exactly what the missing capability tells
     * it not to do. */
    printf("# --- %s ---\n", off30.name);
    smb2_env_open_wire(&env, &opts, &off30);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == 0x0300,
          "%s: NEGOTIATE 3.0 -> 0x%08x (0x%04x)", off30.name, st, c->dialect);
    CHECK(!(c->server_caps & SMB2_GLOBAL_CAP_ENCRYPTION),
          "%s: SMB2_GLOBAL_CAP_ENCRYPTION not echoed (caps 0x%08x)",
          off30.name, c->server_caps);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_off */

/* smb_encryption "available": the server offers encryption but does not ask
 * for it.  A client that encrypts on its own is answered encrypted; the
 * session is not marked ENCRYPT_DATA, so the same session's cleartext
 * requests are answered in cleartext. */
static void
run_encryption_available(const struct smb2_wire_profile *w)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st, before;

    printf("# --- %s ---\n", w->name);
    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == w->max_dialect,
          "%s: NEGOTIATE -> 0x%08x (0x%04x)", w->name, st, c->dialect);
    if (w->max_dialect == 0x0311) {
        CHECK(c->encryption_ctx && c->cipher == w->cipher,
              "%s: ENCRYPTION_CAPABILITIES context selects 0x%04x (got %d, 0x%04x)",
              w->name, w->cipher, c->encryption_ctx, c->cipher);
    } else {
        CHECK(c->server_caps & SMB2_GLOBAL_CAP_ENCRYPTION,
              "%s: SMB2_GLOBAL_CAP_ENCRYPTION echoed (caps 0x%08x)",
              w->name, c->server_caps);
    }
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);
    CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
          "%s: session not marked ENCRYPT_DATA (0x%04x)", w->name,
          c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);

    /* The client encrypts on its own (the profile arms it at SESSION_SETUP). */
    before = c->encrypted_replies;
    wire_io(c, w->name, "encrypted");
    CHECK(c->encrypted_replies >= before + 4,
          "%s: encrypted requests answered encrypted (%u of 4)", w->name,
          c->encrypted_replies - before);

    /* The same session in cleartext, signed as a client that does not
     * encrypt would. */
    c->encrypt_on = 0;
    c->signing_on = 1;
    before        = c->encrypted_replies;
    wire_io(c, w->name, "cleartext");
    CHECK(c->encrypted_replies == before,
          "%s: cleartext requests answered in cleartext (%u encrypted)",
          w->name, c->encrypted_replies - before);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_available */

/* "enabled" and "required" still force encryption: the session is marked
 * ENCRYPT_DATA. */
static void
run_encryption_forced(const struct smb2_wire_profile *w)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st, before;

    printf("# --- %s ---\n", w->name);
    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w->name, st);
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);
    CHECK(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA,
          "%s: session marked ENCRYPT_DATA (0x%04x)", w->name, c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);
    before = c->encrypted_replies;
    wire_io(c, w->name, "encrypted");
    CHECK(c->encrypted_replies >= before + 4,
          "%s: requests answered encrypted (%u of 4)", w->name,
          c->encrypted_replies - before);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_forced */

/* Off, but the share is marked encrypt_data: the server still supports
 * encryption (MS-SMB2 IsEncryptionSupported), so the client is told so -- a
 * 3.1.1 client by a cipher in the ENCRYPTION_CAPABILITIES context, a 3.0 or
 * 3.0.2 client by SMB2_GLOBAL_CAP_ENCRYPTION -- and can reach the share
 * encrypted.  The session itself is not encrypt-all; the share is what asks
 * for encryption. */
static void
run_encryption_off_share(const struct smb2_wire_profile *w)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st, before;

    printf("# --- %s ---\n", w->name);
    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");
    CHECK(chimera_server_share_set_encrypt_data(env.server, "share") == 0,
          "%s: share marked encrypt_data", w->name);
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == w->max_dialect,
          "%s: NEGOTIATE -> 0x%08x (0x%04x)", w->name, st, c->dialect);
    if (w->max_dialect == 0x0311) {
        CHECK(c->encryption_ctx && c->cipher == w->cipher,
              "%s: ENCRYPTION_CAPABILITIES context selects 0x%04x (got %d, 0x%04x)",
              w->name, w->cipher, c->encryption_ctx, c->cipher);
    } else {
        CHECK(c->server_caps & SMB2_GLOBAL_CAP_ENCRYPTION,
              "%s: SMB2_GLOBAL_CAP_ENCRYPTION echoed (caps 0x%08x)",
              w->name, c->server_caps);
    }
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);
    CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
          "%s: session not marked ENCRYPT_DATA (0x%04x)", w->name,
          c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);
    before = c->encrypted_replies;
    wire_io(c, w->name, "encrypted");
    CHECK(c->encrypted_replies >= before + 4,
          "%s: encrypted requests answered encrypted (%u of 4)", w->name,
          c->encrypted_replies - before);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_off_share */

/* "available" lets in a client that does not encrypt at all: a 3.0 client
 * that signs and never advertises encryption gets a session that is not
 * ENCRYPT_DATA, and its cleartext requests are answered in cleartext. */
static void
run_encryption_available_cleartext(void)
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile w = {
        .name = "E11 3.0 server encryption available, client signs only",
        .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1,
        .server_encryption = SMB2W_SERVER_ENC_AVAILABLE
    };
    /* *INDENT-ON* */
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st;

    printf("# --- %s ---\n", w.name);
    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == 0x0300,
          "%s: NEGOTIATE 3.0 -> 0x%08x (0x%04x)", w.name, st, c->dialect);
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w.name, st);
    CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
          "%s: session not marked ENCRYPT_DATA (0x%04x)", w.name,
          c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w.name, st);
    wire_io(c, w.name, "signed");
    CHECK(c->encrypted_replies == 0,
          "%s: answered in cleartext (%u encrypted)", w.name,
          c->encrypted_replies);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_available_cleartext */

/* A READ that asks for a compressed reply, on a request the client
 * encrypted, is answered compressed AND encrypted (MS-SMB2 3.3.4.1.4,
 * compress-then-encrypt 3.1.4.4), whether the encryption came from the
 * client's own choice under "available" or from a share marked
 * encrypt_data.  Neither marks the session ENCRYPT_DATA, which is the case a
 * compression decision keyed on that flag alone sends in cleartext. */
static void
run_encryption_compressed(
    const struct smb2_wire_profile *w,
    int                             share_encrypt)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn      *c;
    struct smb2_create_out co;
    static uint8_t         payload[32 * 1024];
    static uint8_t         readback[16 * 1024];
    uint32_t               st, count = 0, rlen = 0, enc0, comp0;
    int                    off;

    for (off = 0; off < (int) sizeof(payload); off++) {
        payload[off] = (uint8_t) ("WIREPROBE"[off % 9]);
    }

    printf("# --- %s ---\n", w->name);
    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");
    if (share_encrypt) {
        CHECK(chimera_server_share_set_encrypt_data(env.server, "share") == 0,
              "%s: share marked encrypt_data", w->name);
    }
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->compress_on && c->encryption_ctx &&
          c->cipher == w->cipher,
          "%s: NEGOTIATE selects compression and 0x%04x (0x%08x, comp %d, ctx %d)",
          w->name, w->cipher, st, c->compress_on, c->encryption_ctx);
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);
    CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
          "%s: session not marked ENCRYPT_DATA (0x%04x)", w->name,
          c->session_flags);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);
    st = smb2_create(c, "comp.dat", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: CREATE -> 0x%08x", w->name, st);
    for (off = 0; off < (int) sizeof(payload); off += 16384) {
        st = smb2_write(c, co.file_id, (uint64_t) off, payload + off, 16384,
                        &count);
        CHECK(st == ST_SUCCESS && count == 16384,
              "%s: WRITE at %d -> 0x%08x (count=%u)", w->name, off, st, count);
    }

    enc0  = c->encrypted_replies;
    comp0 = c->compressed_replies;
    st    = smb2_read(c, co.file_id, 0, sizeof(readback), readback, &rlen);
    CHECK(st == ST_SUCCESS && rlen == sizeof(readback) &&
          memcmp(readback, payload, sizeof(readback)) == 0,
          "%s: READ returns the bytes written (0x%08x, len=%u)", w->name, st,
          rlen);
    CHECK(c->compressed_replies - comp0 == 1,
          "%s: READ reply compressed (%u)", w->name,
          c->compressed_replies - comp0);
    CHECK(c->encrypted_replies - enc0 == 1,
          "%s: compressed READ reply encrypted (%u)", w->name,
          c->encrypted_replies - enc0);
    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "%s: CLOSE -> 0x%08x", w->name, st);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_encryption_compressed */

static void
run_encryption_modes(void)
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile available[] = {
        { .name = "E3 3.1.1 server encryption available, aes-128-gcm",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .cipher = SMB2W_CIPHER_AES128_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE },
        { .name = "E4 3.0 server encryption available",
          .max_dialect = 0x0300, .ntlmv2 = 1, .encrypt = 1,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE },
        { .name = "E5 3.0.2 server encryption available",
          .max_dialect = 0x0302, .ntlmv2 = 1, .encrypt = 1,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE },
    };
    static const struct smb2_wire_profile forced[] = {
        { .name = "E6 3.1.1 server encryption enabled",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .cipher = SMB2W_CIPHER_AES128_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_ENABLED },
        { .name = "E7 3.1.1 server encryption required",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .cipher = SMB2W_CIPHER_AES128_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_REQUIRED },
    };
    static const struct smb2_wire_profile off_share[] = {
        { .name = "E8 3.1.1 server encryption off, share encrypt_data",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .cipher = SMB2W_CIPHER_AES128_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_OFF },
        { .name = "E12 3.0 server encryption off, share encrypt_data",
          .max_dialect = 0x0300, .ntlmv2 = 1, .encrypt = 1,
          .server_encryption = SMB2W_SERVER_ENC_OFF },
    };
    static const struct smb2_wire_profile comp_available = {
        .name = "E9 3.1.1 server encryption available, compressed READ",
        .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
        .cipher = SMB2W_CIPHER_AES128_GCM,
        .signing_alg = SMB2W_SIGN_AES_GMAC,
        .compress = 1, .compress_alg = SMB2_COMPRESSION_LZ77,
        .server_encryption = SMB2W_SERVER_ENC_AVAILABLE
    };
    static const struct smb2_wire_profile comp_share = {
        .name = "E10 3.1.1 server encryption off, share encrypt_data, compressed READ",
        .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
        .cipher = SMB2W_CIPHER_AES128_GCM,
        .signing_alg = SMB2W_SIGN_AES_GMAC,
        .compress = 1, .compress_alg = SMB2_COMPRESSION_LZ77,
        .server_encryption = SMB2W_SERVER_ENC_OFF
    };
    /* *INDENT-ON* */
    unsigned int i;

    run_encryption_off();
    for (i = 0; i < sizeof(off_share) / sizeof(off_share[0]); i++) {
        run_encryption_off_share(&off_share[i]);
    }
    for (i = 0; i < sizeof(available) / sizeof(available[0]); i++) {
        run_encryption_available(&available[i]);
    }
    for (i = 0; i < sizeof(forced) / sizeof(forced[0]); i++) {
        run_encryption_forced(&forced[i]);
    }
    run_encryption_compressed(&comp_available, 0);
    run_encryption_compressed(&comp_share, 1);
    run_encryption_available_cleartext();
} /* run_encryption_modes */

/* The server offers encryption but no cipher the client can use: a 3.1.1
 * client gets an ENCRYPTION_CAPABILITIES context carrying cipher 0 (MS-SMB2
 * 3.3.5.4), a 3.0/3.0.2 client no SMB2_GLOBAL_CAP_ENCRYPTION.  Under
 * "available" the session goes on in cleartext; under "required" it is
 * refused. */
static void
run_cipher_none(const struct smb2_wire_profile *w)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c;
    uint32_t             st, before;
    int                  required = (w->server_encryption == SMB2W_SERVER_ENC_REQUIRED);

    printf("# --- %s ---\n", w->name);
    smb2_env_open_wire(&env, &opts, w);
    smb2_env_fs_setup(&env, "fs0");
    c  = smb2_conn_open(&env);
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS && c->dialect == w->max_dialect,
          "%s: NEGOTIATE -> 0x%08x (0x%04x)", w->name, st, c->dialect);
    if (w->max_dialect == 0x0311) {
        CHECK(c->encryption_ctx && c->cipher == 0,
              "%s: ENCRYPTION_CAPABILITIES context carries cipher 0 (got %d, 0x%04x)",
              w->name, c->encryption_ctx, c->cipher);
    } else {
        CHECK(!(c->server_caps & SMB2_GLOBAL_CAP_ENCRYPTION),
              "%s: no SMB2_GLOBAL_CAP_ENCRYPTION (caps 0x%08x)",
              w->name, c->server_caps);
    }
    st = smb2_session_setup(c);
    if (required) {
        CHECK(st == ST_ACCESS_DENIED,
              "%s: SESSION_SETUP refused -> 0x%08x", w->name, st);
    } else {
        CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w->name, st);
        CHECK(!(c->session_flags & WIRE_SESSION_FLAG_ENCRYPT_DATA),
              "%s: session not marked ENCRYPT_DATA (0x%04x)", w->name,
              c->session_flags);
        /* A client with no cipher signs instead. */
        c->encrypt_on = 0;
        c->signing_on = 1;
        st            = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
        CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w->name, st);
        before = c->encrypted_replies;
        wire_io(c, w->name, "cleartext");
        CHECK(c->encrypted_replies == before,
              "%s: cleartext requests answered in cleartext (%u encrypted)",
              w->name, c->encrypted_replies - before);
    }
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_cipher_none */

/* A SESSION_SETUP the server refuses after authenticating releases the
 * request's security buffer like any other: under AddressSanitizer a refusal
 * that skips it fails the run on the leak.  "required" refusing a session that
 * cannot encrypt is covered by C5 and C7; this is the other refusal, a
 * reconnect whose PreviousSessionId names a session of another dialect
 * (MS-SMB2 3.3.5.5.1: STATUS_USER_SESSION_DELETED). */
static void
run_reconnect_other_dialect(void)
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile w30 = {
        .name = "L1 3.0 session, then a 3.1.1 reconnect naming it",
        .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1,
    };
    static const struct smb2_wire_profile w311 = {
        .name = "L1 3.1.1 reconnect",
        .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
        .signing_alg = SMB2W_SIGN_AES_CMAC,
    };
    /* *INDENT-ON* */
    struct smb2_env      env;
    struct smb2_env_opts opts = { .leases = 1, .oplocks = 1 };
    struct smb2_conn    *c1, *c2;
    uint32_t             st;

    printf("# --- %s ---\n", w30.name);
    smb2_env_open_wire(&env, &opts, &w30);
    smb2_env_fs_setup(&env, "fs0");
    c1 = smb2_conn_open(&env);
    st = smb2_negotiate(c1);
    CHECK(st == ST_SUCCESS && c1->dialect == 0x0300,
          "%s: first NEGOTIATE -> 0x%08x (0x%04x)", w30.name, st, c1->dialect);
    st = smb2_session_setup(c1);
    CHECK(st == ST_SUCCESS, "%s: first SESSION_SETUP -> 0x%08x", w30.name, st);

    c2                  = smb2_conn_open(&env);
    c2->wire            = &w311;
    c2->prev_session_id = c1->session_id;
    st                  = smb2_negotiate(c2);
    CHECK(st == ST_SUCCESS && c2->dialect == 0x0311,
          "%s: reconnect NEGOTIATE -> 0x%08x (0x%04x)", w30.name, st, c2->dialect);
    st = smb2_session_setup(c2);
    CHECK(st == 0xC0000203u,
          "%s: reconnect SESSION_SETUP -> 0x%08x (want USER_SESSION_DELETED)",
          w30.name, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_reconnect_other_dialect */

/* smb_encryption_ciphers: the server selects from the client's list in the
 * client's order, skipping any cipher the setting leaves out. */
static void
run_cipher_list(void)
{
    /* *INDENT-OFF* */
    static const struct smb2_wire_profile selected[] = {
        /* .cipher is the cipher the server must select. */
        { .name = "C1 3.1.1 client prefers aes-128-ccm over aes-128-gcm",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .ciphers = { SMB2W_CIPHER_AES128_CCM, SMB2W_CIPHER_AES128_GCM },
          .ncipher = 2, .cipher = SMB2W_CIPHER_AES128_CCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE },
        { .name = "C2 3.1.1 server allows only aes-256-gcm, client lists it second",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .ciphers = { SMB2W_CIPHER_AES128_GCM, SMB2W_CIPHER_AES256_GCM },
          .ncipher = 2, .cipher = SMB2W_CIPHER_AES256_GCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_256_GCM },
        { .name = "C3 3.1.1 client's first cipher not allowed, second taken",
          .max_dialect = 0x0311, .ntlmv2 = 1, .encrypt = 1,
          .ciphers = { SMB2W_CIPHER_AES256_GCM, SMB2W_CIPHER_AES128_CCM,
                       SMB2W_CIPHER_AES128_GCM },
          .ncipher = 3, .cipher = SMB2W_CIPHER_AES128_CCM,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_128_CCM |
                            CHIMERA_SMB_CIPHER_AES_128_GCM },
    };
    static const struct smb2_wire_profile none[] = {
        { .name = "C4 3.1.1 available, no allowed cipher in the client's list",
          .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
          .ciphers = { SMB2W_CIPHER_AES128_GCM, SMB2W_CIPHER_AES128_CCM },
          .ncipher = 2,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_256_GCM },
        { .name = "C5 3.1.1 required, no allowed cipher in the client's list",
          .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
          .ciphers = { SMB2W_CIPHER_AES128_GCM },
          .ncipher = 1,
          .signing_alg = SMB2W_SIGN_AES_GMAC,
          .server_encryption = SMB2W_SERVER_ENC_REQUIRED,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_256_GCM },
        { .name = "C6 3.0 available, aes-128-ccm not allowed",
          .max_dialect = 0x0300, .ntlmv2 = 1, .encrypt = 1,
          .server_encryption = SMB2W_SERVER_ENC_AVAILABLE,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_128_GCM |
                            CHIMERA_SMB_CIPHER_AES_256_GCM },
        { .name = "C7 3.0.2 required, aes-128-ccm not allowed",
          .max_dialect = 0x0302, .ntlmv2 = 1, .encrypt = 1,
          .server_encryption = SMB2W_SERVER_ENC_REQUIRED,
          .server_ciphers = CHIMERA_SMB_CIPHER_AES_256_GCM },
    };
    /* *INDENT-ON* */
    unsigned int i;

    for (i = 0; i < sizeof(selected) / sizeof(selected[0]); i++) {
        run_encryption_available(&selected[i]);
    }
    for (i = 0; i < sizeof(none) / sizeof(none[0]); i++) {
        run_cipher_none(&none[i]);
    }
} /* run_cipher_list */

#ifdef SMB2W_WBSTUB
/* The account the stub answers for. */
#define WINBIND_USER        WBSTUB_USER
#define WINBIND_PASSWORD    WBSTUB_PASSWORD
#else /* ifdef SMB2W_WBSTUB */
/* The domain user scripts/ad_test_wrapper.sh creates. */
#define WINBIND_USER        "testuser1"
#define WINBIND_PASSWORD    "Password1!"
#endif /* ifdef SMB2W_WBSTUB */

/* STATUS_LOGON_FAILURE.  The server returns it for every logon winbind
 * refuses, whatever the reason, so on its own it does not say the password
 * was the reason.  The WB profiles that log on with the right password in the
 * same run are what show that. */
#define WB_ST_LOGON_FAILURE 0xc000006du

/* The winbind counterpart of W9: the domain controller (or the stub playing
 * one) must refuse a proof made with the wrong password, or the WB profiles
 * passing would show nothing about the winbind branch's checks. */
static void
run_bad_winbind_password(const char *domain)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;
    uint32_t             st;

    /* *INDENT-OFF* */
    const struct smb2_wire_profile w = {
        .name = "WB9 bad password through winbind",
        .max_dialect = 0x0300, .ntlmv2 = 1, .winbind_domain = domain
    };
    /* *INDENT-ON* */

    printf("# --- %s ---\n", w.name);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w.name, st);

    st = smb2_session_setup_as(c, WINBIND_USER, "not-the-password");
    CHECK(st == WB_ST_LOGON_FAILURE,
          "%s: SESSION_SETUP is REFUSED as a logon failure (0x%08x)", w.name, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_bad_winbind_password */

#ifdef SMB2W_WBSTUB
/* The identity half of the winbind logon.  A file the domain user creates must
 * be owned by the user's SID, with the primary group's SID as its group.  The
 * owner is the SID the logon returned.  The group comes back through the gid
 * it mapped to, so a primary group that failed to map (the server then falls
 * back to gid = uid) shows here as the uid's algorithmic SID. */
static void
run_winbind_identity(const char *domain)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { 0 };
    struct smb2_conn      *c;
    struct smb2_create_out co;
    struct smb2_sd         sd;
    uint8_t                buf[2048];
    uint32_t               st, len = 0;

    /* *INDENT-OFF* */
    const struct smb2_wire_profile w = {
        .name = "WB15 a new file is owned by the domain user",
        .max_dialect = 0x0311, .ntlmv2 = 1, .sign = 1,
        .signing_alg = SMB2W_SIGN_AES_GMAC, .winbind_domain = domain
    };
    /* *INDENT-ON* */

    printf("# --- %s ---\n", w.name);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", w.name, st);

    st = smb2_session_setup_as(c, WINBIND_USER, WINBIND_PASSWORD);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", w.name, st);

    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", w.name, st);

    st = smb2_create(c, "owned.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: CREATE -> 0x%08x", w.name, st);

    if (st == ST_SUCCESS) {
        st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                             SMB2_SEC_OWNER | SMB2_SEC_GROUP, buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS, "%s: QUERY_INFO security -> 0x%08x", w.name, st);

        if (st == ST_SUCCESS && smb2_sd_parse(buf, len, &sd) == 0) {
            CHECK(sd.have_owner && strcmp(sd.owner, WBSTUB_USER_SID) == 0,
                  "%s: owner is the user's SID %s (got %s)", w.name,
                  WBSTUB_USER_SID, sd.have_owner ? sd.owner : "none");
            CHECK(sd.have_group && strcmp(sd.group, WBSTUB_GROUP_SID) == 0,
                  "%s: group is the primary group's SID %s (got %s)", w.name,
                  WBSTUB_GROUP_SID, sd.have_group ? sd.group : "none");
        } else if (st == ST_SUCCESS) {
            CHECK(0, "%s: security descriptor parses (%u bytes)", w.name, len);
        }

        st = smb2_close(c, co.file_id);
        CHECK(st == ST_SUCCESS, "%s: CLOSE -> 0x%08x", w.name, st);
    }

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_winbind_identity */
#endif /* ifdef SMB2W_WBSTUB */

static int
run_winbind(void)
{
#ifdef SMB2W_WBSTUB
    const char *domain = WBSTUB_DOMAIN;
#else /* ifdef SMB2W_WBSTUB */
    const char *domain     = getenv("AD_DOMAIN");
    const char *socket_dir = getenv("WINBINDD_SOCKET_DIR");

    if (!domain || !socket_dir) {
        printf("# SKIP: --winbind needs AD_DOMAIN and WINBINDD_SOCKET_DIR "
               "(run under scripts/ad_test_wrapper.sh)\n");
        return 77;
    }
#endif /* ifdef SMB2W_WBSTUB */

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
    run_bad_winbind_password(domain);
#ifdef SMB2W_WBSTUB
    run_winbind_identity(domain);

    /* Every check above can fail for one reason: the server's libwbclient
     * calls bound to the real library, not the stub.  Name it. */
    CHECK(wbstub_auth_calls > 0,
          "the server's winbind logons reached the stub (%u calls)",
          wbstub_auth_calls);
#endif /* ifdef SMB2W_WBSTUB */
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
        run_encryption_modes();
        run_cipher_list();
        run_reconnect_other_dialect();
    }

    if (failures) {
        fprintf(stderr, "%d wire-protection check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 wire-protection checks passed\n");
    return 0;
} /* main */
