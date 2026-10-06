// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include <stdatomic.h>
#include "smb2_mbt_common.h"

extern atomic_int lifetime_arm, lifetime_held, lifetime_resume, lifetime_attempts;
extern atomic_int lifetime_fallback_close, lifetime_native_closes;

static uint32_t
replay_bind_leg(
    struct smb2_conn *c,
    const uint8_t    *blob,
    int               length)
{
    int      offset = smb2c_begin(c, SMB2_SESSION_SETUP, 0);
    uint8_t *body   = c->sbuf + offset;

    p16(body, 0, 25); body[2] = 1; body[3] = 1;
    p16(body, 12, SMB2_HDR_SIZE + 24); p16(body, 14, length);
    memcpy(body + 24, blob, length);
    return smb2c_xfer(c, 24 + length);
} /* replay_bind_leg */

static struct smb2_conn *
replay_bind_channel(
    struct smb2_env  *env,
    struct smb2_conn *first)
{
    struct smb2_conn *c = smb2_conn_open(env);

    c->guid_tag = first->guid_tag;
    assert(smb2_negotiate(c) == ST_SUCCESS && c->dialect == 0x0300);
    c->session_id = first->session_id; c->tree_id = first->tree_id;
    memcpy(c->signing_key, first->signing_key, sizeof(c->signing_key));
    c->signing_on = 1;
    uint8_t           blob[2048];
    int               length = smb2w_ntlm_negotiate(blob, false);
    assert(replay_bind_leg(c, blob, length) == ST_MORE_PROCESSING_REQUIRED);
    const uint8_t    *body = c->rbuf + 4 + SMB2_HDR_SIZE;
    uint16_t          offset = g16(body, 4), size = g16(body, 6);
    length = smb2w_ntlm_auth_ntlmv2(c->rbuf + 4 + offset, size,
                                    SMB2W_USER, SMB2W_PASSWORD, SMB2W_DOMAIN, blob, c->session_key);
    assert(replay_bind_leg(c, blob, length) == ST_SUCCESS);
    smb2c_arm_protection(c, NULL);
    return c;
} /* replay_bind_channel */


static void
run_case(
    struct smb2_env  *env,
    struct smb2_conn *conn,
    struct smb2_conn *channel,
    bool              ea,
    bool              retry,
    bool              fallback_close,
    const char       *name,
    bool              doc)
{
    struct smb2_create_out file, reopened;
    uint32_t               written;

    assert(smb2_create_opts(conn, name, MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, doc ? MBT_FILE_DELETE_ON_CLOSE : 0,
                            NULL, &file) == ST_SUCCESS);
    assert(smb2_write(conn, file.file_id, 0, "original", 8, &written) == ST_SUCCESS);
    assert(written == 8);
    uint8_t                value[16] = { 0 };
    value[5] = 3; p16(value, 6, 3);
    memcpy(value + 8, "key", 3); memcpy(value + 12, "val", 3);
    if (ea) {
        assert(smb2_set_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                             file.file_id, value, 15) == ST_SUCCESS);
        assert(smb2_close(conn, file.file_id) == ST_SUCCESS);
        assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES | 8,
                           MBT_FILE_SHARE_RWD, NULL, &file) == ST_SUCCESS);
    }
    atomic_store(&lifetime_held, 0); atomic_store(&lifetime_resume, 0);
    atomic_store(&lifetime_attempts, 0);
    atomic_store(&lifetime_arm, retry ? 2 : 1);
    atomic_store(&lifetime_fallback_close, fallback_close);
    int      offset = smb2c_begin(conn, ea ? SMB2_QUERY_INFO : SMB2_READ, 0);
    uint8_t *body   = conn->sbuf + offset;
    if (ea) {
        p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_FULL_EA_INFO_T;
        p32(body, 4, 1024); memcpy(body + 24, file.file_id, 16);
    } else {
        p16(body, 0, 49); body[2] = SMB2_HDR_SIZE + 16;
        p32(body, 4, 8); memcpy(body + 16, file.file_id, 16);
    }
    smb2c_send(conn, ea ? 40 : 49);
    uint64_t       deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&lifetime_held)) {
        assert(smb2c_now_ms() < deadline && !conn->reply_ready);
        smb2_pump(env);
    }
    assert(smb2_close(channel, file.file_id) == ST_SUCCESS);
    uint8_t        out[16]; uint32_t length;
    assert(smb2_read(channel, file.file_id, 0, sizeof(out), out, &length) == ST_FILE_CLOSED);
    assert(!conn->reply_ready);
    atomic_store(&lifetime_resume, 1);
    smb2c_wait(conn);
    fprintf(stderr, "case %s ea=%d retry=%d fallback_close=%d status=%08x\n",
            name, ea, retry, fallback_close, g32(conn->rbuf + 4, 8));
    assert(g32(conn->rbuf + 4, 8) == ST_SUCCESS);
    const uint8_t *reply       = conn->rbuf + 4 + SMB2_HDR_SIZE;
    unsigned       data_offset = ea ? g16(reply, 2) : reply[2];
    assert(ea ? g32(reply, 4) >= 15 : g32(reply, 4) == 8);
    assert(!memcmp(conn->rbuf + 4 + data_offset, ea ? value : (const uint8_t *) "original",
                   ea ? 15 : 8));
    assert(atomic_load(&lifetime_attempts) == (retry ? 2 : 1));
    atomic_store(&lifetime_fallback_close, 0);
    if (doc) {
        assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_OBJECT_NAME_NOT_FOUND);
    }
} /* run_case */

static void
copy_case(
    struct smb2_env  *env,
    struct smb2_conn *conn,
    struct smb2_conn *channel,
    bool              fallback_close)
{
    struct smb2_create_out src, dst, reopened;
    uint32_t               length, status;

    assert(smb2_create(conn, "copy-src", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &src) == ST_SUCCESS);
    assert(smb2_create(conn, "copy-dst", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &dst) == ST_SUCCESS);
    assert(smb2_write(conn, src.file_id, 0, "abcdefgh", 8, &length) == ST_SUCCESS && length == 8);
    const uint8_t         *key = smb2_ioctl_out(conn, SMB2_FSCTL_SRV_REQUEST_RESUME_KEY,
                                                src.file_id, NULL, 0, 4096, &status, &length);
    assert(status == ST_SUCCESS && key && length >= 24);
    uint8_t                input[80] = { 0 };
    memcpy(input, key, 24); p32(input, 24, 2);
    p64(input, 32, 4); p64(input, 40, 0); p32(input, 48, 4);
    p64(input, 56, 0); p64(input, 64, 4); p32(input, 72, 4);
    atomic_store(&lifetime_held, 0); atomic_store(&lifetime_resume, 0);
    atomic_store(&lifetime_attempts, 0); atomic_store(&lifetime_arm, 2);
    atomic_store(&lifetime_fallback_close, fallback_close);
    int                    offset = smb2c_begin(conn, SMB2_IOCTL, 0);
    uint8_t               *body   = conn->sbuf + offset;
    p16(body, 0, 57); p32(body, 4, SMB2_FSCTL_SRV_COPYCHUNK);
    memcpy(body + 8, dst.file_id, 16); p32(body, 24, SMB2_HDR_SIZE + 56);
    p32(body, 28, sizeof(input)); p32(body, 44, 4096); p32(body, 48, SMB2C_IOCTL_IS_FSCTL);
    memcpy(body + 56, input, sizeof(input));
    smb2c_send(conn, 56 + sizeof(input));
    uint64_t               deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&lifetime_held)) {
        assert(smb2c_now_ms() < deadline && !conn->reply_ready);
        smb2_pump(env);
    }
    /* The held source GETATTR is read-only. Retry it after both FileIds have
     * closed, then build and execute two real mutating chunk compounds. */
    assert(smb2_close(channel, src.file_id) == ST_SUCCESS);
    assert(smb2_close(channel, dst.file_id) == ST_SUCCESS);
    atomic_store(&lifetime_resume, 1); smb2c_wait(conn);
    assert(g32(conn->rbuf + 4, 8) == ST_SUCCESS);
    const uint8_t *reply = conn->rbuf + 4 + SMB2_HDR_SIZE;
    assert(g32(reply, 36) == 12);
    const uint8_t *result = conn->rbuf + 4 + g32(reply, 32);
    assert(g32(result, 0) == 2 && g32(result, 8) == 8);
    assert(atomic_load(&lifetime_attempts) == 2);
    atomic_store(&lifetime_fallback_close, 0);
    assert(smb2_create(conn, "copy-dst", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_SUCCESS);
    uint8_t        data[8];
    assert(smb2_read(conn, reopened.file_id, 0, 8, data, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(data, "efghabcd", 8));
    assert(smb2_close(conn, reopened.file_id) == ST_SUCCESS);
} /* copy_case */

int
main(void)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .named_streams = 1 };

    smb2_env_open_wire(&env, &opts, smb2_wire_profile_find("signed30"));
    smb2_env_fs_setup(&env, "fs0");
    struct smb2_conn    *conn    = smb2_conn_open(&env); smb2_handshake(conn);
    struct smb2_conn    *channel = replay_bind_channel(&env, conn);
    /* First case reproduces the lost handle when LISTXATTRS finish
     * resumes and the next GETXATTR compound borrows the now-closed handle. */
    for (unsigned fallback = 0; fallback < 2; fallback++) {
        for (unsigned retry = 0; retry < 2; retry++) {
            run_case(&env, conn, channel, true, retry, fallback, "ea-file", false);
            run_case(&env, conn, channel, false, retry, fallback, "read-file", false);
            run_case(&env, conn, channel, false, retry, fallback, "stream-base:fork", false);
        }
        /* A successful read may finish after DOC; do not fake a rollback of
         * the concurrently accepted unlink by retrying that read. */
        run_case(&env, conn, channel, false, false, fallback, "doc-file", true);
        copy_case(&env, conn, channel, fallback);
    }
    assert(atomic_load(&lifetime_native_closes) >= 7);
    smb2_conn_reset(&env);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    return 0;
} /* main */
