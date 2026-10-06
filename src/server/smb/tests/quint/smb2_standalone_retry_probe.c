// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "common/compound_retry.h"
#include "vfs/vfs_internal_procs.h"

/* Exercise the fallback handlers even when the shared builder supports the
 * command. Rejections are read-only or stop a mutation before dispatch; these
 * tests do not assume memfs can roll back filesystem changes. */
struct chimera_smb_compound;
static atomic_int   bypass, armed, attempts, submissions, speculative_write;
static int          target_type, reject_count, reject_error, stop_mutation;
static unsigned int expected_groups;

/* A minimal transactional WRITE provider: the speculative operation succeeds
 * without publishing data, and finish aborts it. This exercises success at the
 * operation layer followed by rejection without pretending memfs rolled back. */
__attribute__((visibility("default"))) void
chimera_vfs_write_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_mask,
    uint64_t                          post_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_write_callback_t      callback,
    void                             *arg)
{
    if (atomic_load(&speculative_write)) {
        struct chimera_vfs_attrs pre = { 0 }, post = { 0 };
        callback(CHIMERA_VFS_OK, count, sync, &pre, &post, arg);
        return;
    }
    __typeof__(&chimera_vfs_write_view) next = dlsym(RTLD_NEXT, "chimera_vfs_write_view");
    assert(next);
    next(thread, cred, handle, offset, count, sync, pre_mask, post_mask, iov, niov,
         view, callback, arg);
} /* chimera_vfs_write_view */

__attribute__((visibility("default"))) int
chimera_smb_vfs_compound_try(struct chimera_smb_compound *wire)
{
    typedef int (*try_fn)(
        struct chimera_smb_compound *);
    try_fn next = dlsym(RTLD_NEXT, "chimera_smb_vfs_compound_try");
    assert(next);
    return atomic_load(&bypass) ? 0 : next(wire);
} /* chimera_smb_vfs_compound_try */

static void
prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *arg)
{
    (void) cp; (void) index; (void) arg;
    if (atomic_load(&attempts) < reject_count) {
        *status = CHIMERA_VFS_EACCES;
    }
} /* prepare */

static void
finish(
    struct chimera_vfs_compound *cp,
    void                        *arg)
{
    (void) arg;
    int attempt = atomic_fetch_add(&attempts, 1);
    chimera_vfs_compound_finish_result(cp,
                                       attempt < reject_count ? reject_error : CHIMERA_VFS_OK);
} /* finish */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *cp,
    chimera_vfs_compound_callback_t callback,
    void                           *arg)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn next = dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        for (unsigned int i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
            if (chimera_vfs_compound_op(cp, i)->type != target_type) {
                continue;
            }
            atomic_fetch_add(&submissions, 1);
            if (expected_groups) {
                assert(chimera_vfs_compound_num_groups(cp) == expected_groups);
            }
            if (stop_mutation) {
                chimera_vfs_compound_set_op_prepare(cp, i, prepare, NULL);
            }
            chimera_vfs_compound_set_finish_handler(cp, finish, NULL);
            break;
        }
    }
    next(cp, callback, arg);
} /* chimera_vfs_compound_submit */

static void
arm(
    int  type,
    int  rejects,
    int  error,
    bool mutation)
{
    target_type     = type; reject_count = rejects; reject_error = error;
    stop_mutation   = mutation;
    expected_groups = 0;
    atomic_store(&attempts, 0); atomic_store(&submissions, 0);
    atomic_store(&armed, 1);
} /* arm */

static void check(
    int wanted);

static void
doc_chain(struct smb2_conn *conn)
{
    int          length = smb2c_build_create_full(conn, "doc-chain", MBT_FILE_CREATE,
                                                  MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, 0,
                                                  NULL, NULL, 0);
    uint8_t     *wire   = conn->sbuf + 4;
    uint64_t     mid    = g64(wire, 24);
    unsigned int offset = (SMB2_HDR_SIZE + length + 7) & ~7u;

    memset(wire + SMB2_HDR_SIZE + length, 0, offset - SMB2_HDR_SIZE - length);
    p32(wire, 20, offset);
    for (unsigned int i = 0; i < 2; i++) {
        uint8_t *header = wire + offset, *body = header + SMB2_HDR_SIZE;
        memset(header, 0, i ? 112 : 120);
        memcpy(header, "\xfeSMB", 4); p16(header, 4, SMB2_HDR_SIZE); p16(header, 6, 1);
        p16(header, 12, i ? SMB2_READ : SMB2_WRITE); p16(header, 14, 32);
        p32(header, 16, SMB2_FLAGS_RELATED_OPERATIONS); p32(header, 20, i ? 0 : 120);
        p64(header, 24, mid + i + 1); p32(header, 36, UINT32_MAX); p64(header, 40, UINT64_MAX);
        p16(body, 0, 49); p32(body, 4, 4); memset(body + 16, 0xff, 16);
        if (!i) {
            p16(body, 2, 112); memcpy(body + 48, "data", 4);
        }
        offset += i ? 112 : 120;
    }
    arm(CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS, 0, CHIMERA_VFS_OK, false);
    expected_groups                                        = 3;
    smb2c_send(conn, offset - SMB2_HDR_SIZE); conn->msg_id = mid + 3; smb2c_wait(conn);
    check(1);
    struct smb2_create_out created;
    smb2c_parse_create(conn, &created);
    assert(created.status == ST_SUCCESS);
    const uint8_t         *reply = conn->rbuf + 4;
    reply += g32(reply, 20);
    assert(g32(reply, 8) == ST_SUCCESS && g32(reply + SMB2_HDR_SIZE, 4) == 4);
    reply += g32(reply, 20);
    assert(g32(reply, 8) == ST_SUCCESS && g32(reply + SMB2_HDR_SIZE, 4) == 4);
    assert(!memcmp(reply + reply[SMB2_HDR_SIZE + 2], "data", 4));
    assert(smb2_close(conn, created.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "doc-chain", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &created) == ST_OBJECT_NAME_NOT_FOUND);
} /* doc_chain */

static void
check(int wanted)
{
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    assert(atomic_load(&attempts) == wanted);
} /* check */

/* A namespace-only DOC opener completes after the break is sent. Its peer
 * need not acknowledge before CREATE returns; CLOSE still settles deletion. */
static void
doc_notify_only(struct smb2_conn *conn)
{
    struct smb2_conn      *peer = smb2_conn_open(conn->env);
    struct smb2_create_out holder, deleting;
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RH };
    struct smb2_break      brk;

    memset(lease.lease_key, 0x6d, sizeof(lease.lease_key));
    smb2_handshake(peer);
    assert(smb2_create(peer, "doc-notify", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
    assert(holder.lease_state == SMB2_LEASE_RH);
    arm(CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS, 0, CHIMERA_VFS_OK, false);
    assert(smb2_create_opts(conn, "doc-notify", MBT_FILE_OPEN,
                            0x00010000u /* DELETE */ | MBT_FILE_READ_ATTRIBUTES,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL,
                            &deleting) == ST_SUCCESS);
    check(1);
    assert(smb2_conn_pop_break(peer, &brk) && brk.is_lease && brk.new_state == SMB2_LEASE_READ);
    if (brk.ack_required) {
        assert(smb2_lease_break_ack(peer, brk.lease_key, brk.new_state) == ST_SUCCESS);
    }
    assert(smb2_close(peer, holder.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, deleting.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "doc-notify", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &holder) == ST_OBJECT_NAME_NOT_FOUND);
} /* doc_notify_only */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .oplocks = 1, .leases = 1, .named_streams = 1 };

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn      *conn = smb2_conn_open(&env);
    smb2_handshake(conn);
    struct smb2_create_out file, dir, doc;
    uint8_t                out[8192], expected[8192];
    uint32_t               length, expected_length, written;
    assert(smb2_create(conn, "retry-file", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &file) == ST_SUCCESS);
    assert(smb2_write(conn, file.file_id, 0, "original", 8, &written) == ST_SUCCESS);
    assert(smb2_create_opts(conn, "retry-dir", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    atomic_store(&bypass, 1);

    /* A fallback CREATE can retry before its live CLAIM barrier. Once that
    * barrier executes, rejected finish must discard admission and fail. */
    struct smb2_create_out retry_open;
    arm(CHIMERA_VFS_COMPOUND_OP_OPEN, 2, CHIMERA_VFS_EAGAIN, true);
    assert(smb2_create(conn, "retry-file", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &retry_open) == ST_SUCCESS);
    check(3);
    assert(smb2_close(conn, retry_open.file_id) == ST_SUCCESS);
    arm(CHIMERA_VFS_COMPOUND_OP_CLAIM, 100, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_create(conn, "retry-file", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &retry_open) != ST_SUCCESS);
    check(1);
    assert(smb2_create(conn, "retry-file", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &retry_open) == ST_SUCCESS);
    assert(smb2_close(conn, retry_open.file_id) == ST_SUCCESS);

    arm(CHIMERA_VFS_COMPOUND_OP_READ, 2, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_read(conn, file.file_id, 0, sizeof(out), out, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(out, "original", 8)); check(3);
    arm(CHIMERA_VFS_COMPOUND_OP_GETATTR, 2, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                           file.file_id, 0, out, sizeof(out), &length) == ST_SUCCESS);
    assert(g64(out, 8) == 8); check(3);

    uint8_t ea[16] = { 0 };
    ea[5] = 3; p16(ea, 6, 3);
    memcpy(ea + 8, "key", 3); memcpy(ea + 12, "val", 3);
    assert(smb2_set_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                         file.file_id, ea, 15) == ST_SUCCESS);
    arm(CHIMERA_VFS_COMPOUND_OP_GETXATTR, 2, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_query_eas_restart(conn, file.file_id, out, sizeof(out), &length) == ST_SUCCESS);
    assert(length >= 15 && !memcmp(out + 8, "KEY", 3) && !memcmp(out + 12, "val", 3));
    check(3);
    arm(CHIMERA_VFS_COMPOUND_OP_GETXATTR, 100, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_query_eas_restart(conn, file.file_id, out, sizeof(out), &length) != ST_SUCCESS);
    assert(!length); check(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);

    arm(CHIMERA_VFS_COMPOUND_OP_RENAME, 2, CHIMERA_VFS_EAGAIN, true);
    assert(smb2_rename(conn, file.file_id, "renamed-file", false) == ST_SUCCESS);
    check(3);
    arm(CHIMERA_VFS_COMPOUND_OP_LOOKUP, 100, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_rename(conn, file.file_id, "rejected-file", false) != ST_SUCCESS);
    check(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    assert(smb2_rename(conn, file.file_id, "retry-file", false) == ST_SUCCESS);

    assert(smb2_query_directory(conn, 12, 1, dir.file_id, "*", sizeof(expected),
                                expected, sizeof(expected), &expected_length) == ST_SUCCESS);
    arm(CHIMERA_VFS_COMPOUND_OP_READDIR, 2, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_query_directory(conn, 12, 1, dir.file_id, "*", sizeof(out),
                                out, sizeof(out), &length) == ST_SUCCESS);
    assert(length == expected_length && !memcmp(out, expected, length)); check(3);

    arm(CHIMERA_VFS_COMPOUND_OP_WRITE, 2, CHIMERA_VFS_EAGAIN, true);
    assert(smb2_write(conn, file.file_id, 0, "accepted", 8, &written) == ST_SUCCESS);
    assert(written == 8); check(3);
    arm(CHIMERA_VFS_COMPOUND_OP_WRITE, 100, CHIMERA_VFS_EAGAIN, true);
    assert(smb2_write(conn, file.file_id, 0, "rejected", 8, &written) != ST_SUCCESS);
    check(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    assert(smb2_read(conn, file.file_id, 0, sizeof(out), out, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(out, "accepted", 8));

    atomic_store(&speculative_write, 1);
    arm(CHIMERA_VFS_COMPOUND_OP_WRITE, 100, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_write(conn, file.file_id, 0, "rejected", 8, &written) != ST_SUCCESS);
    check(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    arm(CHIMERA_VFS_COMPOUND_OP_WRITE, 1, CHIMERA_VFS_EIO, false);
    assert(smb2_write(conn, file.file_id, 0, "rejected", 8, &written) != ST_SUCCESS);
    check(1);
    atomic_store(&speculative_write, 0);
    assert(smb2_read(conn, file.file_id, 0, sizeof(out), out, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(out, "accepted", 8));

    arm(CHIMERA_VFS_COMPOUND_OP_COMMIT, 2, CHIMERA_VFS_EAGAIN, true);
    assert(smb2_flush(conn, file.file_id) == ST_SUCCESS); check(3);
    arm(CHIMERA_VFS_COMPOUND_OP_READ, 100, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_read(conn, file.file_id, 0, sizeof(out), out, &length) != ST_SUCCESS);
    check(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);

    /* Native CREATE-time DOC must not arm a rejected attempt's handle: freeing
     * it would unlink the name before retry. Only final accepted CLOSE deletes. */
    atomic_store(&bypass, 0);
    doc_chain(conn);
    doc_notify_only(conn);
    assert(smb2_close(conn, file.file_id) == ST_SUCCESS);
    arm(CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS, 2, CHIMERA_VFS_EAGAIN, false);
    assert(smb2_create_opts(conn, "retry-file", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &doc) == ST_SUCCESS);
    check(3);
    assert(smb2_read(conn, doc.file_id, 0, sizeof(out), out, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(out, "accepted", 8));
    assert(smb2_close(conn, doc.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "retry-file", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &file) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_close(conn, dir.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    return 0;
} /* main */
