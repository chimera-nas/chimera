// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Real NextCommand-chained SMB packets, unlike the model replayer's individual
 * command round trips. Finish rejection is restricted to QUERY_INFO/READ: no
 * test here claims rollback of an executed filesystem mutation. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#include <stddef.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"
#include "common/compound_retry.h"

static atomic_int   armed, submissions, attempts, held, release_finish;
static int          reject_count, pause_finish, operation_error;
static unsigned int expected_groups;
static              _Thread_local struct evpl *owner_evpl;

struct injection {
    chimera_vfs_compound_callback_t    callback;
    void                              *private_data;
    unsigned int                       seen;
    chimera_vfs_compound_op_callback_t prepare;
    void                              *prepare_private;
    struct evpl                       *evpl;
    struct evpl_timer                  timer;
    struct chimera_vfs_compound       *held_compound;
    enum chimera_vfs_error held_status;
};

/* Allocation and submission run on the compound's owning VFS thread. */
__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*alloc_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *);
    alloc_fn next = (alloc_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next);
    owner_evpl = thread->evpl;
    return next(thread, cred);
} /* chimera_vfs_compound_alloc */

/* A one-shot timer is removed before entry, so accepting/retrying may free
 * this fixture. Re-arming waits for explicit client release, not elapsed time. */
static void
finish_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct injection *ctx = (struct injection *)
        ((char *) timer - offsetof(struct injection, timer));

    if (!atomic_exchange(&release_finish, 0)) {
        evpl_add_oneshot_timer(evpl, timer, finish_poll, 1000);
        return;
    }
    assert(atomic_load(&held));
    atomic_store(&held, 0);
    chimera_vfs_compound_finish_result(ctx->held_compound, ctx->held_status);
} /* finish_poll */

static void
read_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct injection *ctx = private_data;

    if (ctx->prepare) {
        ctx->prepare(cp, index, status, ctx->prepare_private);
    }
    if (*status == CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_EAGAIN;
    }
} /* read_prepare */

static void
finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct injection      *ctx    = private_data;
    enum chimera_vfs_error status = ctx->seen++ < reject_count ?
        CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK;

    atomic_store(&attempts, ctx->seen);
    if (pause_finish && ctx->seen == 1) {
        ctx->held_compound = cp;
        ctx->held_status   = status;
        assert(ctx->evpl);
        evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, finish_poll, 1000);
        atomic_store(&held, 1);
        return;
    }
    chimera_vfs_compound_finish_result(cp, status);
} /* finish */

static void
complete(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct injection               *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(cp) != CHIMERA_VFS_EAGAIN ||
        ctx->seen == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1) {
        free(ctx);
    }
    callback(cp, arg);
} /* complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *cp,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn         next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (!atomic_load(&armed)) {
        next(cp, callback, private_data);
        return;
    }
    assert(atomic_fetch_add(&submissions, 1) == 0);
    assert(chimera_vfs_compound_num_groups(cp) == expected_groups);
    struct injection *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->callback     = callback;
    ctx->private_data = private_data;
    ctx->evpl         = owner_evpl;
    int               injected = 0;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, i);
        /* FLUSH is exercised only with accepted finish. */
        assert(!reject_count || op->type != CHIMERA_VFS_COMPOUND_OP_COMMIT);
        if (operation_error && !injected && op->type == CHIMERA_VFS_COMPOUND_OP_READ) {
            ctx->prepare         = op->prepare;
            ctx->prepare_private = op->prepare_private;
            chimera_vfs_compound_set_op_prepare(cp, i, read_prepare, ctx);
            injected = 1;
        }
    }
    assert(!operation_error || injected);
    chimera_vfs_compound_set_finish_handler(cp, finish, ctx);
    next(cp, complete, ctx);
} /* chimera_vfs_compound_submit */

struct command {
    uint16_t       opcode;
    const uint8_t *fid;
    uint32_t       flags;
    uint64_t       offset;
    uint32_t       status;
};

/* Sign each NextCommand span independently, then send the linked packet. */
static uint64_t
send_compound(
    struct smb2_conn     *c,
    const struct command *cmd,
    unsigned int          count)
{
    uint8_t      packet[2048] = {
        0
    };
    uint64_t     first_mid = c->msg_id;
    unsigned int size      = 0;

    assert(count);
    for (unsigned int i = 0; i < count; i++) {
        uint8_t     *h = packet + size;
        uint8_t     *b = h + SMB2_HDR_SIZE;
        unsigned int body_len;
        memcpy(h, "\xfeSMB", 4);
        p16(h, 4, SMB2_HDR_SIZE);
        p16(h, 6, 1);
        p16(h, 12, cmd[i].opcode);
        p16(h, 14, 32);
        p32(h, 16, cmd[i].flags);
        p64(h, 24, first_mid + i);
        p32(h, 36, (cmd[i].flags & SMB2_FLAGS_RELATED_OPERATIONS) ? UINT32_MAX : c->tree_id);
        p64(h, 40, (cmd[i].flags & SMB2_FLAGS_RELATED_OPERATIONS) ? UINT64_MAX : c->session_id);
        switch (cmd[i].opcode) {
            case SMB2_CREATE: {
                const char  *name     = "compound-created.txt";
                unsigned int name_len = strlen(name);
                p16(b, 0, 57);
                p32(b, 4, 2); /* Impersonation */
                p32(b, 24, cmd[i].offset ? MBT_FILE_READ_ATTRIBUTES : MBT_FILE_ALL_ACCESS);
                p32(b, 32, MBT_FILE_SHARE_RWD);
                p32(b, 36, cmd[i].offset ? MBT_FILE_OPEN : MBT_FILE_CREATE);
                p32(b, 40, MBT_FILE_NON_DIRECTORY_FILE);
                p16(b, 44, SMB2_HDR_SIZE + 56);
                p16(b, 46, name_len * 2);
                for (unsigned int j = 0; j < name_len; j++) {
                    p16(b, 56 + j * 2, name[j]);
                }
                body_len = 56 + name_len * 2;
                break;
            }
            case SMB2_CLOSE:
                p16(b, 0, 24);
                memcpy(b + 8, cmd[i].fid, 16);
                body_len = 24;
                break;
            case SMB2_QUERY_INFO:
                p16(b, 0, 41);
                b[2] = SMB2_INFO_FILE_T;
                b[3] = cmd[i].offset ? cmd[i].offset : SMB2_FILE_BASIC_INFO_T;
                p32(b, 4, 4096);
                memcpy(b + 24, cmd[i].fid, 16);
                body_len = 40;
                break;
            case SMB2_WRITE:
                p16(b, 0, 49);
                p16(b, 2, SMB2_HDR_SIZE + 48);
                p32(b, 4, 4);
                p64(b, 8, cmd[i].offset);
                memcpy(b + 16, cmd[i].fid, 16);
                memcpy(b + 48, "abcdefgh" + cmd[i].offset, 4);
                body_len = 52;
                break;
            case SMB2_SET_INFO:
                p16(b, 0, 33);
                b[2] = SMB2_INFO_FILE_T;
                b[3] = 14; /* FilePositionInformation */
                p32(b, 4, 8);
                p16(b, 8, SMB2_HDR_SIZE + 32);
                memcpy(b + 16, cmd[i].fid, 16);
                p64(b, 32, cmd[i].offset);
                body_len = 40;
                break;
            case SMB2_QUERY_DIRECTORY:
                p16(b, 0, 33);
                b[2] = 12; /* FileNamesInformation */
                b[3] = cmd[i].offset; /* restart / single entry flags */
                memcpy(b + 8, cmd[i].fid, 16);
                p16(b, 24, SMB2_HDR_SIZE + 32);
                p16(b, 26, 2);
                p32(b, 28, 4096);
                p16(b, 32, '*');
                body_len = 34;
                break;
            case SMB2_READ:
                p16(b, 0, 49);
                b[2] = SMB2_HDR_SIZE + 16;
                p32(b, 4, 4);
                p64(b, 8, cmd[i].offset);
                memcpy(b + 16, cmd[i].fid, 16);
                body_len = 49;
                break;
            case SMB2_FLUSH:
                p16(b, 0, 24);
                memcpy(b + 8, cmd[i].fid, 16);
                body_len = 24;
                break;
            default:
                abort();
        } /* switch */
        unsigned int len = SMB2_HDR_SIZE + body_len;
        if (i + 1 < count) {
            len = (len + 7) & ~7u;
            p32(h, 20, len);
        }
        if (c->signing_on) {
            uint8_t signature[16];
            p32(h, 16, g32(h, 16) | SMB2_FLAGS_SIGNED);
            smb2w_sign(c->dialect, c->signing_alg, c->signing_key, h, len, signature);
            memcpy(h + 48, signature, sizeof(signature));
        }
        size += len;
        assert(size <= sizeof(packet));
    }
    memcpy(c->sbuf + 4, packet, size);
    int signing = c->signing_on;
    c->signing_on = 0; /* Packet already carries per-command signatures. */
    smb2c_send(c, size - SMB2_HDR_SIZE);
    c->signing_on = signing;
    c->msg_id     = first_mid + count;
    return first_mid;
} /* send_compound */

static void
check_reply(
    struct smb2_conn     *c,
    const struct command *cmd,
    unsigned int          count,
    uint64_t              first_mid,
    int                   exhausted)
{
    unsigned int offset = 4;
    uint8_t      previous_name[512];
    unsigned int previous_length = 0;

    for (unsigned int i = 0; i < count; i++) {
        assert(offset + SMB2_HDR_SIZE <= (unsigned int) c->rlen);
        uint8_t       *h        = c->rbuf + offset;
        const uint8_t *b        = h + SMB2_HDR_SIZE;
        uint32_t       next     = g32(h, 20);
        unsigned int   limit    = next ? next : c->rlen - offset;
        uint32_t       expected = exhausted ? ST_INTERNAL_ERROR : cmd[i].status;
        fprintf(stderr, "command %u opcode %u: status %08x expected %08x\n",
                i, cmd[i].opcode, g32(h, 8), expected);
        assert(g16(h, 12) == cmd[i].opcode);
        assert(g64(h, 24) == first_mid + i);
        assert(g32(h, 8) == expected);
        assert(offset + limit <= (unsigned int) c->rlen);
        if (c->signing_on) {
            uint8_t received[16], calculated[16];
            assert(g32(h, 16) & SMB2_FLAGS_SIGNED);
            memcpy(received, h + 48, sizeof(received));
            memset(h + 48, 0, sizeof(received));
            smb2w_sign(c->dialect, c->signing_alg, c->signing_key, h, limit, calculated);
            memcpy(h + 48, received, sizeof(received));
            assert(!memcmp(received, calculated, sizeof(received)));
        }
        if (expected == ST_SUCCESS && cmd[i].opcode == SMB2_QUERY_INFO) {
            assert(limit >= SMB2_HDR_SIZE + 8);
            assert(g32(b, 4) == (cmd[i].offset == 14 ? 8 : 40));
            assert(g16(b, 2) + g32(b, 4) <= limit);
            if (cmd[i].offset == 14) {
                /* Earlier READ advanced to four; a later SET_POSITION must
                 * not overwrite this command's private response snapshot. */
                assert(g64(h + g16(b, 2), 0) == 4);
            }
        } else if (expected == ST_SUCCESS && cmd[i].opcode == SMB2_WRITE) {
            assert(g16(b, 0) == 17 && g32(b, 4) == 4);
        } else if (expected == ST_SUCCESS && cmd[i].opcode == SMB2_QUERY_DIRECTORY) {
            unsigned int   length = g32(b, 4);
            const uint8_t *entry  = h + g16(b, 2);
            assert(length >= 12 && g16(b, 2) + length <= limit);
            assert(g32(entry, 0) == 0); /* RETURN_SINGLE_ENTRY */
            unsigned int   name_length = g32(entry, 8);
            assert(name_length && name_length <= sizeof(previous_name) && name_length + 12 <= length);
            assert(name_length != previous_length || memcmp(previous_name, entry + 12, name_length));
            memcpy(previous_name, entry + 12, name_length);
            previous_length = name_length;
        } else if (expected == ST_SUCCESS && cmd[i].opcode == SMB2_READ) {
            assert(limit >= SMB2_HDR_SIZE + 16);
            assert(g32(b, 4) == 4 && b[2] + 4 <= limit);
            assert(!memcmp(h + b[2], "abcdefgh" + cmd[i].offset, 4));
        } else if (expected != ST_SUCCESS) {
            assert(limit >= SMB2_HDR_SIZE + 8);
            assert(g16(b, 0) == 9);
            assert(g32(b, 4) == 0); /* rejected attempts expose no READ data */
        }
        assert((i + 1 < count) == (next != 0));
        if (next) {
            assert(next >= SMB2_HDR_SIZE && !(next & 7));
        }
        offset += limit;
    }
    assert(offset == (unsigned int) c->rlen);
} /* check_reply */

static void
run(
    struct smb2_conn     *c,
    const char           *name,
    const struct command *cmd,
    unsigned int          count,
    int                   rejects,
    int                   pause,
    int                   op_error)
{
    fprintf(stderr, "# %s\n", name);
    reject_count    = rejects;
    pause_finish    = pause;
    operation_error = op_error;
    expected_groups = count;
    atomic_store(&submissions, 0);
    atomic_store(&attempts, 0);
    atomic_store(&held, 0);
    atomic_store(&release_finish, 0);
    int      replies = c->nreply_app;
    atomic_store(&armed, 1);
    uint64_t mid = send_compound(c, cmd, count);
    if (pause) {
        uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&held)) {
            smb2_pump(c->env);
            assert(smb2c_now_ms() < deadline && !c->disconnected);
        }
        assert(smb2_echo_barrier(c) == 0);
        assert(c->nreply_app == replies);
        atomic_store(&release_finish, 1);
        assert(smb2c_pump_for_nreply(c, replies, name));
    } else {
        (void) smb2c_wait(c);
    }
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    int exhausted = rejects > CHIMERA_FRONTEND_COMPOUND_RETRIES;
    assert(atomic_load(&attempts) == (exhausted ? CHIMERA_FRONTEND_COMPOUND_RETRIES + 1 : rejects + 1));
    assert(c->nreply_app == replies + 1);
    check_reply(c, cmd, count, mid, exhausted);
    assert(smb2_echo_barrier(c) == 0);
    assert(c->nreply_app == replies + 1);
} /* run */

/* SMB holder self-exemption uses the actual open identity. In particular a
 * retained synthetic I/O handle must not turn a lock owner's own READ into a
 * conflicting READ, while a second open must still be denied. */
static void
run_lock_owner(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    struct smb2_create_out other;

    assert(smb2_create(c, "compound.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &other) == ST_SUCCESS);
    assert(smb2_lock(c, file_id, 0, 4,
                     SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) == ST_SUCCESS);
/* *INDENT-OFF* */
    struct command locked[] = {
        { SMB2_READ, file_id, 0, 0, ST_SUCCESS },
        { SMB2_READ, other.file_id, 0, 0, ST_FILE_LOCK_CONFLICT },
        { SMB2_QUERY_INFO, other.file_id, 0, 0, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "own lock permits owner read and denies other open", locked, 3, 0, 0, 0);
    run(c, "lock-owner admission survives readonly finish retries", locked, 3, 2, 0, 0);
    assert(smb2_lock(c, file_id, 0, 4, SMB2_LOCKFLAG_UNLOCK) == ST_SUCCESS);
/* *INDENT-OFF* */
    struct command unlocked[] = {
        { SMB2_READ, other.file_id, 0, 0, ST_SUCCESS },
        { SMB2_READ, file_id, 0, 4, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "other open reads after exact unlock", unlocked, 2, 0, 0, 0);
    assert(smb2_close(c, other.file_id) == ST_SUCCESS);
} /* run_lock_owner */

/* The original tree must stay alive for accepted replies while being retired
* immediately for new requests. Reusing the connection must not reuse the
* pinned tree storage or mutate an open belonging to its replacement tree. */
static void
run_tree_retirement(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
/* *INDENT-OFF* */
    struct command cmd[] = {
        { SMB2_QUERY_INFO, file_id, 0, 0, ST_SUCCESS },
        { SMB2_READ, file_id, 0, 4, ST_SUCCESS }
    };
/* *INDENT-ON* */
    uint32_t old_tree = c->tree_id;

    reject_count    = 0;
    pause_finish    = 1;
    operation_error = 0;
    expected_groups = 2;
    atomic_store(&submissions, 0);
    atomic_store(&attempts, 0);
    atomic_store(&held, 0);
    atomic_store(&release_finish, 0);
    int      replies = c->nreply_app;
    atomic_store(&armed, 1);
    uint64_t mid      = send_compound(c, cmd, 2);
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&held)) {
        smb2_pump(c->env);
        assert(smb2c_now_ms() < deadline && !c->disconnected);
    }
    atomic_store(&armed, 0);
    assert(smb2_echo_barrier(c) == 0 && c->nreply_app == replies);
    assert(smb2_tree_disconnect(c) == ST_SUCCESS);
    assert(smb2_tree_connect(c, "\\\\server\\share") == ST_SUCCESS);
    struct smb2_create_out replacement;
    assert(smb2_create(c, "compound.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &replacement) == ST_SUCCESS);
    uint8_t                data[8];
    uint32_t               length;
    assert(smb2_read(c, replacement.file_id, 0, 4, data, &length) == ST_SUCCESS);
    assert(length == 4 && !memcmp(data, "abcd", 4));
    replies = c->nreply_app;
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "held result after tree retirement"));
    assert(atomic_load(&submissions) == 1 && atomic_load(&attempts) == 1);
    assert(c->nreply_app == replies + 1);
    check_reply(c, cmd, 2, mid, 0);
    assert(g32(c->rbuf + 4, 36) == old_tree);
    assert(smb2_echo_barrier(c) == 0);
    assert(c->nreply_app == replies + 1);
    assert(smb2_read(c, replacement.file_id, 4, 4, data, &length) == ST_SUCCESS);
    assert(length == 4 && !memcmp(data, "efgh", 4));
    assert(smb2_close(c, replacement.file_id) == ST_SUCCESS);
} /* run_tree_retirement */

int
main(
    int    argc,
    char **argv)
{
    struct smb2_env                 env;
    struct smb2_create_out          opened;
    uint8_t                         invalid[16] = {
        0
    }, related[16];
    uint32_t                        written;

    memset(related, 0xff, sizeof(related));
    assert(argc <= 2);
    const struct smb2_wire_profile *profile = argc == 2 ? smb2_wire_profile_find(argv[1]) : NULL;
    assert(argc == 1 || profile);
    struct smb2_env_opts            options = {
        0
    };
    smb2_env_open_wire(&env, &options, profile);
    smb2_env_fs_setup(&env, "fs0");
    struct smb2_conn               *c = smb2_conn_open(&env);
    smb2_handshake(c);
    assert(smb2_create(c, "compound.txt", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &opened) == ST_SUCCESS);
    assert(smb2_write(c, opened.file_id, 0, "abcdefgh", 8, &written) == ST_SUCCESS && written == 8);
/* *INDENT-OFF* */
    struct command ordinary[] = {
        { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS },
        { SMB2_READ, opened.file_id, 0, 0, ST_SUCCESS },
        { SMB2_FLUSH, opened.file_id, 0, 0, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "query/read/flush coalesced", ordinary, 3, 0, 0, 0);
    struct command         created[] = {

        {
            SMB2_CREATE,
            NULL,
            0,
            0,

            ST_SUCCESS
        },

        {
            SMB2_WRITE,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        },

        {
            SMB2_QUERY_INFO,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        },

        {
            SMB2_READ,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        },

        {
            SMB2_CLOSE,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        }

    };
    run(c, "create/write/query/read/close coalesced with provisional handle", created, 5, 0, 0, 0);
    struct command         create_failed[6];
    memcpy(create_failed, created, sizeof(created));
    for (unsigned int i = 0; i < 5; i++) {
        create_failed[i].status = ST_OBJECT_NAME_COLLISION;
    }
    create_failed[5] = (struct command) { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS };
    run(c, "failed create propagates related error and independent group continues", create_failed, 6, 0, 0, 0);
    struct command         metadata_open[] = {

        {
            SMB2_CREATE,
            NULL,
            0,
            1,

            ST_SUCCESS
        },

        {
            SMB2_QUERY_INFO,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        },

        {
            SMB2_READ,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_ACCESS_DENIED
        },

        {
            SMB2_CLOSE,
            related,
            SMB2_FLAGS_RELATED_OPERATIONS,
            0,

            ST_SUCCESS
        }

    };
    run(c, "metadata open/query/denied read/close coalesced", metadata_open, 4, 0, 0, 0);
    struct command         write_info[] = {

        {
            SMB2_WRITE,
            opened.file_id,
            0,
            0,
            ST_SUCCESS
        },

        {
            SMB2_READ,
            opened.file_id,
            0,
            0,
            ST_SUCCESS
        },

        {
            SMB2_QUERY_INFO,
            opened.file_id,
            0,
            14,
            ST_SUCCESS
        },

        {
            SMB2_SET_INFO,
            opened.file_id,
            0,
            7,
            ST_SUCCESS
        }

    };
    run(c, "write/read/query/set position share one compound", write_info, 4, 0, 0, 0);
    struct command         cursor_snapshots[] = {

        {
            SMB2_READ,
            opened.file_id,
            0,
            0,
            ST_SUCCESS
        },

        {
            SMB2_QUERY_INFO,
            opened.file_id,
            0,
            14,
            ST_SUCCESS
        },

        {
            SMB2_READ,
            opened.file_id,
            0,
            4,
            ST_SUCCESS
        }

    };
    run(c, "position snapshot survives later cursor change and retry", cursor_snapshots, 3, 2, 1, 0);
    struct smb2_create_out directory;
    assert(smb2_create_opts(c, "compound-dir", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    struct command         pages[] = {

        {
            SMB2_QUERY_DIRECTORY,
            directory.file_id,
            0,
            3,
            ST_SUCCESS

        },

        {
            SMB2_QUERY_DIRECTORY,
            directory.file_id,
            0,
            2,
            ST_SUCCESS

        },

        {
            SMB2_QUERY_INFO,
            opened.file_id,
            0,
            0,
            ST_SUCCESS

        }

    };
    run(c, "directory pages share private cursor across finish retries", pages, 3, 2, 1, 0);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
/* *INDENT-OFF* */
    struct command failure[] = {
        { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS },
        { SMB2_QUERY_INFO, invalid, 0, 0, ST_FILE_CLOSED },
        { SMB2_READ, opened.file_id, 0, 4, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "independent command after invalid FileId", failure, 3, 0, 0, 0);
/* *INDENT-OFF* */
    struct command missing_first[] = {
        { SMB2_QUERY_INFO, invalid, 0, 0, ST_FILE_CLOSED },
        { SMB2_READ, opened.file_id, SMB2_FLAGS_RELATED_OPERATIONS, 0, ST_FILE_CLOSED },
        { SMB2_QUERY_INFO, related, SMB2_FLAGS_RELATED_OPERATIONS, 0, ST_FILE_CLOSED }
    };
/* *INDENT-ON* */
    run(c, "missing first handle prevents related explicit handle reseeding", missing_first, 3, 0, 0, 0);
/* *INDENT-OFF* */
    struct command inherited[] = {
        { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS },
        { SMB2_READ, related, SMB2_FLAGS_RELATED_OPERATIONS, 0, ST_SUCCESS },
        { SMB2_READ, related, SMB2_FLAGS_RELATED_OPERATIONS, 4, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "related FileIds inherit existing open", inherited, 3, 0, 0, 0);
/* *INDENT-OFF* */
    struct command related_failure[] = {
        { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS },
        { SMB2_QUERY_INFO, invalid, SMB2_FLAGS_RELATED_OPERATIONS, 0, ST_FILE_CLOSED },
        { SMB2_READ, related, SMB2_FLAGS_RELATED_OPERATIONS, 4, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "failed related command preserves established handle", related_failure, 3, 0, 0, 0);
    uint8_t half_related[16] = {
        0
    };
    memset(half_related, 0xff, 8);
    inherited[1].fid = half_related;
    run(c, "one all-ones FileId half inherits the whole handle", inherited, 3, 0, 0, 0);
    inherited[1].fid = related;
    run(c, "readonly finish retries publish only after acceptance", inherited, 3, 2, 1, 0);
    run(c, "readonly finish exhaustion has no stale success/data", inherited, 3,
        CHIMERA_FRONTEND_COMPOUND_RETRIES + 1, 0, 0);
/* *INDENT-OFF* */
    struct command op_failure[] = {
        { SMB2_READ, opened.file_id, 0, 0, ST_INTERNAL_ERROR },
        { SMB2_QUERY_INFO, opened.file_id, 0, 0, ST_SUCCESS }
    };
/* *INDENT-ON* */
    run(c, "operation EAGAIN does not replay and next group executes", op_failure, 2, 0, 0, 1);
    run_lock_owner(c, opened.file_id);
    run_tree_retirement(c, opened.file_id);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
