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
#include "vfs/vfs_compound.h"
#include "common/compound_retry.h"
#include "vfs/vfs_notify.h"

/* Finish rejection covers deferred existing CLOSE only, never filesystem
 * creation or mutation requiring backend transaction rollback. */
static atomic_int                         armed, submissions;
static int                                legacy_ea_adapter;
static int                                ea_budget_boundary;
static const char                        *data_chain_name;
static int                                fail_stream_share;
static unsigned                           expected_groups;
static int                                reject_count = -1;
static int                                cancel_close;
static int                                fail_directory_admission;
static int                                fail_overwrite;
static chimera_vfs_compound_op_callback_t overwrite_prepare;
static void                              *overwrite_prepare_private;
static int                                data_boundary;
static int                                reparse_collision_boundary;
static int                                data_open_access;
static int                                expected_create_action = -1;
static int64_t                            expected_create_size   = -1;
static const uint8_t                     *expected_ea;
static uint32_t                           expected_ea_len;
static atomic_int                         failed_directory_notices;
static chimera_vfs_compound_op_callback_t directory_prepare;
static void                              *directory_prepare_private;

static void
reject_directory_admission(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) private_data;
    if (directory_prepare) {
        directory_prepare(compound, index, status, directory_prepare_private);
    }
    if (*status == CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_EACCES;
    }
} /* reject_directory_admission */

static void
reject_overwrite(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) private_data;
    if (overwrite_prepare) {
        overwrite_prepare(compound, index, status, overwrite_prepare_private);
    }
    if (*status == CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_EACCES;
    }
} /* reject_overwrite */

__attribute__((visibility("default"))) void
chimera_vfs_notify_emit_lease(
    struct chimera_vfs_notify *notify,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_len,
    uint64_t                   skip_lo,
    uint64_t                   skip_hi,
    bool                       has_skip)
{
    typedef void (*emit_fn)(
        struct chimera_vfs_notify *,
        const uint8_t *,
        uint16_t,
        uint32_t,
        const char *,
        uint16_t,
        const char *,
        uint16_t,
        uint64_t,
        uint64_t,
        bool);
    emit_fn           next = (emit_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_lease");
    assert(next);
    static const char failed_name[] = "created-admission-failure";
    if ((action & CHIMERA_VFS_NOTIFY_DIR_ADDED) && name_len == sizeof(failed_name) - 1 &&
        !memcmp(name, failed_name, name_len)) {
        atomic_fetch_add(&failed_directory_notices, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_len, skip_lo, skip_hi, has_skip);
} /* chimera_vfs_notify_emit_lease */

/* Actor and legacy entrypoints emit independently; observe each exactly once. */
__attribute__((visibility("default"))) void
chimera_vfs_notify_emit_actor(
    struct chimera_vfs_notify        *notify,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    uint32_t                          action,
    const char                       *name,
    uint16_t                          name_len,
    const char                       *old_name,
    uint16_t                          old_len,
    const struct chimera_claim_actor *actor)
{
    typedef void (*emit_fn)(
        struct chimera_vfs_notify *,
        const uint8_t *,
        uint16_t,
        uint32_t,
        const char *,
        uint16_t,
        const char *,
        uint16_t,
        const struct chimera_claim_actor *);
    emit_fn           next = (emit_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_actor");
    assert(next);
    static const char failed_name[] = "created-admission-failure";
    if ((action & CHIMERA_VFS_NOTIFY_DIR_ADDED) && name_len == sizeof(failed_name) - 1 &&
        !memcmp(name, failed_name, name_len)) {
        atomic_fetch_add(&failed_directory_notices, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_len, actor);
} /* chimera_vfs_notify_emit_actor */
static atomic_int attempts, held, release_finish;
static            _Thread_local struct evpl *owner_evpl;

struct close_injection {
    chimera_vfs_compound_callback_t    callback;
    void                              *private_data;
    struct evpl                       *evpl;
    struct evpl_timer                  timer;
    struct chimera_vfs_compound       *compound;
    enum chimera_vfs_error result;
    unsigned int                       seen;
    chimera_vfs_compound_op_callback_t retired;
    void                              *retired_private;
};

static void
cancel_after_retirement(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct close_injection *ctx = private_data;

    if (ctx->retired) {
        ctx->retired(compound, index, status, ctx->retired_private);
    }
    if (*status == CHIMERA_VFS_OK) {
        assert(chimera_vfs_compound_cancel(compound));
    }
} /* cancel_after_retirement */

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

static void
close_finish_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct close_injection *ctx = (struct close_injection *)
        ((char *) timer - offsetof(struct close_injection, timer));

    if (!atomic_exchange(&release_finish, 0)) {
        evpl_add_oneshot_timer(evpl, timer, close_finish_poll, 1000);
        return;
    }
    atomic_store(&held, 0);
    chimera_vfs_compound_finish_result(ctx->compound, ctx->result);
} /* close_finish_poll */

static void
close_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct close_injection *ctx = private_data;

    ctx->result = ctx->seen++ < (unsigned int) reject_count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK;
    atomic_store(&attempts, ctx->seen);
    if (ctx->seen == 1) {
        ctx->compound = compound;
        evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, close_finish_poll, 1000);
        atomic_store(&held, 1);
        return;
    }
    chimera_vfs_compound_finish_result(compound, ctx->result);
} /* close_finish */

static void
close_injected_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct close_injection         *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN ||
        ctx->seen == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1) {
        free(ctx);
    }
    callback(compound, arg);
} /* close_injected_complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        unsigned int submission     = atomic_fetch_add(&submissions, 1);
        bool         boundary_first = data_boundary && submission == 0;
        assert(chimera_vfs_compound_num_groups(compound) ==
               (reparse_collision_boundary ? (submission ? 0 : 1) :
                ea_budget_boundary ? (submission ? 0 : 1) :
                data_boundary && submission ? (submission == 1 ? 0 : 5) : expected_groups));
        if (legacy_ea_adapter && submission == 0) {
            assert(chimera_vfs_compound_num_ops(compound) == 6);
            assert(chimera_vfs_compound_op(compound, 1)->type == CHIMERA_VFS_COMPOUND_OP_OPEN);
        }
        if (legacy_ea_adapter && submission > 0) {
            assert(chimera_vfs_compound_num_ops(compound) == 2);
            assert(chimera_vfs_compound_op(compound, 0)->type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE);
            assert(chimera_vfs_compound_op(compound, 1)->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS);
        }
        if (fail_stream_share) {
            int                                   target       = -1;
            unsigned int                          reservations = 0;
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                if (chimera_vfs_compound_op(compound, i)->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
                    target = i; reservations++;
                }
            }
            assert(reservations == 2 && target >= 0);
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, target);
            directory_prepare = op->prepare; directory_prepare_private = op->prepare_private;
            chimera_vfs_compound_set_op_prepare(compound, target, reject_directory_admission, NULL);
        }
        if (fail_overwrite) {
            unsigned int found    = 0;
            bool         reserved = false;
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
                if (op->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
                    reserved = true;
                }
                bool                                  target = fail_overwrite == 1 ?
                    op->type == CHIMERA_VFS_COMPOUND_OP_OVERWRITE :
                    reserved && op->type == CHIMERA_VFS_COMPOUND_OP_COORDINATE;
                if (!target) {
                    continue;
                }
                overwrite_prepare = op->prepare; overwrite_prepare_private = op->prepare_private;
                chimera_vfs_compound_set_op_prepare(compound, i, reject_overwrite, NULL);
                found++;
                break;
            }
            assert(found == 1);
        }
        if (fail_directory_admission || boundary_first) {
            int found = 0;
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
                if (op->type != CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
                    continue;
                }
                directory_prepare = op->prepare; directory_prepare_private = op->prepare_private;
                chimera_vfs_compound_set_op_prepare(compound, i, reject_directory_admission, NULL);
                found++;
            }
            assert(found == 1);
        }
        if (reject_count >= 0 && (!data_boundary || boundary_first)) {
            struct close_injection *ctx = calloc(1, sizeof(*ctx));
            assert(ctx);
            ctx->callback = callback; ctx->private_data = private_data; ctx->evpl = owner_evpl;
            if (cancel_close) {
                int found = 0;
                for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
                    if (op->type != CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS) {
                        continue;
                    }
                    chimera_vfs_compound_op_callback_t    prepare         = op->prepare;
                    void                                 *prepare_private = op->prepare_private;
                    ctx->retired = op->complete; ctx->retired_private = op->callback_private;
                    chimera_vfs_compound_set_op_callbacks(compound, i, prepare, cancel_after_retirement, ctx);
                    chimera_vfs_compound_set_op_prepare(compound, i, prepare, prepare_private);
                    found++;
                }
                assert(found == 1);
            }
            chimera_vfs_compound_set_finish_handler(compound, close_finish, ctx);
            next(compound, close_injected_complete, ctx);
            return;
        }
    }
    next(compound, callback, private_data);
} /* chimera_vfs_compound_submit */

static void
related_header(
    uint8_t *header,
    uint16_t opcode,
    uint64_t mid,
    uint32_t next)
{
    memset(header, 0, SMB2_HDR_SIZE);
    memcpy(header, "\xfeSMB", 4);
    p16(header, 4, SMB2_HDR_SIZE);
    p16(header, 6, 1);
    p16(header, 12, opcode);
    p16(header, 14, 32);
    p32(header, 16, SMB2_FLAGS_RELATED_OPERATIONS);
    p32(header, 20, next);
    p64(header, 24, mid);
    p32(header, 36, UINT32_MAX);
    p64(header, 40, UINT64_MAX);
} /* related_header */

/* Exercise an execution-discovered boundary with request-owned WRITE input and
 * preallocated READ/QDIR output storage. Only the initial discovery attempt is
 * finish-rejected; it contains no filesystem mutation. */
static void
data_open_chain(
    struct smb2_conn *conn,
    const uint8_t     prefix_fid[16],
    int               rejects)
{
    const char  *name     = data_chain_name ? data_chain_name : "contexts.txt";
    int          body_len = smb2c_build_create_full(conn, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                                                    MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE, NULL, NULL, 0);
    uint8_t      create[2048];
    unsigned int create_len = (SMB2_HDR_SIZE + body_len + 7) & ~7u;

    assert(create_len <= sizeof(create));
    memcpy(create, conn->sbuf + 4, SMB2_HDR_SIZE + body_len);
    memset(create + SMB2_HDR_SIZE + body_len, 0, create_len - SMB2_HDR_SIZE - body_len);
    uint8_t     *wire = conn->sbuf + 4;
    uint64_t     mid  = g64(create, 24);
    unsigned int offset = 0, slot = 0;
    if (prefix_fid) {
        memcpy(wire, create, SMB2_HDR_SIZE);
        p16(wire, 12, SMB2_QUERY_INFO); p32(wire, 20, 104);
        uint8_t *body = wire + SMB2_HDR_SIZE;
        memset(body, 0, 40); p16(body, 0, 41);
        body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
        p32(body, 4, 4096); memcpy(body + 24, prefix_fid, 16);
        offset = 104; slot++;
        p64(create, 24, mid + slot);
    }
    memcpy(wire + offset, create, create_len);
    p32(wire + offset, 20, create_len);
    offset += create_len; slot++;
    const uint16_t     codes[] = {
        SMB2_WRITE,
        SMB2_QUERY_DIRECTORY,
        SMB2_QUERY_INFO,
        SMB2_READ,
        SMB2_CLOSE
    };
    const unsigned int sizes[] = {
        120,
        104,
        104,
        112,
        88
    };
    for (unsigned int i = 0; i < 5; i++, slot++) {
        uint8_t *header = wire + offset;
        related_header(header, codes[i], mid + slot, i == 4 ? 0 : sizes[i]);
        uint8_t *body = header + SMB2_HDR_SIZE;
        memset(body, 0, sizes[i] - SMB2_HDR_SIZE);
        if (i == 0) {
            p16(body, 0, 49); p16(body, 2, 112); p32(body, 4, 4);
            memset(body + 16, 0xff, 16); memcpy(body + 48, "data", 4);
        } else if (i == 1) {
            p16(body, 0, 33); body[2] = 37; body[3] = 1;
            memset(body + 8, 0xff, 16); p16(body, 24, 96); p16(body, 26, 2);
            p32(body, 28, 4096); p16(body, 32, '*');
        } else if (i == 2) {
            p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
            p32(body, 4, 4096); memset(body + 24, 0xff, 16);
        } else if (i == 3) {
            p16(body, 0, 49); p32(body, 4, 4); memset(body + 16, 0xff, 16);
        } else {
            p16(body, 0, 24); memset(body + 8, 0xff, 16);
        }
        offset += sizes[i];
    }
    expected_groups = slot;
    data_boundary   = rejects >= 0;
    reject_count    = rejects;
    atomic_store(&submissions, 0); atomic_store(&attempts, 0);
    atomic_store(&release_finish, 1); atomic_store(&armed, 1);
    smb2c_send(conn, offset - SMB2_HDR_SIZE);
    conn->msg_id = mid + slot;
    smb2c_wait(conn);
    atomic_store(&armed, 0); data_boundary = 0; reject_count = -1;
    bool           exhausted = rejects == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1;
    assert(atomic_load(&submissions) == (rejects >= 0 && !exhausted ? 3 : 1));
    if (rejects >= 0) {
        assert(atomic_load(&attempts) == (exhausted ? rejects : rejects + 1));
    }
    const uint8_t *reply = conn->rbuf + 4;
    for (unsigned int i = 0; i < slot; i++) {
        uint16_t code     = g16(reply, 12);
        uint32_t expected = exhausted ? ST_INTERNAL_ERROR :
            (code == SMB2_QUERY_DIRECTORY ? ST_INVALID_PARAMETER : ST_SUCCESS);
        if (g32(reply, 8) != expected) {
            fprintf(stderr, "data chain rejects=%d prefix=%d slot=%u opcode=%u actual=%08x expected=%08x\n",
                    rejects, !!prefix_fid, i, code, g32(reply, 8), expected);
        }
        assert(g32(reply, 8) == expected);
        if (!exhausted && code == SMB2_READ) {
            const uint8_t *body = reply + SMB2_HDR_SIZE;
            assert(g32(body, 4) == 4 && !memcmp(reply + body[2], "data", 4));
        }
        uint32_t next = g32(reply, 20);
        if (i + 1 < slot) {
            assert(next); reply += next;
        } else {
            assert(!next);
        }
    }
} /* data_open_chain */

static void
live_cache_open(
    struct smb2_env  *env,
    struct smb2_conn *conn,
    uint8_t           level)
{
    struct smb2_conn      *holder = smb2_conn_open(env);

    smb2_handshake(holder);
    const char            *name    = level == SMB2_OPLOCK_LEVEL_BATCH ? "recall-batch.txt" : "recall-exclusive.txt";
    struct smb2_oplock_req caching = {
        .level = level
    };
    struct smb2_create_out held_open, opened;
    assert(smb2_create(holder, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                       &caching, &held_open) == ST_SUCCESS && held_open.oplock == level);
    expected_groups = 1; reject_count = 2;
    atomic_store(&attempts, 0); atomic_store(&submissions, 0);
    atomic_store(&release_finish, 1); atomic_store(&armed, 1);
    smb2_create_post(conn, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL);
    uint64_t               deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!smb2_conn_nbreaks(holder)) {
        smb2_pump(env);
        assert(smb2c_now_ms() < deadline);
    }
    assert(!conn->reply_ready);
    struct smb2_break      brk;
    assert(smb2_conn_pop_break(holder, &brk) && !brk.is_lease);
    assert(smb2_oplock_break_ack(holder, held_open.file_id, brk.oplock_level) == ST_SUCCESS);
    smb2c_wait(conn);
    atomic_store(&armed, 0); reject_count = -1;
    smb2c_parse_create(conn, &opened);
    assert(opened.status == ST_SUCCESS);
    assert(atomic_load(&submissions) == 1 && atomic_load(&attempts) == 3);
    assert(!smb2_conn_nbreaks(holder));
    assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    assert(smb2_close(holder, held_open.file_id) == ST_SUCCESS);
    smb2_conn_disconnect(holder);
} /* live_cache_open */

static void
create_query_close(
    struct smb2_conn       *conn,
    const char             *name,
    uint32_t                disposition,
    uint32_t                options,
    const struct smb2_cctx *contexts,
    int                     count,
    int                     directory)
{
    fprintf(stderr, "# CREATE lifecycle %s disposition %u contexts %d\n", name, disposition, count);
    int      body_len = smb2c_build_create_full(conn, name, disposition,
                                                disposition == MBT_FILE_CREATE || data_open_access ? MBT_FILE_ALL_ACCESS
    :
                                                MBT_FILE_READ_ATTRIBUTES,
                                                MBT_FILE_SHARE_RWD, options, NULL, contexts, count);
    uint8_t *first     = conn->sbuf + 4;
    uint64_t mid       = g64(first, 24);
    uint32_t first_len = (SMB2_HDR_SIZE + body_len + 7) & ~7u;
    memset(first + SMB2_HDR_SIZE + body_len, 0, first_len - SMB2_HDR_SIZE - body_len);
    p32(first, 20, first_len);
    uint8_t *query = first + first_len;
    related_header(query, SMB2_QUERY_INFO, mid + 1, SMB2_HDR_SIZE + 40);
    uint8_t *body = query + SMB2_HDR_SIZE;
    memset(body, 0, 40);
    p16(body, 0, 41);
    body[2] = SMB2_INFO_FILE_T;
    body[3] = expected_ea ? SMB2_FILE_FULL_EA_INFO_T :
        directory ? SMB2_FILE_BASIC_INFO_T : 6; /* InternalInformation */
    p32(body, 4, 4096);
    memset(body + 24, 0xff, 16);
    uint8_t *close = query + SMB2_HDR_SIZE + 40;
    related_header(close, SMB2_CLOSE, mid + 2, 0);
    body = close + SMB2_HDR_SIZE;
    memset(body, 0, 24);
    p16(body, 0, 24);
    p16(body, 2, 1); /* POSTQUERY_ATTRIB */
    memset(body + 8, 0xff, 16);
    expected_groups = 3;
    atomic_store(&submissions, 0);
    atomic_store(&armed, 1);
    smb2c_send(conn, first_len + SMB2_HDR_SIZE + 40 + SMB2_HDR_SIZE + 24 - SMB2_HDR_SIZE);
    conn->msg_id = mid + 3;
    smb2c_wait(conn);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    struct smb2_create_out created;
    smb2c_parse_create(conn, &created);
    assert(created.status == ST_SUCCESS);
    if (expected_create_action >= 0) {
        assert(created.action == (unsigned int) expected_create_action);
    }
    if (expected_create_size >= 0) {
        assert(created.end_of_file == (uint64_t) expected_create_size);
    }
    const struct smb2_rsp_ctx *mxac = smb2c_create_ctx_find(&created, "MxAc");
    const struct smb2_rsp_ctx *qfid = smb2c_create_ctx_find(&created, "QFid");
    for (int i = 0; i < count; i++) {
        if (contexts[i].name_len == 4 && !memcmp(contexts[i].name, "MxAc", 4)) {
            assert(mxac && mxac->data_len == 8 && g32(mxac->data, 0) == ST_SUCCESS);
        }
        if (contexts[i].name_len == 4 && !memcmp(contexts[i].name, "QFid", 4)) {
            assert(qfid && qfid->data_len == 32);
        }
    }
    const uint8_t *header = conn->rbuf + 4;
    uint32_t       offset = g32(header, 20);
    assert(offset && offset + SMB2_HDR_SIZE + 8 <= (unsigned int) conn->rlen - 4);
    header += offset;
    assert(g16(header, 12) == SMB2_QUERY_INFO && g64(header, 24) == mid + 1);
    assert(g32(header, 8) == ST_SUCCESS);
    const uint8_t *reply           = header + SMB2_HDR_SIZE;
    const uint8_t *data            = header + g16(reply, 2);
    uint32_t       expected_length = expected_ea ? (expected_ea_len + 3) & ~3u : directory ? 40u : 8u;
    if (g32(reply, 4) != expected_length) {
        fprintf(stderr, "CREATE lifecycle %s QUERY length %u expected %u\n",
                name, g32(reply, 4), expected_length);
    }
    assert(g32(reply, 4) == expected_length);
    if (expected_ea) {
        /* EA enumeration aligns every record then clears the final NextEntry
         * offset; its trailing padding is retained in OutputBufferLength. */
        assert(!memcmp(data, expected_ea, expected_ea_len));
        for (uint32_t i = expected_ea_len; i < expected_length; i++) {
            assert(data[i] == 0);
        }
    } else if (directory) {
        assert(g32(data, 32) & SMB2_FILE_ATTRIBUTE_DIRECTORY);
    } else if (qfid) {
        assert(g64(data, 0) == g64(qfid->data, 0));
    }
    offset = g32(header, 20);
    assert(offset);
    header += offset;
    assert(g16(header, 12) == SMB2_CLOSE && g64(header, 24) == mid + 2);
    assert(g32(header, 8) == ST_SUCCESS && g32(header, 20) == 0);
    /* The private FileId was never published when CLOSE consumed its slot. */
    assert(smb2_close(conn, created.file_id) == ST_FILE_CLOSED);
} /* create_query_close */

static void
existing_query_close(
    struct smb2_conn *conn,
    const char       *name,
    int               rejects)
{
    struct smb2_create_out opened;

    assert(smb2_create_opts(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES,
                            MBT_FILE_SHARE_RWD, 0, NULL, &opened) == ST_SUCCESS);
    /* Build a normal QUERY followed by a related CLOSE of the existing FID. */
    int                    body_len = smb2c_build_create_full(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES,
                                                              MBT_FILE_SHARE_RWD, 0, NULL, NULL, 0);
    (void) body_len;
    uint8_t               *query = conn->sbuf + 4;
    uint64_t               mid   = g64(query, 24);
    p16(query, 12, SMB2_QUERY_INFO);
    p32(query, 20, SMB2_HDR_SIZE + 40);
    uint8_t               *body = query + SMB2_HDR_SIZE;
    memset(body, 0, 40);
    p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
    p32(body, 4, 4096); memcpy(body + 24, opened.file_id, 16);
    uint8_t               *close = query + SMB2_HDR_SIZE + 40;
    related_header(close, SMB2_CLOSE, mid + 1, 0);
    body = close + SMB2_HDR_SIZE;
    memset(body, 0, 24); p16(body, 0, 24); p16(body, 2, 1);
    memset(body + 8, 0xff, 16);
    expected_groups = 2;
    reject_count    = rejects;
    atomic_store(&held, 0); atomic_store(&release_finish, 0); atomic_store(&attempts, 0);
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    smb2c_send(conn, 40 + SMB2_HDR_SIZE + 24);
    conn->msg_id = mid + 2;
    uint64_t       deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&held)) {
        smb2_pump(conn->env);
        assert(smb2c_now_ms() < deadline && !conn->disconnected);
    }
    atomic_store(&armed, 0);
    /* The retirement journal and resource CLOSE are still tentative. */
    uint8_t        data[64]; uint32_t length;
    assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                           opened.file_id, 0, data, sizeof(data), &length) == ST_SUCCESS);
    int            replies = conn->nreply_app;
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(conn, replies, "accepted or rejected CLOSE"));
    assert(atomic_load(&submissions) == 1);
    bool           exhausted = rejects > CHIMERA_FRONTEND_COMPOUND_RETRIES;
    assert(atomic_load(&attempts) == (exhausted ? CHIMERA_FRONTEND_COMPOUND_RETRIES + 1 : rejects + 1));
    uint32_t       expected = exhausted ? 0xc00000e5u : ST_SUCCESS; /* INTERNAL_ERROR */
    const uint8_t *reply    = conn->rbuf + 4;
    assert(g16(reply, 12) == SMB2_QUERY_INFO && g32(reply, 8) == expected && g64(reply, 24) == mid);
    uint32_t       offset = g32(reply, 20);
    assert(offset && offset + SMB2_HDR_SIZE <= (unsigned int) conn->rlen - 4);
    reply += offset;
    assert(g16(reply, 12) == SMB2_CLOSE);
    assert(cancel_close ? g32(reply, 8) != ST_SUCCESS : g32(reply, 8) == expected);
    reject_count = -1;
    if (exhausted) {
        assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                               opened.file_id, 0, data, sizeof(data), &length) == ST_SUCCESS);
        assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    } else {
        assert(smb2_close(conn, opened.file_id) == ST_FILE_CLOSED);
    }
} /* existing_query_close */

static void
overwrite_denied(
    struct smb2_conn *conn,
    const char       *name,
    uint32_t          disposition,
    const uint8_t     held_fid[16],
    uint32_t          expected,
    uint32_t          desired,
    uint32_t          share)
{
    int          length = smb2c_build_create_full(conn, name, disposition, desired,
                                                  share, MBT_FILE_NON_DIRECTORY_FILE, NULL, NULL, 0);
    uint8_t     *header    = conn->sbuf + 4;
    uint64_t     mid       = g64(header, 24);
    unsigned int first_len = (SMB2_HDR_SIZE + length + 7) & ~7u;

    memset(header + SMB2_HDR_SIZE + length, 0, first_len - SMB2_HDR_SIZE - length);
    p32(header, 20, first_len);
    uint8_t     *query = header + first_len;
    memcpy(query, header, SMB2_HDR_SIZE);
    p16(query, 12, SMB2_QUERY_INFO); p32(query, 20, 0); p64(query, 24, mid + 1);
    uint8_t     *body = query + SMB2_HDR_SIZE;
    memset(body, 0, 40); p16(body, 0, 41);
    body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
    p32(body, 4, 4096); memcpy(body + 24, held_fid, 16);
    expected_groups = 2; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    smb2c_send(conn, first_len + 40);
    conn->msg_id = mid + 2;
    smb2c_wait(conn);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    const uint8_t *reply = conn->rbuf + 4;
    if (g32(reply, 8) != expected) {
        fprintf(stderr, "overwrite %u denial actual=%08x expected=%08x\n", disposition, g32(reply, 8), expected);
    }
    assert(g32(reply, 8) == expected);
    assert(g32(reply, 20)); reply += g32(reply, 20);
    assert(g16(reply, 12) == SMB2_QUERY_INFO && g32(reply, 8) == ST_SUCCESS);
    uint8_t        data[5]; uint32_t bytes;
    assert(smb2_read(conn, held_fid, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
           bytes == sizeof(data) && !memcmp(data, "keep!", sizeof(data)));
} /* overwrite_denied */

/* A failed CREATE must not leave its provisional denied-W reservation
 * blocking the next independent CREATE in the same VFS submission. The error
 * is injected before OVERWRITE dispatch, so the original bytes must survive. */
static void
overwrite_failure_continuation(
    struct smb2_conn *conn,
    int               failure_phase,
    uint32_t          disposition,
    uint32_t          desired,
    bool              stream)
{
    char                   name[96];

    snprintf(name, sizeof(name), "overwrite-reservation-%d-%u-%u.txt%s",
             failure_phase, disposition, desired, stream ? ":fork" : "");
    struct smb2_create_out seed;
    uint32_t               bytes;
    assert(smb2_create(conn, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &seed) == ST_SUCCESS)
    ;
    assert(smb2_write(conn, seed.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS && bytes == 5);
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
    uint8_t                packet[2048] = {
        0
    };
    int                    first_body = smb2c_build_create_full(conn, name, disposition,
                                                                desired, failure_phase ? MBT_FILE_SHARE_READ :
                                                                MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                                                NULL, 0
                                                                );
    uint64_t               mid       = g64(conn->sbuf + 4, 24);
    unsigned int           first_len = (SMB2_HDR_SIZE + first_body + 7) & ~7u;
    memcpy(packet, conn->sbuf + 4, SMB2_HDR_SIZE + first_body);
    p32(packet, 20, first_len);
    int                    second_body = smb2c_build_create_full(conn, name, MBT_FILE_OPEN,
                                                                 failure_phase ? MBT_FILE_ALL_ACCESS :
                                                                 MBT_FILE_READ_ACCESS,
                                                                 failure_phase ? MBT_FILE_SHARE_RWD :
                                                                 MBT_FILE_SHARE_READ,
                                                                 MBT_FILE_NON_DIRECTORY_FILE, NULL, NULL, 0);
    unsigned int           second_len = (SMB2_HDR_SIZE + second_body + 7) & ~7u;
    uint8_t               *second     = packet + first_len;
    memcpy(second, conn->sbuf + 4, SMB2_HDR_SIZE + second_body);
    p32(second, 20, second_len); p64(second, 24, mid + 1);
    uint8_t               *query = second + second_len;
    related_header(query, SMB2_QUERY_INFO, mid + 2, SMB2_HDR_SIZE + 40);
    uint8_t               *body = query + SMB2_HDR_SIZE;
    p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
    p32(body, 4, 4096); memset(body + 24, 0xff, 16);
    uint8_t               *close = query + SMB2_HDR_SIZE + 40;
    related_header(close, SMB2_CLOSE, mid + 3, 0);
    body = close + SMB2_HDR_SIZE;
    p16(body, 0, 24); memset(body + 8, 0xff, 16);
    unsigned int           size = first_len + second_len + SMB2_HDR_SIZE + 40 + SMB2_HDR_SIZE + 24;
    assert(size <= sizeof(packet)); memcpy(conn->sbuf + 4, packet, size);
    expected_groups = 4; fail_overwrite = failure_phase;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    smb2c_send(conn, size - SMB2_HDR_SIZE); conn->msg_id = mid + 4; smb2c_wait(conn);
    atomic_store(&armed, 0); fail_overwrite              = 0;
    assert(atomic_load(&submissions) == 1);
    struct smb2_create_out first_open;
    if (!failure_phase) {
        smb2c_parse_create(conn, &first_open);
    }
    const uint8_t         *reply = conn->rbuf + 4;
    for (unsigned int i = 0; i < 4; i++) {
        uint32_t expected = i == 0 && failure_phase ? ST_ACCESS_DENIED : ST_SUCCESS;
        if (g32(reply, 8) != expected) {
            fprintf(stderr, "failed overwrite continuation command %u: %08x expected %08x\n",
                    i, g32(reply, 8), expected);
        }
        assert(g32(reply, 8) == expected);
        if (i < 3) {
            assert(g32(reply, 20)); reply += g32(reply, 20);
        }
    }
    if (!failure_phase) {
        assert(smb2_close(conn, first_open.file_id) == ST_SUCCESS);
    }
    assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS, MBT_FILE_SHARE_RWD, NULL, &seed) == ST_SUCCESS);
    uint8_t  data[5];
    uint32_t status = smb2_read(conn, seed.file_id, 0, sizeof(data), data, &bytes);
    assert(failure_phase ? status == ST_SUCCESS && bytes == sizeof(data) &&
           !memcmp(data, "keep!", sizeof(data)) : status == ST_END_OF_FILE);
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
} /* overwrite_failure_continuation */

static void
overwrite_cases(struct smb2_conn *conn)
{
    const uint32_t dispositions[] = {
        MBT_FILE_OVERWRITE,
        MBT_FILE_OVERWRITE_IF,
        MBT_FILE_SUPERSEDE
    };

    for (unsigned int i = 0; i < sizeof(dispositions) / sizeof(dispositions[0]); i++) {
        char                   name[64]; snprintf(name, sizeof(name), "overwrite-%u.txt", dispositions[i]);
        struct smb2_create_out seed, held;
        uint32_t               bytes;
        assert(smb2_create(conn, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &seed) ==
               ST_SUCCESS);
        assert(smb2_write(conn, seed.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS && bytes == 5);
        assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
        assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                           MBT_FILE_SHARE_READ, NULL, &held) == ST_SUCCESS);
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD);
        /* Existing denied-W blocks even a metadata-only truncating request. */
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_RWD);
        assert(smb2_close(conn, held.file_id) == ST_SUCCESS);
        assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &held) ==
               ST_SUCCESS);
        /* Conversely, the truncating metadata request's own deny bits must
         * arbitrate against existing readers/writers before bytes change. */
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_READ | MBT_FILE_SHARE_DELETE);
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_WRITE | MBT_FILE_SHARE_DELETE);
        uint8_t basic[40] = {
            0
        }; p32(basic, 32, 1); /* READONLY */
        assert(smb2_set_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                             held.file_id, basic, sizeof(basic)) == ST_SUCCESS);
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_ACCESS_DENIED,
                         MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD);
        p32(basic, 32, 0x80); /* NORMAL */
        assert(smb2_set_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                             held.file_id, basic, sizeof(basic)) == ST_SUCCESS);
        data_open_access       = 1;
        expected_create_action = dispositions[i] == MBT_FILE_SUPERSEDE ? 0 : 3;
        expected_create_size   = 0;
        create_query_close(conn, name, dispositions[i], MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
        uint8_t data[5];
        assert(smb2_read(conn, held.file_id, 0, sizeof(data), data, &bytes) == ST_END_OF_FILE);
        assert(smb2_close(conn, held.file_id) == ST_SUCCESS);
        if (dispositions[i] != MBT_FILE_OVERWRITE) {
            snprintf(name, sizeof(name), "overwrite-new-%u.txt", dispositions[i]);
            expected_create_action = 2;
            create_query_close(conn, name, dispositions[i], MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
        }
        expected_create_action = -1; expected_create_size = -1; data_open_access = 0;
    }
} /* overwrite_cases */

static uint32_t
ea_encode(
    uint8_t    *out,
    const char *name,
    const void *value,
    uint16_t    length,
    bool        last)
{
    unsigned int namelen = strlen(name);
    uint32_t     size    = 9 + namelen + length;

    if (!last) {
        size = (size + 3) & ~3u;
    }
    memset(out, 0, size); p32(out, 0, last ? 0 : size);
    out[5] = namelen; p16(out, 6, length);
    memcpy(out + 8, name, namelen);
    if (length) {
        memcpy(out + 9 + namelen, value, length);
    }
    return size;
} /* ea_encode */

static void
create_ea_cases(struct smb2_conn *conn)
{
    uint8_t          value[2048], large[2100], small[64], expect[64], list[128];

    for (unsigned int i = 0; i < sizeof(value); i++) {
        value[i] = i;
    }
    uint32_t         large_len = ea_encode(large, "Large", value, sizeof(value), true);
    uint32_t         small_len = ea_encode(small, "Foo", "one", 3, true);
    struct smb2_cctx ctx       = {
        (const uint8_t *) "ExtA",
        4,
        small,
        small_len
    };
    data_open_access = 1;
    expected_ea      = small; expected_ea_len = small_len;
    create_query_close(conn, "create-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    /* Exercise the dynamically appended MKDIR prepare path as well as the
     * regular-file OPEN path before the same-compound EA query and close. */
    create_query_close(conn, "create-ea-dir", MBT_FILE_CREATE, MBT_FILE_DIRECTORY_FILE, &ctx, 1, 0);
    /* Existing OPEN also applies ExtA, retaining the stored spelling. */
    small_len       = ea_encode(small, "fOo", "two", 3, true);
    expected_ea_len = ea_encode(expect, "Foo", "two", 3, true); expected_ea = expect;
    ctx.data_len    = small_len;
    create_query_close(conn, "create-ea.txt", MBT_FILE_OPEN, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    /* OVERWRITE updates bytes first and then installs the supplied EAs. */
    small_len       = ea_encode(small, "FOO", "new", 3, true); ctx.data_len = small_len;
    expected_ea_len = ea_encode(expect, "Foo", "new", 3, true);
    create_query_close(conn, "create-ea.txt", MBT_FILE_OVERWRITE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    ctx.data = large; ctx.data_len = large_len; expected_ea = large; expected_ea_len = large_len;
    create_query_close(conn, "create-large-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    /* A later duplicate replaces and releases an earlier overflow buffer. */
    struct smb2_cctx duplicate[] = {
        ctx,
        {
            (const uint8_t *) "ExtA",
            4,
            small,
            small_len
        }
    };
    expected_ea = small; expected_ea_len = small_len;
    create_query_close(conn, "create-duplicate-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, duplicate, 2, 0);
    uint32_t         pos = ea_encode(list, "absent", NULL, 0, false);
    memcpy(list + pos, small, small_len);
    ctx.data = list; ctx.data_len = pos + small_len;
    create_query_close(conn, "create-delete-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    pos         = ea_encode(list, "NewName", "first", 5, false);
    pos        += ea_encode(list + pos, "newname", "second", 6, true);
    ctx.data    = list; ctx.data_len = pos;
    expected_ea = expect; expected_ea_len = ea_encode(expect, "NewName", "second", 6, true);
    create_query_close(conn, "create-case-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    pos          = ea_encode(list, "NEWNAME", NULL, 0, false);
    pos         += ea_encode(list + pos, "newNAME", "third", 5, true);
    ctx.data_len = pos; expected_ea_len = ea_encode(expect, "newNAME", "third", 5, true);
    create_query_close(conn, "create-case-ea.txt", MBT_FILE_OPEN, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    pos          = ea_encode(list, "Fresh", "one", 3, false);
    pos         += ea_encode(list + pos, "fresh", NULL, 0, false);
    pos         += ea_encode(list + pos, "FRESH", "two", 3, true);
    ctx.data_len = pos; expected_ea_len = ea_encode(expect, "FRESH", "two", 3, true);
    create_query_close(conn, "create-case-delete-ea.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    expected_ea = NULL; expected_ea_len = 0;
    /* A later invalid entry fails CREATE but preserves the earlier EA and
     * created file as ordinary accepted prefix effects. */
    pos          = ea_encode(list, "Prefix", "ok", 2, false);
    pos         += ea_encode(list + pos, "bad:name", "x", 1, true);
    ctx.data_len = pos;
    int                    len = smb2c_build_create_full(conn, "create-error-ea.txt", MBT_FILE_CREATE,
                                                         MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                                         MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                                         &ctx, 1);
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    uint32_t               status = smb2c_xfer(conn, len);
    atomic_store(&armed, 0);
    assert(status == 0x80000013u); /* INVALID_EA_NAME */
    assert(atomic_load(&submissions) == 1);
    struct smb2_create_out opened;
    assert(smb2_create(conn, "create-error-ea.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &opened) == ST_SUCCESS);
    uint8_t                result[128]; uint32_t result_len;
    assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, opened.file_id, 0,
                           result, sizeof(result), &result_len) == ST_SUCCESS);
    uint32_t               want = ea_encode(expect, "Prefix", "ok", 2, true);
    assert(result_len == ((want + 3) & ~3u) && !memcmp(result, expect, want));
    for (uint32_t i = want; i < result_len; i++) {
        assert(result[i] == 0);
    }
    assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    data_open_access = 0;
} /* create_ea_cases */

/* Mandatory-oplock CREATE retains its explicit legacy lifecycle boundary;
 * its EA filesystem suffix must nevertheless submit exactly one compound. */
static void
legacy_create_ea_cases(struct smb2_conn *conn)
{
    struct smb2_oplock_req caching = {
        .is_lease    = 1,

        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE |
            SMB2_LEASE_HANDLE,

        .lease_key   = {
            0x92,
            0x31
        }
    };
    uint8_t                list[256], expected[64], actual[256];

    for (unsigned int phase = 0; phase < 2; phase++) {
        uint32_t length = 0;
        if (!phase) {
            length += ea_encode(list + length, "Legacy", "one", 3, false);
            length += ea_encode(list + length, "legacy", "two", 3, true);
        } else {
            length += ea_encode(list + length, "LEGACY", NULL, 0, false);
            length += ea_encode(list + length, "leGacy", "three", 5, false);
            length += ea_encode(list + length, "LEgACY", "four", 4, true);
        }
        struct smb2_cctx       ctx = {
            (const uint8_t *) "ExtA",
            4,
            list,
            length
        };
        int                    body_len = smb2c_build_create_full(conn, "legacy-create-ea.txt",
                                                                  phase ? MBT_FILE_OPEN : MBT_FILE_CREATE,
                                                                  MBT_FILE_ALL_ACCESS,
                                                                  MBT_FILE_SHARE_RWD,
                                                                  MBT_FILE_NON_DIRECTORY_FILE | 0x00010000u /* FILE_OPEN_REQUIRING_OPLOCK */
                                                                  , &caching, &ctx, 1);
        expected_groups = 0; legacy_ea_adapter = 1;
        atomic_store(&submissions, 0); atomic_store(&armed, 1);
        uint32_t               status = smb2c_xfer(conn, body_len);
        atomic_store(&armed, 0); legacy_ea_adapter = 0;
        assert(status == ST_SUCCESS && atomic_load(&submissions) == 2);
        struct smb2_create_out opened; smb2c_parse_create(conn, &opened);
        assert(opened.status == ST_SUCCESS && opened.oplock == SMB2_OPLOCK_LEVEL_LEASE &&
               opened.has_lease && opened.lease_state == caching.lease_state);
        uint32_t               actual_len;
        assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, opened.file_id, 0,
                               actual, sizeof(actual), &actual_len) == ST_SUCCESS);
        uint32_t               expected_len = phase ? ea_encode(expected, "leGacy", "four", 4, true) :
            ea_encode(expected, "Legacy", "two", 3, true);
        assert(actual_len == ((expected_len + 3) & ~3u) && !memcmp(actual, expected, expected_len));
        for (uint32_t j = expected_len; j < actual_len; j++) {
            assert(actual[j] == 0);
        }
        assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    }
    /* Failed CREATE never exposes a usable FileId. Its accepted first EA
     * remains, but its deny-all SHARE reservation must not leak in the tree. */
    uint32_t               length = ea_encode(list, "Prefix", "kept", 4, false);
    length += ea_encode(list + length, "bad:name", "x", 1, true);
    struct smb2_cctx       ctx = {
        (const uint8_t *) "ExtA",
        4,
        list,
        length
    };
    int                    body_len = smb2c_build_create_full(conn, "legacy-error-ea.txt", MBT_FILE_CREATE,
                                                              MBT_FILE_ALL_ACCESS, 0, MBT_FILE_NON_DIRECTORY_FILE |
                                                              0x00010000u                                                       /* FILE_OPEN_REQUIRING_OPLOCK */
                                                              , &caching, &ctx, 1);
    expected_groups = 0; legacy_ea_adapter = 1;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    uint32_t               status = smb2c_xfer(conn, body_len);
    atomic_store(&armed, 0); legacy_ea_adapter = 0;
    assert(status == 0x80000013u && atomic_load(&submissions) == 2);
    struct smb2_create_out opened;
    assert(smb2_create(conn, "legacy-error-ea.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       0, NULL, &opened) == ST_SUCCESS);
    uint32_t               actual_len;
    assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, opened.file_id, 0,
                           actual, sizeof(actual), &actual_len) == ST_SUCCESS);
    uint32_t               expected_len = ea_encode(expected, "Prefix", "kept", 4, true);
    assert(actual_len == ((expected_len + 3) & ~3u) && !memcmp(actual, expected, expected_len));
    assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
} /* legacy_create_ea_cases */

static void
create_ea_budget_case(
    struct smb2_conn *conn,
    const char       *name)
{
    enum { ENTRIES = 1100 };
    uint8_t               *list = calloc(ENTRIES, 20);
    assert(list);
    uint32_t               length = 0;
    for (unsigned int i = 0; i < ENTRIES; i++) {
        length += ea_encode(list + length, i & 1 ? "budget" : "Budget",
                            i == ENTRIES - 1 ? "last" : "x", i == ENTRIES - 1 ? 4 : 1,
                            i == ENTRIES - 1);
    }
    struct smb2_cctx       ctx = {
        (const uint8_t *) "ExtA",
        4,
        list,
        length
    };
    int                    body_len = smb2c_build_create_full(conn, name, MBT_FILE_CREATE,
                                                              MBT_FILE_ALL_ACCESS, 0, MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                                              &ctx, 1
                                                              );
    ea_budget_boundary = 1;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    uint32_t               status = smb2c_xfer(conn, body_len);
    atomic_store(&armed, 0); ea_budget_boundary = 0;
    assert(status == ST_SUCCESS && atomic_load(&submissions) >= 3);
    struct smb2_create_out opened; smb2c_parse_create(conn, &opened);
    uint8_t                actual[64], expected[64]; uint32_t actual_len;
    assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, opened.file_id, 0,
                           actual, sizeof(actual), &actual_len) == ST_SUCCESS);
    uint32_t               expected_len = ea_encode(expected, "Budget", "last", 4, true);
    assert(actual_len == ((expected_len + 3) & ~3u) && !memcmp(actual, expected, expected_len));
    assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    free(list);
} /* create_ea_budget_case */

static void
stream_create_cases(struct smb2_conn *conn)
{
    struct smb2_create_out base, stream;
    uint32_t               length;
    uint8_t                data[4];

    assert(smb2_create(conn, "stream-native.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_write(conn, base.file_id, 0, "base", 4, &length) == ST_SUCCESS && length == 4);
    data_open_access       = 1;
    expected_create_action = 2;
    create_query_close(conn, "stream-native.txt:fork:$DATA", MBT_FILE_CREATE,
                       MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
    expected_create_action = 1;
    create_query_close(conn, "stream-native.txt:fork", MBT_FILE_OPEN_IF,
                       MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
    expected_create_action = -1;
    data_chain_name        = "stream-native.txt:fork";
    data_open_chain(conn, NULL, -1);
    data_chain_name = NULL;
    assert(smb2_read(conn, base.file_id, 0, 4, data, &length) == ST_SUCCESS &&
           length == 4 && !memcmp(data, "base", 4));
    /* A surviving stream transfers both ACCESS owners and the base anchor;
     * a collision must withdraw its temporary base reservation. */
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    uint32_t               status = smb2_create(conn, "stream-native.txt:fork", MBT_FILE_OPEN,
                                                MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &stream);
    atomic_store(&armed, 0);
    assert(status == ST_SUCCESS && atomic_load(&submissions) == 1);
    assert(smb2_read(conn, stream.file_id, 0, 4, data, &length) == ST_SUCCESS &&
           length == 4 && !memcmp(data, "data", 4));
    struct smb2_create_out collision;
    assert(smb2_create(conn, "stream-native.txt:fork", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &collision) == ST_OBJECT_NAME_COLLISION);
    assert(smb2_close(conn, stream.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);
    expected_create_action = 2;
    create_query_close(conn, "stream-created-base.txt:new", MBT_FILE_OPEN_IF,
                       MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
    /* Ordinary post-create admission failure preserves both created forks,
    * while readiness cleanup removes both anonymous SHARE reservations. */
    expected_groups = 1; fail_stream_share = 1;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    status = smb2_create(conn, "stream-failed-base.txt:fork", MBT_FILE_CREATE,
                         MBT_FILE_ALL_ACCESS, 0, NULL, &stream);
    atomic_store(&armed, 0); fail_stream_share = 0;
    assert(status == ST_SHARING_VIOLATION && atomic_load(&submissions) == 1);
    assert(smb2_create(conn, "stream-failed-base.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       0, NULL, &base) == ST_SUCCESS);
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "stream-failed-base.txt:fork", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       0, NULL, &stream) == ST_SUCCESS);
    assert(smb2_close(conn, stream.file_id) == ST_SUCCESS);
    expected_create_action = -1; data_open_access = 0;
} /* stream_create_cases */

/* Stream truncation must occur after admission, affect only the named fork,
 * and leave both provisional owners usable by related commands. */
static void
stream_extended_cases(struct smb2_conn *conn)
{
    struct smb2_create_out base, sibling, seed, held, rejected;
    uint32_t               bytes;
    uint8_t                data[8];

    assert(smb2_create(conn, "stream-overwrite", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_write(conn, base.file_id, 0, "base", 4, &bytes) == ST_SUCCESS);
    assert(smb2_create(conn, "stream-overwrite:sibling", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &sibling) == ST_SUCCESS);
    assert(smb2_write(conn, sibling.file_id, 0, "sibling", 7, &bytes) == ST_SUCCESS);
    const uint32_t         dispositions[] = {
        MBT_FILE_OVERWRITE,
        MBT_FILE_OVERWRITE_IF,
        MBT_FILE_SUPERSEDE
    };
    for (unsigned int i = 0; i < 3; i++) {
        char name[96]; snprintf(name, sizeof(name), "stream-overwrite:fork-%u", i);
        assert(smb2_create(conn, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &seed) == ST_SUCCESS);
        assert(smb2_write(conn, seed.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS);
        overwrite_denied(conn, name, dispositions[i], seed.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_READ | MBT_FILE_SHARE_DELETE);
        assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
        assert(smb2_create(conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                           MBT_FILE_SHARE_READ, NULL, &held) == ST_SUCCESS);
        overwrite_denied(conn, name, dispositions[i], held.file_id, ST_SHARING_VIOLATION,
                         MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD);
        assert(smb2_close(conn, held.file_id) == ST_SUCCESS);
        data_open_access       = 1;
        expected_create_action = dispositions[i] == MBT_FILE_SUPERSEDE ? 0 : 3;
        expected_create_size   = 0;
        create_query_close(conn, name, dispositions[i], MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
        assert(smb2_read(conn, base.file_id, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
               bytes == 4 && !memcmp(data, "base", 4));
        assert(smb2_read(conn, sibling.file_id, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
               bytes == 7 && !memcmp(data, "sibling", 7));
        if (dispositions[i] != MBT_FILE_OVERWRITE) {
            snprintf(name, sizeof(name), "stream-overwrite-new-%u:fork", i);
            expected_create_action = 2;
            create_query_close(conn, name, dispositions[i], MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
        }
    }
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    assert(smb2_create(conn, "stream-overwrite-absent:fork", MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
    assert(smb2_create(conn, "stream-overwrite-absent", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(conn, "stream-overwrite:absent", MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
    /* Old DOS attributes must be checked before OPEN_STREAM creates a fork
     * and stamps shared metadata, including metadata-only truncating opens. */
    for (unsigned int bit = 1; bit <= 4; bit <<= 1) {
        uint8_t basic[40] = {
            0
        }; p32(basic, 32, bit);
        assert(smb2_set_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                             base.file_id, basic, sizeof(basic)) == ST_SUCCESS);
        for (unsigned int i = 0; i < 3; i++) {
            expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
            assert(smb2_create(conn, "stream-overwrite:protected-new", dispositions[i],
                               MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_RWD, NULL, &rejected) == ST_ACCESS_DENIED);
            atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
            assert(smb2_create(conn, "stream-overwrite:protected-new", MBT_FILE_OPEN,
                               MBT_FILE_READ_ATTRIBUTES, MBT_FILE_SHARE_RWD, NULL, &rejected) ==
                   ST_OBJECT_NAME_NOT_FOUND);
        }
        uint8_t attrs[40]; uint32_t attr_len;
        assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                               base.file_id, 0, attrs, sizeof(attrs), &attr_len) == ST_SUCCESS);
        assert(attr_len == sizeof(attrs) && (g32(attrs, 32) & bit));
    }
    assert(smb2_close(conn, sibling.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);
    expected_create_action = -1; expected_create_size = -1;

    /* EAs belong to the base metadata, including case-preserving sequential
     * updates made through a stream producer and queried by related FileId. */
    uint8_t          list[128], expected[64], result[128];
    uint32_t         length = ea_encode(list, "ForkEA", "one", 3, false);
    length += ea_encode(list + length, "forkea", "two", 3, true);
    struct smb2_cctx ctx = {
        (const uint8_t *) "ExtA",
        4,
        list,
        length
    };
    expected_ea = expected; expected_ea_len = ea_encode(expected, "ForkEA", "two", 3, true);
    create_query_close(conn, "stream-ea:fork", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    length          = ea_encode(list, "FORKEA", NULL, 0, false);
    length         += ea_encode(list + length, "forkEA", "new", 3, true);
    ctx.data_len    = length;
    expected_ea_len = ea_encode(expected, "forkEA", "new", 3, true);
    create_query_close(conn, "stream-ea:fork", MBT_FILE_OVERWRITE, MBT_FILE_NON_DIRECTORY_FILE, &ctx, 1, 0);
    expected_ea = NULL; expected_ea_len = 0;
    assert(smb2_create(conn, "stream-ea", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    uint32_t result_len;
    assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, base.file_id, 0,
                           result, sizeof(result), &result_len) == ST_SUCCESS);
    uint32_t want = ea_encode(expected, "forkEA", "new", 3, true);
    assert(result_len == ((want + 3) & ~3u) && !memcmp(result, expected, want));
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);

    /* A later EA error preserves created forks and the successful EA prefix,
     * but neither reservation nor a public FileId may escape failed readiness. */
    length       = ea_encode(list, "Prefix", "ok", 2, false);
    length      += ea_encode(list + length, "bad:name", "x", 1, true);
    ctx.data_len = length;
    int body = smb2c_build_create_full(conn, "stream-ea-failed:fork", MBT_FILE_CREATE,
                                       MBT_FILE_ALL_ACCESS, 0, MBT_FILE_NON_DIRECTORY_FILE, NULL, &ctx, 1);
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    assert(smb2c_xfer(conn, body) == 0x80000013u);
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
    assert(smb2_create(conn, "stream-ea-failed", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0, NULL, &base) == ST_SUCCESS);
    assert(smb2_query_info(conn, 1, SMB2_FILE_FULL_EA_INFO_T, base.file_id, 0,
                           result, sizeof(result), &result_len) == ST_SUCCESS);
    want = ea_encode(expected, "Prefix", "ok", 2, true);
    assert(result_len == ((want + 3) & ~3u) && !memcmp(result, expected, want));
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "stream-ea-failed:fork", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0, NULL, &seed) == ST_SUCCESS)
    ;
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
    data_open_access = 0;
} /* stream_extended_cases */

static void
unbuffered_create_cases(struct smb2_conn *conn)
{
    const uint32_t access[] = {
        MBT_FILE_READ_ATTRIBUTES | 4u,
        2u | 4u,
        0x40000000u,
        0x10000000u,
        0x02000000u
    };
    const uint32_t granted[] = {
        MBT_FILE_READ_ATTRIBUTES,
        2u,
        0x00120112u,
        0x001f01fbu,
        0
    };

    for (unsigned int i = 0; i < sizeof(access) / sizeof(access[0]); i++) {
        char                   name[64]; snprintf(name, sizeof(name), "unbuffered-%u.txt", i);
        int                    length = smb2c_build_create_full(conn, name, MBT_FILE_CREATE, access[i],
                                                                MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE | 8u,
                                                                NULL, NULL
                                                                , 0);
        uint8_t               *first     = conn->sbuf + 4;
        uint64_t               mid       = g64(first, 24);
        unsigned int           first_len = (SMB2_HDR_SIZE + length + 7) & ~7u;
        memset(first + SMB2_HDR_SIZE + length, 0, first_len - SMB2_HDR_SIZE - length);
        p32(first, 20, first_len);
        uint8_t               *query = first + first_len;
        related_header(query, SMB2_QUERY_INFO, mid + 1, SMB2_HDR_SIZE + 40);
        uint8_t               *body = query + SMB2_HDR_SIZE;
        memset(body, 0, 40); p16(body, 0, 41); body[2] = 1; body[3] = SMB2_FILE_ACCESS_INFO_T;
        p32(body, 4, 4096); memset(body + 24, 0xff, 16);
        uint8_t               *close = query + SMB2_HDR_SIZE + 40;
        related_header(close, SMB2_CLOSE, mid + 2, 0);
        body = close + SMB2_HDR_SIZE; memset(body, 0, 24)
        ; p16(body, 0, 24); memset(body + 8, 0xff, 16);
        expected_groups = 3; atomic_store(&submissions, 0);
        atomic_store(&armed, 1);
        smb2c_send(conn, first_len + SMB2_HDR_SIZE + 40 + 24); conn->msg_id = mid + 3; smb2c_wait(conn);
        atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
        const uint8_t         *reply = conn->rbuf + 4;
        assert(g32(reply, 8) == ST_SUCCESS && g32(reply, 20)); reply += g32(reply, 20);
        assert(g32(reply, 8) == ST_SUCCESS);
        const uint8_t         *out = reply + SMB2_HDR_SIZE;
        assert(g32(out, 4) == 4);
        uint32_t               mask = g32(reply + g16(out, 2), 0);
        assert(!(mask & 4u));
        assert(granted[i] ? mask == granted[i] : (mask & 2u) != 0);
        assert(g32(reply, 20)); reply += g32(reply, 20); assert(g32(reply, 8) == ST_SUCCESS);
        /* OPEN uses the same normalization, including denied WRITE when the
         * only input data right was APPEND. No fake finish rejection here. */
        struct smb2_create_out opened;
        expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
        assert(smb2_create_opts(conn, name, MBT_FILE_OPEN, access[i], MBT_FILE_SHARE_RWD,
                                MBT_FILE_NON_DIRECTORY_FILE | 8u, NULL, &opened) == ST_SUCCESS);
        atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
        uint32_t               bytes;
        assert(smb2_write(conn, opened.file_id, 0, "x", 1, &bytes) == (i ? ST_SUCCESS : ST_ACCESS_DENIED));
        assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    }
    struct smb2_create_out denied;
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    assert(smb2_create_opts(conn, "unbuffered-append-only.txt", MBT_FILE_CREATE, 4u,
                            MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE | 8u, NULL, &denied) == ST_ACCESS_DENIED);
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
    assert(smb2_create(conn, "unbuffered-append-only.txt", MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES,
                       MBT_FILE_SHARE_RWD, NULL, &denied) == ST_OBJECT_NAME_NOT_FOUND);
} /* unbuffered_create_cases */

static void
reparse_option_cases(struct smb2_conn *conn)
{
    const uint32_t         option = 0x00200000u;

    data_open_access = 1;
    create_query_close(conn, "reparse-option.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE | option, NULL, 0, 0);
    const uint32_t         dispositions[] = {
        MBT_FILE_OPEN,
        MBT_FILE_OPEN_IF,
        MBT_FILE_OVERWRITE,
        MBT_FILE_OVERWRITE_IF
        ,
        MBT_FILE_SUPERSEDE
    };
    for (unsigned int i = 0; i < sizeof(dispositions) / sizeof(dispositions[0]); i++) {
        create_query_close(conn, "reparse-option.txt", dispositions[i], MBT_FILE_NON_DIRECTORY_FILE | option, NULL, 0, 0
                           );
    }
    create_query_close(conn, "reparse-option-dir", MBT_FILE_CREATE, MBT_FILE_DIRECTORY_FILE | option, NULL, 0, 1);
    create_query_close(conn, "reparse-option-dir", MBT_FILE_OPEN_IF, MBT_FILE_DIRECTORY_FILE | option, NULL, 0, 1);
    data_open_access = 0;
    struct smb2_create_out target, link, rejected;
    const char            *target_name = "reparse-target.txt";
    assert(smb2_create(conn, target_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &target) ==
           ST_SUCCESS);
    uint32_t               bytes;
    assert(smb2_write(conn, target.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS);
    assert(smb2_create(conn, "reparse-link.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &link)
           ==
           ST_SUCCESS);
    uint8_t                reparse[256] = {
        0
    };
    unsigned int           nlen = strlen(target_name) * 2;
    p32(reparse, 0, SMB2_IO_REPARSE_TAG_SYMLINK); p16(reparse, 4, 12 + 2 * nlen);
    p16(reparse, 10, nlen); p16(reparse, 12, nlen); p16(reparse, 14, nlen); p32(reparse, 16, 1);
    utf16le(target_name, reparse + 20); utf16le(target_name, reparse + 20 + nlen);
    assert(smb2_ioctl(conn, SMB2_FSCTL_SET_REPARSE_POINT, link.file_id, reparse, 20 + 2 * nlen) == ST_SUCCESS);
    assert(smb2_close(conn, link.file_id) == ST_SUCCESS);
    /* The discovery compound must stop before touching the existing link;
     * legacy MBT_FILE_CREATE retains its collision result rather than following. */
    reparse_collision_boundary = 1; expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    assert(smb2_create_opts(conn, "reparse-link.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE | option, NULL, &rejected) ==
           ST_OBJECT_NAME_COLLISION);
    atomic_store(&armed, 0); reparse_collision_boundary = 0; assert(atomic_load(&submissions) == 2);
    assert(smb2_create_opts(conn, "reparse-link.txt", MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES,
                            MBT_FILE_SHARE_RWD, option, NULL, &link) == ST_SUCCESS);
    uint32_t       status, length;
    const uint8_t *out = smb2_ioctl_out(conn, SMB2_FSCTL_GET_REPARSE_POINT, link.file_id,
                                        NULL, 0, sizeof(reparse), &status, &length);
    assert(status == ST_SUCCESS && out && length >= 20 && g32(out, 0) == SMB2_IO_REPARSE_TAG_SYMLINK);
    assert(g16(out, 10) == nlen && !memcmp(out + 20 + g16(out, 8), reparse + 20, nlen));
    uint8_t        data[5];
    assert(smb2_read(conn, target.file_id, 0, 5, data, &bytes) == ST_SUCCESS && bytes == 5 && !memcmp(data, "keep!", 5))
    ;
    assert(smb2_close(conn, link.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, target.file_id) == ST_SUCCESS);
} /* reparse_option_cases */

static void
related_creates(struct smb2_conn *conn)
{
    uint8_t      packet[2048] = {
        0
    };
    int          first_body = smb2c_build_create_full(conn, "related-first.txt", MBT_FILE_CREATE,
                                                      MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                                      MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                                      NULL, 0);
    uint64_t     mid       = g64(conn->sbuf + 4, 24);
    unsigned int first_len = (SMB2_HDR_SIZE + first_body + 7) & ~7u;

    memcpy(packet, conn->sbuf + 4, SMB2_HDR_SIZE + first_body);
    p32(packet, 20, first_len);
    int          second_body = smb2c_build_create_full(conn, "related-second.txt", MBT_FILE_CREATE,
                                                       MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                                       MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                                       NULL, 0);
    unsigned int second_len = (SMB2_HDR_SIZE + second_body + 7) & ~7u;
    uint8_t     *second     = packet + first_len;
    memcpy(second + SMB2_HDR_SIZE, conn->sbuf + 4 + SMB2_HDR_SIZE, second_body);
    related_header(second, SMB2_CREATE, mid + 1, second_len);
    uint8_t     *query = second + second_len;
    related_header(query, SMB2_QUERY_INFO, mid + 2, SMB2_HDR_SIZE + 40);
    uint8_t     *body = query + SMB2_HDR_SIZE;
    p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T; body[3] = SMB2_FILE_BASIC_INFO_T;
    p32(body, 4, 4096); memset(body + 24, 0xff, 16);
    uint8_t     *close = query + SMB2_HDR_SIZE + 40;
    related_header(close, SMB2_CLOSE, mid + 3, 0);
    body = close + SMB2_HDR_SIZE;
    p16(body, 0, 24); memset(body + 8, 0xff, 16);
    unsigned int size = first_len + second_len + SMB2_HDR_SIZE + 40 + SMB2_HDR_SIZE + 24;
    assert(size <= sizeof(packet));
    memcpy(conn->sbuf + 4, packet, size);
    expected_groups = 4;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    smb2c_send(conn, size - SMB2_HDR_SIZE);
    conn->msg_id = mid + 4;
    smb2c_wait(conn);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    struct smb2_create_out first_open;
    smb2c_parse_create(conn, &first_open);
    const uint8_t         *reply = conn->rbuf + 4;
    for (unsigned int i = 0; i < 4; i++) {
        assert(g32(reply, 8) == ST_SUCCESS && g64(reply, 24) == mid + i);
        uint32_t offset = g32(reply, 20);
        if (i < 3) {
            assert(offset); reply += offset;
        } else {
            assert(!offset);
        }
    }
    assert(smb2_close(conn, first_open.file_id) == ST_SUCCESS);
} /* related_creates */

int
main(void)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = {
        .oplocks       = 1,
        .leases        = 1,
        .named_streams = 1
    };

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn    *conn = smb2_conn_open(&env);
    smb2_handshake(conn);
    /* A path-sized parser buffer must never permit an oversized basename
     * to reach the fixed-size provisional-open/name-token constructors. */
    for (unsigned int length = 256; length <= 512; length += 256) {
        char                   oversized[513];
        memset(oversized, 'x', length); oversized[length] = 0;
        struct smb2_create_out rejected;
        assert(smb2_create(conn, oversized, MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &rejected) == 0xc0000106u); /* NAME_TOO_LONG */
    }
    uint8_t          allocation[8] = {
        0
    }, sd[48] = {
        0
    };
    p64(allocation, 0, 4096);
    sd[0]  = 1; p16(sd, 2, 0x8004); p32(sd, 16, 20); /* self-relative DACL */
    sd[20] = 2; p16(sd, 22, 28); p16(sd, 24, 1);
    p16(sd, 30, 20); p32(sd, 32, MBT_FILE_ALL_ACCESS); /* allow Everyone */
    sd[36] = 1; sd[37] = 1; sd[43] = 1; p32(sd, 44, 0);
    struct smb2_cctx contexts[] = {

        {
            (const uint8_t *) "MxAc",
            4,
            NULL,
            0
        },

        {
            (const uint8_t *) "QFid",
            4,
            NULL,
            0
        },

        {
            (const uint8_t *) "AlSi",
            4,
            allocation,
            sizeof(allocation)
        },

        {
            (const uint8_t *) "SecD",
            4,
            sd,
            sizeof(sd)
        },


    };
    create_query_close(conn, "contexts.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE,
                       contexts, 4, 0);
    create_query_close(conn, "contexts.txt", MBT_FILE_OPEN, MBT_FILE_NON_DIRECTORY_FILE,
                       contexts, 2, 0);
    data_open_access = 1;
    create_query_close(conn, "contexts.txt", MBT_FILE_OPEN, MBT_FILE_NON_DIRECTORY_FILE,
                       contexts, 2, 0);
    data_open_access = 0;
    struct smb2_create_out prefix_open;
    assert(smb2_create(conn, "contexts.txt", MBT_FILE_OPEN, MBT_FILE_READ_ATTRIBUTES,
                       MBT_FILE_SHARE_RWD, NULL, &prefix_open) == ST_SUCCESS);
    data_open_chain(conn, NULL, -1);
    data_open_chain(conn, NULL, 0);
    data_open_chain(conn, prefix_open.file_id, 2);
    data_open_chain(conn, prefix_open.file_id, CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    assert(smb2_close(conn, prefix_open.file_id) == ST_SUCCESS);
    live_cache_open(&env, conn, SMB2_OPLOCK_LEVEL_BATCH);
    live_cache_open(&env, conn, SMB2_OPLOCK_LEVEL_EXCLUSIVE);
    data_open_access       = 1;
    expected_create_action = 2; /* FILE_CREATED */
    create_query_close(conn, "open-if-new.txt", MBT_FILE_OPEN_IF, MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
    expected_create_action = 1; /* FILE_OPENED */
    create_query_close(conn, "open-if-new.txt", MBT_FILE_OPEN_IF, MBT_FILE_NON_DIRECTORY_FILE, NULL, 0, 0);
    create_query_close(conn, "contexts.txt", MBT_FILE_OPEN_IF, MBT_FILE_NON_DIRECTORY_FILE, contexts, 2, 0);
    struct smb2_create_out preserved;
    uint8_t                preserved_data[4]; uint32_t preserved_length;
    assert(smb2_create(conn, "contexts.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &preserved) == ST_SUCCESS);
    assert(smb2_read(conn, preserved.file_id, 0, sizeof(preserved_data), preserved_data,
                     &preserved_length) == ST_SUCCESS && preserved_length == 4 && !memcmp(preserved_data, "data", 4));
    assert(smb2_close(conn, preserved.file_id) == ST_SUCCESS);
    expected_create_action = 2;
    create_query_close(conn, "open-if-dir", MBT_FILE_OPEN_IF, MBT_FILE_DIRECTORY_FILE, NULL, 0, 1);
    expected_create_action = 1;
    create_query_close(conn, "open-if-dir", MBT_FILE_OPEN_IF, MBT_FILE_DIRECTORY_FILE, NULL, 0, 1);
    expected_create_action = -1; data_open_access = 0;
    overwrite_cases(conn);
    create_ea_cases(conn);
    legacy_create_ea_cases(conn);
    create_ea_budget_case(conn, "create-ea-budget.txt");
    create_ea_budget_case(conn, "create-ea-budget-stream.txt:fork:$DATA");
    stream_create_cases(conn);
    stream_extended_cases(conn);
    unbuffered_create_cases(conn);
    reparse_option_cases(conn);
    overwrite_failure_continuation(conn, 1, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS, false);
    overwrite_failure_continuation(conn, 1, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS, true);
    overwrite_failure_continuation(conn, 2, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS, false);
    overwrite_failure_continuation(conn, 2, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS, true);
    const uint32_t truncate_dispositions[] = {
        MBT_FILE_OVERWRITE,
        MBT_FILE_OVERWRITE_IF,
        MBT_FILE_SUPERSEDE
    };
    for (unsigned int i = 0; i < 3; i++) {
        overwrite_failure_continuation(conn, 0, truncate_dispositions[i], MBT_FILE_READ_ATTRIBUTES, false);
        overwrite_failure_continuation(conn, 0, truncate_dispositions[i], MBT_FILE_READ_ATTRIBUTES, true);
        overwrite_failure_continuation(conn, 0, truncate_dispositions[i], MBT_FILE_READ_ACCESS, false);
        overwrite_failure_continuation(conn, 0, truncate_dispositions[i], MBT_FILE_READ_ACCESS, true);
    }
    existing_query_close(conn, "contexts.txt", 0);
    existing_query_close(conn, "contexts.txt", 2);
    existing_query_close(conn, "contexts.txt", CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    cancel_close = 1;
    existing_query_close(conn, "contexts.txt", 0);
    cancel_close = 0;
    related_creates(conn);
    create_query_close(conn, "", MBT_FILE_OPEN, MBT_FILE_DIRECTORY_FILE, contexts, 2, 1);
    struct smb2_create_out dir;
    assert(smb2_create_opts(conn, "nested", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    assert(smb2_close(conn, dir.file_id) == ST_SUCCESS);
    create_query_close(conn, "nested\\child.txt", MBT_FILE_CREATE, MBT_FILE_NON_DIRECTORY_FILE,
                       contexts, 2, 0);
    create_query_close(conn, "nested", MBT_FILE_OPEN, MBT_FILE_DIRECTORY_FILE, contexts, 2, 1);
    create_query_close(conn, "compound-directory", MBT_FILE_CREATE, MBT_FILE_DIRECTORY_FILE, contexts, 2, 1);
    create_query_close(conn, "compound-directory\\nested", MBT_FILE_CREATE, MBT_FILE_DIRECTORY_FILE, contexts, 2, 1);
    /* MKDIR is an accepted prefix even when later admission ordinarily fails. */
    struct smb2_create_out failed_directory;
    expected_groups = 1; fail_directory_admission = 1;
    atomic_store(&failed_directory_notices, 0);
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
    uint32_t               failed_status = smb2_create_opts(conn, "created-admission-failure", MBT_FILE_CREATE,
                                                            MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                                            MBT_FILE_DIRECTORY_FILE, NULL,
                                                            &failed_directory);
    atomic_store(&armed, 0); fail_directory_admission = 0;
    assert(failed_status == ST_SHARING_VIOLATION && atomic_load(&submissions) == 1);
    assert(atomic_load(&failed_directory_notices) == 1);
    existing_query_close(conn, "created-admission-failure", 0);
    existing_query_close(conn, "nested", 0);
    existing_query_close(conn, "", 0);

    /* Keep 257 opens alive: there are 256 SMB registry buckets, so at least
    * one accepted publication inserts into an occupied bucket. Then retire
    * the whole tree, exercising both transferred and merged hash tables. */
    for (unsigned int i = 0; i < 257; i++) {
        char                   name[48];
        struct smb2_create_out opened;
        snprintf(name, sizeof(name), "registry-%u.txt", i);
        expected_groups = 1;
        atomic_store(&submissions, 0);
        atomic_store(&armed, 1);
        uint32_t               result = smb2_create(conn, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                                    MBT_FILE_SHARE_RWD, NULL, &opened);
        atomic_store(&armed, 0);
        assert(result == ST_SUCCESS && atomic_load(&submissions) == 1);
    }
    assert(smb2_tree_disconnect(conn) == ST_SUCCESS);
    assert(smb2_tree_connect(conn, "\\\\server\\share") == ST_SUCCESS);
    create_query_close(conn, "registry-256.txt", MBT_FILE_OPEN, MBT_FILE_NON_DIRECTORY_FILE,
                       contexts, 2, 0);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
