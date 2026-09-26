// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#define _GNU_SOURCE
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include <stdatomic.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"

extern void notify_test_inspect(void *, unsigned int);
extern void notify_test_track_free(const uint8_t file_id[16]);
extern atomic_int notify_test_target_freed;
extern atomic_int notify_test_cleanups, notify_test_attached;
extern atomic_int notify_test_retired_session, notify_test_retired_freed;
extern atomic_int notify_test_wire_capture, notify_test_wire_count;
extern atomic_int notify_test_delete_cutoff;
extern atomic_int notify_test_cross_capture, notify_test_cross_count, notify_test_cross_freed;
static atomic_int arm, paused, release_hold, completed, check_waiters, checked, expected_waiters;
static int pause_before_submit, pause_before_retire, reject_first;
static _Thread_local struct evpl *owner_evpl;
typedef void (*submit_fn)(struct chimera_vfs_compound *, chimera_vfs_compound_callback_t, void *);
struct hold {
    struct evpl_timer timer;
    struct evpl *evpl;
    struct chimera_vfs_compound *compound;
    chimera_vfs_compound_callback_t callback;
    void *private_data, *command;
    int finishes;
    chimera_vfs_compound_coordinate_t coordinate;
    void *coordinate_private;
    uint32_t coordinate_index, fh_len;
    uint64_t coordinate_token;
    const uint8_t *fh;
};

__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(struct chimera_vfs_thread *thread, const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next); owner_evpl = thread->evpl;
    return next(thread, cred);
}

static void
hold_complete(struct chimera_vfs_compound *compound, void *private_data)
{
    struct hold *ctx = private_data;
    bool accepted = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void *arg = ctx->private_data;
    if (accepted) { free(ctx); }
    callback(compound, arg);
    if (accepted) { atomic_store(&completed, 1); }
}

static void
poll_hold(struct evpl *evpl, struct evpl_timer *timer)
{
    struct hold *ctx = (struct hold *) ((char *) timer - offsetof(struct hold, timer));
    if (atomic_exchange(&check_waiters, 0)) {
        notify_test_inspect(ctx->command, atomic_load(&expected_waiters));
        atomic_store(&checked, 1);
    }
    if (!atomic_exchange(&release_hold, 0)) {
        evpl_add_oneshot_timer(evpl, timer, poll_hold, 1000);
        return;
    }
    atomic_store(&paused, 0);
    if (ctx->coordinate) {
        ctx->coordinate(ctx->compound, ctx->coordinate_index, ctx->coordinate_token,
                        ctx->fh, ctx->fh_len, ctx->coordinate_private);
    } else if (pause_before_submit) {
        submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
        assert(next);
        next(ctx->compound, hold_complete, ctx);
    } else {
        chimera_vfs_compound_finish_result(ctx->compound,
            reject_first ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
    }
}

static void
coordinate_hold(struct chimera_vfs_compound *compound, uint32_t index,
                uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct hold *ctx = private_data;
    ctx->compound = compound; ctx->coordinate_index = index; ctx->coordinate_token = token;
    ctx->fh = fh; ctx->fh_len = fh_len;
    evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, poll_hold, 1000);
    atomic_store(&paused, 1);
}

static void
finish_hold(struct chimera_vfs_compound *compound, void *private_data)
{
    struct hold *ctx = private_data;
    if (ctx->finishes++) {
        /* The first attempt only performed read/resource operations. */
        notify_test_inspect(ctx->command, atomic_load(&expected_waiters));
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
        return;
    }
    ctx->compound = compound;
    evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, poll_hold, 1000);
    atomic_store(&paused, 1);
}

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(struct chimera_vfs_compound *compound,
                            chimera_vfs_compound_callback_t callback, void *private_data)
{
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    void *command = NULL;
    for (unsigned int i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS) { command = op->callback_private; }
    }
    if (command && atomic_exchange(&arm, 0)) {
        struct hold *ctx = calloc(1, sizeof(*ctx));
        assert(ctx && owner_evpl);
        ctx->compound = compound; ctx->callback = callback; ctx->private_data = private_data;
        ctx->evpl = owner_evpl; ctx->command = command;
        if (pause_before_submit) {
            evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, poll_hold, 1000);
            atomic_store(&paused, 1);
            return;
        }
        if (pause_before_retire) {
            for (unsigned int i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                struct chimera_vfs_compound_op *op =
                    (struct chimera_vfs_compound_op *) chimera_vfs_compound_op(compound, i);
                if (op->type != CHIMERA_VFS_COMPOUND_OP_COORDINATE) { continue; }
                ctx->coordinate = op->coordinate; ctx->coordinate_private = op->coordinate_private;
                op->coordinate = coordinate_hold; op->coordinate_private = ctx;
                break;
            }
            assert(ctx->coordinate);
        } else { chimera_vfs_compound_set_finish_handler(compound, finish_hold, ctx); }
        next(compound, hold_complete, ctx);
        return;
    }
    next(compound, callback, private_data);
}

static void
wait_flag_at(struct smb2_env *env, atomic_int *flag, const char *name, const char *where, int line)
{
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(flag) && smb2c_now_ms() < deadline) { smb2_pump(env); }
    if (!atomic_load(flag)) {
        fprintf(stderr, "notify probe timeout: %s at %s:%d (arm=%d paused=%d completed=%d)\n",
                name, where, line, atomic_load(&arm), atomic_load(&paused), atomic_load(&completed));
    }
    assert(atomic_load(flag));
}
#define wait_flag(env, flag) wait_flag_at(env, flag, #flag, __func__, __LINE__)

static void
close_post(struct smb2_conn *c, const uint8_t *fid)
{
    int offset = smb2c_begin(c, SMB2_CLOSE, 0);
    uint8_t *body = c->sbuf + offset;
    p16(body, 0, 24); p16(body, 2, 1); p32(body, 4, 0); memcpy(body + 8, fid, 16);
    smb2c_send(c, 24);
}

static uint64_t
notify_post(struct smb2_env *env, struct smb2_conn *c, const uint8_t *fid)
{
    int count = c->ninterim;
    uint64_t id = c->msg_id;
    smb2c_post_change_notify(c, fid, 0, 4096, SMB2_NOTIFY_CHANGE_FILE_NAME);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (c->ninterim == count && smb2c_now_ms() < deadline) { smb2_pump(env); }
    assert(c->ninterim == count + 1 && c->last_async_id == id);
    return id;
}

static void
notify_wait(struct smb2_env *env, struct smb2_conn *c, uint64_t id, uint32_t status)
{
    struct smb2_notify_msg reply;
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (smb2c_now_ms() < deadline) {
        if (smb2_conn_take_notify(c, id, &reply)) { assert(reply.status == status); return; }
        smb2_pump(env);
    }
    assert(!"missing notify completion");
}

static void
assert_waiters(struct smb2_env *env, unsigned int count)
{
    atomic_store(&expected_waiters, count);
    atomic_store(&checked, 0); atomic_store(&check_waiters, 1);
    wait_flag(env, &checked);
}

static void
nonfinal_notify(struct smb2_env *env, struct smb2_conn *c, const uint8_t *fid)
{
    int replies = c->nreply_app, interims = c->ninterim;
    int offset = smb2c_begin(c, SMB2_CHANGE_NOTIFY, 0);
    uint8_t *h = c->sbuf + 4, *body = c->sbuf + offset, *echo = h + 96;
    p16(body, 0, 32); p32(body, 4, 4096); memcpy(body + 8, fid, 16);
    p32(body, 24, SMB2_NOTIFY_CHANGE_FILE_NAME);
    memcpy(echo, h, 64); p16(echo, 12, SMB2_ECHO);
    p64(echo, 24, c->msg_id + 1); p16(echo + 64, 0, 4);
    p32(h, 20, 96);
    smb2c_send(c, 100); c->msg_id++;
    assert(smb2c_pump_for_nreply(c, replies, "nonfinal deferred notify"));
    assert(g32(c->rbuf + 4, 8) == ST_INTERNAL_ERROR);
    uint32_t next = g32(c->rbuf + 4, 20);
    assert(next && g32(c->rbuf + 4 + next, 8) == ST_SUCCESS);
    assert(c->ninterim == interims);
    assert_waiters(env, 0);
}

static void
late_notify_cases(struct smb2_env *env, struct smb2_conn *primary)
{
    for (unsigned int mode = 0; mode < 4; mode++) {
        struct smb2_conn *c = primary;
        if (mode == 3) { c = smb2_conn_open(env); smb2_handshake(c); }
        char name[64]; snprintf(name, sizeof(name), "notify-after-close-%u", mode);
        struct smb2_create_out file;
        assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
        pause_before_submit = 0; reject_first = mode == 1;
        atomic_store(&completed, 0); atomic_store(&arm, 1);
        int replies = c->nreply_app;
        close_post(c, file.file_id);
        wait_flag(env, &paused);
        if (mode == 0) {
            nonfinal_notify(env, c, file.file_id);
            replies = c->nreply_app;
        }
        uint64_t id = notify_post(env, c, file.file_id);
        assert_waiters(env, 1);
        if (mode == 2) {
            smb2c_post_cancel_async(c, id);
            notify_wait(env, c, id, ST_CANCELLED);
            assert_waiters(env, 0);
        }
        if (mode == 3) {
            notify_test_track_free(file.file_id);
            smb2_conn_disconnect(c);
            wait_flag(env, &notify_test_target_freed);
            assert_waiters(env, 0);
        }
        atomic_store(&release_hold, 1);
        wait_flag(env, &completed);
        if (mode != 3) {
            assert(smb2c_pump_for_nreply(c, replies, "native CLOSE acceptance"));
            assert(g32(c->rbuf + 4, 8) == ST_SUCCESS);
            if (mode != 2) { notify_wait(env, c, id, ST_FILE_CLOSED); }
            assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
        }
    }
}

static void
early_notify_case(struct smb2_env *env, struct smb2_conn *c)
{
    struct smb2_create_out file;
    assert(smb2_create_opts(c, "notify-before-close-fence", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
    pause_before_submit = 1; reject_first = 0;
    atomic_store(&completed, 0); atomic_store(&arm, 1);
    int closes = atomic_load(&notify_test_cleanups), replies = c->nreply_app;
    close_post(c, file.file_id); wait_flag(env, &paused);
    uint64_t id = notify_post(env, c, file.file_id);
    atomic_store(&release_hold, 1); wait_flag(env, &completed);
    assert(smb2c_pump_for_nreply(c, replies, "late-watch native CLOSE acceptance"));
    assert(g32(c->rbuf + 4, 8) == ST_SUCCESS);
    notify_wait(env, c, id, ST_NOTIFY_CLEANUP);
    assert(atomic_load(&notify_test_cleanups) == closes + 1);
    assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
}

/* Existing attachments must survive both execution and a rejected finish.
 * The arm can only be consumed by a typed RETIRE_OPEN_CLAIMS compound. */
static void
attached_notify_cases(struct smb2_env *env)
{
    for (unsigned int mode = 0; mode < 5; mode++) {
        struct smb2_conn *c = smb2_conn_open(env); smb2_handshake(c);
        struct smb2_create_out file;
        char name[64]; snprintf(name, sizeof(name), "notify-attached-native-%u", mode);
        assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
        uint64_t id = notify_post(env, c, file.file_id);
        atomic_store(&notify_test_attached, 1);
        pause_before_submit = 0; pause_before_retire = mode == 3; reject_first = mode == 1;
        atomic_store(&completed, 0); atomic_store(&arm, 1);
        int replies = c->nreply_app, cleanups = atomic_load(&notify_test_cleanups);
        uint64_t close_id = c->msg_id;
        close_post(c, file.file_id); wait_flag(env, &paused);
        assert_waiters(env, 1);
        assert(atomic_load(&notify_test_cleanups) == cleanups);
        if (mode == 2 || mode == 3) {
            int offset = smb2c_begin(c, SMB2_CANCEL, 0);
            p64(c->sbuf + 4, 24, close_id); p16(c->sbuf + offset, 0, 4);
            smb2c_send(c, 4);
            /* ECHO orders processing of CANCEL before releasing held work. */
            (void) smb2_echo_barrier(c);
            replies = c->nreply_app;
            assert_waiters(env, 1);
        }
        if (mode == 4) {
            notify_test_track_free(file.file_id);
            smb2_conn_disconnect(c);
            wait_flag(env, &notify_test_target_freed);
            /* The client disconnect callback does not establish that the
             * server has run teardown. Observe its detached-state cleanup
             * hook before asking the owning loop to inspect the empty list. */
            uint64_t deadline = smb2c_now_ms() + 10000;
            while (atomic_load(&notify_test_cleanups) == cleanups &&
                   smb2c_now_ms() < deadline) { smb2_pump(env); }
            if (atomic_load(&notify_test_cleanups) != cleanups + 1) {
                fprintf(stderr, "notify probe: missing server-side disconnect cleanup\n");
            }
            assert(atomic_load(&notify_test_cleanups) == cleanups + 1);
            atomic_store(&notify_test_attached, 0);
            assert_waiters(env, 0);
        }
        atomic_store(&release_hold, 1); wait_flag(env, &completed);
        if (mode != 4) {
            assert(smb2c_pump_for_nreply(c, replies, "watched native CLOSE"));
            assert(g32(c->rbuf + 4, 8) == (mode == 3 ? ST_CANCELLED : ST_SUCCESS));
            if (mode == 3) {
                /* Cancellation before retirement leaves the original watch
                 * attached; cancel just that nr, then close the idle watch. */
                assert(atomic_load(&notify_test_cleanups) == cleanups);
                smb2c_post_cancel_async(c, id);
                notify_wait(env, c, id, ST_CANCELLED);
                assert(smb2_close(c, file.file_id) == ST_SUCCESS);
            } else { notify_wait(env, c, id, ST_NOTIFY_CLEANUP); }
            assert(atomic_load(&notify_test_cleanups) == cleanups + 1);
            assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
        }
        atomic_store(&notify_test_attached, 0);
        pause_before_retire = 0;
    }
}

static void
watched_doc_case(struct smb2_env *env, struct smb2_conn *c)
{
    struct smb2_create_out file;
    assert(smb2_create_opts(c, "notify-doc-native", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE | FILE_DELETE_ON_CLOSE, NULL, &file) == ST_SUCCESS);
    uint64_t id = notify_post(env, c, file.file_id);
    /* Directory DOC remains a legacy CLOSE boundary. Its real removal must
     * still settle this watch as cleanup, with no native/retry claim here. */
    assert(smb2_close(c, file.file_id) == ST_SUCCESS);
    notify_wait(env, c, id, ST_NOTIFY_CLEANUP);
    assert(smb2_create_opts(c, "notify-doc-native", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &file) == ST_OBJECT_NAME_NOT_FOUND);
}

/* Internal ordering fixture: inject a VFS deletion notification immediately
 * before/after cleanup's cutoff, without pretending to delete the backend. */
static void
delete_cutoff_cases(struct smb2_env *env, struct smb2_conn *c)
{
    for (unsigned int after = 0; after < 2; after++) {
        struct smb2_create_out file;
        char name[64]; snprintf(name, sizeof(name), "notify-delete-cutoff-%u", after);
        assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
        uint64_t id = notify_post(env, c, file.file_id);
        atomic_store(&notify_test_delete_cutoff, after ? 2 : 1);
        assert(smb2_close(c, file.file_id) == ST_SUCCESS);
        notify_wait(env, c, id, after ? ST_NOTIFY_CLEANUP : ST_DELETE_PENDING);
        assert(!atomic_load(&notify_test_delete_cutoff));
    }
}

static void
cross_worker_cleanup_case(struct smb2_env *env, struct smb2_conn *c)
{
    struct smb2_conn *peer = smb2_conn_open(env); smb2_handshake(peer);
    struct smb2_create_out first, second;
    assert(smb2_create_opts(c, "notify-cross-worker-a", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &first) == ST_SUCCESS);
    assert(smb2_create_opts(peer, "notify-cross-worker-b", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &second) == ST_SUCCESS);
    atomic_store(&notify_test_cross_capture, 1);
    uint64_t id1 = notify_post(env, c, first.file_id);
    uint64_t id2 = notify_post(env, peer, second.file_id);
    assert(atomic_load(&notify_test_cross_count) == 2);
    assert(smb2_close(c, first.file_id) == ST_SUCCESS);
    notify_wait(env, c, id1, ST_NOTIFY_CLEANUP);
    notify_wait(env, peer, id2, ST_NOTIFY_CLEANUP);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (atomic_load(&notify_test_cross_freed) != 2 && smb2c_now_ms() < deadline) { smb2_pump(env); }
    assert(atomic_load(&notify_test_cross_freed) == 2);
    atomic_store(&notify_test_cross_capture, 0);
    assert(smb2_close(peer, second.file_id) == ST_SUCCESS);
}

static struct smb2_conn *
replace_session(struct smb2_env *env, struct smb2_conn *old)
{
    struct smb2_conn *c = smb2_conn_reopen_raw(env, old);
    assert(!c->wire && smb2_negotiate(c) == ST_SUCCESS);
    uint8_t blob[2048];
    int length = ntlm_type1(blob);
    assert(smb2c_session_setup_leg(c, blob, length) == ST_MORE_PROCESSING_REQUIRED);
    c->session_id = g64(c->rbuf + 4, 40);
    length = ntlm_type3_anon(blob);
    int offset = smb2c_begin(c, SMB2_SESSION_SETUP, 0);
    uint8_t *body = c->sbuf + offset;
    p16(body, 0, 25); body[3] = 1;
    p16(body, 12, SMB2_HDR_SIZE + 24); p16(body, 14, length);
    p64(body, 16, old->session_id);
    memcpy(body + 24, blob, length);
    assert(smb2c_xfer(c, 24 + length) == ST_SUCCESS);
    assert(smb2_tree_connect(c, "\\\\chimera\\share") == ST_SUCCESS);
    return c;
}

static void
previous_session_cases(struct smb2_env *env)
{
    for (unsigned int waiting = 0; waiting < 2; waiting++) {
        struct smb2_conn *c = smb2_conn_open(env); smb2_handshake(c);
        char name[64]; snprintf(name, sizeof(name), "notify-previous-session-%u", waiting);
        struct smb2_create_out base, file;
        assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, NULL, &base) == ST_SUCCESS);
        assert(smb2_close(c, base.file_id) == ST_SUCCESS);
        struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
        memset(lease.lease_key, 0xc1 + waiting, 16);
        struct smb2_durable_req durable = { .dh2q = 1 };
        memset(durable.create_guid, 0xd1 + waiting, 16);
        assert(smb2_create_dur_opts(c, name, FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, &lease, &durable, &file) == ST_SUCCESS && file.has_dh2q);
        if (waiting) {
            pause_before_submit = 0; reject_first = 0;
            atomic_store(&completed, 0); atomic_store(&arm, 1);
            close_post(c, file.file_id); wait_flag(env, &paused);
        }
        uint64_t id = notify_post(env, c, file.file_id);
        if (waiting) { assert_waiters(env, 1); }
        struct smb2_conn *replacement = replace_session(env, c);
        /* No fence release is needed to observe session invalidation. */
        notify_wait(env, c, id, ST_NOTIFY_CLEANUP);
        if (waiting) {
            assert_waiters(env, 0);
            atomic_store(&release_hold, 1); wait_flag(env, &completed);
        } else {
            struct smb2_durable_req reclaim = { .dh2c = 1 };
            memcpy(reclaim.file_id, file.file_id, 16);
            memcpy(reclaim.create_guid, durable.create_guid, 16);
            assert(smb2_create_dur_opts(replacement, "", FILE_OPEN, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &reclaim, &file) == ST_SUCCESS);
            /* The old closing watch must have been detached before parking,
             * so a reclaimed durable directory can admit a fresh watcher. */
            uint64_t fresh = notify_post(env, replacement, file.file_id);
            smb2c_post_cancel_async(replacement, fresh);
            notify_wait(env, replacement, fresh, ST_CANCELLED);
            assert(smb2_close(replacement, file.file_id) == ST_SUCCESS);
        }
    }
}

static uint32_t
bind_leg(struct smb2_conn *c, const uint8_t *blob, int length)
{
    int offset = smb2c_begin(c, SMB2_SESSION_SETUP, 0);
    uint8_t *body = c->sbuf + offset;
    p16(body, 0, 25); body[2] = 1; body[3] = 1;
    p16(body, 12, SMB2_HDR_SIZE + 24); p16(body, 14, length);
    memcpy(body + 24, blob, length);
    return smb2c_xfer(c, 24 + length);
}

/* A real authenticated SMB3 channel binding. Initial binding requests and
 * SESSION_SETUP replies use the original session signing key; subsequent
 * messages use the independently negotiated channel signing key. */
static struct smb2_conn *
bind_channel(struct smb2_env *env, struct smb2_conn *first)
{
    struct smb2_conn *c = smb2_conn_open(env);
    c->guid_tag = first->guid_tag;
    assert(smb2_negotiate(c) == ST_SUCCESS && c->dialect == 0x0300);
    c->session_id = first->session_id; c->tree_id = first->tree_id;
    memcpy(c->signing_key, first->signing_key, sizeof(c->signing_key));
    c->signing_on = 1;
    uint8_t blob[2048];
    int length = smb2w_ntlm_negotiate(blob, false);
    assert(bind_leg(c, blob, length) == ST_MORE_PROCESSING_REQUIRED);
    const uint8_t *body = c->rbuf + 4 + SMB2_HDR_SIZE;
    uint16_t offset = g16(body, 4), size = g16(body, 6);
    length = smb2w_ntlm_auth_ntlmv2(c->rbuf + 4 + offset, size,
        SMB2W_USER, SMB2W_PASSWORD, SMB2W_DOMAIN, blob, c->session_key);
    assert(bind_leg(c, blob, length) == ST_SUCCESS);
    smb2c_arm_protection(c, NULL);
    return c;
}

static void
sibling_tdis_case(struct smb2_env *env, struct smb2_conn *c, struct smb2_conn *peer)
{
    struct smb2_create_out file;
    assert(smb2_create_opts(c, "notify-sibling-tdis", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
    uint64_t id = notify_post(env, peer, file.file_id);
    atomic_store(&notify_test_attached, 1);
    pause_before_submit = 0; reject_first = 0;
    atomic_store(&completed, 0); atomic_store(&arm, 1);
    int replies = c->nreply_app;
    close_post(c, file.file_id); wait_flag(env, &paused);
    assert_waiters(env, 0); /* watcher belongs to the other connection */
    notify_test_track_free(file.file_id);
    assert(smb2_tree_disconnect(peer) == ST_SUCCESS);
    notify_wait(env, peer, id, ST_NOTIFY_CLEANUP);
    wait_flag(env, &notify_test_target_freed);
    atomic_store(&notify_test_attached, 0);
    assert_waiters(env, 0);
    atomic_store(&release_hold, 1); wait_flag(env, &completed);
    assert(smb2c_pump_for_nreply(c, replies, "CLOSE after sibling TDIS"));
    assert(g32(c->rbuf + 4, 8) == ST_SUCCESS);
    assert(smb2_tree_connect(c, "\\\\chimera\\share") == ST_SUCCESS);
    peer->tree_id = c->tree_id;
}

static void
signed_logoff_compound(struct smb2_conn *c, bool setup_suffix, bool related)
{
    int offset = smb2c_begin(c, SMB2_ECHO, SMB2_FLAGS_SIGNED);
    uint8_t *first = c->sbuf + 4;
    uint8_t *logoff = first + 72, *last = first + 144;
    p16(c->sbuf + offset, 0, 4);
    memcpy(logoff, first, 68); memcpy(last, first, 68);
    p32(first, 20, 72); p32(logoff, 20, 72);
    p16(logoff, 12, SMB2_LOGOFF);
    p64(logoff, 24, c->msg_id + 1); p64(last, 24, c->msg_id + 2);
    int last_len = 68;
    if (setup_suffix) {
        uint8_t blob[2048];
        int length = smb2w_ntlm_negotiate(blob, false);
        p16(last, 12, SMB2_SESSION_SETUP);
        p16(last + 64, 0, 25); last[64 + 3] = 1;
        p16(last + 64, 12, SMB2_HDR_SIZE + 24); p16(last + 64, 14, length);
        memcpy(last + 64 + 24, blob, length);
        last_len = 64 + 24 + length;
    }
    if (related) {
        p32(last, 16, SMB2_FLAGS_SIGNED | SMB2_FLAGS_RELATED_OPERATIONS);
        p64(last, 40, UINT64_MAX); p32(last, 36, UINT32_MAX);
    }
    for (int i = 0; i < 3; i++) {
        uint8_t *header = first + 72 * i, signature[16];
        memset(header + 48, 0, 16);
        smb2w_sign(c->dialect, c->signing_alg, c->signing_key,
            header, i == 2 ? last_len : 72, signature);
        memcpy(header + 48, signature, 16);
    }
    /* Per-element signatures, not the harness's single-element auto-sign. */
    c->signing_on = 0;
    smb2c_send(c, 80 + last_len); c->msg_id += 2;
    c->signing_on = 1;
    assert(smb2c_wait(c) == ST_SUCCESS);
    const uint8_t *reply = c->rbuf + 4;
    for (int i = 0; i < 3; i++) {
        assert(g16(reply, 12) == (i == 1 ? SMB2_LOGOFF :
            (i == 2 && setup_suffix ? SMB2_SESSION_SETUP : SMB2_ECHO)));
        assert(g32(reply, 8) == (i == 2 ? 0xC0000203U : ST_SUCCESS));
        uint32_t next = g32(reply, 20);
        if (i < 2) { assert(next); reply += next; }
        else { assert(!next); }
    }
}

static void
logoff_security_cases(const struct smb2_env_opts *opts)
{
    for (unsigned int encrypted = 0; encrypted < 2; encrypted++) {
        struct smb2_env env;
        smb2_env_open_wire(&env, opts,
            smb2_wire_profile_find(encrypted ? "encrypted311" : "signed30"));
        smb2_env_fs_setup(&env, "fs0");
        struct smb2_conn *c = smb2_conn_open(&env); smb2_handshake(c);
        struct smb2_conn *peer = encrypted ? NULL : bind_channel(&env, c);
        if (peer) { sibling_tdis_case(&env, c, peer); }
        struct smb2_create_out file;
        assert(smb2_create_opts(c, "notify-logoff-security", FILE_CREATE, FILE_ALL_ACCESS,
            FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &file) == ST_SUCCESS);
        if (peer) { atomic_store(&notify_test_wire_capture, 1); }
        uint64_t id = notify_post(&env, c, file.file_id);
        uint64_t peer_id = peer ? notify_post(&env, peer, file.file_id) : 0;
        if (peer) {
            assert(atomic_load(&notify_test_wire_count) == 2);
            atomic_store(&notify_test_wire_capture, 0);
        }
        if (encrypted) { atomic_store(&notify_test_retired_session, 1); }
        if (peer) { signed_logoff_compound(c, false, false); }
        else { assert(smb2_logoff(c) == ST_SUCCESS); }
        notify_wait(&env, c, id, ST_NOTIFY_CLEANUP);
        if (peer) {
            notify_wait(&env, peer, peer_id, ST_NOTIFY_CLEANUP);
            assert(smb2_close(peer, file.file_id) == 0xC0000203U /* USER_SESSION_DELETED */);
            for (unsigned int related = 0; related < 2; related++) {
                struct smb2_conn *fresh = smb2_conn_open(&env); smb2_handshake(fresh);
                signed_logoff_compound(fresh, true, related);
            }
        } else {
            wait_flag(&env, &notify_test_retired_freed);
            atomic_store(&notify_test_retired_session, 0);
        }
        /* Deleted sibling channels still retain their session's other opens.
         * Explicitly drop them before rmfs; its bounded retry must then drain. */
        smb2_conn_reset(&env);
        smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    }
}

int
main(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = 1, .directory_leases = 1,
        .persistent_handles = 1 };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *c = smb2_conn_open(&env); smb2_handshake(c);
    late_notify_cases(&env, c);
    early_notify_case(&env, c);
    attached_notify_cases(&env);
    watched_doc_case(&env, c);
    delete_cutoff_cases(&env, c);
    cross_worker_cleanup_case(&env, c);
    previous_session_cases(&env);
    smb2_conn_reset(&env);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    logoff_security_cases(&opts);
    return 0;
}
