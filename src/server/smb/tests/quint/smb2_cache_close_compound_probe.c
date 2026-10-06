// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
/* Real wire oplock CLOSE: no completed mutation is rejected at finish. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"

extern void smb_cache_close_inspect(
    void *,
    unsigned int);
extern void smb_durable_close_inspect(
    void *,
    bool,
    bool);
extern void smb_durable_close_accepted(
    void);
extern void smb_persisted_close_record_check(
    void *,
    bool,
    void (*)(void *),
    void *);
static unsigned int         expected_members = 1;
static int                  expect_durable, park_durable, reject_first;
enum persistent_case { PERSIST_NONE, PERSIST_NORMAL, PERSIST_PARK, PERSIST_CANCEL, PERSIST_CANCEL_DELETE,
                       PERSIST_MISSING, PERSIST_FAILURE };
static enum persistent_case persistent_case;
static atomic_int           delete_next, delete_seen, delete_status;
static atomic_int           finish_count;
static atomic_int           armed, submissions, hold_next, held, release_finish;
static unsigned int         expected_groups[4];
static                      _Thread_local struct evpl *owner_evpl;
struct finish_hold {
    struct evpl                    *evpl;
    struct evpl_timer               timer;
    struct chimera_vfs_compound    *compound;
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    finishes;
    bool                            check_parked;
};

struct delete_probe {
    struct chimera_vfs_thread        *thread;
    const struct chimera_vfs_cred    *cred;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE], key[64];
    uint32_t                          fh_len, key_len;
    chimera_vfs_delete_key_callback_t callback;
    void                             *private_data;
};

static void
delete_observed(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct delete_probe              *ctx      = private_data;
    chimera_vfs_delete_key_callback_t callback = ctx->callback;
    void                             *arg      = ctx->private_data;

    free(ctx);
    atomic_store(&delete_status, status);
    atomic_store(&delete_seen, 1);
    callback(status, arg);
} /* delete_observed */

static void
delete_missing(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct delete_probe *ctx = private_data;

    assert(status == CHIMERA_VFS_OK); /* This key really existed. */
    typedef void (*delete_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *,
        const void *,
        int,
        const void *,
        uint32_t,
        chimera_vfs_delete_key_callback_t,
        void *);
    delete_fn next = (delete_fn) dlsym(RTLD_NEXT, "chimera_vfs_delete_key_at");
    assert(next);
    next(ctx->thread, ctx->cred, ctx->fh, ctx->fh_len, ctx->key, ctx->key_len,
         delete_observed, ctx);
} /* delete_missing */

__attribute__((visibility("default"))) void
chimera_vfs_delete_key_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fh_len,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data)
{
    typedef void (*delete_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *,
        const void *,
        int,
        const void *,
        uint32_t,
        chimera_vfs_delete_key_callback_t,
        void *);
    delete_fn            next = (delete_fn) dlsym(RTLD_NEXT, "chimera_vfs_delete_key_at");
    assert(next);
    if (!atomic_exchange(&delete_next, 0)) {
        next(thread, cred, fh, fh_len, key, key_len, callback, private_data);
        return;
    }
    struct delete_probe *ctx = calloc(1, sizeof(*ctx));
    assert(ctx && key_len <= sizeof(ctx->key));
    ctx->thread = thread; ctx->cred = cred; ctx->callback = callback; ctx->private_data = private_data;
    ctx->fh_len = fh_len; ctx->key_len = key_len;
    memcpy(ctx->fh, fh, fh_len); memcpy(ctx->key, key, key_len);
    if (persistent_case == PERSIST_FAILURE) {
        delete_observed(CHIMERA_VFS_EIO, ctx); return;
    }
    next(thread, cred, fh, fh_len, key, key_len,
         persistent_case == PERSIST_MISSING ? delete_missing : delete_observed, ctx);
} /* chimera_vfs_delete_key_at */

__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next);
    owner_evpl = thread->evpl;
    return next(thread, cred);
} /* chimera_vfs_compound_alloc */

static void
finish_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct finish_hold *ctx = (struct finish_hold *) ((char *) timer - offsetof(struct finish_hold, timer));

    if (!atomic_exchange(&release_finish, 0)) {
        evpl_add_oneshot_timer(evpl, timer, finish_poll, 1000);
        return;
    }
    atomic_store(&held, 0);
    chimera_vfs_compound_finish_result(ctx->compound,
                                       reject_first ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* finish_poll */

static void
start_finish_hold(void *private_data)
{
    struct finish_hold *ctx = private_data;

    evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, finish_poll, 1000);
    atomic_store(&held, 1);
} /* start_finish_hold */

static void
hold_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_hold *ctx = private_data;

    ctx->compound = compound;
    /* The leading QUERY and terminal CLOSE address the same open. */
    void               *command = chimera_vfs_compound_op(compound, 0)->prepare_private;
    if (expected_members) {
        smb_cache_close_inspect(command, expected_members);
    }
    if (expect_durable) {
        smb_durable_close_inspect(command, park_durable && !ctx->finishes,
                                  persistent_case != PERSIST_NONE);
        ctx->check_parked = park_durable;
    }
    atomic_fetch_add(&finish_count, 1);
    if (ctx->finishes++) {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
        return;
    }
    if (persistent_case != PERSIST_NONE) {
        assert(!reject_first && atomic_load(&delete_seen));
        enum chimera_vfs_error wanted = persistent_case == PERSIST_MISSING ? CHIMERA_VFS_ENOENT :
            persistent_case == PERSIST_FAILURE ? CHIMERA_VFS_EIO : CHIMERA_VFS_OK;
        assert(atomic_load(&delete_status) == wanted);
        smb_persisted_close_record_check(command, persistent_case == PERSIST_FAILURE,
                                         start_finish_hold, ctx);
    } else {
        start_finish_hold(ctx);
    }
} /* hold_finish */

static void (*retire_complete)(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void
cancel_after_retire(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    assert(*status == CHIMERA_VFS_OK);
    retire_complete(compound, index, status, private_data);
    chimera_vfs_compound_cancel(compound);
} /* cancel_after_retire */

static void
hold_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_hold             *ctx          = private_data;
    chimera_vfs_compound_callback_t callback     = ctx->callback;
    void                           *arg          = ctx->private_data;
    bool                            accepted     = chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN;
    bool                            check_parked = accepted && ctx->check_parked;

    if (accepted) {
        free(ctx);
    }
    callback(compound, arg);
    if (check_parked) {
        smb_durable_close_accepted();
    }
} /* hold_complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        unsigned int index = atomic_fetch_add(&submissions, 1);
        assert(index < 4 && chimera_vfs_compound_num_groups(compound) == expected_groups[index]);
        if (atomic_exchange(&hold_next, 0)) {
            if (persistent_case == PERSIST_CANCEL || persistent_case == PERSIST_CANCEL_DELETE) {
                bool wrapped = false;
                for (unsigned int i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
                    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
                    if (op->type != (persistent_case == PERSIST_CANCEL ?
                                     CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS :
                                     CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT)) {
                        continue;
                    }
                    assert(!wrapped && op->complete);
                    wrapped = true; retire_complete = op->complete;
                    /* SMB binding wraps prepare with a different context. */
                    void *prepare_private = op->prepare_private;
                    void  (*prepare)(
                        struct chimera_vfs_compound *,
                        uint32_t,
                        enum chimera_vfs_error *,
                        void *) = op->prepare;
                    chimera_vfs_compound_set_op_callbacks(compound, i, NULL,
                                                          cancel_after_retire, op->callback_private);
                    chimera_vfs_compound_set_op_prepare(compound, i, prepare, prepare_private);
                }
                assert(wrapped);
            }
            struct finish_hold *ctx = calloc(1, sizeof(*ctx));
            assert(ctx && owner_evpl);
            ctx->evpl = owner_evpl; ctx->callback = callback; ctx->private_data = private_data;
            chimera_vfs_compound_set_finish_handler(compound, hold_finish, ctx);
            next(compound, hold_complete, ctx);
            return;
        }
    }
    next(compound, callback, private_data);
} /* chimera_vfs_compound_submit */

struct packet { uint8_t data[1024]; unsigned int length, previous, count; uint64_t mid; };
static uint8_t *
append(
    struct packet    *p,
    struct smb2_conn *c,
    uint16_t          command,
    unsigned int      size)
{
    unsigned int start = (p->length + 7) & ~7u;

    if (p->count) {
        p32(p->data + p->previous, 20, start - p->previous);
    } else {
        p->mid = c->msg_id;
    }
    uint8_t     *h = p->data + start;
    assert(start + SMB2_HDR_SIZE + size <= sizeof(p->data));
    memcpy(h, "\xfeSMB", 4); p16(h, 4, 64); p16(h, 6, 1);
    p16(h, 12, command); p16(h, 14, 32); p64(h, 24, p->mid + p->count);
    p32(h, 36, c->tree_id); p64(h, 40, c->session_id);
    p->count++; p->previous = start; p->length = start + SMB2_HDR_SIZE + size;
    return h + SMB2_HDR_SIZE;
} /* append */
static void
query(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t    *fid)
{
    uint8_t *b = append(p, c, SMB2_QUERY_INFO, 40);

    p16(b, 0, 41); b[2] = 1; b[3] = 4; p32(b, 4, 2048); memcpy(b + 24, fid, 16);
} /* query */
static void
close_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t    *fid)
{
    uint8_t *b = append(p, c, SMB2_CLOSE, 24);

    p16(b, 0, 24); p16(b, 2, 1); memcpy(b + 8, fid, 16);
} /* close_op */
static void
send_packet(
    struct packet    *p,
    struct smb2_conn *c)
{
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE);
    c->msg_id = p->mid + p->count;
} /* send_packet */
static void
check_packet(
    struct packet    *p,
    struct smb2_conn *c,
    const uint32_t   *statuses)
{
    const uint8_t *h = c->rbuf + 4;

    for (unsigned int i = 0; i < p->count; i++) {
        assert(h + 64 <= c->rbuf + c->rlen);
        assert(g64(h, 24) == p->mid + i && g32(h, 8) == statuses[i]);
        uint32_t next = g32(h, 20);
        assert((next != 0) == (i + 1 < p->count));
        if (next) {
            assert(!(next & 7) && next >= 64); h += next;
        }
    }
} /* check_packet */
static void
arm(
    unsigned int first,
    unsigned int second)
{
    expected_groups[0] = first; expected_groups[1] = second;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
} /* arm */

static void
held_cache_close(
    struct smb2_env  *env,
    struct smb2_conn *c,
    const uint8_t    *fid,
    const uint8_t    *other,
    unsigned int      members)
{
    struct packet packet = { 0 };
    uint32_t      ok[]   = { ST_SUCCESS, ST_SUCCESS, ST_SUCCESS };

    query(&packet, c, fid); close_op(&packet, c, fid); query(&packet, c, other);
    int           replies = c->nreply_app;
    expected_members = members;
    arm(2, 1); atomic_store(&hold_next, 1); send_packet(&packet, c);
    uint64_t      deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
        smb2_pump(env);
    }
    assert(atomic_load(&held) && c->nreply_app == replies);
    assert(atomic_load(&submissions) == 1);
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "accepted specialized cache CLOSE"));
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 2);
    check_packet(&packet, c, ok);
    assert(smb2_close(c, fid) == ST_FILE_CLOSED);
} /* held_cache_close */

static void
directory_cache_close_cases(
    struct smb2_env  *env,
    struct smb2_conn *c,
    struct smb2_conn *peer,
    const uint8_t    *other)
{
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
    struct smb2_create_out first, second, child;

    memset(lease.lease_key, 0x73, sizeof(lease.lease_key));
    assert(c->dialect >= SMB2_DIALECT_0300);
    assert(smb2_create_opts(c, "directory-cache-close", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, &lease, &first) == ST_SUCCESS);
    assert(first.has_lease && first.lease_state == SMB2_LEASE_RH);
    assert(smb2_create_opts(c, "directory-cache-close", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, &lease, &second) == ST_SUCCESS);
    assert(second.has_lease && second.lease_state == SMB2_LEASE_RH);
    held_cache_close(env, c, first.file_id, other, 2);
    /* A surviving directory lease must still be recalled for content changes;
     * CLOSE of the other FileId must not drop this member's cache rights. */
    int      breaks = smb2_conn_nbreaks(c), replies = peer->nreply_app;
    smb2_create_post(peer, "directory-cache-close\\child", MBT_FILE_CREATE,
                     MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (smb2_conn_nbreaks(c) == breaks && smb2c_now_ms() < deadline) {
        smb2_pump(env);
    }
    assert(smb2_conn_nbreaks(c) > breaks);
    held_cache_close(env, c, second.file_id, other, 1);
    assert(smb2c_pump_for_nreply(peer, replies, "directory CLOSE settles content recall"));
    smb2c_parse_create(peer, &child); assert(child.status == ST_SUCCESS);
    assert(smb2_close(peer, child.file_id) == ST_SUCCESS);
    assert(smb2_create(peer, "directory-cache-close\\child", MBT_FILE_OPEN,
                       MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    assert(smb2_close(peer, child.file_id) == ST_SUCCESS);
} /* directory_cache_close_cases */

static void
stream_cache_close_cases(
    struct smb2_env  *env,
    struct smb2_conn *c,
    struct smb2_conn *peer,
    const uint8_t    *other)
{
    for (unsigned int directory = 0; directory < 2; directory++) {
        const char            *base   = directory ? "stream-cache-directory" : "stream-cache-base";
        const char            *stream = directory ? "stream-cache-directory:fork:$DATA" :
            "stream-cache-base:fork:$DATA";
        struct smb2_create_out base_open, first, second, opened;
        struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
        memset(lease.lease_key, 0x74 + directory, sizeof(lease.lease_key));
        assert(smb2_create_opts(c, base, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                directory ? MBT_FILE_DIRECTORY_FILE : MBT_FILE_NON_DIRECTORY_FILE, NULL, &base_open) ==
               ST_SUCCESS);
        assert(smb2_create(c, stream, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           &lease, &first) == ST_SUCCESS);
        assert(first.has_lease && first.lease_state == SMB2_LEASE_RWH);
        assert(smb2_create(c, stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           &lease, &second) == ST_SUCCESS);
        assert(second.has_lease && second.lease_state == SMB2_LEASE_RWH);
        /* The stream identity, not its base, owns this key. Reuse on another
         * stream or the base must fail without creating a fork as a side effect. */
        const char *sibling = directory ? "stream-cache-directory:sibling:$DATA" :
            "stream-cache-base:sibling:$DATA";
        const char *missing = directory ? "stream-cache-directory:missing:$DATA" :
            "stream-cache-base:missing:$DATA";
        assert(smb2_create(c, sibling, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           NULL, &opened) == ST_SUCCESS);
        assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
        assert(smb2_create(c, sibling, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           &lease, &opened) == ST_INVALID_PARAMETER);
        assert(smb2_create_opts(c, base, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                directory ? MBT_FILE_DIRECTORY_FILE : MBT_FILE_NON_DIRECTORY_FILE,
                                &lease, &opened) == ST_INVALID_PARAMETER);
        const uint32_t create_missing[] = { MBT_FILE_CREATE,       MBT_FILE_OPEN_IF,
                                            MBT_FILE_OVERWRITE_IF, MBT_FILE_SUPERSEDE };
        for (unsigned int i = 0; i < sizeof(create_missing) / sizeof(create_missing[0]); i++) {
            assert(smb2_create(c, missing, create_missing[i], MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, &lease, &opened) == ST_INVALID_PARAMETER);
            assert(smb2_create(c, missing, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &opened) == ST_OBJECT_NAME_NOT_FOUND);
        }
        held_cache_close(env, c, first.file_id, other, 2);
        int            breaks = smb2_conn_nbreaks(c), replies = peer->nreply_app;
        smb2_create_post(peer, stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL);
        uint64_t       deadline = smb2c_now_ms() + 10000;
        while (smb2_conn_nbreaks(c) == breaks && smb2c_now_ms() < deadline) {
            smb2_pump(env);
        }
        assert(smb2_conn_nbreaks(c) > breaks);
        /* A peer OPEN can be waiting for this CLOSE to settle its break. No
         * synthetic ACK, grant removal, or early publication is needed. */
        held_cache_close(env, c, second.file_id, other, 1);
        assert(smb2c_pump_for_nreply(peer, replies, "stream CLOSE settles conflicting OPEN"));
        smb2c_parse_create(peer, &opened); assert(opened.status == ST_SUCCESS);
        assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);
        assert(smb2_close(c, base_open.file_id) == ST_SUCCESS);

        /* Legacy II/EX/BATCH stream grants use the same terminal path. */
        struct smb2_oplock_req batch = { .level = SMB2_OPLOCK_LEVEL_BATCH };
        assert(smb2_create(c, stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           &batch, &first) == ST_SUCCESS && first.oplock == SMB2_OPLOCK_LEVEL_BATCH);
        held_cache_close(env, c, first.file_id, other, 1);
        /* A deny-all base open proves both stream ACCESS reservations were
        * retired; a leaked base DELETE reservation would conflict here. */
        assert(smb2_create_opts(peer, base, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0,
                                directory ? MBT_FILE_DIRECTORY_FILE : MBT_FILE_NON_DIRECTORY_FILE, NULL, &opened) ==
               ST_SUCCESS)
        ;
        assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);
    }

    /* Delete intent remains a deliberate boundary: cached stream CLOSE must
    * execute the stream-specific last-close delete, preserving the base. */
    struct smb2_create_out base, stream, check;
    struct smb2_oplock_req batch = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    assert(smb2_create(c, "cache-close-doc-base", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_create_opts(c, "cache-close-doc-base:fork:$DATA", MBT_FILE_CREATE,
                            MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE |
                            MBT_FILE_DELETE_ON_CLOSE,
                            &batch, &stream) == ST_SUCCESS && stream.oplock == SMB2_OPLOCK_LEVEL_BATCH);
    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "cache-close-doc-base:fork:$DATA", MBT_FILE_OPEN,
                       MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &check) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "cache-close-doc-base", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &check) == ST_SUCCESS);
    assert(smb2_close(c, check.file_id) == ST_SUCCESS);
} /* stream_cache_close_cases */

/* Actual durable grants remain legacy CREATEs, but their clean CLOSEs use
 * the native terminal group. CLOSE only retires in-memory state; rejecting its
 * first finish is safe on nontransactional backends. */
static void
durable_close_cases(
    struct smb2_env  *env,
    struct smb2_conn *c,
    const uint8_t    *other)
{
    for (unsigned int mode = 0; mode < 4; mode++) {
        struct smb2_durable_req durable = { .dhnq = mode == 0, .dh2q = mode != 0 };
        memset(durable.create_guid, 0xb0 + mode, sizeof(durable.create_guid));
        struct smb2_oplock_req  req = { .level = SMB2_OPLOCK_LEVEL_BATCH };
        if (mode == 2) {
            req.is_lease = 1; req.lease_state = SMB2_LEASE_RWH;
            memset(req.lease_key, 0xca, sizeof(req.lease_key));
        }
        char                    name[64]; snprintf(name, sizeof(name), "durable-cache-close-%u", mode);
        struct smb2_create_out  file, survivor, reopened;
        assert(smb2_create_dur(c, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, &req, &durable, &file) == ST_SUCCESS);
        assert(mode == 0 ? file.has_dhnq : file.has_dh2q);
        assert(!(file.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT));
        if (mode == 2) {
            assert(smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, &req, &survivor) == ST_SUCCESS);
            assert(survivor.lease_state == SMB2_LEASE_RWH);
        }
        expect_durable   = 1;
        reject_first     = mode == 1;
        park_durable     = mode == 3;
        expected_members = mode == 2 ? 2 : 1;
        atomic_store(&finish_count, 0);
        struct packet packet = { 0 };
        query(&packet, c, file.file_id); close_op(&packet, c, file.file_id);
        if (!park_durable) {
            query(&packet, c, other);
        }
        int           replies = c->nreply_app;
        arm(2, park_durable ? 0 : 1);
        atomic_store(&hold_next, 1); send_packet(&packet, c);
        uint64_t      deadline = smb2c_now_ms() + 10000;
        while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
            smb2_pump(env);
        }
        assert(atomic_load(&held) && c->nreply_app == replies);
        assert(atomic_load(&finish_count) == 1 && atomic_load(&submissions) == 1);
        atomic_store(&release_finish, 1);
        assert(smb2c_pump_for_nreply(c, replies, "accepted durable CLOSE"));
        atomic_store(&armed, 0);
        assert(atomic_load(&submissions) == (park_durable ? 1 : 2));
        assert(atomic_load(&finish_count) == (reject_first ? 2 : 1));
        uint32_t                statuses[] = { ST_SUCCESS, ST_SUCCESS, ST_SUCCESS };
        check_packet(&packet, c, statuses);
        expect_durable = park_durable = reject_first = 0;
        assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
        struct smb2_durable_req reconnect = { .dhnc = mode == 0, .dh2c = mode != 0 };
        memcpy(reconnect.file_id, file.file_id, sizeof(reconnect.file_id));
        memcpy(reconnect.create_guid, durable.create_guid, sizeof(reconnect.create_guid));
        assert(smb2_create_dur(c, "", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                               &req, &reconnect, &reopened) == ST_OBJECT_NAME_NOT_FOUND);
        if (mode == 2) {
            held_cache_close(env, c, survivor.file_id, other, 1);
        }
        /* A leaked parked owner would retain its ACCESS reservation here. */
        assert(smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0, NULL, &reopened) == ST_SUCCESS);
        assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
    }
} /* durable_close_cases */

/* Persistence is a real backend mutation: every held finish here is accepted.
 * The missing-key and backend-error cases exercise the existing best-effort
 * deletion policy. The inspector reads the recovery key from the actual store,
 * and removes the deliberately retained error record only as fixture cleanup. */
static void
persistent_close_cases(
    struct smb2_env  *env,
    struct smb2_conn *c,
    const uint8_t    *other)
{
    for (unsigned int fixture = 0; fixture < 11; fixture++) {
        unsigned int            kind = fixture < 5 ? fixture : fixture == 10 ? 2 : 1;
        persistent_case = fixture < 5 ? PERSIST_NORMAL :
            fixture == 5 ? PERSIST_PARK : fixture == 6 ? PERSIST_CANCEL :
            fixture == 7 ? PERSIST_MISSING : fixture == 8 ? PERSIST_FAILURE :
            fixture == 9 ? PERSIST_CANCEL_DELETE : PERSIST_NORMAL;
        struct smb2_conn       *original_conn = c;
        if (fixture == 10) {
            c = smb2_conn_open(env); smb2_handshake(c);
        }
        char                    base[64], name[80];
        snprintf(base, sizeof(base), "persistent-native-close-%u", fixture);
        snprintf(name, sizeof(name), "%s", base);
        struct smb2_create_out  file, base_open, reopened;
        uint32_t                options = kind == 2 ? MBT_FILE_DIRECTORY_FILE :
            kind == 4 ? 0 : MBT_FILE_NON_DIRECTORY_FILE;
        if (kind == 2) {
            /* Fresh mkdir and stream CREATE currently advertise persistent
             * contexts without creating a backend record. Exercise the actual
             * persisted directory OPEN path, keeping record assertions strict. */
            assert(smb2_create_opts(c, base, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                    MBT_FILE_DIRECTORY_FILE, NULL, &base_open) == ST_SUCCESS);
            assert(smb2_close(c, base_open.file_id) == ST_SUCCESS);
        }
        struct smb2_oplock_req  lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
        if (kind == 3) {
            lease.is_lease = 0; lease.level = SMB2_OPLOCK_LEVEL_BATCH;
        }
        memset(lease.lease_key, 0x90 + fixture, sizeof(lease.lease_key));
        struct smb2_durable_req durable = { .dh2q = 1, .flags = SMB2_DHANDLE_FLAG_PERSISTENT };
        memset(durable.create_guid, 0xa0 + fixture, sizeof(durable.create_guid));
        assert(smb2_create_dur_opts(c, name, kind == 2 ? MBT_FILE_OPEN : MBT_FILE_CREATE,
                                    MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                    options, kind ? &lease : NULL, &durable, &file) == ST_SUCCESS);
        assert(file.has_dh2q && (file.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT));
        if (kind == 3) {
            assert(file.oplock == SMB2_OPLOCK_LEVEL_BATCH);
        } else if (kind) {
            assert(file.has_lease && file.lease_state);
        }
        if (fixture == 10) {
            struct smb2_durable_req reclaim = { .dh2c = 1, .flags = SMB2_DHANDLE_FLAG_PERSISTENT };
            memcpy(reclaim.file_id, file.file_id, 16);
            memcpy(reclaim.create_guid, durable.create_guid, 16);
            smb2_conn_disconnect(c);
            c = smb2_conn_reopen(env, c);
            assert(smb2_create_dur_opts(c, "", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                        options, &lease, &reclaim, &file) == ST_SUCCESS);
            assert(!memcmp(file.file_id, reclaim.file_id, 16));
        }
        expect_durable = 1; park_durable = persistent_case == PERSIST_PARK;
        reject_first   = 0; expected_members = kind ? 1 : 0;
        atomic_store(&delete_next, 1); atomic_store(&delete_seen, 0);
        atomic_store(&finish_count, 0);
        struct packet packet = { 0 };
        query(&packet, c, file.file_id); close_op(&packet, c, file.file_id);
        bool          cancel = persistent_case == PERSIST_CANCEL || persistent_case == PERSIST_CANCEL_DELETE;
        bool          suffix = !park_durable && !cancel && fixture != 10;
        if (suffix) {
            query(&packet, c, other);
        }
        int           replies = c->nreply_app;
        arm(2, suffix ? 1 : 0); atomic_store(&hold_next, 1); send_packet(&packet, c);
        uint64_t      deadline = smb2c_now_ms() + 10000;
        while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
            smb2_pump(env);
        }
        assert(atomic_load(&held) && c->nreply_app == replies);
        assert(atomic_load(&finish_count) == 1 && atomic_load(&submissions) == 1);
        atomic_store(&release_finish, 1);
        assert(smb2c_pump_for_nreply(c, replies, "accepted persistent CLOSE"));
        atomic_store(&armed, 0);
        assert(atomic_load(&submissions) == (suffix ? 2 : 1));
        uint32_t                statuses[] = { ST_SUCCESS,
                                               cancel ? ST_CANCELLED : ST_SUCCESS, ST_SUCCESS };
        check_packet(&packet, c, statuses);
        expect_durable   = park_durable = 0; persistent_case = PERSIST_NONE;
        expected_members = 1;
        assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
        struct smb2_durable_req reconnect = { .dh2c = 1, .flags = SMB2_DHANDLE_FLAG_PERSISTENT };
        memcpy(reconnect.file_id, file.file_id, 16);
        memcpy(reconnect.create_guid, durable.create_guid, 16);
        assert(smb2_create_dur(c, "", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                               kind ? &lease : NULL, &reconnect, &reopened) == ST_OBJECT_NAME_NOT_FOUND);
        assert(smb2_create_opts(c, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0, options,
                                NULL, &reopened) == ST_SUCCESS);
        assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
        c = original_conn;
    }
} /* persistent_close_cases */

/* Cover both sides of a durable stream's identity and a durable directory's
 * DIR_LEASE construct. All mutation finishes before the rejection hook arms. */
static void
durable_specialized_close_cases(
    struct smb2_env  *env,
    struct smb2_conn *c,
    const uint8_t    *other)
{
    for (unsigned int kind = 0; kind < 3; kind++) {
        bool stream = kind != 0, directory_base = kind != 1;
        for (unsigned int mode = 0; mode < 4; mode++) {
            char                   base[80], name[110], child[110];
            snprintf(base, sizeof(base), "durable-specialized-%u-%u", kind, mode);
            snprintf(name, sizeof(name), "%s%s", base, stream ? ":fork:$DATA" : "");
            snprintf(child, sizeof(child), "%s\\child", base);
            uint32_t               options = stream ? MBT_FILE_NON_DIRECTORY_FILE : MBT_FILE_DIRECTORY_FILE;
            uint32_t               access  = stream ? MBT_FILE_READ_ACCESS | MBT_FILE_WRITE_ACCESS : MBT_FILE_ALL_ACCESS
            ;
            uint32_t               share = stream ? MBT_FILE_SHARE_READ | MBT_FILE_SHARE_WRITE : MBT_FILE_SHARE_RWD;
            struct smb2_create_out base_open, file, survivor, reopened;
            assert(smb2_create_opts(c, base, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                                    directory_base ? MBT_FILE_DIRECTORY_FILE : MBT_FILE_NON_DIRECTORY_FILE,
                                    NULL, &base_open) == ST_SUCCESS);
            assert(smb2_close(c, base_open.file_id) == ST_SUCCESS);
            if (directory_base) {
                assert(smb2_create(c, child, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                   MBT_FILE_SHARE_RWD, NULL, &base_open) == ST_SUCCESS);
                assert(smb2_close(c, base_open.file_id) == ST_SUCCESS);
            }
            struct smb2_conn       *holder = c;
            if (mode == 3) {
                holder = smb2_conn_open(env); smb2_handshake(holder);
            }
            struct smb2_oplock_req  req = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
            memset(req.lease_key, 0xd0 + kind * 4 + mode, sizeof(req.lease_key));
            if (stream && mode != 0) {
                req.is_lease = 0; req.level = SMB2_OPLOCK_LEVEL_BATCH;
            }
            bool                    v1      = stream && mode == 1;
            struct smb2_durable_req durable = { .dhnq = v1, .dh2q = !v1 };
            memset(durable.create_guid, 0xe0 + kind * 4 + mode, sizeof(durable.create_guid));
            assert(smb2_create_dur_opts(holder, name, stream ? MBT_FILE_CREATE : MBT_FILE_OPEN,
                                        access, share, options, &req, &durable, &file) == ST_SUCCESS);
            assert(v1 ? file.has_dhnq : file.has_dh2q);
            assert(!(file.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT));
            if (req.is_lease) {
                assert(file.has_lease && file.lease_state ==
                       (stream ? SMB2_LEASE_RWH : SMB2_LEASE_RH));
            } else {
                assert(file.oplock == SMB2_OPLOCK_LEVEL_BATCH);
            }
            if (stream) {
                uint32_t written = 0;
                assert(smb2_write(holder, file.file_id, 0, "durable-fork", 12, &written) == ST_SUCCESS);
                assert(written == 12);
            }
            if (mode == 0) {
                assert(smb2_create_opts(holder, name, MBT_FILE_OPEN, access, share, options,
                                        &req, &survivor) == ST_SUCCESS);
                assert(survivor.lease_state == file.lease_state);
            }
            struct smb2_durable_req reconnect = { .dhnc = v1, .dh2c = !v1 };
            memcpy(reconnect.file_id, file.file_id, sizeof(reconnect.file_id));
            memcpy(reconnect.create_guid, durable.create_guid, sizeof(reconnect.create_guid));
            if (mode == 3) {
                smb2_conn_disconnect(holder);
                holder = smb2_conn_reopen(env, holder);
                assert(smb2_create_dur_opts(holder, "", MBT_FILE_OPEN, access, share, options,
                                            &req, &reconnect, &file) == ST_SUCCESS);
                assert(!memcmp(file.file_id, reconnect.file_id, sizeof(file.file_id)));
                if (stream) {
                    assert(file.end_of_file == 12);
                }
            }
            expect_durable   = 1;
            reject_first     = mode == 1;
            park_durable     = mode == 2;
            expected_members = mode == 0 ? 2 : 1;
            atomic_store(&finish_count, 0);
            struct packet packet = { 0 };
            query(&packet, holder, file.file_id); close_op(&packet, holder, file.file_id);
            int           replies = holder->nreply_app;
            arm(2, 0); atomic_store(&hold_next, 1); send_packet(&packet, holder);
            uint64_t      deadline = smb2c_now_ms() + 10000;
            while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
                smb2_pump(env);
            }
            assert(atomic_load(&held) && holder->nreply_app == replies);
            assert(atomic_load(&finish_count) == 1 && atomic_load(&submissions) == 1);
            atomic_store(&release_finish, 1);
            assert(smb2c_pump_for_nreply(holder, replies, "accepted specialized durable CLOSE"));
            atomic_store(&armed, 0);
            assert(atomic_load(&submissions) == 1);
            assert(atomic_load(&finish_count) == (reject_first ? 2 : 1));
            uint32_t statuses[] = { ST_SUCCESS, ST_SUCCESS };
            check_packet(&packet, holder, statuses);
            expect_durable = park_durable = reject_first = 0;
            assert(smb2_close(holder, file.file_id) == ST_FILE_CLOSED);
            assert(smb2_create_dur_opts(holder, "", MBT_FILE_OPEN, access, share, options,
                                        &req, &reconnect, &reopened) == ST_OBJECT_NAME_NOT_FOUND);
            if (mode == 0) {
                held_cache_close(env, c, survivor.file_id, other, 1);
            }
            /* Deny-all base OPEN requires both the primary stream and base
             * DELETE reservation to have drained, including after reconnect. */
            assert(smb2_create_opts(c, base, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, 0,
                                    directory_base ? MBT_FILE_DIRECTORY_FILE : MBT_FILE_NON_DIRECTORY_FILE,
                                    NULL, &reopened) == ST_SUCCESS);
            assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
            if (stream) {
                uint8_t bytes[32]; uint32_t length = 0;
                assert(smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                                   MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_SUCCESS);
                assert(smb2_read(c, reopened.file_id, 0, sizeof(bytes), bytes, &length) == ST_SUCCESS);
                assert(length == 12 && !memcmp(bytes, "durable-fork", 12));
                assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
            }
            if (directory_base) {
                assert(smb2_create(c, child, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                                   MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_SUCCESS);
                assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
            }
            if (mode == 3) {
                smb2_conn_disconnect(holder);
            }
        }
    }
} /* durable_specialized_close_cases */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_create_out file, other, opened;
    struct smb2_oplock_req oplock = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    struct packet          p      = { 0 };
    uint32_t               ok[]   = { ST_SUCCESS, ST_SUCCESS, ST_SUCCESS, ST_SUCCESS };
    struct smb2_env_opts   opts   = { .oplocks          = 1, .leases        = 1,
                                      .directory_leases = 1, .named_streams = 1, .persistent_handles   = 1 };

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn      *c = smb2_conn_open(&env), *peer = smb2_conn_open(&env);
    smb2_handshake(c); smb2_handshake(peer);
    assert(smb2_create(c, "cache-close-other", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &other) == ST_SUCCESS);

    const uint8_t          levels[] = { SMB2_OPLOCK_LEVEL_II, SMB2_OPLOCK_LEVEL_EXCLUSIVE, SMB2_OPLOCK_LEVEL_BATCH };
    for (unsigned int i = 0; i < sizeof(levels); i++) {
        char name[64]; snprintf(name, sizeof(name), "cache-close-%u", i);
        oplock.level = levels[i];
        assert(smb2_create(c, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, &oplock, &file) ==
               ST_SUCCESS);
        assert(file.oplock == levels[i]);
        memset(&p, 0, sizeof(p));
        query(&p, c, file.file_id); close_op(&p, c, file.file_id); query(&p, c, other.file_id);
        arm(2, 1); send_packet(&p, c); (void) smb2c_wait(c); atomic_store(&armed, 0);
        assert(atomic_load(&submissions) == 2); check_packet(&p, c, ok);
        assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
        assert(smb2_create(peer, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &opened) ==
               ST_SUCCESS);
        assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);
    }

    /* Until acceptance, the FileId, member and cache rights remain public.
     * Accepting CLOSE must satisfy a competing OPEN's break without an ACK. */
    oplock.level = SMB2_OPLOCK_LEVEL_BATCH;
    assert(smb2_create(c, "cache-close-held", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &oplock, &file) == ST_SUCCESS && file.oplock == SMB2_OPLOCK_LEVEL_BATCH);
    memset(&p, 0, sizeof(p)); query(&p, c, file.file_id); close_op(&p, c, file.file_id);
    int      replies = c->nreply_app;
    arm(2, 0); atomic_store(&hold_next, 1); send_packet(&p, c);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
        smb2_pump(&env);
    }
    assert(atomic_load(&held) && c->nreply_app == replies && atomic_load(&submissions) == 1);
    atomic_store(&armed, 0);
    int      peer_replies = peer->nreply_app, breaks = smb2_conn_nbreaks(c);
    smb2_create_post(peer, "cache-close-held", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL);
    deadline = smb2c_now_ms() + 10000;
    while (smb2_conn_nbreaks(c) == breaks && smb2c_now_ms() < deadline) {
        smb2_pump(&env);
    }
    assert(smb2_conn_nbreaks(c) > breaks && peer->nreply_app == peer_replies);
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "accepted cache CLOSE")); check_packet(&p, c, ok);
    assert(smb2c_pump_for_nreply(peer, peer_replies, "cache CLOSE settles conflicting OPEN"));
    smb2c_parse_create(peer, &opened); assert(opened.status == ST_SUCCESS);
    assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);

    /* A failed command before CLOSE does not suppress its independent group. */
    assert(smb2_create(c, "cache-close-error", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &oplock, &file) == ST_SUCCESS);
    uint8_t                invalid[16]; memset(invalid, 0xab, sizeof(invalid));
    memset(&p, 0, sizeof(p)); query(&p, c, invalid); close_op(&p, c, file.file_id);
    arm(2, 0); send_packet(&p, c); (void) smb2c_wait(c); atomic_store(&armed, 0);
    uint32_t               errors[] = { ST_FILE_CLOSED, ST_SUCCESS };
    assert(atomic_load(&submissions) == 1); check_packet(&p, c, errors);
    assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);

    /* Shared-key CLOSE coalesces its metadata prefix and keeps every member
     * and right public until acceptance. Closing one member retains the
     * surviving member's write-cache rights: a
     * share-compatible peer OPEN breaks W, while RH may survive that OPEN. */
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
    memset(lease.lease_key, 0x69, sizeof(lease.lease_key));
    assert(smb2_create(c, "shared-close", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &lease, &file) == ST_SUCCESS && file.has_lease &&
           (file.lease_state & SMB2_LEASE_WRITE));
    assert(smb2_create(c, "shared-close", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &lease, &opened) == ST_SUCCESS && opened.has_lease &&
           (opened.lease_state & SMB2_LEASE_WRITE));
    memset(&p, 0, sizeof(p));
    query(&p, c, file.file_id); close_op(&p, c, file.file_id); query(&p, c, other.file_id);
    replies          = c->nreply_app;
    expected_members = 2;
    arm(2, 1); atomic_store(&hold_next, 1); send_packet(&p, c);
    deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
        smb2_pump(&env);
    }
    assert(atomic_load(&held) && c->nreply_app == replies && atomic_load(&submissions) == 1);
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "accepted shared lease CLOSE"));
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == 2);
    check_packet(&p, c, ok);
    assert(smb2_close(c, file.file_id) == ST_FILE_CLOSED);
    peer_replies = peer->nreply_app; breaks = smb2_conn_nbreaks(c);
    smb2_create_post(peer, "shared-close", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL);
    deadline = smb2c_now_ms() + 10000;
    while (smb2_conn_nbreaks(c) == breaks && smb2c_now_ms() < deadline) {
        smb2_pump(&env);
    }
    assert(smb2_conn_nbreaks(c) > breaks);
    /* Closing the surviving member settles its lease, without granting a
     * test-only ACK or bypassing the normal last-holder teardown. */
    memset(&p, 0, sizeof(p)); query(&p, c, opened.file_id); close_op(&p, c, opened.file_id);
    replies          = c->nreply_app;
    expected_members = 1;
    arm(2, 0); atomic_store(&hold_next, 1); send_packet(&p, c);
    deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) {
        smb2_pump(&env);
    }
    assert(atomic_load(&held) && c->nreply_app == replies);
    atomic_store(&armed, 0);
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "accepted last shared lease CLOSE"));
    check_packet(&p, c, ok);
    assert(smb2c_pump_for_nreply(peer, peer_replies, "shared grant last close"));
    smb2c_parse_create(peer, &opened); assert(opened.status == ST_SUCCESS);
    assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);

    durable_close_cases(&env, c, other.file_id);
    durable_specialized_close_cases(&env, c, other.file_id);
    directory_cache_close_cases(&env, c, peer, other.file_id);
    stream_cache_close_cases(&env, c, peer, other.file_id);
    assert(smb2_close(c, other.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    /* Keep all resource-only finish-rejection fixtures on a non-CA share.
     * Actual persisted-record deletion runs on a distinct server environment
     * and every held finish in that environment must accept. */
    opts.continuous_availability = 1;
    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env); smb2_handshake(c);
    assert(smb2_create(c, "persistent-close-other", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &other) == ST_SUCCESS);
    persistent_close_cases(&env, c, other.file_id);
    assert(smb2_close(c, other.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    return 0;
} /* main */
