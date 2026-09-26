// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Real wire IOCTL and stream groups, with one VFS submission per chain.
 * Mutation tests do not inject rejection after backend effects. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_procs.h"
#include "common/compound_retry.h"

static atomic_int armed, submissions, attempts, expected_groups;
static int        reject_count;
static uint32_t   expected_status = ST_SUCCESS;
static int        inspect_resiliency, resiliency_expected;
static uint64_t   resiliency_timeout;
extern void smb_resiliency_inspect(
    void *,
    int,
    uint64_t);

extern void reparse_test_before(struct chimera_vfs_compound *, bool);
extern void reparse_test_after(bool);
static atomic_int reparse_arm, reparse_seen, reparse_pause, reparse_release, reparse_completed;
static atomic_int reparse_getattr_hold, reparse_create_hold;
static int reparse_retry, reparse_finish_hold, reparse_teardown;
static _Thread_local struct evpl *reparse_evpl;

__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(struct chimera_vfs_thread *thread, const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next); reparse_evpl = thread->evpl;
    return next(thread, cred);
}

struct reparse_hold {
    struct evpl_timer timer;
    struct chimera_vfs_compound *compound;
    chimera_vfs_compound_callback_t callback;
    void *private_data;
    unsigned prepares, finishes;
};

static void reparse_poll_finish(struct evpl *evpl, struct evpl_timer *timer)
{
    struct reparse_hold *ctx = (struct reparse_hold *) ((char *) timer - offsetof(struct reparse_hold, timer));
    if (!atomic_exchange(&reparse_release, 0)) {
        evpl_add_oneshot_timer(evpl, timer, reparse_poll_finish, 1000); return;
    }
    atomic_store(&reparse_pause, 0);
    chimera_vfs_compound_finish_result(ctx->compound, CHIMERA_VFS_OK);
}

static void reparse_readonly_stop(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    struct reparse_hold *ctx = private_data;
    (void) compound; (void) index;
    if (!ctx->prepares++) *status = CHIMERA_VFS_EIO;
}

static void reparse_finish(struct chimera_vfs_compound *compound, void *private_data)
{
    struct reparse_hold *ctx = private_data;
    bool retry = reparse_retry && !ctx->finishes++;
    bool ready = chimera_vfs_compound_op(compound, 9)->completed;
    if (!reparse_teardown) reparse_test_before(compound, ready);
    if (retry) {
        assert(!chimera_vfs_compound_op(compound, 2)->completed);
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_EAGAIN);
    } else if (reparse_finish_hold) {
        ctx->compound = compound;
        evpl_add_oneshot_timer(reparse_evpl, &ctx->timer, reparse_poll_finish, 1000);
        atomic_store(&reparse_pause, 1);
    } else {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
    }
}

static void reparse_complete(struct chimera_vfs_compound *compound, void *private_data)
{
    struct reparse_hold *ctx = private_data;
    bool accepted = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK;
    bool migrated = accepted && chimera_vfs_compound_op(compound, 9)->completed;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void *arg = ctx->private_data;
    if (accepted) free(ctx);
    callback(compound, arg);
    if (accepted) {
        if (!reparse_teardown) reparse_test_after(migrated);
        atomic_store(&reparse_completed, 1);
    }
}

struct reparse_io_hold {
    struct evpl_timer timer;
    chimera_vfs_getattr_callback_t getattr;
    chimera_vfs_symlink_at_callback_t symlink;
    enum chimera_vfs_error error;
    struct chimera_vfs_attrs attr, pre, post;
    void *private_data;
};
static void reparse_poll_io(struct evpl *evpl, struct evpl_timer *timer)
{
    struct reparse_io_hold *ctx = (struct reparse_io_hold *) ((char *) timer - offsetof(struct reparse_io_hold, timer));
    if (!atomic_exchange(&reparse_release, 0)) {
        evpl_add_oneshot_timer(evpl, timer, reparse_poll_io, 1000); return;
    }
    atomic_store(&reparse_pause, 0);
    if (ctx->getattr) ctx->getattr(ctx->error, &ctx->attr, ctx->private_data);
    else ctx->symlink(ctx->error, &ctx->attr, &ctx->pre, &ctx->post, ctx->private_data);
    free(ctx);
}
static void reparse_getattr_done(enum chimera_vfs_error error, struct chimera_vfs_attrs *attr, void *private_data)
{
    struct reparse_io_hold *ctx = private_data;
    ctx->error = error; if (attr) ctx->attr = *attr;
    evpl_add_oneshot_timer(reparse_evpl, &ctx->timer, reparse_poll_io, 1000);
    atomic_store(&reparse_pause, 1);
}
static void reparse_create_done(enum chimera_vfs_error error, struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *pre, struct chimera_vfs_attrs *post, void *private_data)
{
    struct reparse_io_hold *ctx = private_data;
    ctx->error = error;
    if (attr) ctx->attr = *attr;
    if (pre) ctx->pre = *pre;
    if (post) ctx->post = *post;
    assert(error == CHIMERA_VFS_OK);
    evpl_add_oneshot_timer(reparse_evpl, &ctx->timer, reparse_poll_io, 1000);
    atomic_store(&reparse_pause, 1);
}
__attribute__((visibility("default"))) void
chimera_vfs_getattr(struct chimera_vfs_thread *thread, const struct chimera_vfs_cred *cred,
    struct chimera_vfs_open_handle *handle, uint64_t mask,
    chimera_vfs_getattr_callback_t callback, void *private_data)
{
    typedef void (*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *,
        struct chimera_vfs_open_handle *, uint64_t, chimera_vfs_getattr_callback_t, void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_getattr");
    assert(next);
    if (atomic_exchange(&reparse_getattr_hold, 0)) {
        struct reparse_io_hold *ctx = calloc(1, sizeof(*ctx)); assert(ctx);
        ctx->getattr = callback; ctx->private_data = private_data;
        next(thread, cred, handle, mask, reparse_getattr_done, ctx);
    } else next(thread, cred, handle, mask, callback, private_data);
}

/* SET_REPARSE has a restricted typed identity-replacement path. Inject only
 * result failures, never reject/rewind completed filesystem mutations. */
static atomic_int reparse_fault, fail_reparse_open, reparse_fault_seen;
struct reparse_symlink_fault {
    chimera_vfs_symlink_at_callback_t callback;
    void *private_data;
    int fault;
};

static void
reparse_symlink_complete(enum chimera_vfs_error error,
    struct chimera_vfs_attrs *attr, struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post, void *private_data)
{
    struct reparse_symlink_fault *ctx = private_data;
    chimera_vfs_symlink_at_callback_t callback = ctx->callback;
    void *arg = ctx->private_data;
    int fault = ctx->fault;
    free(ctx);
    assert(error == CHIMERA_VFS_OK);
    atomic_fetch_add(&reparse_fault_seen, 1);
    if (fault == 2) { atomic_store(&fail_reparse_open, 1); }
    callback(error, fault == 1 ? NULL : attr, pre, post, arg);
}

__attribute__((visibility("default"))) void
chimera_vfs_symlink_at_flags(struct chimera_vfs_thread *thread,
    const struct chimera_vfs_cred *cred, struct chimera_vfs_open_handle *handle,
    const char *name, int namelen, const char *target, int targetlen,
    struct chimera_vfs_attrs *attrs, unsigned int flags, uint64_t mask, uint64_t pre, uint64_t post,
    chimera_vfs_symlink_at_callback_t callback, void *private_data)
{
    typedef void (*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *,
        struct chimera_vfs_open_handle *, const char *, int, const char *, int,
        struct chimera_vfs_attrs *, unsigned int, uint64_t, uint64_t, uint64_t,
        chimera_vfs_symlink_at_callback_t, void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_symlink_at_flags");
    assert(next);
    int fault = atomic_exchange(&reparse_fault, 0);
    if (atomic_exchange(&reparse_create_hold, 0)) {
        struct reparse_io_hold *ctx = calloc(1, sizeof(*ctx)); assert(ctx);
        ctx->symlink = callback; ctx->private_data = private_data;
        next(thread, cred, handle, name, namelen, target, targetlen, attrs,
             flags, mask, pre, post, reparse_create_done, ctx);
        return;
    }
    if (fault) {
        struct reparse_symlink_fault *ctx = calloc(1, sizeof(*ctx));
        assert(ctx);
        ctx->callback = callback; ctx->private_data = private_data; ctx->fault = fault;
        next(thread, cred, handle, name, namelen, target, targetlen, attrs,
             flags, mask, pre, post, reparse_symlink_complete, ctx);
    } else {
        next(thread, cred, handle, name, namelen, target, targetlen, attrs,
             flags, mask, pre, post, callback, private_data);
    }
}

__attribute__((visibility("default"))) void
chimera_vfs_open_fh(struct chimera_vfs_thread *thread,
    const struct chimera_vfs_cred *cred, const void *fh, int fhlen,
    unsigned int flags, chimera_vfs_open_fh_callback_t callback, void *private_data)
{
    typedef void (*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *,
        const void *, int, unsigned int, chimera_vfs_open_fh_callback_t, void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_open_fh");
    assert(next);
    if (atomic_exchange(&fail_reparse_open, 0)) {
        callback(CHIMERA_VFS_EIO, NULL, private_data);
        return;
    }
    next(thread, cred, fh, fhlen, flags, callback, private_data);
}

struct finish_injection {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    seen;
};

static void
readonly_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_injection *ctx    = private_data;
    enum chimera_vfs_error   status = ctx->seen++ < (unsigned int) reject_count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK;

    atomic_store(&attempts, ctx->seen);
    if (inspect_resiliency) {
        smb_resiliency_inspect(chimera_vfs_compound_op(compound, 0)->prepare_private,
                               resiliency_expected, resiliency_timeout);
    }
    chimera_vfs_compound_finish_result(compound, status);
} /* readonly_finish */

static void
readonly_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_injection        *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN ||
        ctx->seen == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1) {
        free(ctx);
    }
    callback(compound, arg);
} /* readonly_complete */

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
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (chimera_vfs_compound_num_ops(cp) == 10 &&
        chimera_vfs_compound_op(cp, 2)->type == CHIMERA_VFS_COMPOUND_OP_REMOVE &&
        chimera_vfs_compound_op(cp, 8)->type == CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS &&
        atomic_exchange(&reparse_arm, 0)) {
        struct reparse_hold *ctx = calloc(1, sizeof(*ctx)); assert(ctx);
        ctx->callback = callback; ctx->private_data = private_data;
        assert(chimera_vfs_compound_op(cp, 2)->remove_match_child_fh);
        assert(chimera_vfs_compound_op(cp, 2)->remove_flags & CHIMERA_VFS_REMOVE_NO_NOTIFY);
        assert(chimera_vfs_compound_op(cp, 3)->namespace_flags);
        assert(chimera_vfs_compound_num_groups(cp) == 1);
        if (reparse_retry) chimera_vfs_compound_set_op_prepare(cp, 0, reparse_readonly_stop, ctx);
        chimera_vfs_compound_set_finish_handler(cp, reparse_finish, ctx);
        atomic_fetch_add(&reparse_seen, 1);
        next(cp, reparse_complete, ctx);
        return;
    }
    if (atomic_load(&armed)) {
        atomic_fetch_add(&submissions, 1);
        assert(chimera_vfs_compound_num_groups(cp) == (uint32_t) atomic_load(&expected_groups));
        if (inspect_resiliency) {
            smb_resiliency_inspect(chimera_vfs_compound_op(cp, 0)->prepare_private,
                                   resiliency_expected, resiliency_timeout);
        }
        if (reject_count) {
            struct finish_injection *ctx = calloc(1, sizeof(*ctx));
            assert(ctx);
            ctx->callback     = callback;
            ctx->private_data = private_data;
            chimera_vfs_compound_set_finish_handler(cp, readonly_finish, ctx);
            next(cp, readonly_complete, ctx);
            return;
        }
    }
    next(cp, callback, private_data);
} /* chimera_vfs_compound_submit */

struct packet {
    uint8_t      data[4096];
    unsigned int length, previous, count;
    uint64_t     first_mid;
};

static uint8_t *
append(
    struct packet    *p,
    struct smb2_conn *c,
    uint16_t          command,
    unsigned int      body_size)
{
    unsigned int start = (p->length + 7) & ~7u;

    if (p->count) {
        p32(p->data + p->previous, 20, start - p->previous);
    } else {
        p->first_mid = c->msg_id;
    }
    assert(start + SMB2_HDR_SIZE + body_size <= sizeof(p->data));
    uint8_t     *h = p->data + start;
    memcpy(h, "\xfeSMB", 4);
    p16(h, 4, SMB2_HDR_SIZE);
    p16(h, 6, 1);
    p16(h, 12, command);
    p16(h, 14, 32);
    p64(h, 24, p->first_mid + p->count);
    p32(h, 36, c->tree_id);
    p64(h, 40, c->session_id);
    p->count++;
    p->previous = start;
    p->length   = start + SMB2_HDR_SIZE + body_size;
    return h + SMB2_HDR_SIZE;
} /* append */

static void
query(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint8_t           type,
    uint8_t           level)
{
    uint8_t *b = append(p, c, SMB2_QUERY_INFO, 40);

    p16(b, 0, 41);
    b[2] = type;
    b[3] = level;
    p32(b, 4, 2048);
    if (type == 3) {
        p32(b, 16, 7);
    }                                 /* owner/group/DACL */
    memcpy(b + 24, fid, 16);
} /* query */

static void
read_four(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16])
{
    uint8_t *b = append(p, c, SMB2_READ, 49);

    p16(b, 0, 49);
    b[2] = SMB2_HDR_SIZE + 16;
    p32(b, 4, 4);
    memcpy(b + 16, fid, 16);
} /* read_four */

static void
ioctl_send(
    struct packet    *p,
    struct smb2_conn *c)
{
    int replies = c->nreply_app;

    atomic_store(&submissions, 0);
    atomic_store(&expected_groups, p->count);
    atomic_store(&attempts, 0);
    atomic_store(&armed, 1);
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE);
    c->msg_id = p->first_mid + p->count;
    (void) smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    assert(c->nreply_app == replies + 1);
    unsigned int off = 4;
    for (unsigned int i = 0; i < p->count; i++) {
        assert(off + SMB2_HDR_SIZE <= (unsigned int) c->rlen);
        const uint8_t *h = c->rbuf + off;
        fprintf(stderr, "ioctl response %u command %u status %08x\n", i, g16(h, 12), g32(h, 8));
        assert(g64(h, 24) == p->first_mid + i);
        assert(g32(h, 8) == expected_status);
        if (expected_status == ST_INTERNAL_ERROR) {
            assert(g16(h + SMB2_HDR_SIZE, 0) == 9);
            assert(g32(h + SMB2_HDR_SIZE, 4) == 0);
        }
        uint32_t       next = g32(h, 20);
        assert((i + 1 < p->count) == (next != 0));
        if (next) {
            assert(next >= SMB2_HDR_SIZE && !(next & 7));
        }
        off += next;
    }
} /* ioctl_send */

static const uint8_t *
result(
    struct smb2_conn *c,
    unsigned int      index,
    uint32_t         *length)
{
    const uint8_t *h = c->rbuf + 4;

    for (unsigned int i = 0; i < index; i++) {
        h += g32(h, 20);
    }
    const uint8_t *b = h + SMB2_HDR_SIZE;
    assert(g16(h, 12) == SMB2_QUERY_INFO);
    assert(g16(b, 0) == 9);
    *length = g32(b, 4);
    const uint8_t *out = h + g16(b, 2);
    assert(out + *length <= c->rbuf + c->rlen);
    return out;
} /* result */

static void
ioctl_append(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint32_t          code,
    const void       *input,
    uint32_t          length)
{
    uint8_t *b = append(p, c, SMB2_IOCTL, 56 + length);

    p16(b, 0, 57);
    p32(b, 4, code);
    memcpy(b + 8, fid, 16);
    p32(b, 24, SMB2_HDR_SIZE + 56);
    p32(b, 28, length);
    p32(b, 36, SMB2_HDR_SIZE + 56);
    p32(b, 44, 2048);
    p32(b, 48, SMB2C_IOCTL_IS_FSCTL);
    if (length) {
        memcpy(b + 56, input, length);
    }
} /* ioctl_append */

static const uint8_t *
wire_body(
    struct smb2_conn *c,
    unsigned int      index,
    uint16_t          command)
{
    const uint8_t *h = c->rbuf + 4;

    for (unsigned int i = 0; i < index; i++) {
        h += g32(h, 20);
    }
    assert(g16(h, 12) == command);
    return h + SMB2_HDR_SIZE;
} /* wire_body */

static const uint8_t *
ioctl_result(
    struct smb2_conn *c,
    unsigned int      index,
    uint32_t         *length)
{
    const uint8_t *b = wire_body(c, index, SMB2_IOCTL);

    *length = g32(b, 36);
    const uint8_t *out = b - SMB2_HDR_SIZE + g32(b, 32);
    assert(out + *length <= c->rbuf + c->rlen);
    return out;
} /* ioctl_result */

static void
expect_read_four(
    struct smb2_conn *c,
    unsigned int      index,
    const char       *expected)
{
    const uint8_t *b = wire_body(c, index, SMB2_READ);

    assert(g32(b, 4) == 4);
    assert(!memcmp(b - SMB2_HDR_SIZE + b[2], expected, 4));
} /* expect_read_four */

static unsigned int
reparse_symlink_input(uint8_t *input, const char *target)
{
    int length = utf16le(target, input + 20);
    p32(input, 0, SMB2_IO_REPARSE_TAG_SYMLINK);
    p16(input, 4, 12 + length * 2);
    p16(input, 6, 0);
    p16(input, 8, 0);
    p16(input, 10, length);
    p16(input, 12, length);
    p16(input, 14, length);
    p32(input, 16, 1);
    memcpy(input + 20 + length, input + 20, length);
    return 20 + length * 2;
}

static void reparse_wait_flag(struct smb2_env *env, atomic_int *flag)
{
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(flag) && smb2c_now_ms() < deadline) smb2_pump(env);
    assert(atomic_load(flag));
}
static void reparse_arm_case(void)
{
    atomic_store(&reparse_completed, 0);
    atomic_store(&reparse_pause, 0); atomic_store(&reparse_release, 0);
    atomic_store(&reparse_arm, 1);
}
static uint64_t reparse_post(struct smb2_conn *c, const uint8_t *fid, const uint8_t *input, uint32_t length)
{
    struct packet packet = { 0 };
    ioctl_append(&packet, c, fid, SMB2_FSCTL_SET_REPARSE_POINT, input, length);
    memcpy(c->sbuf + 4, packet.data, packet.length);
    smb2c_send(c, packet.length - SMB2_HDR_SIZE);
    c->msg_id = packet.first_mid + packet.count;
    return packet.first_mid;
}
static void reparse_wait_reply(struct smb2_conn *c, uint64_t id, uint32_t status)
{
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!(c->reply_ready && c->reply_mid == id) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(c->reply_ready && c->reply_mid == id);
    assert(g32(c->rbuf + 4, 8) == status);
}
static void reparse_cancel(struct smb2_conn *c, uint64_t id)
{
    int body = smb2c_begin(c, SMB2_CANCEL, 0);
    p64(c->sbuf + 4, 24, id); p16(c->sbuf + body, 0, 4);
    smb2c_send(c, 4);
    (void) smb2_echo_barrier(c);
}
static void reparse_native_checks(struct smb2_env *env, struct smb2_conn *c)
{
    uint8_t input[256] = { 0 }, bytes[16];
    uint32_t length, status, count;
    struct smb2_create_out open;
    unsigned input_len = reparse_symlink_input(input, "target");
    int before = atomic_load(&reparse_seen);
    /* A rejected read-only attempt precedes a real accepted replacement. */
    assert(smb2_create(c, "reparse-retry", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    reparse_retry = 1; reparse_arm_case();
    assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, open.file_id, input, input_len) == ST_SUCCESS);
    reparse_wait_flag(env, &reparse_completed); reparse_retry = 0;
    assert(atomic_load(&reparse_seen) == before + 1 && !atomic_load(&reparse_arm));
    (void) smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, open.file_id, NULL, 0, 4096, &status, &length);
    assert(status == ST_SUCCESS && length >= 20);
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* Held accepted finish keeps all public identity/claim/namespace fields old.
     * Both a native GET and legacy CLOSE must reject before pinning that FileId. */
    assert(smb2_create(c, "reparse-held", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    reparse_finish_hold = 1; reparse_arm_case();
    uint64_t id = reparse_post(c, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause);
    (void) smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, open.file_id, NULL, 0, 4096, &status, &length);
    assert(status == 0xC0000467u); /* FILE_NOT_AVAILABLE */
    assert(smb2_close(c, open.file_id) == 0xC0000467u);
    atomic_store(&reparse_release, 1); reparse_wait_reply(c, id, ST_SUCCESS);
    reparse_wait_flag(env, &reparse_completed); reparse_finish_hold = 0;
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* An already running native consumer wins admission first: SET must
     * leave its handle and namespace untouched instead of waiting on it. */
    assert(smb2_create(c, "reparse-reader-first", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    struct packet reader = { 0 };
    ioctl_append(&reader, c, open.file_id, SMB2_FSCTL_GET_REPARSE_POINT, NULL, 0);
    atomic_store(&reparse_getattr_hold, 1); atomic_store(&reparse_pause, 0);
    memcpy(c->sbuf + 4, reader.data, reader.length);
    smb2c_send(c, reader.length - SMB2_HDR_SIZE); c->msg_id = reader.first_mid + reader.count;
    reparse_wait_flag(env, &reparse_pause);
    assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, open.file_id, input, input_len) == 0xC0000467u);
    atomic_store(&reparse_release, 1);
    reparse_wait_reply(c, reader.first_mid, 0xC0000275u); /* NOT_A_REPARSE_POINT */
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* Wire CANCEL before the first mutation leaves a normal writable file. */
    assert(smb2_create(c, "reparse-cancel-before", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    assert(smb2_write(c, open.file_id, 0, "keep", 4, &count) == ST_SUCCESS);
    reparse_arm_case(); atomic_store(&reparse_getattr_hold, 1);
    id = reparse_post(c, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause); reparse_cancel(c, id);
    atomic_store(&reparse_release, 1); reparse_wait_reply(c, id, ST_CANCELLED);
    reparse_wait_flag(env, &reparse_completed);
    assert(smb2_read(c, open.file_id, 0, 4, bytes, &length) == ST_SUCCESS && length == 4 && !memcmp(bytes, "keep", 4));
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* A late CANCEL after REMOVE/CREATE must drain new OPEN + ACCESS migration. */
    assert(smb2_create(c, "reparse-cancel-after", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    reparse_arm_case(); atomic_store(&reparse_create_hold, 1);
    id = reparse_post(c, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause); reparse_cancel(c, id);
    atomic_store(&reparse_release, 1); reparse_wait_reply(c, id, ST_SUCCESS);
    reparse_wait_flag(env, &reparse_completed);
    (void) smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, open.file_id, NULL, 0, 4096, &status, &length);
    assert(status == ST_SUCCESS);
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* FIFO/socket migrate; forbidden device creation must preserve placeholder. */
    const uint64_t types[] = { UINT64_C(0x524843), UINT64_C(0x4b4c42),
        UINT64_C(0x4f464946), UINT64_C(0x4b434f53) }; /* CHR, BLK, FIFO, SOCK */
    for (unsigned i = 0; i < 4; ++i) {
        char name[64]; snprintf(name, sizeof(name), "reparse-node-%u", i);
        assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
        memset(input, 0, sizeof(input)); p32(input, 0, 0x80000014u); /* IO_REPARSE_TAG_NFS */
        p16(input, 4, i < 2 ? 16 : 8); p64(input, 8, types[i]);
        if (i < 2) { p32(input, 16, 1); p32(input, 20, 7); }
        if (i < 2) {
            /* Anonymous callers cannot mknod devices. Reject before REMOVE,
             * preserving both the old FileId's bytes and its pathname. */
            assert(smb2_write(c, open.file_id, 0, "keep", 4, &count) == ST_SUCCESS);
            int seen = atomic_load(&reparse_seen);
            assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, open.file_id, input, 24) == ST_ACCESS_DENIED);
            assert(atomic_load(&reparse_seen) == seen);
            assert(smb2_read(c, open.file_id, 0, 4, bytes, &length) == ST_SUCCESS && length == 4 && !memcmp(bytes, "keep", 4));
            struct smb2_create_out peer;
            assert(smb2_create(c, name, FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD, NULL, &peer) == ST_SUCCESS);
            assert(smb2_read(c, peer.file_id, 0, 4, bytes, &length) == ST_SUCCESS && length == 4 && !memcmp(bytes, "keep", 4));
            assert(smb2_close(c, peer.file_id) == ST_SUCCESS);
        } else {
            reparse_arm_case();
            assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, open.file_id, input, 16) == ST_SUCCESS);
            reparse_wait_flag(env, &reparse_completed);
            (void) smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, open.file_id, NULL, 0, 4096, &status, &length);
            assert(status == ST_SUCCESS && length >= 16);
        }
        assert(smb2_close(c, open.file_id) == ST_SUCCESS);
    }

    /* A read-only nonregular classification cannot launch the legacy
     * destructive fallback after a CANCEL received during held finish. */
    assert(smb2_create(c, "reparse-fallback-cancel", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    memset(input, 0, sizeof(input)); p32(input, 0, 0x80000014u);
    p16(input, 4, 8); p64(input, 8, UINT64_C(0x4f464946)); /* FIFO */
    reparse_arm_case();
    assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, open.file_id, input, 16) == ST_SUCCESS);
    reparse_wait_flag(env, &reparse_completed);
    input_len = reparse_symlink_input(input, "target");
    reparse_finish_hold = 1; reparse_arm_case();
    id = reparse_post(c, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause); reparse_cancel(c, id);
    atomic_store(&reparse_release, 1); reparse_wait_reply(c, id, ST_CANCELLED);
    reparse_wait_flag(env, &reparse_completed); reparse_finish_hold = 0;
    const uint8_t *output = smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT,
        open.file_id, NULL, 0, 4096, &status, &length);
    assert(status == ST_SUCCESS && length == 16 && g64(output, 8) == UINT64_C(0x4f464946));
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* Same wire CREATE -> SET -> GET retains the accepted producer FileId. */
    input_len = reparse_symlink_input(input, "target");
    int create_len = smb2c_build_create(c, "reparse-related", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE, NULL);
    struct packet packet = { 0 };
    uint8_t *body = append(&packet, c, SMB2_CREATE, create_len);
    memcpy(body, c->sbuf + 4 + SMB2_HDR_SIZE, create_len);
    uint8_t related[16]; memset(related, 0xff, sizeof(related));
    ioctl_append(&packet, c, related, SMB2_FSCTL_SET_REPARSE_POINT, input, input_len);
    p32(packet.data + packet.previous, 16, SMB2_FLAGS_RELATED_OPERATIONS);
    ioctl_append(&packet, c, related, SMB2_FSCTL_GET_REPARSE_POINT, NULL, 0);
    p32(packet.data + packet.previous, 16, SMB2_FLAGS_RELATED_OPERATIONS);
    reparse_arm_case();
    memcpy(c->sbuf + 4, packet.data, packet.length);
    smb2c_send(c, packet.length - SMB2_HDR_SIZE); c->msg_id = packet.first_mid + packet.count;
    assert(smb2c_wait(c) == ST_SUCCESS);
    reparse_wait_flag(env, &reparse_completed);
    const uint8_t *header = c->rbuf + 4;
    memcpy(open.file_id, header + SMB2_HDR_SIZE + 64, 16);
    for (unsigned i = 0; i < 3; ++i) {
        assert(g32(header, 8) == ST_SUCCESS);
        if (i < 2) { assert(g32(header, 20)); header += g32(header, 20); }
    }
    assert(smb2_close(c, open.file_id) == ST_SUCCESS);

    /* TDIS is a logical cutoff even while the migration owns its gate. */
    struct smb2_conn *tdis = smb2_conn_open(env); smb2_handshake(tdis);
    assert(smb2_create(tdis, "reparse-tdis", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    reparse_teardown = 1; reparse_arm_case(); atomic_store(&reparse_create_hold, 1);
    id = reparse_post(tdis, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause);
    assert(smb2_tree_disconnect(tdis) == ST_SUCCESS);
    atomic_store(&reparse_release, 1); reparse_wait_reply(tdis, id, ST_FILE_CLOSED);
    reparse_wait_flag(env, &reparse_completed); reparse_teardown = 0;

    /* Disconnect while actual CREATE completion is held never resurrects an
     * unhashed FileId. Owner-loop cancellation drains safely without a reply. */
    struct smb2_conn *drop = smb2_conn_open(env); smb2_handshake(drop);
    assert(smb2_create(drop, "reparse-disconnect", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &open) == ST_SUCCESS);
    reparse_teardown = 1; reparse_arm_case(); atomic_store(&reparse_create_hold, 1);
    (void) reparse_post(drop, open.file_id, input, input_len);
    reparse_wait_flag(env, &reparse_pause);
    smb2_conn_disconnect(drop);
    atomic_store(&reparse_release, 1); reparse_wait_flag(env, &reparse_completed);
    reparse_teardown = 0;
    assert(!atomic_load(&reparse_arm));
}

static void
reparse_identity_checks(struct smb2_conn *c)
{
    struct smb2_create_out old, replacement, verify;
    uint8_t input[256] = { 0 }, bytes[16];
    uint32_t length, written;
    unsigned int input_len = reparse_symlink_input(input, "target");

    assert(smb2_create(c, "reparse-stale", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &old) == ST_SUCCESS);
    assert(smb2_write(c, old.file_id, 0, "old", 3, &written) == ST_SUCCESS);
    assert(smb2_create(c, "reparse-replacement", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &replacement) == ST_SUCCESS);
    assert(smb2_write(c, replacement.file_id, 0, "keep", 4, &written) == ST_SUCCESS);
    assert(smb2_rename(c, replacement.file_id, "reparse-stale", 1) == ST_SUCCESS);
    uint32_t status = smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, old.file_id,
                                 input, input_len);
    fprintf(stderr, "stale SET_REPARSE status %08x\n", status);
    assert(status == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_read(c, old.file_id, 0, sizeof(bytes), bytes, &length) == ST_SUCCESS &&
           length == 3 && !memcmp(bytes, "old", 3));
    assert(smb2_create(c, "reparse-stale", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, NULL, &verify) == ST_SUCCESS);
    assert(smb2_read(c, verify.file_id, 0, sizeof(bytes), bytes, &length) == ST_SUCCESS &&
           length == 4 && !memcmp(bytes, "keep", 4));
    assert(smb2_close(c, verify.file_id) == ST_SUCCESS);
    assert(smb2_close(c, replacement.file_id) == ST_SUCCESS);
    assert(smb2_close(c, old.file_id) == ST_SUCCESS);

    /* Unknown NFS types used to unlink first, then return NOT_IMPLEMENTED. */
    assert(smb2_create(c, "reparse-unsupported", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &old) == ST_SUCCESS);
    assert(smb2_write(c, old.file_id, 0, "keep", 4, &written) == ST_SUCCESS);
    memset(input, 0, sizeof(input));
    p32(input, 0, 0x80000014u); /* IO_REPARSE_TAG_NFS */
    p16(input, 4, 8);
    p64(input, 8, UINT64_MAX);
    struct packet boundary = { 0 };
    ioctl_append(&boundary, c, old.file_id, SMB2_FSCTL_SET_REPARSE_POINT, input, 16);
    read_four(&boundary, c, old.file_id);
    /* The failed SET remains an explicit legacy boundary. Its independent
     * READ suffix is exactly one native group and still completes normally. */
    atomic_store(&submissions, 0);
    atomic_store(&expected_groups, 1);
    atomic_store(&armed, 1);
    memcpy(c->sbuf + 4, boundary.data, boundary.length);
    smb2c_send(c, boundary.length - SMB2_HDR_SIZE);
    c->msg_id = boundary.first_mid + boundary.count;
    (void) smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    const uint8_t *first = c->rbuf + 4;
    assert(g32(first, 8) == 0xC0000002u); /* NOT_IMPLEMENTED */
    assert(g32(first, 20) != 0);
    const uint8_t *second = first + g32(first, 20);
    assert(g32(second, 8) == ST_SUCCESS && g32(second, 20) == 0);
    expect_read_four(c, 1, "keep");
    assert(smb2_create(c, "reparse-unsupported", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, NULL, &verify) == ST_SUCCESS);
    assert(smb2_read(c, verify.file_id, 0, sizeof(bytes), bytes, &length) == ST_SUCCESS &&
           length == 4 && !memcmp(bytes, "keep", 4));
    assert(smb2_close(c, verify.file_id) == ST_SUCCESS);
    assert(smb2_close(c, old.file_id) == ST_SUCCESS);

    const char *names[] = { "reparse-no-identity", "reparse-reopen-error" };
    input_len = reparse_symlink_input(input, "target");
    for (int i = 0; i < 2; i++) {
        assert(smb2_create(c, names[i], FILE_CREATE, FILE_ALL_ACCESS,
            FILE_SHARE_RWD, NULL, &old) == ST_SUCCESS);
        atomic_store(&reparse_fault, i + 1);
        assert(smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, old.file_id,
            input, input_len) == ST_INTERNAL_ERROR);
        assert(atomic_load(&reparse_fault) == 0 && atomic_load(&fail_reparse_open) == 0);
        /* No fake rollback: the replacement symlink exists even though SET
         * cannot report successful handle rebinding. */
        assert(smb2_create_opts(c, names[i], FILE_OPEN, FILE_READ_ATTRIBUTES,
            FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE | 0x00200000u, NULL, &verify) == ST_SUCCESS);
        uint32_t output_length, output_status;
        (void) smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, verify.file_id,
            NULL, 0, 4096, &output_status, &output_length);
        assert(output_status == ST_SUCCESS && output_length >= 20);
        assert(smb2_close(c, verify.file_id) == ST_SUCCESS);
        assert(smb2_close(c, old.file_id) == ST_SUCCESS);
    }
    assert(atomic_load(&reparse_fault_seen) == 2);
}

int
main(void)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .named_streams = 1 };
    struct smb2_create_out src, dst, stream;
    struct packet          p = { 0 };
    uint8_t                input[544] = { 0 }, token[512];
    uint32_t               written, length;

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn      *c = smb2_conn_open(&env);
    smb2_handshake(c);
    assert(smb2_create(c, "copy-src", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &src) == ST_SUCCESS);
    assert(smb2_create(c, "copy-dst", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &dst) == ST_SUCCESS);
    assert(smb2_write(c, src.file_id, 0, "abcdefgh", 8, &written) == ST_SUCCESS && written == 8);
    assert(smb2_write(c, dst.file_id, 0, "XXXXXXXX", 8, &written) == ST_SUCCESS && written == 8);

    memcpy(input, src.file_id, 16);
    p32(input, 24, 2);
    p32(input, 48, 4);
    p64(input, 56, 4);
    p64(input, 64, 4);
    p32(input, 72, 4);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_SRV_COPYCHUNK, input, 80);
    read_four(&p, c, dst.file_id);
    query(&p, c, dst.file_id, 1, 5);
    memset(input, 0, sizeof(input));
    p32(input, 0, 32);
    p64(input, 24, 8);
    ioctl_append(&p, c, src.file_id, SMB2_FSCTL_OFFLOAD_READ, input, 32);
    ioctl_send(&p, c);
    const uint8_t *out = ioctl_result(c, 0, &length);
    assert(length == 12 && g32(out, 0) == 2 && g32(out, 8) == 8);
    expect_read_four(c, 1, "abcd");
    out = result(c, 2, &length);
    assert(length == 24 && g64(out, 8) == 8);
    out = ioctl_result(c, 3, &length);
    assert(length == 528 && g64(out, 8) == 8);
    memcpy(token, out + 16, sizeof(token));

    memset(&p, 0, sizeof(p));
    memset(input, 0, sizeof(input));
    p32(input, 0, 544);
    p64(input, 16, 4);
    p64(input, 24, 4);
    memcpy(input + 32, token, sizeof(token));
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_OFFLOAD_WRITE, input, 544);
    read_four(&p, c, dst.file_id);
    ioctl_send(&p, c);
    out = ioctl_result(c, 0, &length);
    assert(length == 16 && g64(out, 8) == 4);
    expect_read_four(c, 1, "efgh");

    memset(&p, 0, sizeof(p));
    memset(input, 0, sizeof(input));
    memcpy(input, src.file_id, 16);
    p64(input, 24, 4);
    p64(input, 32, 4);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_DUPLICATE_EXTENTS, input, 40);
    query(&p, c, dst.file_id, 1, 5);
    ioctl_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 24 && g64(out, 8) == 8);

    memset(&p, 0, sizeof(p));
    input[0] = 1;
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_SET_SPARSE, input, 1);
    query(&p, c, dst.file_id, 1, 4);
    memset(input, 0, sizeof(input));
    p64(input, 8, 4);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_SET_ZERO_DATA, input, 16);
    read_four(&p, c, dst.file_id);
    p64(input, 8, 8);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_QUERY_ALLOCATED_RANGES, input, 16);
    ioctl_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 40 && (g32(out, 32) & 0x200u));
    expect_read_four(c, 3, "\0\0\0\0");
    out = ioctl_result(c, 4, &length);
    assert(!(length % 16));
    for (uint32_t i = 0; i < length; i += 16) {
        assert(g64(out + i, 0) + g64(out + i, 8) <= 8);
    }

    memset(&p, 0, sizeof(p));
    memset(input, 0, sizeof(input));
    p16(input, 0, 1);
    p32(input, 4, 2);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_SET_INTEGRITY_INFO, input, 8);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_GET_INTEGRITY_INFO, NULL, 0);
    p16(input, 0, 2);
    p32(input, 4, 1);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_SET_INTEGRITY_INFO, input, 8);
    ioctl_append(&p, c, dst.file_id, SMB2_FSCTL_GET_INTEGRITY_INFO, NULL, 0);
    ioctl_send(&p, c);
    out = ioctl_result(c, 1, &length);
    assert(length == 16 && g16(out, 0) == 1 && g32(out, 4) == 2);
    out = ioctl_result(c, 3, &length);
    assert(length == 16 && g16(out, 0) == 2 && g32(out, 4) == 1);

    assert(smb2_create(c, "copy-src:alt", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    query(&p, c, src.file_id, 1, 0x16);
    query(&p, c, stream.file_id, 1, 0x16);
    query(&p, c, stream.file_id, 1, 4);
    ioctl_send(&p, c);
    out = result(c, 0, &length);
    assert(length >= 64);
    uint8_t  first[2048];
    memcpy(first, out, length);
    uint32_t second_len;
    out = result(c, 1, &second_len);
    assert(second_len == length && !memcmp(first, out, length));

    /* These three operations only read filesystem state. Reject completed
     * attempts, then verify the accepted response has fully rebuilt data. */
    uint8_t  ea[14] = { 0, 0, 0, 0, 0, 3, 2, 0, 'F', 'o', 'o', 0, 'o', 'k' };
    assert(smb2_set_info(c, 1, SMB2_FILE_FULL_EA_INFO_T, src.file_id, ea, sizeof(ea)) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    query(&p, c, src.file_id, 1, SMB2_FILE_FULL_EA_INFO_T);
    query(&p, c, src.file_id, 3, 0);
    query(&p, c, stream.file_id, 1, 0x16);
    reject_count = 2;
    ioctl_send(&p, c);
    assert(atomic_load(&attempts) == 3);
    out = result(c, 0, &length);
    assert(length >= 14 && !memcmp(out + 8, "Foo\0ok", 6));
    out = result(c, 1, &length);
    assert(length >= 20 && out[0] == 1);
    out = result(c, 2, &length);
    assert(length >= 64);

    memset(&p, 0, sizeof(p));
    query(&p, c, src.file_id, 1, SMB2_FILE_FULL_EA_INFO_T);
    query(&p, c, src.file_id, 3, 0);
    query(&p, c, stream.file_id, 1, 0x16);
    reject_count    = CHIMERA_FRONTEND_COMPOUND_RETRIES + 1;
    expected_status = ST_INTERNAL_ERROR;
    ioctl_send(&p, c);
    assert(atomic_load(&attempts) == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    reject_count    = 0;
    expected_status = ST_SUCCESS;

    /* Resiliency is entirely private through rejected finishes. Two grants
     * in one batch must publish exactly one registry entry and the last timeout. */
    struct smb2_create_out resilient;
    assert(smb2_create(c, "resilient", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                       NULL, &resilient) == ST_SUCCESS);
    inspect_resiliency  = 1;
    resiliency_expected = 0;
    memset(&p, 0, sizeof(p));
    memset(input, 0, sizeof(input));
    p32(input, 0, UINT32_MAX);
    ioctl_append(&p, c, resilient.file_id, 0x001401d4, input, 8);
    expected_status = ST_INVALID_PARAMETER;
    ioctl_send(&p, c);
    memset(&p, 0, sizeof(p));
    p32(input, 0, 30000);
    ioctl_append(&p, c, resilient.file_id, 0x001401d4, input, 8);
    query(&p, c, resilient.file_id, 1, 5);
    reject_count    = CHIMERA_FRONTEND_COMPOUND_RETRIES + 1;
    expected_status = ST_INTERNAL_ERROR;
    ioctl_send(&p, c);
    assert(atomic_load(&attempts) == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);

    memset(&p, 0, sizeof(p));
    ioctl_append(&p, c, resilient.file_id, 0x001401d4, input, 8);
    p32(input, 0, 45000);
    ioctl_append(&p, c, resilient.file_id, 0x001401d4, input, 8);
    query(&p, c, resilient.file_id, 1, 5);
    reject_count    = 2;
    expected_status = ST_SUCCESS;
    ioctl_send(&p, c);
    assert(atomic_load(&attempts) == 3);
    resiliency_expected = 1;
    resiliency_timeout  = 45000;
    memset(&p, 0, sizeof(p));
    p32(input, 0, 60000);
    ioctl_append(&p, c, resilient.file_id, 0x001401d4, input, 8);
    query(&p, c, resilient.file_id, 1, 5);
    reject_count    = CHIMERA_FRONTEND_COMPOUND_RETRIES + 1;
    expected_status = ST_INTERNAL_ERROR;
    ioctl_send(&p, c);
    assert(atomic_load(&attempts) == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    expected_status = ST_SUCCESS;
    reject_count    = 0;
    memset(&p, 0, sizeof(p));
    query(&p, c, resilient.file_id, 1, 5);
    uint8_t *close_body = append(&p, c, SMB2_CLOSE, 24);
    p16(close_body, 0, 24);
    memcpy(close_body + 8, resilient.file_id, 16);
    ioctl_send(&p, c);
    inspect_resiliency = 0;
    assert(smb2_close(c, resilient.file_id) == ST_FILE_CLOSED);

    /* A grant can target an unpublished related CREATE in the same compound. */
    memset(&p, 0, sizeof(p));
    int     create_size = smb2c_build_create_full(c, "private-resilient", FILE_CREATE,
                                                  FILE_ALL_ACCESS, FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE, NULL, NULL,
                                                  0);
    p.length    = SMB2_HDR_SIZE + create_size;
    p.count     = 1;
    p.first_mid = g64(c->sbuf + 4, 24);
    memcpy(p.data, c->sbuf + 4, p.length);
    uint8_t inherited[16];
    memset(inherited, 0xff, sizeof(inherited));
    p32(input, 0, 25000);
    ioctl_append(&p, c, inherited, 0x001401d4, input, 8);
    p32(p.data + p.previous, 16, SMB2_FLAGS_RELATED_OPERATIONS);
    ioctl_send(&p, c);
    smb2c_parse_create(c, &resilient);
    inspect_resiliency = 1;
    resiliency_timeout = 25000;
    memset(&p, 0, sizeof(p));
    query(&p, c, resilient.file_id, 1, 5);
    close_body = append(&p, c, SMB2_CLOSE, 24);
    p16(close_body, 0, 24);
    memcpy(close_body + 8, resilient.file_id, 16);
    ioctl_send(&p, c);
    inspect_resiliency = 0;

    reparse_identity_checks(c);
    reparse_native_checks(&env, c);

    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    assert(smb2_close(c, dst.file_id) == ST_SUCCESS);
    assert(smb2_close(c, src.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
