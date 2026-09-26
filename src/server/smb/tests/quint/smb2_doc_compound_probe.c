// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Real wire disposition/QUERY/CLOSE groups. Finish rejection is injected only
 * for metadata-only intent batches, never after a backend REMOVE or CLOSE. */
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

static atomic_int armed, submissions, expected_groups;
static atomic_int attempts;
static int        reject_count, pause_finish;
static atomic_int held, release_finish;
static            _Thread_local struct evpl *owner_evpl;

struct finish_injection {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    seen;
    struct evpl                    *evpl;
    struct evpl_timer               timer;
    struct chimera_vfs_compound    *held_compound;
    enum chimera_vfs_error held_status;
};

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
finish_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct finish_injection *ctx = (struct finish_injection *)
        ((char *) timer - offsetof(struct finish_injection, timer));

    if (!atomic_exchange(&release_finish, 0)) {
        evpl_add_oneshot_timer(evpl, timer, finish_poll, 1000);
        return;
    }
    atomic_store(&held, 0);
    chimera_vfs_compound_finish_result(ctx->held_compound, ctx->held_status);
} /* finish_poll */

static void
intent_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_injection *ctx    = private_data;
    enum chimera_vfs_error   status = ctx->seen++ < (unsigned int) reject_count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK;

    atomic_store(&attempts, ctx->seen);
    if (pause_finish && ctx->seen == 1) {
        ctx->held_compound = compound;
        ctx->held_status   = status;
        assert(ctx->evpl);
        evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, finish_poll, 1000);
        atomic_store(&held, 1);
        return;
    }
    chimera_vfs_compound_finish_result(compound, status);
} /* intent_finish */

static void
intent_complete(
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
} /* intent_complete */



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
    if (atomic_load(&armed)) {
        atomic_fetch_add(&submissions, 1);
        assert(chimera_vfs_compound_num_groups(cp) == (uint32_t) atomic_load(&expected_groups));
        if (reject_count || pause_finish) {
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
                int type = chimera_vfs_compound_op(cp, i)->type;
                assert(type == CHIMERA_VFS_COMPOUND_OP_CHECKPOINT ||
                       type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE ||
                       type == CHIMERA_VFS_COMPOUND_OP_GETATTR ||
                       type == CHIMERA_VFS_COMPOUND_OP_COORDINATE);
            }
            struct finish_injection *ctx = calloc(1, sizeof(*ctx));
            assert(ctx);
            ctx->evpl         = owner_evpl;
            ctx->callback     = callback;
            ctx->private_data = private_data;
            chimera_vfs_compound_set_finish_handler(cp, intent_finish, ctx);
            next(cp, intent_complete, ctx);
            return;
        }

    }
    next(cp, callback, private_data);
} /* chimera_vfs_compound_submit */

struct packet {
    uint8_t      data[4096];
    unsigned int length, previous, count;
    uint64_t     first_mid;
    uint32_t     expected[32];
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
set(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint8_t           level,
    const void       *value,
    uint32_t          length)
{
    uint8_t *b = append(p, c, SMB2_SET_INFO, 32 + length);

    p16(b, 0, 33);
    b[2] = SMB2_INFO_FILE_T;
    b[3] = level;
    p32(b, 4, length);
    p16(b, 8, SMB2_HDR_SIZE + 32);
    memcpy(b + 16, fid, 16);
    memcpy(b + 32, value, length);
} /* set */

static void
doc_send(
    struct packet    *p,
    struct smb2_conn *c)
{
    int replies = c->nreply_app;

    atomic_store(&submissions, 0);
    atomic_store(&expected_groups, p->count);
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
        fprintf(stderr, "DOC response %u command %u status %08x\n", i, g16(h, 12), g32(h, 8));
        assert(g64(h, 24) == p->first_mid + i);
        assert(g32(h, 8) == p->expected[i]);
        uint32_t       next = g32(h, 20);
        assert((i + 1 < p->count) == (next != 0));
        if (next) {
            assert(next >= SMB2_HDR_SIZE && !(next & 7));
        }
        off += next;
    }
} /* doc_send */

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
close_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16])
{
    uint8_t *b = append(p, c, SMB2_CLOSE, 24);

    p16(b, 0, 24);
    memcpy(b + 8, fid, 16);
} /* close_op */

static void
stat_open(
    struct packet    *p,
    struct smb2_conn *c,
    const char       *name)
{
    unsigned int len = strlen(name);
    uint8_t     *b   = append(p, c, SMB2_CREATE, 56 + 2 * len);

    p16(b, 0, 57);
    p32(b, 4, 2); /* impersonation */
    p32(b, 24, 0x80u); /* READ_ATTRIBUTES: coalesced existing-object OPEN */
    p32(b, 32, MBT_FILE_SHARE_RWD);
    p32(b, 36, MBT_FILE_OPEN);
    p16(b, 44, SMB2_HDR_SIZE + 56);
    p16(b, 46, 2 * len);
    for (unsigned int i = 0; i < len; i++) {
        p16(b, 56 + 2 * i, name[i]);
    }
} /* stat_open */

static void
pending(
    struct smb2_conn *c,
    unsigned int      index,
    bool              expected)
{
    uint32_t       length;
    const uint8_t *out = result(c, index, &length);

    assert(length == 24 && !!out[20] == expected);
} /* pending */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_create_out a, b, probe;
    struct packet          p = { 0 };
    uint8_t                yes = 1, no = 0, out[128];
    uint32_t               length;

    smb2_env_start(&env);
    struct smb2_conn      *c = smb2_conn_open(&env);
    smb2_handshake(c);

    assert(smb2_create(c, "doc-pair", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "doc-pair", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    query(&p, c, b.file_id, 1, 0x05);
    close_op(&p, c, a.file_id);
    query(&p, c, b.file_id, 1, 0x05);
    close_op(&p, c, b.file_id);
    doc_send(&p, c);
    pending(c, 1, true);
    pending(c, 3, true);
    assert(smb2_create(c, "doc-pair", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    assert(smb2_create(c, "doc-clear", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "doc-clear", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    query(&p, c, b.file_id, 1, 0x05);
    set(&p, c, b.file_id, SMB2_FILE_DISPOSITION_T, &no, 1);
    query(&p, c, a.file_id, 1, 0x05);
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    doc_send(&p, c);
    pending(c, 1, true);
    pending(c, 3, false);
    assert(smb2_create(c, "doc-clear", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);

    /* Closing a plain peer first must preserve CREATE-DOC on the shared cache
     * handle until the deleting opener closes. */
    assert(smb2_create_opts(c, "doc-create", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "doc-create", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    close_op(&p, c, b.file_id);
    close_op(&p, c, a.file_id);
    doc_send(&p, c);
    assert(smb2_create(c, "doc-create", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* Clearing link disposition does not disarm another opener's CREATE
     * MBT_FILE_DELETE_ON_CLOSE mode, regardless of VFS cache handle sharing. */
    assert(smb2_create_opts(c, "doc-clear-control", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "doc-clear-control", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    assert(smb2_set_info(c, 1, SMB2_FILE_DISPOSITION_T, b.file_id, &no, 1) == ST_SUCCESS);
    assert(smb2_close(c, a.file_id) == ST_SUCCESS);
    assert(smb2_close(c, b.file_id) == ST_SUCCESS);
    uint32_t peer_clear_status = smb2_create(c, "doc-clear-control", MBT_FILE_OPEN,
                                             MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe);
    assert(peer_clear_status == ST_OBJECT_NAME_NOT_FOUND);
    if (peer_clear_status == ST_SUCCESS) {
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    }
    fprintf(stderr, "DOC sequential peer clear reopen status %08x\n", peer_clear_status);
    assert(smb2_create_opts(c, "doc-create-clear", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "doc-create-clear", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, b.file_id, SMB2_FILE_DISPOSITION_T, &no, 1);
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    doc_send(&p, c);
    assert(smb2_create(c, "doc-create-clear", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == peer_clear_status);
    if (peer_clear_status == ST_SUCCESS) {
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    }

    /* Even its own disposition clear preserves the distinct CREATE mode. */
    assert(smb2_create_opts(c, "doc-self-clear", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &a) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &no, 1);
    query(&p, c, a.file_id, 1, 0x05);
    close_op(&p, c, a.file_id);
    doc_send(&p, c);
    pending(c, 1, false);
    assert(smb2_create(c, "doc-self-clear", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* A private metadata OPEN already closed earlier in the batch is not a
     * live peer merely because its provisional ACCESS row remains linked. */
    assert(smb2_create(c, "doc-private-peer", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    stat_open(&p, c, "doc-private-peer");
    uint8_t inherited[16];
    memset(inherited, 0xff, sizeof(inherited));
    close_op(&p, c, inherited);
    p32(p.data + p.previous, 16, 4); /* RELATED_OPERATIONS */
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    close_op(&p, c, a.file_id);
    doc_send(&p, c);
    assert(smb2_create(c, "doc-private-peer", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* An unaccepted intent is private to its batch. Fresh OPEN and conflicting
     * setters are fenced, while an existing peer can still query/read metadata. */
    struct smb2_conn      *d = smb2_conn_open(&env);
    smb2_handshake(d);
    assert(smb2_create(c, "doc-held", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(d, "doc-held", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    struct smb2_conn      *departing = smb2_conn_open(&env);
    struct smb2_create_out departing_open, unwind_open, unwind_peer;
    smb2_handshake(departing);
    assert(smb2_create(departing, "doc-held", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &departing_open) == ST_SUCCESS);
    assert(smb2_create(d, "doc-unwind", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &unwind_open) == ST_SUCCESS);
    assert(smb2_create(departing, "doc-unwind", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &unwind_peer) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    query(&p, c, a.file_id, 1, 0x05);
    pause_finish = 1;
    atomic_store(&held, 0);
    atomic_store(&release_finish, 0);
    atomic_store(&submissions, 0);
    atomic_store(&expected_groups, p.count);
    atomic_store(&armed, 1);
    int      replies = c->nreply_app;
    memcpy(c->sbuf + 4, p.data, p.length);
    smb2c_send(c, p.length - SMB2_HDR_SIZE);
    c->msg_id = p.first_mid + p.count;
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&held)) {
        smb2_pump(&env);
        assert(smb2c_now_ms() < deadline && !c->disconnected);
    }
    atomic_store(&armed, 0);
    assert(c->nreply_app == replies && atomic_load(&submissions) == 1);
    assert(smb2_query_info(d, 1, 0x05, b.file_id, 0, out, sizeof(out), &length) == ST_SUCCESS);
    assert(length == 24 && !out[20]);
    uint32_t      fenced_open_status = smb2_create(d, "doc-held", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                                                   MBT_FILE_SHARE_RWD, NULL, &probe);
    fprintf(stderr, "DOC fenced new OPEN status %08x\n", fenced_open_status);
    assert(fenced_open_status == 0xC0000467u); /* FILE_NOT_AVAILABLE */
    struct packet waiting = { 0 };
    /* B then A: failure acquiring held A must release the earlier B fence. */
    set(&waiting, d, unwind_open.file_id, SMB2_FILE_DISPOSITION_T, &no, 1);
    set(&waiting, d, b.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    int           peer_replies = d->nreply_app;
    memcpy(d->sbuf + 4, waiting.data, waiting.length);
    smb2c_send(d, waiting.length - SMB2_HDR_SIZE);
    d->msg_id = waiting.first_mid + waiting.count;
    uint64_t      wait_until = smb2c_now_ms() + 10;
    while (smb2c_now_ms() < wait_until) {
        smb2_pump(&env);
        assert(d->nreply_app == peer_replies && !d->disconnected);
    }
    assert(smb2_close(departing, unwind_peer.file_id) == ST_SUCCESS);
    assert(atomic_load(&held) && d->nreply_app == peer_replies);
    /* A disconnected waiter must abandon its unexecuted clear. Teardown may
     * drop cache rights, but DOC/path retirement waits for the held batch. */
    struct packet abandoned = { 0 };
    set(&abandoned, departing, departing_open.file_id, SMB2_FILE_DISPOSITION_T, &no, 1);
    int           abandoned_replies = departing->nreply_app;
    memcpy(departing->sbuf + 4, abandoned.data, abandoned.length);
    smb2c_send(departing, abandoned.length - SMB2_HDR_SIZE);
    departing->msg_id = abandoned.first_mid + abandoned.count;
    wait_until        = smb2c_now_ms() + 10;
    while (smb2c_now_ms() < wait_until) {
        smb2_pump(&env);
        assert(departing->nreply_app == abandoned_replies && !departing->disconnected);
    }
    smb2_conn_disconnect(departing);
    assert(atomic_load(&held) && c->nreply_app == replies);
    atomic_store(&release_finish, 1);
    assert(smb2c_pump_for_nreply(c, replies, "DOC held acceptance"));
    pause_finish = 0;
    assert(g32(c->rbuf + 4, 8) == ST_SUCCESS);
    pending(c, 1, true);
    assert(smb2c_pump_for_nreply(d, peer_replies, "DOC conflicting setter wait"));
    assert(g32(d->rbuf + 4, 8) == ST_SUCCESS);
    assert(g32(d->rbuf + 4 + g32(d->rbuf + 4, 20), 8) == ST_SUCCESS);
    assert(smb2_close(d, unwind_open.file_id) == ST_SUCCESS);
    assert(smb2_set_info(d, 1, SMB2_FILE_DISPOSITION_T, b.file_id, &no, 1) == ST_SUCCESS);
    assert(smb2_close(c, a.file_id) == ST_SUCCESS);
    assert(smb2_close(d, b.file_id) == ST_SUCCESS);

    /* A failed validation must neither stage intent nor abort an independent
     * following query/close in the same wire compound. */
    assert(smb2_create(c, "doc-readonly", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    uint8_t basic[40] = { 0 };
    p32(basic, 32, 1); /* FILE_ATTRIBUTE_READONLY */
    assert(smb2_set_info(c, 1, SMB2_FILE_BASIC_INFO_T, a.file_id, basic, sizeof(basic)) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    p.expected[0] = 0xC0000121u; /* CANNOT_DELETE */
    query(&p, c, a.file_id, 1, 0x05);
    close_op(&p, c, a.file_id);
    doc_send(&p, c);
    pending(c, 1, false);
    assert(smb2_create(c, "doc-readonly", MBT_FILE_OPEN, 0x00000001u /* FILE_READ_DATA */,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);

    /* Retry/exhaustion touch no filesystem mutation: the setter only stages
     * intent, and rejected callbacks cannot publish delete_pending. */
    assert(smb2_create(c, "doc-retry", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    query(&p, c, a.file_id, 1, 0x05);
    reject_count = 2;
    doc_send(&p, c);
    assert(atomic_load(&attempts) == 3);
    pending(c, 1, true);
    reject_count = 0;
    assert(smb2_set_info(c, 1, SMB2_FILE_DISPOSITION_T, a.file_id, &no, 1) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    set(&p, c, a.file_id, SMB2_FILE_DISPOSITION_T, &yes, 1);
    query(&p, c, a.file_id, 1, 0x05);
    p.expected[0] = p.expected[1] = 0xC00000E5u; /* INTERNAL_ERROR exhaustion */
    reject_count  = CHIMERA_FRONTEND_COMPOUND_RETRIES + 2;
    doc_send(&p, c);
    assert(atomic_load(&attempts) == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    reject_count = 0;
    assert(smb2_query_info(c, 1, 0x05, a.file_id, 0, out, sizeof(out), &length) == ST_SUCCESS);
    assert(length == 24 && !out[20]);
    assert(smb2_close(c, a.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "doc-retry", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
