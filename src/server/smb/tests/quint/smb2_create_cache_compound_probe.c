// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"

extern void smb_create_cache_inspect(void *, unsigned int, unsigned int, unsigned int);
extern void smb_persistent_create_expect(bool, int);
extern unsigned int smb_persistent_create_checks(void);
extern unsigned int smb_lease_key_pending_count(void);
extern void smb_lease_unlink_after_lookup(const char *);
extern unsigned int smb_lease_unlink_count(void);
static unsigned int expected_lease_members, expected_lease_epoch, expected_lease_state;
static atomic_int armed, submissions, hold_next, held, release_finish, finishes;
static unsigned int groups[4];
static int reject_first;
static bool expected_overwrite;
static _Thread_local struct evpl *owner_evpl;
struct finish_hold {
    struct evpl *evpl;
    struct evpl_timer timer;
    struct chimera_vfs_compound *compound;
    chimera_vfs_compound_callback_t callback;
    void *private_data;
    unsigned int seen;
};

__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(struct chimera_vfs_thread *thread, const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*fn)(struct chimera_vfs_thread *, const struct chimera_vfs_cred *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next); owner_evpl = thread->evpl;
    return next(thread, cred);
}
static void finish_poll(struct evpl *evpl, struct evpl_timer *timer)
{
    struct finish_hold *ctx = (void *) ((char *) timer - offsetof(struct finish_hold, timer));
    if (!atomic_exchange(&release_finish, 0)) {
        evpl_add_oneshot_timer(evpl, timer, finish_poll, 1000); return;
    }
    atomic_store(&held, 0);
    chimera_vfs_compound_finish_result(ctx->compound,
        reject_first ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}
static void inspect_finish(struct chimera_vfs_compound *compound, void *private_data)
{
    struct finish_hold *ctx = private_data;
    unsigned int found = 0, overwrites = 0;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (expected_overwrite && op->type == CHIMERA_VFS_COMPOUND_OP_OVERWRITE) {
            assert(op->status == CHIMERA_VFS_OK && op->attr.va_size == 0);
            overwrites++;
        }
        if (op->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
            smb_create_cache_inspect(op->callback_private, expected_lease_members, expected_lease_epoch, expected_lease_state); found++;
        }
    }
    assert(found == 1 && (!expected_overwrite || overwrites == 1));
    atomic_fetch_add(&finishes, 1);
    if (ctx->seen++) { chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK); return; }
    ctx->compound = compound;
    evpl_add_oneshot_timer(ctx->evpl, &ctx->timer, finish_poll, 1000);
    atomic_store(&held, 1);
}
static void hold_complete(struct chimera_vfs_compound *compound, void *private_data)
{
    struct finish_hold *ctx = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void *arg = ctx->private_data;
    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN) free(ctx);
    callback(compound, arg);
}
__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(struct chimera_vfs_compound *compound,
    chimera_vfs_compound_callback_t callback, void *private_data)
{
    typedef void (*fn)(struct chimera_vfs_compound *, chimera_vfs_compound_callback_t, void *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        unsigned int index = atomic_fetch_add(&submissions, 1);
        assert(index < 4 && chimera_vfs_compound_num_groups(compound) == groups[index]);
        if (atomic_exchange(&hold_next, 0)) {
            struct finish_hold *ctx = calloc(1, sizeof(*ctx));
            assert(ctx && owner_evpl);
            ctx->evpl = owner_evpl; ctx->callback = callback; ctx->private_data = private_data;
            chimera_vfs_compound_set_finish_handler(compound, inspect_finish, ctx);
            next(compound, hold_complete, ctx); return;
        }
    }
    next(compound, callback, private_data);
}
static void arm(unsigned int first, unsigned int second)
{
    groups[0] = first; groups[1] = second;
    atomic_store(&submissions, 0); atomic_store(&armed, 1);
}
static void disarm(unsigned int count)
{
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == count);
}
struct packet { uint8_t data[2048]; unsigned int length, previous, count; uint64_t mid; };
static uint8_t *append(struct packet *p, struct smb2_conn *c, uint16_t opcode, unsigned int size, bool related)
{
    unsigned int start = (p->length + 7) & ~7u;
    if (p->count) p32(p->data + p->previous, 20, start - p->previous);
    else p->mid = c->msg_id;
    uint8_t *h = p->data + start;
    assert(start + SMB2_HDR_SIZE + size <= sizeof(p->data));
    memcpy(h, "\xfeSMB", 4); p16(h, 4, 64); p16(h, 6, 1); p16(h, 12, opcode); p16(h, 14, 32);
    p64(h, 24, p->mid + p->count); p32(h, 36, c->tree_id); p64(h, 40, c->session_id);
    if (related) p32(h, 16, SMB2_FLAGS_RELATED_OPERATIONS);
    p->count++; p->previous = start; p->length = start + SMB2_HDR_SIZE + size;
    return h + SMB2_HDR_SIZE;
}
static void query(struct packet *p, struct smb2_conn *c, const uint8_t *fid)
{
    uint8_t *b = append(p, c, SMB2_QUERY_INFO, 40, !fid);
    p16(b, 0, 41); b[2] = 1; b[3] = 4; p32(b, 4, 2048);
    if (fid) memcpy(b + 24, fid, 16); else memset(b + 24, 0xff, 16);
}
static void create_chain(struct smb2_conn *c, const uint8_t *prefix, const char *name, uint8_t level)
{
    struct packet p = { 0 };
    struct smb2_oplock_req oplock = { .level = level };
    query(&p, c, prefix);
    int size = smb2c_build_create(c, name, FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE, &oplock);
    uint8_t *b = append(&p, c, SMB2_CREATE, size, false);
    memcpy(b, c->sbuf + 4 + SMB2_HDR_SIZE, size);
    query(&p, c, NULL);
    b = append(&p, c, SMB2_CLOSE, 24, true); p16(b, 0, 24); memset(b + 8, 0xff, 16);
    memcpy(c->sbuf + 4, p.data, p.length);
    arm(3, 1); smb2c_send(c, p.length - SMB2_HDR_SIZE); c->msg_id = p.mid + p.count;
    smb2c_wait(c); disarm(2);
    const uint8_t *h = c->rbuf + 4;
    for (unsigned int i = 0; i < p.count; i++) {
        assert(g64(h, 24) == p.mid + i && g32(h, 8) == ST_SUCCESS);
        if (i == 1) assert(h[SMB2_HDR_SIZE + 2] == level);
        uint32_t next = g32(h, 20); assert(!!next == (i + 1 < p.count));
        if (next) h += next;
    }
}
static void suffix_create(struct packet *p, struct smb2_conn *c, const char *name,
    uint32_t access, const struct smb2_oplock_req *cache)
{
    int size = smb2c_build_create(c, name, FILE_OPEN, access, FILE_SHARE_RWD,
        FILE_NON_DIRECTORY_FILE, cache);
    uint8_t *b = append(p, c, SMB2_CREATE, size, false);
    memcpy(b, c->sbuf + 4 + SMB2_HDR_SIZE, size);
}
static void suffix_read(struct packet *p, struct smb2_conn *c)
{
    uint8_t *b = append(p, c, SMB2_READ, 49, true);
    p16(b, 0, 49); b[2] = SMB2_HDR_SIZE + 16; p32(b, 4, 4);
    memset(b + 16, 0xff, 16);
}
static void suffix_write(struct packet *p, struct smb2_conn *c)
{
    uint8_t *b = append(p, c, SMB2_WRITE, 52, true);
    p16(b, 0, 49); p16(b, 2, SMB2_HDR_SIZE + 48); p32(b, 4, 4);
    memset(b + 16, 0xff, 16); memcpy(b + 48, "next", 4);
}
static void suffix_flush(struct packet *p, struct smb2_conn *c)
{
    uint8_t *b = append(p, c, SMB2_FLUSH, 24, true);
    p16(b, 0, 24); memset(b + 8, 0xff, 16);
}
static void suffix_send(struct packet *p, struct smb2_conn *c)
{
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE); c->msg_id = p->mid + p->count;
}
static void suffix_check(struct packet *p, struct smb2_conn *c,
    unsigned int fail_at, uint32_t failure, const char *data)
{
    const uint8_t *h = c->rbuf + 4;
    for (unsigned int i = 0; i < p->count; i++) {
        uint32_t expected = i == fail_at ? failure : ST_SUCCESS;
        assert(g64(h, 24) == p->mid + i && g32(h, 8) == expected);
        if (expected == ST_SUCCESS && g16(h, 12) == SMB2_READ) {
            const uint8_t *b = h + SMB2_HDR_SIZE;
            assert(g32(b, 4) == 4 && !memcmp(h + b[2], data, 4));
        }
        uint32_t next = g32(h, 20); assert(!!next == (i + 1 < p->count));
        if (next) h += next;
    }
}

/* No grant/member/FileId is visible during a compound with a supported
 * private suffix, including a rejected read-only attempt. */
static void caching_create_suffixes(struct smb2_conn *c, const uint8_t *other_fid)
{
    const uint8_t levels[] = { SMB2_OPLOCK_LEVEL_II, SMB2_OPLOCK_LEVEL_EXCLUSIVE,
        SMB2_OPLOCK_LEVEL_BATCH };
    for (unsigned int i = 0; i < sizeof(levels); i++) {
        char name[64]; snprintf(name, sizeof(name), "cache-private-suffix-%u", i);
        struct smb2_create_out setup, opened;
        uint32_t bytes;
        assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            NULL, &setup) == ST_SUCCESS);
        assert(smb2_write(c, setup.file_id, 0, "seed", 4, &bytes) == ST_SUCCESS && bytes == 4);
        assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
        struct smb2_oplock_req cache = { .level = levels[i] };
        struct packet p = {0};
        suffix_create(&p, c, name, FILE_ALL_ACCESS, &cache);
        query(&p, c, NULL); suffix_read(&p, c); suffix_flush(&p, c);
        reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(4, 0);
        suffix_send(&p, c);
        uint64_t deadline = smb2c_now_ms() + 10000;
        while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
        assert(atomic_load(&held) && !smb2_conn_nbreaks(c));
        atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
        assert(atomic_load(&finishes) == 2); reject_first = 0;
        suffix_check(&p, c, UINT32_MAX, 0, "seed");
        smb2c_parse_create(c, &opened); assert(opened.oplock == levels[i]);
        assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    }

    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = {0xb0, 0x20}, .lease_epoch = 70 };
    struct smb2_create_out holder, joined;
    uint32_t bytes;
    assert(smb2_create(c, "lease-private-suffix", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
    assert(smb2_write(c, holder.file_id, 0, "seed", 4, &bytes) == ST_SUCCESS && bytes == 4);
    expected_lease_members = 1; expected_lease_epoch = holder.lease_epoch;
    expected_lease_state = holder.lease_state;
    struct packet p = {0};
    suffix_create(&p, c, "lease-private-suffix", FILE_ALL_ACCESS, &lease);
    query(&p, c, NULL); suffix_read(&p, c);
    reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(3, 0);
    suffix_send(&p, c);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 2); reject_first = 0;
    suffix_check(&p, c, UINT32_MAX, 0, "seed"); smb2c_parse_create(c, &joined);
    assert(joined.lease_state == holder.lease_state && joined.lease_epoch == holder.lease_epoch);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);

    /* WRITE must keep the private producer's lease key: the already-public
     * same-key holder is coherent and must not receive a self-break. No finish
     * rejection is injected after this real filesystem mutation. */
    memset(&p, 0, sizeof(p));
    suffix_create(&p, c, "lease-private-suffix", FILE_ALL_ACCESS, &lease);
    suffix_write(&p, c); suffix_read(&p, c); suffix_flush(&p, c);
    atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(4, 0);
    suffix_send(&p, c);
    deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 1);
    suffix_check(&p, c, UINT32_MAX, 0, "next"); smb2c_parse_create(c, &joined);
    assert(joined.lease_state == holder.lease_state && joined.lease_epoch == holder.lease_epoch);
    assert(!smb2_conn_nbreaks(c)); assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
    expected_lease_members = expected_lease_epoch = expected_lease_state = 0;
    assert(smb2_close(c, holder.file_id) == ST_SUCCESS);

    /* A failed suffix is an accepted CREATE prefix; later safe commands still
     * run and the caller receives a usable cached handle. */
    memset(&p, 0, sizeof(p));
    suffix_create(&p, c, "lease-private-suffix", FILE_READ_ATTRIBUTES, &lease);
    suffix_read(&p, c); query(&p, c, NULL); arm(3, 0); suffix_send(&p, c);
    smb2c_wait(c); disarm(1); suffix_check(&p, c, 1, ST_ACCESS_DENIED, NULL);
    smb2c_parse_create(c, &joined); assert(joined.has_lease && joined.lease_state);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);

    /* A specific/unrelated existing handle is not the producer's private
     * identity and must execute only after its cached CREATE is accepted. */
    memset(&p, 0, sizeof(p));
    suffix_create(&p, c, "lease-private-suffix", FILE_ALL_ACCESS, &lease);
    query(&p, c, other_fid); arm(1, 1); suffix_send(&p, c);
    smb2c_wait(c); disarm(2); suffix_check(&p, c, UINT32_MAX, 0, NULL);
    smb2c_parse_create(c, &joined); assert(smb2_close(c, joined.file_id) == ST_SUCCESS);

    /* Legacy LEVEL_II WRITE keeps its acceptance boundary and its ordinary
     * self-break; caching CREATE+QUERY+CLOSE boundaries are covered above. */
    struct smb2_oplock_req level2 = { .level = SMB2_OPLOCK_LEVEL_II };
    memset(&p, 0, sizeof(p));
    suffix_create(&p, c, "cache-private-suffix-0", FILE_ALL_ACCESS, &level2);
    suffix_write(&p, c); arm(1, 1); suffix_send(&p, c);
    smb2c_wait(c); disarm(2); suffix_check(&p, c, UINT32_MAX, 0, NULL);
    smb2c_parse_create(c, &joined); assert(joined.oplock == SMB2_OPLOCK_LEVEL_II);
    deadline = smb2c_now_ms() + 10000;
    while (!smb2_conn_nbreaks(c) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    struct smb2_break brk;
    assert(smb2_conn_pop_break(c, &brk) && !brk.is_lease && !brk.oplock_level);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
}

static uint8_t captured_create[4096];
static int captured_create_len;
static int capture_create(struct smb2_conn *c, const uint8_t *wire, int length)
{
    (void) c;
    assert(length > 0 && (size_t) length <= sizeof(captured_create));
    memcpy(captured_create, wire, length); captured_create_len = length;
    return 1;
}

static void overwrite_client_cap(struct smb2_conn *c)
{
    const uint32_t dispositions[] = { FILE_OVERWRITE, FILE_OVERWRITE_IF, FILE_SUPERSEDE };
    for (unsigned int i = 0; i < 3; i++) {
        char name[64]; snprintf(name, sizeof(name), "same-client-overwrite-%u", i);
        struct smb2_oplock_req lease = { .is_lease = 1,
            .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
            .lease_key = { 0x64, 0x12 } };
        lease.lease_key[2] = i;
        struct smb2_oplock_req req = { .level = SMB2_OPLOCK_LEVEL_EXCLUSIVE };
        struct smb2_create_out holder, opened;
        assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS,
            FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
        assert(holder.has_lease && holder.lease_state == lease.lease_state);
        /* CREATE and its lease ACK use the same client connection. Preserve
         * CREATE independently if it completes before the ACK response. */
        captured_create_len = 0; c->capture_create = capture_create;
        arm(1, 0);
        smb2_create_post(c, name, dispositions[i], FILE_READ_ATTRIBUTES, FILE_SHARE_RWD, &req);
        uint64_t deadline = smb2c_now_ms() + 10000;
        while (!smb2_conn_nbreaks(c) && smb2c_now_ms() < deadline) smb2_pump(c->env);
        struct smb2_break brk;
        assert(smb2_conn_pop_break(c, &brk) && brk.is_lease && brk.ack_required && !brk.new_state);
        assert(smb2_lease_break_ack(c, brk.lease_key, brk.new_state) == ST_SUCCESS);
        while (!captured_create_len && smb2c_now_ms() < deadline) smb2_pump(c->env);
        assert(captured_create_len); disarm(1); c->capture_create = NULL;
        memcpy(c->rbuf, captured_create, captured_create_len); c->rlen = captured_create_len;
        smb2c_parse_create(c, &opened);
        assert(opened.status == ST_SUCCESS && opened.oplock == SMB2_OPLOCK_LEVEL_NONE);
        assert(!smb2_conn_nbreaks(c));
        assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
        assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
    }
}

static unsigned int lease_context_length(const struct smb2_create_out *out)
{
    const struct smb2_rsp_ctx *context = smb2c_create_ctx_find(out, "RqLs");
    assert(context);
    return context->wire_len;
}
static void regular_lease_chain(struct smb2_conn *c, const uint8_t *prefix, unsigned int mode)
{
    struct packet p = {0};
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = mode,
        .lease_key = { 0x63, 0x13 } };
    char name[64]; snprintf(name, sizeof(name), "lease-chain-%u", mode);
    query(&p, c, prefix);
    int size = smb2c_build_create(c, name, FILE_CREATE, FILE_READ_ACCESS,
        FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE, &lease);
    uint8_t *b = append(&p, c, SMB2_CREATE, size, false);
    memcpy(b, c->sbuf + 4 + SMB2_HDR_SIZE, size);
    query(&p, c, NULL);
    b = append(&p, c, SMB2_CLOSE, 24, true); p16(b, 0, 24); memset(b + 8, 0xff, 16);
    memcpy(c->sbuf + 4, p.data, p.length);
    arm(3, 1); smb2c_send(c, p.length - SMB2_HDR_SIZE); c->msg_id = p.mid + p.count;
    smb2c_wait(c); disarm(2);
    const uint8_t *h = c->rbuf + 4;
    for (unsigned int i = 0; i < p.count; i++) {
        assert(g64(h, 24) == p.mid + i && g32(h, 8) == ST_SUCCESS);
        if (i == 1) assert(h[SMB2_HDR_SIZE + 2] == SMB2_OPLOCK_LEVEL_LEASE);
        uint32_t next = g32(h, 20); assert(!!next == (i + 1 < p.count));
        if (next) h += next;
    }
}
static void read_lease_cases(struct smb2_conn *c, struct smb2_conn *peer, const uint8_t *prefix)
{
    for (unsigned int mode = 1; mode < 8; mode += 2) { regular_lease_chain(c, prefix, mode); }
    for (unsigned int v1 = 0; v1 < 2; v1++) {
        char name[64]; snprintf(name, sizeof(name), "native-read-lease-%u", v1);
        struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_READ,
            .lease_key = { 0x64, 0x13 }, .lease_epoch = 42, .force_v1 = v1 };
        lease.lease_key[2] = v1;
        struct smb2_create_out holder, joined, none;
        arm(1, 0);
        assert(smb2_create(c, name, FILE_CREATE, FILE_READ_ACCESS,
            FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS); disarm(1);
        assert(holder.has_lease && holder.lease_state == SMB2_LEASE_READ);
        assert(holder.lease_epoch == (v1 ? 0 : 43));
        assert(lease_context_length(&holder) == (v1 ? 32 : 52));
        /* The lease's version/epoch wins over the joining request's version
         * and arbitrary epoch, including a NONE request. */
        lease.force_v1 = !v1; lease.lease_epoch = 900;
        arm(1, 0);
        assert(smb2_create(c, name, FILE_OPEN, FILE_READ_ACCESS,
            FILE_SHARE_RWD, &lease, &joined) == ST_SUCCESS); disarm(1);
        assert(joined.lease_state == holder.lease_state && joined.lease_epoch == holder.lease_epoch);
        assert(lease_context_length(&joined) == lease_context_length(&holder));
        lease.lease_state = 0; arm(1, 0);
        assert(smb2_create(c, name, FILE_OPEN, FILE_READ_ATTRIBUTES,
            FILE_SHARE_RWD, &lease, &none) == ST_SUCCESS); disarm(1);
        assert(none.lease_state == holder.lease_state && none.lease_epoch == holder.lease_epoch);
        assert(lease_context_length(&none) == lease_context_length(&holder));
        /* Lease keys cannot migrate to another inode, including NONE reopens.
         * Reject missing create-capable names before namespace mutation. */
        struct smb2_create_out rejected;
        arm(1, 0);
        assert(smb2_create(c, "cache-create-prefix", FILE_OPEN, FILE_READ_ATTRIBUTES,
            FILE_SHARE_RWD, &lease, &rejected) == ST_INVALID_PARAMETER); disarm(1);
        lease.lease_state = SMB2_LEASE_READ;
        arm(1, 0);
        assert(smb2_create(c, "cache-create-prefix", FILE_OPEN, FILE_READ_ATTRIBUTES,
            FILE_SHARE_RWD, &lease, &rejected) == ST_INVALID_PARAMETER); disarm(1);
        for (unsigned int create_if = 0; create_if < 2; create_if++) {
            char other[64]; snprintf(other, sizeof(other), "read-lease-wrong-%u-%u", v1, create_if);
            arm(1, 0);
            assert(smb2_create(c, other, create_if ? FILE_OPEN_IF : FILE_CREATE, FILE_READ_ACCESS,
                FILE_SHARE_RWD, &lease, &rejected) == ST_INVALID_PARAMETER); disarm(1);
            assert(smb2_create(c, other, FILE_OPEN, FILE_READ_ACCESS,
                FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
        }
        assert(!smb2_conn_nbreaks(c));
        assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
        assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
        assert(smb2_close(c, none.file_id) == ST_SUCCESS);
    }
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x65, 0x13 }, .lease_epoch = 51 };
    struct smb2_create_out holder, joined, opened;
    assert(smb2_create(c, "native-read-join-strong", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
    assert(holder.lease_epoch == 52 && holder.lease_state == lease.lease_state);
    /* Reject an existing OPEN only: shared grant membership stays unchanged
     * until the second accepted finish; no filesystem mutation is retried. */
    lease.lease_state = SMB2_LEASE_READ; lease.lease_epoch = 999;
    expected_lease_members = 1; expected_lease_epoch = holder.lease_epoch;
    expected_lease_state = holder.lease_state;
    reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(1, 0);
    int replies = c->nreply_app;
    smb2_create_post(c, "native-read-join-strong", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &lease);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && c->nreply_app == replies && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 2); reject_first = 0;
    expected_lease_members = expected_lease_epoch = 0;
    smb2c_parse_create(c, &joined);
    assert(joined.status == ST_SUCCESS && joined.lease_state == holder.lease_state &&
        joined.lease_epoch == holder.lease_epoch);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);

    /* A native same-key reopen during another client's recall must return
     * BREAK_IN_PROGRESS without waiting for or overwriting that lease epoch. */
    smb2_create_post(peer, "native-read-join-strong", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, NULL);
    deadline = smb2c_now_ms() + 10000;
    while (!smb2_conn_nbreaks(c) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    struct smb2_break brk;
    assert(smb2_conn_pop_break(c, &brk) && brk.is_lease && brk.ack_required);
    assert(brk.new_epoch == 53);
    arm(1, 0);
    assert(smb2_create(c, "native-read-join-strong", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &lease, &joined) == ST_SUCCESS); disarm(1);
    assert(joined.lease_flags & SMB2_LEASE_FLAG_BREAK_IN_PROGRESS);
    assert(joined.lease_epoch == brk.new_epoch && joined.lease_state == holder.lease_state);
    /* A full-mode same-key request must also bypass its own pending ACK,
     * preserve the active break epoch and report BREAK_IN_PROGRESS. */
    struct smb2_create_out high_join;
    lease.lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE;
    lease.lease_epoch = 4096;
    arm(1, 0);
    assert(smb2_create(c, "native-read-join-strong", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &lease, &high_join) == ST_SUCCESS); disarm(1);
    assert(high_join.lease_flags & SMB2_LEASE_FLAG_BREAK_IN_PROGRESS);
    assert(high_join.lease_epoch == brk.new_epoch && high_join.lease_state == holder.lease_state);
    assert(!smb2_conn_nbreaks(c));
    assert(smb2_lease_break_ack(c, brk.lease_key, brk.new_state) == ST_SUCCESS);
    smb2c_wait(peer); smb2c_parse_create(peer, &opened); assert(opened.status == ST_SUCCESS);
    assert(smb2_close(peer, opened.file_id) == ST_SUCCESS);
    assert(smb2_close(c, high_join.file_id) == ST_SUCCESS);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
    assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
}

/* Exercise fresh higher-mode grants, invalid masks, same-key upgrades and
 * opportunistic caps through native groups. Read-only finish retry must leave
 * the old grant's mode and epoch unchanged until accepted publication. */
static void regular_lease_cases(struct smb2_conn *c, struct smb2_conn *peer)
{
    for (unsigned int v1 = 0; v1 < 2; v1++) {
        for (unsigned int mode = 0; mode < 8; mode++) {
            char name[64]; snprintf(name, sizeof(name), "native-full-lease-%u-%u", v1, mode);
            struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = mode,
                .lease_key = { 0x74, 0x14 }, .lease_epoch = 60, .force_v1 = v1 };
            lease.lease_key[2] = v1; lease.lease_key[3] = mode;
            struct smb2_create_out first, joined;
            uint32_t granted = mode & SMB2_LEASE_READ ? mode : 0;
            arm(1, 0);
            assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, &lease, &first) == ST_SUCCESS); disarm(1);
            assert(first.has_lease && first.lease_state == granted);
            if (granted) assert(first.lease_epoch == (v1 ? 0 : 61));
            lease.lease_epoch = 900;
            arm(1, 0);
            assert(smb2_create(c, name, FILE_OPEN_IF, FILE_READ_ATTRIBUTES,
                FILE_SHARE_RWD, &lease, &joined) == ST_SUCCESS); disarm(1);
            assert(joined.lease_state == granted);
            if (granted) assert(joined.lease_epoch == first.lease_epoch);
            assert(!smb2_conn_nbreaks(c));
            assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
            assert(smb2_close(c, first.file_id) == ST_SUCCESS);
        }
    }
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_READ,
        .lease_key = { 0x75, 0x14 }, .lease_epoch = 70 };
    struct smb2_create_out holder, joined, opened;
    arm(1, 0);
    assert(smb2_create(c, "native-upgrade-retry", FILE_CREATE, FILE_READ_ACCESS,
        FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS); disarm(1);
    lease.lease_state |= SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE;
    lease.lease_epoch = 900;
    expected_lease_members = 1; expected_lease_epoch = holder.lease_epoch;
    expected_lease_state = holder.lease_state;
    reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(1, 0);
    int replies = c->nreply_app;
    smb2_create_post(c, "native-upgrade-retry", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &lease);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && c->nreply_app == replies && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 2); reject_first = 0;
    expected_lease_members = expected_lease_epoch = expected_lease_state = 0;
    smb2c_parse_create(c, &joined);
    assert(joined.status == ST_SUCCESS && joined.lease_state == lease.lease_state &&
        joined.lease_epoch == holder.lease_epoch + 1);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
    assert(smb2_close(c, holder.file_id) == ST_SUCCESS);

    /* A transparent metadata OPEN cannot recall a peer's write cache just
     * to obtain its own RWH. No-grant is an ordinary accepted CREATE result. */
    lease.lease_key[2] = 1; lease.lease_epoch = 80;
    assert(smb2_create(peer, "native-high-cap", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
    assert(holder.lease_state == lease.lease_state);
    struct smb2_oplock_req requester = lease; requester.lease_key[2] = 2;
    arm(1, 0);
    assert(smb2_create(c, "native-high-cap", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &requester, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.has_lease && !opened.lease_state && !smb2_conn_nbreaks(peer));
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);

    /* A real data OPEN recalls the peer before acceptance. Keeping that
     * peer's keyed READ grant allows RH, never a fresh conflicting W. */
    arm(1, 0);
    smb2_create_post(c, "native-high-cap", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, &requester);
    deadline = smb2c_now_ms() + 10000;
    while (!smb2_conn_nbreaks(peer) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    struct smb2_break brk;
    assert(smb2_conn_pop_break(peer, &brk) && brk.is_lease && brk.ack_required);
    assert(smb2_lease_break_ack(peer, brk.lease_key, brk.new_state) == ST_SUCCESS);
    smb2c_wait(c); disarm(1); smb2c_parse_create(c, &opened);
    assert(opened.status == ST_SUCCESS && opened.has_lease &&
        opened.lease_state == (SMB2_LEASE_READ | SMB2_LEASE_HANDLE));
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    assert(smb2_close(peer, holder.file_id) == ST_SUCCESS);
}

/* All six dispositions use the same typed pipeline with and without the
 * NON_DIRECTORY hint. Supported directory leases also remain native. */
static void hintless_and_overwrite_cases(struct smb2_conn *c)
{
    const uint32_t dispositions[] = { FILE_CREATE, FILE_OPEN, FILE_OPEN_IF,
        FILE_OVERWRITE, FILE_OVERWRITE_IF, FILE_SUPERSEDE };
    for (unsigned int hinted = 0; hinted < 2; hinted++) {
        for (unsigned int existing = 0; existing < 2; existing++) {
            for (unsigned int i = 0; i < sizeof(dispositions) / sizeof(dispositions[0]); i++) {
                char name[64]; snprintf(name, sizeof(name), "lease-disposition-%u-%u-%u", hinted, existing, i);
                struct smb2_create_out seed, opened;
                uint32_t bytes;
                if (existing) {
                    assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS,
                        FILE_SHARE_RWD, NULL, &seed) == ST_SUCCESS);
                    assert(smb2_write(c, seed.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS && bytes == 5);
                    assert(smb2_close(c, seed.file_id) == ST_SUCCESS);
                }
                struct smb2_oplock_req lease = { .is_lease = 1,
                    .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
                    .lease_key = { 0x77, 0x15 }, .lease_epoch = 100 };
                lease.lease_key[2] = hinted; lease.lease_key[3] = existing; lease.lease_key[4] = i;
                uint32_t expected = existing && dispositions[i] == FILE_CREATE ? ST_OBJECT_NAME_COLLISION :
                    !existing && (dispositions[i] == FILE_OPEN || dispositions[i] == FILE_OVERWRITE) ?
                    ST_OBJECT_NAME_NOT_FOUND : ST_SUCCESS;
                arm(1, 0);
                assert(smb2_create_opts(c, name, dispositions[i], FILE_ALL_ACCESS,
                    FILE_SHARE_RWD, hinted ? FILE_NON_DIRECTORY_FILE : 0, &lease, &opened) == expected);
                disarm(1);
                if (expected != ST_SUCCESS) { continue; }
                assert(opened.has_lease && opened.lease_state == lease.lease_state && opened.lease_epoch == 101);
                bool truncated = i >= 3;
                assert(opened.end_of_file == (existing && !truncated ? 5 : 0));
                assert(opened.action == (!existing ? 2 : dispositions[i] == FILE_SUPERSEDE ? 0 : truncated ? 3 : 1));
                uint8_t data[5];
                assert(smb2_read(c, opened.file_id, 0, sizeof(data), data, &bytes) ==
                    (existing && !truncated ? ST_SUCCESS : ST_END_OF_FILE));
                if (existing && !truncated) { assert(bytes == 5 && !memcmp(data, "keep!", 5)); }
                assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
            }
        }
    }

    /* Same-key overwrite has an existing live cache member. Its private
     * reservation, completed truncate and reply remain unpublished at finish;
     * the existing grant must not recall itself or change epoch/membership. */
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x78, 0x15 }, .lease_epoch = 110 };
    struct smb2_create_out holder, opened;
    uint32_t bytes;
    assert(smb2_create_opts(c, "lease-own-overwrite", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, 0, &lease, &holder) == ST_SUCCESS);
    assert(smb2_write(c, holder.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS && bytes == 5);
    expected_lease_members = 1; expected_lease_epoch = holder.lease_epoch;
    expected_lease_state = holder.lease_state; expected_overwrite = true;
    reject_first = 0; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(1, 0);
    int replies = c->nreply_app;
    smb2_create_post_opts(c, "lease-own-overwrite", FILE_OVERWRITE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, 0, &lease);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && c->nreply_app == replies && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 1); expected_overwrite = false;
    expected_lease_members = expected_lease_epoch = expected_lease_state = 0;
    smb2c_parse_create(c, &opened);
    assert(opened.status == ST_SUCCESS && opened.action == 3 && !opened.end_of_file &&
        opened.lease_state == holder.lease_state && opened.lease_epoch == holder.lease_epoch);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);

    /* A key already bound to a different file rejects before truncation, and
     * rejects creating variants before any new namespace entry can appear. */
    struct smb2_create_out victim;
    assert(smb2_create(c, "lease-key-victim", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &victim) == ST_SUCCESS);
    assert(smb2_write(c, victim.file_id, 0, "keep!", 5, &bytes) == ST_SUCCESS && bytes == 5);
    for (unsigned int i = 3; i < sizeof(dispositions) / sizeof(dispositions[0]); i++) {
        arm(1, 0);
        assert(smb2_create_opts(c, "lease-key-victim", dispositions[i], FILE_ALL_ACCESS,
            FILE_SHARE_RWD, 0, &lease, &opened) == ST_INVALID_PARAMETER); disarm(1);
        uint8_t data[5];
        assert(smb2_read(c, victim.file_id, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
            bytes == 5 && !memcmp(data, "keep!", 5));
        arm(1, 0);
        assert(smb2_create_opts(c, "lease-key-missing", dispositions[i], FILE_ALL_ACCESS,
            FILE_SHARE_RWD, 0, &lease, &opened) ==
            (dispositions[i] == FILE_OVERWRITE ? ST_OBJECT_NAME_NOT_FOUND : ST_INVALID_PARAMETER)); disarm(1);
        assert(smb2_create(c, "lease-key-missing", FILE_OPEN, FILE_READ_ACCESS,
            FILE_SHARE_RWD, NULL, &opened) == ST_OBJECT_NAME_NOT_FOUND);
    }
    arm(1, 0);
    assert(smb2_create_opts(c, "lease-key-missing", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, 0, &lease, &opened) == ST_OBJECT_NAME_NOT_FOUND); disarm(1);
    assert(smb2_close(c, victim.file_id) == ST_SUCCESS);
    assert(smb2_close(c, holder.file_id) == ST_SUCCESS);

    /* Hintless directory identity uses the native lease lifecycle, preserving
     * existing children and their data. */
    struct smb2_create_out directory, child;
    assert(smb2_create_opts(c, "lease-hintless-directory", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    assert(smb2_create(c, "lease-hintless-directory\\child", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    assert(smb2_write(c, child.file_id, 0, "child", 5, &bytes) == ST_SUCCESS);
    assert(smb2_close(c, child.file_id) == ST_SUCCESS);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    lease.lease_key[0] = 0x79;
    for (unsigned int i = 0; i < 2; i++) {
        arm(1, 0);
        assert(smb2_create_opts(c, "lease-hintless-directory", i ? FILE_OPEN_IF : FILE_OPEN,
            FILE_READ_ATTRIBUTES, FILE_SHARE_RWD, 0, &lease, &directory) == ST_SUCCESS); disarm(1);
        assert(directory.has_lease && directory.lease_state == (SMB2_LEASE_READ | SMB2_LEASE_HANDLE));
        assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    }
    assert(smb2_create(c, "lease-hintless-directory\\child", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    uint8_t data[5];
    assert(smb2_read(c, child.file_id, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
        bytes == 5 && !memcmp(data, "child", 5));
    assert(smb2_close(c, child.file_id) == ST_SUCCESS);
}

/* Explicit directory CREATE/OPEN_IF, hintless joins, invalid masks and a
 * read-only retry all use native groups with accepted DIR_LEASE publication. */
static void directory_lease_cases(struct smb2_conn *c, struct smb2_conn *peer,
                                  const uint8_t *prefix)
{
    for (unsigned int mode = 0; mode < 8; mode++) {
        char name[64]; snprintf(name, sizeof(name), "native-directory-lease-%u", mode);
        struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = mode,
            .lease_key = { 0x81, 0x16 }, .lease_epoch = 120 };
        lease.lease_key[2] = mode;
        struct smb2_create_out first, joined;
        unsigned int granted = (mode & SMB2_LEASE_READ) ?
            mode & (SMB2_LEASE_READ | SMB2_LEASE_HANDLE) : 0;
        arm(1, 0);
        if (mode == 7) {
            /* A real mkdir is held only for acceptance, never rejected: no
             * backend rollback exists. FileId and fresh grant stay private. */
            reject_first = 0; atomic_store(&finishes, 0); atomic_store(&hold_next, 1);
            int replies = c->nreply_app;
            smb2_create_post_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease);
            uint64_t deadline = smb2c_now_ms() + 10000;
            while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
            assert(atomic_load(&held) && c->nreply_app == replies);
            atomic_store(&release_finish, 1); smb2c_wait(c);
            smb2c_parse_create(c, &first);
            assert(first.status == ST_SUCCESS && atomic_load(&finishes) == 1);
        } else {
            assert(smb2_create_opts(c, name, mode & 1 ? FILE_CREATE : FILE_OPEN_IF,
                FILE_ALL_ACCESS, FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &first) == ST_SUCCESS);
        }
        disarm(1);
        assert(first.has_lease && first.lease_state == granted &&
            first.lease_epoch == (granted ? 121 : 120) && first.end_of_file == 0);
        lease.lease_epoch = 900;
        arm(1, 0);
        assert(smb2_create_opts(c, name, FILE_OPEN_IF, FILE_READ_ATTRIBUTES,
            FILE_SHARE_RWD, 0, &lease, &joined) == ST_SUCCESS); disarm(1);
        assert(joined.has_lease && joined.lease_state == granted &&
            joined.lease_epoch == (granted ? first.lease_epoch : 900));
        assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
        assert(smb2_close(c, first.file_id) == ST_SUCCESS);
    }

    /* A prefix shares the directory CREATE run; suffix commands see only its
     * accepted grant and cached CLOSE remains terminal. */
    struct packet p = {0};
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x82, 0x16 }, .lease_epoch = 130 };
    query(&p, c, prefix);
    int size = smb2c_build_create(c, "native-directory-chain", FILE_CREATE,
        FILE_ALL_ACCESS, FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease);
    uint8_t *b = append(&p, c, SMB2_CREATE, size, false);
    memcpy(b, c->sbuf + 4 + SMB2_HDR_SIZE, size);
    query(&p, c, NULL);
    b = append(&p, c, SMB2_CLOSE, 24, true); p16(b, 0, 24); memset(b + 8, 0xff, 16);
    memcpy(c->sbuf + 4, p.data, p.length);
    arm(3, 1); smb2c_send(c, p.length - SMB2_HDR_SIZE); c->msg_id = p.mid + p.count;
    smb2c_wait(c); disarm(2);
    const uint8_t *h = c->rbuf + 4;
    for (unsigned int i = 0; i < p.count; i++) {
        assert(g64(h, 24) == p.mid + i && g32(h, 8) == ST_SUCCESS);
        if (i == 1) assert(h[SMB2_HDR_SIZE + 2] == SMB2_OPLOCK_LEVEL_LEASE);
        uint32_t next = g32(h, 20); assert(!!next == (i + 1 < p.count));
        if (next) h += next;
    }

    /* Existing directory OPEN is read-only: reject finish once while checking
     * old membership/epoch and hidden FileId on both attempts. */
    struct smb2_create_out holder, joined, child, rejected;
    lease.lease_key[0] = 0x83;
    lease.lease_state = SMB2_LEASE_READ;
    assert(smb2_create_opts(c, "native-directory-retry", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &holder) == ST_SUCCESS);
    lease.lease_state |= SMB2_LEASE_HANDLE | SMB2_LEASE_WRITE;
    expected_lease_members = 1; expected_lease_epoch = holder.lease_epoch;
    expected_lease_state = holder.lease_state;
    reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(1, 0);
    int replies = c->nreply_app;
    smb2_create_post_opts(c, "native-directory-retry", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held) && c->nreply_app == replies && !smb2_conn_nbreaks(c));
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 2); reject_first = 0;
    expected_lease_members = expected_lease_epoch = expected_lease_state = 0;
    smb2c_parse_create(c, &joined);
    assert(joined.status == ST_SUCCESS &&
        joined.lease_state == (SMB2_LEASE_READ | SMB2_LEASE_HANDLE) &&
        joined.lease_epoch == holder.lease_epoch + 1);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);

    /* A wrong-key directory CREATE must not leave a new namespace entry. */
    arm(1, 0);
    assert(smb2_create_opts(c, "native-directory-wrong-key", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &rejected) == ST_INVALID_PARAMETER);
    disarm(1);
    assert(smb2_create_opts(c, "native-directory-wrong-key", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);

    /* Child creation must recall the directory grant (DIR_CONTENT, not the
     * regular file WRITE trigger). A settled lease re-arms on a later join. */
    assert(smb2_create(peer, "native-directory-retry\\child", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    deadline = smb2c_now_ms() + 10000;
    while (!smb2_conn_nbreaks(c) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    struct smb2_break brk;
    assert(smb2_conn_pop_break(c, &brk) && brk.is_lease && !brk.new_state);
    assert(!memcmp(brk.lease_key, lease.lease_key, sizeof(lease.lease_key)));
    if (brk.ack_required) assert(smb2_lease_break_ack(c, brk.lease_key, brk.new_state) == ST_SUCCESS);
    arm(1, 0);
    assert(smb2_create_opts(c, "native-directory-retry", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &joined) == ST_SUCCESS); disarm(1);
    assert(joined.lease_state == (SMB2_LEASE_READ | SMB2_LEASE_HANDLE) &&
        joined.lease_epoch == brk.new_epoch + 1);
    assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
    assert(smb2_close(peer, child.file_id) == ST_SUCCESS);
    assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
}

/* Unsupported directory policies must retain the existing legacy decision.
 * Explicit DIRECTORY_FILE is rejected by native eligibility; hintless OPEN
 * first resolves type in one native group and defers without mutation. */
static void directory_lease_fallback(struct smb2_conn *c, const char *name, bool v1)
{
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x85, 0x16 }, .force_v1 = v1 };
    struct smb2_create_out directory;
    arm(0, 0);
    assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
        FILE_DIRECTORY_FILE, &lease, &directory) == ST_SUCCESS); disarm(0);
    assert(directory.oplock == SMB2_OPLOCK_LEVEL_NONE && !directory.lease_state);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    arm(1, 0);
    assert(smb2_create_opts(c, name, FILE_OPEN, FILE_READ_ATTRIBUTES, FILE_SHARE_RWD,
        0, &lease, &directory) == ST_SUCCESS); disarm(1);
    assert(directory.oplock == SMB2_OPLOCK_LEVEL_NONE && !directory.lease_state);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
}

static void directory_parent_key_compound(struct smb2_conn *c, struct smb2_conn *peer)
{
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_HANDLE, .lease_key = { 0x86, 0x18 } };
    /* Same bytes on another client must not share the parent exemption. */
    struct smb2_oplock_req other = lease;
    struct smb2_create_out parent, observer, child;
    assert(smb2_create_opts(c, "parent-key-native", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &parent) == ST_SUCCESS);
    assert(smb2_create_opts(peer, "parent-key-native", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &other, &observer) == ST_SUCCESS);
    for (unsigned int directory = 0; directory < 2; directory++) {
        struct smb2_oplock_req child_lease = lease; child_lease.lease_key[2] = 2 + directory;
        int size = smb2c_build_create(c,
            directory ? "parent-key-native\\directory" : "parent-key-native\\file", FILE_CREATE,
            FILE_ALL_ACCESS, FILE_SHARE_RWD,
            directory ? FILE_DIRECTORY_FILE : FILE_NON_DIRECTORY_FILE, &child_lease);
        uint8_t *header = c->sbuf + 4;
        uint8_t *ctx = header + g32(header + SMB2_HDR_SIZE, 48);
        assert(g32(ctx, 12) == 52);
        uint8_t *data = ctx + g16(ctx, 10);
        p32(data, 20, 4); /* SMB2_LEASE_FLAG_PARENT_LEASE_KEY_SET */
        memcpy(data + 32, lease.lease_key, 16);
        atomic_store(&hold_next, 1); atomic_store(&finishes, 0); arm(1, 0);
        smb2c_send(c, size);
        uint64_t deadline = smb2c_now_ms() + 10000;
        while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
        assert(atomic_load(&held) && !smb2_conn_nbreaks(c) && !smb2_conn_nbreaks(peer));
        atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
        smb2c_parse_create(c, &child);
        assert(child.status == ST_SUCCESS && child.has_lease &&
            child.lease_state == child_lease.lease_state && (child.lease_flags & 4));
        const struct smb2_rsp_ctx *reply = smb2c_create_ctx_find(&child, "RqLs");
        assert(reply && reply->data_len == 52 && !memcmp(reply->data + 32, lease.lease_key, 16));
        deadline = smb2c_now_ms() + 10000;
        while (!smb2_conn_nbreaks(peer) && smb2c_now_ms() < deadline) smb2_pump(c->env);
        struct smb2_break brk;
        assert(smb2_conn_pop_break(peer, &brk) && brk.is_lease && !brk.new_state);
        if (brk.ack_required) assert(smb2_lease_break_ack(peer, brk.lease_key, brk.new_state) == ST_SUCCESS);
        assert(!smb2_conn_nbreaks(c));
        assert(smb2_close(c, child.file_id) == ST_SUCCESS);
        assert(smb2_close(peer, observer.file_id) == ST_SUCCESS);
        if (!directory) {
            assert(smb2_create_opts(peer, "parent-key-native", FILE_OPEN, FILE_READ_ATTRIBUTES,
                FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &other, &observer) == ST_SUCCESS);
        }
    }
    assert(smb2_close(c, parent.file_id) == ST_SUCCESS);
}

/* Replacement LINK and durable DOC intentionally cross the legacy boundary.
 * Their VFS notifications must retain the actor's client identity as well as
 * ParentLeaseKey, even when another client chose the same key bytes. */
static void legacy_namespace_parent_key(struct smb2_conn *c, struct smb2_conn *peer)
{
    for (unsigned int doc = 0; doc < 2; doc++) {
        char dirname[64], source[80], target[80];
        snprintf(dirname, sizeof(dirname), "legacy-parent-key-%u", doc);
        snprintf(source, sizeof(source), "%s\\source", dirname);
        snprintf(target, sizeof(target), "%s\\target", dirname);
        struct smb2_create_out setup, child, parent, observer;
        assert(smb2_create_opts(c, dirname, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, NULL, &setup) == ST_SUCCESS);
        assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
        if (!doc) {
            assert(smb2_create(c, target, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                NULL, &setup) == ST_SUCCESS);
            assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
        }
        struct smb2_oplock_req lease = { .is_lease = 1,
            .lease_state = SMB2_LEASE_READ | SMB2_LEASE_HANDLE,
            .lease_key = { 0xe1, 0x19 } };
        lease.lease_key[2] = doc;
        struct smb2_oplock_req child_lease = lease;
        child_lease.lease_key[3] = 1;
        if (!doc) child_lease.lease_state = 0;
        struct smb2_durable_req durable = { .dh2q = 1, .create_guid = { 0xe2, 0x19 } };
        struct smb2_cctx contexts[4]; uint8_t scratch[100];
        int nctx = smb2c_durable_contexts(doc ? &durable : NULL, scratch, contexts);
        int size = smb2c_build_create_full(c, source, FILE_CREATE, FILE_ALL_ACCESS,
            FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE | (doc ? FILE_DELETE_ON_CLOSE : 0),
            &child_lease, contexts, nctx);
        uint8_t *header = c->sbuf + 4;
        uint8_t *ctx = header + g32(header + SMB2_HDR_SIZE, 48);
        for (;;) {
            if (g16(ctx, 6) == 4 && !memcmp(ctx + g16(ctx, 4), "RqLs", 4)) break;
            assert(g32(ctx, 0)); ctx += g32(ctx, 0);
        }
        assert(g32(ctx, 12) == 52);
        uint8_t *data = ctx + g16(ctx, 10);
        p32(data, 20, 4); memcpy(data + 32, lease.lease_key, 16);
        smb2c_send(c, size); smb2c_wait(c); smb2c_parse_create(c, &child);
        assert(child.status == ST_SUCCESS && child.has_lease && (child.lease_flags & 4));
        if (doc) assert(smb2c_create_ctx_find(&child, "DH2Q"));
        assert(smb2_create_opts(c, dirname, FILE_OPEN, FILE_READ_ATTRIBUTES, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, &lease, &parent) == ST_SUCCESS);
        assert(smb2_create_opts(peer, dirname, FILE_OPEN, FILE_READ_ATTRIBUTES, FILE_SHARE_RWD,
            FILE_DIRECTORY_FILE, &lease, &observer) == ST_SUCCESS);
        assert(parent.lease_state == lease.lease_state && observer.lease_state == lease.lease_state);
        assert(!smb2_conn_nbreaks(c) && !smb2_conn_nbreaks(peer));
        if (doc) {
            assert(smb2_close(c, child.file_id) == ST_SUCCESS);
        } else {
            uint8_t link[256] = {1}; size_t len = strlen(target);
            assert(20 + len * 2 <= sizeof(link)); p32(link, 16, len * 2);
            for (size_t i = 0; i < len; i++) p16(link, 20 + i * 2, target[i]);
            assert(smb2_set_info(c, SMB2_INFO_FILE_T, 0x0b, child.file_id,
                link, 20 + len * 2) == ST_SUCCESS);
        }
        uint64_t deadline = smb2c_now_ms() + 10000;
        while (!smb2_conn_nbreaks(peer) && smb2c_now_ms() < deadline) smb2_pump(c->env);
        struct smb2_break brk;
        assert(smb2_conn_pop_break(peer, &brk) && brk.is_lease && !brk.new_state);
        assert(!memcmp(brk.lease_key, lease.lease_key, 16));
        if (brk.ack_required) assert(smb2_lease_break_ack(peer, brk.lease_key, brk.new_state) == ST_SUCCESS);
        assert(!smb2_conn_nbreaks(c));
        if (!doc) assert(smb2_close(c, child.file_id) == ST_SUCCESS);
        assert(smb2_close(peer, observer.file_id) == ST_SUCCESS);
        assert(smb2_close(c, parent.file_id) == ST_SUCCESS);
    }
}

/* A same-client second session must see an in-flight reservation before its
 * first FileId is public, then the surviving binding after accepted finish.
 * Different clients keep independent key namespaces. */
static void client_lease_key_cases(struct smb2_conn *c, struct smb2_conn *peer)
{
    struct smb2_conn *sibling = smb2_conn_reopen(c->env, c);
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x91, 0x18 } };
    struct smb2_create_out first, second, rejected;
    assert(smb2_create(c, "global-key-first", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &first) == ST_SUCCESS);
    assert(smb2_close(c, first.file_id) == ST_SUCCESS);
    atomic_store(&hold_next, 1); atomic_store(&finishes, 0); arm(1, 0);
    smb2_create_post(c, "global-key-first", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD, &lease);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(atomic_load(&held)); disarm(1);
    int replies = sibling->nreply_app;
    unsigned int pending = smb_lease_key_pending_count();
    smb2_create_post(sibling, "global-key-rejected", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, &lease);
    /* Pump while finish is held; the second CREATE must not publish a FileId
     * before the constructor holding its client/key reservation is accepted. */
    deadline = smb2c_now_ms() + 10000;
    while (smb_lease_key_pending_count() == pending && smb2c_now_ms() < deadline) smb2_pump(c->env);
    assert(smb_lease_key_pending_count() > pending && sibling->nreply_app == replies);
    atomic_store(&release_finish, 1); smb2c_wait(c); smb2c_parse_create(c, &first);
    assert(first.status == ST_SUCCESS && first.lease_state == lease.lease_state);
    smb2c_wait(sibling); smb2c_parse_create(sibling, &rejected);
    assert(rejected.status == ST_INVALID_PARAMETER);
    assert(smb2_create(sibling, "global-key-rejected", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(sibling, "global-key-first", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &lease, &second) == ST_SUCCESS && second.lease_epoch == first.lease_epoch);
    assert(smb2_close(sibling, second.file_id) == ST_SUCCESS);
    assert(smb2_create(peer, "global-key-other-client", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &second) == ST_SUCCESS);
    assert(smb2_close(peer, second.file_id) == ST_SUCCESS);

    /* ACK on a same-client session which owns no member must find and settle
     * the original session's grant. Otherwise this data OPEN stalls. */
    smb2_create_post(peer, "global-key-first", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD, NULL);
    deadline = smb2c_now_ms() + 10000;
    while (!smb2_conn_nbreaks(c) && smb2c_now_ms() < deadline) smb2_pump(c->env);
    struct smb2_break brk;
    assert(smb2_conn_pop_break(c, &brk) && brk.is_lease && brk.ack_required);
    assert(smb2_lease_break_ack(sibling, brk.lease_key, brk.new_state) == ST_SUCCESS);
    smb2c_wait(peer); smb2c_parse_create(peer, &second);
    assert(second.status == ST_SUCCESS);
    assert(smb2_close(peer, second.file_id) == ST_SUCCESS);
    assert(smb2_close(c, first.file_id) == ST_SUCCESS);
    /* Last retirement releases the key for a different file. */
    assert(smb2_create(sibling, "global-key-rejected", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &second) == ST_SUCCESS);
    assert(smb2_close(sibling, second.file_id) == ST_SUCCESS);
}

/* Existing-key CREATE must never recreate a leaf removed by another
 * frontend after lookup. Exercise both native and forced-legacy constructors. */
static void bound_lease_disappearing_name(struct smb2_conn *c)
{
    for (unsigned int legacy = 0; legacy < 2; legacy++) {
        for (unsigned int directory = 0; directory < 2; directory++) {
            char name[80]; snprintf(name, sizeof(name), "bound-key-disappears-%u-%u", legacy, directory);
            struct smb2_oplock_req lease = { .is_lease = 1, .lease_key = { 0xa1, 0x18 } };
            lease.lease_key[2] = legacy; lease.lease_key[3] = directory;
            struct smb2_durable_req dur = { .dh2q = 1, .create_guid = { 0xa2, 0x18 } };
            dur.create_guid[2] = legacy; dur.create_guid[3] = directory;
            struct smb2_create_out holder, opened;
            unsigned int options = directory ? FILE_DIRECTORY_FILE : FILE_NON_DIRECTORY_FILE;
            assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, options, &lease, &holder) == ST_SUCCESS && holder.has_lease);
            assert(smb2_create_dur_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                options, &lease, legacy ? &dur : NULL, &opened) == ST_OBJECT_NAME_COLLISION);
            smb_lease_unlink_after_lookup(name);
            assert(smb2_create_dur_opts(c, name, FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                options, &lease, legacy ? &dur : NULL, &opened) == ST_INVALID_PARAMETER);
            assert(smb_lease_unlink_count() == 1);
            assert(smb2_create_opts(c, name, FILE_OPEN, FILE_READ_ATTRIBUTES,
                FILE_SHARE_RWD, options, NULL, &opened) == ST_OBJECT_NAME_NOT_FOUND);
            assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
        }
    }
}

/* Nonlease durable reconnect permits another ClientGuid. It still owns the
 * original canonical ACCESS/RANGE/cache identities: self-I/O must neither
 * conflict with its surviving lock nor recall its surviving BATCH grant. */
static void cross_client_durable_io(struct smb2_env *env)
{
    struct smb2_conn *original = smb2_conn_open(env);
    struct smb2_conn *reclaimed = smb2_conn_open(env);
    smb2_handshake(original); smb2_handshake(reclaimed);
    assert(original->guid_tag != reclaimed->guid_tag);
    struct smb2_oplock_req batch = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    struct smb2_durable_req durable = { .dh2q = 1, .create_guid = { 0xc1, 0x18 } };
    struct smb2_create_out first, reopened;
    assert(smb2_create_dur(original, "durable-cross-client-actor", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &batch, &durable, &first) == ST_SUCCESS);
    assert(first.has_dh2q && first.oplock == SMB2_OPLOCK_LEVEL_BATCH);
    uint32_t length;
    assert(smb2_write(original, first.file_id, 0, "seed", 4, &length) == ST_SUCCESS && length == 4);
    assert(smb2_lock(original, first.file_id, 0, 4,
        SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) == ST_SUCCESS);
    smb2_conn_disconnect(original); smb2_quiesce(env);
    struct smb2_durable_req reconnect = { .dh2c = 1 };
    memcpy(reconnect.create_guid, durable.create_guid, 16);
    memcpy(reconnect.file_id, first.file_id, 16);
    assert(smb2_create_dur(reclaimed, "", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &reconnect, &reopened) == ST_SUCCESS);
    assert(reopened.oplock == SMB2_OPLOCK_LEVEL_BATCH && !memcmp(reopened.file_id, first.file_id, 16));
    uint8_t data[4];
    arm(1, 0);
    assert(smb2_read(reclaimed, reopened.file_id, 0, sizeof(data), data, &length) == ST_SUCCESS &&
        length == 4 && !memcmp(data, "seed", 4)); disarm(1);
    arm(1, 0);
    assert(smb2_write(reclaimed, reopened.file_id, 0, "live", 4, &length) == ST_SUCCESS && length == 4);
    disarm(1);
    smb2_quiesce(env);
    assert(!smb2_conn_nbreaks(reclaimed));
    assert(smb2_lock(reclaimed, reopened.file_id, 0, 4, SMB2_LOCKFLAG_UNLOCK) == ST_SUCCESS);
    assert(smb2_close(reclaimed, reopened.file_id) == ST_SUCCESS);
}

static void directory_disabled_policy(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = 1, .directory_leases = 0 };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *c = smb2_conn_open(&env);
    smb2_handshake(c);
    directory_lease_fallback(c, "directory-leasing-disabled", false);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
}

/* Policy variants need separate server instances because these options are
 * fixed at server/share construction. Both still dispatch native compounds. */
static void regular_lease_policy_cases(bool enabled)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = enabled,
        .force_level2 = enabled, .directory_leases = 1 };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *c = smb2_conn_open(&env);
    smb2_handshake(c);
    const unsigned int modes[] = { SMB2_LEASE_READ | SMB2_LEASE_HANDLE,
        SMB2_LEASE_READ | SMB2_LEASE_WRITE,
        SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE };
    for (unsigned int v1 = 0; v1 < 2; v1++) {
        for (unsigned int i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
            char name[64]; snprintf(name, sizeof(name), "lease-policy-%u-%u", v1, i);
            struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = modes[i],
                .lease_key = { 0x76, 0x14 }, .lease_epoch = 90, .force_v1 = v1 };
            lease.lease_key[2] = v1; lease.lease_key[3] = i;
            struct smb2_create_out first, joined;
            arm(1, 0);
            assert(smb2_create(c, name, FILE_CREATE, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, &lease, &first) == ST_SUCCESS); disarm(1);
            assert(first.has_lease && first.lease_state == (enabled ? SMB2_LEASE_READ : 0));
            assert(first.lease_epoch == (v1 ? 0 : enabled ? 91 : 90));
            lease.lease_epoch = 900;
            arm(1, 0);
            assert(smb2_create(c, name, FILE_OPEN, FILE_READ_ATTRIBUTES,
                FILE_SHARE_RWD, &lease, &joined) == ST_SUCCESS); disarm(1);
            assert(joined.has_lease && joined.lease_state == first.lease_state);
            /* An established READ lease preserves its epoch; a disabled
             * lease has no grant and echoes each request's epoch. */
            assert(joined.lease_epoch == (v1 ? 0 : enabled ? first.lease_epoch : 900));
            assert(!smb2_conn_nbreaks(c));
            assert(smb2_close(c, joined.file_id) == ST_SUCCESS);
            assert(smb2_close(c, first.file_id) == ST_SUCCESS);
        }
    }
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE,
        .lease_key = { 0x84, 0x16 }, .lease_epoch = 140 };
    struct smb2_create_out directory;
    arm(1, 0);
    assert(smb2_create_opts(c, "directory-lease-policy", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &lease, &directory) == ST_SUCCESS); disarm(1);
    assert(directory.has_lease && directory.lease_state == (enabled ? SMB2_LEASE_READ : 0));
    assert(directory.lease_epoch == (enabled ? 141 : 140));
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
}

/* Persistence is granted only where a successful backend record exists.
 * Unsupported create routes may still grant ordinary cache-backed durability. */
static void persistent_create_truth_cases(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = 1, .directory_leases = 1,
        .named_streams = 1, .persistent_handles = 1, .continuous_availability = 1 };
    smb2_env_start_opts(&env, &opts);
    unsigned int expected_checks = smb_persistent_create_checks();
    for (unsigned int fixture = 0; fixture < 4; fixture++) {
        for (unsigned int cached = 0; cached < 2; cached++) {
            struct smb2_conn *c = smb2_conn_open(&env);
            smb2_handshake(c);
            char base[80], name[96];
            snprintf(base, sizeof(base), "persistent-unsupported-%u-%u", fixture, cached);
            bool stream = fixture >= 2;
            bool base_directory = fixture == 1 || fixture == 3;
            struct smb2_create_out setup, created, reopened;
            if (fixture != 0) {
                assert(smb2_create_opts(c, base, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                    base_directory ? FILE_DIRECTORY_FILE : FILE_NON_DIRECTORY_FILE,
                    NULL, &setup) == ST_SUCCESS);
                assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
            }
            snprintf(name, sizeof(name), stream ? "%s:fork" : "%s", base);
            struct smb2_oplock_req lease = { .is_lease = 1,
                .lease_state = SMB2_LEASE_READ | SMB2_LEASE_HANDLE,
                .lease_key = { 0x91, 0x17 } };
            lease.lease_key[2] = fixture; lease.lease_key[3] = cached;
            struct smb2_durable_req dur = { .dh2q = 1,
                .flags = SMB2_DHANDLE_FLAG_PERSISTENT, .create_guid = { 0x92, 0x17 } };
            dur.create_guid[2] = fixture; dur.create_guid[3] = cached;
            smb_persistent_create_expect(false, false); expected_checks++;
            assert(smb2_create_dur_opts(c, name, fixture == 1 ? FILE_OPEN_IF : FILE_CREATE,
                FILE_ALL_ACCESS, FILE_SHARE_READ | FILE_SHARE_WRITE,
                stream ? FILE_NON_DIRECTORY_FILE : FILE_DIRECTORY_FILE,
                cached ? &lease : NULL, &dur, &created) == ST_SUCCESS);
            assert(!(created.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT));
            assert(created.has_dh2q == !!cached);
            if (!cached) {
                assert(smb2_close(c, created.file_id) == ST_SUCCESS); continue;
            }
            uint32_t bytes;
            if (stream) assert(smb2_write(c, created.file_id, 0, "fork", 4, &bytes) == ST_SUCCESS);
            smb2_conn_disconnect(c); smb2_quiesce(&env);
            struct smb2_conn *reconnect = smb2_conn_reopen(&env, c);
            /* The binding belongs to the parked open, including a named fork,
             * not the disconnected session. A wrong-key new name must remain
             * absent while that durable handle can still be reclaimed. */
            char wrong_name[112]; snprintf(wrong_name, sizeof(wrong_name), "%s-wrong-key", base);
            assert(smb2_create(reconnect, wrong_name, FILE_CREATE, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, &lease, &setup) == ST_INVALID_PARAMETER);
            assert(smb2_create(reconnect, wrong_name, FILE_OPEN, FILE_READ_ATTRIBUTES,
                FILE_SHARE_RWD, NULL, &setup) == ST_OBJECT_NAME_NOT_FOUND);
            struct smb2_durable_req claim = { .dh2c = 1,
                .reconnect_flags = SMB2_DHANDLE_FLAG_PERSISTENT };
            memcpy(claim.file_id, created.file_id, sizeof(claim.file_id));
            memcpy(claim.create_guid, dur.create_guid, sizeof(claim.create_guid));
            assert(smb2_create_dur(reconnect, "", FILE_OPEN, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, &lease, &claim, &reopened) == ST_INVALID_PARAMETER);
            claim.reconnect_flags = 0;
            assert(smb2_create_dur(reconnect, "", FILE_OPEN, FILE_ALL_ACCESS,
                FILE_SHARE_RWD, &lease, &claim, &reopened) == ST_SUCCESS);
            assert(!memcmp(reopened.file_id, created.file_id, sizeof(created.file_id)));
            if (stream) {
                uint8_t data[4];
                assert(smb2_read(reconnect, reopened.file_id, 0, sizeof(data), data, &bytes) == ST_SUCCESS &&
                    bytes == sizeof(data) && !memcmp(data, "fork", sizeof(data)));
            }
            assert(smb2_close(reconnect, reopened.file_id) == ST_SUCCESS);
            /* Stream CLOSE/reconnect must drain the retained base ACCESS owner. */
            assert(smb2_create_opts(reconnect, base, FILE_OPEN, FILE_ALL_ACCESS, 0,
                fixture == 2 ? FILE_NON_DIRECTORY_FILE : FILE_DIRECTORY_FILE,
                NULL, &setup) == ST_SUCCESS);
            assert(smb2_close(reconnect, setup.file_id) == ST_SUCCESS);
        }
    }
    struct smb2_conn *c = smb2_conn_open(&env);
    smb2_handshake(c);
    struct smb2_create_out created, setup;
    struct smb2_durable_req dur = { .dh2q = 1, .flags = SMB2_DHANDLE_FLAG_PERSISTENT,
        .create_guid = { 0x93, 0x17 } };
    for (unsigned int directory = 0; directory < 2; directory++) {
        const char *name = directory ? "persistent-record-directory" : "persistent-record-regular";
        if (directory) {
            assert(smb2_create_opts(c, name, FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                FILE_DIRECTORY_FILE, NULL, &setup) == ST_SUCCESS);
            assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
        }
        dur.create_guid[2] = directory;
        smb_persistent_create_expect(true, false); expected_checks++;
        assert(smb2_create_dur_opts(c, name, directory ? FILE_OPEN : FILE_CREATE,
            FILE_ALL_ACCESS, FILE_SHARE_RWD, directory ? FILE_DIRECTORY_FILE : FILE_NON_DIRECTORY_FILE,
            NULL, &dur, &created) == ST_SUCCESS);
        assert(created.has_dh2q && (created.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT));
        assert(smb2_close(c, created.file_id) == ST_SUCCESS);
    }

    /* The default-KV error follows a successful filesystem CREATE. Return an
     * error with no recovery record, retain the new name and release its handle. */
    dur.create_guid[2] = 2;
    smb_persistent_create_expect(false, true); expected_checks++;
    assert(smb2_create_dur(c, "persistent-put-failure", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &dur, &created) == 0xc0000185u); /* STATUS_IO_DEVICE_ERROR */
    assert(smb2_create(c, "persistent-put-failure", FILE_OPEN, FILE_ALL_ACCESS, 0,
        NULL, &setup) == ST_SUCCESS);
    assert(smb2_close(c, setup.file_id) == ST_SUCCESS);

    dur.create_guid[2] = 4;
    smb_persistent_create_expect(false, 2); expected_checks++;
    assert(smb2_create_dur(c, "persistent-ambiguous-put", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &dur, &created) == 0xc0000185u); /* STATUS_IO_DEVICE_ERROR */
    assert(smb2_create(c, "persistent-ambiguous-put", FILE_OPEN, FILE_ALL_ACCESS, 0,
        NULL, &setup) == ST_SUCCESS);
    assert(smb2_close(c, setup.file_id) == ST_SUCCESS);

    /* A successful record write followed by failed SMB share admission must
     * remove that unpublished record before replying with the original error. */
    assert(smb2_create(c, "persistent-share-denied", FILE_CREATE, FILE_ALL_ACCESS, 0,
        NULL, &setup) == ST_SUCCESS);
    dur.create_guid[2] = 3;
    smb_persistent_create_expect(false, false); expected_checks++;
    assert(smb2_create_dur(c, "persistent-share-denied", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &dur, &created) == ST_SHARING_VIOLATION);
    assert(smb2_close(c, setup.file_id) == ST_SUCCESS);
    assert(smb_persistent_create_checks() == expected_checks);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
}

int main(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = 1, .persistent_handles = 1, .directory_leases = 1 };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *c = smb2_conn_open(&env), *peer = smb2_conn_open(&env);
    smb2_handshake(c); smb2_handshake(peer);
    struct smb2_create_out prefix, opened, holder;
    assert(smb2_create(c, "cache-create-prefix", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &prefix) == ST_SUCCESS);
    const uint8_t levels[] = { SMB2_OPLOCK_LEVEL_II, SMB2_OPLOCK_LEVEL_EXCLUSIVE, SMB2_OPLOCK_LEVEL_BATCH };
    for (unsigned int i = 0; i < sizeof(levels); i++) {
        char name[64]; snprintf(name, sizeof(name), "cache-create-%u", i);
        create_chain(c, prefix.file_id, name, levels[i]);
    }
    /* Reject only a read-only existing OPEN. No filesystem mutation is undone.
     * Before both attempts finish, FileId/grant/member remain unpublished. */
    struct smb2_oplock_req req = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    assert(smb2_create(c, "cache-retry", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &opened) == ST_SUCCESS);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    int replies = c->nreply_app;
    reject_first = 1; atomic_store(&finishes, 0); atomic_store(&hold_next, 1); arm(1, 0);
    smb2_create_post(c, "cache-retry", FILE_OPEN, FILE_READ_ATTRIBUTES, FILE_SHARE_RWD, &req);
    uint64_t deadline = smb2c_now_ms() + 10000;
    while (!atomic_load(&held) && smb2c_now_ms() < deadline) smb2_pump(&env);
    assert(atomic_load(&held) && c->nreply_app == replies);
    atomic_store(&release_finish, 1); smb2c_wait(c); disarm(1);
    assert(atomic_load(&finishes) == 2);
    smb2c_parse_create(c, &opened); assert(opened.status == ST_SUCCESS && opened.oplock == req.level);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS); reject_first = 0;
    /* A peer reader permits at most II; a peer write cache blocks even II for
     * a transparent stat-open. Opportunistic admission never sends a break. */
    req.level = SMB2_OPLOCK_LEVEL_II;
    assert(smb2_create(peer, "cache-cap", FILE_CREATE, FILE_READ_ACCESS,
        FILE_SHARE_RWD, &req, &holder) == ST_SUCCESS && holder.oplock == req.level);
    req.level = SMB2_OPLOCK_LEVEL_BATCH; int breaks = smb2_conn_nbreaks(peer);
    arm(1, 0);
    assert(smb2_create(c, "cache-cap", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &req, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_II && smb2_conn_nbreaks(peer) == breaks);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS); assert(smb2_close(peer, holder.file_id) == ST_SUCCESS);
    assert(smb2_create(peer, "cache-cap", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &req, &holder) == ST_SUCCESS && holder.oplock == req.level);
    breaks = smb2_conn_nbreaks(peer); arm(1, 0);
    assert(smb2_create(c, "cache-cap", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &req, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_NONE && smb2_conn_nbreaks(peer) == breaks);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS); assert(smb2_close(peer, holder.file_id) == ST_SUCCESS);
    /* A same-client HANDLE lease excludes legacy oplocks even when the
     * ordinary conflict matrix would otherwise allow a shared READ grant. */
    struct smb2_oplock_req lease = { .is_lease = 1,
        .lease_state = SMB2_LEASE_READ | SMB2_LEASE_HANDLE,
        .lease_key = { 0x63, 0x12 } };
    assert(smb2_create(c, "same-client-cache", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &holder) == ST_SUCCESS);
    assert(holder.has_lease && (holder.lease_state & SMB2_LEASE_HANDLE));
    breaks = smb2_conn_nbreaks(c); arm(1, 0);
    assert(smb2_create(c, "same-client-cache", FILE_OPEN, FILE_READ_ATTRIBUTES,
        FILE_SHARE_RWD, &req, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_NONE && smb2_conn_nbreaks(c) == breaks);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS); assert(smb2_close(c, holder.file_id) == ST_SUCCESS);
    overwrite_client_cap(c);
    /* V1 durable requests without BATCH caching have no replay identity and
     * are declined in the native path, with no durable response context. */
    struct smb2_durable_req dur = { .dhnq = 1 };
    req.level = SMB2_OPLOCK_LEVEL_II; arm(1, 0);
    assert(smb2_create_dur(c, "declined-dhnq", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &req, &dur, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_II && !opened.has_dhnq);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    /* Directories refuse legacy oplocks and can remain ordinary compounds. */
    req.level = SMB2_OPLOCK_LEVEL_BATCH; arm(1, 0);
    assert(smb2_create_opts(c, "directory-oplock", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, &req, &opened) == ST_SUCCESS); disarm(1);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_NONE);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    /* A real durable-v1 grant still takes the explicit legacy boundary. */
    arm(0, 0);
    assert(smb2_create_dur(c, "actual-durable", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &req, &dur, &opened) == ST_SUCCESS); disarm(0);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_BATCH && opened.has_dhnq);
    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    caching_create_suffixes(c, prefix.file_id);
    read_lease_cases(c, peer, prefix.file_id);
    regular_lease_cases(c, peer);
    hintless_and_overwrite_cases(c);
    directory_lease_cases(c, peer, prefix.file_id);
    directory_lease_fallback(c, "directory-lease-v1", true);
    directory_parent_key_compound(c, peer);
    legacy_namespace_parent_key(c, peer);
    client_lease_key_cases(c, peer);
    bound_lease_disappearing_name(c);
    cross_client_durable_io(&env);
    assert(smb2_close(c, prefix.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    regular_lease_policy_cases(true);
    regular_lease_policy_cases(false);
    directory_disabled_policy();
    persistent_create_truth_cases();
    return 0;
}
