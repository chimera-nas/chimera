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
#include "common/compound_retry.h"

#define ST_INSUFFICIENT_RESOURCES 0xC000009Au
#define ST_RANGE_NOT_LOCKED      0xC000007Eu

extern atomic_int lock_test_arm, lock_test_seen, lock_test_claims, lock_test_rehomed;
void lock_test_inspect(struct chimera_vfs_compound *compound);
static atomic_int armed, submissions, attempts, last_execution;
static unsigned reject_count, expected_groups;
struct injection { chimera_vfs_compound_callback_t callback; void *private_data; unsigned seen; };
static void finish(struct chimera_vfs_compound *cp, void *private_data)
{
    struct injection *ctx = private_data;
    atomic_store(&last_execution, chimera_vfs_compound_execution_status(cp));
    atomic_store(&attempts, ++ctx->seen);
    chimera_vfs_compound_finish_result(cp, ctx->seen <= reject_count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}
static void complete(struct chimera_vfs_compound *cp, void *private_data)
{
    struct injection *ctx = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void *arg = ctx->private_data;
    lock_test_inspect(cp);
    if (chimera_vfs_compound_finish_status(cp) != CHIMERA_VFS_EAGAIN ||
        ctx->seen == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1) free(ctx);
    callback(cp, arg);
}
__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(struct chimera_vfs_compound *cp,
                            chimera_vfs_compound_callback_t callback, void *private_data)
{
    typedef void (*submit_fn)(struct chimera_vfs_compound *, chimera_vfs_compound_callback_t, void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    struct injection *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->callback = callback;
    ctx->private_data = private_data;
    if (!atomic_load(&armed)) { next(cp, complete, ctx); return; }
    assert(atomic_fetch_add(&submissions, 1) == 0);
    assert(chimera_vfs_compound_num_groups(cp) == expected_groups);
    if (reject_count) {
        for (unsigned i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
            unsigned type = chimera_vfs_compound_op(cp, i)->type;
            /* Only reads and journaled claims may be rejected: no pretend FS rollback. */
            assert(type != CHIMERA_VFS_COMPOUND_OP_WRITE && type != CHIMERA_VFS_COMPOUND_OP_COMMIT);
        }
    }
    chimera_vfs_compound_set_finish_handler(cp, finish, ctx);
    next(cp, complete, ctx);
}
struct command {
    unsigned opcode;
    const uint8_t *fid;
    uint64_t offset, length;
    uint32_t flags, status;
    const char *create_name;
    bool related;
};
static uint64_t send_commands(struct smb2_conn *c, const struct command *commands, unsigned count)
{
    uint8_t packet[2048] = {0};
    uint64_t mid = c->msg_id;
    unsigned size = 0;
    assert(!c->signing_on && !c->encrypt_on);
    for (unsigned i = 0; i < count; i++) {
        const struct command *cmd = &commands[i];
        uint8_t *h = packet + size, *b = h + SMB2_HDR_SIZE;
        unsigned body;
        memcpy(h, "\xfeSMB", 4);
        p16(h,4,64); p16(h,6,1); p16(h,12,cmd->opcode); p16(h,14,32);
        p64(h,24,mid+i); p32(h,36,cmd->related ? UINT32_MAX : c->tree_id);
        p64(h,40,cmd->related ? UINT64_MAX : c->session_id);
        if (cmd->related) p32(h,16,SMB2_FLAGS_RELATED_OPERATIONS);
        if (cmd->opcode == SMB2_CREATE) {
            unsigned length = strlen(cmd->create_name);
            p16(b,0,57); p32(b,4,2); p32(b,24,FILE_ALL_ACCESS);
            p32(b,32,FILE_SHARE_RWD); p32(b,36,FILE_CREATE); p32(b,40,FILE_NON_DIRECTORY_FILE);
            p16(b,44,120); p16(b,46,length*2);
            for (unsigned n=0;n<length;n++) p16(b,56+n*2,(uint8_t)cmd->create_name[n]);
            body=56+length*2;
        } else if (cmd->opcode == SMB2_LOCK) {
            p16(b,0,48); p16(b,2,1); memcpy(b+8,cmd->fid,16);
            p64(b,24,cmd->offset); p64(b,32,cmd->length); p32(b,40,cmd->flags); body=48;
        } else if (cmd->opcode == SMB2_WRITE) {
            p16(b,0,49); p16(b,2,112); p32(b,4,4); p64(b,8,cmd->offset);
            memcpy(b+16,cmd->fid,16); memcpy(b+48,"data",4); body=52;
        } else if (cmd->opcode == SMB2_QUERY_INFO) {
            p16(b,0,41); b[2]=SMB2_INFO_FILE_T; b[3]=SMB2_FILE_BASIC_INFO_T;
            p32(b,4,4096); memcpy(b+24,cmd->fid,16); body=40;
        } else {
            assert(cmd->opcode == SMB2_CLOSE);
            p16(b,0,24); memcpy(b+8,cmd->fid,16); body=24;
        }
        unsigned len = SMB2_HDR_SIZE + body;
        if (i+1<count) { len=(len+7)&~7u; p32(h,20,len); }
        size+=len; assert(size<=sizeof(packet));
    }
    memcpy(c->sbuf+4,packet,size);
    smb2c_send(c,size-SMB2_HDR_SIZE); c->msg_id=mid+count;
    return mid;
}
static void run(struct smb2_conn *c, const struct command *commands, unsigned count, unsigned rejects)
{
    atomic_store(&submissions,0); atomic_store(&attempts,0); reject_count=rejects;
    int before=c->nreply_app; expected_groups=count;
    atomic_store(&armed,1);
    uint64_t mid=send_commands(c,commands,count);
    smb2c_wait(c);
    atomic_store(&armed,0);
    assert(atomic_load(&submissions)==1 && atomic_load(&attempts)==rejects+1);
    assert(c->nreply_app==before+1);
    unsigned offset=4;
    for(unsigned i=0;i<count;i++) {
        const uint8_t *h=c->rbuf+offset;
        fprintf(stderr,"lock compound op %u status %08x expected %08x\n",commands[i].opcode,g32(h,8),commands[i].status);
        assert(g64(h,24)==mid+i && g32(h,8)==commands[i].status);
        unsigned next=g32(h,20);
        assert((i+1<count)==(next!=0)); offset+=next;
    }
}
static void wait_interim(struct smb2_conn *c, int before)
{
    uint64_t deadline=smb2c_now_ms()+SMB2C_HANG_MS;
    while(c->ninterim==before) { smb2_pump(c->env); assert(smb2c_now_ms()<deadline); }
}
static void blocking(struct smb2_conn *holder, struct smb2_conn *waiter,
                     const uint8_t *held, const uint8_t *wanted, unsigned mode)
{
    assert(smb2_lock(holder,held,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY)==ST_SUCCESS);
    struct command cmd={SMB2_LOCK,wanted,0,4,SMB2_LOCKFLAG_EXCLUSIVE,ST_SUCCESS};
    int interim=waiter->ninterim, replies=waiter->nreply_app;
    atomic_store(&submissions,0); atomic_store(&attempts,0); reject_count=0; expected_groups=1;
    atomic_store(&armed,1);
    send_commands(waiter,&cmd,1);
    wait_interim(waiter,interim);
    atomic_store(&armed,0);
    assert(atomic_load(&submissions)==1 && waiter->nreply_app==replies);
    if(mode==0) {
        assert(smb2_lock(holder,held,0,4,SMB2_LOCKFLAG_UNLOCK)==ST_SUCCESS);
    } else if(mode==1) {
        smb2c_post_cancel_async(waiter,waiter->last_async_id);
    } else {
        struct command close={SMB2_CLOSE,wanted,0,0,0,ST_SUCCESS};
        send_commands(waiter,&close,1);
    }
    uint64_t deadline=smb2c_now_ms()+SMB2C_HANG_MS;
    int expected=replies+(mode==2?2:1);
    while(waiter->nreply_app<expected) { smb2_pump(waiter->env); assert(smb2c_now_ms()<deadline); }
    assert(atomic_load(&attempts)==1);
    if(mode<2) assert(g32(waiter->rbuf+4,8)==(mode==0?ST_SUCCESS:ST_CANCELLED));
    else assert(atomic_load(&last_execution)==CHIMERA_VFS_EINTR);
    if(mode==0) assert(smb2_lock(waiter,wanted,0,4,SMB2_LOCKFLAG_UNLOCK)==ST_SUCCESS);
    else assert(smb2_lock(holder,held,0,4,SMB2_LOCKFLAG_UNLOCK)==ST_SUCCESS);
}
static void provisional(struct smb2_conn *c, struct smb2_conn *peer)
{
    uint8_t related[16]; memset(related,0xff,sizeof(related));
    struct command closed[]={
        {.opcode=SMB2_CREATE,.status=ST_SUCCESS,.create_name="private-lock-closed.txt"},
        {.opcode=SMB2_LOCK,.fid=related,.length=4,.flags=0,.status=ST_INVALID_PARAMETER,.related=true},
        {.opcode=SMB2_LOCK,.fid=related,.length=4,.flags=SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,.status=ST_SUCCESS,.related=true},
        {.opcode=SMB2_WRITE,.fid=related,.status=ST_SUCCESS,.related=true},
        {.opcode=SMB2_LOCK,.fid=related,.length=4,.flags=SMB2_LOCKFLAG_UNLOCK,.status=ST_SUCCESS,.related=true},
        {.opcode=SMB2_CLOSE,.fid=related,.status=ST_SUCCESS,.related=true},
    };
    run(c,closed,6,0); /* Invalid first LOCK must not strand token production. */
    struct command surviving[]={
        {.opcode=SMB2_CREATE,.status=ST_SUCCESS,.create_name="private-lock-survives.txt"},
        {.opcode=SMB2_LOCK,.fid=related,.length=4,.flags=SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,.status=ST_SUCCESS,.related=true},
    };
    run(c,surviving,2,0);
    struct smb2_create_out owner,other;
    smb2c_parse_create(c,&owner);
    assert(smb2_create(peer,"private-lock-survives.txt",FILE_OPEN,FILE_ALL_ACCESS,FILE_SHARE_RWD,NULL,&other)==ST_SUCCESS);
    assert(smb2_lock(peer,other.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY)==ST_LOCK_NOT_GRANTED);
    assert(smb2_lock(c,owner.file_id,0,4,SMB2_LOCKFLAG_UNLOCK)==ST_SUCCESS);
    assert(smb2_lock(peer,other.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY)==ST_SUCCESS);
    assert(smb2_close(c,owner.file_id)==ST_SUCCESS);
    assert(smb2_close(peer,other.file_id)==ST_SUCCESS);
    struct command retired[]={
        {.opcode=SMB2_CREATE,.status=ST_SUCCESS,.create_name="private-lock-retired.txt"},
        {.opcode=SMB2_LOCK,.fid=related,.length=4,.flags=SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,.status=ST_SUCCESS,.related=true},
        {.opcode=SMB2_CLOSE,.fid=related,.status=ST_SUCCESS,.related=true},
    };
    run(c,retired,3,0); /* CLOSE retires a still-held provisional acquisition. */
    assert(smb2_create(peer,"private-lock-retired.txt",FILE_OPEN,FILE_ALL_ACCESS,FILE_SHARE_RWD,NULL,&other)==ST_SUCCESS);
    assert(smb2_lock(peer,other.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY)==ST_SUCCESS);
    assert(smb2_close(peer,other.file_id)==ST_SUCCESS);
}

struct lock_element { uint64_t offset, length; uint32_t flags; };

static void
checked_ranges(struct smb2_conn *c, const uint8_t *fid,
               const struct lock_element *ranges, unsigned count,
               uint32_t sequence, uint32_t expected, unsigned remaining,
               bool fail_allocation, unsigned retries)
{
    int before = atomic_load(&lock_test_seen);
    atomic_store(&lock_test_claims, remaining);
    atomic_store(&lock_test_arm, fail_allocation ? 1 : 2);
    if (retries) {
        assert(!fail_allocation);
        atomic_store(&submissions, 0); atomic_store(&attempts, 0);
        reject_count = retries; expected_groups = 1; atomic_store(&armed, 1);
    }
    int start = smb2c_begin(c, SMB2_LOCK, 0);
    uint8_t *body = c->sbuf + start;
    p16(body, 0, 48); p16(body, 2, count); p32(body, 4, sequence);
    memcpy(body + 8, fid, 16);
    for (unsigned i = 0; i < count; i++) {
        uint8_t *element = body + 24 + 24 * i;
        p64(element, 0, ranges[i].offset); p64(element, 8, ranges[i].length);
        p32(element, 16, ranges[i].flags);
    }
    uint32_t status = smb2c_xfer(c, 24 + 24 * count);
    fprintf(stderr, "compound LOCK count=%u allocation_failure=%u status=%08x expected=%08x\n",
            count, fail_allocation, status, expected);
    assert(status == expected);
    assert(!atomic_load(&lock_test_arm) && atomic_load(&lock_test_seen) == before + 1);
    if (retries) {
        atomic_store(&armed, 0);
        assert(atomic_load(&submissions) == 1 && atomic_load(&attempts) == retries + 1);
    }
}

static void
checked_lock(struct smb2_conn *c, const uint8_t *fid, const uint64_t *offsets,
             unsigned count, bool unlock, unsigned remaining, bool fail_allocation)
{
    struct lock_element ranges[16];
    assert(count <= 16);
    for (unsigned i = 0; i < count; i++) {
        ranges[i] = (struct lock_element) { offsets[i], 4, unlock ? SMB2_LOCKFLAG_UNLOCK :
            SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY };
    }
    checked_ranges(c, fid, ranges, count, 0,
        fail_allocation ? ST_INSUFFICIENT_RESOURCES : ST_SUCCESS, remaining, fail_allocation, 0);
}

static void
batch_semantics(struct smb2_conn *c, struct smb2_conn *peer)
{
    struct smb2_create_out a, b;
    uint32_t written;
    const uint32_t acquire = SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY;
    assert(smb2_create(c, "exact-batches", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(peer, "exact-batches", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    assert(smb2_lock(peer, b.file_id, 8, 4, acquire) == ST_SUCCESS);
    struct lock_element ranges[] = { {0,4,acquire}, {8,4,acquire}, {16,4,acquire} };
    checked_ranges(c, a.file_id, ranges, 2, 0, ST_LOCK_NOT_GRANTED, 0, false, 1);
    /* Conflict in the second element must release the first reservation. */
    assert(smb2_write(peer, b.file_id, 0, "free", 4, &written) == ST_SUCCESS);
    assert(smb2_lock(peer, b.file_id, 8, 4, SMB2_LOCKFLAG_UNLOCK) == ST_SUCCESS);
    checked_ranges(c, a.file_id, ranges, 3, 0, ST_SUCCESS, 3, false, 1);
    struct lock_element missing[] = {
        {0,4,SMB2_LOCKFLAG_UNLOCK}, {32,4,SMB2_LOCKFLAG_UNLOCK}, {8,4,SMB2_LOCKFLAG_UNLOCK},
    };
    checked_ranges(c, a.file_id, missing, 3, 0, ST_RANGE_NOT_LOCKED, 2, false, 2);
    assert(smb2_write(peer, b.file_id, 0, "free", 4, &written) == ST_SUCCESS);
    assert(smb2_write(peer, b.file_id, 8, "deny", 4, &written) == ST_FILE_LOCK_CONFLICT);
    struct lock_element mixed[] = { {8,4,SMB2_LOCKFLAG_UNLOCK}, {24,4,acquire} };
    checked_ranges(c, a.file_id, mixed, 2, 0, ST_INVALID_PARAMETER, 1, false, 1);
    assert(smb2_write(peer, b.file_id, 8, "free", 4, &written) == ST_SUCCESS);
    assert(smb2_write(peer, b.file_id, 16, "deny", 4, &written) == ST_FILE_LOCK_CONFLICT);
    const uint64_t last = 16;
    checked_lock(c, a.file_id, &last, 1, true, 0, false);

    /* Exact zero-length and final-byte ranges retain their original geometry. */
    struct lock_element zero = {40,0,acquire};
    checked_ranges(c, a.file_id, &zero, 1, 0, ST_SUCCESS, 1, false, 1);
    struct lock_element wrong = {40,1,SMB2_LOCKFLAG_UNLOCK};
    checked_ranges(c, a.file_id, &wrong, 1, 0, ST_RANGE_NOT_LOCKED, 1, false, 0);
    zero.flags = SMB2_LOCKFLAG_UNLOCK;
    checked_ranges(c, a.file_id, &zero, 1, 0, ST_SUCCESS, 0, false, 0);
    struct lock_element end = {UINT64_MAX,1,acquire};
    checked_ranges(c, a.file_id, &end, 1, 0, ST_SUCCESS, 1, false, 0);
    end.flags = SMB2_LOCKFLAG_UNLOCK;
    checked_ranges(c, a.file_id, &end, 1, 0, ST_SUCCESS, 0, false, 0);
    struct lock_element overlap[] = {
        {64,4,SMB2_LOCKFLAG_SHARED | SMB2_LOCKFLAG_FAIL_IMMEDIATELY},
        {64,4,SMB2_LOCKFLAG_SHARED | SMB2_LOCKFLAG_FAIL_IMMEDIATELY},
    };
    checked_ranges(c, a.file_id, overlap, 2, 0, ST_INVALID_PARAMETER, 0, false, 0);
    assert(smb2_close(c, a.file_id) == ST_SUCCESS);
    assert(smb2_close(peer, b.file_id) == ST_SUCCESS);
}

static void
canonical_owner_cases(struct smb2_env *env, struct smb2_conn *c, struct smb2_conn *peer)
{
    const uint64_t offsets[] = { 0, 8, 16, 24 };
    struct smb2_oplock_req lease = { .is_lease = 1, .lease_state = SMB2_LEASE_RWH };
    memset(lease.lease_key, 0x71, sizeof(lease.lease_key));
    struct smb2_create_out owner, same;
    assert(smb2_create(c, "canonical-same-lease", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &owner) == ST_SUCCESS && owner.lease_state == SMB2_LEASE_RWH);
    assert(smb2_create(c, "canonical-same-lease", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &lease, &same) == ST_SUCCESS && same.lease_state == SMB2_LEASE_RWH);
    int breaks = smb2_conn_nbreaks(c);
    /* Every range carries the retained FileId owner plus its lease key. */
    checked_lock(c, owner.file_id, offsets, 2, false, 2, false);
    uint32_t written, received; uint8_t data[4];
    assert(smb2_write(c, same.file_id, 0, "peer", 4, &written) == ST_SUCCESS && written == 4);
    assert(smb2_read(c, same.file_id, 0, 4, data, &received) == ST_SUCCESS && received == 4);
    assert(!memcmp(data, "peer", 4));
    /* A later single-element request uses the same canonical owner. */
    checked_lock(c, owner.file_id, offsets + 2, 1, false, 3, false);
    assert(smb2_write(c, same.file_id, 16, "same", 4, &written) == ST_SUCCESS);
    (void) smb2_echo_barrier(c);
    assert(smb2_conn_nbreaks(c) == breaks);
    checked_lock(c, owner.file_id, offsets, 3, true, 0, false);
    assert(smb2_close(c, owner.file_id) == ST_SUCCESS);
    assert(smb2_close(c, same.file_id) == ST_SUCCESS);

    assert(smb2_create(c, "canonical-distinct", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &owner) == ST_SUCCESS);
    assert(smb2_create(peer, "canonical-distinct", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &same) == ST_SUCCESS);
    /* Resource failure still seeds the inherited FileId for a valid suffix.
     * Only QUERY is submitted after the first LOCK's allocation fails. */
    uint8_t related[16]; memset(related, 0xff, sizeof(related));
    struct command allocation_suffix[] = {
        {SMB2_LOCK,owner.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY,
         ST_INSUFFICIENT_RESOURCES},
        {.opcode=SMB2_QUERY_INFO,.fid=related,.status=ST_SUCCESS,.related=true},
    };
    int inspected = atomic_load(&lock_test_seen);
    atomic_store(&submissions, 0); atomic_store(&attempts, 0);
    reject_count = 0; expected_groups = 1;
    atomic_store(&lock_test_arm, 1); atomic_store(&armed, 1);
    uint64_t mid = send_commands(c, allocation_suffix, 2);
    smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1 && atomic_load(&attempts) == 1);
    assert(atomic_load(&lock_test_seen) == inspected + 1);
    const uint8_t *reply = c->rbuf + 4;
    assert(g64(reply,24) == mid && g32(reply,8) == ST_INSUFFICIENT_RESOURCES);
    assert(g32(reply,20)); reply += g32(reply,20);
    assert(g64(reply,24) == mid + 1 && g32(reply,8) == ST_SUCCESS && !g32(reply,20));
    checked_lock(c, owner.file_id, offsets, 1, false, 0, true);
    assert(smb2_write(peer, same.file_id, 0, "free", 4, &written) == ST_SUCCESS);
    checked_lock(c, owner.file_id, offsets, 1, false, 1, false);
    checked_lock(c, owner.file_id, offsets, 1, true, 1, true);
    assert(smb2_write(c, owner.file_id, 0, "self", 4, &written) == ST_SUCCESS);
    assert(smb2_write(peer, same.file_id, 0, "deny", 4, &written) == ST_FILE_LOCK_CONFLICT);
    checked_lock(c, owner.file_id, offsets + 1, 2, false, 3, false);
    assert(smb2_write(peer, same.file_id, 8, "deny", 4, &written) == ST_FILE_LOCK_CONFLICT);
    /* Held ranges no longer create a CLOSE boundary. Retirement retries
     * safely and shares this compound with the preceding metadata query. */
    struct command close[] = {
        {SMB2_QUERY_INFO,owner.file_id,0,0,0,ST_SUCCESS},
        {SMB2_CLOSE,owner.file_id,0,0,0,ST_SUCCESS},
    };
    run(c, close, 2, 1);
    assert(smb2_write(peer, same.file_id, 0, "free", 4, &written) == ST_SUCCESS);
    assert(smb2_close(peer, same.file_id) == ST_SUCCESS);

    struct smb2_conn *original = smb2_conn_open(env); smb2_handshake(original);
    struct smb2_conn *reclaimer = smb2_conn_open(env); smb2_handshake(reclaimer);
    assert(original->guid_tag != reclaimer->guid_tag);
    struct smb2_oplock_req batch = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    struct smb2_durable_req durable = { .dh2q = 1 };
    memset(durable.create_guid, 0x83, sizeof(durable.create_guid));
    assert(smb2_create_dur(original, "canonical-cross-client-durable", FILE_CREATE,
        FILE_ALL_ACCESS, FILE_SHARE_RWD, &batch, &durable, &owner) == ST_SUCCESS);
    assert(owner.has_dh2q && owner.oplock == SMB2_OPLOCK_LEVEL_BATCH);
    struct lock_element replay = {0,4,SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY};
    /* A failed allocation must not publish a LockSequence outcome. Retrying
     * that sequence acquires once; a later replay cannot add another range. */
    checked_ranges(original, owner.file_id, &replay, 1, 0x10, ST_INSUFFICIENT_RESOURCES, 0, true, 0);
    checked_ranges(original, owner.file_id, &replay, 1, 0x10, ST_SUCCESS, 1, false, 1);
    replay.offset = 128;
    checked_ranges(original, owner.file_id, &replay, 1, 0x10, ST_SUCCESS, 1, false, 0);
    assert(smb2_logoff(original) == ST_SUCCESS); /* synchronous durable park */
    struct smb2_durable_req reconnect = { .dh2c = 1 };
    memcpy(reconnect.file_id, owner.file_id, 16);
    memcpy(reconnect.create_guid, durable.create_guid, 16);
    assert(smb2_create_dur(reclaimer, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
        &batch, &reconnect, &same) == ST_SUCCESS);
    atomic_store(&lock_test_rehomed, 1);
    checked_lock(reclaimer, same.file_id, offsets + 1, 2, false, 3, false);
    checked_lock(reclaimer, same.file_id, offsets + 3, 1, false, 4, false);
    for (unsigned i = 0; i < 4; i++) {
        assert(smb2_write(reclaimer, same.file_id, offsets[i], "kept", 4, &written) == ST_SUCCESS);
    }
    checked_lock(reclaimer, same.file_id, offsets, 4, true, 0, false);
    atomic_store(&lock_test_rehomed, 0);
    assert(smb2_close(reclaimer, same.file_id) == ST_SUCCESS);
}

int main(void)
{
    struct smb2_env env;
    /* Enable ordinary durable handles for the cross-client reconnect case.
     * The share is not continuously available: no persistent-record mutation
     * is introduced into the existing native retry cases. */
    struct smb2_env_opts options = { .oplocks = 1, .leases = 1, .persistent_handles = 1 };
    smb2_env_start_opts(&env, &options);
    struct smb2_conn *c=smb2_conn_open(&env), *peer=smb2_conn_open(&env);
    smb2_handshake(c); smb2_handshake(peer);
    canonical_owner_cases(&env, c, peer);
    batch_semantics(c, peer);
    provisional(c,peer);
    struct smb2_create_out a,b,p;
    assert(smb2_create(c,"locks.txt",FILE_OPEN_IF,FILE_ALL_ACCESS,FILE_SHARE_RWD,NULL,&a)==ST_SUCCESS);
    assert(smb2_create(c,"locks.txt",FILE_OPEN,FILE_ALL_ACCESS,FILE_SHARE_RWD,NULL,&b)==ST_SUCCESS);
    assert(smb2_create(peer,"locks.txt",FILE_OPEN,FILE_ALL_ACCESS,FILE_SHARE_RWD,NULL,&p)==ST_SUCCESS);
    struct command io[]={
        {SMB2_LOCK,a.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,ST_SUCCESS},
        {SMB2_WRITE,a.file_id,0,4,0,ST_SUCCESS},
        {SMB2_WRITE,b.file_id,0,4,0,ST_FILE_LOCK_CONFLICT},
        {SMB2_LOCK,a.file_id,0,4,SMB2_LOCKFLAG_UNLOCK,ST_SUCCESS},
        {SMB2_WRITE,b.file_id,0,4,0,ST_SUCCESS},
    };
    run(c,io,5,0);
    struct command retry[]={io[0],{SMB2_QUERY_INFO,a.file_id,0,0,0,ST_SUCCESS},io[3]};
    run(c,retry,3,2);
    assert(smb2_lock(peer,p.file_id,0,4,SMB2_LOCKFLAG_EXCLUSIVE|SMB2_LOCKFLAG_FAIL_IMMEDIATELY)==ST_SUCCESS);
    assert(smb2_lock(peer,p.file_id,0,4,SMB2_LOCKFLAG_UNLOCK)==ST_SUCCESS);
    struct command stacked[]={
        {SMB2_LOCK,a.file_id,8,4,SMB2_LOCKFLAG_SHARED|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,ST_SUCCESS},
        {SMB2_LOCK,a.file_id,8,4,SMB2_LOCKFLAG_SHARED|SMB2_LOCKFLAG_FAIL_IMMEDIATELY,ST_SUCCESS},
        {SMB2_LOCK,a.file_id,8,4,SMB2_LOCKFLAG_UNLOCK,ST_SUCCESS},
        {SMB2_WRITE,b.file_id,8,4,0,ST_FILE_LOCK_CONFLICT},
        {SMB2_LOCK,a.file_id,8,4,SMB2_LOCKFLAG_UNLOCK,ST_SUCCESS},
        {SMB2_WRITE,b.file_id,8,4,0,ST_SUCCESS},
    };
    run(c,stacked,6,0);
    blocking(peer,c,p.file_id,a.file_id,0);
    blocking(peer,c,p.file_id,a.file_id,1);
    blocking(peer,c,p.file_id,a.file_id,2);
    assert(smb2_close(c,b.file_id)==ST_SUCCESS);
    assert(smb2_close(peer,p.file_id)==ST_SUCCESS);
    smb2_conn_reset(&env);
    smb2_env_fs_teardown(&env,"fs0"); smb2_env_stop(&env);
    return 0;
}
