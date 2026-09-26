// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Real wire groups: earlier replies describe state at their own command,
 * while later commands observe attempt-private positions and filesystem attrs.
 * Executed mutations are never followed by injected finish rejection. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_notify.h"

static atomic_int   armed, submissions, expected_groups, expect_cold_root;
static int          chunk_mode;
static atomic_int   ea_chunks;
static unsigned int expected_run_groups[4];
static atomic_int   link_attempts, replacement_links, link_notifications;
static atomic_int   hold_link, link_held, release_link;
static const char  *watched_link;
static struct {
    struct evpl_timer               timer;
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    const void                     *fh, *dir_fh;
    int                             fhlen, dir_fhlen, namelen;
    const char                     *name;
    unsigned int                    replace, flags;
    uint64_t                        attr_mask, pre_mask, post_mask;
    const uint8_t                  *lease_key;
    struct chimera_vfs_open_handle *handle;
    struct chimera_claim_actor      actor;
    bool                            have_actor;
    chimera_vfs_link_at_callback_t  callback;
    void                           *private_data;
} link_gate;

static void
link_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_exchange(&release_link, 0)) {
        evpl_add_oneshot_timer(evpl, timer, link_poll, 1000); return;
    }
    __typeof__(&chimera_vfs_link_at_flags_actor) next = dlsym(RTLD_NEXT, "chimera_vfs_link_at_flags_actor");
    assert(next);
    atomic_store(&link_held, 0);
    next(link_gate.thread, link_gate.cred, link_gate.fh, link_gate.fhlen,
         link_gate.dir_fh, link_gate.dir_fhlen, link_gate.name, link_gate.namelen,
         link_gate.replace, link_gate.flags, link_gate.attr_mask, link_gate.pre_mask,
         link_gate.post_mask, link_gate.lease_key, link_gate.handle,
         link_gate.have_actor ? &link_gate.actor : NULL, link_gate.callback, link_gate.private_data);
} /* link_poll */

__attribute__((visibility("default"))) void
chimera_vfs_link_at_flags_actor(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const void                       *dir_fh,
    int                               dir_fhlen,
    const char                       *name,
    int                               namelen,
    unsigned int                      replace,
    unsigned int                      flags,
    uint64_t                          attr_mask,
    uint64_t                          pre_mask,
    uint64_t                          post_mask,
    const uint8_t                    *lease_key,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_actor *actor,
    chimera_vfs_link_at_callback_t    callback,
    void                             *private_data)
{
    __typeof__(&chimera_vfs_link_at_flags_actor) next = dlsym(RTLD_NEXT, "chimera_vfs_link_at_flags_actor");

    assert(next);
    if (atomic_load(&armed)) {
        atomic_fetch_add(replace ? &replacement_links : &link_attempts, 1);
        if (replace && watched_link) {
            assert(!atomic_load(&link_notifications));
        }
    }
    if (!replace && atomic_exchange(&hold_link, 0)) {
        link_gate.thread     = thread; link_gate.cred = cred;
        link_gate.fh         = fh; link_gate.fhlen = fhlen;
        link_gate.dir_fh     = dir_fh; link_gate.dir_fhlen = dir_fhlen;
        link_gate.name       = name; link_gate.namelen = namelen;
        link_gate.replace    = replace; link_gate.flags = flags;
        link_gate.attr_mask  = attr_mask; link_gate.pre_mask = pre_mask; link_gate.post_mask = post_mask;
        link_gate.lease_key  = lease_key; link_gate.handle = handle;
        link_gate.have_actor = actor != NULL;
        if (actor) {
            link_gate.actor = *actor;
        }
        link_gate.callback = callback; link_gate.private_data = private_data;
        evpl_add_oneshot_timer(thread->evpl, &link_gate.timer, link_poll, 1000);
        atomic_store(&link_held, 1); return;
    }
    next(thread, cred, fh, fhlen, dir_fh, dir_fhlen, name, namelen, replace,
         flags, attr_mask, pre_mask, post_mask, lease_key, handle, actor, callback, private_data);
} /* chimera_vfs_link_at_flags_actor */

__attribute__((visibility("default"))) void
chimera_vfs_notify_emit_lease(
    struct chimera_vfs_notify *notify,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len,
    uint64_t                   skip_lo,
    uint64_t                   skip_hi,
    bool                       has_skip)
{
    __typeof__(&chimera_vfs_notify_emit_lease) next = dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_lease");

    assert(next);
    if (atomic_load(&armed) && watched_link && action == CHIMERA_VFS_NOTIFY_FILE_ADDED &&
        name && strlen(watched_link) == name_len && !memcmp(watched_link, name, name_len)) {
        atomic_fetch_add(&link_notifications, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len,
         skip_lo, skip_hi, has_skip);
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
    uint16_t                          old_name_len,
    const struct chimera_claim_actor *actor)
{
    __typeof__(&chimera_vfs_notify_emit_actor) next = dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_actor");

    assert(next);
    if (atomic_load(&armed) && watched_link && action == CHIMERA_VFS_NOTIFY_FILE_ADDED &&
        name && strlen(watched_link) == name_len && !memcmp(watched_link, name, name_len)) {
        atomic_fetch_add(&link_notifications, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len,
         actor);
} /* chimera_vfs_notify_emit_actor */

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
        if (atomic_exchange(&expect_cold_root, 0)) {
            unsigned int root_lookups = 0;
            /* Both wire names are root leaves. A LOOKUP_PATH here can only
             * be CREATE's authoritative share-root resolution; a warm tree
             * would have no such descriptor. */
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, i);
                if (op->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH) {
                    root_lookups++;
                }
            }
            assert(root_lookups == 1);
        }
        unsigned int run    = atomic_fetch_add(&submissions, 1);
        unsigned     groups = chimera_vfs_compound_num_groups(cp);
        if (chunk_mode && !groups) {
            int chunk = atomic_fetch_add(&ea_chunks, 1);
            assert(chimera_vfs_compound_num_ops(cp) == 2);
            assert(chimera_vfs_compound_op(cp, 0)->type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE);
            assert(chimera_vfs_compound_op(cp, 1)->type == (chunk ?
                                                            CHIMERA_VFS_COMPOUND_OP_CHECKPOINT :
                                                            CHIMERA_VFS_COMPOUND_OP_LISTXATTRS));
        } else {
            if (atomic_load(&expected_groups) < 0) {
                assert(run < 4 && groups == expected_run_groups[run]);
            } else {
                assert(groups == (uint32_t) atomic_load(&expected_groups));
            }
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

static uint32_t
ea_entry(
    uint8_t    *buffer,
    const char *name,
    uint8_t     name_len,
    const void *value,
    uint16_t    value_len,
    int         last)
{
    uint32_t length  = 9 + name_len + value_len;
    uint32_t aligned = last ? length : (length + 3) & ~3u;

    memset(buffer, 0, aligned);
    p32(buffer, 0, last ? 0 : aligned);
    buffer[5] = name_len;
    p16(buffer, 6, value_len);
    memcpy(buffer + 8, name, name_len);
    if (value_len) {
        memcpy(buffer + 9 + name_len, value, value_len);
    }
    return aligned;
} /* ea_entry */

static void
hardlink(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *name)
{
    uint8_t      value[512] = { 0 };
    unsigned int len        = strlen(name);

    assert(20 + len * 2 <= sizeof(value));
    p32(value, 16, len * 2);
    for (unsigned int i = 0; i < len; i++) {
        p16(value, 20 + i * 2, name[i]);
    }
    set(p, c, fid, 0x0b, value, 20 + len * 2); /* FILE_LINK_INFORMATION */
} /* hardlink */

static void
replace_link(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t    *fid,
    const char       *name)
{
    hardlink(p, c, fid, name);
    p->data[p->previous + SMB2_HDR_SIZE + 32] = 1;
} /* replace_link */

static void
create_op(
    struct packet    *p,
    struct smb2_conn *c,
    const char       *name)
{
    uint8_t *b = append(p, c, SMB2_CREATE, 56 + strlen(name) * 2);

    p16(b, 0, 57); p32(b, 4, 2); p32(b, 24, MBT_FILE_ALL_ACCESS);
    p32(b, 28, MBT_FILE_ATTRIBUTE_NORMAL); p32(b, 32, MBT_FILE_SHARE_RWD);
    p32(b, 36, MBT_FILE_CREATE); p32(b, 40, MBT_FILE_NON_DIRECTORY_FILE);
    p16(b, 44, SMB2_HDR_SIZE + 56); p16(b, 46, utf16le(name, b + 56));
} /* create_op */

static void
close_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t    *fid)
{
    uint8_t *b = append(p, c, SMB2_CLOSE, 24);

    p16(b, 0, 24); memcpy(b + 8, fid, 16);
} /* close_op */
static void related(struct packet *p) { p32(p->data + p->previous, 16, 4); }

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
metadata_check(
    struct packet    *p,
    struct smb2_conn *c)
{
    unsigned int off = 4;

    for (unsigned int i = 0; i < p->count; i++) {
        assert(off + SMB2_HDR_SIZE <= (unsigned int) c->rlen);
        const uint8_t *h = c->rbuf + off;
        fprintf(stderr, "metadata response %u command %u status %08x\n", i, g16(h, 12), g32(h, 8));
        assert(g64(h, 24) == p->first_mid + i);
        assert(g32(h, 8) == p->expected[i]);
        uint32_t       next = g32(h, 20);
        assert((i + 1 < p->count) == (next != 0));
        if (next) {
            assert(next >= SMB2_HDR_SIZE && !(next & 7));
        }
        off += next;
    }
} /* metadata_check */

static void
metadata_send(
    struct packet    *p,
    struct smb2_conn *c)
{
    int replies = c->nreply_app;

    atomic_store(&submissions, 0);
    atomic_store(&link_attempts, 0); atomic_store(&replacement_links, 0);
    atomic_store(&link_notifications, 0);
    atomic_store(&expected_groups, p->count);
    atomic_store(&armed, 1);
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE);
    c->msg_id = p->first_mid + p->count;
    (void) smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    assert(c->nreply_app == replies + 1);
    metadata_check(p, c);
} /* metadata_send */

static void
metadata_send_split(
    struct packet    *p,
    struct smb2_conn *c,
    unsigned          first,
    unsigned          second)
{
    atomic_store(&submissions, 0);
    atomic_store(&link_attempts, 0); atomic_store(&replacement_links, 0);
    atomic_store(&link_notifications, 0);
    expected_run_groups[0] = first; expected_run_groups[1] = 0; expected_run_groups[2] = second;
    atomic_store(&expected_groups, -1); atomic_store(&armed, 1);
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE); c->msg_id = p->first_mid + p->count;
    (void) smb2c_wait(c); atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 3);
    metadata_check(p, c);
} /* metadata_send_split */

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

/* ReplaceIfExists keeps absent targets native, including a related CREATE
 * producer before the tree's root handle has been published. */
static void
replace_link_producer(struct smb2_conn *c)
{
    struct packet p = { 0 };
    uint8_t       inherited[16];
    uint32_t      length;

    memset(inherited, 0xff, sizeof(inherited));
    watched_link = "cold-link";
    atomic_store(&expect_cold_root, 1);
    create_op(&p, c, "cold-source");
    replace_link(&p, c, inherited, watched_link); related(&p);
    query(&p, c, inherited, 1, 0x05); related(&p);
    close_op(&p, c, inherited); related(&p);
    metadata_send(&p, c);
    assert(atomic_load(&link_attempts) == 1 && !atomic_load(&replacement_links));
    assert(atomic_load(&link_notifications) == 1);
    const uint8_t *out = result(c, 2, &length);
    assert(length == 24 && g32(out, 16) == 2);

    /* The CREATE prefix must publish its producer before a collision falls
     * back; related QUERY/CLOSE then consume that same accepted handle. */
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "producer-replace-source");
    replace_link(&p, c, inherited, watched_link); related(&p);
    query(&p, c, inherited, 1, 0x05); related(&p);
    close_op(&p, c, inherited); related(&p);
    metadata_send_split(&p, c, 4, 2);
    assert(atomic_load(&link_attempts) == 1 && atomic_load(&replacement_links) == 1);
    assert(atomic_load(&link_notifications) == 1);
    out = result(c, 2, &length);
    assert(length == 24 && g32(out, 16) == 2);
    watched_link = NULL;
} /* replace_link_producer */

static void
replace_link_cases(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_create_out source, target, probe, directory;
    struct smb2_conn      *peer = smb2_conn_open(env);
    uint8_t                data[16];
    uint32_t               length, written;

    smb2_handshake(peer);
    assert(smb2_create(c, "replace-link-source", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
    assert(smb2_write(c, source.file_id, 0, "source", 6, &written) == ST_SUCCESS && written == 6);
    assert(smb2_create_opts(c, "replace-link-dir", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    struct packet          p = { 0 };
    replace_link(&p, c, source.file_id, "replace-link-absent");
    query(&p, c, source.file_id, 1, 0x05);
    replace_link(&p, c, source.file_id, "replace-link-dir\\nested");
    query(&p, c, source.file_id, 1, 0x05);
    metadata_send(&p, c);
    assert(atomic_load(&link_attempts) == 2 && !atomic_load(&replacement_links));
    const uint8_t         *out = result(c, 1, &length);
    assert(length == 24 && g32(out, 16) == 2);
    out = result(c, 3, &length);
    assert(length == 24 && g32(out, 16) == 3);

    /* A same-inode occupied destination remains a legacy replacement. The
     * native nonreplace attempt must preserve its collision, not fabricate a
     * no-op result or an extra link. */
    memset(&p, 0, sizeof(p));
    query(&p, c, source.file_id, 1, 0x05);
    replace_link(&p, c, source.file_id, "replace-link-absent");
    query(&p, c, source.file_id, 1, 0x05);
    watched_link = "replace-link-absent";
    metadata_send_split(&p, c, 3, 1);
    assert(atomic_load(&link_attempts) == 1 && atomic_load(&replacement_links) == 1);
    out = result(c, 2, &length);
    assert(length == 24 && g32(out, 16) == 3);
    watched_link = NULL;

    /* A real distinct destination appears only after typed parent resolution.
     * The first LINK must atomically collide without mutation or notification;
     * the accepted prefix then crosses the legacy replacement boundary. */
    memset(&p, 0, sizeof(p));
    query(&p, c, source.file_id, 1, 0x05);
    replace_link(&p, c, source.file_id, "replace-link-race");
    query(&p, c, source.file_id, 1, 0x05);
    watched_link = "replace-link-race";
    atomic_store(&submissions, 0); atomic_store(&link_attempts, 0);
    atomic_store(&replacement_links, 0); atomic_store(&link_notifications, 0);
    expected_run_groups[0] = 3; expected_run_groups[1] = 0; expected_run_groups[2] = 1;
    atomic_store(&expected_groups, -1); atomic_store(&link_held, 0);
    atomic_store(&release_link, 0); atomic_store(&hold_link, 1); atomic_store(&armed, 1);
    int      replies = c->nreply_app;
    memcpy(c->sbuf + 4, p.data, p.length);
    smb2c_send(c, p.length - SMB2_HDR_SIZE); c->msg_id = p.first_mid + p.count;
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&link_held)) {
        smb2_pump(env);
        assert(smb2c_now_ms() < deadline && !c->disconnected);
    }
    assert(c->nreply_app == replies && !atomic_load(&link_notifications));
    atomic_store(&armed, 0);
    assert(smb2_create(peer, watched_link, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
    assert(smb2_write(peer, target.file_id, 0, "target", 6, &written) == ST_SUCCESS && written == 6);
    atomic_store(&armed, 1); atomic_store(&release_link, 1);
    assert(smb2c_pump_for_nreply(c, replies, "ReplaceIfExists LINK destination race"));
    atomic_store(&armed, 0); metadata_check(&p, c);
    assert(atomic_load(&submissions) == 3 && atomic_load(&link_attempts) == 1 &&
           atomic_load(&replacement_links) == 1 && atomic_load(&link_notifications) == 1);
    out = result(c, 0, &length);
    assert(length == 24 && g32(out, 16) == 3);
    out = result(c, 2, &length);
    assert(length == 24 && g32(out, 16) == 4);
    assert(smb2_create(c, watched_link, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_read(c, probe.file_id, 0, 6, data, &length) == ST_SUCCESS &&
           length == 6 && !memcmp(data, "source", 6));
    assert(smb2_read(peer, target.file_id, 0, 6, data, &length) == ST_SUCCESS &&
           length == 6 && !memcmp(data, "target", 6));
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_close(peer, target.file_id) == ST_SUCCESS);
    watched_link = NULL;

    /* Unrelated errors retain their original mapping and do not run a
     * replacement. An independent suffix still sees the untouched source. */
    memset(&p, 0, sizeof(p));
    replace_link(&p, c, source.file_id, "missing-link-parent\\child");
    p.expected[0] = ST_OBJECT_PATH_NOT_FOUND;
    query(&p, c, source.file_id, 1, 0x05);
    metadata_send(&p, c);
    assert(!atomic_load(&replacement_links));
    out = result(c, 1, &length);
    assert(length == 24 && g32(out, 16) == 4);
    assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
} /* replace_link_cases */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_create_out opened;
    struct packet          p = { 0 };
    uint8_t                value[40] = { 0 }, standalone[2048];
    uint32_t               written, length;

    smb2_env_start(&env);
    struct smb2_conn      *c = smb2_conn_open(&env);
    smb2_handshake(c);
    replace_link_producer(c);
    replace_link_cases(&env, c);
    assert(smb2_create(c, "metadata.txt", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &opened) == ST_SUCCESS);
    assert(smb2_write(c, opened.file_id, 0, "abcdefgh", 8, &written) == ST_SUCCESS && written == 8);
    p64(value, 0, 1);
    set(&p, c, opened.file_id, 0x0e, value, 8); /* POSITION */
    query(&p, c, opened.file_id, 1, 0x0e);
    p64(value, 0, 7);
    set(&p, c, opened.file_id, 0x0e, value, 8);
    query(&p, c, opened.file_id, 1, 0x12); /* ALL */
    read_four(&p, c, opened.file_id);
    query(&p, c, opened.file_id, 1, 0x0e);
    metadata_send(&p, c);
    const uint8_t *out = result(c, 1, &length);
    assert(length == 8 && g64(out, 0) == 1);
    out = result(c, 3, &length);
    assert(length >= 100 && g64(out, 80) == 7);
    out = result(c, 5, &length);
    assert(length == 8 && g64(out, 0) == 4);
    assert(smb2_query_info(c, 1, 0x0e, opened.file_id, 0, standalone,
                           sizeof(standalone), &length) == ST_SUCCESS && g64(standalone, 0) == 4);

    memset(&p, 0, sizeof(p));
    memset(value, 0, sizeof(value));
    query(&p, c, opened.file_id, 1, SMB2_FILE_BASIC_INFO_T);
    p32(value, 32, 0x02u);
    set(&p, c, opened.file_id, SMB2_FILE_BASIC_INFO_T, value, 40);
    query(&p, c, opened.file_id, 1, SMB2_FILE_BASIC_INFO_T);
    metadata_send(&p, c);
    out = result(c, 0, &length);
    assert(length == 40 && !(g32(out, 32) & 0x02u));
    out = result(c, 2, &length);
    assert(length == 40 && (g32(out, 32) & 0x02u));

    /* Both root and nested target parents are resolved inside this one VFS
     * compound; a later query observes each accepted namespace step. */
    struct smb2_create_out directory, alias;
    assert(smb2_create_opts(c, "linkdir", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    hardlink(&p, c, opened.file_id, "metadata-link.txt");
    query(&p, c, opened.file_id, 1, 0x05);
    hardlink(&p, c, opened.file_id, "linkdir\\nested.txt");
    query(&p, c, opened.file_id, 1, 0x05);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 24 && g32(out, 16) == 2);
    out = result(c, 3, &length);
    assert(length == 24 && g32(out, 16) == 3);
    assert(smb2_create(c, "linkdir\\nested.txt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &alias) == ST_SUCCESS);
    assert(smb2_read(c, alias.file_id, 0, 8, standalone, &length) == ST_SUCCESS);
    assert(length == 8 && !memcmp(standalone, "abcdefgh", 8));
    assert(smb2_close(c, alias.file_id) == ST_SUCCESS);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    hardlink(&p, c, opened.file_id, "metadata-link.txt");
    p.expected[0] = ST_OBJECT_NAME_COLLISION;
    hardlink(&p, c, opened.file_id, "missing-parent\\entry.txt");
    p.expected[1] = ST_OBJECT_PATH_NOT_FOUND;
    query(&p, c, opened.file_id, 1, 0x05);
    metadata_send(&p, c);
    out = result(c, 2, &length);
    assert(length == 24 && g32(out, 16) == 3);

    memset(&p, 0, sizeof(p));
    p64(value, 0, 4);
    set(&p, c, opened.file_id, 0x14, value, 8); /* EOF */
    query(&p, c, opened.file_id, 1, 0x05); /* STANDARD */
    p64(value, 0, 2);
    set(&p, c, opened.file_id, 0x13, value, 8); /* ALLOCATION */
    query(&p, c, opened.file_id, 1, 0x05);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 24 && g64(out, 8) == 4);
    out = result(c, 3, &length);
    assert(length == 24 && g64(out, 8) == 2);

    memset(&p, 0, sizeof(p));
    uint8_t ea[14] = { 0, 0, 0, 0, 0, 3, 2, 0, 'F', 'o', 'o', 0, 'o', 'k' };
    set(&p, c, opened.file_id, 0x0f, ea, sizeof(ea));
    query(&p, c, opened.file_id, 1, 0x0f);
    query(&p, c, opened.file_id, 3, 0);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length >= 14 && out[5] == 3 && g16(out, 6) == 2);
    assert(!memcmp(out + 8, "Foo\0ok", 6));
    out = result(c, 2, &length);
    assert(length >= 20 && out[0] == 1);

    /* Each entry sees successful earlier entries in this same SET list.
     * The initial backend LIST must not create case-variant duplicate keys. */
    uint8_t  sequence[1024];
    uint32_t used = ea_entry(sequence, "Foo", 3, NULL, 0, 0);
    used += ea_entry(sequence + used, "NEW", 3, "one", 3, 0);
    used += ea_entry(sequence + used, "nEw", 3, "two", 3, 1);
    memset(&p, 0, sizeof(p));
    set(&p, c, opened.file_id, 0x0f, sequence, used);
    query(&p, c, opened.file_id, 1, 0x0f);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 16 && !g32(out, 0) && out[5] == 3 && g16(out, 6) == 3);
    assert(!memcmp(out + 8, "NEW\0two", 7));

    /* A deletion shadows the original spelling. A later recreation introduces
     * its own spelling, which the following case variant must preserve. */
    used  = ea_entry(sequence, "nEw", 3, NULL, 0, 0);
    used += ea_entry(sequence + used, "new", 3, "new", 3, 0);
    used += ea_entry(sequence + used, "NEW", 3, "end", 3, 1);
    memset(&p, 0, sizeof(p));
    set(&p, c, opened.file_id, 0x0f, sequence, used);
    query(&p, c, opened.file_id, 1, 0x0f);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 16 && !g32(out, 0) && out[5] == 3 && g16(out, 6) == 3);
    assert(!memcmp(out + 8, "new\0end", 7));

    /* An invalid later entry fails its command, retaining the successful
     * deletion/recreation prefix for the independent query group. */
    used  = ea_entry(sequence, "NEW", 3, NULL, 0, 0);
    used += ea_entry(sequence + used, "Fresh", 5, "ok", 2, 0);
    used += ea_entry(sequence + used, "?", 1, "bad", 3, 1);
    memset(&p, 0, sizeof(p));
    set(&p, c, opened.file_id, 0x0f, sequence, used);
    p.expected[0] = 0x80000013u; /* INVALID_EA_NAME */
    query(&p, c, opened.file_id, 1, 0x0f);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 16 && !g32(out, 0) && out[5] == 5 && g16(out, 6) == 2);
    assert(!memcmp(out + 8, "Fresh\0ok", 8));

    /* A full 255-byte VFS key (250-byte SMB name) remains in bounds when
     * reused from private history. ASan guards the canonicalization buffer. */
    char long_name[250], lower_name[250];
    memset(long_name, 'A', sizeof(long_name));
    memset(lower_name, 'a', sizeof(lower_name));
    used  = ea_entry(sequence, "Fresh", 5, NULL, 0, 0);
    used += ea_entry(sequence + used, long_name, sizeof(long_name), "old", 3, 0);
    used += ea_entry(sequence + used, lower_name, sizeof(lower_name), "end", 3, 1);
    memset(&p, 0, sizeof(p));
    set(&p, c, opened.file_id, 0x0f, sequence, used);
    query(&p, c, opened.file_id, 1, 0x0f);
    metadata_send(&p, c);
    out = result(c, 1, &length);
    assert(length == 264 && !g32(out, 0) && out[5] == 250 && g16(out, 6) == 3);
    assert(!memcmp(out + 8, long_name, sizeof(long_name)) && out[258] == 0);
    assert(!memcmp(out + 259, "end", 3));

    /* Long valid input crosses the operation capacity with accepted chunks.
     * More than 4KiB of names ensures a second small LIST would not recover
     * canonical spelling: committed name history must survive the boundary. */
    struct smb2_create_out many;
    assert(smb2_create(c, "many-eas.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &many) == ST_SUCCESS);
    uint8_t               *large = calloc(1, 65536);
    assert(large);
    used = 0;
    char                   key[16];
    for (unsigned i = 0; i < 1022; i++) {
        int key_len = snprintf(key, sizeof(key), "Key%04u", i);
        used += ea_entry(large + used, key, key_len, "old", 3, 0);
    }
    used += ea_entry(large + used, "KEY0000", 7, "end", 3, 0);
    for (unsigned i = 1; i < 1022; i++) {
        int key_len = snprintf(key, sizeof(key), "key%04u", i);
        used += ea_entry(large + used, key, key_len, NULL, 0, i == 1021);
    }
    assert(used < 65536);
    atomic_store(&submissions, 0);
    atomic_store(&ea_chunks, 0);
    atomic_store(&expected_groups, 1);
    chunk_mode = 1;
    atomic_store(&armed, 1);
    uint32_t ea_status = smb2_set_info(c, SMB2_INFO_FILE_T, 0x0f, many.file_id, large, used);
    atomic_store(&armed, 0);
    chunk_mode = 0;
    assert(ea_status == ST_SUCCESS);
    assert(atomic_load(&submissions) == 3 && atomic_load(&ea_chunks) == 2);
    free(large);
    memset(&p, 0, sizeof(p));
    query(&p, c, many.file_id, 1, 0x0f);
    metadata_send(&p, c);
    out = result(c, 0, &length);
    assert(length == 20 && !g32(out, 0) && out[5] == 7 && g16(out, 6) == 3);
    assert(!memcmp(out + 8, "Key0000\0end", 11));
    assert(smb2_close(c, many.file_id) == ST_SUCCESS);

    assert(smb2_close(c, opened.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
