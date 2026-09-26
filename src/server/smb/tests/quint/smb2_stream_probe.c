/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 alternate-data-stream (ADS / named stream) ground-truth probe.
 *
 * The MBT corpus never enables named streams, so the whole ADS lifecycle --
 * creating a named stream with the "file:stream" CREATE syntax, writing and
 * reading its independent content, enumerating streams with
 * FILE_STREAM_INFORMATION, and deleting a stream -- is dark to the model.  It
 * is also the SMB view of the exact VFS named-stream storage that NFSv4 named
 * attributes project onto (open_stream/list_streams/remove_stream,
 * CAP_NAMED_STREAMS, the memfs per-inode stream list), so this probe is the SMB
 * half of the cross-protocol stream coverage.
 *
 * Like the other ground-truth probes the classes constrain each other: a
 * stream's content read back must equal what was written; the size reported by
 * FILE_STREAM_INFORMATION must equal the bytes written; a write to one stream
 * must not perturb the base file's data or the sibling streams; and a deleted
 * stream must disappear from the enumeration while its siblings survive.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_notify.h"

/* Interpose only the checked stream-DOC compound, not surrounding legacy
 * CLOSE work. Mutation is never rejected: mode 1 holds acceptance; mode 2
 * replaces the name before the checked request reaches the backend. */
static atomic_int stream_test_mode, stream_test_held, stream_test_release;
static atomic_int stream_test_submissions, stream_test_notifications;
static atomic_int stream_close_armed, stream_close_submissions, stream_close_finishes;
static atomic_int stream_close_notifications;
static            _Thread_local struct chimera_vfs_thread *stream_test_thread;
static            _Thread_local struct chimera_vfs_cred stream_test_cred;

typedef void (*stream_submit_fn)(
    struct chimera_vfs_compound *,
    chimera_vfs_compound_callback_t,
    void *);

struct stream_test_ctx {
    struct chimera_vfs_compound    *compound;
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    struct evpl_timer               timer;
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
    stream_test_thread = thread;
    stream_test_cred   = *cred;
    return next(thread, cred);
} /* chimera_vfs_compound_alloc */

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
    typedef void (*notify_fn)(
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
    notify_fn next = (notify_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_lease");
    assert(next);
    if (atomic_load(&stream_close_armed) && (action & CHIMERA_VFS_NOTIFY_FILE_MODIFIED)) {
        assert(name_len == strlen("compound-stream") && !memcmp(name, "compound-stream", name_len));
        atomic_fetch_add(&stream_close_notifications, 1);
    }
    if (atomic_load(&stream_test_mode) && (action & CHIMERA_VFS_NOTIFY_STREAM_NAME)) {
        assert(action == CHIMERA_VFS_NOTIFY_STREAM_NAME);
        assert(name_len == strlen("checked-stream") && !memcmp(name, "checked-stream", name_len));
        atomic_fetch_add(&stream_test_notifications, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len, skip_lo, skip_hi, has_skip);
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
    typedef void (*notify_fn)(
        struct chimera_vfs_notify *,
        const uint8_t *,
        uint16_t,
        uint32_t,
        const char *,
        uint16_t,
        const char *,
        uint16_t,
        const struct chimera_claim_actor *);
    notify_fn next = (notify_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_actor");
    assert(next);
    if (atomic_load(&stream_close_armed) && (action & CHIMERA_VFS_NOTIFY_FILE_MODIFIED)) {
        assert(name_len == strlen("compound-stream") && !memcmp(name, "compound-stream", name_len));
        atomic_fetch_add(&stream_close_notifications, 1);
    }
    if (atomic_load(&stream_test_mode) && (action & CHIMERA_VFS_NOTIFY_STREAM_NAME)) {
        assert(action == CHIMERA_VFS_NOTIFY_STREAM_NAME);
        assert(name_len == strlen("checked-stream") && !memcmp(name, "checked-stream", name_len));
        atomic_fetch_add(&stream_test_notifications, 1);
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len, actor);
} /* chimera_vfs_notify_emit_actor */

static void
stream_close_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    (void) private_data;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!atomic_load(&stream_close_notifications));
    /* Only query/resource-close and private claim retirement have executed.
     * The published logical open still owns its original handle reference. */
    int attempt = atomic_fetch_add(&stream_close_finishes, 1);
    chimera_vfs_compound_finish_result(cp, attempt ? CHIMERA_VFS_OK : CHIMERA_VFS_EAGAIN);
} /* stream_close_finish */

static void
stream_test_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct stream_test_ctx *ctx = (void *) ((char *) timer - offsetof(struct stream_test_ctx, timer));

    if (!atomic_exchange(&stream_test_release, 0)) {
        evpl_add_oneshot_timer(evpl, timer, stream_test_poll, 1000);
        return;
    }
    chimera_vfs_compound_finish_result(ctx->compound, CHIMERA_VFS_OK);
} /* stream_test_poll */

static void
stream_test_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct stream_test_ctx *ctx = private_data;

    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!atomic_load(&stream_test_notifications));
    ctx->compound = cp;
    evpl_add_oneshot_timer(stream_test_thread->evpl, &ctx->timer, stream_test_poll, 1000);
    atomic_store(&stream_test_held, 1);
} /* stream_test_finish */

static void
stream_test_done(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct stream_test_ctx         *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    free(ctx);
    callback(cp, arg);
} /* stream_test_done */

static void
stream_test_rebound(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct stream_test_ctx *ctx = private_data;

    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    stream_submit_fn        next = (stream_submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    next(ctx->compound, stream_test_done, ctx);
} /* stream_test_rebound */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *cp,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    stream_submit_fn                      next = (stream_submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");

    assert(next);
    if (atomic_load(&stream_close_armed)) {
        assert(chimera_vfs_compound_num_groups(cp) == 3);
        atomic_fetch_add(&stream_close_submissions, 1);
        chimera_vfs_compound_set_finish_handler(cp, stream_close_finish, NULL);
    }
    unsigned                              mode   = atomic_load(&stream_test_mode);
    const struct chimera_vfs_compound_op *remove = NULL;
    for (uint32_t i = 0; mode && i < chimera_vfs_compound_num_ops(cp); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, i);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM &&
            (op->remove_flags & CHIMERA_VFS_REMOVE_STREAM_MATCH_FH)) {
            remove = op;
        }
    }
    if (!remove) {
        next(cp, callback, private_data);
        return;
    }
    assert(chimera_vfs_compound_num_ops(cp) == 3);
    atomic_fetch_add(&stream_test_submissions, 1);
    struct stream_test_ctx *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->compound     = cp;
    ctx->callback     = callback;
    ctx->private_data = private_data;
    if (mode == 1) {
        chimera_vfs_compound_set_finish_handler(cp, stream_test_finish, ctx);
        next(cp, stream_test_done, ctx);
    } else {
        const struct chimera_vfs_compound_op *base = chimera_vfs_compound_op(cp, 0);
        assert(base->type == CHIMERA_VFS_COMPOUND_OP_PUTFH);
        struct chimera_vfs_compound          *rebind = chimera_vfs_compound_alloc(stream_test_thread, &stream_test_cred)
        ;
        chimera_vfs_compound_add_putfh(rebind, base->arg_fh, base->arg_fh_len);
        chimera_vfs_compound_add_open_current(rebind, CHIMERA_VFS_OPEN_INFERRED, 0);
        chimera_vfs_compound_add_remove_stream(rebind, remove->name, remove->name_len);
        chimera_vfs_compound_add_open_stream(rebind, remove->name, remove->name_len,
                                             CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_CREATE |
                                             CHIMERA_VFS_OPEN_EXCLUSIVE, NULL, 0);
        next(rebind, stream_test_rebound, ctx);
    }
} /* chimera_vfs_compound_submit */


static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

/* Enumerate the named streams of the file open on `fid` via
 * FILE_STREAM_INFORMATION.  Returns the number of entries; if `want` is
 * non-NULL, sets *want_size to the size of the stream whose (case-sensitive)
 * name contains `want` and *want_found to 1 when it was seen. */
static int
enum_streams(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *want,
    uint64_t         *want_size,
    int              *want_found)
{
    uint8_t  out[2048];
    uint32_t st, len = 0, off = 0;
    int      entries = 0;

    if (want_found) {
        *want_found = 0;
    }
    if (want_size) {
        *want_size = 0;
    }

    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STREAM_INFO_T,
                         fid, 0, out, sizeof(out), &len);
    if (st != ST_SUCCESS || len == 0) {
        return -1;
    }

    /* Each record: NextEntryOffset(4) StreamNameLength(4) StreamSize(8)
    * StreamAllocationSize(8) StreamName(UTF-16LE).  (MS-FSCC 2.4.43) */
    while (off + 24 <= len) {
        uint32_t next = g32(out, (int) off);
        uint32_t nlen = g32(out, (int) off + 4);
        uint64_t size = g64(out, (int) off + 8);
        char     nm[256];
        uint32_t i;

        entries++;
        if (nlen / 2 < sizeof(nm) - 1 && off + 24 + nlen <= len) {
            for (i = 0; i < nlen / 2; i++) {
                nm[i] = (char) out[off + 24 + i * 2];
            }
            nm[nlen / 2] = '\0';
            if (want && strstr(nm, want)) {
                if (want_found) {
                    *want_found = 1;
                }
                if (want_size) {
                    *want_size = size;
                }
            }
        }
        if (next == 0) {
            break;
        }
        off += next;
    }
    return entries;
} /* enum_streams */

/* The full ADS lifecycle against one base file. */
static void
probe_stream_lifecycle(struct smb2_conn *c)
{
    struct smb2_create_out base, alt, beta, reopen;
    uint8_t                rd[64];
    uint32_t               st, cnt = 0, rlen = 0;
    uint64_t               sz = 0;
    int                    entries, found = 0;

    printf("# --- ADS lifecycle ---\n");

    /* Base file with its own data stream. */
    st = smb2_create(c, "ads.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE base ads.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);
    CHECK(st == ST_SUCCESS && cnt == 8, "WRITE 8 bytes to the base data stream");

    /* Only the unnamed default stream exists so far. */
    entries = enum_streams(c, base.file_id, NULL, NULL, NULL);
    CHECK(entries == 1, "enumeration reports only the default stream (%d)",
          entries);

    /* Create an alternate named stream via the file:stream CREATE syntax and
     * give it content distinct from the base. */
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &alt);
    CHECK(st == ST_SUCCESS, "CREATE ads.bin:alt -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_write(c, alt.file_id, 0, "ALTERNATE", 9, &cnt);
        CHECK(st == ST_SUCCESS && cnt == 9, "WRITE 9 bytes to :alt");

        /* Read the stream back through its own handle: byte-identical. */
        st = smb2_read(c, alt.file_id, 0, sizeof(rd), rd, &rlen);
        CHECK(st == ST_SUCCESS && rlen == 9 && memcmp(rd, "ALTERNATE", 9) == 0,
              "READ :alt returns its 9 bytes byte-identical (0x%08x, %u)", st,
              rlen);
        smb2_close(c, alt.file_id);
    }

    /* Enumeration now lists the default stream plus :alt at 9 bytes. */
    entries = enum_streams(c, base.file_id, "alt", &sz, &found);
    CHECK(entries == 2, "enumeration lists both streams (%d)", entries);
    CHECK(found && sz == 9, ":alt is named and carries its 9 bytes (sz=%llu)",
          (unsigned long long) sz);

    /* A second named stream is independent again. */
    st = smb2_create(c, "ads.bin:beta", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &beta);
    CHECK(st == ST_SUCCESS, "CREATE ads.bin:beta -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_write(c, beta.file_id, 0, "BB", 2, &cnt);
        smb2_close(c, beta.file_id);
    }
    entries = enum_streams(c, base.file_id, "beta", &sz, &found);
    CHECK(entries == 3, "enumeration lists the default + two streams (%d)",
          entries);
    CHECK(found && sz == 2, ":beta carries its 2 bytes (sz=%llu)",
          (unsigned long long) sz);

    /* Base/stream independence: the base data stream still reads back its own
     * 8 bytes, untouched by the stream writes. */
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 8 && memcmp(rd, "BASEDATA", 8) == 0,
          "the base data stream is intact after the stream writes (%u)", rlen);

    /* Delete :alt via delete-on-close disposition on its own handle
     * (MBT_FILE_ALL_ACCESS already includes the DELETE right). */
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS, "re-open :alt for delete -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_set_disposition(c, reopen.file_id, 1);
        CHECK(st == ST_SUCCESS, "set delete-on-close on :alt -> 0x%08x", st);
        smb2_close(c, reopen.file_id);
    }

    /* :alt is gone; the default stream and :beta survive. */
    entries = enum_streams(c, base.file_id, "alt", &sz, &found);
    CHECK(entries == 2, "after delete the enumeration lists two streams (%d)",
          entries);
    CHECK(!found, ":alt no longer appears in the enumeration");

    /* Opening the deleted stream by name is refused. */
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st != ST_SUCCESS, "opening the deleted stream :alt is refused (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }

    /* Overwriting one stream preserves the sibling and base forks. */
    st = smb2_create(c, "ads.bin:beta", MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS && reopen.end_of_file == 0,
          "stream overwrite reports the new zero length");
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 8 && memcmp(rd, "BASEDATA", 8) == 0,
          "stream overwrite preserves base bytes");
    entries = enum_streams(c, base.file_id, "beta", &sz, &found);
    CHECK(entries == 2 && found && sz == 0,
          "stream overwrite retains its stream entry");

    /* A base overwrite removes alternate streams, as OPEN_TRUNCATE did. */
    st = smb2_create(c, "ads.bin", MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS && reopen.end_of_file == 0,
          "base overwrite reports the new zero length");
    if (st == ST_SUCCESS) {
        entries = enum_streams(c, reopen.file_id, "beta", &sz, &found);
        CHECK(entries == 1 && !found, "base overwrite removes alternate streams");
        smb2_close(c, reopen.file_id);
    }
    smb2_close(c, base.file_id);
} /* probe_stream_lifecycle */
/* A failed overwrite must leave bytes intact.  Include metadata-only opens:
* truncation still needs a transient WRITE share grant for these handles. */
static void
check_refused_overwrites(
    struct smb2_conn *c,
    const char       *name)
{
    const uint32_t         dispositions[] = { MBT_FILE_OVERWRITE, MBT_FILE_OVERWRITE_IF, MBT_FILE_SUPERSEDE };
    const uint32_t         accesses[]     = { MBT_FILE_ALL_ACCESS, MBT_FILE_READ_ATTRIBUTES };
    const char             payload[]      = "preserve";
    struct smb2_create_out held, refused, accepted;
    uint8_t                bytes[32];
    uint32_t               count, len, st;

    st = smb2_create(c, name, MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_READ, NULL, &held);
    CHECK(st == ST_SUCCESS, "overwrite fixture %s opens", name);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, held.file_id, 0, payload, 8, &count);
    CHECK(st == ST_SUCCESS && count == 8, "overwrite fixture %s has data", name);
    for (unsigned int i = 0; i < sizeof(dispositions) / sizeof(dispositions[0]); i++) {
        for (unsigned int j = 0; j < sizeof(accesses) / sizeof(accesses[0]); j++) {
            st = smb2_create(c, name, dispositions[i], accesses[j],
                             MBT_FILE_SHARE_RWD, NULL, &refused);
            CHECK(st == ST_SHARING_VIOLATION,
                  "overwrite %s disposition%u access%u denied", name, dispositions[i], accesses[j]);
            if (st == ST_SUCCESS) {
                smb2_close(c, refused.file_id);
            }
            st = smb2_read(c, held.file_id, 0, 8, bytes, &len);
            CHECK(st == ST_SUCCESS && len == 8 && memcmp(bytes, payload, 8) == 0,
                  "refused overwrite %s preserves bytes", name);
        }
    }
    smb2_close(c, held.file_id);
    st = smb2_create(c, name, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &accepted);
    CHECK(st == ST_SUCCESS && accepted.end_of_file == 0,
          "admitted overwrite %s truncates and reports EOF0", name);
    if (st == ST_SUCCESS) {
        smb2_close(c, accepted.file_id);
    }
} /* check_refused_overwrites */

static void
probe_stream_last_close(struct smb2_conn *c)
{
    struct smb2_create_out base, owner, peer, lookup;
    uint32_t               st, count, length;
    uint8_t                bytes[8];
    char                   base_name[64], stream_name[96];

    printf("# --- named-stream last-close ownership ---\n");
    for (int mode = 0; mode < 5; mode++) {
        struct smb2_conn *peer_conn = smb2_conn_open(c->env);
        smb2_handshake(peer_conn);
        snprintf(base_name, sizeof(base_name), "stream-last-close-%d", mode);
        snprintf(stream_name, sizeof(stream_name), "%s:held:$DATA", base_name);
        st = smb2_create(c, base_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &base);
        CHECK(st == ST_SUCCESS, "stream last-close base setup %d", mode);
        if (st != ST_SUCCESS) {
            continue;
        }
        st = smb2_create_opts(c, stream_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                              MBT_FILE_SHARE_RWD, (mode == 1 || mode == 4) ? MBT_FILE_DELETE_ON_CLOSE : 0,
                              NULL, &owner);
        CHECK(st == ST_SUCCESS, "stream last-close owner setup %d", mode);
        if (st != ST_SUCCESS) {
            smb2_close(c, base.file_id);
            continue;
        }
        st = smb2_write(c, owner.file_id, 0, "keep", 4, &count);
        CHECK(st == ST_SUCCESS && count == 4, "stream last-close data setup %d", mode);
        st = smb2_create(peer_conn, stream_name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &peer);
        CHECK(st == ST_SUCCESS, "same-stream peer setup %d", mode);
        if (st != ST_SUCCESS) {
            smb2_close(c, owner.file_id);
            smb2_close(c, base.file_id);
            continue;
        }
        if (mode == 4) {
            st = smb2_set_disposition(c, owner.file_id, 0);
            CHECK(st == ST_SUCCESS, "plain stream clear preserves CREATE delete mode");
            uint8_t  standard[24];
            uint32_t standard_len = 0;
            st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                                 owner.file_id, 0, standard, sizeof(standard), &standard_len);
            CHECK(st == ST_SUCCESS && standard_len == sizeof(standard) && standard[20] == 0,
                  "stream clear reports no pending deletion before CREATE-mode close");
        } else if (mode != 1) {
            st = smb2_set_disposition(c, owner.file_id, 1);
            CHECK(st == ST_SUCCESS, "mark stream delete pending %d", mode);
        }
        st = smb2_close(c, owner.file_id);
        CHECK(st == ST_SUCCESS, "stream delete owner closes %d", mode);
        st = smb2_create(c, stream_name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_DELETE_PENDING,
              "stream name stays pending while peer lives %d -> 0x%08x", mode, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
        st = smb2_create(c, stream_name, MBT_FILE_OVERWRITE, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_DELETE_PENDING,
              "pending-stream overwrite is rejected before truncation %d -> 0x%08x", mode, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
        uint8_t  standard[24];
        uint32_t standard_len = 0;
        st = smb2_query_info(peer_conn, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                             peer.file_id, 0, standard, sizeof(standard), &standard_len);
        CHECK(st == ST_SUCCESS && standard_len == sizeof(standard) && standard[20] == 1,
              "surviving stream peer reports shared delete-pending %d", mode);
        st = smb2_read(peer_conn, peer.file_id, 0, sizeof(bytes), bytes, &length);
        CHECK(st == ST_SUCCESS && length == 4 && memcmp(bytes, "keep", 4) == 0,
              "pending stream peer retains data %d", mode);
        if (mode == 2) {
            st = smb2_set_disposition(peer_conn, peer.file_id, 0);
            CHECK(st == ST_SUCCESS, "peer cancels stream deletion after owner closes");
        }
        if (mode == 3) {
            smb2_conn_disconnect(peer_conn);
        } else {
            st = smb2_close(peer_conn, peer.file_id);
            CHECK(st == ST_SUCCESS, "last stream peer close %d", mode);
        }
        uint64_t deadline = smb2c_now_ms() + 5000;
        do {
            st = smb2_create(c, stream_name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                             MBT_FILE_SHARE_RWD, NULL, &lookup);
            if (mode != 3 || st != ST_DELETE_PENDING) {
                break;
            }
            smb2_pump(c->env);
        } while (smb2c_now_ms() < deadline);
        CHECK(st == (mode == 2 ? ST_SUCCESS : ST_OBJECT_NAME_NOT_FOUND),
              "last stream close/disconnect honors deletion or cancellation %d -> 0x%08x", mode, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
        smb2_close(c, base.file_id);
        st = smb2_create(c, base_name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_SUCCESS, "stream deletion preserves base name %d", mode);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
    }
} /* probe_stream_last_close */

static void
probe_checked_stream_delete(struct smb2_conn *c)
{
    struct smb2_create_out base, stream, replacement;

    CHECK(smb2_create(c, "checked-stream", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                      MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS, "create checked stream base");
    for (unsigned mode = 1; mode <= 2; mode++) {
        CHECK(smb2_create(c, "checked-stream:fork", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS, "create checked stream mode %u", mode);
        CHECK(smb2_set_disposition(c, stream.file_id, 1) == ST_SUCCESS,
              "set checked stream deletion mode %u", mode);
        atomic_store(&stream_test_submissions, 0);
        atomic_store(&stream_test_notifications, 0);
        atomic_store(&stream_test_held, 0);
        atomic_store(&stream_test_mode, mode);
        int b = smb2c_begin(c, SMB2_CLOSE, 0);
        p16(c->sbuf + b, 0, 24);
        memcpy(c->sbuf + b + 8, stream.file_id, 16);
        smb2c_send(c, 24);
        if (mode == 1) {
            uint64_t deadline = smb2c_now_ms() + 5000;
            while (!atomic_load(&stream_test_held) && smb2c_now_ms() < deadline) {
                smb2_pump(c->env);
            }
            CHECK(atomic_load(&stream_test_held), "checked stream delete reaches held acceptance");
            CHECK(!atomic_load(&stream_test_notifications), "stream removal has no pre-accept notification");
            CHECK(!c->reply_ready, "CLOSE waits for checked stream acceptance");
            atomic_store(&stream_test_release, 1);
        }
        uint32_t status = smb2c_wait(c);
        CHECK(atomic_load(&stream_test_submissions) == 1, "CLOSE uses one checked stream compound mode %u", mode);
        CHECK(atomic_load(&stream_test_notifications) == (mode == 1 ? 1 : 0),
              "accepted stream notify exactly once, rebound source never notifies mode %u", mode);
        if (mode == 1) {
            CHECK(status == ST_SUCCESS, "accepted checked stream CLOSE succeeds");
        }
        atomic_store(&stream_test_mode, 0);
        status = smb2_create(c, "checked-stream:fork", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                             MBT_FILE_SHARE_RWD, NULL, &replacement);
        CHECK(status == (mode == 1 ? ST_OBJECT_NAME_NOT_FOUND : ST_SUCCESS),
              "checked stream unlink preserves replacement identity mode %u -> 0x%08x", mode, status);
        if (status == ST_SUCCESS) {
            uint32_t count = 0, length = 0;
            uint8_t  data[8];
            CHECK(smb2_write(c, replacement.file_id, 0, "new", 3, &count) == ST_SUCCESS && count == 3,
                  "replacement stream remains writable");
            CHECK(smb2_read(c, replacement.file_id, 0, sizeof(data), data, &length) == ST_SUCCESS &&
                  length == 3 && !memcmp(data, "new", 3), "replacement stream data survives stale DOC");
            smb2_close(c, replacement.file_id);
        }
    }
    smb2_close(c, base.file_id);
} /* probe_checked_stream_delete */

static void
probe_stream_compound_close(struct smb2_conn *c)
{
    struct smb2_create_out base, stream, denied, reopened;
    uint32_t               count = 0;

    assert(smb2_create(c, "compound-stream", MBT_FILE_CREATE, MBT_FILE_READ_ATTRIBUTES,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_create(c, "compound-stream:fork", MBT_FILE_CREATE,
                       MBT_FILE_READ_ACCESS | MBT_FILE_WRITE_ACCESS,
                       MBT_FILE_SHARE_READ | MBT_FILE_SHARE_WRITE, NULL, &stream) == ST_SUCCESS);
    assert(smb2_write(c, stream.file_id, 0, "payload", 7, &count) == ST_SUCCESS && count == 7);
    assert(smb2_create(c, "compound-stream", MBT_FILE_OPEN, 0x00010000u /* DELETE */,
                       MBT_FILE_SHARE_RWD, NULL, &denied) == ST_SHARING_VIOLATION);

    uint8_t  packet[512] = { 0 };
    uint32_t length      = 0;
    uint64_t first_mid   = c->msg_id;
    for (unsigned i = 0; i < 3; i++) {
        uint8_t *h = packet + length, *b = h + SMB2_HDR_SIZE;
        memcpy(h, "\xfeSMB", 4);
        p16(h, 4, SMB2_HDR_SIZE); p16(h, 6, 1);
        p16(h, 12, i == 1 ? SMB2_CLOSE : SMB2_QUERY_INFO);
        p16(h, 14, 32); p64(h, 24, first_mid + i);
        p32(h, 36, c->tree_id); p64(h, 40, c->session_id);
        uint32_t span;
        if (i == 1) {
            p16(b, 0, 24); p16(b, 2, 1); /* POSTQUERY */
            memcpy(b + 8, stream.file_id, 16);
            span = SMB2_HDR_SIZE + 24;
        } else {
            p16(b, 0, 41); b[2] = SMB2_INFO_FILE_T; b[3] = SMB2_FILE_STANDARD_INFO_T;
            p32(b, 4, 256); memcpy(b + 24, i ? base.file_id : stream.file_id, 16);
            span = SMB2_HDR_SIZE + 40;
        }
        if (i != 2) {
            p32(h, 20, span);
        }
        if (c->signing_on) {
            uint8_t signature[16];
            p32(h, 16, SMB2_FLAGS_SIGNED);
            smb2w_sign(c->dialect, c->signing_alg, c->signing_key, h, span, signature);
            memcpy(h + 48, signature, 16);
        }
        length += span;
    }
    memcpy(c->sbuf + 4, packet, length);
    atomic_store(&stream_close_submissions, 0);
    atomic_store(&stream_close_finishes, 0);
    atomic_store(&stream_close_notifications, 0);
    atomic_store(&stream_close_armed, 1);
    int signing = c->signing_on;
    c->signing_on = 0;
    smb2c_send(c, length - SMB2_HDR_SIZE);
    c->signing_on = signing;
    c->msg_id     = first_mid + 3;
    (void) smb2c_wait(c);
    atomic_store(&stream_close_armed, 0);
    assert(atomic_load(&stream_close_submissions) == 1);
    assert(atomic_load(&stream_close_finishes) == 2);
    assert(atomic_load(&stream_close_notifications) == 1);
    uint32_t offset = 4;
    for (unsigned i = 0; i < 3; i++) {
        assert(offset + SMB2_HDR_SIZE + 8 <= (uint32_t) c->rlen);
        const uint8_t *h = c->rbuf + offset, *b = h + SMB2_HDR_SIZE;
        assert(g32(h, 8) == ST_SUCCESS && g64(h, 24) == first_mid + i);
        uint32_t       next = g32(h, 20), span = next ? next : c->rlen - offset;
        if (i != 1) {
            assert(g32(b, 4) >= 24 && g16(b, 2) + g32(b, 4) <= span);
            assert(g64(h + g16(b, 2), 8) == (i ? 0 : 7));
        } else {
            assert(span >= SMB2_HDR_SIZE + 60 && g16(b, 0) == 60 && (g16(b, 2) & 1));
            assert(g64(b, 48) == 7);
        }
        assert((i != 2) == (next != 0));
        offset += span;
    }
    assert(offset == (uint32_t) c->rlen);
    assert(smb2_create(c, "compound-stream", MBT_FILE_OPEN, 0x00010000u /* DELETE */,
                       MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_SUCCESS);
    assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "compound-stream:fork", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &reopened) == ST_SUCCESS);
    uint8_t bytes[8];
    assert(smb2_read(c, reopened.file_id, 0, sizeof(bytes), bytes, &count) == ST_SUCCESS);
    assert(count == 7 && !memcmp(bytes, "payload", 7));
    assert(smb2_close(c, reopened.file_id) == ST_SUCCESS);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);
    printf("ok   - stream QUERY/CLOSE/base QUERY coalesce and retry with both share claims retired\n");
} /* probe_stream_compound_close */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    /* Named streams are off in the server by default; the ADS surface needs
     * them advertised (CAP_NAMED_STREAMS on the backend + the config knob). */
    struct smb2_env_opts opts = { .named_streams = 1 };
    struct smb2_conn    *c;

    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# dialect=0x%04x\n", c->dialect);

    probe_stream_lifecycle(c);
    probe_stream_last_close(c);
    probe_checked_stream_delete(c);
    probe_stream_compound_close(c);

    check_refused_overwrites(c, "overwrite-base:stream:$DATA");

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d ADS stream check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 named-stream checks passed\n");
    return 0;
} /* main */
