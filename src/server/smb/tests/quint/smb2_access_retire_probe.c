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
#include "vfs/vfs_claim_access.h"

/* Hold a real canonical ACCESS edit while a second wire client triggers the
 * legacy AppInstance failover path. This exercises the lifetime prerequisite
 * for public compound CLOSE without pretending that its mapper is enabled. */
extern void * smb_access_retire_test_handle(
    void *open);
extern int smb_access_retire_test_closed(
    void *open);
extern int smb_access_retire_test_timer(
    void              *open,
    struct evpl_timer *timer);

static atomic_int capture, delay_retirement, delayed_retirement;
static _Atomic(void *) delayed_open;
static _Atomic(struct chimera_vfs_claim_access_owner *) captured;

__attribute__((visibility("default"))) struct chimera_vfs_claim_access_owner *
chimera_vfs_claim_access_owner_alloc(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *template_claim)
{
    typedef struct chimera_vfs_claim_access_owner *(*alloc_fn)(
        struct chimera_vfs_file_state *,
        const struct chimera_vfs_claim *);
    alloc_fn                               next = (alloc_fn) dlsym(RTLD_NEXT, "chimera_vfs_claim_access_owner_alloc");
    assert(next);
    struct chimera_vfs_claim_access_owner *owner = next(file, template_claim);
    if (owner && template_claim->cb_private && atomic_exchange(&capture, 0)) {
        chimera_vfs_claim_access_owner_ref(owner);
        atomic_store(&captured, owner);
    }
    return owner;
} /* chimera_vfs_claim_access_owner_alloc */

/* A long timer makes the shutdown regression deterministic: only explicit
 * destructor draining, not a lucky timer tick, can reclaim this retirement. */
__attribute__((visibility("default"))) void
evpl_add_oneshot_timer(
    struct evpl          *evpl,
    struct evpl_timer    *timer,
    evpl_timer_callback_t callback,
    uint64_t              delay_us)
{
    typedef void (*timer_fn)(
        struct evpl *,
        struct evpl_timer *,
        evpl_timer_callback_t,
        uint64_t);
    timer_fn next = (timer_fn) dlsym(RTLD_NEXT, "evpl_add_oneshot_timer");
    assert(next);
    if (atomic_load(&delay_retirement) &&
        smb_access_retire_test_timer(atomic_load(&delayed_open), timer)) {
        delay_us = UINT64_C(3600000000);
        atomic_fetch_add(&delayed_retirement, 1);
    }
    next(evpl, timer, callback, delay_us);
} /* evpl_add_oneshot_timer */

static const uint8_t app_guid[16] = {
    0x45, 0xbc, 0xa6, 0x6a, 0xef, 0xa7, 0xf7, 0x4a,
    0x90, 0x08, 0xfa, 0x46, 0x2e, 0x14, 0x4d, 0x74
};

static uint32_t
app_open(
    struct smb2_conn       *conn,
    const char             *name,
    uint32_t                disposition,
    struct smb2_create_out *result)
{
    uint8_t          app[20] = { 0 };

    p32(app, 0, sizeof(app));
    memset(app + 4, 0x39, 16);
    struct smb2_cctx context = { app_guid, sizeof(app_guid), app, sizeof(app) };
    smb2c_send(conn, smb2c_build_create_full(conn, name, disposition,
                                             MBT_FILE_ALL_ACCESS, 0, MBT_FILE_NON_DIRECTORY_FILE, NULL, &context, 1));
    smb2c_wait(conn);
    smb2c_parse_create(conn, result);
    return result->status;
} /* app_open */

static void
pump_timers(struct smb2_env *env)
{
    struct timespec start, now;

    clock_gettime(CLOCK_MONOTONIC, &start);
    do {
        smb2_pump(env);
        clock_gettime(CLOCK_MONOTONIC, &now);
    } while ((now.tv_sec - start.tv_sec) * INT64_C(1000000000) +
             now.tv_nsec - start.tv_nsec < INT64_C(10000000));
} /* pump_timers */

static void
run(
    struct smb2_env  *env,
    struct smb2_conn *first,
    struct smb2_conn *second,
    const char       *name,
    int               accepted)
{
    struct smb2_create_out                   original, replacement;

    atomic_store(&captured, NULL);
    atomic_store(&capture, 1);
    assert(app_open(first, name, MBT_FILE_CREATE, &original) == ST_SUCCESS);
    struct chimera_vfs_claim_access_owner   *owner = atomic_load(&captured);
    assert(owner);
    struct chimera_vfs_claim                *claim = chimera_vfs_claim_access_owner_claim(owner);
    void                                    *open  = claim->cb_private;
    assert(open && smb_access_retire_test_handle(open));
    void                                    *handle  = smb_access_retire_test_handle(open);
    struct chimera_vfs_claim_access_journal *journal = chimera_vfs_claim_access_journal_alloc(1);
    assert(journal);
    assert(chimera_vfs_claim_access_journal_retire(journal, owner) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_journal_seal(journal);

    /* Admission still sees the pinned public reservation. Failover cuts off
     * FileId lookup, but cannot destroy the journal's frontend anchors. */
    assert(app_open(second, name, MBT_FILE_OPEN, &replacement) == ST_SHARING_VIOLATION);
    assert(smb_access_retire_test_closed(open));
    assert(smb_access_retire_test_handle(open) == handle);
    assert(claim->cb_private == open && claim->op_handle == handle);
    assert(!chimera_vfs_claim_access_owner_is_retired(owner));
    assert(smb2_close(first, original.file_id) == ST_FILE_CLOSED);
    pump_timers(env);
    assert(smb_access_retire_test_handle(open) == handle); /* timer cannot bypass a held journal */

    if (accepted) {
        chimera_vfs_claim_access_journal_publish(journal);
        chimera_vfs_claim_access_journal_complete(journal);
    } else {
        chimera_vfs_claim_access_journal_reset(journal);
    }
    chimera_vfs_claim_access_journal_free(journal);
    /* Reset honors the independent AppInstance cutoff; it cannot resurrect a
     * logically closed owner. The token ref is safe after the open is freed. */
    assert(chimera_vfs_claim_access_owner_is_retired(owner));
    chimera_vfs_claim_access_owner_put(owner);
    assert(app_open(second, name, MBT_FILE_OPEN, &replacement) == ST_SUCCESS);
    assert(smb2_close(second, replacement.file_id) == ST_SUCCESS);
    pump_timers(env);
} /* run */

static struct smb2_conn *
park_pipe_read(struct smb2_env *env)
{
    struct smb2_conn      *conn = smb2_conn_open(env);

    smb2_handshake(conn);
    assert(smb2_tree_connect(conn, "\\\\server\\IPC$") == ST_SUCCESS);
    struct smb2_create_out pipe;
    assert(smb2_create(conn, "srvsvc", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &pipe) == ST_SUCCESS);
    int                    offset = smb2c_begin(conn, SMB2_READ, 0);
    uint8_t               *body   = conn->sbuf + offset;
    p16(body, 0, 49);
    body[2] = SMB2_HDR_SIZE + 16;
    p32(body, 4, 4096);
    memcpy(body + 16, pipe.file_id, 16);
    smb2c_send(conn, 49);
    while (!conn->interim_pending) {
        smb2_pump(env);
    }
    return conn;
} /* park_pipe_read */

static void
abandon_parked_create(struct smb2_env *env)
{
    struct smb2_conn      *holder = smb2_conn_open(env);
    struct smb2_conn      *waiter = smb2_conn_open(env);

    smb2_handshake(holder);
    smb2_handshake(waiter);
    struct smb2_oplock_req caching = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    struct smb2_create_out opened;
    assert(smb2_create(holder, "abandon-create.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, &caching, &opened) == ST_SUCCESS);
    assert(opened.oplock == SMB2_OPLOCK_LEVEL_BATCH);
    smb2_create_post(waiter, "abandon-create.txt", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL);
    uint64_t               deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!waiter->interim_pending) {
        assert(smb2c_now_ms() < deadline);
        smb2_pump(env);
    }
    /* No break acknowledgement: disconnect must retire the parked CREATE's
     * timer/open reference and its original wire compound, not orphan it. */
    smb2_conn_disconnect(waiter);
} /* abandon_parked_create */

static void
shutdown_pending(
    struct smb2_env  *env,
    struct smb2_conn *first,
    struct smb2_conn *second)
{
    struct smb2_create_out original, replacement;

    /* This READ has no VFS callback and must be completed by disconnect,
     * otherwise exact wire-lifetime shutdown accounting would wait forever. */
    (void) park_pipe_read(env);
    abandon_parked_create(env);
    atomic_store(&captured, NULL);
    atomic_store(&capture, 1);
    assert(app_open(first, "app-shutdown.txt", MBT_FILE_CREATE, &original) == ST_SUCCESS);
    struct chimera_vfs_claim_access_owner   *owner = atomic_load(&captured);
    assert(owner);
    void                                    *open    = chimera_vfs_claim_access_owner_claim(owner)->cb_private;
    struct chimera_vfs_claim_access_journal *journal = chimera_vfs_claim_access_journal_alloc(1);
    assert(journal && chimera_vfs_claim_access_journal_retire(journal, owner) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_journal_seal(journal);
    atomic_store(&delayed_open, open);
    atomic_store(&delay_retirement, 1);
    assert(app_open(second, "app-shutdown.txt", MBT_FILE_OPEN, &replacement) == ST_SHARING_VIOLATION);
    assert(atomic_load(&delayed_retirement) > 0);
    assert(smb_access_retire_test_closed(open));
    chimera_vfs_claim_access_journal_reset(journal);
    chimera_vfs_claim_access_journal_free(journal);
    chimera_vfs_claim_access_owner_put(owner);
    /* No pump, CLOSE, unmount or sleep: a retirement timer remains parked one
     * hour away when the protocol worker begins destruction. */
    smb2_env_stop(env);
    atomic_store(&delay_retirement, 0);
    atomic_store(&delayed_open, NULL);
} /* shutdown_pending */

int
main(void)
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .oplocks = 1 };

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn    *first  = smb2_conn_open(&env);
    struct smb2_conn    *second = smb2_conn_open(&env);
    smb2_handshake(first);
    smb2_handshake(second); /* distinct default client GUIDs */
    run(&env, first, second, "app-access-abort.txt", 0);
    run(&env, first, second, "app-access-accept.txt", 1);
    shutdown_pending(&env, first, second);
    return 0;
} /* main */
