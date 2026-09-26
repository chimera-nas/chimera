// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Real FUSE handlers and VFS journal, with a synthetic kernel wire peer.
 * Finish rejection is injected only into typed local lock compounds, whose
 * tentative ranges the journal can abort. No filesystem mutation is rejected. */
#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <stdatomic.h>
#include <pthread.h>
#include "fuse_sim.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_lock.h"
#include "common/compound_retry.h"

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

static atomic_int      fixture_armed;
static atomic_uint     fixture_rejections;
static atomic_uint     fixture_finishes;
static atomic_bool     fixture_pause;
static atomic_bool     fixture_resume;
static _Atomic(struct chimera_vfs_compound *) fixture_held;
static pthread_mutex_t fixture_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  fixture_cond  = PTHREAD_COND_INITIALIZER;

static void
wait_held(void)
{
    struct timespec deadline;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 10;
    pthread_mutex_lock(&fixture_mutex);
    while (!atomic_load(&fixture_held)) {
        CHECK(pthread_cond_timedwait(&fixture_cond, &fixture_mutex, &deadline) == 0);
    }
    pthread_mutex_unlock(&fixture_mutex);
} /* wait_held */

struct resume_completion {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
};

static void
resume_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct resume_completion       *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;
    struct chimera_vfs_compound    *held     = atomic_exchange(&fixture_held, NULL);

    CHECK(held != NULL);
    free(ctx);
    callback(compound, arg);
    /* The control GETATTR runs on the same synthetic-mount worker as the
     * held operation. Do not finish a compound from the test-client thread. */
    chimera_vfs_compound_finish_result(held, CHIMERA_VFS_OK);
} /* resume_finish */

static void
lock_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    unsigned int left = atomic_load(&fixture_rejections);

    atomic_fetch_add(&fixture_finishes, 1);
    if (atomic_exchange(&fixture_pause, false)) {
        pthread_mutex_lock(&fixture_mutex);
        CHECK(atomic_exchange(&fixture_held, compound) == NULL);
        pthread_cond_broadcast(&fixture_cond);
        pthread_mutex_unlock(&fixture_mutex);
        return;
    }
    if (left) {
        atomic_fetch_sub(&fixture_rejections, 1);
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_EAGAIN);
    } else {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
    }
} /* lock_finish */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    static void (*real_submit)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    bool        local_lock = false;

    if (!real_submit) {
        real_submit = dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
        CHECK(real_submit != NULL);
    }
    for (unsigned int i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_LOCK_TEST ||
            op->type == CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE) {
            local_lock = true;
        }
    }
    if (local_lock && atomic_exchange(&fixture_armed, 0)) {
        chimera_vfs_compound_set_finish_handler(compound, lock_finish, NULL);
    }
    if (!local_lock && atomic_exchange(&fixture_resume, false)) {
        struct resume_completion *ctx = malloc(sizeof(*ctx));
        CHECK(ctx != NULL);
        ctx->callback     = callback;
        ctx->private_data = private_data;
        real_submit(compound, resume_finish, ctx);
        return;
    }
    real_submit(compound, callback, private_data);
} /* chimera_vfs_compound_submit */

static void
arm(unsigned int rejections)
{
    atomic_store(&fixture_finishes, 0);
    atomic_store(&fixture_rejections, rejections);
    atomic_store(&fixture_armed, 1);
} /* arm */

static int
change(
    struct fuse_sim             *sim,
    const struct fuse_entry_out *entry,
    const struct fuse_open_out  *file,
    uint64_t                     owner,
    uint32_t                     type,
    uint64_t                     start,
    uint64_t                     end)
{
    return fuse_sim_lock(sim, entry->nodeid, file->fh, owner, FUSE_SETLK,
                         type, start, end, 0, NULL);
} /* change */

static struct fuse_file_lock
probe(
    struct fuse_sim             *sim,
    const struct fuse_entry_out *entry,
    const struct fuse_open_out  *file,
    uint64_t                     owner,
    uint64_t                     start,
    uint64_t                     end)
{
    struct fuse_file_lock conflict;

    CHECK(fuse_sim_lock(sim, entry->nodeid, file->fh, owner, FUSE_GETLK,
                        F_WRLCK, start, end, 0, &conflict) == 0);
    return conflict;
} /* probe */

/* Send a parked request then a GETATTR barrier. A successful barrier proves
 * the worker has admitted the lock and is servicing later requests while it
 * waits; no timing-based sleep is needed to establish the race setup. */
static uint64_t
park(
    struct fuse_sim             *sim,
    const struct fuse_entry_out *entry,
    const struct fuse_open_out  *file,
    uint64_t                     owner,
    uint64_t                     start,
    uint64_t                     end)
{
    struct fuse_lk_in    in = {
        .fh = file->fh, .owner = owner,
        .lk = { .start = start,.end   = end,  .type                              = F_WRLCK },
    };
    struct fuse_attr_out attr;
    uint64_t             unique;

    fuse_sim_send(sim, FUSE_SETLKW, entry->nodeid, &in, sizeof(in), NULL, NULL, 0);
    unique = sim->unique;
    CHECK(fuse_sim_getattr(sim, entry->nodeid, &attr) == 0);
    return unique;
} /* park */

/* Unlock/close can both reply and wake a waiter in either order. */
static void
receive_pair(
    struct fuse_sim *sim,
    uint64_t         first,
    int              first_error,
    uint64_t         second,
    int              second_error)
{
    bool first_seen = false, second_seen = false;

    while (!first_seen || !second_seen) {
        struct fuse_out_header *hdr = (struct fuse_out_header *) sim->buf;
        ssize_t                 len;
        do {
            len = read(sim->fd, sim->buf, sizeof(sim->buf));
        } while (len < 0 && errno == EINTR);
        CHECK(len >= (ssize_t) sizeof(*hdr));
        if (hdr->unique == 0) {
            fuse_sim_record_notify(sim, hdr, sim->buf + sizeof(*hdr), len - sizeof(*hdr));
            continue;
        }
        if (hdr->unique == first) {
            CHECK(!first_seen && -hdr->error == first_error);
            first_seen = true;
        } else {
            CHECK(hdr->unique == second && !second_seen && -hdr->error == second_error);
            second_seen = true;
        }
    }
} /* receive_pair */

int
main(void)
{
    struct fuse_sim       sim;
    struct fuse_entry_out entry;
    struct fuse_open_out  file;
    struct fuse_file_lock conflict;
    uint64_t              pending, action;

    setvbuf(stdout, NULL, _IONBF, 0);
    fuse_sim_open(&sim, "lockfs");
    CHECK(fuse_sim_create(&sim, FUSE_ROOT_ID, "locks", 0600, &entry, &file) == 0);

    /* Ordinary range coverage, splitting, replacement and EOF formatting. */
    CHECK(change(&sim, &entry, &file, 1, F_WRLCK, 10, 99) == 0);
    conflict = probe(&sim, &entry, &file, 2, 0, 200);
    CHECK(conflict.type == F_WRLCK && conflict.start == 10 && conflict.end == 99);
    CHECK(probe(&sim, &entry, &file, 1, 0, 200).type == F_UNLCK);
    CHECK(change(&sim, &entry, &file, 1, F_UNLCK, 30, 69) == 0);
    CHECK(probe(&sim, &entry, &file, 2, 30, 69).type == F_UNLCK);
    conflict = probe(&sim, &entry, &file, 2, 10, 29);
    CHECK(conflict.type == F_WRLCK && conflict.start == 10 && conflict.end == 29);
    conflict = probe(&sim, &entry, &file, 2, 70, 99);
    CHECK(conflict.type == F_WRLCK && conflict.start == 70 && conflict.end == 99);
    CHECK(change(&sim, &entry, &file, 1, F_RDLCK, 70, 99) == 0);
    CHECK(probe(&sim, &entry, &file, 2, 70, 99).type == F_RDLCK);
    CHECK(change(&sim, &entry, &file, 1, F_WRLCK, 1000, FUSE_SIM_LOCK_EOF) == 0);
    conflict = probe(&sim, &entry, &file, 2, 2000, FUSE_SIM_LOCK_EOF);
    CHECK(conflict.type == F_WRLCK && conflict.start == 1000 && conflict.end == FUSE_SIM_LOCK_EOF);

    /* Finish rejection reruns GETLK and stages mutation until acceptance. */
    arm(2);
    CHECK(probe(&sim, &entry, &file, 2, 10, 29).type == F_WRLCK);
    CHECK(atomic_load(&fixture_finishes) == 3);
    struct fuse_lk_in  query = {
        .fh = file.fh, .owner = 2,
        .lk = { .start = 10,.end   = 29,.type                           = F_WRLCK },
    };
    struct fuse_lk_out rejected_reply;
    size_t             rejected_length = sizeof(rejected_reply);
    arm(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(fuse_sim_call(&sim, FUSE_GETLK, entry.nodeid, &query, sizeof(query),
                        NULL, NULL, 0, &rejected_reply, sizeof(rejected_reply),
                        &rejected_length) == EAGAIN);
    CHECK(rejected_length == 0);
    arm(2);
    CHECK(change(&sim, &entry, &file, 1, F_WRLCK, 200, 299) == 0);
    CHECK(atomic_load(&fixture_finishes) == 3);
    CHECK(probe(&sim, &entry, &file, 2, 200, 299).type == F_WRLCK);

    arm(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(change(&sim, &entry, &file, 1, F_UNLCK, 200, 299) == EAGAIN);
    CHECK(atomic_load(&fixture_finishes) == CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(probe(&sim, &entry, &file, 2, 200, 299).type == F_WRLCK);
    arm(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(change(&sim, &entry, &file, 1, F_RDLCK, 200, 299) == EAGAIN);
    CHECK(probe(&sim, &entry, &file, 2, 200, 299).type == F_WRLCK);
    arm(CHIMERA_FRONTEND_COMPOUND_RETRIES + 1);
    CHECK(change(&sim, &entry, &file, 3, F_WRLCK, 300, 399) == EAGAIN);
    CHECK(probe(&sim, &entry, &file, 2, 300, 399).type == F_UNLCK);
    arm(0);
    CHECK(change(&sim, &entry, &file, 2, F_WRLCK, 200, 299) == EAGAIN);
    CHECK(atomic_load(&fixture_finishes) == 1); /* operation conflict, no retry */
    arm(2);
    CHECK(change(&sim, &entry, &file, 1, F_UNLCK, 200, 299) == 0);
    CHECK(atomic_load(&fixture_finishes) == 3);
    CHECK(probe(&sim, &entry, &file, 2, 200, 299).type == F_UNLCK);

    /* While an unlock is fully executed but its finish is undecided, GETLK
     * from another owner must still observe the old committed range. */
    CHECK(change(&sim, &entry, &file, 1, F_WRLCK, 400, 499) == 0);
    struct fuse_lk_in held_unlock = {
        .fh = file.fh, .owner = 1,
        .lk = { .start = 400,.end   = 499,.type                       = F_UNLCK },
    };
    arm(0);
    atomic_store(&fixture_pause, true);
    fuse_sim_send(&sim, FUSE_SETLK, entry.nodeid, &held_unlock,
                  sizeof(held_unlock), NULL, NULL, 0);
    pending = sim.unique;
    wait_held();
    CHECK(probe(&sim, &entry, &file, 2, 400, 499).type == F_WRLCK);
    CHECK(atomic_load(&fixture_held) != NULL);
    struct fuse_getattr_in control = { 0 };
    atomic_store(&fixture_resume, true);
    fuse_sim_send(&sim, FUSE_GETATTR, entry.nodeid, &control,
                  sizeof(control), NULL, NULL, 0);
    action = sim.unique;
    receive_pair(&sim, pending, 0, action, 0);
    CHECK(probe(&sim, &entry, &file, 2, 400, 499).type == F_UNLCK);

    /* Close must also detach an added provisional reservation whose finish
    * has not returned. Another owner can acquire immediately, and the older
    * attempt cannot publish over it when its finish eventually accepts. */
    struct fuse_lk_in held_write = {
        .fh = file.fh, .owner = 4,
        .lk = { .start = 600,.end   = 699,.type               = F_WRLCK },
    };
    arm(0);
    atomic_store(&fixture_pause, true);
    fuse_sim_send(&sim, FUSE_SETLK, entry.nodeid, &held_write,
                  sizeof(held_write), NULL, NULL, 0);
    pending = sim.unique;
    wait_held();
    CHECK(probe(&sim, &entry, &file, 5, 600, 699).type == F_UNLCK);
    CHECK(atomic_load(&fixture_held) != NULL);
    CHECK(fuse_sim_flush(&sim, entry.nodeid, file.fh, 4) == 0);
    CHECK(change(&sim, &entry, &file, 5, F_WRLCK, 600, 699) == 0);
    atomic_store(&fixture_resume, true);
    fuse_sim_send(&sim, FUSE_GETATTR, entry.nodeid, &control,
                  sizeof(control), NULL, NULL, 0);
    action = sim.unique;
    receive_pair(&sim, pending, EINTR, action, 0);
    CHECK(probe(&sim, &entry, &file, 4, 600, 699).type == F_WRLCK);
    CHECK(change(&sim, &entry, &file, 5, F_UNLCK, 600, 699) == 0);

    /* A parked lock does not block unlock; grant and reply each happen once. */
    pending = park(&sim, &entry, &file, 2, 10, 29);
    struct fuse_lk_in unlock = {
        .fh = file.fh, .owner = 1, .lk = { .start = 10, .end = 29, .type = F_UNLCK },
    };
    fuse_sim_send(&sim, FUSE_SETLK, entry.nodeid, &unlock, sizeof(unlock), NULL, NULL, 0);
    action = sim.unique;
    receive_pair(&sim, pending, 0, action, 0);
    CHECK(probe(&sim, &entry, &file, 1, 10, 29).type == F_WRLCK);
    CHECK(change(&sim, &entry, &file, 2, F_UNLCK, 0, FUSE_SIM_LOCK_EOF) == 0);

    /* INTERRUPT cancels a parked request without releasing another owner's
     * lock; its no-reply protocol is preserved. */
    pending = park(&sim, &entry, &file, 2, 1000, FUSE_SIM_LOCK_EOF);
    struct fuse_interrupt_in interrupt = { .unique = pending };
    fuse_sim_send(&sim, FUSE_INTERRUPT, 0, &interrupt, sizeof(interrupt), NULL, NULL, 0);
    CHECK(fuse_sim_recv(&sim, pending, NULL, 0, NULL) == EINTR);
    CHECK(probe(&sim, &entry, &file, 2, 1000, FUSE_SIM_LOCK_EOF).type == F_WRLCK);

    /* Closing an owner with a queued request cuts off that admission even
     * though another owner retains the conflicting range. */
    pending = park(&sim, &entry, &file, 2, 1000, FUSE_SIM_LOCK_EOF);
    struct fuse_flush_in     flush = { .fh = file.fh, .lock_owner = 2 };
    fuse_sim_send(&sim, FUSE_FLUSH, entry.nodeid, &flush, sizeof(flush), NULL, NULL, 0);
    action = sim.unique;
    receive_pair(&sim, pending, EINTR, action, 0);
    CHECK(change(&sim, &entry, &file, 1, F_UNLCK, 0, FUSE_SIM_LOCK_EOF) == 0);
    CHECK(probe(&sim, &entry, &file, 3, 0, FUSE_SIM_LOCK_EOF).type == F_UNLCK);
    /* A genuinely new post-close request receives the new owner generation. */
    CHECK(change(&sim, &entry, &file, 2, F_WRLCK, 1000, FUSE_SIM_LOCK_EOF) == 0);
    CHECK(fuse_sim_flush(&sim, entry.nodeid, file.fh, 2) == 0);
    CHECK(probe(&sim, &entry, &file, 3, 0, FUSE_SIM_LOCK_EOF).type == F_UNLCK);

    /* Shutdown cancels outstanding waits even when RELEASE already removed
     * the open-file object. The parked request pins its own VFS handle. */
    CHECK(change(&sim, &entry, &file, 1, F_WRLCK, 0, 99) == 0);
    park(&sim, &entry, &file, 2, 0, 99);
    fuse_sim_release(&sim, entry.nodeid, file.fh);
    fuse_sim_close(&sim);
    puts("ok: FUSE typed locks retry, split, interrupt, release and close without resurrecting queued grants");
    return 0;
} /* main */
