// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Command groups preserve errors, isolate cursors/credentials and replay only
 * private results. Rejected attempts below perform no filesystem mutations. */
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#undef NDEBUG
#include <assert.h>
#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_notify.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_cred.h"
#include "common/logging.h"
#include "prometheus-c.h"

struct fixture {
    struct evpl *evpl;
    struct chimera_vfs *vfs;
    struct chimera_vfs_thread *thread;
    unsigned done, attempts, finishes, callbacks, logs;
    unsigned order[16];
    uint64_t token;
    uint8_t fh[CHIMERA_VFS_FH_SIZE];
    uint32_t fh_len;
    enum chimera_vfs_error mount_status;
    int dynamic_open;
};

static void
wait_done(struct fixture *f)
{
    while (!f->done) {
        evpl_continue(f->evpl);
    }
    f->done = 0;
}

static void
mounted(struct chimera_vfs_thread *thread, enum chimera_vfs_error status, void *private)
{
    struct fixture *f = private;
    (void) thread;
    f->mount_status = status;
    f->done = 1;
}

static void
looked_up(enum chimera_vfs_error status, struct chimera_vfs_attrs *attr, void *private)
{
    struct fixture *f = private;
    assert(status == CHIMERA_VFS_OK);
    f->fh_len = attr->va_fh_len;
    memcpy(f->fh, attr->va_fh, f->fh_len);
    f->done = 1;
}

static void
completed(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    (void) cp;
    f->callbacks++;
    f->done = 1;
}

static void
add_group(struct chimera_vfs_compound *cp, unsigned first, unsigned count,
          const struct chimera_vfs_cred *cred, int dependency, bool proceed,
          struct fixture *f)
{
    struct chimera_vfs_compound_group_config config = {
        .first_op = first, .num_ops = count, .cred = cred, .context = f,
        .dependency = dependency, .dependency_error = CHIMERA_VFS_EACCES,
        .continue_on_error = proceed,
    };
    assert(chimera_vfs_compound_add_group(cp, &config) >= 0);
}

static void
fail_again(struct chimera_vfs_compound *cp, uint32_t index,
           enum chimera_vfs_error *status, void *private)
{
    (void) cp; (void) index; (void) private;
    *status = CHIMERA_VFS_EAGAIN;
}

static void
record(struct chimera_vfs_compound *cp, uint32_t index,
       enum chimera_vfs_error *status, void *private)
{
    struct fixture *f = private;
    (void) cp; (void) status;
    assert(f->logs < 16);
    f->order[f->logs++] = index;
}

static void
append_suffix(struct chimera_vfs_compound *cp, uint32_t index,
              enum chimera_vfs_error *status, void *private)
{
    int added = chimera_vfs_compound_add_checkpoint(cp);
    assert(added >= 0);
    chimera_vfs_compound_set_op_callbacks(cp, added, NULL, record, private);
    if (index == 0) {
        record(cp, index, status, private);
    }
}

static void
attempt_reset(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    f->attempts++;
    f->logs = 0;
    assert(chimera_vfs_compound_num_ops(cp) == 3);
    for (unsigned i = 0; i < 3; i++) {
        assert(!chimera_vfs_compound_op(cp, i)->completed);
    }
    assert(chimera_vfs_compound_group_status(cp, 0) == CHIMERA_VFS_UNSET);
    assert(chimera_vfs_compound_group_status(cp, 1) == CHIMERA_VFS_UNSET);
}

static void
reject_first(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    f->finishes++;
    assert(!chimera_vfs_compound_cancel(cp)); /* finish already owns outcome */
    chimera_vfs_compound_finish_result(cp,
        f->finishes == 1 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}

static void
retry_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    f->callbacks++;
    assert(f->logs == 5);
    const unsigned expected[] = { 0, 1, 3, 4, 2 };
    assert(!memcmp(f->order, expected, sizeof(expected)));
    assert(chimera_vfs_compound_num_completed(cp) == 5);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    f->done = 1;
}

static void
park(struct chimera_vfs_compound *cp, uint32_t index, uint64_t token,
     const uint8_t *fh, uint32_t fh_len, void *private)
{
    struct fixture *f = private;
    (void) cp; (void) index;
    assert(fh_len == f->fh_len && !memcmp(fh, f->fh, fh_len));
    f->token = token;
}

static void
append_open(struct chimera_vfs_compound *cp, uint32_t index,
            enum chimera_vfs_error *status, void *private)
{
    struct fixture *f = private;
    (void) index;
    assert(*status == CHIMERA_VFS_OK);
    f->dynamic_open = chimera_vfs_compound_add_open(cp, NULL, 0,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, CHIMERA_VFS_ATTR_MODE);
    assert(f->dynamic_open >= 0);
}

static void
bind_dynamic_open(struct chimera_vfs_compound *cp, uint32_t index,
                  enum chimera_vfs_error *status, void *private)
{
    struct fixture *f = private;
    const struct chimera_vfs_compound_op *producer = chimera_vfs_compound_op(cp, f->dynamic_open);
    assert(f->dynamic_open > (int) index);
    if (!producer->completed || producer->status != CHIMERA_VFS_OK || !producer->out_handle) {
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    chimera_vfs_compound_op_args(cp, index)->in_handle = producer->out_handle;
}

static void
bind_dynamic_cursor(struct chimera_vfs_compound *cp, uint32_t index,
                    enum chimera_vfs_error *status, void *private)
{
    struct fixture *f = private;
    (void) status;
    assert(f->dynamic_open > (int) index);
    chimera_vfs_compound_op_use_handle(cp, index, f->dynamic_open);
}

static void
provenance_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    f->callbacks++;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!chimera_vfs_compound_op(cp, f->dynamic_open)->out_handle);
    assert(chimera_vfs_compound_op(cp, 3)->out_handle); /* independent GETHANDLE */
    assert(!chimera_vfs_compound_op(cp, 4)->close_handle); /* no external owner */
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    f->done = 1;
}

static void
append_forbidden_lock(struct chimera_vfs_compound *cp, uint32_t index,
                      enum chimera_vfs_error *status, void *private)
{
    (void) index; (void) status; (void) private;
    unsigned before = chimera_vfs_compound_num_ops(cp);
    /* The dynamic guard must reject before validating arguments, allocating
     * an op/lock attempt, or bypassing submit's dedicated-lock check. */
    assert(chimera_vfs_compound_add_lock_change(cp, NULL, NULL, NULL) < 0);
    assert(chimera_vfs_compound_num_ops(cp) == before);
}

struct range_compound_test {
    struct fixture *fixture;
    struct chimera_vfs_claim_owner *owner;
    struct chimera_vfs_open_handle handle;
    unsigned retired;
};

static void
range_retired(void *private)
{
    struct range_compound_test *test = private;
    test->retired++;
}

static void
range_view_check(struct chimera_vfs_compound *cp, uint32_t index,
                 enum chimera_vfs_error *status, void *private)
{
    struct range_compound_test *test = private;
    (void) status;
    assert(chimera_vfs_compound_io_denied(cp, &test->handle, 10, 1, false, NULL) == (index == 1));
}

static void
range_compound_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct range_compound_test *test = private;
    test->fixture->callbacks++;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, 0)->range_result.applied == 1);
    assert(chimera_vfs_compound_op(cp, 2)->range_result.applied == 1);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    /* A successfully published local lock journal is no longer replayable,
     * even if no handle/iovec was taken by the frontend. */
    assert(!chimera_vfs_compound_retry(cp));
    assert(chimera_vfs_claim_owner_retire(test->owner, range_retired, test));
    assert(!test->retired); /* deferred until accepted protocol publication/free */
    test->fixture->done = 1;
}

static void
cancel_prepare(struct chimera_vfs_compound *cp, uint32_t index,
               enum chimera_vfs_error *status, void *private)
{
    (void) index; (void) status; (void) private;
    assert(chimera_vfs_compound_cancel(cp));
}

struct access_compound_test {
    struct fixture *fixture;
    struct chimera_vfs_claim_access_owner *old, *taken;
    struct chimera_vfs_file_state *file, *taken_file;
    struct chimera_vfs_claim template;
    unsigned observations;
};

static void
access_observe(struct chimera_vfs_compound *cp, uint32_t index,
               enum chimera_vfs_error *status, void *private)
{
    struct access_compound_test *test = private;
    (void) index; (void) status;
    assert(chimera_vfs_claim_test(test->file, &test->template, NULL) == CHIMERA_CLAIM_DENIED);
    const struct chimera_vfs_compound_op *reserve = chimera_vfs_compound_op(cp, 3);
    assert(reserve->access_owner && reserve->claim_held);
    assert(reserve->claim != &test->template && !test->template.file);
    assert(!reserve->claim->admit_excluded && !reserve->claim->admit_num_excluded);
    assert(!reserve->claim->admission_cookie);
    test->observations++;
}

static void
access_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct access_compound_test *test = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(!chimera_vfs_compound_take_access_owner(cp, 3, &test->taken_file));
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(!chimera_vfs_compound_take_reservation(cp, 3)); /* transfer cannot orphan token */
    test->taken = chimera_vfs_compound_take_access_owner(cp, 3, &test->taken_file);
    assert(test->taken && test->taken_file == test->file);
    assert(!chimera_vfs_compound_retry(cp));
    assert(!chimera_vfs_claim_access_owner_is_retired(test->old));
    test->fixture->done = 1;
}

static void
check_access_compound(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct access_compound_test test = { .fixture = f };
    test.file = chimera_vfs_state_get(f->vfs->vfs_state, f->fh, f->fh_len,
        chimera_vfs_hash(f->fh, f->fh_len), true);
    struct chimera_claim_owner identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
                                            .client_key = 810, .owner_lo = 810 };
    struct chimera_vfs_claim old;
    chimera_vfs_claim_init_smb_open(&old, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
    test.old = chimera_vfs_claim_access_owner_alloc(test.file, &old);
    assert(test.old);
    assert(chimera_vfs_claim_try_acquire(f->vfs->vfs_state, test.file,
        chimera_vfs_claim_access_owner_claim(test.old), NULL) == CHIMERA_CLAIM_GRANTED);
    identity.owner_lo++;
    chimera_vfs_claim_init_smb_open(&test.template, CHIMERA_CLAIM_W, 0, &identity);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    assert(chimera_vfs_compound_add_retire_access(cp, test.old) == 0);
    add_group(cp, 0, 1, NULL, -1, true, f);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_open(cp, NULL, 0, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0);
    assert(chimera_vfs_compound_add_reserve_access(cp, 2, &test.template) == 3);
    chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_prepare(cp, 4, access_observe, &test);
    add_group(cp, 1, 4, NULL, -1, true, f);
    struct chimera_vfs_claim_access_fence fence = {0};
    assert(chimera_vfs_compound_set_admission_cookie(cp, &test));
    assert(chimera_vfs_claim_access_fence_acquire(&fence, test.file, &test));
    f->finishes = 0;
    chimera_vfs_compound_set_finish_handler(cp, reject_first, f);
    chimera_vfs_compound_submit(cp, access_complete, &test);
    wait_done(f);
    assert(test.observations == 2 && f->finishes == 2);
    chimera_vfs_compound_free(cp);
    chimera_vfs_claim_access_fence_release(&fence);
    assert(chimera_vfs_claim_access_owner_is_retired(test.old));
    chimera_vfs_claim_access_owner_put(test.old);
    chimera_vfs_claim_access_owner_retire(test.taken);
    chimera_vfs_claim_access_owner_put(test.taken);
    chimera_vfs_state_put(f->vfs->vfs_state, test.taken_file);
    chimera_vfs_state_put(f->vfs->vfs_state, test.file);
}

struct private_access_test {
    struct fixture *fixture;
    struct chimera_vfs_claim_access_owner *old;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim producer;
    unsigned mode, finishes;
    int reserve, ready, next;
};

static void
private_access_prepare(struct chimera_vfs_compound *cp, uint32_t index,
                       enum chimera_vfs_error *status, void *private)
{
    struct private_access_test *test = private;
    if ((int) index == test->ready) {
        if (test->mode == 1) chimera_vfs_compound_op_skip(cp, index);
    } else if (test->mode == 0 || test->mode == 4 || test->mode == 5) {
        *status = CHIMERA_VFS_EACCES;
    } else if (test->mode == 3) {
        assert(chimera_vfs_compound_cancel(cp));
    }
}

static void
private_access_append(struct chimera_vfs_compound *cp, uint32_t index,
                      enum chimera_vfs_error *status, void *private)
{
    struct private_access_test *test = private;
    (void) index; (void) status;
    test->reserve = chimera_vfs_compound_add_reserve_access(cp, 2, &test->producer);
    int failed = chimera_vfs_compound_add_checkpoint(cp);
    test->ready = chimera_vfs_compound_add_checkpoint(cp);
    assert(test->reserve > test->next); /* physical order differs from execution */
    chimera_vfs_compound_set_op_prepare(cp, failed, private_access_prepare, test);
    assert(chimera_vfs_compound_reserve_access_until(cp, test->reserve, test->ready));
}

static void
private_access_finish(struct chimera_vfs_compound *cp, void *private)
{
    struct private_access_test *test = private;
    const struct chimera_vfs_compound_op *reserve = chimera_vfs_compound_op(cp, test->reserve);
    bool retained = test->mode == 2 || test->mode == 4;
    assert(reserve->access_owner && reserve->claim_file == test->file);
    assert(reserve->claim_held == retained);
    assert((chimera_vfs_claim_access_owner_claim(reserve->access_owner)->file != NULL) == retained);
    /* An earlier successful reservation and CLOSE edit are both intact. */
    assert(chimera_vfs_compound_op(cp, 3)->claim_held);
    assert(chimera_vfs_claim_access_owner_claim(test->old)->file == test->file);
    const struct chimera_vfs_compound_op *next = chimera_vfs_compound_op(cp, test->next);
    if (test->mode == 3) {
        assert(!next->completed && chimera_vfs_compound_is_canceled(cp));
    } else {
        assert(next->completed && next->status == (retained ? CHIMERA_VFS_EACCES : CHIMERA_VFS_OK));
    }
    bool reject = (test->mode == 0 || test->mode == 5) && !test->finishes;
    test->finishes++;
    chimera_vfs_compound_finish_result(cp, reject ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}

static void
private_access_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct private_access_test *test = private;
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(!chimera_vfs_claim_access_owner_claim(test->old)->file);
    assert(chimera_vfs_compound_op(cp, 3)->claim_held);
    if (test->mode != 2 && test->mode != 4) {
        struct chimera_vfs_file_state *file = NULL;
        assert(!chimera_vfs_compound_take_access_owner(cp, test->reserve, &file));
    }
    test->fixture->done = 1;
}

static void
check_private_access_cleanup(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    for (unsigned mode = 0; mode < 6; mode++) {
        struct private_access_test test = { .fixture = f, .mode = mode };
        test.file = chimera_vfs_state_get(f->vfs->vfs_state, f->fh, f->fh_len,
            chimera_vfs_hash(f->fh, f->fh_len), true);
        struct chimera_claim_owner owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = 950, .owner_lo = 950 };
        struct chimera_vfs_claim old, earlier, next;
        chimera_vfs_claim_init_smb_open(&old, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &owner);
        test.old = chimera_vfs_claim_access_owner_alloc(test.file, &old);
        assert(test.old);
        assert(chimera_vfs_claim_try_acquire(f->vfs->vfs_state, test.file,
            chimera_vfs_claim_access_owner_claim(test.old), NULL) == CHIMERA_CLAIM_GRANTED);
        owner.owner_lo++;
        chimera_vfs_claim_init_smb_open(&earlier, CHIMERA_CLAIM_R, 0, &owner);
        owner.owner_lo++;
        chimera_vfs_claim_init_smb_open(&test.producer, CHIMERA_CLAIM_W, CHIMERA_CLAIM_W, &owner);
        owner.owner_lo++;
        chimera_vfs_claim_init_smb_open(&next, CHIMERA_CLAIM_W, 0, &owner);
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        assert(chimera_vfs_compound_add_retire_access(cp, test.old) == 0);
        add_group(cp, 0, 1, NULL, -1, true, f);
        chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
        assert(chimera_vfs_compound_add_open(cp, NULL, 0,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0) == 2);
        assert(chimera_vfs_compound_add_reserve_access(cp, 2, &earlier) == 3);
        add_group(cp, 1, 3, NULL, -1, true, f);
        if (mode == 5) {
            assert(chimera_vfs_compound_add_checkpoint(cp) == 4);
            chimera_vfs_compound_set_op_callbacks(cp, 4, NULL, private_access_append, &test);
            add_group(cp, 4, 1, NULL, -1, true, f);
        } else {
            test.reserve = chimera_vfs_compound_add_reserve_access(cp, 2, &test.producer);
            int failed = chimera_vfs_compound_add_checkpoint(cp);
            test.ready = chimera_vfs_compound_add_checkpoint(cp);
            chimera_vfs_compound_set_op_prepare(cp, failed, private_access_prepare, &test);
            chimera_vfs_compound_set_op_prepare(cp, test.ready, private_access_prepare, &test);
            if (mode != 4) assert(chimera_vfs_compound_reserve_access_until(cp, test.reserve, test.ready));
            add_group(cp, 4, 3, NULL, -1, true, f);
        }
        test.next = chimera_vfs_compound_add_reserve_access(cp, 2, &next);
        add_group(cp, test.next, 1, NULL, -1, true, f);
        chimera_vfs_compound_set_finish_handler(cp, private_access_finish, &test);
        chimera_vfs_compound_submit(cp, private_access_complete, &test);
        wait_done(f);
        assert(test.finishes == ((mode == 0 || mode == 5) ? 2 : 1));
        chimera_vfs_compound_free(cp);
        assert(chimera_vfs_claim_access_owner_is_retired(test.old));
        chimera_vfs_claim_access_owner_put(test.old);
        chimera_vfs_state_put(f->vfs->vfs_state, test.file);
    }
    /* A readiness endpoint in another group must fail before admission. */
    struct chimera_vfs_claim claim;
    struct chimera_claim_owner owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2, .owner_lo = 960 };
    chimera_vfs_claim_init_smb_open(&claim, CHIMERA_CLAIM_R, 0, &owner);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_open(cp, NULL, 0,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0);
    assert(chimera_vfs_compound_add_reserve_access(cp, 1, &claim) == 2);
    add_group(cp, 0, 3, NULL, -1, true, f);
    assert(chimera_vfs_compound_add_checkpoint(cp) == 3);
    add_group(cp, 3, 1, NULL, -1, true, f);
    assert(chimera_vfs_compound_reserve_access_until(cp, 2, 3));
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    assert(!chimera_vfs_compound_op(cp, 2)->access_owner);
    chimera_vfs_compound_free(cp);
}

struct narrow_compound_test {
    struct fixture *fixture;
    unsigned mode, finishes;
};

static void
narrow_ready_prepare(struct chimera_vfs_compound *cp, uint32_t index,
                      enum chimera_vfs_error *status, void *private)
{
    struct narrow_compound_test *test = private;
    (void) cp; (void) index;
    if (test->mode) *status = CHIMERA_VFS_EACCES;
}

static void
narrow_compound_finish(struct chimera_vfs_compound *cp, void *private)
{
    struct narrow_compound_test *test = private;
    const struct chimera_vfs_compound_op *reserve = chimera_vfs_compound_op(cp, 2);
    assert(chimera_vfs_compound_op(cp, 3)->status == CHIMERA_VFS_OK);
    assert(reserve->claim_held == !test->mode && reserve->access_owner);
    /* Public rows stay conservative even though the later reservation used
     * the narrowed view (or the failed producer's private cutoff). */
    assert(reserve->claim->used == (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W));
    assert(reserve->claim->denied == CHIMERA_CLAIM_D && reserve->claim->file);
    assert(chimera_vfs_compound_op(cp, 5)->claim_held);
    assert(chimera_vfs_compound_op(cp, 5)->status == CHIMERA_VFS_OK);
    chimera_vfs_compound_finish_result(cp, test->finishes++ ? CHIMERA_VFS_OK : CHIMERA_VFS_EAGAIN);
}

static void
narrow_compound_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct narrow_compound_test *test = private;
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    const struct chimera_vfs_compound_op *reserve = chimera_vfs_compound_op(cp, 2);
    if (test->mode) {
        assert(!reserve->claim->file);
    } else {
        assert(reserve->claim->file && reserve->claim->used == CHIMERA_CLAIM_R && !reserve->claim->denied);
    }
    test->fixture->done = 1;
}

static void
check_compound_narrow(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    for (unsigned mode = 0; mode < 2; mode++) {
        struct narrow_compound_test test = { .fixture = f, .mode = mode };
        struct chimera_claim_owner identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = 970, .owner_lo = 970 };
        struct chimera_vfs_claim transient, next;
        chimera_vfs_claim_init_smb_open(&transient, CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, CHIMERA_CLAIM_D, &identity);
        identity.owner_lo++;
        chimera_vfs_claim_init_smb_open(&next, mode ? CHIMERA_CLAIM_W : CHIMERA_CLAIM_R,
            mode ? CHIMERA_CLAIM_R : CHIMERA_CLAIM_W, &identity);
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
        assert(chimera_vfs_compound_add_open(cp, NULL, 0,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0) == 1);
        assert(chimera_vfs_compound_add_reserve_access(cp, 1, &transient) == 2);
        assert(chimera_vfs_compound_add_narrow_access(cp, 2, CHIMERA_CLAIM_R, 0) == 3);
        assert(chimera_vfs_compound_add_checkpoint(cp) == 4);
        chimera_vfs_compound_set_op_prepare(cp, 4, narrow_ready_prepare, &test);
        assert(chimera_vfs_compound_reserve_access_until(cp, 2, 4));
        add_group(cp, 0, 5, NULL, -1, true, f);
        assert(chimera_vfs_compound_add_reserve_access(cp, 1, &next) == 5);
        add_group(cp, 5, 1, NULL, -1, true, f);
        chimera_vfs_compound_set_finish_handler(cp, narrow_compound_finish, &test);
        chimera_vfs_compound_submit(cp, narrow_compound_complete, &test);
        wait_done(f);
        assert(test.finishes == 2);
        chimera_vfs_compound_free(cp);
    }
}

struct cancel_scope_test {
    unsigned mode, middle, end;
};

static void
cancel_scope_prepare(struct chimera_vfs_compound *cp, uint32_t index,
                     enum chimera_vfs_error *status, void *private)
{
    struct cancel_scope_test *test = private;
    if (index == 0) {
        if (test->mode == 2) assert(chimera_vfs_compound_cancel(cp));
        if (test->mode == 3) *status = CHIMERA_VFS_EACCES;
        if (test->mode == 4) chimera_vfs_compound_op_skip(cp, index);
    } else if (index == 1) {
        test->middle++;
        if (test->mode == 1 || test->mode == 5) assert(chimera_vfs_compound_cancel(cp));
        if (test->mode == 5) *status = CHIMERA_VFS_EACCES;
    } else if (index == 2) {
        test->end++;
    }
}

static void
cancel_scope_complete(struct chimera_vfs_compound *cp, uint32_t index,
                      enum chimera_vfs_error *status, void *private)
{
    struct cancel_scope_test *test = private;
    if (index == 0 && (test->mode == 0 || test->mode == 3 || test->mode == 4)) {
        /* A callback cannot turn a failed/skipped start into a commit point. */
        if (test->mode == 3) *status = CHIMERA_VFS_OK;
        assert(chimera_vfs_compound_cancel(cp));
    }
}

static void
check_cancel_scopes(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    for (unsigned mode = 0; mode < 7; mode++) {
        struct cancel_scope_test test = { .mode = mode };
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        assert(chimera_vfs_compound_add_retire_open_claims(cp, NULL, NULL, NULL) == 0);
        for (unsigned i = 1; i < 4; i++) assert(chimera_vfs_compound_add_checkpoint(cp) == (int)i);
        for (unsigned i = 0; i < 3; i++) {
            chimera_vfs_compound_set_op_callbacks(cp, i, cancel_scope_prepare, cancel_scope_complete, &test);
        }
        assert(chimera_vfs_compound_set_cancel_scope(cp, 0, 2));
        assert(!chimera_vfs_compound_set_cancel_scope(cp, 1, 3)); /* No overlap. */
        add_group(cp, 0, mode == 6 ? 2 : 3, NULL, -1, true, f);
        add_group(cp, mode == 6 ? 2 : 3, mode == 6 ? 2 : 1, NULL, -1, true, f);
        chimera_vfs_compound_submit(cp, completed, f);
        wait_done(f);
        if (mode == 6) {
            assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_EINVAL);
            assert(chimera_vfs_compound_num_completed(cp) == 0);
        } else {
            assert(chimera_vfs_compound_execution_status(cp) ==
                   (mode == 5 ? CHIMERA_VFS_EACCES : CHIMERA_VFS_EINTR));
            assert(test.middle == (mode < 2 || mode == 5));
            assert(test.end == (mode < 2));
            assert(!chimera_vfs_compound_op(cp, 3)->completed);
            assert(!chimera_vfs_compound_retry(cp));
        }
        chimera_vfs_compound_free(cp);
    }
}

struct range_owner_test {
    struct fixture *fixture;
    struct chimera_vfs_claim_owner *taken, *rejected;
    unsigned bindings;
    bool transfer;
};

static void
range_owner_bind(struct chimera_vfs_compound *cp, uint32_t index,
                 enum chimera_vfs_error *status, void *private)
{
    struct range_owner_test *test = private;
    (void) status;
    if (test->rejected) {
        assert(chimera_vfs_claim_owner_is_retired(test->rejected));
        assert(!chimera_vfs_claim_owner_has_locks(test->rejected));
        chimera_vfs_claim_owner_put(test->rejected);
        test->rejected = NULL;
    }
    const struct chimera_vfs_compound_op *producer = chimera_vfs_compound_op(cp, 0);
    assert(producer->completed && producer->status == CHIMERA_VFS_OK);
    assert(producer->out_range_owner);
    chimera_vfs_compound_op_args(cp, index)->range_owner = producer->out_range_owner;
    test->bindings++;
}

static void
range_owner_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct range_owner_test *test = private;
    struct chimera_vfs_claim_owner *owner = chimera_vfs_compound_op(cp, 0)->out_range_owner;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK && owner);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(!chimera_vfs_compound_take_range_owner(cp, 0));
        test->rejected = owner;
        chimera_vfs_claim_owner_ref(owner);
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_claim_owner_has_locks(owner));
    if (test->transfer) {
        test->taken = chimera_vfs_compound_take_range_owner(cp, 0);
        assert(test->taken == owner);
        assert(!chimera_vfs_compound_take_range_owner(cp, 0));
    } else {
        test->taken = owner;
        chimera_vfs_claim_owner_ref(owner); /* Observe compound-owned cleanup. */
    }
    test->fixture->done = 1;
}

static void
check_range_owner_production(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_open_handle handle = { .fh_len = f->fh_len };
    memcpy(handle.fh, f->fh, f->fh_len);
    handle.fh_hash = chimera_vfs_hash(handle.fh, handle.fh_len);
    struct chimera_claim_actor actor = {
        .owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2, .client_key = 819, .owner_lo = 819 },
        .op_handle = &handle,
    };
    struct chimera_vfs_claim_exact_range range = { 71, 4, true };
    for (unsigned transfer = 0; transfer < 2; transfer++) {
        struct range_owner_test test = { .fixture = f, .transfer = transfer };
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        assert(chimera_vfs_compound_add_range_owner(cp, &actor, true) == 0);
        chimera_vfs_compound_op_args(cp, 0)->in_handle = &handle;
        assert(chimera_vfs_compound_add_range_batch(cp, NULL, &range, 1, false) == 1);
        chimera_vfs_compound_set_op_prepare(cp, 1, range_owner_bind, &test);
        f->finishes = 0;
        chimera_vfs_compound_set_finish_handler(cp, reject_first, f);
        chimera_vfs_compound_submit(cp, range_owner_complete, &test);
        wait_done(f);
        assert(test.bindings == 2 && f->finishes == 2 && !test.rejected);
        chimera_vfs_compound_free(cp);
        assert(chimera_vfs_claim_owner_is_retired(test.taken) == !transfer);
        assert(chimera_vfs_claim_owner_has_locks(test.taken) == !!transfer);
        chimera_vfs_claim_owner_retire(test.taken, NULL, NULL);
        chimera_vfs_claim_owner_put(test.taken);
    }
}

struct closed_anchor_test {
    struct fixture *fixture;
    struct chimera_vfs_open_handle *anchor, *previous;
    uint32_t count;
    unsigned attempts, resets;
};

static uint32_t
anchor_count(struct fixture *f, struct chimera_vfs_open_handle *handle)
{
    assert(handle);
    struct vfs_open_cache *cache = chimera_vfs_get_cache_for_handle(f->thread, handle);
    assert(cache);
    struct vfs_open_cache_shard *shard = &cache->shards[handle->fh_hash & cache->shard_mask];
    pthread_mutex_lock(&shard->lock);
    uint32_t count = handle->opencnt;
    pthread_mutex_unlock(&shard->lock);
    return count;
}

static void
closed_anchor_prepare(struct chimera_vfs_compound *cp, uint32_t index,
                      enum chimera_vfs_error *status, void *private)
{
    struct closed_anchor_test *test = private;
    (void) status;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(cp, index);
    if (index == 2) {
        test->anchor = chimera_vfs_compound_op(cp, 1)->out_handle;
        test->count = anchor_count(test->fixture, test->anchor);
        op->io_owner.op_handle = test->anchor;
    } else if (index == 3) {
        op->range_owner = chimera_vfs_compound_op(cp, 2)->out_range_owner;
    } else {
        op->range_retire_owner = chimera_vfs_compound_op(cp, 2)->out_range_owner;
    }
}

static void
closed_anchor_reset(struct chimera_vfs_compound *cp, void *private)
{
    struct closed_anchor_test *test = private;
    (void) cp;
    if (test->previous) {
        /* Exactly the closed producer ref dropped; our observer ref remains. */
        assert(anchor_count(test->fixture, test->previous) == test->count);
        chimera_vfs_release(test->fixture->thread, test->previous);
        test->previous = NULL;
        test->resets++;
    }
}

static void
closed_anchor_finish(struct chimera_vfs_compound *cp, void *private)
{
    struct closed_anchor_test *test = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!chimera_vfs_compound_op(cp, 1)->out_handle);
    assert(chimera_vfs_compound_op(cp, 5)->closed_output_handle == test->anchor);
    assert(!chimera_vfs_compound_op(cp, 5)->close_handle);
    assert(anchor_count(test->fixture, test->anchor) == test->count);
    test->attempts++;
    if (test->attempts == 1) {
        test->previous = test->anchor;
        chimera_vfs_dup_handle(test->fixture->thread, test->previous);
        chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_EAGAIN);
    } else {
        test->fixture->done = 1; /* Hold accepted attempt before finish result. */
    }
}

static void
closed_anchor_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct closed_anchor_test *test = private;
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    test->fixture->done = 1;
}

static void
check_closed_producer_anchor(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct closed_anchor_test test = { .fixture = f };
    struct chimera_claim_actor actor = {
        .owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2, .client_key = 818, .owner_lo = 818 },
    };
    struct chimera_vfs_claim_exact_range range = { 81, 4, true };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    assert(chimera_vfs_compound_add_open(cp, NULL, 0,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0) == 1);
    assert(chimera_vfs_compound_add_range_owner(cp, &actor, true) == 2);
    assert(chimera_vfs_compound_add_range_batch(cp, NULL, &range, 1, false) == 3);
    assert(chimera_vfs_compound_add_retire_range_owner(cp, NULL) == 4);
    assert(chimera_vfs_compound_add_close(cp) == 5);
    for (unsigned i = 2; i <= 4; i++) chimera_vfs_compound_set_op_prepare(cp, i, closed_anchor_prepare, &test);
    chimera_vfs_compound_set_attempt_reset(cp, closed_anchor_reset, &test);
    chimera_vfs_compound_set_finish_handler(cp, closed_anchor_finish, &test);
    chimera_vfs_compound_submit(cp, closed_anchor_complete, &test);
    wait_done(f);
    assert(test.attempts == 2 && test.resets == 1);
    chimera_vfs_dup_handle(f->thread, test.anchor); /* Keep observation safe after free. */
    chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    wait_done(f);
    chimera_vfs_compound_free(cp);
    assert(anchor_count(f, test.anchor) == test.count);
    chimera_vfs_release(f->thread, test.anchor);
}

struct open_retire_test {
    struct fixture *fixture;
    struct chimera_vfs_claim_owner *old_range, *new_range;
    struct chimera_vfs_claim_access_owner *access, *base, *prior;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_open_handle handle;
    bool fail_base;
    unsigned observations;
};

static void
open_retire_observe(struct chimera_vfs_compound *cp, uint32_t index,
                     enum chimera_vfs_error *status, void *private)
{
    struct open_retire_test *test = private;
    (void) index; (void) status;
    assert(chimera_vfs_compound_io_denied(cp, &test->handle, 10, 1, true, NULL) == test->fail_base);
    /* Earlier successful LOCK survives a failed combined admission, and is
     * included in successful whole-owner retirement. */
    assert(chimera_vfs_compound_io_denied(cp, &test->handle, 30, 1, true, NULL) == test->fail_base);
    assert(chimera_vfs_claim_access_owner_claim(test->access)->file == test->file);
    assert(chimera_vfs_claim_access_owner_claim(test->base)->file == test->file);
    test->observations++;
}

static void
open_retire_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct open_retire_test *test = private;
    assert(chimera_vfs_compound_execution_status(cp) ==
        (test->fail_base ? CHIMERA_VFS_EBUSY : CHIMERA_VFS_OK));
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_compound_op(cp, 0)->range_result.applied == 1);
    assert(!chimera_vfs_claim_access_owner_claim(test->prior)->file);
    if (test->fail_base) {
        assert(chimera_vfs_claim_owner_has_locks(test->old_range));
        assert(chimera_vfs_claim_access_owner_claim(test->access)->file == test->file);
        assert(chimera_vfs_claim_access_owner_claim(test->base)->file == test->file);
    } else {
        assert(!chimera_vfs_claim_owner_has_locks(test->old_range));
        assert(!chimera_vfs_claim_owner_is_retired(test->old_range));
        assert(!chimera_vfs_claim_access_owner_claim(test->access)->file);
        assert(!chimera_vfs_claim_access_owner_claim(test->base)->file);
        assert(chimera_vfs_compound_op(cp, 4)->range_result.applied == 1);
    }
    test->fixture->done = 1;
}

static void
check_open_claim_retirement(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    for (unsigned failure = 0; failure < 2; failure++) {
        struct open_retire_test test = { .fixture = f, .fail_base = failure };
        test.file = chimera_vfs_state_get(f->vfs->vfs_state, f->fh, f->fh_len,
            chimera_vfs_hash(f->fh, f->fh_len), true);
        test.handle.fh_len = f->fh_len;
        test.handle.fh_hash = test.file->fh_hash;
        memcpy(test.handle.fh, f->fh, f->fh_len);
        struct chimera_claim_owner identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
                                                .client_key = 820, .owner_lo = 820 };
        test.old_range = chimera_vfs_claim_owner_create(test.file, &identity);
        identity.owner_lo++;
        test.new_range = chimera_vfs_claim_owner_create(test.file, &identity);
        struct chimera_vfs_claim claim;
        chimera_vfs_claim_init_smb_open(&claim, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
        test.access = chimera_vfs_claim_access_owner_alloc(test.file, &claim);
        identity.owner_lo++;
        chimera_vfs_claim_init_smb_open(&claim, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
        test.base = chimera_vfs_claim_access_owner_alloc(test.file, &claim);
        identity.owner_lo++;
        chimera_vfs_claim_init_smb_open(&claim, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
        test.prior = chimera_vfs_claim_access_owner_alloc(test.file, &claim);
        assert(test.old_range && test.new_range && test.access && test.base && test.prior);
        struct chimera_vfs_claim_access_owner *owners[] = { test.access, test.base, test.prior };
        for (unsigned i = 0; i < 3; i++) {
            assert(chimera_vfs_claim_try_acquire(f->vfs->vfs_state, test.file,
                chimera_vfs_claim_access_owner_claim(owners[i]), NULL) == CHIMERA_CLAIM_GRANTED);
        }
        struct chimera_vfs_claim_exact_range old = { 10, 1, true }, earlier = { 30, 1, true };
        struct chimera_vfs_claim_journal *seed = chimera_vfs_claim_journal_alloc(1, 1);
        struct chimera_vfs_claim_batch_result result;
        chimera_vfs_claim_journal_acquire(seed, test.old_range, &old, 1, &result);
        assert(result.status == CHIMERA_VFS_OK);
        chimera_vfs_claim_journal_seal(seed);
        chimera_vfs_claim_journal_publish(seed);
        chimera_vfs_claim_journal_complete(seed);
        chimera_vfs_claim_journal_free(seed);
        struct chimera_vfs_claim_access_journal *peer = chimera_vfs_claim_access_journal_alloc(1);
        if (failure) assert(chimera_vfs_claim_access_journal_retire(peer, test.base) == CHIMERA_VFS_OK);
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        assert(chimera_vfs_compound_add_range_batch(cp, test.old_range, &earlier, 1, false) == 0);
        assert(chimera_vfs_compound_add_retire_access(cp, test.prior) == 1);
        add_group(cp, 0, 2, NULL, -1, true, f);
        assert(chimera_vfs_compound_add_retire_open_claims(cp, test.old_range, test.access, test.base) == 2);
        add_group(cp, 2, 1, NULL, -1, true, f);
        assert(chimera_vfs_compound_add_checkpoint(cp) == 3);
        chimera_vfs_compound_set_op_prepare(cp, 3, open_retire_observe, &test);
        if (!failure) assert(chimera_vfs_compound_add_range_batch(cp, test.new_range, &old, 1, false) == 4);
        add_group(cp, 3, failure ? 1 : 2, NULL, -1, true, f);
        f->finishes = 0;
        if (!failure) chimera_vfs_compound_set_finish_handler(cp, reject_first, f);
        chimera_vfs_compound_submit(cp, open_retire_complete, &test);
        wait_done(f);
        assert(test.observations == (failure ? 1 : 2));
        chimera_vfs_compound_free(cp);
        assert(chimera_vfs_claim_access_owner_is_retired(test.prior));
        if (!failure) assert(chimera_vfs_claim_owner_is_retired(test.old_range));
        chimera_vfs_claim_access_journal_free(peer);
        for (unsigned i = 0; i < 3; i++) {
            chimera_vfs_claim_access_owner_retire(owners[i]);
            chimera_vfs_claim_access_owner_put(owners[i]);
        }
        chimera_vfs_claim_owner_retire(test.old_range, NULL, NULL);
        chimera_vfs_claim_owner_retire(test.new_range, NULL, NULL);
        chimera_vfs_claim_owner_put(test.old_range);
        chimera_vfs_claim_owner_put(test.new_range);
        chimera_vfs_state_put(f->vfs->vfs_state, test.file);
    }
}

static void
search_page_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, 1);
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(op->kv_num_entries == 2 && op->kv_more && op->kv_next_key_len == 4);
    for (unsigned i = 0; i < 2; i++) {
        assert(op->kv_entries[i].key_len == 4 && op->kv_entries[i].value_len == 2);
        assert(op->kv_entries[i].key[1] == 0 && op->kv_entries[i].key[3] == i);
        assert(op->kv_entries[i].value[0] == 0 && op->kv_entries[i].value[1] == i);
    }
    assert(op->kv_next_key[3] == 2);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    f->done = 1;
}

static int
invalid_search_entry(const void *key, uint32_t key_len,
                      const void *value, uint32_t value_len, void *private)
{
    (void) key; (void) key_len; (void) value; (void) value_len; (void) private;
    assert(!"invalid routed search dispatched a callback");
    return 1;
}

static void
invalid_search_complete(enum chimera_vfs_error status, void *private)
{
    struct fixture *f = private;
    f->mount_status = status;
    f->done = 1;
}

static void
hs_denied(enum chimera_vfs_error status, struct chimera_vfs_open_handle *handle,
          struct chimera_vfs_attrs *set_attr, struct chimera_vfs_attrs *attr,
          struct chimera_vfs_attrs *pre, struct chimera_vfs_attrs *post, void *private)
{
    struct fixture *f = private;
    (void) set_attr; (void) attr; (void) pre; (void) post;
    assert(status == CHIMERA_VFS_EACCES && !handle);
    f->done = 1;
}

/* A successful backend OPEN is not yet authorized persistence: the VFS's
 * post-open DAC check must run before its default-KV recovery-record write. */
static void
check_handle_state_dac(struct fixture *f, const struct chimera_vfs_cred *root,
                       const struct chimera_vfs_cred *user)
{
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0000 };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_PATH, 0);
    int parent_op = chimera_vfs_compound_add_gethandle(cp);
    int child_op = chimera_vfs_compound_add_open_at(cp, "hs-private", 10,
        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    struct chimera_vfs_open_handle *parent = chimera_vfs_compound_take_handle(cp, parent_op);
    struct chimera_vfs_open_handle *child = chimera_vfs_compound_take_handle(cp, child_op);
    assert(parent && child);
    chimera_vfs_compound_free(cp);
    const char key[] = "hs-denied", value[] = "must-not-be-stored";
    struct chimera_vfs_handle_state hs = {
        .key = key, .key_len = sizeof(key), .value = value, .value_len = sizeof(value),
        .r_created = 1, /* entrypoint must reset this reused descriptor output */
    };
    attrs.va_set_mask = 0;
    chimera_vfs_open_at_hs(f->thread, user, parent, "hs-private", 10,
        CHIMERA_VFS_OPEN_READ_ONLY, &attrs, CHIMERA_VFS_ATTR_FH, 0, 0,
        &hs, hs_denied, f);
    wait_done(f);
    assert(!hs.r_created);
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_puthandle(cp, child, CHIMERA_VFS_OPEN_INFERRED);
    int searched = chimera_vfs_compound_add_search_keys_at(cp,
        key, sizeof(key), key, sizeof(key), 0, 1, 1024);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, searched)->kv_num_entries == 0);
    chimera_vfs_compound_free(cp);
    chimera_vfs_release(f->thread, child);
    chimera_vfs_release(f->thread, parent);
}

static void
check_search_key_pages(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    uint8_t keys[3][4] = { { 'k', 0, 'p', 0 }, { 'k', 0, 'p', 1 }, { 'k', 0, 'p', 2 } };
    uint8_t end[4] = { 'k', 0, 'q', 0 }, start[4];
    chimera_vfs_search_keys_at(f->thread, cred, f->fh, f->fh_len,
        keys[0], UINT32_MAX, NULL, 0, 0, invalid_search_entry, invalid_search_complete, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ERANGE);
    chimera_vfs_search_keys_at(f->thread, cred, f->fh, f->fh_len,
        keys[0], CHIMERA_VFS_PLUGIN_DATA_SIZE - 1, keys[0], 1, 0,
        invalid_search_entry, invalid_search_complete, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ERANGE); /* namespace scratch overhead */
    chimera_vfs_search_keys_at(f->thread, cred, f->fh, f->fh_len,
        keys[0], 4, NULL, 1, 0, invalid_search_entry, invalid_search_complete, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_EINVAL);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    for (unsigned i = 0; i < 3; i++) {
        uint8_t value[] = { 0, i };
        assert(chimera_vfs_compound_add_put_key_at(cp, keys[i], 4, value, 2) > 0);
    }
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);

    cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    memcpy(start, keys[0], 4);
    assert(chimera_vfs_compound_add_search_keys_at(cp, start, 4, end, 4,
        CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE, 2, 1024) == 1);
    memset(start, 0xff, 4); /* input bounds are immutable copies */
    f->finishes = 0;
    chimera_vfs_compound_set_finish_handler(cp, reject_first, f);
    chimera_vfs_compound_submit(cp, search_page_complete, f);
    wait_done(f);
    assert(f->finishes == 2);
    const struct chimera_vfs_compound_op *page = chimera_vfs_compound_op(cp, 1);
    struct chimera_vfs_compound *next = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(next, f->fh, f->fh_len);
    assert(chimera_vfs_compound_add_search_keys_at(next, page->kv_next_key, page->kv_next_key_len,
        end, 4, CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE, 2, 1024) == 1);
    chimera_vfs_compound_free(cp); /* next page owns the continuation copy */
    chimera_vfs_compound_submit(next, completed, f);
    wait_done(f);
    page = chimera_vfs_compound_op(next, 1);
    assert(page->status == CHIMERA_VFS_OK && page->kv_num_entries == 1 && !page->kv_more);
    assert(page->kv_entries[0].key[3] == 2 && page->kv_entries[0].value[1] == 2);
    chimera_vfs_compound_free(next);

    for (unsigned mode = 0; mode < 3; mode++) {
        cp = chimera_vfs_compound_alloc(f->thread, cred);
        chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
        assert(chimera_vfs_compound_add_search_keys_at(cp, keys[0], 4, mode == 2 ? keys[1] : end, 4,
            CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE, 3, mode == 1 ? 1 : 6) == 1);
        chimera_vfs_compound_submit(cp, completed, f);
        wait_done(f);
        page = chimera_vfs_compound_op(cp, 1);
        if (mode == 1) {
            assert(page->status == CHIMERA_VFS_ERANGE && !page->kv_num_entries && !page->kv_more);
        } else {
            assert(page->status == CHIMERA_VFS_OK && page->kv_num_entries == 1);
            assert(page->kv_more == (mode == 0));
            if (mode == 0) assert(page->kv_next_key[3] == 1);
        }
        chimera_vfs_compound_free(cp);
    }
    cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    for (unsigned i = 0; i < 3; i++) chimera_vfs_compound_add_delete_key_at(cp, keys[i], 4);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
}

struct remove_test {
    struct fixture *fixture;
    unsigned notifications;
};

static void
remove_notified(struct chimera_vfs_notify_watch *watch, void *private)
{
    struct remove_test *test = private;
    test->notifications++;
    struct chimera_vfs_notify_sync_event *event = chimera_vfs_notify_drain_sync(watch);
    while (event) {
        struct chimera_vfs_notify_sync_event *next = event->next;
        chimera_vfs_notify_gate_ack(test->fixture->vfs->vfs_notify, event);
        event = next;
    }
}

static void
remove_hold_finish(struct chimera_vfs_compound *cp, void *private)
{
    struct remove_test *test = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!test->notifications);
    test->fixture->done = 1;
}

static void
remove_retry_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    assert(chimera_vfs_compound_op(cp, 1)->remove_unmatched);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    f->done = 1;
}

static void
remove_status(enum chimera_vfs_error status, struct chimera_vfs_attrs *pre,
               struct chimera_vfs_attrs *post, void *private)
{
    (void) pre; (void) post;
    invalid_search_complete(status, private);
}

static struct chimera_vfs_open_handle *
remove_create(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0600 };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int opened = chimera_vfs_compound_add_open_at(cp, "matched-remove", 14,
        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    struct chimera_vfs_open_handle *handle = chimera_vfs_compound_take_handle(cp, opened);
    assert(handle);
    chimera_vfs_compound_free(cp);
    return handle;
}

static void
check_matched_remove(struct fixture *f, const struct chimera_vfs_cred *root,
                      const struct chimera_vfs_cred *user)
{
    struct chimera_vfs_open_handle *original = remove_create(f, root);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_remove(cp, "matched-remove", 14, 0);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    struct chimera_vfs_open_handle *replacement = remove_create(f, root);
    assert(original->fh_len != replacement->fh_len ||
        memcmp(original->fh, replacement->fh, original->fh_len));
    struct remove_test test = { .fixture = f };
    struct chimera_vfs_notify_watch *parent = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_FILE_REMOVED, 0, remove_notified, &test);
    struct chimera_vfs_notify_watch *child = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        replacement->fh, replacement->fh_len, CHIMERA_VFS_NOTIFY_FILE_REMOVED, 0, remove_notified, &test);
    assert(parent && child);
    chimera_vfs_notify_watch_set_sync(f->vfs->vfs_notify, parent, &test);
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int removed = chimera_vfs_compound_add_remove(cp, "matched-remove", 14, CHIMERA_VFS_REMOVE_NO_NOTIFY);
    struct chimera_vfs_compound_op *args = chimera_vfs_compound_op_args(cp, removed);
    args->remove_match_child_fh = 1;
    args->arg_fh_len = original->fh_len;
    memcpy(args->arg_fh, original->fh, original->fh_len);
    f->finishes = 0;
    chimera_vfs_compound_set_finish_handler(cp, reject_first, f);
    chimera_vfs_compound_submit(cp, remove_retry_complete, f);
    wait_done(f);
    assert(f->finishes == 2 && !test.notifications);
    chimera_vfs_compound_free(cp);
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int lookup = chimera_vfs_compound_add_lookup(cp, "matched-remove", 14, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, lookup)->attr.va_fh_len == replacement->fh_len);
    assert(!memcmp(chimera_vfs_compound_op(cp, lookup)->attr.va_fh, replacement->fh, replacement->fh_len));
    chimera_vfs_compound_free(cp);

    struct chimera_vfs_module unsupported = { 0 };
    struct chimera_vfs_open_handle fake_parent = { .vfs_module = &unsupported };
    uint8_t unmatched = 9;
    chimera_vfs_remove_at_match_fh_flags(f->thread, root, &fake_parent, "matched-remove", 14,
        replacement->fh, replacement->fh_len, CHIMERA_VFS_REMOVE_NO_NOTIFY, 0, 0,
        NULL, NULL, &unmatched, remove_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ENOTSUP && !unmatched);

    cp = chimera_vfs_compound_alloc(f->thread, user);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int opened = chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_op_args(cp, opened)->namespace_cred = root;
    removed = chimera_vfs_compound_add_remove(cp, "matched-remove", 14, CHIMERA_VFS_REMOVE_NO_NOTIFY);
    args = chimera_vfs_compound_op_args(cp, removed);
    args->remove_match_child_fh = 1;
    args->namespace_cred = root;
    args->arg_fh_len = replacement->fh_len;
    memcpy(args->arg_fh, replacement->fh, replacement->fh_len);
    int access = chimera_vfs_compound_add_access(cp, CHIMERA_ACE_WRITE_DATA);
    chimera_vfs_compound_set_finish_handler(cp, remove_hold_finish, &test);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f); /* finish is held after actual removal, never rejected */
    assert(!chimera_vfs_compound_op(cp, removed)->remove_unmatched);
    assert(!(chimera_vfs_compound_op(cp, access)->granted & CHIMERA_ACE_WRITE_DATA));
    assert(!test.notifications && !chimera_vfs_notify_watch_take_deleted(child));
    chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK && !test.notifications);
    chimera_vfs_compound_free(cp);
    /* A protocol publisher now owns exactly-once notification. */
    chimera_vfs_notify_emit_lease(f->vfs->vfs_notify, f->fh, f->fh_len,
        CHIMERA_VFS_NOTIFY_FILE_REMOVED, "matched-remove", 14, NULL, 0, 0, 0, false);
    chimera_vfs_notify_emit_delete(f->vfs->vfs_notify, replacement->fh, replacement->fh_len);
    assert(test.notifications && chimera_vfs_notify_watch_take_deleted(child));
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, parent);
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, child);
    chimera_vfs_release(f->thread, original);
    chimera_vfs_release(f->thread, replacement);
}

static void
stream_remove_status(enum chimera_vfs_error status, const struct chimera_vfs_attrs *pre,
                     const struct chimera_vfs_attrs *post, void *private)
{
    (void) pre; (void) post;
    invalid_search_complete(status, private);
}

static struct chimera_vfs_open_handle *
stream_test_open(struct fixture *f, const struct chimera_vfs_cred *cred,
                 struct chimera_vfs_open_handle *base, bool create)
{
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_puthandle(cp, base, CHIMERA_VFS_OPEN_PATH);
    int opened = chimera_vfs_compound_add_open_stream(cp, "fork", 4,
        CHIMERA_VFS_OPEN_INFERRED | (create ? CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE : 0),
        NULL, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    struct chimera_vfs_open_handle *handle = chimera_vfs_compound_take_handle(cp, opened);
    assert(handle);
    chimera_vfs_compound_free(cp);
    return handle;
}

static void
stream_reject_first(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_ESTALE);
    f->finishes++;
    chimera_vfs_compound_finish_result(cp,
        f->finishes == 1 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}

static void
stream_retry_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct fixture *f = private;
    assert(chimera_vfs_compound_op(cp, 1)->status == CHIMERA_VFS_ESTALE);
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    f->done = 1;
}

static void
check_matched_remove_stream(struct fixture *f, const struct chimera_vfs_cred *root)
{
    struct chimera_vfs_open_handle *base = remove_create(f, root);
    struct chimera_vfs_open_handle *original = stream_test_open(f, root, base, true);
    chimera_vfs_remove_stream(f->thread, root, base, "fork", 4, stream_remove_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_OK);
    struct chimera_vfs_open_handle *replacement = stream_test_open(f, root, base, true);
    assert(original->fh_len != replacement->fh_len ||
        memcmp(original->fh, replacement->fh, original->fh_len));
    struct remove_test test = { .fixture = f };
    struct chimera_vfs_notify_watch *watch = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_STREAM_NAME, 0, remove_notified, &test);
    assert(watch);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_puthandle(cp, base, CHIMERA_VFS_OPEN_PATH);
    uint8_t expected[CHIMERA_VFS_FH_SIZE];
    memcpy(expected, original->fh, original->fh_len);
    assert(chimera_vfs_compound_add_remove_stream_checked(cp, "fork", 4, expected, original->fh_len) == 1);
    memset(expected, 0xff, sizeof(expected)); /* builder owns immutable identity */
    f->finishes = 0;
    chimera_vfs_compound_set_finish_handler(cp, stream_reject_first, f);
    chimera_vfs_compound_submit(cp, stream_retry_complete, f);
    wait_done(f);
    assert(f->finishes == 2 && !test.notifications);
    chimera_vfs_compound_free(cp);
    struct chimera_vfs_open_handle *looked_up = stream_test_open(f, root, base, false);
    assert(looked_up->fh_len == replacement->fh_len &&
        !memcmp(looked_up->fh, replacement->fh, replacement->fh_len));
    chimera_vfs_release(f->thread, looked_up);

    /* Advertising ordinary streams cannot silently permit checked deletion. */
    struct chimera_vfs_module unsupported = { .capabilities = CHIMERA_VFS_CAP_NAMED_STREAMS };
    struct chimera_vfs_open_handle fake = { .vfs_module = &unsupported };
    chimera_vfs_remove_stream_checked(f->thread, root, &fake, "fork", 4,
        CHIMERA_VFS_REMOVE_STREAM_MATCH_FH, replacement->fh, replacement->fh_len,
        stream_remove_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ENOTSUP);

    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_puthandle(cp, base, CHIMERA_VFS_OPEN_PATH);
    chimera_vfs_compound_add_remove_stream_checked(cp, "fork", 4, replacement->fh, replacement->fh_len);
    chimera_vfs_compound_set_finish_handler(cp, remove_hold_finish, &test);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f); /* actual mutation held for acceptance, never rejected */
    assert(!test.notifications);
    chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK && !test.notifications);
    chimera_vfs_compound_free(cp);
    chimera_vfs_notify_emit_lease(f->vfs->vfs_notify, f->fh, f->fh_len,
        CHIMERA_VFS_NOTIFY_STREAM_NAME, "matched-remove", 14, NULL, 0, 0, 0, false);
    assert(test.notifications == 1);
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, watch);
    chimera_vfs_remove_stream_checked(f->thread, root, base, "fork", 4,
        CHIMERA_VFS_REMOVE_STREAM_MATCH_FH, replacement->fh, replacement->fh_len,
        stream_remove_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ENOENT);
    chimera_vfs_release(f->thread, replacement);
    chimera_vfs_release(f->thread, original);
    chimera_vfs_release(f->thread, base);
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_remove(cp, "matched-remove", 14, 0);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
}

static void
overwrite_check_data(struct fixture *f, const struct chimera_vfs_cred *root,
                     struct chimera_vfs_open_handle *handle, unsigned length)
{
    struct evpl_iovec iov[16];
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    int read = chimera_vfs_compound_add_read(cp, handle, 0, 8, iov, 16, NULL);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, read);
    assert(op->read_len == length);
    if (length) {
        assert(op->niov == 1);
        assert(!memcmp(evpl_iovec_data(&iov[0]), "contents", length));
    }
    chimera_vfs_compound_free(cp);
}

static void
check_overwrite(struct fixture *f, const struct chimera_vfs_cred *root)
{
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0600 };
    struct chimera_vfs_open_handle *handles[3];
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int opened = chimera_vfs_compound_add_open_at(cp, "overwrite", 9,
        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
        CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    handles[0] = chimera_vfs_compound_take_handle(cp, opened);
    assert(handles[0]);
    chimera_vfs_compound_free(cp);
    for (unsigned i = 1; i < 3; i++) {
        cp = chimera_vfs_compound_alloc(f->thread, root);
        chimera_vfs_compound_add_puthandle(cp, handles[0], CHIMERA_VFS_OPEN_PATH);
        opened = chimera_vfs_compound_add_open_stream(cp, i == 1 ? "one" : "two", 3,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE | CHIMERA_VFS_OPEN_INFERRED,
            NULL, CHIMERA_VFS_ATTR_FH);
        chimera_vfs_compound_submit(cp, completed, f);
        wait_done(f);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        handles[i] = chimera_vfs_compound_take_handle(cp, opened);
        assert(handles[i]);
        chimera_vfs_compound_free(cp);
    }
    struct evpl_iovec payload;
    assert(evpl_iovec_alloc(f->evpl, 8, 0, 1, 0, &payload) == 1);
    memcpy(evpl_iovec_data(&payload), "contents", 8);
    cp = chimera_vfs_compound_alloc(f->thread, root);
    for (unsigned i = 0; i < 3; i++) {
        chimera_vfs_compound_add_write(cp, handles[i], 0, 8, 2, &payload, 1, NULL);
    }
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    evpl_iovec_release(f->evpl, &payload);

    /* Reject invalid truncation before changing either data or stream names. */
    attrs.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    attrs.va_size = 1;
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_overwrite(cp, handles[0], &attrs, CHIMERA_VFS_ATTR_SIZE, NULL);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    chimera_vfs_compound_free(cp);
    for (unsigned i = 0; i < 3; i++) overwrite_check_data(f, root, handles[i], 8);

    /* A selected stream truncates independently; a base overwrite removes all
     * named forks. These mutations are accepted once, never finish-rejected. */
    attrs.va_size = 0;
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_puthandle(cp, handles[1], CHIMERA_VFS_OPEN_INFERRED);
    int overwritten = chimera_vfs_compound_add_overwrite(cp, NULL, &attrs, CHIMERA_VFS_ATTR_SIZE, NULL);
    chimera_vfs_compound_add_puthandle(cp, handles[0], CHIMERA_VFS_OPEN_PATH);
    int listed = chimera_vfs_compound_add_list_streams(cp, 0, 4096, false);
    attrs.va_size = 99; /* builder owns its attribute snapshot */
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, overwritten)->attr.va_size == 0);
    assert(chimera_vfs_compound_op(cp, listed)->buffer_count == 3); /* base + two forks */
    chimera_vfs_compound_free(cp);
    overwrite_check_data(f, root, handles[0], 8);
    overwrite_check_data(f, root, handles[1], 0);
    overwrite_check_data(f, root, handles[2], 8);
    attrs.va_size = 0;
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_overwrite(cp, handles[0], &attrs, CHIMERA_VFS_ATTR_SIZE, NULL);
    chimera_vfs_compound_add_puthandle(cp, handles[0], CHIMERA_VFS_OPEN_PATH);
    listed = chimera_vfs_compound_add_list_streams(cp, 0, 4096, false);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, listed)->buffer_count == 1); /* unnamed fork only */
    chimera_vfs_compound_free(cp);
    overwrite_check_data(f, root, handles[0], 0);
    for (unsigned i = 0; i < 3; i++) chimera_vfs_release(f->thread, handles[i]);
}

static void
check_mkdir_notification(struct fixture *f, const struct chimera_vfs_cred *root)
{
    struct remove_test test = { .fixture = f };
    struct chimera_vfs_notify_watch *parent = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_DIR_ADDED, 0, remove_notified, &test);
    assert(parent);
    chimera_vfs_notify_watch_set_sync(f->vfs->vfs_notify, parent, &test);
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0700 };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0);
    int created = chimera_vfs_compound_add_create(cp, CHIMERA_VFS_COMPOUND_CREATE_DIR,
        "quiet-directory", 15, NULL, 0, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_op_args(cp, created)->namespace_flags = CHIMERA_VFS_MKDIR_NO_NOTIFY;
    chimera_vfs_compound_set_finish_handler(cp, remove_hold_finish, &test);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f); /* Successful real mutation held at finish; never rejected. */
    assert(chimera_vfs_compound_op(cp, created)->attr.va_fh_len);
    assert(!test.notifications);
    chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK && !test.notifications);
    chimera_vfs_compound_free(cp);
    chimera_vfs_notify_emit(f->vfs->vfs_notify, f->fh, f->fh_len,
        CHIMERA_VFS_NOTIFY_DIR_ADDED, "quiet-directory", 15, NULL, 0);
    assert(test.notifications);
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, parent);
    /* Name/attribute caching still describes the newly created directory. */
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int looked = chimera_vfs_compound_add_lookup(cp, "quiet-directory", 15, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, looked)->attr.va_fh_len);
    chimera_vfs_compound_free(cp);
}

/* Both observer paths must remain quiet until accepted publication. Exercise
 * root dispatch and an unprivileged successful authorization gate, and verify
 * ordinary callers still notify and the resulting identity remains cached. */
static void
check_special_create_notification(struct fixture *f, const struct chimera_vfs_cred *root,
                                  const struct chimera_vfs_cred *user)
{
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0777 };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int dir = chimera_vfs_compound_add_create(cp, CHIMERA_VFS_COMPOUND_CREATE_DIR,
        "special-notify", 14, NULL, 0, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    uint8_t fh[CHIMERA_VFS_FH_SIZE];
    uint32_t fh_len = chimera_vfs_compound_op(cp, dir)->attr.va_fh_len;
    memcpy(fh, chimera_vfs_compound_op(cp, dir)->attr.va_fh, fh_len);
    chimera_vfs_compound_free(cp);
    for (unsigned sync = 0; sync < 2; sync++) {
        struct remove_test test = { .fixture = f };
        struct chimera_vfs_notify_watch *watch = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
            fh, fh_len, CHIMERA_VFS_NOTIFY_FILE_ADDED, 0, remove_notified, &test);
        assert(watch);
        if (sync) chimera_vfs_notify_watch_set_sync(f->vfs->vfs_notify, watch, &test);
        for (unsigned unpriv = 0; unpriv < 2; unpriv++) {
            const struct chimera_vfs_cred *cred = unpriv ? user : root;
            for (unsigned symlink = 0; symlink < 2; symlink++) {
                for (unsigned quiet = 0; quiet < 2; quiet++) {
                    char name[32];
                    int len = snprintf(name, sizeof(name), "entry-%u-%u-%u-%u", sync, unpriv, symlink, quiet);
                    attrs.va_mode = symlink ? 0777 : S_IFIFO | 0600;
                    cp = chimera_vfs_compound_alloc(f->thread, cred);
                    chimera_vfs_compound_add_putfh(cp, fh, fh_len);
                    int created = chimera_vfs_compound_add_create(cp, symlink ?
                        CHIMERA_VFS_COMPOUND_CREATE_SYMLINK : CHIMERA_VFS_COMPOUND_CREATE_NODE,
                        name, len, symlink ? "target" : NULL, symlink ? 6 : 0,
                        &attrs, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE);
                    if (quiet) {
                        chimera_vfs_compound_op_args(cp, created)->namespace_flags = symlink ?
                            CHIMERA_VFS_SYMLINK_NO_NOTIFY : CHIMERA_VFS_MKNOD_NO_NOTIFY;
                        chimera_vfs_compound_set_finish_handler(cp, remove_hold_finish, &test);
                    }
                    test.notifications = 0;
                    chimera_vfs_compound_submit(cp, completed, f);
                    wait_done(f);
                    if (quiet) {
                        assert(!test.notifications);
                        chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
                        wait_done(f); /* accept real mutation; never simulate rollback */
                        assert(!test.notifications);
                    } else {
                        assert(test.notifications);
                    }
                    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
                    const struct chimera_vfs_attrs *result = &chimera_vfs_compound_op(cp, created)->attr;
                    uint8_t child[CHIMERA_VFS_FH_SIZE];
                    uint32_t child_len = result->va_fh_len;
                    assert(child_len);
                    memcpy(child, result->va_fh, child_len);
                    assert(symlink ? S_ISLNK(result->va_mode) : S_ISFIFO(result->va_mode));
                    chimera_vfs_compound_free(cp);
                    if (quiet) {
                        chimera_vfs_notify_emit(f->vfs->vfs_notify, fh, fh_len,
                            CHIMERA_VFS_NOTIFY_FILE_ADDED, name, len, NULL, 0);
                        assert(test.notifications);
                    }
                    cp = chimera_vfs_compound_alloc(f->thread, cred);
                    chimera_vfs_compound_add_putfh(cp, fh, fh_len);
                    int looked = chimera_vfs_compound_add_lookup(cp, name, len, CHIMERA_VFS_ATTR_FH);
                    chimera_vfs_compound_submit(cp, completed, f);
                    wait_done(f);
                    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
                    result = &chimera_vfs_compound_op(cp, looked)->attr;
                    assert(result->va_fh_len == child_len && !memcmp(result->va_fh, child, child_len));
                    chimera_vfs_compound_free(cp);
                }
            }
        }
        chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, watch);
    }
    /* NO_NOTIFY is not an authorization bypass. */
    attrs.va_mode = 0700;
    cp = chimera_vfs_compound_alloc(f->thread, root);
    chimera_vfs_compound_add_putfh(cp, fh, fh_len);
    dir = chimera_vfs_compound_add_create(cp, CHIMERA_VFS_COMPOUND_CREATE_DIR,
        "private", 7, NULL, 0, &attrs, CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    fh_len = chimera_vfs_compound_op(cp, dir)->attr.va_fh_len;
    memcpy(fh, chimera_vfs_compound_op(cp, dir)->attr.va_fh, fh_len);
    chimera_vfs_compound_free(cp);
    for (unsigned symlink = 0; symlink < 2; symlink++) {
        attrs.va_mode = symlink ? 0777 : S_IFIFO | 0600;
        cp = chimera_vfs_compound_alloc(f->thread, user);
        chimera_vfs_compound_add_putfh(cp, fh, fh_len);
        int created = chimera_vfs_compound_add_create(cp, symlink ?
            CHIMERA_VFS_COMPOUND_CREATE_SYMLINK : CHIMERA_VFS_COMPOUND_CREATE_NODE,
            "denied", 6, symlink ? "target" : NULL, symlink ? 6 : 0, &attrs, CHIMERA_VFS_ATTR_FH);
        chimera_vfs_compound_op_args(cp, created)->namespace_flags = symlink ?
            CHIMERA_VFS_SYMLINK_NO_NOTIFY : CHIMERA_VFS_MKNOD_NO_NOTIFY;
        chimera_vfs_compound_submit(cp, completed, f);
        wait_done(f);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EACCES);
        chimera_vfs_compound_free(cp);
    }

}

/* Exercise strict matching at both the public gate and the backend boundary:
 * the latter deliberately bypasses preflight lookup, as a rebinding race can. */
static void
rename_status(enum chimera_vfs_error status, struct chimera_vfs_attrs *a,
              struct chimera_vfs_attrs *b, struct chimera_vfs_attrs *c,
              struct chimera_vfs_attrs *d, void *private)
{
    (void) a; (void) b; (void) c; (void) d;
    invalid_search_complete(status, private);
}

static void
rename_backend_complete(struct chimera_vfs_request *request)
{
    struct fixture *f = request->proto_private_data;
    f->mount_status = request->status;
    f->done = 1;
    chimera_vfs_request_free(request->thread, request);
}

static struct chimera_vfs_compound *
rename_compound(struct fixture *f, const struct chimera_vfs_cred *cred,
                struct chimera_vfs_open_handle *expected, const char *destination)
{
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_savefh(cp);
    int renamed = chimera_vfs_compound_add_rename(cp, "matched-remove", 14,
        destination, strlen(destination), CHIMERA_VFS_RENAME_NO_NOTIFY |
        CHIMERA_VFS_RENAME_NOREPLACE | CHIMERA_VFS_RENAME_MATCH_SOURCE_FH);
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(cp, renamed);
    op->arg_fh_len = expected->fh_len;
    memcpy(op->arg_fh, expected->fh, expected->fh_len);
    return cp;
}

static void
rename_expect_name(struct fixture *f, const struct chimera_vfs_cred *cred,
                   const char *name, struct chimera_vfs_open_handle *expected)
{
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int looked = chimera_vfs_compound_add_lookup(cp, name, strlen(name), CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, looked);
    assert(op->status == (expected ? CHIMERA_VFS_OK : CHIMERA_VFS_ENOENT));
    if (expected) {
        assert(op->attr.va_fh_len == expected->fh_len);
        assert(!memcmp(op->attr.va_fh, expected->fh, expected->fh_len));
    }
    chimera_vfs_compound_free(cp);
}

static void
check_matched_rename(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_open_handle *old = remove_create(f, cred);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_remove(cp, "matched-remove", 14, 0);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    struct chimera_vfs_open_handle *replacement = remove_create(f, cred);
    assert(old->fh_len != replacement->fh_len || memcmp(old->fh, replacement->fh, old->fh_len));
    cp = rename_compound(f, cred, old, "quiet-renamed");
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ESTALE);
    chimera_vfs_compound_free(cp);

    struct chimera_vfs_request *request = chimera_vfs_request_alloc(f->thread, cred, f->fh, f->fh_len);
    assert(!CHIMERA_VFS_IS_ERR(request));
    request->opcode = CHIMERA_VFS_OP_RENAME_AT;
    request->complete = rename_backend_complete;
    request->proto_private_data = f;
    memset(&request->rename_at, 0, sizeof(request->rename_at));
    request->rename_at.name = "matched-remove";
    request->rename_at.namelen = 14;
    request->rename_at.name_hash = chimera_vfs_hash("matched-remove", 14);
    request->rename_at.new_fh = f->fh;
    request->rename_at.new_fhlen = f->fh_len;
    request->rename_at.new_fh_hash = chimera_vfs_hash(f->fh, f->fh_len);
    request->rename_at.new_name = "quiet-renamed";
    request->rename_at.new_namelen = 13;
    request->rename_at.new_name_hash = chimera_vfs_hash("quiet-renamed", 13);
    request->rename_at.flags = CHIMERA_VFS_RENAME_NO_NOTIFY | CHIMERA_VFS_RENAME_NOREPLACE |
        CHIMERA_VFS_RENAME_MATCH_SOURCE_FH;
    request->rename_at.match_source_fh_len = old->fh_len;
    memcpy(request->rename_at.match_source_fh, old->fh, old->fh_len);
    struct chimera_vfs_module *backend = chimera_vfs_get_module(f->thread, f->fh, f->fh_len);
    backend->dispatch(request, f->thread->module_private[backend->fh_magic]);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ESTALE);
    rename_expect_name(f, cred, "matched-remove", replacement);
    rename_expect_name(f, cred, "quiet-renamed", NULL);

    /* The single-threaded fixture temporarily removes the advertised feature;
     * strict admission must stop before dispatch and leave both names alone. */
    struct chimera_vfs_module *module = chimera_vfs_get_module(f->thread, f->fh, f->fh_len);
    uint64_t caps = module->capabilities;
    module->capabilities &= ~CHIMERA_VFS_CAP_RENAME_MATCH_FH;
    chimera_vfs_rename_at_checked(f->thread, cred, f->fh, f->fh_len, "matched-remove", 14,
        f->fh, f->fh_len, "quiet-renamed", 13, NULL, 0,
        CHIMERA_VFS_RENAME_NOREPLACE | CHIMERA_VFS_RENAME_MATCH_SOURCE_FH, 0, 0,
        NULL, NULL, replacement->fh, replacement->fh_len, rename_status, f);
    wait_done(f);
    module->capabilities = caps;
    assert(f->mount_status == CHIMERA_VFS_ENOTSUP);
    rename_expect_name(f, cred, "matched-remove", replacement);

    /* An occupied destination, including the same link, must fail atomically. */
    cp = rename_compound(f, cred, replacement, "matched-remove");
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EEXIST);
    chimera_vfs_compound_free(cp);
    cp = rename_compound(f, cred, replacement, "quiet-directory");
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EEXIST);
    chimera_vfs_compound_free(cp);
    rename_expect_name(f, cred, "matched-remove", replacement);

    struct remove_test test = { .fixture = f };
    struct chimera_vfs_notify_watch *parent = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_RENAMED, 0, remove_notified, &test);
    assert(parent);
    chimera_vfs_notify_watch_set_sync(f->vfs->vfs_notify, parent, &test);
    cp = rename_compound(f, cred, replacement, "quiet-renamed");
    chimera_vfs_compound_set_finish_handler(cp, remove_hold_finish, &test);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f); /* Actual mutation is held but never rejected. */
    assert(!test.notifications);
    chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK && !test.notifications);
    chimera_vfs_compound_free(cp);
    rename_expect_name(f, cred, "matched-remove", NULL);
    rename_expect_name(f, cred, "quiet-renamed", replacement);
    chimera_vfs_notify_emit(f->vfs->vfs_notify, f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_RENAMED,
        "quiet-renamed", 13, "matched-remove", 14);
    assert(test.notifications);
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, parent);

    /* Strict destination identity is independent of source identity. Exercise
     * root and nonroot DAC-gated entrypoints against a stale expected target;
     * neither may replace the actual destination selected after a race. */
    struct chimera_vfs_open_handle *target = remove_create(f, cred);
    struct chimera_vfs_attrs public_dir = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0777 };
    cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_setattr(cp, NULL, &public_dir, 0);
    chimera_vfs_compound_submit(cp, completed, f); wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    struct chimera_vfs_cred user = { .flavor = CHIMERA_VFS_AUTH_UNIX, .uid = 1234, .gid = 1234 };
    enum chimera_vfs_rename_outcome outcome;
    unsigned flags = CHIMERA_VFS_RENAME_MATCH_SOURCE_FH | CHIMERA_VFS_RENAME_MATCH_DEST_FH;
    for (unsigned int unprivileged = 0; unprivileged < 2; unprivileged++) {
        outcome = CHIMERA_VFS_RENAME_OUTCOME_MOVED;
        chimera_vfs_rename_at_checked_result(f->thread, unprivileged ? &user : cred,
            f->fh, f->fh_len, "quiet-renamed", 13, f->fh, f->fh_len, "matched-remove", 14,
            old->fh, old->fh_len, flags, 0, 0, NULL, NULL,
            replacement->fh, replacement->fh_len, &outcome, rename_status, f);
        wait_done(f);
        assert(f->mount_status == CHIMERA_VFS_ESTALE && outcome == CHIMERA_VFS_RENAME_OUTCOME_UNKNOWN);
        rename_expect_name(f, cred, "quiet-renamed", replacement);
        rename_expect_name(f, cred, "matched-remove", target);
    }
    chimera_vfs_rename_at_checked_result(f->thread, cred,
        f->fh, f->fh_len, "quiet-renamed", 13, f->fh, f->fh_len, "missing-target", 14,
        target->fh, target->fh_len, flags, 0, 0, NULL, NULL,
        replacement->fh, replacement->fh_len, &outcome, rename_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_ESTALE && outcome == CHIMERA_VFS_RENAME_OUTCOME_UNKNOWN);
    rename_expect_name(f, cred, "missing-target", NULL);

    module->capabilities &= ~CHIMERA_VFS_CAP_RENAME_MATCH_DEST_FH;
    outcome = CHIMERA_VFS_RENAME_OUTCOME_MOVED;
    chimera_vfs_rename_at_checked_result(f->thread, cred,
        f->fh, f->fh_len, "quiet-renamed", 13, f->fh, f->fh_len, "matched-remove", 14,
        target->fh, target->fh_len, flags, 0, 0, NULL, NULL,
        replacement->fh, replacement->fh_len, &outcome, rename_status, f);
    wait_done(f);
    module->capabilities = caps;
    assert(f->mount_status == CHIMERA_VFS_ENOTSUP && outcome == CHIMERA_VFS_RENAME_OUTCOME_UNKNOWN);

    /* Exact occupied-target replacement reports a real move. */
    chimera_vfs_rename_at_checked_result(f->thread, &user,
        f->fh, f->fh_len, "quiet-renamed", 13, f->fh, f->fh_len, "matched-remove", 14,
        target->fh, target->fh_len, flags, 0, 0, NULL, NULL,
        replacement->fh, replacement->fh_len, &outcome, rename_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_OK && outcome == CHIMERA_VFS_RENAME_OUTCOME_MOVED);
    rename_expect_name(f, cred, "matched-remove", replacement);
    rename_expect_name(f, cred, "quiet-renamed", NULL);

    cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, replacement->fh, replacement->fh_len);
    chimera_vfs_compound_add_savefh(cp);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    chimera_vfs_compound_add_link(cp, "matched-alias", 13, 0);
    chimera_vfs_compound_submit(cp, completed, f); wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    test.notifications = 0;
    parent = chimera_vfs_notify_watch_create(f->vfs->vfs_notify,
        f->fh, f->fh_len, CHIMERA_VFS_NOTIFY_RENAMED, 0, remove_notified, &test);
    assert(parent);
    chimera_vfs_notify_watch_set_sync(f->vfs->vfs_notify, parent, &test);
    chimera_vfs_rename_at_checked_result(f->thread, cred,
        f->fh, f->fh_len, "matched-remove", 14, f->fh, f->fh_len, "matched-alias", 13,
        replacement->fh, replacement->fh_len, flags, 0, 0, NULL, NULL,
        replacement->fh, replacement->fh_len, &outcome, rename_status, f);
    wait_done(f);
    assert(f->mount_status == CHIMERA_VFS_OK && outcome == CHIMERA_VFS_RENAME_OUTCOME_NOOP);
    assert(!test.notifications);
    rename_expect_name(f, cred, "matched-remove", replacement);
    rename_expect_name(f, cred, "matched-alias", replacement);
    chimera_vfs_notify_watch_destroy(f->vfs->vfs_notify, parent);
    chimera_vfs_release(f->thread, target);
    chimera_vfs_release(f->thread, old);
    chimera_vfs_release(f->thread, replacement);
}

struct coordinate_identity_cell {
    unsigned calls_a, calls_b;
};

struct coordinate_identity_test {
    struct fixture *fixture;
    struct coordinate_identity_cell cells[3];
    unsigned finishes;
};

static void
coordinate_identity_a(struct chimera_vfs_compound *cp, uint32_t index, uint64_t token,
                      const uint8_t *fh, uint32_t fh_len, void *private)
{
    struct coordinate_identity_cell *cell = private;
    (void) index; (void) fh; (void) fh_len;
    cell->calls_a++;
    assert(chimera_vfs_compound_coordinate_done(cp, token, CHIMERA_VFS_OK));
}

static void
coordinate_identity_b(struct chimera_vfs_compound *cp, uint32_t index, uint64_t token,
                      const uint8_t *fh, uint32_t fh_len, void *private)
{
    struct coordinate_identity_cell *cell = private;
    (void) index; (void) fh; (void) fh_len;
    cell->calls_b++;
    assert(chimera_vfs_compound_coordinate_done(cp, token, CHIMERA_VFS_OK));
}

static void
coordinate_identity_append(struct chimera_vfs_compound *cp, uint32_t index,
                           enum chimera_vfs_error *status, void *private)
{
    struct coordinate_identity_test *test = private;
    (void) index;
    assert(*status == CHIMERA_VFS_OK);
    /* Attempt two reuses both physical slots for different semantic work:
     * slot 2 changes the function, slot 3 changes only the private context.
     * Attempt three recreates attempt two exactly and must replay its memos. */
    assert(chimera_vfs_compound_add_coordinate(cp,
        test->finishes ? coordinate_identity_b : coordinate_identity_a,
        &test->cells[0]) == 2);
    assert(chimera_vfs_compound_add_coordinate(cp, coordinate_identity_b,
        &test->cells[test->finishes ? 2 : 1]) == 3);
}

static void
coordinate_identity_finish(struct chimera_vfs_compound *cp, void *private)
{
    struct coordinate_identity_test *test = private;
    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(test->cells[0].calls_a == 1 && test->cells[1].calls_b == 1);
    assert(test->cells[0].calls_b == (test->finishes != 0));
    assert(test->cells[2].calls_b == (test->finishes != 0));
    test->finishes++;
    chimera_vfs_compound_finish_result(cp,
        test->finishes < 3 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}

static void
coordinate_identity_complete(struct chimera_vfs_compound *cp, void *private)
{
    struct coordinate_identity_test *test = private;
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    test->fixture->done = 1;
}

static void
coordinate_identity_bad_prepare(struct chimera_vfs_compound *cp, uint32_t index,
                                enum chimera_vfs_error *status, void *private)
{
    (void) index; (void) status;
    assert(chimera_vfs_compound_add_coordinate(cp, coordinate_identity_a, private) == -1);
}

static void
check_coordinate_identity(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct coordinate_identity_test test = { .fixture = f };
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    int append = chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_callbacks(cp, append, NULL, coordinate_identity_append, &test);
    add_group(cp, 0, 2, NULL, -1, false, f);
    chimera_vfs_compound_set_finish_handler(cp, coordinate_identity_finish, &test);
    chimera_vfs_compound_submit(cp, coordinate_identity_complete, &test);
    wait_done(f);
    assert(test.finishes == 3);
    chimera_vfs_compound_free(cp);

    cp = chimera_vfs_compound_alloc(f->thread, cred);
    chimera_vfs_compound_add_putfh(cp, f->fh, f->fh_len);
    append = chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_prepare(cp, append, coordinate_identity_bad_prepare, &test.cells[0]);
    add_group(cp, 0, 2, NULL, -1, false, f);
    chimera_vfs_compound_submit(cp, completed, f);
    wait_done(f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    assert(test.cells[0].calls_a == 1);
    chimera_vfs_compound_free(cp);
}

struct range_wait_test {
    struct chimera_vfs_claim_owner *owner, *holder;
    struct chimera_vfs_claim_journal *held;
    struct chimera_vfs_claim_exact_range range;
    unsigned mode, notified, retired;
    bool canceled;
};

static void
range_wait_retired(void *private)
{
    struct range_wait_test *test = private;
    test->retired++;
}

static void
range_waiting(struct chimera_vfs_compound *cp, uint32_t index, void *private)
{
    struct range_wait_test *test = private;
    struct chimera_vfs_claim_batch_result result;
    test->notified++;
    assert(test->notified == 1);
    if (test->mode == 0) {
        assert(chimera_vfs_compound_range_cancel(cp, index));
    } else if (test->mode == 1) {
        assert(chimera_vfs_compound_cancel(cp));
    } else if (test->mode == 2) {
        chimera_vfs_claim_journal_reset(test->held);
        chimera_vfs_claim_journal_unlock(test->held, test->holder, &test->range, 1, &result);
        assert(result.status == CHIMERA_VFS_OK);
        chimera_vfs_claim_journal_seal(test->held);
        chimera_vfs_claim_journal_publish(test->held);
        chimera_vfs_claim_journal_complete(test->held);
    } else if (test->mode == 3) {
        /* Failed admission releases the idle owner pin before notification. */
        assert(chimera_vfs_claim_owner_retire(test->owner, range_wait_retired, test));
        assert(test->retired == 1);
    } else if (test->mode == 5) {
        test->canceled = true;
    }
}

static bool
range_wait_canceled(struct chimera_vfs_compound *cp, uint32_t index, void *private)
{
    struct range_wait_test *test = private;
    (void) cp; (void) index;
    return test->canceled;
}

static void
check_range_waits(struct fixture *f, const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(f->thread->vfs->vfs_state,
        f->fh, f->fh_len, chimera_vfs_hash(f->fh, f->fh_len), true);
    for (unsigned mode = 0; mode < 6; mode++) {
        struct range_wait_test test = { .range = { 0, 8, true }, .mode = mode };
        struct chimera_claim_owner identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
                                                .client_key = 900, .owner_lo = 900 };
        test.owner = chimera_vfs_claim_owner_create(file, &identity);
        identity.owner_lo++;
        test.holder = chimera_vfs_claim_owner_create(file, &identity);
        test.held = chimera_vfs_claim_journal_alloc(1, 1);
        struct chimera_vfs_claim_batch_result result;
        chimera_vfs_claim_journal_acquire(test.held, test.holder, &test.range, 1, &result);
        assert(result.status == CHIMERA_VFS_OK);
        chimera_vfs_claim_journal_seal(test.held);
        chimera_vfs_claim_journal_publish(test.held);
        chimera_vfs_claim_journal_complete(test.held);
        struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, cred);
        chimera_vfs_compound_add_range_batch(cp, test.owner, &test.range, 1, false);
        struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(cp, 0);
        op->range_wait = 1;
        op->range_timeout_ms = mode == 4 ? 3 : 0;
        op->range_on_wait = range_waiting;
        op->range_is_canceled = range_wait_canceled;
        op->range_wait_private = &test;
        add_group(cp, 0, 1, NULL, -1, true, f);
        chimera_vfs_compound_add_checkpoint(cp);
        add_group(cp, 1, 1, NULL, -1, true, f);
        chimera_vfs_compound_submit(cp, completed, f);
        wait_done(f);
        assert(test.notified == 1);
        enum chimera_vfs_error expected = mode == 2 ? CHIMERA_VFS_OK :
            mode == 4 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_EINTR;
        assert(chimera_vfs_compound_group_status(cp, 0) == expected);
        assert(chimera_vfs_compound_op(cp, 1)->completed == (mode != 1));
        assert(!chimera_vfs_compound_range_cancel(cp, 0));
        chimera_vfs_compound_free(cp);
        if (!test.retired) assert(chimera_vfs_claim_owner_retire(test.owner, NULL, NULL));
        assert(chimera_vfs_claim_owner_retire(test.holder, NULL, NULL));
        chimera_vfs_claim_owner_put(test.owner);
        chimera_vfs_claim_owner_put(test.holder);
        chimera_vfs_claim_journal_free(test.held);
    }
    chimera_vfs_state_put(f->thread->vfs->vfs_state, file);
}

int
main(void)
{
    struct fixture f = { 0 };
    struct chimera_vfs_module_cfg modules[2] = { 0 };
    struct chimera_vfs_cred root, user;
    struct chimera_vfs_compound *cp;
    struct prometheus_metrics *metrics;
    uint8_t root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    strcpy(modules[0].module_name, "memfs");
    strcpy(modules[1].module_name, "memkv");
    metrics = prometheus_metrics_create(NULL, NULL, 0);
    evpl_init(NULL);
    struct evpl_thread_config *config = evpl_thread_config_init();
    evpl_thread_config_set_wait_ms(config, 1);
    f.evpl = evpl_create(config);
    f.vfs = chimera_vfs_init(0, 0, modules, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(f.vfs);
    f.thread = chimera_vfs_thread_init(f.evpl, f.vfs);
    assert(f.thread);
    chimera_vfs_cred_init_unix(&root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&user, 12345, 12345, 0, NULL);
    chimera_vfs_mkfs(f.thread, &root, "memfs", "groups", NULL, mounted, &f);
    wait_done(&f);
    assert(f.mount_status == CHIMERA_VFS_OK);
    chimera_vfs_mount(f.thread, &root, "/mem", "memfs", "groups", NULL, mounted, &f);
    wait_done(&f);
    assert(f.mount_status == CHIMERA_VFS_OK);
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(f.thread, &root, root_fh, root_fh_len, "mem", 3,
                      CHIMERA_VFS_ATTR_FH, 0, looked_up, &f);
    wait_done(&f);

    /* The first group's failed op is real EAGAIN, its suffix and dependent
     * group do not execute, and an unrelated final group still succeeds. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    for (unsigned i = 0; i < 4; i++) {
        assert(chimera_vfs_compound_add_checkpoint(cp) == (int) i);
    }
    chimera_vfs_compound_set_op_prepare(cp, 0, fail_again, &f);
    add_group(cp, 0, 2, NULL, -1, true, &f);
    add_group(cp, 2, 1, NULL, 0, true, &f);
    add_group(cp, 3, 1, NULL, -1, true, &f);
    f.callbacks = 0;
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(f.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EAGAIN);
    assert(chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_num_completed(cp) == 2);
    assert(chimera_vfs_compound_group_status(cp, 0) == CHIMERA_VFS_EAGAIN);
    assert(chimera_vfs_compound_group_status(cp, 1) == CHIMERA_VFS_EACCES);
    assert(chimera_vfs_compound_group_status(cp, 2) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_group_context(cp, 2) == &f);
    for (unsigned i = 1; i <= 2; i++) {
        assert(chimera_vfs_compound_op(cp, i)->status == CHIMERA_VFS_UNSET);
        assert(!chimera_vfs_compound_op(cp, i)->completed);
    }
    assert(chimera_vfs_compound_op(cp, 3)->completed);
    chimera_vfs_compound_free(cp);

    /* Static op 2 executes AFTER dynamically appended physical slots 3/4,
     * from both a complete callback and a prepare callback. Retry discards
     * the suffix and reconstructs it with identical stable original indices. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    for (unsigned i = 0; i < 3; i++) {
        assert(chimera_vfs_compound_add_checkpoint(cp) == (int) i);
        chimera_vfs_compound_set_op_callbacks(cp, i, NULL, record, &f);
    }
    chimera_vfs_compound_set_op_callbacks(cp, 0, NULL, append_suffix, &f);
    chimera_vfs_compound_set_op_prepare(cp, 1, append_suffix, &f);
    add_group(cp, 0, 2, NULL, -1, true, &f);
    add_group(cp, 2, 1, NULL, 0, true, &f);
    f.attempts = f.finishes = f.callbacks = 0;
    chimera_vfs_compound_set_attempt_reset(cp, attempt_reset, &f);
    chimera_vfs_compound_set_finish_handler(cp, reject_first, &f);
    chimera_vfs_compound_submit(cp, retry_complete, &f);
    wait_done(&f);
    assert(f.attempts == 2 && f.finishes == 2 && f.callbacks == 2);
    chimera_vfs_compound_free(cp);

    /* Cursor isolation: GETFH in a following group cannot reuse a prior
     * group's object, even if that prior group failed after PUTFH. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    int failed = chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_prepare(cp, failed, fail_again, &f);
    add_group(cp, 0, 2, NULL, -1, true, &f);
    int getfh = chimera_vfs_compound_add_getfh(cp);
    add_group(cp, 2, 1, NULL, -1, true, &f);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_op(cp, getfh)->status == CHIMERA_VFS_EINVAL);
    assert(chimera_vfs_compound_group_status(cp, 1) == CHIMERA_VFS_EINVAL);
    chimera_vfs_compound_free(cp);

    /* Exact credentials are selected independently per group. The mounted
     * root is made read-only for other users; ACCESS must not reuse root's
     * cursor or inferred authorization in either direction. */
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0755 };
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    chimera_vfs_compound_add_setattr(cp, NULL, &attrs, 0);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    for (unsigned i = 0; i < 3; i++) {
        chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
        chimera_vfs_compound_add_access(cp, CHIMERA_ACE_WRITE_DATA);
        add_group(cp, i * 2, 2, i == 1 ? &user : NULL, -1, true, &f);
    }
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, 1)->granted & CHIMERA_ACE_WRITE_DATA);
    assert(!(chimera_vfs_compound_op(cp, 3)->granted & CHIMERA_ACE_WRITE_DATA));
    assert(chimera_vfs_compound_op(cp, 5)->granted & CHIMERA_ACE_WRITE_DATA);
    chimera_vfs_compound_free(cp);

    /* A dynamically appended OPEN survives the next group's cursor reset;
     * its stable result can address a physically earlier consumer. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    int discover = chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_callbacks(cp, discover, NULL, append_open, &f);
    add_group(cp, 0, 2, NULL, -1, true, &f);
    int consume = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
    chimera_vfs_compound_set_op_prepare(cp, consume, bind_dynamic_open, &f);
    add_group(cp, 2, 1, NULL, 0, true, &f);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, consume)->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
    assert(chimera_vfs_compound_num_completed(cp) == 4);
    chimera_vfs_compound_free(cp);

    /* Result-to-cursor binding preserves ownership across groups, CLOSE,
     * a rejected read-only attempt, and a separate GETHANDLE reference. Test
     * both a static producer and a dynamic producer with a higher index. */
    for (unsigned dynamic = 0; dynamic < 2; dynamic++) {
        cp = chimera_vfs_compound_alloc(f.thread, &root);
        chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
        if (dynamic) {
            int checkpoint = chimera_vfs_compound_add_checkpoint(cp);
            chimera_vfs_compound_set_op_callbacks(cp, checkpoint, NULL, append_open, &f);
        } else {
            f.dynamic_open = chimera_vfs_compound_add_open(cp, NULL, 0,
                CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0);
        }
        add_group(cp, 0, 2, NULL, -1, true, &f);
        int cursor = chimera_vfs_compound_add_puthandle_from(cp, dynamic ? -1 : 1,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY);
        assert(cursor == 2);
        if (dynamic) {
            chimera_vfs_compound_set_op_prepare(cp, cursor, bind_dynamic_cursor, &f);
        }
        assert(chimera_vfs_compound_add_gethandle(cp) == 3);
        assert(chimera_vfs_compound_add_close(cp) == 4);
        add_group(cp, 2, 3, NULL, 0, true, &f);
        f.finishes = f.callbacks = 0;
        chimera_vfs_compound_set_finish_handler(cp, reject_first, &f);
        chimera_vfs_compound_submit(cp, provenance_complete, &f);
        wait_done(&f);
        assert(f.finishes == 2 && f.callbacks == 2);
        struct chimera_vfs_open_handle *held = chimera_vfs_compound_take_handle(cp, 3);
        assert(held);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(f.thread, held);
    }

    /* Saved/current aliases of the SAME result do not become independently
     * owning references; closing one invalidates the other. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    int producer = chimera_vfs_compound_add_open(cp, NULL, 0,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0, NULL, 0);
    chimera_vfs_compound_add_savehandle(cp);
    chimera_vfs_compound_add_puthandle_from(cp, producer,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY);
    chimera_vfs_compound_add_close(cp);
    int restored = chimera_vfs_compound_add_restorehandle(cp);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    assert(chimera_vfs_compound_op(cp, restored)->status == CHIMERA_VFS_EINVAL);
    assert(!chimera_vfs_compound_op(cp, producer)->out_handle);
    chimera_vfs_compound_free(cp);

    /* Group-local append cannot sneak typed locks past dedicated-compound
     * validation. The rejection neither appends nor allocates a lock attempt. */
    for (unsigned grouped = 0; grouped < 2; grouped++) {
        cp = chimera_vfs_compound_alloc(f.thread, &root);
        chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_set_op_callbacks(cp, 0, NULL, append_forbidden_lock, &f);
        if (grouped) {
            add_group(cp, 0, 1, NULL, -1, true, &f);
            add_group(cp, 1, 1, NULL, -1, true, &f);
        }
        chimera_vfs_compound_submit(cp, completed, &f);
        wait_done(&f);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTSUP);
        assert(chimera_vfs_compound_num_completed(cp) == 1);
        assert(!chimera_vfs_compound_op(cp, 1)->completed);
        chimera_vfs_compound_free(cp);
    }

    /* Typed exact claims run within execution groups and expose the overlay
     * to intervening I/O checks. Finish rejection discards the full attempt;
     * retirement notification waits for accepted frontend publication/free. */
    {
        struct range_compound_test test = { .fixture = &f };
        struct chimera_claim_owner identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
                                               .client_key = 777, .owner_lo = 777 };
        struct chimera_vfs_file_state *file = chimera_vfs_state_get(f.vfs->vfs_state,
            f.fh, f.fh_len, chimera_vfs_hash(f.fh, f.fh_len), true);
        assert(file);
        test.owner = chimera_vfs_claim_owner_create(file, &identity);
        chimera_vfs_state_put(f.vfs->vfs_state, file);
        assert(test.owner);
        test.handle.fh_len = f.fh_len;
        memcpy(test.handle.fh, f.fh, f.fh_len);
        test.handle.fh_hash = chimera_vfs_hash(f.fh, f.fh_len);
        struct chimera_vfs_claim_exact_range range = { .offset = 10, .length = 5, .exclusive = true };
        cp = chimera_vfs_compound_alloc(f.thread, &root);
        assert(chimera_vfs_compound_add_range_batch(cp, test.owner, &range, 1, false) == 0);
        add_group(cp, 0, 1, NULL, -1, true, &f);
        assert(chimera_vfs_compound_add_checkpoint(cp) == 1);
        chimera_vfs_compound_set_op_prepare(cp, 1, range_view_check, &test);
        add_group(cp, 1, 1, NULL, -1, true, &f);
        assert(chimera_vfs_compound_add_range_batch(cp, test.owner, &range, 1, true) == 2);
        add_group(cp, 2, 1, NULL, -1, true, &f);
        assert(chimera_vfs_compound_add_checkpoint(cp) == 3);
        chimera_vfs_compound_set_op_prepare(cp, 3, range_view_check, &test);
        add_group(cp, 3, 1, NULL, -1, true, &f);
        f.finishes = f.callbacks = 0;
        chimera_vfs_compound_set_finish_handler(cp, reject_first, &f);
        chimera_vfs_compound_submit(cp, range_compound_complete, &test);
        wait_done(&f);
        assert(f.finishes == 2 && f.callbacks == 2);
        chimera_vfs_compound_free(cp);
        assert(test.retired == 1);
        chimera_vfs_claim_owner_put(test.owner);
    }

    check_cancel_scopes(&f, &root);
    check_range_owner_production(&f, &root);
    check_closed_producer_anchor(&f, &root);
    check_access_compound(&f, &root);
    check_private_access_cleanup(&f, &root);
    check_compound_narrow(&f, &root);
    check_open_claim_retirement(&f, &root);
    check_handle_state_dac(&f, &root, &user);
    check_search_key_pages(&f, &root);
    check_matched_remove(&f, &root, &user);
    check_matched_remove_stream(&f, &root);
    check_overwrite(&f, &root);
    check_mkdir_notification(&f, &root);
    check_special_create_notification(&f, &root, &user);
    check_matched_rename(&f, &root);
    check_coordinate_identity(&f, &root);
    check_range_waits(&f, &root);

    /* Cancellation does not free/complete a parked coordination. Its owner
     * drains exactly once; the next group stays untouched and retry fails. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    chimera_vfs_compound_add_coordinate(cp, park, &f);
    add_group(cp, 0, 2, NULL, -1, true, &f);
    chimera_vfs_compound_add_checkpoint(cp);
    add_group(cp, 2, 1, NULL, -1, true, &f);
    f.callbacks = 0;
    f.token = 0;
    chimera_vfs_compound_submit(cp, completed, &f);
    assert(f.token && !f.callbacks);
    assert(chimera_vfs_compound_cancel(cp));
    assert(chimera_vfs_compound_cancel(cp));
    assert(!f.callbacks);
    assert(chimera_vfs_compound_coordinate_done(cp, f.token, CHIMERA_VFS_OK));
    wait_done(&f);
    assert(f.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINTR);
    assert(chimera_vfs_compound_group_status(cp, 0) == CHIMERA_VFS_EINTR);
    assert(chimera_vfs_compound_group_status(cp, 1) == CHIMERA_VFS_UNSET);
    assert(!chimera_vfs_compound_op(cp, 2)->completed);
    assert(!chimera_vfs_compound_retry(cp));
    assert(!chimera_vfs_compound_cancel(cp));
    assert(!chimera_vfs_compound_coordinate_done(cp, f.token, CHIMERA_VFS_OK));
    chimera_vfs_compound_free(cp);

    /* Cancellation from prepare prevents even the current operation's action. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_putfh(cp, f.fh, f.fh_len);
    chimera_vfs_compound_set_op_prepare(cp, 0, cancel_prepare, &f);
    add_group(cp, 0, 1, NULL, -1, true, &f);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_op(cp, 0)->status == CHIMERA_VFS_EINTR);
    assert(!chimera_vfs_compound_op(cp, 0)->fh_len);
    chimera_vfs_compound_free(cp);

    /* Default behavior and explicit stop policy both stop at the first error. */
    for (unsigned grouped = 0; grouped < 2; grouped++) {
        cp = chimera_vfs_compound_alloc(f.thread, &root);
        chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_set_op_prepare(cp, 0, fail_again, &f);
        if (grouped) {
            add_group(cp, 0, 1, NULL, -1, false, &f);
            add_group(cp, 1, 1, NULL, -1, true, &f);
        }
        chimera_vfs_compound_submit(cp, completed, &f);
        wait_done(&f);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EAGAIN);
        assert(chimera_vfs_compound_num_completed(cp) == 1);
        assert(!chimera_vfs_compound_op(cp, 1)->completed);
        chimera_vfs_compound_free(cp);
    }

    /* Incomplete static coverage fails before any execution. */
    cp = chimera_vfs_compound_alloc(f.thread, &root);
    chimera_vfs_compound_add_checkpoint(cp);
    add_group(cp, 0, 1, NULL, -1, true, &f);
    chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_submit(cp, completed, &f);
    wait_done(&f);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    assert(chimera_vfs_compound_num_completed(cp) == 0);
    chimera_vfs_compound_free(cp);

    chimera_vfs_umount(f.thread, &root, "/mem", mounted, &f);
    wait_done(&f);
    chimera_vfs_thread_destroy(f.thread);
    chimera_vfs_destroy(f.vfs);
    evpl_destroy(f.evpl);
    prometheus_metrics_destroy(metrics);
    fprintf(stderr, "vfs_compound_groups_test: all checks passed\n");
    return 0;
}
