// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense

/* Real NLM RPCs. Only local range journals and bookkeeping opens are rejected;
 * the fixture never asserts rollback of filesystem mutations. Control TESTs
 * use the same NLM connection/worker as held LOCK/UNLOCK requests. */
#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "nfs_aux_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "vfs/vfs_compound.h"

static atomic_int  armed, pause_before, pause_finish, resume_held;
static atomic_uint submissions, finishes, reject_count;
static atomic_bool fail_finish;
struct fixture {
    chimera_vfs_compound_callback_t callback;
    void                           *arg;
    struct chimera_vfs_compound    *compound;
    unsigned                        attempts;
    bool                            before;
};
static _Atomic(struct fixture *) held;
typedef void (*submit_fn)(
    struct chimera_vfs_compound *,
    chimera_vfs_compound_callback_t,
    void *);

static void
finish(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    struct fixture        *f = arg;

    f->attempts++;
    atomic_fetch_add(&finishes, 1);
    if (atomic_exchange(&pause_finish, 0)) {
        assert(!atomic_exchange(&held, f));
        return;
    }
    enum chimera_vfs_error status = CHIMERA_VFS_OK;
    if (atomic_load(&fail_finish)) {
        status = CHIMERA_VFS_EIO;
    } else if (atomic_load(&reject_count)) {
        atomic_fetch_sub(&reject_count, 1);
        status = CHIMERA_VFS_EAGAIN;
    }
    chimera_vfs_compound_finish_result(compound, status);
} /* finish */

static void
complete(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    struct fixture                 *f        = arg;
    chimera_vfs_compound_callback_t callback = f->callback;
    void                           *caller   = f->arg;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN || f->attempts == 9) {
        free(f);
    }
    callback(compound, caller);
} /* complete */

static void
resume(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    struct fixture *control = arg;
    struct fixture *f       = atomic_exchange(&held, NULL);

    assert(f);
    control->callback(compound, control->arg);
    free(control);
    if (f->before) {
        submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
        next(f->compound, complete, f);
    } else {
        chimera_vfs_compound_finish_result(f->compound, CHIMERA_VFS_OK);
    }
} /* resume */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *arg)
{
    submit_fn                             next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");

    assert(next);
    const struct chimera_vfs_compound_op *last = chimera_vfs_compound_op(compound,
                                                                         chimera_vfs_compound_num_ops(compound) - 1);
    if (last->type == CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE && atomic_exchange(&armed, 0)) {
        struct fixture *f = calloc(1, sizeof(*f));
        assert(f);
        f->callback = callback;
        f->arg      = arg;
        f->compound = compound;
        assert(chimera_vfs_compound_num_ops(compound) == 1 ||
               chimera_vfs_compound_num_ops(compound) == 2 ||
               chimera_vfs_compound_num_ops(compound) == 4);
        atomic_fetch_add(&submissions, 1);
        chimera_vfs_compound_set_finish_handler(compound, finish, f);
        if (atomic_exchange(&pause_before, 0)) {
            f->before = true;
            assert(!atomic_exchange(&held, f));
            return;
        }
        next(compound, complete, f);
    } else if (last->type == CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST && atomic_exchange(&resume_held, 0)) {
        struct fixture *f = calloc(1, sizeof(*f));
        assert(f);
        f->callback = callback;
        f->arg      = arg;
        next(compound, resume, f);
    } else {
        next(compound, callback, arg);
    }
} /* chimera_vfs_compound_submit */

static void
arm(unsigned rejects)
{
    atomic_store(&submissions, 0);
    atomic_store(&finishes, 0);
    atomic_store(&reject_count, rejects);
    atomic_store(&fail_finish, false);
    atomic_store(&armed, 1);
} /* arm */

static void
checked(
    struct mbt_aux_result *r,
    unsigned               status,
    unsigned               attempts)
{
    assert(r->nlm_stat == status);
    assert(atomic_load(&submissions) == 1);
    assert(atomic_load(&finishes) == attempts);
} /* checked */

static struct mbt_aux_result *
lock(
    struct mbt_env *env,
    struct mbt_fh  *fh,
    const char     *owner,
    int             msg,
    int             exclusive,
    int             block,
    uint64_t        offset,
    uint64_t        length)
{
    static const uint8_t oh = 1;

    return mbt_nlm_lock(env, msg ? 7 : 2, owner, fh, &oh, 1, 1,
                        exclusive, block, false, 0, offset, length, NULL, 0);
} /* lock */

static struct mbt_aux_result *
unlock(
    struct mbt_env *env,
    struct mbt_fh  *fh,
    int             msg,
    uint64_t        offset,
    uint64_t        length)
{
    static const uint8_t oh = 1;

    return mbt_nlm_unlock(env, msg, "owner", fh, &oh, 1, 1, offset, length, NULL, 0);
} /* unlock */

static unsigned
test(
    struct mbt_env *env,
    struct mbt_fh  *fh,
    uint64_t        offset,
    uint64_t        length)
{
    static const uint8_t oh = 2;

    return mbt_nlm_test(env, 0, "probe", fh, &oh, 1, 2, true, offset, length, NULL, 0)->nlm_stat;
} /* test */

static void
wait_held(struct mbt_env *env)
{
    for (unsigned i = 0; !atomic_load(&held) && i < 1000; i++) {
        mbt_aux_drain_us(env, 1000);
    }
    assert(atomic_load(&held));
    assert(mbt_aux(env)->nasync == 0);
} /* wait_held */

static void
pending_reply(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *reply,
    int                          status,
    void                        *private_data)
{
    unsigned *result = private_data;

    assert(!status);
    *result = reply->stat + 1;
} /* pending_reply */

static void
send_pending_lock(
    struct mbt_env *env,
    struct mbt_fh  *fh,
    unsigned       *result)
{
    static struct mbt_nlm_args scratch;
    static const uint8_t       oh   = 1;
    struct nlm4_lockargs       args = { .exclusive = true };

    mbt_nlm_fill_lock(&args.alock, &scratch, "owner", fh, &oh, 1, 1, 0, 100);
    env->nlm_v4.send_call_NLMPROC4_LOCK(&env->nlm_v4.rpc2, env->evpl,
                                        env->nlm_conn, &env->aux_cred, &args, 0, 0, NULL, 0, 0, pending_reply, result);
} /* send_pending_lock */

int
main(void)
{
    struct mbt_env     *env  = calloc(1, sizeof(*env));
    struct mbt_env_opts opts = { .disable_caches = 1 };

    assert(env);
    mbt_watchdog_arm(90);
    mbt_aux_env_open(env, &opts);
    mbt_env_fs_setup(env, "fs0");
    struct mbt_result  *res = mbt_mnt(env, "/fs0");
    assert(res->status == MNT3_OK);
    struct mbt_fh       root = res->obj_fh;
    res = mbt_create(env, &root, "file", 4, UNCHECKED, 0600, NULL);
    assert(res->status == NFS3_OK);
    struct mbt_fh       fh = res->obj_fh;

    arm(2);
    checked(lock(env, &fh, "owner", 0, true, false, 0, 100), NLM4_GRANTED, 3);
    assert(test(env, &fh, 0, 100) == NLM4_DENIED);
    arm(2);
    checked(unlock(env, &fh, 0, 20, 60), NLM4_GRANTED, 3);
    assert(test(env, &fh, 20, 60) == NLM4_GRANTED);
    assert(test(env, &fh, 0, 20) == NLM4_DENIED);
    assert(test(env, &fh, 80, 20) == NLM4_DENIED);
    arm(0);
    atomic_store(&fail_finish, true);
    checked(unlock(env, &fh, 0, 0, 0), NLM4_DENIED, 1);
    atomic_store(&fail_finish, false);
    assert(test(env, &fh, 0, 20) == NLM4_DENIED);
    assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED);
    arm(9);
    checked(lock(env, &fh, "owner", 0, true, false, 0, 100), NLM4_DENIED, 9);
    assert(test(env, &fh, 0, 100) == NLM4_GRANTED);

    /* EOF is the exclusive endpoint 2^64, not UINT64_MAX. */
    assert(lock(env, &fh, "owner", 0, true, false, UINT64_MAX, 0)->nlm_stat == NLM4_GRANTED);
    arm(1);
    checked(unlock(env, &fh, 0, UINT64_MAX, 0), NLM4_GRANTED, 2);
    assert(test(env, &fh, UINT64_MAX, 0) == NLM4_GRANTED);

    assert(lock(env, &fh, "owner", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
    mbt_aux_async_reset(env);
    arm(0);
    atomic_store(&pause_finish, 1);
    unlock(env, &fh, 1, 0, 0);
    wait_held(env);
    assert(test(env, &fh, 0, 100) == NLM4_DENIED);
    atomic_store(&resume_held, 1);
    assert(test(env, &fh, 0, 100) == NLM4_DENIED);
    mbt_aux_drain_for(env, 1, 1000000);
    assert(mbt_aux(env)->nasync == 1 && mbt_aux(env)->async[0].stat == NLM4_GRANTED);
    assert(test(env, &fh, 0, 100) == NLM4_GRANTED);

    /* Recovery owns the cutoff; pending request storage belongs to completion.
     * Cover both a compound not started yet and a granted-but-unfinished one. */
    for (unsigned before = 0; before < 2; before++) {
        mbt_aux_async_reset(env);
        arm(0);
        atomic_store(before ? &pause_before : &pause_finish, 1);
        lock(env, &fh, "owner", 1, true, false, 0, 100);
        wait_held(env);
        mbt_nlm_free_all(env, "owner", 1);
        atomic_store(&resume_held, 1);
        test(env, &fh, 0, 100);
        mbt_aux_drain_us(env, 10000);
        assert(!atomic_load(&held) && mbt_aux(env)->nasync == 0);
        assert(test(env, &fh, 0, 100) == NLM4_GRANTED);
        assert(lock(env, &fh, "owner", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
        assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED);
    }
    /* Cancellation before OPEN and during finish must not publish a lock. */
    for (unsigned before = 0; before < 2; before++) {
        static const uint8_t oh = 1;
        mbt_aux_async_reset(env);
        arm(0);
        atomic_store(before ? &pause_before : &pause_finish, 1);
        lock(env, &fh, "owner", 1, true, false, 0, 100);
        wait_held(env);
        assert(mbt_nlm_cancel(env, 0, "owner", &fh, &oh, 1, 1, true, false, 0, 100, NULL, 0)->nlm_stat == NLM4_GRANTED);
        atomic_store(&resume_held, 1);
        test(env, &fh, 0, 100);
        mbt_aux_drain_for(env, 1, 1000000);
        assert(mbt_aux(env)->nasync == 1 && mbt_aux(env)->async[0].stat == NLM4_DENIED);
        assert(test(env, &fh, 0, 100) == NLM4_GRANTED);
    }
    /* Recovery before BLOCKED still owes a reply to the original sync RPC. */
    for (unsigned before = 0; before < 2; before++) {
        unsigned result = 0;
        mbt_aux_async_reset(env);
        arm(0);
        atomic_store(before ? &pause_before : &pause_finish, 1);
        send_pending_lock(env, &fh, &result);
        wait_held(env);
        assert(!result);
        mbt_nlm_free_all(env, "owner", 1);
        atomic_store(&resume_held, 1);
        test(env, &fh, 0, 100);
        for (unsigned i = 0; !result && i < 1000; i++) {
            mbt_aux_drain_us(env, 1000);
        }
        assert(result == NLM4_DENIED + 1);
        assert(test(env, &fh, 0, 100) == NLM4_GRANTED);
    }

    /* A parked logical LOCK may retry, but must not resend BLOCKED. */
    assert(lock(env, &fh, "blocker", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
    mbt_aux_async_reset(env);
    arm(2);
    lock(env, &fh, "owner", 1, true, true, 0, 100);
    mbt_aux_drain_for(env, 1, 1000000);
    assert(mbt_aux(env)->nasync == 1 && mbt_aux(env)->async[0].stat == NLM4_BLOCKED);
    mbt_nlm_free_all(env, "blocker", 1);
    for (unsigned i = 0; atomic_load(&finishes) != 3 && i < 1000; i++) {
        mbt_aux_drain_us(env, 1000);
    }
    assert(atomic_load(&finishes) == 3 && mbt_aux(env)->nasync == 1);
    assert(test(env, &fh, 0, 100) == NLM4_DENIED);
    assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED);

    /* FREE_ALL releases in acquisition order. Grants for the same owner must
     * then replace intervals in arbiter order, regardless of doorbell order. */
    assert(lock(env, &fh, "blocker", 0, true, false, 16, 8)->nlm_stat == NLM4_GRANTED);
    assert(lock(env, &fh, "blocker", 0, true, false, 8, 8)->nlm_stat == NLM4_GRANTED);
    assert(lock(env, &fh, "owner", 0, true, true, 16, 8)->nlm_stat == NLM4_BLOCKED);
    assert(lock(env, &fh, "owner", 0, true, true, 8, 0)->nlm_stat == NLM4_BLOCKED);
    mbt_nlm_free_all(env, "blocker", 1);
    mbt_aux_drain_us(env, 10000);
    assert(test(env, &fh, 8, 0) == NLM4_DENIED);
    assert(mbt_aux(env)->r.holder_offset == 8 && mbt_aux(env)->r.holder_length == 0);
    assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED);

    /* NM_LOCK is non-monitored, but may still request blocking admission.
     * Reaping it after BLOCKED must never send a second reply on its encoding. */
    assert(lock(env, &fh, "blocker", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
    static const uint8_t nm_oh = 1;
    assert(mbt_nlm_lock(env, 22, "owner", &fh, &nm_oh, 1, 1,
                        true, true, false, 0, 0, 100, NULL, 0)->nlm_stat == NLM4_BLOCKED);
    mbt_nlm_free_all(env, "owner", 1);
    mbt_nlm_free_all(env, "blocker", 1);
    mbt_aux_drain_us(env, 10000);
    assert(test(env, &fh, 0, 100) == NLM4_GRANTED);

    /* An accepted handle anchor permits unlock after namespace removal. */
    assert(lock(env, &fh, "owner", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
    assert(mbt_remove(env, &root, "file", 4)->status == NFS3_OK);
    assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED);
    assert(unlock(env, &fh, 0, 0, 0)->nlm_stat == NLM4_GRANTED); /* stale, already unlocked */
    mbt_nlm_free_all(env, "owner", 1);
    /* Shutdown owns cancellation and drains the original worker. */
    res = mbt_create(env, &root, "shutdown", 8, UNCHECKED, 0600, NULL);
    assert(res->status == NFS3_OK);
    fh = res->obj_fh;
    assert(lock(env, &fh, "blocker", 0, true, false, 0, 100)->nlm_stat == NLM4_GRANTED);
    assert(lock(env, &fh, "owner", 0, true, true, 0, 100)->nlm_stat == NLM4_BLOCKED);
    mbt_env_stop(env);
    free(env);
    return 0;
} /* main */
