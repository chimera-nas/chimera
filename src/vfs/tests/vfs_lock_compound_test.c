// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the shared NLM/FUSE lock contract without a protocol transport:
 * metadata composition, rejected finish, runtime scope checks and retirement
 * across the gap between frontend admission and worker-side construction. */
#include <fcntl.h>
#include <stdio.h>
#include "compound_test_util.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_lock.h"
#include "vfs/vfs_release.h"
#include "prometheus-c.h"

struct fixture {
    struct evpl                    *evpl;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_lock_domain *domain;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_cred         cred;
    struct chimera_vfs_lock_request request;
    unsigned                        finishes, callouts;
    int                             done;
};

static void
mounted(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct fixture *f = private_data;

    (void) thread;
    assert(status == CHIMERA_VFS_OK);
    f->done = 1;
} /* mounted */

static void
wait_done(struct fixture *f)
{
    while (!f->done) {
        evpl_continue(f->evpl);
    }
    f->done = 0;
} /* wait_done */

static void
check_unpinned(
    struct fixture                 *f,
    struct chimera_vfs_open_handle *handle)
{
    assert(!chimera_vfs_state_get(f->vfs->vfs_state, handle->fh, handle->fh_len,
                                  handle->fh_hash, false));
} /* check_unpinned */

static void
hold_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct fixture *f = private_data;

    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    f->finishes++;
} /* hold_finish */

static void
check_retirement(struct fixture *f)
{
    /* A synthetic identity avoids unrelated open-cache references masking a
     * leaked domain pin. Local lock operations never dereference its backend. */
    struct chimera_vfs_open_handle handle = { .fh = { 0xfe, 1 }, .fh_len = 2 };

    handle.fh_hash = chimera_vfs_hash(handle.fh, handle.fh_len);
    for (unsigned all = 0; all < 2; all++) {
        struct chimera_vfs_lock_request request = {
            .owner = { .client_key = 17, .owner_lo = 23 },
            .type  = CHIMERA_VFS_LOCK_WRITE, .whence = SEEK_SET, .length = 100,
        };
        request.owner.proto = all ? CHIMERA_CLAIM_PROTO_NLM : CHIMERA_CLAIM_PROTO_FUSE;
        request.generation  = chimera_vfs_lock_domain_admit(f->domain, &handle, &request.owner);
        assert(request.generation);
        check_unpinned(f, &handle); /* admission alone has no file-state pin */
        struct chimera_vfs_compound    *cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
        assert(chimera_vfs_compound_add_lock_change(cp, f->domain, &handle, &request) == 0);
        assert(compound_test_run(f->evpl, cp) == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
        assert(chimera_vfs_lock_domain_has_locks(f->domain, handle.fh, handle.fh_len, &request.owner));
        if (all) {
            chimera_vfs_lock_domain_retire_all(f->domain);
        } else {
            chimera_vfs_lock_domain_retire(f->domain, handle.fh, handle.fh_len, &request.owner);
        }
        check_unpinned(f, &handle);

        /* An admission queued before close must still fail after the file
         * state is reclaimed and a worker finally constructs its compound. */
        cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
        assert(chimera_vfs_compound_add_lock_change(cp, f->domain, &handle, &request) == 0);
        assert(compound_test_run(f->evpl, cp) == CHIMERA_VFS_EINTR);
        chimera_vfs_compound_free(cp);
        check_unpinned(f, &handle);
        uint64_t old_generation = request.generation;
        request.generation = chimera_vfs_lock_domain_admit(f->domain, &handle, &request.owner);
        assert(request.generation > old_generation);

        /* Keep a finish-pending attempt alive across the same cutoff: the
         * reservation is detached, but its snapshot remains pinned until free. */
        cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
        chimera_vfs_compound_add_lock_change(cp, f->domain, &handle, &request);
        f->finishes = 0;
        chimera_vfs_compound_set_finish_handler(cp, hold_finish, f);
        chimera_vfs_compound_submit(cp, compound_test_done_cb, &f->done);
        while (!f->finishes) {
            evpl_continue(f->evpl);
        }
        assert(!f->done);
        if (all) {
            chimera_vfs_lock_domain_retire_all(f->domain);
        } else {
            chimera_vfs_lock_domain_retire(f->domain, handle.fh, handle.fh_len, &request.owner);
        }
        chimera_vfs_compound_finish_result(cp, CHIMERA_VFS_OK);
        wait_done(f);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINTR);
        chimera_vfs_compound_free(cp);
        check_unpinned(f, &handle);
    }
} /* check_retirement */

static void
append_metadata(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct fixture *f = private_data;

    (void) index; (void) status;
    f->callouts++;
    assert(chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE) >= 0);
} /* append_metadata */

static void
retry_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct fixture *f = private_data;

    assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_OK);
    assert(!chimera_vfs_lock_domain_has_locks(f->domain, f->handle->fh,
                                              f->handle->fh_len, &f->request.owner));
    assert((chimera_vfs_compound_op(cp, chimera_vfs_compound_num_ops(cp) - 1)->attr.va_mode & 0777) == 0600);
    chimera_vfs_compound_finish_result(cp, ++f->finishes == 1 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* retry_finish */

static void
retry_complete(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct fixture *f = private_data;

    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    f->done = 1;
} /* retry_complete */

static void
append_mutation(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_vfs_attrs attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0000 };

    (void) index; (void) status; (void) private_data;
    assert(chimera_vfs_compound_add_setattr(cp, NULL, &attrs, 0, 0) >= 0);
} /* append_mutation */

static void
prepare_truncate(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) status; (void) private_data;
    chimera_vfs_compound_op_args(cp, index)->open_flags |= CHIMERA_VFS_OPEN_TRUNCATE;
} /* prepare_truncate */

static void
check_composition(struct fixture *f)
{
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f->thread, &f->cred);

    /* Resolve a runtime path, retain its handle, stage the lock, then append
     * result queries. Every callout and dynamic suffix repeats after retry. */
    chimera_vfs_compound_add_putroot(cp);
    chimera_vfs_compound_add_lookup(cp, "mem", 3, 0, 0);
    chimera_vfs_compound_add_lookup(cp, "lock", 4, 0, 0);
    chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_gethandle(cp);
    chimera_vfs_compound_add_lock_change(cp, f->domain, f->handle, &f->request);
    int check = chimera_vfs_compound_add_checkpoint(cp);
    chimera_vfs_compound_set_op_prepare(cp, check, append_metadata, f);
    f->finishes = f->callouts = 0;
    chimera_vfs_compound_set_finish_handler(cp, retry_finish, f);
    chimera_vfs_compound_submit(cp, retry_complete, f);
    wait_done(f);
    assert(f->finishes == 2 && f->callouts == 2);
    assert(chimera_vfs_compound_num_ops(cp) == 8);
    assert(chimera_vfs_lock_domain_has_locks(f->domain, f->handle->fh,
                                             f->handle->fh_len, &f->request.owner));
    chimera_vfs_compound_free(cp);

    for (unsigned variant = 0; variant < 5; variant++) {
        cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
        int lock = chimera_vfs_compound_add_lock_change_fh(cp, f->domain, f->handle->fh,
                                                           f->handle->fh_len, &f->request);
        assert(lock == 3);
        switch (variant) {
            case 0: /* Reject a static mutation before even PUTFH executes. */
                append_mutation(cp, 0, NULL, NULL);
                break;
            case 1: /* The old four-op shape allowed mutable OPEN flags. */
                chimera_vfs_compound_set_op_prepare(cp, 1, prepare_truncate, NULL);
                break;
            case 2: /* A callback must not evade submit's scope check. */
                check = chimera_vfs_compound_add_checkpoint(cp);
                chimera_vfs_compound_set_op_prepare(cp, check, append_mutation, NULL);
                break;
            case 3: /* Two journals for one owner would deadlock admission. */
                chimera_vfs_compound_add_lock_change(cp, f->domain, f->handle, &f->request);
                break;
            case 4: /* Projection keeps the dedicated compound contract. */
                chimera_vfs_compound_free(cp);
                cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
                struct chimera_vfs_lock_request projected = f->request;
                projected.project_backend = true;
                chimera_vfs_compound_add_lock_change(cp, f->domain, f->handle, &projected);
                chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
                break;
        } /* switch */
        assert(compound_test_run(f->evpl, cp) == CHIMERA_VFS_ENOTSUP);
        if (variant == 0 || variant == 3 || variant == 4) {
            assert(!chimera_vfs_compound_op(cp, 0)->completed);
        }
        chimera_vfs_compound_free(cp);
    }
    cp = chimera_vfs_compound_alloc(f->thread, &f->cred);
    chimera_vfs_compound_add_puthandle(cp, f->handle, CHIMERA_VFS_OPEN_INFERRED);
    int attr = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
    assert(compound_test_run(f->evpl, cp) == CHIMERA_VFS_OK);
    assert((chimera_vfs_compound_op(cp, attr)->attr.va_mode & 0777) == 0600);
    chimera_vfs_compound_free(cp);
} /* check_composition */

int
main(void)
{
    struct fixture                f          = { 0 };
    struct chimera_vfs_module_cfg modules[2] = { { .module_name = "memfs" }, { .module_name = "memkv" } };
    struct prometheus_metrics    *metrics    = prometheus_metrics_create(NULL, NULL, 0);

    evpl_init(NULL);
    f.evpl = evpl_create(NULL);
    f.vfs  = chimera_vfs_init(0, 0, modules, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(f.vfs);
    f.thread = chimera_vfs_thread_init(f.evpl, f.vfs);
    assert(f.thread);
    chimera_vfs_cred_init_unix(&f.cred, 0, 0, 0, NULL);
    f.domain = chimera_vfs_lock_domain_create(f.vfs);
    assert(f.domain);
    check_retirement(&f);
    chimera_vfs_mkfs(f.thread, &f.cred, "memfs", "locks", NULL, mounted, &f);
    wait_done(&f);
    chimera_vfs_mount(f.thread, &f.cred, "/mem", "memfs", "locks", NULL, mounted, &f);
    wait_done(&f);
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(f.thread, &f.cred);
    chimera_vfs_compound_add_putroot(cp);
    chimera_vfs_compound_add_lookup(cp, "mem", 3, 0, 0);
    struct chimera_vfs_attrs     attrs  = { .va_set_mask = CHIMERA_VFS_ATTR_MODE, .va_mode = 0600 };
    int                          opened = chimera_vfs_compound_add_open(cp, "lock", 4, CHIMERA_VFS_OPEN_CREATE, 0, &
                                                                        attrs, 0, 0, 0);
    assert(compound_test_run(f.evpl, cp) == CHIMERA_VFS_OK);
    f.handle = chimera_vfs_compound_take_handle(cp, opened);
    assert(f.handle);
    chimera_vfs_compound_free(cp);
    f.request = (struct chimera_vfs_lock_request) {
        .owner = { .proto = CHIMERA_CLAIM_PROTO_FUSE, .client_key = 99, .owner_lo = 99 },
        .type  = CHIMERA_VFS_LOCK_WRITE, .whence = SEEK_SET, .length = 100,
    };
    f.request.generation = chimera_vfs_lock_domain_admit(f.domain, f.handle, &f.request.owner);
    assert(f.request.generation);
    check_composition(&f);
    chimera_vfs_lock_domain_shutdown(NULL, f.domain);
    chimera_vfs_lock_domain_destroy(f.domain);
    chimera_vfs_release(f.thread, f.handle);
    chimera_vfs_thread_destroy(f.thread);
    chimera_vfs_destroy(f.vfs);
    evpl_destroy(f.evpl);
    prometheus_metrics_destroy(metrics);
    puts("ok: local lock composition, retry, runtime scope and retirement lifetimes");
    return 0;
} /* main */
