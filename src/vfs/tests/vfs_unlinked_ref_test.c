// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The open cache's bookkeeping for a file that lost its last link while it was
 * still referenced (src/vfs/vfs_unlinked.h).
 *
 * The test creates the record directly with chimera_vfs_unlinked_track, so it
 * exercises the open cache without an unlink.  Each case checks only what the
 * VFS reports: the record's counts, the results of opens, the events, and where
 * the purge of the file runs (a hook the close thread calls).
 *
 * The idle-close sweep is made slow (1.5 s) so that an idle handle stays put
 * until the test marks it for close, which makes the counts at each step deterministic.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_open_cache.h"
#include "vfs/vfs_unlinked.h"
#include "vfs/vfs_unlink_track.h"
#include "vfs/vfs_ref.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "common/logging.h"
#include "common/mbt_watchdog.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "CHECK FAILED %s:%d: %s\n", __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

static _Thread_local int is_main_thread;

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *handle;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_cred         root, other;
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        dir_fh_len;
    struct chimera_vfs_open_handle *dir;
};

static struct {
    int fired;
    int carrierless;
    int off_main_thread;
    int stall;
    int entered;
    int go;
} hook;

static void
purge_hook(
    const uint8_t *fh,
    uint16_t       fh_len,
    int            carrierless,
    void          *arg)
{
    __atomic_store_n(&hook.carrierless, carrierless, __ATOMIC_SEQ_CST);
    __atomic_store_n(&hook.off_main_thread, !is_main_thread, __ATOMIC_SEQ_CST);

    if (__atomic_load_n(&hook.stall, __ATOMIC_SEQ_CST)) {
        __atomic_store_n(&hook.entered, 1, __ATOMIC_SEQ_CST);
        while (!__atomic_load_n(&hook.go, __ATOMIC_SEQ_CST)) {
            usleep(1000);
        }
    }

    __atomic_add_fetch(&hook.fired, 1, __ATOMIC_SEQ_CST);
} /* purge_hook */

#define WAIT_FOR(cond) \
        do { \
            int _n = 0; \
            while (!(cond)) { \
                usleep(1000); \
                if (++_n > 10000) { \
                    fprintf(stderr, "TIMEOUT %s:%d: %s\n", __FILE__, __LINE__, #cond); \
                    abort(); \
                } \
            } \
        } while (0)

static void
hook_reset(void)
{
    memset(&hook, 0, sizeof(hook));
} /* hook_reset */

static void
wait_done(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_done */

static void
mount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_cb */

static void
lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* lookup_cb */

static void
openfh_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* openfh_cb */

static void
openat_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre,
    struct chimera_vfs_attrs       *dir_post,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, oh->fh, oh->fh_len);
        ctx->fh_len = oh->fh_len;
    }
    ctx->done = 1;
} /* openat_cb */

static struct chimera_vfs_open_handle *
create_file(
    struct test_ctx *ctx,
    const char      *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root, ctx->dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* create_file */

static struct chimera_vfs_open_handle *
open_by_name(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const char                    *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));

    chimera_vfs_open_at(ctx->vfs_thread, cred, ctx->dir, name, strlen(name),
                        0, &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* open_by_name */

static enum chimera_vfs_error
open_fh(
    struct test_ctx                 *ctx,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    uint32_t                         fh_len,
    struct chimera_vfs_open_handle **r_handle)
{
    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    *r_handle = ctx->handle;
    return ctx->status;
} /* open_fh */

static struct chimera_vfs_unlinked_info
info_of(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len)
{
    struct chimera_vfs_unlinked_info info;

    chimera_vfs_unlinked_test_info(ctx->vfs, fh, fh_len, &info);
    return info;
} /* info_of */

static struct vfs_open_cache_shard *
shard_of(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len)
{
    struct vfs_open_cache *cache = ctx->vfs->vfs_open_file_cache;

    return &cache->shards[chimera_vfs_hash(fh, fh_len) & cache->shard_mask];
} /* shard_of */

/* Let the close sweep take every idle handle of the file on its next pass. */
static void
mark_for_close(
    struct test_ctx *ctx,
    const uint8_t   *fh)
{
    chimera_vfs_open_cache_mark_for_close_by_mount(ctx->vfs->vfs_open_file_cache, fh);
} /* mark_for_close */

static int
cache_has(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len)
{
    return chimera_vfs_open_cache_exists(ctx->vfs->vfs_open_file_cache, fh, fh_len,
                                         chimera_vfs_hash(fh, fh_len));
} /* cache_has */

/* A pending open, as the open-by-handle path leaves one while its backend
 * open is in flight: the cache entry exists, nothing has been dispatched. */
static struct chimera_vfs_open_handle *g_pending;

static void
pending_cb(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    g_pending = handle;
} /* pending_cb */

static struct chimera_vfs_open_handle *
open_pending(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    struct chimera_vfs_request *req = calloc(1, sizeof(*req));

    req->cred = cred;
    chimera_vfs_open_cache_acquire(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache,
                                   chimera_vfs_get_module(ctx->vfs_thread, fh, fh_len),
                                   req, fh, fh_len, chimera_vfs_hash(fh, fh_len),
                                   UINT64_MAX, CHIMERA_VFS_OPEN_INFERRED, 0, pending_cb);
    free(req);
    CHECK(g_pending && (g_pending->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING));
    return g_pending;
} /* open_pending */

/* Finish a pending open as if it succeeded without a backend open, so a later
 * close has nothing to send to the backend. */
static void
complete_pending(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h)
{
    chimera_vfs_open_cache_populate(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache, h, 0);
    h->flags |= CHIMERA_VFS_OPEN_HANDLE_NO_BACKEND_OPEN;
} /* complete_pending */

static void
close_out(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len)
{
    mark_for_close(ctx, fh);
    WAIT_FOR(!cache_has(ctx, fh, fh_len));
} /* close_out */

/* ---- cases ---- */

static void
test_inert_without_record(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    /* Tracking off: no record can be made and nothing reports a reference. */
    h      = create_file(ctx, "inert");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);

    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_EINVAL);
    CHECK(!chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    CHECK(!info_of(ctx, fh, fh_len).exists);
    CHECK(!(h->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED));

    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);

    CHECK(hook.fired == 0);
    CHECK(chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_TRACKED) == 0);
    TEST_PASS("without tracking or a record the cache behaves as before");

    chimera_vfs_ref_tracking_enable(ctx->vfs);

    /* Tracking on but the file never unlinked: still nothing. */
    h      = create_file(ctx, "inert2");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(!chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);
    CHECK(hook.fired == 0);
    TEST_PASS("tracking on, a file with no record is untouched");
} /* test_inert_without_record */

static void
test_nothing_to_track(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    h      = create_file(ctx, "none");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);

    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_ENOENT);
    CHECK(!info_of(ctx, fh, fh_len).exists);
    CHECK(!chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    TEST_PASS("a file with no handle in the cache gets no record");
} /* test_nothing_to_track */

static void
test_counts_per_file(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle  *h1, *h2, *h3;
    struct chimera_vfs_unlinked_info i;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;

    hook_reset();

    h1     = create_file(ctx, "counts");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(open_fh(ctx, &ctx->other, fh, fh_len, &h2) == CHIMERA_VFS_OK);
    CHECK(h1 != h2);
    chimera_vfs_dup_handle(ctx->vfs_thread, h1);

    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    CHECK((h1->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED) && (h2->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED));
    i = info_of(ctx, fh, fh_len);
    CHECK(i.exists && i.state == CHIMERA_VFS_UNLINKED_TRACKED);
    CHECK(i.handles == 2);
    CHECK(i.strong == 3);
    CHECK(chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    TEST_PASS("counts are per file: two handles, three references");

    /* A third handle, opened after the record exists, joins it. */
    {
        struct chimera_vfs_cred third;

        chimera_vfs_cred_init_unix(&third, 2000, 2000, 0, NULL);
        CHECK(open_fh(ctx, &third, fh, fh_len, &h3) == CHIMERA_VFS_OK);
        CHECK(h3->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);
        i = info_of(ctx, fh, fh_len);
        CHECK(i.handles == 3 && i.strong == 4);
        chimera_vfs_release(ctx->vfs_thread, h3);
    }
    TEST_PASS("a handle opened after the record exists is counted");

    chimera_vfs_release(ctx->vfs_thread, h1);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.strong == 2);
    chimera_vfs_release(ctx->vfs_thread, h1);
    chimera_vfs_release(ctx->vfs_thread, h2);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.strong == 0 && i.handles == 3);
    CHECK(!chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));

    /* Idle handles still count as handles, and an open revives one. */
    CHECK(open_fh(ctx, &ctx->root, fh, fh_len, &h1) == CHIMERA_VFS_OK);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.strong == 1 && i.handles == 3);
    CHECK(chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    chimera_vfs_release(ctx->vfs_thread, h1);
    TEST_PASS("idle handles stay counted and an open revives them");

    CHECK(hook.fired == 0);
    mark_for_close(ctx, fh);
    WAIT_FOR(hook.fired == 1);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    CHECK(hook.off_main_thread && !hook.carrierless);
    CHECK(chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_TRACKED) == 1);
    WAIT_FOR(chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_PURGED) == 1);
    usleep(50000);
    CHECK(hook.fired == 1);
    TEST_PASS("the sweep routes the idle handles to the close thread; the last one purges, once");
} /* test_counts_per_file */

static void
test_detached_last_reference(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle  *h;
    struct chimera_vfs_unlinked_info i;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;

    hook_reset();

    /* Detached after the record exists. */
    h      = create_file(ctx, "det1");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    chimera_vfs_open_cache_evict(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache, fh, fh_len);
    CHECK(h->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.handles == 1 && i.strong == 1);
    usleep(30000);
    CHECK(hook.fired == 0);
    chimera_vfs_release(ctx->vfs_thread, h);
    WAIT_FOR(hook.fired == 1);
    CHECK(hook.off_main_thread && !hook.carrierless);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a detached handle holding the last reference is purged from the close thread");

    /* Detached before the record exists: it must still be found. */
    hook_reset();
    h      = create_file(ctx, "det2");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    chimera_vfs_open_cache_evict(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache, fh, fh_len);
    CHECK(h->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    CHECK(h->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.handles == 1 && i.strong == 1);
    CHECK(chimera_vfs_ref_unlinked_referenced(ctx->vfs, fh, fh_len));
    chimera_vfs_release(ctx->vfs_thread, h);
    WAIT_FOR(hook.fired == 1);
    CHECK(hook.off_main_thread);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a handle detached before the record was made is counted");
} /* test_detached_last_reference */

static void
test_pending(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle  *h1, *p;
    struct chimera_vfs_unlinked_info i;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;

    /* A pending open is counted when the record is made. */
    hook_reset();
    h1     = create_file(ctx, "pend1");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    p = open_pending(ctx, &ctx->other, fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    CHECK(p->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.handles == 2 && i.strong == 2);
    complete_pending(ctx, p);
    chimera_vfs_release(ctx->vfs_thread, p);
    chimera_vfs_release(ctx->vfs_thread, h1);
    mark_for_close(ctx, fh);
    WAIT_FOR(hook.fired == 1);
    CHECK(!hook.carrierless);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a pending open is counted and the purge waits for it");

    /* A failed pending open that is not the last handle only leaves the count. */
    hook_reset();
    h1     = create_file(ctx, "pend2");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    p = open_pending(ctx, &ctx->other, fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    chimera_vfs_open_cache_release(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache, p, CHIMERA_VFS_EIO);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.exists && i.handles == 1 && i.strong == 1);
    usleep(30000);
    CHECK(hook.fired == 0);
    chimera_vfs_release(ctx->vfs_thread, h1);
    mark_for_close(ctx, fh);
    WAIT_FOR(hook.fired == 1);
    CHECK(!hook.carrierless);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a failed pending open that is not the last handle leaves the record standing");

    /* A failed pending open that is the last handle: purge without a carrier. */
    hook_reset();
    h1     = create_file(ctx, "pend3");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    chimera_vfs_release(ctx->vfs_thread, h1);
    close_out(ctx, fh, fh_len);
    p = open_pending(ctx, &ctx->root, fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.handles == 1 && i.strong == 1);
    chimera_vfs_open_cache_release(ctx->vfs_thread, ctx->vfs->vfs_open_file_cache, p, CHIMERA_VFS_EIO);
    WAIT_FOR(hook.fired == 1);
    CHECK(hook.carrierless && hook.off_main_thread);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a failed pending open as the last handle purges without a carrier");
} /* test_pending */

static void
test_estale_while_purging(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h, *h2;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    hook_reset();
    h      = create_file(ctx, "stale");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);

    __atomic_store_n(&hook.stall, 1, __ATOMIC_SEQ_CST);
    chimera_vfs_release(ctx->vfs_thread, h);
    mark_for_close(ctx, fh);
    WAIT_FOR(__atomic_load_n(&hook.entered, __ATOMIC_SEQ_CST));

    CHECK(info_of(ctx, fh, fh_len).state == CHIMERA_VFS_UNLINKED_PURGING);
    CHECK(open_fh(ctx, &ctx->root, fh, fh_len, &h2) == CHIMERA_VFS_ESTALE);
    CHECK(open_fh(ctx, &ctx->other, fh, fh_len, &h2) == CHIMERA_VFS_ESTALE);
    TEST_PASS("an open by handle is refused with ESTALE while the purge runs");

    __atomic_store_n(&hook.go, 1, __ATOMIC_SEQ_CST);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    CHECK(open_fh(ctx, &ctx->root, fh, fh_len, &h2) == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, h2);
    close_out(ctx, fh, fh_len);
    TEST_PASS("the refusal ends with the record");
} /* test_estale_while_purging */

static void
test_insert_and_eviction_sites(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle  *h1, *h2, *h3;
    struct chimera_vfs_unlinked_info i;
    struct vfs_open_cache_shard     *shard;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;

    /* insert: replacing an in-use handle detaches it, replacing an idle one
     * routes it; both stay in the count until the close thread has them. */
    hook_reset();
    h1     = create_file(ctx, "ins");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);

    h2 = open_by_name(ctx, &ctx->root, "ins");
    CHECK(h2 != h1 && (h1->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED));
    CHECK(h2->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.handles == 2 && i.strong == 2);

    chimera_vfs_release(ctx->vfs_thread, h1);
    WAIT_FOR(info_of(ctx, fh, fh_len).handles == 1);
    CHECK(hook.fired == 0);

    chimera_vfs_release(ctx->vfs_thread, h2);
    h3 = open_by_name(ctx, &ctx->root, "ins");
    CHECK(h3 != h2);
    WAIT_FOR(info_of(ctx, fh, fh_len).handles == 1);
    CHECK(hook.fired == 0);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.strong == 1);
    TEST_PASS("insert detaches an in-use handle and routes an idle one");

    /* insert and acquire evicting another handle to make room.  The shard's
     * limit is forced down so that the new handle (a different credential, so
     * a different key) must push out the idle one. */
    chimera_vfs_release(ctx->vfs_thread, h3);
    shard                 = shard_of(ctx, fh, fh_len);
    shard->max_open_files = 0;
    h1                    = open_by_name(ctx, &ctx->other, "ins");
    WAIT_FOR(info_of(ctx, fh, fh_len).handles == 1);
    i = info_of(ctx, fh, fh_len);
    CHECK(i.strong == 1);
    CHECK(hook.fired == 0);
    chimera_vfs_release(ctx->vfs_thread, h1);
    TEST_PASS("insert evicting an idle handle routes it");

    {
        struct chimera_vfs_cred third;

        chimera_vfs_cred_init_unix(&third, 3000, 3000, 0, NULL);
        CHECK(open_fh(ctx, &third, fh, fh_len, &h2) == CHIMERA_VFS_OK);
        WAIT_FOR(info_of(ctx, fh, fh_len).handles == 1);
        i = info_of(ctx, fh, fh_len);
        CHECK(i.strong == 1);
        CHECK(hook.fired == 0);
        chimera_vfs_release(ctx->vfs_thread, h2);
    }
    shard->max_open_files = 4;
    TEST_PASS("acquire evicting an idle handle routes it");

    mark_for_close(ctx, fh);
    WAIT_FOR(hook.fired == 1);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
} /* test_insert_and_eviction_sites */

static void
test_umount(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *dir, *h;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE], dir_fh[CHIMERA_VFS_FH_SIZE], fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len, dir_fh_len, fh_len;
    struct chimera_vfs_attrs        sattr;

    hook_reset();

    chimera_vfs_mkfs(ctx->vfs_thread, NULL, "memfs", "fs1", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_mount(ctx->vfs_thread, NULL, "/m1", "memfs", "fs1", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx->vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root, root_fh, root_fh_len, "m1", 2,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0, lookup_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    dir_fh_len = ctx->fh_len;
    memcpy(dir_fh, ctx->fh, dir_fh_len);
    CHECK(open_fh(ctx, &ctx->root, dir_fh, dir_fh_len, &dir) == CHIMERA_VFS_OK);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root, dir, "u", 1, CHIMERA_VFS_OPEN_CREATE,
                        &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    h      = ctx->handle;
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);

    CHECK(chimera_vfs_unlinked_track(ctx->vfs, fh, fh_len) == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, h);
    chimera_vfs_release(ctx->vfs_thread, dir);

    /* The handle is idle and the sweep is slow, so umount takes it out of the
     * cache itself.  It must not finish before the file's close has run. */
    chimera_vfs_umount(ctx->vfs_thread, NULL, "/m1", mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    CHECK(hook.fired == 1);
    CHECK(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("umount waits for the close of an unlinked file's idle handle");

    chimera_vfs_rmfs(ctx->vfs_thread, NULL, "memfs", "fs1", mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
} /* test_umount */


/* ---- the unlink and the purge ----
 *
 * These cases go through remove_at and rename_at themselves.  The metadata
 * file system gets a second memfs mount as its data server (the way the pNFS
 * probes do), so a first write gives a file a backing file there and a layout
 * attribute that names it.  What a case checks is the VFS's own: statuses, the
 * backing file's presence in the data server's directory, the events, and the
 * order of the purge's steps as the close thread reports them. */

static uint8_t                         ds_fh[CHIMERA_VFS_FH_SIZE];
static uint32_t                        ds_fh_len;
static struct chimera_vfs_open_handle *ds_dir;

static struct {
    int log[16];
    int nlog;
    int stall_step;
    int entered;
    int go;
} stephook = { .stall_step = -1 };

static void
step_hook(
    enum chimera_vfs_unlinked_step step,
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    void                          *arg)
{
    int n = __atomic_fetch_add(&stephook.nlog, 1, __ATOMIC_SEQ_CST);

    if (n < 16) {
        stephook.log[n] = (int) step;
    }

    if (__atomic_load_n(&stephook.stall_step, __ATOMIC_SEQ_CST) == (int) step) {
        __atomic_store_n(&stephook.entered, 1, __ATOMIC_SEQ_CST);
        while (!__atomic_load_n(&stephook.go, __ATOMIC_SEQ_CST)) {
            usleep(1000);
        }
    }
} /* step_hook */

static void
step_reset(void)
{
    __atomic_store_n(&stephook.nlog, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&stephook.stall_step, -1, __ATOMIC_SEQ_CST);
    __atomic_store_n(&stephook.entered, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&stephook.go, 0, __ATOMIC_SEQ_CST);
} /* step_reset */

struct snap {
    uint64_t pins, lookups, tracked, purged, failed, relinked;
};

static struct snap
snap_now(struct test_ctx *ctx)
{
    struct snap s;

    chimera_vfs_unlinked_test_stats(&s.pins, &s.lookups);
    s.tracked  = chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_TRACKED);
    s.purged   = chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_PURGED);
    s.failed   = chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_PURGE_FAILED);
    s.relinked = chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_RELINKED);
    return s;
} /* snap_now */

static void
remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* remove_cb */

static void
rename_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *a,
    struct chimera_vfs_attrs *b,
    struct chimera_vfs_attrs *c,
    struct chimera_vfs_attrs *d,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* rename_cb */

static void
link_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *a,
    struct chimera_vfs_attrs *b,
    struct chimera_vfs_attrs *c,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* link_cb */

static enum chimera_vfs_error
unlink_name(
    struct test_ctx *ctx,
    const char      *name)
{
    chimera_vfs_remove_at(ctx->vfs_thread, &ctx->root, ctx->dir, name, strlen(name),
                          NULL, 0, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* unlink_name */

static enum chimera_vfs_error
rename_name(
    struct test_ctx *ctx,
    const char      *from,
    const char      *to)
{
    chimera_vfs_rename_at(ctx->vfs_thread, &ctx->root, ctx->dir_fh, ctx->dir_fh_len,
                          from, strlen(from), ctx->dir_fh, ctx->dir_fh_len, to, strlen(to),
                          NULL, 0, 0, 0, 0, NULL, NULL, rename_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* rename_name */

static enum chimera_vfs_error
link_name(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *name)
{
    chimera_vfs_link_at(ctx->vfs_thread, &ctx->root, fh, fh_len, ctx->dir_fh, ctx->dir_fh_len,
                        name, strlen(name), 0, 0, 0, 0, NULL, NULL, link_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* link_name */

static void
write_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* write_cb */

static void
mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre,
    struct chimera_vfs_attrs *dir_post,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* mkdir_cb */

static uint64_t g_ino;

static void
getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    g_ino       = 0;
    if (error_code == CHIMERA_VFS_OK) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM) {
            g_ino = attr->va_ino;
        }
        ctx->fh_len = (attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) ? attr->va_pnfs_len : 0;
    }
    ctx->done = 1;
} /* getattr_cb */

static void
setup_data_server(struct test_ctx *ctx)
{
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;
    int      idx;

    chimera_vfs_mkfs(ctx->vfs_thread, NULL, "memfs", "ds0", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_mount(ctx->vfs_thread, NULL, "/ds0", "memfs", "ds0", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx->vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root, root_fh, root_fh_len, "ds0", 3,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0, lookup_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    ds_fh_len = ctx->fh_len;
    memcpy(ds_fh, ctx->fh, ds_fh_len);

    chimera_vfs_pnfs_set_enabled(ctx->vfs, 1);
    idx = chimera_vfs_pnfs_add_device(ctx->vfs, "tcp", "127.0.0.1.8.1", NULL, "/ds0", 3, 0);
    CHECK(idx >= 0);
    chimera_vfs_pnfs_set_device_root(ctx->vfs, idx, ds_fh, ds_fh_len);
    chimera_vfs_pnfs_get_device(ctx->vfs, idx)->backing_local = 1;

    CHECK(open_fh(ctx, &ctx->root, ds_fh, ds_fh_len, &ds_dir) == CHIMERA_VFS_OK);
} /* setup_data_server */

struct pfile {
    struct chimera_vfs_open_handle *h;   /* NULL once released */
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    char                            backing[CHIMERA_VFS_PNFS_BACKING_NAME_MAX];
};

/* A regular file with data on the data server: the first write places it. */
static void
make_pfile(
    struct test_ctx *ctx,
    const char      *name,
    struct pfile    *f)
{
    struct evpl_iovec iov;

    f->h      = create_file(ctx, name);
    f->fh_len = ctx->fh_len;
    memcpy(f->fh, ctx->fh, f->fh_len);

    CHECK(evpl_iovec_alloc(ctx->evpl, 8, 0, 1, 0, &iov) == 1);
    memcpy(iov.data, "pnfsdata", 8);
    chimera_vfs_write(ctx->vfs_thread, &ctx->root, f->h, 0, 8, 1, 0, 0, &iov, 1, write_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    evpl_iovec_release(ctx->evpl, &iov);

    chimera_vfs_getattr(ctx->vfs_thread, &ctx->root, f->h,
                        CHIMERA_VFS_ATTR_INUM | CHIMERA_VFS_ATTR_PNFS_LAYOUT, getattr_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    CHECK(g_ino != 0);
    CHECK(ctx->fh_len > 0);
    ctx->fh_len = f->fh_len;
    chimera_vfs_pnfs_backing_name(f->backing, f->fh, g_ino);
} /* make_pfile */

static int
backing_exists(
    struct test_ctx *ctx,
    const char      *name)
{
    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root, ds_fh, ds_fh_len, name, strlen(name),
                       CHIMERA_VFS_ATTR_FH, 0, lookup_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK || ctx->status == CHIMERA_VFS_ENOENT);
    return ctx->status == CHIMERA_VFS_OK;
} /* backing_exists */

static void
drop_pfile(
    struct test_ctx *ctx,
    struct pfile    *f)
{
    if (f->h) {
        chimera_vfs_release(ctx->vfs_thread, f->h);
        f->h = NULL;
    }
} /* drop_pfile */

/* The file is closed and out of the cache, as it is for a client that has
 * long since closed it. */
static void
idle_pfile(
    struct test_ctx *ctx,
    struct pfile    *f)
{
    drop_pfile(ctx, f);
    close_out(ctx, f->fh, f->fh_len);
} /* idle_pfile */

static void
remove_backing_by_hand(
    struct test_ctx *ctx,
    const char      *name)
{
    chimera_vfs_remove_at(ctx->vfs_thread, &ctx->root, ds_dir, name, strlen(name),
                          NULL, 0, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
} /* remove_backing_by_hand */

#define WAIT_BACKING_GONE(ctx, name) WAIT_FOR(!backing_exists(ctx, name))

static void
test_unlink_inert_without_tracking(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h;
    struct snap                     a, b;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    /* Run before anything turns tracking on. */
    h      = create_file(ctx, "plain");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);

    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "plain") == CHIMERA_VFS_OK);
    b = snap_now(ctx);
    CHECK(b.pins == a.pins && b.lookups == a.lookups);
    CHECK(b.tracked == a.tracked && !info_of(ctx, fh, fh_len).exists);
    CHECK(!cache_has(ctx, fh, fh_len));
    TEST_PASS("tracking off: an unlink takes no pin, does no lookup and makes no record");
} /* test_unlink_inert_without_tracking */

static void
test_unlink_no_handle(struct test_ctx *ctx)
{
    struct pfile f;
    struct snap  a, b;

    step_reset();
    make_pfile(ctx, "nohandle", &f);
    idle_pfile(ctx, &f);
    CHECK(backing_exists(ctx, f.backing));

    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "nohandle") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    b = snap_now(ctx);
    CHECK(b.tracked == a.tracked + 1);
    CHECK(b.failed == a.failed && b.relinked == a.relinked);
    CHECK(b.pins == a.pins + 1);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    TEST_PASS("an unlink with no other handle deletes the backing file at once");

    /* The order the close thread takes. */
    WAIT_FOR(stephook.nlog == 3);
    CHECK(stephook.log[0] == CHIMERA_VFS_UNLINKED_STEP_RECHECK);
    CHECK(stephook.log[1] == CHIMERA_VFS_UNLINKED_STEP_BACKING_DELETE);
    CHECK(stephook.log[2] == CHIMERA_VFS_UNLINKED_STEP_BACKEND_CLOSE);
    TEST_PASS("the purge rechecks, deletes the backing file, then closes, in that order");
} /* test_unlink_no_handle */

static void
test_unlink_while_open(struct test_ctx *ctx)
{
    struct pfile                     f;
    struct snap                      a;
    struct chimera_vfs_unlinked_info i;

    make_pfile(ctx, "held", &f);
    a = snap_now(ctx);

    CHECK(unlink_name(ctx, "held") == CHIMERA_VFS_OK);
    i = info_of(ctx, f.fh, f.fh_len);
    CHECK(i.exists && i.state == CHIMERA_VFS_UNLINKED_TRACKED);
    CHECK(i.strong == 1);
    usleep(150000);
    CHECK(backing_exists(ctx, f.backing));
    CHECK(snap_now(ctx).purged == a.purged);
    TEST_PASS("an unlink while a handle is open keeps the backing file");

    drop_pfile(ctx, &f);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    TEST_PASS("the backing file goes at the last close, not before");
} /* test_unlink_while_open */

static void
test_relink_keeps_data(struct test_ctx *ctx)
{
    struct pfile f;
    struct snap  a;

    make_pfile(ctx, "relink", &f);
    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "relink") == CHIMERA_VFS_OK);
    CHECK(info_of(ctx, f.fh, f.fh_len).exists);

    CHECK(link_name(ctx, f.fh, f.fh_len, "relinked") == CHIMERA_VFS_OK);

    drop_pfile(ctx, &f);
    mark_for_close(ctx, f.fh);
    WAIT_FOR(snap_now(ctx).relinked == a.relinked + 1);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    usleep(50000);
    CHECK(backing_exists(ctx, f.backing));
    CHECK(snap_now(ctx).purged == a.purged);
    TEST_PASS("a link made after the unlink keeps the backing file and counts as relinked");

    CHECK(unlink_name(ctx, "relinked") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, f.backing);
    TEST_PASS("unlinking the new link then removes it");
} /* test_relink_keeps_data */

static void
test_unlink_one_of_two_links(struct test_ctx *ctx)
{
    struct pfile f;
    struct snap  a;

    make_pfile(ctx, "two_a", &f);
    CHECK(link_name(ctx, f.fh, f.fh_len, "two_b") == CHIMERA_VFS_OK);
    idle_pfile(ctx, &f);

    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "two_a") == CHIMERA_VFS_OK);
    usleep(100000);
    CHECK(snap_now(ctx).tracked == a.tracked);
    CHECK(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("unlinking one of two links makes no record and keeps the backing file");

    CHECK(unlink_name(ctx, "two_b") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, f.backing);
    TEST_PASS("unlinking the last link deletes it");
} /* test_unlink_one_of_two_links */

static void
test_rename_over_last_link(struct test_ctx *ctx)
{
    struct pfile victim, mover;
    struct snap  a;

    make_pfile(ctx, "victim", &victim);
    make_pfile(ctx, "mover", &mover);
    idle_pfile(ctx, &victim);
    idle_pfile(ctx, &mover);

    a = snap_now(ctx);
    CHECK(rename_name(ctx, "mover", "victim") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, victim.backing);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    CHECK(snap_now(ctx).tracked == a.tracked + 1);
    CHECK(backing_exists(ctx, mover.backing));
    TEST_PASS("a rename over the last link of a file deletes that file's backing file only");

    CHECK(unlink_name(ctx, "victim") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, mover.backing);
} /* test_rename_over_last_link */

static void
test_rename_replaces_nothing(struct test_ctx *ctx)
{
    struct pfile f, other;
    struct snap  a;

    make_pfile(ctx, "same_a", &f);
    CHECK(link_name(ctx, f.fh, f.fh_len, "same_b") == CHIMERA_VFS_OK);
    idle_pfile(ctx, &f);

    /* Onto another link of the same file. */
    a = snap_now(ctx);
    (void) rename_name(ctx, "same_a", "same_b");
    usleep(100000);
    CHECK(snap_now(ctx).tracked == a.tracked);
    CHECK(snap_now(ctx).pins == a.pins);
    CHECK(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("a rename onto another link of the same file takes no pin and makes no record");

    /* Onto itself. */
    a = snap_now(ctx);
    (void) rename_name(ctx, "same_b", "same_b");
    usleep(100000);
    CHECK(snap_now(ctx).tracked == a.tracked);
    CHECK(snap_now(ctx).pins == a.pins);
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("a rename onto itself takes no pin and makes no record");

    /* Over one of two links of another file: that file keeps a link. */
    make_pfile(ctx, "other_a", &other);
    CHECK(link_name(ctx, other.fh, other.fh_len, "other_b") == CHIMERA_VFS_OK);
    idle_pfile(ctx, &other);
    a = snap_now(ctx);
    CHECK(rename_name(ctx, "same_b", "other_b") == CHIMERA_VFS_OK);
    usleep(100000);
    CHECK(snap_now(ctx).tracked == a.tracked);
    CHECK(backing_exists(ctx, other.backing));
    TEST_PASS("a rename over a file that keeps another link makes no record");

    (void) unlink_name(ctx, "same_a");
    (void) unlink_name(ctx, "other_b");
    (void) unlink_name(ctx, "other_a");
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_BACKING_GONE(ctx, other.backing);
} /* test_rename_replaces_nothing */

static void
test_correct_backing_name(struct test_ctx *ctx)
{
    struct pfile a, b;

    make_pfile(ctx, "name_a", &a);
    make_pfile(ctx, "name_b", &b);
    CHECK(strcmp(a.backing, b.backing) != 0);
    idle_pfile(ctx, &a);
    idle_pfile(ctx, &b);
    CHECK(backing_exists(ctx, a.backing) && backing_exists(ctx, b.backing));

    CHECK(unlink_name(ctx, "name_a") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, a.backing);
    usleep(50000);
    CHECK(backing_exists(ctx, b.backing));
    TEST_PASS("the backing file is deleted under its own mount-id and file-id name, and no other");

    CHECK(unlink_name(ctx, "name_b") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, b.backing);
} /* test_correct_backing_name */

static void
test_backing_already_gone(struct test_ctx *ctx)
{
    struct pfile f;
    struct snap  a;

    make_pfile(ctx, "gone", &f);
    idle_pfile(ctx, &f);
    remove_backing_by_hand(ctx, f.backing);

    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "gone") == CHIMERA_VFS_OK);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    CHECK(snap_now(ctx).failed == a.failed);
    TEST_PASS("a backing file that is already gone counts as purged, not as a failure");
} /* test_backing_already_gone */

static void
test_backing_delete_fails(struct test_ctx *ctx)
{
    struct pfile           f;
    struct snap            a;
    struct chimera_vfs_ds *ds = chimera_vfs_pnfs_get_device(ctx->vfs, 0);
    uint32_t               saved;

    make_pfile(ctx, "fails", &f);
    idle_pfile(ctx, &f);

    /* A data server whose root is not available cannot be asked to delete. */
    saved           = ds->root_fh_len;
    ds->root_fh_len = 0;
    a               = snap_now(ctx);
    CHECK(unlink_name(ctx, "fails") == CHIMERA_VFS_OK);
    WAIT_FOR(snap_now(ctx).failed == a.failed + 1);
    CHECK(snap_now(ctx).purged == a.purged);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    ds->root_fh_len = saved;
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("a failed backing delete is counted as purge_failed and the record still goes");

    remove_backing_by_hand(ctx, f.backing);
} /* test_backing_delete_fails */

static void
test_purge_order_observed(struct test_ctx *ctx)
{
    struct pfile                     f;
    struct chimera_vfs_unlinked_info i;

    /* Stalled before the recheck: nothing has been deleted yet. */
    step_reset();
    __atomic_store_n(&stephook.stall_step, CHIMERA_VFS_UNLINKED_STEP_RECHECK, __ATOMIC_SEQ_CST);
    make_pfile(ctx, "order1", &f);
    idle_pfile(ctx, &f);
    CHECK(unlink_name(ctx, "order1") == CHIMERA_VFS_OK);
    WAIT_FOR(__atomic_load_n(&stephook.entered, __ATOMIC_SEQ_CST));
    i = info_of(ctx, f.fh, f.fh_len);
    CHECK(i.exists && i.state == CHIMERA_VFS_UNLINKED_PURGING);
    CHECK(backing_exists(ctx, f.backing));
    __atomic_store_n(&stephook.go, 1, __ATOMIC_SEQ_CST);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    TEST_PASS("the backing file is still there when the recheck starts");

    /* Stalled before the backend close: the backing file is already gone and
     * the record still stands. */
    step_reset();
    __atomic_store_n(&stephook.stall_step, CHIMERA_VFS_UNLINKED_STEP_BACKEND_CLOSE, __ATOMIC_SEQ_CST);
    make_pfile(ctx, "order2", &f);
    idle_pfile(ctx, &f);
    CHECK(unlink_name(ctx, "order2") == CHIMERA_VFS_OK);
    WAIT_FOR(__atomic_load_n(&stephook.entered, __ATOMIC_SEQ_CST));
    CHECK(!backing_exists(ctx, f.backing));
    i = info_of(ctx, f.fh, f.fh_len);
    CHECK(i.exists && i.state == CHIMERA_VFS_UNLINKED_PURGING);
    __atomic_store_n(&stephook.go, 1, __ATOMIC_SEQ_CST);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(stephook.log[0] == CHIMERA_VFS_UNLINKED_STEP_RECHECK);
    CHECK(stephook.log[1] == CHIMERA_VFS_UNLINKED_STEP_BACKING_DELETE);
    CHECK(stephook.log[2] == CHIMERA_VFS_UNLINKED_STEP_BACKEND_CLOSE);
    TEST_PASS("the backing file is gone before the backend close starts");
    step_reset();
} /* test_purge_order_observed */

static void
test_open_racing_unlink(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct chimera_vfs_open_handle *p, *h2;
    struct snap                     a;

    /* An open by handle that is still being opened when the unlink lands keeps
     * the data until it is closed. */
    make_pfile(ctx, "race", &f);
    p = open_pending(ctx, &ctx->other, f.fh, f.fh_len);
    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "race") == CHIMERA_VFS_OK);
    CHECK(p->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);
    drop_pfile(ctx, &f);
    mark_for_close(ctx, f.fh);
    usleep(150000);
    CHECK(backing_exists(ctx, f.backing));
    CHECK(info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(snap_now(ctx).purged == a.purged);
    TEST_PASS("an open in flight when the unlink lands keeps the data");

    complete_pending(ctx, p);
    chimera_vfs_release(ctx->vfs_thread, p);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);

    /* An open that arrives while the purge is stalled is refused, and what it
     * cannot do afterwards is bring the file back. */
    step_reset();
    __atomic_store_n(&stephook.stall_step, CHIMERA_VFS_UNLINKED_STEP_BACKING_DELETE, __ATOMIC_SEQ_CST);
    make_pfile(ctx, "race2", &f);
    idle_pfile(ctx, &f);
    CHECK(unlink_name(ctx, "race2") == CHIMERA_VFS_OK);
    WAIT_FOR(__atomic_load_n(&stephook.entered, __ATOMIC_SEQ_CST));
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_ESTALE);
    __atomic_store_n(&stephook.go, 1, __ATOMIC_SEQ_CST);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) != CHIMERA_VFS_OK);
    step_reset();
    TEST_PASS("an open racing the purge sees ESTALE and the file is never resurrected");
} /* test_open_racing_unlink */

static void
test_not_regular_or_not_placed(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h;
    struct snap                     a, b;
    struct chimera_vfs_attrs        sattr;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    /* A directory is not pinned. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &ctx->root, ctx->dir, "subdir", 6, &sattr,
                         CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "subdir") == CHIMERA_VFS_OK);
    b = snap_now(ctx);
    CHECK(b.pins == a.pins && b.tracked == a.tracked);
    TEST_PASS("removing a directory takes no pin and makes no record");

    /* A regular file that was never placed on a data server is pinned for the
     * link count and left alone: no record, and it is closed as it always was. */
    h      = create_file(ctx, "unplaced");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);
    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "unplaced") == CHIMERA_VFS_OK);
    b = snap_now(ctx);
    CHECK(b.tracked == a.tracked && b.pins == a.pins + 1);
    WAIT_FOR(!cache_has(ctx, fh, fh_len));
    CHECK(!info_of(ctx, fh, fh_len).exists);
    TEST_PASS("a file with no data on a data server leaves no record");
} /* test_not_regular_or_not_placed */


/* ---- recallable references ----
 *
 * A holder registers a recallable reference on an open handle.  Once the file
 * is unlinked and no real open is left, the VFS calls the holder's callback
 * once, on the close thread, outside its locks.  The callback below records
 * where and why it ran, and can be told to call back into the VFS the way a
 * real holder would (look at the file, release its own reference, open
 * another file) so that a lock held across the call shows up as a hang. */

#define HOLDER_REFS 4

struct holder {
    struct chimera_vfs_recallable   r[HOLDER_REFS];
    int                             nr;
    struct test_ctx                *ctx;
    int                             calls;
    int                             reason;
    int                             off_main;
    int                             release_in_cb;
    int                             probe_in_cb;
    int                             probed;
    uint8_t                         other_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        other_fh_len;
    int                             open_started;
    int                             open_done;
    int                             open_status;
    struct chimera_vfs_open_handle *open_handle;
};

static void
holder_open_done(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct holder *hd = private_data;

    hd->open_status = error_code;
    hd->open_handle = oh;
    __atomic_store_n(&hd->open_done, 1, __ATOMIC_SEQ_CST);
} /* holder_open_done */

static void
recall_cb(
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    enum chimera_vfs_recall_reason reason,
    void                          *arg)
{
    struct holder *hd = arg;

    hd->reason   = (int) reason;
    hd->off_main = !is_main_thread;

    if (hd->probe_in_cb) {
        struct chimera_vfs_unlinked_info i;
        struct chimera_vfs_thread       *ct = hd->ctx->vfs->close_thread.vfs_thread;

        /* Each of these takes the open cache shard lock of the file. */
        (void) chimera_vfs_ref_unlinked_referenced(hd->ctx->vfs, fh, fh_len);
        chimera_vfs_unlinked_test_info(hd->ctx->vfs, fh, fh_len, &i);

        if (hd->other_fh_len) {
            hd->open_started = 1;
            chimera_vfs_open_fh(ct, &hd->ctx->root, hd->other_fh, hd->other_fh_len,
                                CHIMERA_VFS_OPEN_INFERRED, holder_open_done, hd);
        }

        if (hd->release_in_cb) {
            chimera_vfs_ref_release_recallable(ct, &hd->r[0]);
        }

        hd->probed = 1;
    }

    __atomic_add_fetch(&hd->calls, 1, __ATOMIC_SEQ_CST);
} /* recall_cb */

#define HCALLS(hd) __atomic_load_n(&(hd)->calls, __ATOMIC_SEQ_CST)

static void
holder_init(
    struct holder   *hd,
    struct test_ctx *ctx)
{
    memset(hd, 0, sizeof(*hd));
    hd->ctx = ctx;
} /* holder_init */

/* Turn a reference the caller holds on `h` into the holder's next recallable one. */
static void
hold_ref(
    struct test_ctx                *ctx,
    struct holder                  *hd,
    struct chimera_vfs_open_handle *h)
{
    struct chimera_vfs_recallable *r = &hd->r[hd->nr++];

    CHECK(hd->nr <= HOLDER_REFS);
    memset(r, 0, sizeof(*r));
    r->fn  = recall_cb;
    r->arg = hd;
    CHECK(chimera_vfs_ref_make_recallable(ctx->vfs_thread, h, r) == CHIMERA_VFS_OK);
} /* hold_ref */

static void
give_ref(
    struct test_ctx *ctx,
    struct holder   *hd,
    int              i)
{
    chimera_vfs_ref_release_recallable(ctx->vfs_thread, &hd->r[i]);
} /* give_ref */

static uint64_t
recalled_events(struct test_ctx *ctx)
{
    return chimera_vfs_unlinked_test_event(ctx->vfs, CHIMERA_VFS_UNLINKED_EV_RECALLED);
} /* recalled_events */

static void
test_recall_at_last_close(struct test_ctx *ctx)
{
    struct pfile                     f;
    struct holder                    hd;
    struct chimera_vfs_open_handle  *h2;
    struct chimera_vfs_unlinked_info i;
    struct snap                      a;
    uint64_t                         rec0 = recalled_events(ctx);

    holder_init(&hd, ctx);
    hd.probe_in_cb = 1;
    make_pfile(ctx, "rc1", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    a = snap_now(ctx);

    CHECK(unlink_name(ctx, "rc1") == CHIMERA_VFS_OK);
    i = info_of(ctx, f.fh, f.fh_len);
    CHECK(i.exists && i.strong == 1 && i.recallable == 1);
    usleep(150000);
    CHECK(HCALLS(&hd) == 0);
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("no recall at the unlink while a real open remains");

    drop_pfile(ctx, &f);
    WAIT_FOR(HCALLS(&hd) == 1);
    CHECK(hd.off_main && hd.reason == CHIMERA_VFS_RECALL_UNLINKED);
    CHECK(hd.probed);
    usleep(100000);
    CHECK(HCALLS(&hd) == 1);
    CHECK(backing_exists(ctx, f.backing));
    CHECK(info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(recalled_events(ctx) == rec0 + 1);
    TEST_PASS("the last real close recalls once, on the close thread, and keeps the data");

    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    CHECK(HCALLS(&hd) == 1);
    TEST_PASS("releasing the last recallable reference purges the file");
} /* test_recall_at_last_close */

static void
test_recall_not_while_open(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   hd;
    struct chimera_vfs_open_handle *h2;

    holder_init(&hd, ctx);
    make_pfile(ctx, "rc2", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    chimera_vfs_dup_handle(ctx->vfs_thread, f.h);

    CHECK(unlink_name(ctx, "rc2") == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, f.h);
    usleep(150000);
    CHECK(HCALLS(&hd) == 0);
    TEST_PASS("no recall while one of two real opens remains");

    drop_pfile(ctx, &f);
    WAIT_FOR(HCALLS(&hd) == 1);

    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
} /* test_recall_not_while_open */

static void
test_recall_dedup(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   a, b;
    struct chimera_vfs_open_handle *h2, *h3;
    struct chimera_vfs_cred         third;
    uint64_t                        rec0 = recalled_events(ctx);

    holder_init(&a, ctx);
    holder_init(&b, ctx);
    chimera_vfs_cred_init_unix(&third, 2000, 2000, 0, NULL);

    make_pfile(ctx, "rc3", &f);

    /* Holder a: three references on one handle.  Holder b: one on another. */
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    chimera_vfs_dup_handle(ctx->vfs_thread, h2);
    chimera_vfs_dup_handle(ctx->vfs_thread, h2);
    hold_ref(ctx, &a, h2);
    hold_ref(ctx, &a, h2);
    hold_ref(ctx, &a, h2);
    CHECK(open_fh(ctx, &third, f.fh, f.fh_len, &h3) == CHIMERA_VFS_OK);
    hold_ref(ctx, &b, h3);

    CHECK(unlink_name(ctx, "rc3") == CHIMERA_VFS_OK);
    CHECK(info_of(ctx, f.fh, f.fh_len).recallable == 4);
    drop_pfile(ctx, &f);

    WAIT_FOR(HCALLS(&a) == 1 && HCALLS(&b) == 1);
    usleep(150000);
    CHECK(HCALLS(&a) == 1 && HCALLS(&b) == 1);
    CHECK(recalled_events(ctx) == rec0 + 2);
    TEST_PASS("three references of one holder and one of another make two calls");

    give_ref(ctx, &a, 0);
    give_ref(ctx, &a, 1);
    give_ref(ctx, &a, 2);
    mark_for_close(ctx, f.fh);
    usleep(200000);
    CHECK(backing_exists(ctx, f.backing));
    give_ref(ctx, &b, 0);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(HCALLS(&a) == 1 && HCALLS(&b) == 1);
    TEST_PASS("the data waits for the last recallable reference of any holder");
} /* test_recall_dedup */

static void
test_recall_reentry(struct test_ctx *ctx)
{
    struct pfile                    f, g;
    struct holder                   hd;
    struct chimera_vfs_open_handle *h2;

    holder_init(&hd, ctx);
    hd.probe_in_cb   = 1;
    hd.release_in_cb = 1;

    make_pfile(ctx, "rc4g", &g);
    hd.other_fh_len = g.fh_len;
    memcpy(hd.other_fh, g.fh, g.fh_len);

    make_pfile(ctx, "rc4", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    CHECK(unlink_name(ctx, "rc4") == CHIMERA_VFS_OK);
    drop_pfile(ctx, &f);

    WAIT_FOR(HCALLS(&hd) == 1);
    WAIT_FOR(__atomic_load_n(&hd.open_done, __ATOMIC_SEQ_CST));
    CHECK(hd.open_status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, hd.open_handle);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    TEST_PASS("a callback that looks at the file, opens another and releases its own reference does not deadlock");

    drop_pfile(ctx, &g);
    close_out(ctx, g.fh, g.fh_len);
} /* test_recall_reentry */

static void
test_recall_linked_never(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   hd;
    struct chimera_vfs_open_handle *h2;

    holder_init(&hd, ctx);
    make_pfile(ctx, "rc5", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    drop_pfile(ctx, &f);
    mark_for_close(ctx, f.fh);
    usleep(250000);
    CHECK(HCALLS(&hd) == 0);
    CHECK(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(cache_has(ctx, f.fh, f.fh_len));
    TEST_PASS("a recallable reference on a linked file is never recalled");

    give_ref(ctx, &hd, 0);
    close_out(ctx, f.fh, f.fh_len);
    CHECK(HCALLS(&hd) == 0);
} /* test_recall_linked_never */

static void
test_recall_eagain_and_at_once(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   hd;
    struct chimera_vfs_recallable   r;
    struct chimera_vfs_open_handle *h2;

    /* The caller's reference is the file's last strong one. */
    holder_init(&hd, ctx);
    make_pfile(ctx, "rc6", &f);
    CHECK(unlink_name(ctx, "rc6") == CHIMERA_VFS_OK);
    CHECK(info_of(ctx, f.fh, f.fh_len).exists);

    memset(&r, 0, sizeof(r));
    r.fn  = recall_cb;
    r.arg = &hd;
    CHECK(chimera_vfs_ref_make_recallable(ctx->vfs_thread, f.h, &r) == CHIMERA_VFS_EAGAIN);
    CHECK(f.h->recallcnt == 0);
    CHECK(info_of(ctx, f.fh, f.fh_len).recallable == 0);
    usleep(100000);
    CHECK(HCALLS(&hd) == 0);
    drop_pfile(ctx, &f);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    CHECK(HCALLS(&hd) == 0);
    TEST_PASS("registering on an unlinked file with no other real open is refused with EAGAIN");

    /* A file that only has recallable references when it is unlinked: the
     * recall goes out as soon as the unlink has made its record. */
    holder_init(&hd, ctx);
    make_pfile(ctx, "rc7", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    drop_pfile(ctx, &f);
    CHECK(unlink_name(ctx, "rc7") == CHIMERA_VFS_OK);
    WAIT_FOR(HCALLS(&hd) == 1);
    CHECK(hd.off_main && hd.reason == CHIMERA_VFS_RECALL_UNLINKED);
    CHECK(backing_exists(ctx, f.backing));
    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    TEST_PASS("a file with only recallable references is recalled as soon as it is unlinked");
} /* test_recall_eagain_and_at_once */

static void
test_recall_any_file(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h, *h2;
    struct holder                   hd;
    struct snap                     a, b;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;

    /* No layout attribute, so nothing places this file on a data server; a
     * recallable reference alone makes it worth tracking. */
    holder_init(&hd, ctx);
    h      = create_file(ctx, "rc8");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    CHECK(open_fh(ctx, &ctx->other, fh, fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);

    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "rc8") == CHIMERA_VFS_OK);
    b = snap_now(ctx);
    CHECK(b.tracked == a.tracked + 1);
    CHECK(info_of(ctx, fh, fh_len).exists);
    usleep(100000);
    CHECK(HCALLS(&hd) == 0);

    chimera_vfs_release(ctx->vfs_thread, h);
    WAIT_FOR(HCALLS(&hd) == 1);
    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, fh);
    WAIT_FOR(!info_of(ctx, fh, fh_len).exists);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    TEST_PASS("a file with recallable references is tracked and recalled without a layout attribute");

    /* The same file without one: no record, as before. */
    h      = create_file(ctx, "rc9");
    fh_len = ctx->fh_len;
    memcpy(fh, ctx->fh, fh_len);
    a = snap_now(ctx);
    CHECK(unlink_name(ctx, "rc9") == CHIMERA_VFS_OK);
    b = snap_now(ctx);
    CHECK(b.tracked == a.tracked);
    CHECK(!info_of(ctx, fh, fh_len).exists);
    chimera_vfs_release(ctx->vfs_thread, h);
    close_out(ctx, fh, fh_len);
    TEST_PASS("without a layout attribute or a recallable reference the unlink stays untracked");
} /* test_recall_any_file */

static void
test_recall_relink(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   hd;
    struct chimera_vfs_open_handle *h2;
    struct snap                     a;

    holder_init(&hd, ctx);
    make_pfile(ctx, "rc10", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    CHECK(unlink_name(ctx, "rc10") == CHIMERA_VFS_OK);
    a = snap_now(ctx);
    drop_pfile(ctx, &f);
    WAIT_FOR(HCALLS(&hd) == 1);

    CHECK(link_name(ctx, f.fh, f.fh_len, "rc10-again") == CHIMERA_VFS_OK);

    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, f.fh);
    WAIT_FOR(snap_now(ctx).relinked == a.relinked + 1);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
    CHECK(backing_exists(ctx, f.backing));
    TEST_PASS("a file linked again while it is being recalled keeps its data");
    CHECK(unlink_name(ctx, "rc10-again") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, f.backing);
} /* test_recall_relink */

static void
test_recall_refire(struct test_ctx *ctx)
{
    struct pfile                    f;
    struct holder                   a, b;
    struct chimera_vfs_open_handle *h2, *hc, *hd2;

    holder_init(&a, ctx);
    holder_init(&b, ctx);
    make_pfile(ctx, "rc11", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &a, h2);
    CHECK(unlink_name(ctx, "rc11") == CHIMERA_VFS_OK);
    drop_pfile(ctx, &f);
    WAIT_FOR(HCALLS(&a) == 1);

    /* A real open after the recall started: allowed, and it recalls nothing
     * more when it closes. */
    CHECK(open_fh(ctx, &ctx->root, f.fh, f.fh_len, &hc) == CHIMERA_VFS_OK);
    CHECK(info_of(ctx, f.fh, f.fh_len).strong == 1);
    chimera_vfs_release(ctx->vfs_thread, hc);
    usleep(150000);
    CHECK(HCALLS(&a) == 1);
    TEST_PASS("a real open during the recall does not recall again when it closes");

    /* A reference registered after that fires at the next strong-to-zero. */
    CHECK(open_fh(ctx, &ctx->root, f.fh, f.fh_len, &hc) == CHIMERA_VFS_OK);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &hd2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &b, hd2);
    usleep(100000);
    CHECK(HCALLS(&b) == 0);
    chimera_vfs_release(ctx->vfs_thread, hc);
    WAIT_FOR(HCALLS(&b) == 1);
    usleep(100000);
    CHECK(HCALLS(&a) == 1 && HCALLS(&b) == 1);
    TEST_PASS("a reference registered after a recall is recalled at the next last close, once");

    give_ref(ctx, &a, 0);
    give_ref(ctx, &b, 0);
    mark_for_close(ctx, f.fh);
    WAIT_BACKING_GONE(ctx, f.backing);
    WAIT_FOR(!info_of(ctx, f.fh, f.fh_len).exists);
} /* test_recall_refire */

static void
test_recall_unreleased_blocks_nothing(struct test_ctx *ctx)
{
    struct pfile                    x, y;
    struct holder                   hd;
    struct chimera_vfs_open_handle *h2;
    struct snap                     a;

    holder_init(&hd, ctx);
    make_pfile(ctx, "rc12x", &x);
    CHECK(open_fh(ctx, &ctx->other, x.fh, x.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, &hd, h2);
    CHECK(unlink_name(ctx, "rc12x") == CHIMERA_VFS_OK);
    drop_pfile(ctx, &x);
    WAIT_FOR(HCALLS(&hd) == 1);

    /* The holder has been asked and never answers. */
    a = snap_now(ctx);
    make_pfile(ctx, "rc12y", &y);
    idle_pfile(ctx, &y);
    CHECK(unlink_name(ctx, "rc12y") == CHIMERA_VFS_OK);
    WAIT_BACKING_GONE(ctx, y.backing);
    WAIT_FOR(snap_now(ctx).purged == a.purged + 1);
    CHECK(backing_exists(ctx, x.backing));
    TEST_PASS("a holder that never releases does not block the purge of an unrelated file");

    give_ref(ctx, &hd, 0);
    mark_for_close(ctx, x.fh);
    WAIT_BACKING_GONE(ctx, x.backing);
} /* test_recall_unreleased_blocks_nothing */

static void
test_recall_umount(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *dir, *h;
    struct holder                   hd;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE], dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len, dir_fh_len;
    struct chimera_vfs_attrs        sattr;
    uint64_t                        rec0 = recalled_events(ctx);

    holder_init(&hd, ctx);
    hd.probe_in_cb   = 1;
    hd.release_in_cb = 1;

    chimera_vfs_mkfs(ctx->vfs_thread, NULL, "memfs", "fs2", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_mount(ctx->vfs_thread, NULL, "/m2", "memfs", "fs2", NULL, mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx->vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root, root_fh, root_fh_len, "m2", 2,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0, lookup_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    dir_fh_len = ctx->fh_len;
    memcpy(dir_fh, ctx->fh, dir_fh_len);
    CHECK(open_fh(ctx, &ctx->root, dir_fh, dir_fh_len, &dir) == CHIMERA_VFS_OK);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root, dir, "l", 1, CHIMERA_VFS_OPEN_CREATE,
                        &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    h = ctx->handle;

    /* A layout-like reference on a linked file, which only umount can recall. */
    hold_ref(ctx, &hd, h);
    chimera_vfs_release(ctx->vfs_thread, dir);

    chimera_vfs_umount(ctx->vfs_thread, NULL, "/m2", mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
    CHECK(HCALLS(&hd) == 1);
    CHECK(hd.off_main && hd.reason == CHIMERA_VFS_RECALL_UMOUNT);
    CHECK(recalled_events(ctx) == rec0 + 1);
    TEST_PASS("umount recalls a recallable reference and completes once it is released");

    chimera_vfs_rmfs(ctx->vfs_thread, NULL, "memfs", "fs2", mount_cb, ctx);
    wait_done(ctx);
    CHECK(ctx->status == CHIMERA_VFS_OK);
} /* test_recall_umount */

/* Run last: the instance is destroyed with a recallable reference still held. */
static void
test_recall_shutdown_leak(struct test_ctx *ctx)
{
    struct pfile                    f;
    /* Outlives the case: the instance is destroyed with its reference held. */
    static struct holder            hd_storage;
    struct holder                  *hdp = &hd_storage;
    struct chimera_vfs_open_handle *h2;

    holder_init(hdp, ctx);
    make_pfile(ctx, "rc13", &f);
    CHECK(open_fh(ctx, &ctx->other, f.fh, f.fh_len, &h2) == CHIMERA_VFS_OK);
    hold_ref(ctx, hdp, h2);
    CHECK(unlink_name(ctx, "rc13") == CHIMERA_VFS_OK);
    drop_pfile(ctx, &f);
    WAIT_FOR(HCALLS(hdp) == 1);
} /* test_recall_shutdown_leak */

/* A case name as the first argument runs only that case of the unlink and
 * purge group; with none, every case runs. */
static const char *only_case;

#define RUN(name) \
        do { \
            if (!only_case || strcmp(only_case, #name) == 0) { \
                test_ ## name(&ctx); \
            } \
        } while (0)

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;
    uint64_t                      leaked_before;
    int                           leak_case;

    is_main_thread = 1;
    only_case      = argc > 1 ? argv[1] : NULL;
    leak_case      = !only_case || strcmp(only_case, "recall_shutdown_leak") == 0;

    /* Idle handles stay until a test marks them; the sweep itself runs often. */
    setenv("CHIMERA_CLOSE_SWEEP_MIN_AGE_MS", "1500", 1);
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 1);

    chimera_log_init();

    mbt_watchdog_arm(120);
    mbt_watchdog_at("unlinked ref", 0, "open cache bookkeeping");

    chimera_vfs_cred_init_unix(&ctx.root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&ctx.other, 1000, 1000, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    CHECK(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    CHECK(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    CHECK(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    CHECK(ctx.vfs_thread != NULL);

    chimera_vfs_unlinked_test_set_purge_hook(purge_hook, NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL, mount_cb, &ctx);
    wait_done(&ctx);
    CHECK(ctx.status == CHIMERA_VFS_OK);
    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "fs0", NULL, mount_cb, &ctx);
    wait_done(&ctx);
    CHECK(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx.vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &ctx.root, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0, lookup_cb, &ctx);
    wait_done(&ctx);
    CHECK(ctx.status == CHIMERA_VFS_OK);
    ctx.dir_fh_len = ctx.fh_len;
    memcpy(ctx.dir_fh, ctx.fh, ctx.fh_len);
    CHECK(open_fh(&ctx, &ctx.root, ctx.dir_fh, ctx.dir_fh_len, &ctx.dir) == CHIMERA_VFS_OK);

    chimera_vfs_unlinked_test_set_purge_hook(purge_hook, NULL);

    /* Marking a handle for close stamps it with time zero, which only counts as
     * old enough once the process has been up for the sweep's minimum age. */
    usleep(1700000);

    chimera_vfs_unlinked_test_set_step_hook(step_hook, NULL);

    test_unlink_inert_without_tracking(&ctx);

    test_inert_without_record(&ctx);
    test_nothing_to_track(&ctx);
    test_counts_per_file(&ctx);
    test_detached_last_reference(&ctx);
    test_pending(&ctx);
    test_estale_while_purging(&ctx);
    test_insert_and_eviction_sites(&ctx);

    setup_data_server(&ctx);
    RUN(unlink_no_handle);
    RUN(unlink_while_open);
    RUN(relink_keeps_data);
    RUN(unlink_one_of_two_links);
    RUN(rename_over_last_link);
    RUN(rename_replaces_nothing);
    RUN(correct_backing_name);
    RUN(backing_already_gone);
    RUN(backing_delete_fails);
    RUN(purge_order_observed);
    RUN(open_racing_unlink);
    RUN(not_regular_or_not_placed);
    RUN(recall_at_last_close);
    RUN(recall_not_while_open);
    RUN(recall_dedup);
    RUN(recall_reentry);
    RUN(recall_linked_never);
    RUN(recall_eagain_and_at_once);
    RUN(recall_any_file);
    RUN(recall_relink);
    RUN(recall_refire);
    RUN(recall_unreleased_blocks_nothing);
    RUN(recall_umount);
    RUN(recall_shutdown_leak);

    chimera_vfs_release(ctx.vfs_thread, ds_dir);
    chimera_vfs_release(ctx.vfs_thread, ctx.dir);

    test_umount(&ctx);

    mbt_watchdog_disarm();

    /* The leak case leaves a recallable reference on a file of /test, so /test
     * cannot be unmounted; the instance is destroyed with it still mounted and
     * the reference still held. */
    leaked_before = chimera_vfs_unlinked_test_total_event(CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN);

    if (!leak_case) {
        chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
        wait_done(&ctx);
        CHECK(ctx.status == CHIMERA_VFS_OK);
    }

    chimera_vfs_unlinked_test_set_step_hook(NULL, NULL);
    chimera_vfs_thread_destroy(ctx.vfs_thread);

    mbt_watchdog_arm(60);
    mbt_watchdog_at("unlinked ref", 1, "shutdown with a recallable reference held");
    chimera_vfs_destroy(ctx.vfs);
    mbt_watchdog_disarm();

    if (leak_case) {
        CHECK(chimera_vfs_unlinked_test_total_event(CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN) ==
              leaked_before + 1);
        TEST_PASS("shutdown with a recallable reference held neither hangs nor leaks, and counts the backing file");
    }

    chimera_vfs_unlinked_test_set_purge_hook(NULL, NULL);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "vfs_unlinked_ref_test: ALL PASS\n");
    return 0;
} /* main */
