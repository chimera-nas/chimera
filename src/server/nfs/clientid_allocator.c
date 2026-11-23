// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The common wrapper every client ID allocator backend sits under.
 *
 * It owns the configuration schema, the closed status set, the max_pending
 * cap, and result delivery.  Delivery uses ONE evpl deferral per home event
 * loop (plus, for backends that complete on other threads, one doorbell per
 * loop), never one per request: libevpl keeps armed deferrals and timers in
 * fixed 256-entry arrays without a bounds check, so per-request deferrals
 * could overflow them during a re-registration storm.  Completed requests
 * wait on the loop's own list, which has no capacity limit.
 */

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "common/platform.h"
#include "common/export.h"
#include "clientid_allocator_internal.h"
#include "clientid_format.h"
#include "nfs_internal.h"

#define CLIENTID_SCHEMA_VERSION           1
#define CLIENTID_DEFAULT_MAX_PENDING      4096
#define CLIENTID_MAX_MAX_PENDING          ((uint64_t) 1 << 20)
#define CLIENTID_DEFAULT_RETRY_LIMIT      5
#define CLIENTID_MAX_RETRY_LIMIT          100
#define CLIENTID_DEFAULT_RETRY_BACKOFF_MS 10
#define CLIENTID_MAX_RETRY_BACKOFF_MS     10000

struct clientid_loop;

struct clientid_req {
    struct clientid_req       *next;
    struct clientid_allocator *alloc;
    struct clientid_loop      *loop;
    clientid_alloc_cb          done;
    void                      *arg;
    int                        status;
    uint64_t                   clientid;
    uint64_t                   submit_ns;
};

/* Per home event loop delivery state. */
struct clientid_loop {
    struct clientid_loop        *next;
    struct clientid_allocator   *alloc;
    struct evpl                 *evpl;
    evpl_thread_id_t             thread;
    evpl_mutex_t                 lock;    /* protects head/tail/queued/pending */
    struct clientid_req         *head;
    struct clientid_req         *tail;
    uint64_t                     queued;  /* completed, not yet delivered */
    uint64_t                     pending; /* accepted on this loop, not yet delivered */
    bool                         retired; /* detached; never matched again */
    struct evpl_deferral         deferral;
    bool                         has_doorbell;
    struct evpl_doorbell         doorbell;
    struct evpl_doorbell_sender *sender;
};

struct clientid_allocator {
    const struct clientid_backend_ops *ops;
    void                              *state;
    uint8_t                            type;
    evpl_mutex_t                       lock; /* pending, shutting_down, loops, stats */
    uint64_t                           pending;
    uint64_t                           max_pending;
    unsigned                           retry_limit;
    unsigned                           retry_backoff_ms;
    bool                               shutting_down;
    struct clientid_loop              *loops;
    struct clientid_allocator_stats    stats;
    uint64_t                           last_transient_log_ns;
#ifdef CHIMERA_CLIENTID_TEST_BACKEND
    /* Test hook: fail the next fail_signals doorbell signals with fail_err. */
    unsigned                           fail_signals;
    int                                fail_err;
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
};

static const struct clientid_backend_ops *const clientid_backends[] = {
    &clientid_local_ops,
#ifdef CHIMERA_CLIENTID_TEST_BACKEND
    &clientid_test_ops,
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
};

static uint64_t
clientid_now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* clientid_now_ns */

static bool
clientid_status_in_set(int status)
{
    switch (status) {
        case 0:
        case EAGAIN:
        case ENOMEM:
        case ESHUTDOWN:
        case ENOSPC:
        case EIO:
        case EINVAL:
            return true;
        default:
            return false;
    } /* switch */
} /* clientid_status_in_set */

/* ------------------------------------------------------------------ *
*  configuration                                                     *
* ------------------------------------------------------------------ */

static const struct clientid_backend_ops *
clientid_find_backend(const char *name)
{
    for (size_t i = 0; i < sizeof(clientid_backends) / sizeof(clientid_backends[0]); i++) {
        if (strcmp(clientid_backends[i]->name, name) == 0) {
            return clientid_backends[i];
        }
    }
    return NULL;
} /* clientid_find_backend */

static bool
clientid_backend_has_type(
    const struct clientid_backend_ops *ops,
    uint8_t                            type)
{
    for (int i = 0; i < ops->ntypes; i++) {
        if (ops->types[i] == type) {
            return true;
        }
    }
    return false;
} /* clientid_backend_has_type */

/* EINVAL if `obj` holds a key that is not in the NULL-ended `allowed` list. */
static int
clientid_check_keys(
    const json_t      *obj,
    const char *const *allowed,
    const char        *where)
{
    const char *key;
    json_t     *value;

    json_object_foreach((json_t *) obj, key, value)
    {
        bool known = false;

        (void) value;
        for (int i = 0; allowed[i]; i++) {
            if (strcmp(key, allowed[i]) == 0) {
                known = true;
                break;
            }
        }
        if (!known) {
            chimera_nfs_error("clientid allocator: unknown key \"%s\" in %s", key, where);
            return EINVAL;
        }
    }
    return 0;
} /* clientid_check_keys */

static int
clientid_get_uint(
    const json_t *obj,
    const char   *key,
    uint64_t      min,
    uint64_t      max,
    uint64_t      dflt,
    uint64_t     *out)
{
    json_t    *v = json_object_get(obj, key);
    json_int_t i;

    if (!v) {
        *out = dflt;
        return 0;
    }
    if (!json_is_integer(v)) {
        chimera_nfs_error("clientid allocator: \"%s\" must be an integer", key);
        return EINVAL;
    }
    i = json_integer_value(v);
    if (i < 0 || (uint64_t) i < min || (uint64_t) i > max) {
        chimera_nfs_error("clientid allocator: \"%s\" = %lld is out of range [%llu, %llu]",
                          key, (long long) i, (unsigned long long) min,
                          (unsigned long long) max);
        return EINVAL;
    }
    *out = (uint64_t) i;
    return 0;
} /* clientid_get_uint */

struct clientid_resolved {
    const struct clientid_backend_ops *ops;
    uint8_t                            type;
    const json_t                      *kwargs;
    uint64_t                           max_pending;
    uint64_t                           retry_limit;
    uint64_t                           retry_backoff_ms;
};

static int
clientid_resolve(
    const struct clientid_allocator_config *config,
    struct clientid_resolved               *r)
{
    static const char *const top_keys[] = {
        "schema", "backend", "type", "kwargs", "common", NULL
    };
    static const char *const common_keys[] = {
        "max_pending", "defer_retry_limit", "defer_retry_backoff_ms", NULL
    };
    const json_t            *root, *jschema, *jbackend, *jtype, *common;
    json_int_t               schema, type;
    int                      rc;

    if (!config || !config->json || !json_is_object(config->json)) {
        chimera_nfs_error("clientid allocator: configuration must be a JSON object");
        return EINVAL;
    }
    root = config->json;

    rc = clientid_check_keys(root, top_keys, "the allocator configuration");
    if (rc) {
        return rc;
    }

    jschema = json_object_get(root, "schema");
    if (!json_is_integer(jschema)) {
        chimera_nfs_error("clientid allocator: \"schema\" is required and must be an integer");
        return EINVAL;
    }
    schema = json_integer_value(jschema);
    if (schema != CLIENTID_SCHEMA_VERSION) {
        chimera_nfs_error("clientid allocator: schema %lld is not supported (this build: %d)",
                          (long long) schema, CLIENTID_SCHEMA_VERSION);
        return ENOTSUP;
    }

    jbackend = json_object_get(root, "backend");
    if (!json_is_string(jbackend)) {
        chimera_nfs_error("clientid allocator: \"backend\" is required and must be a string");
        return EINVAL;
    }

    jtype = json_object_get(root, "type");
    if (!json_is_integer(jtype)) {
        chimera_nfs_error("clientid allocator: \"type\" is required and must be an integer");
        return EINVAL;
    }
    type = json_integer_value(jtype);
    if (type < 0 || type > 15) {
        chimera_nfs_error("clientid allocator: type %lld is outside 0..15", (long long) type);
        return EINVAL;
    }
    if (type == CLIENTID_TYPE_RESERVED) {
        chimera_nfs_error("clientid allocator: type %lld is reserved", (long long) type);
        return EINVAL;
    }

    r->ops = clientid_find_backend(json_string_value(jbackend));
    if (!r->ops) {
        chimera_nfs_error("clientid allocator: backend \"%s\" is not available in this build",
                          json_string_value(jbackend));
        return ENOTSUP;
    }
    if (!clientid_backend_has_type(r->ops, (uint8_t) type)) {
        chimera_nfs_error("clientid allocator: backend \"%s\" does not implement type %lld",
                          r->ops->name, (long long) type);
        return ENOTSUP;
    }
    r->type = (uint8_t) type;

    r->kwargs = json_object_get(root, "kwargs");
    if (!json_is_object(r->kwargs)) {
        chimera_nfs_error("clientid allocator: \"kwargs\" is required and must be an object");
        return EINVAL;
    }

    common = json_object_get(root, "common");
    if (common && !json_is_object(common)) {
        chimera_nfs_error("clientid allocator: \"common\" must be an object");
        return EINVAL;
    }
    if (common) {
        rc = clientid_check_keys(common, common_keys, "\"common\"");
        if (rc) {
            return rc;
        }
    }

    rc = clientid_get_uint(common, "max_pending", 1, CLIENTID_MAX_MAX_PENDING,
                           CLIENTID_DEFAULT_MAX_PENDING, &r->max_pending);
    if (!rc) {
        rc = clientid_get_uint(common, "defer_retry_limit", 0, CLIENTID_MAX_RETRY_LIMIT,
                               CLIENTID_DEFAULT_RETRY_LIMIT, &r->retry_limit);
    }
    if (!rc) {
        rc = clientid_get_uint(common, "defer_retry_backoff_ms", 0,
                               CLIENTID_MAX_RETRY_BACKOFF_MS,
                               CLIENTID_DEFAULT_RETRY_BACKOFF_MS, &r->retry_backoff_ms);
    }
    if (rc) {
        return rc;
    }

    return r->ops->supported(r->type, r->kwargs);
} /* clientid_resolve */

SYMBOL_EXPORT int
clientid_allocator_supported(const struct clientid_allocator_config *config)
{
    struct clientid_resolved r;

    return clientid_resolve(config, &r);
} /* clientid_allocator_supported */

/* Log the configuration once, with the backend's secret kwargs redacted. */
static void
clientid_log_config(
    const struct clientid_allocator_config *config,
    const struct clientid_resolved         *r)
{
    json_t *copy = json_deep_copy(config->json);
    json_t *kwargs;
    char   *text;

    if (!copy) {
        return;
    }
    kwargs = json_object_get(copy, "kwargs");
    if (kwargs && r->ops->secret_keys) {
        for (int i = 0; r->ops->secret_keys[i]; i++) {
            if (json_object_get(kwargs, r->ops->secret_keys[i])) {
                json_object_set_new(kwargs, r->ops->secret_keys[i], json_string("***"));
            }
        }
    }
    text = json_dumps(copy, JSON_COMPACT | JSON_SORT_KEYS);
    if (text) {
        chimera_nfs_info("NFSv4 client ID allocator: %s", text);
        free(text);
    }
    json_decref(copy);
} /* clientid_log_config */

SYMBOL_EXPORT int
clientid_allocator_create(
    const struct clientid_allocator_config *config,
    struct clientid_allocator             **out)
{
    struct clientid_resolved   r;
    struct clientid_allocator *a;
    int                        rc;

    if (!out) {
        return EINVAL;
    }
    *out = NULL;

    rc = clientid_resolve(config, &r);
    if (rc) {
        return rc;
    }

    a = calloc(1, sizeof(*a));
    if (!a) {
        return ENOMEM;
    }

    a->ops              = r.ops;
    a->type             = r.type;
    a->max_pending      = r.max_pending;
    a->retry_limit      = (unsigned) r.retry_limit;
    a->retry_backoff_ms = (unsigned) r.retry_backoff_ms;
    evpl_mutex_init(&a->lock, NULL);

    rc = r.ops->create(r.type, r.kwargs, &a->state);
    if (rc) {
        evpl_mutex_destroy(&a->lock);
        free(a);
        return rc;
    }

    clientid_log_config(config, &r);

    *out = a;
    return 0;
} /* clientid_allocator_create */

/* ------------------------------------------------------------------ *
*  delivery                                                          *
* ------------------------------------------------------------------ */

static void
clientid_loop_drain(struct clientid_loop *loop)
{
    struct clientid_allocator *a = loop->alloc;
    struct clientid_req       *list, *r;

    evpl_mutex_lock(&loop->lock);
    list         = loop->head;
    loop->head   = NULL;
    loop->tail   = NULL;
    loop->queued = 0;
    evpl_mutex_unlock(&loop->lock);

    while (list) {
        r    = list;
        list = r->next;

        /* No allocator lock is held while the caller's callback runs. */
        r->done(r->arg, r->status, r->clientid);

        evpl_mutex_lock(&loop->lock);
        loop->pending--;
        evpl_mutex_unlock(&loop->lock);

        evpl_mutex_lock(&a->lock);
        a->pending--;
        if (r->status) {
            a->stats.failed++;
            a->stats.failed_by_status[clientid_allocator_status_index(r->status)]++;
        } else {
            a->stats.issued++;
        }
        evpl_mutex_unlock(&a->lock);

        free(r);
    }
} /* clientid_loop_drain */

static void
clientid_loop_deferral_cb(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    clientid_loop_drain(private_data);
} /* clientid_loop_deferral_cb */

static void
clientid_loop_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct clientid_loop *loop = (struct clientid_loop *) ((char *) doorbell -
                                                           offsetof(struct clientid_loop, doorbell));

    (void) evpl;
    clientid_loop_drain(loop);
} /* clientid_loop_doorbell_cb */

/* Find or create the delivery state for `home`.  Runs on home's thread
 * (allocate_async's contract), which is what evpl_add_doorbell requires. */
static struct clientid_loop *
clientid_get_loop(
    struct clientid_allocator *a,
    struct evpl               *home)
{
    struct clientid_loop *loop;

    evpl_mutex_lock(&a->lock);
    for (loop = a->loops; loop; loop = loop->next) {
        if (!loop->retired && loop->evpl == home) {
            evpl_mutex_unlock(&a->lock);
            return loop;
        }
    }
    evpl_mutex_unlock(&a->lock);

    loop = calloc(1, sizeof(*loop));
    if (!loop) {
        return NULL;
    }
    loop->alloc  = a;
    loop->evpl   = home;
    loop->thread = evpl_current_thread();
    evpl_mutex_init(&loop->lock, NULL);
    evpl_deferral_init(&loop->deferral, clientid_loop_deferral_cb, loop);

    if (a->ops->cross_thread) {
        evpl_add_doorbell(home, &loop->doorbell, clientid_loop_doorbell_cb);
        loop->sender = evpl_doorbell_sender(&loop->doorbell);
        if (!loop->sender) {
            evpl_remove_doorbell(home, &loop->doorbell);
            evpl_mutex_destroy(&loop->lock);
            free(loop);
            return NULL;
        }
        loop->has_doorbell = true;
    }

    evpl_mutex_lock(&a->lock);
    loop->next = a->loops;
    a->loops   = loop;
    evpl_mutex_unlock(&a->lock);

    return loop;
} /* clientid_get_loop */

/* What the abort message reports about a request.  Copied before the request
 * is queued: once it is on the home loop's list the loop may deliver and free
 * it at any moment. */
struct clientid_req_snapshot {
    int      status;
    uint64_t clientid;
    uint64_t submit_ns;
};

static void
clientid_abort_undeliverable(
    struct clientid_allocator          *a,
    struct clientid_loop               *loop,
    const struct clientid_req_snapshot *snap,
    unsigned                            attempts,
    int                                 err)
{
    char     idbuf[160];
    uint64_t depth;

    evpl_mutex_lock(&loop->lock);
    depth = loop->queued;
    evpl_mutex_unlock(&loop->lock);

    clientid_format(snap->clientid, idbuf, sizeof(idbuf));
    chimera_nfs_abort(
        "NFSv4 client ID allocator: could not hand a completed allocation back to "
        "its home event loop after %u attempts (last error %d).  type=%u backend=%s "
        "home_evpl=%p request_age_ms=%llu status=%d clientid=%s pending=%llu "
        "max_pending=%llu loop_queue_depth=%llu retry_limit=%u backoff_ms=%u.  "
        "The callback is owed exactly once and must not run inline; dropping it "
        "would leak the caller's registration forever.",
        attempts, err, a->type, a->ops->name, (void *) loop->evpl,
        (unsigned long long) ((clientid_now_ns() - snap->submit_ns) / 1000000ULL),
        snap->status, snap->status ? "none" : idbuf,
        (unsigned long long) a->pending, (unsigned long long) a->max_pending,
        (unsigned long long) depth, a->retry_limit, a->retry_backoff_ms);
} /* clientid_abort_undeliverable */

SYMBOL_EXPORT void
clientid_allocator_complete(
    struct clientid_req *r,
    int                  status,
    uint64_t             clientid)
{
    struct clientid_allocator *a    = r->alloc;
    struct clientid_loop      *loop = r->loop;

    if (!clientid_status_in_set(status)) {
        chimera_nfs_error("NFSv4 client ID allocator: backend %s returned status %d, "
                          "outside the closed set; reporting EIO", a->ops->name, status);
        evpl_mutex_lock(&a->lock);
        a->stats.unknown_status++;
        evpl_mutex_unlock(&a->lock);
        status = EIO;
    }
    struct clientid_req_snapshot snap;
    unsigned                     attempts = 0;
    bool                         same_thread;

    r->status   = status;
    r->clientid = status ? 0 : clientid;
    r->next     = NULL;

    snap.status    = r->status;
    snap.clientid  = r->clientid;
    snap.submit_ns = r->submit_ns;
    same_thread    = evpl_thread_equal(evpl_current_thread(), loop->thread);

    if (!same_thread && !loop->has_doorbell) {
        chimera_nfs_abort("NFSv4 client ID allocator: backend %s completed on a "
                          "foreign thread but did not declare cross_thread", a->ops->name);
    }

    evpl_mutex_lock(&loop->lock);
    if (loop->tail) {
        loop->tail->next = r;
    } else {
        loop->head = r;
    }
    loop->tail = r;
    loop->queued++;
    evpl_mutex_unlock(&loop->lock);

    /* From here on `r` belongs to the home loop, which may deliver and free it
     * at any moment (an earlier doorbell or deferral can drain the list), so
     * nothing below touches it.  `loop` stays valid: loops are freed only by
     * clientid_allocator_destroy, after the backend has been torn down. */

    if (same_thread) {
        /* Same thread: one deferral per loop, re-armed at most once per pass. */
        evpl_defer(loop->evpl, &loop->deferral);
        return;
    }

    for (;;) {
        int err;

#ifdef CHIMERA_CLIENTID_TEST_BACKEND
        err = 0;
        evpl_mutex_lock(&a->lock);
        if (a->fail_signals) {
            a->fail_signals--;
            err = a->fail_err;
        }
        evpl_mutex_unlock(&a->lock);
        if (!err) {
            err = evpl_doorbell_signal(loop->sender);
        }
#else  /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
        err = evpl_doorbell_signal(loop->sender);
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */

        if (err == 0) {
            return;
        }

        /* The doorbell is owed only while something is waiting on the list.
         * If the loop has already drained it (a doorbell rung for another
         * request delivered ours too), the result was handed back and there
         * is nothing to retry or abort for. */
        evpl_mutex_lock(&loop->lock);
        if (!loop->head) {
            evpl_mutex_unlock(&loop->lock);
            return;
        }
        evpl_mutex_unlock(&loop->lock);

        if (attempts >= a->retry_limit) {
            clientid_abort_undeliverable(a, loop, &snap, attempts + 1, err);
        }
        attempts++;
        /* We are on the backend's own thread, never an event loop, so a
         * short sleep is the backoff. */
        chimera_thread_sleep_us(a->retry_backoff_ms * 1000u);
    }
} /* clientid_allocator_complete */

SYMBOL_EXPORT int
clientid_allocator_allocate_async(
    struct clientid_allocator *a,
    struct evpl               *home,
    clientid_alloc_cb          done,
    void                      *arg)
{
    struct clientid_loop *loop;
    struct clientid_req  *r;
    uint64_t              now;

    if (!a) {
        return EINVAL;
    }
    if (!home || !done) {
        evpl_mutex_lock(&a->lock);
        a->stats.rejected[CLIENTID_STATUS_EINVAL]++;
        evpl_mutex_unlock(&a->lock);
        return EINVAL;
    }

    evpl_mutex_lock(&a->lock);
    if (a->shutting_down) {
        a->stats.rejected[CLIENTID_STATUS_ESHUTDOWN]++;
        evpl_mutex_unlock(&a->lock);
        return ESHUTDOWN;
    }
    if (a->pending >= a->max_pending) {
        a->stats.rejected[CLIENTID_STATUS_EAGAIN]++;
        now = clientid_now_ns();
        if (now - a->last_transient_log_ns > 1000000000ULL) {
            a->last_transient_log_ns = now;
            chimera_nfs_error("NFSv4 client ID allocator: %llu allocations pending "
                              "(max_pending); answering NFS4ERR_DELAY",
                              (unsigned long long) a->pending);
        }
        evpl_mutex_unlock(&a->lock);
        return EAGAIN;
    }
    a->pending++;
    evpl_mutex_unlock(&a->lock);

    r    = calloc(1, sizeof(*r));
    loop = r ? clientid_get_loop(a, home) : NULL;
    if (!r || !loop) {
        /* Queueing could not be set up: fail before acceptance, no callback. */
        free(r);
        evpl_mutex_lock(&a->lock);
        a->pending--;
        a->stats.rejected[r ? CLIENTID_STATUS_EAGAIN : CLIENTID_STATUS_ENOMEM]++;
        evpl_mutex_unlock(&a->lock);
        return r ? EAGAIN : ENOMEM;
    }

    /* A known loop answers only on its own thread (the contract's "home
     * loop"): a call from elsewhere would have its callback run on a thread
     * that does not own the caller's state. */
    if (!evpl_thread_equal(evpl_current_thread(), loop->thread)) {
        free(r);
        evpl_mutex_lock(&a->lock);
        a->pending--;
        a->stats.rejected[CLIENTID_STATUS_EINVAL]++;
        evpl_mutex_unlock(&a->lock);
        chimera_nfs_error("NFSv4 client ID allocator: allocate_async called off "
                          "its home loop's thread (caller bug); answering EINVAL");
        return EINVAL;
    }

    evpl_mutex_lock(&loop->lock);
    loop->pending++;
    evpl_mutex_unlock(&loop->lock);

    r->alloc     = a;
    r->loop      = loop;
    r->done      = done;
    r->arg       = arg;
    r->submit_ns = clientid_now_ns();

    a->ops->start(a->state, r);
    return 0;
} /* clientid_allocator_allocate_async */

SYMBOL_EXPORT void
clientid_allocator_shutdown(struct clientid_allocator *a)
{
    if (!a) {
        return;
    }
    evpl_mutex_lock(&a->lock);
    a->shutting_down = true;
    evpl_mutex_unlock(&a->lock);
} /* clientid_allocator_shutdown */

SYMBOL_EXPORT void
clientid_allocator_detach(
    struct clientid_allocator *a,
    struct evpl               *home)
{
    struct clientid_loop *loop;
    uint64_t              pending;

    if (!a || !home) {
        return;
    }

    evpl_mutex_lock(&a->lock);
    for (loop = a->loops; loop; loop = loop->next) {
        if (!loop->retired && loop->evpl == home) {
            break;
        }
    }
    evpl_mutex_unlock(&a->lock);
    if (!loop) {
        return;
    }

    /* Complete everything admitted on this loop.  Each one is owed a
     * callback here, and a cross-thread backend still holding one would
     * otherwise find the doorbell retired. */
    for (;;) {
        evpl_mutex_lock(&loop->lock);
        pending = loop->pending;
        evpl_mutex_unlock(&loop->lock);
        if (!pending) {
            break;
        }
        evpl_continue(home);
    }

    /* Nothing is owed to this loop any more, so no backend thread will
     * signal it (one that loses a race with the drain sees an empty list and
     * returns).  Retire the doorbell on its own thread. */
    if (loop->has_doorbell) {
        evpl_remove_doorbell(home, &loop->doorbell);
    }

    evpl_mutex_lock(&a->lock);
    loop->retired = true;
    evpl_mutex_unlock(&a->lock);
} /* clientid_allocator_detach */

SYMBOL_EXPORT void
clientid_allocator_destroy(struct clientid_allocator *a)
{
    struct clientid_loop *loop, *next;
    bool                  leak;

    if (!a) {
        return;
    }

    evpl_mutex_lock(&a->lock);
    a->shutting_down = true;
    leak             = a->pending != 0;
    evpl_mutex_unlock(&a->lock);

    if (leak) {
        /* Freeing the delivery state now would let an armed deferral or a
         * doorbell fire into freed memory.  Shutdown ordering was broken (the
         * home loops must be detached, which drains them, first); say so and
         * leak instead. */
        chimera_nfs_error("NFSv4 client ID allocator destroyed with %llu allocations "
                          "still pending; leaking their delivery state",
                          (unsigned long long) a->pending);
        return;
    }

    /* The backend first: once its threads are gone nothing can still be
     * inside clientid_allocator_complete() using a loop. */
    a->ops->destroy(a->state);

    for (loop = a->loops; loop; loop = next) {
        next = loop->next;
        if (loop->has_doorbell) {
            /* A detached loop retired its doorbell already.  Otherwise
             * evpl_remove_doorbell must run on the loop's thread; if we are
             * elsewhere the loop has already been destroyed (documented
             * ordering), which retired the doorbell. */
            if (!loop->retired &&
                evpl_thread_equal(evpl_current_thread(), loop->thread)) {
                evpl_remove_doorbell(loop->evpl, &loop->doorbell);
            }
            evpl_doorbell_sender_release(loop->sender);
        }
        evpl_mutex_destroy(&loop->lock);
        free(loop);
    }

    evpl_mutex_destroy(&a->lock);
    free(a);
} /* clientid_allocator_destroy */

SYMBOL_EXPORT void
clientid_allocator_get_stats(
    struct clientid_allocator       *a,
    struct clientid_allocator_stats *stats)
{
    evpl_mutex_lock(&a->lock);
    *stats         = a->stats;
    stats->pending = a->pending;
    evpl_mutex_unlock(&a->lock);

    stats->remaining = 0;
    if (a->ops == &clientid_local_ops) {
        stats->remaining = clientid_local_remaining(a->state);
    }
#ifdef CHIMERA_CLIENTID_TEST_BACKEND
    else if (a->ops == &clientid_test_ops) {
        stats->remaining = clientid_local_remaining(*(struct clientid_local **) a->state);
    }
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
} /* clientid_allocator_get_stats */

SYMBOL_EXPORT void
clientid_allocator_local_set_next_sequence(
    struct clientid_allocator *a,
    uint32_t                   sequence)
{
    if (a->ops == &clientid_local_ops) {
        clientid_local_set_next_sequence(a->state, sequence);
    }
#ifdef CHIMERA_CLIENTID_TEST_BACKEND
    else if (a->ops == &clientid_test_ops) {
        clientid_local_set_next_sequence(*(struct clientid_local **) a->state, sequence);
    }
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
} /* clientid_allocator_local_set_next_sequence */

#ifdef CHIMERA_CLIENTID_TEST_BACKEND
SYMBOL_EXPORT unsigned
clientid_allocator_test_fail_signals(
    struct clientid_allocator *a,
    unsigned                   count,
    int                        err)
{
    unsigned left;

    evpl_mutex_lock(&a->lock);
    left            = a->fail_signals;
    a->fail_signals = count;
    a->fail_err     = err;
    evpl_mutex_unlock(&a->lock);
    return left;
} /* clientid_allocator_test_fail_signals */
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
