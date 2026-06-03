// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: GPL-3.0-only

/*
 * elbencho external I/O backend plugin for the chimera native client.
 *
 * Implements the elbencho_plugin.h ABI on top of the chimera client async API,
 * so elbencho can drive the chimera VFS natively (with real queue depth via
 * --iodepth), analogous to the fio engine in src/fio/fio.c.
 *
 * This object is GPL-3.0-only (to combine with the GPL-3.0 elbencho host) and
 * links the LGPL libchimera_client. It is loaded at runtime via
 *   elbencho --backend-plugin <this.so> --backend-plugin-config <config.json> ...
 * where <config.json> has the same "modules"/"mounts" shape as the fio engine's
 * test configs.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <jansson.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "metrics/metrics.h"
#include "common/macros.h"

#include "elbencho_plugin.h"

/* ---- process-global state (one shared chimera_client for all workers) ---- */

static pthread_mutex_t            g_mutex        = PTHREAD_MUTEX_INITIALIZER;
static struct chimera_client     *g_client       = NULL;
static struct prometheus_metrics *g_metrics      = NULL;
static int                        g_refcount     = 0;
static int                        g_max_inflight = 0; /* process-wide peak concurrent async ops */

#define ELB_CHIMERA_RING_SIZE 8192   /* per-worker completion ring (>= max iodepth) */

/* A registered I/O buffer handed out by iobuf_alloc(): an evpl iovec whose data
 * pointer elbencho uses directly as an I/O buffer, enabling zero-copy read_into/
 * writerv (no bounce through a scratch buffer). Each holder is individually
 * allocated and never moved, because an evpl_iovec must not be struct-copied
 * (the Debug canary tracks its owner address). */
struct elb_regbuf {
    void              *base; /* == evpl_iovec_data(&iov), the pointer elbencho sees */
    struct evpl_iovec  iov;
    struct elb_regbuf *next;
};

/* per-worker context returned by worker_init() */
struct elb_chimera_ctx {
    struct evpl                  *evpl;
    struct chimera_client_thread *client;

    struct elb_regbuf            *regbufs; /* registered I/O buffers (iobuf_alloc) */

    /* async completion ring (slot indices + results), drained by aio_reap() */
    struct elbencho_io_completion ring[ELB_CHIMERA_RING_SIZE];
    int                           ring_head;
    int                           ring_tail;

    /* per-slot async bookkeeping; grown to cover the largest slot index seen */
    struct elb_chimera_slot      *slots;
    size_t                        num_slots;

    /* async concurrency tracking: ops submitted but not yet completed, and the peak
     * reached. A peak > 1 demonstrates that the plugin keeps multiple chimera ops in
     * flight concurrently (true async) rather than serializing them. */
    int                           inflight;
    int                           max_inflight;
};

/* per in-flight async op (one live op per slot at a time) */
struct elb_chimera_slot {
    struct elb_chimera_ctx *ctx;
    uint64_t                slot;     /* elbencho's ioVecIdx, echoed back in completion */
    void                   *user_buf; /* elbencho buffer to copy read data into (copy path) */
    struct evpl_iovec       iov;      /* scratch (copy path) or registered-buffer clone (zerocopy) */
    uint32_t                length;
    int                     is_write;
    int                     zerocopy; /* 1 if iov is a clone of a registered buffer (no memcpy) */
};

/* ---- helpers ---- */

static inline void
elb_ring_enqueue(
    struct elb_chimera_ctx *ctx,
    uint64_t                slot,
    int64_t                 result)
{
    ctx->ring[ctx->ring_head].slot   = slot;
    ctx->ring[ctx->ring_head].result = result;
    ctx->ring_head                   = (ctx->ring_head + 1) & (ELB_CHIMERA_RING_SIZE - 1);
} /* elb_ring_enqueue */

static inline int
elb_ring_count(struct elb_chimera_ctx *ctx)
{
    return (ctx->ring_head - ctx->ring_tail) & (ELB_CHIMERA_RING_SIZE - 1);
} /* elb_ring_count */

/* translate POSIX open(2) flags to chimera VFS open flags */
static unsigned int
elb_translate_open_flags(int posix_flags)
{
    unsigned int flags = 0;

    if (posix_flags & O_CREAT) {
        flags |= CHIMERA_VFS_OPEN_CREATE;
    }
    if (posix_flags & O_TRUNC) {
        flags |= CHIMERA_VFS_OPEN_TRUNCATE;
    }
    if (posix_flags & O_EXCL) {
        flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
    }
    if ((posix_flags & O_ACCMODE) == O_RDONLY) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }

    return flags;
} /* elb_translate_open_flags */

/* Find the registered I/O buffer whose data pointer is `buf`, or NULL if `buf`
 * was not handed out by iobuf_alloc(). */
static struct evpl_iovec *
elb_regbuf_lookup(
    struct elb_chimera_ctx *ctx,
    const void             *buf)
{
    struct elb_regbuf *rb;

    for (rb = ctx->regbufs; rb; rb = rb->next) {
        if (rb->base == buf) {
            return &rb->iov;
        }
    }

    return NULL;
} /* elb_regbuf_lookup */

/* ---- backend lifecycle ---- */

struct elb_mount_ctx {
    int status;
    int complete;
    int total;
};

static void
elb_mount_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct elb_mount_ctx *ctx = private_data;

    if (status) {
        ctx->status = status;
    }
    ctx->complete++;
} /* elb_mount_callback */

/*
 * Load modules + perform mounts from the JSON config file at config_json.
 * Mirrors the config handling in src/fio/fio.c.
 */
static int
elb_chimera_load_config(const char *config_path)
{
    json_t *config, *modules, *module, *mounts, *mount;
    json_t *module_name, *module_path, *mount_point, *config_obj;
    int     i;

    config = json_load_file(config_path, 0, NULL);
    if (!config) {
        fprintf(stderr, "elbencho-chimera: failed to load config file %s\n", config_path);
        return -EINVAL;
    }

    /* Build the client config (modules), then init the client. */
    struct chimera_client_config *client_config = chimera_client_config_init();

    modules = json_object_get(config, "modules");
    if (modules) {
        json_array_foreach(modules, i, module)
        {
            module_name = json_object_get(module, "module");
            module_path = json_object_get(module, "module_path");
            config_obj  = json_object_get(module, "config");

            if (!module_name || !module_path) {
                fprintf(stderr, "elbencho-chimera: invalid module config\n");
                json_decref(config);
                return -EINVAL;
            }

            char *config_str = NULL;
            if (config_obj && json_is_object(config_obj)) {
                config_str = json_dumps(config_obj, JSON_COMPACT);
            }

            chimera_client_config_add_module(client_config,
                                             json_string_value(module_name),
                                             json_string_value(module_path),
                                             config_str ? config_str : "");
            free(config_str);
        }
    }

    /* evpl: init once for the process with a buffer large enough for typical
     * block sizes (single-iovec read_into/writerv require bs <= buffer size). */
    {
        struct evpl_global_config *evpl_config = evpl_global_config_init();
        uint64_t                   buffer_size = 16 * 1024 * 1024;

        evpl_global_config_set_buffer_size(evpl_config, buffer_size);
        evpl_global_config_set_slab_size(evpl_config, 4 * buffer_size);
        evpl_init(evpl_config);
    }

    g_metrics = prometheus_metrics_create(NULL, NULL, 0);

    struct chimera_vfs_cred root_cred;
    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

    g_client = chimera_client_init(client_config, &root_cred, g_metrics);
    if (!g_client) {
        fprintf(stderr, "elbencho-chimera: chimera_client_init failed\n");
        json_decref(config);
        return -EINVAL;
    }

    /* perform mounts on a temporary client thread */
    mounts = json_object_get(config, "mounts");
    if (mounts) {
        struct evpl                  *evpl          = evpl_create(NULL);
        struct chimera_client_thread *client_thread = chimera_client_thread_init(evpl, g_client);
        struct elb_mount_ctx          mount_ctx     = { 0 };

        mount_ctx.total = json_array_size(mounts);

        json_array_foreach(mounts, i, mount)
        {
            module      = json_object_get(mount, "module");
            module_path = json_object_get(mount, "module_path");
            mount_point = json_object_get(mount, "mount_point");

            if (!module || !module_path || !mount_point) {
                fprintf(stderr, "elbencho-chimera: invalid mount config\n");
                chimera_client_thread_shutdown(evpl, client_thread);
                evpl_destroy(evpl);
                json_decref(config);
                return -EINVAL;
            }

            json_t     *mount_opts = json_object_get(mount, "options");
            const char *opts_str   = mount_opts ? json_string_value(mount_opts) : NULL;

            chimera_mount(client_thread,
                          json_string_value(mount_point),
                          json_string_value(module),
                          json_string_value(module_path),
                          opts_str,
                          elb_mount_callback,
                          &mount_ctx);
        }

        while (mount_ctx.complete < mount_ctx.total) {
            evpl_continue(evpl);
        }

        chimera_client_thread_shutdown(evpl, client_thread);
        evpl_destroy(evpl);

        if (mount_ctx.status != 0) {
            fprintf(stderr, "elbencho-chimera: mount failed (status %d)\n", mount_ctx.status);
            json_decref(config);
            return -EIO;
        }
    }

    json_decref(config);
    return 0;
} /* elb_chimera_load_config */

static int
elb_chimera_backend_init(
    uint32_t               host_api_version,
    const char            *config_json,
    elbencho_backend_priv *out_priv)
{
    int rc = 0;

    (void) host_api_version;

    if (!config_json) {
        fprintf(stderr, "elbencho-chimera: --backend-plugin-config <config.json> is required\n");
        return -EINVAL;
    }

    pthread_mutex_lock(&g_mutex);

    if (g_client == NULL) {
        rc = elb_chimera_load_config(config_json);
    }

    if (rc == 0) {
        g_refcount++;
        *out_priv = g_client;
    }

    pthread_mutex_unlock(&g_mutex);

    return rc;
} /* elb_chimera_backend_init */

static void
elb_chimera_backend_destroy(elbencho_backend_priv priv)
{
    (void) priv;

    pthread_mutex_lock(&g_mutex);

    if (--g_refcount == 0 && g_client) {
        /* Report the peak number of chimera ops that were in flight at once. A value > 1
         * proves the plugin drove chimera fully asynchronously (multiple ops outstanding
         * rather than serialized). Emitted as a stable token for test assertions. */
        fprintf(stderr, "elbencho-chimera: peak-async-inflight=%d\n", g_max_inflight);

        chimera_destroy(g_client);
        g_client = NULL;
        if (g_metrics) {
            prometheus_metrics_destroy(g_metrics);
            g_metrics = NULL;
        }
    }

    pthread_mutex_unlock(&g_mutex);
} /* elb_chimera_backend_destroy */

/* ---- per-worker lifecycle ---- */

static int
elb_chimera_worker_init(
    elbencho_backend_priv priv,
    size_t                worker_rank,
    elbencho_worker_ctx  *out_ctx)
{
    struct elb_chimera_ctx *ctx;

    (void) priv;
    (void) worker_rank;

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return -ENOMEM;
    }

    ctx->evpl   = evpl_create(NULL);
    ctx->client = chimera_client_thread_init(ctx->evpl, g_client);

    /* Allocate the per-slot async state once at a fixed capacity. It must NOT be
     * reallocated later: in-flight ops hold pointers to their slot (passed as the
     * completion callback's private_data), and a realloc would move the array and
     * dangle those pointers -- which crashes on a truly async backend (diskfs) where
     * many ops are outstanding at once. iodepth is bounded by the ring size. */
    ctx->slots = calloc(ELB_CHIMERA_RING_SIZE, sizeof(*ctx->slots));
    if (!ctx->slots) {
        chimera_client_thread_shutdown(ctx->evpl, ctx->client);
        evpl_destroy(ctx->evpl);
        free(ctx);
        return -ENOMEM;
    }
    ctx->num_slots = ELB_CHIMERA_RING_SIZE;

    *out_ctx = ctx;
    return 0;
} /* elb_chimera_worker_init */

static void
elb_chimera_worker_destroy(elbencho_worker_ctx octx)
{
    struct elb_chimera_ctx *ctx = octx;

    if (!ctx) {
        return;
    }

    /* fold this worker's peak concurrency into the process-wide maximum */
    pthread_mutex_lock(&g_mutex);
    if (ctx->max_inflight > g_max_inflight) {
        g_max_inflight = ctx->max_inflight;
    }
    pthread_mutex_unlock(&g_mutex);

    /* release any registered I/O buffers elbencho didn't free (must precede
     * evpl_destroy, since releasing an iovec needs the loop) */
    while (ctx->regbufs) {
        struct elb_regbuf *rb = ctx->regbufs;
        ctx->regbufs = rb->next;
        evpl_iovec_release(ctx->evpl, &rb->iov);
        free(rb);
    }

    chimera_client_thread_shutdown(ctx->evpl, ctx->client);
    evpl_destroy(ctx->evpl);
    free(ctx->slots);
    free(ctx);
} /* elb_chimera_worker_destroy */

/* ---- registered I/O buffers (zero-copy) ---- */

static void *
elb_chimera_iobuf_alloc(
    elbencho_worker_ctx octx,
    size_t              len,
    size_t              alignment)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_regbuf      *rb;
    int                     niov;

    rb = calloc(1, sizeof(*rb));
    if (!rb) {
        return NULL;
    }

    /* allocate directly into rb->iov (never struct-copy an evpl_iovec) */
    niov = evpl_iovec_alloc(ctx->evpl, (unsigned int) len,
                            (unsigned int) (alignment ? alignment : 4096), 1, 0, &rb->iov);
    if (niov != 1) {
        if (niov > 1) {
            evpl_iovecs_release(ctx->evpl, &rb->iov, niov);
        }
        free(rb);
        return NULL;
    }

    rb->base     = evpl_iovec_data(&rb->iov);
    rb->next     = ctx->regbufs;
    ctx->regbufs = rb;

    return rb->base;
} /* elb_chimera_iobuf_alloc */

static void
elb_chimera_iobuf_free(
    elbencho_worker_ctx octx,
    void               *buf,
    size_t              len)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_regbuf     **pp;

    (void) len;

    for (pp = &ctx->regbufs; *pp; pp = &(*pp)->next) {
        struct elb_regbuf *rb = *pp;

        if (rb->base == buf) {
            *pp = rb->next;
            evpl_iovec_release(ctx->evpl, &rb->iov);
            free(rb);
            return;
        }
    }
} /* elb_chimera_iobuf_free */

/* ---- open / close ---- */

struct elb_open_ctx {
    int                             done;
    enum chimera_vfs_error status;
    struct chimera_vfs_open_handle *handle;
};

static void
elb_open_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct elb_open_ctx *octx = private_data;

    octx->status = status;
    octx->handle = (status == CHIMERA_VFS_OK) ? handle : NULL;
    octx->done   = 1;
} /* elb_open_callback */

static int
elb_chimera_file_open(
    elbencho_worker_ctx octx,
    const char         *path,
    int                 flags,
    mode_t              mode,
    elbencho_fh        *out_fh)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_open_ctx     oc  = { 0 };

    (void) mode;

    chimera_open(ctx->client, path, (int) strlen(path),
                 elb_translate_open_flags(flags), elb_open_callback, &oc);

    while (!oc.done) {
        evpl_continue(ctx->evpl);
    }

    if (oc.status != CHIMERA_VFS_OK) {
        return -EIO;
    }

    *out_fh = oc.handle;
    return 0;
} /* elb_chimera_file_open */

static int
elb_chimera_file_close(
    elbencho_worker_ctx octx,
    elbencho_fh         fh)
{
    struct elb_chimera_ctx *ctx = octx;

    chimera_close(ctx->client, (struct chimera_vfs_open_handle *) fh);

    return 0;
} /* elb_chimera_file_close */

/* ---- synchronous positional I/O (iodepth==1 path) ---- */

struct elb_sync_io_ctx {
    int      done;
    enum chimera_vfs_error status;
    uint32_t count;
};

static void
elb_sync_read_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint32_t                      count,
    uint32_t                      eof,
    void                         *private_data)
{
    struct elb_sync_io_ctx *sc = private_data;

    (void) eof;
    sc->status = status;
    sc->count  = count;
    sc->done   = 1;
} /* elb_sync_read_callback */

static void
elb_sync_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct elb_sync_io_ctx *sc = private_data;

    sc->status = status;
    sc->done   = 1;
} /* elb_sync_write_callback */

static ssize_t
elb_chimera_pread(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    void               *buf,
    size_t              nbytes,
    off_t               offset)
{
    struct elb_chimera_ctx *ctx    = octx;
    struct elb_sync_io_ctx  sc     = { 0 };
    struct evpl_iovec      *regiov = elb_regbuf_lookup(ctx, buf);
    struct evpl_iovec       iov;
    int                     zerocopy = (regiov != NULL);
    int                     niov;

    if (zerocopy) {
        /* read straight into elbencho's (registered) buffer -- no bounce/copy */
        evpl_iovec_clone_segment(&iov, regiov, 0, (unsigned int) nbytes);
    } else {
        niov = evpl_iovec_alloc(ctx->evpl, (unsigned int) nbytes, 4096, 1, 0, &iov);
        if (niov != 1) {
            if (niov > 1) {
                evpl_iovecs_release(ctx->evpl, &iov, niov);
            }
            return -EINVAL;
        }
    }

    chimera_read_into(ctx->client, (struct chimera_vfs_open_handle *) fh, offset,
                      (uint32_t) nbytes, &iov, 1, elb_sync_read_callback, &sc);

    while (!sc.done) {
        evpl_continue(ctx->evpl);
    }

    if (sc.status != CHIMERA_VFS_OK) {
        evpl_iovec_release(ctx->evpl, &iov);
        return -EIO;
    }

    if (!zerocopy) {
        memcpy(buf, evpl_iovec_data(&iov), sc.count);
    }
    evpl_iovec_release(ctx->evpl, &iov);

    return (ssize_t) sc.count;
} /* elb_chimera_pread */

static ssize_t
elb_chimera_pwrite(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    const void         *buf,
    size_t              nbytes,
    off_t               offset)
{
    struct elb_chimera_ctx *ctx    = octx;
    struct elb_sync_io_ctx  sc     = { 0 };
    struct evpl_iovec      *regiov = elb_regbuf_lookup(ctx, buf);
    struct evpl_iovec       iov;
    int                     niov;

    if (regiov) {
        /* write straight from elbencho's (registered) buffer -- no bounce/copy.
         * writerv consumes this clone (a ref); the underlying buffer stays alive. */
        evpl_iovec_clone_segment(&iov, regiov, 0, (unsigned int) nbytes);
    } else {
        niov = evpl_iovec_alloc(ctx->evpl, (unsigned int) nbytes, 4096, 1, 0, &iov);
        if (niov != 1) {
            if (niov > 1) {
                evpl_iovecs_release(ctx->evpl, &iov, niov);
            }
            return -EINVAL;
        }
        memcpy(evpl_iovec_data(&iov), buf, nbytes);
    }

    /* chimera_writerv moves ownership of the iovec, so we must not release it. */
    chimera_writerv(ctx->client, (struct chimera_vfs_open_handle *) fh, offset,
                    (uint32_t) nbytes, &iov, 1, elb_sync_write_callback, &sc);

    while (!sc.done) {
        evpl_continue(ctx->evpl);
    }

    if (sc.status != CHIMERA_VFS_OK) {
        return -EIO;
    }

    return (ssize_t) nbytes;
} /* elb_chimera_pwrite */

/* ---- async submit / reap (iodepth>1 path) ---- */

static void
elb_async_read_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint32_t                      count,
    uint32_t                      eof,
    void                         *private_data)
{
    struct elb_chimera_slot *s   = private_data;
    struct elb_chimera_ctx  *ctx = s->ctx;

    (void) eof;

    ctx->inflight--;

    if (status == CHIMERA_VFS_OK) {
        /* zero-copy: data already landed in elbencho's registered buffer.
         * copy path: bounce it from the scratch iovec into the user buffer. */
        if (!s->zerocopy) {
            memcpy(s->user_buf, evpl_iovec_data(&s->iov), count);
        }
        elb_ring_enqueue(ctx, s->slot, (int64_t) count);
    } else {
        elb_ring_enqueue(ctx, s->slot, -EIO);
    }

    evpl_iovec_release(ctx->evpl, &s->iov);
} /* elb_async_read_callback */

static void
elb_async_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct elb_chimera_slot *s   = private_data;
    struct elb_chimera_ctx  *ctx = s->ctx;

    ctx->inflight--;

    /* chimera_writerv already consumed s->iov (ownership moved) */
    elb_ring_enqueue(ctx, s->slot,
                     (status == CHIMERA_VFS_OK) ? (int64_t) s->length : -EIO);
} /* elb_async_write_callback */

static int
elb_chimera_aio_submit(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    uint64_t            slot,
    int                 is_write,
    void               *buf,
    size_t              nbytes,
    off_t               offset)
{
    struct elb_chimera_ctx  *ctx = octx;
    struct elb_chimera_slot *s;
    struct evpl_iovec       *regiov;
    int                      niov;

    if (slot >= ctx->num_slots) {
        /* iodepth exceeds our fixed slot/ring capacity */
        return -EINVAL;
    }

    s           = &ctx->slots[slot];
    s->ctx      = ctx;
    s->slot     = slot;
    s->user_buf = buf;
    s->length   = (uint32_t) nbytes;
    s->is_write = is_write;

    regiov      = elb_regbuf_lookup(ctx, buf);
    s->zerocopy = (regiov != NULL);

    if (s->zerocopy) {
        /* clone a ref of elbencho's registered buffer: reads land directly in it
         * and writes go straight from it -- no bounce/copy. */
        evpl_iovec_clone_segment(&s->iov, regiov, 0, (unsigned int) nbytes);
    } else {
        niov = evpl_iovec_alloc(ctx->evpl, (unsigned int) nbytes, 4096, 1, 0, &s->iov);
        if (niov != 1) {
            if (niov > 1) {
                evpl_iovecs_release(ctx->evpl, &s->iov, niov);
            }
            return -EINVAL;
        }
    }

    /* Count this op as in flight before submitting and record the peak. A synchronous
     * backend may fire its completion (and decrement) inline during the submit call, so
     * incrementing first keeps the counter balanced; for a truly async backend the
     * completions are deferred until aio_reap() pumps the loop, so inflight climbs to the
     * full queue depth -- that peak (> 1) is the proof of concurrent async operation. */
    ctx->inflight++;
    if (ctx->inflight > ctx->max_inflight) {
        ctx->max_inflight = ctx->inflight;
    }

    if (is_write) {
        if (!s->zerocopy) {
            memcpy(evpl_iovec_data(&s->iov), buf, nbytes);
        }
        chimera_writerv(ctx->client, (struct chimera_vfs_open_handle *) fh, offset,
                        (uint32_t) nbytes, &s->iov, 1, elb_async_write_callback, s);
    } else {
        chimera_read_into(ctx->client, (struct chimera_vfs_open_handle *) fh, offset,
                          (uint32_t) nbytes, &s->iov, 1, elb_async_read_callback, s);
    }

    return 0;
} /* elb_chimera_aio_submit */

static int
elb_chimera_aio_reap(
    elbencho_worker_ctx            octx,
    unsigned                       min_complete,
    unsigned                       max,
    struct elbencho_io_completion *out,
    int                           *out_n)
{
    struct elb_chimera_ctx *ctx = octx;
    int                     n   = 0;

    while ((unsigned) elb_ring_count(ctx) < min_complete) {
        evpl_continue(ctx->evpl);
    }

    while ((unsigned) n < max && elb_ring_count(ctx) > 0) {
        out[n++]       = ctx->ring[ctx->ring_tail];
        ctx->ring_tail = (ctx->ring_tail + 1) & (ELB_CHIMERA_RING_SIZE - 1);
    }

    *out_n = n;
    return 0;
} /* elb_chimera_aio_reap */

/* ---- metadata ops ---- */

struct elb_meta_ctx {
    int                 done;
    enum chimera_vfs_error status;
    struct chimera_stat st;
    int                 have_st;
};

struct elb_statfs_ctx {
    int                    done;
    enum chimera_vfs_error status;
    struct chimera_statvfs st;
};

static void
elb_meta_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct elb_meta_ctx *mc = private_data;

    mc->status = status;
    mc->done   = 1;
} /* elb_meta_callback */

static void
elb_stat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct elb_meta_ctx *mc = private_data;

    mc->status = status;
    if (status == CHIMERA_VFS_OK && st) {
        mc->st      = *st;
        mc->have_st = 1;
    }
    mc->done = 1;
} /* elb_stat_callback */

static int
elb_meta_wait(
    struct elb_chimera_ctx *ctx,
    struct elb_meta_ctx    *mc)
{
    while (!mc->done) {
        evpl_continue(ctx->evpl);
    }
    return (mc->status == CHIMERA_VFS_OK) ? 0 : -EIO;
} /* elb_meta_wait */

static int
elb_chimera_op_mkdir(
    elbencho_worker_ctx octx,
    const char         *path,
    mode_t              mode)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    (void) mode;
    chimera_mkdir(ctx->client, path, (int) strlen(path), elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_mkdir */

static int
elb_chimera_op_rmdir(
    elbencho_worker_ctx octx,
    const char         *path)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    chimera_remove(ctx->client, path, (int) strlen(path), elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_rmdir */

static int
elb_chimera_op_unlink(
    elbencho_worker_ctx octx,
    const char         *path)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    chimera_remove(ctx->client, path, (int) strlen(path), elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_unlink */

static int
elb_chimera_op_rename(
    elbencho_worker_ctx octx,
    const char         *from,
    const char         *to)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    chimera_rename(ctx->client, from, (int) strlen(from), to, (int) strlen(to),
                   elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_rename */

static void
elb_stat_to_statbuf(
    const struct chimera_stat *cs,
    struct stat               *out)
{
    memset(out, 0, sizeof(*out));
    out->st_dev   = cs->st_dev;
    out->st_ino   = cs->st_ino;
    out->st_mode  = cs->st_mode;
    out->st_nlink = cs->st_nlink;
    out->st_uid   = cs->st_uid;
    out->st_gid   = cs->st_gid;
    out->st_rdev  = cs->st_rdev;
    out->st_size  = cs->st_size;
    out->st_atim  = cs->st_atim;
    out->st_mtim  = cs->st_mtim;
    out->st_ctim  = cs->st_ctim;
} /* elb_stat_to_statbuf */

static int
elb_chimera_op_stat(
    elbencho_worker_ctx octx,
    const char         *path,
    struct stat        *out)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };
    int                     rc;

    chimera_stat(ctx->client, path, (int) strlen(path), elb_stat_callback, &mc);
    rc = elb_meta_wait(ctx, &mc);

    if (rc == 0 && mc.have_st) {
        elb_stat_to_statbuf(&mc.st, out);
    }

    return rc;
} /* elb_chimera_op_stat */

static int
elb_chimera_op_fstat(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    struct stat        *out)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };
    int                     rc;

    chimera_fstat(ctx->client, (struct chimera_vfs_open_handle *) fh, elb_stat_callback, &mc);
    rc = elb_meta_wait(ctx, &mc);

    if (rc == 0 && mc.have_st) {
        elb_stat_to_statbuf(&mc.st, out);
    }

    return rc;
} /* elb_chimera_op_fstat */

static int
elb_chimera_op_fsync(
    elbencho_worker_ctx octx,
    elbencho_fh         fh)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    chimera_commit(ctx->client, (struct chimera_vfs_open_handle *) fh, elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_fsync */

static int
elb_chimera_op_ftruncate(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    off_t               length)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    chimera_ftruncate(ctx->client, (struct chimera_vfs_open_handle *) fh, (uint64_t) length,
                      elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_ftruncate */

static int
elb_chimera_op_fallocate(
    elbencho_worker_ctx octx,
    elbencho_fh         fh,
    int                 mode,
    off_t               offset,
    off_t               length)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_meta_ctx     mc  = { 0 };

    (void) mode;

    /* chimera has no dedicated allocate op; ensure the file is at least offset+length
     * bytes (matches elbencho's posix_fallocate(fd, 0, fileSize) preallocation use). */
    chimera_ftruncate(ctx->client, (struct chimera_vfs_open_handle *) fh,
                      (uint64_t) (offset + length), elb_meta_callback, &mc);
    return elb_meta_wait(ctx, &mc);
} /* elb_chimera_op_fallocate */

static void
elb_statfs_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_statvfs *st,
    void                         *private_data)
{
    struct elb_statfs_ctx *sc = private_data;

    sc->status = status;
    if (status == CHIMERA_VFS_OK && st) {
        sc->st = *st;
    }
    sc->done = 1;
} /* elb_statfs_callback */

static int
elb_chimera_op_statfs(
    elbencho_worker_ctx octx,
    const char         *path,
    struct statvfs     *out)
{
    struct elb_chimera_ctx *ctx = octx;
    struct elb_statfs_ctx   sc  = { 0 };

    chimera_statfs(ctx->client, path, (int) strlen(path), elb_statfs_callback, &sc);

    while (!sc.done) {
        evpl_continue(ctx->evpl);
    }

    if (sc.status != CHIMERA_VFS_OK) {
        return -EIO;
    }

    memset(out, 0, sizeof(*out));
    out->f_bsize   = sc.st.f_bsize;
    out->f_frsize  = sc.st.f_frsize;
    out->f_blocks  = sc.st.f_blocks;
    out->f_bfree   = sc.st.f_bfree;
    out->f_bavail  = sc.st.f_bavail;
    out->f_files   = sc.st.f_files;
    out->f_ffree   = sc.st.f_ffree;
    out->f_favail  = sc.st.f_favail;
    out->f_fsid    = sc.st.f_fsid;
    out->f_flag    = sc.st.f_flag;
    out->f_namemax = sc.st.f_namemax;

    return 0;
} /* elb_chimera_op_statfs */

/* ---- path classification (runs on the master thread before workers start) ---- */

/*
 * Classify a bench path as dir vs file so elbencho can pick dir-mode vs file-mode without doing
 * its own stat(). Uses a transient evpl + client thread since the master thread has no per-worker
 * context yet. A non-existent path is reported as a file (it may be created in a write phase).
 */
static int
elb_chimera_path_check(
    elbencho_backend_priv priv,
    const char           *path,
    int                  *out_is_dir)
{
    struct chimera_client        *client = priv;
    struct evpl                  *evpl;
    struct chimera_client_thread *thread;
    struct elb_meta_ctx           mc = { 0 };

    *out_is_dir = 0;

    evpl   = evpl_create(NULL);
    thread = chimera_client_thread_init(evpl, client);

    chimera_stat(thread, path, (int) strlen(path), elb_stat_callback, &mc);

    while (!mc.done) {
        evpl_continue(evpl);
    }

    if (mc.status == CHIMERA_VFS_OK && mc.have_st && S_ISDIR(mc.st.st_mode)) {
        *out_is_dir = 1;
    }

    chimera_client_thread_shutdown(evpl, thread);
    evpl_destroy(evpl);

    return 0;
} /* elb_chimera_path_check */

/* ---- registration ---- */

static const struct elbencho_backend_ops elb_chimera_ops = {
    .struct_size = sizeof(struct elbencho_backend_ops),
    .api_version = ELBENCHO_PLUGIN_API_VERSION,
    .name        = "chimera",
    .caps        = ELBENCHO_CAP_DIRMODE | ELBENCHO_CAP_FILEMODE | ELBENCHO_CAP_ASYNC |
        ELBENCHO_CAP_OWNS_PATHS | ELBENCHO_CAP_REGBUF,

    .backend_init    = elb_chimera_backend_init,
    .backend_destroy = elb_chimera_backend_destroy,
    .worker_init     = elb_chimera_worker_init,
    .worker_destroy  = elb_chimera_worker_destroy,

    .iobuf_alloc = elb_chimera_iobuf_alloc,
    .iobuf_free  = elb_chimera_iobuf_free,

    .file_open  = elb_chimera_file_open,
    .file_close = elb_chimera_file_close,

    .pread  = elb_chimera_pread,
    .pwrite = elb_chimera_pwrite,

    .aio_submit = elb_chimera_aio_submit,
    .aio_reap   = elb_chimera_aio_reap,

    .op_mkdir     = elb_chimera_op_mkdir,
    .op_rmdir     = elb_chimera_op_rmdir,
    .op_unlink    = elb_chimera_op_unlink,
    .op_rename    = elb_chimera_op_rename,
    .op_stat      = elb_chimera_op_stat,
    .op_fstat     = elb_chimera_op_fstat,
    .op_ftruncate = elb_chimera_op_ftruncate,
    .op_fallocate = elb_chimera_op_fallocate,
    .op_fsync     = elb_chimera_op_fsync,
    .op_statfs    = elb_chimera_op_statfs,
    .path_check   = elb_chimera_path_check,
};

SYMBOL_EXPORT const struct elbencho_backend_ops *
elbencho_plugin_register(uint32_t host_api_version)
{
    if (host_api_version != ELBENCHO_PLUGIN_API_VERSION) {
        return NULL;
    }

    return &elb_chimera_ops;
} /* elbencho_plugin_register */
