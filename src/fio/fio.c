// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: GPL-2.0-only

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <jansson.h>
#include <xxhash.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "metrics/metrics.h"

#include "config-host.h"
#include "fio.h"
#include "optgroup.h"

#include "common/macros.h"
#include "common/common_config.h"


#define chimera_fio_debug(...) chimera_debug("fio", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fio_info(...)  chimera_info("fio", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fio_error(...) chimera_error("fio", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fio_fatal(...) chimera_fatal("fio", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fio_abort(...) chimera_abort("fio", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_fio_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "fio", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_fio_abort_if(cond, ...) \
        chimera_abort_if(cond, "fio", __FILE__, __LINE__, __VA_ARGS__)


pthread_mutex_t               ChimeraClientMutex  = PTHREAD_MUTEX_INITIALIZER;
int                           ChimeraNumClients   = 0;
struct prometheus_metrics    *ChimeraMetrics      = NULL;
struct chimera_client_config *ChimeraClientConfig = NULL;
struct chimera_client        *ChimeraClient       = NULL;
char                         *ChimeraMetricsFile  = NULL;

struct chimera_fio_thread {
    int                              event_head;
    int                              event_tail;
    int                              event_mask;
    int                              event_size;
    int                              max_handles;
    struct evpl                     *evpl;
    struct chimera_client_thread    *client;
    struct evpl_iovec                iov;
    struct chimera_vfs_open_handle **handles;
    struct io_u                     *events[1024];
};

struct chimera_options {
    void              *pad;
    char              *config;
    char              *logfile;
    int                debug;
    unsigned long long buffer_size;
};

static struct fio_option options[] = {
    {
        .name     = "chimera_config",
        .lname    = "Chimera Config Filename",
        .type     = FIO_OPT_STR_STORE,
        .off1     = offsetof(struct chimera_options, config),
        .help     = "Set path to chimera config file",
        .category = FIO_OPT_C_ENGINE,
        .group    = FIO_OPT_G_INVALID,
    },
    {
        .name  = "chimera_log",
        .lname = "Chimera Log Filename",
        .type  = FIO_OPT_STR_STORE,
        .off1  = offsetof(struct chimera_options, logfile),
        .help  =
            "Direct chimera and evpl log output to this file (truncated at start of run); if unset, chimera logging is disabled",
        .category = FIO_OPT_C_ENGINE,
        .group    = FIO_OPT_G_INVALID,
    },
    {
        .name     = "chimera_debug",
        .lname    = "Chimera Debug Logging",
        .type     = FIO_OPT_BOOL,
        .off1     = offsetof(struct chimera_options, debug),
        .help     = "Enable debug-level chimera logging (same as the daemon's -d)",
        .def      = "0",
        .category = FIO_OPT_C_ENGINE,
        .group    = FIO_OPT_G_INVALID,
    },
    {
        .name     = "chimera_buffer_size",
        .lname    = "Chimera evpl buffer size",
        .type     = FIO_OPT_STR_VAL,
        .off1     = offsetof(struct chimera_options, buffer_size),
        .help     = "Override the libevpl buffer size in bytes; 0 (default) auto-sizes to the largest job's iodepth*bs",
        .def      = "0",
        .category = FIO_OPT_C_ENGINE,
        .group    = FIO_OPT_G_INVALID,
    },
};

static struct io_u *
fio_chimera_event(
    struct thread_data *td,
    int                 event)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;
    struct io_u               *io_u;
    int                        slot;

    slot = (chimera_thread->event_tail + event) & chimera_thread->event_mask;

    io_u = chimera_thread->events[slot];

    chimera_thread->events[slot] = NULL;

    return io_u;
} /* fio_chimera_event */

static inline int
fio_chimera_num_events(struct chimera_fio_thread *chimera_thread)
{
    return ((chimera_thread->event_size + chimera_thread->event_head) - chimera_thread->event_tail) & chimera_thread->
           event_mask;
} /* fio_chimera_num_events */

static int
fio_chimera_getevents(
    struct thread_data    *td,
    unsigned int           min,
    unsigned int           max,
    const struct timespec *t)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;
    int                        n;

 again:

    while (chimera_thread->event_tail != chimera_thread->event_head &&
           chimera_thread->events[chimera_thread->event_tail] == NULL) {
        chimera_thread->event_tail = (chimera_thread->event_tail + 1) & chimera_thread->event_mask;
    }

    n = fio_chimera_num_events(chimera_thread);

    if (n > max) {
        n = max;
    }

    if (n < min) {
        evpl_continue(chimera_thread->evpl);
        goto again;
    }

    return n;
} /* fio_chimera_getevents */

static int
fio_chimera_invalidate(
    struct thread_data *td,
    struct fio_file    *file)
{
    return 0;
} /* fio_chimera_invalidate */

static int
fio_chimera_commit(struct thread_data *td)
{
    return 0;
} /* fio_chimera_commit */

static int
fio_chimera_open_file(
    struct thread_data *td,
    struct fio_file    *file)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;

    file->engine_data = chimera_thread->handles[file->engine_pos];

    if (!file->engine_data) {
        return -ENOENT;
    }

    return 0;
} /* fio_chimera_open_file */

static int
fio_chimera_close_file(
    struct thread_data *td,
    struct fio_file    *file)
{

    file->engine_data = NULL;

    return 0;
} /* fio_chimera_close_file */


static int
fio_chimera_iomem_alloc(
    struct thread_data *td,
    size_t              total_mem)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;
    int                        niov;

    /* The whole I/O pool must fit in one contiguous registered evpl buffer.
     * buffer_size was sized for the largest job at init, so this should always
     * succeed; fail loudly rather than hand fio a garbage orig_buffer if it
     * ever doesn't (e.g. a pool larger than the configured buffer). */
    niov = evpl_iovec_alloc(chimera_thread->evpl, total_mem, 4096, 1, 0, &chimera_thread->iov);

    if (niov != 1) {
        chimera_fio_fatal(
            "I/O buffer pool of %zu bytes exceeds the libevpl buffer size; "
            "raise chimera_buffer_size", total_mem);
        return 1;
    }

    td->orig_buffer = evpl_iovec_data(&chimera_thread->iov);

    return 0;
} /* fio_chimera_iomem_alloc */

static void
fio_chimera_iomem_free(struct thread_data *td)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;

    if (td->orig_buffer) {
        evpl_iovec_release(chimera_thread->evpl, &chimera_thread->iov);
        td->orig_buffer = NULL;
    }

} /* fio_chimera_iomem_free */

static void
fio_chimera_atexit(void)
{

    pthread_mutex_lock(&ChimeraClientMutex);

    if (ChimeraNumClients == 0) {

        /* Persist a final metrics scrape (common.metrics_file) before the
         * registry is destroyed; fio runs are often too short to scrape live. */
        if (ChimeraMetricsFile) {
            chimera_metrics_dump_file(ChimeraMetrics, ChimeraMetricsFile);
        }

        chimera_destroy(ChimeraClient);
        prometheus_metrics_destroy(ChimeraMetrics);

        free(ChimeraMetricsFile);
        ChimeraMetricsFile = NULL;
    }

    pthread_mutex_unlock(&ChimeraClientMutex);
} /* fio_chimera_atexit */

struct mount_ctx {
    int status;
    int complete;
    int total;
};

static void
mount_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct mount_ctx *ctx = private_data;

    if (status) {
        ctx->status = status;
    }
    ctx->complete++;
} /* mount_callback */

static int
fio_chimera_init(struct thread_data *td)
{
    struct chimera_fio_thread    *chimera_thread;
    int                           i;
    json_t                       *config = NULL, *mounts, *mount;
    json_t                       *modules, *module, *module_name, *mount_point, *module_path, *config_obj;
    struct chimera_options       *o         = td->eo;
    struct mount_ctx              mount_ctx = { 0 };
    struct evpl                  *evpl;
    struct chimera_client_thread *client_thread;


    pthread_mutex_lock(&ChimeraClientMutex);

    if (ChimeraClient == NULL) {

        if (o->logfile) {
            FILE *logfp = fopen(o->logfile, "w");

            if (!logfp) {
                fprintf(stderr, "Failed to open chimera log file %s\n", o->logfile);
                pthread_mutex_unlock(&ChimeraClientMutex);
                return EINVAL;
            }

            chimera_log_set_file(logfp);
        } else {
            chimera_log_disable();
        }

        chimera_log_init();

        if (o->debug) {
            ChimeraLogLevel = CHIMERA_LOG_DEBUG;
        }

        evpl_set_log_fn(chimera_vlog, chimera_log_flush);


        ChimeraMetrics = prometheus_metrics_create(NULL, NULL, 0);

        ChimeraClientConfig = chimera_client_config_init();

        if (o->config) {

            fprintf(stderr, "Loading config file %s\n", o->config);
            config = json_load_file(o->config, 0, NULL);

            if (!config) {
                fprintf(stderr, "Failed to load config file %s\n", o->config);
                return EINVAL;
            }

            modules = json_object_get(config, "modules");

            if (modules) {
                json_array_foreach(modules, i, module)
                {
                    module_name = json_object_get(module, "module");
                    module_path = json_object_get(module, "module_path");
                    config_obj  = json_object_get(module, "config");

                    if (!module_name || !module_path) {
                        fprintf(stderr, "Invalid module config\n");
                        return EINVAL;
                    }

                    char *config_str = NULL;

                    if (config_obj && json_is_object(config_obj)) {
                        config_str = json_dumps(config_obj, JSON_COMPACT);
                    }

                    fprintf(stderr, "Loading module %s path %s config %s\n", json_string_value(module_name),
                            json_string_value(module_path), config_str ? config_str : "");

                    chimera_client_config_add_module(ChimeraClientConfig, json_string_value(module_name),
                                                     json_string_value(module_path), config_str ? config_str : "");

                    free(config_str);


                }
            }
        }

        /* Honor the shared common tcp_flavor for outbound client connections. */
        chimera_client_config_set_tcp_flavor(ChimeraClientConfig,
                                             chimera_common_tcp_flavor(config));

        struct chimera_vfs_cred root_cred;
        chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

        /* Initialize evpl before the client (and the first evpl_create below),
         * applying the shared "common" config (huge pages / slab size) from the
         * loaded config.  `config` may be NULL when no config file was given. */
        {
            struct evpl_global_config *evpl_config = evpl_global_config_init();
            uint64_t                   buffer_size = 2 * 1024 * 1024; /* evpl default */
            uint64_t                   page        = (uint64_t) sysconf(_SC_PAGESIZE);
            uint64_t                   slab_size;

            chimera_apply_common_config(config, evpl_config);

            /* The engine backs each fio thread's entire I/O buffer pool
             * (iodepth * bs) with a SINGLE contiguous, RDMA-registered evpl
             * iovec, so the evpl buffer must be at least as large as the
             * biggest job's pool.  evpl_init() bakes the buffer size in and
             * runs once for the whole process, so size it here for the largest
             * job across all jobs (matches fio's own sizing in backend.c). */
            for_each_td(t)
            {
                struct chimera_options *to   = t->eo;
                uint64_t                pool = (uint64_t) t->o.iodepth * td_max_bs(t);

                if (t->o.odirect || t->o.mem_align) {
                    pool += page + t->o.mem_align;
                }
                if (pool > buffer_size) {
                    buffer_size = pool;
                }
                if (to && to->buffer_size > buffer_size) {
                    buffer_size = to->buffer_size;
                }
            }
            end_for_each();

            /* Round up to a 2 MiB multiple. */
            buffer_size = (buffer_size + (2 * 1024 * 1024 - 1)) & ~((uint64_t) (2 * 1024 * 1024) - 1);

            evpl_global_config_set_buffer_size(evpl_config, buffer_size);

            /* Keep several buffers per slab (buffers_per_slab = slab / buffer). */
            slab_size = 4 * buffer_size;
            if (slab_size > (uint64_t) 1024 * 1024 * 1024) {
                evpl_global_config_set_slab_size(evpl_config, slab_size);
            }

            chimera_fio_info("evpl buffer size %lu bytes (largest fio I/O pool)",
                             (unsigned long) buffer_size);

            evpl_init(evpl_config);
        }

        /* Stash the shutdown metrics-dump path while the config is still
         * loaded; it is consumed in fio_chimera_atexit() after config is freed. */
        {
            const char *metrics_file = chimera_common_metrics_file(config);

            if (metrics_file) {
                ChimeraMetricsFile = strdup(metrics_file);
            }
        }

        ChimeraClient = chimera_client_init(ChimeraClientConfig, &root_cred, ChimeraMetrics);

        evpl          = evpl_create(NULL);
        client_thread = chimera_client_thread_init(evpl, ChimeraClient);

        if (config) {

            mounts = json_object_get(config, "mounts");

            if (mounts) {

                mount_ctx.total = json_array_size(mounts);

                json_array_foreach(mounts, i, mount)
                {
                    module      = json_object_get(mount, "module");
                    module_path = json_object_get(mount, "module_path");
                    mount_point = json_object_get(mount, "mount_point");

                    if (!module || !module_path || !mount_point) {
                        fprintf(stderr, "Invalid mount config\n");
                        return EINVAL;
                    }

                    /* Optional comma-separated mount options (e.g. "rdma", "vers=4", "port=20049"). */
                    json_t     *mount_opts = json_object_get(mount, "options");
                    const char *opts_str   = mount_opts ? json_string_value(mount_opts) : NULL;

                    fprintf(stderr, "Mounting %s:%s at %s options=%s\n", json_string_value(module),
                            json_string_value(module_path), json_string_value(mount_point),
                            opts_str ? opts_str : "");

                    chimera_mount(client_thread,
                                  json_string_value(mount_point),
                                  json_string_value(module),
                                  json_string_value(module_path),
                                  opts_str,
                                  mount_callback,
                                  &mount_ctx);
                }

                while (mount_ctx.complete < mount_ctx.total) {
                    evpl_continue(evpl);
                }

                if (mount_ctx.status != 0) {
                    fprintf(stderr, "Failed to mount test module\n");
                    return 1;
                }
            }

            chimera_client_thread_shutdown(evpl, client_thread);
            evpl_destroy(evpl);

            json_decref(config);

            atexit(fio_chimera_atexit);
        }
    }

    ChimeraNumClients++;

    pthread_mutex_unlock(&ChimeraClientMutex);

    chimera_thread = calloc(1, sizeof(*chimera_thread));

    chimera_thread->evpl = evpl_create(NULL);

    chimera_thread->client = chimera_client_thread_init(chimera_thread->evpl, ChimeraClient);

    td->io_ops_data = chimera_thread;

    return 0;
} /* fio_chimera_file_setup */

static inline void
fio_chimera_ring_enqueue(
    struct chimera_fio_thread *chimera_thread,
    struct io_u               *io_u)
{

    chimera_fio_abort_if(((chimera_thread->event_head + 1) & chimera_thread->event_mask) == chimera_thread->event_tail,
                         "RING FULL: head=%u tail=%u\n", chimera_thread->event_head, chimera_thread->event_tail);

    chimera_fio_abort_if(chimera_thread->events[chimera_thread->event_head] != NULL, "event head is not NULL");

    chimera_thread->events[chimera_thread->event_head] = io_u;

    chimera_thread->event_head = (chimera_thread->event_head + 1) & chimera_thread->event_mask;

} /* fio_chimera_ring_enqueue */

static void
fio_chimera_read_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct io_u               *io_u           = private_data;
    struct thread_data        *td             = io_u->mmap_data;
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;
    uint64_t                   returned       = 0;
    uint32_t                   left, chunk, i;
    void                      *p;

    if (td->o.verify) {
        left = io_u->xfer_buflen;
        p    = io_u->xfer_buf;

        for (i = 0; i < niov && left; i++) {

            chunk = left < iov[i].length ? left : iov[i].length;
            memcpy(p, iov[i].data, chunk);
            p    += chunk;
            left -= chunk;
        }
    }

    for (i = 0; i < niov; i++) {
        returned += iov[i].length;
        evpl_iovec_release(chimera_thread->evpl, &iov[i]);
    }

    /* Report what actually came back rather than blindly crediting the full
     * request: a backend error fails the I/O, and a short read (e.g. past EOF)
     * leaves a residual so fio only counts the bytes that were really moved. */
    if (status != CHIMERA_VFS_OK) {
        io_u->error = EIO;
    } else {
        io_u->error = 0;
        io_u->resid = returned < io_u->xfer_buflen ? io_u->xfer_buflen - returned : 0;
    }

    fio_chimera_ring_enqueue(chimera_thread, io_u);


} /* fio_chimera_read_callback */


static void
fio_chimera_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct io_u               *io_u           = private_data;
    struct thread_data        *td             = io_u->mmap_data;
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;

    io_u->error = status != CHIMERA_VFS_OK ? EIO : 0;

    fio_chimera_ring_enqueue(chimera_thread, io_u);


} /* fio_chimera_write_callback */

static enum fio_q_status
fio_chimera_queue(
    struct thread_data *td,
    struct io_u        *io_u)
{
    struct chimera_fio_thread      *chimera_thread = td->io_ops_data;
    struct evpl_iovec               iov;
    enum fio_q_status               rc = FIO_Q_QUEUED;
    struct chimera_vfs_open_handle *fh;

    fio_ro_check(td, io_u);

    fh = io_u->file->engine_data;

    io_u->mmap_data = td;

    switch (io_u->ddir) {
        case DDIR_READ:
            chimera_read(chimera_thread->client, fh, io_u->offset, io_u->xfer_buflen,
                         fio_chimera_read_callback, io_u);
            break;
        case DDIR_WRITE:
        {
            unsigned int buf_offset = (unsigned int) ((char *) io_u->xfer_buf -
                                                      (char *) evpl_iovec_data(&chimera_thread->iov));

            evpl_iovec_clone_segment(&iov, &chimera_thread->iov, buf_offset, io_u->xfer_buflen);

            chimera_writerv(chimera_thread->client, fh, io_u->offset, io_u->xfer_buflen, &iov, 1,
                            fio_chimera_write_callback, io_u);
        }
        break;
        default:
            rc = FIO_Q_COMPLETED;
            break;
    } /* switch */


    io_u->error = 0;

    return rc;
} /* fio_chimera_queue */

static void
fio_chimera_open_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *fh,
    void                           *private_data)
{
    struct chimera_vfs_open_handle **fhp = private_data;

    if (status != CHIMERA_VFS_OK) {
        fprintf(stderr, "Failed to open file\n");
        *fhp = NULL;
        return;
    }

    *fhp = fh;
} /* fio_chimera_open_callback */

static int
fio_chimera_post_init(struct thread_data *td)
{
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;
    int                        i;
    struct fio_file           *f;

    chimera_thread->max_handles = td->o.nr_files;
    chimera_thread->handles     = calloc(chimera_thread->max_handles, sizeof(*chimera_thread->handles));

    chimera_thread->event_size = 1024;
    chimera_thread->event_mask = chimera_thread->event_size - 1;

    for_each_file(td, f, i)
    {

        f->engine_pos = i;

        chimera_open(
            chimera_thread->client,
            f->file_name,
            strlen(f->file_name),
            CHIMERA_VFS_OPEN_CREATE,
            fio_chimera_open_callback,
            &chimera_thread->handles[i]);
    }

    chimera_drain(chimera_thread->client);

    return 0;
} /* fio_chimera_post_init */



static void
fio_chimera_cleanup(struct thread_data *td)
{
    struct chimera_fio_thread      *chimera_thread;
    struct chimera_vfs_open_handle *fh;
    int                             i;

    chimera_thread = td->io_ops_data;

    for (i = 0; i < chimera_thread->max_handles; i++) {
        fh = chimera_thread->handles[i];

        if (fh) {
            chimera_close(chimera_thread->client, fh);
        }
    }

    chimera_client_thread_shutdown(chimera_thread->evpl, chimera_thread->client);

    evpl_destroy(chimera_thread->evpl);

    free(chimera_thread->handles);
    free(chimera_thread);

    pthread_mutex_lock(&ChimeraClientMutex);
    ChimeraNumClients--;
    pthread_mutex_unlock(&ChimeraClientMutex);
} /* fio_chimera_file_cleanup */

/*
 * Report the file size from the job options so fio's setup is satisfied for a
 * diskless engine and never falls back to laying the files out on the client's
 * local filesystem.  The files themselves live in the chimera namespace and are
 * created/written through the engine.
 */
static int
fio_chimera_get_file_size(
    struct thread_data *td,
    struct fio_file    *f)
{
    unsigned long long size = td->o.file_size_low;

    if (fio_file_size_known(f)) {
        return 0;
    }

    if (!size && td->o.size && td->o.nr_files) {
        size = td->o.size / td->o.nr_files;
    }

    f->real_file_size = size;
    fio_file_set_size_known(f);

    return 0;
} /* fio_chimera_get_file_size */

SYMBOL_EXPORT struct ioengine_ops ioengine =
{
    .name               = "chimera",
    .version            = FIO_IOOPS_VERSION,
    .flags              = FIO_DISKLESSIO,
    .init               = fio_chimera_init,
    .post_init          = fio_chimera_post_init,
    .get_file_size      = fio_chimera_get_file_size,
    .cleanup            = fio_chimera_cleanup,
    .iomem_alloc        = fio_chimera_iomem_alloc,
    .iomem_free         = fio_chimera_iomem_free,
    .queue              = fio_chimera_queue,
    .getevents          = fio_chimera_getevents,
    .event              = fio_chimera_event,
    .open_file          = fio_chimera_open_file,
    .close_file         = fio_chimera_close_file,
    .invalidate         = fio_chimera_invalidate,
    .options            = options,
    .option_struct_size = sizeof(struct chimera_options),
}; /* fio_chimera_file_setup */

