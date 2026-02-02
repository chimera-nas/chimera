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
    void *pad;
    char *config;
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

    evpl_iovec_alloc(chimera_thread->evpl, total_mem, 4096, 1, 0, &chimera_thread->iov);

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

        chimera_destroy(ChimeraClient);
        prometheus_metrics_destroy(ChimeraMetrics);
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

        chimera_log_init();

        //ChimeraLogLevel = CHIMERA_LOG_DEBUG;

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

        struct chimera_vfs_cred root_cred;
        chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);
        ChimeraClient = chimera_client_init(ChimeraClientConfig, &root_cred, ChimeraMetrics);

        evpl          = evpl_create(NULL);
        client_thread = chimera_client_thread_init(evpl, ChimeraClient);

        if (config) {

            mounts = json_object_get(config, "mounts");

            if (mounts) {

                json_array_foreach(mounts, i, mount)
                {
                    module      = json_object_get(mount, "module");
                    module_path = json_object_get(mount, "module_path");
                    mount_point = json_object_get(mount, "mount_point");

                    if (!module || !module_path || !mount_point) {
                        fprintf(stderr, "Invalid mount config\n");
                        return EINVAL;
                    }

                    fprintf(stderr, "Mounting %s:%s at %s\n", json_string_value(module),
                            json_string_value(module_path), json_string_value(mount_point));

                    chimera_mount(client_thread,
                                  json_string_value(mount_point),
                                  json_string_value(module),
                                  json_string_value(module_path),
                                  NULL,
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
        evpl_iovec_release(chimera_thread->evpl, &iov[i]);
    }

    fio_chimera_ring_enqueue(chimera_thread, io_u);


} /* fio_chmera_io_callback */


static void
fio_chimera_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct io_u               *io_u           = private_data;
    struct thread_data        *td             = io_u->mmap_data;
    struct chimera_fio_thread *chimera_thread = td->io_ops_data;

    fio_chimera_ring_enqueue(chimera_thread, io_u);


} /* fio_chmera_io_callback */

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

SYMBOL_EXPORT struct ioengine_ops ioengine =
{
    .name               = "chimera",
    .version            = FIO_IOOPS_VERSION,
    .flags              = 0,
    .init               = fio_chimera_init,
    .post_init          = fio_chimera_post_init,
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

