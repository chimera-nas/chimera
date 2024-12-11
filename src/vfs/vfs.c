#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "vfs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "root/vfs_root.h"
#include "vfs_dump.h"
#include "vfs/memfs/memfs.h"
#include "vfs/linux/linux.h"

#include "common/misc.h"
#include "uthash/utlist.h"
#include "thread/thread.h"

static void *
chimera_vfs_syncthread_init(
    struct evpl *evpl,
    void        *private_data)
{
    return NULL;
} /* chimera_vfs_syncthread_init */

static void
chimera_vfs_syncthread_wake(
    struct evpl *evpl,
    void        *private_data)
{
} /* chimera_vfs_syncthread_wake */

static void
chimera_vfs_syncthread_destroy(void *private_data)
{
} /* chimera_vfs_sync_destroy */

static void *
chimera_vfs_close_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;

    close_thread->evpl       = evpl;
    close_thread->vfs_thread = chimera_vfs_thread_init(evpl, close_thread->vfs);

    return private_data;
} /* chimera_vfs_close_thread_init */

static void
chimera_vfs_close_thread_wake(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;
    struct chimera_vfs_thread       *thread       = close_thread->vfs_thread;
    struct timespec                  now;
    uint64_t                         min_age;
    struct chimera_vfs_open_handle  *handles, *handle;

    clock_gettime(CLOCK_MONOTONIC, &now);

    if (close_thread->shutdown) {
        min_age = 0;
    } else {
        min_age = 100000000UL;
    }

    handles = chimera_vfs_open_cache_defer_close(
        close_thread->vfs->vfs_open_cache, &now, min_age);

    if (handles == NULL && close_thread->shutdown
        && close_thread->num_pending == 0) {
        pthread_cond_signal(&close_thread->cond);
        return;
    }

    while (handles) {
        handle = handles;
        LL_DELETE(handles, handle);

        chimera_vfs_close(thread, handle->vfs_module,
                          handle->fh, handle->fh_len,
                          handle->vfs_private);

        free(handle);
    }


} /* chimera_vfs_close_thread_wake */

static void
chimera_vfs_close_thread_shutdown(void *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;

    chimera_vfs_thread_destroy(close_thread->vfs_thread);
} /* chimera_vfs_close_thread_shutdown */

static void
chimera_vfs_close_thread_destroy(void *private_data)
{
} /* chimera_vfs_close_thread_destroy */

struct chimera_vfs *
chimera_vfs_init(void)
{
    struct chimera_vfs *vfs;

    vfs = calloc(1, sizeof(*vfs));

    vfs->vfs_open_cache = chimera_vfs_open_cache_init();

    chimera_vfs_info("Initializing VFS root module...");
    chimera_vfs_register(vfs, &vfs_root);

    chimera_vfs_info("Initializing VFS memfs module...");
    chimera_vfs_register(vfs, &vfs_memvfs);

    chimera_vfs_info("Initializing VFS linux module...");
    chimera_vfs_register(vfs, &vfs_linux);

    vfs->syncthreads = evpl_threadpool_create(16,
                                              chimera_vfs_syncthread_init,
                                              chimera_vfs_syncthread_wake,
                                              NULL,
                                              chimera_vfs_syncthread_destroy,
                                              -1,
                                              vfs);

    pthread_mutex_init(&vfs->close_thread.lock, NULL);
    pthread_cond_init(&vfs->close_thread.cond, NULL);
    vfs->close_thread.vfs      = vfs;
    vfs->close_thread.shutdown = 0;

    vfs->close_thread.evpl_thread = evpl_thread_create(
        chimera_vfs_close_thread_init,
        chimera_vfs_close_thread_wake,
        chimera_vfs_close_thread_shutdown,
        chimera_vfs_close_thread_destroy,
        10,
        &vfs->close_thread);

    return vfs;
} /* chimera_vfs_init */

void
chimera_vfs_destroy(struct chimera_vfs *vfs)
{
    struct chimera_vfs_module *module;
    struct chimera_vfs_share  *share;
    int                        i;

    for (i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
        if (vfs->metrics[i].num_requests > 0) {
            chimera_vfs_info(
                "VFS metrics for %10s: %8d requests, %8lldns avg latency, %8lldns min latency, %8lldns max latency",
                chimera_vfs_op_name(i),
                vfs->metrics[i].num_requests,
                vfs->metrics[i].total_latency / vfs->metrics[i].num_requests,
                vfs->metrics[i].min_latency,
                vfs->metrics[i].max_latency);
        }
    }

    evpl_threadpool_destroy(vfs->syncthreads);

    pthread_mutex_lock(&vfs->close_thread.lock);
    vfs->close_thread.shutdown = 1;
    evpl_thread_wake(vfs->close_thread.evpl_thread);
    pthread_cond_wait(&vfs->close_thread.cond, &vfs->close_thread.lock);
    pthread_mutex_unlock(&vfs->close_thread.lock);

    evpl_thread_destroy(vfs->close_thread.evpl_thread);

    while (vfs->shares) {
        share = vfs->shares;
        DL_DELETE(vfs->shares, share);
        free(share->name);
        free(share->path);
        free(share);
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        module->destroy(vfs->module_private[i]);
    }

    chimera_vfs_open_cache_destroy(vfs->vfs_open_cache);

    free(vfs);
} /* chimera_vfs_destroy */

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs)
{
    struct chimera_vfs_thread *thread;
    struct chimera_vfs_module *module;
    int                        i;

    thread       = calloc(1, sizeof(*thread));
    thread->evpl = evpl;
    thread->vfs  = vfs;

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        thread->module_private[i] = module->thread_init(
            evpl, vfs->module_private[i]);
    }

    return thread;
} /* chimera_vfs_thread_init */

void
chimera_vfs_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs         *vfs = thread->vfs;
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    int                         i;

    for (i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
        if (thread->metrics[i].num_requests == 0) {
            continue;
        }

        vfs->metrics[i].num_requests += thread->metrics[i].num_requests;

        vfs->metrics[i].total_latency += thread->metrics[i].total_latency;

        if (thread->metrics[i].max_latency > vfs->metrics[i].max_latency) {
            vfs->metrics[i].max_latency = thread->metrics[i].max_latency;
        }

        if (thread->metrics[i].min_latency < vfs->metrics[i].min_latency ||
            vfs->metrics[i].min_latency == 0) {
            vfs->metrics[i].min_latency = thread->metrics[i].min_latency;
        }
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = thread->vfs->modules[i];

        if (!module) {
            continue;
        }

        module->thread_destroy(thread->module_private[i]);
    }

    while (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
        free(request);
    }

    free(thread);
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init();
} /* chimera_vfs_register */

int
chimera_vfs_create_share(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *share_path,
    const char         *module_path)
{
    struct chimera_vfs_share  *share;
    struct chimera_vfs_module *module;
    int                        i;

    share = calloc(1, sizeof(*share));

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            share->module = module;
            break;
        }
    }

    if (!share->module) {
        chimera_vfs_error("chimera_vfs_create_share: module %s not found",
                          module_name);
        return -1;
    }

    share->name = strdup(share_path);
    share->path = strdup(module_path);

    DL_APPEND(vfs->shares, share);
    return 0;
} /* chimera_vfs_create_share */