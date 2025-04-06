#include <stdio.h>
#include <sys/stat.h>
#include <dlfcn.h>
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
#include "vfs/io_uring/io_uring.h"
#include "vfs/cairn/cairn.h"
#include "vfs/demofs/demofs.h"
#include "common/misc.h"
#include "common/macros.h"
#include "uthash/utlist.h"


static void
chimera_vfs_delegation_thread_wake(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_vfs_delegation_thread *delegation_thread = container_of(doorbell, struct
                                                                           chimera_vfs_delegation_thread,
                                                                           doorbell);
    struct chimera_vfs_thread            *thread = delegation_thread->vfs_thread;
    struct chimera_vfs_request           *requests, *request;
    struct chimera_vfs_module            *module;

    pthread_mutex_lock(&delegation_thread->lock);
    requests                    = delegation_thread->requests;
    delegation_thread->requests = NULL;
    pthread_mutex_unlock(&delegation_thread->lock);

    while (requests) {
        request = requests;
        LL_DELETE(requests, request);

        module = request->module;
        module->dispatch(request, thread->module_private[module->fh_magic]);
    }
} /* chimera_vfs_delegation_thread_wake */

static void *
chimera_vfs_delegation_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_delegation_thread *delegation_thread = private_data;

    delegation_thread->evpl       = evpl;
    delegation_thread->vfs_thread = chimera_vfs_thread_init(evpl, delegation_thread->vfs);

    evpl_add_doorbell(evpl, &delegation_thread->doorbell,
                      chimera_vfs_delegation_thread_wake);

    return private_data;
} /* chimera_vfs_delegation_thread_init */

static void
chimera_vfs_delegation_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_delegation_thread *delegation_thread = private_data;

    evpl_remove_doorbell(evpl, &delegation_thread->doorbell);

    chimera_vfs_thread_destroy(delegation_thread->vfs_thread);
} /* chimera_vfs_delegation_thread_shutdown */

static void
chimera_vfs_close_thread_callback(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_open_handle  *handle       = private_data;
    struct chimera_vfs_close_thread *close_thread = handle->close_private;

    close_thread->num_pending--;
    free(handle);
} /* chimera_vfs_close_thread_callback */

static uint64_t
chimera_vfs_close_thread_sweep(
    struct evpl                     *evpl,
    struct chimera_vfs_close_thread *close_thread,
    struct vfs_open_cache           *cache,
    uint64_t                         min_age)
{
    struct chimera_vfs_thread      *thread = close_thread->vfs_thread;
    struct timespec                 now;
    uint64_t                        count = 0;
    struct chimera_vfs_open_handle *handles, *handle;

    clock_gettime(CLOCK_MONOTONIC, &now);

    handles = chimera_vfs_open_cache_defer_close(cache, &now, min_age, &count);

    while (handles) {

        handle = handles;
        LL_DELETE(handles, handle);

        close_thread->num_pending++;

        handle->close_private = close_thread;
        chimera_vfs_close(thread,
                          handle->fh,
                          handle->fh_len,
                          handle->vfs_private,
                          chimera_vfs_close_thread_callback,
                          handle);
    }

    return count;
} /* chimera_vfs_close_thread_sweep */

static void
chimera_vfs_close_thread_wake_shutdown(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_vfs_close_thread *close_thread = container_of(doorbell, struct chimera_vfs_close_thread, doorbell);
    uint64_t                         min_age, count;

    pthread_mutex_lock(&close_thread->lock);

    if (close_thread->shutdown) {
        min_age = 0;
    } else {
        min_age = 100000000UL;
    }

    count  = chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_path_cache, min_age);
    count += chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_file_cache, min_age);

    if (close_thread->shutdown && count == 0 && close_thread->num_pending == 0) {
        pthread_cond_signal(&close_thread->cond);
        close_thread->signaled = 1;
        pthread_mutex_unlock(&close_thread->lock);
        return;
    }

    if (close_thread->shutdown) {
        evpl_ring_doorbell(doorbell);
    }

    pthread_mutex_unlock(&close_thread->lock);

} /* chimera_vfs_close_thread_wake */

static void
chimera_vfs_close_thread_wake_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_vfs_close_thread *close_thread = container_of(timer, struct chimera_vfs_close_thread, timer);
    uint64_t                         min_age;

    pthread_mutex_lock(&close_thread->lock);

    min_age = 100000000UL;

    chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_path_cache, min_age);
    chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_file_cache, min_age);

    pthread_mutex_unlock(&close_thread->lock);

} /* chimera_vfs_close_thread_wake */

static void *
chimera_vfs_close_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;

    close_thread->evpl       = evpl;
    close_thread->vfs_thread = chimera_vfs_thread_init(evpl, close_thread->vfs);

    evpl_add_doorbell(evpl, &close_thread->doorbell,
                      chimera_vfs_close_thread_wake_shutdown);

    evpl_add_timer(evpl, &close_thread->timer,
                   chimera_vfs_close_thread_wake_timer,
                   100000UL);

    return private_data;
} /* chimera_vfs_close_thread_init */

static void
chimera_vfs_close_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;

    evpl_remove_doorbell(evpl, &close_thread->doorbell);
    evpl_remove_timer(evpl, &close_thread->timer);

    chimera_vfs_thread_destroy(close_thread->vfs_thread);
} /* chimera_vfs_close_thread_shutdown */

SYMBOL_EXPORT struct chimera_vfs *
chimera_vfs_init(
    int                                  num_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules)
{
    struct chimera_vfs        *vfs;
    struct chimera_vfs_module *module;
    char                       modsym[80];

    vfs = calloc(1, sizeof(*vfs));

    pthread_rwlock_init(&vfs->mounts_lock, NULL);

    vfs->vfs_open_path_cache = chimera_vfs_open_cache_init(CHIMERA_VFS_OPEN_ID_PATH, 10, 128 * 1024);
    vfs->vfs_open_file_cache = chimera_vfs_open_cache_init(CHIMERA_VFS_OPEN_ID_FILE, 10, 128 * 1024);

    for (int i = 0; i < num_modules; i++) {
        chimera_vfs_info("Initializing VFS module %s...", module_cfgs[i].module_name);

        snprintf(modsym, sizeof(modsym), "vfs_%s", module_cfgs[i].module_name);
        module = dlsym(RTLD_DEFAULT, modsym);
        chimera_vfs_abort_if(!module, "module %s symbol %s not found", module_cfgs[i].module_name, modsym);
        chimera_vfs_register(vfs, module, module_cfgs[i].config_path);
    }

    vfs->num_delegation_threads = num_delegation_threads;
    vfs->delegation_threads     = calloc(num_delegation_threads, sizeof(struct chimera_vfs_delegation_thread));

    for (int i = 0; i < vfs->num_delegation_threads; i++) {
        vfs->delegation_threads[i].vfs = vfs;
        pthread_mutex_init(&vfs->delegation_threads[i].lock, NULL);

        vfs->delegation_threads[i].evpl_thread = evpl_thread_create(
            NULL,
            chimera_vfs_delegation_thread_init,
            chimera_vfs_delegation_thread_shutdown,
            &vfs->delegation_threads[i]);
    }

    pthread_mutex_init(&vfs->close_thread.lock, NULL);
    pthread_cond_init(&vfs->close_thread.cond, NULL);
    vfs->close_thread.vfs      = vfs;
    vfs->close_thread.shutdown = 0;

    vfs->close_thread.evpl_thread = evpl_thread_create(
        NULL,
        chimera_vfs_close_thread_init,
        chimera_vfs_close_thread_shutdown,
        &vfs->close_thread);

    return vfs;
} /* chimera_vfs_init */

SYMBOL_EXPORT void
chimera_vfs_destroy(struct chimera_vfs *vfs)
{
    struct chimera_vfs_module *module;
    struct chimera_vfs_mount  *mount;
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

    pthread_mutex_lock(&vfs->close_thread.lock);
    vfs->close_thread.shutdown = 1;

    __sync_synchronize();

    evpl_ring_doorbell(&vfs->close_thread.doorbell);

    pthread_cond_wait(&vfs->close_thread.cond, &vfs->close_thread.lock);
    pthread_mutex_unlock(&vfs->close_thread.lock);

    evpl_thread_destroy(vfs->close_thread.evpl_thread);

    for (i = 0; i < vfs->num_delegation_threads; i++) {
        evpl_thread_destroy(vfs->delegation_threads[i].evpl_thread);
    }
    free(vfs->delegation_threads);

    while (vfs->mounts) {
        mount = vfs->mounts;
        DL_DELETE(vfs->mounts, mount);
        free(mount->name);
        free(mount->path);
        free(mount);
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        module->destroy(vfs->module_private[i]);
    }

    chimera_vfs_open_cache_destroy(vfs->vfs_open_path_cache);
    chimera_vfs_open_cache_destroy(vfs->vfs_open_file_cache);

    free(vfs);
} /* chimera_vfs_destroy */

static void
chimera_vfs_process_completion(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_vfs_thread  *thread = container_of(doorbell, struct chimera_vfs_thread, doorbell);
    struct chimera_vfs_request *complete_requests, *unblocked_requests, *request;

    pthread_mutex_lock(&thread->lock);
    complete_requests                 = thread->pending_complete_requests;
    unblocked_requests                = thread->unblocked_requests;
    thread->pending_complete_requests = NULL;
    thread->unblocked_requests        = NULL;
    pthread_mutex_unlock(&thread->lock);

    while (complete_requests) {
        request = complete_requests;
        DL_DELETE(complete_requests, request);
        request->complete_delegate(request);
    }

    while (unblocked_requests) {
        request = unblocked_requests;
        LL_DELETE(unblocked_requests, request);
        request->unblock_callback(request, request->pending_handle);
    }

} /* chimera_vfs_process_completion */

SYMBOL_EXPORT void
chimera_vfs_watchdog(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_request *request;
    struct timespec             now;
    uint64_t                    elapsed;

    request = thread->active_requests;

    if (!request) {
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &now);

    elapsed = chimera_get_elapsed_ns(&now, &request->start_time);

    if (elapsed > 10000000000UL) {
        chimera_vfs_debug("oldest request has been active for %lu ns", elapsed);
        chimera_vfs_dump_request(request);
    }

} /* chimera_vfs_watchdog_callback */

SYMBOL_EXPORT struct chimera_vfs_thread *
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


    pthread_mutex_init(&thread->lock, NULL);

    evpl_add_doorbell(evpl, &thread->doorbell, chimera_vfs_process_completion);

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

SYMBOL_EXPORT void
chimera_vfs_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs             *vfs = thread->vfs;
    struct chimera_vfs_module      *module;
    struct chimera_vfs_request     *request;
    struct chimera_vfs_open_handle *handle;
    int                             i;

    evpl_remove_doorbell(thread->evpl, &thread->doorbell);

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

    while (thread->free_synth_handles) {
        handle = thread->free_synth_handles;
        LL_DELETE(thread->free_synth_handles, handle);
        free(handle);
    }

    while (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
        free(request->plugin_data);
        free(request);
    }

    free(thread);
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module,
    const char                *cfgfile)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init(cfgfile);
} /* chimera_vfs_register */

SYMBOL_EXPORT int
chimera_vfs_mount(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *mount_path,
    const char         *module_path)
{
    struct chimera_vfs_mount  *mount;
    struct chimera_vfs_module *module;
    int                        i;

    mount = calloc(1, sizeof(*mount));

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            mount->module = module;
            break;
        }
    }

    if (!mount->module) {
        chimera_vfs_error("chimera_vfs_mount: module %s not found",
                          module_name);
        return -1;
    }

    mount->name = strdup(mount_path);
    mount->path = strdup(module_path);

    pthread_rwlock_wrlock(&vfs->mounts_lock);
    DL_APPEND(vfs->mounts, mount);
    pthread_rwlock_unlock(&vfs->mounts_lock);

    return 0;
} /* chimera_vfs_mount */

SYMBOL_EXPORT int
chimera_vfs_umount(
    struct chimera_vfs *vfs,
    const char         *mount_path)
{
    struct chimera_vfs_mount *mount, *tmp;

    pthread_rwlock_wrlock(&vfs->mounts_lock);
    DL_FOREACH_SAFE(vfs->mounts, mount, tmp)
    {
        if (strcmp(mount->name, mount_path) == 0) {
            DL_DELETE(vfs->mounts, mount);
            break;
        }
    }
    pthread_rwlock_unlock(&vfs->mounts_lock);

    if (!mount) {
        return CHIMERA_VFS_ENOENT;
    }

    free(mount->name);
    free(mount->path);
    free(mount);

    return CHIMERA_VFS_OK;

} /* chimera_vfs_umount */