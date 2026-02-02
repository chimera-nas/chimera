// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE 1

#include <stdio.h>
#include <sys/stat.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <utlist.h>

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_open_cache.h"
#include "vfs/root/vfs_root.h"
#include "vfs/vfs_dump.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_attr_cache.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/memfs/memfs.h"
#include "vfs/linux/linux.h"

#ifdef HAVE_IO_URING
#include "vfs/io_uring/io_uring.h"
#endif /* ifdef HAVE_IO_URING */

#ifdef HAVE_CAIRN
#include "vfs/cairn/cairn.h"
#endif /* ifdef HAVE_CAIRN */

#include "vfs/demofs/demofs.h"
#include "common/misc.h"
#include "common/macros.h"
#include "prometheus-c.h"


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
    struct chimera_vfs_close_thread *close_thread = private_data;

    close_thread->num_pending--;
    /* Handle is freed by chimera_vfs_close() */
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

        chimera_vfs_close(thread,
                          handle,
                          chimera_vfs_close_thread_callback,
                          close_thread);

        free(handle);
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

static void
chimera_vfs_synthesize_machine_name(struct chimera_vfs *vfs)
{
    char  hostname[64];
    char  machine_id[64];
    int   len;
    FILE *fp;

    /* Get hostname */
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        snprintf(hostname, sizeof(hostname), "unknown");
    }
    hostname[sizeof(hostname) - 1] = '\0';

    /* Get unique machine identifier from /etc/machine-id */
    machine_id[0] = '\0';
    fp            = fopen("/etc/machine-id", "r");

    if (fp) {
        if (fgets(machine_id, sizeof(machine_id), fp)) {
            /* Remove trailing newline if present */
            len = strlen(machine_id);

            if (len > 0 && machine_id[len - 1] == '\n') {
                machine_id[len - 1] = '\0';
            }

            /* Truncate to first 16 chars for brevity */
            if (strlen(machine_id) > 16) {
                machine_id[16] = '\0';
            }
        }
        fclose(fp);
    }

    /* If machine-id not available, try /sys/class/dmi/id/product_uuid */
    if (machine_id[0] == '\0') {
        fp = fopen("/sys/class/dmi/id/product_uuid", "r");

        if (fp) {
            if (fgets(machine_id, sizeof(machine_id), fp)) {
                len = strlen(machine_id);

                if (len > 0 && machine_id[len - 1] == '\n') {
                    machine_id[len - 1] = '\0';
                }

                if (strlen(machine_id) > 16) {
                    machine_id[16] = '\0';
                }
            }
            fclose(fp);
        }
    }

    /* Fall back to gethostid if no machine-id found */
    if (machine_id[0] == '\0') {
        snprintf(machine_id, sizeof(machine_id), "%08lx", gethostid());
    }

    /* Synthesize machine name: hostname chimera version machine-id */
    vfs->machine_name_len = snprintf(vfs->machine_name,
                                     sizeof(vfs->machine_name),
                                     "%s chimera %s %s",
                                     hostname,
                                     CHIMERA_VERSION,
                                     machine_id);

    /* Ensure we don't exceed buffer - snprintf returns what would have been
     * written, so clamp to actual buffer size */
    if (vfs->machine_name_len >= (int) sizeof(vfs->machine_name)) {
        vfs->machine_name_len = sizeof(vfs->machine_name) - 1;
    }

    chimera_vfs_info("Machine name: %.*s", vfs->machine_name_len, vfs->machine_name);
} /* chimera_vfs_synthesize_machine_name */

SYMBOL_EXPORT struct chimera_vfs *
chimera_vfs_init(
    int                                  num_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules,
    int                                  cache_ttl,
    struct prometheus_metrics           *metrics)
{
    struct chimera_vfs        *vfs;
    struct chimera_vfs_module *module;
    char                       modsym[80];
    void                      *handle;

    vfs = calloc(1, sizeof(*vfs));

    /* Synthesize machine name for identification */
    chimera_vfs_synthesize_machine_name(vfs);

    vfs->mount_table = chimera_vfs_mount_table_create(4);

    if (metrics) {
        vfs->metrics.metrics    = metrics;
        vfs->metrics.op_latency = prometheus_metrics_create_histogram_exponential(metrics,
                                                                                  "chimera_vfs_op_latency",
                                                                                  "The latency of VFS operations",
                                                                                  24);

        vfs->metrics.op_latency_series = calloc(CHIMERA_VFS_OP_NUM, sizeof(struct prometheus_histogram_series *));

        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            vfs->metrics.op_latency_series[i] = prometheus_histogram_create_series(vfs->metrics.op_latency,
                                                                                   (const char *[]) { "name" },
                                                                                   (const char *[]) {
                chimera_vfs_op_name(i)
            },
                                                                                   1);
        }
    }

    vfs->vfs_open_path_cache = chimera_vfs_open_cache_init(CHIMERA_VFS_OPEN_ID_PATH, 10, 128 * 1024, metrics,
                                                           "path_handles");
    vfs->vfs_open_file_cache = chimera_vfs_open_cache_init(CHIMERA_VFS_OPEN_ID_FILE, 10, 128 * 1024, metrics,
                                                           "file_handles");

    vfs->vfs_name_cache = chimera_vfs_name_cache_create(8, 4, 2, cache_ttl, metrics);
    vfs->vfs_attr_cache = chimera_vfs_attr_cache_create(8, 4, 2, cache_ttl, metrics);

    /* Register the root pseudo-filesystem module */
    chimera_vfs_register(vfs, &vfs_root, NULL);
    /* Create the root mount entry in the mount table */
    chimera_vfs_root_register_mount(vfs);

    for (int i = 0; i < num_modules; i++) {
        chimera_vfs_info("Initializing VFS module %s...", module_cfgs[i].module_name);
        snprintf(modsym, sizeof(modsym), "vfs_%s", module_cfgs[i].module_name);

        // If a module path is specified, attempt to load the shared object
        if (module_cfgs[i].module_path[0] != '\0') {
            // Check if the symbol is already present (module already loaded)
            if (dlsym(RTLD_DEFAULT, modsym) != NULL) {
                chimera_vfs_error("Module %s already loaded, skipping dlopen of %s",
                                  module_cfgs[i].module_name, module_cfgs[i].module_path);
            } else {
                // Attempt to load the module shared object
                handle = dlopen(module_cfgs[i].module_path, RTLD_NOW | RTLD_GLOBAL);
                if (!handle) {
                    chimera_vfs_abort_if(1, "Failed to load module %s from %s: %s",
                                         module_cfgs[i].module_name,
                                         module_cfgs[i].module_path,
                                         dlerror());
                }
                chimera_vfs_info("Module %s loaded from %s", module_cfgs[i].module_name, module_cfgs[i].module_path);
            }
        }

        // Lookup the module symbol (should be present after dlopen or if statically linked)
        module = dlsym(RTLD_DEFAULT, modsym);
        chimera_vfs_abort_if(!module,
                             "Module %s symbol %s not found after loading %s",
                             module_cfgs[i].module_name,
                             modsym,
                             module_cfgs[i].module_path);

        // Register the module with the VFS, passing its config path
        chimera_vfs_register(vfs, module, module_cfgs[i].config_data);
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
    int                        i;

    pthread_mutex_lock(&vfs->close_thread.lock);
    vfs->close_thread.shutdown = 1;

    __sync_synchronize();

    evpl_ring_doorbell(&vfs->close_thread.doorbell);

    pthread_cond_wait(&vfs->close_thread.cond, &vfs->close_thread.lock);
    pthread_mutex_unlock(&vfs->close_thread.lock);

    /* Destroy delegation threads before the close thread so that any
     * in-flight delegated close operations finish and their completion
     * doorbell rings execute before the close thread's vfs_thread
     * (and its doorbell) is freed. */
    for (i = 0; i < vfs->num_delegation_threads; i++) {
        evpl_thread_destroy(vfs->delegation_threads[i].evpl_thread);
    }
    free(vfs->delegation_threads);

    evpl_thread_destroy(vfs->close_thread.evpl_thread);

    chimera_vfs_mount_table_destroy(vfs->mount_table);

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (!vfs->module_private[i]) {
            continue;
        }

        module->destroy(vfs->module_private[i]);
    }

    if (vfs->vfs_name_cache) {
        chimera_vfs_name_cache_destroy(vfs->vfs_name_cache);
    }

    if (vfs->vfs_attr_cache) {
        chimera_vfs_attr_cache_destroy(vfs->vfs_attr_cache);
    }

    chimera_vfs_open_cache_destroy(vfs->vfs_open_path_cache);
    chimera_vfs_open_cache_destroy(vfs->vfs_open_file_cache);

    if (vfs->metrics.op_latency) {
        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            prometheus_histogram_destroy_series(vfs->metrics.op_latency, vfs->metrics.op_latency_series[i]);
        }
        free(vfs->metrics.op_latency_series);
        prometheus_histogram_destroy(vfs->metrics.metrics, vfs->metrics.op_latency);
    }

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

    if (vfs->metrics.metrics) {
        thread->metrics.op_latency_series = calloc(CHIMERA_VFS_OP_NUM, sizeof(struct prometheus_histogram_instance *));

        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            thread->metrics.op_latency_series[i] = prometheus_histogram_series_create_instance(vfs->metrics.
                                                                                               op_latency_series[i]);
        }
    }

    urcu_memb_register_thread();

    pthread_mutex_init(
        &thread->lock,
        NULL);

    evpl_add_doorbell(evpl, &thread->doorbell, chimera_vfs_process_completion);

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (!vfs->module_private[i]) {
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
    struct chimera_vfs_module      *module;
    struct chimera_vfs_request     *request;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_find_result *find_result;
    int                             i;

    evpl_remove_doorbell(thread->evpl, &thread->doorbell);

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = thread->vfs->modules[i];

        if (!module) {
            continue;
        }

        if (!thread->module_private[i]) {
            continue;
        }

        module->thread_destroy(thread->module_private[i]);
    }

    while (thread->free_find_results) {
        find_result = thread->free_find_results;
        LL_DELETE(thread->free_find_results, find_result);
        free(find_result);
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

    if (thread->metrics.op_latency_series) {
        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            prometheus_histogram_series_destroy_instance(thread->vfs->metrics.op_latency_series[i],
                                                         thread->metrics.op_latency_series[i]);
        }
        free(thread->metrics.op_latency_series);
    }

    free(thread);

    urcu_memb_unregister_thread();
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module,
    const char                *cfgfile)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init(cfgfile);

    if (vfs->module_private[module->fh_magic] == NULL) {
        chimera_vfs_error("Failed to initialize module %s", module->name);
    }

} /* chimera_vfs_register */

SYMBOL_EXPORT void
chimera_vfs_thread_drain(struct chimera_vfs_thread *thread)
{
    while (thread->num_active_requests) {
        evpl_continue(thread->evpl);
    }
} /* chimera_vfs_thread_drain */

SYMBOL_EXPORT void
chimera_vfs_get_root_fh(
    uint8_t  *fh,
    uint32_t *fh_len)
{
    chimera_vfs_root_get_fh(fh, fh_len);
} /* chimera_vfs_get_root_fh */