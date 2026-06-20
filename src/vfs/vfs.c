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
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_identity.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/memfs/memfs.h"
#include "vfs/memkv/memkv.h"
#include "vfs/linux/linux.h"

#ifdef HAVE_IO_URING
#include "vfs/io_uring/io_uring.h"
#endif /* ifdef HAVE_IO_URING */

#ifdef HAVE_CAIRN
#include "vfs/cairn/cairn.h"
#endif /* ifdef HAVE_CAIRN */

#include "vfs/diskfs/diskfs.h"
#include "common/misc.h"
#include "common/macros.h"
#include "prometheus-c.h"

SYMBOL_EXPORT struct chimera_vfs_clock chimera_vfs_clock;

SYMBOL_EXPORT void
chimera_vfs_clock_init(void)
{
    struct timespec ts;

    if (chimera_vfs_clock.initialized) {
        return; /* process-global singleton; first vfs wins */
    }

    stopwatch_context_init(&chimera_vfs_clock.ctx);

    clock_gettime(CLOCK_REALTIME, &ts);
    chimera_vfs_clock.base_wall_ns = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
    stopwatch_start(&chimera_vfs_clock.ctx, &chimera_vfs_clock.base_sw);
    chimera_vfs_clock.delta_ns         = 0;
    chimera_vfs_clock.last_refresh     = 0;
    chimera_vfs_clock.refresh_interval = chimera_vfs_ns_to_ticks(1000000000ULL); /* ~1s */
    chimera_vfs_clock.initialized      = 1;
} /* chimera_vfs_clock_init */

SYMBOL_EXPORT void
chimera_vfs_clock_shutdown(void)
{
    chimera_vfs_clock.initialized = 0;
} /* chimera_vfs_clock_shutdown */

static void
chimera_vfs_delegation_drain(struct chimera_vfs_delegation_thread *delegation_thread)
{
    struct chimera_vfs_thread  *thread = delegation_thread->vfs_thread;
    struct chimera_vfs_request *requests, *request;
    struct chimera_vfs_module  *module;

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
} /* chimera_vfs_delegation_drain */

static void
chimera_vfs_delegation_thread_wake(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_vfs_delegation_thread *delegation_thread = container_of(doorbell, struct
                                                                           chimera_vfs_delegation_thread,
                                                                           doorbell);

    chimera_vfs_delegation_drain(delegation_thread);
} /* chimera_vfs_delegation_thread_wake */

static void
chimera_vfs_delegation_thread_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_delegation_thread *delegation_thread = private_data;

    chimera_vfs_delegation_drain(delegation_thread);
} /* chimera_vfs_delegation_thread_poll */

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

    if (delegation_thread->mode == CHIMERA_VFS_DELEGATION_ASYNC) {
        delegation_thread->poll = evpl_add_poll(evpl,
                                                NULL,
                                                NULL,
                                                chimera_vfs_delegation_thread_poll,
                                                delegation_thread);
    }

    return private_data;
} /* chimera_vfs_delegation_thread_init */

static void
chimera_vfs_delegation_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_delegation_thread *delegation_thread = private_data;

    if (delegation_thread->poll) {
        evpl_remove_poll(evpl, delegation_thread->poll);
        delegation_thread->poll = NULL;
    }

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
} /* chimera_vfs_close_thread_callback */

static uint64_t
chimera_vfs_close_thread_sweep(
    struct evpl                     *evpl,
    struct chimera_vfs_close_thread *close_thread,
    struct vfs_open_cache           *cache,
    uint64_t                         min_age)
{
    struct chimera_vfs_thread      *thread = close_thread->vfs_thread;
    uint64_t                        count  = 0;
    struct chimera_vfs_open_handle *handles, *handle;

    handles = chimera_vfs_open_cache_defer_close(cache, chimera_vfs_now_ticks(), min_age, &count);

    while (handles) {

        handle = handles;
        LL_DELETE(handles, handle);

        if (chimera_vfs_open_handle_needs_backend_close(handle)) {
            close_thread->num_pending++;

            chimera_vfs_close(thread,
                              handle->vfs_module,
                              handle->vfs_private,
                              handle->fh_hash,
                              chimera_vfs_close_thread_callback,
                              close_thread);
        }

        /* defer_close removed the handle from the bucket but frees the struct
         * here (not via open_cache_free), so drop its anchored lease ref. */
        chimera_vfs_file_state_release(handle->file_state);

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
    int                              shutdown     = close_thread->shutdown;
    uint64_t                         min_age, count;

    min_age = shutdown ? 0 : 100000000UL;

    /* Sweep OUTSIDE close_thread->lock.  A close can re-enter this thread's
     * event loop -- diskfs commit drains its submission queue with a nested
     * evpl_continue() -- which re-fires this doorbell (and the periodic timer)
     * on this same thread; holding the lock across the sweep would self-deadlock
     * when the re-entrant callback tries to re-acquire it.  The lock guards only
     * the shutdown cond handshake with chimera_vfs_destroy, and the sweep state
     * it touches (num_pending, the open caches) is owned by this thread / the
     * per-shard cache locks, so it needs no protection here. */
    count  = chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_path_cache, min_age);
    count += chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_file_cache, min_age);

    if (!shutdown) {
        return;
    }

    if (count == 0 && close_thread->num_pending == 0) {
        /* Fully drained: hand off to the waiter in chimera_vfs_destroy.  Signal
         * under the lock so the wakeup can't be lost against its cond_wait. */
        pthread_mutex_lock(&close_thread->lock);
        close_thread->signaled = 1;
        pthread_cond_signal(&close_thread->cond);
        pthread_mutex_unlock(&close_thread->lock);
        return;
    }

    /* Closes still in flight -- keep sweeping until they complete. */
    evpl_ring_doorbell(doorbell);

} /* chimera_vfs_close_thread_wake_shutdown */

static void
chimera_vfs_close_thread_wake_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_vfs_close_thread *close_thread = container_of(timer, struct chimera_vfs_close_thread, timer);
    uint64_t                         min_age;

    min_age = 100000000UL;

    /* No lock: the periodic sweep touches only this thread's state (num_pending)
     * and the open caches (guarded by their own per-shard locks).  Holding
     * close_thread->lock here would self-deadlock if a close re-enters the event
     * loop and re-fires this timer (see chimera_vfs_close_thread_wake_shutdown). */
    chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_path_cache, min_age);
    chimera_vfs_close_thread_sweep(evpl, close_thread, close_thread->vfs->vfs_open_file_cache, min_age);

    /* Drop implicit I/O leases that have gone idle, bounding resident
     * per-file state for write-once / read-once workloads. */
    if (close_thread->vfs->vfs_state) {
        chimera_vfs_state_reap_idle(close_thread->vfs->vfs_state,
                                    close_thread->vfs->vfs_state->implicit_idle_ms);
    }

} /* chimera_vfs_close_thread_wake */

static void *
chimera_vfs_close_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_vfs_close_thread *close_thread = private_data;

    close_thread->evpl       = evpl;
    close_thread->vfs_thread = chimera_vfs_thread_init(evpl, close_thread->vfs);

    /* The close thread doubles as the lease service thread: all CAP_LEASE
     * backend ops and recall-driven breaks are issued from here.  Its
     * vfs_thread->doorbell runs chimera_vfs_process_completion, which drains the
     * backend-lease work queue. */
    if (close_thread->vfs->vfs_state) {
        chimera_vfs_state_set_service_thread(close_thread->vfs->vfs_state,
                                             close_thread->vfs_thread);
    }

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

static struct chimera_vfs_delegation_thread *
chimera_vfs_spawn_delegation_pool(
    struct chimera_vfs              *vfs,
    int                              count,
    enum chimera_vfs_delegation_mode mode)
{
    struct chimera_vfs_delegation_thread *pool;

    if (count <= 0) {
        return NULL;
    }

    pool = calloc(count, sizeof(struct chimera_vfs_delegation_thread));

    for (int i = 0; i < count; i++) {
        pool[i].vfs  = vfs;
        pool[i].mode = mode;
        pthread_mutex_init(&pool[i].lock, NULL);

        pool[i].evpl_thread = evpl_thread_create(
            NULL,
            chimera_vfs_delegation_thread_init,
            chimera_vfs_delegation_thread_shutdown,
            &pool[i]);
    }

    return pool;
} /* chimera_vfs_spawn_delegation_pool */

/*
 * Bring up the liburcu call_rcu reclaim workers.
 *
 * nworkers <= 0 (or >= the CPU count) requests one worker per CPU via liburcu's
 * create_all_cpu_call_rcu_data() -- maximum reclaim parallelism, but hundreds of
 * threads on a many-core host.  A smaller positive nworkers creates exactly that
 * many workers and round-robins every CPU onto them, capping thread/memory
 * overhead (and teardown cost) for short-lived or lightly-loaded instances.
 * Best-effort throughout: any failure leaves liburcu's single default worker in
 * place, which is correct, just slower to reclaim.
 */
static void
chimera_vfs_create_call_rcu_workers(int nworkers)
{
    long                   ncpu = sysconf(_SC_NPROCESSORS_CONF);
    struct call_rcu_data **workers;

    if (nworkers <= 0 || (ncpu > 0 && nworkers >= ncpu)) {
        if (create_all_cpu_call_rcu_data(0) != 0) {
            chimera_vfs_error("Failed to create per-CPU call_rcu workers; "
                              "falling back to the default RCU reclaim thread");
        }
        return;
    }

    if (ncpu <= 0) {
        /* Unknown CPU count -- nothing to map workers onto; keep the default. */
        return;
    }

    workers = calloc(nworkers, sizeof(*workers));
    if (!workers) {
        return;
    }

    for (int i = 0; i < nworkers; i++) {
        workers[i] = create_call_rcu_data(0, -1);
        if (!workers[i]) {
            chimera_vfs_error("Failed to create call_rcu worker %d/%d; "
                              "falling back to the default RCU reclaim thread", i, nworkers);
            /* Tear down the ones we did create and bail to the default worker. */
            for (int j = 0; j < i; j++) {
                call_rcu_data_free(workers[j]);
            }
            free(workers);
            return;
        }
    }

    for (long cpu = 0; cpu < ncpu; cpu++) {
        set_cpu_call_rcu_data(cpu, workers[cpu % nworkers]);
    }

    free(workers);
} /* chimera_vfs_create_call_rcu_workers */

SYMBOL_EXPORT struct chimera_vfs *
chimera_vfs_init(
    int                                  num_sync_delegation_threads,
    int                                  num_async_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules,
    const char                          *kv_module_name,
    int                                  cache_ttl,
    int                                  num_rcu_reclaim_threads,
    struct prometheus_metrics           *metrics)
{
    struct chimera_vfs        *vfs;
    struct chimera_vfs_module *module;
    char                       modsym[80];
    void                      *handle;
    const char                *effective_kv_module;

    /* Bring up the process-wide TSC clock before any cache/timestamp use. */
    chimera_vfs_clock_init();

    /* Scale RCU reclaim so the fungible-cache retire callbacks (attr/name/rpl)
     * keep up with per-request churn; without dedicated workers liburcu uses a
     * single default worker for the whole process.  num_rcu_reclaim_threads <= 0
     * means one worker per CPU (the default); a positive value caps the worker
     * count, which bounds thread/memory overhead and process-teardown cost on
     * many-core hosts (e.g. short-lived CI test daemons).  Best-effort -- on
     * failure call_rcu falls back to the default worker. */
    chimera_vfs_create_call_rcu_workers(num_rcu_reclaim_threads);

    vfs = calloc(1, sizeof(*vfs));

    /* Synthesize machine name for identification */
    chimera_vfs_synthesize_machine_name(vfs);

    vfs->mount_table = chimera_vfs_mount_table_create(4);

    if (metrics) {
        vfs->metrics.metrics    = metrics;
        vfs->metrics.op_latency = prometheus_metrics_create_histogram_time(metrics,
                                                                           "chimera_vfs_op_latency_nanoseconds",
                                                                           "The latency of VFS operations in nanoseconds",
                                                                           34);

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

    vfs->vfs_user_cache = chimera_vfs_user_cache_create(8192, 600);
    vfs->identity       = chimera_vfs_identity_create(vfs, 4);

    vfs->vfs_notify = chimera_vfs_notify_init(vfs);
    vfs->vfs_state  = chimera_vfs_state_init();
    vfs->pnfs       = chimera_vfs_pnfs_create();

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

    /* Set up the default KV module - a KV-only backend (memkv or sqlite) that
     * backs the global KV API and stores handle-state/KV records on behalf of
     * filesystem backends that cannot persist them natively.  Defaults to the
     * in-memory memkv when not specified. */
    effective_kv_module = (kv_module_name && kv_module_name[0] != '\0') ? kv_module_name : "memkv";

    /* Find the KV module among those already registered. */
    vfs->kv_module = NULL;
    for (int i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        if (vfs->modules[i] && strcmp(vfs->modules[i]->name, effective_kv_module) == 0) {
            vfs->kv_module = vfs->modules[i];
            break;
        }
    }

    /* The default KV is a VFS-core facility, not a share backend, so callers
     * need not list it among module_cfgs.  If it wasn't explicitly registered,
     * auto-register it from its built-in symbol (vfs_memkv / vfs_sqlite, linked
     * into chimera_vfs). */
    if (!vfs->kv_module) {
        if (strcmp(effective_kv_module, "memkv") == 0) {
            /* memkv is built into chimera_vfs; reference it directly so the
            * symbol is always retained regardless of linker --as-needed. */
            module = &vfs_memkv;
        } else {
            snprintf(modsym, sizeof(modsym), "vfs_%s", effective_kv_module);
            module = dlsym(RTLD_DEFAULT, modsym);
        }
        chimera_vfs_abort_if(!module,
                             "KV module '%s' not found (symbol vfs_%s)",
                             effective_kv_module, effective_kv_module);
        chimera_vfs_register(vfs, module, NULL);
        vfs->kv_module = vfs->modules[module->fh_magic];
    }

    chimera_vfs_abort_if(!vfs->kv_module,
                         "KV module '%s' not found", effective_kv_module);

    chimera_vfs_abort_if(!(vfs->kv_module->capabilities & CHIMERA_VFS_CAP_KV),
                         "KV module '%s' does not support KV operations (missing CHIMERA_VFS_CAP_KV)",
                         effective_kv_module);

    chimera_vfs_info("Using '%s' as KV backend", effective_kv_module);

    vfs->num_sync_delegation_threads = num_sync_delegation_threads;
    vfs->sync_delegation_threads     = chimera_vfs_spawn_delegation_pool(
        vfs, num_sync_delegation_threads, CHIMERA_VFS_DELEGATION_SYNC);

    vfs->num_async_delegation_threads = num_async_delegation_threads;
    vfs->async_delegation_threads     = chimera_vfs_spawn_delegation_pool(
        vfs, num_async_delegation_threads, CHIMERA_VFS_DELEGATION_ASYNC);

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
chimera_vfs_set_tcp_flavor(
    struct chimera_vfs     *vfs,
    enum chimera_tcp_flavor flavor)
{
    vfs->tcp_flavor = flavor;
} /* chimera_vfs_set_tcp_flavor */

SYMBOL_EXPORT int
chimera_vfs_fh_is_plausible(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    return chimera_vfs_get_module(thread, fh, fhlen) != NULL;
} /* chimera_vfs_fh_is_plausible */

SYMBOL_EXPORT int
chimera_vfs_can_persist_handle_state(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    if (!handle || !handle->vfs_module) {
        return 0;
    }

    /* Either the backend persists handle-state atomically with the open, or the
     * VFS core can persist it to the default KV on the backend's behalf. */
    if (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE) {
        return 1;
    }

    return thread->vfs->kv_module != NULL;
} /* chimera_vfs_can_persist_handle_state */

/* Capabilities of the backend module owning fh, or 0 if no mount matches.
 * Lets callers without an open handle (e.g. the NFS attribute marshaller)
 * learn a file's backend pNFS capabilities. */
SYMBOL_EXPORT uint64_t
chimera_vfs_module_capabilities(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    struct chimera_vfs_module *module = chimera_vfs_get_module(thread, fh, fhlen);

    return module ? module->capabilities : 0;
} /* chimera_vfs_module_capabilities */

/* Upper bound on the helper threads used to tear the per-CPU call_rcu workers
 * down in parallel (see chimera_vfs_free_all_cpu_call_rcu_data_parallel). */
#define CHIMERA_RCU_TEARDOWN_MAX_THREADS 64

struct chimera_rcu_teardown_ctx {
    pthread_t              thread;
    struct call_rcu_data **crdps;
    int                    count;
    int                    started;
};

static void *
chimera_vfs_rcu_teardown_worker(void *arg)
{
    struct chimera_rcu_teardown_ctx *ctx = arg;

    for (int i = 0; i < ctx->count; i++) {
        call_rcu_data_free(ctx->crdps[i]);
    }
    return NULL;
} /* chimera_vfs_rcu_teardown_worker */

/*
 * Tear down the per-CPU call_rcu workers created by create_all_cpu_call_rcu_data().
 *
 * liburcu's free_all_cpu_call_rcu_data() joins the workers one at a time, and
 * each worker can take up to its ~10ms idle poll interval to notice the stop
 * request.  On a many-core host that serial join dominates process shutdown
 * (~4s with several hundred workers on a 384-core box), which in turn is paid
 * on every short-lived test daemon.
 *
 * The joins are mutually independent, so we replicate liburcu's teardown but
 * fan the call_rcu_data_free() calls out across a bounded thread pool.  The
 * per-worker poll latencies then overlap instead of summing, collapsing the
 * teardown to tens of milliseconds while keeping shutdown clean (no SIGKILL),
 * so AddressSanitizer leak/UAF checks still run on a graceful exit.
 */
static void
chimera_vfs_free_all_cpu_call_rcu_data_parallel(void)
{
    long                             ncpu = sysconf(_SC_NPROCESSORS_CONF);
    struct call_rcu_data            *defaultcrdp;
    struct call_rcu_data           **crdps;
    struct chimera_rcu_teardown_ctx *ctx;
    int                              n = 0, nthreads, per, idx;

    if (ncpu <= 0) {
        free_all_cpu_call_rcu_data();
        return;
    }

    defaultcrdp = get_default_call_rcu_data();
    crdps       = calloc(ncpu, sizeof(*crdps));

    if (!crdps) {
        free_all_cpu_call_rcu_data();
        return;
    }

    /* Detach each per-CPU worker from the registry (cheap, no join) and collect
     * the unique crdps to free.  Skip the shared default worker -- liburcu owns
     * its lifetime and call_rcu_data_free() refuses to free it anyway. */
    for (long cpu = 0; cpu < ncpu; cpu++) {
        struct call_rcu_data *crdp = get_cpu_call_rcu_data(cpu);
        int                   dup  = 0;

        if (crdp == NULL || crdp == defaultcrdp) {
            continue;
        }

        set_cpu_call_rcu_data(cpu, NULL);

        for (int i = 0; i < n; i++) {
            if (crdps[i] == crdp) {
                dup = 1;
                break;
            }
        }

        if (!dup) {
            crdps[n++] = crdp;
        }
    }

    if (n == 0) {
        free(crdps);
        return;
    }

    /* Wait for any in-flight call_rcu() that read an old per-CPU pointer to
     * become quiescent before freeing the workers -- mirrors the synchronize_rcu()
     * inside liburcu's own free_all_cpu_call_rcu_data(). */
    synchronize_rcu();

    nthreads = n < CHIMERA_RCU_TEARDOWN_MAX_THREADS ? n : CHIMERA_RCU_TEARDOWN_MAX_THREADS;
    ctx      = calloc(nthreads, sizeof(*ctx));

    if (!ctx) {
        /* Fall back to a serial free of everything we detached. */
        for (int i = 0; i < n; i++) {
            call_rcu_data_free(crdps[i]);
        }
        free(crdps);
        return;
    }

    per = (n + nthreads - 1) / nthreads;
    idx = 0;

    for (int t = 0; t < nthreads && idx < n; t++) {
        ctx[t].crdps = &crdps[idx];
        ctx[t].count = (idx + per <= n) ? per : (n - idx);
        idx         += ctx[t].count;

        if (pthread_create(&ctx[t].thread, NULL, chimera_vfs_rcu_teardown_worker, &ctx[t]) == 0) {
            ctx[t].started = 1;
        } else {
            /* Spawn failed -- free this chunk inline so nothing leaks. */
            chimera_vfs_rcu_teardown_worker(&ctx[t]);
        }
    }

    for (int t = 0; t < nthreads; t++) {
        if (ctx[t].started) {
            pthread_join(ctx[t].thread, NULL);
        }
    }

    free(ctx);
    free(crdps);
} /* chimera_vfs_free_all_cpu_call_rcu_data_parallel */

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

    /* Stop the identity resolver first: its workers ring protocol/delegation
     * thread doorbells and write the user cache, both of which must still be
     * alive while the workers drain. */
    if (vfs->identity) {
        chimera_vfs_identity_destroy(vfs->identity);
        vfs->identity = NULL;
    }

    /* Stop the close thread before the delegation threads.  The shutdown-drain
     * handshake above (signaled only when num_pending == 0) already guarantees
     * every close the close thread dispatched to a delegation thread has
     * completed and rung back, so no in-flight delegated close remains.  But the
     * close thread's event loop is still running after that handshake -- its
     * periodic timer sweep and idle-lease reaper can issue *new* closes, which
     * for a CHIMERA_VFS_CAP_BLOCKING backend (cairn, diskfs) route through a
     * delegation thread and ring its doorbell.  If the delegation threads (and
     * their doorbells) were torn down first, such a late close would ring a
     * closed doorbell fd -> EBADF fatal abort (a use-after-close of the
     * doorbell's eventfd).  Quiescing the close thread first closes that window;
     * its own doorbell/timer-driven completions need no delegation thread. */
    evpl_thread_destroy(vfs->close_thread.evpl_thread);

    for (i = 0; i < vfs->num_sync_delegation_threads; i++) {
        evpl_thread_destroy(vfs->sync_delegation_threads[i].evpl_thread);
    }
    free(vfs->sync_delegation_threads);

    for (i = 0; i < vfs->num_async_delegation_threads; i++) {
        evpl_thread_destroy(vfs->async_delegation_threads[i].evpl_thread);
    }
    free(vfs->async_delegation_threads);

    chimera_vfs_mount_table_destroy(vfs->mount_table);

    chimera_vfs_pnfs_destroy(vfs->pnfs);

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

    if (vfs->vfs_notify) {
        chimera_vfs_notify_destroy(vfs->vfs_notify);
    }

    if (vfs->vfs_state) {
        chimera_vfs_state_destroy(vfs->vfs_state);
    }

    if (vfs->vfs_user_cache) {
        chimera_vfs_user_cache_destroy(vfs->vfs_user_cache);
    }

    if (vfs->vfs_name_cache) {
        chimera_vfs_name_cache_destroy(vfs->vfs_name_cache);
    }

    if (vfs->vfs_attr_cache) {
        chimera_vfs_attr_cache_destroy(vfs->vfs_attr_cache);
    }

    chimera_vfs_open_cache_destroy(vfs->vfs_open_path_cache);
    chimera_vfs_open_cache_destroy(vfs->vfs_open_file_cache);

    /* All RCU caches are destroyed above and each drained via rcu_barrier(), so
     * no callbacks remain; tear down the per-CPU call_rcu workers.  Use the
     * parallel teardown -- liburcu's free_all_cpu_call_rcu_data() joins them
     * serially, which dominates shutdown on many-core hosts. */
    chimera_vfs_free_all_cpu_call_rcu_data_parallel();

    if (vfs->metrics.op_latency) {
        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            prometheus_histogram_destroy_series(vfs->metrics.op_latency, vfs->metrics.op_latency_series[i]);
        }
        free(vfs->metrics.op_latency_series);
        prometheus_histogram_destroy(vfs->metrics.metrics, vfs->metrics.op_latency);
    }

    chimera_vfs_clock_shutdown();

    free(vfs);
} /* chimera_vfs_destroy */

static void
chimera_vfs_process_completion(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_vfs_thread     *thread = container_of(doorbell, struct chimera_vfs_thread, doorbell);
    struct chimera_vfs_request    *complete_requests, *unblocked_requests, *io_resume_requests, *request;
    struct chimera_vfs_file_state *lease_work;

    pthread_mutex_lock(&thread->lock);
    complete_requests                 = thread->pending_complete_requests;
    unblocked_requests                = thread->unblocked_requests;
    io_resume_requests                = thread->pending_io_resume;
    lease_work                        = thread->pending_lease_work;
    thread->pending_complete_requests = NULL;
    thread->unblocked_requests        = NULL;
    thread->pending_io_resume         = NULL;
    thread->pending_lease_work        = NULL;
    /* Clear the queued flag under the lock so a poster racing this drain
     * re-queues the file (and rings the doorbell again) rather than losing the
     * wakeup; the link itself is now privately owned by this drain. */
    for (struct chimera_vfs_file_state *f = lease_work; f; f = f->bl_work_next) {
        f->bl_work_queued = 0;
    }
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

    /* Resume parked I/O/metadata requests on this (their owning) thread. */
    while (io_resume_requests) {
        request = io_resume_requests;
        DL_DELETE(io_resume_requests, request);
        chimera_vfs_state_io_resume(request);
    }

    /* Run backend-lease state-machine steps marshaled to this (the service)
     * thread.  Each file carries a reference taken at post time; drop it after
     * running the step. */
    while (lease_work) {
        struct chimera_vfs_file_state *file = lease_work;

        lease_work         = file->bl_work_next;
        file->bl_work_next = NULL;
        chimera_vfs_backend_lease_run(thread->vfs->vfs_state, file);
        chimera_vfs_state_put(thread->vfs->vfs_state, file);
    }

    /* Issue queued KV-fallback lease persistence (put/delete) + CAP_LEASE
     * backend lock releases for jobs posted to the service thread. */
    chimera_vfs_lease_kv_drain(thread);

    /* Run protocol-lease (RANGE/SHARE) re-grants the break pump marshaled to
     * this (the owning) thread: authoritative backend projection + callback. */
    chimera_vfs_proto_lease_project_drain(thread);

    /* Deliver any identity-resolver jobs that completed for this thread. */
    chimera_vfs_identity_thread_complete(thread);

} /* chimera_vfs_process_completion */

SYMBOL_EXPORT void
chimera_vfs_watchdog(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_request *request;
    uint64_t                    elapsed;

    request = thread->active_requests;

    if (!request) {
        return;
    }

    elapsed = prometheus_stopwatch_elapsed_ns(&request->start_time);

    if (elapsed > 10000000000UL) {
        /* A request alive this long is wedged, and at the default (info) log
         * level a debug-only report is invisible -- a CI hang then yields no
         * server-side evidence at all (a stuck WRITE wedged an NFS4.1 session
         * slot for 290s with an empty log).  Identify the request at error
         * level; rate-limit to one report per thread per ~30s so a long wedge
         * does not flood. */
        struct timespec ts;

        clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);

        if (ts.tv_sec - thread->watchdog_last_report >= 30) {
            thread->watchdog_last_report = ts.tv_sec;
            if (request->wait_reason) {
                uint64_t now_ns = (uint64_t) ts.tv_sec * 1000000000ULL +
                    (uint64_t) ts.tv_nsec;
                uint64_t wait_ns = request->wait_since_ns ?
                    now_ns - request->wait_since_ns : 0;

                chimera_vfs_error(
                    "request %p op %s active for %lu sec wait=%s wait_sec=%llu arg0=%llu arg1=%llu arg2=%llu (oldest on this thread)",
                    (void *) request,
                    chimera_vfs_op_name(request->opcode),
                    elapsed / 1000000000UL,
                    request->wait_reason,
                    (unsigned long long) (wait_ns / 1000000000ULL),
                    (unsigned long long) request->wait_arg0,
                    (unsigned long long) request->wait_arg1,
                    (unsigned long long) request->wait_arg2);
            } else {
                chimera_vfs_error(
                    "request %p op %s active for %lu sec (oldest on this thread)",
                    (void *) request,
                    chimera_vfs_op_name(request->opcode),
                    elapsed / 1000000000UL);
            }
        }
        chimera_vfs_dump_request(request);
    }

} /* chimera_vfs_watchdog_callback */

/*
 * userspace-RCU runs in QSBR mode: read-side locks are free, but every
 * registered thread must announce quiescent states (or step out of the
 * grace-period quorum while it blocks), or grace periods stall process-wide.
 * The event-loop threads are the only RCU readers, so they are the only
 * threads we register; the loop hooks drive their quiescence:
 *
 *   iteration_end -> quiescent state once per evpl_continue() pass
 *   pre_wait      -> go offline before a (possibly indefinite) core wait
 *   post_wait     -> come back online before post-wait callbacks read RCU data
 *
 * (The cache maintenance / identity-resolver threads are pure writers --
 * call_rcu + rcu_assign, no read side -- so they are not registered at all and
 * never gate a grace period; see vfs_user_cache.h, vfs_identity.c,
 * s3_cred_cache.h.)
 */
static void
chimera_vfs_rcu_quiescent(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    (void) private_data;
    urcu_qsbr_quiescent_state();
} /* chimera_vfs_rcu_quiescent */

static void
chimera_vfs_rcu_offline(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    (void) private_data;
    urcu_qsbr_thread_offline();
} /* chimera_vfs_rcu_offline */

static void
chimera_vfs_rcu_online(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    (void) private_data;
    urcu_qsbr_thread_online();
} /* chimera_vfs_rcu_online */

static const struct evpl_loop_hooks chimera_vfs_rcu_hooks = {
    .iteration_end = chimera_vfs_rcu_quiescent,
    .pre_wait      = chimera_vfs_rcu_offline,
    .post_wait     = chimera_vfs_rcu_online,
};

/*
 * One OS thread can enter chimera_vfs_thread_init more than once -- e.g. a
 * server VFS module (diskfs) loaded in-process gives the thread an inner VFS
 * context on top of the outer one.  liburcu aborts on a double register, so
 * register (and install the loop hooks) exactly once per thread, on the first
 * entry, and tear down on the last.  Thread-local, so no locking is needed.
 */
static __thread int chimera_vfs_rcu_refs;

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

    /* Assign this thread a stable RCU recycle stripe (shared across its pools).
     * Entries it allocates record this stripe and are returned to it after the
     * grace period, so its recycle traffic stays on one stripe regardless of
     * CPU migration -- and never strands in a stripe nothing pops. */
    {
        static uint32_t rcu_stripe_seq;
        uint32_t        s = __atomic_fetch_add(&rcu_stripe_seq, 1, __ATOMIC_RELAXED);
        int             p;

        for (p = 0; p < CHIMERA_RCU_POOL_COUNT; p++) {
            thread->rcu_magazines[p].stripe = s;
        }
    }

    if (vfs->metrics.metrics) {
        thread->metrics.op_latency_series = calloc(CHIMERA_VFS_OP_NUM, sizeof(struct prometheus_histogram_instance *));

        for (int i = 0; i < CHIMERA_VFS_OP_NUM; i++) {
            thread->metrics.op_latency_series[i] = prometheus_histogram_series_create_instance(vfs->metrics.
                                                                                               op_latency_series[i]);
        }
    }

    if (chimera_vfs_rcu_refs++ == 0) {
        urcu_qsbr_register_thread();
        evpl_set_loop_hooks(evpl, &chimera_vfs_rcu_hooks);
    }

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

    /* Return this thread's recycled RCU cache entries to their pool depots so
     * they are reclaimed at cache destroy (the pools outlive the threads). */
    for (i = 0; i < CHIMERA_RCU_POOL_COUNT; i++) {
        chimera_rcu_magazine_drain(&thread->rcu_magazines[i]);
    }

    if (--chimera_vfs_rcu_refs == 0) {
        evpl_set_loop_hooks(thread->evpl, NULL);
        urcu_qsbr_unregister_thread();
    }

    free(thread);
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module,
    const char                *cfgdata)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init(cfgdata, vfs->metrics.metrics);

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

SYMBOL_EXPORT int
chimera_vfs_add_user(
    struct chimera_vfs *vfs,
    const char         *username,
    const char         *password,
    const char         *smbpasswd,
    const char         *sid,
    uint32_t            uid,
    uint32_t            gid,
    uint32_t            ngids,
    const uint32_t     *gids,
    int                 pinned)
{
    return chimera_vfs_user_cache_add(vfs->vfs_user_cache,
                                      username, password, smbpasswd, sid,
                                      uid, gid, ngids, gids, pinned);
} /* chimera_vfs_add_user */

SYMBOL_EXPORT int
chimera_vfs_remove_user(
    struct chimera_vfs *vfs,
    const char         *username)
{
    return chimera_vfs_user_cache_remove(vfs->vfs_user_cache, username);
} /* chimera_vfs_remove_user */

SYMBOL_EXPORT const struct chimera_vfs_user *
chimera_vfs_lookup_user_by_name(
    struct chimera_vfs *vfs,
    const char         *username)
{
    return chimera_vfs_user_cache_lookup_by_name(vfs->vfs_user_cache, username);
} /* chimera_vfs_lookup_user_by_name */

SYMBOL_EXPORT int
chimera_vfs_user_is_member(
    struct chimera_vfs *vfs,
    uint32_t            uid,
    uint32_t            gid)
{
    return chimera_vfs_user_cache_is_member(vfs->vfs_user_cache, uid, gid);
} /* chimera_vfs_user_is_member */

SYMBOL_EXPORT int
chimera_vfs_identity_uid_to_sid(
    struct chimera_vfs *vfs,
    uint32_t            uid,
    char               *buf,
    int                 buflen)
{
    const struct chimera_vfs_user *user;
    int                            rc = -1;

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_uid(vfs->vfs_user_cache, uid);

    if (user && user->sid[0]) {
        int len = (int) strlen(user->sid);

        if (len + 1 <= buflen) {
            memcpy(buf, user->sid, len + 1);
            rc = len;
        }
    }

    urcu_qsbr_read_unlock();

    return rc;
} /* chimera_vfs_identity_uid_to_sid */

SYMBOL_EXPORT int
chimera_vfs_identity_sid_to_uid(
    struct chimera_vfs *vfs,
    const char         *sid,
    uint32_t           *uid)
{
    const struct chimera_vfs_user *user;
    int                            rc = -1;

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_sid(vfs->vfs_user_cache, sid);

    if (user) {
        *uid = user->uid;
        rc   = 0;
    }

    urcu_qsbr_read_unlock();

    return rc;
} /* chimera_vfs_identity_sid_to_uid */


SYMBOL_EXPORT void
chimera_vfs_iterate_builtin_users(
    struct chimera_vfs         *vfs,
    chimera_vfs_user_iterate_cb callback,
    void                       *data)
{
    chimera_vfs_user_cache_iterate_builtin(vfs->vfs_user_cache, callback, data);
} /* chimera_vfs_iterate_builtin_users */
