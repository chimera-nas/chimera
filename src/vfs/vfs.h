// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>
#include <sys/time.h>
#include <uthash.h>
#include "sdk/chimera_vfs_sdk.h"
#include "vfs_dump.h"
#include "vfs_pnfs.h"
#include "evpl/evpl.h"
#include "prometheus-c.h"
#include "vfs_clock.h"
#include "common/tcp_flavor.h"
#include "oteltracing.h"

/* Module tables are indexed by the one-byte fh_magic, so they span the
 * full 8-bit range (see vfs_fh_magic.h for reserved values). */
#define CHIMERA_VFS_MAX_MODULES 256
struct evpl;

struct chimera_vfs;
struct chimera_vfs_user;
struct chimera_vfs_user_cache;
struct prometheus_metrics;

/* RCU recycle pools: one per fungible cache (attr/name/rpl).  The per-thread
 * magazine type is defined here (no urcu dependency) so chimera_vfs_thread can
 * embed it; the pool itself and the helpers live in vfs_rcu_pool.h. */
enum chimera_rcu_pool_id {
    CHIMERA_RCU_POOL_ATTR = 0,
    CHIMERA_RCU_POOL_NAME,
    CHIMERA_RCU_POOL_RPL,
    CHIMERA_RCU_POOL_COUNT
};

#define CHIMERA_RCU_MAGAZINE_CAP 64

struct chimera_rcu_node;
struct chimera_rcu_magazine {
    struct chimera_rcu_node *head;
    uint32_t                 count;
    /* Stable depot stripe for this thread+pool, assigned once at thread init.
     * An entry's recycle home follows the thread that allocated it (carried in
     * the entry), not the transient CPU, so a worker that migrates across CPUs
     * still recovers its own retired entries instead of stranding them. */
    uint32_t                 stripe;
};

struct chimera_vfs_metrics {
    struct prometheus_metrics           *metrics;
    struct prometheus_histogram         *op_latency;
    struct prometheus_histogram_series **op_latency_series;
};

struct chimera_vfs_thread_metrics {
    struct prometheus_histogram_instance **op_latency_series;
};

/* Drop the per-file lease-state reference an open handle holds in
 * handle->file_state (see that field).  Uses the file_state's own back-pointer
 * to the owning state, so the open-cache teardown can release it without
 * threading the state through.  No-op safe to call with a NULL argument. */
void chimera_vfs_file_state_release(
    struct chimera_vfs_file_state *file);

struct chimera_vfs_find_result {
    int                             path_len;
    int                             emitted;
    struct chimera_vfs_request     *child_request;
    struct chimera_vfs_find_result *prev;
    struct chimera_vfs_find_result *next;
    struct chimera_vfs_attrs        attrs;
    char                            path[CHIMERA_VFS_PATH_MAX];
};

/* One deferred change-notify emission recorded by an ENLISTED namespace
 * mutation (see the notify_events list in struct chimera_vfs_compound and
 * chimera_vfs_notify_defer in vfs_internal.h).  `kind` selects the replay
 * call at COMPOUND_END:
 *
 *   EMIT     -> chimera_vfs_notify_emit_lease(fh, action, name, old_name,
 *               skip_lo/skip_hi/has_skip).  has_skip == 0 replays exactly
 *               the plain chimera_vfs_notify_emit (no lease self-exemption).
 *   DELETE   -> chimera_vfs_notify_emit_delete(fh): the object at fh was
 *               itself removed (DELETE_PENDING wakeup for watches armed on
 *               it, plus the late-arm tombstone).
 *   OVERFLOW -> chimera_vfs_notify_emit_overflow(fh): coarse degradation
 *               once the compound exceeded its record cap -- every watch on
 *               fh is marked overflowed (upstream: STATUS_NOTIFY_ENUM_DIR,
 *               i.e. "rescan") and fh's directory leases are broken. */
#define CHIMERA_VFS_DEFERRED_NOTIFY_EMIT         1
#define CHIMERA_VFS_DEFERRED_NOTIFY_DELETE       2
#define CHIMERA_VFS_DEFERRED_NOTIFY_OVERFLOW     3

/* Per-compound bounds: at most NOTIFY_MAX fine-grained (EMIT/DELETE)
 * records; past that each affected fh gets one OVERFLOW record, at most
 * NOTIFY_OVERFLOW_MAX of them (anything beyond even that is counted in
 * compound->notify_dropped and logged once at end). */
#define CHIMERA_VFS_COMPOUND_NOTIFY_MAX          64
#define CHIMERA_VFS_COMPOUND_NOTIFY_OVERFLOW_MAX 8

struct chimera_vfs_deferred_notify {
    struct chimera_vfs_deferred_notify *next;
    uint8_t                             kind;         /* CHIMERA_VFS_DEFERRED_NOTIFY_* */
    uint8_t                             has_skip;     /* EMIT: spare the lease named below */
    uint16_t                            fh_len;
    uint16_t                            name_len;
    uint16_t                            old_name_len;
    uint32_t                            action;       /* CHIMERA_VFS_NOTIFY_* */
    uint64_t                            skip_lo;      /* ParentLeaseKey to spare (EMIT) */
    uint64_t                            skip_hi;
    uint8_t                             fh[CHIMERA_VFS_FH_SIZE];
    char                                name[CHIMERA_VFS_NAME_MAX];
    char                                old_name[CHIMERA_VFS_NAME_MAX];
};

/* mount->attrs.flags bits */
#define CHIMERA_VFS_MOUNT_ATTR_READONLY (1ULL << 0)

struct chimera_vfs_mount_attrs {
    uint64_t flags;
};

struct chimera_vfs_mount {
    struct chimera_vfs_module     *module;
    char                          *path;
    char                          *module_path;
    char                          *options;
    uint32_t                       pathlen;
    int                            root_fh_len;
    void                          *mount_private;
    /* Set once umount has committed to tearing this mount down.  From that
    * point chimera_vfs_get_module refuses to route new work here, so the
    * set of handles referencing the mount can only shrink -- without it,
    * umount could drain the cache and have a fresh open land behind it. */
    int                            unmounting;
    struct chimera_vfs_mount_attrs attrs;
    struct chimera_vfs_mount      *prev;
    struct chimera_vfs_mount      *next;

    /* The first CHIMERA_VFS_MOUNT_ID_SIZE (16) bytes of root_fh is the mount_id,
     * which is itself a 128-bit hash. The remaining bytes are the fh_fragment.
     * Extra space is provided for NFS file handles which may exceed CHIMERA_VFS_FH_SIZE.
     */
    uint8_t                        root_fh[CHIMERA_VFS_FH_SIZE + 16];
};

enum chimera_vfs_delegation_mode {
    CHIMERA_VFS_DELEGATION_SYNC,
    CHIMERA_VFS_DELEGATION_ASYNC,
};

struct chimera_vfs_delegation_thread {
    struct evpl                *evpl;
    struct chimera_vfs         *vfs;
    struct evpl_thread         *evpl_thread;
    struct chimera_vfs_thread  *vfs_thread;
    struct chimera_vfs_request *requests;
    pthread_mutex_t             lock;
    struct evpl_doorbell        doorbell;
    enum chimera_vfs_delegation_mode mode;
    struct evpl_poll           *poll;
};

struct chimera_vfs_close_thread {
    struct evpl               *evpl;
    struct chimera_vfs        *vfs;
    struct evpl_thread        *evpl_thread;
    struct chimera_vfs_thread *vfs_thread;
    int                        shutdown;
    int                        num_pending;
    int                        signaled;
    struct evpl_doorbell       doorbell;
    struct evpl_timer          timer;
    pthread_mutex_t            lock;
    pthread_cond_t             cond;
};

struct chimera_vfs_mount_table;

struct chimera_vfs_notify;
struct chimera_vfs_state;
struct chimera_vfs_pnfs;

struct chimera_vfs {
    struct chimera_vfs_module            *modules[CHIMERA_VFS_MAX_MODULES];
    void                                 *module_private[CHIMERA_VFS_MAX_MODULES];
    struct chimera_vfs_module            *kv_module;
    struct vfs_open_cache                *vfs_open_path_cache;
    struct vfs_open_cache                *vfs_open_file_cache;
    struct chimera_vfs_name_cache        *vfs_name_cache;
    struct chimera_vfs_attr_cache        *vfs_attr_cache;
    struct chimera_vfs_user_cache        *vfs_user_cache;
    struct chimera_vfs_identity          *identity;
    struct chimera_vfs_notify            *vfs_notify;
    struct chimera_vfs_state             *vfs_state;
    struct chimera_vfs_mount_table       *mount_table;
    struct chimera_vfs_pnfs              *pnfs;
    int                                   num_sync_delegation_threads;
    struct chimera_vfs_delegation_thread *sync_delegation_threads;
    int                                   num_async_delegation_threads;
    struct chimera_vfs_delegation_thread *async_delegation_threads;
    struct chimera_vfs_close_thread       close_thread;
    struct chimera_vfs_metrics            metrics;
    enum chimera_tcp_flavor               tcp_flavor;
    /* True when any cross-protocol caching lease can exist (NFSv4
     * delegations, SMB2 leases, or SMB oplocks are enabled).  Gates the
     * name->FH resolution the remove/rename paths do so that an unlink of a
     * name another protocol caches recalls that holder first.  When clear
     * (single-protocol deployments) the extra lookup is skipped. */
    int                                   caching_enabled;
    /* How long umount waits for a mount's handles to be dropped before
     * reporting EBUSY (microseconds).  0 waits only as long as the closes it
     * issues itself take. */
    uint64_t                              umount_timeout_us;
    /* Allocation size of the pooled per-thread compound blobs: the maximum
     * compound_size over every registered CHIMERA_VFS_CAP_COMPOUND module,
     * floored at the bare core header so an unbound compound always fits.
     * Tracked in chimera_vfs_register; every module registers before any
     * vfs thread allocates a compound. */
    uint32_t                              max_compound_size;
    int                                   machine_name_len;
    char                                  machine_name[256];
};

/* Compound priority (`core.ts`) layout: the high bits carry a per-thread,
 * TSC-anchored, strictly-increasing counter (age order -> a longer-lived compound
 * outranks a newcomer, which keeps WFG victim selection starvation-free), and
 * the low CHIMERA_VFS_COMPOUND_THREAD_BITS carry the allocating thread's dense id so
 * values are globally unique without a shared atomic.  See
 * chimera_vfs_compound_alloc_ts(). */
#define CHIMERA_VFS_COMPOUND_THREAD_BITS 16

struct chimera_vfs_thread {
    struct evpl                         *evpl;
    struct chimera_vfs                  *vfs;
    void                                *module_private[CHIMERA_VFS_MAX_MODULES];
    /* Thread-local recycle magazines for the fungible RCU caches. */
    struct chimera_rcu_magazine          rcu_magazines[CHIMERA_RCU_POOL_COUNT];
    struct chimera_vfs_find_result      *free_find_results;
    struct chimera_vfs_request          *free_requests;
    struct chimera_vfs_request          *active_requests;
    uint64_t                             num_active_requests;
    struct chimera_vfs_open_handle      *free_synth_handles;
    /* Pool for the deferred change-notify records enlisted mutations hang
     * on their compound (chimera_vfs_notify_defer); recorded and emitted on
     * this thread only, so the list is unlocked like the pools above. */
    struct chimera_vfs_deferred_notify  *free_deferred_notify;

    /* Explicit-compound machinery (vfs_proc_compound_begin.c / _end.c):
     * the registry of live compounds this thread began (a DL list; any
     * survivor at thread destroy is a consumer bug), the pool of fixed-size
     * compound blobs (each vfs->max_compound_size bytes), and the per-thread
     * LOOSE singleton chimera_vfs_compound_loose() hands out (allocated at
     * thread init, never registered, never recycled). */
    struct chimera_vfs_compound         *active_compounds;
    struct chimera_vfs_compound         *free_compounds;
    struct chimera_vfs_compound         *loose_compound;

    struct chimera_vfs_request          *pending_complete_requests;
    struct chimera_vfs_request          *unblocked_requests;
    struct chimera_vfs_identity_request *pending_identity;
    /* Parked I/O requests being resumed on their owning thread (the lease
     * pump runs on whatever thread released/broke a lease, but a request's
     * dispatch+reply must run on the thread that owns its connection iovecs). */
    struct chimera_vfs_request          *pending_io_resume;
    /* Monotonic seconds of the last watchdog stuck-request report. */
    time_t                               watchdog_last_report;
    struct evpl_doorbell                 doorbell;
    pthread_mutex_t                      lock;
    uint64_t                             anon_fh_key;

    /* Trace parent for the next VFS op allocated on this thread: the protocol
     * span that is issuing it.  Set by the protocol immediately before each
     * chimera_vfs_* call and consumed (cleared) synchronously at request alloc,
     * so a forgotten set drops the child span rather than misattributing it. */
    struct otel_span                    *otel_parent;

    /* Compound-priority allocation (chimera_vfs_compound_alloc_ts): a dense thread
     * id assigned once at thread init (low bits of every ts -> global
     * uniqueness) plus the last high-part handed out (kept strictly increasing
     * for per-thread uniqueness within a TSC tick).  No shared atomic. */
    uint32_t                             compound_thread_id;
    uint64_t                             compound_ts_hi;

    struct chimera_vfs_thread_metrics    metrics;
};

struct chimera_vfs_module_cfg {
    char module_name[64];
    char module_path[256];
    char config_data[4096];
};

struct chimera_vfs *
chimera_vfs_init(
    int                                  num_sync_delegation_threads,
    int                                  num_async_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules,
    const char                          *kv_module_name,
    int                                  cache_ttl,
    int                                  attr_cache_enabled,
    int                                  name_cache_enabled,
    int                                  num_rcu_reclaim_threads,
    struct prometheus_metrics           *metrics);

void
chimera_vfs_destroy(
    struct chimera_vfs *vfs);

/* Select the TCP transport flavor used for outbound (client) connections.
 * Defaults to CHIMERA_TCP_FLAVOR_PLAIN; honored by VFS modules that open
 * their own TCP connections (e.g. the NFS client). */
void
chimera_vfs_set_tcp_flavor(
    struct chimera_vfs     *vfs,
    enum chimera_tcp_flavor flavor);

/* Declare whether any cross-protocol caching lease can exist (NFSv4
 * delegations / SMB2 leases / SMB oplocks enabled).  When set, the VFS
 * remove/rename paths resolve a by-name victim to its FH so a caching holder
 * is recalled before the namespace change; when clear the lookup is skipped. */
void
chimera_vfs_set_caching_enabled(
    struct chimera_vfs *vfs,
    int                 enabled);

/* Bound on how long umount waits for a mount's open handles to be dropped
 * before giving up with EBUSY, in milliseconds.  Without a bound a client
 * holding a file open would wedge the umount indefinitely. */
void
chimera_vfs_set_umount_timeout(
    struct chimera_vfs *vfs,
    int                 timeout_ms);

/* Get the root pseudo-filesystem's file handle */
void
chimera_vfs_get_root_fh(
    uint8_t  *fh,
    uint32_t *fh_len);

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs);

void
chimera_vfs_thread_destroy(
    struct chimera_vfs_thread *thread);

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module,
    const char                *cfgdata);

void
chimera_vfs_thread_drain(
    struct chimera_vfs_thread *thread);


int
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
    int                 pinned);

int
chimera_vfs_remove_user(
    struct chimera_vfs *vfs,
    const char         *username);

const struct chimera_vfs_user *
chimera_vfs_lookup_user_by_name(
    struct chimera_vfs *vfs,
    const char         *username);

int
chimera_vfs_user_is_member(
    struct chimera_vfs *vfs,
    uint32_t            uid,
    uint32_t            gid);

/*
 * Identity bridge used by ACL marshalling to round-trip *real* Windows SIDs
 * through the user cache (the single identity authority).  uid_to_sid copies
 * the cached SID for `uid` into `buf` and returns its length, or -1 when the
 * user has no known real SID (the caller then falls back to the algorithmic
 * idmap SID).  sid_to_uid resolves a SID string to its cached uid (0 on
 * success, -1 on miss).  The gid pair is the exact group-side counterpart,
 * backed by the group chains of the same cache, and follows the same return
 * conventions.  All four are RCU-safe and must be called from a VFS-registered
 * thread.
 */
int
chimera_vfs_identity_uid_to_sid(
    struct chimera_vfs *vfs,
    uint32_t            uid,
    char               *buf,
    int                 buflen);

int
chimera_vfs_identity_sid_to_uid(
    struct chimera_vfs *vfs,
    const char         *sid,
    uint32_t           *uid);

int
chimera_vfs_identity_gid_to_sid(
    struct chimera_vfs *vfs,
    uint32_t            gid,
    char               *buf,
    int                 buflen);

int
chimera_vfs_identity_sid_to_gid(
    struct chimera_vfs *vfs,
    const char         *sid,
    uint32_t           *gid);


typedef int (*chimera_vfs_user_iterate_cb)(
    const struct chimera_vfs_user *user,
    void                          *data);

void
chimera_vfs_iterate_builtin_users(
    struct chimera_vfs         *vfs,
    chimera_vfs_user_iterate_cb callback,
    void                       *data);
void
chimera_vfs_watchdog(
    struct chimera_vfs_thread *thread);
