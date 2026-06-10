// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "nfs/nfs4_lease.h"
#include "s3/s3.h"
#include "smb/smb.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_release.h"
#include "common/macros.h"
#include "server/server.h"
#include "smb/smb2.h"
#include "rest/rest.h"

#define CHIMERA_SERVER_MAX_MODULES 64

struct chimera_server_config_smb_auth {
    int  winbind_enabled;
    int  kerberos_enabled;
    char winbind_domain[256];
    char kerberos_keytab[256];
    char kerberos_realm[256];
};

struct chimera_server_config {
    int                                   nfs_rdma;
    int                                   nfs_rdma_port;
    int                                   nfs_tcp_rdma_port;
    int                                   nfs_lockmgr_port;
    int                                   nfs_port;
    int                                   s3_port;
    int                                   nfs_data_server;
    uint64_t                              nfs_server_scope;
    int                                   external_portmap;
    char                                  portmap_hostname[256];
    int                                   soft_fail_bad_req;
    rlim_t                                max_open_files;
    int                                   core_threads;
    int                                   sync_delegation;
    int                                   sync_delegation_threads;
    int                                   async_delegation;
    int                                   async_delegation_threads;
    int                                   cache_ttl;
    int                                   rcu_reclaim_threads;
    int                                   nfs4_session_slots;
    int                                   nfs4_delegations;
    uint32_t                              nfs4_lease_time_s;
    uint32_t                              nfs4_grace_time_s;
    uint32_t                              nfs4_courtesy_time_s;
    int                                   num_modules;
    int                                   metrics_port;
    int                                   rest_http_port;
    int                                   rest_https_port;
    int                                   rest_debug_fsops;
    int                                   smb_num_dialects;
    uint32_t                              smb_dialects[16];
    int                                   smb_persistent_handles;
    int                                   smb_named_streams;
    int                                   smb_signing_required;
    int                                   smb_encryption;
    int                                   smb_compression;
    int                                   smb_leases;
    int                                   smb_oplocks;
    int                                   smb_notify_disabled;
    int                                   smb_acl_inherited_canonicalize;
    int                                   smb_num_nic_info;
    uint32_t                              anonuid;
    uint32_t                              anongid;
    enum chimera_tcp_flavor               tcp_flavor;
    char                                  nfs_rdma_hostname[256];
    char                                  kv_module[64];
    char                                  state_dir[256];
    char                                  rest_ssl_cert[256];
    char                                  rest_ssl_key[256];
    struct chimera_vfs_module_cfg         modules[CHIMERA_SERVER_MAX_MODULES];
    struct chimera_server_config_smb_nic  smb_nic_info[16];
    struct chimera_server_config_smb_auth smb_auth;
    int                                   pnfs_enabled;
    int                                   pnfs_num_ds;
    struct chimera_server_config_pnfs_ds {
        char netid[8];
        char uaddr[64];
        char rdma_uaddr[64];              /* optional RDMA netaddr (empty = none)   */
        char backing_path[CHIMERA_PNFS_BACKING_MAX];
        int  version;                     /* NFS version the client uses for DS I/O */
        int  minorversion;                /* NFS minor version (4.x)                */
    }                                     pnfs_ds[CHIMERA_PNFS_MAX_DS];
};

struct chimera_server {
    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;
    struct evpl_threadpool             *pool;
    struct chimera_server_protocol     *protocols[3];
    void                               *protocol_private[3];
    void                               *s3_shared;
    void                               *smb_shared;
    void                               *nfs_shared;
    struct chimera_rest_server         *rest;
    int                                 num_protocols;
    int                                 threads_online;
    pthread_mutex_t                     lock;
    pthread_cond_t                      all_threads_online;
};

struct chimera_thread {
    struct chimera_server     *server;
    struct chimera_vfs_thread *vfs_thread;
    void                      *protocol_private[3];
    void                      *rest_thread;
    struct evpl_timer          watchdog;
};

SYMBOL_EXPORT struct chimera_server_config *
chimera_server_config_init(void)
{
    struct chimera_server_config *config;

    config = calloc(1, sizeof(struct chimera_server_config));

    config->core_threads             = 8;
    config->max_open_files           = 65535;
    config->sync_delegation          = 1;
    config->sync_delegation_threads  = 8;
    config->async_delegation         = 0;
    config->async_delegation_threads = 8;
    config->nfs_rdma                 = 0;
    config->external_portmap         = 0;
    config->portmap_hostname[0]      = '\0';
    config->soft_fail_bad_req        = 0;
    config->rest_debug_fsops         = 0;
    config->tcp_flavor               = CHIMERA_TCP_FLAVOR_PLAIN;

    config->smb_num_dialects = 5;
    config->smb_dialects[0]  = SMB2_DIALECT_2_0_2;
    config->smb_dialects[1]  = SMB2_DIALECT_2_1;
    config->smb_dialects[2]  = SMB2_DIALECT_3_0;
    config->smb_dialects[3]  = SMB2_DIALECT_3_0_2;
    config->smb_dialects[4]  = SMB2_DIALECT_3_1_1;

    config->smb_num_nic_info = 0;

    /* SMB3 durable/persistent handles are off by default; they are an
     * opt-in feature gated by the "smb_persistent_handles" config flag. */
    config->smb_persistent_handles = 0;

    /* Named streams (SMB ADS) are off by default; opt-in via the
     * "smb_named_streams" config flag and only honored on backends that
     * advertise CHIMERA_VFS_CAP_NAMED_STREAMS. */
    config->smb_named_streams = 0;

    /* Server signing is advertised as enabled-but-optional by default; the
     * "smb_signing_required" config flag makes the server advertise signing as
     * mandatory (SMB2_SIGNING_REQUIRED). */
    config->smb_signing_required = 0;

    /* SMB3 transport encryption is off by default; the "smb_encryption" config
     * flag enables (1) or requires (2) it. */
    config->smb_encryption = 0;

    /* SMB3 transport compression is off by default; the "smb_compression" flag
     * enables (1) advertising/using it. */
    config->smb_compression = 0;

    /* SMB2 leases (RqLs) and legacy SMB oplocks are off by default: the server
     * grants neither, so clients run uncached (every op hits the server) and no
     * lease/oplock break can stall a conflicting open.  Opt in via the
     * "smb_leases" / "smb_oplocks" config flags; the smbtorture and WPTS test
     * harnesses enable them to exercise the leasing/oplock suites. */
    config->smb_leases  = 0;
    config->smb_oplocks = 0;

    /* CHANGE_NOTIFY is enabled by default; the "smb_notify_disabled" flag makes
     * the server reject CHANGE_NOTIFY with STATUS_NOT_IMPLEMENTED. */
    config->smb_notify_disabled = 0;

    /* Windows-style canonicalisation of the DACL_AUTO_INHERITED bit on
     * SET_SECURITY: a client-supplied AUTO_INHERITED without the matching
     * AUTO_INHERIT_REQ is silently stripped before storage (the default,
     * matching Samba's "acl flag inherited canonicalization = yes" and the
     * Windows server reference behaviour).  Set to 0 to preserve the bit
     * verbatim (Samba's "= no" mode that the smb2.acls_non_canonical suite
     * exercises). */
    config->smb_acl_inherited_canonicalize = 1;

    // SMB auth config defaults - local NTLM only
    config->smb_auth.winbind_enabled    = 0;
    config->smb_auth.kerberos_enabled   = 0;
    config->smb_auth.winbind_domain[0]  = '\0';
    config->smb_auth.kerberos_keytab[0] = '\0';
    config->smb_auth.kerberos_realm[0]  = '\0';

    config->anonuid = 65534;
    config->anongid = 65534;

    /* pNFS layouts are disabled by default. */
    config->pnfs_enabled = 0;
    config->pnfs_num_ds  = 0;

    /* NFS service port (default 2049); data-server mode binds only the NFSv4
     * service so a pNFS data server can coexist with an MDS on one host. */
    config->nfs_port        = 2049;
    config->s3_port         = 5000;
    config->nfs_data_server = 0;

    /* NFSv4.1 server identity (EXCHANGE_ID eir_server_scope).  Clients treat two
     * server addresses returning the same scope as the same server (shared
     * state, eligible for trunking).  Independent chimera servers that do not
     * share state -- e.g. a pNFS data server co-deployed with its MDS -- must
     * advertise distinct scopes or the client coalesces them and misroutes I/O.
     * Default preserves the historical value; override per instance via
     * "nfs_server_scope". */
    config->nfs_server_scope = 42;

    config->cache_ttl = 60;

    /* Number of liburcu call_rcu reclaim worker threads.  0 (the default) means
     * one worker per CPU (create_all_cpu_call_rcu_data) to keep RCU reclaim up
     * with per-request cache churn under heavy load.  On a many-core host that
     * spawns hundreds of threads, which is wasteful for short-lived or
     * lightly-loaded instances (memory, and process-teardown cost); set a small
     * positive value (e.g. the CI test daemons) to cap the worker count. */
    config->rcu_reclaim_threads = 0;

    /* Default NFSv4.1 fore-channel session slots (server cap on the number
     * of concurrent SEQUENCE requests a client may have outstanding per
     * session).  The chimera proxy client partitions slots one block per evpl
     * thread, so a high-thread-count client (e.g. fio numjobs) needs this many
     * slots or surplus threads collide on a shared slot; 256 covers typical
     * fan-out out of the box and the replay-slot table sizes to it.  Raise
     * further for very wide clients. */
    config->nfs4_session_slots = 256;

    /* NFSv4 protocol delegations (OPEN_DELEGATE_READ/WRITE) are disabled by
     * default.  When off, every OPEN returns OPEN_DELEGATE_NONE and the
     * callback channel is never established.  Distinct from the VFS
     * sync_delegation/async_delegation thread-pool knobs above. */
    config->nfs4_delegations     = 0;
    config->nfs4_lease_time_s    = NFS4_LEASE_TIME_DEFAULT_S;
    config->nfs4_grace_time_s    = NFS4_GRACE_TIME_DEFAULT_S;
    config->nfs4_courtesy_time_s = NFS4_COURTESY_TIME_DEFAULT_S;

    strncpy(config->nfs_rdma_hostname, "0.0.0.0", sizeof(config->nfs_rdma_hostname));
    config->nfs_rdma_port    = 20049;
    config->nfs_lockmgr_port = 32803;

    snprintf(config->state_dir, sizeof(config->state_dir), "%s", CHIMERA_STATE_DIR);

    strncpy(config->modules[0].module_name, "root", sizeof(config->modules[0].module_name));
    config->modules[0].config_data[0] = '\0';
    config->modules[0].module_path[0] = '\0';

    strncpy(config->modules[1].module_name, "nfs", sizeof(config->modules[1].module_name));
    config->modules[1].config_data[0] = '\0';
    config->modules[1].module_path[0] = '\0';

    strncpy(config->modules[2].module_name, "memfs", sizeof(config->modules[2].module_name));
    config->modules[2].config_data[0] = '\0';
    config->modules[2].module_path[0] = '\0';

    strncpy(config->modules[3].module_name, "linux", sizeof(config->modules[3].module_name));
    config->modules[3].config_data[0] = '\0';
    config->modules[3].module_path[0] = '\0';

    config->num_modules = 4;

#ifdef HAVE_IO_URING
    strncpy(config->modules[4].module_name, "io_uring", sizeof(config->modules[4].module_name));
    config->modules[4].config_data[0] = '\0';
    config->modules[4].module_path[0] = '\0';

    config->num_modules = 5;
#endif /* ifdef HAVE_IO_URING */

    /* The default KV module (memkv) is auto-registered by chimera_vfs_init; it
     * need not be listed here. */

    return config;
} /* chimera_server_config_init */

SYMBOL_EXPORT void
chimera_server_config_set_core_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->core_threads = threads;
} /* chimera_server_config_set_core_threads */

SYMBOL_EXPORT void
chimera_server_config_set_sync_delegation(
    struct chimera_server_config *config,
    int                           enable)
{
    config->sync_delegation = enable;
} /* chimera_server_config_set_sync_delegation */

SYMBOL_EXPORT void
chimera_server_config_set_sync_delegation_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->sync_delegation_threads = threads;
} /* chimera_server_config_set_sync_delegation_threads */

SYMBOL_EXPORT void
chimera_server_config_set_async_delegation(
    struct chimera_server_config *config,
    int                           enable)
{
    config->async_delegation = enable;
} /* chimera_server_config_set_async_delegation */

SYMBOL_EXPORT void
chimera_server_config_set_async_delegation_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->async_delegation_threads = threads;
} /* chimera_server_config_set_async_delegation_threads */

SYMBOL_EXPORT void
chimera_server_config_set_smb_persistent_handles(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_persistent_handles = enable;
} /* chimera_server_config_set_smb_persistent_handles */

SYMBOL_EXPORT int
chimera_server_config_get_smb_persistent_handles(const struct chimera_server_config *config)
{
    return config->smb_persistent_handles;
} /* chimera_server_config_get_smb_persistent_handles */

SYMBOL_EXPORT void
chimera_server_config_set_smb_named_streams(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_named_streams = enable;
} /* chimera_server_config_set_smb_named_streams */

SYMBOL_EXPORT int
chimera_server_config_get_smb_named_streams(const struct chimera_server_config *config)
{
    return config->smb_named_streams;
} /* chimera_server_config_get_smb_named_streams */

SYMBOL_EXPORT void
chimera_server_config_set_smb_signing_required(
    struct chimera_server_config *config,
    int                           required)
{
    config->smb_signing_required = required;
} /* chimera_server_config_set_smb_signing_required */

SYMBOL_EXPORT int
chimera_server_config_get_smb_signing_required(const struct chimera_server_config *config)
{
    return config->smb_signing_required;
} /* chimera_server_config_get_smb_signing_required */

SYMBOL_EXPORT void
chimera_server_config_set_smb_encryption(
    struct chimera_server_config *config,
    int                           mode)
{
    config->smb_encryption = mode;
} /* chimera_server_config_set_smb_encryption */

SYMBOL_EXPORT int
chimera_server_config_get_smb_encryption(const struct chimera_server_config *config)
{
    return config->smb_encryption;
} /* chimera_server_config_get_smb_encryption */

SYMBOL_EXPORT void
chimera_server_config_set_smb_compression(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_compression = enabled;
} /* chimera_server_config_set_smb_compression */

SYMBOL_EXPORT int
chimera_server_config_get_smb_compression(const struct chimera_server_config *config)
{
    return config->smb_compression;
} /* chimera_server_config_get_smb_compression */

SYMBOL_EXPORT void
chimera_server_config_set_smb_leases(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_leases = enabled;
} /* chimera_server_config_set_smb_leases */

SYMBOL_EXPORT int
chimera_server_config_get_smb_leases(const struct chimera_server_config *config)
{
    return config->smb_leases;
} /* chimera_server_config_get_smb_leases */

SYMBOL_EXPORT void
chimera_server_config_set_smb_oplocks(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_oplocks = enabled;
} /* chimera_server_config_set_smb_oplocks */

SYMBOL_EXPORT int
chimera_server_config_get_smb_oplocks(const struct chimera_server_config *config)
{
    return config->smb_oplocks;
} /* chimera_server_config_get_smb_oplocks */

SYMBOL_EXPORT void
chimera_server_config_set_smb_notify_disabled(
    struct chimera_server_config *config,
    int                           disabled)
{
    config->smb_notify_disabled = disabled;
} /* chimera_server_config_set_smb_notify_disabled */

SYMBOL_EXPORT int
chimera_server_config_get_smb_notify_disabled(const struct chimera_server_config *config)
{
    return config->smb_notify_disabled;
} /* chimera_server_config_get_smb_notify_disabled */

SYMBOL_EXPORT void
chimera_server_config_set_smb_acl_inherited_canonicalize(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_acl_inherited_canonicalize = enable;
} /* chimera_server_config_set_smb_acl_inherited_canonicalize */

SYMBOL_EXPORT int
chimera_server_config_get_smb_acl_inherited_canonicalize(const struct chimera_server_config *config)
{
    return config->smb_acl_inherited_canonicalize;
} /* chimera_server_config_get_smb_acl_inherited_canonicalize */

SYMBOL_EXPORT void
chimera_server_config_set_max_open_files(
    struct chimera_server_config *config,
    int                           open_files)
{
    config->max_open_files = (rlim_t) open_files;
} /* chimera_server_config_set_max_open_files */

SYMBOL_EXPORT void
chimera_server_config_set_external_portmap(
    struct chimera_server_config *config,
    int                           enable)
{
    config->external_portmap = enable;
} /* chimera_server_config_set_external_portmap */

SYMBOL_EXPORT void
chimera_server_config_set_portmap_hostname(
    struct chimera_server_config *config,
    const char                   *hostname)
{
    strncpy(config->portmap_hostname, hostname, sizeof(config->portmap_hostname) - 1);
    config->portmap_hostname[sizeof(config->portmap_hostname) - 1] = '\0';
} /* chimera_server_config_set_portmap_hostname */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs_rdma = enable;
} /* chimera_server_config_set_nfs_rdma */

SYMBOL_EXPORT void
chimera_server_config_set_cache_ttl(
    struct chimera_server_config *config,
    int                           ttl)
{
    config->cache_ttl = ttl;
} /* chimera_server_config_set_cache_ttl */

SYMBOL_EXPORT int
chimera_server_config_get_cache_ttl(const struct chimera_server_config *config)
{
    return config->cache_ttl;
} /* chimera_server_config_get_cache_ttl */

SYMBOL_EXPORT void
chimera_server_config_set_rcu_reclaim_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->rcu_reclaim_threads = threads;
} /* chimera_server_config_set_rcu_reclaim_threads */

SYMBOL_EXPORT void
chimera_server_config_set_nfs4_session_slots(
    struct chimera_server_config *config,
    int                           slots)
{
    config->nfs4_session_slots = slots;
} /* chimera_server_config_set_nfs4_session_slots */

SYMBOL_EXPORT int
chimera_server_config_get_nfs4_session_slots(const struct chimera_server_config *config)
{
    return config->nfs4_session_slots;
} /* chimera_server_config_get_nfs4_session_slots */

SYMBOL_EXPORT void
chimera_server_config_set_nfs4_delegations(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs4_delegations = enable;
} /* chimera_server_config_set_nfs4_delegations */

SYMBOL_EXPORT int
chimera_server_config_get_nfs4_delegations(const struct chimera_server_config *config)
{
    return config->nfs4_delegations;
} /* chimera_server_config_get_nfs4_delegations */

SYMBOL_EXPORT void
chimera_server_config_set_nfs4_lease_time(
    struct chimera_server_config *config,
    uint32_t                      seconds)
{
    config->nfs4_lease_time_s = seconds;
} /* chimera_server_config_set_nfs4_lease_time */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_nfs4_lease_time(const struct chimera_server_config *config)
{
    return config->nfs4_lease_time_s;
} /* chimera_server_config_get_nfs4_lease_time */

SYMBOL_EXPORT void
chimera_server_config_set_nfs4_grace_time(
    struct chimera_server_config *config,
    uint32_t                      seconds)
{
    config->nfs4_grace_time_s = seconds;
} /* chimera_server_config_set_nfs4_grace_time */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_nfs4_grace_time(const struct chimera_server_config *config)
{
    return config->nfs4_grace_time_s;
} /* chimera_server_config_get_nfs4_grace_time */

SYMBOL_EXPORT void
chimera_server_config_set_nfs4_courtesy_time(
    struct chimera_server_config *config,
    uint32_t                      seconds)
{
    config->nfs4_courtesy_time_s = seconds;
} /* chimera_server_config_set_nfs4_courtesy_time */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_nfs4_courtesy_time(const struct chimera_server_config *config)
{
    return config->nfs4_courtesy_time_s;
} /* chimera_server_config_get_nfs4_courtesy_time */

SYMBOL_EXPORT void
chimera_server_config_set_kv_module(
    struct chimera_server_config *config,
    const char                   *kv_module)
{
    strncpy(config->kv_module, kv_module, sizeof(config->kv_module) - 1);
    config->kv_module[sizeof(config->kv_module) - 1] = '\0';
} /* chimera_server_config_set_kv_module */

SYMBOL_EXPORT const char *
chimera_server_config_get_kv_module(const struct chimera_server_config *config)
{
    return config->kv_module;
} /* chimera_server_config_get_kv_module */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_rdma(const struct chimera_server_config *config)
{
    return config->nfs_rdma;
} /* chimera_server_config_get_nfs_rdma */

SYMBOL_EXPORT void
chimera_server_config_set_pnfs_enabled(
    struct chimera_server_config *config,
    int                           enable)
{
    config->pnfs_enabled = enable;
} /* chimera_server_config_set_pnfs_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_pnfs_enabled(const struct chimera_server_config *config)
{
    return config->pnfs_enabled;
} /* chimera_server_config_get_pnfs_enabled */

SYMBOL_EXPORT int
chimera_server_config_add_pnfs_ds(
    struct chimera_server_config *config,
    const char                   *netid,
    const char                   *uaddr,
    const char                   *rdma_uaddr,
    const char                   *backing_path,
    int                           version,
    int                           minorversion)
{
    struct chimera_server_config_pnfs_ds *ds;
    int                                   idx;

    if (config->pnfs_num_ds >= CHIMERA_PNFS_MAX_DS) {
        return -1;
    }

    idx = config->pnfs_num_ds++;
    ds  = &config->pnfs_ds[idx];

    snprintf(ds->netid, sizeof(ds->netid), "%s", netid ? netid : "tcp");
    snprintf(ds->uaddr, sizeof(ds->uaddr), "%s", uaddr ? uaddr : "");
    snprintf(ds->rdma_uaddr, sizeof(ds->rdma_uaddr), "%s", rdma_uaddr ? rdma_uaddr : "");
    snprintf(ds->backing_path, sizeof(ds->backing_path), "%s", backing_path ? backing_path : "");
    ds->version      = version ? version : 3;
    ds->minorversion = minorversion;

    return idx;
} /* chimera_server_config_add_pnfs_ds */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_port = port;
} /* chimera_server_config_set_nfs_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_port(const struct chimera_server_config *config)
{
    return config->nfs_port;
} /* chimera_server_config_get_nfs_port */

SYMBOL_EXPORT void
chimera_server_config_set_s3_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->s3_port = port;
} /* chimera_server_config_set_s3_port */

SYMBOL_EXPORT int
chimera_server_config_get_s3_port(const struct chimera_server_config *config)
{
    return config->s3_port;
} /* chimera_server_config_get_s3_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_data_server(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs_data_server = enable;
} /* chimera_server_config_set_nfs_data_server */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_data_server(const struct chimera_server_config *config)
{
    return config->nfs_data_server;
} /* chimera_server_config_get_nfs_data_server */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_server_scope(
    struct chimera_server_config *config,
    uint64_t                      scope)
{
    config->nfs_server_scope = scope;
} /* chimera_server_config_set_nfs_server_scope */

SYMBOL_EXPORT uint64_t
chimera_server_config_get_nfs_server_scope(const struct chimera_server_config *config)
{
    return config->nfs_server_scope;
} /* chimera_server_config_get_nfs_server_scope */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma_hostname(
    struct chimera_server_config *config,
    const char                   *hostname)
{
    config->nfs_rdma = 1;
    strncpy(config->nfs_rdma_hostname, hostname, sizeof(config->nfs_rdma_hostname) - 1);
} /* chimera_server_config_set_nfs_rdma_hostname */

SYMBOL_EXPORT const char *
chimera_server_config_get_nfs_rdma_hostname(const struct chimera_server_config *config)
{
    if (!config->nfs_rdma) {
        return NULL;
    }

    return config->nfs_rdma_hostname;
} /* chimera_server_config_get_nfs_rdma_hostname */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_rdma_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_rdma_port = port;
} /* chimera_server_config_set_nfs_rdma_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_rdma_port(const struct chimera_server_config *config)
{
    return config->nfs_rdma_port;
} /* chimera_server_config_get_nfs_rdma_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_tcp_rdma_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_tcp_rdma_port = port;
} /* chimera_server_config_set_nfs_tcp_rdma_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_tcp_rdma_port(const struct chimera_server_config *config)
{
    return config->nfs_tcp_rdma_port;
} /* chimera_server_config_get_nfs_tcp_rdma_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_lockmgr_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_lockmgr_port = port;
} /* chimera_server_config_set_nfs_lockmgr_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_lockmgr_port(const struct chimera_server_config *config)
{
    return config->nfs_lockmgr_port;
} /* chimera_server_config_get_nfs_lockmgr_port */

SYMBOL_EXPORT void
chimera_server_config_set_state_dir(
    struct chimera_server_config *config,
    const char                   *dir)
{
    snprintf(config->state_dir, sizeof(config->state_dir), "%s", dir);
} /* chimera_server_config_set_state_dir */

SYMBOL_EXPORT const char *
chimera_server_config_get_state_dir(const struct chimera_server_config *config)
{
    return config->state_dir;
} /* chimera_server_config_get_state_dir */

SYMBOL_EXPORT int
chimera_server_config_get_external_portmap(const struct chimera_server_config *config)
{
    return config->external_portmap;
} /* chimera_server_config_get_external_portmap */

SYMBOL_EXPORT const char *
chimera_server_config_get_portmap_hostname(const struct chimera_server_config *config)
{
    if (config->portmap_hostname[0] == '\0') {
        return NULL;
    }
    return config->portmap_hostname;
} /* chimera_server_config_get_portmap_hostname */

SYMBOL_EXPORT void
chimera_server_resolve_ipv4(
    const char *hostname,
    char       *out_buf,
    size_t      out_size)
{
    struct addrinfo  hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM };
    struct addrinfo *res;
    int              gai_rc;

    gai_rc = getaddrinfo(hostname, NULL, &hints, &res);
    chimera_server_fatal_if(gai_rc != 0,
                            "Failed to resolve hostname '%s': %s",
                            hostname, gai_strerror(gai_rc));

    inet_ntop(AF_INET,
              &((struct sockaddr_in *) res->ai_addr)->sin_addr,
              out_buf,
              out_size);

    if (res->ai_next != NULL) {
        chimera_server_info("Hostname '%s' resolved to multiple addresses; using %s",
                            hostname, out_buf);
    }

    freeaddrinfo(res);
} /* chimera_server_resolve_ipv4 */

SYMBOL_EXPORT void
chimera_server_config_add_module(
    struct chimera_server_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_data)
{
    struct chimera_vfs_module_cfg *module_cfg = NULL;
    int                            i;

    /* A module name maps to a single backend (one fh_magic), so configuring a
     * built-in module (e.g. memfs with ds_mode/block_size) must override its
     * default entry rather than register a duplicate -- a double registration
     * leaks the first instance's private state. */
    for (i = 0; i < config->num_modules; i++) {
        if (strcmp(config->modules[i].module_name, module_name) == 0) {
            module_cfg = &config->modules[i];
            break;
        }
    }

    if (!module_cfg) {
        module_cfg = &config->modules[config->num_modules++];
    }

    snprintf(module_cfg->module_name, sizeof(module_cfg->module_name), "%s", module_name);
    snprintf(module_cfg->config_data, sizeof(module_cfg->config_data), "%s", config_data);
    if (module_path) {
        snprintf(module_cfg->module_path, sizeof(module_cfg->module_path), "%s", module_path);
    } else {
        /* We don't specify a path for preloaded modules like diskfs */
        module_cfg->module_path[0] = '\0';
    }
} /* chimera_server_config_add_module */

SYMBOL_EXPORT void
chimera_server_config_set_metrics_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->metrics_port = port;
} /* chimera_server_config_set_metrics_port */

SYMBOL_EXPORT void
chimera_server_config_set_rest_http_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->rest_http_port = port;
} /* chimera_server_config_set_rest_http_port */

SYMBOL_EXPORT int
chimera_server_config_get_rest_http_port(const struct chimera_server_config *config)
{
    return config->rest_http_port;
} /* chimera_server_config_get_rest_http_port */

SYMBOL_EXPORT void
chimera_server_config_set_rest_debug_fsops(
    struct chimera_server_config *config,
    int                           enable)
{
    config->rest_debug_fsops = enable;
} /* chimera_server_config_set_rest_debug_fsops */

SYMBOL_EXPORT int
chimera_server_config_get_rest_debug_fsops(const struct chimera_server_config *config)
{
    return config->rest_debug_fsops;
} /* chimera_server_config_get_rest_debug_fsops */

SYMBOL_EXPORT void
chimera_server_config_set_rest_https_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->rest_https_port = port;
} /* chimera_server_config_set_rest_https_port */

SYMBOL_EXPORT int
chimera_server_config_get_rest_https_port(const struct chimera_server_config *config)
{
    return config->rest_https_port;
} /* chimera_server_config_get_rest_https_port */

SYMBOL_EXPORT void
chimera_server_config_set_rest_ssl_cert(
    struct chimera_server_config *config,
    const char                   *cert_path)
{
    strncpy(config->rest_ssl_cert, cert_path, sizeof(config->rest_ssl_cert) - 1);
    config->rest_ssl_cert[sizeof(config->rest_ssl_cert) - 1] = '\0';
} /* chimera_server_config_set_rest_ssl_cert */

SYMBOL_EXPORT const char *
chimera_server_config_get_rest_ssl_cert(const struct chimera_server_config *config)
{
    return config->rest_ssl_cert;
} /* chimera_server_config_get_rest_ssl_cert */

SYMBOL_EXPORT void
chimera_server_config_set_rest_ssl_key(
    struct chimera_server_config *config,
    const char                   *key_path)
{
    strncpy(config->rest_ssl_key, key_path, sizeof(config->rest_ssl_key) - 1);
    config->rest_ssl_key[sizeof(config->rest_ssl_key) - 1] = '\0';
} /* chimera_server_config_set_rest_ssl_key */

SYMBOL_EXPORT const char *
chimera_server_config_get_rest_ssl_key(const struct chimera_server_config *config)
{
    return config->rest_ssl_key;
} /* chimera_server_config_get_rest_ssl_key */

SYMBOL_EXPORT int
chimera_server_config_get_smb_num_dialects(const struct chimera_server_config *config)
{
    return config->smb_num_dialects;
} /* chimera_server_config_get_smb_num_dialects */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_smb_dialects(
    const struct chimera_server_config *config,
    int                                 index)
{
    return config->smb_dialects[index];
} /* chimera_server_config_get_smb_dialects */

SYMBOL_EXPORT void
chimera_server_config_set_smb_min_dialect(
    struct chimera_server_config *config,
    uint32_t                      min_dialect)
{
    /* Every SMB2/3 dialect chimera can speak, ascending.  The advertised set is
     * this list filtered to >= min_dialect, so lowering the floor (e.g. to
     * SMB 2.0.2) just widens the bottom of the range.  The default floor is
     * SMB 2.1; SMB 2.0.2 is off by default because it lacks large-MTU/leasing
     * and is only needed by conformance cases that explicitly request it. */
    static const uint32_t all_dialects[] = {
        SMB2_DIALECT_2_0_2,
        SMB2_DIALECT_2_1,
        SMB2_DIALECT_3_0,
        SMB2_DIALECT_3_0_2,
        SMB2_DIALECT_3_1_1,
    };
    int                   n = 0;

    for (unsigned int i = 0; i < sizeof(all_dialects) / sizeof(all_dialects[0]); i++) {
        if (all_dialects[i] >= min_dialect) {
            config->smb_dialects[n++] = all_dialects[i];
        }
    }

    config->smb_num_dialects = n;
} /* chimera_server_config_set_smb_min_dialect */

SYMBOL_EXPORT int
chimera_server_config_get_smb_num_nic_info(const struct chimera_server_config *config)
{
    return config->smb_num_nic_info;
} /* chimera_server_config_get_smb_num_nic_info */

SYMBOL_EXPORT const struct chimera_server_config_smb_nic *
chimera_server_config_get_smb_nic_info(
    const struct chimera_server_config *config,
    int                                 index)
{
    return &config->smb_nic_info[index];
} /* chimera_server_config_get_smb_nic_info */

SYMBOL_EXPORT void
chimera_server_config_set_smb_nic_info(
    struct chimera_server_config               *config,
    int                                         num_nic_info,
    const struct chimera_server_config_smb_nic *smb_nic_info)
{
    config->smb_num_nic_info = num_nic_info;
    memcpy(config->smb_nic_info, smb_nic_info, num_nic_info * sizeof(struct chimera_server_config_smb_nic));
} /* chimera_server_config_set_smb_nic_info */

SYMBOL_EXPORT void
chimera_server_config_set_anonuid(
    struct chimera_server_config *config,
    uint32_t                      anonuid)
{
    config->anonuid = anonuid;
} /* chimera_server_config_set_anonuid */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_anonuid(const struct chimera_server_config *config)
{
    return config->anonuid;
} /* chimera_server_config_get_anonuid */

SYMBOL_EXPORT void
chimera_server_config_set_anongid(
    struct chimera_server_config *config,
    uint32_t                      anongid)
{
    config->anongid = anongid;
} /* chimera_server_config_set_anongid */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_anongid(const struct chimera_server_config *config)
{
    return config->anongid;
} /* chimera_server_config_get_anongid */

SYMBOL_EXPORT void
chimera_server_config_set_soft_fail_bad_req(
    struct chimera_server_config *config,
    int                           enable)
{
    config->soft_fail_bad_req = enable;
} /* chimera_server_config_set_soft_fail_bad_req */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_soft_fail_bad_req(const struct chimera_server_config *config)
{
    return config->soft_fail_bad_req;
} /* chimera_server_config_get_soft_fail_bad_req */

SYMBOL_EXPORT void
chimera_server_config_set_tcp_flavor(
    struct chimera_server_config *config,
    enum chimera_tcp_flavor       flavor)
{
    config->tcp_flavor = flavor;
} /* chimera_server_config_set_tcp_flavor */

SYMBOL_EXPORT enum chimera_tcp_flavor
chimera_server_config_get_tcp_flavor(const struct chimera_server_config *config)
{
    return config->tcp_flavor;
} /* chimera_server_config_get_tcp_flavor */

SYMBOL_EXPORT enum evpl_protocol_id
chimera_server_config_get_tcp_stream_protocol(const struct chimera_server_config *config)
{
    return chimera_tcp_flavor_to_protocol(config->tcp_flavor);
} /* chimera_server_config_get_tcp_stream_protocol */

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_thread *thread = container_of(timer, struct chimera_thread, watchdog);

    chimera_vfs_watchdog(thread->vfs_thread);
} /* chimera_server_thread_wake */

static void *
chimera_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server *server = data;
    struct chimera_thread *thread;

    thread = calloc(1, sizeof(*thread));

    evpl_add_timer(evpl, &thread->watchdog, chimera_server_thread_wake, 1000000);

    thread->server = server;

    thread->vfs_thread = chimera_vfs_thread_init(evpl, server->vfs);

    for (int i = 0; i < server->num_protocols; i++) {
        thread->protocol_private[i] = server->protocols[i]->thread_init(evpl,
                                                                        thread->vfs_thread,
                                                                        server->
                                                                        protocol_private
                                                                        [i]);
    }

    thread->rest_thread = chimera_rest_thread_init(evpl, server->rest, thread->vfs_thread);

    pthread_mutex_lock(&server->lock);
    if (++server->threads_online == server->config->core_threads) {
        pthread_cond_signal(&server->all_threads_online);
    }
    pthread_mutex_unlock(&server->lock);

    return thread;
} /* chimera_server_thread_init */

struct mount_ctx {
    int done;
    int status;
};

static void
chimera_server_mount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->done   = 1;
    ctx->status = status;
} /* chimera_server_mount_callback */

SYMBOL_EXPORT int
chimera_server_mount(
    struct chimera_server *server,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path,
    const char            *options)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_mount(thread, NULL, mount_path, module_name, module_path, options, chimera_server_mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;

} /* chimera_server_create_share */

/*
 * Ensure a directory path exists inside a backend before it is mounted, for the
 * "create" mount option.  A backend's MOUNT op only walks the path read-only
 * (it returns ENOENT for a missing component), and creating a directory needs
 * an open handle to its parent -- which in turn needs the backend registered in
 * the mount table.  So: transiently mount the backend root, walk the target
 * path component by component creating any that are missing, unmount, and let
 * the caller perform the real mount of the now-existing path.
 *
 * Synchronous (init-time) wrapper around an async lookup-or-mkdir chain, driven
 * on a private evpl like chimera_server_mount.  Returns 0 on success.
 */
#define CHIMERA_MKPATH_TMP_NAME "__chimera_mkpath_tmp"

struct chimera_mkpath_ctx {
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    char                            path[256];
    int                             pathlen;
    int                             offset;
    uint32_t                        mode;
    struct chimera_vfs_open_handle *oh;      /* current parent directory */
    char                            comp[256];
    int                             complen;
    uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t                        child_fh_len;
    struct chimera_vfs_attrs        set_attr;
    enum chimera_vfs_error status;
    int                             done;
};

static void chimera_mkpath_next(
    struct chimera_mkpath_ctx *ctx);

static void
chimera_mkpath_umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    ctx->done = 1;
} /* chimera_mkpath_umount_cb */

/* Release the current handle and tear down the transient root mount, recording
 * the final walk status.  Both the success and failure paths funnel here. */
static void
chimera_mkpath_finish(
    struct chimera_mkpath_ctx *ctx,
    enum chimera_vfs_error     status)
{
    ctx->status = status;

    if (ctx->oh) {
        chimera_vfs_release(ctx->thread, ctx->oh);
        ctx->oh = NULL;
    }

    chimera_vfs_umount(ctx->thread, chimera_vfs_get_server_cred(),
                       CHIMERA_MKPATH_TMP_NAME, chimera_mkpath_umount_cb, ctx);
} /* chimera_mkpath_finish */

/* The just-resolved/created child becomes the new parent; descend. */
static void
chimera_mkpath_descend_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_mkpath_finish(ctx, error_code);
        return;
    }

    if (ctx->oh) {
        chimera_vfs_release(ctx->thread, ctx->oh);
    }
    ctx->oh = oh;
    chimera_mkpath_next(ctx);
} /* chimera_mkpath_descend_cb */

static void
chimera_mkpath_open_child(struct chimera_mkpath_ctx *ctx)
{
    chimera_vfs_open_fh(ctx->thread, chimera_vfs_get_server_cred(),
                        ctx->child_fh, ctx->child_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_mkpath_descend_cb, ctx);
} /* chimera_mkpath_open_child */

static void
chimera_mkpath_mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK || attr->va_fh_len == 0) {
        chimera_mkpath_finish(ctx, error_code != CHIMERA_VFS_OK ? error_code : CHIMERA_VFS_EIO);
        return;
    }

    memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
    ctx->child_fh_len = attr->va_fh_len;
    chimera_mkpath_open_child(ctx);
} /* chimera_mkpath_mkdir_cb */

static void
chimera_mkpath_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        /* Component already exists; descend into it. */
        if (attr->va_fh_len == 0) {
            chimera_mkpath_finish(ctx, CHIMERA_VFS_EIO);
            return;
        }
        memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
        ctx->child_fh_len = attr->va_fh_len;
        chimera_mkpath_open_child(ctx);
        return;
    }

    if (error_code == CHIMERA_VFS_ENOENT) {
        memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
        ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE |
            CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
        ctx->set_attr.va_mode = S_IFDIR | (ctx->mode & 07777);
        ctx->set_attr.va_uid  = 0;
        ctx->set_attr.va_gid  = 0;

        chimera_vfs_mkdir_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->oh,
                             ctx->comp, ctx->complen, &ctx->set_attr,
                             CHIMERA_VFS_ATTR_FH, 0, 0,
                             chimera_mkpath_mkdir_cb, ctx);
        return;
    }

    chimera_mkpath_finish(ctx, error_code);
} /* chimera_mkpath_lookup_cb */

/* Advance to the next '/'-separated component; when none remain the path is
 * fully present and we finish successfully. */
static void
chimera_mkpath_next(struct chimera_mkpath_ctx *ctx)
{
    int start;

    while (ctx->offset < ctx->pathlen && ctx->path[ctx->offset] == '/') {
        ctx->offset++;
    }

    if (ctx->offset >= ctx->pathlen) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    start = ctx->offset;
    while (ctx->offset < ctx->pathlen && ctx->path[ctx->offset] != '/') {
        ctx->offset++;
    }

    ctx->complen = ctx->offset - start;
    if (ctx->complen >= (int) sizeof(ctx->comp)) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_ENAMETOOLONG);
        return;
    }
    memcpy(ctx->comp, ctx->path + start, ctx->complen);
    ctx->comp[ctx->complen] = '\0';

    chimera_vfs_lookup_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->oh,
                          ctx->comp, ctx->complen, CHIMERA_VFS_ATTR_FH, 0,
                          chimera_mkpath_lookup_cb, ctx);
} /* chimera_mkpath_next */

static void
chimera_mkpath_root_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_mkpath_finish(ctx, error_code);
        return;
    }
    ctx->oh = oh;
    chimera_mkpath_next(ctx);
} /* chimera_mkpath_root_open_cb */

static void
chimera_mkpath_mounted_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;
    struct chimera_vfs_mount  *m;

    if (status != CHIMERA_VFS_OK) {
        /* Root mount failed -- nothing registered, nothing to unmount. */
        ctx->status = status;
        ctx->done   = 1;
        return;
    }

    m = chimera_vfs_mount_table_find_by_path(ctx->vfs->mount_table,
                                             CHIMERA_MKPATH_TMP_NAME,
                                             strlen(CHIMERA_MKPATH_TMP_NAME));
    if (!m) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    chimera_vfs_open_fh(thread, chimera_vfs_get_server_cred(),
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_mkpath_root_open_cb, ctx);
} /* chimera_mkpath_mounted_cb */

SYMBOL_EXPORT int
chimera_server_mkpath(
    struct chimera_server *server,
    const char            *module_name,
    const char            *module_path,
    uint32_t               mode)
{
    struct evpl              *evpl;
    struct chimera_mkpath_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));

    if (!module_name || !module_path ||
        strlen(module_path) >= sizeof(ctx.path)) {
        return -1;
    }

    evpl        = evpl_create(NULL);
    ctx.vfs     = server->vfs;
    ctx.thread  = chimera_vfs_thread_init(evpl, server->vfs);
    ctx.mode    = mode ? mode : 0755;
    ctx.status  = CHIMERA_VFS_OK;
    ctx.pathlen = snprintf(ctx.path, sizeof(ctx.path), "%s", module_path);

    /* Transiently mount the backend root so we have a handle to walk under. */
    chimera_vfs_mount(ctx.thread, chimera_vfs_get_server_cred(),
                      CHIMERA_MKPATH_TMP_NAME, module_name, "/", NULL,
                      chimera_mkpath_mounted_cb, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx.thread);
    evpl_destroy(evpl);

    return ctx.status == CHIMERA_VFS_OK ? 0 : -1;
} /* chimera_server_mkpath */

static void
chimera_server_umount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->done   = 1;
    ctx->status = status;
} /* chimera_server_umount_callback */

SYMBOL_EXPORT int
chimera_server_unmount(
    struct chimera_server *server,
    const char            *mount_path)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_umount(thread, NULL, mount_path, chimera_server_umount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;
} /* chimera_server_unmount */

SYMBOL_EXPORT int
chimera_server_pnfs_resolve(struct chimera_server *server)
{
    struct chimera_vfs *vfs = server->vfs;
    int                 n   = chimera_vfs_pnfs_num_devices(vfs);
    int                 i, resolved = 0;

    for (i = 0; i < n; i++) {
        struct chimera_vfs_ds    *ds = chimera_vfs_pnfs_get_device(vfs, i);
        struct chimera_vfs_mount *m;
        const char               *bpath = ds->backing_path;

        /* Mounts are registered with leading slashes stripped (see
         * chimera_vfs_mount), so normalize the configured backing path the same
         * way -- otherwise "/ds0" only prefix-matches the empty-path root. */
        while (*bpath == '/') {
            bpath++;
        }

        m = chimera_vfs_mount_table_find_by_path(vfs->mount_table,
                                                 bpath,
                                                 strlen(bpath));
        if (!m) {
            chimera_server_error(
                "pNFS data server %d: backing mount '%s' not found (mount it via the nfs module)",
                i, ds->backing_path);
            continue;
        }

        chimera_vfs_pnfs_set_device_root(vfs, i, m->root_fh, m->root_fh_len);

        /* If the backing mount is not the nfs proxy, the data server is local
         * to this node: this server itself serves the backing handle, so the
         * handle handed to the client is the backing handle as-is (no proxy
         * wrapper to strip).  See chimera_nfs4_encode_ff_layout callers. */
        ds->backing_local = (m->module &&
                             m->module->fh_magic != CHIMERA_VFS_FH_MAGIC_NFS) ? 1 : 0;

        chimera_server_info(
            "pNFS data server %d backing root resolved via '%s' (module=%s path=%s root_fh_len=%d local=%d)",
            i, ds->backing_path,
            m->module ? m->module->name : "?",
            m->path ? m->path : "?", m->root_fh_len, ds->backing_local);
        resolved++;
    }

    return (resolved == n) ? 0 : -1;
} /* chimera_server_pnfs_resolve */

SYMBOL_EXPORT int
chimera_server_create_bucket(
    struct chimera_server *server,
    const char            *bucket_name,
    const char            *bucket_path)
{
    if (!server->s3_shared) {
        return -1;
    }

    chimera_s3_add_bucket(server->s3_shared, bucket_name, bucket_path);

    return 0;
} /* chimera_server_create_bucket */

SYMBOL_EXPORT int
chimera_server_set_s3_bucket_root(
    struct chimera_server *server,
    const char            *bucket_root_path)
{
    if (!server->s3_shared) {
        return -1;
    }

    chimera_s3_set_bucket_root(server->s3_shared, bucket_root_path);

    return 0;
} /* chimera_server_set_s3_bucket_root */

SYMBOL_EXPORT int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path,
    int                    continuous_availability)
{
    if (!server->smb_shared) {
        return -1;
    }

    chimera_smb_add_share(server->smb_shared, share_name, share_path,
                          continuous_availability);

    return 0;
} /* chimera_server_create_share */

SYMBOL_EXPORT int
chimera_server_share_set_access_based_enum(
    struct chimera_server *server,
    const char            *share_name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_share_set_access_based_enum(server->smb_shared,
                                                   share_name);
} /* chimera_server_share_set_access_based_enum */

SYMBOL_EXPORT int
chimera_server_share_set_encrypt_data(
    struct chimera_server *server,
    const char            *share_name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_share_set_encrypt_data(server->smb_shared,
                                              share_name);
} /* chimera_server_share_set_encrypt_data */

SYMBOL_EXPORT int
chimera_server_create_export(
    struct chimera_server *server,
    const char            *name,
    const char            *path)
{
    if (!server->nfs_shared) {
        return -1;
    }

    chimera_nfs_add_export(server->nfs_shared, name, path);

    return 0;
} /* chimera_server_create_export */

static void
chimera_server_thread_shutdown(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    /* Drain VFS thread first to ensure all in-flight operations complete
     * before we destroy the protocol threads (NFS/RPC2 connections).
     * This prevents use-after-free when VFS callbacks try to send RPC replies
     * on already-destroyed connections. */
    chimera_vfs_thread_drain(thread->vfs_thread);

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->thread_destroy(thread->protocol_private[i]);
    }

    chimera_rest_thread_destroy(thread->rest_thread);

    evpl_remove_timer(evpl, &thread->watchdog);
    chimera_vfs_thread_destroy(thread->vfs_thread);
    free(thread);
} /* chimera_server_thread_shutdown */

SYMBOL_EXPORT struct chimera_server *
chimera_server_init(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics)
{
    struct chimera_server *server;
    int                    i;
    struct rlimit          rl;

    if (!config) {
        config = chimera_server_config_init();
    }

    chimera_log_init();

    /* Need to set the filedescriptor limits */
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        if (rl.rlim_cur < config->max_open_files) {
            rl.rlim_cur = config->max_open_files;
            if (rl.rlim_cur > rl.rlim_max) {
                rl.rlim_max = rl.rlim_cur;
            }
            if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
                chimera_server_error("Failed to set file descriptor limit to %ld: %s",
                                     rl.rlim_cur, strerror(errno));
            } else {
                chimera_server_info("Setting file descriptor limit to %ld", rl.rlim_cur);
            }
        } else {
            chimera_server_info("File descriptor limit is sufficient: %ld", rl.rlim_cur);
        }
    } else {
        chimera_server_error("Failed to get file descriptor limit: %s", strerror(errno));
    }

    server = calloc(1, sizeof(*server));

    server->config = config;

    pthread_mutex_init(&server->lock, NULL);
    pthread_cond_init(&server->all_threads_online, NULL);

    chimera_server_info("Initializing VFS...");
    server->vfs = chimera_vfs_init(config->sync_delegation ? config->sync_delegation_threads : 0,
                                   config->async_delegation ? config->async_delegation_threads : 0,
                                   config->modules,
                                   config->num_modules,
                                   config->kv_module,
                                   config->cache_ttl,
                                   config->rcu_reclaim_threads,
                                   metrics);

    /* Propagate the common TCP flavor so VFS client modules (e.g. nfs)
     * open outbound connections with the same transport. */
    chimera_vfs_set_tcp_flavor(server->vfs, config->tcp_flavor);

    /* Enable the pNFS feature whenever configured.  Orchestrated flex-files
     * needs a data-server table (below); a layout-sourcing backend (e.g. diskfs
     * block mode) produces its own layouts and needs no data servers, so the
     * feature is enabled with an empty table. */
    if (config->pnfs_enabled) {
        chimera_vfs_pnfs_set_enabled(server->vfs, 1);
        for (i = 0; i < config->pnfs_num_ds; i++) {
            chimera_vfs_pnfs_add_device(server->vfs,
                                        config->pnfs_ds[i].netid,
                                        config->pnfs_ds[i].uaddr,
                                        config->pnfs_ds[i].rdma_uaddr,
                                        config->pnfs_ds[i].backing_path,
                                        config->pnfs_ds[i].version,
                                        config->pnfs_ds[i].minorversion);
        }
        chimera_server_info("pNFS enabled with %d data server(s)", config->pnfs_num_ds);
    }

    chimera_server_info("Initializing protocols...");
    server->protocols[server->num_protocols++] = &nfs_protocol;

    /* A pNFS data server speaks only NFS; skipping SMB/S3 also frees their
     * fixed listen ports so it can share a host with the metadata server. */
    if (!config->nfs_data_server) {
        server->protocols[server->num_protocols++] = &smb_protocol;
        server->protocols[server->num_protocols++] = &s3_protocol;
    }

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(config, server->vfs, metrics);
    }

    server->nfs_shared = server->protocol_private[0];
    if (!config->nfs_data_server) {
        server->smb_shared = server->protocol_private[1];
        server->s3_shared  = server->protocol_private[2];
    }

    chimera_server_info("Initializing REST API...");
    server->rest = chimera_rest_init(config, server, server->vfs, metrics);

    return server;
} /* chimera_server_init */

SYMBOL_EXPORT void
chimera_server_start(struct chimera_server *server)
{
    int i;

    server->pool = evpl_threadpool_create(NULL,
                                          server->config->core_threads,
                                          chimera_server_thread_init,
                                          chimera_server_thread_shutdown,
                                          server);

    chimera_server_info("Waiting for %d threads to start...", server->config->core_threads);

    pthread_mutex_lock(&server->lock);
    while (server->threads_online < server->config->core_threads) {
        pthread_cond_wait(&server->all_threads_online, &server->lock);
    }
    pthread_mutex_unlock(&server->lock);

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->start(server->protocol_private[i]);
    }

    chimera_rest_start(server->rest);

    chimera_server_info("Server is ready.");
} /* chimera_server_start */

SYMBOL_EXPORT void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->stop(server->protocol_private[i]);
    }

    chimera_rest_stop(server->rest);

    evpl_threadpool_destroy(server->pool);

    /* Destroy protocols before VFS so they can release any open handles */
    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    chimera_vfs_destroy(server->vfs);

    chimera_rest_destroy(server->rest);

    free((void *) server->config);
    free(server);
} /* chimera_server_destroy */

SYMBOL_EXPORT int
chimera_server_add_user(
    struct chimera_server *server,
    const char            *username,
    const char            *password,
    const char            *smbpasswd,
    const char            *sid,
    uint32_t               uid,
    uint32_t               gid,
    uint32_t               ngids,
    const uint32_t        *gids,
    int                    pinned)
{
    return chimera_vfs_add_user(server->vfs, username, password, smbpasswd, sid,
                                uid, gid, ngids, gids, pinned);
} /* chimera_server_add_user */

SYMBOL_EXPORT int
chimera_server_remove_user(
    struct chimera_server *server,
    const char            *username)
{
    return chimera_vfs_remove_user(server->vfs, username);
} /* chimera_server_remove_user */

SYMBOL_EXPORT const struct chimera_vfs_user *
chimera_server_get_user(
    struct chimera_server *server,
    const char            *username)
{
    return chimera_vfs_lookup_user_by_name(server->vfs, username);
} /* chimera_server_get_user */

SYMBOL_EXPORT void
chimera_server_iterate_users(
    struct chimera_server         *server,
    chimera_server_user_iterate_cb callback,
    void                          *data)
{
    chimera_vfs_iterate_builtin_users(server->vfs, callback, data);
} /* chimera_server_iterate_users */

SYMBOL_EXPORT int
chimera_server_remove_export(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->nfs_shared) {
        return -1;
    }

    return chimera_nfs_remove_export(server->nfs_shared, name);
} /* chimera_server_remove_export */

SYMBOL_EXPORT const struct chimera_nfs_export *
chimera_server_get_export(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->nfs_shared) {
        return NULL;
    }

    return chimera_nfs_get_export(server->nfs_shared, name);
} /* chimera_server_get_export */

SYMBOL_EXPORT void
chimera_server_iterate_exports(
    struct chimera_server           *server,
    chimera_server_export_iterate_cb callback,
    void                            *data)
{
    if (!server->nfs_shared) {
        return;
    }

    chimera_nfs_iterate_exports(server->nfs_shared, callback, data);
} /* chimera_server_iterate_exports */

SYMBOL_EXPORT int
chimera_server_remove_share(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_remove_share(server->smb_shared, name);
} /* chimera_server_remove_share */

SYMBOL_EXPORT const struct chimera_smb_share *
chimera_server_get_share(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->smb_shared) {
        return NULL;
    }

    return chimera_smb_get_share(server->smb_shared, name);
} /* chimera_server_get_share */

SYMBOL_EXPORT void
chimera_server_iterate_shares(
    struct chimera_server          *server,
    chimera_server_share_iterate_cb callback,
    void                           *data)
{
    if (!server->smb_shared) {
        return;
    }

    chimera_smb_iterate_shares(server->smb_shared, callback, data);
} /* chimera_server_iterate_shares */

SYMBOL_EXPORT int
chimera_server_remove_bucket(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->s3_shared) {
        return -1;
    }

    return chimera_s3_remove_bucket(server->s3_shared, name);
} /* chimera_server_remove_bucket */

SYMBOL_EXPORT const struct s3_bucket *
chimera_server_get_bucket(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->s3_shared) {
        return NULL;
    }

    return chimera_s3_get_bucket(server->s3_shared, name);
} /* chimera_server_get_bucket */

SYMBOL_EXPORT void
chimera_server_release_bucket(struct chimera_server *server)
{
    if (!server->s3_shared) {
        return;
    }

    chimera_s3_release_bucket(server->s3_shared);
} /* chimera_server_release_bucket */

SYMBOL_EXPORT void
chimera_server_iterate_buckets(
    struct chimera_server           *server,
    chimera_server_bucket_iterate_cb callback,
    void                            *data)
{
    if (!server->s3_shared) {
        return;
    }

    chimera_s3_iterate_buckets(server->s3_shared, callback, data);
} /* chimera_server_iterate_buckets */

struct mount_iterate_ctx {
    chimera_server_mount_iterate_cb callback;
    void                           *data;
};

static int
mount_iterate_wrapper(
    struct chimera_vfs_mount *mount,
    void                     *data)
{
    struct mount_iterate_ctx *ctx = data;

    return ctx->callback(mount->path,
                         mount->module ? mount->module->name : "",
                         mount->module_path ? mount->module_path : "",
                         ctx->data);
} /* mount_iterate_wrapper */

SYMBOL_EXPORT void
chimera_server_iterate_mounts(
    struct chimera_server          *server,
    chimera_server_mount_iterate_cb callback,
    void                           *data)
{
    struct mount_iterate_ctx ctx = { .callback = callback, .data = data };

    chimera_vfs_mount_table_foreach(server->vfs->mount_table,
                                    mount_iterate_wrapper, &ctx);
} /* chimera_server_iterate_mounts */

struct mount_in_use_ctx {
    struct chimera_vfs *vfs;
    const char         *target;
    int                 target_len;
    int                 in_use;
};

/*
 * Resolve the path of a share/export/bucket to its owning mount and flag a
 * match against the target mount.  Paths are normalized (leading slashes
 * stripped) the same way chimera_vfs_mount registers them.  Returns non-zero
 * once a match is found to stop the enclosing iteration early.
 */
static int
mount_in_use_check_path(
    struct mount_in_use_ctx *ctx,
    const char              *path)
{
    struct chimera_vfs_mount *mount;

    while (*path == '/') {
        path++;
    }

    mount = chimera_vfs_mount_table_find_by_path(ctx->vfs->mount_table,
                                                 path, strlen(path));

    if (mount &&
        mount->pathlen == (uint32_t) ctx->target_len &&
        memcmp(mount->path, ctx->target, ctx->target_len) == 0) {
        ctx->in_use = 1;
        return 1;
    }

    return 0;
} /* mount_in_use_check_path */

static int
mount_in_use_share_cb(
    const struct chimera_smb_share *share,
    void                           *data)
{
    return mount_in_use_check_path(data, chimera_smb_share_get_path(share));
} /* mount_in_use_share_cb */

static int
mount_in_use_export_cb(
    const struct chimera_nfs_export *export,
    void                            *data)
{
    return mount_in_use_check_path(data, chimera_nfs_export_get_path(export));
} /* mount_in_use_export_cb */

static int
mount_in_use_bucket_cb(
    const struct s3_bucket *bucket,
    void                   *data)
{
    return mount_in_use_check_path(data, chimera_s3_bucket_get_path(bucket));
} /* mount_in_use_bucket_cb */

SYMBOL_EXPORT int
chimera_server_mount_in_use(
    struct chimera_server *server,
    const char            *mount_path)
{
    struct mount_in_use_ctx ctx;
    const char             *target = mount_path;

    while (*target == '/') {
        target++;
    }

    ctx.vfs        = server->vfs;
    ctx.target     = target;
    ctx.target_len = strlen(target);
    ctx.in_use     = 0;

    if (server->smb_shared) {
        chimera_smb_iterate_shares(server->smb_shared, mount_in_use_share_cb, &ctx);
    }

    if (!ctx.in_use && server->nfs_shared) {
        chimera_nfs_iterate_exports(server->nfs_shared, mount_in_use_export_cb, &ctx);
    }

    if (!ctx.in_use && server->s3_shared) {
        chimera_s3_iterate_buckets(server->s3_shared, mount_in_use_bucket_cb, &ctx);
    }

    return ctx.in_use;
} /* chimera_server_mount_in_use */

SYMBOL_EXPORT struct chimera_vfs *
chimera_server_get_vfs(struct chimera_server *server)
{
    return server->vfs;
} /* chimera_server_get_vfs */

SYMBOL_EXPORT int
chimera_server_add_s3_cred(
    struct chimera_server *server,
    const char            *access_key,
    const char            *secret_key,
    int                    pinned)
{
    if (!server->s3_shared) {
        return -1;
    }

    return chimera_s3_add_cred(server->s3_shared, access_key, secret_key, pinned);
} /* chimera_server_add_s3_cred */

SYMBOL_EXPORT void
chimera_server_config_set_smb_winbind_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_auth.winbind_enabled = enabled;
} /* chimera_server_config_set_smb_winbind_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_smb_winbind_enabled(const struct chimera_server_config *config)
{
    return config->smb_auth.winbind_enabled;
} /* chimera_server_config_get_smb_winbind_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_smb_winbind_domain(
    struct chimera_server_config *config,
    const char                   *domain)
{
    strncpy(config->smb_auth.winbind_domain, domain,
            sizeof(config->smb_auth.winbind_domain) - 1);
} /* chimera_server_config_set_smb_winbind_domain */

SYMBOL_EXPORT const char *
chimera_server_config_get_smb_winbind_domain(const struct chimera_server_config *config)
{
    return config->smb_auth.winbind_domain;
} /* chimera_server_config_get_smb_winbind_domain */

SYMBOL_EXPORT void
chimera_server_config_set_smb_kerberos_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_auth.kerberos_enabled = enabled;
} /* chimera_server_config_set_smb_kerberos_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_smb_kerberos_enabled(const struct chimera_server_config *config)
{
    return config->smb_auth.kerberos_enabled;
} /* chimera_server_config_get_smb_kerberos_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_smb_kerberos_keytab(
    struct chimera_server_config *config,
    const char                   *keytab)
{
    strncpy(config->smb_auth.kerberos_keytab, keytab,
            sizeof(config->smb_auth.kerberos_keytab) - 1);
} /* chimera_server_config_set_smb_kerberos_keytab */

SYMBOL_EXPORT const char *
chimera_server_config_get_smb_kerberos_keytab(const struct chimera_server_config *config)
{
    return config->smb_auth.kerberos_keytab;
} /* chimera_server_config_get_smb_kerberos_keytab */

SYMBOL_EXPORT void
chimera_server_config_set_smb_kerberos_realm(
    struct chimera_server_config *config,
    const char                   *realm)
{
    strncpy(config->smb_auth.kerberos_realm, realm,
            sizeof(config->smb_auth.kerberos_realm) - 1);
} /* chimera_server_config_set_smb_kerberos_realm */

SYMBOL_EXPORT const char *
chimera_server_config_get_smb_kerberos_realm(const struct chimera_server_config *config)
{
    return config->smb_auth.kerberos_realm;
} /* chimera_server_config_get_smb_kerberos_realm */
