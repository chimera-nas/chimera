// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only


/* The server configuration: its defaults, getters and setters.  It sits
 * below the server and the protocol servers, which read it, so the
 * protocols depend on it rather than on the server itself. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include "common/thread.h"
#ifndef _WIN32
#include <sys/resource.h>
#endif /* ifndef _WIN32 */
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <sys/socket.h>
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <netdb.h>
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <arpa/inet.h>
#endif /* ifdef _WIN32 */
#include <errno.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "nfs/nfs4_lease.h"
#include "s3/s3.h"
#include "smb/smb.h"
#ifdef __linux__
#include "fuse/fuse.h"
#endif /* ifdef __linux__ */
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_release.h"
#include "common/macros.h"
#include "common/chimera_tracing.h"
#include "server/server.h"
#include "common/common_config.h"
#include "smb_common/smb2.h"
#include "rest/rest.h"
#include "server_config_internal.h"

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
    config->rest_auth_enabled        = 1;
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

    /* SMB3 directory leases are off by default; they are an opt-in feature
     * gated by the "smb_directory_leases" config flag.  When enabled the server
     * advertises SMB2_GLOBAL_CAP_DIRECTORY_LEASING and grants R/H leases on
     * directory opens (SMB 3.0+, RqLs v2 only). */
    config->smb_directory_leases = 0;

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

    /* Emit POSIX mode as an S-1-5-88-3 "modefromsid" ACE on QUERY SECURITY so a
     * POSIX-semantics (CIFS-style) client reads the exact mode back, instead of
     * the Windows ACL memfs synthesizes from it.  Off by default (Windows
     * clients and the ACL conformance suites want the translated ACL); the
     * POSIX-over-SMB loopback enables it. */
    config->smb_mode_from_sid = 0;
    config->smb_posix_rename  = 0;

    /* How a replayed durable-v2 CREATE is answered when its create_guid matches
     * an open whose own CREATE has not completed yet (it is deferred on a
     * conflicting holder's oplock/lease break).  MS-SMB2 3.3.5.9 / 3.3.5.9.10
     * do not cover this race -- the spec's replay lookup assumes a fully
     * initialised Open -- so the two real implementations diverge:
     *
     *   0 (default): STATUS_FILE_NOT_AVAILABLE, as Samba answers.  A client
     *      RETRIES on this status (three times, then every 5s until the
     *      resiliency timeout), so the replay succeeds once the original create
     *      completes -- which is the point of replaying it after a channel
     *      failure.  smb2.replay.dhv2-pending*-sane assert this.
     *   1: STATUS_ACCESS_DENIED, as Windows servers answer (it falls out of
     *      the "Open.DurableOwner is NULL" rule in 3.3.5.9, DurableOwner not
     *      being set until the open completes).  A client reports this to the
     *      application rather than retrying.  Also suppresses replay detection
     *      entirely while the original create is deferred on a SHARE conflict,
     *      which is the other half of the Windows profile.
     *      smb2.replay.dhv2-pending*-windows assert this.
     *
     * The two profiles are mutually exclusive on identical wire traffic (the
     * -sane and -windows smbtorture variants send byte-identical sequences and
     * differ only in the status they require), so one daemon satisfies one of
     * them: the smbtorture driver runs each half against a daemon in the matching
     * profile.  See https://bugzilla.samba.org/show_bug.cgi?id=14449. */
    config->smb_replay_pending_windows = 0;

    /* Per-connection ceiling on outstanding async (STATUS_PENDING) operations.
     * 512 matches the value smb2.credits.*_ipc_max_async_credits asserts. */
    config->smb2_max_async_credits = 512;

    /* FileFsSectorSizeInformation defaults: report 4KiB physical sectors and
     * aligned + partition-aligned flags for modern storage.  Per MS-FSCC
     * 2.5.8 those two flags are bits 0 and 1; every higher bit is reserved. */
    config->smb_fs_physical_bytes_per_sector = 4096;
    config->smb_fs_sector_size_flags         = SMB2_SSINFO_FLAGS_ALIGNED_DEVICE |
        SMB2_SSINFO_FLAGS_PARTITION_ALIGNED_ON_DEVICE;

    // SMB auth config defaults - local NTLM only
    config->smb_auth.winbind_enabled             = 0;
    config->smb_auth.kerberos_enabled            = 0;
    config->smb_auth.kerberos_anonymous_fallback = 0;
    config->smb_auth.winbind_domain[0]           = '\0';
    config->smb_auth.kerberos_keytab[0]          = '\0';
    config->smb_auth.kerberos_realm[0]           = '\0';
    config->nfs_auth.kerberos_enabled            = 0;
    config->nfs_auth.kerberos_keytab[0]          = '\0';
    config->nfs_auth.num_principal_map           = 0;

    config->anonuid = 65534;
    config->anongid = 65534;

    /* Concurrent NFS export count cap; distinct from the export id space
     * (1..CHIMERA_NFS_EXPORT_ID_MAX), which is fixed by the 16-bit wire
     * file-handle field regardless of this cap. */
    config->nfs_max_exports = CHIMERA_NFS_MAX_EXPORTS_DEFAULT;

    /* Sign NFS wire file handles by default (unforgeable handles); the key is
     * generated/persisted at NFS init unless nfs_fh_key is configured. */
    config->nfs_fh_sign   = 1;
    config->nfs_fh_key[0] = '\0';

    /* pNFS layouts are disabled by default. */
    config->pnfs_enabled = 0;
    config->pnfs_num_ds  = 0;

    /* Every protocol is opt-in: a server serves nothing until its config
     * explicitly enables a protocol, so an instance brought up for one
     * purpose (a test fixture, a data server, an admin-only daemon) never
     * surprise-binds the others' well-known ports.  The port fields keep the
     * customary defaults and take effect only once the protocol is enabled. */
    config->nfs_enabled  = 0;
    config->smb_enabled  = 0;
    config->s3_enabled   = 0;
    config->fuse_enabled = 0;

    /* FUSE-over-io_uring whenever the kernel offers it. */
    config->fuse_io_uring = 1;

    /* NFS service port (default 2049); data-server mode binds only the NFSv4
     * service so a pNFS data server can coexist with an MDS on one host. */
    config->nfs_port        = 2049;
    config->s3_port         = 5000;
    config->s3_anon_uid     = CHIMERA_S3_ANON_UID;
    config->s3_anon_gid     = CHIMERA_S3_ANON_GID;
    config->smb_port        = 445;
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

    /* The VFS attribute cache is on by default (common.attr_cache). */
    config->attr_cache_enabled = 1;
    config->umount_timeout_ms  = CHIMERA_COMMON_UMOUNT_TIMEOUT_MS_DEFAULT;

    /* The VFS name (lookup) cache is on by default (common.name_cache). */
    config->name_cache_enabled = 1;

    /* Number of liburcu call_rcu reclaim worker threads.  0 means one worker
     * per CPU (create_all_cpu_call_rcu_data) to keep RCU reclaim up with
     * per-request cache churn under heavy load.  On a many-core host that
     * spawns hundreds of threads per server instance, which is wasteful for
     * short-lived or lightly-loaded instances (memory, process-teardown cost)
     * AND is a correctness hazard: liburcu's call_rcu_data_init does NOT retry
     * evpl_native_thread_create and abort()s the whole process on a transient EAGAIN
     * ("Unrecoverable error: Resource temporarily unavailable").  Spawning
     * hundreds of RCU threads at once -- as many in-process server instances do
     * in parallel under a -j CI run -- makes that EAGAIN abort likely, which is
     * the root cause of the intermittent lockf/fcntl NFS3 test flakes.  Default
     * to a small cap (matching the chimera client default) so the common case
     * stays well clear of the thread-exhaustion abort; a real server with a
     * heavy workload can raise it via common.rcu_reclaim_threads.  Without
     * liburcu there is no such pool and the setting is inert. */
    config->rcu_reclaim_threads = 4;

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
    config->nfs4_delegations = 0;

    /* Persistent reply cache (DRC) is disabled by default.  When on, the
     * NFSv4.1 per-session reply cache is written through to the KV store and
     * the server advertises CREATE_SESSION4_FLAG_PERSIST, so a client's
     * retransmit of a non-idempotent op replays its cached reply across a
     * server restart.  Recovery-record persistence is independent and always
     * on. */
    config->nfs4_drc = 0;
    /* NFSv3 duplicate-request cache.  Default off.  When enabled, the reply of
     * a non-idempotent NFSv3 op is captured and (if kv_module is persistent)
     * written through to the KV store keyed by {client, xid, proc, checksum},
     * so a retransmit after a server restart replays the cached reply instead
     * of re-executing.  There is no session and no on-wire advertisement -- the
     * cache is transparent to the client. */
    config->nfs3_drc = 0;
    /* 0 = auto-derive a stable node_id from the machine name; operators sharing
     * one KV store across instances should set distinct values explicitly. */
    config->nfs4_node_id         = 0;
    config->nfs4_lease_time_s    = NFS4_LEASE_TIME_DEFAULT_S;
    config->nfs4_grace_time_s    = NFS4_GRACE_TIME_DEFAULT_S;
    config->nfs4_courtesy_time_s = NFS4_COURTESY_TIME_DEFAULT_S;

    strncpy(config->nfs_rdma_hostname, "0.0.0.0", sizeof(config->nfs_rdma_hostname));
    config->nfs_rdma_port    = 20049;
    config->nfs_lockmgr_port = 32803;
    config->nfs_nsm_port     = 32765;
    /* The well-known MOUNT and portmap numbers (NFS_MOUNT_PORT /
     * NFS_PORTMAP_PORT in nfs_external_portmap.h), spelled out here like the
     * two ports above rather than pulling an NFS header into this file. */
    config->nfs_mount_port   = 20048;
    config->nfs_portmap_port = 111;

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

    config->num_modules = 3;

    /* The passthrough backends below are Linux-only (see src/vfs/CMakeLists.txt)
     * and are not built elsewhere, so do not ask the VFS to load them there --
     * a module that cannot be loaded is fatal at init.  Indices follow
     * num_modules so the list stays contiguous whichever ones are present. */
#ifdef __linux__
    strncpy(config->modules[config->num_modules].module_name, "linux",
            sizeof(config->modules[config->num_modules].module_name));
    config->modules[config->num_modules].config_data[0] = '\0';
    config->modules[config->num_modules].module_path[0] = '\0';

    config->num_modules++;
#endif /* ifdef __linux__ */

#ifdef HAVE_IO_URING
    strncpy(config->modules[config->num_modules].module_name, "io_uring",
            sizeof(config->modules[config->num_modules].module_name));
    config->modules[config->num_modules].config_data[0] = '\0';
    config->modules[config->num_modules].module_path[0] = '\0';

    config->num_modules++;
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
chimera_server_config_set_smb_directory_leases(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_directory_leases = enable;
} /* chimera_server_config_set_smb_directory_leases */

SYMBOL_EXPORT int
chimera_server_config_get_smb_directory_leases(const struct chimera_server_config *config)
{
    return config->smb_directory_leases;
} /* chimera_server_config_get_smb_directory_leases */

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

/* Protocol-neutral view of the named-streams knob.  Named streams are one VFS
* feature exposed by two protocols -- SMB alternate data streams and NFSv4 named
* attributes -- so both gate on the same switch (originally named for SMB). */
SYMBOL_EXPORT int
chimera_server_config_get_named_streams(const struct chimera_server_config *config)
{
    return config->smb_named_streams;
} /* chimera_server_config_get_named_streams */

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
chimera_server_config_set_smb_mode_from_sid(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_mode_from_sid = enable;
} /* chimera_server_config_set_smb_mode_from_sid */

SYMBOL_EXPORT int
chimera_server_config_get_smb_mode_from_sid(const struct chimera_server_config *config)
{
    return config->smb_mode_from_sid;
} /* chimera_server_config_get_smb_mode_from_sid */

SYMBOL_EXPORT void
chimera_server_config_set_smb_posix_rename(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_posix_rename = enable;
} /* chimera_server_config_set_smb_posix_rename */

SYMBOL_EXPORT int
chimera_server_config_get_smb_posix_rename(const struct chimera_server_config *config)
{
    return config->smb_posix_rename;
} /* chimera_server_config_get_smb_posix_rename */

SYMBOL_EXPORT void
chimera_server_config_set_smb_replay_pending_windows(
    struct chimera_server_config *config,
    int                           enable)
{
    config->smb_replay_pending_windows = enable;
} /* chimera_server_config_set_smb_replay_pending_windows */

SYMBOL_EXPORT int
chimera_server_config_get_smb_replay_pending_windows(const struct chimera_server_config *config)
{
    return config->smb_replay_pending_windows;
} /* chimera_server_config_get_smb_replay_pending_windows */

SYMBOL_EXPORT void
chimera_server_config_set_smb2_max_async_credits(
    struct chimera_server_config *config,
    int                           value)
{
    config->smb2_max_async_credits = value;
} /* chimera_server_config_set_smb2_max_async_credits */

SYMBOL_EXPORT int
chimera_server_config_get_smb2_max_async_credits(const struct chimera_server_config *config)
{
    return config->smb2_max_async_credits;
} /* chimera_server_config_get_smb2_max_async_credits */

SYMBOL_EXPORT void
chimera_server_config_set_smb_fs_physical_bytes_per_sector(
    struct chimera_server_config *config,
    uint32_t                      value)
{
    config->smb_fs_physical_bytes_per_sector = value;
} /* chimera_server_config_set_smb_fs_physical_bytes_per_sector */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_smb_fs_physical_bytes_per_sector(const struct chimera_server_config *config)
{
    return config->smb_fs_physical_bytes_per_sector;
} /* chimera_server_config_get_smb_fs_physical_bytes_per_sector */

SYMBOL_EXPORT void
chimera_server_config_set_smb_fs_sector_size_flags(
    struct chimera_server_config *config,
    uint32_t                      value)
{
    config->smb_fs_sector_size_flags = value;
} /* chimera_server_config_set_smb_fs_sector_size_flags */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_smb_fs_sector_size_flags(const struct chimera_server_config *config)
{
    return config->smb_fs_sector_size_flags;
} /* chimera_server_config_get_smb_fs_sector_size_flags */

SYMBOL_EXPORT void
chimera_server_config_set_max_open_files(
    struct chimera_server_config *config,
    int                           open_files)
{
    config->max_open_files = (uint64_t) open_files;
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
chimera_server_config_set_umount_timeout(
    struct chimera_server_config *config,
    int                           timeout_ms)
{
    config->umount_timeout_ms = timeout_ms;
} /* chimera_server_config_set_umount_timeout */

SYMBOL_EXPORT void
chimera_server_config_set_attr_cache_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->attr_cache_enabled = enabled;
} /* chimera_server_config_set_attr_cache_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_name_cache_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->name_cache_enabled = enabled;
} /* chimera_server_config_set_name_cache_enabled */

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
chimera_server_config_set_nfs4_drc(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs4_drc = enable;
} /* chimera_server_config_set_nfs4_drc */

SYMBOL_EXPORT int
chimera_server_config_get_nfs4_drc(const struct chimera_server_config *config)
{
    return config->nfs4_drc;
} /* chimera_server_config_get_nfs4_drc */

SYMBOL_EXPORT void
chimera_server_config_set_nfs3_drc(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs3_drc = enable;
} /* chimera_server_config_set_nfs3_drc */

SYMBOL_EXPORT int
chimera_server_config_get_nfs3_drc(const struct chimera_server_config *config)
{
    return config->nfs3_drc;
} /* chimera_server_config_get_nfs3_drc */

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
chimera_server_config_set_nfs4_node_id(
    struct chimera_server_config *config,
    int                           node_id)
{
    config->nfs4_node_id = node_id;
} /* chimera_server_config_set_nfs4_node_id */

SYMBOL_EXPORT int
chimera_server_config_get_nfs4_node_id(const struct chimera_server_config *config)
{
    return config->nfs4_node_id;
} /* chimera_server_config_get_nfs4_node_id */

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
chimera_server_config_set_s3_anon_ids(
    struct chimera_server_config *config,
    uint32_t                      uid,
    uint32_t                      gid)
{
    config->s3_anon_uid = uid;
    config->s3_anon_gid = gid;
} /* chimera_server_config_set_s3_anon_ids */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_s3_anon_uid(const struct chimera_server_config *config)
{
    return config->s3_anon_uid;
} /* chimera_server_config_get_s3_anon_uid */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_s3_anon_gid(const struct chimera_server_config *config)
{
    return config->s3_anon_gid;
} /* chimera_server_config_get_s3_anon_gid */

SYMBOL_EXPORT void
chimera_server_config_set_smb_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->smb_port = port;
} /* chimera_server_config_set_smb_port */

SYMBOL_EXPORT int
chimera_server_config_get_smb_port(const struct chimera_server_config *config)
{
    return config->smb_port;
} /* chimera_server_config_get_smb_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->nfs_enabled = enabled;
} /* chimera_server_config_set_nfs_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_enabled(const struct chimera_server_config *config)
{
    return config->nfs_enabled;
} /* chimera_server_config_get_nfs_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_smb_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_enabled = enabled;
} /* chimera_server_config_set_smb_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_smb_enabled(const struct chimera_server_config *config)
{
    return config->smb_enabled;
} /* chimera_server_config_get_smb_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_s3_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->s3_enabled = enabled;
} /* chimera_server_config_set_s3_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_s3_enabled(const struct chimera_server_config *config)
{
    return config->s3_enabled;
} /* chimera_server_config_get_s3_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_fuse_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->fuse_enabled = enabled;
} /* chimera_server_config_set_fuse_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_fuse_enabled(const struct chimera_server_config *config)
{
    return config->fuse_enabled;
} /* chimera_server_config_get_fuse_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_fuse_io_uring(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->fuse_io_uring = enabled;
} /* chimera_server_config_set_fuse_io_uring */

SYMBOL_EXPORT int
chimera_server_config_get_fuse_io_uring(const struct chimera_server_config *config)
{
    return config->fuse_io_uring;
} /* chimera_server_config_get_fuse_io_uring */

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
chimera_server_config_set_nfs_mount_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_mount_port = port;
} /* chimera_server_config_set_nfs_mount_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_mount_port(const struct chimera_server_config *config)
{
    return config->nfs_mount_port;
} /* chimera_server_config_get_nfs_mount_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_portmap_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_portmap_port = port;
} /* chimera_server_config_set_nfs_portmap_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_portmap_port(const struct chimera_server_config *config)
{
    return config->nfs_portmap_port;
} /* chimera_server_config_get_nfs_portmap_port */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_nsm_port(
    struct chimera_server_config *config,
    int                           port)
{
    config->nfs_nsm_port = port;
} /* chimera_server_config_set_nfs_nsm_port */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_nsm_port(const struct chimera_server_config *config)
{
    return config->nfs_nsm_port;
} /* chimera_server_config_get_nfs_nsm_port */

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
     * default module (e.g. memfs with ds_mode/block_size) must override its
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
        /* No path: the VFS loads the module by name from its module directory */
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
chimera_server_config_set_rest_auth_enabled(
    struct chimera_server_config *config,
    int                           enable)
{
    config->rest_auth_enabled = enable;
} /* chimera_server_config_set_rest_auth_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_rest_auth_enabled(const struct chimera_server_config *config)
{
    return config->rest_auth_enabled;
} /* chimera_server_config_get_rest_auth_enabled */

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

SYMBOL_EXPORT void
chimera_server_config_set_nfs_max_exports(
    struct chimera_server_config *config,
    uint32_t                      nfs_max_exports)
{
    /* Clamp rather than trust the caller: 0 would reject every export create
    * with a baffling "limit reached (0)", and a cap beyond the id space
    * could never be reached (ids are unique per export).  The daemon config
    * parser validates before calling; this guards direct library callers. */
    if (nfs_max_exports < 1 || nfs_max_exports > CHIMERA_NFS_EXPORT_ID_MAX) {
        chimera_server_error("nfs_max_exports %u out of range (1..%u); clamping",
                             nfs_max_exports, CHIMERA_NFS_EXPORT_ID_MAX);
        nfs_max_exports = nfs_max_exports < 1 ?
            1 : CHIMERA_NFS_EXPORT_ID_MAX;
    }
    config->nfs_max_exports = nfs_max_exports;
} /* chimera_server_config_set_nfs_max_exports */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_nfs_max_exports(const struct chimera_server_config *config)
{
    return config->nfs_max_exports;
} /* chimera_server_config_get_nfs_max_exports */

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
chimera_server_config_set_nfs_fh_sign(
    struct chimera_server_config *config,
    int                           enable)
{
    config->nfs_fh_sign = enable;
} /* chimera_server_config_set_nfs_fh_sign */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_fh_sign(const struct chimera_server_config *config)
{
    return config->nfs_fh_sign;
} /* chimera_server_config_get_nfs_fh_sign */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_fh_key(
    struct chimera_server_config *config,
    const char                   *hexkey)
{
    snprintf(config->nfs_fh_key, sizeof(config->nfs_fh_key), "%s", hexkey);
} /* chimera_server_config_set_nfs_fh_key */

SYMBOL_EXPORT const char *
chimera_server_config_get_nfs_fh_key(const struct chimera_server_config *config)
{
    return config->nfs_fh_key;
} /* chimera_server_config_get_nfs_fh_key */

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
chimera_server_config_set_smb_kerberos_anonymous_fallback(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->smb_auth.kerberos_anonymous_fallback = enabled;
} /* chimera_server_config_set_smb_kerberos_anonymous_fallback */

SYMBOL_EXPORT int
chimera_server_config_get_smb_kerberos_anonymous_fallback(const struct chimera_server_config *config)
{
    return config->smb_auth.kerberos_anonymous_fallback;
} /* chimera_server_config_get_smb_kerberos_anonymous_fallback */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_kerberos_enabled(
    struct chimera_server_config *config,
    int                           enabled)
{
    config->nfs_auth.kerberos_enabled = enabled;
} /* chimera_server_config_set_nfs_kerberos_enabled */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_kerberos_enabled(const struct chimera_server_config *config)
{
    return config->nfs_auth.kerberos_enabled;
} /* chimera_server_config_get_nfs_kerberos_enabled */

SYMBOL_EXPORT void
chimera_server_config_set_nfs_kerberos_keytab(
    struct chimera_server_config *config,
    const char                   *keytab)
{
    strncpy(config->nfs_auth.kerberos_keytab, keytab,
            sizeof(config->nfs_auth.kerberos_keytab) - 1);
} /* chimera_server_config_set_nfs_kerberos_keytab */

SYMBOL_EXPORT const char *
chimera_server_config_get_nfs_kerberos_keytab(const struct chimera_server_config *config)
{
    return config->nfs_auth.kerberos_keytab;
} /* chimera_server_config_get_nfs_kerberos_keytab */

/*
 * Add a principal -> UNIX identity mapping.
 *
 * `principal` is matched against the authenticated name with its realm
 * stripped, so "u1000" matches "u1000@EXAMPLE.COM"; a two-component service
 * name is written in full ("nfs/host").  Supplementary groups are carried
 * because that is the part nsswitch resolution most often gets wrong for a
 * Kerberos-only user: there is no local account to read them from.
 */
SYMBOL_EXPORT int
chimera_server_config_add_nfs_principal_map(
    struct chimera_server_config *config,
    const char                   *principal,
    uint32_t                      uid,
    uint32_t                      gid,
    uint32_t                      num_gids,
    const uint32_t               *gids)
{
    struct chimera_server_config_principal_map_entry *e;
    uint32_t                                          i;

    if (!principal || !*principal ||
        strlen(principal) >= sizeof(e->principal) ||
        config->nfs_auth.num_principal_map >= CHIMERA_NFS_MAX_PRINCIPAL_MAP) {
        return -1;
    }

    if (num_gids > CHIMERA_VFS_CRED_MAX_GIDS) {
        num_gids = CHIMERA_VFS_CRED_MAX_GIDS;
    }

    e = &config->nfs_auth.principal_map[config->nfs_auth.num_principal_map++];

    snprintf(e->principal, sizeof(e->principal), "%s", principal);
    e->uid      = uid;
    e->gid      = gid;
    e->num_gids = num_gids;

    for (i = 0; i < num_gids; i++) {
        e->gids[i] = gids[i];
    }

    return 0;
} /* chimera_server_config_add_nfs_principal_map */

SYMBOL_EXPORT int
chimera_server_config_get_nfs_principal_map_count(const struct chimera_server_config *config)
{
    return config->nfs_auth.num_principal_map;
} /* chimera_server_config_get_nfs_principal_map_count */

SYMBOL_EXPORT const char *
chimera_server_config_get_nfs_principal_map_entry(
    const struct chimera_server_config *config,
    int                                 index,
    uint32_t                           *uid,
    uint32_t                           *gid,
    uint32_t                           *num_gids,
    const uint32_t                    **gids)
{
    const struct chimera_server_config_principal_map_entry *e;

    if (index < 0 || index >= config->nfs_auth.num_principal_map) {
        return NULL;
    }

    e = &config->nfs_auth.principal_map[index];

    *uid      = e->uid;
    *gid      = e->gid;
    *num_gids = e->num_gids;
    *gids     = e->gids;

    return e->principal;
} /* chimera_server_config_get_nfs_principal_map_entry */

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
