// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/resource.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "vfs/vfs.h"

struct chimera_server_config;
struct chimera_server;
struct chimera_thread;
struct prometheus_metrics;
struct chimera_vfs_user;
struct chimera_nfs_export;
struct chimera_smb_share;
struct s3_bucket;


struct chimera_server_config_smb_nic {
    char     address[80];
    uint64_t speed;
    uint8_t  rdma;
};


struct chimera_server_config *
chimera_server_config_init(
    void);

void
chimera_server_config_set_tcp_flavor(
    struct chimera_server_config *config,
    enum chimera_tcp_flavor       flavor);

enum chimera_tcp_flavor
chimera_server_config_get_tcp_flavor(
    const struct chimera_server_config *config);

enum evpl_protocol_id
chimera_server_config_get_tcp_stream_protocol(
    const struct chimera_server_config *config);

void
chimera_server_config_set_core_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_sync_delegation(
    struct chimera_server_config *config,
    int                           enable);

void
chimera_server_config_set_sync_delegation_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_async_delegation(
    struct chimera_server_config *config,
    int                           enable);

void
chimera_server_config_set_async_delegation_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_smb_persistent_handles(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_smb_persistent_handles(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_directory_leases(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_smb_directory_leases(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_named_streams(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_smb_named_streams(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_signing_required(
    struct chimera_server_config *config,
    int                           required);

int
chimera_server_config_get_smb_signing_required(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_encryption(
    struct chimera_server_config *config,
    int                           mode);

int
chimera_server_config_get_smb_encryption(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_compression(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_smb_compression(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_leases(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_smb_leases(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_oplocks(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_smb_oplocks(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_notify_disabled(
    struct chimera_server_config *config,
    int                           disabled);

int
chimera_server_config_get_smb_notify_disabled(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_acl_inherited_canonicalize(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_smb_acl_inherited_canonicalize(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb2_max_async_credits(
    struct chimera_server_config *config,
    int                           value);

int
chimera_server_config_get_smb2_max_async_credits(
    const struct chimera_server_config *config);

void
chimera_server_config_set_cache_ttl(
    struct chimera_server_config *config,
    int                           ttl);

int
chimera_server_config_get_cache_ttl(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rcu_reclaim_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_nfs4_session_slots(
    struct chimera_server_config *config,
    int                           slots);

int
chimera_server_config_get_nfs4_session_slots(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_delegations(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs4_delegations(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_drc(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs4_drc(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs3_drc(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs3_drc(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_lease_time(
    struct chimera_server_config *config,
    uint32_t                      seconds);

uint32_t
chimera_server_config_get_nfs4_lease_time(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_grace_time(
    struct chimera_server_config *config,
    uint32_t                      seconds);

uint32_t
chimera_server_config_get_nfs4_grace_time(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_node_id(
    struct chimera_server_config *config,
    int                           node_id);

int
chimera_server_config_get_nfs4_node_id(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs4_courtesy_time(
    struct chimera_server_config *config,
    uint32_t                      seconds);

uint32_t
chimera_server_config_get_nfs4_courtesy_time(
    const struct chimera_server_config *config);

void
chimera_server_config_set_kv_module(
    struct chimera_server_config *config,
    const char                   *kv_module);

const char *
chimera_server_config_get_kv_module(
    const struct chimera_server_config *config);

void
chimera_server_config_set_max_open_files(
    struct chimera_server_config *config,
    int                           open_files);

int
chimera_server_config_get_external_portmap(
    const struct chimera_server_config *config);

void
chimera_server_config_set_external_portmap(
    struct chimera_server_config *config,
    int                           enable);

void
chimera_server_config_set_portmap_hostname(
    struct chimera_server_config *config,
    const char                   *hostname);

const char *
chimera_server_config_get_portmap_hostname(
    const struct chimera_server_config *config);

/*
 * Resolve a hostname or IPv4 dotted-quad string to an IPv4 dotted-quad string.
 * Writes the result (NUL-terminated) into out_buf. out_size must be at least
 * INET_ADDRSTRLEN (16). Aborts startup with a clear error on resolution
 * failure.
 */
void
chimera_server_resolve_ipv4(
    const char *hostname,
    char       *out_buf,
    size_t      out_size);

uint32_t
chimera_server_config_get_soft_fail_bad_req(
    const struct chimera_server_config *config);

void
chimera_server_config_set_soft_fail_bad_req(
    struct chimera_server_config *config,
    int                           enable);

void
chimera_server_config_set_nfs_rdma(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs_rdma(
    const struct chimera_server_config *config);

void
chimera_server_config_set_pnfs_enabled(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_pnfs_enabled(
    const struct chimera_server_config *config);

int
chimera_server_config_add_pnfs_ds(
    struct chimera_server_config *config,
    const char                   *netid,
    const char                   *uaddr,
    const char                   *rdma_uaddr,
    const char                   *backing_path,
    int                           version,
    int                           minorversion);

/* After mounts are established, resolve each pNFS data server's backing root
 * (its nfs-mounted export directory) into the device table so the MDS can
 * create backing files there.  Returns 0 on success. */
int
chimera_server_pnfs_resolve(
    struct chimera_server *server);

void
chimera_server_config_set_nfs_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_s3_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_s3_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_data_server(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs_data_server(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_server_scope(
    struct chimera_server_config *config,
    uint64_t                      scope);

uint64_t
chimera_server_config_get_nfs_server_scope(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_rdma_hostname(
    struct chimera_server_config *config,
    const char                   *hostname);

const char *
chimera_server_config_get_nfs_rdma_hostname(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_rdma_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_rdma_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_tcp_rdma_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_tcp_rdma_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_lockmgr_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_lockmgr_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_nsm_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_nsm_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_state_dir(
    struct chimera_server_config *config,
    const char                   *dir);

const char *
chimera_server_config_get_state_dir(
    const struct chimera_server_config *config);

void
chimera_server_config_add_module(
    struct chimera_server_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_data);

void
chimera_server_config_set_metrics_port(
    struct chimera_server_config *config,
    int                           port);

void
chimera_server_config_set_rest_http_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_rest_http_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rest_https_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_rest_https_port(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rest_debug_fsops(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_rest_debug_fsops(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rest_auth_enabled(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_rest_auth_enabled(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rest_ssl_cert(
    struct chimera_server_config *config,
    const char                   *cert_path);

const char *
chimera_server_config_get_rest_ssl_cert(
    const struct chimera_server_config *config);

void
chimera_server_config_set_rest_ssl_key(
    struct chimera_server_config *config,
    const char                   *key_path);

const char *
chimera_server_config_get_rest_ssl_key(
    const struct chimera_server_config *config);

int
chimera_server_config_get_smb_num_dialects(
    const struct chimera_server_config *config);

uint32_t
chimera_server_config_get_smb_dialects(
    const struct chimera_server_config *config,
    int                                 index);

void
chimera_server_config_set_smb_min_dialect(
    struct chimera_server_config *config,
    uint32_t                      min_dialect);

int
chimera_server_config_get_smb_num_nic_info(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_nic_info(
    struct chimera_server_config               *config,
    int                                         num_nic_info,
    const struct chimera_server_config_smb_nic *smb_nic_info);

const struct chimera_server_config_smb_nic *
chimera_server_config_get_smb_nic_info(
    const struct chimera_server_config *config,
    int                                 index);

void
chimera_server_config_set_anonuid(
    struct chimera_server_config *config,
    uint32_t                      anonuid);

uint32_t
chimera_server_config_get_anonuid(
    const struct chimera_server_config *config);

void
chimera_server_config_set_anongid(
    struct chimera_server_config *config,
    uint32_t                      anongid);

uint32_t
chimera_server_config_get_anongid(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_fh_sign(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs_fh_sign(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_fh_key(
    struct chimera_server_config *config,
    const char                   *hexkey);

const char *
chimera_server_config_get_nfs_fh_key(
    const struct chimera_server_config *config);

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer);

int
chimera_server_mount(
    struct chimera_server *server,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path,
    const char            *options);

/* Ensure module_path exists inside module_name (creating intermediate dirs with
 * the given mode, owner 0/0) before it is mounted -- backs the "create" mount
 * option.  Returns 0 on success, -1 if any component could not be created. */
int
chimera_server_mkpath(
    struct chimera_server *server,
    const char            *module_name,
    const char            *module_path,
    uint32_t               mode);

/* Seed the symbolic-link fixtures the WPTS MS-SMB2 CreateClose symlink cases
 * expect to pre-exist on a memfs-backed share (a directory holding an in-path
 * link, plus a dangling root-level link).  A test-harness aid -- the server
 * never resolves these links (it returns STATUS_STOPPED_ON_SYMLINK).  Returns
 * 0 on success, -1 otherwise. */
int
chimera_server_seed_symlinks(
    struct chimera_server *server,
    const char            *module_name);

int
chimera_server_unmount(
    struct chimera_server *server,
    const char            *mount_path);

int
chimera_server_mount_in_use(
    struct chimera_server *server,
    const char            *mount_path);

typedef int (*chimera_server_mount_iterate_cb)(
    const char *mount_path,
    const char *module_name,
    const char *module_path,
    void       *data);

void
chimera_server_iterate_mounts(
    struct chimera_server          *server,
    chimera_server_mount_iterate_cb callback,
    void                           *data);

int
chimera_server_create_bucket(
    struct chimera_server *server,
    const char            *bucket_name,
    const char            *bucket_path);

int
chimera_server_set_s3_bucket_root(
    struct chimera_server *server,
    const char            *bucket_root_path);

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path,
    int                    continuous_availability);

int
chimera_server_share_set_access_based_enum(
    struct chimera_server *server,
    const char            *share_name);

int
chimera_server_share_set_encrypt_data(
    struct chimera_server *server,
    const char            *share_name);

int
chimera_server_create_export(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path);

int
chimera_server_export_set_options(
    struct chimera_server *server,
    const char            *name,
    uint32_t               options,
    uint32_t               squash,
    uint32_t               anonuid,
    uint32_t               anongid);

int
chimera_server_export_set_sec(
    struct chimera_server *server,
    const char            *name,
    uint32_t               sec_allowed);

struct chimera_server *
chimera_server_init(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics);

void
chimera_server_start(
    struct chimera_server *server);

void
chimera_server_destroy(
    struct chimera_server *server);

int
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
    int                    pinned);

int
chimera_server_remove_user(
    struct chimera_server *server,
    const char            *username);

const struct chimera_vfs_user *
chimera_server_get_user(
    struct chimera_server *server,
    const char            *username);

typedef int (*chimera_server_user_iterate_cb)(
    const struct chimera_vfs_user *user,
    void                          *data);

void
chimera_server_iterate_users(
    struct chimera_server         *server,
    chimera_server_user_iterate_cb callback,
    void                          *data);

int
chimera_server_remove_export(
    struct chimera_server *server,
    const char            *name);

const struct chimera_nfs_export *
chimera_server_get_export(
    struct chimera_server *server,
    const char            *name);

typedef int (*chimera_server_export_iterate_cb)(
    const struct chimera_nfs_export *export,
    void *data);

void
chimera_server_iterate_exports(
    struct chimera_server           *server,
    chimera_server_export_iterate_cb callback,
    void                            *data);

int
chimera_server_remove_share(
    struct chimera_server *server,
    const char            *name);

const struct chimera_smb_share *
chimera_server_get_share(
    struct chimera_server *server,
    const char            *name);

typedef int (*chimera_server_share_iterate_cb)(
    const struct chimera_smb_share *share,
    void                           *data);

void
chimera_server_iterate_shares(
    struct chimera_server          *server,
    chimera_server_share_iterate_cb callback,
    void                           *data);

int
chimera_server_remove_bucket(
    struct chimera_server *server,
    const char            *name);

const struct s3_bucket *
chimera_server_get_bucket(
    struct chimera_server *server,
    const char            *name);

void
chimera_server_release_bucket(
    struct chimera_server *server);

typedef int (*chimera_server_bucket_iterate_cb)(
    const struct s3_bucket *bucket,
    void                   *data);

void
chimera_server_iterate_buckets(
    struct chimera_server           *server,
    chimera_server_bucket_iterate_cb callback,
    void                            *data);

struct chimera_vfs *
chimera_server_get_vfs(
    struct chimera_server *server);

int
chimera_server_add_s3_cred(
    struct chimera_server *server,
    const char            *access_key,
    const char            *secret_key,
    int                    pinned);

void
chimera_server_config_set_smb_winbind_enabled(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_smb_winbind_enabled(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_winbind_domain(
    struct chimera_server_config *config,
    const char                   *domain);

const char *
chimera_server_config_get_smb_winbind_domain(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_kerberos_enabled(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_smb_kerberos_enabled(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_kerberos_keytab(
    struct chimera_server_config *config,
    const char                   *keytab);

const char *
chimera_server_config_get_smb_kerberos_keytab(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_kerberos_enabled(
    struct chimera_server_config *config,
    int                           enabled);

int
chimera_server_config_get_nfs_kerberos_enabled(
    const struct chimera_server_config *config);

void
chimera_server_config_set_nfs_kerberos_keytab(
    struct chimera_server_config *config,
    const char                   *keytab);

const char *
chimera_server_config_get_nfs_kerberos_keytab(
    const struct chimera_server_config *config);

void
chimera_server_config_set_smb_kerberos_realm(
    struct chimera_server_config *config,
    const char                   *realm);

const char *
chimera_server_config_get_smb_kerberos_realm(
    const struct chimera_server_config *config);
