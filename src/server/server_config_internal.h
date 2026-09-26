// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only


#pragma once

#include "vfs/vfs.h"

#define CHIMERA_SERVER_MAX_MODULES   64

#define CHIMERA_SERVER_MAX_PROTOCOLS 4

struct chimera_server_config_smb_auth {
    int  winbind_enabled;
    int  kerberos_enabled;
    int  kerberos_anonymous_fallback;
    char winbind_domain[256];
    char kerberos_keytab[256];
    char kerberos_realm[256];
};

/*
 * One entry of the static principal map: an authenticated Kerberos principal
 * and the UNIX identity it stands for.
 *
 * RPCSEC_GSS puts no uid on the wire -- the caller is whoever the context
 * authenticated -- so the server has to decide for itself what local identity
 * a principal means.  Resolving the name through nsswitch is the usual answer
 * and remains the fallback, but it requires every Kerberos user to also be a
 * local account, which is not true of every deployment and is awkward to
 * arrange in a test.  An explicit map is what nfsidmap's static method and
 * Ganesha both offer for the same reason.
 */
struct chimera_server_config_principal_map_entry {
    char     principal[256];
    uint32_t uid;
    uint32_t gid;
    uint32_t num_gids;
    uint32_t gids[CHIMERA_VFS_CRED_MAX_GIDS];
};

#define CHIMERA_NFS_MAX_PRINCIPAL_MAP 64

struct chimera_server_config_nfs_auth {
    int                                              kerberos_enabled; /* enable RPCSEC_GSS (sec=krb5) on the NFS server */
    char                                             kerberos_keytab[256]; /* keytab path; empty => KRB5_KTNAME default */
    /* Consulted before nsswitch; empty means nsswitch alone, as before. */
    struct chimera_server_config_principal_map_entry principal_map[CHIMERA_NFS_MAX_PRINCIPAL_MAP];
    int                                              num_principal_map;
};

struct chimera_server_config {
    int                                   nfs_rdma;
    int                                   nfs_rdma_port;
    int                                   nfs_tcp_rdma_port;
    int                                   nfs_lockmgr_port;
    int                                   nfs_nsm_port;
    /* The MOUNT and portmap services.  Configurable for the same reason the
     * two above are: their well-known numbers are only a default, and two NFS
     * servers in one address space (a pNFS metadata server and an NFS proxy in
     * front of it, say) need distinct ones or the second one's listen fails. */
    int                                   nfs_mount_port;
    int                                   nfs_portmap_port;
    int                                   nfs_port;
    int                                   s3_port;
    /* Identity an S3 access key acts as when its configuration binds it to no
     * user.  Defaults to nobody/nogroup so an unbound key is unprivileged. */
    uint32_t                              s3_anon_uid;
    uint32_t                              s3_anon_gid;
    int                                   smb_port;
    int                                   nfs_enabled;
    int                                   smb_enabled;
    int                                   s3_enabled;
    int                                   fuse_enabled;
    int                                   fuse_io_uring;
    int                                   nfs_data_server;
    uint64_t                              nfs_server_scope;
    int                                   external_portmap;
    char                                  portmap_hostname[256];
    int                                   soft_fail_bad_req;
    uint64_t                              max_open_files;
    int                                   core_threads;
    int                                   sync_delegation;
    int                                   sync_delegation_threads;
    int                                   async_delegation;
    int                                   async_delegation_threads;
    int                                   cache_ttl;
    int                                   attr_cache_enabled;
    int                                   umount_timeout_ms;
    int                                   name_cache_enabled;
    int                                   rcu_reclaim_threads;
    int                                   nfs4_session_slots;
    int                                   nfs4_delegations;
    int                                   nfs4_drc;
    int                                   nfs3_drc;
    int                                   nfs4_node_id;
    uint32_t                              nfs4_lease_time_s;
    uint32_t                              nfs4_grace_time_s;
    uint32_t                              nfs4_courtesy_time_s;
    int                                   num_modules;
    int                                   metrics_port;
    int                                   rest_http_port;
    int                                   rest_https_port;
    int num_rest_modules;
    struct chimera_server_rest_module_config rest_modules[CHIMERA_REST_MAX_MODULES];
    int                                   rest_auth_enabled;
    int                                   smb_num_dialects;
    uint32_t                              smb_dialects[16];
    int                                   smb_persistent_handles;
    int                                   smb_directory_leases;
    int                                   smb_named_streams;
    int                                   smb_signing_required;
    int                                   smb_encryption;
    int                                   smb_compression;
    int                                   smb_leases;
    int                                   smb_oplocks;
    int                                   smb_notify_disabled;
    int                                   smb_acl_inherited_canonicalize;
    int                                   smb_mode_from_sid;
    int                                   smb_posix_rename;
    int                                   smb_replay_pending_windows;
    int                                   smb2_max_async_credits;
    uint32_t                              smb_fs_physical_bytes_per_sector;
    uint32_t                              smb_fs_sector_size_flags;
    int                                   smb_num_nic_info;
    uint32_t                              anonuid;
    uint32_t                              anongid;
    uint32_t                              nfs_max_exports;   /* concurrent-export count cap */
    int                                   nfs_fh_sign;       /* sign wire file handles (default on) */
    /* Optional 32-hex-char (128-bit) signing key.  Held with room to spare so
     * an over-long key is not truncated into a well-formed one: NFS init
     * validates the length and reports the mistake. */
    char                                  nfs_fh_key[65];
    enum chimera_tcp_flavor               tcp_flavor;
    char                                  nfs_rdma_hostname[256];
    char                                  kv_module[64];
    char                                  state_dir[256];
    char                                  rest_ssl_cert[256];
    char                                  rest_ssl_key[256];
    struct chimera_vfs_module_cfg         modules[CHIMERA_SERVER_MAX_MODULES];
    struct chimera_server_config_smb_nic  smb_nic_info[16];
    struct chimera_server_config_smb_auth smb_auth;
    struct chimera_server_config_nfs_auth nfs_auth;
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
