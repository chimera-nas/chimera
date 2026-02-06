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
chimera_server_config_set_core_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_delegation_threads(
    struct chimera_server_config *config,
    int                           threads);

void
chimera_server_config_set_cache_ttl(
    struct chimera_server_config *config,
    int                           ttl);

int
chimera_server_config_get_cache_ttl(
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
chimera_server_config_set_nfs_rdma(
    struct chimera_server_config *config,
    int                           enable);

int
chimera_server_config_get_nfs_rdma(
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

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer);

int
chimera_server_mount(
    struct chimera_server *server,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path);

int
chimera_server_create_bucket(
    struct chimera_server *server,
    const char            *bucket_name,
    const char            *bucket_path);

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path);

int
chimera_server_create_export(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path);

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
