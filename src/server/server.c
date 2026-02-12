// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/resource.h>
#include <errno.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "s3/s3.h"
#include "smb/smb.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
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
    int                                   external_portmap;
    rlim_t                                max_open_files;
    int                                   core_threads;
    int                                   delegation_threads;
    int                                   cache_ttl;
    int                                   num_modules;
    int                                   metrics_port;
    int                                   rest_http_port;
    int                                   rest_https_port;
    int                                   smb_num_dialects;
    uint32_t                              smb_dialects[16];
    int                                   smb_num_nic_info;
    int                                   watchdog_timeout_secs;
    uint32_t                              anonuid;
    uint32_t                              anongid;
    char                                  nfs_rdma_hostname[256];
    char                                  kv_module[64];
    char                                  rest_ssl_cert[256];
    char                                  rest_ssl_key[256];
    struct chimera_vfs_module_cfg         modules[CHIMERA_SERVER_MAX_MODULES];
    struct chimera_server_config_smb_nic  smb_nic_info[16];
    struct chimera_server_config_smb_auth smb_auth;
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

    config->core_threads       = 16;
    config->max_open_files     = 65535;
    config->delegation_threads = 64;
    config->nfs_rdma           = 0;
    config->external_portmap   = 0;

    config->smb_num_dialects = 2;
    config->smb_dialects[0]  = SMB2_DIALECT_2_1;
    config->smb_dialects[1]  = SMB2_DIALECT_3_0;

    config->smb_num_nic_info = 0;

    // SMB auth config defaults - local NTLM only
    config->smb_auth.winbind_enabled    = 0;
    config->smb_auth.kerberos_enabled   = 0;
    config->smb_auth.winbind_domain[0]  = '\0';
    config->smb_auth.kerberos_keytab[0] = '\0';
    config->smb_auth.kerberos_realm[0]  = '\0';

    config->anonuid = 65534;
    config->anongid = 65534;

    config->watchdog_timeout_secs = 10;

    config->cache_ttl = 60;

    strncpy(config->nfs_rdma_hostname, "0.0.0.0", sizeof(config->nfs_rdma_hostname));
    config->nfs_rdma_port = 20049;

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
chimera_server_config_set_delegation_threads(
    struct chimera_server_config *config,
    int                           threads)
{
    config->delegation_threads = threads;
} /* chimera_server_config_set_delegation_threads */

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

SYMBOL_EXPORT int
chimera_server_config_get_external_portmap(const struct chimera_server_config *config)
{
    return config->external_portmap;
} /* chimera_server_config_get_external_portmap */

SYMBOL_EXPORT void
chimera_server_config_add_module(
    struct chimera_server_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_data)
{
    struct chimera_vfs_module_cfg *module_cfg;

    module_cfg = &config->modules[config->num_modules];

    strncpy(module_cfg->module_name, module_name, sizeof(module_cfg->module_name));
    strncpy(module_cfg->config_data, config_data, sizeof(module_cfg->config_data));
    if (module_path) {
        strncpy(module_cfg->module_path, module_path, sizeof(module_cfg->module_path));
    } else {
        /* We don't specify a path for preloaded modules like demofs */
        module_cfg->module_path[0] = '\0';
    }
    config->num_modules++;
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

SYMBOL_EXPORT void
chimera_server_config_set_watchdog_timeout(
    struct chimera_server_config *config,
    int                           watchdog_timeout_secs)
{
    config->watchdog_timeout_secs = watchdog_timeout_secs;
} /* chimera_server_config_set_watchdog_timeout */

SYMBOL_EXPORT int
chimera_server_config_get_watchdog_timeout(const struct chimera_server_config *config)
{
    return config->watchdog_timeout_secs;
} /* chimera_server_config_get_watchdog_timeout */

SYMBOL_EXPORT uint32_t
chimera_server_config_get_anongid(const struct chimera_server_config *config)
{
    return config->anongid;
} /* chimera_server_config_get_anongid */

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_thread *thread = container_of(timer, struct chimera_thread, watchdog);
    uint64_t               timeout_ns;

    timeout_ns = (uint64_t) chimera_server_config_get_watchdog_timeout(
        thread->server->config) * 1000000000ULL;

    chimera_vfs_watchdog(thread->vfs_thread, timeout_ns);

    for (int i = 0; i < thread->server->num_protocols; i++) {
        if (thread->server->protocols[i]->watchdog) {
            thread->server->protocols[i]->watchdog(
                thread->protocol_private[i], timeout_ns);
        }
    }
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

    thread->rest_thread = chimera_rest_thread_init(evpl, server->rest);

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
    const char            *module_path)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_mount(thread, NULL, mount_path, module_name, module_path, NULL, chimera_server_mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;

} /* chimera_server_create_share */

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
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path)
{
    if (!server->smb_shared) {
        return -1;
    }

    chimera_smb_add_share(server->smb_shared, share_name, share_path);

    return 0;
} /* chimera_server_create_share */

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
    server->vfs = chimera_vfs_init(config->delegation_threads,
                                   config->modules,
                                   config->num_modules,
                                   config->kv_module,
                                   config->cache_ttl,
                                   metrics);

    chimera_server_info("Initializing protocols...");
    server->protocols[server->num_protocols++] = &nfs_protocol;
    server->protocols[server->num_protocols++] = &smb_protocol;
    server->protocols[server->num_protocols++] = &s3_protocol;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(config, server->vfs, metrics);
    }

    server->s3_shared  = server->protocol_private[2];
    server->smb_shared = server->protocol_private[1];
    server->nfs_shared = server->protocol_private[0];

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
