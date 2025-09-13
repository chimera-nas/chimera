// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

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
chimera_server_config_add_module(
    struct chimera_server_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_path);

void
chimera_server_config_set_metrics_port(
    struct chimera_server_config *config,
    int                           port);

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