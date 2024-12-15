#pragma once

struct chimera_server;
struct chimera_server_config;

struct chimera_server_config * chimera_server_config_init(
    void);

struct chimera_server * chimera_server_init(
    const struct chimera_server_config *config);

void chimera_server_config_set_nfs_rdma(
    struct chimera_server_config *config,
    int                           enable);

void chimera_server_config_set_nfs_rdma_hostname(
    struct chimera_server_config *config,
    const char                   *hostname);

int
chimera_server_config_get_nfs_rdma(
    const struct chimera_server_config *config);

const char *
chimera_server_config_get_nfs_rdma_hostname(
    const struct chimera_server_config *config);

void chimera_server_config_set_nfs_rdma_port(
    struct chimera_server_config *config,
    int                           port);

int
chimera_server_config_get_nfs_rdma_port(
    const struct chimera_server_config *config);

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *module_name,
    const char            *share_path,
    const char            *module_path);

void chimera_server_destroy(
    struct chimera_server *server);

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *module_name,
    const char            *share_path,
    const char            *module_path);
