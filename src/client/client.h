#pragma once


struct chimera_client;
struct chimera_client_thread;
struct chimera_client_config;

struct chimera_client_config *
chimera_client_config_init(
    void);

struct chimera_client *
chimera_client_init(
    const struct chimera_client_config *config);

void
chimera_client_destroy(
    struct chimera_client *client);