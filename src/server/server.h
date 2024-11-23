#pragma once

struct chimera_server;

struct chimera_server * chimera_server_init(
    const char *cfgfile);

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *module_name,
    const char            *share_path,
    const char            *module_path);

void chimera_server_destroy(
    struct chimera_server *server);
