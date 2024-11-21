#pragma once

struct chimera_server;

struct chimera_server * chimera_server_init(
    const char *cfgfile);

void chimera_server_destroy(
    struct chimera_server *server);
