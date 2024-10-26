#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "server.h"

struct chimera_server {

};


struct chimera_server *
chimera_server_init(const char *cfgfile)
{
    struct chimera_server *server;

    server = calloc(1, sizeof(*server));

    return server;
}

void
chimera_destroy(struct chimera_server *server)
{
    free(server);
}
