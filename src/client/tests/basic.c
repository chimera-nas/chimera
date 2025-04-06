#include <stdio.h>

#include "client/client.h"

int
main(
    int    argc,
    char **argv)
{
    struct chimera_client        *client;
    struct chimera_client_config *config;
    int                           rc;

    config = chimera_client_config_init();

    client = chimera_client_init(config);

    rc = chimera_client_mount(client, "/memfs", "memfs", "/");

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        return 1;
    }

    chimera_client_destroy(client);

    return 0;
} /* main */
