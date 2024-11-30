#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <jansson.h>

#include "server/server.h"
#include "server/server_internal.h"
int SigInt = 0;

void
signal_handler(int sig)
{
    SigInt = 1;
} /* signal_handler */

int
main(
    int    argc,
    char **argv)
{
    const char  *config_path = "/usr/local/etc/chimera.json";
    extern char *optarg;
    int          opt;
    const char  *share_name;
    const char  *share_module;
    const char  *share_path;
    json_t      *config, *shares, *share;
    json_error_t error;


    while ((opt = getopt(argc, argv, "c:v")) != -1) {
        switch (opt) {
            case 'c':
                config_path = optarg;
                break;
            case 'v':
                printf("Version: %s\n", CHIMERA_VERSION);
                return 0;
        } /* switch */
    }

    config = json_load_file(config_path, 0, &error);

    if (!config) {
        fprintf(stderr, "Failed to load configuration file: %s\n", error.text);
        return 1;
    }

    struct chimera_server *server;

    signal(SIGINT, signal_handler);

    chimera_server_info("Initializing server...");

    server = chimera_server_init(NULL);

    shares = json_object_get(config, "shares");

    if (shares) {
        json_object_foreach(shares, share_name, share)
        {
            share_module = json_string_value(json_object_get(share, "module"));
            share_path   = json_string_value(json_object_get(share, "path"));

            chimera_server_info("Initializing share %s (%s://%s)...", share_name
                                ,
                                share_module, share_path);
            chimera_server_create_share(server, share_module, share_name,
                                        share_path);
        }
    }

    while (!SigInt) {
        sleep(1);
    }

    chimera_server_info("Shutting down server...");

    chimera_server_destroy(server);

    chimera_server_info("Server shutdown complete.");

    json_decref(config);

    return 0;
} /* main */