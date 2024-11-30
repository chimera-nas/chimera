#include <stdio.h>
#include <signal.h>
#include <unistd.h>

#include "server/server.h"

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
    int optarg;


    while ((optarg = getopt(argc, argv, "v")) != -1) {
        switch (optarg) {
            case 'v':
                printf("Version: %s\n", CHIMERA_VERSION);
                return 0;
        } /* switch */
    }

    struct chimera_server *server;

    signal(SIGINT, signal_handler);

    server = chimera_server_init(NULL);

    while (!SigInt) {
        sleep(1);
    }

    chimera_server_destroy(server);

    return 0;
} /* main */