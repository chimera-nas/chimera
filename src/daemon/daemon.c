#include <signal.h>
#include <unistd.h>

#include "server/server.h"

int SigInt = 0;

void signal_handler(int sig)
{
    SigInt = 1;
}

int main(int argc, char **argv)
{
    struct chimera_server *server;

    signal(SIGINT, signal_handler);

    server = chimera_server_init(NULL);

    while (!SigInt)
    {
        sleep(1);
    }

    chimera_server_destroy(server);

    return 0;
}