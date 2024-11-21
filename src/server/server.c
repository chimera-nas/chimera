#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "core/evpl.h"
#include "thread/thread.h"
#include "server.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "vfs/vfs.h"

struct chimera_server {
    struct chimera_vfs             *vfs;
    struct evpl_threadpool         *pool;
    struct chimera_server_protocol *protocols[2];
    void                           *protocol_private[2];
    int                             num_protocols;
};

struct chimera_thread {
    struct chimera_server *server;
    void                  *protocol_private[2];
};

static void *
chimera_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server *server = data;
    struct chimera_thread *thread;

    thread = calloc(1, sizeof(*thread));

    thread->server = server;

    for (int i = 0; i < server->num_protocols; i++) {
        thread->protocol_private[i] = server->protocols[i]->thread_init(evpl,
                                                                        server->
                                                                        protocol_private
                                                                        [i]);
    }

    return thread;
} /* chimera_server_thread_init */

static void
chimera_server_thread_destroy(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->thread_destroy(evpl, thread->protocol_private[i]);
    }

    free(thread);
} /* chimera_server_thread_destroy */

struct chimera_server *
chimera_server_init(const char *cfgfile)
{
    struct chimera_server *server;
    int                    i;

    evpl_init_auto(NULL);

    server = calloc(1, sizeof(*server));

    server->vfs = chimera_vfs_init();

    server->protocols[server->num_protocols++] = &nfs_protocol;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(server->vfs);
    }

    server->pool = evpl_threadpool_create(1, chimera_server_thread_init, NULL,
                                          chimera_server_thread_destroy, server)
    ;

    return server;
} /* chimera_server_init */

void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    evpl_threadpool_destroy(server->pool);

    chimera_vfs_destroy(server->vfs);

    free(server);
} /* chimera_server_destroy */
