#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include "core/evpl.h"
#include "thread/thread.h"
#include "server.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "vfs/vfs.h"

struct chimera_server {
    struct chimera_vfs             *vfs;
    struct evpl_threadpool         *pool;
    struct chimera_server_protocol *protocols[2];
    void                           *protocol_private[2];
    int                             num_protocols;
    int                             threads_online;
    pthread_mutex_t                 lock;
};

struct chimera_thread {
    struct chimera_server     *server;
    struct chimera_vfs_thread *vfs_thread;
    void                      *protocol_private[2];
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

    thread->vfs_thread = chimera_vfs_thread_init(evpl, server->vfs);

    pthread_mutex_lock(&server->lock);
    server->threads_online++;
    pthread_mutex_unlock(&server->lock);

    return thread;
} /* chimera_server_thread_init */

int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *module_name,
    const char            *share_path,
    const char            *module_path)
{
    return chimera_vfs_create_share(server->vfs, module_name, share_path,
                                    module_path);
} /* chimera_server_create_share */

static void
chimera_server_thread_destroy(void *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->thread_destroy(thread->protocol_private[i]);
    }

    chimera_vfs_thread_destroy(thread->vfs_thread);
    free(thread);
} /* chimera_server_thread_destroy */

struct chimera_server *
chimera_server_init(const char *cfgfile)
{
    struct chimera_server *server;
    int                    i;

    server = calloc(1, sizeof(*server));

    pthread_mutex_init(&server->lock, NULL);

    server->vfs = chimera_vfs_init();

    server->protocols[server->num_protocols++] = &nfs_protocol;

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(server->vfs);
    }

    server->pool = evpl_threadpool_create(1, chimera_server_thread_init, NULL,
                                          chimera_server_thread_destroy, server)
    ;

    while (server->threads_online < 1) {
        usleep(100);
    }

    return server;
} /* chimera_server_init */

void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    evpl_threadpool_destroy(server->pool);

    for (i = 0; i < server->num_protocols; i++) {
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    chimera_vfs_destroy(server->vfs);

    free(server);
} /* chimera_server_destroy */
