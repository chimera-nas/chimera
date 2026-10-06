// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "common/export.h"
#include "vfs_daos.h"

static pthread_mutex_t        library_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned int           library_users;
static struct vfs_daos_state *quarantined_states;

int
vfs_daos_connect(struct vfs_daos_state *s)
{
    dfs_t *connected = NULL;
    int    rc;

    if (s->dfs) {
        return 0;
    }
    rc = dfs_connect(s->config.pool, s->config.system, s->config.container,
                     s->config.read_only ? O_RDONLY : O_RDWR,
                     NULL, &connected);
    if (!rc) {
        s->dfs = connected;
    }
    return rc;
} /* vfs_daos_connect */

int
vfs_daos_disconnect(struct vfs_daos_state *s)
{
    int rc;

    s->draining = true;
    if (s->uncertain_release_count) {
        return EIO;
    }
    if (s->objects) {
        return EBUSY;
    }
    vfs_daos_cursors_clear(s);
    if (s->root) {
        dfs_obj_t *root = s->root;
        s->root = NULL;
        rc      = vfs_daos_release(s, root);
        if (rc) {
            s->unavailable = true;
            fprintf(stderr, "vfs_daos: root release uncertain: %d\n", rc);
            return rc;
        }
    }
    rc = vfs_daos_registry_clear(s);
    if (rc) {
        return rc;
    }
    if (s->owned) {
        return EBUSY;
    }
    if (s->dfs) {
        rc = dfs_disconnect(s->dfs);
        if (rc) {
            s->unavailable = true;
            return rc;
        }
        s->dfs = NULL;
    }
    s->incarnation++;
    s->active      = false;
    s->draining    = false;
    s->unavailable = false;
    return 0;
} /* vfs_daos_disconnect */

static void *
vfs_daos_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    struct vfs_daos_state *s = calloc(1, sizeof(*s));
    struct timespec        now;
    int                    rc;

    (void) metrics;
    if (!s) {
        return NULL;
    }
    if (pthread_mutex_init(&s->mutex, NULL)) {
        free(s);
        return NULL;
    }
    if (pthread_cond_init(&s->drained, NULL)) {
        pthread_mutex_destroy(&s->mutex);
        free(s);
        return NULL;
    }
    s->error = vfs_daos_config_parse(cfgdata, &s->config);
    if (s->error) {
        return s;
    }
    clock_gettime(CLOCK_MONOTONIC, &now);
    s->incarnation = ((uint64_t) now.tv_sec << 32) ^ now.tv_nsec;
    s->generation  = s->incarnation | 1;
    s->next_cookie = s->generation;
    pthread_mutex_lock(&library_mutex);
    /* dfs_init owns daos_init; an additional raw init would leak a reference. */
    rc = library_users ? 0 : dfs_init();
    if (!rc) {
        library_users++;
        s->initialized = true;
    }
    pthread_mutex_unlock(&library_mutex);
    s->error = rc;
    return s;
} /* vfs_daos_init */

static void
vfs_daos_destroy(void *private_data)
{
    struct vfs_daos_state  *s = private_data;
    struct vfs_daos_object *o;

    if (!s) {
        return;
    }
    pthread_mutex_lock(&s->mutex);
    while ((o = s->objects)) {
        s->objects = o->next;
        if (o->obj) {
            int rc = vfs_daos_release(s, o->obj);
            if (rc) {
                fprintf(stderr, "vfs_daos: final object release: %d\n", rc);
                s->unavailable = true;
            }
        }
        free(o);
    }
    vfs_daos_disconnect(s);
    vfs_daos_cursors_clear(s);
    pthread_mutex_unlock(&s->mutex);
    pthread_mutex_lock(&library_mutex);
    if (s->dfs || s->uncertain_release_count) {
        s->next_quarantined = quarantined_states;
        quarantined_states  = s;
        fprintf(stderr, "vfs_daos: shutdown retains unresolved library ownership\n");
        pthread_mutex_unlock(&library_mutex);
        return;
    }
    /* Keep the library alive if uncertain cleanup still owns a connection. */
    if (s->initialized && !s->dfs && --library_users == 0) {
        int rc = dfs_fini();
        if (rc) {
            fprintf(stderr, "vfs_daos: dfs_fini: %d\n", rc);
        }
    }
    pthread_mutex_unlock(&library_mutex);
    vfs_daos_config_free(&s->config);
    pthread_cond_destroy(&s->drained);
    pthread_mutex_destroy(&s->mutex);
    free(s);
} /* vfs_daos_destroy */

static void *
vfs_daos_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    return private_data;
} /* vfs_daos_thread_init */

static void
vfs_daos_thread_destroy(void *private_data)
{
    (void) private_data;
} /* vfs_daos_thread_destroy */

static uint64_t
monotonic_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000 + ts.tv_nsec;
} /* monotonic_ns */

static void
vfs_daos_dispatch(
    struct chimera_vfs_request *r,
    void                       *private_data)
{
    struct vfs_daos_state *s     = private_data;
    uint64_t               start = monotonic_ns();
    uint64_t               entered;

    if (!s) {
        r->status = CHIMERA_VFS_EIO;
    } else {
        pthread_mutex_lock(&s->mutex);
        entered          = monotonic_ns();
        s->lock_wait_ns += entered - start;
        if (r->opcode == CHIMERA_VFS_OP_UMOUNT && r->umount.mount_private == s) {
            s->draining = true;
            while (s->objects && !s->unavailable) {
                pthread_cond_wait(&s->drained, &s->mutex);
            }
        }
        r->status = vfs_daos_operation(s, r);
        if (r->opcode == CHIMERA_VFS_OP_CLOSE) {
            pthread_cond_broadcast(&s->drained);
        }
        if (r->opcode < CHIMERA_VFS_OP_NUM) {
            s->operations[r->opcode]++;
            s->latency_ns[r->opcode] += monotonic_ns() - entered;
        }
        pthread_mutex_unlock(&s->mutex);
    }
    r->complete(r);
} /* vfs_daos_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_daos = {
    .sdk_version  = CHIMERA_VFS_SDK_VERSION,
    .name         = "daos",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_DAOS,
    .capabilities = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP |
        CHIMERA_VFS_CAP_BLOCKING | CHIMERA_VFS_CAP_CREATE_GID_ENGINE,
    .init           = vfs_daos_init,
    .destroy        = vfs_daos_destroy,
    .thread_init    = vfs_daos_thread_init,
    .thread_destroy = vfs_daos_thread_destroy,
    .dispatch       = vfs_daos_dispatch,
};
