// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include "common/thread.h"
#ifndef _WIN32
#include <sys/resource.h>
#endif /* ifndef _WIN32 */
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <sys/socket.h>
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <netdb.h>
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <arpa/inet.h>
#endif /* ifdef _WIN32 */
#include <errno.h>

#include "evpl/evpl.h"
#include "server_internal.h"
#include "protocol.h"
#include "nfs/nfs.h"
#include "nfs/nfs4_lease.h"
#include "s3/s3.h"
#include "smb/smb.h"
#ifdef __linux__
#include "fuse/fuse.h"
#endif /* ifdef __linux__ */
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_release.h"
#include "common/macros.h"
#include "common/chimera_tracing.h"
#include "server/server.h"
#include "common/common_config.h"
#include "smb_common/smb2.h"
#include "server_mgmt.h"

#include "server_config_internal.h"


struct chimera_server {
    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;
    struct evpl_threadpool             *pool;
    struct chimera_server_protocol     *protocols[CHIMERA_SERVER_MAX_PROTOCOLS];
    void                               *protocol_private[CHIMERA_SERVER_MAX_PROTOCOLS];
    void                               *s3_shared;
    void                               *smb_shared;
    void                               *nfs_shared;
    void                               *fuse_shared;
    const struct chimera_server_mgmt   *mgmt;
    void                               *mgmt_private;
    int                                 num_protocols;
    int                                 threads_online;
    evpl_mutex_t                        lock;
    evpl_cond_t                         all_threads_online;
};

struct chimera_thread {
    struct chimera_server     *server;
    struct chimera_vfs_thread *vfs_thread;
    void                      *protocol_private[CHIMERA_SERVER_MAX_PROTOCOLS];
    void                      *mgmt_thread;
    struct evpl_timer          watchdog;
};

static void
chimera_server_thread_wake(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_thread *thread = container_of(timer, struct chimera_thread, watchdog);

    chimera_vfs_watchdog(thread->vfs_thread);
} /* chimera_server_thread_wake */

static void *
chimera_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server *server = data;
    struct chimera_thread *thread;

    thread = calloc(1, sizeof(*thread));

    evpl_add_timer(evpl, &thread->watchdog, chimera_server_thread_wake, 1000000);

    thread->server = server;

    thread->vfs_thread = chimera_vfs_thread_init(evpl, server->vfs);

    /* This core thread produces spans (starts/ends them); register it with the
     * tracer (no-op unless tracing is active). */
    chimera_tracing_thread_register();

    for (int i = 0; i < server->num_protocols; i++) {
        /* A protocol whose init returned NULL is disabled. */
        if (!server->protocol_private[i]) {
            thread->protocol_private[i] = NULL;
            continue;
        }
        thread->protocol_private[i] = server->protocols[i]->thread_init(evpl,
                                                                        thread->vfs_thread,
                                                                        server->
                                                                        protocol_private
                                                                        [i]);
    }

    thread->mgmt_thread = server->mgmt ?
        server->mgmt->thread_init(evpl, server->mgmt_private, thread->vfs_thread) : NULL;

    evpl_mutex_lock(&server->lock);
    if (++server->threads_online == server->config->core_threads) {
        evpl_cond_signal(&server->all_threads_online);
    }
    evpl_mutex_unlock(&server->lock);

    return thread;
} /* chimera_server_thread_init */

struct mount_ctx {
    int done;
    int status;
};

static void
chimera_server_mount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->done   = 1;
    ctx->status = status;
} /* chimera_server_mount_callback */

SYMBOL_EXPORT int
chimera_server_mount(
    struct chimera_server *server,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path,
    const char            *options)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_mount(thread, NULL, mount_path, module_name, module_path, options, chimera_server_mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;

} /* chimera_server_create_share */

SYMBOL_EXPORT int
chimera_server_mkfs(
    struct chimera_server *server,
    const char            *module_name,
    const char            *fsname,
    const char            *options)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_mkfs(thread, NULL, module_name, fsname, options,
                     chimera_server_mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;
} /* chimera_server_mkfs */

SYMBOL_EXPORT int
chimera_server_rmfs(
    struct chimera_server *server,
    const char            *module_name,
    const char            *fsname)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_rmfs(thread, NULL, module_name, fsname,
                     chimera_server_mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;
} /* chimera_server_rmfs */

/*
 * Ensure a directory path exists inside a backend before it is mounted, for the
 * "create" mount option.  A backend's MOUNT op only walks the path read-only
 * (it returns ENOENT for a missing component), and creating a directory needs
 * an open handle to its parent -- which in turn needs the backend registered in
 * the mount table.  So: transiently mount the backend root, walk the target
 * path component by component creating any that are missing, unmount, and let
 * the caller perform the real mount of the now-existing path.
 *
 * Synchronous (init-time) wrapper around an async lookup-or-mkdir chain, driven
 * on a private evpl like chimera_server_mount.  Returns 0 on success.
 */
#define CHIMERA_MKPATH_TMP_NAME "__chimera_mkpath_tmp"

struct chimera_mkpath_ctx {
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    char                            path[256];
    int                             pathlen;
    int                             offset;
    uint32_t                        mode;
    struct chimera_vfs_open_handle *oh;      /* current parent directory */
    char                            comp[256];
    int                             complen;
    uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t                        child_fh_len;
    struct chimera_vfs_attrs        set_attr;
    enum chimera_vfs_error status;
    int                             done;
};

static void chimera_mkpath_next(
    struct chimera_mkpath_ctx *ctx);

static void
chimera_mkpath_umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    ctx->done = 1;
} /* chimera_mkpath_umount_cb */

/* Release the current handle and tear down the transient root mount, recording
 * the final walk status.  Both the success and failure paths funnel here. */
static void
chimera_mkpath_finish(
    struct chimera_mkpath_ctx *ctx,
    enum chimera_vfs_error     status)
{
    ctx->status = status;

    if (ctx->oh) {
        chimera_vfs_release(ctx->thread, ctx->oh);
        ctx->oh = NULL;
    }

    chimera_vfs_umount(ctx->thread, chimera_vfs_get_server_cred(),
                       CHIMERA_MKPATH_TMP_NAME, chimera_mkpath_umount_cb, ctx);
} /* chimera_mkpath_finish */

/* The just-resolved/created child becomes the new parent; descend. */
static void
chimera_mkpath_descend_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_mkpath_finish(ctx, error_code);
        return;
    }

    if (ctx->oh) {
        chimera_vfs_release(ctx->thread, ctx->oh);
    }
    ctx->oh = oh;
    chimera_mkpath_next(ctx);
} /* chimera_mkpath_descend_cb */

static void
chimera_mkpath_open_child(struct chimera_mkpath_ctx *ctx)
{
    chimera_vfs_open_fh(ctx->thread, chimera_vfs_get_server_cred(),
                        ctx->child_fh, ctx->child_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_mkpath_descend_cb, ctx);
} /* chimera_mkpath_open_child */

static void
chimera_mkpath_mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK || attr->va_fh_len == 0) {
        chimera_mkpath_finish(ctx, error_code != CHIMERA_VFS_OK ? error_code : CHIMERA_VFS_EIO);
        return;
    }

    memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
    ctx->child_fh_len = attr->va_fh_len;
    chimera_mkpath_open_child(ctx);
} /* chimera_mkpath_mkdir_cb */

static void
chimera_mkpath_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        /* Component already exists; descend into it. */
        if (attr->va_fh_len == 0) {
            chimera_mkpath_finish(ctx, CHIMERA_VFS_EIO);
            return;
        }
        memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
        ctx->child_fh_len = attr->va_fh_len;
        chimera_mkpath_open_child(ctx);
        return;
    }

    if (error_code == CHIMERA_VFS_ENOENT) {
        memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
        ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE |
            CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
        ctx->set_attr.va_mode = S_IFDIR | (ctx->mode & 07777);
        /* Own created dirs as the server identity (0/0 when running as root,
         * as CI does).  Hardcoding 0/0 would make the next component's
         * ADD_SUBDIRECTORY gate deny an unprivileged server its own
         * freshly-created parent. */
        ctx->set_attr.va_uid = chimera_vfs_get_server_cred()->uid;
        ctx->set_attr.va_gid = chimera_vfs_get_server_cred()->gid;

        chimera_vfs_mkdir_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->oh,
                             ctx->comp, ctx->complen, &ctx->set_attr,
                             CHIMERA_VFS_ATTR_FH, 0, 0,
                             chimera_mkpath_mkdir_cb, ctx);
        return;
    }

    chimera_mkpath_finish(ctx, error_code);
} /* chimera_mkpath_lookup_cb */

/* Advance to the next '/'-separated component; when none remain the path is
 * fully present and we finish successfully. */
static void
chimera_mkpath_next(struct chimera_mkpath_ctx *ctx)
{
    int start;

    while (ctx->offset < ctx->pathlen && ctx->path[ctx->offset] == '/') {
        ctx->offset++;
    }

    if (ctx->offset >= ctx->pathlen) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    start = ctx->offset;
    while (ctx->offset < ctx->pathlen && ctx->path[ctx->offset] != '/') {
        ctx->offset++;
    }

    ctx->complen = ctx->offset - start;
    if (ctx->complen >= (int) sizeof(ctx->comp)) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_ENAMETOOLONG);
        return;
    }
    memcpy(ctx->comp, ctx->path + start, ctx->complen);
    ctx->comp[ctx->complen] = '\0';

    chimera_vfs_lookup_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->oh,
                          ctx->comp, ctx->complen, CHIMERA_VFS_ATTR_FH, 0,
                          chimera_mkpath_lookup_cb, ctx);
} /* chimera_mkpath_next */

static void
chimera_mkpath_root_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_mkpath_finish(ctx, error_code);
        return;
    }
    ctx->oh = oh;
    chimera_mkpath_next(ctx);
} /* chimera_mkpath_root_open_cb */

static void
chimera_mkpath_mounted_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_mkpath_ctx *ctx = private_data;
    struct chimera_vfs_mount  *m;

    if (status != CHIMERA_VFS_OK) {
        /* Root mount failed -- nothing registered, nothing to unmount. */
        ctx->status = status;
        ctx->done   = 1;
        return;
    }

    m = chimera_vfs_mount_table_find_exact(ctx->vfs->mount_table,
                                           CHIMERA_MKPATH_TMP_NAME,
                                           strlen(CHIMERA_MKPATH_TMP_NAME));
    if (!m) {
        chimera_mkpath_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    chimera_vfs_open_fh(thread, chimera_vfs_get_server_cred(),
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_mkpath_root_open_cb, ctx);
} /* chimera_mkpath_mounted_cb */

SYMBOL_EXPORT int
chimera_server_mkpath(
    struct chimera_server *server,
    const char            *module_name,
    const char            *module_path,
    uint32_t               mode)
{
    struct evpl               *evpl;
    struct chimera_mkpath_ctx  ctx;
    struct chimera_vfs_module *module = NULL;
    const char                *walk   = module_path;
    const char                *slash;
    char                       mount_root[256] = "/";
    int                        i;

    memset(&ctx, 0, sizeof(ctx));

    if (!module_name || !module_path ||
        strlen(module_path) >= sizeof(ctx.path)) {
        return -1;
    }

    /* For a CAP_MKFS module the leading component of module_path names the
     * filesystem: the transient mount targets that filesystem's root and the
     * walk covers only the remainder.  Other modules mount "/" and walk the
     * whole path. */
    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        if (server->vfs->modules[i] &&
            strcmp(server->vfs->modules[i]->name, module_name) == 0) {
            module = server->vfs->modules[i];
            break;
        }
    }

    if (module && (module->capabilities & CHIMERA_VFS_CAP_MKFS)) {
        while (*walk == '/') {
            walk++;
        }
        slash = strchr(walk, '/');
        if (slash) {
            snprintf(mount_root, sizeof(mount_root), "%.*s",
                     (int) (slash - walk), walk);
            walk = slash + 1;
        } else {
            snprintf(mount_root, sizeof(mount_root), "%s", walk);
            walk += strlen(walk);
        }
    }

    evpl        = evpl_create(NULL);
    ctx.vfs     = server->vfs;
    ctx.thread  = chimera_vfs_thread_init(evpl, server->vfs);
    ctx.mode    = mode ? mode : 0755;
    ctx.status  = CHIMERA_VFS_OK;
    ctx.pathlen = snprintf(ctx.path, sizeof(ctx.path), "%s", walk);

    /* Transiently mount the backend root so we have a handle to walk under. */
    chimera_vfs_mount_detached(ctx.thread, chimera_vfs_get_server_cred(),
                               CHIMERA_MKPATH_TMP_NAME, module_name, mount_root, NULL,
                               chimera_mkpath_mounted_cb, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx.thread);
    evpl_destroy(evpl);

    return ctx.status == CHIMERA_VFS_OK ? 0 : -1;
} /* chimera_server_mkpath */

/* ---- test-only symbolic-link seeding -------------------------------------
 * The WPTS MS-SMB2 CreateClose symlink cases expect a small set of symbolic
 * links to already exist on the share:
 *   <root>/SymlinkTest          (a directory)
 *   <root>/SymlinkTest/link  -> "target"      (a link reached mid-path / as the leaf)
 *   <root>/badlink           -> "nonexistent" (a deliberately dangling link)
 * The server never resolves these -- a CREATE that traverses one returns
 * STATUS_STOPPED_ON_SYMLINK -- so the link targets need not exist.  No client
 * can create a Windows reparse-point symlink against an empty share, so the
 * fixtures are seeded here at startup, mirroring chimera_server_mkpath's
 * transient-mount/evpl_continue drive loop.  Best-effort: failures are logged
 * but do not abort the daemon (only the symlink WPTS cases depend on them). */
#define CHIMERA_SEED_TMP_NAME "__chimera_seed_tmp"

struct chimera_seed_ctx {
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_open_handle *root_oh;
    struct chimera_vfs_open_handle *dir_oh;
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t                        dir_fh_len;
    enum chimera_vfs_error status;
    int                             done;
};

static void
chimera_seed_umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    ctx->done = 1;
} /* chimera_seed_umount_cb */

static void
chimera_seed_finish(
    struct chimera_seed_ctx *ctx,
    enum chimera_vfs_error   status)
{
    ctx->status = status;

    if (ctx->dir_oh) {
        chimera_vfs_release(ctx->thread, ctx->dir_oh);
        ctx->dir_oh = NULL;
    }
    if (ctx->root_oh) {
        chimera_vfs_release(ctx->thread, ctx->root_oh);
        ctx->root_oh = NULL;
    }

    chimera_vfs_umount(ctx->thread, chimera_vfs_get_server_cred(),
                       CHIMERA_SEED_TMP_NAME, chimera_seed_umount_cb, ctx);
} /* chimera_seed_finish */

/* Creating the dangling root-level "badlink" is the last step. */
static void
chimera_seed_badlink_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    /* EEXIST is fine -- idempotent re-seed of a persistent backend. */
    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_EEXIST) {
        chimera_seed_finish(ctx, error_code);
        return;
    }
    chimera_seed_finish(ctx, CHIMERA_VFS_OK);
} /* chimera_seed_badlink_cb */

/* The in-subfolder "SymlinkTest/link" was created; now make the dangling
 * root-level "badlink". */
static void
chimera_seed_dirlink_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_EEXIST) {
        chimera_seed_finish(ctx, error_code);
        return;
    }

    chimera_vfs_symlink_at(ctx->thread, chimera_vfs_get_server_cred(),
                           ctx->root_oh, "badlink", 7,
                           "nonexistent", 11, NULL,
                           0, 0, 0, chimera_seed_badlink_cb, ctx);
} /* chimera_seed_dirlink_cb */

/* The "SymlinkTest" directory is open; create the symlink "link" inside it. */
static void
chimera_seed_diropen_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_seed_finish(ctx, error_code);
        return;
    }

    ctx->dir_oh = oh;

    chimera_vfs_symlink_at(ctx->thread, chimera_vfs_get_server_cred(),
                           ctx->dir_oh, "link", 4,
                           "target", 6, NULL,
                           0, 0, 0, chimera_seed_dirlink_cb, ctx);
} /* chimera_seed_diropen_cb */

/* The "SymlinkTest" directory was created (or already existed); open it. */
static void
chimera_seed_mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK || attr->va_fh_len == 0) {
        chimera_seed_finish(ctx, error_code != CHIMERA_VFS_OK ? error_code : CHIMERA_VFS_EIO);
        return;
    }

    memcpy(ctx->dir_fh, attr->va_fh, attr->va_fh_len);
    ctx->dir_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(ctx->thread, chimera_vfs_get_server_cred(),
                        ctx->dir_fh, ctx->dir_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_seed_diropen_cb, ctx);
} /* chimera_seed_mkdir_cb */

static void
chimera_seed_rootopen_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;
    struct chimera_vfs_attrs set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_seed_finish(ctx, error_code);
        return;
    }

    ctx->root_oh = oh;

    memset(&set_attr, 0, sizeof(set_attr));
    set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    set_attr.va_mode = S_IFDIR | 0755;
    set_attr.va_uid  = 0;
    set_attr.va_gid  = 0;

    chimera_vfs_mkdir_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->root_oh,
                         "SymlinkTest", 11, &set_attr,
                         CHIMERA_VFS_ATTR_FH, 0, 0,
                         chimera_seed_mkdir_cb, ctx);
} /* chimera_seed_rootopen_cb */

static void
chimera_seed_mounted_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_seed_ctx  *ctx = private_data;
    struct chimera_vfs_mount *m;

    if (status != CHIMERA_VFS_OK) {
        ctx->status = status;
        ctx->done   = 1;
        return;
    }

    m = chimera_vfs_mount_table_find_exact(ctx->vfs->mount_table,
                                           CHIMERA_SEED_TMP_NAME,
                                           strlen(CHIMERA_SEED_TMP_NAME));
    if (!m) {
        chimera_seed_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    chimera_vfs_open_fh(thread, chimera_vfs_get_server_cred(),
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_seed_rootopen_cb, ctx);
} /* chimera_seed_mounted_cb */

SYMBOL_EXPORT int
chimera_server_seed_symlinks(
    struct chimera_server *server,
    const char            *module_name,
    const char            *module_path)
{
    struct evpl            *evpl;
    struct chimera_seed_ctx ctx;

    if (!module_name) {
        return -1;
    }

    memset(&ctx, 0, sizeof(ctx));

    evpl       = evpl_create(NULL);
    ctx.vfs    = server->vfs;
    ctx.thread = chimera_vfs_thread_init(evpl, server->vfs);
    ctx.status = CHIMERA_VFS_OK;

    chimera_vfs_mount_detached(ctx.thread, chimera_vfs_get_server_cred(),
                               CHIMERA_SEED_TMP_NAME, module_name, module_path, NULL,
                               chimera_seed_mounted_cb, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx.thread);
    evpl_destroy(evpl);

    return ctx.status == CHIMERA_VFS_OK ? 0 : -1;
} /* chimera_server_seed_symlinks */

/* ---- test-only MS-FSA fixture seeding -------------------------------------
 * The WPTS MS-FSA suite opens two fixtures it expects to pre-exist on the share:
 *   <root>/ExistingFolder    (a directory)
 *   <root>/ExistingFile.txt  (a regular file)
 * Seed them at startup (memfs shares one tree across mounts), gated by env
 * CHIMERA_SMB_SEED_FSA naming the backend module.  Best-effort; reuses the
 * symlink seed's transient-mount/evpl_continue drive loop. */

static void
chimera_seed_fsa_fileopen_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;

    if (oh) {
        chimera_vfs_release(ctx->thread, oh);
    }
    chimera_seed_finish(ctx, (error_code == CHIMERA_VFS_OK ||
                              error_code == CHIMERA_VFS_EEXIST) ?
                        CHIMERA_VFS_OK : error_code);
} /* chimera_seed_fsa_fileopen_cb */

static void
chimera_seed_fsa_mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;
    struct chimera_vfs_attrs file_attr;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_EEXIST) {
        chimera_seed_finish(ctx, error_code);
        return;
    }

    memset(&file_attr, 0, sizeof(file_attr));
    file_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    file_attr.va_mode = S_IFREG | 0644;
    file_attr.va_uid  = 0;
    file_attr.va_gid  = 0;

    chimera_vfs_open_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->root_oh,
                        "ExistingFile.txt", 16,
                        CHIMERA_VFS_OPEN_CREATE, &file_attr,
                        CHIMERA_VFS_ATTR_FH, 0, 0,
                        chimera_seed_fsa_fileopen_cb, ctx);
} /* chimera_seed_fsa_mkdir_cb */

static void
chimera_seed_fsa_rootopen_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_seed_ctx *ctx = private_data;
    struct chimera_vfs_attrs set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_seed_finish(ctx, error_code);
        return;
    }

    ctx->root_oh = oh;

    memset(&set_attr, 0, sizeof(set_attr));
    set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    set_attr.va_mode = S_IFDIR | 0755;
    set_attr.va_uid  = 0;
    set_attr.va_gid  = 0;

    chimera_vfs_mkdir_at(ctx->thread, chimera_vfs_get_server_cred(), ctx->root_oh,
                         "ExistingFolder", 14, &set_attr,
                         CHIMERA_VFS_ATTR_FH, 0, 0,
                         chimera_seed_fsa_mkdir_cb, ctx);
} /* chimera_seed_fsa_rootopen_cb */

static void
chimera_seed_fsa_mounted_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_seed_ctx  *ctx = private_data;
    struct chimera_vfs_mount *m;

    if (status != CHIMERA_VFS_OK) {
        ctx->status = status;
        ctx->done   = 1;
        return;
    }

    m = chimera_vfs_mount_table_find_exact(ctx->vfs->mount_table,
                                           CHIMERA_SEED_TMP_NAME,
                                           strlen(CHIMERA_SEED_TMP_NAME));
    if (!m) {
        chimera_seed_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    chimera_vfs_open_fh(thread, chimera_vfs_get_server_cred(),
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        chimera_seed_fsa_rootopen_cb, ctx);
} /* chimera_seed_fsa_mounted_cb */

SYMBOL_EXPORT int
chimera_server_seed_fsa(
    struct chimera_server *server,
    const char            *module_name,
    const char            *module_path)
{
    struct evpl            *evpl;
    struct chimera_seed_ctx ctx;

    if (!module_name) {
        return -1;
    }

    memset(&ctx, 0, sizeof(ctx));

    evpl       = evpl_create(NULL);
    ctx.vfs    = server->vfs;
    ctx.thread = chimera_vfs_thread_init(evpl, server->vfs);
    ctx.status = CHIMERA_VFS_OK;

    chimera_vfs_mount_detached(ctx.thread, chimera_vfs_get_server_cred(),
                               CHIMERA_SEED_TMP_NAME, module_name, module_path, NULL,
                               chimera_seed_fsa_mounted_cb, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx.thread);
    evpl_destroy(evpl);

    return ctx.status == CHIMERA_VFS_OK ? 0 : -1;
} /* chimera_server_seed_fsa */

static void
chimera_server_umount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->done   = 1;
    ctx->status = status;
} /* chimera_server_umount_callback */

SYMBOL_EXPORT int
chimera_server_unmount(
    struct chimera_server *server,
    const char            *mount_path)
{
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;
    struct mount_ctx           ctx = { .done = 0, .status = 0 };

    evpl = evpl_create(NULL);

    thread = chimera_vfs_thread_init(evpl, server->vfs);

    chimera_vfs_umount(thread, NULL, mount_path, chimera_server_umount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(thread);

    evpl_destroy(evpl);

    return ctx.status;
} /* chimera_server_unmount */

SYMBOL_EXPORT int
chimera_server_pnfs_resolve(struct chimera_server *server)
{
    struct chimera_vfs *vfs = server->vfs;
    int                 n   = chimera_vfs_pnfs_num_devices(vfs);
    int                 i, resolved = 0;

    for (i = 0; i < n; i++) {
        struct chimera_vfs_ds    *ds = chimera_vfs_pnfs_get_device(vfs, i);
        struct chimera_vfs_mount *m;
        const char               *bpath = ds->backing_path;

        /* Mounts are registered with leading slashes stripped (see
         * chimera_vfs_mount), so normalize the configured backing path the same
         * way -- otherwise "/ds0" only prefix-matches the empty-path root. */
        while (*bpath == '/') {
            bpath++;
        }

        /* Startup callers need not be registered RCU readers.  Hold the
         * writer mutex until the backing root and mount metadata are copied,
         * so a concurrent unmount cannot retire the mount under us. */
        evpl_mutex_lock(&vfs->mount_table->lock);
        m = chimera_vfs_mount_table_find_by_path_protected(vfs->mount_table,
                                                           bpath,
                                                           strlen(bpath));
        if (!m) {
            evpl_mutex_unlock(&vfs->mount_table->lock);
            chimera_server_error(
                "pNFS data server %d: backing mount '%s' not found (mount it via the nfs module)",
                i, ds->backing_path);
            continue;
        }

        chimera_vfs_pnfs_set_device_root(vfs, i, m->root_fh, m->root_fh_len);

        /* If the backing mount is not the nfs proxy, the data server is local
         * to this node: this server itself serves the backing handle, so the
         * handle handed to the client is the backing handle as-is (no proxy
         * wrapper to strip).  See chimera_nfs4_encode_ff_layout callers. */
        ds->backing_local = (m->module &&
                             m->module->fh_magic != CHIMERA_VFS_FH_MAGIC_NFS) ? 1 : 0;

        chimera_server_info(
            "pNFS data server %d backing root resolved via '%s' (module=%s path=%s root_fh_len=%d local=%d)",
            i, ds->backing_path,
            m->module ? m->module->name : "?",
            m->path ? m->path : "?", m->root_fh_len, ds->backing_local);
        evpl_mutex_unlock(&vfs->mount_table->lock);
        resolved++;
    }

    return (resolved == n) ? 0 : -1;
} /* chimera_server_pnfs_resolve */

SYMBOL_EXPORT int
chimera_server_create_bucket(
    struct chimera_server *server,
    const char            *bucket_name,
    const char            *bucket_path)
{
    if (!server->s3_shared) {
        return -1;
    }

    chimera_s3_add_bucket(server->s3_shared, bucket_name, bucket_path);

    return 0;
} /* chimera_server_create_bucket */

SYMBOL_EXPORT int
chimera_server_set_s3_bucket_root(
    struct chimera_server *server,
    const char            *bucket_root_path)
{
    if (!server->s3_shared) {
        return -1;
    }

    chimera_s3_set_bucket_root(server->s3_shared, bucket_root_path);

    return 0;
} /* chimera_server_set_s3_bucket_root */

SYMBOL_EXPORT int
chimera_server_create_share(
    struct chimera_server *server,
    const char            *share_name,
    const char            *share_path,
    int                    continuous_availability)
{
    if (!server->smb_shared) {
        return -1;
    }

    chimera_smb_add_share(server->smb_shared, share_name, share_path,
                          continuous_availability);

    return 0;
} /* chimera_server_create_share */

SYMBOL_EXPORT int
chimera_server_create_fuse_mount(
    struct chimera_server *server,
    const char            *mountpoint,
    const char            *path,
    const char            *options)
{
#ifdef __linux__
    if (!server->fuse_shared) {
        return -1;
    }

    return chimera_fuse_add_mount(server->fuse_shared, mountpoint, path, options);
#else  /* ifdef __linux__ */
    (void) server;
    (void) mountpoint;
    (void) path;
    (void) options;

    return -1;
#endif /* ifdef __linux__ */
} /* chimera_server_create_fuse_mount */

SYMBOL_EXPORT int
chimera_server_create_fuse_synthetic_mount(
    struct chimera_server *server,
    const char            *path,
    int                    fd)
{
#ifdef __linux__
    if (!server->fuse_shared) {
        return -1;
    }

    return chimera_fuse_add_synthetic_mount(server->fuse_shared, path, fd);
#else  /* ifdef __linux__ */
    (void) server;
    (void) path;
    (void) fd;

    return -1;
#endif /* ifdef __linux__ */
} /* chimera_server_create_fuse_synthetic_mount */

SYMBOL_EXPORT int
chimera_server_share_set_access_based_enum(
    struct chimera_server *server,
    const char            *share_name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_share_set_access_based_enum(server->smb_shared,
                                                   share_name);
} /* chimera_server_share_set_access_based_enum */

SYMBOL_EXPORT int
chimera_server_share_set_encrypt_data(
    struct chimera_server *server,
    const char            *share_name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_share_set_encrypt_data(server->smb_shared,
                                              share_name);
} /* chimera_server_share_set_encrypt_data */

SYMBOL_EXPORT int
chimera_server_share_set_force_level2_oplock(
    struct chimera_server *server,
    const char            *share_name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_share_set_force_level2_oplock(server->smb_shared,
                                                     share_name);
} /* chimera_server_share_set_force_level2_oplock */

SYMBOL_EXPORT int
chimera_server_create_export(
    struct chimera_server                *server,
    const char                           *name,
    const char                           *path,
    uint32_t                              export_id,
    const struct chimera_nfs_export_opts *opts)
{
    if (!server->nfs_shared) {
        return -1;
    }

    return chimera_nfs_add_export(server->nfs_shared, name, path, export_id,
                                  opts);
} /* chimera_server_create_export */

SYMBOL_EXPORT int
chimera_server_export_set_options(
    struct chimera_server *server,
    const char            *name,
    uint32_t               access,
    uint32_t               squash,
    uint32_t               anonuid,
    uint32_t               anongid)
{
    if (!server->nfs_shared) {
        return -1;
    }

    return chimera_nfs_export_set_options(server->nfs_shared, name, access,
                                          squash, anonuid, anongid);
} /* chimera_server_export_set_options */

SYMBOL_EXPORT int
chimera_server_export_set_sec(
    struct chimera_server *server,
    const char            *name,
    uint32_t               sec_allowed)
{
    if (!server->nfs_shared) {
        return -1;
    }

    return chimera_nfs_export_set_sec(server->nfs_shared, name, sec_allowed);
} /* chimera_server_export_set_sec */

static void
chimera_server_thread_shutdown(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_thread *thread = data;
    struct chimera_server *server = thread->server;
    int                    i;

    /* Drain VFS thread first to ensure all in-flight operations complete
     * before we destroy the protocol threads (NFS/RPC2 connections).
     * This prevents use-after-free when VFS callbacks try to send RPC replies
     * on already-destroyed connections. */
    chimera_vfs_thread_drain(thread->vfs_thread);

    for (i = 0; i < server->num_protocols; i++) {
        if (!thread->protocol_private[i]) {
            continue;
        }
        server->protocols[i]->thread_destroy(thread->protocol_private[i]);
    }

    if (server->mgmt) {
        server->mgmt->thread_destroy(thread->mgmt_thread);
    }

    /* Protocol thread_destroy can issue fresh VFS operations (e.g. the SMB
     * durable/persistent handle drain releases parked opens, NFS releases
     * state).  For a CAP_BLOCKING backend like cairn those are posted to a
     * shared delegation thread and completed asynchronously back to *this*
     * vfs_thread's doorbell.  Drain again so every such late request lands
     * before we free the vfs_thread below -- otherwise the delegation thread
     * rings (and writes to) an already-closed doorbell and dereferences the
     * freed thread, aborting the server.  The delegation pool is still alive
     * here; it is not torn down until chimera_vfs_destroy, after every protocol
     * thread has joined. */
    chimera_vfs_thread_drain(thread->vfs_thread);

    evpl_remove_timer(evpl, &thread->watchdog);

    /* Unregister from the tracer (drains this thread's remaining spans) before
     * tearing down its VFS thread. */
    chimera_tracing_thread_unregister();

    chimera_vfs_thread_destroy(thread->vfs_thread);
    free(thread);
} /* chimera_server_thread_shutdown */

SYMBOL_EXPORT struct chimera_server *
chimera_server_init_with_mgmt(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics,
    const struct chimera_server_mgmt   *mgmt)
{
    struct chimera_server *server;
    int                    i;

#ifndef _WIN32
    struct rlimit          rl;
#endif /* ifndef _WIN32 */

    if (!config) {
        config = chimera_server_config_init();
    }

    chimera_log_init();

#ifndef _WIN32
    /* Need to set the filedescriptor limits */
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        if (rl.rlim_cur < config->max_open_files) {
            rl.rlim_cur = config->max_open_files;
            if (rl.rlim_cur > rl.rlim_max) {
                rl.rlim_max = rl.rlim_cur;
            }
            if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
                chimera_server_error("Failed to set file descriptor limit to %ld: %s",
                                     rl.rlim_cur, strerror(errno));
            } else {
                chimera_server_info("Setting file descriptor limit to %ld", rl.rlim_cur);
            }
        } else {
            chimera_server_info("File descriptor limit is sufficient: %ld", rl.rlim_cur);
        }
    } else {
        chimera_server_error("Failed to get file descriptor limit: %s", strerror(errno));
    }

#endif /* ifndef _WIN32 */

    server = calloc(1, sizeof(*server));

    server->config = config;

    evpl_mutex_init(&server->lock, NULL);
    evpl_cond_init(&server->all_threads_online, NULL);

    chimera_server_info("Initializing VFS...");
    server->vfs = chimera_vfs_init(config->sync_delegation ? config->sync_delegation_threads : 0,
                                   config->async_delegation ? config->async_delegation_threads : 0,
                                   config->modules,
                                   config->num_modules,
                                   config->kv_module,
                                   config->cache_ttl,
                                   config->attr_cache_enabled,
                                   config->name_cache_enabled,
                                   config->rcu_reclaim_threads,
                                   metrics);

    /* Propagate the common TCP flavor so VFS client modules (e.g. nfs)
     * open outbound connections with the same transport. */
    chimera_vfs_set_tcp_flavor(server->vfs, config->tcp_flavor);

    /* Tell the VFS whether any caching protocol is enabled, so its
     * remove/rename paths know to resolve a by-name victim's FH and recall a
     * cross-protocol holder before the namespace change (see
     * chimera_vfs_remove_at).  When none are enabled the lookup is skipped.
     * FUSE counts: the kernel caches dentries and attributes on its own, and
     * the victim FH is also what invalidates the clobbered inode's cached
     * attributes -- without it an fd held across a rename-over keeps reading
     * the pre-rename link count. */
    chimera_vfs_set_umount_timeout(server->vfs, config->umount_timeout_ms);

    chimera_vfs_set_caching_enabled(server->vfs,
                                    config->nfs4_delegations ||
                                    config->smb_leases ||
                                    config->smb_oplocks ||
                                    config->fuse_enabled);

    /* Enable the pNFS feature whenever configured.  Orchestrated flex-files
     * needs a data-server table (below); a layout-sourcing backend (e.g. diskfs
     * block mode) produces its own layouts and needs no data servers, so the
     * feature is enabled with an empty table. */
    if (config->pnfs_enabled) {
        chimera_vfs_pnfs_set_enabled(server->vfs, 1);
        for (i = 0; i < config->pnfs_num_ds; i++) {
            chimera_vfs_pnfs_add_device(server->vfs,
                                        config->pnfs_ds[i].netid,
                                        config->pnfs_ds[i].uaddr,
                                        config->pnfs_ds[i].rdma_uaddr,
                                        config->pnfs_ds[i].backing_path,
                                        config->pnfs_ds[i].version,
                                        config->pnfs_ds[i].minorversion);
        }
        chimera_server_info("pNFS enabled with %d data server(s)", config->pnfs_num_ds);
    }

    chimera_server_info("Initializing protocols...");
    server->protocols[server->num_protocols++] = &nfs_protocol;

    /* A pNFS data server speaks only NFS; skipping SMB/S3 also frees their
     * fixed listen ports so it can share a host with the metadata server. */
    if (!config->nfs_data_server) {
        server->protocols[server->num_protocols++] = &smb_protocol;
        server->protocols[server->num_protocols++] = &s3_protocol;
    }

    /* FUSE binds no ports, so it registers regardless of data-server mode.
     * Registered last so the fixed-index shared-pointer assignments below
     * stay valid; its index is captured because it moves with the branch
     * above.  Linux-only: the raw /dev/fuse ABI has no counterpart
     * elsewhere. */
    int fuse_idx = -1;
#ifdef __linux__
    fuse_idx                                   = server->num_protocols;
    server->protocols[server->num_protocols++] = &fuse_protocol;
#endif /* ifdef __linux__ */

    for (i = 0; i < server->num_protocols; i++) {
        server->protocol_private[i] = server->protocols[i]->init(config, server->vfs, metrics);
    }

    server->nfs_shared = server->protocol_private[0];
    if (!config->nfs_data_server) {
        server->smb_shared = server->protocol_private[1];
        server->s3_shared  = server->protocol_private[2];
    }

    if (fuse_idx >= 0) {
        server->fuse_shared = server->protocol_private[fuse_idx];
    }

    server->mgmt = mgmt;
    if (mgmt) {
        chimera_server_info("Initializing the management plane...");
        server->mgmt_private = mgmt->init(config, server, server->vfs, metrics);
    }

    return server;
} /* chimera_server_init_with_mgmt */

SYMBOL_EXPORT void
chimera_server_start(struct chimera_server *server)
{
    int i;

    server->pool = evpl_threadpool_create(NULL,
                                          server->config->core_threads,
                                          chimera_server_thread_init,
                                          chimera_server_thread_shutdown,
                                          server);

    chimera_server_info("Waiting for %d threads to start...", server->config->core_threads);

    evpl_mutex_lock(&server->lock);
    while (server->threads_online < server->config->core_threads) {
        evpl_cond_wait(&server->all_threads_online, &server->lock);
    }
    evpl_mutex_unlock(&server->lock);

    for (i = 0; i < server->num_protocols; i++) {
        if (!server->protocol_private[i]) {
            continue;
        }
        server->protocols[i]->start(server->protocol_private[i]);
    }

    if (server->mgmt) {
        server->mgmt->start(server->mgmt_private);
    }

    chimera_server_info("Server is ready.");
} /* chimera_server_start */

SYMBOL_EXPORT void
chimera_server_destroy(struct chimera_server *server)
{
    int i;

    for (i = 0; i < server->num_protocols; i++) {
        if (!server->protocol_private[i]) {
            continue;
        }
        server->protocols[i]->stop(server->protocol_private[i]);
    }

    if (server->mgmt) {
        server->mgmt->stop(server->mgmt_private);
    }

    evpl_threadpool_destroy(server->pool);

    /* Destroy protocols before VFS so they can release any open handles */
    for (i = 0; i < server->num_protocols; i++) {
        if (!server->protocol_private[i]) {
            continue;
        }
        server->protocols[i]->destroy(server->protocol_private[i]);
    }

    chimera_vfs_destroy(server->vfs);

    if (server->mgmt) {
        server->mgmt->destroy(server->mgmt_private);
    }

    free((void *) server->config);
    free(server);
} /* chimera_server_destroy */

SYMBOL_EXPORT int
chimera_server_add_user(
    struct chimera_server *server,
    const char            *username,
    const char            *password,
    const char            *smbpasswd,
    const char            *sid,
    uint32_t               uid,
    uint32_t               gid,
    uint32_t               ngids,
    const uint32_t        *gids,
    int                    pinned)
{
    return chimera_vfs_add_user(server->vfs, username, password, smbpasswd, sid,
                                uid, gid, ngids, gids, pinned);
} /* chimera_server_add_user */

SYMBOL_EXPORT int
chimera_server_remove_user(
    struct chimera_server *server,
    const char            *username)
{
    return chimera_vfs_remove_user(server->vfs, username);
} /* chimera_server_remove_user */

SYMBOL_EXPORT int
chimera_server_add_group(
    struct chimera_server *server,
    const char            *groupname,
    const char            *sid,
    uint32_t               gid,
    int                    pinned)
{
    return chimera_vfs_add_group(server->vfs, groupname, sid, gid, pinned);
} /* chimera_server_add_group */

SYMBOL_EXPORT int
chimera_server_remove_group(
    struct chimera_server *server,
    const char            *groupname)
{
    return chimera_vfs_remove_group(server->vfs, groupname);
} /* chimera_server_remove_group */

SYMBOL_EXPORT const struct chimera_vfs_user *
chimera_server_get_user(
    struct chimera_server *server,
    const char            *username)
{
    return chimera_vfs_lookup_user_by_name(server->vfs, username);
} /* chimera_server_get_user */

SYMBOL_EXPORT void
chimera_server_iterate_users(
    struct chimera_server         *server,
    chimera_server_user_iterate_cb callback,
    void                          *data)
{
    chimera_vfs_iterate_builtin_users(server->vfs, callback, data);
} /* chimera_server_iterate_users */

SYMBOL_EXPORT int
chimera_server_remove_export(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->nfs_shared) {
        return -1;
    }

    return chimera_nfs_remove_export(server->nfs_shared, name);
} /* chimera_server_remove_export */

SYMBOL_EXPORT const struct chimera_nfs_export *
chimera_server_get_export(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->nfs_shared) {
        return NULL;
    }

    return chimera_nfs_get_export(server->nfs_shared, name);
} /* chimera_server_get_export */

SYMBOL_EXPORT void
chimera_server_iterate_exports(
    struct chimera_server           *server,
    chimera_server_export_iterate_cb callback,
    void                            *data)
{
    if (!server->nfs_shared) {
        return;
    }

    chimera_nfs_iterate_exports(server->nfs_shared, callback, data);
} /* chimera_server_iterate_exports */

SYMBOL_EXPORT int
chimera_server_remove_share(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->smb_shared) {
        return -1;
    }

    return chimera_smb_remove_share(server->smb_shared, name);
} /* chimera_server_remove_share */

SYMBOL_EXPORT const struct chimera_smb_share *
chimera_server_get_share(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->smb_shared) {
        return NULL;
    }

    return chimera_smb_get_share(server->smb_shared, name);
} /* chimera_server_get_share */

SYMBOL_EXPORT void
chimera_server_iterate_shares(
    struct chimera_server          *server,
    chimera_server_share_iterate_cb callback,
    void                           *data)
{
    if (!server->smb_shared) {
        return;
    }

    chimera_smb_iterate_shares(server->smb_shared, callback, data);
} /* chimera_server_iterate_shares */

SYMBOL_EXPORT int
chimera_server_remove_bucket(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->s3_shared) {
        return -1;
    }

    return chimera_s3_remove_bucket(server->s3_shared, name);
} /* chimera_server_remove_bucket */

SYMBOL_EXPORT const struct s3_bucket *
chimera_server_get_bucket(
    struct chimera_server *server,
    const char            *name)
{
    if (!server->s3_shared) {
        return NULL;
    }

    return chimera_s3_get_bucket(server->s3_shared, name);
} /* chimera_server_get_bucket */

SYMBOL_EXPORT void
chimera_server_release_bucket(struct chimera_server *server)
{
    if (!server->s3_shared) {
        return;
    }

    chimera_s3_release_bucket(server->s3_shared);
} /* chimera_server_release_bucket */

SYMBOL_EXPORT void
chimera_server_iterate_buckets(
    struct chimera_server           *server,
    chimera_server_bucket_iterate_cb callback,
    void                            *data)
{
    if (!server->s3_shared) {
        return;
    }

    chimera_s3_iterate_buckets(server->s3_shared, callback, data);
} /* chimera_server_iterate_buckets */

struct mount_snapshot {
    struct mount_snapshot *next;
    char                  *path;
    char                  *module;
    char                  *module_path;
    char                  *options;
};

SYMBOL_EXPORT void
chimera_server_iterate_mounts(
    struct chimera_server          *server,
    chimera_server_mount_iterate_cb callback,
    void                           *data)
{
    struct chimera_vfs_mount_table *table = server->vfs->mount_table;
    struct mount_snapshot          *head = NULL, **tail = &head, *item;
    uint32_t                        i;
    int                             stopped = 0;

    /* This public API also runs on application threads without an RCU
     * registration. Snapshot the metadata under the writer lock, then invoke
     * callbacks unlocked so they can call back into the server safely. */
    evpl_mutex_lock(&table->lock);
    for (i = 0; i < table->num_buckets; i++) {
        struct chimera_vfs_mount_table_entry *entry;

        for (entry = table->buckets[i]; entry; entry = entry->next) {
            struct chimera_vfs_mount *mount = entry->mount;

            /* The built-in rootfs and detached mounts are VFS-internal: no
             * one configured them and there is nothing to manage. */
            if (mount->detached || mount->builtin) {
                continue;
            }

            item = calloc(1, sizeof(*item));
            if (!item) {
                abort();
            }
            item->path        = strdup(mount->path);
            item->module      = strdup(mount->module ? mount->module->name : "");
            item->module_path = strdup(mount->module_path ? mount->module_path : "");
            item->options     = mount->options ? strdup(mount->options) : NULL;
            if (!item->path || !item->module || !item->module_path ||
                (mount->options && !item->options)) {
                abort();
            }
            *tail = item;
            tail  = &item->next;
        }
    }
    evpl_mutex_unlock(&table->lock);

    while (head) {
        item = head;
        head = item->next;
        if (!stopped) {
            stopped = callback(item->path, item->module, item->module_path,
                               item->options, data);
        }
        free(item->path);
        free(item->module);
        free(item->module_path);
        free(item->options);
        free(item);
    }
} /* chimera_server_iterate_mounts */

struct mount_in_use_ctx {
    struct chimera_vfs *vfs;
    const char         *target;
    int                 target_len;
    int                 in_use;
};

/*
 * Resolve the path of a share/export/bucket to its owning mount and flag a
 * match against the target mount.  Paths are normalized (leading slashes
 * stripped) the same way chimera_vfs_mount registers them.  Returns non-zero
 * once a match is found to stop the enclosing iteration early.
 */
static int
mount_in_use_check_path(
    struct mount_in_use_ctx *ctx,
    const char              *path)
{
    struct chimera_vfs_mount *mount;

    while (*path == '/') {
        path++;
    }

    /* Like mount enumeration, this public query can originate on a thread
     * with no VFS context. Consume the mount pointer while holding the lock. */
    evpl_mutex_lock(&ctx->vfs->mount_table->lock);
    mount = chimera_vfs_mount_table_find_by_path_protected(ctx->vfs->mount_table,
                                                           path, strlen(path));

    if (mount &&
        mount->pathlen == (uint32_t) ctx->target_len &&
        memcmp(mount->path, ctx->target, ctx->target_len) == 0) {
        ctx->in_use = 1;
    }

    evpl_mutex_unlock(&ctx->vfs->mount_table->lock);
    return ctx->in_use;
} /* mount_in_use_check_path */

static int
mount_in_use_share_cb(
    const struct chimera_smb_share *share,
    void                           *data)
{
    return mount_in_use_check_path(data, chimera_smb_share_get_path(share));
} /* mount_in_use_share_cb */

static int
mount_in_use_export_cb(
    const struct chimera_nfs_export *export,
    void                            *data)
{
    return mount_in_use_check_path(data, chimera_nfs_export_get_path(export));
} /* mount_in_use_export_cb */

static int
mount_in_use_bucket_cb(
    const struct s3_bucket *bucket,
    void                   *data)
{
    return mount_in_use_check_path(data, chimera_s3_bucket_get_path(bucket));
} /* mount_in_use_bucket_cb */

SYMBOL_EXPORT int
chimera_server_mount_in_use(
    struct chimera_server *server,
    const char            *mount_path)
{
    struct mount_in_use_ctx ctx;
    const char             *target = mount_path;

    while (*target == '/') {
        target++;
    }

    ctx.vfs        = server->vfs;
    ctx.target     = target;
    ctx.target_len = strlen(target);
    ctx.in_use     = 0;

    if (server->smb_shared) {
        chimera_smb_iterate_shares(server->smb_shared, mount_in_use_share_cb, &ctx);
    }

    if (!ctx.in_use && server->nfs_shared) {
        chimera_nfs_iterate_exports(server->nfs_shared, mount_in_use_export_cb, &ctx);
    }

    if (!ctx.in_use && server->s3_shared) {
        chimera_s3_iterate_buckets(server->s3_shared, mount_in_use_bucket_cb, &ctx);
    }

    return ctx.in_use;
} /* chimera_server_mount_in_use */

SYMBOL_EXPORT struct chimera_vfs *
chimera_server_get_vfs(struct chimera_server *server)
{
    return server->vfs;
} /* chimera_server_get_vfs */

SYMBOL_EXPORT int
chimera_server_add_s3_cred(
    struct chimera_server *server,
    const char            *access_key,
    const char            *secret_key,
    const char            *username,
    const char            *canon_id,
    const char            *display_name,
    int                    pinned)
{
    const struct chimera_vfs_user *user;
    uint32_t                       uid = 0, gid = 0, gids[CHIMERA_VFS_CRED_MAX_GIDS];
    uint32_t                       ngids        = 0;
    int                            has_identity = 0;

    if (!server->s3_shared) {
        return -1;
    }

    /* Resolve the key's user through the same identity store the other
     * protocols authenticate against, so an S3 key and an SMB or NFS login for
     * the same person act as one uid.  An unresolvable name is a configuration
     * error and must not silently fall back to a privileged identity, so it is
     * reported and left unbound (anonymous). */
    if (username && *username) {
        user = chimera_server_get_user(server, username);

        if (user) {
            uid   = user->uid;
            gid   = user->gid;
            ngids = user->ngids;
            if (ngids > CHIMERA_VFS_CRED_MAX_GIDS) {
                ngids = CHIMERA_VFS_CRED_MAX_GIDS;
            }
            memcpy(gids, user->gids, ngids * sizeof(*gids));
            has_identity = 1;
        } else {
            chimera_server_error(
                "S3 access key %s names unknown user \"%s\"; leaving it unbound",
                access_key, username);
        }
    }

    /* A key bound to a user displays as that user by default: the operator
     * already named the principal there, and echoing the access key would leak
     * a credential into every ACL reply. */
    return chimera_s3_add_cred(server->s3_shared, access_key, secret_key,
                               has_identity, uid, gid, ngids, gids,
                               canon_id,
                               (display_name && *display_name) ? display_name : username,
                               pinned);
} /* chimera_server_add_s3_cred */

SYMBOL_EXPORT int
chimera_server_remove_s3_cred(
    struct chimera_server *server,
    const char            *access_key)
{
    return chimera_s3_remove_cred(server->s3_shared, access_key);
} /* chimera_server_remove_s3_cred */

SYMBOL_EXPORT void
chimera_server_advance_s3_cred_clock(
    struct chimera_server *server,
    int64_t                seconds)
{
    chimera_s3_advance_cred_clock(server->s3_shared, seconds);
} /* chimera_server_advance_s3_cred_clock */
