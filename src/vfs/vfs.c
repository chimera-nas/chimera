#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "vfs.h"
#include "vfs_internal.h"
#include "vfs_root.h"
#include "common/misc.h"
#include "uthash/utlist.h"

struct chimera_vfs *
chimera_vfs_init(void)
{
    struct chimera_vfs *vfs;

    vfs = calloc(1, sizeof(*vfs));

    chimera_vfs_register(vfs, &vfs_root);

    return vfs;
} /* chimera_vfs_init */

void
chimera_vfs_destroy(struct chimera_vfs *vfs)
{
    struct chimera_vfs_module *module;
    struct chimera_vfs_share  *share;
    int                        i;

    while (vfs->shares) {
        share = vfs->shares;
        DL_DELETE(vfs->shares, share);
        free(share->name);
        free(share->path);
        free(share);
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        module->destroy(vfs->module_private[i]);
    }

    free(vfs);
} /* chimera_vfs_destroy */

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs)
{
    struct chimera_vfs_thread *thread;
    struct chimera_vfs_module *module;
    int                        i;

    thread      = calloc(1, sizeof(*thread));
    thread->vfs = vfs;

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        thread->module_private[i] = module->thread_init(
            evpl, vfs->module_private[i]);
    }

    return thread;
} /* chimera_vfs_thread_init */

void
chimera_vfs_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    int                         i;

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = thread->vfs->modules[i];

        if (!module) {
            continue;
        }

        module->thread_destroy(thread->module_private[i]);
    }

    while (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
        free(request);
    }

    free(thread);
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init();
} /* chimera_vfs_register */

int
chimera_vfs_create_share(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *share_path,
    const char         *module_path)
{
    struct chimera_vfs_share  *share;
    struct chimera_vfs_module *module;
    int                        i;

    share = calloc(1, sizeof(*share));

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            share->module = module;
            break;
        }
    }

    if (!share->module) {
        chimera_vfs_error("chimera_vfs_create_share: module %s not found",
                          module_name);
        return -1;
    }

    share->name = strdup(share_path);
    share->path = strdup(module_path);

    DL_APPEND(vfs->shares, share);
    return 0;
} /* chimera_vfs_create_share */