// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include "vfs/sdk/chimera_vfs_sdk.h"

/* Load the real host library first; do not fake exported SDK helpers. */
int
main(
    int   argc,
    char *argv[])
{
    void                      *host;
    void                      *module;
    struct chimera_vfs_module *descriptor;
    int                        status = 1;

    if (argc != 3) {
        fprintf(stderr, "usage: %s /path/to/libchimera_vfs.so /path/to/libchimera_vfs_daos.so\n", argv[0]);
        return 2;
    }
    host = dlopen(argv[1], RTLD_NOW | RTLD_GLOBAL);
    if (!host) {
        fprintf(stderr, "host load failed: %s\n", dlerror());
        return 1;
    }
    module = dlopen(argv[2], RTLD_NOW | RTLD_GLOBAL);
    if (!module) {
        fprintf(stderr, "module load failed: %s\n", dlerror());
        dlclose(host);
        return 1;
    }
    descriptor = dlsym(module, "vfs_daos");
    if (!descriptor) {
        fprintf(stderr, "missing vfs_daos export: %s\n", dlerror());
    } else if (descriptor->sdk_version != CHIMERA_VFS_SDK_VERSION) {
        fprintf(stderr, "SDK version mismatch\n");
    } else if (!descriptor->name || strcmp(descriptor->name, "daos") ||
               !descriptor->init || !descriptor->destroy ||
               !descriptor->thread_init || !descriptor->thread_destroy ||
               !descriptor->dispatch) {
        fprintf(stderr, "invalid module descriptor\n");
    } else {
        puts("vfs_daos descriptor and host symbol resolution passed");
        status = 0;
    }
    /* No init/connect is called: this is only a loader smoke check. */
    dlclose(module);
    dlclose(host);
    return status;
} /* main */
