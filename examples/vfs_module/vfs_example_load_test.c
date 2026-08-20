// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Smoke test for the out-of-tree module path: dlopen the example module
 * exactly the way chimera_vfs_init does for a module_path share, resolve
 * its vfs_<name> symbol, and verify the SDK version handshake and magic
 * reservation.  Linked against chimera_vfs so the module's references to
 * exported SDK helpers (chimera_vfs_hash) resolve at load time, as they
 * would inside the server process.
 */

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vfs/sdk/chimera_vfs_sdk.h"

int
main(
    int    argc,
    char **argv)
{
    void                      *handle;
    struct chimera_vfs_module *module;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <vfs_example.so>\n", argv[0]);
        return 1;
    }

    handle = dlopen(argv[1], RTLD_NOW | RTLD_GLOBAL);

    if (!handle) {
        fprintf(stderr, "dlopen %s failed: %s\n", argv[1], dlerror());
        return 1;
    }

    module = dlsym(handle, "vfs_example");

    if (!module) {
        fprintf(stderr, "vfs_example symbol not found: %s\n", dlerror());
        return 1;
    }

    if (module->sdk_version != CHIMERA_VFS_SDK_VERSION) {
        fprintf(stderr, "sdk_version %u != %u\n",
                module->sdk_version, CHIMERA_VFS_SDK_VERSION);
        return 1;
    }

    if (strcmp(module->name, "example") != 0) {
        fprintf(stderr, "unexpected module name %s\n", module->name);
        return 1;
    }

    if (module->fh_magic != CHIMERA_VFS_FH_MAGIC_VENDOR0) {
        fprintf(stderr, "unexpected fh_magic %u\n", module->fh_magic);
        return 1;
    }

    if (!module->dispatch) {
        fprintf(stderr, "module has no dispatch\n");
        return 1;
    }

    printf("vfs_example loaded: sdk_version=%u fh_magic=%u\n",
           module->sdk_version, module->fh_magic);
    return 0;
} /* main */
