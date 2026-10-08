// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

/* The new filesystem's case policy, from its "case=" option or the module's
 * default: mixed where the module keeps a folded-name index, else
 * sensitive.  Returns -1 for an unknown policy or one the module cannot
 * keep. */
static int
chimera_vfs_mkfs_case_policy(
    const struct chimera_vfs_module        *module,
    const struct chimera_vfs_mount_options *options,
    enum chimera_vfs_case_policy           *r_policy)
{
    int casefold = !!(module->capabilities & CHIMERA_VFS_CAP_CASEFOLD);

    *r_policy = casefold ? CHIMERA_VFS_CASE_MIXED : CHIMERA_VFS_CASE_SENSITIVE;

    for (int i = 0; i < options->num_options; i++) {
        const char                  *key   = options->options[i].key;
        const char                  *value = options->options[i].value;
        enum chimera_vfs_case_policy policy;

        if (!key || strcmp(key, "case") != 0) {
            continue;
        }

        if (value && strcmp(value, "sensitive") == 0) {
            policy = CHIMERA_VFS_CASE_SENSITIVE;
        } else if (value && strcmp(value, "mixed") == 0) {
            policy = CHIMERA_VFS_CASE_MIXED;
        } else if (value && strcmp(value, "insensitive") == 0) {
            policy = CHIMERA_VFS_CASE_INSENSITIVE;
        } else {
            chimera_vfs_error("chimera_vfs_mkfs: unknown case policy '%s' "
                              "(sensitive, mixed or insensitive)",
                              value ? value : "");
            return -1;
        }

        if (policy != CHIMERA_VFS_CASE_SENSITIVE && !casefold) {
            chimera_vfs_error("chimera_vfs_mkfs: module %s matches names "
                              "exactly; case=%s needs case-insensitive lookup",
                              module->name, value);
            return -1;
        }

        *r_policy = policy;
    }

    return 0;
} /* chimera_vfs_mkfs_case_policy */

static void
chimera_vfs_mkfs_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_mkfs_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(thread, request->status, request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_mkfs_complete */

SYMBOL_EXPORT void
chimera_vfs_mkfs(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *module_name,
    const char                    *fsname,
    const char                    *options,
    chimera_vfs_mkfs_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module = NULL;
    int                         i, rc;
    struct chimera_vfs_request *request;

    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            break;
        }
    }

    if (i == CHIMERA_VFS_MAX_MODULES) {
        chimera_vfs_error("chimera_vfs_mkfs: module %s not found",
                          module_name);
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (!(module->capabilities & CHIMERA_VFS_CAP_MKFS)) {
        callback(thread, CHIMERA_VFS_ENOTSUP, private_data);
        return;
    }

    if (!fsname || !fsname[0] || strchr(fsname, '/')) {
        chimera_vfs_error("chimera_vfs_mkfs: invalid filesystem name: %s",
                          fsname ? fsname : "(null)");
        callback(thread, CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    /* Like mount, the module is known up front - no mount exists to resolve */
    request = chimera_vfs_request_alloc_with_module(thread, cred,
                                                    &module->fh_magic, 1,
                                                    chimera_vfs_hash(&module->fh_magic, 1),
                                                    module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(thread, CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    rc = chimera_vfs_parse_mount_options(options,
                                         &request->mkfs.options,
                                         request->mkfs.options_buffer,
                                         sizeof(request->mkfs.options_buffer),
                                         NULL, 0);
    if (rc) {
        chimera_vfs_error("chimera_vfs_mkfs: invalid options: %s",
                          options ? options : "(null)");
        chimera_vfs_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    if (chimera_vfs_mkfs_case_policy(module, &request->mkfs.options,
                                     &request->mkfs.case_policy)) {
        chimera_vfs_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_MKFS;
    request->complete           = chimera_vfs_mkfs_complete;
    request->mkfs.name          = fsname;
    request->mkfs.namelen       = strlen(fsname);
    request->mkfs.raw_options   = options;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mkfs */
