// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <errno.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "vfs_mount_table.h"
#include "common/macros.h"

static int
chimera_vfs_parse_mount_options(
    const char                       *options,
    struct chimera_vfs_mount_options *mount_options,
    char                             *buffer,
    int                               buffer_size)
{
    const char *p, *end, *eq;
    int         opt_idx    = 0;
    int         buf_offset = 0;
    int         key_len, value_len;

    mount_options->num_options = 0;

    if (!options || !options[0]) {
        return 0;
    }

    p = options;

    while (*p) {
        /* Skip leading whitespace */
        while (*p == ' ' || *p == '\t') {
            p++;
        }

        if (!*p) {
            break;
        }

        /* Find end of this option (comma or end of string) */
        end = p;
        while (*end && *end != ',') {
            end++;
        }

        /* Check for too many options */
        if (opt_idx >= CHIMERA_VFS_MOUNT_OPT_MAX) {
            return -EINVAL;
        }

        /* Find '=' if present */
        eq = p;
        while (eq < end && *eq != '=') {
            eq++;
        }

        if (eq == p) {
            /* Empty key */
            return -EINVAL;
        }

        key_len = eq - p;

        /* Check buffer space for key + null terminator */
        if (buf_offset + key_len + 1 > buffer_size) {
            return -EINVAL;
        }

        /* Copy key to buffer */
        memcpy(buffer + buf_offset, p, key_len);
        buffer[buf_offset + key_len]        = '\0';
        mount_options->options[opt_idx].key = buffer + buf_offset;
        buf_offset                         += key_len + 1;

        /* Check for value */
        if (eq < end && *eq == '=') {
            eq++;             /* Skip '=' */
            value_len = end - eq;

            /* Check buffer space for value + null terminator */
            if (buf_offset + value_len + 1 > buffer_size) {
                return -EINVAL;
            }

            memcpy(buffer + buf_offset, eq, value_len);
            buffer[buf_offset + value_len]        = '\0';
            mount_options->options[opt_idx].value = buffer + buf_offset;
            buf_offset                           += value_len + 1;
        } else {
            mount_options->options[opt_idx].value = NULL;
        }

        opt_idx++;

        /* Move past comma if present */
        p = end;
        if (*p == ',') {
            p++;
        }
    }

    mount_options->num_options = opt_idx;
    return 0;
} /* chimera_vfs_parse_mount_options */


static void
chimera_vfs_mount_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread   *thread = request->thread;
    struct chimera_vfs          *vfs    = thread->vfs;
    struct chimera_vfs_mount    *mount;
    chimera_vfs_mount_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    if (request->status != CHIMERA_VFS_OK) {
        callback(thread, request->status, request->proto_private_data);
        return;
    }

    mount = calloc(1, sizeof(*mount));

    mount->module        = request->mount.module;
    mount->path          = strdup(request->mount.mount_path);
    mount->pathlen       = strlen(request->mount.mount_path);
    mount->mount_private = request->mount.r_mount_private;

    /* Store the root FH (first 16 bytes is the mount_id) */
    memcpy(mount->root_fh, request->mount.r_attr.va_fh, request->mount.r_attr.va_fh_len);
    mount->root_fh_len = request->mount.r_attr.va_fh_len;

    chimera_vfs_mount_table_insert(vfs->mount_table, mount);

    callback(thread, CHIMERA_VFS_OK, request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_mount_complete */

SYMBOL_EXPORT void
chimera_vfs_mount(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    const char                    *module_name,
    const char                    *module_path,
    const char                    *options,
    chimera_vfs_mount_callback_t   callback,
    void                          *private_data)
{
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module = NULL;
    int                         i, rc;
    struct chimera_vfs_request *request;

    while (mount_path[0] == '/') {
        mount_path++;
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            break;
        }
    }

    if (i == CHIMERA_VFS_FH_MAGIC_MAX) {
        chimera_vfs_error("chimera_vfs_mount: module %s not found",
                          module_name);
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, &module->fh_magic, 1);

    /* For mount operations, the module is already known - set it directly
     * since chimera_vfs_get_module returns NULL (no mount exists yet) */
    request->module = module;

    /* Parse mount options directly into request buffer */
    rc = chimera_vfs_parse_mount_options(options,
                                         &request->mount.options,
                                         request->mount.options_buffer,
                                         sizeof(request->mount.options_buffer));
    if (rc) {
        chimera_vfs_error("chimera_vfs_mount: invalid mount options: %s",
                          options ? options : "(null)");
        chimera_vfs_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    request->opcode                   = CHIMERA_VFS_OP_MOUNT;
    request->complete                 = chimera_vfs_mount_complete;
    request->mount.path               = module_path;
    request->mount.pathlen            = strlen(module_path);
    request->mount.module             = module;
    request->mount.mount_path         = mount_path;
    request->mount.mount_pathlen      = strlen(mount_path);
    request->mount.r_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_CACHEABLE | CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_set_mask = 0;
    request->proto_callback           = callback;
    request->proto_private_data       = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mount */