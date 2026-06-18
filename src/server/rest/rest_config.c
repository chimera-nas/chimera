// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "server/nfs/nfs.h"
#include "server/smb/smb.h"
#include "server/s3/s3.h"
#include "rest_internal.h"

/*
 * GET /api/v1/config
 *
 * Reconstructs a JSON document compatible with the chimera.json file format
 * from live runtime state.  The "users" section is intentionally omitted (it
 * would expose passwords) and the "server" section is omitted (a partially
 * reconstructed server section is more confusing than useful).  The internal
 * "root" pseudo-mount is skipped.
 */

static int
config_mount_callback(
    const char *mount_path,
    const char *module_name,
    const char *module_path,
    void       *data)
{
    json_t *mounts = data;
    json_t *obj;

    /* The "root" pseudo-mount is a Chimera-internal entry, not part of the
     * user-facing configuration. */
    if (strcmp(module_name, "root") == 0) {
        return 0;
    }

    obj = json_object();
    json_object_set_new(obj, "module", json_string(module_name));
    json_object_set_new(obj, "path", json_string(module_path));

    json_object_set_new(mounts, mount_path, obj);

    return 0;
} /* config_mount_callback */

static int
config_export_callback(
    const struct chimera_nfs_export *export,
    void                            *data)
{
    json_t *exports = data;
    json_t *obj;

    obj = json_object();
    json_object_set_new(obj, "path",
                        json_string(chimera_nfs_export_get_path(export)));
    json_object_set_new(obj, "options",
                        json_string(chimera_nfs_export_get_options(export) &
                                    CHIMERA_NFS_EXPORT_OPT_RO ? "ro" : "rw"));
    switch (chimera_nfs_export_get_squash(export)) {
        case CHIMERA_NFS_SQUASH_ALL:
            json_object_set_new(obj, "squash", json_string("all"));
            break;
        case CHIMERA_NFS_SQUASH_NONE:
            json_object_set_new(obj, "squash", json_string("none"));
            break;
        default:
            json_object_set_new(obj, "squash", json_string("root"));
            break;
    } /* switch */
    json_object_set_new(obj, "anonuid",
                        json_integer(chimera_nfs_export_get_anonuid(export)));
    json_object_set_new(obj, "anongid",
                        json_integer(chimera_nfs_export_get_anongid(export)));

    json_object_set_new(exports, chimera_nfs_export_get_name(export), obj);

    return 0;
} /* config_export_callback */

static int
config_share_callback(
    const struct chimera_smb_share *share,
    void                           *data)
{
    json_t *shares = data;
    json_t *obj;

    obj = json_object();
    json_object_set_new(obj, "path",
                        json_string(chimera_smb_share_get_path(share)));

    json_object_set_new(shares, chimera_smb_share_get_name(share), obj);

    return 0;
} /* config_share_callback */

static int
config_bucket_callback(
    const struct s3_bucket *bucket,
    void                   *data)
{
    json_t *buckets = data;
    json_t *obj;

    obj = json_object();
    json_object_set_new(obj, "path",
                        json_string(chimera_s3_bucket_get_path(bucket)));

    json_object_set_new(buckets, chimera_s3_bucket_get_name(bucket), obj);

    return 0;
} /* config_bucket_callback */

void
chimera_rest_handle_config(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct chimera_server *server = thread->shared->server;
    json_t                *root;
    json_t                *mounts;
    json_t                *exports;
    json_t                *shares;
    json_t                *buckets;

    root = json_object();

    /* mounts (the internal "root" mount is filtered out) */
    mounts = json_object();
    chimera_server_iterate_mounts(server, config_mount_callback, mounts);
    json_object_set_new(root, "mounts", mounts);

    /* exports */
    exports = json_object();
    chimera_server_iterate_exports(server, config_export_callback, exports);
    json_object_set_new(root, "exports", exports);

    /* shares */
    shares = json_object();
    chimera_server_iterate_shares(server, config_share_callback, shares);
    json_object_set_new(root, "shares", shares);

    /* buckets (no-op when S3 is not configured) */
    buckets = json_object();
    chimera_server_iterate_buckets(server, config_bucket_callback, buckets);
    json_object_set_new(root, "buckets", buckets);

    chimera_rest_send_json(evpl, request, 200, root);
} /* chimera_rest_handle_config */
