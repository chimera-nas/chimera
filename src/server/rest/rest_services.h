// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* Private interface for the modules shipped with this build. Unlike the public
 * SDK, this follows Chimera's internal types and is not a third-party ABI.
 * Passing host services avoids linking a second VFS/server runtime into a DLL. */
#include "rest_internal.h"
#include "rest_auth.h"
#include "server/server.h"
#include "server/nfs/nfs.h"
#include "server/smb/smb.h"
#include "server/s3/s3.h"
#include "vfs/vfs_procs.h"
#include "vfs/sdk/vfs_cred.h"

#define CHIMERA_REST_BIND_ENTRY       "chimera_rest_module_bind_v1"
#define CHIMERA_REST_SERVICES_VERSION 1
#define CHIMERA_REST_SERVICE_FUNCTIONS(X) \
        X(chimera_nfs_export_get_access) \
        X(chimera_nfs_export_get_anongid) \
        X(chimera_nfs_export_get_anonuid) \
        X(chimera_nfs_export_get_id) \
        X(chimera_nfs_export_get_name) \
        X(chimera_nfs_export_get_path) \
        X(chimera_nfs_export_get_sec) \
        X(chimera_nfs_export_get_squash) \
        X(chimera_nfs_export_name_valid) \
        X(chimera_rest_handle_auth_login) \
        X(chimera_rest_reply) \
        X(chimera_rest_send_error) \
        X(chimera_rest_send_json) \
        X(chimera_rest_send_json_response) \
        X(chimera_s3_bucket_get_name) \
        X(chimera_s3_bucket_get_path) \
        X(chimera_server_add_user) \
        X(chimera_server_create_bucket) \
        X(chimera_server_create_export) \
        X(chimera_server_create_share) \
        X(chimera_server_get_bucket) \
        X(chimera_server_get_export) \
        X(chimera_server_get_share) \
        X(chimera_server_get_user) \
        X(chimera_server_iterate_buckets) \
        X(chimera_server_iterate_exports) \
        X(chimera_server_iterate_mounts) \
        X(chimera_server_iterate_shares) \
        X(chimera_server_iterate_users) \
        X(chimera_server_mount_in_use) \
        X(chimera_server_release_bucket) \
        X(chimera_server_remove_bucket) \
        X(chimera_server_remove_export) \
        X(chimera_server_remove_share) \
        X(chimera_server_remove_user) \
        X(chimera_smb_share_get_name) \
        X(chimera_smb_share_get_path) \
        X(chimera_vfs_get_root_fh) \
        X(chimera_vfs_get_server_cred) \
        X(chimera_vfs_link) \
        X(chimera_vfs_lookup) \
        X(chimera_vfs_mkfs) \
        X(chimera_vfs_mount) \
        X(chimera_vfs_mount_options_valid) \
        X(chimera_vfs_open_fh) \
        X(chimera_vfs_remove) \
        X(chimera_vfs_rename) \
        X(chimera_vfs_rmfs) \
        X(chimera_vfs_setattr) \
        X(chimera_vfs_umount)

struct chimera_rest_services {
    uint32_t                                       version;
    size_t                                         struct_size;
    const struct chimera_rest_host                *sdk;
#define REST_SERVICE_FIELD(name) __typeof__(&name) name;
    CHIMERA_REST_SERVICE_FUNCTIONS(REST_SERVICE_FIELD)
#undef REST_SERVICE_FIELD
    void (*vfs_release)(struct chimera_vfs_thread *, struct chimera_vfs_open_handle *);
};

typedef int (*chimera_rest_bind_fn)(
    const struct chimera_rest_services *services);
extern const struct chimera_rest_services  chimera_rest_host_services;

#ifdef CHIMERA_REST_BUNDLED_MODULE
extern const struct chimera_rest_services *chimera_rest_services;
#define chimera_rest_host               (*chimera_rest_services->sdk)
#define chimera_vfs_release             chimera_rest_services->vfs_release
#define chimera_nfs_export_get_access   chimera_rest_services->chimera_nfs_export_get_access
#define chimera_nfs_export_get_anongid  chimera_rest_services->chimera_nfs_export_get_anongid
#define chimera_nfs_export_get_anonuid  chimera_rest_services->chimera_nfs_export_get_anonuid
#define chimera_nfs_export_get_id       chimera_rest_services->chimera_nfs_export_get_id
#define chimera_nfs_export_get_name     chimera_rest_services->chimera_nfs_export_get_name
#define chimera_nfs_export_get_path     chimera_rest_services->chimera_nfs_export_get_path
#define chimera_nfs_export_get_sec      chimera_rest_services->chimera_nfs_export_get_sec
#define chimera_nfs_export_get_squash   chimera_rest_services->chimera_nfs_export_get_squash
#define chimera_nfs_export_name_valid   chimera_rest_services->chimera_nfs_export_name_valid
#define chimera_rest_handle_auth_login  chimera_rest_services->chimera_rest_handle_auth_login
#define chimera_rest_reply              chimera_rest_services->chimera_rest_reply
#define chimera_rest_send_error         chimera_rest_services->chimera_rest_send_error
#define chimera_rest_send_json          chimera_rest_services->chimera_rest_send_json
#define chimera_rest_send_json_response chimera_rest_services->chimera_rest_send_json_response
#define chimera_s3_bucket_get_name      chimera_rest_services->chimera_s3_bucket_get_name
#define chimera_s3_bucket_get_path      chimera_rest_services->chimera_s3_bucket_get_path
#define chimera_server_add_user         chimera_rest_services->chimera_server_add_user
#define chimera_server_create_bucket    chimera_rest_services->chimera_server_create_bucket
#define chimera_server_create_export    chimera_rest_services->chimera_server_create_export
#define chimera_server_create_share     chimera_rest_services->chimera_server_create_share
#define chimera_server_get_bucket       chimera_rest_services->chimera_server_get_bucket
#define chimera_server_get_export       chimera_rest_services->chimera_server_get_export
#define chimera_server_get_share        chimera_rest_services->chimera_server_get_share
#define chimera_server_get_user         chimera_rest_services->chimera_server_get_user
#define chimera_server_iterate_buckets  chimera_rest_services->chimera_server_iterate_buckets
#define chimera_server_iterate_exports  chimera_rest_services->chimera_server_iterate_exports
#define chimera_server_iterate_mounts   chimera_rest_services->chimera_server_iterate_mounts
#define chimera_server_iterate_shares   chimera_rest_services->chimera_server_iterate_shares
#define chimera_server_iterate_users    chimera_rest_services->chimera_server_iterate_users
#define chimera_server_mount_in_use     chimera_rest_services->chimera_server_mount_in_use
#define chimera_server_release_bucket   chimera_rest_services->chimera_server_release_bucket
#define chimera_server_remove_bucket    chimera_rest_services->chimera_server_remove_bucket
#define chimera_server_remove_export    chimera_rest_services->chimera_server_remove_export
#define chimera_server_remove_share     chimera_rest_services->chimera_server_remove_share
#define chimera_server_remove_user      chimera_rest_services->chimera_server_remove_user
#define chimera_smb_share_get_name      chimera_rest_services->chimera_smb_share_get_name
#define chimera_smb_share_get_path      chimera_rest_services->chimera_smb_share_get_path
#define chimera_vfs_get_root_fh         chimera_rest_services->chimera_vfs_get_root_fh
#define chimera_vfs_get_server_cred     chimera_rest_services->chimera_vfs_get_server_cred
#define chimera_vfs_link                chimera_rest_services->chimera_vfs_link
#define chimera_vfs_lookup              chimera_rest_services->chimera_vfs_lookup
#define chimera_vfs_mkfs                chimera_rest_services->chimera_vfs_mkfs
#define chimera_vfs_mount               chimera_rest_services->chimera_vfs_mount
#define chimera_vfs_mount_options_valid chimera_rest_services->chimera_vfs_mount_options_valid
#define chimera_vfs_open_fh             chimera_rest_services->chimera_vfs_open_fh
#define chimera_vfs_remove              chimera_rest_services->chimera_vfs_remove
#define chimera_vfs_rename              chimera_rest_services->chimera_vfs_rename
#define chimera_vfs_rmfs                chimera_rest_services->chimera_vfs_rmfs
#define chimera_vfs_setattr             chimera_rest_services->chimera_vfs_setattr
#define chimera_vfs_umount              chimera_rest_services->chimera_vfs_umount
#endif // ifdef CHIMERA_REST_BUNDLED_MODULE
