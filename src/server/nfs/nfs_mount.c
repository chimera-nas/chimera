// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <utlist.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_mount.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs_mount_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_NULL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_null */

static void
chimera_nfs_mount_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct evpl                      *evpl   = thread->evpl;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int32_t                           auth_flavors[2];
    struct mountres3                  res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        res.fhs_status                 = MNT3_OK;
        res.mountinfo.num_auth_flavors = 2;
        res.mountinfo.auth_flavors     = auth_flavors;
        auth_flavors[0]                = AUTH_NONE;
        auth_flavors[1]                = AUTH_SYS;

        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS mount: no file handle was returned");

        rc = xdr_dbuf_alloc_opaque(&res.mountinfo.fhandle,
                                   attr->va_fh_len,
                                   req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to allocate opaque");
        memcpy(res.mountinfo.fhandle.data,
               attr->va_fh,
               attr->va_fh_len);
    } else {
        res.fhs_status = MNT3ERR_NOENT;
    }

    rc = shared->mount_v3.send_reply_MOUNTPROC3_MNT(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs_mount_lookup_complete */

void
chimera_nfs_mount_mnt(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mountarg3          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_nfs_export        *export = NULL, *cur_export;
    char                             *full_path;
    const char                       *suffix = NULL;
    size_t                            export_name_len, path_len;
    uint8_t                           root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          root_fh_len;

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred(&req->cred, cred);

    // Map the nfs export to a path lookup
    pthread_mutex_lock(&shared->exports_lock);
    LL_FOREACH(shared->exports, cur_export)
    {
        export_name_len = strlen(cur_export->name);
        chimera_nfs_info("checking export '%s' against '%s'", cur_export->name, args->path.str);
        if (strncasecmp(cur_export->name, args->path.str, export_name_len) == 0) {
            // Check if this is a valid prefix match (at path boundary)
            if (args->path.str[export_name_len] == '\0' ||
                args->path.str[export_name_len] == '/') {
                export = cur_export;
                suffix = args->path.str + export_name_len;
                // Skip leading slash and check if empty
                if (*suffix == '/') {
                    suffix++;
                }
                if (*suffix == '\0') {
                    suffix = NULL;
                }
                break;
            }
        }
    }
    pthread_mutex_unlock(&shared->exports_lock);
    if (!export) {
        // Export not found, return error
        chimera_nfs_debug("NFS mount request for unknown export '%s'", args->path.str);
        chimera_nfs_mount_lookup_complete(CHIMERA_VFS_ENOENT, NULL, req);
        return;
    }

    // Construct full path: export->path + suffix
    if (suffix && *suffix != '\0') {
        path_len  = strlen(export->path) + strlen(suffix) + 1;
        full_path = malloc(path_len + 1);
        chimera_nfs_abort_if(full_path == NULL, "Failed to allocate path");
        snprintf(full_path, path_len + 1, "%s/%s", export->path, suffix);
    } else {
        full_path = strdup(export->path);
        chimera_nfs_abort_if(full_path == NULL, "Failed to allocate path");
    }

    chimera_vfs_lookup(thread->vfs_thread,
                       &req->cred,
                       root_fh,
                       root_fh_len,
                       full_path,
                       strlen(full_path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_nfs_mount_lookup_complete,
                       req);

    free(full_path);

} /* chimera_nfs_mount_mnt */

void
chimera_nfs_mount_dump(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_debug("Received MOUNTPROC3_DUMP request");
} /* chimera_nfs_mount_dump */

void
chimera_nfs_mount_umnt(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mountarg3          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_UMNT(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_umnt */

void
chimera_nfs_mount_umntall(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_debug("Received MOUNTPROC3_UMNTALL request");
} /* chimera_nfs_mount_umntall */

void
chimera_nfs_mount_export(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct exportres                  export;
    int                               rc;

    export.exports = NULL;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_EXPORT(evpl, NULL, &export, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_export */
