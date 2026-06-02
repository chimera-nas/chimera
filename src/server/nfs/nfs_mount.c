// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <utlist.h>

#include "nfs.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_mount.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/*
 * Derive the rmtab hostname for a connection: the caller's remote address with
 * the transport port stripped (IPv4 "addr:port" and IPv6 "[addr]:port").
 */
static void
chimera_nfs_mount_client_host(
    struct evpl_rpc2_conn *conn,
    char                  *out,
    int                    out_len)
{
    char   addr[64];
    char  *rbracket, *colon;
    size_t n;

    evpl_rpc2_conn_get_remote_address(conn, addr, sizeof(addr));

    if (addr[0] == '[') {
        /* IPv6 "[addr]:port" -> addr */
        rbracket = strchr(addr, ']');
        if (rbracket) {
            n = rbracket - (addr + 1);
            if (n >= (size_t) out_len) {
                n = out_len - 1;
            }
            memcpy(out, addr + 1, n);
            out[n] = '\0';
            return;
        }
    }

    /* IPv4 "addr:port" -> addr */
    snprintf(out, out_len, "%s", addr);
    colon = strrchr(out, ':');
    if (colon) {
        *colon = '\0';
    }
} /* chimera_nfs_mount_client_host */

/*
 * Record a mount entry in the rmtab.  Deduplicates on (hostname, directory)
 * so a re-mount does not create a second entry.
 */
static void
chimera_nfs_mount_record(
    struct chimera_server_nfs_shared *shared,
    const char                       *hostname,
    const char                       *directory)
{
    struct chimera_nfs_mount_entry *entry;

    pthread_mutex_lock(&shared->mount_entries_lock);

    LL_FOREACH(shared->mount_entries, entry)
    {
        if (strcmp(entry->hostname, hostname) == 0 &&
            strcmp(entry->directory, directory) == 0) {
            pthread_mutex_unlock(&shared->mount_entries_lock);
            return;
        }
    }

    entry = calloc(1, sizeof(*entry));
    snprintf(entry->hostname, sizeof(entry->hostname), "%s", hostname);
    snprintf(entry->directory, sizeof(entry->directory), "%s", directory);
    LL_PREPEND(shared->mount_entries, entry);
    shared->num_mount_entries++;

    pthread_mutex_unlock(&shared->mount_entries_lock);
} /* chimera_nfs_mount_record */

/* Remove the rmtab entry matching (hostname, directory), if any (UMNT). */
static void
chimera_nfs_mount_remove(
    struct chimera_server_nfs_shared *shared,
    const char                       *hostname,
    const char                       *directory)
{
    struct chimera_nfs_mount_entry *entry;

    pthread_mutex_lock(&shared->mount_entries_lock);
    LL_FOREACH(shared->mount_entries, entry)
    {
        if (strcmp(entry->hostname, hostname) == 0 &&
            strcmp(entry->directory, directory) == 0) {
            LL_DELETE(shared->mount_entries, entry);
            shared->num_mount_entries--;
            chimera_nfs_abort_if(shared->num_mount_entries < 0,
                                 "num_mount_entries went negative");
            free(entry);
            break;
        }
    }
    pthread_mutex_unlock(&shared->mount_entries_lock);
} /* chimera_nfs_mount_remove */

/* Remove all rmtab entries for a host (UMNTALL). */
static void
chimera_nfs_mount_remove_host(
    struct chimera_server_nfs_shared *shared,
    const char                       *hostname)
{
    struct chimera_nfs_mount_entry *entry, *next, *keep = NULL;

    pthread_mutex_lock(&shared->mount_entries_lock);
    /* Single pass partitioning the list: free entries for this host, re-link
     * the survivors.  next is captured before any free so the cursor never
     * touches freed memory. */
    entry = shared->mount_entries;
    while (entry) {
        next = entry->next;
        if (strcmp(entry->hostname, hostname) == 0) {
            shared->num_mount_entries--;
            chimera_nfs_abort_if(shared->num_mount_entries < 0,
                                 "num_mount_entries went negative");
            free(entry);
        } else {
            entry->next = keep;
            keep        = entry;
        }
        entry = next;
    }
    shared->mount_entries = keep;
    pthread_mutex_unlock(&shared->mount_entries_lock);
} /* chimera_nfs_mount_remove_host */

/* Copy an XDR mount path argument into a NUL-terminated, length-bounded buffer. */
static void
chimera_nfs_mount_copy_path(
    const xdr_string *path,
    char             *out,
    int               out_len)
{
    uint32_t len = path->len;

    if (len > (uint32_t) (out_len - 1)) {
        len = out_len - 1;
    }
    memcpy(out, path->str, len);
    out[len] = '\0';
} /* chimera_nfs_mount_copy_path */

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
    int                               rc;
    char                             *full_path = NULL;
    uint8_t                           root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          root_fh_len;
    char                              hostname[64];
    char                              directory[MNTPATHLEN + 1];

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred(&req->cred, cred);

    rc = chimera_nfs_find_export_path(shared, args->path.str, args->path.len, &full_path);
    if (rc) {
        // Export not found, return error
        chimera_nfs_debug("NFS mount request for unknown export '%s'", args->path.str);
        if (full_path) {
            free(full_path);
        }
        chimera_nfs_mount_lookup_complete(CHIMERA_VFS_ENOENT, NULL, req);
        return;
    }

    /*
     * Record the mount in the rmtab keyed by the client-requested path (which
     * is what UMNT will present and what showmount displays).  The export
     * resolved, so the subsequent VFS lookup of its root is expected to
     * succeed; recording here keeps the advisory rmtab simple without
     * threading the path through the async completion.
     */
    chimera_nfs_mount_client_host(conn, hostname, sizeof(hostname));
    chimera_nfs_mount_copy_path(&args->path, directory, sizeof(directory));
    chimera_nfs_mount_record(shared, hostname, directory);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(thread->vfs_thread,
                       &req->cred, NULL,
                       root_fh,
                       root_fh_len,
                       full_path,
                       strlen(full_path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_nfs_mount_lookup_complete,
                       req);
    if (full_path) {
        free(full_path);
    }

} /* chimera_nfs_mount_mnt */

void
chimera_nfs_mount_dump(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct chimera_nfs_mount_entry   *entry;
    struct mountdumpres               res;
    struct mountbody                 *head = NULL, *tail = NULL, *node;
    int                               rc;

    pthread_mutex_lock(&shared->mount_entries_lock);
    LL_FOREACH(shared->mount_entries, entry)
    {
        node = xdr_dbuf_alloc_space(sizeof(*node), encoding->dbuf);
        if (!node) {
            chimera_nfs_error("MOUNTPROC3_DUMP: reply buffer exhausted, truncating mount list");
            break;
        }
        node->ml_next = NULL;

        if (xdr_dbuf_alloc_string(&node->ml_hostname, entry->hostname,
                                  strlen(entry->hostname), encoding->dbuf) ||
            xdr_dbuf_alloc_string(&node->ml_directory, entry->directory,
                                  strlen(entry->directory), encoding->dbuf)) {
            chimera_nfs_error("MOUNTPROC3_DUMP: reply buffer exhausted, truncating mount list");
            break;
        }

        if (tail) {
            tail->ml_next = node;
        } else {
            head = node;
        }
        tail = node;
    }
    pthread_mutex_unlock(&shared->mount_entries_lock);

    res.mounts = head;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_DUMP(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
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
    char                              hostname[64];
    char                              directory[MNTPATHLEN + 1];
    int                               rc;

    chimera_nfs_mount_client_host(conn, hostname, sizeof(hostname));
    chimera_nfs_mount_copy_path(&args->path, directory, sizeof(directory));
    chimera_nfs_mount_remove(shared, hostname, directory);

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
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    char                              hostname[64];
    int                               rc;

    chimera_nfs_mount_client_host(conn, hostname, sizeof(hostname));
    chimera_nfs_mount_remove_host(shared, hostname);

    rc = shared->mount_v3.send_reply_MOUNTPROC3_UMNTALL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
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
    struct chimera_nfs_export        *export;
    struct exportres                  res;
    struct exportnode                *head = NULL, *tail = NULL, *node;
    int                               rc;

    pthread_mutex_lock(&shared->exports_lock);
    LL_FOREACH(shared->exports, export)
    {
        node = xdr_dbuf_alloc_space(sizeof(*node), encoding->dbuf);
        if (!node) {
            chimera_nfs_error("MOUNTPROC3_EXPORT: reply buffer exhausted, truncating export list");
            break;
        }
        /* No per-export group restriction; ex_groups == NULL means exported to
         * everyone.  Per-client access control is tracked separately (#69). */
        node->ex_groups = NULL;
        node->ex_next   = NULL;

        if (xdr_dbuf_alloc_string(&node->ex_dir, export->name,
                                  strlen(export->name), encoding->dbuf)) {
            chimera_nfs_error("MOUNTPROC3_EXPORT: reply buffer exhausted, truncating export list");
            break;
        }

        if (tail) {
            tail->ex_next = node;
        } else {
            head = node;
        }
        tail = node;
    }
    pthread_mutex_unlock(&shared->exports_lock);

    res.exports = head;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_EXPORT(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_export */
