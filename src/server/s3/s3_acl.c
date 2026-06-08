// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 access-control mapped onto the filesystem permission model.
 *
 * Chimera exposes S3 as a view of a real filesystem shared with NFS and SMB,
 * so rather than maintaining a separate S3 ACL store we project a small,
 * well-defined subset of S3 access control onto POSIX mode bits and let the
 * unified VFS/ACL permission gate enforce it. This keeps a single source of
 * truth across all three protocols.
 *
 * Canned ACL -> mode mapping (the rwx bits; the type bits are added by the
 * caller):
 *
 *   private             0600 file / 0700 dir   (owner only)
 *   public-read         0644 file / 0755 dir   (world read; dirs world traverse)
 *   public-read-write   0666 file / 0777 dir   (world read+write)
 *   authenticated-read  treated as public-read (0644 / 0755)
 *
 * The authenticated-read choice is deliberate: a filesystem mode has no bit
 * that distinguishes "any authenticated S3 principal" from "everyone", so we
 * collapse it to world-read. This is a documented, slightly-more-permissive
 * approximation; tightening it would require a real ACL with an
 * AuthenticatedUsers group ACE, which is a follow-up.
 *
 * GetObjectAcl / GetBucketAcl reverse the projection: they read the target's
 * owner uid + mode and synthesize an <AccessControlPolicy> with the owner
 * holding FULL_CONTROL plus AllUsers READ / WRITE grants derived from the
 * world rwx bits.
 *
 * The <Owner> reported by GetXxxAcl reflects the *authenticated requester's*
 * canonical id / display name (we do not yet keep a uid -> canonical-id reverse
 * map). In practice the requester is the owner in every ACL flow, which matches
 * real S3 semantics for these operations.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_acl.h"
#include "s3_status.h"

#define S3_ACL_ALLUSERS_URI "http://acs.amazonaws.com/groups/global/AllUsers"

int
chimera_s3_parse_canned_acl(struct evpl_http_request *request)
{
    const char *acl = evpl_http_request_header(request, "x-amz-acl");

    if (!acl) {
        return CHIMERA_S3_CANNED_NONE;
    }

    if (strcasecmp(acl, "private") == 0) {
        return CHIMERA_S3_CANNED_PRIVATE;
    } else if (strcasecmp(acl, "public-read") == 0) {
        return CHIMERA_S3_CANNED_PUBLIC_READ;
    } else if (strcasecmp(acl, "public-read-write") == 0) {
        return CHIMERA_S3_CANNED_PUBLIC_READ_WRITE;
    } else if (strcasecmp(acl, "authenticated-read") == 0) {
        return CHIMERA_S3_CANNED_AUTHENTICATED_READ;
    }

    /* Unrecognized / unsupported canned value (e.g. bucket-owner-read). */
    return -2;
} /* chimera_s3_parse_canned_acl */

uint32_t
chimera_s3_canned_acl_to_mode(
    int canned,
    int is_dir)
{
    switch (canned) {
        case CHIMERA_S3_CANNED_PRIVATE:
            return is_dir ? 0700 : 0600;
        case CHIMERA_S3_CANNED_PUBLIC_READ:
        case CHIMERA_S3_CANNED_AUTHENTICATED_READ:
            return is_dir ? 0755 : 0644;
        case CHIMERA_S3_CANNED_PUBLIC_READ_WRITE:
            return is_dir ? 0777 : 0666;
        default:
            /* No canned ACL: a sensible owner-and-world-read default. */
            return is_dir ? 0755 : 0644;
    } /* switch */
} /* chimera_s3_canned_acl_to_mode */

/* ------------------------------------------------------------ GetXxxAcl --- */

static void
chimera_s3_get_acl_finish(
    struct chimera_s3_request      *request,
    const struct chimera_vfs_attrs *attr)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;
    struct evpl_iovec                iov;
    char                            *bp, *start;
    uint32_t                         mode;
    const char                      *id;
    const char                      *display;

    mode = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ?
        (attr->va_mode & 0777) : 0;

    id      = request->owner_id[0] ? request->owner_id : "chimera";
    display = request->owner_display[0] ? request->owner_display : id;

    evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &iov);
    start = evpl_iovec_data(&iov);
    bp    = start;

    bp += sprintf(bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    bp += sprintf(bp,
                  "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    bp += sprintf(bp, "  <Owner>\n");
    bp += sprintf(bp, "    <ID>%s</ID>\n", id);
    bp += sprintf(bp, "    <DisplayName>%s</DisplayName>\n", display);
    bp += sprintf(bp, "  </Owner>\n");
    bp += sprintf(bp, "  <AccessControlList>\n");

    /* Emit AllUsers group grants (derived from the world bits) before the
     * owner's FULL_CONTROL grant. The ceph/s3-tests check_grants helper sorts
     * by DisplayName only when the first grant carries one, and crashes if a
     * group grant (no DisplayName) follows; leading with the group grant(s)
     * avoids that and matches the expected READ-then-FULL_CONTROL order. */

    /* World-read bit -> AllUsers READ. */
    if (mode & S_IROTH) {
        bp += sprintf(bp, "    <Grant>\n");
        bp += sprintf(bp,
                      "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                      " xsi:type=\"Group\">\n");
        bp += sprintf(bp, "        <URI>%s</URI>\n", S3_ACL_ALLUSERS_URI);
        bp += sprintf(bp, "      </Grantee>\n");
        bp += sprintf(bp, "      <Permission>READ</Permission>\n");
        bp += sprintf(bp, "    </Grant>\n");
    }

    /* World-write bit -> AllUsers WRITE. */
    if (mode & S_IWOTH) {
        bp += sprintf(bp, "    <Grant>\n");
        bp += sprintf(bp,
                      "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                      " xsi:type=\"Group\">\n");
        bp += sprintf(bp, "        <URI>%s</URI>\n", S3_ACL_ALLUSERS_URI);
        bp += sprintf(bp, "      </Grantee>\n");
        bp += sprintf(bp, "      <Permission>WRITE</Permission>\n");
        bp += sprintf(bp, "    </Grant>\n");
    }

    /* The owner always holds FULL_CONTROL. */
    bp += sprintf(bp, "    <Grant>\n");
    bp += sprintf(bp,
                  "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                  " xsi:type=\"CanonicalUser\">\n");
    bp += sprintf(bp, "        <ID>%s</ID>\n", id);
    bp += sprintf(bp, "        <DisplayName>%s</DisplayName>\n", display);
    bp += sprintf(bp, "      </Grantee>\n");
    bp += sprintf(bp, "      <Permission>FULL_CONTROL</Permission>\n");
    bp += sprintf(bp, "    </Grant>\n");

    bp += sprintf(bp, "  </AccessControlList>\n");
    bp += sprintf(bp, "</AccessControlPolicy>\n");

    evpl_iovec_set_length(&iov, bp - start);
    evpl_http_request_add_datav(request->http_request, &iov, 1);

    request->is_list          = 1;
    request->file_length      = bp - start;
    request->file_real_length = request->file_length;
    request->file_offset      = 0;
    request->status           = CHIMERA_S3_STATUS_OK;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_get_acl_finish */

static void
chimera_s3_get_acl_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code) {
        request->status = (error_code == CHIMERA_VFS_EACCES ||
                           error_code == CHIMERA_VFS_EPERM) ?
            CHIMERA_S3_STATUS_ACCESS_DENIED : CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    chimera_s3_get_acl_finish(request, attr);
} /* chimera_s3_get_acl_lookup_cb */

void
chimera_s3_get_acl(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    /* For a bucket ACL the path is empty and the bucket dir fh is the target;
     * for an object ACL we resolve the key relative to the bucket. */
    if (request->path_len == 0) {
        chimera_vfs_lookup(thread->vfs, &request->cred,
                           request->bucket_fh, request->bucket_fhlen,
                           ".", 1,
                           CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_get_acl_lookup_cb, request);
    } else {
        chimera_vfs_lookup(thread->vfs, &request->cred,
                           request->bucket_fh, request->bucket_fhlen,
                           request->path, request->path_len,
                           CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_get_acl_lookup_cb, request);
    }
} /* chimera_s3_get_acl */

/* ------------------------------------------------------------ PutXxxAcl --- */

static void
chimera_s3_put_acl_finish(
    struct chimera_s3_request *request,
    enum chimera_s3_status     status)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;

    request->status           = status;
    request->file_length      = 0;
    request->file_real_length = 0;
    request->file_offset      = 0;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_put_acl_finish */

static void
chimera_s3_put_acl_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    enum chimera_s3_status           status  = CHIMERA_S3_STATUS_OK;

    chimera_vfs_release(thread->vfs, request->file_handle);
    request->file_handle = NULL;

    if (error_code) {
        status = (error_code == CHIMERA_VFS_EACCES ||
                  error_code == CHIMERA_VFS_EPERM) ?
            CHIMERA_S3_STATUS_ACCESS_DENIED : CHIMERA_S3_STATUS_INTERNAL_ERROR;
    }

    chimera_s3_put_acl_finish(request, status);
} /* chimera_s3_put_acl_setattr_cb */

static void
chimera_s3_put_acl_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    int                              is_dir;
    uint32_t                         mode;

    if (error_code) {
        chimera_s3_put_acl_finish(request,
                                  (error_code == CHIMERA_VFS_EACCES ||
                                   error_code == CHIMERA_VFS_EPERM) ?
                                  CHIMERA_S3_STATUS_ACCESS_DENIED :
                                  CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    request->file_handle = oh;

    is_dir = (request->path_len == 0);
    mode   = chimera_s3_canned_acl_to_mode(request->canned_acl, is_dir);

    memset(&request->set_attr, 0, sizeof(request->set_attr));
    request->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    request->set_attr.va_mode     = mode | (is_dir ? S_IFDIR : 0);

    chimera_vfs_setattr(thread->vfs, &request->cred,
                        request->file_handle,
                        &request->set_attr,
                        0, 0,
                        chimera_s3_put_acl_setattr_cb, request);
} /* chimera_s3_put_acl_open_cb */

static void
chimera_s3_put_acl_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    int                              is_dir  = (request->path_len == 0);

    if (error_code || !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_s3_put_acl_finish(request,
                                  (error_code == CHIMERA_VFS_EACCES ||
                                   error_code == CHIMERA_VFS_EPERM) ?
                                  CHIMERA_S3_STATUS_ACCESS_DENIED :
                                  CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    chimera_vfs_open_fh(thread->vfs, &request->cred,
                        attr->va_fh, attr->va_fh_len,
                        is_dir ? (CHIMERA_VFS_OPEN_PATH |
                                  CHIMERA_VFS_OPEN_INFERRED |
                                  CHIMERA_VFS_OPEN_DIRECTORY) :
                        (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED),
                        chimera_s3_put_acl_open_cb, request);
} /* chimera_s3_put_acl_lookup_cb */

void
chimera_s3_put_acl(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    /* PutObjectAcl / PutBucketAcl currently honor only the canned ACL supplied
     * via x-amz-acl. A full grant-list XML body is a documented follow-up; if
     * no canned value is present we treat it as a no-op success rather than
     * failing the request (the object/bucket keeps its current mode). */
    if (request->canned_acl == CHIMERA_S3_CANNED_NONE) {
        chimera_s3_put_acl_finish(request, CHIMERA_S3_STATUS_OK);
        return;
    }

    if (request->canned_acl < 0) {
        /* Unsupported canned value. */
        chimera_s3_put_acl_finish(request, CHIMERA_S3_STATUS_BAD_REQUEST);
        return;
    }

    if (request->path_len == 0) {
        chimera_vfs_lookup(thread->vfs, &request->cred,
                           request->bucket_fh, request->bucket_fhlen,
                           ".", 1,
                           CHIMERA_VFS_ATTR_FH,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_put_acl_lookup_cb, request);
    } else {
        chimera_vfs_lookup(thread->vfs, &request->cred,
                           request->bucket_fh, request->bucket_fhlen,
                           request->path, request->path_len,
                           CHIMERA_VFS_ATTR_FH,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_put_acl_lookup_cb, request);
    }
} /* chimera_s3_put_acl */
