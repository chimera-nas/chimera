// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include <sys/stat.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_etag.h"


static inline void
chimera_s3_list_append(
    char      **p,
    const char *fmt,
    ...)
{
    va_list ap;

    va_start(ap, fmt);
    *p += vsprintf(*p, fmt, ap);
    va_end(ap);
} /* chimera_s3_list_append */


static int
chimera_s3_list_filter(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;
    int                        match, len = pathlen;

    if (request->list.filter_len < len) {
        len = request->list.filter_len;
    }

    match = strncmp(path, request->list.filter, len);

    return match != 0;
} /* chimera_s3_list_filter */


static int
chimera_s3_list_find_callback(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;
    int                        match, len = pathlen;
    char                       date[64], etag[64];

    chimera_s3_abort_if((attr->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT)) !=
                        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT),
                        "find return missing expected attributes");

    if ((attr->va_mode & S_IFMT) == S_IFDIR) {
        return 0;
    }

    if (request->list.filter_len < len) {
        len = request->list.filter_len;
    }

    match = strncmp(path, request->list.filter, len);

    if (match != 0) {
        return 0;
    }

    chimera_s3_format_date(date, sizeof(date), &attr->va_mtime);

    chimera_s3_etag_hex(etag, sizeof(etag), attr);

    chimera_s3_list_append(&request->list.rp, " <Contents>\n");

    if (request->list.base_path_len) {
        chimera_s3_list_append(&request->list.rp, "  <Key>/%.*s%.*s</Key>\n",
                               request->list.base_path_len, request->list.base_path,
                               pathlen, path);
    } else {
        chimera_s3_list_append(&request->list.rp, "  <Key>%.*s</Key>\n", pathlen, path);
    }
    chimera_s3_list_append(&request->list.rp, "  <LastModified>%s</LastModified>\n", date);
    chimera_s3_list_append(&request->list.rp, "  <ETag>%s</ETag>\n", etag);
    chimera_s3_list_append(&request->list.rp, "  <Size>%lu</Size>\n", attr->va_size);
    chimera_s3_list_append(&request->list.rp, "  <StorageClass>STANDARD</StorageClass>\n");
    chimera_s3_list_append(&request->list.rp, " </Contents>\n");

    return 0;
} /* chimera_s3_list_find_callback */

static void
chimera_s3_list_find_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    struct evpl_iovec                prefix_iov, suffix_iov;
    char                            *prefix, *suffix;

    evpl_iovec_alloc(evpl, 4096, 0, 1, &prefix_iov);
    evpl_iovec_alloc(evpl, 4096, 0, 1, &suffix_iov);

    prefix = evpl_iovec_data(&prefix_iov);
    suffix = evpl_iovec_data(&suffix_iov);

    chimera_s3_list_append(&prefix, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    chimera_s3_list_append(&prefix, "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    chimera_s3_list_append(&prefix, " <Name>%.*s</Name>\n", request->bucket_namelen, request->bucket_name);
    chimera_s3_list_append(&prefix, " <Prefix>%.*s</Prefix>\n", request->list.prefix_len, request->list.prefix);
    //chimera_s3_list_append(&prefix, " <Marker></Marker>\n");
    chimera_s3_list_append(&prefix, " <MaxKeys>%d</MaxKeys>\n", request->list.max_keys);
    //chimera_s3_list_append(&prefix, " <Delimiter>/</Delimiter>\n");
    chimera_s3_list_append(&prefix, " <IsTruncated>false</IsTruncated>\n");

    chimera_s3_list_append(&suffix, "</ListBucketResult>\n");

    evpl_iovec_set_length(&prefix_iov, prefix - (char *) evpl_iovec_data(&prefix_iov));
    evpl_iovec_set_length(&suffix_iov, suffix - (char *) evpl_iovec_data(&suffix_iov));
    evpl_iovec_set_length(&request->list.response, request->list.rp - (char *) evpl_iovec_data(&request->list.response))
    ;

    evpl_http_request_add_datav(request->http_request, &prefix_iov, 1);
    evpl_http_request_add_datav(request->http_request, &request->list.response, 1);
    evpl_http_request_add_datav(request->http_request, &suffix_iov, 1);

    request->file_length = evpl_iovec_length(&prefix_iov) +
        evpl_iovec_length(&request->list.response) +
        evpl_iovec_length(&suffix_iov);

    request->file_real_length = request->file_length;
    request->file_offset      = 0;

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_list_find_complete */

static void
chimera_s3_list_lookup_path_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    const char                      *slash;
    const void                      *root_fh;
    int                              root_fh_len;

    if (error_code || !S_ISDIR(attr->va_mode)) {
        /* The path prefix was not a valid path, so take the largest
         * prefix of it that must be a valid path and start the find from there */

        slash = rindex(request->path, '/');

        if (slash) {
            request->list.base_path_len = slash - request->path;
            memcpy(request->list.base_path, request->path, request->list.base_path_len);

            request->list.filter_len = request->path_len - request->list.base_path_len;
            memcpy(request->list.filter, request->path + request->list.base_path_len, request->list.filter_len);
        } else {
            request->list.base_path_len = 0;
            request->list.filter_len    = request->path_len;
            memcpy(request->list.filter, request->path, request->list.filter_len);
        }


        root_fh     = request->bucket_fh;
        root_fh_len = request->bucket_fhlen;
    } else {
        /* Path prefix turned out to be a valid path, we can start from there */
        memcpy(request->list.root_fh, attr->va_fh, attr->va_fh_len);
        root_fh     = request->list.root_fh;
        root_fh_len = attr->va_fh_len;

        request->list.base_path_len = request->path_len;
        memcpy(request->list.base_path, request->path, request->list.base_path_len);

        request->list.filter_len = 0;
    }

    chimera_vfs_find(thread->vfs,
                     root_fh,
                     root_fh_len,
                     CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                     chimera_s3_list_filter,
                     chimera_s3_list_find_callback,
                     chimera_s3_list_find_complete,
                     request);

} /* chimera_s3_list_lookup_path_callback */

void
chimera_s3_list(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    while (*request->path == '/') {
        request->path++;
        request->path_len--;
    }

    evpl_iovec_alloc(evpl, 1024 * 1024, 0, 1, &request->list.response);

    request->list.rp = evpl_iovec_data(&request->list.response);

    if (request->path_len == 0) {

        request->list.base_path_len = 0;
        request->list.filter_len    = 0;

        chimera_vfs_find(thread->vfs,
                         request->bucket_fh,
                         request->bucket_fhlen,
                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                         chimera_s3_list_filter,
                         chimera_s3_list_find_callback,
                         chimera_s3_list_find_complete,
                         request);
    } else {

        /* If we are lucky, the path prefix is a valid path and we can start
         * the find from that location */

        chimera_vfs_lookup_path(
            thread->vfs,
            request->bucket_fh,
            request->bucket_fhlen,
            request->path,
            request->path_len,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            chimera_s3_list_lookup_path_callback,
            request);
    }
} /* chimera_s3_list */