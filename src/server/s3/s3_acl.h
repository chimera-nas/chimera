// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct evpl;
struct evpl_http_request;
struct chimera_server_s3_thread;
struct chimera_s3_request;

/*
 * Canned-ACL identifiers (the values S3 accepts in the "x-amz-acl" header).
 * CHIMERA_S3_CANNED_NONE means no x-amz-acl header was supplied.
 */
enum chimera_s3_canned_acl {
    CHIMERA_S3_CANNED_NONE    = -1,
    CHIMERA_S3_CANNED_PRIVATE = 0,
    CHIMERA_S3_CANNED_PUBLIC_READ,
    CHIMERA_S3_CANNED_PUBLIC_READ_WRITE,
    CHIMERA_S3_CANNED_AUTHENTICATED_READ,
};

/*
 * Parse the "x-amz-acl" header on a request into a canned-ACL id.
 * Returns CHIMERA_S3_CANNED_NONE when the header is absent and -2 for an
 * unrecognized value (caller maps that to InvalidArgument).
 */
int
chimera_s3_parse_canned_acl(
    struct evpl_http_request *request);

/*
 * Translate a canned-ACL id to a POSIX permission mask (the rwx bits, no type
 * bits). is_dir selects the directory variant (execute/traverse bits set).
 */
uint32_t
chimera_s3_canned_acl_to_mode(
    int canned,
    int is_dir);

/*
 * GetObjectAcl / GetBucketAcl: GET <object|bucket>?acl. Resolves the target's
 * owner + mode and renders an <AccessControlPolicy> XML body.
 */
void
chimera_s3_get_acl(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);

/*
 * PutObjectAcl / PutBucketAcl: PUT <object|bucket>?acl. Applies the canned ACL
 * from x-amz-acl to the target's mode via chimera_vfs_setattr.
 */
void
chimera_s3_put_acl(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request);
