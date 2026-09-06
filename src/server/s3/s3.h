// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct s3_bucket;

void
chimera_s3_add_bucket(
    void       *s3_shared,
    const char *name,
    const char *path);

int
chimera_s3_remove_bucket(
    void       *s3_shared,
    const char *name);

/* Configure the VFS path (relative to the server root) under which runtime
 * CreateBucket requests materialize new bucket directories. */
void
chimera_s3_set_bucket_root(
    void       *s3_shared,
    const char *path);

const struct s3_bucket *
chimera_s3_get_bucket(
    void       *s3_shared,
    const char *name);

void
chimera_s3_release_bucket(
    void *s3_shared);

typedef int (*chimera_s3_bucket_iterate_cb)(
    const struct s3_bucket *bucket,
    void                   *data);

void
chimera_s3_iterate_buckets(
    void                        *s3_shared,
    chimera_s3_bucket_iterate_cb callback,
    void                        *data);

const char *
chimera_s3_bucket_get_name(
    const struct s3_bucket *bucket);

const char *
chimera_s3_bucket_get_path(
    const struct s3_bucket *bucket);

int
chimera_s3_add_cred(
    void           *s3_shared,
    const char     *access_key,
    const char     *secret_key,
    int             has_identity,
    uint32_t        uid,
    uint32_t        gid,
    uint32_t        ngids,
    const uint32_t *gids,
    const char     *canon_id,
    const char     *display_name,
    int             pinned);

int
chimera_s3_remove_cred(
    void       *s3_shared,
    const char *access_key);

/* Advance the S3 credential cache's synthetic clock by `seconds` and sweep
 * expired credentials synchronously.  Test instrumentation: lets a harness
 * exercise TTL expiry by ticking time deterministically instead of waiting
 * out the sweeper's wall-clock cadence. */
void
chimera_s3_advance_cred_clock(
    void   *s3_shared,
    int64_t seconds);

extern struct chimera_server_protocol s3_protocol;