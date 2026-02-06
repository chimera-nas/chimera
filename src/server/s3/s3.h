// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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
    void       *s3_shared,
    const char *access_key,
    const char *secret_key,
    int         pinned);

extern struct chimera_server_protocol s3_protocol;