// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "rest_internal.h"

/* External handlers from rest_users.c */
void chimera_rest_handle_users_list(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_users_get(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_users_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_users_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

/* External handlers from rest_shares.c */
void chimera_rest_handle_exports_list(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_exports_get(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_exports_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_exports_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

void chimera_rest_handle_shares_list(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_shares_get(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_shares_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_shares_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

void chimera_rest_handle_buckets_list(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_buckets_get(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_buckets_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_buckets_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

void chimera_rest_handle_mounts_list(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_mounts_get(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_mounts_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_mounts_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

/* External handlers from rest_filesystems.c */
void chimera_rest_handle_filesystems_create(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_filesystems_delete(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *,
    const char *);

/* External handler from rest_config.c */
void chimera_rest_handle_config(
    struct evpl *,
    struct chimera_rest_request *,
    struct chimera_rest_thread *);
