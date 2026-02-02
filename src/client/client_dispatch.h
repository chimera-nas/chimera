// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

/*
 * Error dispatch helper functions.
 *
 * These are intentionally non-inline functions that take ownership of the
 * request and handle error completion. By having the error paths go through
 * external functions (rather than inline code), the static analyzer can
 * properly track ownership transfer in all code paths.
 */

void
chimera_dispatch_error_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);

void
chimera_dispatch_error_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);

void
chimera_dispatch_error_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);

void
chimera_dispatch_error_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);

void
chimera_dispatch_error_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);

void
chimera_dispatch_error_mknod(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code);
