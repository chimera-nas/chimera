// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_dispatch.h"
#include "common/macros.h"

SYMBOL_EXPORT void
chimera_dispatch_error_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_mkdir_callback_t callback     = request->mkdir.callback;
    void                    *callback_arg = request->mkdir.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_mkdir */

SYMBOL_EXPORT void
chimera_dispatch_error_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_remove_callback_t callback     = request->remove.callback;
    void                     *callback_arg = request->remove.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_remove */

SYMBOL_EXPORT void
chimera_dispatch_error_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_symlink_callback_t callback     = request->symlink.callback;
    void                      *callback_arg = request->symlink.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_symlink */

SYMBOL_EXPORT void
chimera_dispatch_error_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_rename_callback_t callback     = request->rename.callback;
    void                     *callback_arg = request->rename.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_rename */

SYMBOL_EXPORT void
chimera_dispatch_error_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_link_callback_t callback     = request->link.callback;
    void                   *callback_arg = request->link.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_link */

SYMBOL_EXPORT void
chimera_dispatch_error_mknod(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         error_code)
{
    chimera_mknod_callback_t callback     = request->mknod.callback;
    void                    *callback_arg = request->mknod.private_data;

    chimera_client_request_free(thread, request);
    callback(thread, error_code, callback_arg);
} /* chimera_dispatch_error_mknod */
