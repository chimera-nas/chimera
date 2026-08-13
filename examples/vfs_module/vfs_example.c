// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: Unlicense

/*
 * Minimal out-of-tree VFS module.
 *
 * This module exists to prove, at build time, that the SDK boundary is
 * real: it is compiled with ONLY src/vfs/sdk (plus the SDK's declared
 * external dependencies: libevpl, prometheus-c, oteltracing-c) on the
 * include path, so it cannot reach any chimera-internal header.  It is
 * also loaded by a smoke test to exercise the dlopen + sdk_version path
 * an out-of-tree module uses in production.
 *
 * As a module it does nothing useful: every operation completes with
 * CHIMERA_VFS_ENOTSUP.
 */

#include <stddef.h>

#include "chimera_vfs_sdk.h"

#define VFS_EXAMPLE_EXPORT __attribute__((visibility("default")))

static void *
vfs_example_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) cfgdata;
    (void) metrics;

    chimera_vfs_info("example module initialized");

    /* No global state; any non-NULL pointer signals success. */
    return (void *) 1;
} /* vfs_example_init */

static void
vfs_example_destroy(void *private_data)
{
    (void) private_data;
} /* vfs_example_destroy */

static void *
vfs_example_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;

    return private_data;
} /* vfs_example_thread_init */

static void
vfs_example_thread_destroy(void *private_data)
{
    (void) private_data;
} /* vfs_example_thread_destroy */

static void
vfs_example_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    (void) private_data;

    /* Exercise an exported SDK helper so the smoke test proves symbol
     * resolution against the chimera_vfs library, not just compilation. */
    (void) chimera_vfs_hash(request->fh, request->fh_len);

    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* vfs_example_dispatch */

VFS_EXAMPLE_EXPORT struct chimera_vfs_module vfs_example = {
    .sdk_version    = CHIMERA_VFS_SDK_VERSION,
    .name           = "example",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_VENDOR0,
    .capabilities   = CHIMERA_VFS_CAP_FS,
    .init           = vfs_example_init,
    .destroy        = vfs_example_destroy,
    .thread_init    = vfs_example_thread_init,
    .thread_destroy = vfs_example_thread_destroy,
    .dispatch       = vfs_example_dispatch,
};
