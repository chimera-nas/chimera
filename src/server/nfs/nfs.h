// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>

extern struct chimera_server_protocol nfs_protocol;

struct chimera_nfs_export;


/**
 * @brief Adds a new NFS export to the shared context.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param name       Name of the export.
 * @param path       Filesystem path for the export.
 */
void chimera_nfs_add_export(
    void       *nfs_shared,
    const char *name,
    const char *path);


/**
 * @brief Removes an NFS export from the shared context.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param name       Name of the export to remove.
 * @return 0 on success, negative value on error.
 */
int
chimera_nfs_remove_export(
    void       *nfs_shared,
    const char *name);


/**
 * @brief Returns the number of NFS exports in the shared context.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @return Number of exports.
 */
int
chimera_nfs_export_count(
    void *nfs_shared);


/**
 * @brief Retrieves the full filesystem path for a given NFS export path.
 *        This function is used to resolve the export path during NFS mount operations.
 *        The function will search for the longest matching export path prefix and return the corresponding full path.
 * @param nfs_shared    Pointer to the NFS shared context.
 * @param path          The export path to resolve (not necessarily null-terminated).
 * @param path_len      Length of the export path.
 * @param out_full_path Output pointer to receive the allocated full path string. The caller is responsible for freeing this string.
 * @return 0 on success, non-zero if the export was not found or an error occurred.
 */
int
chimera_nfs_find_export_path(
    void       *nfs_shared,
    const char *path,
    uint32_t    path_len,
    char      **out_full_path);


/**
 * @brief Retrieves an NFS export by name.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param name       Name of the export.
 * @return Pointer to the export if found, NULL otherwise.
 */
const struct chimera_nfs_export *
chimera_nfs_get_export(
    void       *nfs_shared,
    const char *name);


/**
 * @brief Callback type for iterating over NFS exports.
 *
 * @param export Pointer to the current export.
 * @param data   User data pointer.
 * @return Implementation-defined.
 */
typedef int (*chimera_nfs_export_iterate_cb)(
    const struct chimera_nfs_export *export,
    void *data);


/**
 * @brief Iterates over all NFS exports, invoking a callback for each.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param callback   Callback function to invoke for each export.
 * @param data       User data pointer passed to the callback.
 */
void
chimera_nfs_iterate_exports(
    void                         *nfs_shared,
    chimera_nfs_export_iterate_cb callback,
    void                         *data);


/**
 * @brief Returns the name of the specified NFS export.
 *
 * @param export Pointer to the export.
 * @return Name of the export.
 */
const char *
chimera_nfs_export_get_name(
    const struct chimera_nfs_export *export);


/**
 * @brief Returns the path of the specified NFS export.
 *
 * @param export Pointer to the export.
 * @return Path of the export.
 */
const char *
chimera_nfs_export_get_path(
    const struct chimera_nfs_export *export);