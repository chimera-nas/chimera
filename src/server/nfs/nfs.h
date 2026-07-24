// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>

extern struct chimera_server_protocol nfs_protocol;

struct chimera_nfs_export;

/*
 * Per-export access-control constants (see struct chimera_nfs_export).
 * Declared here so the daemon JSON config parser can translate user-supplied
 * access strings into these values.
 */
#define CHIMERA_NFS_EXPORT_ACCESS_RO    0x00000001u
#define CHIMERA_NFS_EXPORT_ACCESS_RW    0x00000002u

#define CHIMERA_NFS_SQUASH_NONE         0u /* no_root_squash (default): pass through   */
#define CHIMERA_NFS_SQUASH_ROOT         1u /* root_squash: uid 0 -> anon                */
#define CHIMERA_NFS_SQUASH_ALL          2u /* all_squash: every caller -> anon          */

/*
 * Per-export allowed RPC security flavors, as a bitmask.  sec_allowed == 0
 * permits any flavor (the historical pass-through default); a non-zero mask
 * restricts the export to exactly those flavors (others -> NFS4ERR_WRONGSEC).
 * krb5/krb5i/krb5p all ride RPCSEC_GSS, distinguished by the GSS service.
 */
#define CHIMERA_NFS_SEC_SYS             0x01u /* AUTH_SYS (and AUTH_NONE)         */
#define CHIMERA_NFS_SEC_KRB5            0x02u /* RPCSEC_GSS, service = none       */
#define CHIMERA_NFS_SEC_KRB5I           0x04u /* RPCSEC_GSS, service = integrity  */
#define CHIMERA_NFS_SEC_KRB5P           0x08u /* RPCSEC_GSS, service = privacy    */
#define CHIMERA_NFS_SEC_ALL \
        (CHIMERA_NFS_SEC_SYS | CHIMERA_NFS_SEC_KRB5 | \
         CHIMERA_NFS_SEC_KRB5I | CHIMERA_NFS_SEC_KRB5P)

/*
 * Largest export id an operator may assign.  Ids are embedded in wire file
 * handles as a 16-bit field, so the id space is the full uint16_t range with
 * id 0 reserved as invalid/pseudo-root.  The id space is distinct from the
 * concurrent-export count cap (server config nfs_max_exports, default
 * CHIMERA_NFS_MAX_EXPORTS_DEFAULT): any id in 1..65535 may be pinned
 * regardless of how many exports the cap allows to exist at once.
 */
#define CHIMERA_NFS_EXPORT_ID_MAX       65535u

/* Default concurrent-export count cap (server config nfs_max_exports). */
#define CHIMERA_NFS_MAX_EXPORTS_DEFAULT 4096u

/*
 * Optional per-export settings applied atomically at creation time, so an
 * export is never visible to NFS request threads half-configured (an export
 * published with defaults and tightened afterward would be briefly rw and
 * unsquashed).  Each field is applied only when its has_ flag is set; a NULL
 * pointer or zeroed struct leaves every field at its default (rw, no
 * squashing, configured global anon ids, any security flavor).
 */
struct chimera_nfs_export_opts {
    unsigned has_access  : 1;
    unsigned has_squash  : 1;
    unsigned has_anonuid : 1;
    unsigned has_anongid : 1;
    unsigned has_sec     : 1;
    uint32_t access;       /* CHIMERA_NFS_EXPORT_ACCESS_* */
    uint32_t squash;       /* CHIMERA_NFS_SQUASH_*        */
    uint32_t anonuid;
    uint32_t anongid;
    uint32_t sec_allowed;  /* CHIMERA_NFS_SEC_* bitmask   */
};

/**
 * @brief Adds a new NFS export to the shared context.
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param name       Name of the export.
 * @param path       Filesystem path for the export.
 * @param export_id  Explicit export id (1..CHIMERA_NFS_EXPORT_ID_MAX), or 0
 *                   to auto-assign the next free id.  The id is embedded in
 *                   wire file handles, so clustered servers exporting the
 *                   same directory must pin identical ids.
 * @param opts       Optional per-export settings (may be NULL); see
 *                   struct chimera_nfs_export_opts.
 * @return 0 on success, -EINVAL if export_id is out of range, -EEXIST if an
 *         export with the same name exists, -EADDRINUSE if the id is already
 *         in use, -ENOSPC if the configured export limit (nfs_max_exports)
 *         is reached, -ENOMEM on allocation failure.
 */
int chimera_nfs_add_export(
    void                                 *nfs_shared,
    const char                           *name,
    const char                           *path,
    uint32_t                              export_id,
    const struct chimera_nfs_export_opts *opts);


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
 * @param out_export    Optional output (may be NULL) receiving the matched export.
 * @return 0 on success, non-zero if the export was not found or an error occurred.
 */
int
chimera_nfs_find_export_path(
    void                             *nfs_shared,
    const char                       *path,
    uint32_t                          path_len,
    char                            **out_full_path,
    const struct chimera_nfs_export **out_export);


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

/**
 * @brief Sets per-export options (RO/RW access mode, squash, anon uid/gid).
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param name       Name of the export.
 * @param access     CHIMERA_NFS_EXPORT_ACCESS_* bitmask.
 * @param squash     CHIMERA_NFS_SQUASH_* policy.
 * @param anonuid    Anonymous uid squashed callers are mapped to.
 * @param anongid    Anonymous gid squashed callers are mapped to.
 * @return 0 on success, -1 if no export with that name exists.
 */
int
chimera_nfs_export_set_options(
    void       *nfs_shared,
    const char *name,
    uint32_t    access,
    uint32_t    squash,
    uint32_t    anonuid,
    uint32_t    anongid);

int
chimera_nfs_export_set_sec(
    void       *nfs_shared,
    const char *name,
    uint32_t    sec_allowed);

/**
 * @brief Retrieves an NFS export by its stable id (as embedded in file handles).
 *
 * @param nfs_shared Pointer to the NFS shared context.
 * @param id         Export id (1-based; 0 is invalid).
 * @return Pointer to the export if found, NULL otherwise.
 */
const struct chimera_nfs_export *
chimera_nfs_get_export_by_id(
    void    *nfs_shared,
    uint16_t id);

/**
 * @brief Per-export attribute accessors.
 */
uint16_t
chimera_nfs_export_get_id(
    const struct chimera_nfs_export *export);

uint32_t
chimera_nfs_export_get_access(
    const struct chimera_nfs_export *export);

uint32_t
chimera_nfs_export_get_squash(
    const struct chimera_nfs_export *export);

uint32_t
chimera_nfs_export_get_anonuid(
    const struct chimera_nfs_export *export);

uint32_t
chimera_nfs_export_get_anongid(
    const struct chimera_nfs_export *export);

uint32_t
chimera_nfs_export_get_sec(
    const struct chimera_nfs_export *export);