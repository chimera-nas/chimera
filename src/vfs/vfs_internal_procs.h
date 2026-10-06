// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"
#ifndef CHIMERA_VFS_INTERNAL_API
#error "The per-operation VFS API is private; frontend consumers must use vfs_compound.h"
#endif // ifndef CHIMERA_VFS_INTERNAL_API

#include <stddef.h>
#include "vfs.h"
#include "vfs_kv.h"
#include "vfs_claim.h"
#include "vfs_dirent.h"
#include "vfs_release.h"

struct evpl_iovec;

typedef void (*chimera_vfs_lookup_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_lookup_at(
    struct chimera_vfs_thread       *vfs,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    uint64_t                         attr_mask,
    uint64_t                         dir_attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data);

/* Answer a name lookup with the handle it resolved to on another filesystem
 * (a mount point's root, or the parent of a mount root): open it, fetch
 * attr_mask, and complete as lookup_at would.  See vfs_mount_cross.c. */
void
chimera_vfs_lookup_at_redirect(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    int                              fhlen,
    uint64_t                         attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data);

SYMBOL_EXPORT void
chimera_vfs_lookup(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    uint64_t                       attr_mask,
    uint32_t                       flags,
    chimera_vfs_lookup_callback_t  callback,
    void                          *private_data);


SYMBOL_EXPORT void
chimera_vfs_create(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_create_callback_t  callback,
    void                          *private_data);

/* Path-based operations */

SYMBOL_EXPORT void
chimera_vfs_open(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_callback_t    callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_mkdir(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_mkdir_callback_t   callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_remove(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    chimera_vfs_remove_callback_t  callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_rename(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    const char                    *new_path,
    int                            new_pathlen,
    chimera_vfs_rename_callback_t  callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_symlink(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    const char                    *target,
    int                            targetlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_symlink_callback_t callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_link(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    /* CHIMERA_VFS_LOOKUP_FOLLOW resolves a final-component symlink in
     * old_path and links its target (linkat AT_SYMLINK_FOLLOW); 0 links
     * the symlink itself. */
    unsigned int                   source_lookup_flags,
    const char                    *new_path,
    int                            new_pathlen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    chimera_vfs_link_callback_t    callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_mknod(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_mknod_callback_t   callback,
    void                          *private_data);

SYMBOL_EXPORT void
chimera_vfs_find(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attr_mask,
    chimera_vfs_filter_callback_t  filter,
    chimera_vfs_find_callback_t    callback,
    chimera_vfs_find_complete_t    complete,
    void                          *private_data);



typedef void (*chimera_vfs_getattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_getattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    chimera_vfs_getattr_callback_t  callback,
    void                           *private_data);

typedef void (*chimera_vfs_setattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

/* Internal metadata completion of an already authorized data mutation.
 * Skips authorization, recall, and pNFS redirection; never use for client SETATTR. */
SYMBOL_EXPORT void chimera_vfs_setattr_after_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_setattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

/* Compound admission view: retain the actor through authorization/recalls,
 * or admit anonymous size changes with the attempt's private exclusions.
 * The actor is copied; exclusions remain borrowed through completion. */
void
chimera_vfs_setattr_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_setattr_callback_t    callback,
    void                             *private_data);

/* Descriptor-originated variant: WRITE_DATA-only mutations (ftruncate,
 * futimens-to-now) are authorized by the handle's open-time access grant
 * rather than the file's current mode (POSIX rights retention). */
void
chimera_vfs_fsetattr_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_setattr_callback_t    callback,
    void                             *private_data);

SYMBOL_EXPORT void
chimera_vfs_fsetattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

/* Apply an already-admitted overwrite to this exact open object.  set_attr
 * must request SIZE=0.  In addition to ordinary setattr semantics, a base
 * file overwrite removes its named streams, matching OPEN_TRUNCATE; an
 * individual named stream overwrite preserves the base and sibling forks.
 * Callers must hold their overwrite/share reservation until completion. */
SYMBOL_EXPORT void
chimera_vfs_overwrite(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          post_attr_mask,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_setattr_callback_t    callback,
    void                             *private_data);

SYMBOL_EXPORT void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    const char                     *match_pattern,
    int                             match_pattern_len,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data);

typedef void (*chimera_vfs_open_fh_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_open_fh(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    unsigned int                   flags,
    chimera_vfs_open_fh_callback_t callback,
    void                          *private_data);

/* Variant that persists an opaque handle-state record atomically with the
 * open (backends advertising CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE); handle_state
 * may be NULL, in which case it behaves exactly like chimera_vfs_open_fh. */
SYMBOL_EXPORT void
chimera_vfs_open_fh_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    unsigned int                     flags,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_fh_callback_t   callback,
    void                            *private_data);

typedef void (*chimera_vfs_open_at_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data);

/* Answer an open_at whose name is a mount point by opening the mounted
 * root (handle fh) instead.  See vfs_mount_cross.c. */
void
chimera_vfs_open_at_redirect(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    int                            fhlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_at_callback_t callback,
    void                          *private_data);

/* Variant that persists an opaque handle-state record atomically with the
 * open (backends advertising CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE); handle_state
 * may be NULL, in which case it behaves exactly like chimera_vfs_open_at. */
SYMBOL_EXPORT void
chimera_vfs_open_at_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data);


typedef void (*chimera_vfs_create_unlinked_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_create_unlinked(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const uint8_t                         *fh,
    int                                    fh_len,
    struct chimera_vfs_attrs              *attr,
    uint64_t                               attr_mask,
    chimera_vfs_create_unlinked_callback_t callback,
    void                                  *private_data);

typedef void (*chimera_vfs_mkdir_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_mkdir_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_at_callback_t callback,
    void                           *private_data);

/* NO_NOTIFY suppresses observer publication, preserving authorization and
* namespace/attribute caches. The frontend emits after accepted finish. */
void
chimera_vfs_mkdir_at_flags(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint32_t                        flags,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_at_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_mknod_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_mknod_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_at_callback_t callback,
    void                           *private_data);

/* NO_NOTIFY defers observer publication; authorization and caches remain active. */
void
chimera_vfs_mknod_at_flags(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint32_t                        flags,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_at_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_remove_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_remove_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data);

/* Inode-scoped remove (only unlinks the name while it still resolves to
 * child_fh).  Same signature as chimera_vfs_remove_at; see its definition. */
SYMBOL_EXPORT void
chimera_vfs_remove_at_match_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data);

void
chimera_vfs_remove_at_match_fh_actor(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const uint8_t                    *child_fh,
    int                               child_fh_len,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const uint8_t                    *parent_lease_skip,
    const struct chimera_claim_actor *actor,
    chimera_vfs_remove_at_callback_t  callback,
    void                             *private_data);

/* Strict atomic identity match; requires CAP_REMOVE_MATCH_FH. NO_NOTIFY only
 * defers external events: cache invalidation and recall still execute. Optional
 * unmatched_out remains valid through callback and reports successful no-op. */
void chimera_vfs_remove_at_match_fh_flags(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const uint8_t                    *child_fh,
    int                               child_fh_len,
    unsigned int                      flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const uint8_t                    *parent_lease_skip,
    const struct chimera_claim_actor *actor,
    uint8_t                          *unmatched_out,
    chimera_vfs_remove_at_callback_t  callback,
    void                             *private_data);

typedef void (*chimera_vfs_read_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *iov,
    int                             niov,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data);

/* As chimera_vfs_read(), but attributes the I/O to `io_owner` (a lease-
 * holding client's owner) so its own delegation/oplock is not recalled by
 * its own read.  Pass NULL to have chimera hold an implicit lease on behalf
 * of a leaseless actor (equivalent to chimera_vfs_read()). */
SYMBOL_EXPORT void
chimera_vfs_read_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               niov,
    uint64_t                          attrmask,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_read_callback_t       callback,
    void                             *private_data);

/* Separate admission view preserves anonymous claim checks with scoped exclusions. */
void
chimera_vfs_read_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               niov,
    uint64_t                          attrmask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_read_callback_t       callback,
    void                             *private_data);

/* As chimera_vfs_read(), but the caller supplies its own destination buffers
 * (dest_iov/dest_niov) for the data to land in.  work_iov/work_niov is scratch
 * the core/backend reads through; on completion the data is guaranteed to be in
 * dest_iov (zero-copy where a backend can land it there directly, a scatter-
 * copy otherwise).  The caller retains ownership of dest_iov (borrow): it must
 * keep the buffers alive until the callback and release them afterwards.  The
 * callback's iov/niov reference dest_iov. */
SYMBOL_EXPORT void
chimera_vfs_read_into(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *work_iov,
    int                             work_niov,
    struct evpl_iovec              *dest_iov,
    int                             dest_niov,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data);

typedef void (*chimera_vfs_write_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data);

/* As chimera_vfs_write(), but attributes the I/O to `io_owner` (a lease-
 * holding client's owner) so its own write delegation/oplock is not recalled
 * by its own write, while other holders' read caches are still invalidated.
 * Pass NULL to have chimera hold an implicit lease on behalf of a leaseless
 * actor (equivalent to chimera_vfs_write()). */
SYMBOL_EXPORT void
chimera_vfs_write_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data);

/* Separate admission view preserves anonymous claim checks with scoped exclusions. */
void
chimera_vfs_write_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data);

typedef void (*chimera_vfs_commit_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_commit(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        count,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_commit_callback_t   callback,
    void                           *private_data);

/* pNFS: ask a layout-sourcing backend (CHIMERA_VFS_CAP_LAYOUT_SOURCE) where a
 * file's data lives.  segments/devices are valid only while the callback runs. */
typedef void (*chimera_vfs_get_layout_callback_t)(
    enum chimera_vfs_error                   error_code,
    uint32_t                                 layout_class,
    uint32_t                                 num_segments,
    const struct chimera_vfs_layout_segment *segments,
    uint32_t                                 num_devices,
    const struct chimera_vfs_layout_device  *devices,
    void                                    *private_data);

void
chimera_vfs_get_layout(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    uint32_t                          iomode,
    uint32_t                          layout_class,
    uint32_t                          max_segments,
    chimera_vfs_get_layout_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_symlink_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_symlink_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const char                       *target,
    int                               targetlen,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_symlink_at_callback_t callback,
    void                             *private_data);

/* NO_NOTIFY defers observer publication; authorization and caches remain active. */
void
chimera_vfs_symlink_at_flags(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const char                       *target,
    int                               targetlen,
    struct chimera_vfs_attrs         *set_attr,
    uint32_t                          flags,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_symlink_at_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_readlink_callback_t)(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_readlink(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    void                           *target,
    uint32_t                        target_maxlength,
    uint64_t                        attr_mask,
    chimera_vfs_readlink_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_rename_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data);

void
chimera_vfs_rename_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data);

/* Optional strict namespace semantics: NOREPLACE and MATCH_SOURCE_FH require
 * backend capabilities before authorization/recall/dispatch. Source mismatch
 * returns ESTALE, occupied destination EEXIST (including same-inode aliases).
 * Inputs are immutable through callback. NO_NOTIFY preserves VFS cache updates
 * but leaves namespace observer publication to accepted frontend completion. */
void
chimera_vfs_rename_at_checked(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    const uint8_t                   *match_source_fh,
    uint32_t                         match_source_fh_len,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data);

/* Result storage is caller-owned through callback, initialized UNKNOWN on entry.
* MATCH_DEST_FH uses target_fh, requires its strict backend capability, and may
* not be combined with NOREPLACE. OUTCOME capability makes successful results
* authoritative, including same-inode no-ops. Older entrypoints are preserved. */
void
chimera_vfs_rename_at_checked_result(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    const uint8_t                   *match_source_fh,
    uint32_t                         match_source_fh_len,
    enum chimera_vfs_rename_outcome *outcome,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data);

/* Copies the actor and borrows pinned exclusions through callback completion.
 * ParentLeaseKey only overrides the directory-notify key. */
void
chimera_vfs_rename_at_checked_result_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const char                       *name,
    int                               namelen,
    const void                       *new_fh,
    int                               new_fhlen,
    const char                       *new_name,
    int                               new_namelen,
    const uint8_t                    *target_fh,
    int                               target_fh_len,
    unsigned int                      flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const uint8_t                    *parent_lease_skip,
    struct chimera_vfs_open_handle   *op_handle,
    const struct chimera_vfs_io_view *view,
    const uint8_t                    *match_source_fh,
    uint32_t                          match_source_fh_len,
    enum chimera_vfs_rename_outcome  *outcome,
    chimera_vfs_rename_at_callback_t  callback,
    void                             *private_data);

typedef void (*chimera_vfs_link_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_link_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    const void                     *fh,
    int                             fhlen,
    const void                     *dir_fh,
    int                             dir_fhlen,
    const char                     *name,
    int                             namelen,
    unsigned int                    replace,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    const uint8_t                  *parent_lease_skip,
    struct chimera_vfs_open_handle *op_handle,
    chimera_vfs_link_at_callback_t  callback,
    void                           *private_data);

/* A compound frontend takes responsibility for accepted-only FILE_ADDED
 * notification when it requests suppression during tentative execution. */

void
chimera_vfs_link_at_flags(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    const void                     *fh,
    int                             fhlen,
    const void                     *dir_fh,
    int                             dir_fhlen,
    const char                     *name,
    int                             namelen,
    unsigned int                    replace,
    unsigned int                    flags,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    const uint8_t                  *parent_lease_skip,
    struct chimera_vfs_open_handle *op_handle,
    chimera_vfs_link_at_callback_t  callback,
    void                           *private_data);

/* Copies the actor and borrows pinned exclusions through callback completion.
 * ParentLeaseKey only overrides the directory-notify key. */
void
chimera_vfs_link_at_flags_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const void                       *dir_fh,
    int                               dir_fhlen,
    const char                       *name,
    int                               namelen,
    unsigned int                      replace,
    unsigned int                      flags,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const uint8_t                    *parent_lease_skip,
    struct chimera_vfs_open_handle   *op_handle,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_link_at_callback_t    callback,
    void                             *private_data);

typedef void (*chimera_vfs_allocate_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

/* Allocation admission uses the compound actor/private exclusions. The
 * caller validates mandatory ranges against its transactional view. */
void
chimera_vfs_allocate_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_allocate_callback_t   callback,
    void                             *private_data);

typedef void (*chimera_vfs_copy_range_callback_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data);

/* Preserve endpoint claim identities through the streaming fallback. Anonymous
 * endpoints (NULL actor), including SDK/SMB/S3 callers, use bounded read/write
 * fallback even on native-copy modules so claim admission cannot be bypassed.
 * Fully owned copies without exclusions retain the native optimization. */
void
chimera_vfs_copy_range_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_claim_actor *src_owner,
    const struct chimera_claim_actor *dst_owner,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data);

/* Separate admission view preserves anonymous claim checks with scoped exclusions. */
void
chimera_vfs_copy_range_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *src_view,
    const struct chimera_vfs_io_view *dst_view,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_clone_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

/* Claim-owning native clone. Both endpoints retain their frontend grants;
 * destination WRITE invalidation must settle before backend dispatch. */
void
chimera_vfs_clone_range_view(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *src_handle,
    uint64_t                           src_offset,
    struct chimera_vfs_open_handle    *dst_handle,
    uint64_t                           dst_offset,
    uint64_t                           length,
    uint64_t                           pre_attr_mask,
    uint64_t                           post_attr_mask,
    const struct chimera_claim_actor  *src_owner,
    const struct chimera_vfs_io_view  *dst_view,
    chimera_vfs_clone_range_callback_t callback,
    void                              *private_data);

SYMBOL_EXPORT void
chimera_vfs_clone_range(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *src_handle,
    uint64_t                           src_offset,
    struct chimera_vfs_open_handle    *dst_handle,
    uint64_t                           dst_offset,
    uint64_t                           length,
    uint64_t                           pre_attr_mask,
    uint64_t                           post_attr_mask,
    chimera_vfs_clone_range_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_read_plus_callback_t)(
    enum chimera_vfs_error error_code,
    uint32_t               is_data,
    uint64_t               length,
    uint32_t               eof,
    void                  *private_data);

void
chimera_vfs_read_plus_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_read_plus_callback_t  callback,
    void                             *private_data);

typedef void (*chimera_vfs_write_same_callback_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  count,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_write_same_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          block_size,
    uint64_t                          block_count,
    const void                       *pattern,
    uint32_t                          pattern_len,
    uint32_t                          reloff_pattern,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_write_same_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_move_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *src_post_attr,
    struct chimera_vfs_attrs *dst_pre_attr,
    struct chimera_vfs_attrs *dst_post_attr,
    void                     *private_data);

SYMBOL_EXPORT void
chimera_vfs_move_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          src_post_attr_mask,
    uint64_t                          dst_pre_attr_mask,
    uint64_t                          dst_post_attr_mask,
    chimera_vfs_move_range_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_seek_callback_t)(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data);

void
chimera_vfs_seek_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          what,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_seek_callback_t       callback,
    void                             *private_data);

typedef void (*chimera_vfs_getparent_callback_t)(
    enum chimera_vfs_error error_code,
    const uint8_t         *parent_fh,
    uint16_t               parent_fh_len,
    const char            *name,
    uint16_t               name_len,
    void                  *private_data);

SYMBOL_EXPORT void
chimera_vfs_getparent(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    chimera_vfs_getparent_callback_t callback,
    void                            *private_data);

/*
 * RFC 8276 extended attribute operations.
 */

typedef void (*chimera_vfs_get_xattr_callback_t)(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data);

SYMBOL_EXPORT void
chimera_vfs_get_xattr(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    void                            *value,
    uint32_t                         value_maxlen,
    chimera_vfs_get_xattr_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_set_xattr_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_set_xattr(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    uint32_t                         option,
    const char                      *name,
    uint32_t                         namelen,
    const void                      *value,
    uint32_t                         value_len,
    chimera_vfs_set_xattr_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_list_xattrs_callback_t)(
    enum chimera_vfs_error error_code,
    const char            *names,    /* back-to-back NUL-terminated names */
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data);

SYMBOL_EXPORT void
chimera_vfs_list_xattrs(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *handle,
    uint64_t                           cookie,
    void                              *buffer,
    uint32_t                           max_bytes,
    chimera_vfs_list_xattrs_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_remove_xattr_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_remove_xattr(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    const char                         *name,
    uint32_t                            namelen,
    chimera_vfs_remove_xattr_callback_t callback,
    void                               *private_data);

/*
 * Named-stream (SMB Alternate Data Stream) operations.
 * Gated by CHIMERA_VFS_CAP_NAMED_STREAMS.
 */

/* Open (and optionally create/truncate) a named data fork on the base file
 * referenced by `handle`.  On success `oh` is a VFS open handle for the
 * stream: read/write/getattr/setattr against it operate on the stream's own
 * data and size, while metadata (mode/owner/timestamps) mirror the base file.
 * `set_attr` may be NULL.  Stream-open `flags` use CHIMERA_VFS_OPEN_* (CREATE/
 * EXCLUSIVE/TRUNCATE) with the same semantics as chimera_vfs_open_at. */
typedef void (*chimera_vfs_open_stream_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_open_stream(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *handle,
    const char                        *name,
    uint32_t                           namelen,
    uint32_t                           flags,
    struct chimera_vfs_attrs          *set_attr,
    uint64_t                           attr_mask,
    chimera_vfs_open_stream_callback_t callback,
    void                              *private_data);

/* Enumerate the named streams of the base file referenced by `handle`.  The
 * buffer is filled with packed struct chimera_vfs_stream_entry records (each
 * followed by name_len name bytes); the default unnamed data fork is reported
 * first as an entry with an empty name and the file's size. */
typedef void (*chimera_vfs_list_streams_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *records,  /* packed chimera_vfs_stream_entry + names */
    uint32_t               records_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    uint64_t               verifier,
    void                  *private_data);

SYMBOL_EXPORT void
chimera_vfs_list_streams(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    uint64_t                            cookie,
    uint64_t                            verifier,
    void                               *buffer,
    uint32_t                            max_bytes,
    int                                 want_fh,
    chimera_vfs_list_streams_callback_t callback,
    void                               *private_data);

/* Rename a named stream of the base file referenced by `handle` (MS-FSA
 * 2.1.5.15.12.1).  An existing stream of the new name is replaced only with
 * CHIMERA_VFS_RENAME_STREAM_REPLACE (EEXIST otherwise), and only while it is
 * empty (EINVAL otherwise).  Open handles to the renamed stream stay valid. */
typedef void (*chimera_vfs_rename_stream_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_rename_stream(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    const char                          *new_name,
    uint32_t                             new_namelen,
    uint32_t                             flags,
    chimera_vfs_rename_stream_callback_t callback,
    void                                *private_data);

/* Remove a single named stream from the base file referenced by `handle`. */
typedef void (*chimera_vfs_remove_stream_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

SYMBOL_EXPORT void
chimera_vfs_remove_stream(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    chimera_vfs_remove_stream_callback_t callback,
    void                                *private_data);

/* Strict checked unlink is atomic and never emulated with lookup+remove.
 * MATCH_FH requires CAP_REMOVE_STREAM_MATCH_FH; mismatch returns ESTALE.
 * This API emits no notifications; protocol publication follows acceptance. */
void
chimera_vfs_remove_stream_checked(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    uint32_t                             flags,
    const uint8_t                       *expected_fh,
    uint32_t                             expected_fh_len,
    chimera_vfs_remove_stream_callback_t callback,
    void                                *private_data);
