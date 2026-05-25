// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"

struct evpl_iovec;

/* Synchronous, no-I/O check that a file handle is structurally valid and
 * resolves to a currently-mounted VFS module. Returns 1 if the handle could
 * name an object on this server, 0 if it is malformed or names an unknown
 * mount (the caller should map 0 to NFS4ERR_BADHANDLE / NFS3ERR_BADHANDLE).
 * It does NOT verify that the target object still exists. */
int
chimera_vfs_fh_is_plausible(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen);

typedef void (*chimera_vfs_mount_callback_t)(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data);

void
chimera_vfs_mount(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    const char                    *module_name,
    const char                    *module_path,
    const char                    *options,
    chimera_vfs_mount_callback_t   callback,
    void                          *private_data);

typedef void (*chimera_vfs_umount_callback_t)(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data);

void
chimera_vfs_umount(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    chimera_vfs_umount_callback_t  callback,
    void                          *private_data);

typedef void (*chimera_vfs_lookup_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

void
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

void
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


void
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

void
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

void
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

void
chimera_vfs_remove(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    chimera_vfs_remove_callback_t  callback,
    void                          *private_data);

void
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

void
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

void
chimera_vfs_link(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    const char                    *new_path,
    int                            new_pathlen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    chimera_vfs_link_callback_t    callback,
    void                          *private_data);

void
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

void
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

void
chimera_vfs_setattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data);

typedef void (*chimera_vfs_open_fh_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
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
void
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

void
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

/* Variant that persists an opaque handle-state record atomically with the
 * open (backends advertising CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE); handle_state
 * may be NULL, in which case it behaves exactly like chimera_vfs_open_at. */
void
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

void
chimera_vfs_create_unlinked(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const uint8_t                         *fh,
    int                                    fh_len,
    struct chimera_vfs_attrs              *attr,
    uint64_t                               attr_mask,
    chimera_vfs_create_unlinked_callback_t callback,
    void                                  *private_data);

typedef void (*chimera_vfs_close_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_close(
    struct chimera_vfs_thread   *thread,
    struct chimera_vfs_module   *vfs_module,
    uint64_t                     vfs_private,
    uint64_t                     fh_hash,
    chimera_vfs_close_callback_t callback,
    void                        *private_data);

typedef void (*chimera_vfs_mkdir_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
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

typedef void (*chimera_vfs_mknod_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
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

typedef void (*chimera_vfs_remove_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_remove_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_read_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
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
void
chimera_vfs_read_owned(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    struct evpl_iovec                    *iov,
    int                                   niov,
    uint64_t                              attrmask,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_read_callback_t           callback,
    void                                 *private_data);

typedef void (*chimera_vfs_write_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
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
void
chimera_vfs_write_owned(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    uint32_t                              sync,
    uint64_t                              pre_attr_mask,
    uint64_t                              post_attr_mask,
    struct evpl_iovec                    *iov,
    int                                   niov,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_write_callback_t          callback,
    void                                 *private_data);

typedef void (*chimera_vfs_commit_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
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

uint64_t
chimera_vfs_module_capabilities(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen);

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

void
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

typedef void (*chimera_vfs_readlink_callback_t)(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
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
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_link_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data);

void
chimera_vfs_link_at(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *dir_fh,
    int                            dir_fhlen,
    const char                    *name,
    int                            namelen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_link_at_callback_t callback,
    void                          *private_data);

/* Key-Value Operations */

void
chimera_vfs_put_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data);

void
chimera_vfs_get_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    chimera_vfs_get_key_callback_t callback,
    void                          *private_data);

void
chimera_vfs_delete_key(
    struct chimera_vfs_thread        *thread,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data);

/* fh-routed variants: operate on the backend serving `fh` rather than the
 * global kv_module (used for per-share handle-state records). */
void
chimera_vfs_delete_key_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data);

void
chimera_vfs_search_keys(
    struct chimera_vfs_thread         *thread,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data);

void
chimera_vfs_search_keys_at(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    const void                        *fh,
    int                                fhlen,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data);

typedef void (*chimera_vfs_allocate_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_allocate(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_allocate_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_copy_range_callback_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_clone_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
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

typedef void (*chimera_vfs_move_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *src_post_attr,
    struct chimera_vfs_attrs *dst_pre_attr,
    struct chimera_vfs_attrs *dst_post_attr,
    void                     *private_data);

void
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
chimera_vfs_seek(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        what,
    chimera_vfs_seek_callback_t     callback,
    void                           *private_data);

typedef void (*chimera_vfs_lock_callback_t)(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data);

void
chimera_vfs_lock(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    int32_t                         whence,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        lock_type,
    uint32_t                        flags,
    chimera_vfs_lock_callback_t     callback,
    void                           *private_data);

typedef void (*chimera_vfs_getparent_callback_t)(
    enum chimera_vfs_error error_code,
    const uint8_t         *parent_fh,
    uint16_t               parent_fh_len,
    const char            *name,
    uint16_t               name_len,
    void                  *private_data);

void
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

void
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

void
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

void
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

void
chimera_vfs_remove_xattr(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    const char                         *name,
    uint32_t                            namelen,
    chimera_vfs_remove_xattr_callback_t callback,
    void                               *private_data);