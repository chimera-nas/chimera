// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"

struct evpl_iovec;


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

typedef void (*chimera_vfs_lookup_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

void
chimera_vfs_lookup(
    struct chimera_vfs_thread      *vfs,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    uint32_t                        namelen,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    chimera_vfs_lookup_callback_t   callback,
    void                           *private_data);

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *vfs,
    const struct chimera_vfs_cred     *cred,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    uint64_t                           attr_mask,
    uint32_t                           flags,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data);


void
chimera_vfs_create_path(
    struct chimera_vfs_thread         *vfs,
    const struct chimera_vfs_cred     *cred,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    struct chimera_vfs_attrs          *set_attr,
    uint64_t                           attr_mask,
    chimera_vfs_create_path_callback_t callback,
    void                              *private_data);

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

typedef void (*chimera_vfs_open_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_vfs_open(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    unsigned int                   flags,
    chimera_vfs_open_callback_t    callback,
    void                          *private_data);

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
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_vfs_close_callback_t    callback,
    void                           *private_data);

typedef void (*chimera_vfs_mkdir_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_callback_t    callback,
    void                           *private_data);

typedef void (*chimera_vfs_mknod_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_mknod(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_callback_t    callback,
    void                           *private_data);

typedef void (*chimera_vfs_remove_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_remove(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    const uint8_t                  *child_fh,
    int                             child_fh_len,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_remove_callback_t   callback,
    void                           *private_data);

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

typedef void (*chimera_vfs_symlink_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_symlink(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    const char                     *target,
    int                             targetlen,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_symlink_callback_t  callback,
    void                           *private_data);

typedef void (*chimera_vfs_readlink_callback_t)(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data);

void
chimera_vfs_readlink(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    void                           *target,
    uint32_t                        target_maxlength,
    chimera_vfs_readlink_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_rename_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data);

void
chimera_vfs_rename(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *name,
    int                            namelen,
    const void                    *new_fh,
    int                            new_fhlen,
    const char                    *new_name,
    int                            new_namelen,
    const uint8_t                 *target_fh,
    int                            target_fh_len,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_rename_callback_t  callback,
    void                          *private_data);

typedef void (*chimera_vfs_link_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data);

void
chimera_vfs_link(
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
    chimera_vfs_link_callback_t    callback,
    void                          *private_data);