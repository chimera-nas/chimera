#pragma once

#include "vfs.h"

struct evpl_iovec;

void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread *thread,
    void                      *fh,
    int                       *fh_len);

typedef void (*chimera_vfs_lookup_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

void
chimera_vfs_lookup(
    struct chimera_vfs_thread      *vfs,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    uint32_t                        namelen,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    chimera_vfs_lookup_callback_t   callback,
    void                           *private_data);

typedef void (*chimera_vfs_lookup_path_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *vfs,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    uint64_t                           attr_mask,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_getattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_getattr(
    struct chimera_vfs_thread      *thread,
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
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_setattr_callback_t callback,
    void                          *private_data);

typedef void (*chimera_vfs_readdir_complete_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  cookie,
    uint32_t                  eof,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_readdir(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attr_mask,
    uint64_t                       dir_attr_mask,
    uint64_t                       cookie,
    chimera_vfs_readdir_callback_t callback,
    chimera_vfs_readdir_complete_t complete,
    void                          *private_data);

typedef void (*chimera_vfs_open_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_vfs_open(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    unsigned int                flags,
    chimera_vfs_open_callback_t callback,
    void                       *private_data);

typedef void (*chimera_vfs_open_at_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data);

void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
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

typedef void (*chimera_vfs_close_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_close(
    struct chimera_vfs_thread   *thread,
    const void                  *fh,
    uint32_t                     fhlen,
    uint64_t                     vfs_private,
    chimera_vfs_close_callback_t callback,
    void                        *private_data);

typedef void (*chimera_vfs_mkdir_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_callback_t    callback,
    void                           *private_data);

typedef void (*chimera_vfs_remove_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_remove(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
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
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    const struct evpl_iovec        *iov,
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
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
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
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    const char                    *name,
    int                            namelen,
    const char                    *target,
    int                            targetlen,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_symlink_callback_t callback,
    void                          *private_data);

typedef void (*chimera_vfs_readlink_callback_t)(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data);

void
chimera_vfs_readlink(
    struct chimera_vfs_thread      *thread,
    const void                     *fh,
    int                             fhlen,
    void                           *target,
    uint32_t                        target_maxlength,
    chimera_vfs_readlink_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_rename_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_rename(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    int                           namelen,
    const void                   *new_fh,
    int                           new_fhlen,
    const char                   *new_name,
    int                           new_namelen,
    chimera_vfs_rename_callback_t callback,
    void                         *private_data);

typedef void (*chimera_vfs_link_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_link(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    const void                 *dir_fh,
    int                         dir_fhlen,
    const char                 *name,
    int                         namelen,
    chimera_vfs_link_callback_t callback,
    void                       *private_data);