#pragma once

#include "vfs.h"
void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread *thread,
    void                      *fh,
    int                       *fh_len);

typedef void (*chimera_vfs_lookup_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *fh,
    int                    fh_len,
    void                  *private_data);

void
chimera_vfs_lookup(
    struct chimera_vfs_thread    *vfs,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    uint32_t                      namelen,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data);

typedef void (*chimera_vfs_lookup_path_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *fh,
    int                    fh_len,
    void                  *private_data);

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *vfs,
    const char                        *path,
    int                                pathlen,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_getattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_getattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attr_mask,
    chimera_vfs_getattr_callback_t callback,
    void                          *private_data);

typedef void (*chimera_vfs_readdir_complete_t)(
    enum chimera_vfs_error error_code,
    uint64_t               cookie,
    uint32_t               eof,
    void                  *private_data);

void
chimera_vfs_readdir(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attrmask,
    uint64_t                       cookie,
    chimera_vfs_readdir_callback_t callback,
    chimera_vfs_readdir_complete_t complete,
    void                          *private_data);

typedef void (*chimera_vfs_open_callback_t)(
    enum chimera_vfs_error          error_code,
    const void                     *fh,
    int                             fhlen,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_vfs_open_at(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    const char                 *name,
    int                         namelen,
    unsigned int                flags,
    unsigned int                mode,
    chimera_vfs_open_callback_t callback,
    void                       *private_data);

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
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread   *thread,
    const void                  *fh,
    int                          fhlen,
    const char                  *name,
    int                          namelen,
    unsigned int                 mode,
    chimera_vfs_mkdir_callback_t callback,
    void                        *private_data);

typedef void (*chimera_vfs_remove_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_remove(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    int                           namelen,
    chimera_vfs_remove_callback_t callback,
    void                         *private_data);
