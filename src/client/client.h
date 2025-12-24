// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"

#define CHIMERA_CLIENT_IOV_MAX 260

struct chimera_client;
struct chimera_client_thread;
struct chimera_client_config;
struct chimera_client_fh;
struct prometheus_metrics;

struct chimera_client_config *
chimera_client_config_init(
    void);

void
chimera_client_config_add_module(
    struct chimera_client_config *config,
    const char                   *module_name,
    const char                   *module_path,
    const char                   *config_path);

struct chimera_client *
chimera_client_init(
    const struct chimera_client_config *config,
    struct prometheus_metrics          *metrics);

struct chimera_client_thread *
chimera_client_thread_init(
    struct evpl           *evpl,
    struct chimera_client *client);

void
chimera_client_thread_shutdown(
    struct evpl                  *evpl,
    struct chimera_client_thread *thread);

typedef void (*chimera_mount_callback_t)(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data);


void
chimera_mount(
    struct chimera_client_thread *client,
    const char                   *mount_path,
    const char                   *module_name,
    const char                   *module_path,
    chimera_mount_callback_t      callback,
    void                         *private_data);

typedef void (*chimera_umount_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_umount(
    struct chimera_client_thread *thread,
    const char                   *mount_path,
    chimera_umount_callback_t     callback,
    void                         *private_data);


void
chimera_drain(
    struct chimera_client_thread *thread);

typedef void (*chimera_open_callback_t)(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_open(
    struct chimera_client_thread *client,
    const char                   *path,
    int                           path_len,
    unsigned int                  flags,
    chimera_open_callback_t       callback,
    void                         *private_data);

typedef void (*chimera_mkdir_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_mkdir(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_mkdir_callback_t      callback,
    void                         *private_data);

typedef void (*chimera_read_callback_t)(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data);

void
chimera_read(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    chimera_read_callback_t         callback,
    void                           *private_data);

typedef void (*chimera_write_callback_t)(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_write(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_write_callback_t        callback,
    void                           *private_data);


void
chimera_close(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *oh);

typedef void (*chimera_symlink_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_symlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    const char                   *target,
    int                           target_len,
    chimera_symlink_callback_t    callback,
    void                         *private_data);

typedef void (*chimera_link_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_link(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_link_callback_t       callback,
    void                         *private_data);

typedef void (*chimera_remove_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_remove(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_remove_callback_t     callback,
    void                         *private_data);

typedef void (*chimera_rename_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_rename(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_rename_callback_t     callback,
    void                         *private_data);

#include <sys/time.h>
#include <stdint.h>

struct chimera_stat {
    uint64_t        st_dev;
    uint64_t        st_ino;
    uint64_t        st_mode;
    uint64_t        st_nlink;
    uint64_t        st_uid;
    uint64_t        st_gid;
    uint64_t        st_rdev;
    uint64_t        st_size;
    struct timespec st_atim;
    struct timespec st_mtim;
    struct timespec st_ctim;
};

typedef void (*chimera_readlink_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const char                   *target,
    int                           targetlen,
    void                         *private_data);

void
chimera_readlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    char                         *target,
    uint32_t                      target_maxlength,
    chimera_readlink_callback_t   callback,
    void                         *private_data);

typedef void (*chimera_stat_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data);

void
chimera_stat(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_stat_callback_t       callback,
    void                         *private_data);

typedef void (*chimera_fstat_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data);

void
chimera_fstat(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_fstat_callback_t        callback,
    void                           *private_data);

void
chimera_destroy(
    struct chimera_client *client);