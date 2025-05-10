#pragma once

#include "vfs/vfs.h"

#define CHIMERA_CLIENT_IOV_MAX 256

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

int
chimera_mount(
    struct chimera_client *client,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path);

int
chimera_umount(
    struct chimera_client *client,
    const char            *mount_path);


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

void
chimera_destroy(
    struct chimera_client *client);