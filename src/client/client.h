#pragma once

#include "vfs/vfs.h"

struct chimera_client;
struct chimera_client_thread;
struct chimera_client_config;
struct chimera_client_fh;
struct prometheus_metrics;
struct chimera_client_config *
chimera_client_config_init(
    void);

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
chimera_client_mount(
    struct chimera_client *client,
    const char            *mount_path,
    const char            *module_name,
    const char            *module_path);

int
chimera_client_umount(
    struct chimera_client *client,
    const char            *mount_path);

typedef void (*chimera_client_open_callback_t)(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_client_open(
    struct chimera_client_thread  *client,
    const char                    *path,
    int                            path_len,
    unsigned int                   flags,
    chimera_client_open_callback_t callback,
    void                          *private_data);

typedef void (*chimera_client_mkdir_callback_t)(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data);

void
chimera_client_mkdir(
    struct chimera_client_thread   *thread,
    const char                     *path,
    int                             path_len,
    chimera_client_mkdir_callback_t callback,
    void                           *private_data);

void
chimera_client_close(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *oh);

void
chimera_client_destroy(
    struct chimera_client *client);