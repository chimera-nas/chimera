#pragma once

#include "vfs/vfs.h"

struct chimera_client;
struct chimera_client_thread;
struct chimera_client_config;
struct chimera_client_fh;

struct chimera_client_config *
chimera_client_config_init(
    void);

struct chimera_client *
chimera_client_init(
    const struct chimera_client_config *config);

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



typedef void (*chimera_client_lookup_path_callback_t)(
    struct chimera_client    *client,
    enum chimera_vfs_error    status,
    struct chimera_client_fh *fh,
    void                     *private_data);

void
chimera_client_lookup_path(
    struct chimera_client                *client,
    const char                           *path,
    chimera_client_lookup_path_callback_t callback,
    void                                 *private_data);

typedef void (*chimera_client_open_path_callback_t)(
    struct chimera_client    *client,
    enum chimera_vfs_error    status,
    struct chimera_client_fh *fh,
    void                     *private_data);

void
chimera_client_open_path(
    struct chimera_client              *client,
    const char                         *path,
    unsigned int                        flags,
    chimera_client_open_path_callback_t callback,
    void                               *private_data);

void
chimera_client_destroy(
    struct chimera_client *client);