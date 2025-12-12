// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_POSIX_INTERNAL_H
#define CHIMERA_POSIX_INTERNAL_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "../client/client.h"
#include "../client/client_internal.h"
#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "posix.h"

enum chimera_posix_request_type {
    CHIMERA_POSIX_REQ_OPEN,
    CHIMERA_POSIX_REQ_CLOSE,
    CHIMERA_POSIX_REQ_READ,
    CHIMERA_POSIX_REQ_WRITE,
    CHIMERA_POSIX_REQ_MKDIR,
    CHIMERA_POSIX_REQ_SYMLINK,
    CHIMERA_POSIX_REQ_LINK,
    CHIMERA_POSIX_REQ_REMOVE,
    CHIMERA_POSIX_REQ_RENAME,
    CHIMERA_POSIX_REQ_READLINK,
    CHIMERA_POSIX_REQ_STAT,
    CHIMERA_POSIX_REQ_MOUNT,
    CHIMERA_POSIX_REQ_UMOUNT,
};

struct chimera_posix_request {
    pthread_mutex_t                 lock;
    pthread_cond_t                  cond;
    int                             done;
    enum chimera_vfs_error          status;
    ssize_t                         result;
    struct chimera_vfs_open_handle *handle;
    struct chimera_stat             st;
    int                             target_len;
    enum chimera_posix_request_type type;
    struct chimera_posix_request   *next;
    union {
        struct {
            char *path;
            int   flags;
        } open;
        struct {
            struct chimera_vfs_open_handle *handle;
        } close;
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            size_t                          length;
            void                           *buf;
        } read;
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            size_t                          length;
            const void                     *buf;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
            int                             niov;
        } write;
        struct {
            char *path;
        } mkdir;
        struct {
            char *path;
            char *target;
        } symlink;
        struct {
            char *oldpath;
            char *newpath;
        } link;
        struct {
            char *path;
        } remove;
        struct {
            char *oldpath;
            char *newpath;
        } rename;
        struct {
            char  *path;
            char  *buf;
            size_t buflen;
        } readlink;
        struct {
            char *path;
        } stat;
        struct {
            char *mount_path;
            char *module_name;
            char *module_path;
        } mount;
        struct {
            char *mount_path;
        } umount;
    } u;
};

struct chimera_posix_fd_entry {
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    int                             in_use;
};

struct chimera_posix_worker {
    pthread_mutex_t               lock;
    struct chimera_posix_request *head;
    struct chimera_posix_request *tail;
    struct evpl_doorbell          doorbell;
    struct chimera_client_thread *client_thread;
    struct chimera_posix_client  *parent;
    int                           index;
    struct evpl                  *evpl;
};

struct chimera_posix_client {
    struct chimera_client         *client;
    struct evpl_threadpool        *pool;
    struct chimera_posix_worker   *workers;
    int                            nworkers;
    atomic_uint                    next_worker;
    pthread_mutex_t                fd_lock;
    struct chimera_posix_fd_entry *fds;
    int                            fd_cap;
    int                            next_fd;
    atomic_int                     init_cursor;
    int                            owns_config;
};

extern struct chimera_posix_client *chimera_posix_global;

struct chimera_posix_client * chimera_posix_get_global(
    void);

struct chimera_posix_request * chimera_posix_request_create(
    enum chimera_posix_request_type type);
void chimera_posix_request_destroy(
    struct chimera_posix_request *req);
void chimera_posix_request_finish(
    struct chimera_posix_request *req,
    enum chimera_vfs_error        status);

void chimera_posix_worker_enqueue(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
struct chimera_posix_worker * chimera_posix_choose_worker(
    struct chimera_posix_client *posix);
int chimera_posix_wait(
    struct chimera_posix_request *req);

int chimera_posix_errno_from_status(
    enum chimera_vfs_error status);
void chimera_posix_fill_stat(
    struct stat               *dst,
    const struct chimera_stat *src);
void chimera_posix_iovec_memcpy(
    struct evpl_iovec *iov,
    const void        *buf,
    size_t             len);
unsigned int chimera_posix_to_chimera_flags(
    int flags);

struct chimera_posix_fd_entry * chimera_posix_fd_get(
    struct chimera_posix_client *posix,
    int                          fd);
int chimera_posix_fd_put(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle);
void chimera_posix_fd_clear(
    struct chimera_posix_client *posix,
    int                          fd);

void * chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

void chimera_posix_dispatch_request(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);

void chimera_posix_exec_open(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_close(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_read(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_write(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_mkdir(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_symlink(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_link(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_remove(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_rename(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_readlink(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_stat(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_mount(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_umount(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);

#endif /* CHIMERA_POSIX_INTERNAL_H */

