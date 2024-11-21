#pragma once
#include <stdint.h>
#include <sys/time.h>
#include "vfs_dump.h"

#define CHIMERA_VFS_FH_SIZE          128

struct chimera_vfs;

#define CHIMERA_VFS_ATTR_DEV         (1UL << 0)
#define CHIMERA_VFS_ATTR_INUM        (1UL << 1)
#define CHIMERA_VFS_ATTR_MODE        (1UL << 2)
#define CHIMERA_VFS_ATTR_NLINK       (1UL << 3)
#define CHIMERA_VFS_ATTR_UID         (1UL << 4)
#define CHIMERA_VFS_ATTR_GID         (1UL << 5)
#define CHIMERA_VFS_ATTR_RDEV        (1UL << 6)
#define CHIMERA_VFS_ATTR_SIZE        (1UL << 7)
#define CHIMERA_VFS_ATTR_ATIME       (1UL << 8)
#define CHIMERA_VFS_ATTR_MTIME       (1UL << 9)
#define CHIMERA_VFS_ATTR_CTIME       (1UL << 10)

#define CHIMERA_VFS_ATTR_SPACE_AVAIL (1UL << 11)
#define CHIMERA_VFS_ATTR_SPACE_FREE  (1UL << 12)
#define CHIMERA_VFS_ATTR_SPACE_TOTAL (1UL << 13)
#define CHIMERA_VFS_ATTR_SPACE_USED  (1UL << 14)

#define CHIMERA_VFS_ATTR_FH          (1UL << 15)

#define CHIMERA_VFS_ATTR_MASK_STAT   ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_VFS_ATTR_MASK_STATFS ( \
            CHIMERA_VFS_ATTR_SPACE_AVAIL | \
            CHIMERA_VFS_ATTR_SPACE_FREE | \
            CHIMERA_VFS_ATTR_SPACE_TOTAL | \
            CHIMERA_VFS_ATTR_SPACE_USED)

struct chimera_vfs_attrs {
    uint64_t        va_mask;

    uint64_t        va_dev;
    uint64_t        va_ino;
    uint64_t        va_mode;
    uint64_t        va_nlink;
    uint64_t        va_uid;
    uint64_t        va_gid;
    uint64_t        va_rdev;
    uint64_t        va_size;
    struct timespec va_atime;
    struct timespec va_mtime;
    struct timespec va_ctime;

    uint64_t        va_space_avail;
    uint64_t        va_space_free;
    uint64_t        va_space_total;
    uint64_t        va_space_used;

    uint8_t         va_fh[128];
    uint32_t        va_fh_len;
};

#define CHIMERA_VFS_OP_LOOKUP_PATH 1
#define CHIMERA_VFS_OP_LOOKUP      2
#define CHIMERA_VFS_OP_GETATTR     3
#define CHIMERA_VFS_OP_READDIR     4
#define CHIMERA_VFS_OP_READLINK    5
#define CHIMERA_VFS_OP_OPEN        6
#define CHIMERA_VFS_OP_OPEN_AT     7
#define CHIMERA_VFS_OP_CLOSE       8
#define CHIMERA_VFS_OP_READ        9
#define CHIMERA_VFS_OP_WRITE       10
#define CHIMERA_VFS_OP_REMOVE      11
#define CHIMERA_VFS_OP_MKDIR       12

#define CHIMERA_VFS_OPEN_CREATE    (1U << 0)
#define CHIMERA_VFS_OPEN_RDONLY    (1U << 1)
#define CHIMERA_VFS_OPEN_WRONLY    (1U << 2)
#define CHIMERA_VFS_OPEN_RDWR      (1U << 3)

typedef void (*chimera_vfs_complete_callback_t)(
    void);

typedef int (*chimera_vfs_readdir_callback_t)(
    uint64_t                        cookie,
    const char                     *name,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg);

struct chimera_vfs_request {
    uint32_t opcode;
    uint32_t status;

    union {
        struct {
            const char *path;
            uint8_t    *r_fh;
            uint32_t   *r_fh_len;
        } lookup_path;

        struct {
            const void *fh;
            uint32_t    fh_len;
            const char *component;
            void       *r_fh;
            uint32_t   *r_fh_len;
        } lookup;

        struct {
            const void               *fh;
            uint32_t                  fh_len;
            uint64_t                  attr_mask;
            struct chimera_vfs_attrs *r_attr;
        } getattr;

        struct {
            const void                    *fh;
            uint32_t                       fh_len;
            uint64_t                       cookie;
            chimera_vfs_readdir_callback_t readdir_cb;
            void                          *readdir_cb_arg;
        } readdir;

        struct {
            const void *fh;
            uint32_t    fh_len;
            void       *linkdata;
            uint32_t   *linkdata_length;
        } readlink;

        struct {
            const void *fh;
            uint32_t    fh_len;
            const char *name;
            void       *r_fh;
            uint32_t   *r_fh_len;
        } mkdir;

        struct {
            const void *fh;
            uint32_t    fh_len;
            uint32_t    flags;
            uint64_t   *vfs_private;
        } open;

        struct {
            const void *parent_fh;
            uint32_t    parent_fh_len;
            void       *fh;
            uint32_t   *fh_len;
            const char *name;
            uint32_t    flags;
            uint32_t    mode;
            uint64_t   *vfs_private;
        } open_at;

        struct {
            uint64_t vfs_private;
        } close;

        struct {
            uint64_t  vfs_private;
            void     *buffer;
            uint64_t  offset;
            uint32_t  length;
            uint32_t *result_length;
            uint32_t *result_eof;
        } read;

        struct {
            uint64_t    vfs_private;
            const void *buffer;
            uint64_t    offset;
            uint32_t    length;
            uint32_t   *result_length;
        } write;

        struct {
            const void *fh;
            uint32_t    fh_len;
            const char *name;
        } remove;

    };

    chimera_vfs_complete_callback_t complete_cb;
    void                           *private_data;
};



/* Called once at startup, returns opaque global private data */
typedef void * (*chimera_vfs_init_callback_t)(
    void);

/* Called once at shutdown, destroys private data allowed above */
typedef void (*chimera_vfs_destroy_callback_t)(
    void *private_data);

/* Called once per thread to allocate thread-specific private data,
 * given pointer the global private data allocated above
 */
typedef void * (*chimera_vfs_thread_init_callback_t)(
    void *private_data);

/* Called once at shutdown to destroy thread-specific private data */
typedef void (*chimera_vfs_thread_destroy_callback_t)(
    void *arg);

/* Called to submit a request for processing,
 * recipient should call chimera_vfs_complete(status, req) when complete.
 */
typedef void (*chimera_vfs_dispatch_callback_t)(
    struct chimera_vfs         *vfs,
    struct chimera_vfs_request *request);

/* Each module must have a unique FH_MAGIC value
 * that can never be changed.  They can be reserved
 * here.
 *
 * The 1-byte magic must be the first byte of all
 * file handles returned by the plugin to ensure
 * uniqueness across plugins.
 *
 */

enum CHIMERA_FS_FH_MAGIC {
    CHIMERA_VFS_FH_MAGIC_RESERVED = 0,
    CHIMERA_VFS_FH_MAGIC_LINUX    = 1,
};

struct chimera_vfs_module {
    const char                           *name;
    uint8_t                               fh_magic;
    chimera_vfs_init_callback_t           init_cb;
    chimera_vfs_destroy_callback_t        destroy_cb;
    chimera_vfs_thread_init_callback_t    thread_init_cb;
    chimera_vfs_thread_destroy_callback_t thread_destroy_cb;
    chimera_vfs_dispatch_callback_t       dispatch_cb;
};

struct chimera_vfs_share {
    struct chimera_vfs_module *vfs;
    const char                *name;
    const char                *path;
};

struct chimera_vfs {
    struct chimera_vfs_module *modules;
    struct chimera_vfs_share  *shares;
    int                        nmodules;
    int                        nshares;
};

struct chimera_vfs *
chimera_vfs_init(
    void);

void
chimera_vfs_destroy(
    struct chimera_vfs *vfs);

void
chimera_vfs_getrootfh(
    struct chimera_vfs *vfs,
    void               *fh,
    int                *fh_len);

typedef void (*chimera_vfs_lookup_callback_t)(
    int         error_code,
    const void *fh,
    int         fh_len,
    void       *private_data);

void
chimera_vfs_lookup(
    struct chimera_vfs           *vfs,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data);
