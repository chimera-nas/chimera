#pragma once
#include <stdint.h>
#include <sys/time.h>
#include "vfs_dump.h"
#include "vfs_error.h"

struct evpl;

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

    uint8_t         va_fh[CHIMERA_VFS_FH_SIZE];
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
    struct chimera_vfs_request *request);

typedef int (*chimera_vfs_readdir_callback_t)(
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg);

struct chimera_vfs_open_handle {
    struct chimera_vfs_module *vfs_module;
    uint64_t                   vfs_private;
};

struct chimera_vfs_request {
    struct chimera_vfs_thread      *thread;
    uint32_t                        opcode;
    enum chimera_vfs_error          status;
    chimera_vfs_complete_callback_t complete;
    void                           *proto_callback;
    void                           *proto_private_data;
    struct chimera_vfs_request     *prev;
    struct chimera_vfs_request     *next;

    union {
        struct {
            const char *path;
            uint32_t    pathlen;
            uint8_t     r_fh[CHIMERA_VFS_FH_SIZE];
            uint32_t    r_fh_len;
        } lookup_path;

        struct {
            const void *fh;
            uint32_t    fh_len;
            const char *component;
            uint32_t    component_len;
            uint8_t     r_fh[CHIMERA_VFS_FH_SIZE];
            uint32_t    r_fh_len;
        } lookup;

        struct {
            const void              *fh;
            uint32_t                 fh_len;
            uint64_t                 attr_mask;
            struct chimera_vfs_attrs r_attr;
        } getattr;

        struct {
            const void                    *fh;
            uint32_t                       fh_len;
            uint64_t                       attrmask;
            uint64_t                       cookie;
            uint64_t                       r_cookie;
            uint32_t                       r_eof;
            chimera_vfs_readdir_callback_t callback;
        } readdir;

        struct {
            const void *fh;
            uint32_t    fh_len;
            void       *linkdata;
            uint32_t   *linkdata_length;
        } readlink;

        struct {
            const void  *fh;
            uint32_t     fh_len;
            const char  *name;
            uint32_t     name_len;
            unsigned int mode;
            uint8_t      r_fh[CHIMERA_VFS_FH_SIZE];
            uint32_t     r_fh_len;
        } mkdir;

        struct {
            const void *fh;
            uint32_t    fh_len;
            uint32_t    flags;
            uint64_t    vfs_private;
        } open;

        struct {
            const void                    *parent_fh;
            uint32_t                       parent_fh_len;
            uint8_t                        fh[CHIMERA_VFS_FH_SIZE];
            uint32_t                       fh_len;
            const char                    *name;
            int                            namelen;
            uint32_t                       flags;
            uint32_t                       mode;
            struct chimera_vfs_open_handle handle;
        } open_at;

        struct {
            struct chimera_vfs_open_handle *handle;
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
            int         namelen;
        } remove;

    };
};




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
    /* Reserved for internal use by chimera */
    CHIMERA_VFS_FH_MAGIC_ROOT  = 0,
    CHIMERA_VFS_FH_MAGIC_MEMFS = 1,
    CHIMERA_VFS_FH_MAGIC_LINUX = 2,
    CHIMERA_VFS_FH_MAGIC_MAX   = 3
};

struct chimera_vfs_module {
    /* Required
     * Short name for the module to be used in creating shares
     */

    const char *name;

    /* Required
     * Set to CHIMERA_FS_FH_MAGIC value reserved above
     */

    uint8_t     fh_magic;

    /* Required
     * See dispatch function description below
     */

    uint8_t     blocking;

    /* Optional
     * Called once at initialization to setup global state
     * Return a pointer to global state structure
     */

    void      * (*init)(
        void);

    /* Optional
     * Called once at destruction to clean up global state
     * returned from the init function
     */

    void        (*destroy)(
        void *);

    /* Optional
     * Called once per thread at initialization to setup per-thread state
     * Receives global state pointer as an argument
     * Return a pointer to per-thread state structure
     */

    void      * (*thread_init)(
        struct evpl *evpl,
        void        *private_data);

    /* Optional
     * Called once per thread at destruction to clean up per-thread state
     * Receives per-thread state pointer as an argument
     */

    void        (*thread_destroy)(
        void *);

    /* Required
     * Called to dispatch a request to the module
     * Receives request and per-thread state pointer as an argument
     *
     * Module shuold call request->complete(request) when the
     * request processing is completed.
     *
     * If dispatch logic is blocking, set the blocking flag to 1 above.
     *
     * If blocking flag is unset, requests will be dispatched from
     * chimera's main threadpool, ie the same threadpool that is
     * pumping network traffic.  In this case the dispatch function is
     * expected to quickly complete and then asynchronously make the
     * complete callback later after any underlying slow operations
     * such as I/O have been asynchronously completed.
     *
     * If blocking flag is set, requests will be dispatched from a
     * separate dedicated pool of threads which will expect to process
     * only one request at a time.  The thread handoff adds overhead,
     * but nonetheless this scheme avoids stalling the main network
     * threads due to blocking inside VFS modules.
     *
     * Implementing VFS modules in a non-blocking manner is recommended
     * where feasible.
     */

    void (*dispatch)(
        struct chimera_vfs_request *request,
        void                       *private_data);

};

struct chimera_vfs_share {
    struct chimera_vfs_module *module;
    char                      *name;
    char                      *path;
    struct chimera_vfs_share  *prev;
    struct chimera_vfs_share  *next;
};

struct chimera_vfs {
    struct chimera_vfs_module *modules[CHIMERA_VFS_FH_MAGIC_MAX];
    void                      *module_private[CHIMERA_VFS_FH_MAGIC_MAX];
    struct chimera_vfs_share  *shares;
    struct evpl_threadpool    *syncthreads;
};

struct chimera_vfs_thread {
    struct chimera_vfs         *vfs;
    void                       *module_private[CHIMERA_VFS_FH_MAGIC_MAX];
    struct chimera_vfs_request *free_requests;
};

struct chimera_vfs *
chimera_vfs_init(
    void);

void
chimera_vfs_destroy(
    struct chimera_vfs *vfs);

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs);

void
chimera_vfs_thread_destroy(
    struct chimera_vfs_thread *thread);

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module);

int
chimera_vfs_create_share(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *share_path,
    const char         *module_path);