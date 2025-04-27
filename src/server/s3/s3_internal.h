#pragma once

#include <time.h>

#include "evpl/evpl_http.h"
#include "common/logging.h"
#include "s3_status.h"
#include "vfs/vfs.h"
#include "uthash/utlist.h"

enum chimera_s3_vfs_state {
    CHIMERA_S3_VFS_STATE_INIT,
    CHIMERA_S3_VFS_STATE_RECV,
    CHIMERA_S3_VFS_STATE_RECVED,
    CHIMERA_S3_VFS_STATE_SEND,
    CHIMERA_S3_VFS_STATE_SENT,
    CHIMERA_S3_VFS_STATE_COMPLETE,
};

enum chimera_s3_http_state {
    CHIMERA_S3_HTTP_STATE_INIT,
    CHIMERA_S3_HTTP_STATE_RECVED,
    CHIMERA_S3_HTTP_STATE_SEND,
    CHIMERA_S3_HTTP_STATE_COMPLETE,
};

#define CHIMERA_S3_IOV_MAX 256

struct chimera_server_s3_thread;

struct chimera_s3_io {
    struct chimera_s3_request *request;
    int                        niov;
    struct chimera_s3_io      *next;
    struct evpl_iovec          iov[CHIMERA_S3_IOV_MAX];
};

struct chimera_s3_request {
    enum chimera_s3_status           status;
    enum chimera_s3_vfs_state        vfs_state;
    enum chimera_s3_http_state       http_state;
    const char                      *bucket_name;
    int                              bucket_namelen;
    int                              bucket_fhlen;
    int                              io_pending;
    int                              name_len;
    int                              path_len;
    int                              is_list;
    int64_t                          file_offset;
    int64_t                          file_cur_offset;
    int64_t                          file_length;
    int64_t                          file_real_length;
    int64_t                          file_left;
    uint64_t                         elapsed;
    uint64_t                         etag[2];
    const char                      *path;
    const char                      *name;
    struct evpl_http_request        *http_request;
    struct chimera_server_s3_thread *thread;
    struct chimera_vfs_open_handle  *dir_handle;
    struct chimera_vfs_open_handle  *file_handle;
    struct timespec                  start_time;
    struct timespec                  end_time;
    struct chimera_s3_request       *prev;
    struct chimera_s3_request       *next;
    struct chimera_vfs_attrs         set_attr;
    uint8_t                          bucket_fh[CHIMERA_VFS_FH_SIZE];

    union {
        struct {
            int                      tmp_name_len;
            struct chimera_vfs_attrs set_attr;
            char                     tmp_name[64];
        } put;

        struct {
            char              prefix[256];
            int               prefix_len;
            int               max_keys;
            char             *rp;
            struct evpl_iovec response;
        } list;
    };
};

struct chimera_server_s3_thread {
    struct evpl                     *evpl;
    struct evpl_http_agent          *agent;
    struct evpl_http_server         *server;
    struct chimera_server_s3_shared *shared;
    struct chimera_vfs_thread       *vfs;
    struct chimera_s3_request       *free_requests;
    struct chimera_s3_io            *free_ios;
};

struct chimera_server_s3_shared {
    struct s3_bucket_map *bucket_map;
    struct evpl_endpoint *endpoint;
    struct evpl_listener *listener;
};

static inline struct chimera_s3_io *
chimera_s3_io_alloc(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_s3_io *io = thread->free_ios;

    if (io) {
        LL_DELETE(thread->free_ios, io);
    } else {
        io = calloc(1, sizeof(*io));
    }

    io->request = request;

    return io;
} /* chimera_s3_io_alloc */

static inline void
chimera_s3_io_free(
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_io            *io)
{
    LL_PREPEND(thread->free_ios, io);
} /* chimera_s3_io_free */

static inline int
chimera_s3_format_date(
    char                  *buf,
    size_t                 len,
    const struct timespec *ts)
{
    struct tm tm;

    gmtime_r(&ts->tv_sec, &tm);

    int       ret = strftime(buf, len, "%Y-%m-%dT%H:%M:%S", &tm);
    if (ret > 0) {
        ret += snprintf(buf + ret, len - ret, ".%03ldZ", ts->tv_nsec / 1000000);
    }
    return ret;

} /* chimera_s3_format_date */

void
s3_server_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

#define chimera_s3_debug(...) chimera_debug("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_info(...)  chimera_info("s3", \
                                           __FILE__, \
                                           __LINE__, \
                                           __VA_ARGS__)
#define chimera_s3_error(...) chimera_error("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_fatal(...) chimera_fatal("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_s3_abort(...) chimera_abort("s3", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)

#define chimera_s3_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "s3", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

#define chimera_s3_abort_if(cond, ...) \
        chimera_abort_if(cond, "s3", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)